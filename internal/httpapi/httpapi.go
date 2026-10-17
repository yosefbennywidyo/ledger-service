// Package httpapi exposes the ledger service over HTTP for internal callers
// (e.g. the payment gateway), using the standard library only.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"

	"ledger-rail/ledger-service/internal/account"
	"ledger-rail/ledger-service/internal/ledger"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Server wires HTTP routes to the ledger domain service.
type Server struct {
	svc        *ledger.Service
	accountSvc *account.Service
	mux        *http.ServeMux
}

// NewServer builds a Server ready to be used as an http.Handler.
func NewServer(svc *ledger.Service, accountSvc *account.Service) *Server {
	s := &Server{svc: svc, accountSvc: accountSvc, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /transactions", s.handlePostTransaction)
	s.mux.HandleFunc("POST /customers", s.handlePostCustomer)
	s.mux.HandleFunc("POST /accounts", s.handlePostAccount)
	s.mux.HandleFunc("GET /accounts/{id}", s.handleGetAccount)
	s.mux.HandleFunc("GET /accounts/{id}/transactions", s.handleGetAccountTransactions)
	s.mux.HandleFunc("POST /accounts/{id}/close", s.handlePostAccountClose)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

type postTransactionRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	Entries        []struct {
		AccountID   string `json:"account_id"`
		AmountCents int64  `json:"amount_cents"`
	} `json:"entries"`
}

func (s *Server) handlePostTransaction(w http.ResponseWriter, r *http.Request) {
	var req postTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	tx := ledger.Transaction{IdempotencyKey: req.IdempotencyKey}
	for _, e := range req.Entries {
		tx.Entries = append(tx.Entries, ledger.Entry{AccountID: e.AccountID, AmountCents: e.AmountCents})
	}

	status, body, err := s.svc.Post(r.Context(), tx)
	if err != nil {
		switch {
		case errors.Is(err, ledger.ErrUnbalancedTransaction):
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, ledger.ErrConflict):
			writeJSONError(w, http.StatusConflict, err.Error())
		default:
			writeJSONError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

type postCustomerRequest struct {
	Name           string `json:"name"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handlePostCustomer(w http.ResponseWriter, r *http.Request) {
	var req postCustomerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.IdempotencyKey == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	status, body, err := s.accountSvc.CreateCustomer(r.Context(), req.Name, req.IdempotencyKey)
	writeAccountResult(w, status, body, err)
}

type postAccountRequest struct {
	CustomerID     string `json:"customer_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handlePostAccount(w http.ResponseWriter, r *http.Request) {
	var req postAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CustomerID == "" || req.IdempotencyKey == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if !uuidPattern.MatchString(req.CustomerID) {
		writeJSONError(w, http.StatusBadRequest, "customer_id must be a UUID")
		return
	}

	status, body, err := s.accountSvc.CreateAccount(r.Context(), req.CustomerID, req.IdempotencyKey)
	writeAccountResult(w, status, body, err)
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	status, body, err := s.accountSvc.GetAccount(r.Context(), id)
	writeAccountResult(w, status, body, err)
}

func (s *Server) handleGetAccountTransactions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	page := 1
	if v := r.URL.Query().Get("page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid page")
			return
		}
		page = n
	}

	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = n
	}
	if limit > 100 {
		limit = 100
	}

	status, body, err := s.accountSvc.ListTransactions(r.Context(), id, page, limit)
	writeAccountResult(w, status, body, err)
}

type postAccountCloseRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handlePostAccountClose(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req postAccountCloseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IdempotencyKey == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	status, body, err := s.accountSvc.CloseAccount(r.Context(), id, req.IdempotencyKey)
	writeAccountResult(w, status, body, err)
}

// writeAccountResult writes an account.Service result the way
// handlePostTransaction writes a ledger.Service result: a business outcome
// (status, body) is written through as-is, and only a Go error — a
// still-processing idempotency key, or an unexpected failure — gets special
// handling.
func writeAccountResult(w http.ResponseWriter, status int, body []byte, err error) {
	if err != nil {
		if errors.Is(err, account.ErrConflict) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

// writeJSONError answers a structured JSON error body instead of plain text,
// so callers (payment-gateway, and bff after it) can always parse a
// ledger-service error response as JSON rather than getting a null body on
// a failed .json() parse of plain text.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
