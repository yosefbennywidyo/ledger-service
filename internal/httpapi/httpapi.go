// Package httpapi exposes the ledger service over HTTP for internal callers
// (e.g. the payment gateway), using the standard library only.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"ledger-rail/ledger-service/internal/ledger"
)

// Server wires HTTP routes to the ledger domain service.
type Server struct {
	svc *ledger.Service
	mux *http.ServeMux
}

// NewServer builds a Server ready to be used as an http.Handler.
func NewServer(svc *ledger.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /transactions", s.handlePostTransaction)
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

// writeJSONError answers a structured JSON error body instead of plain text,
// so callers (payment-gateway, and bff after it) can always parse a
// ledger-service error response as JSON rather than getting a null body on
// a failed .json() parse of plain text.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
