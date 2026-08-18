// Package httpapi exposes the ledger service over HTTP for internal callers
// (e.g. the payment gateway), using the standard library only.
package httpapi

import (
	"encoding/json"
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
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	tx := ledger.Transaction{IdempotencyKey: req.IdempotencyKey}
	for _, e := range req.Entries {
		tx.Entries = append(tx.Entries, ledger.Entry{AccountID: e.AccountID, AmountCents: e.AmountCents})
	}

	if err := s.svc.Post(r.Context(), tx); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
