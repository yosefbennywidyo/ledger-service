// Package ledger implements double-entry balance logic for the core ledger.
package ledger

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrUnbalancedTransaction = errors.New("unbalanced transaction")
	ErrConflict              = errors.New("idempotency key still processing")
)

// Entry is one leg of a double-entry transaction.
type Entry struct {
	AccountID   string
	AmountCents int64 // positive = credit, negative = debit
}

// Transaction is a balanced set of entries — the sum of AmountCents must be zero.
type Transaction struct {
	IdempotencyKey string
	Entries        []Entry
}

type ClaimState int

const (
	ClaimAcquired ClaimState = iota
	ClaimCompleted
	ClaimProcessing
)

// Claim is the outcome of attempting to claim an idempotency key.
// ResponseStatus/ResponseBody are only populated when State is ClaimCompleted.
type Claim struct {
	State          ClaimState
	ResponseStatus int
	ResponseBody   []byte
}

// Repository is the persistence boundary the ledger service depends on.
type Repository interface {
	// ClaimIdempotencyKey atomically claims key for processing, or reports
	// that it's already completed (with the stored response to replay) or
	// still genuinely in flight.
	ClaimIdempotencyKey(ctx context.Context, key string) (Claim, error)
	// ApplyLocked locks every account tx touches, checks balances, and
	// atomically persists the outcome — writing ledger_entries and an
	// outbox row on success, or just the rejection on insufficient balance.
	// A business rejection is returned as (422, body, nil), never as err;
	// err is reserved for unexpected failures (e.g. the database is down).
	ApplyLocked(ctx context.Context, tx Transaction) (status int, body []byte, err error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// Post validates, claims the idempotency key, and applies a balanced
// transaction. Returns the HTTP status and body httpapi should send.
func (s *Service) Post(ctx context.Context, tx Transaction) (status int, body []byte, err error) {
	if verr := validateBalanced(tx); verr != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrUnbalancedTransaction, verr)
	}

	claim, err := s.repo.ClaimIdempotencyKey(ctx, tx.IdempotencyKey)
	if err != nil {
		return 0, nil, fmt.Errorf("claim idempotency key: %w", err)
	}
	switch claim.State {
	case ClaimCompleted:
		return claim.ResponseStatus, claim.ResponseBody, nil
	case ClaimProcessing:
		return 0, nil, ErrConflict
	}

	return s.repo.ApplyLocked(ctx, tx)
}

func validateBalanced(tx Transaction) error {
	var sum int64
	for _, e := range tx.Entries {
		sum += e.AmountCents
	}
	if sum != 0 {
		return fmt.Errorf("entries sum to %d, want 0", sum)
	}
	return nil
}
