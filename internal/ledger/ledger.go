// Package ledger implements double-entry balance logic for the core ledger.
package ledger

import (
	"context"
	"errors"
	"fmt"
)

// ErrInsufficientBalance is returned when a debit would take an account below zero.
var ErrInsufficientBalance = errors.New("insufficient balance")

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

// Repository is the persistence boundary the ledger service depends on.
// Implementations live in internal/postgres; tests can supply an in-memory fake.
type Repository interface {
	Balance(ctx context.Context, accountID string) (int64, error)
	Apply(ctx context.Context, tx Transaction) error
	SeenIdempotencyKey(ctx context.Context, key string) (bool, error)
}

// Service is the ledger domain service.
type Service struct {
	repo Repository
}

// NewService wires a ledger Service to its Repository.
func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// Post validates and applies a balanced transaction.
func (s *Service) Post(ctx context.Context, tx Transaction) error {
	if err := validateBalanced(tx); err != nil {
		return err
	}

	seen, err := s.repo.SeenIdempotencyKey(ctx, tx.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("check idempotency key: %w", err)
	}
	if seen {
		return nil // already applied — safe to no-op on retry
	}

	for _, e := range tx.Entries {
		if e.AmountCents >= 0 {
			continue
		}
		balance, err := s.repo.Balance(ctx, e.AccountID)
		if err != nil {
			return fmt.Errorf("read balance for %s: %w", e.AccountID, err)
		}
		if balance+e.AmountCents < 0 {
			return fmt.Errorf("%w: account %s", ErrInsufficientBalance, e.AccountID)
		}
	}

	if err := s.repo.Apply(ctx, tx); err != nil {
		return fmt.Errorf("apply transaction: %w", err)
	}
	return nil
}

func validateBalanced(tx Transaction) error {
	var sum int64
	for _, e := range tx.Entries {
		sum += e.AmountCents
	}
	if sum != 0 {
		return fmt.Errorf("unbalanced transaction: entries sum to %d, want 0", sum)
	}
	return nil
}
