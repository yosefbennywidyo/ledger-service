// Package ledger implements double-entry balance logic for the core ledger.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"log"
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

// EventPublisher is the async fan-out boundary the ledger service depends on
// after a transaction is durably applied (e.g. fraud-check, notifications,
// reconciliation). Implementations live in internal/kafka; tests can supply
// an in-memory fake.
type EventPublisher interface {
	Publish(ctx context.Context, tx Transaction) error
}

// Service is the ledger domain service.
type Service struct {
	repo      Repository
	publisher EventPublisher
}

// NewService wires a ledger Service to its Repository and EventPublisher.
func NewService(repo Repository, publisher EventPublisher) *Service {
	return &Service{repo: repo, publisher: publisher}
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

	// Postgres is the source of truth and already has the transaction — a
	// publish failure here must not fail the API response, since Kafka is
	// an async, out-of-the-critical-path fan-out.
	if err := s.publisher.Publish(ctx, tx); err != nil {
		log.Printf("publish transaction event for %s: %v", tx.IdempotencyKey, err)
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
