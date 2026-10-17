// Package account implements customer and account management: opening
// accounts, closing them, and reading balance/mutation history. It mirrors
// the ledger package's shape — a Service wrapping a Repository, returning
// (status, body, err) ready for httpapi to write out.
package account

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Kind distinguishes a customer-owned account from the system account that
// funds top-ups.
type Kind string

const (
	KindCustomer Kind = "customer"
	KindSystem   Kind = "system"
)

// Status controls whether an account may still be used in a transaction.
type Status string

const (
	StatusActive Status = "active"
	StatusClosed Status = "closed"
)

type Customer struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

type Account struct {
	ID           string
	CustomerID   string
	Kind         Kind
	Status       Status
	CreatedAt    time.Time
	BalanceCents int64
}

// Entry is one leg of a ledger transaction, as seen from a single account.
type Entry struct {
	AccountID   string
	AmountCents int64
	CreatedAt   time.Time
}

var (
	ErrCustomerNotFound = errors.New("customer not found")
	ErrAccountNotFound  = errors.New("account not found")
	ErrConflict         = errors.New("idempotency key still processing")
)

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

// Repository is the persistence boundary Service depends on. The *Claimed
// methods run the claim-completion and the actual write in one atomic unit
// (mirroring ledger.Repository.ApplyLocked's own claim-row-lock pattern) —
// unlike an earlier version of this interface, a crash between "do the
// write" and "mark the claim completed" can no longer leave an orphaned
// customer/account behind for a reclaim to duplicate.
type Repository interface {
	ClaimKey(ctx context.Context, key string) (Claim, error)
	CreateCustomerClaimed(ctx context.Context, idempotencyKey, name string) (status int, body []byte, err error)
	CreateAccountClaimed(ctx context.Context, idempotencyKey, customerID string) (status int, body []byte, err error)
	CloseAccountClaimed(ctx context.Context, idempotencyKey, accountID string) (status int, body []byte, err error)
	GetAccount(ctx context.Context, accountID string) (Account, error)
	ListEntries(ctx context.Context, accountID string, offset, limit int) (entries []Entry, total int, err error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func errorBody(message string) []byte {
	b, _ := json.Marshal(map[string]string{"error": message})
	return b
}

// claimedRequest runs run only when idempotencyKey isn't already claimed,
// replaying the stored response on a completed claim and returning
// ErrConflict on an in-flight one — the same phase-1 check ledger.Service.Post
// makes before calling ApplyLocked.
func (s *Service) claimedRequest(
	ctx context.Context,
	idempotencyKey string,
	run func() (int, []byte, error),
) (int, []byte, error) {
	claim, err := s.repo.ClaimKey(ctx, idempotencyKey)
	if err != nil {
		return 0, nil, fmt.Errorf("claim idempotency key: %w", err)
	}
	switch claim.State {
	case ClaimCompleted:
		return claim.ResponseStatus, claim.ResponseBody, nil
	case ClaimProcessing:
		return 0, nil, ErrConflict
	}
	return run()
}

func (s *Service) CreateCustomer(ctx context.Context, name, idempotencyKey string) (int, []byte, error) {
	return s.claimedRequest(ctx, idempotencyKey, func() (int, []byte, error) {
		return s.repo.CreateCustomerClaimed(ctx, idempotencyKey, name)
	})
}

func (s *Service) CreateAccount(ctx context.Context, customerID, idempotencyKey string) (int, []byte, error) {
	return s.claimedRequest(ctx, idempotencyKey, func() (int, []byte, error) {
		return s.repo.CreateAccountClaimed(ctx, idempotencyKey, customerID)
	})
}

func (s *Service) CloseAccount(ctx context.Context, accountID, idempotencyKey string) (int, []byte, error) {
	return s.claimedRequest(ctx, idempotencyKey, func() (int, []byte, error) {
		return s.repo.CloseAccountClaimed(ctx, idempotencyKey, accountID)
	})
}

func (s *Service) GetAccount(ctx context.Context, accountID string) (int, []byte, error) {
	acc, err := s.repo.GetAccount(ctx, accountID)
	if errors.Is(err, ErrAccountNotFound) {
		return 404, errorBody("account not found"), nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get account: %w", err)
	}
	body, err := json.Marshal(map[string]any{
		"id":            acc.ID,
		"customer_id":   acc.CustomerID,
		"kind":          acc.Kind,
		"status":        acc.Status,
		"balance_cents": acc.BalanceCents,
	})
	if err != nil {
		return 0, nil, fmt.Errorf("marshal account: %w", err)
	}
	return 200, body, nil
}

type entryJSON struct {
	AccountID   string    `json:"account_id"`
	AmountCents int64     `json:"amount_cents"`
	CreatedAt   time.Time `json:"created_at"`
}

func (s *Service) ListTransactions(ctx context.Context, accountID string, page, limit int) (int, []byte, error) {
	offset := (page - 1) * limit
	entries, total, err := s.repo.ListEntries(ctx, accountID, offset, limit)
	if errors.Is(err, ErrAccountNotFound) {
		return 404, errorBody("account not found"), nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("list entries: %w", err)
	}
	items := make([]entryJSON, 0, len(entries))
	for _, e := range entries {
		items = append(items, entryJSON{AccountID: e.AccountID, AmountCents: e.AmountCents, CreatedAt: e.CreatedAt})
	}
	body, err := json.Marshal(map[string]any{
		"entries": items,
		"page":    page,
		"limit":   limit,
		"total":   total,
	})
	if err != nil {
		return 0, nil, fmt.Errorf("marshal entries: %w", err)
	}
	return 200, body, nil
}
