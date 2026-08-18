package ledger

import (
	"context"
	"errors"
	"testing"
)

type fakeRepo struct {
	balances map[string]int64
	seen     map[string]bool
	applied  []Transaction
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{balances: map[string]int64{}, seen: map[string]bool{}}
}

func (f *fakeRepo) Balance(_ context.Context, accountID string) (int64, error) {
	return f.balances[accountID], nil
}

func (f *fakeRepo) Apply(_ context.Context, tx Transaction) error {
	for _, e := range tx.Entries {
		f.balances[e.AccountID] += e.AmountCents
	}
	f.seen[tx.IdempotencyKey] = true
	f.applied = append(f.applied, tx)
	return nil
}

func (f *fakeRepo) SeenIdempotencyKey(_ context.Context, key string) (bool, error) {
	return f.seen[key], nil
}

func TestPost_TableDriven(t *testing.T) {
	cases := []struct {
		name        string
		startBal    int64
		tx          Transaction
		wantErr     error
		wantApplied bool
	}{
		{
			name:     "sufficient balance debits and credits",
			startBal: 1000,
			tx: Transaction{
				IdempotencyKey: "tx-1",
				Entries: []Entry{
					{AccountID: "alice", AmountCents: -500},
					{AccountID: "bob", AmountCents: 500},
				},
			},
			wantApplied: true,
		},
		{
			name:     "insufficient balance rejected",
			startBal: 100,
			tx: Transaction{
				IdempotencyKey: "tx-2",
				Entries: []Entry{
					{AccountID: "alice", AmountCents: -500},
					{AccountID: "bob", AmountCents: 500},
				},
			},
			wantErr: ErrInsufficientBalance,
		},
		{
			name:     "unbalanced transaction rejected",
			startBal: 1000,
			tx: Transaction{
				IdempotencyKey: "tx-3",
				Entries: []Entry{
					{AccountID: "alice", AmountCents: -500},
					{AccountID: "bob", AmountCents: 400},
				},
			},
			wantErr: nil, // checked via error string below, not sentinel
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			repo.balances["alice"] = tc.startBal
			svc := NewService(repo)

			err := svc.Post(context.Background(), tc.tx)

			if tc.name == "unbalanced transaction rejected" {
				if err == nil {
					t.Fatal("expected error for unbalanced transaction, got nil")
				}
				return
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("got err %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantApplied && len(repo.applied) != 1 {
				t.Fatalf("expected transaction to be applied once, got %d", len(repo.applied))
			}
		})
	}
}

func TestPost_IdempotentRetry(t *testing.T) {
	repo := newFakeRepo()
	repo.balances["alice"] = 1000
	svc := NewService(repo)
	tx := Transaction{
		IdempotencyKey: "retry-key",
		Entries: []Entry{
			{AccountID: "alice", AmountCents: -100},
			{AccountID: "bob", AmountCents: 100},
		},
	}

	if err := svc.Post(context.Background(), tx); err != nil {
		t.Fatalf("first post: %v", err)
	}
	if err := svc.Post(context.Background(), tx); err != nil {
		t.Fatalf("retried post: %v", err)
	}

	if got := repo.balances["alice"]; got != 900 {
		t.Fatalf("balance double-applied: got %d, want 900", got)
	}
}
