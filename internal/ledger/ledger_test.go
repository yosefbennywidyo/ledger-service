package ledger

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type claimRecord struct {
	status   string // "processing" | "completed"
	respCode int
	respBody []byte
}

type fakeRepo struct {
	balances   map[string]int64
	claims     map[string]*claimRecord
	applied    []Transaction
	outboxRows int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{balances: map[string]int64{}, claims: map[string]*claimRecord{}}
}

func (f *fakeRepo) ClaimIdempotencyKey(_ context.Context, key string) (Claim, error) {
	if rec, ok := f.claims[key]; ok {
		if rec.status == "processing" {
			return Claim{State: ClaimProcessing}, nil
		}
		return Claim{State: ClaimCompleted, ResponseStatus: rec.respCode, ResponseBody: rec.respBody}, nil
	}
	f.claims[key] = &claimRecord{status: "processing"}
	return Claim{State: ClaimAcquired}, nil
}

func (f *fakeRepo) ApplyLocked(_ context.Context, tx Transaction) (int, []byte, error) {
	for _, e := range tx.Entries {
		if e.AmountCents < 0 && f.balances[e.AccountID]+e.AmountCents < 0 {
			body := []byte(fmt.Sprintf(`{"error":"insufficient balance: account %s"}`, e.AccountID))
			f.claims[tx.IdempotencyKey] = &claimRecord{status: "completed", respCode: 422, respBody: body}
			return 422, body, nil
		}
	}
	for _, e := range tx.Entries {
		f.balances[e.AccountID] += e.AmountCents
	}
	f.applied = append(f.applied, tx)
	f.outboxRows++
	body := []byte(`{"status":"completed"}`)
	f.claims[tx.IdempotencyKey] = &claimRecord{status: "completed", respCode: 201, respBody: body}
	return 201, body, nil
}

func TestPost_TableDriven(t *testing.T) {
	cases := []struct {
		name       string
		startBal   int64
		tx         Transaction
		wantStatus int
		wantErr    error
	}{
		{
			name:     "sufficient balance debits and credits",
			startBal: 1000,
			tx: Transaction{IdempotencyKey: "tx-1", Entries: []Entry{
				{AccountID: "alice", AmountCents: -500},
				{AccountID: "bob", AmountCents: 500},
			}},
			wantStatus: 201,
		},
		{
			name:     "insufficient balance rejected with 422, not an error",
			startBal: 100,
			tx: Transaction{IdempotencyKey: "tx-2", Entries: []Entry{
				{AccountID: "alice", AmountCents: -500},
				{AccountID: "bob", AmountCents: 500},
			}},
			wantStatus: 422,
		},
		{
			name:     "unbalanced transaction rejected",
			startBal: 1000,
			tx: Transaction{IdempotencyKey: "tx-3", Entries: []Entry{
				{AccountID: "alice", AmountCents: -500},
				{AccountID: "bob", AmountCents: 400},
			}},
			wantErr: ErrUnbalancedTransaction,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			repo.balances["alice"] = tc.startBal
			svc := NewService(repo)

			status, _, err := svc.Post(context.Background(), tc.tx)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("got err %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if status != tc.wantStatus {
				t.Fatalf("got status %d, want %d", status, tc.wantStatus)
			}
		})
	}
}

func TestPost_IdempotentRetryReplaysOriginalResponse(t *testing.T) {
	repo := newFakeRepo()
	repo.balances["alice"] = 1000
	svc := NewService(repo)
	tx := Transaction{IdempotencyKey: "retry-key", Entries: []Entry{
		{AccountID: "alice", AmountCents: -100},
		{AccountID: "bob", AmountCents: 100},
	}}

	status1, body1, err := svc.Post(context.Background(), tx)
	if err != nil {
		t.Fatalf("first post: %v", err)
	}
	status2, body2, err := svc.Post(context.Background(), tx)
	if err != nil {
		t.Fatalf("retried post: %v", err)
	}

	if status1 != status2 || string(body1) != string(body2) {
		t.Fatalf("replay mismatch: (%d, %s) vs (%d, %s)", status1, body1, status2, body2)
	}
	if got := repo.balances["alice"]; got != 900 {
		t.Fatalf("balance double-applied: got %d, want 900", got)
	}
	if repo.outboxRows != 1 {
		t.Fatalf("expected exactly one outbox row despite the retry, got %d", repo.outboxRows)
	}
}

func TestPost_ConflictWhenKeyStillProcessing(t *testing.T) {
	repo := newFakeRepo()
	repo.claims["in-flight-key"] = &claimRecord{status: "processing"}
	svc := NewService(repo)
	tx := Transaction{IdempotencyKey: "in-flight-key", Entries: []Entry{
		{AccountID: "alice", AmountCents: -100},
		{AccountID: "bob", AmountCents: 100},
	}}

	_, _, err := svc.Post(context.Background(), tx)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got err %v, want ErrConflict", err)
	}
	if len(repo.applied) != 0 {
		t.Fatalf("expected no business transaction to run for a conflicting claim, got %d", len(repo.applied))
	}
}

func TestPost_RejectedBusinessOutcomeIsNotAnError(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)
	tx := Transaction{IdempotencyKey: "reject-key", Entries: []Entry{
		{AccountID: "alice", AmountCents: -500},
		{AccountID: "bob", AmountCents: 500},
	}}

	status, body, err := svc.Post(context.Background(), tx)
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 422 {
		t.Fatalf("got status %d, want 422", status)
	}
	if len(body) == 0 {
		t.Fatal("expected a non-empty rejection body")
	}
}

func TestPost_RejectedOutcomeReplaysOnRetry(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)
	tx := Transaction{IdempotencyKey: "reject-retry-key", Entries: []Entry{
		{AccountID: "alice", AmountCents: -500},
		{AccountID: "bob", AmountCents: 500},
	}}

	status1, body1, err := svc.Post(context.Background(), tx)
	if err != nil {
		t.Fatalf("first post: %v", err)
	}
	// Fund the account between attempts: a real re-run of the balance check
	// would now succeed, but a retry of the same idempotency key must still
	// replay the original 422 rather than re-evaluating the balance.
	repo.balances["alice"] = 1000
	status2, body2, err := svc.Post(context.Background(), tx)
	if err != nil {
		t.Fatalf("retried post: %v", err)
	}

	if status1 != 422 || status2 != 422 {
		t.Fatalf("got statuses %d, %d, want 422, 422", status1, status2)
	}
	if string(body1) != string(body2) {
		t.Fatalf("replay mismatch: %s vs %s", body1, body2)
	}
	if repo.balances["alice"] != 1000 {
		t.Fatalf("retry must not touch the balance: got %d, want 1000 (unchanged by the replay)", repo.balances["alice"])
	}
}
