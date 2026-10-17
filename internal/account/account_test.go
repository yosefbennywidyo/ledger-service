package account

import (
	"context"
	"encoding/json"
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
	customers      map[string]Customer
	accounts       map[string]Account
	claims         map[string]*claimRecord
	entries        map[string][]Entry
	nextCustomerID int
	nextAccountID  int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		customers: map[string]Customer{},
		accounts:  map[string]Account{},
		claims:    map[string]*claimRecord{},
		entries:   map[string][]Entry{},
	}
}

func (f *fakeRepo) ClaimKey(_ context.Context, key string) (Claim, error) {
	if rec, ok := f.claims[key]; ok {
		if rec.status == "processing" {
			return Claim{State: ClaimProcessing}, nil
		}
		return Claim{State: ClaimCompleted, ResponseStatus: rec.respCode, ResponseBody: rec.respBody}, nil
	}
	f.claims[key] = &claimRecord{status: "processing"}
	return Claim{State: ClaimAcquired}, nil
}

// complete records status/body as f's real postgres counterpart would in the
// same transaction as the write — the fake has no concept of a crash between
// the two, so it can't exercise the atomicity fix directly, only the
// observable behavior (business rejections still complete the claim; the new
// 422s).
func (f *fakeRepo) complete(key string, status int, body []byte) (int, []byte, error) {
	f.claims[key] = &claimRecord{status: "completed", respCode: status, respBody: body}
	return status, body, nil
}

func (f *fakeRepo) CreateCustomerClaimed(_ context.Context, idempotencyKey, name string) (int, []byte, error) {
	f.nextCustomerID++
	c := Customer{ID: fmt.Sprintf("cust-%d", f.nextCustomerID), Name: name}
	f.customers[c.ID] = c
	body, _ := json.Marshal(map[string]any{"id": c.ID, "name": c.Name, "created_at": c.CreatedAt})
	return f.complete(idempotencyKey, 201, body)
}

func (f *fakeRepo) CreateAccountClaimed(_ context.Context, idempotencyKey, customerID string) (int, []byte, error) {
	if _, ok := f.customers[customerID]; !ok {
		return f.complete(idempotencyKey, 404, []byte(`{"error":"customer not found"}`))
	}
	f.nextAccountID++
	a := Account{ID: fmt.Sprintf("acc-%d", f.nextAccountID), CustomerID: customerID, Kind: KindCustomer, Status: StatusActive}
	f.accounts[a.ID] = a
	body, _ := json.Marshal(map[string]any{"id": a.ID, "customer_id": a.CustomerID, "kind": a.Kind, "status": a.Status})
	return f.complete(idempotencyKey, 201, body)
}

func (f *fakeRepo) CloseAccountClaimed(_ context.Context, idempotencyKey, accountID string) (int, []byte, error) {
	a, ok := f.accounts[accountID]
	if !ok {
		return f.complete(idempotencyKey, 404, []byte(`{"error":"account not found"}`))
	}
	if a.Kind == KindSystem {
		return f.complete(idempotencyKey, 422, []byte(`{"error":"system account cannot be closed"}`))
	}
	var balance int64
	for _, e := range f.entries[accountID] {
		balance += e.AmountCents
	}
	if balance != 0 {
		return f.complete(idempotencyKey, 422, []byte(`{"error":"account has a non-zero balance"}`))
	}
	a.Status = StatusClosed
	f.accounts[accountID] = a
	body, _ := json.Marshal(map[string]any{"id": accountID, "status": "closed"})
	return f.complete(idempotencyKey, 200, body)
}

func (f *fakeRepo) GetAccount(_ context.Context, accountID string) (Account, error) {
	a, ok := f.accounts[accountID]
	if !ok {
		return Account{}, ErrAccountNotFound
	}
	return a, nil
}

func (f *fakeRepo) ListEntries(_ context.Context, accountID string, _, _ int) ([]Entry, int, error) {
	if _, ok := f.accounts[accountID]; !ok {
		return nil, 0, ErrAccountNotFound
	}
	es := f.entries[accountID]
	return es, len(es), nil
}

func TestCreateCustomer_Success(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)

	status, body, err := svc.CreateCustomer(context.Background(), "Alice", "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 201 {
		t.Fatalf("got status %d, want 201", status)
	}
	var resp struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if resp.Name != "Alice" {
		t.Fatalf("got name %q, want Alice", resp.Name)
	}
	if resp.ID == "" {
		t.Fatal("expected a non-empty id")
	}
}

func TestCreateCustomer_IdempotentRetryReplays(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)

	status1, body1, err := svc.CreateCustomer(context.Background(), "Alice", "key-1")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	status2, body2, err := svc.CreateCustomer(context.Background(), "Alice", "key-1")
	if err != nil {
		t.Fatalf("retried create: %v", err)
	}

	if status1 != status2 || string(body1) != string(body2) {
		t.Fatalf("replay mismatch: (%d, %s) vs (%d, %s)", status1, body1, status2, body2)
	}
	if len(repo.customers) != 1 {
		t.Fatalf("expected exactly one customer, got %d", len(repo.customers))
	}
}

func TestCreateAccount_CustomerNotFound(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)

	status, body, err := svc.CreateAccount(context.Background(), "missing-customer", "key-1")
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 404 {
		t.Fatalf("got status %d, want 404", status)
	}
	if string(body) != `{"error":"customer not found"}` {
		t.Fatalf("got body %s", body)
	}
}

func TestCreateAccount_Success(t *testing.T) {
	repo := newFakeRepo()
	repo.customers["cust-1"] = Customer{ID: "cust-1", Name: "Alice"}
	svc := NewService(repo)

	status, body, err := svc.CreateAccount(context.Background(), "cust-1", "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 201 {
		t.Fatalf("got status %d, want 201", status)
	}
	var resp struct {
		Kind   string `json:"kind"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if resp.Kind != "customer" || resp.Status != "active" {
		t.Fatalf("got kind=%q status=%q, want customer/active", resp.Kind, resp.Status)
	}
}

func TestCloseAccount_NotFound(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)

	status, body, err := svc.CloseAccount(context.Background(), "missing", "key-1")
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 404 {
		t.Fatalf("got status %d, want 404", status)
	}
	if string(body) != `{"error":"account not found"}` {
		t.Fatalf("got body %s", body)
	}
}

func TestCloseAccount_Success(t *testing.T) {
	repo := newFakeRepo()
	repo.accounts["acc-1"] = Account{ID: "acc-1", Status: StatusActive}
	svc := NewService(repo)

	status, body, err := svc.CloseAccount(context.Background(), "acc-1", "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Fatalf("got status %d, want 200", status)
	}
	var resp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if resp.Status != "closed" {
		t.Fatalf("got status %q, want closed", resp.Status)
	}
}

func TestGetAccount_NotFound(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)

	status, body, err := svc.GetAccount(context.Background(), "missing")
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 404 {
		t.Fatalf("got status %d, want 404", status)
	}
	if string(body) != `{"error":"account not found"}` {
		t.Fatalf("got body %s", body)
	}
}

func TestGetAccount_Success(t *testing.T) {
	repo := newFakeRepo()
	repo.accounts["acc-1"] = Account{
		ID: "acc-1", CustomerID: "cust-1", Kind: KindCustomer, Status: StatusActive, BalanceCents: 1500,
	}
	svc := NewService(repo)

	status, body, err := svc.GetAccount(context.Background(), "acc-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Fatalf("got status %d, want 200", status)
	}
	var resp struct {
		BalanceCents int64 `json:"balance_cents"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if resp.BalanceCents != 1500 {
		t.Fatalf("got balance_cents %d, want 1500", resp.BalanceCents)
	}
}

func TestListTransactions_NotFound(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo)

	status, body, err := svc.ListTransactions(context.Background(), "missing", 1, 20)
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 404 {
		t.Fatalf("got status %d, want 404", status)
	}
	if string(body) != `{"error":"account not found"}` {
		t.Fatalf("got body %s", body)
	}
}

func TestListTransactions_Success(t *testing.T) {
	repo := newFakeRepo()
	repo.accounts["acc-1"] = Account{ID: "acc-1"}
	repo.entries["acc-1"] = []Entry{{AccountID: "acc-1", AmountCents: 100}}
	svc := NewService(repo)

	status, body, err := svc.ListTransactions(context.Background(), "acc-1", 1, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Fatalf("got status %d, want 200", status)
	}
	var resp struct {
		Page  int `json:"page"`
		Limit int `json:"limit"`
		Total int `json:"total"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if resp.Page != 1 || resp.Limit != 20 || resp.Total != 1 {
		t.Fatalf("got page=%d limit=%d total=%d, want 1/20/1", resp.Page, resp.Limit, resp.Total)
	}
}

func TestCreateAccount_ConflictWhenKeyStillProcessing(t *testing.T) {
	repo := newFakeRepo()
	repo.claims["in-flight-key"] = &claimRecord{status: "processing"}
	svc := NewService(repo)

	_, _, err := svc.CreateAccount(context.Background(), "cust-1", "in-flight-key")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got err %v, want ErrConflict", err)
	}
}

func TestCloseAccount_SystemAccountRejected(t *testing.T) {
	repo := newFakeRepo()
	repo.accounts["system:cash"] = Account{ID: "system:cash", Kind: KindSystem, Status: StatusActive}
	svc := NewService(repo)

	status, body, err := svc.CloseAccount(context.Background(), "system:cash", "key-1")
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 422 {
		t.Fatalf("got status %d, want 422", status)
	}
	if string(body) != `{"error":"system account cannot be closed"}` {
		t.Fatalf("got body %s", body)
	}
	if repo.accounts["system:cash"].Status != StatusActive {
		t.Fatal("system:cash must remain active")
	}
}

func TestCloseAccount_NonZeroBalanceRejected(t *testing.T) {
	repo := newFakeRepo()
	repo.accounts["acc-1"] = Account{ID: "acc-1", Kind: KindCustomer, Status: StatusActive}
	repo.entries["acc-1"] = []Entry{{AccountID: "acc-1", AmountCents: 500}}
	svc := NewService(repo)

	status, body, err := svc.CloseAccount(context.Background(), "acc-1", "key-1")
	if err != nil {
		t.Fatalf("a business rejection must not be a Go error: %v", err)
	}
	if status != 422 {
		t.Fatalf("got status %d, want 422", status)
	}
	if string(body) != `{"error":"account has a non-zero balance"}` {
		t.Fatalf("got body %s", body)
	}
	if repo.accounts["acc-1"].Status != StatusActive {
		t.Fatal("acc-1 must remain active")
	}
}
