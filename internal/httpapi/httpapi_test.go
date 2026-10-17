package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ledger-rail/ledger-service/internal/account"
)

// fakeAccountRepo is a minimal account.Repository that fails the test if any
// write method is invoked — this file only tests validation that must
// reject a request before it ever reaches the repository.
type fakeAccountRepo struct {
	t *testing.T
}

func (f fakeAccountRepo) ClaimKey(context.Context, string) (account.Claim, error) {
	f.t.Fatal("ClaimKey should not be called for a request rejected by validation")
	return account.Claim{}, nil
}

func (f fakeAccountRepo) CreateCustomerClaimed(context.Context, string, string) (int, []byte, error) {
	f.t.Fatal("CreateCustomerClaimed should not be called for a request rejected by validation")
	return 0, nil, nil
}

func (f fakeAccountRepo) CreateAccountClaimed(context.Context, string, string) (int, []byte, error) {
	f.t.Fatal("CreateAccountClaimed should not be called for a request rejected by validation")
	return 0, nil, nil
}

func (f fakeAccountRepo) CloseAccountClaimed(context.Context, string, string) (int, []byte, error) {
	f.t.Fatal("CloseAccountClaimed should not be called for a request rejected by validation")
	return 0, nil, nil
}

func (f fakeAccountRepo) GetAccount(context.Context, string) (account.Account, error) {
	f.t.Fatal("GetAccount should not be called for a request rejected by validation")
	return account.Account{}, nil
}

func (f fakeAccountRepo) ListEntries(context.Context, string, int, int) ([]account.Entry, int, error) {
	f.t.Fatal("ListEntries should not be called for a request rejected by validation")
	return nil, 0, nil
}

func TestHandlePostAccount_RejectsNonUUIDCustomerID(t *testing.T) {
	accountSvc := account.NewService(fakeAccountRepo{t: t})
	s := &Server{accountSvc: accountSvc, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /accounts", s.handlePostAccount)

	req := httptest.NewRequest(http.MethodPost, "/accounts", strings.NewReader(
		`{"customer_id":"not-a-uuid","idempotency_key":"key-1"}`,
	))
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}
