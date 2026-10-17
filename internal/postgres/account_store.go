package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"ledger-rail/ledger-service/internal/account"
)

// ClaimKey claims key in idempotency_claims, the generic claim table for the
// account package's create/close endpoints. It is a fresh copy of
// ClaimIdempotencyKey/inspectExistingClaim's two-phase shape against a
// different table, rather than a shared helper — see account_store.go's
// task note in the implementation plan for why.
func (s *Store) ClaimKey(ctx context.Context, key string) (account.Claim, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO idempotency_claims (key, status, claimed_at) VALUES ($1, 'processing', now())`,
		key,
	)
	if err == nil {
		return account.Claim{State: account.ClaimAcquired}, nil
	}
	if !isUniqueViolation(err) {
		return account.Claim{}, fmt.Errorf("insert idempotency claim: %w", err)
	}
	return s.inspectExistingAccountClaim(ctx, key, 1)
}

func (s *Store) inspectExistingAccountClaim(ctx context.Context, key string, retriesLeft int) (account.Claim, error) {
	var (
		status     string
		claimedAt  time.Time
		respStatus sql.NullInt64
		respBody   sql.NullString
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT status, claimed_at, response_status, response_body FROM idempotency_claims WHERE key = $1`,
		key,
	).Scan(&status, &claimedAt, &respStatus, &respBody)
	if err != nil {
		return account.Claim{}, fmt.Errorf("read idempotency claim: %w", err)
	}

	switch status {
	case "completed":
		if !respStatus.Valid {
			return account.Claim{}, fmt.Errorf("completed idempotency row for key %s has no stored response", key)
		}
		return account.Claim{
			State:          account.ClaimCompleted,
			ResponseStatus: int(respStatus.Int64),
			ResponseBody:   []byte(respBody.String),
		}, nil
	case "processing":
		if time.Since(claimedAt) < processingTimeout {
			return account.Claim{State: account.ClaimProcessing}, nil
		}
		res, err := s.db.ExecContext(ctx,
			`UPDATE idempotency_claims SET claimed_at = now() WHERE key = $1 AND status = 'processing' AND claimed_at < now() - interval '30 seconds'`,
			key,
		)
		if err != nil {
			return account.Claim{}, fmt.Errorf("reclaim idempotency key: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return account.Claim{}, fmt.Errorf("reclaim idempotency key rows affected: %w", err)
		}
		if n == 1 {
			return account.Claim{State: account.ClaimAcquired}, nil
		}
		if retriesLeft > 0 {
			return s.inspectExistingAccountClaim(ctx, key, retriesLeft-1)
		}
		return account.Claim{State: account.ClaimProcessing}, nil
	default:
		return account.Claim{}, fmt.Errorf("unexpected idempotency status %q for key %s", status, key)
	}
}

func (s *Store) CompleteClaim(ctx context.Context, key string, status int, body []byte) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE idempotency_claims SET status = 'completed', response_status = $2, response_body = $3 WHERE key = $1`,
		key, status, string(body),
	)
	if err != nil {
		return fmt.Errorf("complete idempotency claim: %w", err)
	}
	return nil
}

func (s *Store) CreateCustomer(ctx context.Context, name string) (account.Customer, error) {
	var c account.Customer
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO customers (name) VALUES ($1) RETURNING id, name, created_at`,
		name,
	).Scan(&c.ID, &c.Name, &c.CreatedAt)
	if err != nil {
		return account.Customer{}, fmt.Errorf("insert customer: %w", err)
	}
	return c, nil
}

func (s *Store) CreateAccount(ctx context.Context, customerID string) (account.Account, error) {
	var a account.Account
	var kind, status string
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO accounts (id, customer_id, kind) VALUES (gen_random_uuid()::text, $1, 'customer') RETURNING id, customer_id, kind, status, created_at`,
		customerID,
	).Scan(&a.ID, &a.CustomerID, &kind, &status, &a.CreatedAt)
	if isForeignKeyViolation(err) {
		return account.Account{}, account.ErrCustomerNotFound
	}
	if err != nil {
		return account.Account{}, fmt.Errorf("insert account: %w", err)
	}
	a.Kind = account.Kind(kind)
	a.Status = account.Status(status)
	return a, nil
}

func (s *Store) CloseAccount(ctx context.Context, accountID string) error {
	var id string
	err := s.db.QueryRowContext(ctx,
		`UPDATE accounts SET status = 'closed' WHERE id = $1 RETURNING id`,
		accountID,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return account.ErrAccountNotFound
	}
	if err != nil {
		return fmt.Errorf("close account: %w", err)
	}
	return nil
}

func (s *Store) GetAccount(ctx context.Context, accountID string) (account.Account, error) {
	var a account.Account
	var kind, status string
	err := s.db.QueryRowContext(ctx,
		`SELECT a.id, COALESCE(a.customer_id::text, ''), a.kind, a.status, a.created_at,
		        COALESCE((SELECT SUM(amount_cents) FROM ledger_entries WHERE account_id = a.id), 0)
		 FROM accounts a WHERE a.id = $1`,
		accountID,
	).Scan(&a.ID, &a.CustomerID, &kind, &status, &a.CreatedAt, &a.BalanceCents)
	if err == sql.ErrNoRows {
		return account.Account{}, account.ErrAccountNotFound
	}
	if err != nil {
		return account.Account{}, fmt.Errorf("query account: %w", err)
	}
	a.Kind = account.Kind(kind)
	a.Status = account.Status(status)
	return a, nil
}

func (s *Store) ListEntries(ctx context.Context, accountID string, offset, limit int) ([]account.Entry, int, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM accounts WHERE id = $1`, accountID).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, 0, account.ErrAccountNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("check account exists: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT account_id, amount_cents, created_at FROM ledger_entries WHERE account_id = $1 ORDER BY created_at OFFSET $2 LIMIT $3`,
		accountID, offset, limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query entries: %w", err)
	}
	defer rows.Close()

	var entries []account.Entry
	for rows.Next() {
		var e account.Entry
		if err := rows.Scan(&e.AccountID, &e.AmountCents, &e.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate entries: %w", err)
	}

	var total int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ledger_entries WHERE account_id = $1`,
		accountID,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count entries: %w", err)
	}

	return entries, total, nil
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
