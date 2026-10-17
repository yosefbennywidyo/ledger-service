package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// beginClaimedWork starts the transaction a *Claimed method runs its write
// and claim-completion in, and locks the claim row for the rest of it — the
// same shape ApplyLocked uses to serialize a reclaim against the original
// holder still finishing. The returned release func must run via defer
// immediately; it rolls back on an unexpected failure (err != nil at return
// time, checked through the pointer) and expires the claim's lease right
// away instead of leaving a retry locked out for the rest of the 30s window.
func (s *Store) beginClaimedWork(ctx context.Context, idempotencyKey string, outerErr *error) (dbTx *sql.Tx, done func(), err error) {
	dbTx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("begin transaction: %w", err)
	}
	done = func() {
		_ = dbTx.Rollback() //nolint:errcheck // no-op if already committed
		if *outerErr != nil {
			_, _ = s.db.ExecContext(context.WithoutCancel(ctx),
				`UPDATE idempotency_claims SET claimed_at = now() - interval '31 seconds' WHERE key = $1 AND status = 'processing'`,
				idempotencyKey,
			)
		}
	}

	var (
		rowStatus  string
		respStatus sql.NullInt64
		respBody   sql.NullString
	)
	if err := dbTx.QueryRowContext(ctx,
		`SELECT status, response_status, response_body FROM idempotency_claims WHERE key = $1 FOR UPDATE`,
		idempotencyKey,
	).Scan(&rowStatus, &respStatus, &respBody); err != nil {
		return dbTx, done, fmt.Errorf("lock idempotency row: %w", err)
	}
	if rowStatus != "completed" {
		return dbTx, done, nil
	}
	if !respStatus.Valid {
		return dbTx, done, fmt.Errorf("completed idempotency row for key %s has no stored response", idempotencyKey)
	}
	if err := dbTx.Commit(); err != nil {
		return dbTx, done, fmt.Errorf("commit transaction: %w", err)
	}
	return dbTx, done, alreadyCompleted{status: int(respStatus.Int64), body: []byte(respBody.String)}
}

// alreadyCompleted is returned as the error from beginClaimedWork when the
// idempotency key was already completed by a previous attempt, carrying the
// stored response for the caller to replay instead of treating it as a
// failure.
type alreadyCompleted struct {
	status int
	body   []byte
}

func (alreadyCompleted) Error() string { return "idempotency key already completed" }

func completeClaim(ctx context.Context, dbTx *sql.Tx, idempotencyKey string, status int, body []byte) error {
	if _, err := dbTx.ExecContext(ctx,
		`UPDATE idempotency_claims SET status = 'completed', response_status = $2, response_body = $3 WHERE key = $1`,
		idempotencyKey, status, string(body),
	); err != nil {
		return fmt.Errorf("record completion: %w", err)
	}
	return dbTx.Commit()
}

func (s *Store) CreateCustomerClaimed(ctx context.Context, idempotencyKey, name string) (status int, body []byte, err error) {
	dbTx, done, startErr := s.beginClaimedWork(ctx, idempotencyKey, &err)
	if done != nil {
		defer done()
	}
	var replay alreadyCompleted
	if errors.As(startErr, &replay) {
		return replay.status, replay.body, nil
	}
	if startErr != nil {
		return 0, nil, startErr
	}

	var c account.Customer
	if err := dbTx.QueryRowContext(ctx,
		`INSERT INTO customers (name) VALUES ($1) RETURNING id, name, created_at`,
		name,
	).Scan(&c.ID, &c.Name, &c.CreatedAt); err != nil {
		return 0, nil, fmt.Errorf("insert customer: %w", err)
	}
	body, marshalErr := json.Marshal(map[string]any{"id": c.ID, "name": c.Name, "created_at": c.CreatedAt})
	if marshalErr != nil {
		return 0, nil, fmt.Errorf("marshal customer: %w", marshalErr)
	}
	status = http.StatusCreated
	if err := completeClaim(ctx, dbTx, idempotencyKey, status, body); err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

func (s *Store) CreateAccountClaimed(ctx context.Context, idempotencyKey, customerID string) (status int, body []byte, err error) {
	dbTx, done, startErr := s.beginClaimedWork(ctx, idempotencyKey, &err)
	if done != nil {
		defer done()
	}
	var replay alreadyCompleted
	if errors.As(startErr, &replay) {
		return replay.status, replay.body, nil
	}
	if startErr != nil {
		return 0, nil, startErr
	}

	var customerExists int
	lookupErr := dbTx.QueryRowContext(ctx, `SELECT 1 FROM customers WHERE id = $1`, customerID).Scan(&customerExists)
	switch {
	case lookupErr == sql.ErrNoRows:
		status, body = http.StatusNotFound, errorBodyJSON("customer not found")
	case lookupErr != nil:
		return 0, nil, fmt.Errorf("check customer exists: %w", lookupErr)
	default:
		var a account.Account
		var kind, astatus string
		if err := dbTx.QueryRowContext(ctx,
			`INSERT INTO accounts (id, customer_id, kind) VALUES (gen_random_uuid()::text, $1, 'customer') RETURNING id, customer_id, kind, status, created_at`,
			customerID,
		).Scan(&a.ID, &a.CustomerID, &kind, &astatus, &a.CreatedAt); err != nil {
			return 0, nil, fmt.Errorf("insert account: %w", err)
		}
		var marshalErr error
		body, marshalErr = json.Marshal(map[string]any{
			"id": a.ID, "customer_id": a.CustomerID, "kind": kind, "status": astatus, "created_at": a.CreatedAt,
		})
		if marshalErr != nil {
			return 0, nil, fmt.Errorf("marshal account: %w", marshalErr)
		}
		status = http.StatusCreated
	}

	if err := completeClaim(ctx, dbTx, idempotencyKey, status, body); err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

func (s *Store) CloseAccountClaimed(ctx context.Context, idempotencyKey, accountID string) (status int, body []byte, err error) {
	dbTx, done, startErr := s.beginClaimedWork(ctx, idempotencyKey, &err)
	if done != nil {
		defer done()
	}
	var replay alreadyCompleted
	if errors.As(startErr, &replay) {
		return replay.status, replay.body, nil
	}
	if startErr != nil {
		return 0, nil, startErr
	}

	// Lock the account itself (same hashtext lock space ApplyLocked uses)
	// so the balance this reads can't change under a concurrent transfer
	// between the check below and the UPDATE.
	if _, err := dbTx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, accountID); err != nil {
		return 0, nil, fmt.Errorf("lock account: %w", err)
	}

	var kind, astatus string
	lookupErr := dbTx.QueryRowContext(ctx, `SELECT kind, status FROM accounts WHERE id = $1`, accountID).Scan(&kind, &astatus)
	switch {
	case lookupErr == sql.ErrNoRows:
		status, body = http.StatusNotFound, errorBodyJSON("account not found")
	case lookupErr != nil:
		return 0, nil, fmt.Errorf("lookup account: %w", lookupErr)
	case kind == "system":
		// system:cash funds every top-up; closing it would brick the whole
		// demo with no way to reopen it.
		status, body = http.StatusUnprocessableEntity, errorBodyJSON("system account cannot be closed")
	default:
		var balance int64
		if err := dbTx.QueryRowContext(ctx,
			`SELECT COALESCE(SUM(amount_cents), 0) FROM ledger_entries WHERE account_id = $1`,
			accountID,
		).Scan(&balance); err != nil {
			return 0, nil, fmt.Errorf("query balance: %w", err)
		}
		if balance != 0 {
			status, body = http.StatusUnprocessableEntity, errorBodyJSON("account has a non-zero balance")
		} else {
			if _, err := dbTx.ExecContext(ctx, `UPDATE accounts SET status = 'closed' WHERE id = $1`, accountID); err != nil {
				return 0, nil, fmt.Errorf("close account: %w", err)
			}
			status = http.StatusOK
			var marshalErr error
			body, marshalErr = json.Marshal(map[string]any{"id": accountID, "status": "closed"})
			if marshalErr != nil {
				return 0, nil, fmt.Errorf("marshal close response: %w", marshalErr)
			}
		}
	}

	if err := completeClaim(ctx, dbTx, idempotencyKey, status, body); err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

func errorBodyJSON(message string) []byte {
	b, _ := json.Marshal(map[string]string{"error": message})
	return b
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
