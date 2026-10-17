// Package postgres implements ledger.Repository and outbox.Repository against a PostgreSQL database.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"ledger-rail/ledger-service/internal/account"
	"ledger-rail/ledger-service/internal/ledger"
	"ledger-rail/ledger-service/internal/outbox"
)

var (
	_ ledger.Repository  = (*Store)(nil)
	_ account.Repository = (*Store)(nil)
	_ outbox.Repository  = (*Store)(nil)
)

// processingTimeout is how long an idempotency key may sit in 'processing'
// before another request is allowed to reclaim it (the original holder is
// presumed crashed).
const processingTimeout = 30 * time.Second

// Store is a ledger.Repository and outbox.Repository backed by PostgreSQL.
type Store struct {
	db *sql.DB
}

// Open connects to PostgreSQL using dsn (e.g. "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable").
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// ClaimIdempotencyKey claims key in its own committed statement. A unique
// violation means another request already claimed it; the existing row is
// then inspected to decide between replaying, reporting in-flight, or
// reclaiming a stale claim.
func (s *Store) ClaimIdempotencyKey(ctx context.Context, key string) (ledger.Claim, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO ledger_transactions (idempotency_key, status, claimed_at) VALUES ($1, 'processing', now())`,
		key,
	)
	if err == nil {
		return ledger.Claim{State: ledger.ClaimAcquired}, nil
	}
	if !isUniqueViolation(err) {
		return ledger.Claim{}, fmt.Errorf("insert idempotency claim: %w", err)
	}
	return s.inspectExistingClaim(ctx, key, 1)
}

// inspectExistingClaim branches on an already-present idempotency row.
// retriesLeft bounds the re-run after a reclaim UPDATE that matched no rows
// (the row changed between our SELECT and UPDATE, e.g. it completed).
func (s *Store) inspectExistingClaim(ctx context.Context, key string, retriesLeft int) (ledger.Claim, error) {
	var (
		status     string
		claimedAt  time.Time
		respStatus sql.NullInt64
		respBody   sql.NullString
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT status, claimed_at, response_status, response_body FROM ledger_transactions WHERE idempotency_key = $1`,
		key,
	).Scan(&status, &claimedAt, &respStatus, &respBody)
	if err != nil {
		return ledger.Claim{}, fmt.Errorf("read idempotency claim: %w", err)
	}

	switch status {
	case "completed":
		if !respStatus.Valid {
			return ledger.Claim{}, fmt.Errorf("completed idempotency row for key %s has no stored response", key)
		}
		return ledger.Claim{
			State:          ledger.ClaimCompleted,
			ResponseStatus: int(respStatus.Int64),
			ResponseBody:   []byte(respBody.String),
		}, nil
	case "processing":
		if time.Since(claimedAt) < processingTimeout {
			return ledger.Claim{State: ledger.ClaimProcessing}, nil
		}
		res, err := s.db.ExecContext(ctx,
			`UPDATE ledger_transactions SET claimed_at = now() WHERE idempotency_key = $1 AND status = 'processing' AND claimed_at < now() - interval '30 seconds'`,
			key,
		)
		if err != nil {
			return ledger.Claim{}, fmt.Errorf("reclaim idempotency key: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return ledger.Claim{}, fmt.Errorf("reclaim idempotency key rows affected: %w", err)
		}
		if n == 1 {
			return ledger.Claim{State: ledger.ClaimAcquired}, nil
		}
		if retriesLeft > 0 {
			return s.inspectExistingClaim(ctx, key, retriesLeft-1)
		}
		// Still stale per our clock but not reclaimable per the database's: treat as in flight.
		return ledger.Claim{State: ledger.ClaimProcessing}, nil
	default:
		return ledger.Claim{}, fmt.Errorf("unexpected idempotency status %q for key %s", status, key)
	}
}

// ApplyLocked takes a transaction-scoped advisory lock per account (sorted,
// so lock order is consistent), checks balances for every debited account,
// and persists either the entries plus outbox row (201) or only the
// rejection (422) in a single ACID transaction.
func (s *Store) ApplyLocked(ctx context.Context, tx ledger.Transaction) (status int, body []byte, err error) {
	dbTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer dbTx.Rollback() //nolint:errcheck // no-op if already committed
	defer func() {
		if err != nil {
			// Best-effort: on any unexpected failure, expire this key's lease now
			// instead of leaving a retry locked out for the rest of the 30s
			// window. Roll back first — this transaction may still be holding
			// the row lock on the very row this UPDATE needs, and the outer
			// `defer dbTx.Rollback()` wouldn't run until after this closure
			// returns. Use a context detached from the caller's (which may
			// already be canceled — that's often why we're here) so this
			// cleanup query isn't itself immediately canceled.
			_ = dbTx.Rollback()
			_, _ = s.db.ExecContext(context.WithoutCancel(ctx),
				`UPDATE ledger_transactions SET claimed_at = now() - interval '31 seconds' WHERE idempotency_key = $1 AND status = 'processing'`,
				tx.IdempotencyKey,
			)
		}
	}()

	// Lock this key's idempotency row for the rest of the transaction. This
	// serializes competing holders (original claimant vs. reclaimer) and,
	// if another holder already committed, returns its stored result
	// instead of double-posting.
	var (
		rowStatus  string
		respStatus sql.NullInt64
		respBody   sql.NullString
	)
	if err := dbTx.QueryRowContext(ctx,
		`SELECT status, response_status, response_body FROM ledger_transactions WHERE idempotency_key = $1 FOR UPDATE`,
		tx.IdempotencyKey,
	).Scan(&rowStatus, &respStatus, &respBody); err != nil {
		return 0, nil, fmt.Errorf("lock idempotency row: %w", err)
	}
	if rowStatus == "completed" {
		if !respStatus.Valid {
			return 0, nil, fmt.Errorf("completed idempotency row for key %s has no stored response", tx.IdempotencyKey)
		}
		if err := dbTx.Commit(); err != nil {
			return 0, nil, fmt.Errorf("commit transaction: %w", err)
		}
		return int(respStatus.Int64), []byte(respBody.String), nil
	}

	// Net delta per account, so a debited account touched by several entries
	// is checked against its total.
	net := make(map[string]int64)
	for _, e := range tx.Entries {
		net[e.AccountID] += e.AmountCents
	}
	accounts := make([]string, 0, len(net))
	for id := range net {
		accounts = append(accounts, id)
	}
	sort.Strings(accounts)

	// kind/status per involved account, so the checks below can tell a
	// system account (allowed to go negative) from a customer account, and
	// reject any account that has been closed.
	accountInfo := make(map[string]struct{ kind, status string }, len(accounts))
	rows, err := dbTx.QueryContext(ctx, `SELECT id, kind, status FROM accounts WHERE id = ANY($1)`, accounts)
	if err != nil {
		return 0, nil, fmt.Errorf("query account kind/status: %w", err)
	}
	for rows.Next() {
		var id, kind, status string
		if err := rows.Scan(&id, &kind, &status); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("scan account kind/status: %w", err)
		}
		accountInfo[id] = struct{ kind, status string }{kind, status}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, nil, fmt.Errorf("iterate account kind/status: %w", err)
	}
	rows.Close()

	for _, id := range accounts {
		if accountInfo[id].status == "closed" {
			return rejectTransaction(ctx, dbTx, tx.IdempotencyKey, "account closed: "+id)
		}
	}

	for _, id := range accounts {
		if _, err := dbTx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, id); err != nil {
			return 0, nil, fmt.Errorf("lock account %s: %w", id, err)
		}
	}

	for _, id := range accounts {
		if net[id] >= 0 || accountInfo[id].kind == "system" {
			continue
		}
		var balance int64
		if err := dbTx.QueryRowContext(ctx,
			`SELECT COALESCE(SUM(amount_cents), 0) FROM ledger_entries WHERE account_id = $1`,
			id,
		).Scan(&balance); err != nil {
			return 0, nil, fmt.Errorf("query balance for %s: %w", id, err)
		}
		if balance+net[id] < 0 {
			return rejectTransaction(ctx, dbTx, tx.IdempotencyKey, "insufficient balance: account "+id)
		}
	}

	payload, err := ledger.MarshalEvent(tx)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal event: %w", err)
	}

	for _, e := range tx.Entries {
		if _, err := dbTx.ExecContext(ctx,
			`INSERT INTO ledger_entries (idempotency_key, account_id, amount_cents) VALUES ($1, $2, $3)`,
			tx.IdempotencyKey, e.AccountID, e.AmountCents,
		); err != nil {
			return 0, nil, fmt.Errorf("insert entry for %s: %w", e.AccountID, err)
		}
	}

	if _, err := dbTx.ExecContext(ctx,
		`INSERT INTO outbox (idempotency_key, payload) VALUES ($1, $2)`,
		tx.IdempotencyKey, string(payload),
	); err != nil {
		return 0, nil, fmt.Errorf("insert outbox row: %w", err)
	}

	body = []byte(`{"status":"completed"}`)
	if _, err := dbTx.ExecContext(ctx,
		`UPDATE ledger_transactions SET status = 'completed', response_status = 201, response_body = $2 WHERE idempotency_key = $1`,
		tx.IdempotencyKey, string(body),
	); err != nil {
		return 0, nil, fmt.Errorf("record completion: %w", err)
	}

	if err := dbTx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit transaction: %w", err)
	}
	return http.StatusCreated, body, nil
}

// FetchUnpublished returns up to limit outbox rows that have not been published yet.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]outbox.Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, idempotency_key, payload FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query outbox: %w", err)
	}
	defer rows.Close()

	var records []outbox.Record
	for rows.Next() {
		var r outbox.Record
		if err := rows.Scan(&r.ID, &r.Key, &r.Payload); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return records, nil
}

// MarkPublished records that outbox row id has been published.
func (s *Store) MarkPublished(ctx context.Context, id int64) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = $1`,
		id,
	); err != nil {
		return fmt.Errorf("mark outbox row %d published: %w", id, err)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// rejectTransaction records a 422 business rejection for idempotencyKey,
// commits dbTx, and returns the response ApplyLocked should send.
func rejectTransaction(ctx context.Context, dbTx *sql.Tx, idempotencyKey, message string) (int, []byte, error) {
	body, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		return 0, nil, fmt.Errorf("marshal rejection: %w", err)
	}
	if _, err := dbTx.ExecContext(ctx,
		`UPDATE ledger_transactions SET status = 'completed', response_status = 422, response_body = $2 WHERE idempotency_key = $1`,
		idempotencyKey, string(body),
	); err != nil {
		return 0, nil, fmt.Errorf("record rejection: %w", err)
	}
	if err := dbTx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit transaction: %w", err)
	}
	return http.StatusUnprocessableEntity, body, nil
}
