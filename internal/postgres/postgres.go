// Package postgres implements ledger.Repository against a PostgreSQL database.
package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"ledger-rail/ledger-service/internal/ledger"
)

// Store is a ledger.Repository backed by PostgreSQL.
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

func (s *Store) Balance(ctx context.Context, accountID string) (int64, error) {
	var balance int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(amount_cents), 0) FROM ledger_entries WHERE account_id = $1`,
		accountID,
	).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("query balance: %w", err)
	}
	return balance, nil
}

func (s *Store) SeenIdempotencyKey(ctx context.Context, key string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM ledger_transactions WHERE idempotency_key = $1)`,
		key,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query idempotency key: %w", err)
	}
	return exists, nil
}

// Apply writes the transaction and its entries inside a single ACID transaction,
// so a partial write (some entries applied, some not) can never be observed.
func (s *Store) Apply(ctx context.Context, tx ledger.Transaction) error {
	dbTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer dbTx.Rollback() //nolint:errcheck // no-op if already committed

	_, err = dbTx.ExecContext(ctx,
		`INSERT INTO ledger_transactions (idempotency_key) VALUES ($1)`,
		tx.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}

	for _, e := range tx.Entries {
		_, err = dbTx.ExecContext(ctx,
			`INSERT INTO ledger_entries (idempotency_key, account_id, amount_cents) VALUES ($1, $2, $3)`,
			tx.IdempotencyKey, e.AccountID, e.AmountCents,
		)
		if err != nil {
			return fmt.Errorf("insert entry for %s: %w", e.AccountID, err)
		}
	}

	if err := dbTx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
