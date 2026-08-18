-- Minimal schema for the ledger-service skeleton.
-- Apply with: psql "$DATABASE_URL" -f internal/postgres/schema.sql

CREATE TABLE IF NOT EXISTS ledger_transactions (
    idempotency_key TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL REFERENCES ledger_transactions(idempotency_key),
    account_id      TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_id ON ledger_entries(account_id);
