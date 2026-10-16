-- Minimal schema for the ledger-service skeleton.
-- Apply with: psql "$DATABASE_URL" -f internal/postgres/schema.sql

CREATE TABLE IF NOT EXISTS ledger_transactions (
    idempotency_key TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE ledger_transactions
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'completed',
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS response_status INT,
    ADD COLUMN IF NOT EXISTS response_body TEXT;

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL REFERENCES ledger_transactions(idempotency_key),
    payload         JSONB NOT NULL,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- Idempotent backfill: rows that predate response_status/response_body (or were
-- marked 'completed' by the column default above) get a safe, replayable success
-- response instead of NULL. A no-op on an empty database.
UPDATE ledger_transactions SET response_status = 201, response_body = '{"status":"completed"}'
    WHERE response_status IS NULL;

CREATE TABLE IF NOT EXISTS ledger_entries (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL REFERENCES ledger_transactions(idempotency_key),
    account_id      TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_id ON ledger_entries(account_id);
