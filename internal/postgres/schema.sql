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

-- SP-2: customers, accounts, and a generic idempotency-claim table for the
-- new account/customer create+close endpoints (kept separate from
-- ledger_transactions's claim columns above to avoid touching SP-1's
-- already-debugged concurrency path).
CREATE TABLE IF NOT EXISTS customers (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    customer_id UUID REFERENCES customers(id),
    kind        TEXT NOT NULL CHECK (kind IN ('customer', 'system')),
    status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO accounts (id, customer_id, kind, status)
    VALUES ('system:cash', NULL, 'system', 'active')
    ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS idempotency_claims (
    key             TEXT PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'processing',
    claimed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_status INT,
    response_body   TEXT
);

-- Postgres has no ADD CONSTRAINT IF NOT EXISTS, so guard the FK with a
-- duplicate_object catch to keep this file safely re-runnable.
DO $$ BEGIN
    ALTER TABLE ledger_entries
        ADD CONSTRAINT fk_ledger_entries_account FOREIGN KEY (account_id) REFERENCES accounts(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
