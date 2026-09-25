# ledger-service

Core double-entry ledger for [Ledger & Rail](https://github.com/yosefbennywidyo/ledger-rail),
written in Go on PostgreSQL. Every transaction is a balanced set of entries
(amounts sum to zero), applied atomically and idempotently per
`idempotency_key`. After a transaction is applied it is published to the
Kafka/Redpanda topic `transactions` for async consumers (`fraud-check`).

Design notes and layout rationale: [`BEST_PRACTICES.md`](BEST_PRACTICES.md).

## Run

Needs Postgres (with `internal/postgres/schema.sql` applied) and a Kafka-compatible
broker. From the `ledger-rail` superproject root,
`docker compose up -d postgres redpanda` provides both; the schema is applied
automatically on a fresh Postgres volume.

```bash
mise exec go -- go run ./cmd/ledger-service
```

Applying the schema by hand (e.g. against another database):

```bash
psql "$DATABASE_URL" -f internal/postgres/schema.sql
```

| Env var | Default |
|---|---|
| `DATABASE_URL` | `postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable` |
| `PORT` | `8080` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `transactions` |

## API

| Method | Path | Notes |
|---|---|---|
| `GET` | `/healthz` | `200` when up |
| `POST` | `/transactions` | `201` applied (or already applied with this key); `422` unbalanced / insufficient balance; `400` malformed body |

```bash
curl -i -X POST localhost:8080/transactions -H 'content-type: application/json' -d '{
  "idempotency_key": "demo-1",
  "entries": [
    {"account_id": "alice", "amount_cents": -2500},
    {"account_id": "bob",   "amount_cents":  2500}
  ]
}'
```

There is no genesis/top-up account: a debit that would take an account below
zero is rejected, and the API has no way to fund one. For local experiments,
seed a balance directly in Postgres (use a key distinct from any real
transaction):

```bash
docker exec -i ledger-rail-postgres-1 psql -U ledger -d ledger <<'SQL'
BEGIN;
INSERT INTO ledger_transactions (idempotency_key) VALUES ('seed-alice');
INSERT INTO ledger_entries (idempotency_key, account_id, amount_cents) VALUES ('seed-alice', 'alice', 10000);
COMMIT;
SQL
```

## Test

```bash
mise exec go -- go test ./...
```

Unit tests need no database or broker. End-to-end tests covering this service
with the rest of the stack live in the superproject's
[`e2e/`](https://github.com/yosefbennywidyo/ledger-rail/tree/main/e2e).

## Docker

```bash
docker build -t ledger-service .
```

The superproject's `docker-compose.yml` builds and wires this image
(`docker compose up -d ledger-service`).
