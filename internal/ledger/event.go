package ledger

import "encoding/json"

// TransactionEvent is the wire format published to Kafka/Redpanda and
// stored as the outbox payload — fraud-check's consumer expects this shape
// (see fraud-check/src/fraud_check/consumer.py).
type TransactionEvent struct {
	IdempotencyKey string      `json:"idempotency_key"`
	Entries        []EntryJSON `json:"entries"`
}

type EntryJSON struct {
	AccountID   string `json:"account_id"`
	AmountCents int64  `json:"amount_cents"`
}

// MarshalEvent builds the JSON payload for tx, used both for the outbox row
// written at commit time and (unmarshaled) for the message the relay later
// publishes.
func MarshalEvent(tx Transaction) ([]byte, error) {
	event := TransactionEvent{IdempotencyKey: tx.IdempotencyKey}
	for _, e := range tx.Entries {
		event.Entries = append(event.Entries, EntryJSON{AccountID: e.AccountID, AmountCents: e.AmountCents})
	}
	return json.Marshal(event)
}
