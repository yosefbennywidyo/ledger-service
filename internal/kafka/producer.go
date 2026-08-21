// Package kafka implements ledger.EventPublisher against Kafka/Redpanda —
// the async fan-out consumed by fraud-check (see e2e/PLAN.md jalur E-F).
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	segmentio "github.com/segmentio/kafka-go"

	"ledger-rail/ledger-service/internal/ledger"
)

// Producer is a ledger.EventPublisher backed by Kafka/Redpanda.
type Producer struct {
	writer *segmentio.Writer
}

// NewProducer builds a Producer that publishes to topic on the given brokers.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &segmentio.Writer{
			Addr:     segmentio.TCP(brokers...),
			Topic:    topic,
			Balancer: &segmentio.LeastBytes{},
		},
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// transactionEvent is the wire format fraud-check's consumer expects
// (see fraud-check/src/fraud_check/consumer.py).
type transactionEvent struct {
	IdempotencyKey string      `json:"idempotency_key"`
	Entries        []entryJSON `json:"entries"`
}

type entryJSON struct {
	AccountID   string `json:"account_id"`
	AmountCents int64  `json:"amount_cents"`
}

// Publish sends tx as a JSON event keyed by its idempotency key, so all
// entries of one transaction land on the same partition and stay ordered.
func (p *Producer) Publish(ctx context.Context, tx ledger.Transaction) error {
	event := transactionEvent{IdempotencyKey: tx.IdempotencyKey}
	for _, e := range tx.Entries {
		event.Entries = append(event.Entries, entryJSON{AccountID: e.AccountID, AmountCents: e.AmountCents})
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal transaction event: %w", err)
	}

	if err := p.writer.WriteMessages(ctx, segmentio.Message{
		Key:   []byte(tx.IdempotencyKey),
		Value: payload,
	}); err != nil {
		return fmt.Errorf("publish transaction event: %w", err)
	}
	return nil
}
