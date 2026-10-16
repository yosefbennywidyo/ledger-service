// Package kafka implements ledger.EventPublisher against Kafka/Redpanda —
// the async fan-out consumed by fraud-check (see e2e/PLAN.md jalur E-F).
package kafka

import (
	"context"
	"fmt"

	segmentio "github.com/segmentio/kafka-go"
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

// PublishRaw sends an already-encoded event payload to Kafka under key, so
// all entries of one transaction stay on the same partition and in order.
// Used by the outbox relay, which reads already-marshaled payloads from the
// outbox table rather than re-encoding a ledger.Transaction.
func (p *Producer) PublishRaw(ctx context.Context, key string, payload []byte) error {
	if err := p.writer.WriteMessages(ctx, segmentio.Message{
		Key:   []byte(key),
		Value: payload,
	}); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}
