// Package kafka publishes encoded ledger events to Kafka/Redpanda — the async
// fan-out consumed by fraud-check (see e2e/PLAN.md jalur E-F). The outbox relay
// drives it via PublishRaw, which satisfies outbox.Publisher.
package kafka

import (
	"context"
	"fmt"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

// Producer publishes encoded events to Kafka/Redpanda for the outbox relay.
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
			// Default is 1s; the outbox relay writes one message per call, so
			// a long batch timeout would cap relay throughput near 1 event/sec.
			BatchTimeout: 10 * time.Millisecond,
			// kafka-go's Writer defaults RequiredAcks to RequireNone
			// (fire-and-forget) when constructed as a struct literal like
			// this — the "defaults to RequireAll" behavior documented
			// elsewhere only applies to the deprecated NewWriter(config)
			// constructor, which this code doesn't use. Under RequireNone, a
			// write can report success as soon as it's handed to a
			// connection, even if the broker never actually persists it
			// (e.g. it's mid-shutdown) — silently defeating the outbox's
			// entire "event is not lost" guarantee. Require the full ISR to
			// acknowledge before WriteMessages returns.
			RequiredAcks: segmentio.RequireAll,
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
