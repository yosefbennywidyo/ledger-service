package outbox

import (
	"context"
	"log"
	"time"
)

// Record is one unpublished outbox row.
type Record struct {
	ID      int64
	Key     string
	Payload []byte
}

// Repository is the outbox-table persistence boundary the relay depends on.
// postgres.Store implements this.
type Repository interface {
	FetchUnpublished(ctx context.Context, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, id int64) error
}

// Publisher is the broker boundary the relay depends on. kafka.Producer
// implements this via PublishRaw.
type Publisher interface {
	PublishRaw(ctx context.Context, key string, payload []byte) error
}

type Relay struct {
	repo         Repository
	publisher    Publisher
	pollInterval time.Duration
	batchSize    int
}

func NewRelay(repo Repository, publisher Publisher, pollInterval time.Duration, batchSize int) *Relay {
	return &Relay{repo: repo, publisher: publisher, pollInterval: pollInterval, batchSize: batchSize}
}

// Run polls for unpublished outbox rows until ctx is canceled. A publish or
// mark-published failure is logged and retried on the next tick — the row
// stays unpublished until it succeeds, which is what makes this
// at-least-once rather than best-effort.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.drainOnce(ctx)
		}
	}
}

func (r *Relay) drainOnce(ctx context.Context) {
	records, err := r.repo.FetchUnpublished(ctx, r.batchSize)
	if err != nil {
		log.Printf("outbox: fetch unpublished: %v", err)
		return
	}
	for _, rec := range records {
		if err := r.publisher.PublishRaw(ctx, rec.Key, rec.Payload); err != nil {
			log.Printf("outbox: publish %s: %v", rec.Key, err)
			continue
		}
		if err := r.repo.MarkPublished(ctx, rec.ID); err != nil {
			log.Printf("outbox: mark published %d: %v", rec.ID, err)
		}
	}
}
