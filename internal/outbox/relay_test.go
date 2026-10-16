package outbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRepo struct {
	pending   []Record
	published []int64
}

func (f *fakeRepo) FetchUnpublished(_ context.Context, limit int) ([]Record, error) {
	if len(f.pending) > limit {
		return f.pending[:limit], nil
	}
	return f.pending, nil
}

func (f *fakeRepo) MarkPublished(_ context.Context, id int64) error {
	f.published = append(f.published, id)
	for i, r := range f.pending {
		if r.ID == id {
			f.pending = append(f.pending[:i], f.pending[i+1:]...)
			break
		}
	}
	return nil
}

type fakePublisher struct {
	sent []string
}

func (f *fakePublisher) PublishRaw(_ context.Context, key string, _ []byte) error {
	f.sent = append(f.sent, key)
	return nil
}

type failingPublisher struct{}

func (failingPublisher) PublishRaw(context.Context, string, []byte) error {
	return errors.New("broker unreachable")
}

func TestRelay_PublishesAndMarksPublished(t *testing.T) {
	repo := &fakeRepo{pending: []Record{{ID: 1, Key: "tx-1", Payload: []byte(`{}`)}}}
	pub := &fakePublisher{}
	relay := NewRelay(repo, pub, 10*time.Millisecond, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	go relay.Run(ctx)
	<-ctx.Done()

	if len(pub.sent) != 1 || pub.sent[0] != "tx-1" {
		t.Fatalf("expected tx-1 published once, got %v", pub.sent)
	}
	if len(repo.published) != 1 || repo.published[0] != int64(1) {
		t.Fatalf("expected outbox row 1 marked published, got %v", repo.published)
	}
}

func TestRelay_PublishFailureLeavesRowPendingForNextPoll(t *testing.T) {
	repo := &fakeRepo{pending: []Record{{ID: 1, Key: "tx-1", Payload: []byte(`{}`)}}}
	relay := NewRelay(repo, failingPublisher{}, 10*time.Millisecond, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go relay.Run(ctx)
	<-ctx.Done()

	if len(repo.published) != 0 {
		t.Fatalf("expected no row marked published when every publish attempt fails, got %v", repo.published)
	}
	if len(repo.pending) != 1 {
		t.Fatalf("expected the row to remain pending, got %d", len(repo.pending))
	}
}
