package composition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type readyContentionStore struct {
	*memory.Store
	afterRead   chan func(context.Context) error
	observeRead func(context.Context)
}

func TestFullGraphWriteQueueLeavesHTTPHealthAndReadyCapacity(t *testing.T) {
	store := &readyContentionStore{Store: memory.New(), afterRead: make(chan func(context.Context) error, 1)}
	const queueHold = 300 * time.Millisecond
	type writeTiming struct {
		driver.PublicationTiming
		deadline time.Time
		readAt   time.Time
	}
	observed := make(chan writeTiming, 1)
	store.observeRead = func(ctx context.Context) {
		timing, marked := driver.GetPublicationTiming(ctx)
		deadline, bounded := ctx.Deadline()
		if marked && bounded && timing.QueueWait >= queueHold {
			select {
			case observed <- writeTiming{timing, deadline, time.Now()}:
			default:
			}
		}
	}
	f := startRuntime(t, Config{
		Driver: "memory", Project: "ready-contention", HTTP: httpjson.Config{MaxConcurrentRequests: 3},
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	create := func() error {
		_, err := f.client.Call(ctx, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "graph queue capacity"},
		}, "")
		return err
	}
	active, release := make(chan struct{}), make(chan struct{})
	store.afterRead <- func(ctx context.Context) error {
		close(active)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	completed := make(chan error, 3)
	go func() { completed <- create() }()
	select {
	case <-active:
	case <-ctx.Done():
		t.Fatal("first write did not enter")
	}
	for range 2 {
		go func() { completed <- create() }()
	}
	select {
	case err := <-completed:
		var typed *wire.Error
		if !errors.As(err, &typed) || typed.Data.Code != "busy" || !typed.Data.Retryable || typed.Data.Hint != "graph write queue is full" {
			t.Fatalf("overflow request error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("overflow request waited for the active write")
	}
	for _, method := range []string{"admin.health", "bead.ready"} {
		if _, err := f.client.Call(ctx, method, nil, ""); err != nil {
			t.Fatalf("%s while queue was full: %v", method, err)
		}
	}
	// Overflow already proved a waiter is admitted. Hold the first writer
	// long enough to distinguish the queue interval from the pipeline budget.
	select {
	case <-time.After(queueHold + 50*time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releasedAt := time.Now()
	close(release)
	for range 2 {
		select {
		case err := <-completed:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("admitted write did not complete")
		}
	}
	select {
	case timing := <-observed:
		if timing.deadline.Before(releasedAt.Add(1900*time.Millisecond)) || timing.deadline.After(timing.RequestDeadline) {
			t.Fatalf("queued writer did not start its two-second pipeline budget at admission: queue=%s deadline=%s", timing.QueueWait, timing.deadline)
		}
		t.Logf("writer queued for %s, pipeline budget at first store read %s", timing.QueueWait, timing.deadline.Sub(timing.readAt))
	default:
		t.Fatal("did not observe the queued writer's pipeline deadline")
	}
}

func (s *readyContentionStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if s.observeRead != nil {
		s.observeRead(ctx)
	}
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	select {
	case advance := <-s.afterRead:
		if err := advance(ctx); err != nil {
			_ = r.Close()
			return nil, err
		}
	default:
	}
	return r, nil
}

func TestFirstReadyPageRetriesConcurrentGraphPublication(t *testing.T) {
	store := &readyContentionStore{Store: memory.New(), afterRead: make(chan func(context.Context) error, 1)}
	f := startRuntime(t, Config{
		Driver: "memory", Project: "ready-contention",
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
	})
	create := func(ctx context.Context) error {
		_, err := f.client.Call(ctx, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "ready snapshot"},
		}, "")
		return err
	}
	ctx := context.Background()
	for range 2 {
		if err := create(ctx); err != nil {
			t.Fatal(err)
		}
	}
	store.afterRead <- create
	got, err := f.client.Call(ctx, "bead.ready", map[string]any{"limit": 1}, "")
	if err != nil {
		var typed *wire.Error
		if errors.As(err, &typed) {
			t.Fatalf("first ready page: %v, hint = %q", err, typed.Data.Hint)
		}
		t.Fatal(err)
	}
	var page struct {
		NextCursor string `json:"nextCursor"`
		AsOf       struct {
			Revision json.Number `json:"revision"`
		} `json:"asOf"`
	}
	if err := json.Unmarshal(got.JSON, &page); err != nil || page.NextCursor == "" || page.AsOf.Revision.String() != "3" {
		t.Fatalf("first page did not use the new snapshot: %s, error = %v", got.JSON, err)
	}
	store.afterRead <- create
	_, err = f.client.Call(ctx, "bead.ready", map[string]any{"limit": 1, "cursor": page.NextCursor}, "")
	var typed *wire.Error
	if !errors.As(err, &typed) || typed.Data.Code != "cursor_invalid" {
		t.Fatalf("continuation across graph publication error = %v", err)
	}
}
