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
	afterRead chan func(context.Context) error
}

func TestFullGraphWriteQueueLeavesHTTPHealthAndReadyCapacity(t *testing.T) {
	store := &readyContentionStore{Store: memory.New(), afterRead: make(chan func(context.Context) error, 1)}
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
}

func (s *readyContentionStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
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
