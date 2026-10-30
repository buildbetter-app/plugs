package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func decodeReadResult(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	value, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("result is not an object: %v", value)
	}
	return result
}

func TestLocalReadRPCsAndCLICommandsUseRealByteTransport(t *testing.T) {
	l, err := OpenLocal(context.Background(), LocalConfig{Project: "local", Agent: "cli-read-test", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	ctx := context.Background()
	created, err := l.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "first"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	createResult := decodeReadResult(t, created.JSON)
	ref := createResult["results"].([]any)[0].(map[string]any)["ref"]
	createdRevision := createResult["revision"].(json.Number)
	updated, err := l.Client.Call(ctx, "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "second"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	updateResult := decodeReadResult(t, updated.JSON)
	updatedRevision := updateResult["revision"].(json.Number)

	eventsPage, err := l.Client.Call(ctx, "event.list", map[string]any{"filter": map[string]any{}, "limit": 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	eventsResult := decodeReadResult(t, eventsPage.JSON)
	if len(eventsResult["events"].([]any)) != 1 || eventsResult["cursor"].(string) == "" {
		t.Fatalf("event page=%s", eventsPage.JSON)
	}
	resumed, err := l.Client.Call(ctx, "event.list", map[string]any{"filter": map[string]any{}, "after": eventsResult["cursor"], "limit": 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	resumedResult := decodeReadResult(t, resumed.JSON)
	if len(resumedResult["events"].([]any)) != 1 {
		t.Fatalf("resumed event page=%s", resumed.JSON)
	}

	history, err := l.Client.Call(ctx, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": createdRevision}}, "")
	if err != nil || !bytes.Contains(history.JSON, []byte(`"title":"first"`)) {
		t.Fatalf("history.get=%s err=%v", history.JSON, err)
	}
	diff, err := l.Client.Call(ctx, "history.diff", map[string]any{"ref": ref, "fromRevision": createdRevision, "toRevision": updatedRevision}, "")
	if err != nil || !bytes.Contains(diff.JSON, []byte(`"/fields/title"`)) || !bytes.Contains(diff.JSON, []byte(`"second"`)) {
		t.Fatalf("history.diff=%s err=%v", diff.JSON, err)
	}
	healthBefore, err := l.Client.Call(ctx, "admin.health", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	health := decodeReadResult(t, healthBefore.JSON)
	if health["head"] != updatedRevision {
		t.Fatalf("admin.health=%s err=%v", healthBefore.JSON, err)
	}

	var out, errOut bytes.Buffer
	s := &Session{Client: l.Client, Out: &out, Err: &errOut, JSON: true}
	if err := s.Run(ctx, []string{"health"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"driver":"memory"`)) {
		t.Fatalf("CLI health output=%s", out.String())
	}
}

func TestSQLiteHistoryReadsSurviveStoreReopen(t *testing.T) {
	path := t.TempDir() + "/history.db"
	ctx := context.Background()
	open := func() *Local {
		l, err := OpenLocal(ctx, LocalConfig{Project: "local", Agent: "sqlite-read-test", Storage: StorageConfig{Driver: "sqlite", DSN: path}, Factory: func(ctx context.Context, cfg StorageConfig) (driver.Store, func() error, error) {
			store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: cfg.DSN})
			if err != nil {
				return nil, nil, err
			}
			return store, store.Close, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	first := open()
	created, err := first.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "persisted"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	result := decodeReadResult(t, created.JSON)
	ref := result["results"].([]any)[0].(map[string]any)["ref"]
	revision := result["revision"]
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := open()
	t.Cleanup(func() { _ = second.Close() })
	history, err := second.Client.Call(ctx, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": revision}}, "")
	if err != nil || !bytes.Contains(history.JSON, []byte(`"title":"persisted"`)) {
		t.Fatalf("reopened history=%s err=%v", history.JSON, err)
	}
	health, err := second.Client.Call(ctx, "admin.health", nil, "")
	if err != nil || !bytes.Contains(health.JSON, []byte(`"driver":"sqlite"`)) {
		t.Fatalf("reopened health=%s err=%v", health.JSON, err)
	}
	events, err := second.Client.Call(ctx, "event.list", map[string]any{"filter": map[string]any{}, "limit": 1}, "")
	if err != nil || !bytes.Contains(events.JSON, []byte(`"type":"bead.created"`)) {
		t.Fatalf("reopened events=%s err=%v", events.JSON, err)
	}
}

type observedLeaseStore struct {
	driver.Store
	lease driver.ActiveLease
}

func (s *observedLeaseStore) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	lease, err := s.Store.Lease(ctx, holder, ttl)
	s.lease = lease
	return lease, err
}

type resultSignal chan struct{}

func (s resultSignal) Write(p []byte) (int, error) {
	select {
	case s <- struct{}{}:
	default:
	}
	return len(p), nil
}

func TestIdleSessionCancellationReleasesLeaseAndClosesStore(t *testing.T) {
	t.Setenv("PLUG_SERVER", "")
	store := &observedLeaseStore{Store: memory.New()}
	closed, released := 0, false
	factory := func(context.Context, StorageConfig) (driver.Store, func() error, error) {
		return store, func() error {
			closed++
			select {
			case <-store.lease.Lost():
				released = true
			default:
			}
			return store.Close()
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in, producer := io.Pipe()
	defer in.Close()
	defer producer.Close()
	result := make(resultSignal, 1)
	done := make(chan int, 1)
	var errOut bytes.Buffer
	go func() { done <- execute(ctx, []string{"--memory", "--json", "session"}, in, result, &errOut, factory) }()
	if _, err := io.WriteString(producer, "[\"ready\"]\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not return its first result")
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 || !strings.Contains(errOut.String(), "context canceled") {
			t.Fatalf("cancellation was swallowed: exit %d, stderr %s", code, errOut.String())
		}
	case <-time.After(2 * time.Second):
		_ = producer.Close()
		<-done
		t.Fatal("idle input prevented cleanup after cancellation")
	}
	if closed != 1 || !released {
		t.Fatalf("cleanup: store closed %d times, lease released before store close %t", closed, released)
	}
	if _, err := store.Meta(context.Background()); err == nil {
		t.Fatal("canceled session left storage open")
	}
}

type unavailablePost struct{ calls int }

func (p *unavailablePost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls++
	return errors.New("post subscriber is unavailable")
}

func TestRealWarningVisibleInJSONAndHumanModesAndReplay(t *testing.T) {
	for _, jsonMode := range []bool{true, false} {
		t.Run(map[bool]string{true: "json", false: "human"}[jsonMode], func(t *testing.T) {
			post := &unavailablePost{}
			local, err := OpenLocal(context.Background(), LocalConfig{
				Project: "local", Agent: "cli-test", Storage: StorageConfig{Driver: "memory"},
				PipelineOptions: []pipeline.Option{pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "test-warning", Subscriber: post})},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = local.Close() })
			var out, errOut bytes.Buffer
			s := &Session{Client: local.Client, Out: &out, Err: &errOut, JSON: jsonMode}
			args := []string{"create", "--title", "Committed with warning", "--idempotency-key", strings.Repeat("a", 64)}
			for i := 0; i < 2; i++ {
				if err := s.Run(context.Background(), args); err != nil {
					t.Fatal(err)
				}
			}
			if post.calls != 1 || strings.Count(errOut.String(), "warning [post_subscriber_failed]") != 2 || !strings.Contains(errOut.String(), "test-warning") {
				t.Fatalf("warning/replay: calls=%d stderr=%s", post.calls, errOut.String())
			}
			if jsonMode && strings.Count(out.String(), `"code":"post_subscriber_failed"`) != 2 {
				t.Fatalf("JSON lost warnings: %s", out.String())
			}
			_, err = local.Client.Call(context.Background(), "bead.get", map[string]any{"id": map[string]any{"id": "pl-missing"}}, "")
			var typed *wire.Error
			if !errors.As(err, &typed) || typed.Data.Code != "not_found" {
				t.Fatalf("lost typed error: %T %v", err, err)
			}
		})
	}
}

func TestRealClientKeepsReplayMetadataSeparateFromStoredResult(t *testing.T) {
	post := &unavailablePost{}
	l, err := OpenLocal(context.Background(), LocalConfig{
		Project: "local", Agent: "cli-test", Storage: StorageConfig{Driver: "memory"},
		PipelineOptions: []pipeline.Option{pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "test-warning", Subscriber: post})},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	ctx := context.Background()
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": "Stored result"}}
	first, err := l.Client.Call(ctx, "bead.create", params, "")
	if err != nil || first.Replayed {
		t.Fatalf("first committed response: %+v %v", first, err)
	}
	_, err = l.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Later write"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := l.Client.Call(ctx, "bead.create", params, first.IdempotencyKey)
	if err != nil || !replay.Replayed || !bytes.Equal(first.JSON, replay.JSON) || post.calls != 2 {
		t.Fatalf("replay metadata/result/callbacks: first=%s replay=%+v posts=%d error=%v", first.JSON, replay, post.calls, err)
	}
	decoded, err := wire.DecodeJSON(first.JSON)
	if err != nil {
		t.Fatal(err)
	}
	object := decoded.(map[string]any)
	if object["_meta"] != nil || len(object["warnings"].([]any)) != 1 {
		t.Fatalf("response metadata changed stored result: %s", first.JSON)
	}
	ref := object["results"].([]any)[0].(map[string]any)["ref"]
	params = map[string]any{"id": ref, "fields": map[string]any{"title": "Stored result"}}
	noop, err := l.Client.Call(ctx, "bead.update", params, "")
	if err != nil || noop.Replayed || post.calls != 2 {
		t.Fatalf("fresh no-op marked replayed or reran callback: %+v %v posts=%d", noop, err, post.calls)
	}
	replay, err = l.Client.Call(ctx, "bead.update", params, noop.IdempotencyKey)
	if err != nil || !replay.Replayed || !bytes.Equal(noop.JSON, replay.JSON) || post.calls != 2 {
		t.Fatalf("no-op receipt metadata/result: %+v %v posts=%d", replay, err, post.calls)
	}
}

func TestStorageFactoryUsesOneStoreAndClosesOnce(t *testing.T) {
	opened, closed := 0, 0
	factory := func(ctx context.Context, cfg StorageConfig) (driver.Store, func() error, error) {
		opened++
		if cfg != (StorageConfig{Driver: "provided", DSN: "local-test"}) {
			t.Fatalf("factory configuration: %+v", cfg)
		}
		store := memory.New()
		return store, func() error { closed++; return store.Close() }, nil
	}
	l, err := OpenLocal(context.Background(), LocalConfig{Project: "local", Agent: "cli-test", Factory: factory, Storage: StorageConfig{Driver: "provided", DSN: "local-test"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Client.Call(context.Background(), "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Through provided storage"}}, ""); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if opened != 1 || closed != 1 {
		t.Fatalf("store lifetime: opened %d closed %d", opened, closed)
	}
}

func TestInvalidFactoryResultClosesReturnedStore(t *testing.T) {
	store := memory.New()
	l, err := OpenLocal(context.Background(), LocalConfig{Factory: func(context.Context, StorageConfig) (driver.Store, func() error, error) {
		return store, nil, nil
	}})
	if err == nil || l != nil {
		t.Fatalf("invalid factory result accepted: %v %v", l, err)
	}
	if _, err := store.Meta(context.Background()); err == nil {
		t.Fatal("failed composition left the returned store open")
	}
}

func TestRemoteConfigurationRefusesLocalFallback(t *testing.T) {
	t.Setenv("PLUG_SERVER", "http://127.0.0.1:1")
	var out, errOut bytes.Buffer
	code := Execute(context.Background(), []string{"--memory", "ready"}, strings.NewReader(""), &out, &errOut)
	if code != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), "PLUG_SERVER") || strings.Contains(errOut.String(), "memory storage is disposable") {
		t.Fatalf("remote fallback: code=%d out=%s err=%s", code, out.String(), errOut.String())
	}
}

func TestReadyCursorTraversesRealByteBoundary(t *testing.T) {
	l, err := OpenLocal(context.Background(), LocalConfig{Project: "local", Agent: "cli-test", Storage: StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var out, errOut bytes.Buffer
	s := &Session{Client: l.Client, Out: &out, Err: &errOut, JSON: true}
	for _, title := range []string{"First", "Second"} {
		if err := s.Run(context.Background(), []string{"create", "--title", title}); err != nil {
			t.Fatal(err)
		}
	}
	out.Reset()
	if err := s.Run(context.Background(), []string{"ready", "--limit", "1"}); err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodeJSON(bytes.TrimSpace(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	first := decoded.(map[string]any)
	cursor := first["nextCursor"].(string)
	out.Reset()
	if err := s.Run(context.Background(), []string{"ready", "--limit", "1", "--cursor", cursor}); err != nil {
		t.Fatal(err)
	}
	decoded, err = wire.DecodeJSON(bytes.TrimSpace(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	second := decoded.(map[string]any)
	if second["nextCursor"] != nil || first["items"].([]any)[0].(map[string]any)["id"] == second["items"].([]any)[0].(map[string]any)["id"] {
		t.Fatalf("page traversal failed: first=%+v second=%+v", first, second)
	}
}
