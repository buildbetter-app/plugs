package cardb28

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "card-b28"

type fixture struct {
	ctx       context.Context
	store     driver.Store
	transport *inprocess.Transport
	nextKey   int
}

func newFixture(t *testing.T, ids []string, options ...pipeline.Option) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	path := filepath.Join(t.TempDir(), "card-b28.db")
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "card-b28-server", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	now := func() time.Time { return time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC) }
	next := 0
	nextID := func() (string, error) {
		if next >= len(ids) {
			return "", fmt.Errorf("card B28 fixture exhausted its declared IDs")
		}
		id := ids[next]
		next++
		return id, nil
	}
	ontology := service.NewOntology(nil)
	base := []pipeline.Option{pipeline.WithClock(now), pipeline.WithIDGenerator(nextID), pipeline.WithOntology(ontology)}
	base = append(base, options...)
	writer := pipeline.New(store, lease, base...)
	domain := service.New(store, lease, writer, service.WithClock(now), service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: []byte("card-b28-fixed-cursor-key-32bytes")})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{ctx: ctx, store: store, transport: inprocess.New(server)}
}

type envelope struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error,omitempty"`
}

func requestBytes(t *testing.T, id int, method string, params map[string]any, key string) []byte {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	copy := make(map[string]any, len(params)+1)
	for name, value := range params {
		copy[name] = value
	}
	if key != "" {
		copy["_meta"] = map[string]any{"project": project, "agent": "card-b28", "idempotencyKey": key}
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": copy})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f *fixture) roundTrip(t *testing.T, raw []byte) (json.RawMessage, *envelope) {
	t.Helper()
	response, err := f.transport.RoundTrip(f.ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	var got envelope
	if err := json.Unmarshal(response, &got); err != nil {
		t.Fatalf("decode response: %v: %s", err, response)
	}
	return response, &got
}

func (f *fixture) write(t *testing.T, method string, params map[string]any, key string) model.Commit {
	t.Helper()
	f.nextKey++
	_, got := f.roundTrip(t, requestBytes(t, f.nextKey, method, params, key))
	if got.Error != nil {
		t.Fatalf("%s refused with %s", method, got.Error.Data.Code)
	}
	var commit model.Commit
	if err := json.Unmarshal(got.Result, &commit); err != nil {
		t.Fatalf("decode %s result: %v", method, err)
	}
	return commit
}

func (f *fixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func key(label string) string {
	digest := sha256.Sum256([]byte("card-b28/" + label))
	return hex.EncodeToString(digest[:])
}

func createParams(title string) map[string]any {
	return map[string]any{"type": "issue", "fields": map[string]any{"title": title}}
}

func updateParams(id, title string) map[string]any {
	return map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": title}}
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func sameResult(t *testing.T, a, b json.RawMessage) {
	t.Helper()
	var left, right any
	dec := json.NewDecoder(bytes.NewReader(a))
	dec.UseNumber()
	if err := dec.Decode(&left); err != nil {
		t.Fatal(err)
	}
	dec = json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&right); err != nil {
		t.Fatal(err)
	}
	la, _ := json.Marshal(left)
	ra, _ := json.Marshal(right)
	if !bytes.Equal(la, ra) {
		t.Fatalf("result changed: %s != %s", la, ra)
	}
}

type mutatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error)

func (f mutatorFunc) Mutate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return f(ctx, p, r)
}

type validatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (f validatorFunc) Validate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	return f(ctx, p, r)
}

type postFunc func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (f postFunc) After(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
	return f(ctx, c, r)
}
