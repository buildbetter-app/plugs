package cardb01

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "card-b01"

type fixture struct {
	ctx       context.Context
	path      string
	store     driver.Store
	lease     driver.ActiveLease
	transport *inprocess.Transport
	nextKey   int
}

func newFixture(t *testing.T, ids ...string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	path := filepath.Join(t.TempDir(), "card-b01.db")
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "card-b01-server", driver.LeaseTTL)
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
	now := func() time.Time { return time.Date(2026, 10, 30, 8, 30, 0, 0, time.UTC) }
	nextID := 0
	idGenerator := func() (string, error) {
		if nextID >= len(ids) {
			return "", fmt.Errorf("card B01 fixture exhausted its declared IDs")
		}
		id := ids[nextID]
		nextID++
		return id, nil
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(
		store,
		lease,
		pipeline.WithClock(now),
		pipeline.WithIDGenerator(idGenerator),
		pipeline.WithOntology(ontology),
	)
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
	server, err := rpc.New(registry, rpc.Config{
		DefaultProject: project,
		Incarnation:    meta.Incarnation,
		CursorKey:      []byte("card-b01-fixed-cursor-key-32bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{ctx: ctx, path: path, store: store, lease: lease, transport: inprocess.New(server)}
}

type envelope struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error"`
}

type bead struct {
	ID     string         `json:"id"`
	Status string         `json:"status"`
	Fields map[string]any `json:"fields"`
}

type readyPage struct {
	Items      []bead `json:"items"`
	NextCursor string `json:"nextCursor"`
	AsOf       struct {
		Revision json.Number `json:"revision"`
	} `json:"asOf"`
}

func (f *fixture) call(t *testing.T, method string, params map[string]any, write bool) json.RawMessage {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	if write {
		f.nextKey++
		sum := sha256.Sum256([]byte(fmt.Sprintf("card-b01/%d", f.nextKey)))
		params["_meta"] = map[string]any{
			"agent":          "card-b01-recorder",
			"idempotencyKey": hex.EncodeToString(sum[:]),
		}
	}
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      f.nextKey + 1,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.transport.RoundTrip(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var got envelope
	decoder := json.NewDecoder(bytes.NewReader(response))
	decoder.UseNumber()
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode %s response: %v: %s", method, err, response)
	}
	if got.Error != nil {
		t.Fatalf("%s refused with %s: %s", method, got.Error.Data.Code, response)
	}
	if len(got.Result) == 0 {
		t.Fatalf("%s omitted result: %s", method, response)
	}
	return got.Result
}

func decode[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var value T
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (f *fixture) create(t *testing.T, title string, priority int) {
	t.Helper()
	f.call(t, "bead.create", map[string]any{
		"type": "issue",
		"fields": map[string]any{
			"title":    title,
			"priority": priority,
		},
	}, true)
}

func (f *fixture) ready(t *testing.T, limit int, cursor string) readyPage {
	t.Helper()
	params := map[string]any{"limit": limit}
	if cursor != "" {
		params["cursor"] = cursor
	}
	return decode[readyPage](t, f.call(t, "bead.ready", params, false))
}

func (f *fixture) get(t *testing.T, id string) bead {
	t.Helper()
	result := decode[struct {
		Bead bead `json:"bead"`
	}](t, f.call(t, "bead.get", map[string]any{"id": map[string]any{"id": id}}, false))
	return result.Bead
}

func (f *fixture) assertSQLite(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 16 || string(data[:16]) != "SQLite format 3\x00" {
		t.Fatalf("fixture did not use a real SQLite file: %q", data[:min(16, len(data))])
	}
	info := (sqlite.Driver{}).Info()
	if info.Name != "sqlite" {
		t.Fatalf("driver identity = %+v", info)
	}
}

func ids(items []bead) []string {
	result := make([]string, len(items))
	for i, item := range items {
		result[i] = item.ID
	}
	return result
}

func ref(id string) map[string]any { return map[string]any{"id": id} }
