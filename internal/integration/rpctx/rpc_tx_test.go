package rpctx_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "rpc-tx"

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type fixture struct {
	ctx       context.Context
	engine    string
	path      string
	store     driver.Store
	lease     driver.ActiveLease
	ontology  *service.Ontology
	writer    *pipeline.Writer
	domain    *service.Service
	server    *rpc.Server
	client    *client.Client
	nextID    atomic.Uint64
	clock     time.Time
	incarnate string
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, engine: engine, clock: time.Date(2026, 10, 29, 16, 0, 0, 0, time.UTC), incarnate: "rpc-tx-" + engine}
	if engine == "sqlite" {
		f.path = t.TempDir() + "/plug.db"
	}
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: func() time.Time { return f.clock }, StoreID: driver.StoreID("rpc-tx-memory"), Incarnation: f.incarnate})
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	default:
		t.Fatalf("unknown engine %q", f.engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "rpc-tx-"+f.engine, driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatal(err)
	}
	f.ontology = service.NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology),
		pipeline.WithClock(func() time.Time { return f.clock }),
		pipeline.WithIDGenerator(func() (string, error) {
			n := f.nextID.Add(1)
			return fmt.Sprintf("pl-rpc-%s-%03d", f.engine, n), nil
		}),
	)
	f.domain = service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(func() time.Time { return f.clock }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: project, CursorKey: bytes.Repeat([]byte{0x27}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	f.client, err = client.New(inprocess.New(f.server), project, "rpc-tx-agent")
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) close(t *testing.T) {
	t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	if f.engine != "sqlite" {
		t.Fatalf("reopen is only qualified for SQLite")
	}
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.close(t)
	f.open(t)
	after, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.StoreID != after.StoreID || before.Incarnation != after.Incarnation || before.Head != after.Head {
		t.Fatalf("SQLite reopen changed identity or head: before=%+v after=%+v", before, after)
	}
}

func key(label string) string {
	digest := sha256.Sum256([]byte("plug/rpc-tx/" + label))
	return hex.EncodeToString(digest[:])
}

func decodeCommit(t *testing.T, raw []byte) model.Commit {
	t.Helper()
	var commit model.Commit
	if err := json.Unmarshal(raw, &commit); err != nil {
		t.Fatalf("decode commit %s: %v", raw, err)
	}
	return commit
}

func resultRef(t *testing.T, commit model.Commit, index int) model.Ref {
	t.Helper()
	if index >= len(commit.Results) || !commit.Results[index].Ref.IsID() {
		t.Fatalf("missing result %d: %+v", index, commit.Results)
	}
	return commit.Results[index].Ref
}

func (f *fixture) document(t *testing.T, ref model.Ref, at *model.Revision) pipeline.Document {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read %s at %v: rows=%d err=%v", ref.ID, at, len(rows), err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func (f *fixture) state(t *testing.T, ref model.Ref) (driver.Meta, []model.Changeset, []driver.Version) {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	changes, err := r.Changesets(f.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	versions, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 100})
	if err != nil || versions.NextCursor != nil {
		t.Fatalf("versions: %+v err=%v", versions, err)
	}
	return meta, changes, versions.Items
}

func TestTransactionAndDependencyRPCsAcrossMemoryAndSQLite(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ctx := f.ctx

			// The CLI uses the same client and exact byte transport as callers.
			var cliOut, cliErr bytes.Buffer
			session := &cli.Session{Client: f.client, Out: &cliOut, Err: &cliErr, JSON: true}
			if err := session.Run(ctx, []string{"create", "--title", "blocker", "--idempotency-key", key("create-blocker")}); err != nil {
				t.Fatal(err)
			}
			blockerCommit := decodeCommit(t, bytes.TrimSpace(cliOut.Bytes()))
			blocker := resultRef(t, blockerCommit, 0)

			cliOut.Reset()
			if err := session.Run(ctx, []string{"batch", "--changeset", `{"ops":[{"kind":"create","key":"dependent","type":"issue","fields":{"title":"dependent"}},{"kind":"link","from":{"key":"dependent"},"to":{"id":"` + blocker.ID + `"},"linkKind":"blocks"}]}`, "--idempotency-key", key("mixed-batch")}); err != nil {
				t.Fatal(err)
			}
			mixedBytes := bytes.TrimSpace(cliOut.Bytes())
			mixed := decodeCommit(t, mixedBytes)
			dependent := resultRef(t, mixed, 0)
			if mixed.Revision.String() != "2" || len(mixed.Results) != 2 || mixed.Results[0].Ref != dependent || mixed.Results[1].Ref != dependent {
				t.Fatalf("mixed tx was not one atomic revision: %+v", mixed)
			}
			// A retry through the client proves that the CLI's receipt is the same
			// durable response and that replay metadata stays outside the result.
			replay, err := f.client.Call(ctx, "tx.apply", map[string]any{"changeSet": map[string]any{
				"ops": []any{
					map[string]any{"kind": "create", "key": "dependent", "type": "issue", "fields": map[string]any{"title": "dependent"}},
					map[string]any{"kind": "link", "from": map[string]any{"key": "dependent"}, "to": map[string]any{"id": blocker.ID}, "linkKind": "blocks"},
				},
			}}, key("mixed-batch"))
			if err != nil || !replay.Replayed || !bytes.Equal(replay.JSON, mixedBytes) {
				t.Fatalf("tx.apply replay changed receipt: replay=%+v err=%v first=%s", replay, err, mixedBytes)
			}

			// Named link/unlink lower to the same graph operation and remain
			// idempotent through the CLI path.
			cliOut.Reset()
			if err := session.Run(ctx, []string{"link", dependent.ID, blocker.ID, "--kind", "relates-to", "--idempotency-key", key("named-link")}); err != nil {
				t.Fatal(err)
			}
			namedLink := bytes.TrimSpace(cliOut.Bytes())
			cliOut.Reset()
			if err := session.Run(ctx, []string{"link", dependent.ID, blocker.ID, "--kind", "relates-to", "--idempotency-key", key("named-link")}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(cliOut.String(), `"revision":3`) {
				t.Fatalf("named link replay did not preserve revision: %s", cliOut.String())
			}
			if !bytes.Equal(namedLink, bytes.TrimSpace(cliOut.Bytes())) {
				t.Fatalf("named link replay changed receipt: first=%s replay=%s", namedLink, cliOut.Bytes())
			}
			cliOut.Reset()
			if err := session.Run(ctx, []string{"unlink", dependent.ID, blocker.ID, "--kind", "relates-to", "--idempotency-key", key("named-unlink")}); err != nil {
				t.Fatal(err)
			}
			unlink := decodeCommit(t, bytes.TrimSpace(cliOut.Bytes()))
			if unlink.Revision.String() != "4" {
				t.Fatalf("named unlink revision=%s", unlink.Revision)
			}

			// The graph is inspected from the real store, while history and the
			// typed refusal cross the exact RPC bytes.
			doc := f.document(t, dependent, nil)
			if len(doc.Links) != 1 || doc.Links[0].To.ID != blocker.ID || doc.Links[0].Kind != "blocks" {
				t.Fatalf("final graph=%+v", doc.Links)
			}
			history, err := f.client.Call(ctx, "history.query", map[string]any{"asOf": map[string]any{"revision": 2}, "filter": map[string]any{"resources": []string{dependent.ID}}, "limit": 10}, "")
			if err != nil {
				t.Fatal(err)
			}
			var page struct {
				Items []map[string]any `json:"items"`
			}
			decoder := json.NewDecoder(bytes.NewReader(history.JSON))
			decoder.UseNumber()
			if err := decoder.Decode(&page); err != nil || len(page.Items) != 1 || page.Items[0]["revision"] != json.Number("2") {
				t.Fatalf("history through RPC=%s err=%v", history.JSON, err)
			}
			cycle, err := f.client.Call(ctx, "bead.link", map[string]any{"from": map[string]any{"id": blocker.ID}, "to": map[string]any{"id": dependent.ID}, "kind": "blocks"}, key("cycle"))
			var typed *wire.Error
			if err == nil || !errors.As(err, &typed) || typed.Data.Code != "ontology_guard_failed" || cycle.Replayed {
				t.Fatalf("cycle refusal was not typed and non-replay: result=%+v err=%v", cycle, err)
			}

			meta, changes, versions := f.state(t, dependent)
			if meta.Head.String() != "4" || len(changes) != 4 || len(versions) != 3 {
				t.Fatalf("durable graph history changed unexpectedly: head=%s changes=%d versions=%d", meta.Head, len(changes), len(versions))
			}
			principal, err := driver.PrincipalKey(anonymous)
			if err != nil {
				t.Fatal(err)
			}
			r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := r.IdempotencyByKey(ctx, principal, "rpc-tx-agent", "tx.apply", key("mixed-batch"))
			_ = r.Close()
			if err != nil || receipt == nil || receipt.Commit.Revision.String() != "2" {
				t.Fatalf("mixed tx receipt missing: %+v err=%v", receipt, err)
			}

			if engine == "sqlite" {
				f.reopen(t)
				if got := f.document(t, dependent, nil); !reflect.DeepEqual(got.Links, doc.Links) {
					t.Fatalf("reopen changed graph: before=%+v after=%+v", doc.Links, got.Links)
				}
				meta, changes, versions = f.state(t, dependent)
				if meta.Head.String() != "4" || len(changes) != 4 || len(versions) != 3 {
					t.Fatalf("reopen changed durable history: head=%s changes=%d versions=%d", meta.Head, len(changes), len(versions))
				}
				r, err := f.client.Call(ctx, "tx.apply", map[string]any{"changeSet": map[string]any{
					"ops": []any{
						map[string]any{"kind": "create", "key": "dependent", "type": "issue", "fields": map[string]any{"title": "dependent"}},
						map[string]any{"kind": "link", "from": map[string]any{"key": "dependent"}, "to": map[string]any{"id": blocker.ID}, "linkKind": "blocks"},
					},
				}}, key("mixed-batch"))
				if err != nil || !r.Replayed {
					t.Fatalf("reopen did not replay receipt: %+v err=%v", r, err)
				}
			}
		})
	}
}
