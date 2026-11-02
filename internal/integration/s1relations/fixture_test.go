package s1relations_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var actor = model.PrincipalRef{Kind: "user", ID: "s1-relations"}
var fixedTime = time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC)

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type frame struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

type fixture struct {
	t         *testing.T
	ctx       context.Context
	kind, dsn string
	ids       []string
	idIndex   int
	store     driver.Store
	lease     driver.ActiveLease
	transport *inprocess.Transport
	options   []pipeline.Option
	frames    []frame
}

func newFixture(t *testing.T, kind string, ids []string, options ...pipeline.Option) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &fixture{t: t, ctx: rpc.WithPrincipal(ctx, actor), kind: kind, ids: append([]string(nil), ids...), options: options}
	switch kind {
	case "sqlite":
		f.dsn = filepath.Join(t.TempDir(), "s1.sqlite")
	case "postgres":
		f.dsn = freshDatabase(t)
	}
	f.open()
	t.Cleanup(func() {
		f.close()
		if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
			name := strings.ReplaceAll(t.Name(), "/", "_") + ".rpc.json"
			if err := os.WriteFile(filepath.Join(dir, name), jsonBytes(t, f.frames), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	return f
}

func (f *fixture) open() {
	f.t.Helper()
	var err error
	switch f.kind {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: func() time.Time { return fixedTime }, StoreID: "s1-relations", Incarnation: "s1-relations-memory"})
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatalf("unknown required store %q", f.kind)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "s1-relations", driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		f.store = nil
		f.t.Fatal(err)
	}
	f.bind()
}

func (f *fixture) bind() {
	f.t.Helper()
	// Match the original B25 declared-field setup. Do not change global defaults
	// or substitute the separate top-level authorization label attribute.
	document := ontology.DefaultDocument()
	document.Types["issue"].Fields["labels"] = ontology.Field{Type: "strings"}
	snapshot, problems := ontology.Compile(document, nil, nil)
	if len(problems) != 0 {
		f.t.Fatalf("declared B25 ontology: %v", problems)
	}
	rules := service.NewOntology(snapshot.Effective)
	options := []pipeline.Option{
		pipeline.WithOntology(rules),
		pipeline.WithClock(func() time.Time { return fixedTime }),
		pipeline.WithIDGenerator(func() (string, error) {
			if f.idIndex >= len(f.ids) {
				return "", fmt.Errorf("declared S1 ID sequence exhausted")
			}
			id := f.ids[f.idIndex]
			f.idIndex++
			return id, nil
		}),
		pipeline.WithPostFailureStore(f.store.(driver.CheckpointStore)),
	}
	options = append(options, f.options...)
	writer := pipeline.New(f.store, f.lease, options...)
	domain := service.New(f.store, f.lease, writer, service.WithOntology(rules), service.WithClock(func() time.Time { return fixedTime }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "s1", Incarnation: meta.Incarnation,
		AuthMode: "authenticated", OntologyRevision: model.Natural64(1), CursorKey: []byte("s1-relations-cursor-key-0123456789")})
	if err != nil {
		f.t.Fatal(err)
	}
	f.transport = inprocess.New(server)
}

func (f *fixture) close() {
	f.t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) reopen() {
	f.t.Helper()
	if f.kind == "memory" {
		// A fresh server/Writer on the same in-memory store tests durable receipt
		// lookup without claiming restart persistence for the memory driver.
		f.bind()
		return
	}
	f.close()
	f.open()
}

func requestKey(label string) string {
	sum := sha256.Sum256([]byte("s1-relations/" + label))
	return hex.EncodeToString(sum[:])
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func equalJSON(t *testing.T, want, got any) {
	t.Helper()
	canonical := func(value any) []byte {
		var decoded any
		decoder := json.NewDecoder(bytes.NewReader(jsonBytes(t, value)))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		return jsonBytes(t, decoded)
	}
	a, b := canonical(want), canonical(got)
	if !bytes.Equal(a, b) {
		t.Fatalf("JSON differs:\nwant %s\n got %s", a, b)
	}
}

func (f *fixture) call(method, label string, params map[string]any) reply {
	f.t.Helper()
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	copyParams["_meta"] = map[string]any{"project": "s1", "idempotencyKey": requestKey(label)}
	request := jsonBytes(f.t, map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": copyParams})
	response, err := f.transport.RoundTrip(f.ctx, request)
	if err != nil {
		f.t.Fatal(err)
	}
	f.frames = append(f.frames, frame{Request: request, Response: response})
	var out reply
	if err := json.Unmarshal(response, &out); err != nil {
		f.t.Fatalf("invalid RPC response %s: %v", response, err)
	}
	return out
}

func (f *fixture) write(method, label string, params map[string]any) model.Commit {
	f.t.Helper()
	got := f.call(method, label, params)
	if got.Error != nil {
		f.t.Fatalf("%s refused: %s; raw=%s", method, got.Error.Data.Code, f.frames[len(f.frames)-1].Response)
	}
	var commit model.Commit
	if err := json.Unmarshal(got.Result, &commit); err != nil {
		f.t.Fatalf("%s commit %s: %v", method, got.Result, err)
	}
	return commit
}

func (f *fixture) reader() driver.Reader {
	f.t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func (f *fixture) document(id string) pipeline.Document {
	f.t.Helper()
	r := f.reader()
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref(id)})
	if err != nil || len(rows) != 1 {
		f.t.Fatalf("record %s: rows=%d err=%v", id, len(rows), err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return doc
}

// Include every record, version, event and head relevant to this fixture.
// Receipts are checked separately with their actual principal/method/key.
func (f *fixture) image() []byte {
	f.t.Helper()
	r := f.reader()
	defer r.Close()
	refs := make([]model.Ref, len(f.ids))
	for i, id := range f.ids {
		refs[i] = ref(id)
	}
	rows, err := r.Get(f.ctx, refs)
	if err != nil {
		f.t.Fatal(err)
	}
	versions := make(map[string][]driver.Version)
	for _, id := range f.ids {
		page, err := r.Versions(f.ctx, ref(id), driver.PageReq{})
		if err != nil || page.NextCursor != nil {
			f.t.Fatalf("complete version image %s: %+v err=%v", id, page, err)
		}
		versions[id] = page.Items
	}
	changes, err := r.Changesets(f.ctx, nil, 100)
	if err != nil || len(changes) >= 100 {
		f.t.Fatalf("complete changeset image: n=%d err=%v", len(changes), err)
	}
	ancestors, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		f.t.Fatal(err)
	}
	return jsonBytes(f.t, struct {
		Head      *model.Revision
		Rows      []driver.Record
		Versions  map[string][]driver.Version
		Changes   []model.Changeset
		Ancestors []driver.DerivedRow
	}{r.Snapshot().At, rows, versions, changes, ancestors})
}

func (f *fixture) receipt(method, label string) *driver.IdempotencyRecord {
	f.t.Helper()
	r := f.reader()
	defer r.Close()
	principal, err := driver.PrincipalKey(actor)
	if err != nil {
		f.t.Fatal(err)
	}
	got, err := r.IdempotencyByKey(f.ctx, principal, "", method, requestKey(label))
	if err != nil {
		f.t.Fatal(err)
	}
	return got
}

func equalReceipt(t *testing.T, want, got *driver.IdempotencyRecord) {
	t.Helper()
	// No-op receipts intentionally carry no changeset. Marshaling that zero
	// model as a standalone wire changeset would manufacture an invalid kind.
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("durable receipt differs:\nwant %+v\n got %+v", want, got)
	}
}
