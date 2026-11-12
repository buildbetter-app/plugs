package parentedcreateflat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const (
	project     = "s1"
	incarnation = "s1-fixed-incarnation"
	owner       = "s1-owner"
)

// These ordinary fixture helpers are copied unchanged from the public B04
// manifest fixture so this isolated diagnostic package is self-contained.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func decodeObject(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		t.Fatal("accepted trailing JSON value")
	}
	return value
}

type testClock struct {
	*testclock.Clock
	t *testing.T
}

func newClock(t *testing.T) *testClock {
	return &testClock{Clock: testclock.New(t, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)), t: t}
}

func (c *testClock) Advance(d time.Duration) { c.Clock.Advance(c.t, d) }

type fixture struct {
	t         *testing.T
	ctx       context.Context
	backend   string
	path      string
	dsn       string
	store     driver.Store
	lease     driver.ActiveLease
	clock     *testClock
	writer    *pipeline.Writer
	svc       *service.Service
	transport *inprocess.Transport
	ontology  *service.Ontology
	ids       []string
	idIndex   int
	requestMu sync.Mutex
	request   int
}

func openStore(t *testing.T, backend, path, dsn string, clock *testClock) driver.Store {
	t.Helper()
	ctx := context.Background()
	switch backend {
	case "memory":
		store, err := memory.NewWithOptions(memory.Options{Clock: clock.Now, StoreID: "s1-memory", Incarnation: incarnation})
		if err != nil {
			t.Fatal(err)
		}
		return store
	case "sqlite":
		store, err := (sqlite.Driver{Clock: clock.Now}).Open(ctx, driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		return store
	case "postgres":
		store, err := postgres.Open(ctx, driver.Config{DSN: dsn})
		if err != nil {
			t.Fatal(err)
		}
		return store
	default:
		t.Fatalf("unknown backend %q", backend)
		return nil
	}
}

func newFixture(t *testing.T, backend, dsn string, rules *service.Ontology, ids []string) *fixture {
	t.Helper()
	clock := newClock(t)
	if backend == "postgres" {
		dsn = clock.BindPostgres(t, dsn)
	}
	path := ""
	if backend == "sqlite" {
		path = filepath.Join(t.TempDir(), "s1.db")
	}
	store := openStore(t, backend, path, dsn, clock)
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := store.Lease(ctx, "s1-server", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		cancel()
		t.Fatal(err)
	}
	f := &fixture{t: t, ctx: ctx, backend: backend, path: path, dsn: dsn, store: store, lease: lease, clock: clock, ontology: rules, ids: append([]string(nil), ids...)}
	f.install()
	t.Cleanup(func() {
		_ = f.lease.Release(context.Background())
		_ = f.store.Close()
		cancel()
	})
	return f
}

func (f *fixture) install() {
	f.t.Helper()
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithClock(f.clock.Now), pipeline.WithIDGenerator(f.nextID), pipeline.WithOntology(f.ontology))
	f.svc = service.New(f.store, f.lease, f.writer, service.WithClock(f.clock.Now), service.WithOntology(f.ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.svc); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: []byte("s1-b01-b04-fixed-cursor-key-32-bytes-min")})
	if err != nil {
		f.t.Fatal(err)
	}
	f.transport = inprocess.New(server)
}

func (f *fixture) nextID() (string, error) {
	if f.idIndex >= len(f.ids) {
		return "", fmt.Errorf("S1 fixture exhausted fixed IDs")
	}
	id := f.ids[f.idIndex]
	f.idIndex++
	return id, nil
}

func (f *fixture) key(label, method, agent string) string {
	sum := sha256.Sum256([]byte("s1-b01-b04/" + label + "/" + method + "/" + agent))
	return hex.EncodeToString(sum[:])
}

func (f *fixture) meta(label, method, agent string) model.WriteMeta {
	return model.WriteMeta{Project: project, Agent: agent, AgentLabel: agent, Method: method, IdempotencyKey: f.key(label, method, agent)}
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func createOp(id, inProject string, fields map[string]any, parent string) model.Op {
	var p *model.Ref
	if parent != "" {
		value := ref(parent)
		p = &value
	}
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: id, Type: "issue", Project: inProject, Fields: fields, Parent: p}}
}

func patchOp(id string, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(id), Fields: fields}}
}

func linkOp(kind model.OpKind, from, to, linkKind string) model.Op {
	return model.Op{Kind: kind, Link: &model.LinkOp{From: ref(from), To: ref(to), LinkKind: linkKind}}
}

func (f *fixture) apply(label, method string, ops ...model.Op) model.Commit {
	f.t.Helper()
	commit, err := f.writer.Apply(f.ctx, model.PrincipalRef{Kind: "agent", ID: owner}, model.ChangeSet{Ops: ops}, f.meta(label, method, owner))
	if err != nil {
		f.t.Fatalf("%s: %v", label, err)
	}
	return commit
}

func (f *fixture) call(label, method, agent string, params map[string]any) []byte {
	f.t.Helper()
	f.requestMu.Lock()
	f.request++
	requestID := f.request
	f.requestMu.Unlock()
	if params == nil {
		params = map[string]any{}
	}
	copyParams := make(map[string]any, len(params)+1)
	for key, value := range params {
		copyParams[key] = value
	}
	copyParams["_meta"] = map[string]any{"project": project, "agent": agent, "idempotencyKey": f.key(label, method, agent)}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": requestID, "method": method, "params": copyParams})
	if err != nil {
		f.t.Fatal(err)
	}
	response, err := f.transport.RoundTrip(f.ctx, request)
	if err != nil {
		f.t.Fatalf("%s transport: %v", method, err)
	}
	if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
		name := strings.ReplaceAll(f.t.Name(), "/", "_") + ".rpc.jsonl"
		path := filepath.Join(output, name)
		file, openErr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if openErr == nil {
			_, _ = file.Write(append(response, '\n'))
			_ = file.Close()
		}
	}
	return response
}

func result(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var envelope map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["error"] != nil {
		t.Fatalf("unexpected RPC error: %s", raw)
	}
	value, ok := envelope["result"].(map[string]any)
	if !ok {
		t.Fatalf("RPC result is not an object: %s", raw)
	}
	return value
}

func errorCode(t *testing.T, raw []byte) string {
	t.Helper()
	var envelope struct {
		Error *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error == nil {
		return ""
	}
	return envelope.Error.Data.Code
}

func (f *fixture) readyIDs(label, agent string) []string {
	value := result(f.t, f.call(label, "bead.ready", agent, map[string]any{"limit": 1000}))
	items, ok := value["items"].([]any)
	if !ok {
		f.t.Fatalf("ready items missing: %#v", value)
	}
	ids := make([]string, 0, len(items))
	for _, raw := range items {
		item := raw.(map[string]any)
		ids = append(ids, item["id"].(string))
	}
	return ids
}

func (f *fixture) document(id string) pipeline.Document {
	f.t.Helper()
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.metaSnapshot().Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(f.ctx, []driver.Ref{ref(id)})
	if err != nil || len(rows) != 1 {
		f.t.Fatalf("read %s: %v rows=%d", id, err, len(rows))
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return doc
}

func (f *fixture) metaSnapshot() driver.Meta {
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return meta
}

func (f *fixture) imageDigest() string {
	f.t.Helper()
	meta := f.metaSnapshot()
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer reader.Close()
	var records []driver.Record
	var cursor *driver.Cursor
	for {
		page, queryErr := reader.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if queryErr != nil {
			f.t.Fatal(queryErr)
		}
		records = append(records, page.Items...)
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	changes, err := reader.Changesets(f.ctx, nil, 1000)
	if err != nil {
		f.t.Fatal(err)
	}
	payload, err := json.Marshal(struct {
		Head    model.Revision    `json:"head"`
		Records []driver.Record   `json:"records"`
		Changes []model.Changeset `json:"changes"`
	}{meta.Head, records, changes})
	if err != nil {
		f.t.Fatal(err)
	}
	return sha256Hex(payload)
}

func (f *fixture) reopenSQLite() {
	f.t.Helper()
	if f.backend != "sqlite" {
		return
	}
	if err := f.lease.Release(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.store = openStore(f.t, "sqlite", f.path, "", f.clock)
	lease, err := f.store.Lease(f.ctx, "s1-reopened", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease = lease
	f.install()
}

func customDocument() ontology.Document {
	return ontology.Document{
		Statuses: map[string]ontology.Status{
			"queued":    {Name: "Queued", Category: ontology.Unstarted},
			"working":   {Name: "Working", Category: ontology.Started},
			"finished":  {Name: "Finished", Category: ontology.Completed, Outcome: ontology.Success},
			"broken":    {Name: "Broken", Category: ontology.Completed, Outcome: ontology.Failure},
			"abandoned": {Name: "Abandoned", Category: ontology.Canceled},
			"parked":    {Name: "Parked", Category: ontology.Backlog},
			"shared":    {Name: "Shared", Category: ontology.Backlog},
		},
		Workflows: map[string]ontology.Workflow{"issue": {
			Statuses: []string{"queued", "working", "finished", "broken", "abandoned", "parked", "shared"},
			Initial:  "queued",
			Slots: map[ontology.Slot]string{
				ontology.Claim: "working", ontology.Close: "finished", ontology.Fail: "broken",
				ontology.Cancel: "abandoned", ontology.Reopen: "queued", ontology.Defer: "parked",
			},
			Transitions: []ontology.Rule{{From: []string{"queued"}, To: "working", Guards: []ontology.Guard{{Kind: ontology.NoOpenBlockers}}}, {To: "finished", Guards: []ontology.Guard{{Kind: ontology.NoOpenBlockers}}}, {To: "broken"}, {To: "abandoned"}, {To: "queued"}, {To: "parked"}, {To: "shared"}},
		}},
		Types: map[string]ontology.Type{"issue": {Name: "Issue", Workflow: "issue", Fields: map[string]ontology.Field{"title": {Type: "string"}}}},
	}
}

func compileOntology(t *testing.T, base ontology.Document, overlays map[string]ontology.Document) *service.Ontology {
	t.Helper()
	snapshot, problems := ontology.Compile(base, overlays, nil)
	if len(problems) != 0 {
		t.Fatalf("ontology compile: %+v", problems)
	}
	return service.NewOntology(snapshot.Effective)
}

type postgresHarness struct {
	databaseMu sync.Mutex
	name       string
	address    string
	seq        int
}

func docker(args ...string) ([]byte, error) {
	cmd := exec.Command("docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	cmd.Env = os.Environ()
	return cmd.CombinedOutput()
}

func newPostgresHarness(t *testing.T) *postgresHarness {
	t.Helper()
	name := fmt.Sprintf("plug-swarm-s1-b01-b04-%d-%d", os.Getpid(), time.Now().UnixNano())
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	if output, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image, "-c", "shared_preload_libraries=pg_stat_statements", "-c", "session_preload_libraries=auto_explain", "-c", "pg_stat_statements.track=all", "-c", "pg_stat_statements.track_utility=on", "-c", "auto_explain.log_min_duration=250ms", "-c", "auto_explain.log_analyze=on", "-c", "auto_explain.log_buffers=on", "-c", "auto_explain.log_format=json", "-c", "auto_explain.log_nested_statements=on", "-c", "auto_explain.log_timing=off", "-c", "auto_explain.log_settings=on", "-c", "log_min_duration_statement=250", "-c", "log_lock_waits=on", "-c", "deadlock_timeout=100ms", "-c", "log_error_verbosity=verbose", "-c", "log_min_error_statement=error", "-c", "log_line_prefix=%m [%p] %q%u@%d SQLSTATE=%e "); err != nil {
		t.Fatalf("postgres start: %v %s", err, output)
	}
	t.Cleanup(func() { _, _ = docker("rm", "-f", name) })
	output, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("postgres port: %v %s", err, output)
	}
	address := strings.TrimSpace(string(output))
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("postgres bound outside loopback: %q", address)
	}
	deadline := time.Now().Add(45 * time.Second)
	for {
		if _, err := docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("postgres readiness timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("owned PostgreSQL image=%s loopback_only=true", image)
	return &postgresHarness{name: name, address: address}
}

func (h *postgresHarness) dsn(t *testing.T) string {
	t.Helper()
	h.databaseMu.Lock()
	defer h.databaseMu.Unlock()
	h.seq++
	database := fmt.Sprintf("s1_%d", h.seq)
	if output, err := docker("exec", h.name, "createdb", "-h", "127.0.0.1", "-U", "postgres", database); err != nil {
		t.Fatalf("postgres create database: %v %s", err, output)
	}
	return "postgres://postgres@" + h.address + "/" + database + "?sslmode=disable"
}

func sortedStrings(values []string) []string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return copyValues
}

func typedCode(err error) string {
	var value *model.Error
	if !errors.As(err, &value) {
		return ""
	}
	return string(value.Data.Code)
}

// Advancing persisted time can expire the ten-second storage lease. Release it
// explicitly and acquire the next epoch; never stretch its production budget.
func (f *fixture) advance(d time.Duration) {
	f.t.Helper()
	if err := f.lease.Release(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	f.clock.Advance(d)
	lease, err := f.store.Lease(f.ctx, "s1-clock-advance", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease = lease
	f.install()
}
