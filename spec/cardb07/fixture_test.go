package cardb20

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func b07Hash(v []byte) string { h := sha256.Sum256(v); return hex.EncodeToString(h[:]) }
func b07JSON(t *testing.T, path string, v any) {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}
func b07Output(t *testing.T, label string) string {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute test output directory required")
	}
	p := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "_"), label)
	if e := os.MkdirAll(p, 0700); e != nil {
		t.Fatal(e)
	}
	return p
}

type b07Reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Message string          `json:"message"`
		Data    model.ErrorData `json:"data"`
	} `json:"error"`
	Meta map[string]any `json:"_meta"`
}
type b07Host struct {
	t            *testing.T
	ctx          context.Context
	runtime      *composition.Runtime
	config       composition.Config
	root         string
	seq          int
	observations int
	scopes       map[string][2]string
}

func b07NewHost(t *testing.T, engine string) *b07Host {
	t.Helper()
	h := &b07Host{t: t, ctx: context.Background(), root: b07Output(t, "plug"), scopes: map[string][2]string{}, config: composition.Config{Driver: engine, Project: "b07", AuthMode: "anonymous", CursorKey: []byte("b07-public-ready-cursor-signing-key")}}
	root, err := os.MkdirTemp(h.root, "store-")
	if err != nil {
		t.Fatal(err)
	}
	h.root = root

	switch engine {
	case "memory":
	case "sqlite":
		h.config.DSN = filepath.Join(t.TempDir(), "b07.db")
	case "postgres":
		h.config.DSN = b07Postgres(t, h.root)
	default:
		t.Fatal(engine)
	}
	h.open()
	t.Cleanup(h.close)
	return h
}
func (h *b07Host) open() {
	h.t.Helper()
	r, e := composition.Open(h.ctx, h.config)
	if e != nil {
		h.t.Fatal(e)
	}
	h.runtime = r
	meta, e := r.Store.Meta(h.ctx)
	if e != nil {
		h.t.Fatal(e)
	}
	b07JSON(h.t, filepath.Join(h.root, fmt.Sprintf("open-%04d.json", h.seq)), map[string]any{"engine": h.config.Driver, "meta": meta, "handle": fmt.Sprintf("%p", r.Store), "epoch": r.Lease.Epoch(), "locator_sha256": b07Hash([]byte(h.config.DSN))})
}
func (h *b07Host) close() {
	if h.runtime == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if e := h.runtime.Close(ctx); e != nil {
		h.t.Error(e)
	}
	h.runtime = nil
}
func (h *b07Host) reopen() {
	h.t.Helper()
	if h.config.Driver == "memory" {
		h.t.Log("B07 memory uses the same store, not a physical reopen")
		return
	}
	before := h.snapshot()
	old := h.runtime.Store
	epoch := h.runtime.Lease.Epoch()
	h.close()
	h.open()
	if old == h.runtime.Store || h.runtime.Lease.Epoch() <= epoch {
		h.t.Fatal("SQL reopen did not acquire a distinct handle and increasing epoch")
	}
	h.same(before)
	h.t.Logf("B07_SQL_REOPEN engine=%s old=%p new=%p epoch=%v->%v", h.config.Driver, old, h.runtime.Store, epoch, h.runtime.Lease.Epoch())
}
func (h *b07Host) call(method, label string, params map[string]any) b07Reply {
	h.t.Helper()
	h.seq++
	p := map[string]any{}
	for k, v := range params {
		p[k] = v
	}
	meta := map[string]any{"project": h.config.Project, "agent": "b07-public"}
	if label != "" {
		key := b07Hash([]byte(label))
		meta["idempotencyKey"] = key
		h.scopes[label] = [2]string{method, key}
	}
	p["_meta"] = meta
	request, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": h.seq, "method": method, "params": p})
	if e != nil {
		h.t.Fatal(e)
	}
	raw := h.runtime.RPC.Dispatch(rpc.WithPrincipal(h.ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), request)
	stem := filepath.Join(h.root, fmt.Sprintf("rpc-%04d", h.seq))
	for suffix, b := range map[string][]byte{"request": request, "response": raw} {
		if e := os.WriteFile(stem+"."+suffix+".json", b, 0600); e != nil {
			h.t.Fatal(e)
		}
	}
	b07JSON(h.t, stem+".receipt.json", map[string]any{"method": method, "label": label, "request_sha256": b07Hash(request), "response_sha256": b07Hash(raw)})
	var r b07Reply
	if e = json.Unmarshal(raw, &r); e != nil {
		h.t.Fatal(e)
	}
	return r
}
func b07Success(t *testing.T, r b07Reply) model.Commit {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("public refusal: %+v", r.Error)
	}
	var c model.Commit
	if e := json.Unmarshal(r.Result, &c); e != nil {
		t.Fatal(e)
	}
	return c
}
func (h *b07Host) create(label string, parent *model.Ref) model.Ref {
	h.t.Helper()
	p := map[string]any{"type": "issue", "fields": map[string]any{"title": label}}
	if parent != nil {
		p["parent"] = *parent
	}
	c := b07Success(h.t, h.call("bead.create", "create-"+label, p))
	if len(c.Results) != 1 {
		h.t.Fatal(c)
	}
	return c.Results[0].Ref
}
func (h *b07Host) ready() []string {
	h.t.Helper()
	r := h.call("bead.ready", "", map[string]any{"limit": 128})
	if r.Error != nil {
		h.t.Fatal(r.Error)
	}
	var p struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if e := json.Unmarshal(r.Result, &p); e != nil {
		h.t.Fatal(e)
	}
	if p.NextCursor != "" {
		h.t.Fatal("fixture exceeded complete ready-page bound")
	}
	ids := []string{}
	for _, i := range p.Items {
		ids = append(ids, i.ID)
	}
	sort.Strings(ids)
	return ids
}
func b07Without(ids []string, id string) []string {
	out := []string{}
	for _, s := range ids {
		if s != id {
			out = append(out, s)
		}
	}
	return out
}
func b07Has(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}
func (h *b07Host) requireReady(want []string) {
	h.t.Helper()
	got := h.ready()
	if !reflect.DeepEqual(got, want) {
		h.t.Fatalf("ready membership got=%v want=%v", got, want)
	}
}

type b07Receipt struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       *model.Changeset `json:",omitempty"`
	ResponsePending bool
}
type b07State struct {
	Checkpoints map[string]driver.Checkpoint
	Meta        driver.Meta
	Rows        []driver.Record
	Versions    map[string][]driver.Version
	Changes     []model.Changeset
	Derived     map[driver.DerivedTable][]driver.DerivedRow
	Receipts    map[string]b07Receipt
}

func (h *b07Host) snapshot() b07State {
	h.t.Helper()
	m, e := h.runtime.Store.Meta(h.ctx)
	if e != nil {
		h.t.Fatal(e)
	}
	capability, ok := h.runtime.Store.(driver.CheckpointInventoryStore)
	if !ok {
		h.t.Fatal("complete checkpoint inventory unavailable")
	}
	r, e := capability.ReadCheckpointInventory(h.ctx, driver.View{Incarnation: m.Incarnation, At: &m.Head})
	if e != nil {
		h.t.Fatal(e)
	}
	defer func() {
		if e := r.Close(); e != nil {
			h.t.Error(e)
		}
	}()
	s := b07State{Meta: m, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]b07Receipt{}}
	var cursor *driver.Cursor
	for {
		p, e := r.Query(h.ctx, driver.Query{Limit: 128, Cursor: cursor})
		if e != nil {
			h.t.Fatal(e)
		}
		s.Rows = append(s.Rows, p.Items...)
		if p.NextCursor == nil {
			break
		}
		if len(p.Items) == 0 || reflect.DeepEqual(cursor, p.NextCursor) {
			h.t.Fatal("record pagination stalled")
		}
		cursor = p.NextCursor
	}
	sort.Slice(s.Rows, func(i, j int) bool { return s.Rows[i].Ref.ID < s.Rows[j].Ref.ID })
	for _, row := range s.Rows {
		cursor = nil
		for {
			p, e := r.Versions(h.ctx, row.Ref, driver.PageReq{Limit: 128, Cursor: cursor})
			if e != nil {
				h.t.Fatal(e)
			}
			s.Versions[row.Ref.ID] = append(s.Versions[row.Ref.ID], p.Items...)
			if p.NextCursor == nil {
				break
			}
			if len(p.Items) == 0 || reflect.DeepEqual(cursor, p.NextCursor) {
				h.t.Fatal("version pagination stalled")
			}
			cursor = p.NextCursor
		}
	}
	cursor = nil
	if !m.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: m.Incarnation, Revision: m.HistoryFloor}
	}
	for {
		sets, e := r.Changesets(h.ctx, cursor, 128)
		if e != nil {
			h.t.Fatal(e)
		}
		s.Changes = append(s.Changes, sets...)
		if len(sets) < 128 {
			break
		}
		cursor = &driver.Cursor{Incarnation: m.Incarnation, Revision: sets[len(sets)-1].Revision, Index: model.Natural64(0)}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, e := r.DerivedRows(h.ctx, table, "")
		if e != nil {
			h.t.Fatal(e)
		}
		s.Derived[table] = rows
	}
	principal, e := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if e != nil {
		h.t.Fatal(e)
	}
	for label, scope := range h.scopes {
		v, e := r.IdempotencyByKey(h.ctx, principal, "b07-public", scope[0], scope[1])
		if e != nil {
			h.t.Fatal(e)
		}
		if v != nil {
			receipt := b07Receipt{RequestHash: v.RequestHash, Commit: v.Commit, ResponsePending: v.ResponsePending}
			if !reflect.DeepEqual(v.Changeset, model.Changeset{}) {
				c := v.Changeset
				receipt.Changeset = &c
			}
			s.Receipts[label] = receipt
		}
	}
	keys, e := r.CheckpointKeys()
	if e != nil {
		h.t.Fatal(e)
	}
	s.Checkpoints = map[string]driver.Checkpoint{}
	for _, key := range keys {
		checkpoint, e := r.SourcePlaneCheckpoint(key)
		if e != nil {
			h.t.Fatal(e)
		}
		s.Checkpoints[key] = checkpoint
	}
	h.observations++
	b07JSON(h.t, filepath.Join(h.root, fmt.Sprintf("state-%04d.json", h.observations)), s)
	return s
}
func (h *b07Host) same(want b07State) {
	h.t.Helper()
	got := h.snapshot()
	if !reflect.DeepEqual(got, want) {
		h.t.Fatal("full records/versions/history/derived/receipt state changed")
	}
}
func (h *b07Host) quiet(want b07State, label string, r b07Reply) {
	h.t.Helper()
	c := b07Success(h.t, r)
	got := h.snapshot()
	receipt, ok := got.Receipts[label]
	if !ok || receipt.ResponsePending || receipt.Changeset != nil || c.Revision.String() != want.Meta.Head.String() {
		h.t.Fatal("no-op did not retain a completed zero-changeset receipt at the unchanged head")
	}
	delete(got.Receipts, label)
	if !reflect.DeepEqual(got, want) {
		h.t.Fatal("no-op changed more than its one receipt")
	}
}
func (h *b07Host) linkValue(from, to model.Ref, kind string) {
	h.t.Helper()
	s := h.snapshot()
	for _, row := range s.Rows {
		if row.Ref.ID == from.ID {
			d, e := pipeline.DecodeDocument(row.Body)
			if e != nil {
				h.t.Fatal(e)
			}
			if len(d.Links) != 1 || d.Links[0].To.ID != to.ID || d.Links[0].To.Branch.String() != to.Branch.String() || d.Links[0].Kind != kind {
				h.t.Fatalf("edge identity/value changed: %+v", d.Links)
			}
			return
		}
	}
	h.t.Fatal("source row absent")
}

func b07Postgres(t *testing.T, root string) string {
	t.Helper()
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	var nonce [12]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		t.Fatal(e)
	}
	name := "plug-swarm-b07-" + hex.EncodeToString(nonce[:])
	seq := 0
	run := func(args ...string) ([]byte, error) {
		seq++
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, env := range os.Environ() {
			key, _, _ := strings.Cut(env, "=")
			if !strings.HasPrefix(key, "DOCKER_") {
				cmd.Env = append(cmd.Env, env)
			}
		}
		out, e := cmd.CombinedOutput()
		b07JSON(t, filepath.Join(root, fmt.Sprintf("docker-%02d.json", seq)), map[string]any{"argv": cmd.Args, "combined_output": string(out), "error": fmt.Sprint(e), "output_sha256": b07Hash(out)})
		return out, e
	}
	out, e := run("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_b07", "-p", "127.0.0.1::5432", image)
	if e != nil {
		t.Fatalf("owned PostgreSQL start: %v %s", e, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		out, e := run("rm", "-f", name)
		if e != nil || strings.TrimSpace(string(out)) != name {
			t.Errorf("owned PostgreSQL cleanup: %v %s", e, out)
		}
		t.Logf("B07_POSTGRES_REMOVED name=%s start_id=%s", name, id)
	})
	out, e = run("port", name, "5432/tcp")
	if e != nil {
		t.Fatal(e)
	}
	address := strings.TrimSpace(string(out))
	host, _, e := net.SplitHostPort(address)
	if e != nil || host != "127.0.0.1" {
		t.Fatal("PostgreSQL must own a loopback port")
	}
	dsn := "postgres://postgres@" + address + "/plug_b07?sslmode=disable"
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	var version string
	if e := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); e != nil {
		t.Fatal(e)
	}
	b07JSON(t, filepath.Join(root, "postgres.json"), map[string]any{"version": version, "image": image, "container_id": id, "name": name, "address": address})
	t.Logf("B07_REAL_POSTGRES %s", version)
	return dsn
}
