package cardb20

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const frontProject = "b01-canonical-front"

type publicFront struct {
	t                         *testing.T
	ctx                       context.Context
	store                     driver.Store
	lease                     driver.ActiveLease
	clock                     *testclock.Clock
	onto                      *service.Ontology
	transport                 *inprocess.Transport
	engine, dsn, root, nextID string
	seq                       int
	ids                       map[string]string
}

func newPublicFront(t *testing.T, engine, root string, start time.Time) *publicFront {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	f := &publicFront{t: t, ctx: ctx, engine: engine, root: root, clock: testclock.New(t, start), ids: map[string]string{}}
	doc := ontology.DefaultDocument()
	doc.Statuses["b01active"] = ontology.Status{Name: "Custom active", Category: ontology.Unstarted}
	w := doc.Workflows["issue"]
	w.Statuses = append(w.Statuses, "b01active")
	doc.Workflows["issue"] = w
	no := false
	doc.Types["infrastructure"] = ontology.Type{Name: "Hidden infrastructure", Workflow: "issue", ReadyEligible: &no}
	compiled, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	f.onto = service.NewOntology(compiled.Effective)
	switch engine {
	case "memory":
	case "sqlite":
		f.dsn = filepath.Join(t.TempDir(), "front.db")
	case "postgres":
		f.dsn = f.clock.BindPostgres(t, testclock.PostgresDSN(t))
	default:
		t.Fatal("unknown real engine", engine)
	}
	f.open()
	t.Cleanup(func() {
		if f.lease != nil {
			if e := f.lease.Release(context.Background()); e != nil {
				t.Error(e)
			}
		}
		if e := f.store.Close(); e != nil {
			t.Error(e)
		}
	})
	return f
}
func (f *publicFront) open() {
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: f.clock.Now})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.bind()
}
func (f *publicFront) bind() {
	var err error
	f.lease, err = f.store.Lease(f.ctx, "b01-canonical-public", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	writer := pipeline.New(f.store, f.lease, pipeline.WithClock(f.clock.Now), pipeline.WithOntology(f.onto), pipeline.WithIDGenerator(func() (string, error) {
		if f.nextID == "" {
			return "", fmt.Errorf("no declared next fixture ID")
		}
		id := f.nextID
		f.nextID = ""
		return id, nil
	}))
	svc := service.New(f.store, f.lease, writer, service.WithClock(f.clock.Now), service.WithOntology(f.onto))
	reg, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(reg, svc); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(reg, rpc.Config{DefaultProject: frontProject, Incarnation: meta.Incarnation, CursorKey: []byte("b01-canonical-public-stable-cursor-key")})
	if err != nil {
		f.t.Fatal(err)
	}
	f.transport = inprocess.New(server)
}
func (f *publicFront) at(at time.Time) {
	if at.Before(f.clock.Now()) {
		f.t.Fatal("fixture clock must not move backward")
	}
	if err := f.lease.Release(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	f.lease = nil
	f.clock.Advance(f.t, at.Sub(f.clock.Now()))
	f.bind()
}
func (f *publicFront) call(label, method string, params map[string]any) json.RawMessage {
	f.t.Helper()
	f.seq++
	params["_meta"] = map[string]any{"project": frontProject, "agent": "b01-public-recorder", "idempotencyKey": evidenceHash([]byte(fmt.Sprintf("%s/%d/%s", f.root, f.seq, method)))}
	request := frontJSON(f.t, map[string]any{"jsonrpc": "2.0", "id": f.seq, "method": method, "params": params})
	raw, err := f.transport.RoundTrip(f.ctx, request)
	stem := fmt.Sprintf("%03d-%s", f.seq, label)
	frontSave(f.t, f.root, stem+".request.json", request)
	frontSave(f.t, f.root, stem+".response.json", raw)
	frontSave(f.t, f.root, stem+".binding.json", frontJSON(f.t, map[string]any{"method": method, "request_sha256": evidenceHash(request), "response_sha256": evidenceHash(raw), "clock": f.clock.Now(), "transport_error": fmt.Sprint(err)}))
	if err != nil {
		f.t.Fatal(err)
	}
	var out struct {
		Result json.RawMessage
		Error  json.RawMessage
	}
	if err = json.Unmarshal(raw, &out); err != nil {
		f.t.Fatal(err)
	}
	if len(out.Error) != 0 && string(out.Error) != "null" {
		f.t.Fatalf("%s public refusal %s", method, out.Error)
	}
	if len(out.Result) == 0 {
		f.t.Fatal("missing result")
	}
	return out.Result
}
func (f *publicFront) ref(role string) map[string]any { return map[string]any{"id": f.ids[role]} }
func (f *publicFront) populate(c frontCase, anchor time.Time) {
	// Only create-time sequencing is ordered here. Expected ready sequences above
	// are independently literal, and the tie-z/tie-a insertion stays reversed.
	nodes := append([]frontNode(nil), c.Nodes...)
	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].Age > nodes[j].Age })
	for _, n := range nodes {
		f.at(anchor.Add(-n.Age))
		f.nextID = "pl-b01-" + n.Role
		f.ids[n.Role] = f.nextID
		kind := "issue"
		if n.Kind == "agent" {
			kind = "infrastructure"
		}
		fields := map[string]any{"title": n.Role, "priority": n.Priority}
		raw := f.call("create-"+n.Role, "bead.create", map[string]any{"type": kind, "fields": fields})
		var commit model.Commit
		if err := json.Unmarshal(raw, &commit); err != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID != f.ids[n.Role] {
			f.t.Fatalf("actual created ref mismatch %s: %s %v", n.Role, raw, err)
		}
		switch n.Status {
		case "b01active", "in_progress":
			f.call("status-"+n.Role, "bead.update", map[string]any{"id": f.ref(n.Role), "fields": map[string]any{"status": n.Status}})
		case "pinned":
			f.call("pin-as-backlog-"+n.Role, "bead.defer", map[string]any{"id": f.ref(n.Role), "reason": "explicit native pinned to adopted backlog category"})
		case "closed":
			f.call("close-"+n.Role, "bead.close", map[string]any{"id": f.ref(n.Role), "reason": "closed exclusion"})
		}
	}
	f.at(anchor)
	if c.Name == "category_and_blocked_front" {
		f.call("block-front", "bead.link", map[string]any{"from": f.ref("blocked"), "to": f.ref("blocker"), "kind": "blocks"})
	}
	if len(frontPhases(c)) > 1 {
		f.call("parent-child", "bead.link", map[string]any{"from": f.ref("child"), "to": f.ref("parent"), "kind": "parent-child"})
		f.call("child-grandchild", "bead.link", map[string]any{"from": f.ref("grandchild"), "to": f.ref("child"), "kind": "parent-child"})
	}
}

type frontPage struct {
	Items []struct {
		ID        string
		Revision  model.Revision
		CreatedAt time.Time
	} `json:"items"`
	Next *string      `json:"nextCursor"`
	AsOf service.AsOf `json:"asOf"`
}

func (f *publicFront) roles(items []string) []string {
	out := make([]string, len(items))
	for i, id := range items {
		if !strings.HasPrefix(id, "pl-b01-") {
			f.t.Fatal("unexpected public ID", id)
		}
		out[i] = strings.TrimPrefix(id, "pl-b01-")
	}
	return out
}
func (f *publicFront) pages(label string, want []string) []string {
	before := f.snapshot()
	var observed []string
	for _, limit := range []int{1, 2, 1000} {
		params := map[string]any{"limit": limit}
		got := []string{}
		seen := map[string]bool{}
		for pageNo := 0; ; pageNo++ {
			if pageNo > len(want)+1 {
				f.t.Fatal("cursor failed to terminate")
			}
			raw := f.call(fmt.Sprintf("%s-limit%d-page%d", label, limit, pageNo), "bead.ready", params)
			var page frontPage
			if err := json.Unmarshal(raw, &page); err != nil || page.Items == nil || page.AsOf.Incarnation == "" || page.AsOf.Revision.Validate() != nil {
				f.t.Fatalf("invalid page %s %v", raw, err)
			}
			if len(page.Items) > limit {
				f.t.Fatal("page exceeded limit")
			}
			for _, item := range page.Items {
				if seen[item.ID] || item.Revision.Validate() != nil {
					f.t.Fatal("duplicate ID/invalid revision")
				}
				seen[item.ID] = true
				got = append(got, item.ID)
			}
			if page.Next == nil {
				break
			}
			if len(page.Items) != limit || *page.Next == "" {
				f.t.Fatal("short nonterminal page")
			}
			params["cursor"] = *page.Next
		}
		observed = f.roles(got)
		frontEqual(f.t, fmt.Sprintf("%s/limit%d", label, limit), observed, want)
	}
	if after := f.snapshot(); !reflect.DeepEqual(before, after) {
		f.t.Fatal("ready reads changed literal durable rows, versions, journal, meta or stored ranks")
	}
	return observed
}

type frontState struct {
	Meta     driver.Meta
	Rows     []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
	Ranks    []driver.DerivedRow
}

func (f *publicFront) snapshot() frontState {
	f.t.Helper()
	m, e := f.store.Meta(f.ctx)
	if e != nil {
		f.t.Fatal(e)
	}
	r, e := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if e != nil {
		f.t.Fatal(e)
	}
	defer r.Close()
	q, e := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if e != nil || q.NextCursor != nil {
		f.t.Fatal("state query", e)
	}
	s := frontState{Meta: m, Rows: q.Items, Versions: map[string][]driver.Version{}}
	for _, row := range q.Items {
		v, e := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if e != nil || v.NextCursor != nil {
			f.t.Fatal(e)
		}
		s.Versions[row.Ref.ID] = v.Items
	}
	s.Log, e = r.Changesets(f.ctx, nil, 1000)
	if e != nil {
		f.t.Fatal(e)
	}
	s.Ranks, e = r.DerivedRows(f.ctx, driver.DerivedSortKey, "")
	if e != nil {
		f.t.Fatal(e)
	}
	return s
}
func (f *publicFront) reopen(label string) {
	if f.engine == "memory" {
		frontSave(f.t, f.root, label+"-reopen.json", frontJSON(f.t, map[string]any{"engine": "memory", "persistent_reopen": "NOT_SUPPORTED_NOT_CLAIMED"}))
		return
	}
	before := f.snapshot()
	if e := f.lease.Release(f.ctx); e != nil {
		f.t.Fatal(e)
	}
	f.lease = nil
	if e := f.store.Close(); e != nil {
		f.t.Fatal(e)
	}
	f.open()
	after := f.snapshot()
	if !reflect.DeepEqual(before, after) {
		f.t.Fatal("actual SQL close/reopen changed source, versions, journal or ranks")
	}
	if f.engine == "sqlite" {
		raw, e := os.ReadFile(f.dsn)
		if e != nil || len(raw) < 16 || string(raw[:16]) != "SQLite format 3\x00" {
			f.t.Fatal("not an actual SQLite store", e)
		}
	}
	if f.engine == "postgres" && !testclock.PostgresNow(f.t, f.dsn).Equal(f.clock.Now()) {
		f.t.Fatal("actual PostgreSQL clock differs")
	}
	frontSave(f.t, f.root, label+"-reopen.json", frontJSON(f.t, map[string]any{"engine": f.engine, "actual_close_open_rebind": true, "before": before, "after": after}))
}
func runPublicFront(t *testing.T, engine, root string, c frontCase, anchor time.Time) map[string][]string {
	t.Helper()
	f := newPublicFront(t, engine, root, anchor.Add(-100*time.Hour))
	f.populate(c, anchor)
	out := map[string][]string{}
	for _, phase := range frontPhases(c) {
		switch phase {
		case "blocked":
			f.call("block-ancestor", "bead.link", map[string]any{"from": f.ref("parent"), "to": f.ref("blocker"), "kind": "blocks"})
		case "unblocked":
			f.call("close-ancestor-blocker", "bead.close", map[string]any{"id": f.ref("blocker"), "reason": "actual blocker close restores descendant readiness"})
		case "indefinite":
			f.call("defer-ancestor", "bead.defer", map[string]any{"id": f.ref("parent"), "reason": "indefinite adopted transitive ancestor suppression"})
			// Plug deliberately has no automatic timed defer. Keep parent deferred;
			// paired timed native vector remains separately recorded, never equalized.
		}
		if phase == "open" {
			f.parentFilterCapability()
		}
		out[phase] = f.pages(phase, frontWant(c.Name, phase).Plug)
		f.reopen(phase)
		f.pages(phase+"-after-reopen", frontWant(c.Name, phase).Plug)
	}
	frontSave(t, root, "actual-public-vectors.json", frontJSON(t, map[string]any{"engine": engine, "case": c, "vectors": out, "clock": anchor, "native_wisp_to_public_durable_projection": true, "ephemeral_plane_parity": "NOT_CLAIMED", "timed_expiry": "NOT_CLAIMED"}))
	return out
}

func (f *publicFront) parentFilterCapability() {
	before := f.snapshot()
	request := frontJSON(f.t, map[string]any{"jsonrpc": "2.0", "id": "parent-capability", "method": "bead.ready", "params": map[string]any{"limit": 1, "filter": map[string]any{"parent": f.ref("parent")}, "_meta": map[string]any{"project": frontProject, "agent": "b01-public-recorder", "idempotencyKey": evidenceHash([]byte("parent-capability"))}}})
	raw, err := f.transport.RoundTrip(f.ctx, request)
	if err != nil {
		f.t.Fatal(err)
	}
	frontSave(f.t, f.root, "unsupported-parent-filter.request.json", request)
	frontSave(f.t, f.root, "unsupported-parent-filter.response.json", raw)
	var response struct {
		Error  *struct{ Data struct{ Code string } }
		Result json.RawMessage
	}
	if json.Unmarshal(raw, &response) != nil || response.Error == nil || response.Error.Data.Code != "invalid_params" || !strings.Contains(string(raw), "parent") || len(response.Result) != 0 {
		f.t.Fatalf("unsupported parent filter must be refused, not emulated: %s", raw)
	}
	if after := f.snapshot(); !reflect.DeepEqual(before, after) {
		f.t.Fatal("parent filter refusal changed durable state")
	}
}
