package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type b06PlugFixture struct {
	t        *testing.T
	engine   string
	store    driver.Store
	open     func() (driver.Store, error)
	lease    driver.ActiveLease
	domain   *service.Service
	server   *rpc.Server
	by       model.PrincipalRef
	nextID   string
	requests int
	writes   map[string]string
}

func b06OpenPlug(t *testing.T, engine string) *b06PlugFixture {
	t.Helper()
	f := &b06PlugFixture{t: t, engine: engine, by: model.PrincipalRef{Kind: "user", ID: "b06-reader"}, writes: map[string]string{}}
	ctx := context.Background()
	switch engine {
	case "memory":
		f.open = func() (driver.Store, error) { return memory.New(), nil }
	case "sqlite":
		path := filepath.Join(t.TempDir(), "b06.db")
		f.open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path}) }
	case "postgres":
		dsn := startPostgres(t)
		f.open = func() (driver.Store, error) { return (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn}) }
	default:
		t.Fatal("unknown B06 engine")
	}
	var err error
	f.store, err = f.open()
	if err != nil {
		t.Fatal(err)
	}
	f.bind()
	t.Cleanup(f.close)
	return f
}

func (f *b06PlugFixture) bind() {
	t := f.t
	var err error
	f.lease, err = f.store.Lease(context.Background(), "b06-cycles", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	o := service.NewOntology(nil)
	w := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithIDGenerator(func() (string, error) {
		id := f.nextID
		f.nextID = ""
		if id == "" {
			return "", fmt.Errorf("unexpected B06 create")
		}
		return id, nil
	}))
	f.domain = service.New(f.store, f.lease, w, service.WithOntology(o))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err = rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: "b06", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte("b06-canonical-cycles-public-key-0001")})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *b06PlugFixture) close() {
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

func (f *b06PlugFixture) reopen() {
	if f.engine == "memory" {
		f.t.Log("B06_REOPEN engine=memory same_store=true physical=false")
		return
	}
	f.close()
	var err error
	f.store, err = f.open()
	if err != nil {
		f.t.Fatal(err)
	}
	f.bind()
	f.t.Logf("B06_REOPEN engine=%s physical=true reconstructed_writer_service_rpc=true", f.engine)
}

type b06Envelope struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
}

// A real no-op receipt has no changeset. Its zero Changeset is not a valid
// changeset document, so retain that absence explicitly while preserving every
// remaining response/digest field and every ordinary changeset in full.
type b06ReceiptImage struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       *model.Changeset
	ResponsePending bool
}

func (f *b06PlugFixture) call(method string, params map[string]any, write bool) b06Envelope {
	f.t.Helper()
	f.requests++
	meta := map[string]any{"project": "b06", "agent": "b06-recorder"}
	if write {
		meta["idempotencyKey"] = fmt.Sprintf("%032x", f.requests)
		f.writes[method+"/"+fmt.Sprint(f.requests)] = method
	}
	params["_meta"] = meta
	req, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.requests, "method": method, "params": params})
	if err != nil {
		f.t.Fatal(err)
	}
	raw := f.server.Dispatch(rpc.WithPrincipal(context.Background(), f.by), req)
	b06Retain(f.t, fmt.Sprintf("rpc-%03d.json", f.requests), map[string]any{"request": json.RawMessage(req), "response": json.RawMessage(raw)})
	var out b06Envelope
	if err := json.Unmarshal(raw, &out); err != nil {
		f.t.Fatal(err)
	}
	return out
}

func b06RequireSuccess[T any](t *testing.T, got b06Envelope) T {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("B06 unexpected refusal %+v", got.Error.Data)
	}
	var result T
	if err := json.Unmarshal(got.Result, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *b06PlugFixture) create(id string) {
	f.nextID = id
	commit := b06RequireSuccess[model.Commit](f.t, f.call("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": id}}, true))
	if len(commit.Results) != 1 || commit.Results[0].Ref.ID != id {
		f.t.Fatal("source ID was not preserved")
	}
}

func (f *b06PlugFixture) link(edge b06Edge) b06Envelope {
	return f.call("bead.link", map[string]any{"from": model.RefByID(edge.From, model.Natural64(0)), "to": model.RefByID(edge.To, model.Natural64(0)), "kind": edge.Kind}, true)
}

func (f *b06PlugFixture) image(label string) []byte {
	f.t.Helper()
	archive, _, err := restore.Dump(context.Background(), f.store, "full")
	if err != nil {
		f.t.Fatal(err)
	}
	// Dump binds records/history. Explicit readback also binds ordinary and
	// no-op receipts, including absence for the next possible failed mutation.
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			f.t.Error(err)
		}
	}()
	principal, err := driver.PrincipalKey(f.by)
	if err != nil {
		f.t.Fatal(err)
	}
	keys := map[string]string{}
	for key, method := range f.writes {
		keys[key] = method
	}
	for _, method := range []string{"bead.create", "bead.link"} {
		keys[method+"/"+fmt.Sprint(f.requests+1)] = method
	}
	receipts := map[string]b06ReceiptImage{}
	for scoped, method := range keys {
		var index int
		if _, err := fmt.Sscanf(scoped[len(method)+1:], "%d", &index); err != nil {
			f.t.Fatal(err)
		}
		value, err := r.IdempotencyByKey(context.Background(), principal, "", method, fmt.Sprintf("%032x", index))
		if err != nil {
			f.t.Fatal(err)
		}
		if value != nil {
			if value.ResponsePending {
				f.t.Fatal("unfinished response receipt")
			}
			var changeset *model.Changeset
			if value.Changeset.Kind != "" {
				copy := value.Changeset
				changeset = &copy
			} else if !reflect.DeepEqual(value.Changeset, model.Changeset{}) {
				f.t.Fatal("no-op receipt has an unexpected nonzero changeset image")
			}
			receipts[scoped] = b06ReceiptImage{value.RequestHash, value.Commit, changeset, value.ResponsePending}
		}
	}
	if len(f.writes) > 0 && len(receipts) == 0 {
		f.t.Fatal("receipt census did not bind actual successful writes")
	}
	state := map[string]any{"archive": archive, "attempted_receipts": receipts}
	b06Retain(f.t, label+"-full-image.json", state)
	data, err := json.Marshal(state)
	if err != nil {
		f.t.Fatal(err)
	}
	return data
}

func (f *b06PlugFixture) report(tracks bool) service.CyclesResult {
	f.t.Helper()
	before := f.image(fmt.Sprintf("before-report-%d", f.requests+1))
	got := b06RequireSuccess[service.CyclesResult](f.t, f.call("bead.cycles", map[string]any{"includeTracks": tracks}, false))
	direct, err := f.domain.Cycles(context.Background(), f.by, service.CyclesRequest{Project: "b06", IncludeTracks: tracks})
	if err != nil || !reflect.DeepEqual(got, direct) {
		f.t.Fatalf("service/RPC disagreement %+v %+v %v", got, direct, err)
	}
	if !bytes.Equal(before, f.image(fmt.Sprintf("after-report-%d", f.requests))) {
		f.t.Fatal("cycle read changed full state")
	}
	return got
}

func b06PlugPaths(got service.CyclesResult) [][]string {
	paths := make([][]string, 0, len(got.Items))
	for _, cycle := range got.Items {
		path := make([]string, 0, len(cycle.Path))
		for _, m := range cycle.Path {
			if m.Ref == nil {
				path = append(path, "")
			} else {
				path = append(path, m.Ref.ID)
			}
		}
		paths = append(paths, path)
	}
	return paths
}

func b06AssertReport(t *testing.T, got service.CyclesResult, want [][]string) {
	t.Helper()
	if !reflect.DeepEqual(b06PlugPaths(got), want) || got.CycleCount.String() != fmt.Sprint(len(want)) || got.Truncated {
		t.Fatalf("raw report/order/count got=%+v want=%v", got, want)
	}
	for _, c := range got.Items {
		if c.MemberCount.String() != fmt.Sprint(len(c.Path)) || c.Partial || !c.UnreadableCount.IsZero() {
			t.Fatalf("unexpected partial %+v", c)
		}
	}
}

func (f *b06PlugFixture) refuse(edge b06Edge) {
	f.t.Helper()
	before := f.image(fmt.Sprintf("before-refusal-%d", f.requests+1))
	got := f.link(edge)
	if got.Error == nil || got.Error.Data.Code != "ontology_guard_failed" || len(got.Result) != 0 {
		f.t.Fatalf("expected exact graph refusal: %+v", got)
	}
	if !bytes.Equal(before, f.image(fmt.Sprintf("after-refusal-%d", f.requests))) {
		f.t.Fatal("refusal wrote graph/history/receipt")
	}
}

func (f *b06PlugFixture) capability() {
	f.t.Helper()
	before := f.image(fmt.Sprintf("before-capability-%d", f.requests+1))
	got := f.call("bead.cycles", map[string]any{"includeEphemeral": true}, false)
	if got.Error == nil || got.Error.Data.Code != "plugin_durable_state_unavailable" || len(got.Result) != 0 {
		f.t.Fatalf("inconsistent capability refusal %+v", got)
	}
	direct, err := f.domain.Cycles(context.Background(), f.by, service.CyclesRequest{Project: "b06", IncludeEphemeral: true})
	var refused *model.Error
	if !errors.As(err, &refused) || refused.Data.Code != got.Error.Data.Code || !reflect.DeepEqual(direct, service.CyclesResult{}) {
		f.t.Fatalf("direct capability %+v %v", direct, err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	_, err = service.ReadCycles(context.Background(), r, service.CyclesRequest{Project: "b06", IncludeEphemeral: true}, nil)
	if closeErr := r.Close(); closeErr != nil {
		f.t.Fatal(closeErr)
	}
	if !errors.As(err, &refused) || refused.Data.Code != got.Error.Data.Code {
		f.t.Fatal("internal capability differs", err)
	}
	for _, req := range []service.CyclesRequest{{IncludeEphemeral: true}, {Project: "b06", IncludeEphemeral: true, Limit: 1001}} {
		_, err = f.domain.Cycles(context.Background(), f.by, req)
		if !errors.As(err, &refused) {
			f.t.Fatal("missing selector validation")
		}
		want := model.ErrorCode("invalid_params")
		if req.Project == "" {
			want = "project_required"
		}
		if refused.Data.Code != want {
			f.t.Fatalf("validation order got=%v want=%s", err, want)
		}
	}
	bad := f.call("bead.cycles", map[string]any{"includeEphemeral": true, "limit": 1001}, false)
	if bad.Error == nil || bad.Error.Data.Code != "invalid_params" {
		f.t.Fatal("RPC limit validation lost")
	}
	if !bytes.Equal(before, f.image(fmt.Sprintf("after-capability-%d", f.requests))) {
		f.t.Fatal("capability/validation changed full state")
	}
}

func (f *b06PlugFixture) seedParallel() {
	f.t.Helper()
	var changes []conformance.FixtureChange
	for _, id := range []string{"b06-parallel-b", "b06-parallel-a"} {
		doc := pipeline.Document{ID: id, Type: "issue", Project: "b06", Fields: map[string]any{"title": id, "status": "todo", "priority": 0}}
		for _, edge := range b06ParallelEdges() {
			if edge.From == id {
				doc.Links = append(doc.Links, pipeline.Link{To: model.RefByID(edge.To, model.Natural64(0)), Kind: edge.Kind})
			}
		}
		body, err := json.Marshal(doc)
		if err != nil {
			f.t.Fatal(err)
		}
		graph := &conformance.FixtureGraphSource{Parent: doc.Parent}
		for _, link := range doc.Links {
			graph.Links = append(graph.Links, driver.SourceLink{Kind: link.Kind, To: link.To})
		}
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body, Graph: graph})
	}
	b06Retain(f.t, "parallel-diagnostic-setup.json", map[string]any{"route": "conformance.Seed real driver records; not product admission", "raw_edges": b06ParallelEdges(), "changes": changes})
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
		f.t.Fatal(err)
	}
}

func (f *b06PlugFixture) requireEdges(label string, expected []b06Edge) {
	f.t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			f.t.Error(err)
		}
	}()
	page, err := r.Query(context.Background(), driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		f.t.Fatalf("complete fixture graph read failed: %v", err)
	}
	actual := []b06Edge{}
	for _, row := range page.Items {
		if row.Kind != "bead" {
			continue
		}
		var doc pipeline.Document
		if err := json.Unmarshal(row.Body, &doc); err != nil || doc.ID != row.Ref.ID {
			f.t.Fatal("raw fixture source envelope mismatch", err)
		}
		for _, edge := range doc.Links {
			actual = append(actual, b06Edge{doc.ID, edge.To.ID, edge.Kind})
		}
	}
	b06Retain(f.t, label+"-raw-edges.json", map[string]any{"raw_records": page.Items, "raw_edges": actual, "expected_edges": expected, "actual_edge_count": len(actual), "snapshot": r.Snapshot()})
	b06RequireEdges(f.t, actual, expected)
}

func (f *b06PlugFixture) duplicate(edge b06Edge) {
	f.t.Helper()
	type image struct {
		Archive  []byte                     `json:"archive"`
		Receipts map[string]b06ReceiptImage `json:"attempted_receipts"`
	}
	var before, after image
	if err := json.Unmarshal(f.image("before-ordinary-duplicate"), &before); err != nil {
		f.t.Fatal(err)
	}
	commit := b06RequireSuccess[model.Commit](f.t, f.link(edge))
	key := "bead.link/" + fmt.Sprint(f.requests)
	if err := json.Unmarshal(f.image("after-ordinary-duplicate"), &after); err != nil {
		f.t.Fatal(err)
	}
	if !bytes.Equal(before.Archive, after.Archive) || len(after.Receipts) != len(before.Receipts)+1 {
		f.t.Fatal("ordinary duplicate changed graph/history or unexpected receipts")
	}
	for key, value := range before.Receipts {
		if !reflect.DeepEqual(value, after.Receipts[key]) {
			f.t.Fatal("ordinary duplicate changed earlier receipt")
		}
	}
	receipt, ok := after.Receipts[key]
	if !ok || receipt.Changeset != nil || receipt.RequestHash == "" || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, commit) {
		f.t.Fatalf("duplicate did not save its exact final no-op receipt %+v", receipt)
	}
	b06Retain(f.t, "ordinary-duplicate-conservation.json", map[string]any{"before": before, "after": after, "new_receipt_key": key, "actual_response": commit, "graph_and_history": "exactly unchanged", "new_noop_receipts": 1})
}

func (f *b06PlugFixture) deniedMember() {
	f.t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("b06-authority", model.Natural64(0))}
	project := model.RefByID("b06-project", model.Natural64(0))
	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	app, err := authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Namespace: ns, DefaultProject: "b06", Facts: authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: project, Name: "b06", InstallationID: "fixed", Active: true}}}}})
	if err != nil {
		f.t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "user", ID: "user:b06-limited", Caveats: map[string]any{"refs": []model.Ref{model.RefByID("b06-parallel-a", model.Natural64(0))}}}
	key, err := keys.Issue(by, time.Now().Add(time.Hour))
	if err != nil {
		f.t.Fatalf("scoped User API-key issuance: %v", err)
	}
	before := f.image("before-policy-redaction")
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"bead.cycles","params":{"_meta":{"project":"b06"}}}`)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:33333"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key.Token)
	out := httptest.NewRecorder()
	app.Handler.ServeHTTP(out, req)
	b06Retain(f.t, "policy-redaction.json", map[string]any{"request": json.RawMessage(body), "response": json.RawMessage(out.Body.Bytes()), "http_status": out.Code, "principal": by, "condition": "existing row denied by actual installed fixed policy; not physically missing", "native_difference": "native missing-row diagnostics retain raw ID; Plug denies identity and retains a placeholder"})
	var envelope b06Envelope
	if err := json.Unmarshal(out.Body.Bytes(), &envelope); err != nil {
		f.t.Fatal(err)
	}
	got := b06RequireSuccess[service.CyclesResult](f.t, envelope)
	if out.Code != 200 || !reflect.DeepEqual(b06PlugPaths(got), [][]string{{"b06-parallel-a", ""}}) || got.CycleCount.String() != "1" || got.Truncated || len(got.Items) != 1 {
		f.t.Fatalf("policy-scoped count/path %+v HTTP%d", got, out.Code)
	}
	c := got.Items[0]
	if !c.Partial || c.MemberCount.String() != "2" || c.UnreadableCount.String() != "1" || !c.Path[1].Unreadable || c.Path[1].Ref != nil {
		f.t.Fatalf("redacted member disappeared %+v", c)
	}
	if !bytes.Equal(before, f.image("after-policy-redaction")) {
		f.t.Fatal("guarded read changed state")
	}
}

func b06RunPlug(t *testing.T, engine string) [][]string {
	f := b06OpenPlug(t, engine)
	expectedEdges := []b06Edge{}
	for _, c := range b06CycleCases() {
		for _, id := range c.IDs {
			f.create(id)
		}
		for _, edge := range c.Edges {
			b06RequireSuccess[model.Commit](t, f.link(edge))
		}
		expectedEdges = append(expectedEdges, c.Edges...)
		f.requireEdges(c.Name, expectedEdges)
		f.refuse(b06Edge{c.IDs[0], c.IDs[0], "tracks"})
		b06Retain(t, "corpus-"+c.Name+".json", c)
		if c.Name == "pure-tracks-loop" {
			b06AssertReport(t, f.report(false), [][]string{})
			b06AssertReport(t, f.report(true), [][]string{})
		}
	}
	// A repeated ordinary edge is an idempotency control, not parallel proof.
	edge := b06CycleCases()[0].Edges[0]
	f.duplicate(edge)
	f.refuse(b06Edge{edge.From, edge.To, "blocks"})
	f.create("b06-schedule-a")
	f.create("b06-schedule-b")
	b06RequireSuccess[model.Commit](t, f.link(b06Edge{"b06-schedule-a", "b06-schedule-b", "blocks"}))
	expectedEdges = append(expectedEdges, b06Edge{"b06-schedule-a", "b06-schedule-b", "blocks"})
	f.refuse(b06Edge{"b06-schedule-b", "b06-schedule-a", "blocks"})
	f.requireEdges("ordinary", expectedEdges)
	b06AssertReport(t, f.report(false), [][]string{})
	b06AssertReport(t, f.report(true), b06ExpectedPaths(false))
	f.capability()
	f.seedParallel()
	expectedEdges = append(expectedEdges, b06ParallelEdges()...)
	f.requireEdges("parallel", expectedEdges)
	b06AssertReport(t, f.report(false), [][]string{{"b06-parallel-a", "b06-parallel-b"}})
	b06AssertReport(t, f.report(true), b06ExpectedPaths(true))
	f.deniedMember()
	before := f.image("before-reopen")
	f.reopen()
	if !bytes.Equal(before, f.image("after-reopen")) {
		t.Fatal("physical reopen changed full state")
	}
	f.requireEdges("reopened", expectedEdges)
	b06AssertReport(t, f.report(false), [][]string{{"b06-parallel-a", "b06-parallel-b"}})
	got := f.report(true)
	b06AssertReport(t, got, b06ExpectedPaths(true))
	f.capability()
	f.deniedMember()
	t.Logf("B06_CONTROLS engine=%s pure_tracks=excluded mixed=reported self=refused scheduling=refused raw_order=exact parallel_rows=4 parallel_cycles=1 denied_member=placeholder capability=plugin_durable_state_unavailable positive_mixed_plane=OPEN physical_missing_member=OPEN", engine)
	return b06PlugPaths(got)
}

func TestB06DurableCycleControls(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b06RunPlug(t, engine) })
	}
}
