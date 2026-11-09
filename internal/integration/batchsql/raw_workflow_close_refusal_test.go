package batchsql_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

var rawWorkflowSource = model.Ref{ID: "ontology/raw-workflow-close"}
var rawWorkflowPrincipal = model.PrincipalRef{Kind: "user", ID: "user:raw-workflow-close-worker"}

const rawWorkflowCheckpoint = "plugin/workflow/default/durable"

// The observer delegates to the real Writer exactly once and returns its
// outcome/error unchanged. HTTP runs on a different goroutine; every raw call
// binds its wire response to this synchronized observation of that same call.
type rawWorkflowWriter struct {
	*pipeline.Writer
	mu     sync.Mutex
	calls  int
	key    string
	method string
	err    error
}

func (w *rawWorkflowWriter) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	out, err := w.Writer.ApplyWithOutcome(ctx, by, cs, meta)
	w.mu.Lock()
	w.calls++
	w.key, w.method, w.err = meta.IdempotencyKey, meta.Method, err
	w.mu.Unlock()
	return out, err
}

type rawWorkflowReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type rawWorkflowHTTP struct {
	f             *fixture
	api           *httptest.Server
	token         string
	writer        *rawWorkflowWriter
	workflowClock time.Time
	watch         []model.Ref
}

func newRawWorkflowHTTP(t *testing.T, f *fixture) *rawWorkflowHTTP {
	t.Helper()
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: rawWorkflowSource, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	h := &rawWorkflowHTTP{f: f, workflowClock: f.now()}
	h.open(t)
	return h
}

func (h *rawWorkflowHTTP) open(t *testing.T) {
	t.Helper()
	f := h.f
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, ok := f.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("actual checkpoint capability required")
	}
	provider, err := workflowplugin.New(pluginhost.NewDurableState(checkpoints, f.lease, "workflow", "default"))
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(func() time.Time { return h.workflowClock }).WithIdentity("workflow", "default", meta.Incarnation)
	h.writer = &rawWorkflowWriter{Writer: pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(rawWorkflowSource))))}
	domain := service.New(f.store, f.lease, h.writer, service.WithOntology(f.ontology), service.WithClock(f.now), service.WithWorkflow(provider))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "batch-local", AuthMode: "authenticated", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	keys := authnapikey.New(authnapikey.Config{Now: f.now})
	key, err := keys.Issue(rawWorkflowPrincipal, f.now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	h.token = key.Token
	admission, err := authn.New(authn.Config{Provider: keys})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	h.api = httptest.NewServer(handler)
	t.Cleanup(h.api.Close)
}

func (h *rawWorkflowHTTP) call(t *testing.T, method string, params map[string]any, meta model.WriteMeta) rawWorkflowReply {
	t.Helper()
	params["_meta"] = map[string]any{"project": meta.Project, "idempotencyKey": meta.IdempotencyKey}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "raw-workflow-close", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.f.ctx, http.MethodPost, h.api.URL+httpjson.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.token)
	response, err := h.api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("HTTP response: status=%d err=%v body=%s", response.StatusCode, err, raw)
	}
	var reply rawWorkflowReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("decode public response: %v; %s", err, raw)
	}
	return reply
}

func (h *rawWorkflowHTTP) apply(t *testing.T, cs model.ChangeSet, meta model.WriteMeta) rawWorkflowReply {
	t.Helper()
	h.writer.mu.Lock()
	before := h.writer.calls
	h.writer.mu.Unlock()
	reply := h.call(t, "tx.apply", map[string]any{"changeSet": cs}, meta)
	h.writer.mu.Lock()
	defer h.writer.mu.Unlock()
	if h.writer.calls != before+1 || h.writer.key != meta.IdempotencyKey || h.writer.method != "tx.apply" {
		t.Fatal("raw HTTP request did not delegate exactly once through Service to the real Writer")
	}
	var typed *model.Error
	if reply.Error == nil {
		if h.writer.err != nil {
			t.Fatalf("Writer error lost by public binding: %v", h.writer.err)
		}
	} else if !errors.As(h.writer.err, &typed) || typed.Data.Code != reply.Error.Data.Code {
		t.Fatalf("public error differs from this call's typed Writer error: reply=%+v error=%v", reply.Error, h.writer.err)
	}
	return reply
}

func rawWorkflowCommit(t *testing.T, reply rawWorkflowReply) model.Commit {
	t.Helper()
	if reply.Error != nil || len(reply.Result) == 0 || bytes.Equal(reply.Result, []byte("null")) {
		t.Fatalf("expected public commit: error=%+v result=%s", reply.Error, reply.Result)
	}
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || commit.Revision.IsZero() || len(commit.Results) == 0 {
		t.Fatalf("invalid commit: %+v err=%v", commit, err)
	}
	return commit
}

// This census deliberately does not reuse fixture.state(), which filters out
// protected/non-bead records and omits derived rows and operational metadata.
type rawWorkflowState struct {
	Meta       driver.Meta
	Rows       []driver.Record
	Versions   map[string][]driver.Version
	Log        []model.Changeset
	Derived    map[driver.DerivedTable][]driver.DerivedRow
	Observed   []driver.LockState
	Checkpoint driver.Checkpoint
	Receipt    *driver.IdempotencyRecord
}

func (h *rawWorkflowHTTP) state(t *testing.T, request model.WriteMeta) rawWorkflowState {
	t.Helper()
	f := h.f
	s := rawWorkflowState{Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	var err error
	if s.Meta, err = f.store.Meta(f.ctx); err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: s.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("complete all-kind row census: cursor=%v err=%v", page.NextCursor, err)
	}
	s.Rows = page.Items
	if s.Log, err = r.Changesets(f.ctx, nil, 1000); err != nil || len(s.Log) == 1000 {
		t.Fatalf("complete history/event census: err=%v", err)
	}
	refs := map[string]model.Ref{}
	keys := map[string]driver.LockKey{}
	add := func(key driver.LockKey) {
		t.Helper()
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keys[encoded] = key
	}
	for _, row := range s.Rows {
		refs[row.Ref.ID] = row.Ref
		if row.Kind == "bead" {
			for _, constructor := range []func(string) (driver.Sentinel, error){driver.NewBeadSentinel, driver.NewClosureSentinel} {
				sentinel, err := constructor(row.Ref.ID)
				if err != nil {
					t.Fatal(err)
				}
				add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
			}
		}
	}
	for _, entry := range s.Log {
		for _, change := range entry.Changes {
			refs[change.Ref.ID] = change.Ref
		}
	}
	for id, ref := range refs {
		page, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("complete versions for %s: cursor=%v err=%v", id, page.NextCursor, err)
		}
		s.Versions[id] = page.Items
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	}
	for _, ref := range h.watch {
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	}
	for _, key := range []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock} {
		add(key)
	}
	ready, err := driver.NewReadySentinel("batch-local", "issue")
	if err != nil {
		t.Fatal(err)
	}
	add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		s.Derived[table] = rows
		for _, row := range rows {
			add(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
		}
	}
	encoded := make([]string, 0, len(keys))
	for key := range keys {
		encoded = append(encoded, key)
	}
	sort.Strings(encoded)
	ordered := make([]driver.LockKey, 0, len(encoded))
	for _, key := range encoded {
		ordered = append(ordered, keys[key])
	}
	if s.Observed, err = r.Observe(f.ctx, ordered); err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(rawWorkflowPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	if s.Receipt, err = r.IdempotencyByKey(f.ctx, principal, "", request.Method, request.IdempotencyKey); err != nil {
		t.Fatal(err)
	}
	if s.Checkpoint, err = f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, rawWorkflowCheckpoint); err != nil {
		t.Fatal(err)
	}
	return s
}

func rawWorkflowSame(t *testing.T, before, after rawWorkflowState) {
	t.Helper()
	left, right := reflect.ValueOf(before), reflect.ValueOf(after)
	for i := 0; i < left.NumField(); i++ {
		if !reflect.DeepEqual(left.Field(i).Interface(), right.Field(i).Interface()) {
			t.Fatalf("raw workflow-close refusal/replay changed state component %s", left.Type().Field(i).Name)
		}
	}
}

func (h *rawWorkflowHTTP) refuse(t *testing.T, cs model.ChangeSet, meta model.WriteMeta, code model.ErrorCode) {
	t.Helper()
	before := h.state(t, meta)
	if before.Receipt != nil {
		t.Fatal("new refusal key already has an ordinary or no-op receipt")
	}
	reply := h.apply(t, cs, meta)
	rawWorkflowSame(t, before, h.state(t, meta))
	if reply.Error == nil || reply.Error.Data.Code != code || len(reply.Result) != 0 || reply.Meta.Replayed {
		t.Fatalf("expected %s without result/replay: error=%+v result=%s replayed=%t", code, reply.Error, reply.Result, reply.Meta.Replayed)
	}
	if code == model.CodeUnsupported && (reply.Error.Code != -32057 || reply.Error.Message != "operation is unsupported" || reply.Error.Data.Retryable) {
		t.Fatalf("workflow Close did not use canonical unsupported allocation: %+v", reply.Error)
	}
}

func rawWorkflowClose(ref model.Ref) model.Op {
	return model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: ref, Reason: "completed once"}}
}

func rawWorkflowRow(t *testing.T, state rawWorkflowState, id string) driver.Record {
	t.Helper()
	var found driver.Record
	count := 0
	for _, row := range state.Rows {
		if row.Ref.ID == id {
			found = row
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one current row for %s, found %d", id, count)
	}
	return found
}

func (h *rawWorkflowHTTP) requireGCBait(t *testing.T, state rawWorkflowState, active ...model.Ref) {
	t.Helper()
	var namespace struct {
		Values map[string]struct {
			Revision uint64 `json:"revision"`
			Value    struct {
				Runs     map[string]json.RawMessage `json:"runs"`
				Items    map[string]json.RawMessage `json:"items"`
				Receipts map[string]struct {
					RunID     string    `json:"runId"`
					Terminal  bool      `json:"terminal"`
					ExpiresAt time.Time `json:"expiresAt"`
				} `json:"receipts"`
			} `json:"value"`
		} `json:"values"`
	}
	if state.Checkpoint.Generation == 0 || json.Unmarshal(state.Checkpoint.Payload, &namespace) != nil {
		t.Fatal("workflow fixture must have a real, nonempty namespace checkpoint")
	}
	value := namespace.Values["workflow/state"]
	if value.Revision == 0 || len(value.Value.Runs) == 0 {
		t.Fatal("actual workflow state/revision or live run missing")
	}
	for _, ref := range active {
		if len(value.Value.Items[ref.ID]) == 0 {
			t.Fatalf("actual live operational item %s missing", ref.ID)
		}
	}
	for _, receipt := range value.Value.Receipts {
		if receipt.Terminal && !h.workflowClock.Before(receipt.ExpiresAt) && len(value.Value.Runs[receipt.RunID]) == 0 {
			return
		}
	}
	t.Fatal("fixture has no expired terminal receipt without an active run; accidental provider-load GC would be unobservable")
}

// These positive controls create ordinary priority-two issues at the fixed
// fixture clock. B01 republishes their exact rank image on every body mutation,
// including close; this expectation does not call the production classifier.
func rawWorkflowExpectedRank(t *testing.T, id string, revision model.Revision, at time.Time) driver.DerivedRow {
	t.Helper()
	value, err := json.Marshal(pipeline.ReadySortKey{Bucket: 0, Rank: 2, CreatedAt: at, RankedAt: at, Revision: revision})
	if err != nil {
		t.Fatal(err)
	}
	return driver.DerivedRow{Key: "ready/" + id, Value: value}
}

// Construct the complete fixed-fixture publication from its independently
// checked source, without invoking the production projection builder or using
// any observed derived postimage as an expected value.
func rawWorkflowExpectedPublication(t *testing.T, record driver.Record, incarnation, title, status string, at time.Time) map[driver.DerivedTable][]driver.DerivedRow {
	t.Helper()
	doc := decodeDocument(t, record)
	if record.Kind != "bead" || doc.ID != record.Ref.ID || doc.Type != "issue" || doc.Project != "batch-local" || doc.Parent != nil || len(doc.Labels) != 0 || len(doc.Links) != 0 || doc.Lease != nil || doc.CreatedAt == nil || !doc.CreatedAt.Equal(at) || doc.Fields["title"] != title || doc.Fields["status"] != status {
		t.Fatalf("ordinary publication fixture changed: %+v", doc)
	}
	priority, ok := doc.Fields["priority"].(json.Number)
	if !ok || priority.String() != "2" {
		t.Fatal("ordinary publication fixture lost priority two")
	}
	if blocked, present := doc.Fields["blocked"]; present && blocked != false {
		t.Fatal("ordinary publication fixture became blocked")
	}
	canonical, err := json.Marshal(doc)
	if err != nil || !bytes.Equal(canonical, record.Body) {
		t.Fatalf("Writer fixture is not canonical: %v", err)
	}
	ref := model.RefByID(record.Ref.ID, model.Natural64(0))
	if err := driver.ValidateHierarchyRef(record.Ref); err != nil || !driver.SelectionSameRef(record.Ref, ref) {
		t.Fatal("ordinary publication source reference changed its exact ID arm")
	}
	project := "batch-local"
	bead := driver.SelectionBead{Ref: ref, Project: &project, Type: "issue", Status: status, Title: title, Priority: 2}
	if template, present := doc.Fields["template"]; !present {
		bead.Absent = []driver.SelectionColumn{driver.SelectionTemplate}
	} else if template != false {
		t.Fatal("ordinary publication fixture became a template")
	}
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}
	annotate := func(table driver.DerivedTable, key string, value []byte, bead *driver.SelectionBead, facts *driver.SelectionFacts) driver.DerivedRow {
		index := &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: table, Key: key}, Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: source, Bead: bead, Facts: facts}
		if table == driver.DerivedAncestorClosure {
			// Ancestor owners borrow the source Record.Ref; the ancestor target
			// uses the explicit ID constructor. Preserve the engine's source
			// representation as well as every public reference field.
			owner := record.Ref
			index.Source.Key.Ref = &owner
		}
		if bead != nil {
			index.Queue = &driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: at.UTC()}
		}
		return (driver.DerivedRow{Key: key, Value: value, Index: index}).Clone()
	}
	marshal := func(value any) []byte {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	// Bind the literal semantic Ready item to the actual source revision,
	// independently of both projection and maintained-tree producers.
	bead.MembershipLeaf = driver.SelectionDigest(marshal(struct {
		ID       string         `json:"id"`
		Type     string         `json:"type"`
		Project  string         `json:"project"`
		Status   string         `json:"status"`
		Revision model.Revision `json:"revision"`
		Fields   map[string]any `json:"fields,omitempty"`
	}{doc.ID, doc.Type, doc.Project, status, record.Revision, doc.Fields}))
	labels := driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{}}
	links := driver.SelectionFacts{Kind: driver.SelectionLinkFact, Rows: []driver.SelectionFact{}}
	owner := record.Ref
	self := driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &owner, Target: &ref, Depth: 0}}}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(ref.ID))
	return map[driver.DerivedTable][]driver.DerivedRow{
		driver.DerivedSortKey:         {annotate(driver.DerivedSortKey, ref.ID, marshal(bead), &bead, nil), rawWorkflowExpectedRank(t, ref.ID, record.Revision, at)},
		driver.DerivedAttribute:       {annotate(driver.DerivedAttribute, ref.ID, marshal(labels), nil, &labels)},
		driver.DerivedReferenceIndex:  {annotate(driver.DerivedReferenceIndex, ref.ID, marshal(links), nil, &links)},
		driver.DerivedAncestorClosure: {annotate(driver.DerivedAncestorClosure, encoded+"."+encoded, []byte("0"), nil, &self)},
	}
}

// Replace only the independently named publication keys, retaining every
// unrelated row and table. Creation must find none; update must find each once.
func rawWorkflowExpectedDelta(t *testing.T, before, publication map[driver.DerivedTable][]driver.DerivedRow, update bool) map[driver.DerivedTable][]driver.DerivedRow {
	t.Helper()
	want := make(map[driver.DerivedTable][]driver.DerivedRow, len(before))
	for table, rows := range before {
		want[table] = rows
	}
	for table, additions := range publication {
		rows := append([]driver.DerivedRow(nil), before[table]...)
		for _, addition := range additions {
			matches := 0
			for i, previous := range rows {
				if previous.Key == addition.Key {
					rows[i] = addition
					matches++
				}
			}
			if update && matches != 1 || !update && matches != 0 {
				t.Fatalf("publication preimage %s/%s: matches=%d update=%t", table, addition.Key, matches, update)
			}
			if !update {
				rows = append(rows, addition)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
		want[table] = rows
	}
	return want
}

func TestRawWorkflowCloseRefusalPreservesBothPlanesAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			h := newRawWorkflowHTTP(t, f)
			created := rawWorkflowCommit(t, h.apply(t, model.ChangeSet{Ops: []model.Op{createOp("first", "durable close prefix", "batch-local"), createOp("other", "unchanged durable control", "batch-local")}}, f.meta("create")))
			first, other := created.Results[0].Ref, created.Results[1].Ref
			absent := model.Ref{ID: "workflow:item/" + strings.Repeat("0", 32)}
			operational := absent
			var secondOperational model.Ref
			if engine != "memory" {
				invoke := func(label string, count int) []workflowplugin.ItemView {
					t.Helper()
					items := make([]workflowplugin.ChecklistItem, count)
					for i := range items {
						items[i] = workflowplugin.ChecklistItem{Label: string(rune('a' + i)), Title: label, Type: "task"}
					}
					reply := h.call(t, "x.workflow.invoke", map[string]any{"template": label, "project": "batch-local", "mode": "ephemeral", "checklist": items}, f.meta(label))
					var result workflowplugin.InvokeResult
					if reply.Error != nil || json.Unmarshal(reply.Result, &result) != nil || len(result.Items) != count {
						t.Fatalf("real operational invocation failed: error=%+v result=%s", reply.Error, reply.Result)
					}
					return result.Items
				}
				terminal := invoke("expired-terminal-control", 1)[0]
				beforeClose := h.state(t, f.meta("checkpoint-control"))
				terminalReply := h.call(t, "bead.close", map[string]any{"id": model.Ref{ID: terminal.ID}}, f.meta("terminal-close"))
				var terminalCommit model.Commit
				if terminalReply.Error != nil || json.Unmarshal(terminalReply.Result, &terminalCommit) != nil || len(terminalCommit.Results) != 1 || terminalCommit.Results[0].Ref.ID != terminal.ID {
					t.Fatalf("supported named workflow close failed: error=%+v result=%s", terminalReply.Error, terminalReply.Result)
				}
				afterClose := h.state(t, f.meta("checkpoint-control"))
				if afterClose.Checkpoint.Generation <= beforeClose.Checkpoint.Generation || bytes.Equal(beforeClose.Checkpoint.Payload, afterClose.Checkpoint.Payload) {
					t.Fatal("named workflow close did not mutate the actual operational checkpoint")
				}
				beforeClose.Checkpoint, afterClose.Checkpoint = driver.Checkpoint{}, driver.Checkpoint{}
				rawWorkflowSame(t, beforeClose, afterClose)
				live := invoke("active-workflow-control", 2)
				operational, secondOperational = model.Ref{ID: live[0].ID}, model.Ref{ID: live[1].ID}
				// Only the provider's fixture clock advances. A mistaken provider
				// load now performs eligible terminal receipt GC; the real driver,
				// Writer, authentication and lease clocks retain their own time.
				h.workflowClock = h.workflowClock.Add(driver.IdempotencyWindow + time.Second)
			}
			h.watch = []model.Ref{first, other, operational, absent}
			mixed := model.ChangeSet{Ops: []model.Op{rawWorkflowClose(first), rawWorkflowClose(operational)}}
			refusedKey := f.meta("mixed-workflow-close")
			control := h.state(t, refusedKey)
			// Root creates publish self-rows and source-bound projections.
			// Close preserves logical self-rows and refreshes their source binding.
			if len(control.Derived[driver.DerivedAncestorClosure]) != 2 || len(control.Log) == 0 || len(control.Log[len(control.Log)-1].Events) != 2 {
				t.Fatal("root-create derived rows and audit event controls must be nonempty")
			}
			for _, row := range control.Derived[driver.DerivedAncestorClosure] {
				descendant, ancestor, depth, err := pipeline.DecodeAncestorRow(row)
				if err != nil || descendant != ancestor || depth != 0 || descendant != first.ID && descendant != other.ID {
					t.Fatalf("root-create derived control: row=%+v err=%v", row, err)
				}
			}
			initial := rawWorkflowExpectedDelta(t, nil, rawWorkflowExpectedPublication(t, rawWorkflowRow(t, control, first.ID), control.Meta.Incarnation, "durable close prefix", "todo", f.now()), false)
			initial = rawWorkflowExpectedDelta(t, initial, rawWorkflowExpectedPublication(t, rawWorkflowRow(t, control, other.ID), control.Meta.Incarnation, "unchanged durable control", "todo", f.now()), false)
			for table, rows := range initial {
				if !reflect.DeepEqual(control.Derived[table], rows) {
					t.Fatalf("root-create must publish both exact ranks and source/self projections: %s", table)
				}
			}
			if engine != "memory" {
				h.requireGCBait(t, h.state(t, refusedKey), operational, secondOperational)
			}
			if !t.Run("mixed-durable-first", func(t *testing.T) { h.refuse(t, mixed, refusedKey, model.CodeUnsupported) }) {
				return
			}
			if engine != "memory" {
				before := h.state(t, refusedKey)
				h.api.Close()
				f.reopen(t)
				h.open(t)
				rawWorkflowSame(t, before, h.state(t, refusedKey))
				if !t.Run("mixed-after-real-reopen", func(t *testing.T) { h.refuse(t, mixed, refusedKey, model.CodeUnsupported) }) {
					return
				}
			}
			type refusalCase struct {
				name string
				ops  []model.Op
				code model.ErrorCode
			}
			cases := []refusalCase{
				{"singleton", []model.Op{rawWorkflowClose(operational)}, model.CodeUnsupported},
				{"canonical-absent", []model.Op{rawWorkflowClose(absent)}, model.CodeUnsupported},
				{"workflow-first", []model.Op{rawWorkflowClose(operational), rawWorkflowClose(first)}, model.CodeUnsupported},
				{"all-workflow", []model.Op{rawWorkflowClose(operational), rawWorkflowClose(absent)}, model.CodeUnsupported},
				{"earlier-missing-durable", []model.Op{rawWorkflowClose(model.Ref{ID: "missing-durable"}), rawWorkflowClose(operational)}, "not_found"},
				{"earlier-unresolved-alias", []model.Op{rawWorkflowClose(model.RefByKey("later")), rawWorkflowClose(operational)}, "invalid_params"},
				{"earlier-forward-alias", []model.Op{rawWorkflowClose(model.RefByKey("later")), createOp("later", "unreachable create", "batch-local"), rawWorkflowClose(operational)}, "invalid_params"},
				{"earlier-empty-id", []model.Op{rawWorkflowClose(model.RefByID("", model.Natural64(0))), rawWorkflowClose(operational)}, "invalid_params"},
				{"earlier-nonzero-branch", []model.Op{rawWorkflowClose(model.Ref{ID: first.ID, Branch: model.Natural64(1)}), rawWorkflowClose(operational)}, "invalid_params"},
				{"earlier-protected", []model.Op{rawWorkflowClose(rawWorkflowSource), rawWorkflowClose(operational)}, "protected_record"},
				{"workflow-nonzero-branch", []model.Op{rawWorkflowClose(model.Ref{ID: operational.ID, Branch: model.Natural64(1)})}, "invalid_params"},
				{"resolved-alias-durable-prefix", []model.Op{createOp("new-durable", "staged alias prefix", "batch-local"), rawWorkflowClose(model.RefByKey("new-durable")), rawWorkflowClose(operational)}, model.CodeUnsupported},
			}
			for _, lookalike := range []string{"workflow:item/" + strings.Repeat("A", 32), "workflow:item/" + strings.Repeat("a", 30), "workflow:item/" + strings.Repeat("a", 31), "workflow:item/" + strings.Repeat("g", 32), "workflow:item/" + strings.Repeat("a", 34), "workflow:run/" + strings.Repeat("a", 32), "workflow:item/" + strings.Repeat("a", 32) + "/extra", "workflow:item/" + strings.Repeat("a", 32) + " "} {
				cases = append(cases, refusalCase{"noncanonical/" + lookalike, []model.Op{rawWorkflowClose(model.Ref{ID: lookalike}), rawWorkflowClose(operational)}, "not_found"})
			}
			if engine != "memory" {
				cases = append(cases, refusalCase{"all-present-workflow", []model.Op{rawWorkflowClose(operational), rawWorkflowClose(secondOperational)}, model.CodeUnsupported})
			}
			passed := true
			for _, tc := range cases {
				passed = t.Run(tc.name, func(t *testing.T) { h.refuse(t, model.ChangeSet{Ops: tc.ops}, f.meta(tc.name), tc.code) }) && passed
			}
			if !passed {
				return
			}
			if !t.Run("earlier-malformed-ref", func(t *testing.T) {
				meta := f.meta("malformed-ref")
				before := h.state(t, meta)
				h.writer.mu.Lock()
				calls := h.writer.calls
				h.writer.mu.Unlock()
				reply := h.call(t, "tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
					map[string]any{"kind": "close", "target": map[string]any{"id": first.ID, "key": "invalid-union"}},
					rawWorkflowClose(operational),
				}}}, meta)
				rawWorkflowSame(t, before, h.state(t, meta))
				h.writer.mu.Lock()
				defer h.writer.mu.Unlock()
				if reply.Error == nil || reply.Error.Data.Code != "invalid_params" || len(reply.Result) != 0 || reply.Meta.Replayed || h.writer.calls != calls {
					t.Fatalf("strict RPC Ref validation lost precedence: %+v", reply)
				}
			}) {
				return
			}

			// The identical first operation under the previously refused key
			// proves a viable durable prefix and no reservation/no-op receipt.
			before := h.state(t, refusedKey)
			valid := model.ChangeSet{Ops: mixed.Ops[:1]}
			reply := h.apply(t, valid, refusedKey)
			commit := rawWorkflowCommit(t, reply)
			after := h.state(t, refusedKey)
			oldHead, _ := before.Meta.Head.Uint64()
			newHead, _ := after.Meta.Head.Uint64()
			if reply.Meta.Replayed || newHead != oldHead+1 || commit.Revision.String() != after.Meta.Head.String() || len(commit.Results) != 1 || commit.Results[0].Ref.ID != first.ID || len(after.Log) != len(before.Log)+1 || after.Receipt == nil {
				t.Fatal("corrected durable prefix did not commit exactly once under the refused key")
			}
			closedRank := rawWorkflowExpectedRank(t, first.ID, commit.Revision, f.now())
			replacedRank := 0
			for _, row := range before.Derived[driver.DerivedSortKey] {
				if row.Key == closedRank.Key {
					if !reflect.DeepEqual(row, rawWorkflowExpectedRank(t, first.ID, created.Revision, f.now())) {
						t.Fatal("refusals changed the original durable rank")
					}
					replacedRank++
				}
			}
			wantClosedDerived := rawWorkflowExpectedDelta(t, before.Derived, rawWorkflowExpectedPublication(t, rawWorkflowRow(t, after, first.ID), after.Meta.Incarnation, "durable close prefix", "done", f.now()), true)
			if replacedRank != 1 || len(after.Versions[first.ID]) != len(before.Versions[first.ID])+1 || !reflect.DeepEqual(before.Versions[other.ID], after.Versions[other.ID]) || !reflect.DeepEqual(before.Checkpoint, after.Checkpoint) || len(after.Log[len(after.Log)-1].Events) == 0 || !reflect.DeepEqual(wantClosedDerived, after.Derived) {
				t.Fatal("positive close must change its version/event state and exact source projections while conserving every unrelated derived row, the control bead and workflow checkpoint")
			}
			row := rawWorkflowRow(t, after, first.ID)
			doc := decodeDocument(t, row)
			if doc.Fields["status"] != "done" || len(doc.Comments) != 1 || doc.Comments[0].Body != "completed once" || row.Revision.String() != commit.Revision.String() || commit.Results[0].Revision.String() != commit.Revision.String() {
				t.Fatalf("corrected close status/reason/revision: row=%+v doc=%+v commit=%+v", row, doc, commit)
			}
			if !reflect.DeepEqual(rawWorkflowRow(t, before, other.ID), rawWorkflowRow(t, after, other.ID)) {
				t.Fatal("corrected prefix changed current control bead body/revision")
			}
			replay := h.apply(t, valid, refusedKey)
			if !replay.Meta.Replayed || !reflect.DeepEqual(commit, rawWorkflowCommit(t, replay)) || !bytes.Equal(reply.Result, replay.Result) {
				t.Fatal("exact corrected request did not replay the original response")
			}
			rawWorkflowSame(t, after, h.state(t, refusedKey))
			// Replay/hash admission stays ahead of the new classifier.
			reused := h.apply(t, mixed, refusedKey)
			if reused.Error == nil || reused.Error.Data.Code != "idempotency_key_reused" || len(reused.Result) != 0 || reused.Meta.Replayed {
				t.Fatalf("changed successful-key body lost idempotency precedence: %+v", reused)
			}
			rawWorkflowSame(t, after, h.state(t, refusedKey))
			// A canonical-looking alias key still resolves to an ordinary new
			// bead. With no workflow-target op to mask an eager refusal, this
			// positive protects classification of the resolved ID arm only.
			aliasMeta := f.meta("canonical-looking-alias")
			aliasBefore := h.state(t, aliasMeta)
			aliasCommit := rawWorkflowCommit(t, h.apply(t, model.ChangeSet{Ops: []model.Op{
				createOp(operational.ID, "ordinary canonical-looking alias", "batch-local"),
				rawWorkflowClose(model.RefByKey(operational.ID)),
			}}, aliasMeta))
			aliasAfter := h.state(t, aliasMeta)
			if len(aliasCommit.Results) != 2 || aliasCommit.Results[0].Ref.ID == operational.ID || aliasCommit.Results[0].Ref.ID != aliasCommit.Results[1].Ref.ID || !reflect.DeepEqual(aliasBefore.Checkpoint, aliasAfter.Checkpoint) {
				t.Fatal("canonical-looking key was not resolved to the same ordinary durable bead without operational mutation")
			}
			aliasRow := rawWorkflowRow(t, aliasAfter, aliasCommit.Results[0].Ref.ID)
			aliasDoc := decodeDocument(t, aliasRow)
			if len(aliasAfter.Derived[driver.DerivedAncestorClosure]) != len(aliasBefore.Derived[driver.DerivedAncestorClosure])+1 {
				t.Fatal("resolved-alias create did not publish its actual derived self-row")
			}
			foundDerived := 0
			for _, row := range aliasAfter.Derived[driver.DerivedAncestorClosure] {
				descendant, ancestor, depth, err := pipeline.DecodeAncestorRow(row)
				if err != nil {
					t.Fatal(err)
				}
				if descendant == aliasRow.Ref.ID && ancestor == aliasRow.Ref.ID && depth == 0 {
					foundDerived++
					continue
				}
			}
			wantAliasDerived := rawWorkflowExpectedDelta(t, aliasBefore.Derived, rawWorkflowExpectedPublication(t, aliasRow, aliasAfter.Meta.Incarnation, "ordinary canonical-looking alias", "done", f.now()), false)
			if foundDerived != 1 || !reflect.DeepEqual(wantAliasDerived, aliasAfter.Derived) {
				t.Fatal("resolved-alias create must add exactly its self-row, source projections and fixed-clock priority-two rank and preserve every other derived row/table")
			}
			if aliasDoc.Fields["status"] != "done" || len(aliasDoc.Comments) != 1 || aliasDoc.Comments[0].Body != "completed once" || aliasRow.Revision.String() != aliasCommit.Revision.String() || len(aliasAfter.Versions[aliasRow.Ref.ID]) != 1 {
				t.Fatalf("resolved ordinary alias close did not commit its terminal image once: %+v", aliasDoc)
			}
			if !reflect.DeepEqual(rawWorkflowRow(t, after, first.ID), rawWorkflowRow(t, aliasAfter, first.ID)) || !reflect.DeepEqual(rawWorkflowRow(t, after, other.ID), rawWorkflowRow(t, aliasAfter, other.ID)) {
				t.Fatal("resolved-alias positive changed earlier durable controls")
			}
			after = h.state(t, refusedKey)
			if engine != "memory" {
				// A supported named read now consumes the same GC bait. This
				// positive control runs only after every raw refusal/replay oracle.
				h.requireGCBait(t, after, operational, secondOperational)
				named := h.call(t, "bead.get", map[string]any{"id": operational}, f.meta("supported-gc-control"))
				if named.Error != nil || len(named.Result) == 0 {
					t.Fatalf("supported operational read failed: %+v", named)
				}
				collected := h.state(t, refusedKey)
				if collected.Checkpoint.Generation <= after.Checkpoint.Generation || bytes.Equal(collected.Checkpoint.Payload, after.Checkpoint.Payload) {
					t.Fatal("supported provider load did not publish eligible GC: refusal checkpoint conservation was vacuous")
				}
				after.Checkpoint, collected.Checkpoint = driver.Checkpoint{}, driver.Checkpoint{}
				rawWorkflowSame(t, after, collected)
			}
			t.Logf("actual HTTP raw Writer refusal and corrected one-commit/replay passed on %s; workflow payload fixture=%t", engine, engine != "memory")
		})
	}
}
