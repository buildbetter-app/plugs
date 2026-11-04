package s1b05b08

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/claimrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These deliberately misleading keys distinguish the adopted A3 categories
// and slots from Beads literals. In particular, "closed" is unstarted, "open"
// is successful completion, and "pinned" is backlog, not a special close ban.
const (
	b08Unstarted = "closed"
	b08Started   = "working"
	b08Completed = "open"
	b08Failed    = "failure-lane"
	b08Canceled  = "abandoned"
	b08Backlog   = "pinned"
	b08Triage    = "intake"
)

func b08ContractOntology() ontology.Document {
	workflow := ontology.Workflow{
		Initial:  b08Unstarted,
		Statuses: []string{b08Unstarted, b08Started, b08Completed, b08Failed, b08Canceled, b08Backlog, b08Triage},
		Slots: map[ontology.Slot]string{
			ontology.Claim: b08Started, ontology.Close: b08Completed,
			ontology.Reopen: b08Unstarted, ontology.Fail: b08Failed,
			ontology.Cancel: b08Canceled, ontology.Defer: b08Backlog,
		},
		Transitions: []ontology.Rule{
			{From: []string{b08Unstarted}, To: b08Started, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
			{To: b08Completed, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
			{To: b08Failed, Guards: []ontology.Guard{{Kind: ontology.NoOpenChildren}, {Kind: ontology.Permission}}},
			{To: b08Canceled, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: b08Unstarted, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: b08Backlog, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		},
	}
	return ontology.Document{
		Statuses: map[string]ontology.Status{
			b08Unstarted: {Name: "Queue", Category: ontology.Unstarted},
			b08Started:   {Name: "Work", Category: ontology.Started},
			b08Completed: {Name: "Ship", Category: ontology.Completed, Outcome: ontology.Success},
			b08Failed:    {Name: "Failure", Category: ontology.Completed, Outcome: ontology.Failure},
			b08Canceled:  {Name: "Cancel", Category: ontology.Canceled},
			b08Backlog:   {Name: "Later", Category: ontology.Backlog},
			b08Triage:    {Name: "Intake", Category: ontology.Triage},
		},
		Workflows: map[string]ontology.Workflow{"issue": workflow, "gate": workflow},
		Types: map[string]ontology.Type{
			"issue": {Name: "Issue", Workflow: "issue", Fields: map[string]ontology.Field{
				"title": {Type: "string"}, "description": {Type: "string"},
				"is_blocked": {Type: "boolean"},
			}},
			"gate": {Name: "Gate", Workflow: "gate", Fields: map[string]ontology.Field{"title": {Type: "string"}}},
		},
		GateKinds: map[string]ontology.GateKind{"review": {Resolver: "builtin.manual", Params: map[string]any{"min_approvals": 1}}},
	}
}

func b08AddCloseGuards(doc *ontology.Document, guards ...ontology.Guard) {
	w := doc.Workflows["issue"]
	w.Transitions = append([]ontology.Rule(nil), w.Transitions...)
	for i := range w.Transitions {
		if w.Transitions[i].To == w.Slots[ontology.Close] {
			w.Transitions[i].Guards = append(w.Transitions[i].Guards, guards...)
		}
	}
	doc.Workflows["issue"] = w
}

type b08ReceiptScope struct {
	principal, agent, method, key string
}

func (s b08ReceiptScope) identity() string {
	return s.principal + "\x00" + s.agent + "\x00" + s.method + "\x00" + s.key
}

type b08Reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    model.ErrorData `json:"data"`
	} `json:"error"`
	Meta map[string]any `json:"_meta"`
}

type b08CloseFixture struct {
	*fixture
	snapshot        ontology.Snapshot
	scopes          map[string]b08ReceiptScope
	workflow        bool
	ontologyOptions []service.OntologyOption
	extraLocks      []driver.LockKey
}

func newB08CloseFixture(t *testing.T, engine string, doc ontology.Document) *b08CloseFixture {
	t.Helper()
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("B08 fixture ontology: %+v", problems)
	}
	f := &b08CloseFixture{fixture: newFixture(t, engine), snapshot: snapshot, scopes: map[string]b08ReceiptScope{}}
	// Take one current host sample so the real drivers' 24-hour receipt
	// windows remain live. Subsequent fixture advances are explicit; this
	// changes no driver, SQL, production clock, lease budget or timeout.
	f.now = time.Now().UTC().Truncate(time.Second)
	f.install(t)
	return f
}

// Only fixture composition is replaced. The real store, active lease, shared
// Writer, ontology validator and ordinary public RPC lowering remain in use.
func (f *b08CloseFixture) install(t *testing.T) {
	t.Helper()
	o := service.NewOntology(f.snapshot.Effective, f.ontologyOptions...)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(o),
		pipeline.WithClock(func() time.Time { return f.now }),
		pipeline.WithIDGenerator(func() (string, error) {
			if f.index >= len(f.ids) {
				return "", fmt.Errorf("declared B08 IDs exhausted")
			}
			id := f.ids[f.index]
			f.index++
			return id, nil
		}),
	)
	options := []service.Option{service.WithOntology(o), service.WithClock(func() time.Time { return f.now })}
	if f.workflow {
		checkpoints, ok := f.store.(driver.CheckpointStore)
		if !ok {
			t.Fatal("B08 workflow fixture requires the real checkpoint capability")
		}
		provider, err := workflowplugin.New(pluginhost.NewDurableState(checkpoints, f.lease, "workflow", "default"))
		if err != nil {
			t.Fatal(err)
		}
		meta, err := f.store.Meta(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		provider.WithIdentity("workflow", "default", meta.Incarnation).WithClock(func() time.Time { return f.now })
		options = append(options, service.WithWorkflow(provider))
	}
	f.service = service.New(f.store, f.lease, f.writer, options...)
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.service); err != nil {
		t.Fatal(err)
	}
	if err := claimrpc.Bind(registry, f.service); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: bytes.Repeat([]byte{0x08}, 32)})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *b08CloseFixture) reopenB08(t *testing.T) {
	t.Helper()
	f.fixture.reopen(t)
	f.install(t)
}

func (f *b08CloseFixture) scope(method, label, agent string) b08ReceiptScope {
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		panic(err)
	}
	scope := b08ReceiptScope{principal, agent, method, key("b08-contract/" + label)}
	f.scopes[scope.identity()] = scope
	return scope
}

func (f *b08CloseFixture) callB08(t *testing.T, method, label string, params map[string]any) b08Reply {
	t.Helper()
	return f.callB08As(t, method, label, actor, params)
}

func (f *b08CloseFixture) callB08As(t *testing.T, method, label, agent string, params map[string]any) b08Reply {
	t.Helper()
	scope := f.scope(method, label, agent)
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	copyParams["_meta"] = map[string]any{"project": project, "agent": agent, "idempotencyKey": scope.key}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 8, "method": method, "params": copyParams})
	if err != nil {
		t.Fatal(err)
	}
	response := f.server.Dispatch(f.ctx, raw)
	var reply b08Reply
	decoder := json.NewDecoder(bytes.NewReader(response))
	decoder.UseNumber()
	if err := decoder.Decode(&reply); err != nil {
		t.Fatalf("%s response=%s: %v", method, response, err)
	}
	return reply
}

func (f *b08CloseFixture) callB08Authenticated(t *testing.T, handler http.Handler, keys *authnapikey.Provider, by model.PrincipalRef, method, label string, params map[string]any) b08Reply {
	t.Helper()
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	scope := b08ReceiptScope{principal, "", method, key("b08-contract/" + label)}
	f.scopes[scope.identity()] = scope
	issued, err := keys.Issue(by, f.now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	copyParams["_meta"] = map[string]any{"project": project, "agent": actor, "idempotencyKey": scope.key}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 8, "method": method, "params": copyParams})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(raw))
	request.RemoteAddr = "127.0.0.1:12345"
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+issued.Token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticated B08 %s HTTP status=%d", method, response.Code)
	}
	var reply b08Reply
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func b08Commit(t *testing.T, reply b08Reply) model.Commit {
	t.Helper()
	var commit model.Commit
	if reply.Error != nil || len(reply.Result) == 0 || json.Unmarshal(reply.Result, &commit) != nil {
		t.Fatalf("public B08 write refused: error=%+v result=%s", reply.Error, reply.Result)
	}
	return commit
}

func (f *b08CloseFixture) createB08(t *testing.T, title, typ string, fields map[string]any, parent *model.Ref) model.Ref {
	t.Helper()
	copyFields := map[string]any{"title": title}
	for k, v := range fields {
		copyFields[k] = v
	}
	params := map[string]any{"type": typ, "fields": copyFields}
	if parent != nil {
		params["parent"] = *parent
	}
	commit := b08Commit(t, f.callB08(t, "bead.create", "create-"+title, params))
	if len(commit.Results) != 1 {
		t.Fatalf("create results=%+v", commit.Results)
	}
	return commit.Results[0].Ref
}

// The oracle compares complete raw records, versions/patch bytes, changesets
// (including events), all exposed derived tables, relevant lock observations,
// lease/workflow checkpoints and every receipt scope used by this fixture.
// It neither copies private stores nor infers proof from counts alone. No
// driver API enumerates arbitrary receipts/checkpoints, so their boundary is
// the complete set of keys the exercised public operations can address.
type b08Conservation struct {
	Meta        driver.Meta
	Rows        []driver.Record
	Versions    map[string][]driver.Version
	Changes     []model.Changeset
	Derived     map[driver.DerivedTable][]driver.DerivedRow
	Locks       []driver.LockState
	Checkpoints map[string]driver.Checkpoint
	Receipts    map[string]driver.IdempotencyRecord
}

func (f *b08CloseFixture) conservation(t *testing.T) b08Conservation {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	state := b08Conservation{Meta: meta, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Checkpoints: map[string]driver.Checkpoint{}, Receipts: map[string]driver.IdempotencyRecord{}}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{}, Limit: 128, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		state.Rows = append(state.Rows, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
			t.Fatal("record oracle did not advance")
		}
		cursor = page.NextCursor
	}
	sort.Slice(state.Rows, func(i, j int) bool { return state.Rows[i].Ref.ID < state.Rows[j].Ref.ID })
	keys := map[string]driver.LockKey{}
	addKey := func(key driver.LockKey) {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keys[encoded] = key
	}
	for _, sentinel := range []driver.Sentinel{driver.SentinelGraphBlocking, driver.SentinelOntology, driver.SentinelPlugins} {
		addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	for _, key := range f.extraLocks {
		addKey(key)
	}
	checkpointKeys := map[string]bool{"plugin/workflow/default/durable": true}
	for _, row := range state.Rows {
		ref := row.Ref
		addKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if row.Kind == "bead" {
			// Protected ontology/project records retain their record locks and
			// complete versions below; only bead IDs have bead/closure keys.
			for _, makeSentinel := range []func(string) (driver.Sentinel, error){driver.NewBeadSentinel, driver.NewClosureSentinel} {
				sentinel, err := makeSentinel(ref.ID)
				if err != nil {
					t.Fatal(err)
				}
				addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
			}
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			sentinel, err := driver.NewReadySentinel(doc.Project, doc.Type)
			if err != nil {
				t.Fatal(err)
			}
			addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
			checkpointKeys[heartbeatCheckpointKey(ref.ID)] = true
		}
		cursor = nil
		for {
			page, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 128, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			state.Versions[ref.ID] = append(state.Versions[ref.ID], page.Items...)
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
				t.Fatal("version oracle did not advance")
			}
			cursor = page.NextCursor
		}
	}
	cursor = nil
	for {
		changes, err := r.Changesets(f.ctx, cursor, 128)
		if err != nil {
			t.Fatal(err)
		}
		state.Changes = append(state.Changes, changes...)
		if len(changes) < 128 {
			break
		}
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: changes[len(changes)-1].Revision, Index: model.Natural64(0)}
	}
	for _, table := range []driver.DerivedTable{
		driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure,
		driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins,
	} {
		rows, err := r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		state.Derived[table] = rows
		for _, row := range rows {
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			addKey(driver.LockKey{Kind: driver.LockDerived, Derived: &key})
		}
	}
	encodedKeys := make([]string, 0, len(keys))
	for encoded := range keys {
		encodedKeys = append(encodedKeys, encoded)
	}
	sort.Strings(encodedKeys)
	ordered := make([]driver.LockKey, 0, len(keys))
	for _, encoded := range encodedKeys {
		ordered = append(ordered, keys[encoded])
	}
	state.Locks, err = r.Observe(f.ctx, ordered)
	if err != nil || len(state.Locks) != len(ordered) {
		t.Fatalf("conservation observations=%d want=%d: %v", len(state.Locks), len(ordered), err)
	}
	for _, scope := range f.scopes {
		receipt, err := r.IdempotencyByKey(f.ctx, scope.principal, scope.agent, scope.method, scope.key)
		if err != nil {
			t.Fatal(err)
		}
		if receipt != nil {
			state.Receipts[scope.identity()] = *receipt
		}
		checkpointKeys[heartbeatReceiptCheckpointKey(scope.principal, scope.agent, scope.method, scope.key)] = true
	}
	checkpoints, ok := f.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("B08 conservation requires the real checkpoint capability")
	}
	for key := range checkpointKeys {
		checkpoint, err := checkpoints.LoadCheckpoint(f.ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if checkpoint.Generation != 0 || len(checkpoint.Payload) != 0 {
			state.Checkpoints[key] = checkpoint
		}
	}
	return state
}

func b08SameConservation(t *testing.T, before, after b08Conservation) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("B08 changed durable state: meta=%t records=%t versions=%t changes/events=%t derived=%t locks=%t checkpoints=%t receipts=%t",
			reflect.DeepEqual(before.Meta, after.Meta), reflect.DeepEqual(before.Rows, after.Rows),
			reflect.DeepEqual(before.Versions, after.Versions), reflect.DeepEqual(before.Changes, after.Changes),
			reflect.DeepEqual(before.Derived, after.Derived), reflect.DeepEqual(before.Locks, after.Locks),
			reflect.DeepEqual(before.Checkpoints, after.Checkpoints), reflect.DeepEqual(before.Receipts, after.Receipts))
	}
}

func (f *b08CloseFixture) refuseB08(t *testing.T, method, label string, params map[string]any, code model.ErrorCode, guard ontology.GuardKind) {
	t.Helper()
	f.refuseB08As(t, method, label, actor, params, code, guard)
}

func (f *b08CloseFixture) refuseB08As(t *testing.T, method, label, agent string, params map[string]any, code model.ErrorCode, guard ontology.GuardKind) {
	t.Helper()
	scope := f.scope(method, label, agent)
	before := f.conservation(t)
	for attempt := 0; attempt < 2; attempt++ {
		reply := f.callB08As(t, method, label, agent, params)
		b08RequireRefusal(t, method, reply, code, guard)
		after := f.conservation(t)
		if _, exists := after.Receipts[scope.identity()]; exists {
			t.Fatal("refusal stored an ordinary/no-op receipt")
		}
		b08SameConservation(t, before, after)
		if attempt == 0 {
			f.reopenB08(t)
			b08SameConservation(t, before, f.conservation(t))
		}
	}
}

func b08RequireRefusal(t *testing.T, method string, reply b08Reply, code model.ErrorCode, guard ontology.GuardKind) {
	t.Helper()
	if reply.Error == nil || reply.Error.Data.Code != code || len(reply.Result) != 0 || reply.Meta["replayed"] == true {
		t.Fatalf("%s refusal=%+v result=%s want=%s", method, reply.Error, reply.Result, code)
	}
	if guard == "" {
		return
	}
	data, err := json.Marshal(reply.Error.Data.Details["violations"])
	var violations []ontology.Violation
	if err != nil || json.Unmarshal(data, &violations) != nil || len(violations) == 0 {
		t.Fatalf("refusal omitted typed guard violations: %+v", reply.Error)
	}
	found := false
	for _, violation := range violations {
		found = found || violation.Guard == guard && violation.Code == code
	}
	if !found {
		t.Fatalf("refusal violations=%+v want guard=%s", violations, guard)
	}
}

func b08CloseOp(ref model.Ref, reason string) model.Op {
	return model.Op{Kind: model.OpClose, Close: &model.CloseOp{Target: ref, Reason: reason}}
}

func b08Batch(ops ...model.Op) map[string]any {
	return map[string]any{"changeSet": model.ChangeSet{Ops: ops}}
}
