package s1b13b16

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	project = "s1-slice4"
	stamp   = "2026-11-01T09:00:00Z"
)

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type fixture struct {
	t         *testing.T
	ctx       context.Context
	engine    string
	path      string
	now       time.Time
	ids       []string
	nextID    int
	request   int
	store     driver.Store
	lease     driver.ActiveLease
	domain    *service.Service
	server    *rpc.Server
	transport *inprocess.Transport
	ontology  *service.Ontology
}

func newFixture(t *testing.T, engine, name string, ids []string, o *service.Ontology) *fixture {
	t.Helper()
	ctx := context.Background()
	f := &fixture{t: t, ctx: ctx, engine: engine, path: filepath.Join(t.TempDir(), name+".db"), now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC), ids: append([]string(nil), ids...), ontology: o}
	f.open(t)
	t.Cleanup(func() { f.close() })
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{
			Clock: func() time.Time { return f.now }, StoreID: "s1-slice4-memory", Incarnation: "s1-slice4-memory-incarnation",
		})
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	default:
		t.Fatalf("unknown fixture engine %q", f.engine)
	}
	if err != nil {
		t.Fatalf("open %s: %v", f.engine, err)
	}
	f.lease, err = f.store.Lease(f.ctx, "s1-slice4-"+f.engine, driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatalf("lease %s: %v", f.engine, err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.close()
		t.Fatal(err)
	}
	writer := pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology),
		pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)),
		pipeline.WithClock(func() time.Time { return f.now }),
		pipeline.WithIDGenerator(f.nextIDValue),
	)
	f.domain = service.New(f.store, f.lease, writer,
		service.WithOntology(f.ontology),
		service.WithClock(func() time.Time { return f.now }),
	)
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.close()
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		f.close()
		t.Fatalf("bind service: %v", err)
	}
	f.server, err = rpc.New(registry, rpc.Config{
		DefaultProject: project,
		Incarnation:    meta.Incarnation,
		CursorKey:      []byte("s1-slice4-fixed-cursor-key-0000001"),
	})
	if err != nil {
		f.close()
		t.Fatal(err)
	}
	f.transport = inprocess.New(f.server)
}

func (f *fixture) close() {
	if f.lease != nil {
		_ = f.lease.Release(context.Background())
		f.lease = nil
	}
	if f.store != nil {
		_ = f.store.Close()
		f.store = nil
	}
	f.server, f.domain, f.transport = nil, nil, nil
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	if f.engine != "sqlite" {
		t.Fatalf("reopen requested for %s fixture", f.engine)
	}
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.close()
	f.open(t)
	after, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.StoreID != after.StoreID || before.Incarnation != after.Incarnation || before.Head != after.Head {
		t.Fatalf("SQLite reopen changed identity/head: before=%+v after=%+v", before, after)
	}
}

func (f *fixture) nextIDValue() (string, error) {
	if f.nextID >= len(f.ids) {
		return "", fmt.Errorf("declared deterministic ID sequence exhausted")
	}
	id := f.ids[f.nextID]
	f.nextID++
	return id, nil
}

func (f *fixture) meta(label, method string) model.WriteMeta {
	key := sha256.Sum256([]byte("s1-slice4/" + f.engine + "/" + label))
	return model.WriteMeta{Method: method, Project: project, Agent: "s1-slice4-agent", AgentLabel: "s1-slice4-agent", IdempotencyKey: hex.EncodeToString(key[:])}
}

func (f *fixture) call(t *testing.T, ctx context.Context, label, method string, params map[string]any, write bool) map[string]any {
	t.Helper()
	copyParams := make(map[string]any, len(params)+1)
	for key, value := range params {
		copyParams[key] = value
	}
	if write {
		key := sha256.Sum256([]byte("s1-slice4/rpc/" + f.engine + "/" + label))
		copyParams["_meta"] = map[string]any{"project": project, "agent": "s1-slice4-agent", "idempotencyKey": hex.EncodeToString(key[:])}
	}
	f.request++
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.request, "method": method, "params": copyParams})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.transport.RoundTrip(ctx, request)
	if err != nil {
		t.Fatalf("%s round trip: %v", method, err)
	}
	decoded, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatalf("%s response is invalid JSON: %v: %s", method, err, raw)
	}
	reply, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("%s response is not an object: %v", method, decoded)
	}
	return reply
}

func result(t *testing.T, reply map[string]any) map[string]any {
	t.Helper()
	if value, ok := reply["error"]; ok {
		t.Fatalf("unexpected RPC refusal: %v", value)
	}
	value, ok := reply["result"].(map[string]any)
	if !ok {
		t.Fatalf("RPC result is not an object: %v", reply)
	}
	return value
}

func code(reply map[string]any) string {
	data, _ := reply["error"].(map[string]any)
	values, _ := data["data"].(map[string]any)
	value, _ := values["code"].(string)
	return value
}

func refused(t *testing.T, reply map[string]any, want string) {
	t.Helper()
	if got := code(reply); got != want {
		t.Fatalf("refusal code=%q want=%q response=%v", got, want, reply)
	}
	if _, ok := reply["result"]; ok {
		t.Fatalf("typed refusal carried a result: %v", reply)
	}
}

func createdID(t *testing.T, commit map[string]any) string {
	t.Helper()
	rows, ok := commit["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("commit did not contain one result: %v", commit)
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("commit result is not an object: %v", rows[0])
	}
	ref, ok := row["ref"].(map[string]any)
	if !ok {
		t.Fatalf("commit result did not contain ref: %v", row)
	}
	id, ok := ref["id"].(string)
	if !ok || id == "" {
		t.Fatalf("commit ref did not contain id: %v", ref)
	}
	return id
}

func ref(id string) map[string]any { return map[string]any{"id": id} }

func record(t *testing.T, f *fixture, id string) driver.Record {
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
	rows, err := r.Get(f.ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("record %s count=%d", id, len(rows))
	}
	return rows[0]
}

func document(t *testing.T, f *fixture, id string) pipeline.Document {
	t.Helper()
	row := record(t, f, id)
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func changesets(t *testing.T, f *fixture) []model.Changeset {
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
	entries, err := r.Changesets(f.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func head(t *testing.T, f *fixture) model.Revision {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return meta.Head
}

func memoryItems(t *testing.T, value map[string]any) []map[string]any {
	t.Helper()
	items, ok := value["items"].([]any)
	if !ok {
		t.Fatalf("memory page has no items: %v", value)
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("memory item is not an object: %v", item)
		}
		result = append(result, m)
	}
	return result
}

func memoryKeys(t *testing.T, items []map[string]any) []string {
	t.Helper()
	keys := make([]string, 0, len(items))
	for _, item := range items {
		key, ok := item["key"].(string)
		if !ok {
			t.Fatalf("memory item key missing: %v", item)
		}
		keys = append(keys, key)
	}
	return keys
}

func canonical(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}

func sha(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type memoryProjection struct {
	Engine       string   `json:"engine"`
	DerivedKey   string   `json:"derivedKey"`
	ExplicitKey  string   `json:"explicitKey"`
	SearchKeys   []string `json:"searchKeys"`
	VisibleKeys  []string `json:"visibleKeys"`
	AfterDelete  []string `json:"afterDeleteKeys"`
	ReopenedKeys []string `json:"reopenedKeys"`
	Replacement  bool     `json:"replacementKeepsID"`
	Refusals     []string `json:"refusals"`
}

func TestB13MemoryPlaneAndDerivedKeyTwice(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			first := runB13(t, engine)
			second := runB13(t, engine)
			if !bytes.Equal(first, second) {
				t.Fatalf("independent Plug projections differ:\nfirst=%s\nsecond=%s", first, second)
			}
			t.Logf("B13 %s independent projection sha256=%s", engine, sha(first))
		})
	}
}

func runB13(t *testing.T, engine string) []byte {
	t.Helper()
	f := newFixture(t, engine, "b13", []string{"b13-bead", "b13-alpha", "b13-space", "b13-derived"}, service.NewOntology(nil))
	create := result(t, f.call(t, context.Background(), "create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "same plane name"}}, true))
	if createdID(t, create) != "b13-bead" {
		t.Fatal("ordinary bead did not use the declared ID")
	}
	alpha := result(t, f.call(t, context.Background(), "alpha", "memory.save", map[string]any{"key": "alpha", "content": "Alpha body\n東京", "metadata": map[string]any{"kind": "fixture"}}, true))
	alphaID := createdID(t, alpha)
	spaced := result(t, f.call(t, context.Background(), "spaced", "memory.save", map[string]any{"key": "  Explicit Key/Ω  ", "content": "spaced key"}, true))
	spacedID := createdID(t, spaced)
	if alphaID != "b13-alpha" || spacedID != "b13-space" {
		t.Fatalf("memory IDs did not use declared sequence: alpha=%s spaced=%s", alphaID, spacedID)
	}

	derivedContent := "always run tests with -race flag and preserve exact body"
	derived := result(t, f.call(t, context.Background(), "derived", "memory.save", map[string]any{"content": derivedContent}, true))
	gotDerived := result(t, f.call(t, context.Background(), "derived-get", "memory.get", map[string]any{"id": createdID(t, derived)}, false))
	if gotDerived["key"] != "always-run-tests-with-race-flag-and-preserve" || gotDerived["content"] != derivedContent {
		t.Fatalf("derived memory changed key/content: %+v", gotDerived)
	}

	page := memoryItems(t, result(t, f.call(t, context.Background(), "list", "memory.list", map[string]any{"limit": 20}, false)))
	if got := memoryKeys(t, page); !reflect.DeepEqual(got, []string{"  Explicit Key/Ω  ", "alpha", "always-run-tests-with-race-flag-and-preserve"}) {
		t.Fatalf("memory plane leaked/reordered rows: %v", got)
	}
	search := memoryItems(t, result(t, f.call(t, context.Background(), "search", "memory.list", map[string]any{"query": "ALPHA", "limit": 20}, false)))
	if got := memoryKeys(t, search); !reflect.DeepEqual(got, []string{"alpha"}) {
		t.Fatalf("case-insensitive memory search=%v", got)
	}

	replaced := result(t, f.call(t, context.Background(), "replace", "memory.save", map[string]any{"key": "alpha", "content": "replacement body"}, true))
	if id := createdID(t, replaced); id != alphaID {
		t.Fatalf("replacement minted a new memory ID: got=%s want=%s", id, alphaID)
	}
	deleted := result(t, f.call(t, context.Background(), "delete", "memory.delete", map[string]any{"id": spacedID}, true))
	if len(deleted["results"].([]any)) != 1 {
		t.Fatalf("delete did not return one result: %v", deleted)
	}
	afterDelete := memoryItems(t, result(t, f.call(t, context.Background(), "after-delete", "memory.list", map[string]any{"limit": 20}, false)))
	if got := memoryKeys(t, afterDelete); !reflect.DeepEqual(got, []string{"alpha", "always-run-tests-with-race-flag-and-preserve"}) {
		t.Fatalf("deleted memory remained visible: %v", got)
	}

	refusals := []string{}
	if _, err := f.domain.MemorySave(f.ctx, anonymous, service.MemorySaveRequest{Key: "blank", Content: "   "}, f.meta("blank", "memory.save")); err != nil {
		refusals = append(refusals, errorCode(err))
	} else {
		t.Fatal("blank memory content was accepted")
	}
	if _, err := f.domain.MemorySave(f.ctx, anonymous, service.MemorySaveRequest{Content: "!!!"}, f.meta("punctuation", "memory.save")); err != nil {
		refusals = append(refusals, errorCode(err))
	} else {
		t.Fatal("punctuation-only memory content was accepted")
	}
	before := head(t, f)
	wireMissingMeta := f.call(t, context.Background(), "wire-missing-idempotency", "memory.save", map[string]any{"content": "wire omitted key"}, false)
	if got := code(wireMissingMeta); got != "invalid_params" {
		t.Fatalf("wire required-idempotency boundary code=%q response=%v", got, wireMissingMeta)
	}
	if head(t, f) != before {
		t.Fatal("wire descriptor refusal advanced memory head")
	}
	refusals = append(refusals, "wire:"+code(wireMissingMeta))

	if engine == "sqlite" {
		f.reopen(t)
	}
	reopened := memoryItems(t, result(t, f.call(t, context.Background(), "reopened", "memory.list", map[string]any{"limit": 20}, false)))
	if got := memoryKeys(t, reopened); !reflect.DeepEqual(got, []string{"alpha", "always-run-tests-with-race-flag-and-preserve"}) {
		t.Fatalf("reopened memory rows=%v", got)
	}
	return canonical(memoryProjection{Engine: engine, DerivedKey: gotDerived["key"].(string), ExplicitKey: "  Explicit Key/Ω  ", SearchKeys: memoryKeys(t, search), VisibleKeys: []string{"  Explicit Key/Ω  ", "alpha", "always-run-tests-with-race-flag-and-preserve"}, AfterDelete: memoryKeys(t, afterDelete), ReopenedKeys: memoryKeys(t, reopened), Replacement: createdID(t, replaced) == alphaID, Refusals: refusals})
}

func errorCode(err error) string {
	var typed *model.Error
	if errors.As(err, &typed) {
		return string(typed.Data.Code)
	}
	return fmt.Sprintf("%T", err)
}

type commentProjection struct {
	Engine       string     `json:"engine"`
	Bodies       []string   `json:"bodies"`
	Authors      []string   `json:"authors"`
	Times        []string   `json:"times"`
	ChangedPaths [][]string `json:"changedPaths"`
	Events       []string   `json:"events"`
	Refusals     []string   `json:"refusals"`
}

func TestB15CommentsOrderHistoryAndRefusalsTwice(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			first := runB15(t, engine)
			second := runB15(t, engine)
			if !bytes.Equal(first, second) {
				t.Fatalf("independent Plug comment projections differ:\nfirst=%s\nsecond=%s", first, second)
			}
			t.Logf("B15 %s independent projection sha256=%s", engine, sha(first))
		})
	}
}

func runB15(t *testing.T, engine string) []byte {
	t.Helper()
	f := newFixture(t, engine, "b15", []string{"b15-anchor"}, service.NewOntology(nil))
	f.call(t, context.Background(), "create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "anchor"}}, true)
	before := document(t, f, "b15-anchor")
	body1 := "  **literal**\n\nUnicode: café 東京\n"
	f.call(t, context.Background(), "comment-one", "bead.comment", map[string]any{"id": ref("b15-anchor"), "body": body1}, true)
	f.call(t, context.Background(), "comment-two", "bead.comment", map[string]any{"id": ref("b15-anchor"), "body": "second comment"}, true)
	after := document(t, f, "b15-anchor")
	if len(after.Comments) != 2 || after.Comments[0].Body != body1 || after.Comments[1].Body != "second comment" {
		t.Fatalf("comment body/order changed: %+v", after.Comments)
	}
	if !reflect.DeepEqual(before.Fields, after.Fields) {
		t.Fatalf("comment mutated anchor fields: before=%v after=%v", before.Fields, after.Fields)
	}
	if !after.Comments[0].Time.Equal(f.now) || !after.Comments[1].Time.Equal(f.now.Add(time.Second)) {
		t.Fatalf("controlled comment stamps changed: %+v", after.Comments)
	}
	entries := changesets(t, f)
	if len(entries) != 3 {
		t.Fatalf("want create plus two comments, got %d", len(entries))
	}
	changedPaths := make([][]string, 0, 2)
	events := make([]string, 0, 2)
	for _, entry := range entries[1:] {
		if len(entry.Changes) != 1 || entry.Changes[0].Op != model.OpComment {
			t.Fatalf("comment changeset shape=%+v", entry)
		}
		changedPaths = append(changedPaths, append([]string(nil), entry.Changes[0].ChangedPaths...))
		if len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadCommented {
			t.Fatalf("comment event shape=%+v", entry.Events)
		}
		events = append(events, string(entry.Events[0].Type))
	}
	refusals := []string{}
	beforeHead := head(t, f)
	for _, tc := range []struct{ label, id, body string }{{"empty", "b15-anchor", ""}, {"whitespace", "b15-anchor", " \t\n"}, {"unknown", "b15-absent", "nonblank"}} {
		reply := f.call(t, context.Background(), tc.label, "bead.comment", map[string]any{"id": ref(tc.id), "body": tc.body}, true)
		want := "invalid_params"
		if tc.label == "unknown" {
			want = "not_found"
		}
		refused(t, reply, want)
		refusals = append(refusals, tc.label+":"+code(reply))
	}
	if head(t, f) != beforeHead || len(changesets(t, f)) != len(entries) {
		t.Fatal("comment refusal advanced head/history")
	}
	if engine == "sqlite" {
		f.reopen(t)
	}
	reopened := document(t, f, "b15-anchor")
	if !reflect.DeepEqual(after, reopened) {
		t.Fatalf("comment document changed after reopen: before=%+v after=%+v", after, reopened)
	}
	authors := []string{after.Comments[0].Principal.Kind + ":" + after.Comments[0].Principal.ID, after.Comments[1].Principal.Kind + ":" + after.Comments[1].Principal.ID}
	times := []string{after.Comments[0].Time.UTC().Format(time.RFC3339Nano), after.Comments[1].Time.UTC().Format(time.RFC3339Nano)}
	return canonical(commentProjection{Engine: engine, Bodies: []string{body1, "second comment"}, Authors: authors, Times: times, ChangedPaths: changedPaths, Events: events, Refusals: refusals})
}

func gateOntology(t *testing.T) *service.Ontology {
	t.Helper()
	doc := ontology.DefaultDocument()
	doc.GateKinds = map[string]ontology.GateKind{
		"human_approval": {Resolver: "builtin.manual", Params: map[string]any{"min_approvals": json.Number("2")}},
		"external":       {Resolver: "plugin.external"},
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("gate ontology failed to compile: %+v", problems)
	}
	return service.NewOntology(func(string) ontology.Effective { return snapshot.Effective("") })
}

type gateProjection struct {
	Engine          string `json:"engine"`
	FirstState      string `json:"firstState"`
	FirstApprovals  string `json:"firstApprovals"`
	DuplicateState  string `json:"duplicateState"`
	DuplicateCounts string `json:"duplicateApprovals"`
	ResolvedState   string `json:"resolvedState"`
	ResolvedCounts  string `json:"resolvedApprovals"`
	FinalStatus     string `json:"finalStatus"`
	WaitsFor        string `json:"waitsFor"`
	ReopenedStatus  string `json:"reopenedStatus"`
}

func gateFields(t *testing.T, reply map[string]any) map[string]any {
	t.Helper()
	bead, ok := result(t, reply)["bead"].(map[string]any)
	if !ok {
		t.Fatalf("gate get missing bead: %v", reply)
	}
	fields, ok := bead["fields"].(map[string]any)
	if !ok {
		t.Fatalf("gate get missing fields: %v", bead)
	}
	return fields
}

func gateState(t *testing.T, reply map[string]any) (string, string) {
	t.Helper()
	value := result(t, reply)
	state, _ := value["state"].(string)
	approvals, _ := value["approvals"].(json.Number)
	return state, approvals.String()
}

func TestB14GateSponsorResolutionAndExplicitWaitsForBoundaryTwice(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			first := runB14(t, engine)
			second := runB14(t, engine)
			if !bytes.Equal(first, second) {
				t.Fatalf("independent Plug gate projections differ:\nfirst=%s\nsecond=%s", first, second)
			}
			t.Logf("B14 %s independent projection sha256=%s", engine, sha(first))
		})
	}
}

func runB14(t *testing.T, engine string) []byte {
	t.Helper()
	f := newFixture(t, engine, "b14", []string{"b14-subject", "b14-blocker", "b14-gate"}, gateOntology(t))
	ctx := context.Background()
	f.call(t, ctx, "subject", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "subject"}}, true)
	f.call(t, ctx, "blocker", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "blocker"}}, true)
	created := result(t, f.call(t, ctx, "gate-create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "approval gate", "gate_kind": "human_approval"}}, true))
	gateID := createdID(t, created)
	alice := model.PrincipalRef{Kind: "agent", ID: "b14-agent-a", Sponsor: &model.PrincipalRef{Kind: "user", ID: "b14-user-a"}}
	bob := model.PrincipalRef{Kind: "agent", ID: "b14-agent-b", Sponsor: &model.PrincipalRef{Kind: "user", ID: "b14-user-b"}}
	firstReply := f.call(t, rpc.WithPrincipal(ctx, alice), "approve-a", "gate.approve", map[string]any{"id": ref(gateID)}, true)
	firstState, firstApprovals := gateState(t, firstReply)
	if firstState != "pending" || firstApprovals != "1" {
		t.Fatalf("first sponsor approval state=%s approvals=%s", firstState, firstApprovals)
	}
	duplicate := f.call(t, rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "agent", ID: "b14-agent-a-2", Sponsor: &model.PrincipalRef{Kind: "user", ID: "b14-user-a"}}), "approve-duplicate", "gate.approve", map[string]any{"id": ref(gateID)}, true)
	duplicateState, duplicateApprovals := gateState(t, duplicate)
	if duplicateState != "pending" || duplicateApprovals != "1" {
		t.Fatalf("duplicate sponsor approval state=%s approvals=%s", duplicateState, duplicateApprovals)
	}
	resolved := f.call(t, rpc.WithPrincipal(ctx, bob), "approve-b", "gate.approve", map[string]any{"id": ref(gateID)}, true)
	resolvedState, resolvedApprovals := gateState(t, resolved)
	if resolvedState != "resolved" || resolvedApprovals != "2" {
		t.Fatalf("second sponsor approval state=%s approvals=%s", resolvedState, resolvedApprovals)
	}
	got := f.call(t, ctx, "gate-get", "bead.get", map[string]any{"id": ref(gateID)}, false)
	fields := gateFields(t, got)
	if fields["status"] != "done" || fields["gate_state"] != "resolved" {
		t.Fatalf("resolved gate did not settle close slot: %v", fields)
	}
	entries := changesets(t, f)
	if len(entries) != 5 {
		t.Fatalf("duplicate approval unexpectedly published history: got %d entries", len(entries))
	}

	before := head(t, f)
	waitsFor := f.call(t, ctx, "waits-for", "bead.link", map[string]any{"from": ref("b14-subject"), "to": ref("b14-blocker"), "kind": "waits-for"}, true)
	result(t, waitsFor)
	links := document(t, f, "b14-subject").Links
	if head(t, f) == before || len(links) != 1 || links[0].Kind != "waits-for" || links[0].To.ID != "b14-blocker" {
		t.Fatalf("waits-for link was not persisted through the public Writer: %+v", links)
	}
	if engine == "sqlite" {
		f.reopen(t)
	}
	reopened := gateFields(t, f.call(t, ctx, "reopened", "bead.get", map[string]any{"id": ref(gateID)}, false))
	if reopened["status"] != "done" || reopened["gate_state"] != "resolved" {
		t.Fatalf("reopened gate changed terminal state: %v", reopened)
	}
	if got := document(t, f, "b14-subject").Links; !reflect.DeepEqual(got, links) {
		t.Fatalf("reopen changed waits-for link: before=%+v after=%+v", links, got)
	}
	return canonical(gateProjection{Engine: engine, FirstState: firstState, FirstApprovals: firstApprovals, DuplicateState: duplicateState, DuplicateCounts: duplicateApprovals, ResolvedState: resolvedState, ResolvedCounts: resolvedApprovals, FinalStatus: "done", WaitsFor: "persisted", ReopenedStatus: reopened["status"].(string)})
}

type labelBoundaryProjection struct {
	Engine          string   `json:"engine"`
	CreatedLabels   []string `json:"createdLabels"`
	MutationRefusal string   `json:"mutationRefusal"`
	FilterMatchIDs  []string `json:"filterMatchIds"`
	FilterAbsentIDs []string `json:"filterAbsentIds"`
	HeadUnchanged   bool     `json:"headUnchanged"`
	Disposition     string   `json:"disposition"`
}

func TestB16PublicLabelsAndTypedFilterBoundaryTwice(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			first := runB16(t, engine)
			second := runB16(t, engine)
			if !bytes.Equal(first, second) {
				t.Fatalf("independent Plug label-boundary projections differ:\nfirst=%s\nsecond=%s", first, second)
			}
			t.Logf("B16 %s typed label/filter projection sha256=%s (bounded controls)", engine, sha(first))
		})
	}
}

func runB16(t *testing.T, engine string) []byte {
	t.Helper()
	f := newFixture(t, engine, "b16", []string{"b16-normal"}, service.NewOntology(nil))
	create := f.call(t, context.Background(), "labels-create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "label probe", "labels": []any{"alpha"}}}, true)
	result(t, create)
	stored := document(t, f, "b16-normal")
	if !reflect.DeepEqual(stored.Labels, []string{"alpha"}) {
		t.Fatalf("public typed labels were not persisted: %+v", stored)
	}
	before := head(t, f)
	invalid := f.call(t, context.Background(), "labels-invalid", "bead.update", map[string]any{"id": ref("b16-normal"), "fields": map[string]any{"labels": []any{123}}}, true)
	refused(t, invalid, "invalid_params")
	if head(t, f) != before {
		t.Fatal("invalid typed labels advanced head")
	}
	filterIDs := make(map[string][]string, 2)
	for _, check := range []struct {
		label string
		want  []string
	}{
		{label: "alpha", want: []string{"b16-normal"}},
		{label: "absent", want: []string{}},
	} {
		filter := f.call(t, context.Background(), "labels-filter-"+check.label, "bead.ready", map[string]any{"filter": map[string]any{"labels": []any{check.label}}}, false)
		items, ok := result(t, filter)["items"].([]any)
		if !ok {
			t.Fatalf("label filter %q items are not an array: %v", check.label, filter)
		}
		ids := make([]string, 0, len(items))
		for _, value := range items {
			item, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("label filter %q item is not an object: %v", check.label, value)
			}
			id, ok := item["id"].(string)
			if !ok {
				t.Fatalf("label filter %q item has no string ID: %v", check.label, item)
			}
			if !reflect.DeepEqual(item["labels"], []any{"alpha"}) {
				t.Fatalf("label filter %q changed projected labels: %v", check.label, item)
			}
			ids = append(ids, id)
		}
		if !reflect.DeepEqual(ids, check.want) {
			t.Fatalf("label filter %q IDs=%v want=%v", check.label, ids, check.want)
		}
		if head(t, f) != before {
			t.Fatalf("label filter %q advanced head", check.label)
		}
		filterIDs[check.label] = ids
	}
	return canonical(labelBoundaryProjection{Engine: engine, CreatedLabels: stored.Labels, MutationRefusal: code(invalid), FilterMatchIDs: filterIDs["alpha"], FilterAbsentIDs: filterIDs["absent"], HeadUnchanged: head(t, f) == before, Disposition: "Typed durable label state/filter controls only; full label and ephemeral-plane evidence is separate"})
}

func TestIndependentProjectionHasNoMapOrderDependence(t *testing.T) {
	values := map[string]any{"b": 2, "a": 1}
	first := canonical(values)
	second := canonical(values)
	if !bytes.Equal(first, second) {
		t.Fatal("canonical projection changed across repeated map projection")
	}
	if !sort.StringsAreSorted([]string{"a", "b"}) {
		t.Fatal("projection sanity check failed")
	}
}
