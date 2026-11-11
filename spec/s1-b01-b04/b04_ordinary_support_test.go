package s1b01b04

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This fixture is serial and uses the ordinary anonymous HTTP interface.
// It does not use claim-identity, interleaving, authorization or CAS fixtures.
type ordinaryB04 struct {
	f *fixture
	server *httptest.Server
	output string
	seq int
}

type ordinaryB04State struct {
	Meta driver.Meta
	Rows []driver.Record
	History []model.Changeset
	Versions map[string][]driver.Version
}

func newOrdinaryB04(t *testing.T, backend, dsn string, ids []string) *ordinaryB04 {
	t.Helper()
	doc := ontology.DefaultDocument()
	doc.Statuses["awaiting_review"] = ontology.Status{Name: "Awaiting review", Category: ontology.Active}
	doc.Statuses["qa_testing"] = ontology.Status{Name: "QA testing", Category: ontology.WIP}
	w := doc.Workflows["issue"]
	w.Statuses = append(w.Statuses, "awaiting_review", "qa_testing")
	w.Transitions[0].From = append(w.Transitions[0].From, "awaiting_review")
	doc.Workflows["issue"] = w
	f := newFixture(t, backend, dsn, compileOntology(t, doc, nil), ids)
	output := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "ordinary-"+filepath.Base(t.Name()))
	if err := os.MkdirAll(output, 0700); err != nil { t.Fatal(err) }
	o := &ordinaryB04{f: f, output: output}
	o.install()
	t.Cleanup(func() { o.server.Close() })
	return o
}

func (o *ordinaryB04) install() {
	t := o.f.t
	registry, err := rpc.NewRegistry()
	if err != nil { t.Fatal(err) }
	if err := rpc.BindService(registry, o.f.svc); err != nil { t.Fatal(err) }
	s, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: o.f.metaSnapshot().Incarnation, CursorKey: []byte("ordinary-b04-independent-cursor-key-32")})
	if err != nil { t.Fatal(err) }
	a, err := authn.New(authn.Config{})
	if err != nil { t.Fatal(err) }
	h, err := httpjson.New(s, a, httpjson.Config{})
	if err != nil { t.Fatal(err) }
	o.server = httptest.NewServer(h)
}

func (o *ordinaryB04) retain(name string, value any) {
	o.f.t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil { o.f.t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(o.output, name+".json"), append(data, '\n'), 0600); err != nil { o.f.t.Fatal(err) }
}

func (o *ordinaryB04) call(label, method, agent string, params map[string]any) json.RawMessage {
	t := o.f.t
	t.Helper()
	o.seq++
	p := map[string]any{}
	for k, v := range params { p[k] = v }
	p["_meta"] = map[string]any{"project": project, "agent": agent, "idempotencyKey": o.f.key(label, method, agent)}
	request, err := json.Marshal(map[string]any{"jsonrpc":"2.0", "id": o.seq, "method":method, "params":p})
	if err != nil { t.Fatal(err) }
	response, err := o.server.Client().Post(o.server.URL+httpjson.Path, "application/json", bytes.NewReader(request))
	if err != nil { t.Fatal(err) }
	raw, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil { t.Fatalf("HTTP read=%v close=%v", readErr, closeErr) }
	o.retain(fmt.Sprintf("%03d-%s", o.seq, label), map[string]any{"request": json.RawMessage(request), "response":json.RawMessage(raw), "httpStatus": response.StatusCode})
	if response.StatusCode != http.StatusOK { t.Fatalf("HTTP status=%d frame=%s", response.StatusCode, raw) }
	return raw
}

func (o *ordinaryB04) commit(label, method string, params map[string]any) model.Commit {
	o.f.t.Helper()
	raw := o.call(label, method, owner, params)
	var e struct { Result model.Commit `json:"result"`; Error *model.Error `json:"error"` }
	if err := json.Unmarshal(raw, &e); err != nil || e.Error != nil || len(e.Result.Results) == 0 { o.f.t.Fatalf("commit %s: %s (%v)", label, raw, err) }
	return e.Result
}

func (o *ordinaryB04) seed(label string, ops ...model.Op) {
	o.commit(label, "tx.apply", map[string]any{"changeSet":model.ChangeSet{Ops: ops}})
}

func (o *ordinaryB04) state(label string) ordinaryB04State {
	t, f := o.f.t, o.f
	t.Helper()
	s := ordinaryB04State{Meta:f.metaSnapshot(), Rows:[]driver.Record{}, Versions:map[string][]driver.Version{}}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation:s.Meta.Incarnation})
	if err != nil { t.Fatal(err) }
	defer r.Close()
	var cursor *driver.Cursor
	for {
		p, err := r.Query(f.ctx, driver.Query{Filter:map[string]any{"kind":"bead"}, Limit:1000, Cursor:cursor})
		if err != nil { t.Fatal(err) }
		s.Rows = append(s.Rows, p.Items...)
		if p.NextCursor == nil { break }; cursor = p.NextCursor
	}
	s.History, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil { t.Fatal(err) }
	for _, row := range s.Rows {
		v, err := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit:1000})
		if err != nil || v.NextCursor != nil { t.Fatalf("complete versions: %v next=%v", err, v.NextCursor) }
		s.Versions[row.Ref.ID] = v.Items
	}
	o.retain(label, s)
	return s
}

func (o *ordinaryB04) equalState(label string, want ordinaryB04State) {
	o.f.t.Helper()
	if got := o.state(label); !reflect.DeepEqual(got, want) { o.f.t.Fatalf("%s changed exact rows, head, history or versions", label) }
}

func (o *ordinaryB04) empty(label string, params map[string]any) {
	t:=o.f.t;t.Helper()
	before:=o.state(label+"-before")
	raw:=o.call(label,"bead.claimNext",owner,params)
	var e map[string]json.RawMessage
	if err:=json.Unmarshal(raw,&e);err!=nil || !bytes.Equal(e["result"],[]byte("null")) || e["error"]!=nil {t.Fatalf("empty selected ready front must be nil success: %s",raw)}
	after:=o.state(label+"-after")
	h,_:=before.Meta.Head.Int64()
	if after.Meta.Head!=model.Natural64(uint64(h+1)) || len(after.History)!=len(before.History)+1 || !reflect.DeepEqual(after.Rows,before.Rows) || !reflect.DeepEqual(after.Versions,before.Versions) {t.Fatal("terminal null must retain one empty receipt changeset without domain versions")}
	found:=false
	for _,cs:=range after.History {if cs.Revision==after.Meta.Head {found=true;if len(cs.Changes)!=0 || cs.IdempotencyKey!=o.f.key(label,"bead.claimNext",owner) {t.Fatal("terminal null recorded a domain mutation or wrong key")}}}
	if !found {t.Fatal("terminal empty changeset absent")}
}

func (o *ordinaryB04) reopen(label string) {
	t, f := o.f.t, o.f
	t.Helper()
	before := o.state(label+"-before")
	if f.backend == "memory" { return }
	oldStore, oldLease := f.store, f.lease
	o.server.Close()
	if err := oldLease.Release(f.ctx); err != nil { t.Fatal(err) }
	if err := oldStore.Close(); err != nil { t.Fatal(err) }
	f.store = openStore(t, f.backend, f.path, f.dsn, f.clock)
	var err error
	f.lease, err = f.store.Lease(f.ctx, "ordinary-b04-reopened", driver.LeaseTTL)
	if err != nil { t.Fatal(err) }
	f.install()
	o.install()
	o.equalState(label+"-after", before)
	t.Logf("B04_ORDINARY_SQL_REOPEN backend=%s stage=%s old_store_closed=true exact_rows_history_versions=true", f.backend, label)
}

func (o *ordinaryB04) win(label, method, id, sourceStatus string, params map[string]any) model.Commit {
	t, f := o.f.t, o.f
	t.Helper()
	before := o.state(label+"-before")
	var old pipeline.Document
	for _, row := range before.Rows { if row.Ref.ID == id { var err error; old, err = pipeline.DecodeDocument(row.Body); if err != nil { t.Fatal(err) } } }
	if old.ID != id || old.Lease != nil || old.Fields["status"] != sourceStatus || old.StartedAt != nil { t.Fatalf("incorrect unassigned source: %+v", old) }
	c := o.commit(label, method, params)
	after := o.state(label+"-after")
	h, _ := before.Meta.Head.Int64()
	if c.Revision != model.Natural64(uint64(h+1)) || after.Meta.Head != c.Revision || len(after.History) != len(before.History)+1 || len(after.Versions[id]) != len(before.Versions[id])+1 { t.Fatal("winning claim must add one head, changeset and target version") }
	if len(c.Results) != 1 || c.Results[0].Ref != ref(id) || c.Results[0].Revision != c.Revision { t.Fatalf("winning result identity: %+v", c) }
	var actual driver.Record
	for _, row := range after.Rows { if row.Ref.ID == id { actual = row } else {
		for _, prev := range before.Rows { if prev.Ref == row.Ref && !reflect.DeepEqual(prev, row) { t.Fatalf("claim mutated unrelated %s", row.Ref.ID) } }
	} }
	if actual.Revision != c.Revision || !bytes.Equal(actual.Body, c.Results[0].Record) { t.Fatal("hydrated result is not the exact persisted postimage") }
	doc, err := pipeline.DecodeDocument(actual.Body)
	if err != nil { t.Fatal(err) }
	expires := f.clock.Now().Add(300*time.Second)
	if doc.Fields["status"] != "in_progress" || doc.StartedAt == nil || !doc.StartedAt.Equal(f.clock.Now()) || doc.Lease == nil || doc.Lease.Agent != owner || doc.Lease.Principal != (model.PrincipalRef{Kind:"anonymous", ID:"anonymous"}) || !doc.Lease.ExpiresAt.Equal(expires) || doc.Lease.TtlMs != model.Natural64(300000) || c.LeaseExpiresAt == nil || !c.LeaseExpiresAt.Equal(expires) || c.LeaseTtlMs == nil || *c.LeaseTtlMs != model.Natural64(300000) { t.Fatalf("exact winning status/start/ownership/default lease: %+v", doc) }
	// Independent expected postimage: preserve every original field, link,
	// label, comment and timestamp, changing only the named claim fields.
	want := old
	want.Fields = map[string]any{}
	for k,v := range old.Fields { want.Fields[k]=v }
	want.Fields["status"] = "in_progress"
	started := f.clock.Now(); want.StartedAt = &started
	want.Lease = &pipeline.ClaimLease{Principal:model.PrincipalRef{Kind:"anonymous", ID:"anonymous"}, Agent:owner, Epoch:model.Natural64(f.lease.Epoch()), ExpiresAt:expires, TtlMs:model.Natural64(300000)}
	if !reflect.DeepEqual(doc, want) { t.Fatalf("hydrated row differs from independent expected postimage: got=%+v want=%+v", doc,want) }
	return c
}

func (o *ordinaryB04) refusal(label, method, agent, code string, retryable bool, params map[string]any) {
	t := o.f.t; t.Helper()
	before := o.state(label+"-before")
	raw := o.call(label, method, agent, params)
	var e struct { JSONRPC string `json:"jsonrpc"`; ID int `json:"id"`; Error *struct { Code int `json:"code"`; Message string `json:"message"`; Data model.ErrorData `json:"data"` } `json:"error"` }
	decoder:=json.NewDecoder(bytes.NewReader(raw));decoder.DisallowUnknownFields()
	if err := decoder.Decode(&e); err != nil || e.Error == nil { t.Fatalf("closed public error: %s (%v)",raw,err) }
	if e.JSONRPC!="2.0" || e.ID!=o.seq || string(e.Error.Data.Code)!=code || e.Error.Data.Retryable!=retryable || e.Error.Data.TraceID=="" { t.Fatalf("catalog code/retryable/transport trace: %s",raw) }
	catalog,err:=wire.DefaultCatalog();if err!=nil {t.Fatal(err)}
	found:=false
	for _,entry:=range catalog.ErrorCatalog() { if entry.Code==code {found=true;if entry.JSONRPC!=e.Error.Code || entry.Retryable!=e.Error.Data.Retryable {t.Fatalf("public error differs from catalog: %s",raw)} } }
	if !found {t.Fatalf("unknown expected catalog error %s",code)}
	o.equalState(label+"-after",before)
}
