package s1b01b04

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
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
	releaseLease func() error
	receipts map[string]*driver.IdempotencyRecord
	methods map[string]string
	principals map[string]model.PrincipalRef
	authority *ordinaryB04Authority
}

type ordinaryB04State struct {
	Meta driver.Meta
	Rows []driver.Record
	History []model.Changeset
	Versions map[string][]driver.Version
}

// Fixed ordinary vectors are authored from the native public selector
// contract. They are independent of the selection implementation.
type ordinaryB04SelectionRow struct { Role string; Priority int; Kind string; Labels []string; Owned bool }
type ordinaryB04SelectionCase struct { Name string; Rows []ordinaryB04SelectionRow; Native []string; Public map[string]any; Ready []string; ListNative []string; Listed []string }

func ordinaryB04SelectionCases() []ordinaryB04SelectionCase {
	return []ordinaryB04SelectionCase{
		{Name:"all-normalized",Rows:[]ordinaryB04SelectionRow{{"a",0,"task",[]string{"ordinary"},false},{"b",2,"task",[]string{"ordinary","public"},false},{"c",1,"task",[]string{"public"},false}},Native:[]string{"--label"," ordinary ,public,ordinary,, "},Public:map[string]any{"labels":[]string{" ordinary ","public","ordinary",""," "}},Ready:[]string{"b"},ListNative:[]string{"--label"," ordinary ,public,ordinary,, "},Listed:[]string{"b"}},
		{Name:"any-normalized",Rows:[]ordinaryB04SelectionRow{{"a",2,"task",[]string{"red"},false},{"b",1,"task",[]string{"blue"},false},{"c",0,"task",[]string{"outside"},false}},Native:[]string{"--label-any"," red ,blue,red,, "},Public:map[string]any{"labelsAny":[]string{" red ","blue","red",""," "}},Ready:[]string{"b","a"},ListNative:[]string{"--label-any"," red ,blue,red,, "},Listed:[]string{"b","a"}},
		{Name:"exclude-label",Rows:[]ordinaryB04SelectionRow{{"a",0,"task",[]string{"skip"},false},{"b",1,"task",[]string{"keep"},false}},Native:[]string{"--exclude-label"," skip ,skip,, "},Public:map[string]any{"excludeLabels":[]string{" skip ","skip",""," "}},Ready:[]string{"b"},ListNative:[]string{"--exclude-label"," skip ,skip,, "},Listed:[]string{"b"}},
		{Name:"conjunction",Rows:[]ordinaryB04SelectionRow{{"a",0,"task",[]string{"ordinary","red","skip"},false},{"b",2,"task",[]string{"ordinary","blue"},false},{"c",1,"task",[]string{"red"},false},{"d",1,"task",[]string{"ordinary","green"},false}},Native:[]string{"--label","ordinary","--label-any","red,blue","--exclude-label","skip"},Public:map[string]any{"labels":[]string{"ordinary"},"labelsAny":[]string{"red","blue"},"excludeLabels":[]string{"skip"}},Ready:[]string{"b"},ListNative:[]string{"--label","ordinary","--label-any","red,blue","--exclude-label","skip"},Listed:[]string{"b"}},
		{Name:"empty-normalized",Rows:[]ordinaryB04SelectionRow{{"a",0,"task",nil,false},{"b",1,"task",[]string{"anything"},false}},Native:[]string{"--label"," , ","--label-any"," , ","--exclude-label"," , "},Public:map[string]any{"labels":[]string{""," "},"labelsAny":[]string{" ",""},"excludeLabels":[]string{""," "}},Ready:[]string{"a","b"},ListNative:[]string{"--label"," , ","--label-any"," , ","--exclude-label"," , "},Listed:[]string{"a","b"}},
		{Name:"case-sensitive",Rows:[]ordinaryB04SelectionRow{{"a",0,"task",[]string{"public"},false},{"b",1,"task",[]string{"Public"},false}},Native:[]string{"--label","Public"},Public:map[string]any{"labels":[]string{"Public"}},Ready:[]string{"b"},ListNative:[]string{"--label","Public"},Listed:[]string{"b"}},
		{Name:"exclude-type",Rows:[]ordinaryB04SelectionRow{{"a",0,"bug",nil,false},{"b",1,"task",nil,false}},Native:[]string{"--exclude-type","bug"},Public:map[string]any{"excludeTypes":[]string{"bug"}},Ready:[]string{"b"},ListNative:[]string{"--exclude-type","bug"},Listed:[]string{"b"}},
		{Name:"explicit-type-precedence",Rows:[]ordinaryB04SelectionRow{{"a",1,"task",nil,false},{"b",0,"bug",nil,false}},Native:[]string{"--type","task","--exclude-type","task"},Public:map[string]any{"types":[]string{"issue"},"excludeTypes":[]string{"issue"}},Ready:[]string{"a"},ListNative:[]string{"--type","task","--exclude-type","task"},Listed:[]string{}},
		{Name:"unassigned",Rows:[]ordinaryB04SelectionRow{{"a",0,"task",nil,true},{"b",1,"task",nil,false}},Native:[]string{"--unassigned"},Public:map[string]any{"unassigned":true},Ready:[]string{"b"},ListNative:[]string{"--no-assignee"},Listed:[]string{"b"}},
	}
}

func ordinarySelectionID(name,role string) string {
	root:="b04-select-"+name+"-root"
	if role=="root" {return root}
	return fmt.Sprintf("%s.%d",root,int(role[0]-'a')+1)
}

func ordinaryCreateOp(key,inProject string,fields map[string]any,parent string) model.Op {
	copy:=map[string]any{};for k,v:=range fields {if k!="status" {copy[k]=v}}
	op:=createOp(key,inProject,copy,parent)
	if status,ok:=fields["status"].(string);ok {op.Create.TargetStatus=&status}
	return op
}

func (o *ordinaryB04) selectionFixture(v ordinaryB04SelectionCase) map[string]any {
	root:=ordinarySelectionID(v.Name,"root")
	ops:=[]model.Op{ordinaryCreateOp("root",project,map[string]any{"title":root,"priority":4},"")}
	for _,row:=range v.Rows {fields:=map[string]any{"title":ordinarySelectionID(v.Name,row.Role),"priority":row.Priority};if row.Labels!=nil {fields["labels"]=row.Labels};op:=ordinaryCreateOp(row.Role,project,fields,root);if row.Kind=="bug" {op.Create.Type="bug"};ops=append(ops,op)}
	o.seed("selection-create",ops...)
	for _,row:=range v.Rows {if row.Owned {id:=ordinarySelectionID(v.Name,row.Role);o.win("selection-owned-"+row.Role,"bead.claim",id,"todo",map[string]any{"id":ref(id)})}}
	filter:=map[string]any{"parent":root};for k,value:=range v.Public {filter[k]=value}
	return filter
}

func newOrdinaryB04(t *testing.T, backend, dsn string, ids []string, configure ...func(*ontology.Document)) *ordinaryB04 {
	t.Helper()
	doc := ontology.DefaultDocument()
	doc.Statuses["awaiting_review"] = ontology.Status{Name: "Awaiting review", Category: ontology.Active}
	doc.Statuses["qa_testing"] = ontology.Status{Name: "QA testing", Category: ontology.WIP}
	w := doc.Workflows["issue"]
	w.Statuses = append(w.Statuses, "awaiting_review", "qa_testing")
	w.Transitions[0].From = append(w.Transitions[0].From, "awaiting_review")
	w.Transitions = append(w.Transitions,
		ontology.Rule{From: []string{w.Initial}, To: "awaiting_review", Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		ontology.Rule{From: []string{w.Initial}, To: "qa_testing", Guards: []ontology.Guard{{Kind: ontology.Permission}}},
	)
	doc.Workflows["issue"] = w
	doc.Types["bug"] = ontology.Type{Name:"Bug", Workflow:"issue", Fields:doc.Types["issue"].Fields}
	for _, change := range configure { change(&doc) }
	rules := compileOntology(t, doc, nil)
	clock := newClock(t)
	if backend == "postgres" { dsn = clock.BindPostgres(t, dsn) }
	path := ""
	if backend == "sqlite" { path = filepath.Join(t.TempDir(), "s1.db") }
	store := openStore(t, backend, path, dsn, clock)
	ctx, lease, releaseLease, err := converterroundtrip.HoldRestoreLease(context.Background(), store, "s1-server")
	if err != nil { _ = store.Close(); t.Fatal(err) }
	f := &fixture{t:t, ctx:ctx, backend:backend, path:path, dsn:dsn, store:store, lease:lease, clock:clock, ontology:rules, ids:append([]string(nil), ids...)}
	output := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "ordinary-"+strings.ReplaceAll(t.Name(),"/","_"))
	o := &ordinaryB04{f: f, output: output, releaseLease:releaseLease, receipts:map[string]*driver.IdempotencyRecord{}, methods:map[string]string{}, principals:map[string]model.PrincipalRef{}}
	t.Cleanup(func() {
		if o.server != nil { o.server.Close() }
		if err := o.releaseStoreLease(); err != nil { t.Errorf("stop ordinary store lease: %v", err) }
		if err := f.store.Close(); err != nil { t.Errorf("close ordinary store: %v", err) }
	})
	f.install()
	if err := os.MkdirAll(output, 0700); err != nil { t.Fatal(err) }
	o.install()
	return o
}

func (o *ordinaryB04) releaseStoreLease() error {
	if o.releaseLease == nil { return nil }
	release := o.releaseLease
	o.releaseLease = nil
	return release()
}

func (o *ordinaryB04) install() {
	t := o.f.t
	registry, err := rpc.NewRegistry()
	if err != nil { t.Fatal(err) }
	bound := rpc.Service(o.f.svc)
	authConfig := authn.Config{}
	if o.authority != nil { bound, authConfig = o.authority.bind(o) }
	if err := rpc.BindService(registry, bound); err != nil { t.Fatal(err) }
	s, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: o.f.metaSnapshot().Incarnation, CursorKey: []byte("ordinary-b04-independent-cursor-key-32")})
	if err != nil { t.Fatal(err) }
	a, err := authn.New(authConfig)
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
	httpRequest, err := http.NewRequest(http.MethodPost, o.server.URL+httpjson.Path, bytes.NewReader(request))
	if err != nil { t.Fatal(err) }
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Plug-RPC", "1")
	if o.authority != nil { token:=o.authority.writeKey; if method=="bead.ready" || method=="bead.get" { token=o.authority.readKey }; httpRequest.Header.Set("Authorization", "Bearer "+token) }
	response, err := o.server.Client().Do(httpRequest)
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
	return o.commitResponse(label, method, raw)
}

func (o *ordinaryB04) commitResponse(label, method string, raw json.RawMessage) model.Commit {
	o.f.t.Helper()
	var e struct { Result model.Commit `json:"result"`; Error *model.Error `json:"error"` }
	if err := json.Unmarshal(raw, &e); err != nil || e.Error != nil || len(e.Result.Results) == 0 { o.f.t.Fatalf("commit %s: %s (%v)", label, raw, err) }
	entry:=o.receipt(label,method,owner)
	if entry==nil || entry.ResponsePending || !reflect.DeepEqual(entry.Commit,e.Result) { o.f.t.Fatalf("public commit differs from durable response: %s",raw) }
	o.receipts[label]=entry;o.methods[label]=method;o.principals[label]=o.principal()
	return e.Result
}

func (o *ordinaryB04) seed(label string, ops ...model.Op) {
	o.f.t.Helper()
	params := map[string]any{"changeSet": model.ChangeSet{Ops: ops}}
	raw := o.callSeedWithRetry(label, "tx.apply", owner, params)
	o.commitResponse(label, "tx.apply", raw)
}

// callSeedWithRetry retries only typed retryable errors from disposable setup
// writes. The request bytes, JSON-RPC id, and idempotency key remain identical
// across attempts. Calls under test use call and are never retried here.
func (o *ordinaryB04) callSeedWithRetry(label, method, agent string, params map[string]any) json.RawMessage {
	t := o.f.t
	t.Helper()
	o.seq++
	requestParams := map[string]any{}
	for k, v := range params { requestParams[k] = v }
	requestParams["_meta"] = map[string]any{"project": project, "agent": agent, "idempotencyKey": o.f.key(label, method, agent)}
	request, err := json.Marshal(map[string]any{"jsonrpc":"2.0", "id": o.seq, "method": method, "params": requestParams})
	if err != nil { t.Fatal(err) }
	const maxAttempts = 5
	started := time.Now()
	retries := 0
	var last json.RawMessage
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		httpRequest, err := http.NewRequest(http.MethodPost, o.server.URL+httpjson.Path, bytes.NewReader(request))
		if err != nil { t.Fatal(err) }
		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Plug-RPC", "1")
		if o.authority != nil {
			token := o.authority.writeKey
			if method == "bead.ready" || method == "bead.get" { token = o.authority.readKey }
			httpRequest.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := o.server.Client().Do(httpRequest)
		if err != nil { t.Fatal(err) }
		raw, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil { t.Fatalf("HTTP read=%v close=%v", readErr, closeErr) }
		last = append(json.RawMessage(nil), raw...)
		o.retain(fmt.Sprintf("%03d-%s-attempt-%d", o.seq, label, attempt), map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(raw), "httpStatus": response.StatusCode})
		if response.StatusCode != http.StatusOK { t.Fatalf("HTTP status=%d frame=%s", response.StatusCode, raw) }
		var frame struct { Error *struct { Data model.ErrorData `json:"data"` } `json:"error"` }
		if err := json.Unmarshal(raw, &frame); err != nil { t.Fatalf("seed %s response is not JSON: %s", label, raw) }
		if frame.Error == nil || !frame.Error.Data.Retryable {
			t.Logf("B04_SEED_RETRIES engine=%s label=%s attempts=%d retries=%d elapsedMs=%.3f", o.f.backend, label, attempt, retries, time.Since(started).Seconds()*1000)
			return last
		}
		if attempt == maxAttempts {
			t.Logf("B04_SEED_RETRIES engine=%s label=%s attempts=%d retries=%d exhausted=true elapsedMs=%.3f", o.f.backend, label, attempt, retries, time.Since(started).Seconds()*1000)
			return last
		}
		retries++
		t.Logf("B04_SEED_RETRY engine=%s label=%s attempt=%d retry=%d code=%s traceId=%s", o.f.backend, label, attempt, retries, frame.Error.Data.Code, frame.Error.Data.TraceID)
		backoff := 50 * time.Millisecond * time.Duration(1<<(retries-1))
		if backoff > time.Second { backoff = time.Second }
		time.Sleep(backoff)
	}
	return last
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

func (o *ordinaryB04) receipt(label,method,agent string) *driver.IdempotencyRecord {
	t,f:=o.f.t,o.f;t.Helper()
	r,err:=f.store.Read(f.ctx,driver.View{Incarnation:f.metaSnapshot().Incarnation});if err!=nil {t.Fatal(err)};defer r.Close()
	principal,err:=driver.PrincipalKey(o.receiptPrincipal(label));if err!=nil {t.Fatal(err)}
	scope:=agent;if o.receiptPrincipal(label).Kind!="anonymous" {scope=""}
	entry,err:=r.IdempotencyByKey(f.ctx,principal,scope,method,f.key(label,method,agent));if err!=nil {t.Fatal(err)}
	return entry
}

func (o *ordinaryB04) sameHolder(label,id string) {
	t:=o.f.t;t.Helper()
	before:=o.state(label+"-before")
	var row driver.Record
	for _,r:=range before.Rows {if r.Ref.ID==id {row=r}}
	doc,err:=pipeline.DecodeDocument(row.Body);if err!=nil || doc.Lease==nil {t.Fatalf("no-op preimage: %v",err)}
	c:=o.commit(label,"bead.claim",map[string]any{"id":ref(id)})
	if c.Revision!=before.Meta.Head || len(c.Results)!=1 || c.Results[0].Ref!=row.Ref || c.Results[0].Revision!=row.Revision || !bytes.Equal(c.Results[0].Record,row.Body) || c.LeaseExpiresAt==nil || !c.LeaseExpiresAt.Equal(doc.Lease.ExpiresAt) || c.LeaseTtlMs==nil || *c.LeaseTtlMs!=doc.Lease.TtlMs {t.Fatalf("fresh-key no-op returned incorrect row/revision/lease: %+v",c)}
	o.equalState(label+"-after",before)
	o.checkReceipts()
}

func (o *ordinaryB04) publicReadback(label,id string) {
	t:=o.f.t;t.Helper()
	raw:=o.call(label,"bead.get",owner,map[string]any{"id":ref(id)})
	got:=result(t,raw)
	doc:=o.f.document(id)
	var revision model.Revision
	for _,row:=range o.state(label+"-stored").Rows {if row.Ref.ID==id {revision=row.Revision}}
	bead:=map[string]any{"id":id,"type":doc.Type,"project":project,"revision":revision,"status":doc.Fields["status"],"fields":doc.Fields,"metadata":map[string]any{}}
	if doc.CreatedAt!=nil {bead["createdAt"]=doc.CreatedAt.UTC().Format(time.RFC3339Nano)}
	if doc.Labels!=nil {bead["labels"]=doc.Labels}
	if doc.Parent!=nil {bead["parent"]=doc.Parent.ID}
	want:=map[string]any{"bead":bead,"attachments":[]model.Attachment{}}
	if doc.Lease!=nil {want["lease"]=map[string]any{"holder":doc.Lease.Principal.ID,"expiresAt":doc.Lease.ExpiresAt.UTC().Format(time.RFC3339Nano),"ttlMs":doc.Lease.TtlMs,"epoch":doc.Lease.Epoch}}
	encoded,err:=json.Marshal(want);if err!=nil {t.Fatal(err)}
	if !reflect.DeepEqual(got,decodeObject(t,encoded)) {t.Fatalf("public get differs from retained expected row: got=%s want=%s",raw,encoded)}
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
	prior:=[]model.Changeset{};for _,cs:=range after.History {if cs.Revision!=after.Meta.Head {prior=append(prior,cs)}};if !reflect.DeepEqual(prior,before.History) {t.Fatal("empty claim changed previous changesets")}
	entry:=o.receipt(label,"bead.claimNext",owner)
	if entry==nil || entry.ResponsePending || entry.Commit.Revision!=after.Meta.Head || len(entry.Commit.Results)!=0 || len(entry.Changeset.Changes)!=0 {t.Fatal("terminal empty result lacks its exact persisted receipt")}
	o.receipts[label]=entry;o.methods[label]="bead.claimNext";o.principals[label]=o.principal()
	o.retain(label+"-receipt",entry)
	o.checkReceipts()
}

func (o *ordinaryB04) reopen(label string) {
	t, f := o.f.t, o.f
	t.Helper()
	before := o.state(label+"-before")
	o.checkReceipts()
	if f.backend == "memory" { return }
	oldStore := f.store
	o.server.Close()
	if err := o.releaseStoreLease(); err != nil { t.Fatal(err) }
	if err := oldStore.Close(); err != nil { t.Fatal(err) }
	f.store = openStore(t, f.backend, f.path, f.dsn, f.clock)
	var err error
	f.ctx, f.lease, o.releaseLease, err = converterroundtrip.HoldRestoreLease(context.Background(), f.store, "ordinary-b04-reopened")
	if err != nil { t.Fatal(err) }
	f.install()
	o.install()
	o.equalState(label+"-after", before)
	for name,want:=range o.receipts {got:=o.receipt(name,o.methods[name],owner);if !reflect.DeepEqual(got,want) {t.Fatalf("SQL reopen changed durable response %s",name)}}
	t.Logf("B04_ORDINARY_SQL_REOPEN backend=%s stage=%s old_store_closed=true exact_rows_history_versions=true", f.backend, label)
}

func (o *ordinaryB04) win(label, method, id, sourceStatus string, params map[string]any) model.Commit {
	t, f := o.f.t, o.f
	t.Helper()
	before := o.state(label+"-before")
	var old pipeline.Document
	var oldRecord driver.Record
	for _, row := range before.Rows { if row.Ref.ID == id { oldRecord=row;var err error; old, err = pipeline.DecodeDocument(row.Body); if err != nil { t.Fatal(err) } } }
	if old.ID != id || old.Lease != nil || old.Fields["status"] != sourceStatus || old.StartedAt != nil { t.Fatalf("incorrect unassigned source: %+v", old) }
	c := o.commit(label, method, params)
	after := o.state(label+"-after")
	h, _ := before.Meta.Head.Int64()
	if c.Revision != model.Natural64(uint64(h+1)) || after.Meta.Head != c.Revision || len(after.History) != len(before.History)+1 || len(after.Versions[id]) != len(before.Versions[id])+1 { t.Fatal("winning claim must add one head, changeset and target version") }
	if len(c.Results) != 1 || c.Results[0].Ref != ref(id) || c.Results[0].Revision != c.Revision { t.Fatalf("winning result identity: %+v", c) }
	if len(after.Rows)!=len(before.Rows) || len(after.Versions)!=len(before.Versions) {t.Fatal("claim changed the complete row set")}
	var actual driver.Record
	previous:=map[string]driver.Record{};for _,row:=range before.Rows {previous[row.Ref.ID]=row}
	for _,row:=range after.Rows {prev,ok:=previous[row.Ref.ID];if !ok {t.Fatalf("claim inserted unrelated %s",row.Ref.ID)};delete(previous,row.Ref.ID);if row.Ref.ID==id {actual=row}else if !reflect.DeepEqual(prev,row) || !reflect.DeepEqual(before.Versions[row.Ref.ID],after.Versions[row.Ref.ID]) {t.Fatalf("claim changed unrelated row/history %s",row.Ref.ID)}}
	if len(previous)!=0 {t.Fatal("claim deleted unrelated rows")}
	if actual.Revision != c.Revision || !bytes.Equal(actual.Body, c.Results[0].Record) { t.Fatal("hydrated result is not the exact persisted postimage") }
	doc, err := pipeline.DecodeDocument(actual.Body)
	if err != nil { t.Fatal(err) }
	expires := f.clock.Now().Add(300*time.Second)
	if doc.Fields["status"] != "in_progress" || doc.StartedAt == nil || !doc.StartedAt.Equal(f.clock.Now()) || doc.Lease == nil || doc.Lease.Agent != o.leaseAgent() || !reflect.DeepEqual(doc.Lease.Principal,o.principal()) || !doc.Lease.ExpiresAt.Equal(expires) || doc.Lease.TtlMs != model.Natural64(300000) || c.LeaseExpiresAt == nil || !c.LeaseExpiresAt.Equal(expires) || c.LeaseTtlMs == nil || *c.LeaseTtlMs != model.Natural64(300000) { t.Fatalf("exact winning status/start/ownership/default lease: %+v", doc) }
	// Independent expected postimage: preserve every original field, link,
	// label, comment and timestamp, changing only the named claim fields.
	want := old
	want.Fields = map[string]any{}
	for k,v := range old.Fields { want.Fields[k]=v }
	want.Fields["status"] = "in_progress"
	started := f.clock.Now(); want.StartedAt = &started
	want.Lease = &pipeline.ClaimLease{Principal:o.principal(), Agent:o.leaseAgent(), Epoch:model.Natural64(f.lease.Epoch()), ExpiresAt:expires, TtlMs:model.Natural64(300000)}
	if !reflect.DeepEqual(doc, want) { t.Fatalf("hydrated row differs from independent expected postimage: got=%+v want=%+v", doc,want) }
	found:=false
	for _,cs:=range after.History {if cs.Revision==c.Revision {found=true;if len(cs.Changes)!=1 || cs.Changes[0].Ref!=ref(id) || cs.Changes[0].Op!=model.OpPatch || cs.Changes[0].FromRevision!=oldRecord.Revision || cs.IdempotencyKey!=f.key(label,method,owner) || !cs.Time.Equal(f.clock.Now()) || !reflect.DeepEqual(cs.Principal,o.principal()) {t.Fatalf("winning history attribution: %+v",cs)}}}
	if !found {t.Fatal("winning claim missing attributable changeset")}
	priorHistory:=[]model.Changeset{};for _,cs:=range after.History {if cs.Revision!=c.Revision {priorHistory=append(priorHistory,cs)}}
	if !reflect.DeepEqual(priorHistory,before.History) {t.Fatal("claim changed previous changesets")}
	priorVersions:=[]driver.Version{};count:=0
	for _,v:=range after.Versions[id] {if v.Revision!=c.Revision {priorVersions=append(priorVersions,v);continue};count++;if v.Ref!=ref(id) || v.Deleted || !v.CreatedAt.Equal(f.clock.Now()) || !reflect.DeepEqual(v.Principal,o.principal()) {t.Fatal("winning version attribution differs")};body:=v.Body;if !v.Base {var ops []struct{Op,Path string;Value json.RawMessage};if err:=json.Unmarshal(v.Patch,&ops);err!=nil || len(ops)!=1 || ops[0].Op!="replace" || ops[0].Path!="" {t.Fatalf("invalid forward history patch: %s",v.Patch)};body=ops[0].Value};if !bytes.Equal(body,actual.Body) {t.Fatal("winning version does not reconstruct exact committed row")}}
	if count!=1 || !reflect.DeepEqual(priorVersions,before.Versions[id]) {t.Fatal("claim must preserve previous versions and append exactly one")}
	entry:=o.receipt(label,method,owner)
	if entry==nil || entry.ResponsePending || !reflect.DeepEqual(entry.Commit,c) {t.Fatal("winning public response differs from durable receipt")}
	o.checkReceipts()
	return c
}

func (o *ordinaryB04) readyIDs(label string, filter map[string]any) []string {
	t:=o.f.t;t.Helper()
	ids:=[]string{};seen:=map[string]bool{};cursor:=""
	for page:=0;;page++ {params:=map[string]any{"filter":filter,"limit":1000};if cursor!="" {params["cursor"]=cursor};v:=result(t,o.call(fmt.Sprintf("%s-page-%d",label,page),"bead.ready",owner,params));items,ok:=v["items"].([]any);if !ok {t.Fatalf("public ready has no items: %+v",v)};for _,item:=range items {b,ok:=item.(map[string]any);if !ok {t.Fatal("invalid public ready item")};id,ok:=b["id"].(string);if !ok || seen[id] {t.Fatal("public ready empty/duplicate id")};seen[id]=true;ids=append(ids,id)};next,_:=v["nextCursor"].(string);if next=="" {break};if next==cursor {t.Fatal("public ready repeated cursor")};cursor=next}
	return ids
}

func (o *ordinaryB04) refusal(label, method, agent, code string, retryable bool, params map[string]any) {
	t := o.f.t; t.Helper()
	before := o.state(label+"-before")
	if o.receipt(label,method,agent)!=nil {t.Fatal("refusal fixture reused a key")}
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
	o.checkReceipts()
	if o.receipt(label,method,agent)!=nil {t.Fatal("refusal persisted a success receipt")}
}

func (o *ordinaryB04) principal() model.PrincipalRef {
	if o.authority != nil { return o.authority.writer }
	return model.PrincipalRef{Kind:"anonymous",ID:"anonymous"}
}

func (o *ordinaryB04) receiptPrincipal(label string) model.PrincipalRef {
	if p,ok:=o.principals[label];ok {return p}
	return o.principal()
}

func (o *ordinaryB04) checkReceipts() {
	o.f.t.Helper()
	for label,want:=range o.receipts {if got:=o.receipt(label,o.methods[label],owner);!reflect.DeepEqual(got,want) {o.f.t.Fatalf("unrelated retained receipt changed: %s",label)}}
}

func (o *ordinaryB04) listingIDs(label string, filter map[string]any) []string {
	t:=o.f.t;t.Helper()
	before:=o.state(label+"-before")
	value:=result(t,o.call(label,"history.query",owner,map[string]any{"asOf":map[string]any{"revision":o.f.metaSnapshot().Head},"filter":filter,"limit":1000}))
	items,ok:=value["items"].([]any);if !ok {t.Fatalf("snapshot listing has no items: %+v",value)}
	ids:=[]string{};seen:=map[string]bool{}
	for _,item:=range items {row,ok:=item.(map[string]any);if !ok {t.Fatal("snapshot item is not an object")};id,ok:=row["id"].(string);if !ok || id=="" || seen[id] {t.Fatal("invalid/duplicate snapshot ID")};seen[id]=true;ids=append(ids,id)}
	if value["nextCursor"]!=nil {t.Fatal("ordinary small snapshot listing was truncated")}
	o.equalState(label+"-after",before);o.checkReceipts()
	sort.Strings(ids)
	return ids
}

func (o *ordinaryB04) selectionListingControl(filter map[string]any, want []string) {
	t:=o.f.t;t.Helper()
	expected:=append([]string{},want...);sort.Strings(expected)
	if got:=o.listingIDs("selection-snapshot-list",filter);!reflect.DeepEqual(got,expected) {t.Fatalf("snapshot selector membership=%v want=%v",got,expected)}
	if filter["unassigned"]!=true {return}
	base:=map[string]any{};for k,v:=range filter {if k!="unassigned" {base[k]=v}}
	all:=[]string{ordinarySelectionID("unassigned","a"),ordinarySelectionID("unassigned","b")};sort.Strings(all)
	for i:=0;i<2;i++ {q:=map[string]any{};for k,v:=range base {q[k]=v};if i==1 {q["unassigned"]=false};if got:=o.listingIDs(fmt.Sprintf("unassigned-list-%d",i),q);!reflect.DeepEqual(got,all) {t.Fatalf("unassigned omitted/false case%d membership=%v want=%v",i,got,all)}}
}

func (o *ordinaryB04) leaseAgent() string {
	if o.principal().Kind=="anonymous" {return owner}
	return ""
}
