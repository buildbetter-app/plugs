// The package name reuses the existing source-pinned B20 build and SQL engine
// fixtures. This directory owns only B16 public label vectors.
package cardb20

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "reflect"
    "sort"
    "strings"
    "testing"

    "github.com/buildbetter-app/plugs/internal/core/model"
    "github.com/buildbetter-app/plugs/internal/core/pipeline"
    "github.com/buildbetter-app/plugs/internal/core/service"
    "github.com/buildbetter-app/plugs/internal/core/viewops"
    "github.com/buildbetter-app/plugs/internal/server/pluginhost"
    "github.com/buildbetter-app/plugs/internal/server/rpc"
    "github.com/buildbetter-app/plugs/internal/server/viewrpc"
    "github.com/buildbetter-app/plugs/internal/storage/driver"
    "github.com/buildbetter-app/plugs/internal/storage/memory"
    "github.com/buildbetter-app/plugs/internal/storage/postgres"
    "github.com/buildbetter-app/plugs/internal/storage/sqlite"
    workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const b16Project = "b16-public"

func TestMain(m *testing.M) {
    if err:=os.Setenv("PATH","/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"));err!=nil{panic(err)}
    os.Exit(m.Run())
}

type b16Host struct {
    t *testing.T
    engine, dsn string
    store driver.Store
    lease driver.ActiveLease
    server *rpc.Server
    serial, next int
    lastKey string
    frames []map[string]any
}

func b16NewHost(t *testing.T, engine string) *b16Host {
    t.Helper()
    f := &b16Host{t:t, engine:engine}
    if engine == "sqlite" { f.dsn = filepath.Join(t.TempDir(), "b16.db") }
    if engine == "postgres" { f.dsn = startPostgres(t) }
    f.open()
    t.Cleanup(func() {
        f.close()
        if root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); root != "" {
            b16Write(t, filepath.Join(root, strings.ReplaceAll(t.Name(),"/","-")+"-wire.json"), f.frames)
        }
    })
    return f
}

func (f *b16Host) open() {
    f.t.Helper()
    var err error
    switch f.engine {
    case "memory": f.store = memory.New()
    case "sqlite": f.store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN:f.dsn})
    case "postgres": f.store, err = (postgres.Driver{}).Open(context.Background(), driver.Config{DSN:f.dsn})
    default: f.t.Fatal("unknown engine")
    }
    if err != nil { f.t.Fatal(err) }
    f.lease, err = f.store.Lease(context.Background(), "b16-public", driver.LeaseTTL)
    if err != nil { f.t.Fatal(err) }
    ontology := service.NewOntology(nil)
    writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func()(string,error){ f.next++; return fmt.Sprintf("b16-%06d", f.next), nil }))
    options := []service.Option{service.WithOntology(ontology)}
    checkpoints, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
    if err != nil { f.t.Fatal(err) }
    state, err := pluginhost.NewWorkflowRuntimeState(checkpoints, f.lease)
    if err != nil { f.t.Fatal(err) }
    persistent, ok := state.(interface{ Persistent() bool })
    if !ok { f.t.Fatal("workflow state lacks its actual persistence marker") }
    if f.engine == "memory" {
        b16Equal(f.t, checkpoints.WorkflowCheckpointLifetime(), driver.CheckpointLifetimeProcess)
        b16Equal(f.t, persistent.Persistent(), false)
        f.t.Log("actual memory workflow checkpoint lifetime=process Persistent=false; no restart claim")
    } else {
        b16Equal(f.t, checkpoints.WorkflowCheckpointLifetime(), driver.CheckpointLifetimeRestart)
        b16Equal(f.t, persistent.Persistent(), true)
    }
    provider, err := workflowplugin.New(state)
    if err != nil { f.t.Fatal(err) }
    options = append(options, service.WithWorkflow(provider.WithIdentity("workflow", "default", f.meta().Incarnation)))
    domain := service.New(f.store, f.lease, writer, options...)
    views, err := viewops.New(f.store, f.lease, viewops.Config{OntologyRevision:model.Natural64(1)})
    if err != nil { f.t.Fatal(err) }
    registry, err := rpc.NewRegistry()
    if err != nil { f.t.Fatal(err) }
    if err = rpc.BindService(registry, domain); err != nil { f.t.Fatal(err) }
    key := []byte("b16-public-label-cursor-key-00001")
    if err = viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey:key}); err != nil { f.t.Fatal(err) }
    f.server, err = rpc.New(registry, rpc.Config{DefaultProject:b16Project, Incarnation:f.meta().Incarnation, OntologyRevision:model.Natural64(1), CursorKey:key})
    if err != nil { f.t.Fatal(err) }
}

func (f *b16Host) close() {
    if f.lease != nil { if err := f.lease.Release(context.Background()); err != nil { f.t.Error(err) }; f.lease = nil }
    if f.store != nil { if err := f.store.Close(); err != nil { f.t.Error(err) }; f.store = nil }
}
func (f *b16Host) meta() driver.Meta {
    f.t.Helper()
    m, err := f.store.Meta(context.Background()); if err != nil { f.t.Fatal(err) }; return m
}
func b16Decode(t *testing.T, raw []byte, value any) {
    t.Helper(); d:=json.NewDecoder(bytes.NewReader(raw)); d.UseNumber(); if err:=d.Decode(value); err!=nil { t.Fatalf("JSON: %v: %s",err,raw) }
}
func b16Write(t *testing.T, path string, value any) {
    t.Helper(); data,err:=json.MarshalIndent(value,"","  "); if err!=nil { t.Fatal(err) }; if err=os.WriteFile(path,append(data,'\n'),0600);err!=nil {t.Fatal(err)}
}
func b16Equal(t *testing.T, got, want any) {
    t.Helper(); if !reflect.DeepEqual(got,want) {t.Fatalf("exact B16 value got=%#v want=%#v",got,want)}
}
func (f *b16Host) call(method string, params map[string]any, write bool, code string) json.RawMessage {
    return f.request(method,params,write,code,"")
}
func (f *b16Host) request(method string, params map[string]any, write bool, code,key string) json.RawMessage {
    f.t.Helper(); f.serial++
    p:=map[string]any{}; for k,v:=range params {p[k]=v}
    meta:=map[string]any{"project":b16Project,"agent":"b16-public"}; if write {if key==""{key=fmt.Sprintf("%032x",f.serial)};meta["idempotencyKey"]=key;f.lastKey=key};p["_meta"]=meta
    request,err:=json.Marshal(map[string]any{"jsonrpc":"2.0","id":f.serial,"method":method,"params":p});if err!=nil {f.t.Fatal(err)}
    raw:=f.server.Dispatch(rpc.WithPrincipal(context.Background(),model.PrincipalRef{Kind:"user",ID:"b16-user"}),request)
    f.frames=append(f.frames,map[string]any{"request":json.RawMessage(request),"response":json.RawMessage(raw)})
    var reply struct {Result json.RawMessage `json:"result"`; Error *struct {Data model.ErrorData `json:"data"`} `json:"error"`};b16Decode(f.t,raw,&reply)
    if code!="" {if reply.Error==nil || string(reply.Error.Data.Code)!=code || len(reply.Result)!=0 {f.t.Fatalf("%s expected %s: %s",method,code,raw)};return nil}
    if reply.Error!=nil {f.t.Fatalf("%s refused: %s",method,raw)};return reply.Result
}
func b16Ref(id string) map[string]any {return map[string]any{"id":id}}
func (f *b16Host) create(title string, labels []string) string {
    var commit model.Commit;b16Decode(f.t,f.call("bead.create",map[string]any{"type":"issue","fields":map[string]any{"title":title,"labels":labels}},true,""),&commit)
    if len(commit.Results)!=1 {f.t.Fatal("create results")};return commit.Results[0].Ref.ID
}
func (f *b16Host) get(id string) (json.RawMessage,map[string]any) {
    raw:=f.call("bead.get",map[string]any{"id":b16Ref(id)},false,"");var result struct {Bead map[string]any `json:"bead"`};b16Decode(f.t,raw,&result);return raw,result.Bead
}
func b16Strings(t *testing.T, value any) []string {
    t.Helper(); out:=[]string{};if value==nil{return out};rows,ok:=value.([]any);if !ok{t.Fatalf("non-array labels %#v",value)};for _,raw:=range rows{s,ok:=raw.(string);if !ok{t.Fatal("non-string label")};out=append(out,s)};return out
}
func (f *b16Host) labels(id string, want []string) {
    f.t.Helper();_,bead:=f.get(id); b16Equal(f.t,b16Strings(f.t,bead["labels"]),want)
    fields,ok:=bead["fields"].(map[string]any);if !ok{f.t.Fatal("missing fields")}
    if bead["plane"]!="ephemeral" {b16Equal(f.t,b16Strings(f.t,fields["labels"]),want)}
}
func (f *b16Host) update(id string, labels []string) model.Commit {
    f.t.Helper();var commit model.Commit;b16Decode(f.t,f.call("bead.update",map[string]any{"id":b16Ref(id),"fields":map[string]any{"labels":labels}},true,""),&commit);return commit
}
func (f *b16Host) query(query string, want []string) {
    f.t.Helper();before:=f.meta();definition:=map[string]any{"filter":map[string]any{"query":query},"sort":[]string{"id"},"fields":[]string{"fields.title"},"budgetTokens":1000}
    var result struct{Items []struct{ID string `json:"id"`} `json:"items"`;NextCursor any `json:"nextCursor"`};b16Decode(f.t,f.call("view.run",map[string]any{"definition":definition},false,""),&result)
    got:=[]string{};for _,item:=range result.Items{got=append(got,item.ID)};expected:=append([]string{},want...);sort.Strings(expected);b16Equal(f.t,got,expected);if result.NextCursor!=nil{f.t.Fatal("unexpected page continuation")}
    var count struct{Count json.Number `json:"count"`};b16Decode(f.t,f.call("view.run",map[string]any{"definition":definition,"countOnly":true},false,""),&count)
    b16Equal(f.t,string(count.Count),fmt.Sprint(len(want)));b16Equal(f.t,f.meta(),before)
}
// Active membership uses the admitted structured ready filter. Ordinary view
// counts remain a separate qualification; this does not invent active counts.
func (f *b16Host) readyLabels(filter map[string]any, want []string) {
    f.t.Helper(); before:=f.meta()
    var result struct {Items []struct{ID string `json:"id"`} `json:"items"`; NextCursor any `json:"nextCursor"`}
    b16Decode(f.t,f.call("bead.ready",map[string]any{"filter":filter,"limit":1000},false,""),&result)
    got:=[]string{};for _,item:=range result.Items{got=append(got,item.ID)};sort.Strings(got)
    expected:=append([]string{},want...);sort.Strings(expected);b16Equal(f.t,got,expected)
    if result.NextCursor!=nil {f.t.Fatal("bounded ready label set unexpectedly paged")}
    paged:=[]string{};seen:=map[string]bool{};var cursor any
    for page:=0; ;page++ {
        if page>len(want)+1 {f.t.Fatal("ready label continuation did not terminate")}
        params:=map[string]any{"filter":filter,"limit":2};if cursor!=nil {params["cursor"]=cursor}
        var next struct {Items []struct{ID string `json:"id"`} `json:"items"`; NextCursor any `json:"nextCursor"`}
        b16Decode(f.t,f.call("bead.ready",params,false,""),&next)
        if len(next.Items)>2 {f.t.Fatal("ready label page exceeds requested bound")}
        for _,item:=range next.Items {if seen[item.ID] {f.t.Fatalf("repeated filtered ready item %q",item.ID)};seen[item.ID]=true;paged=append(paged,item.ID)}
        cursor=next.NextCursor;if cursor==nil {break}
        if _,ok:=cursor.(string);!ok {f.t.Fatal("ready label cursor is not a string")}
    }
    sort.Strings(paged);b16Equal(f.t,paged,expected);b16Equal(f.t,f.meta(),before)
}

func (f *b16Host) reopen(check func()) {
    f.t.Helper();if f.engine=="memory"{check();return};before:=f.meta();f.close();f.open();b16Equal(f.t,f.meta(),before);check();f.t.Logf("actual %s SQL close/reopen exact label readback",f.engine)
}
