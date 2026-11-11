package cardb20

import (
    "encoding/json"
    "path/filepath"
    "os"
    "strings"
    "testing"

    "github.com/buildbetter-app/plugs/internal/core/model"
    "github.com/buildbetter-app/plugs/internal/server/rpc"
)

// Literal expectations are authored independently of either implementation.
// Native CLI vectors avoid outer whitespace, which its documented argv parser
// trims. Plug exact outer-space behavior remains in the retained label tests.
var b16Initial = []string{"A","a","internal space","z","é","東京"}
var b16FinalPatch = []string{"added","replace"}

type b16Outcome struct {
    Initial []string `json:"initial"`
    Patched []string `json:"patched"`
    Renamed int `json:"renamed"`
    Merged int `json:"merged"`
    Final map[string][]string `json:"final"`
}

func b16DurableVector(t *testing.T, engine string) b16Outcome {
    t.Helper();f:=b16NewHost(t,engine)
    id:=f.create("state",[]string{"z","a","A","internal space","東京","é","a",""})
    f.labels(id,b16Initial)
    _,initialRead:=f.get(id);actualInitial:=b16Strings(t,initialRead["labels"])
    before:=f.meta();_,initial:=f.get(id)
    unchanged:=f.update(id,[]string{"z","a","A","internal space","東京","é","a",""})
    b16Equal(t,f.meta(),before);b16Equal(t,unchanged.Revision.String(),string(initial["revision"].(json.Number)))
    f.update(id,[]string{"A","a","internal space","z","é","東京","added"})
    f.labels(id,[]string{"A","a","added","internal space","z","é","東京"})
    f.update(id,b16Initial);before=f.meta();f.update(id,b16Initial);b16Equal(t,f.meta(),before)
    // The public fold is three ordered whole-field patches, exactly the
    // native replacement -> addition -> removal semantics; remove wins.
    ops:=[]any{}
    for _,labels:=range [][]string{{"replace","both","", "replace"},{"replace","both","added","added",""},b16FinalPatch}{ops=append(ops,map[string]any{"kind":"patch","target":b16Ref(id),"fields":map[string]any{"labels":labels}})}
    f.call("tx.apply",map[string]any{"changeSet":map[string]any{"ops":ops}},true,"")
    f.labels(id,b16FinalPatch)
    max:=strings.Repeat("界",255);bound:=f.create("bounded",[]string{max});f.labels(bound,[]string{max})
    before=f.meta();f.call("bead.update",map[string]any{"id":b16Ref(bound),"fields":map[string]any{"labels":[]string{"lost",strings.Repeat("界",256)}}},true,"invalid_params");b16Equal(t,f.meta(),before);f.labels(bound,[]string{max})
    // Ordinary label state and unrelated scalar/lifecycle writes stay coupled.
    f.call("bead.update",map[string]any{"id":b16Ref(id),"fields":map[string]any{"description":"preserved"}},true,"")
    f.call("bead.close",map[string]any{"id":b16Ref(id)},true,"");f.call("bead.reopen",map[string]any{"id":b16Ref(id)},true,"");f.labels(id,b16FinalPatch)
    a:=f.create("rename",[]string{"Old","old","side"});b:=f.create("merge",[]string{"old","new"});c:=f.create("destination",[]string{"new"});empty:=f.create("empty",[]string{})
    f.query("label=old",[]string{a,b});f.query("label=Old",[]string{a});f.query("label=old AND label=new",[]string{b});f.query("label=old OR label=new",[]string{a,b,c});f.query("NOT label=old AND NOT label=added AND NOT label="+max,[]string{c,empty})
    inputs:=[]json.RawMessage{};for _,target:=range []string{a,b,c,empty}{raw,_:=f.get(target);inputs=append(inputs,raw)}
    for _,pair:=range [][2]string{{"old"," old "},{"old",strings.Repeat("界",256)}}{
        before=f.meta();plan,err:=rpc.PlanLabelRenameFromResults(b16Project,pair[0],pair[1],inputs);if err==nil || len(plan.ChangeSet.Ops)!=0 || plan.Renamed!=0 || plan.Merged!=0 {t.Fatal("invalid rename produced a usable plan")};b16Equal(t,f.meta(),before)
    }
    plan,err:=rpc.PlanLabelRenameFromResults(b16Project,"old","new",inputs);if err!=nil{t.Fatal(err)}
    b16Equal(t,plan.Renamed,2);b16Equal(t,plan.Merged,1)
    var commit model.Commit;b16Decode(t,f.call("tx.apply",map[string]any{"changeSet":plan.ChangeSet},true,""),&commit)
    if len(commit.Results)!=2 {t.Fatalf("successful rename result cardinality %d",len(commit.Results))}
    final:=map[string][]string{"rename":{"Old","new","side"},"merge":{"new"},"destination":{"new"},"empty":{}}
    check:=func(){f.labels(a,final["rename"]);f.labels(b,final["merge"]);f.labels(c,final["destination"]);f.labels(empty,final["empty"]);f.labels(id,b16FinalPatch);f.labels(bound,[]string{max});f.query("label=old",[]string{});f.query("label=new",[]string{a,b,c});f.query("label=Old",[]string{a})}
    check();f.reopen(check)
    current:=[]json.RawMessage{};for _,target:=range []string{a,b,c,empty}{raw,_:=f.get(target);current=append(current,raw)}
    absent,err:=rpc.PlanLabelRenameFromResults(b16Project,"absent","new",current);if err!=nil{t.Fatal(err)};b16Equal(t,absent.Renamed,0);b16Equal(t,absent.Merged,0);b16Equal(t,len(absent.ChangeSet.Ops),0)
    actualFinal:=map[string][]string{};for name,target:=range map[string]string{"rename":a,"merge":b,"destination":c,"empty":empty}{_,value:=f.get(target);actualFinal[name]=b16Strings(t,value["labels"])}
    _,patched:=f.get(id)
    outcome:=b16Outcome{Initial:actualInitial,Patched:b16Strings(t,patched["labels"]),Renamed:plan.Renamed,Merged:plan.Merged,Final:actualFinal}
    if root:=os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR");root!=""{b16Write(t,filepath.Join(root,"b16-"+engine+"-durable-outcome.json"),outcome)}
    return outcome
}
