package cardb20

import (
    "encoding/json"
    "errors"
    "sort"
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
    renameBefore:=map[string][]string{}
    inputs:=[]json.RawMessage{};for _,target:=range []string{a,b,c,empty}{raw,value:=f.get(target);inputs=append(inputs,raw);if target==a || target==b {renameBefore[target]=b16Strings(t,value["labels"])}}
    b16Equal(t,renameBefore[a],[]string{"Old","old","side"});b16Equal(t,renameBefore[b],[]string{"new","old"})
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
    observed:=b16ObservedRename(t,commit,renameBefore,map[string][]string{a:actualFinal["rename"],b:actualFinal["merge"]})
    b16Equal(t,observed,b16Counts{Renamed:2,Merged:1})
    outcome:=b16Outcome{Initial:actualInitial,Patched:b16Strings(t,patched["labels"]),Renamed:observed.Renamed,Merged:observed.Merged,Final:actualFinal}
    if root:=os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR");root!=""{b16Write(t,filepath.Join(root,"b16-"+engine+"-durable-outcome.json"),outcome)}
    return outcome
}

// Counts come from successful results and changed committed rows. Literal
// expected totals are asserted separately; a proposed plan earns no write count.
type b16Counts struct {
	Renamed int `json:"renamed"`
	Merged int `json:"merged"`
}

type b16MixedOutcome struct {
	Counts b16Counts `json:"counts"`
	Final map[string][]string `json:"final"`
}

func b16Has(labels []string, label string) bool {
	for _, candidate := range labels { if candidate == label { return true } }
	return false
}

func b16ObservedRename(t *testing.T, commit model.Commit, before, after map[string][]string) b16Counts {
	t.Helper()
	seen := map[string]bool{}
	counts := b16Counts{}
	for _, result := range commit.Results {
		id := result.Ref.ID
		old, known := before[id]
		if !known || seen[id] { t.Fatalf("unexpected committed rename result %q", id) }
		seen[id] = true
		next, present := after[id]
		if !present || !b16Has(old,"old") || b16Has(next,"old") || !b16Has(next,"new") { t.Fatalf("rename result lacks exact old/new transition %q",id) }
		counts.Renamed++
		if b16Has(old,"new") { counts.Merged++ }
	}
	b16Equal(t,len(seen),len(before))
	return counts
}

func b16ActiveVector(t *testing.T, engine string) b16MixedOutcome {
	t.Helper()
	f := b16NewHost(t,engine)
	request := map[string]any{"template":"b16-labels", "project":b16Project, "mode":"ephemeral", "checklist":[]map[string]any{
		{"label":"rename", "title":"active rename", "type":"issue", "fields":map[string]any{"marker":"preserved"}, "labels":[]string{"z","a","A","internal space","東京","é","a",""}},
		{"label":"merge", "title":"active merge", "type":"issue", "labels":[]string{"old","new"}},
		{"label":"destination", "title":"active destination", "type":"issue", "labels":[]string{"new"}},
		{"label":"empty", "title":"active empty", "type":"issue", "labels":[]string{"",""}},
	}}
	var created struct { Items []struct { ID string `json:"id"` } `json:"items"` }
	b16Decode(t,f.call("x.workflow.invoke",request,true,""),&created)
	b16Equal(t,len(created.Items),4)
	a,b,c,e := created.Items[0].ID,created.Items[1].ID,created.Items[2].ID,created.Items[3].ID
	f.labels(a,b16Initial)
	f.labels(b,[]string{"new","old"}); f.labels(c,[]string{"new"}); f.labels(e,[]string{})
	_,original := f.get(a)
	preservedFields := original["fields"]
	b16Equal(t,preservedFields.(map[string]any)["marker"],"preserved")

	t.Run("sorted_bounded_duplicate_empty_and_idempotent",func(t *testing.T) {
		prior:=f.t; f.t=t; defer func(){f.t=prior}()
		_,before:=f.get(a)
		proposal:=[]string{"z","a","A","internal space","東京","é","a",""}
		first:=f.update(a,proposal); key:=f.lastKey
		_,after:=f.get(a); b16Equal(t,after,before)
		f.update(a,[]string{"A","a","added","internal space","z","é","東京"})
		f.labels(a,[]string{"A","a","added","internal space","z","é","東京"})
		_,added:=f.get(a); f.update(a,[]string{"added","A","a","added","internal space","z","é","東京",""}); _,equalAdd:=f.get(a); b16Equal(t,equalAdd,added)
		f.update(a,b16Initial); _,removed:=f.get(a); f.update(a,b16Initial); _,equalRemove:=f.get(a); b16Equal(t,equalRemove,removed)
		// Replaying the earlier equal-set result must retain intervening state.
		f.update(a,[]string{"fresh","old","side"})
		var replay model.Commit
		b16Decode(t,f.request("bead.update",map[string]any{"id":b16Ref(a),"fields":map[string]any{"labels":proposal}},true,"",key),&replay)
		b16Equal(t,replay,first); f.labels(a,[]string{"fresh","old","side"})
		ops:=[]any{}
		for _,labels:=range [][]string{{"replace","both","","replace"},{"replace","both","added","added",""},b16FinalPatch} {
			ops=append(ops,map[string]any{"kind":"patch","target":b16Ref(a),"fields":map[string]any{"labels":labels}})
		}
		f.call("tx.apply",map[string]any{"changeSet":map[string]any{"ops":ops}},true,""); f.labels(a,b16FinalPatch)
		f.update(a,[]string{"",""}); f.labels(a,[]string{})
		_,empty:=f.get(a); f.update(a,[]string{}); _,equalEmpty:=f.get(a); b16Equal(t,equalEmpty,empty)
		max:=strings.Repeat("界",255); f.update(a,[]string{max}); f.labels(a,[]string{max})
		_,bounded:=f.get(a); meta:=f.meta()
		f.call("bead.update",map[string]any{"id":b16Ref(a),"fields":map[string]any{"title":"must not publish","labels":[]string{"lost",strings.Repeat("界",256)}}},true,"invalid_params")
		_,refused:=f.get(a); b16Equal(t,refused,bounded); b16Equal(t,f.meta(),meta)
		f.update(a,[]string{"Red","red"}); f.labels(a,[]string{"Red","red"})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"red"}},[]string{a});f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"Red"}},[]string{a})
		var caseCommit model.Commit
		b16Decode(t,f.call("tx.apply",map[string]any{"changeSet":map[string]any{"ops":[]any{map[string]any{"kind":"patch","target":b16Ref(a),"fields":map[string]any{"labels":[]string{"Red","blue"}}}}}},true,""),&caseCommit)
		b16Equal(t,len(caseCommit.Results),1);b16Equal(t,caseCommit.Results[0].Ref.ID,a);f.labels(a,[]string{"Red","blue"})
		f.update(a,[]string{"Old","old","side"}); f.labels(a,[]string{"Old","old","side"})
		_,value:=f.get(a); b16Equal(t,value["fields"],preservedFields)
	})

	d1:=f.create("rename",[]string{"Old","old","new","side"})
	d2:=f.create("merge",[]string{"old","new"})
	d3:=f.create("destination",[]string{"new"});d4:=f.create("empty",[]string{})
	t.Run("active_and_durable_all_any_exclude_case_membership",func(t *testing.T){
		prior:=f.t;f.t=t;defer func(){f.t=prior}()
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"old"}},[]string{a,b,d1,d2})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"Old"}},[]string{a,d1})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"old","new"}},[]string{b,d1,d2})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labelsAny":[]string{"old","new"}},[]string{a,b,c,d1,d2,d3})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"old"},"excludeLabels":[]string{"side"}},[]string{b,d2})
		f.readyLabels(map[string]any{"includeEphemeral":true,"excludeLabels":[]string{"old","side","added",strings.Repeat("界",255)}},[]string{c,e,d3,d4})
		f.readyLabels(map[string]any{"includeEphemeral":false,"labels":[]string{"old"}},[]string{d1,d2})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labelsAny":[]string{"unknown"}},[]string{})
	})
	selected:=[]string{a,b,d1,d2}
	before:=map[string][]string{}
	beforeBeads:=map[string]map[string]any{}
	activeInputs:=[]json.RawMessage{}
	for _,id:=range selected { raw,value:=f.get(id); before[id]=b16Strings(t,value["labels"]); beforeBeads[id]=value; if id==a || id==b { activeInputs=append(activeInputs,raw) } }
	t.Run("durable_planner_refuses_active_projection",func(t *testing.T) {
		meta:=f.meta()
		plan,err:=rpc.PlanLabelRenameFromResults(b16Project,"old","new",activeInputs)
		var typed *model.Error
		if !errors.As(err,&typed) || typed.Data.Code!="invalid_params" { t.Fatalf("durable planner typed active refusal: %v",err) }
		b16Equal(t,len(plan.ChangeSet.Ops),0); b16Equal(t,plan.Renamed,0); b16Equal(t,plan.Merged,0)
		b16Equal(t,f.meta(),meta)
		for _,id:=range selected { _,value:=f.get(id); b16Equal(t,value,beforeBeads[id]) }
	})

	// commands.md folds rename into an explicitly selected client migration.
	// This uses actual active projections without changing the durable planner.
	cs:=model.ChangeSet{}
	for _,id:=range selected {
		next:=[]string{}
		for _,label:=range before[id] { if label!="old" { next=append(next,label) } }
		if !b16Has(next,"new") { next=append(next,"new") }; sort.Strings(next)
		cs.Ops=append(cs.Ops,model.Op{Kind:model.OpPatch,Patch:&model.PatchOp{Kind:model.OpPatch,Target:model.RefByID(id,model.Natural64(0)),Fields:map[string]any{"labels":next}}})
	}
	var commit model.Commit
	b16Decode(t,f.call("tx.apply",map[string]any{"changeSet":cs},true,""),&commit)
	after:=map[string][]string{}
	for _,id:=range selected { _,value:=f.get(id); after[id]=b16Strings(t,value["labels"]) }
	observed:=b16ObservedRename(t,commit,before,after)
	b16Equal(t,observed,b16Counts{Renamed:4,Merged:3})
	want:=map[string][]string{"active rename":{"Old","new","side"},"active merge":{"new"},"rename":{"Old","new","side"},"merge":{"new"}}
	names:=map[string]string{"active rename":a,"active merge":b,"rename":d1,"merge":d2}
	check:=func() {
		for name,id:=range names { f.labels(id,want[name]) }
		_,value:=f.get(a); b16Equal(t,value["fields"],preservedFields)
		f.labels(c,[]string{"new"});f.labels(e,[]string{});f.labels(d3,[]string{"new"});f.labels(d4,[]string{})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"old"}},[]string{})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"new"}},[]string{a,b,c,d1,d2,d3})
		f.readyLabels(map[string]any{"includeEphemeral":true,"labels":[]string{"Old"}},[]string{a,d1})
		f.readyLabels(map[string]any{"includeEphemeral":false,"labels":[]string{"new"}},[]string{d1,d2,d3})
	}
	check(); f.reopen(check)
	final:=map[string][]string{}
	for name,id:=range names { _,value:=f.get(id); final[name]=b16Strings(t,value["labels"]) }
	outcome:=b16MixedOutcome{Counts:observed,Final:final}
	if root:=os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); root!="" { b16Write(t,filepath.Join(root,"b16-"+engine+"-mixed-outcome.json"),outcome) }
	t.Logf("successful selected public mixed rename readback renamed=%d merged=%d",observed.Renamed,observed.Merged)
	return outcome
}
