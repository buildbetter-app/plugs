package cardb20

import (
    "encoding/json"
    "testing"

    "github.com/buildbetter-app/plugs/internal/server/rpc"
)

func TestB16PublicNativeCounterpartDurableLabels(t *testing.T) {
    for _,engine:=range []string{"memory","sqlite","postgres"}{t.Run(engine,func(t *testing.T){b16DurableVector(t,engine)})}
}

func TestB16PublicActiveLabelNoopAndTwoPlaneRename(t *testing.T) {
    for _,engine:=range []string{"memory","sqlite","postgres"}{t.Run(engine,func(t *testing.T){
        f:=b16NewHost(t,engine)
        request:=map[string]any{"template":"b16-labels","project":b16Project,"mode":"ephemeral","checklist":[]map[string]any{
            {"label":"rename","title":"active rename","type":"issue","labels":[]string{"old","side","old",""}},
            {"label":"merge","title":"active merge","type":"issue","labels":[]string{"old","new"}},
        }}
        if engine=="memory" {
            before:=f.meta();f.call("x.workflow.invoke",request,true,"plugin_durable_state_unavailable");b16Equal(t,f.meta(),before)
            t.Log("literal memory persistent-workflow capability refusal; no two-plane success credit")
            return
        }
        var created struct{Items []struct{ID string `json:"id"`} `json:"items"`};b16Decode(t,f.call("x.workflow.invoke",request,true,""),&created)
        if len(created.Items)!=2 {t.Fatal("expected two actual active items")};a,b:=created.Items[0].ID,created.Items[1].ID
        _,av:=f.get(a);_,bv:=f.get(b);b16Equal(t,b16Strings(t,av["labels"]),[]string{"old","side"});b16Equal(t,b16Strings(t,bv["labels"]),[]string{"new","old"})
        t.Run("equal_label_set_preserves_item_revision",func(t *testing.T){
            prior:=f.t;f.t=t;defer func(){f.t=prior}();_,before:=f.get(a);f.update(a,[]string{"side","old","old",""});_,after:=f.get(a);b16Equal(t,after["revision"],before["revision"])
        })
        durable:=f.create("durable rename",[]string{"old","new"})
        t.Run("actual_public_mixed_rename",func(t *testing.T){
            prior:=f.t;f.t=t;defer func(){f.t=prior}();inputs:=[]json.RawMessage{}
            for _,id:=range []string{a,b,durable}{raw,_:=f.get(id);inputs=append(inputs,raw)}
            plan,err:=rpc.PlanLabelRenameFromResults(b16Project,"old","new",inputs);if err!=nil{t.Fatalf("actual public active projections must support the B16 migration fold: %v",err)}
            b16Equal(t,plan.Renamed,3);b16Equal(t,plan.Merged,2)
            f.call("tx.apply",map[string]any{"changeSet":plan.ChangeSet},true,"")
            check:=func(){f.labels(a,[]string{"new","side"});f.labels(b,[]string{"new"});f.labels(durable,[]string{"new"})};check();f.reopen(check)
        })
    })}
}
