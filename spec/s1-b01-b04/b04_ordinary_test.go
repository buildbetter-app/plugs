package s1b01b04

import (
    "context"
    "fmt"
    "reflect"
    "testing"
    "time"

    "github.com/buildbetter-app/plugs/internal/core/model"
)

func TestB04OrdinaryPublicHTTPAcrossStores(t *testing.T) {
	h := newPostgresHarness(t)
	for _, backend := range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:=""; if backend=="postgres" { dsn=h.dsn(t) }
			o:=newOrdinaryB04(t,backend,dsn,[]string{"b04-decoy","b04-direct","b04-active","b04-wip"})
			o.seed("create-public",createOp("decoy",project,map[string]any{"title":"unselected decoy","priority":0},""),createOp("direct",project,map[string]any{"title":"ordinary hydrated row","description":"preserve description","labels":[]string{"ordinary","public"},"priority":2},""),createOp("active",project,map[string]any{"title":"native category active","status":"awaiting_review","priority":3},""),createOp("wip",project,map[string]any{"title":"native category wip","status":"qa_testing","priority":1},""))
			o.commit("public-comment","bead.comment",map[string]any{"id":ref("b04-direct"),"body":"hydration retains this comment"})
			o.reopen("before-claim")
			o.win("direct","bead.claim","b04-direct","todo",map[string]any{"id":ref("b04-direct")})
			before:=o.state("same-holder-before")
			original:=o.f.document("b04-direct")
			o.f.clock.Advance(time.Second)
			o.commit("same-holder","bead.claim",map[string]any{"id":ref("b04-direct")})
			o.equalState("same-holder-after",before)
			if !reflect.DeepEqual(o.f.document("b04-direct"),original) { t.Fatal("same holder changed lease/start/row identity") }
			o.refusal("wip-refused","bead.claim",owner,"invalid_params",false,map[string]any{"id":ref("b04-wip")})
			for i,p:= range []map[string]any{{"limit":1},{"offset":1},{"brief":true}} { o.refusal(fmt.Sprintf("validation-%d",i),"bead.claimNext",owner,"invalid_params",false,p) }
			o.refusal("missing-actor","bead.claimNext","","invalid_params",false,nil)
			o.win("active-next","bead.claimNext","b04-active","awaiting_review",map[string]any{"filter":map[string]any{"resources":[]string{"b04-active","b04-wip"}}})
			o.empty("empty-selected",map[string]any{"filter":map[string]any{"resources":[]string{"b04-active","b04-wip"}}})
			if o.f.document("b04-decoy").Lease!=nil { t.Fatal("selected empty front claimed decoy") }
			o.reopen("after-claim-noop-refusal-empty")
			// Exercise public reads through the reconstructed HTTP service too.
			result(t,o.call("reopened-get","bead.get",owner,map[string]any{"id":ref("b04-direct")}))
		})
	}
}

func TestB04OrdinaryPublicParentPriorityAndQueryFilters(t *testing.T) {
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
			o:=newOrdinaryB04(t,backend,dsn,[]string{"b04-root","b04-child","b04-grandchild","b04-outside","b04-priority-decoy"})
			o.seed("filter-create",createOp("root",project,map[string]any{"title":"ordinary root","priority":0},""),createOp("child",project,map[string]any{"title":"ordinary selected child","priority":2},"b04-root"),createOp("grandchild",project,map[string]any{"title":"ordinary selected grandchild","priority":2},"b04-child"),createOp("outside",project,map[string]any{"title":"ordinary outside","priority":2},""),createOp("priority-decoy",project,map[string]any{"title":"ordinary wrong priority","priority":1},"b04-root"))
			o.reopen("filters-before")
			for i,id:=range []string{"b04-child","b04-grandchild"} {o.win(fmt.Sprintf("descendant-%d",i),"bead.claimNext",id,"todo",map[string]any{"filter":map[string]any{"parent":"b04-root","priority":2,"types":[]string{"issue"},"resources":[]string{"b04-root","b04-child","b04-grandchild","b04-outside","b04-priority-decoy"}}})}
			o.empty("descendants-drained",map[string]any{"filter":map[string]any{"parent":"b04-root","priority":2}})
			if o.f.document("b04-root").Lease!=nil || o.f.document("b04-outside").Lease!=nil || o.f.document("b04-priority-decoy").Lease!=nil {t.Fatal("parent/priority intersection leaked")}
			o.win("query-outside","bead.claimNext","b04-outside","todo",map[string]any{"filter":map[string]any{"query":"ordinary outside","priority":2}})
			o.reopen("filters-after")
		})
	}
}

func TestB04OrdinaryPublicWalkBeyondOneThousand(t *testing.T) {
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:=""; if backend=="postgres" { dsn=h.dsn(t) }
			const prefix=1001
			ids:=make([]string,0,prefix+2);ops:=make([]model.Op,0,prefix+2)
			for i:=0;i<prefix;i++ { id:=fmt.Sprintf("b04-skip-%04d",i);ids=append(ids,id);ops=append(ops,createOp(id,project,map[string]any{"title":id,"priority":0,"template":true},"")) }
			ids=append(ids,"b04-walkwinner","b04-walkdecoy")
			ops=append(ops,createOp("winner",project,map[string]any{"title":"winner after full front","priority":3},""),createOp("decoy",project,map[string]any{"title":"excluded by resource filter","priority":1},""))
			o:=newOrdinaryB04(t,backend,dsn,ids)
			for start:=0;start<len(ops);start+=50 { if err:=o.f.lease.Renew(context.Background());err!=nil {t.Fatal(err)};o.seed(fmt.Sprintf("public-seed-%04d",start),ops[start:min(start+50,len(ops))]...) }
			if err:=o.f.lease.Renew(context.Background());err!=nil {t.Fatal(err)}
			o.reopen("full-front-before")
			o.win("walk","bead.claimNext","b04-walkwinner","todo",map[string]any{"filter":map[string]any{"types":[]string{"issue"},"resources":ids[:prefix+1]}})
			o.reopen("full-front-after")
			t.Logf("B04_ORDINARY_FULL_WALK backend=%s stored=%d ineligible_prefix=%d public_seed=true",backend,len(ids),prefix)
		})
	}
}
