package s1b01b04

import (
    "context"
    "fmt"
    "reflect"
    "testing"
    "time"

    "github.com/buildbetter-app/plugs/internal/core/model"
    "github.com/buildbetter-app/plugs/internal/core/ontology"
)

func TestB04OrdinaryPublicHTTPAcrossStores(t *testing.T) {
	h := newPostgresHarness(t)
	for _, backend := range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:=""; if backend=="postgres" { dsn=h.dsn(t) }
			o:=newOrdinaryB04(t,backend,dsn,[]string{"b04-decoy","b04-direct","b04-active","b04-wip"})
			o.empty("never-populated",nil)
			o.seed("create-public",ordinaryCreateOp("decoy",project,map[string]any{"title":"unselected decoy","priority":0},""),ordinaryCreateOp("direct",project,map[string]any{"title":"ordinary hydrated row","description":"preserve description","labels":[]string{"ordinary","public"},"priority":2},""),ordinaryCreateOp("active",project,map[string]any{"title":"native category active","status":"awaiting_review","priority":3},""),ordinaryCreateOp("wip",project,map[string]any{"title":"native category wip","status":"qa_testing","priority":1},""))
			o.commit("public-comment","bead.comment",map[string]any{"id":ref("b04-direct"),"body":"hydration retains this comment"})
			o.reopen("before-claim")
			o.win("direct","bead.claim","b04-direct","todo",map[string]any{"id":ref("b04-direct")})
			o.f.clock.Advance(time.Second)
			o.sameHolder("same-holder","b04-direct")
			o.refusal("wip-refused","bead.claim",owner,"invalid_params",false,map[string]any{"id":ref("b04-wip")})
			for i,p:= range []map[string]any{{"limit":1},{"offset":1},{"brief":true}} { o.refusal(fmt.Sprintf("validation-%d",i),"bead.claimNext",owner,"invalid_params",false,p) }
			o.refusal("missing-actor","bead.claimNext","","invalid_params",false,nil)
			o.win("active-next","bead.claimNext","b04-active","awaiting_review",map[string]any{"filter":map[string]any{"resources":[]string{"b04-active","b04-wip"}}})
			o.empty("empty-selected",map[string]any{"filter":map[string]any{"resources":[]string{"b04-active","b04-wip"}}})
			if o.f.document("b04-decoy").Lease!=nil { t.Fatal("selected empty front claimed decoy") }
			o.reopen("after-claim-noop-refusal-empty")
			// Exercise public reads through the reconstructed HTTP service too.
			o.publicReadback("reopened-get","b04-direct")
		})
	}
}

func TestB04OrdinaryPublicParentPriorityAndQueryFilters(t *testing.T) {
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
			o:=newOrdinaryB04(t,backend,dsn,[]string{"b04-root","b04-outside"})
			bug:=ordinaryCreateOp("type-decoy",project,map[string]any{"title":"ordinary wrong type","priority":2},"b04-root");bug.Create.Type="bug"
			o.seed("filter-create",ordinaryCreateOp("root",project,map[string]any{"title":"ordinary root","priority":0},""),ordinaryCreateOp("child",project,map[string]any{"title":"ordinary selected child","priority":2},"b04-root"),ordinaryCreateOp("grandchild",project,map[string]any{"title":"ordinary selected grandchild","priority":2},"b04-root.1"),ordinaryCreateOp("outside",project,map[string]any{"title":"ordinary outside","priority":2},""),ordinaryCreateOp("priority-decoy",project,map[string]any{"title":"ordinary wrong priority","priority":1},"b04-root"),bug)
			o.reopen("filters-before")
			for i,id:=range []string{"b04-root.1","b04-root.1.1"} {o.win(fmt.Sprintf("descendant-%d",i),"bead.claimNext",id,"todo",map[string]any{"filter":map[string]any{"parent":"b04-root","priority":2,"types":[]string{"issue"},"resources":[]string{"b04-root","b04-root.1","b04-root.1.1","b04-outside","b04-root.2","b04-root.3"}}})}
			o.empty("descendants-drained",map[string]any{"filter":map[string]any{"parent":"b04-root","priority":2,"types":[]string{"issue"}}})
			if o.f.document("b04-root").Lease!=nil || o.f.document("b04-outside").Lease!=nil || o.f.document("b04-root.2").Lease!=nil || o.f.document("b04-root.3").Lease!=nil {t.Fatal("parent/priority/type intersection leaked")}
			for i,p:=range []map[string]any{{"filter":map[string]any{"query":"type=issue"}},{"filter":map[string]any{"events":[]string{"bead.updated"}}},{"viewId":"uninstalled-view"},{"filter":map[string]any{"viewId":"uninstalled-view"}}} {o.refusal(fmt.Sprintf("unsupported-selection-%d",i),"bead.claimNext",owner,"invalid_params",false,p)}
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
			ids:=make([]string,0,2*prefix+2);ops:=make([]model.Op,0,2*prefix+2)
			for i:=0;i<prefix;i++ { id:=fmt.Sprintf("b04-skip-%04d",i);ids=append(ids,id);ops=append(ops,ordinaryCreateOp(id,project,map[string]any{"title":id,"priority":0},"")) }
			ids=append(ids,"b04-walkwinner","b04-walkdecoy")
			ops=append(ops,ordinaryCreateOp("winner",project,map[string]any{"title":"winner after full front","priority":3},""),ordinaryCreateOp("decoy",project,map[string]any{"title":"excluded by resource filter","priority":1},""))
			for i:=0;i<prefix;i++ {id:=fmt.Sprintf("b04-guard-child-%04d",i);ids=append(ids,id);ops=append(ops,ordinaryCreateOp(id,project,map[string]any{"title":id,"priority":4},ids[i]))}
			o:=newOrdinaryB04(t,backend,dsn,ids,func(doc *ontology.Document){w:=doc.Workflows["issue"];w.Transitions[0].Guards=append(w.Transitions[0].Guards,ontology.Guard{Kind:ontology.NoOpenChildren});doc.Workflows["issue"]=w})
			for start:=0;start<len(ops);start+=50 { if err:=o.f.lease.Renew(context.Background());err!=nil {t.Fatal(err)};o.seed(fmt.Sprintf("public-seed-%04d",start),ops[start:min(start+50,len(ops))]...) }
			if err:=o.f.lease.Renew(context.Background());err!=nil {t.Fatal(err)}
			o.reopen("full-front-before")
			filter:=map[string]any{"types":[]string{"issue"},"resources":ids[:prefix+1]}
			if got:=o.readyIDs("full-front-precondition",filter);!reflect.DeepEqual(got,ids[:prefix+1]) {t.Fatalf("actual public ready front differs: got=%v want=%v",got,ids[:prefix+1])}
			o.refusal("prefix-guard","bead.claim",owner,"ontology_guard_failed",false,map[string]any{"id":ref(ids[0])})
			o.win("walk","bead.claimNext","b04-walkwinner","todo",map[string]any{"filter":map[string]any{"types":[]string{"issue"},"resources":ids[:prefix+1]}})
			o.reopen("full-front-after")
			t.Logf("B04_ORDINARY_FULL_WALK backend=%s stored=%d ineligible_prefix=%d public_seed=true",backend,len(ids),prefix)
		})
	}
}

func TestB04OrdinaryAdditiveSelectionAcrossStores(t *testing.T) {
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {t.Run(backend,func(t *testing.T){
		for _,v:=range ordinaryB04SelectionCases() {t.Run(v.Name,func(t *testing.T){
			dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
			ids:=[]string{ordinarySelectionID(v.Name,"root")}
			o:=newOrdinaryB04(t,backend,dsn,ids);filter:=o.selectionFixture(v);o.reopen("selection-before")
			want:=[]string{};for _,role:=range v.Ready {want=append(want,ordinarySelectionID(v.Name,role))}
			if got:=o.readyIDs("selection-front",filter);!reflect.DeepEqual(got,want) {t.Fatalf("public selector membership=%v want=%v",got,want)}
			listed:=[]string{};for _,role:=range v.Listed {listed=append(listed,ordinarySelectionID(v.Name,role))};o.selectionListingControl(filter,listed)
			for i,id:=range want {o.win(fmt.Sprintf("selection-win-%d",i),"bead.claimNext",id,"todo",map[string]any{"filter":filter})}
			o.empty("selection-drained",map[string]any{"filter":filter});o.reopen("selection-after")
		})}
	})}
}

func TestB04OrdinarySelectionUnderStaticAuthority(t *testing.T) {
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {t.Run(backend,func(t *testing.T){
		dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
		ids:=[]string{"b04-hidden-match","b04-visible-winner","b04-visible-mismatch"}
		o:=newOrdinaryB04(t,backend,dsn,ids)
		o.seed("static-seed",ordinaryCreateOp("hidden",project,map[string]any{"title":"hidden matching high rank","priority":0,"labels":[]string{"public-selection"}},""),ordinaryCreateOp("winner",project,map[string]any{"title":"visible matching winner","priority":2,"labels":[]string{"public-selection"}},""),ordinaryCreateOp("mismatch",project,map[string]any{"title":"visible filter mismatch","priority":1,"labels":[]string{"outside"}},""))
		o.server.Close()
		o.authority=ordinaryAuthority(t,[]model.Ref{ref(ids[1]),ref(ids[2])},o.f.clock.Now)
		o.install()
		o.reopen("static-before")
		filter:=map[string]any{"labelsAny":[]string{"public-selection"}}
		before:=o.state("static-ready-before")
		page:=result(t,o.call("static-limit-one","bead.ready",owner,map[string]any{"filter":filter,"limit":1}))
		items,ok:=page["items"].([]any);if !ok || len(items)!=1 {t.Fatalf("authorized matching limit-one page: %+v",page)}
		row,ok:=items[0].(map[string]any);if !ok || row["id"]!=ids[1] || page["nextCursor"]!=nil {t.Fatalf("authorization/filter membership must precede pagination: %+v",page)}
		o.equalState("static-ready-after",before);o.checkReceipts()
		o.win("static-win","bead.claimNext",ids[1],"todo",map[string]any{"filter":filter})
		o.empty("static-empty",map[string]any{"filter":filter})
		o.reopen("static-after")
		if got:=o.readyIDs("static-drained",filter);len(got)!=0 {t.Fatalf("hidden/mismatch row escaped selection: %v",got)}
		o.checkReceipts()
	})}
}
