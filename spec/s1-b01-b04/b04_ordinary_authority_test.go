package s1b01b04

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// The identities, grants and caveats stay fixed for the entire fixture.
// Authentication uses the actual API-key provider; authorization uses the
// installed fixed chain and its production Writer/read adapters.
type ordinaryB04Authority struct {
	reader, writer model.PrincipalRef
	provider *authnapikey.Provider
	readKey, writeKey string
}

func ordinaryAuthority(t *testing.T, refs []model.Ref, now func() time.Time) *ordinaryB04Authority {
	t.Helper()
	reader:=model.PrincipalRef{Kind:"user",ID:"user:b04-reader",Caveats:map[string]any{"refs":refs}}
	sponsor:=model.PrincipalRef{Kind:"user",ID:"user:b04-sponsor"}
	writer:=model.PrincipalRef{Kind:"agent",ID:"agent:b04-static",Sponsor:&sponsor,Caveats:map[string]any{"refs":refs}}
	for _,p:=range []*model.PrincipalRef{&reader,&writer} {data,err:=json.Marshal(p);if err!=nil {t.Fatal(err)};if err:=json.Unmarshal(data,p);err!=nil {t.Fatal(err)}}
	provider:=authnapikey.New(authnapikey.Config{AuthorizationInstalled:func()bool{return true},Now:now})
	r,err:=provider.Issue(reader,now().Add(time.Hour));if err!=nil {t.Fatal(err)}
	w,err:=provider.Issue(writer,now().Add(time.Hour));if err!=nil {t.Fatal(err)}
	return &ordinaryB04Authority{reader:reader,writer:writer,provider:provider,readKey:r.Token,writeKey:w.Token}
}

func (a *ordinaryB04Authority) bind(o *ordinaryB04) (rpc.Service, authn.Config) {
	t,f:=o.f.t,o.f
	meta:=f.metaSnapshot()
	ns:=authz.Namespace{StoreID:string(meta.StoreID),Incarnation:meta.Incarnation,InstallationID:"b04-static",Authority:ref("b04-authority")}
	projectRef:=ref("b04-project")
	facts:=authzfixed.Facts{Namespace:ns,Projects:[]authzfixed.Project{{Ref:projectRef,InstallationID:ns.InstallationID,Name:project,Active:true}},Grants:[]authz.Grant{{Subject:authz.SubjectRef{Kind:"user",ID:a.writer.Sponsor.ID},Scope:authz.Scope{Kind:authz.ProjectScope,Ref:projectRef},Role:"involved"}}}
	checker,err:=authzrpc.NewChecker(context.Background(),f.store,f.lease,f.ontology,authzrpc.AuthorityConfig{Namespace:ns,Facts:authzrpc.StaticFacts{Facts:facts}});if err!=nil {t.Fatal(err)}
	f.writer=pipeline.New(f.store,f.lease,pipeline.WithClock(f.clock.Now),pipeline.WithOntology(f.ontology),pipeline.WithAuthorizer(checker),pipeline.WithEventContext(checker))
	f.svc=service.New(f.store,f.lease,f.writer,service.WithClock(f.clock.Now),service.WithOntology(f.ontology),service.WithAuthorizationInstalled(func()bool{return true}))
	o.retain("static-authority",map[string]any{"namespace":ns,"facts":facts,"reader":a.reader,"writer":a.writer,"policy":"existing fixed chain; immutable grants and caveats"})
	return checker.GuardService(f.svc),authn.Config{Provider:a.provider,AuthorizationInstalled:func()bool{return true}}
}

func TestB04OrdinarySelectionUnderStaticAuthority(t *testing.T) {
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {t.Run(backend,func(t *testing.T){
		dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
		ids:=[]string{"b04-hidden-match","b04-visible-winner","b04-visible-mismatch"}
		o:=newOrdinaryB04(t,backend,dsn,ids)
		o.seed("static-seed",createOp("hidden",project,map[string]any{"title":"hidden matching high rank","priority":0,"labels":[]string{"public-selection"}},""),createOp("winner",project,map[string]any{"title":"visible matching winner","priority":2,"labels":[]string{"public-selection"}},""),createOp("mismatch",project,map[string]any{"title":"visible filter mismatch","priority":1,"labels":[]string{"outside"}},""))
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
