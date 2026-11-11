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
