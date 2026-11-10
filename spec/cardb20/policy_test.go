package cardb20

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This host adapter calls the installed production fixed checker against a
// fresh real-store snapshot for every historical event. It defines no policy
// rules and makes no authorization decision of its own.
type fixedEventPolicy struct {
	store   driver.Store
	checker *authzrpc.Checker
}

func (p fixedEventPolicy) Read(ctx context.Context, by model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	kind, id, ok := strings.Cut(event.Resource, "/")
	if !ok || kind != "bead" || id == "" {
		return coreevents.Visibility{}, fmt.Errorf("unsupported event resource %q", event.Resource)
	}
	meta, err := p.store.Meta(ctx)
	if err != nil {
		return coreevents.Visibility{}, err
	}
	r, err := p.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return coreevents.Visibility{}, err
	}
	defer r.Close()
	bound, err := p.checker.BindAuthorization(ctx, r, by, authz.Read, authz.Bead, "bead.get")
	if err != nil {
		return coreevents.Visibility{}, err
	}
	allowed, err := bound.Check(ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
	if err != nil {
		return coreevents.Visibility{}, err
	}
	if len(allowed) != 1 {
		return coreevents.Visibility{}, fmt.Errorf("fixed checker omitted decision")
	}
	return coreevents.Visibility{Readable: allowed[0], Before: event.Before, After: event.After}, nil
}

func installEventPolicy(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, installed bool) (coreevents.Authorization, string, *authzrpc.Checker) {
	t.Helper()
	if !installed {
		return coreevents.AllowAll{}, "cardb20-no-policy-plugin", nil
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "b20-fixed", Authority: ref("authority")}
	facts := authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: ref("cardb20"), Name: "cardb20", InstallationID: ns.InstallationID, Active: true}}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "cardb20-reader"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: ref("cardb20")}, Role: "involved"}}}
	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	app, err := authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: keys, Namespace: ns, Facts: authzrpc.StaticFacts{Facts: facts}, DefaultProject: "cardb20"})
	if err != nil {
		t.Fatal(err)
	}
	return fixedEventPolicy{store, app.Checker}, "b20-fixed-installed", app.Checker
}

// Install on the same leased real store after public no-plugin seeding. The
// finite production policy supports sponsored-Agent update and User read;
// fixture setup grants no additional actions.
func installPolicyForExistingRuntime(t *testing.T, r *runtime) {
	t.Helper()
	authorization, _, checker := installEventPolicy(t, r.ctx, r.store, r.lease, true)
	ontology := service.NewOntology(nil)
	writer := pipeline.New(r.store, r.lease, pipeline.WithOntology(ontology), pipeline.WithAuthorizer(checker), pipeline.WithEventContext(checker))
	codec, err := serverevents.NewCursorCodec([]byte(b20CursorKey))
	if err != nil {
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: &coreevents.Reader{Log: serverevents.Log{Store: r.store}, Materializer: serverevents.HistoryMaterializer{Store: r.store}, Authorization: authorization}, Cursors: codec}
	domain := service.New(r.store, r.lease, writer, service.WithOntology(ontology), service.WithAuthorizationInstalled(func() bool { return true }), service.WithEventProvider(eventProvider{events: events}))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := r.store.Meta(r.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.server, err = rpc.New(registry, rpc.Config{DefaultProject: "cardb20", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte(b20CursorKey)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestB20InstalledFixedPolicyDeliversRealAuthorizedEvents(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			f := openStore(t, engine)
			r := newRuntime(t, f.store, false, "b20-1")
			create(t, r, "journal anchor")
			installPolicyForExistingRuntime(t, r)
			before, sets := journal(t, r.store)
			refused := r.call(t, "", "bead.update", map[string]any{"id": map[string]any{"id": "b20-1"}, "fields": map[string]any{"title": "user cannot write"}}, true)
			if refused.Error == nil || refused.Error.Data.Code != "forbidden" {
				t.Fatalf("fixed User update refusal=%+v", refused)
			}
			after, again := journal(t, r.store)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sets, again) {
				t.Fatal("policy refusal changed journal")
			}
			reader := r.principal
			r.principal = model.PrincipalRef{Kind: "agent", ID: "b20-writer", Sponsor: &reader}
			update(t, r, "b20-1", "journal edited")
			r.principal = reader
			page := list(t, r, "", 100)
			if len(page.Events) != 2 || page.Events[1].Durable == nil || !reflect.DeepEqual(page.Events[1].Durable.Chain, []string{"authz/fixed"}) {
				t.Fatalf("installed-policy events=%+v", page)
			}
			before, sets = journal(t, r.store)
			r.principal.Caveats = map[string]any{"projects": []model.Ref{model.RefByID("other-project", model.Natural64(0))}}
			denied := list(t, r, "", 100)
			if len(denied.Events) != 0 {
				t.Fatal("installed fixed checker disclosed event outside project caveat")
			}
			after, again = journal(t, r.store)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sets, again) {
				t.Fatal("policy read changed journal")
			}
			retain(t, "plug-"+engine+"-installed-policy.json", map[string]any{"allowed_events": page.Events, "restricted_events": denied.Events, "policy": "authz/fixed", "installation": "b20-fixed", "seed": "actual public no-plugin create before installation", "write": "supported sponsored-Agent update; User update refused", "authentication": "trusted RPC principal injection; no public authentication qualification"})
		})
	}
}
