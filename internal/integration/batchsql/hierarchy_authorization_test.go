package batchsql_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Exercise the real EP2 checker on EP3's complete ordered Writer proposal and
// persisted graph, rather than constructing proposed images in a policy test.
// Authentication and the public RPC adapter are separate acceptance boundaries.
func TestHierarchyFixedAuthorizationAtWriterAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seed := hierarchyApply(t, f,
				hierarchyCreate("old", nil, "old-project"),
				hierarchyCreate("new", nil, "parent-project"),
				hierarchyCreate("other", nil, "parent-project"),
				hierarchyCreate("secret", nil, "secret-project"),
				hierarchyCreate("child", hierarchyRef("old"), "child-project"),
			)
			child, parent, other, secret := seed.Results[4].Ref, seed.Results[1].Ref, seed.Results[2].Ref, seed.Results[3].Ref
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
			base := authzfixed.Facts{Namespace: ns}
			for _, project := range []string{"child-project", "parent-project", "old-project", "secret-project"} {
				base.Projects = append(base.Projects, authzfixed.Project{Ref: model.RefByID(project, model.Natural64(0)), Name: project, InstallationID: "fixed", Active: true})
			}
			for _, project := range []string{"child-project", "parent-project"} {
				base.Grants = append(base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID(project, model.Natural64(0))}, Role: "involved"})
			}
			facts, err := authzrpc.NewMutableFacts(base)
			if err != nil {
				t.Fatal(err)
			}
			provider := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
			writer := func(enabled bool) *pipeline.Writer {
				app, err := authzrpc.New(f.ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: provider, Facts: facts, Namespace: ns, DefaultProject: "child-project", Hierarchy: enabled})
				if err != nil {
					t.Fatal(err)
				}
				return pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithAuthorizer(app.Checker), pipeline.WithEventContext(app.Checker), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID))
			}
			f.writer = writer(true)
			errorData := func(err error) any {
				var typed *model.Error
				if errors.As(err, &typed) {
					return typed.Data
				}
				return err
			}
			by := model.PrincipalRef{Kind: "agent", ID: "agent:hierarchy-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}
			requestMeta := func(label string) model.WriteMeta {
				m := f.meta(label)
				m.Project = "child-project"
				return m
			}
			receipt := func(m model.WriteMeta) *driver.IdempotencyRecord {
				principal, err := driver.PrincipalKey(by)
				if err != nil {
					t.Fatal(err)
				}
				entry, err := hierarchyReader(t, f).IdempotencyByKey(f.ctx, principal, "", m.Method, m.IdempotencyKey)
				if err != nil {
					t.Fatal(err)
				}
				return entry
			}
			batch := model.ChangeSet{Ops: []model.Op{
				hierarchyCreate("epic", nil, "parent-project"),
				hierarchyCreate("task", hierarchyRef("epic"), "child-project"),
			}}
			created, err := f.writer.Apply(f.ctx, by, batch, requestMeta("authorized-ordered-create"))
			if err != nil {
				t.Fatalf("authorized same-batch hierarchy create: %v (%+v)", err, errorData(err))
			}
			doc := decodeDocument(t, f.state(t).Records[created.Results[1].Ref.ID])
			if doc.Project != "child-project" || doc.Parent == nil || doc.Parent.ID != created.Results[0].Ref.ID {
				t.Fatalf("authorized create changed project or lost resolved parent: %+v", doc)
			}
			move := model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &parent)}}
			moveMeta := requestMeta("authorized-move")
			moved, err := f.writer.Apply(f.ctx, by, move, moveMeta)
			if err != nil {
				t.Fatalf("move required permission on the old parent: %v (%+v)", err, errorData(err))
			}
			verifyHierarchy(t, f)
			replayed, err := f.writer.ApplyWithOutcome(f.ctx, by, move, moveMeta)
			if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, moved) {
				t.Fatalf("authorized hierarchy replay: %+v %v", replayed, err)
			}
			refuse := func(label string, cs model.ChangeSet, m model.WriteMeta) {
				t.Helper()
				before, rows, predicates := f.state(t), hierarchyRows(t, f), hierarchyFacts(t, f)
				_, err := f.writer.Apply(f.ctx, by, cs, m)
				requireCode(t, err, "forbidden")
				requireSameState(t, before, f.state(t))
				if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(predicates, hierarchyFacts(t, f)) {
					t.Fatalf("%s published closure or predicates", label)
				}
			}
			refuse("hidden destination", model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &secret)}}, requestMeta("hidden-destination"))
			for _, missing := range []string{"child-project", "parent-project"} {
				revoked := base
				revoked.Grants = nil
				for _, grant := range base.Grants {
					if grant.Scope.Ref.ID != missing {
						revoked.Grants = append(revoked.Grants, grant)
					}
				}
				if err := facts.Replace(revoked); err != nil {
					t.Fatal(err)
				}
				refuse("missing "+missing, model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &other)}}, requestMeta("revoked-move"))
				refuse("revoked replay "+missing, move, moveMeta)
				if entry := receipt(moveMeta); entry == nil || !reflect.DeepEqual(entry.Commit, moved) {
					t.Fatal("revoked replay modified the original receipt")
				}
			}
			if err := facts.Replace(base); err != nil {
				t.Fatal(err)
			}
			f.writer = writer(false)
			disabledMeta := requestMeta("disabled-hierarchy")
			disabledMeta.Method = "bead.reparent"
			refuse("default fixed permissions", model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &other)}}, disabledMeta)
			verifyHierarchy(t, f)
			t.Logf("hierarchy %s: fixed checker authorized ordered alias create and move without old-parent permission; child ownership, both edit grants, revocation/replay, and default-off refusal preserved", engine)
		})
	}
}
