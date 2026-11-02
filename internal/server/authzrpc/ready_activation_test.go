package authzrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func activationFactory(t *testing.T, engine string) storeFactory {
	t.Helper()
	if engine == "postgres" {
		return postgresFactory(t)
	}
	return func(t *testing.T) driver.Store {
		t.Helper()
		s, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "ready-activation.db")})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}
func TestGuardedReadyActivationContractsAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixtureOn(t, activationFactory(t, engine)(t), nil)
			renewReadyScanLease(t, f.lease)
			seedReadyScan(t, f, 2, 3)
			// Real installed direct and one-hop team relations, not a precomputed
			// effective permission mask. The final race revokes their stored source.
			f.base.Grants = append(f.base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("project", model.Natural64(0))}, Role: "involved"})
			if err := f.facts.Replace(f.base); err != nil {
				t.Fatal(err)
			}
			by := model.PrincipalRef{Kind: "user", ID: "user:operator"}
			t.Run("preparation_and_demand", func(t *testing.T) {
				authzrpc.ReadyPreparationContract(t, f.server, by, []model.Ref{fixtureRef(1), fixtureRef(2), fixtureRef(3)})
			})
			t.Run("complete_reference_and_fresh_fence", func(t *testing.T) {
				authzrpc.ReadyReferenceAndFenceContract(t, f.server, by, func() {
					next := f.base
					next.Grants = nil
					if err := f.facts.Replace(next); err != nil {
						t.Fatal(err)
					}
				}, "project")
			})
		})
	}
}
func TestGuardedReadyGenuineCheckOnlyBoundaries(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixtureOn(t, activationFactory(t, engine)(t), nil)
			renewReadyScanLease(t, f.lease)
			seedReadyScan(t, f, 2, 3)
			by := model.PrincipalRef{Kind: "user", ID: "user:checker"}
			t.Run("small_errors_and_budget", func(t *testing.T) { authzrpc.ReadyCheckOnlyContract(t, f.server, by, 3) })
			seedReadyScan(t, f, 4, 20000)
			t.Run("exact_20000_full_batch", func(t *testing.T) { authzrpc.ReadyCheckOnlyContract(t, f.server, by, 20000) })
			seedReadyScan(t, f, 20001, 20001)
			t.Run("over_20000_no_check", func(t *testing.T) { authzrpc.ReadyCheckOnlyContract(t, f.server, by, 20001) })
		})
	}
}

// The complete B page is captured by the real service before a competing
// write, then passed to the private authorization adapter. Ready has no
// public historical-request parameter.
func TestReadySelectionPinnedContentUsesCurrentAuthority(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixtureOn(t, activationFactory(t, engine)(t), nil)
			renewReadyScanLease(t, f.lease)
			change := func(n int, parent int, title string) conformance.FixtureChange {
				doc := map[string]any{"id": fixtureRef(n).ID, "type": "issue", "project": "project", "fields": map[string]any{"status": "todo", "title": title, "priority": 0}}
				if parent > 0 {
					doc["parent"] = fixtureRef(parent)
				}
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				return conformance.FixtureChange{Ref: fixtureRef(n), Kind: "bead", Op: model.OpPatch, Body: body}
			}
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{change(2, 1, "old child"), change(3, 0, "other")}); err != nil {
				t.Fatal(err)
			}
			old := f.reader(t).Snapshot()
			content := authzrpc.ReadyCompleteContract(t, f.server, model.PrincipalRef{Kind: "user", ID: "user:content"}, "project")
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{change(2, 3, "new child")}); err != nil {
				t.Fatal(err)
			}
			childOnly := model.PrincipalRef{Kind: "user", ID: "user:child", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(2)}}}
			p, err := authzrpc.ReadyFilterContract(context.Background(), f.server, childOnly, content, 1, nil)
			if err != nil || len(p.Items) != 1 || p.Items[0].Fields["title"] != "old child" || !p.Items[0].ParentHidden || p.Items[0].Parent != "" || p.AsOf.Revision.String() != old.At.String() {
				t.Fatalf("historical body/hidden parent/current child caveat changed: %+v %v", p, err)
			}
			both := childOnly
			both.Caveats = map[string]any{"refs": []model.Ref{fixtureRef(1), fixtureRef(2)}}
			p, err = authzrpc.ReadyFilterContract(context.Background(), f.server, both, content, 1000, nil)
			if err != nil || len(p.Items) != 2 || p.Items[1].ID != fixtureRef(2).ID || p.Items[1].Parent != fixtureRef(1).ID || p.Items[1].ParentHidden || p.Items[1].Fields["title"] != "old child" {
				t.Fatalf("historical parent replaced by current relationship: %+v %v", p, err)
			}
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: fixtureRef(1), Kind: "bead", Op: model.OpDelete}}); err != nil {
				t.Fatal(err)
			}
			p, err = authzrpc.ReadyFilterContract(context.Background(), f.server, both, content, 1000, nil)
			if err != nil || len(p.Items) != 2 || !p.Items[1].ParentHidden || p.Items[1].Parent != "" {
				t.Fatalf("deleted current parent disclosed old identity: %+v %v", p, err)
			}
			user := model.PrincipalRef{Kind: "user", ID: "user:cursor"}
			first, err := authzrpc.ReadyFilterContract(context.Background(), f.server, user, content, 1, nil)
			if err != nil || first.Next == nil {
				t.Fatalf("historical cursor setup: %+v %v", first, err)
			}
			next := f.base
			next.Grants = nil
			if err := f.facts.Replace(next); err != nil {
				t.Fatal(err)
			}
			p, err = authzrpc.ReadyFilterContract(context.Background(), f.server, user, content, 1, first.Next)
			var failure *model.Error
			if !errors.As(err, &failure) || failure.Data.Code != "cursor_invalid" || len(p.Items) != 0 || p.Next != nil {
				t.Fatalf("old content cursor bypassed changed current ACL: %+v %v", p, err)
			}
			t.Logf("READY_PINNED_CONTENT_CURRENT_AUTHORITY engine=%s content_revision=%s current_acl_change_refused=true", engine, old.At)
		})
	}
}

func TestGuardedReadyCompleteRelationalProjectReference(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := matrixFixture(t, activationFactory(t, engine))
			renewReadyScanLease(t, f.lease)
			by := model.PrincipalRef{Kind: "user", ID: "user:operator"}
			f.base.Grants = append(f.base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: by.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("other", model.Natural64(0))}, Role: "involved"})
			if err := f.facts.Replace(f.base); err != nil {
				t.Fatal(err)
			}
			expected := map[string][]string{"project": {fixtureRef(1).ID, fixtureRef(6).ID, fixtureRef(7).ID}, "inactive": {fixtureRef(4).ID}, "other": {fixtureRef(5).ID}, "unregistered": {fixtureRef(2).ID, fixtureRef(3).ID}}
			authzrpc.ReadyProjectReferenceContract(t, f.server, by, expected, func() {
				next := f.base
				next.Grants = nil
				next.Projects = append(next.Projects[:0:0], next.Projects...)
				for i := range next.Projects {
					next.Projects[i].Active = false
				}
				if err := f.facts.Replace(next); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}
