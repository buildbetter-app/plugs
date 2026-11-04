package s1b05b08

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These assertions exercise ordinary public transactions against real stores.
// They are authored source, not runtime qualification. Missing production
// connections remain strict failures, including the public evidence producer,
// satisfied gate observations, and first-close reason settlement below.
func TestB08CloseA3GuardRefusalMatrix(t *testing.T) {
	guards := []ontology.Guard{
		{Kind: ontology.FieldsRequired, Fields: []string{"description"}},
		{Kind: ontology.EvidenceRequired, Kinds: []string{"link", "commit"}},
		{Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers},
		{Kind: ontology.NotTemplate}, {Kind: ontology.GatesResolved, Kinds: []string{"review"}},
	}
	for _, engine := range testEngines {
		for _, guard := range guards {
			t.Run(engine+"/"+string(guard.Kind), func(t *testing.T) {
				doc := b08ContractOntology()
				b08AddCloseGuards(&doc, guard)
				f := newB08CloseFixture(t, engine, doc)
				fields := map[string]any{"description": ""}
				if guard.Kind == ontology.NotTemplate {
					fields["template"] = true
				}
				target := f.createB08(t, "guard-target", "issue", fields, nil)
				unrelated := f.createB08(t, "earlier-batch-patch", "issue", nil, nil)
				switch guard.Kind {
				case ontology.NoOpenChildren:
					f.createB08(t, "open-child", "issue", nil, &target)
				case ontology.NoOpenBlockers:
					blocker := f.createB08(t, "live-blocker", "issue", nil, nil)
					b08Commit(t, f.callB08(t, "bead.link", "guard-blocker", map[string]any{"from": target, "to": blocker, "kind": "blocks"}))
				case ontology.GatesResolved:
					gate := f.createB08(t, "pending-review", "gate", map[string]any{"gate_kind": "review"}, nil)
					b08Commit(t, f.callB08(t, "bead.link", "guard-gate", map[string]any{"from": target, "to": gate, "kind": "blocks"}))
				}
				f.refuseB08(t, "bead.close", "guard-named", map[string]any{"id": target, "reason": "must not be recorded"}, "ontology_guard_failed", guard.Kind)
				f.refuseB08(t, "tx.apply", "guard-batch", b08Batch(
					model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: unrelated, Fields: map[string]any{"description": "must roll back"}}},
					b08CloseOp(target, "must not be recorded"),
				), "ontology_guard_failed", guard.Kind)
			})
		}
	}
}

func TestB08CloseFieldsAndNoPluginPermissionUseFinalPublicImage(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			doc := b08ContractOntology()
			b08AddCloseGuards(&doc, ontology.Guard{Kind: ontology.FieldsRequired, Fields: []string{"description"}})
			f := newB08CloseFixture(t, engine, doc)
			target := f.createB08(t, "required-fields", "issue", nil, nil)
			params := map[string]any{"id": target}
			f.refuseB08(t, "bead.close", "same-key-after-repair", params, "ontology_guard_failed", ontology.FieldsRequired)
			b08Commit(t, f.callB08(t, "bead.update", "satisfy-required", map[string]any{"id": target, "fields": map[string]any{"description": "public evidence explanation"}}))
			closed := b08Commit(t, f.callB08(t, "bead.close", "same-key-after-repair", params))
			if f.document(t, target).Fields["status"] != b08Completed || len(closed.Results) != 1 {
				t.Fatal("no-plugin permission or previously refused key prevented the repaired close")
			}
			after := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
			replay := f.callB08(t, "bead.close", "same-key-after-repair", params)
			if replay.Error != nil || replay.Meta["replayed"] != true {
				t.Fatalf("repaired close did not replay: %+v", replay)
			}
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB08TrustedFixedCompositionMarksInstalledWithoutInventingTransitionPermission(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
			target := f.createB08(t, "installed-target", "issue", nil, nil)
			control := f.createB08(t, "no-plugin-control", "issue", nil, nil)
			// The ordinary composition retains its no-authorization posture.
			b08Commit(t, f.callB08(t, "bead.claim", "no-plugin-claim", map[string]any{"id": control}))
			b08Commit(t, f.callB08(t, "bead.close", "no-plugin-close", map[string]any{"id": control}))
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed-b08", Authority: model.RefByID("authority", model.Natural64(0))}
			projectRef := model.RefByID("b08-permission-project", model.Natural64(0))
			facts, err := authzrpc.NewMutableFacts(authzfixed.Facts{
				Namespace: ns,
				Projects:  []authzfixed.Project{{Ref: projectRef, Name: project, InstallationID: ns.InstallationID, Active: true}},
				Grants:    []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:b08-operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: projectRef}, Role: "involved"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			aclFact, err := driver.NewReadFact(authz.ACLDependency(ns).Key)
			if err != nil {
				t.Fatal(err)
			}
			f.extraLocks = append(f.extraLocks, driver.LockKey{Kind: driver.LockReadFact, Fact: aclFact})
			keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
			by := model.PrincipalRef{Kind: "agent", ID: "agent:b08-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:b08-operator"}, Caveats: map[string]any{
				"actions": []string{"modify"}, "projects": []model.Ref{projectRef},
			}}
			compose := func() *authzrpc.Composition {
				installed, err := authzrpc.New(f.ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Facts: facts, Namespace: ns, DefaultProject: project})
				if err != nil || installed == nil || installed.Checker == nil || len(installed.Chain.Rules) != 1 || installed.Namespace != ns {
					t.Fatalf("validated fixed composition was not constructed: error=%v composition=%+v", err, installed)
				}
				return installed
			}
			installed := compose()
			// A real supported modify still succeeds. That permit must never
			// become the absent transition/target-status permission.
			b08Commit(t, f.callB08Authenticated(t, installed.Handler, keys, by, "bead.update", "installed-ordinary-update", map[string]any{"id": target, "fields": map[string]any{"description": "permitted ordinary update"}}))
			before := f.conservation(t)
			for attempt := 0; attempt < 2; attempt++ {
				for _, row := range []struct {
					method string
					params map[string]any
					code   model.ErrorCode
					guard  ontology.GuardKind
				}{
					{"bead.claim", map[string]any{"id": target}, "ontology_guard_failed", ontology.Permission},
					{"bead.close", map[string]any{"id": target}, "forbidden", ""},
					{"tx.apply", b08Batch(model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: target, Fields: map[string]any{"description": "must roll back"}}}, b08CloseOp(target, "must not close")), "forbidden", ""},
				} {
					label := "installed-refusal-" + row.method
					reply := f.callB08Authenticated(t, installed.Handler, keys, by, row.method, label, row.params)
					b08RequireRefusal(t, row.method, reply, row.code, row.guard)
					b08SameConservation(t, before, f.conservation(t))
				}
				if attempt == 0 {
					f.reopenB08(t)
					installed = compose()
					b08SameConservation(t, before, f.conservation(t))
				}
			}
			// Incomplete composition never supplies a usable installed route.
			incomplete, err := authzrpc.New(f.ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Namespace: ns, DefaultProject: project})
			if err == nil || incomplete != nil {
				t.Fatal("missing required facts produced an installed composition")
			}
			b08SameConservation(t, before, f.conservation(t))
		})
	}
}

func TestB08ClosePermissionInstalledMissingAndDeniedObservationsFailClosed(t *testing.T) {
	for _, engine := range testEngines {
		for _, observation := range []string{"missing-adapter", "missing-decision", "denied", "unavailable"} {
			t.Run(engine+"/"+observation, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "permission-target", "issue", nil, nil)
				unrelated := f.createB08(t, "permission-atomic-peer", "issue", nil, nil)
				var observer service.TransitionPermissionObserver
				if observation != "missing-adapter" {
					observer = func(_ context.Context, request service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
						if reader == nil || reader.Snapshot().At == nil || request.Action != "transition" || request.Resource.Ref.ID != target.ID || request.Project != project || request.Type != "issue" || request.FromStatus != b08Unstarted || request.ToStatus != b08Completed || request.Method != "bead.close" {
							t.Fatalf("permission observation lost its actual action/status/resource/snapshot: %+v", request)
						}
						if observation == "unavailable" {
							return service.TransitionPermissionObservation{}, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
						}
						facts := service.TransitionPermissionObservation{AuthzInstalled: true}
						if observation == "denied" {
							denied := false
							facts.Permission = &denied
						}
						return facts, nil
					}
				}
				f.ontologyOptions = []service.OntologyOption{service.WithTransitionPermissionObserver(observer)}
				f.install(t)
				code, guard := model.ErrorCode("ontology_guard_failed"), ontology.Permission
				if observation == "unavailable" {
					code, guard = "authz_unavailable", ""
				}
				f.refuseB08(t, "bead.close", "permission-named", map[string]any{"id": target}, code, guard)
				f.refuseB08(t, "tx.apply", "permission-batch", b08Batch(
					model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: unrelated, Fields: map[string]any{"description": "must roll back"}}},
					b08CloseOp(target, "must not commit"),
				), code, guard)
			})
		}
	}
}

func TestB08ClosePermissionSourceUsesTheSameTrackedDriverSnapshot(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			target := f.createB08(t, "permission-observed-target", "issue", nil, nil)
			authority := f.createB08(t, "trusted-source-observation", "issue", map[string]any{"description": "deny"}, nil)
			calls := 0
			// This trusted adapter fixture proves population/guarding of the
			// existing A3 facts through real driver reads. It does not claim the
			// fixed #18 policy implements transition, nor invent that mapping.
			observer := func(ctx context.Context, request service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
				calls++
				if request.Action != "transition" || request.Method != "bead.close" || request.Resource.Kind != "bead" || request.Resource.Ref.ID != target.ID || request.FromStatus != b08Unstarted || request.ToStatus != b08Completed || reader.Snapshot().At == nil {
					t.Fatalf("unbound transition request: %+v", request)
				}
				rows, err := reader.Get(ctx, []model.Ref{authority})
				if err != nil || len(rows) != 1 {
					return service.TransitionPermissionObservation{}, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
				}
				doc, err := pipeline.DecodeDocument(rows[0].Body)
				if err != nil {
					return service.TransitionPermissionObservation{}, err
				}
				allowed := doc.Fields["description"] == "allow"
				return service.TransitionPermissionObservation{AuthzInstalled: true, Permission: &allowed}, nil
			}
			f.ontologyOptions = []service.OntologyOption{service.WithTransitionPermissionObserver(observer)}
			f.install(t)
			f.refuseB08(t, "bead.close", "observed-permission-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.Permission)
			b08Commit(t, f.callB08(t, "bead.update", "observed-authority-change", map[string]any{"id": authority, "fields": map[string]any{"description": "allow"}}))
			b08Commit(t, f.callB08(t, "bead.close", "observed-permission-close", map[string]any{"id": target}))
			if calls < 3 || f.document(t, target).Fields["status"] != b08Completed {
				t.Fatal("current tracked observation did not control the actual close")
			}
			after := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB08ClosePermissionConcurrentRevocationCannotCommitStaleAllow(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			target := f.createB08(t, "revoked-target", "issue", nil, nil)
			authority := f.createB08(t, "revoked-observation", "issue", map[string]any{"description": "allow"}, nil)
			calls, changed := 0, false
			var revoked b08Conservation
			observer := func(ctx context.Context, request service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
				calls++
				rows, err := reader.Get(ctx, []model.Ref{authority})
				if err != nil || len(rows) != 1 || request.Action != "transition" || request.Resource.Ref.ID != target.ID {
					t.Fatalf("revocation fixture failed to observe its bound authority: rows=%v error=%v request=%+v", rows, err, request)
				}
				doc, err := pipeline.DecodeDocument(rows[0].Body)
				if err != nil {
					t.Fatal(err)
				}
				allowed := doc.Fields["description"] == "allow"
				if !changed {
					changed = true
					// This is a real competing public Writer commit after the
					// permission read and before the original Writer takes locks.
					b08Commit(t, f.callB08(t, "bead.update", "revoke-between-observe-and-lock", map[string]any{"id": authority, "fields": map[string]any{"description": "deny"}}))
					revoked = f.conservation(t)
				}
				return service.TransitionPermissionObservation{AuthzInstalled: true, Permission: &allowed}, nil
			}
			f.ontologyOptions = []service.OntologyOption{service.WithTransitionPermissionObserver(observer)}
			f.install(t)
			reply := f.callB08(t, "bead.close", "revoked-close", map[string]any{"id": target})
			if reply.Error == nil || reply.Error.Data.Code != "ontology_guard_failed" || len(reply.Result) != 0 || calls < 2 || !changed {
				t.Fatalf("stale permission escaped real read-set validation: reply=%+v calls=%d changed=%v", reply, calls, changed)
			}
			b08SameConservation(t, revoked, f.conservation(t))
			f.reopenB08(t)
			b08SameConservation(t, revoked, f.conservation(t))
			f.refuseB08(t, "bead.close", "revoked-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.Permission)
		})
	}
}

func TestB08CloseFreshSettlementReobservesCurrentTransitionPermission(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "settlement-permission-target", "issue", nil, nil)
				authority := f.createB08(t, "settlement-permission-authority", "issue", map[string]any{"description": "allow"}, nil)
				settlementObserved := false
				observer := func(ctx context.Context, request service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
					if request.Action != "transition" || request.Method != "bead.close" || request.Resource.Ref.ID != target.ID || request.ToStatus != b08Completed {
						t.Fatalf("settlement lost its actual transition context: %+v", request)
					}
					settlementObserved = settlementObserved || request.FromStatus == b08Completed
					rows, err := reader.Get(ctx, []model.Ref{authority})
					if err != nil || len(rows) != 1 {
						t.Fatalf("settlement authority rows=%v error=%v", rows, err)
					}
					doc, err := pipeline.DecodeDocument(rows[0].Body)
					if err != nil {
						t.Fatal(err)
					}
					allowed := doc.Fields["description"] == "allow"
					return service.TransitionPermissionObservation{AuthzInstalled: true, Permission: &allowed}, nil
				}
				f.ontologyOptions = []service.OntologyOption{service.WithTransitionPermissionObserver(observer)}
				f.install(t)
				params := map[string]any{"id": target}
				if method == "tx.apply" {
					params = b08Batch(b08CloseOp(target, ""))
				}
				b08Commit(t, f.callB08(t, method, "settlement-permission-first", params))
				b08Commit(t, f.callB08(t, "bead.update", "settlement-permission-revoke", map[string]any{"id": authority, "fields": map[string]any{"description": "deny"}}))
				// A fresh key is a newly guarded close settlement. Stored-key
				// replay has its separate current-authorizer boundary in Writer.
				f.refuseB08(t, method, "settlement-permission-fresh", params, "ontology_guard_failed", ontology.Permission)
				if !settlementObserved {
					t.Fatal("same-slot close omitted its current permission observation")
				}
			})
		}
	}
}

func TestB08CloseChildrenUseCategoriesAndBothHierarchyRepresentations(t *testing.T) {
	for _, engine := range testEngines {
		for _, representation := range []string{"parent-field", "parent-child-link"} {
			for _, childStatus := range []struct {
				status string
				open   bool
			}{
				{b08Unstarted, true}, {b08Started, true}, {b08Backlog, true}, {b08Triage, true},
				{b08Completed, false}, {b08Failed, false}, {b08Canceled, false},
			} {
				t.Run(engine+"/"+representation+"/"+childStatus.status, func(t *testing.T) {
					doc := b08ContractOntology()
					childWorkflow := doc.Workflows["issue"]
					childWorkflow.Initial = childStatus.status
					doc.Workflows["child"] = childWorkflow
					doc.Types["child"] = ontology.Type{Name: "Child", Workflow: "child", Fields: map[string]ontology.Field{"title": {Type: "string"}}}
					f := newB08CloseFixture(t, engine, doc)
					parent := f.createB08(t, "category-parent", "issue", nil, nil)
					var parentField *model.Ref
					if representation == "parent-field" {
						parentField = &parent
					}
					child := f.createB08(t, "category-child", "child", nil, parentField)
					if representation == "parent-child-link" {
						b08Commit(t, f.callB08(t, "bead.link", "category-child-link", map[string]any{"from": child, "to": parent, "kind": "parent-child"}))
					}
					if childStatus.open {
						f.refuseB08(t, "bead.close", "category-parent-close", map[string]any{"id": parent}, "ontology_guard_failed", ontology.NoOpenChildren)
						return
					}
					beforeChild := f.state(t, child)
					b08Commit(t, f.callB08(t, "bead.close", "category-parent-close", map[string]any{"id": parent}))
					if !bytes.Equal(beforeChild.Record, f.state(t, child).Record) || beforeChild.Versions != f.state(t, child).Versions || f.document(t, parent).Fields["status"] != b08Completed {
						t.Fatal("terminal child did not permit close or was rewritten by parent settlement")
					}
					after := f.conservation(t)
					f.reopenB08(t)
					b08SameConservation(t, after, f.conservation(t))
				})
			}
		}
	}
}

func TestB08CloseBlockerCategoriesAndOpaqueEdges(t *testing.T) {
	for _, engine := range testEngines {
		for _, kind := range []string{"blocks", "conditional-blocks", "tracks", "relates-to", "b08-custom"} {
			for _, blocker := range []struct {
				status   string
				terminal bool
				failure  bool
			}{
				{b08Unstarted, false, false}, {b08Backlog, false, false},
				{b08Completed, true, false}, {b08Failed, true, true}, {b08Canceled, true, false},
			} {
				t.Run(engine+"/"+kind+"/"+blocker.status, func(t *testing.T) {
					doc := b08ContractOntology()
					w := doc.Workflows["issue"]
					w.Initial = blocker.status
					doc.Workflows["blocker"] = w
					doc.Types["blocker"] = ontology.Type{Name: "Blocker", Workflow: "blocker", Fields: map[string]ontology.Field{"title": {Type: "string"}}}
					f := newB08CloseFixture(t, engine, doc)
					target := f.createB08(t, "category-target", "issue", map[string]any{"is_blocked": true}, nil)
					other := f.createB08(t, "category-blocker", "blocker", nil, nil)
					b08Commit(t, f.callB08(t, "bead.link", "category-blocker-link", map[string]any{"from": target, "to": other, "kind": kind}))
					refuses := kind == "blocks" && !blocker.terminal || kind == "conditional-blocks" && !blocker.failure
					if refuses {
						f.refuseB08(t, "bead.close", "category-target-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.NoOpenBlockers)
						return
					}
					b08Commit(t, f.callB08(t, "bead.close", "category-target-close", map[string]any{"id": target}))
					if f.document(t, target).Fields["status"] != b08Completed {
						t.Fatal("stale is_blocked or a nonblocking annotation prevented close")
					}
					after := f.conservation(t)
					f.reopenB08(t)
					b08SameConservation(t, after, f.conservation(t))
				})
			}
		}
	}
}

func TestB08CloseBacklogAndStaleFlagHaveNoLiteralPolicy(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			target := f.createB08(t, "backlog-stale-flag", "issue", map[string]any{"is_blocked": true}, nil)
			b08Commit(t, f.callB08(t, "bead.defer", "backlog-slot", map[string]any{"id": target}))
			if f.document(t, target).Fields["status"] != b08Backlog {
				t.Fatal("fixture did not enter its actual backlog category")
			}
			b08Commit(t, f.callB08(t, "bead.close", "backlog-close", map[string]any{"id": target}))
			after := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB08CloseAndReopenSettleActualPublicTransitiveReadiness(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			blocker := f.createB08(t, "public-root-blocker", "issue", nil, nil)
			direct := f.createB08(t, "public-direct-dependent", "issue", nil, nil)
			transitive := f.createB08(t, "public-transitive-dependent", "issue", nil, nil)
			for i, edge := range [][2]model.Ref{{direct, blocker}, {transitive, direct}} {
				b08Commit(t, f.callB08(t, "bead.link", "public-settlement-link-"+model.Natural64(uint64(i)).String(), map[string]any{"from": edge[0], "to": edge[1], "kind": "blocks"}))
			}
			requireOpaqueReadyIDs(t, f.fixture, blocker.ID)
			original := f.conservation(t)
			b08Commit(t, f.callB08(t, "bead.close", "public-settlement-root", map[string]any{"id": blocker}))
			requireOpaqueReadyIDs(t, f.fixture, direct.ID)
			for _, ref := range []model.Ref{direct, transitive} {
				if !reflect.DeepEqual(original.Versions[ref.ID], f.conservation(t).Versions[ref.ID]) {
					t.Fatal("readiness settlement rewrote an unchanged dependent's history")
				}
			}
			b08Commit(t, f.callB08(t, "bead.close", "public-settlement-direct", map[string]any{"id": direct}))
			requireOpaqueReadyIDs(t, f.fixture, transitive.ID)
			b08Commit(t, f.callB08(t, "bead.reopen", "public-settlement-reopen-root", map[string]any{"id": blocker}))
			requireOpaqueReadyIDs(t, f.fixture, blocker.ID, transitive.ID)
			b08Commit(t, f.callB08(t, "bead.reopen", "public-settlement-reopen-direct", map[string]any{"id": direct}))
			requireOpaqueReadyIDs(t, f.fixture, blocker.ID)
			after := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
			requireOpaqueReadyIDs(t, f.fixture, blocker.ID)
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB08CloseTemplatesCASAndUnknownOverrideAreStrict(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			template := f.createB08(t, "immutable-template-guard", "issue", map[string]any{"template": true}, nil)
			f.refuseB08(t, "bead.close", "template-plain", map[string]any{"id": template}, "ontology_guard_failed", ontology.NotTemplate)
			for _, unknown := range []string{"override_guard"} {
				// An action name is not an accepted request field. The typed force
				// request has separate policy, authorization and invariant controls.
				f.refuseB08(t, "bead.close", "template-"+unknown, map[string]any{"id": template, unknown: true}, "invalid_params", "")
				f.refuseB08(t, "tx.apply", "template-op-"+unknown, map[string]any{"changeSet": map[string]any{"ops": []any{
					map[string]any{"kind": "close", "target": template, unknown: true},
				}}}, "invalid_params", "")
			}
			target := f.createB08(t, "strict-cas", "issue", nil, nil)
			state := f.conservation(t)
			var stale model.Revision
			for _, row := range state.Rows {
				if row.Ref.ID == target.ID {
					stale = row.Revision
				}
			}
			if stale.IsZero() {
				t.Fatal("CAS fixture did not capture the actual stored revision")
			}
			b08Commit(t, f.callB08(t, "bead.update", "advance-cas", map[string]any{"id": target, "fields": map[string]any{"title": "new image"}}))
			expect := []model.Precondition{model.PreconditionByRevision(target, stale)}
			f.refuseB08(t, "bead.close", "named-stale-cas", map[string]any{"id": target, "expect": expect}, "precondition_failed", "")
			op := b08CloseOp(target, "stale close must not add a reason")
			op.Close.Expect = expect
			f.refuseB08(t, "tx.apply", "ordered-stale-cas", b08Batch(
				model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: template, Fields: map[string]any{"title": "must roll back"}}}, op,
			), "precondition_failed", "")
		})
	}
}

func TestB08CloseFirstResultReplaysAndFreshEmptyCloseIsNoop(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "first-close", "issue", nil, nil)
				params := map[string]any{"id": target, "reason": "first close reason"}
				empty := map[string]any{"id": target}
				if method == "tx.apply" {
					params = b08Batch(b08CloseOp(target, "first close reason"))
					empty = b08Batch(b08CloseOp(target, ""))
				}
				before := f.conservation(t)
				first := f.callB08(t, method, "first-close", params)
				commit := b08Commit(t, first)
				after := f.conservation(t)
				if len(after.Changes) != len(before.Changes)+1 || len(after.Versions[target.ID]) != len(before.Versions[target.ID])+1 || commit.Revision != after.Meta.Head {
					t.Fatal("first close did not publish exactly one complete history change")
				}
				f.now = f.now.Add(time.Minute)
				f.reopenB08(t)
				b08SameConservation(t, after, f.conservation(t))
				replay := f.callB08(t, method, "first-close", params)
				if replay.Error != nil || replay.Meta["replayed"] != true || !bytes.Equal(replay.Result, first.Result) {
					t.Fatalf("close response was not retained exactly across reopen: %+v", replay)
				}
				b08SameConservation(t, after, f.conservation(t))
				fresh := f.callB08(t, method, "new-key-empty-close", empty)
				b08Commit(t, fresh)
				if fresh.Meta["replayed"] == true {
					t.Fatal("a fresh idempotency key was mislabeled as replay")
				}
				noop := f.conservation(t)
				scope := f.scope(method, "new-key-empty-close", actor)
				receipt, exists := noop.Receipts[scope.identity()]
				if !exists || receipt.Changeset.Kind != "" || receipt.ResponsePending {
					t.Fatal("already-closed settlement did not retain a final no-op receipt")
				}
				delete(noop.Receipts, scope.identity())
				b08SameConservation(t, after, noop)
				settled := f.conservation(t)
				f.reopenB08(t)
				b08SameConservation(t, settled, f.conservation(t))
			})
		}
	}
}

func TestB08CloseFreshRepeatKeepsTheFirstReason(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "first-reason", "issue", nil, nil)
				params := func(reason string) map[string]any {
					if method == "tx.apply" {
						return b08Batch(b08CloseOp(target, reason))
					}
					return map[string]any{"id": target, "reason": reason}
				}
				b08Commit(t, f.callB08(t, method, "reason-first", params("original close")))
				before := f.conservation(t)
				f.now = f.now.Add(time.Minute)
				b08Commit(t, f.callB08(t, method, "reason-repeat", params("must not replace or append to the first close")))
				after := f.conservation(t)
				scope := f.scope(method, "reason-repeat", actor)
				receipt, exists := after.Receipts[scope.identity()]
				if !exists || receipt.Changeset.Kind != "" {
					t.Fatal("a repeated close recorded another close/reason instead of retaining the first close")
				}
				delete(after.Receipts, scope.identity())
				b08SameConservation(t, before, after)
				settled := f.conservation(t)
				f.reopenB08(t)
				b08SameConservation(t, settled, f.conservation(t))
			})
		}
	}
}

func TestB08CloseAndReopenSettleLeaseWithoutErasingClaimHistory(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			target := f.createB08(t, "claimed-close", "issue", nil, nil)
			b08Commit(t, f.callB08(t, "bead.claim", "claim-before-close", map[string]any{"id": target}))
			claimed := f.document(t, target)
			if claimed.Lease == nil || claimed.Lease.Principal.ID != anonymous.ID || claimed.Lease.Agent != actor {
				t.Fatal("public claim did not record its actual holder")
			}
			before := f.conservation(t)
			b08Commit(t, f.callB08(t, "bead.close", "claimed-close", map[string]any{"id": target, "reason": "completed by holder"}))
			closed := f.document(t, target)
			if closed.Lease != nil || closed.Fields["status"] != b08Completed {
				t.Fatal("close did not settle the active claim lease")
			}
			b08Commit(t, f.callB08(t, "bead.reopen", "claimed-reopen", map[string]any{"id": target}))
			reopened := f.document(t, target)
			if reopened.Lease != nil || reopened.Fields["status"] != b08Unstarted {
				t.Fatal("reopen did not enter its category slot with the lease settled")
			}
			after := f.conservation(t)
			if len(after.Versions[target.ID]) != len(before.Versions[target.ID])+2 || !reflect.DeepEqual(before.Versions[target.ID], after.Versions[target.ID][:len(before.Versions[target.ID])]) {
				t.Fatal("close/reopen rewrote the persisted claim-holder history")
			}
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
			// The base has no separate adopted current-assignee producer. This
			// proves historical holder retention and active-lease settlement only;
			// it must not be promoted to current-assignee retention qualification.
		})
	}
}

func TestB08CloseForeignAndExpiredClaimsRefuseAtomically(t *testing.T) {
	for _, engine := range testEngines {
		for _, refusal := range []string{"foreign", "expired"} {
			t.Run(engine+"/"+refusal, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "lease-guard-target", "issue", nil, nil)
				peer := f.createB08(t, "lease-guard-peer", "issue", nil, nil)
				b08Commit(t, f.callB08(t, "bead.claim", "lease-guard-claim", map[string]any{"id": target}))
				agent, code := "foreign-agent", model.ErrorCode("lease_not_held")
				if refusal == "expired" {
					agent, code = actor, "lease_expired"
					lease := f.document(t, target).Lease
					if lease == nil {
						t.Fatal("expired-close fixture has no actual claim lease")
					}
					f.now = lease.ExpiresAt
				}
				f.refuseB08As(t, "bead.close", "lease-guard-close", agent, map[string]any{"id": target}, code, "")
				f.refuseB08As(t, "tx.apply", "lease-guard-batch", agent, b08Batch(
					model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: peer, Fields: map[string]any{"description": "must roll back"}}},
					b08CloseOp(target, "must not close"),
				), code, "")
			})
		}
	}
}

func TestB08CloseResolvedGateMustSupplyItsAuthoritativeObservation(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			doc := b08ContractOntology()
			b08AddCloseGuards(&doc, ontology.Guard{Kind: ontology.GatesResolved, Kinds: []string{"review"}})
			f := newB08CloseFixture(t, engine, doc)
			target := f.createB08(t, "review-target", "issue", nil, nil)
			gate := f.createB08(t, "review-gate", "gate", map[string]any{"gate_kind": "review"}, nil)
			b08Commit(t, f.callB08(t, "bead.link", "review-link", map[string]any{"from": target, "to": gate, "kind": "blocks"}))
			f.refuseB08(t, "bead.close", "review-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.GatesResolved)
			approval := f.callB08(t, "gate.approve", "review-approval", map[string]any{"id": gate})
			if approval.Error != nil || f.document(t, gate).Fields["gate_state"] != "resolved" {
				t.Fatalf("public resolver did not resolve the actual gate: %+v", approval)
			}
			resolved := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, resolved, f.conservation(t))
			// This remains a strict success assertion. A service adapter that
			// leaves GuardFacts.Gates unobserved must fail this contract probe.
			b08Commit(t, f.callB08(t, "bead.close", "review-close", map[string]any{"id": target}))
			if f.document(t, target).Fields["status"] != b08Completed {
				t.Fatal("resolved gate did not permit the configured close")
			}
		})
	}
}

func TestB08CloseGateKindsAreConjunctiveAndRequireSuccessfulResolution(t *testing.T) {
	for _, engine := range testEngines {
		for _, lastState := range []string{"pending", "failed", "canceled", "resolved"} {
			t.Run(engine+"/"+lastState, func(t *testing.T) {
				doc := b08ContractOntology()
				doc.GateKinds["review"] = ontology.GateKind{Resolver: "plugin.review"}
				b08AddCloseGuards(&doc, ontology.Guard{Kind: ontology.GatesResolved, Kinds: []string{"review"}})
				f := newB08CloseFixture(t, engine, doc)
				target := f.createB08(t, "all-reviews-target", "issue", nil, nil)
				first := f.createB08(t, "all-reviews-first", "gate", map[string]any{"gate_kind": "review"}, nil)
				last := f.createB08(t, "all-reviews-last", "gate", map[string]any{"gate_kind": "review"}, nil)
				for i, gate := range []model.Ref{first, last} {
					b08Commit(t, f.callB08(t, "bead.link", "all-reviews-link-"+model.Natural64(uint64(i)).String(), map[string]any{"from": target, "to": gate, "kind": "blocks"}))
				}
				approved := f.callB08(t, "gate.report", "all-reviews-report-first", map[string]any{"id": first, "state": "success"})
				if approved.Error != nil || f.document(t, first).Fields["gate_state"] != "resolved" {
					t.Fatalf("actual first gate did not resolve: %+v", approved)
				}
				switch lastState {
				case "failed", "resolved":
					outcome := "failure"
					if lastState == "resolved" {
						outcome = "success"
					}
					reply := f.callB08(t, "gate.report", "all-reviews-report-last", map[string]any{"id": last, "state": outcome})
					if reply.Error != nil {
						t.Fatalf("actual last gate did not report: %+v", reply)
					}
				case "canceled":
					b08Commit(t, f.callB08(t, "bead.cancel", "all-reviews-cancel-last", map[string]any{"id": last}))
				}
				if lastState != "resolved" {
					f.refuseB08(t, "bead.close", "all-reviews-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.GatesResolved)
					return
				}
				b08Commit(t, f.callB08(t, "bead.close", "all-reviews-close", map[string]any{"id": target}))
				after := f.conservation(t)
				f.reopenB08(t)
				b08SameConservation(t, after, f.conservation(t))
			})
		}
	}
}

func TestB08CloseRequiresEveryConfiguredGateKind(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			doc := b08ContractOntology()
			doc.GateKinds["security"] = ontology.GateKind{Resolver: "builtin.manual", Params: map[string]any{"min_approvals": 1}}
			b08AddCloseGuards(&doc, ontology.Guard{Kind: ontology.GatesResolved, Kinds: []string{"review", "security"}})
			f := newB08CloseFixture(t, engine, doc)
			target := f.createB08(t, "required-kinds-target", "issue", nil, nil)
			for _, kind := range []string{"review", "security"} {
				f.refuseB08(t, "bead.close", "required-kinds-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.GatesResolved)
				gate := f.createB08(t, "required-kinds-"+kind, "gate", map[string]any{"gate_kind": kind}, nil)
				b08Commit(t, f.callB08(t, "bead.link", "required-kinds-link-"+kind, map[string]any{"from": target, "to": gate, "kind": "blocks"}))
				f.refuseB08(t, "bead.close", "required-kinds-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.GatesResolved)
				approved := f.callB08(t, "gate.approve", "required-kinds-approve-"+kind, map[string]any{"id": gate})
				if approved.Error != nil || f.document(t, gate).Fields["gate_state"] != "resolved" {
					t.Fatalf("required %s gate did not resolve through its public resolver: %+v", kind, approved)
				}
			}
			b08Commit(t, f.callB08(t, "bead.close", "required-kinds-close", map[string]any{"id": target}))
			after := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB08CloseGateGuardRechecksTheFinalGraphAndRequiredRelation(t *testing.T) {
	for _, engine := range testEngines {
		for _, mutation := range []string{"later-pending-gate", "unlink-required-gate", "association-only", "reopened-gate"} {
			t.Run(engine+"/"+mutation, func(t *testing.T) {
				doc := b08ContractOntology()
				b08AddCloseGuards(&doc, ontology.Guard{Kind: ontology.GatesResolved, Kinds: []string{"review"}})
				f := newB08CloseFixture(t, engine, doc)
				target := f.createB08(t, "final-gate-target", "issue", nil, nil)
				resolved := f.createB08(t, "final-gate-resolved", "gate", map[string]any{"gate_kind": "review"}, nil)
				pending := f.createB08(t, "final-gate-pending", "gate", map[string]any{"gate_kind": "review"}, nil)
				approved := f.callB08(t, "gate.approve", "final-gate-approve", map[string]any{"id": resolved})
				if approved.Error != nil {
					t.Fatalf("actual gate approval failed: %+v", approved)
				}
				kind := "blocks"
				if mutation == "association-only" {
					kind = "relates-to"
				}
				b08Commit(t, f.callB08(t, "bead.link", "final-gate-link", map[string]any{"from": target, "to": resolved, "kind": kind}))
				switch mutation {
				case "later-pending-gate":
					f.refuseB08(t, "tx.apply", "final-gate-close", b08Batch(
						b08CloseOp(target, "must roll back"),
						model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: target, To: pending, LinkKind: "blocks"}},
					), "ontology_guard_failed", ontology.GatesResolved)
				case "unlink-required-gate":
					f.refuseB08(t, "tx.apply", "final-gate-close", b08Batch(
						model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{From: target, To: resolved, LinkKind: "blocks"}},
						b08CloseOp(target, "must roll back"),
					), "ontology_guard_failed", ontology.GatesResolved)
				case "reopened-gate":
					b08Commit(t, f.callB08(t, "bead.reopen", "final-gate-reopen", map[string]any{"id": resolved}))
					fallthrough
				case "association-only":
					f.refuseB08(t, "bead.close", "final-gate-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.GatesResolved)
				}
			})
		}
	}
}

func TestB08CloseEvidenceMustComeFromPublicAttachments(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			doc := b08ContractOntology()
			b08AddCloseGuards(&doc, ontology.Guard{Kind: ontology.EvidenceRequired, Kinds: []string{"link", "commit"}})
			f := newB08CloseFixture(t, engine, doc)
			target := f.createB08(t, "evidence-target", "issue", nil, nil)
			f.refuseB08(t, "bead.close", "evidence-close", map[string]any{"id": target}, "ontology_guard_failed", ontology.EvidenceRequired)
			ops := []model.Op{}
			for _, kind := range []string{"link", "commit"} {
				ops = append(ops, model.Op{Kind: model.OpAttach, Attach: &model.AttachOp{Target: target, Attachment: model.Attachment{
					Kind: kind, URI: "https://example.invalid/b08/" + kind, MediaType: "text/plain", Summary: "public guard evidence",
				}}})
			}
			before := f.conservation(t)
			attached := f.callB08(t, "tx.apply", "public-evidence", b08Batch(ops...))
			if attached.Error != nil {
				b08SameConservation(t, before, f.conservation(t))
				t.Fatalf("adopted public attachment producer is not connected: %+v", attached.Error)
			}
			b08Commit(t, attached)
			if len(f.document(t, target).Attachments) != 2 {
				t.Fatal("public attachment transaction did not persist both required evidence kinds")
			}
			b08Commit(t, f.callB08(t, "bead.close", "evidence-close", map[string]any{"id": target}))
			after := f.conservation(t)
			f.reopenB08(t)
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB08CloseWorkflowMixedPlaneRefusalPreservesBothActualStores(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			f.workflow = true
			f.install(t)
			durable := f.createB08(t, "durable-peer", "issue", nil, nil)
			invoked := f.callB08(t, "x.workflow.invoke", "workflow-producer", map[string]any{
				"template": "b08-checklist", "project": project, "mode": "ephemeral",
				"checklist": []workflowplugin.ChecklistItem{{Label: "one", Title: "actual operational item"}},
			})
			var run workflowplugin.InvokeResult
			if invoked.Error != nil || json.Unmarshal(invoked.Result, &run) != nil || len(run.Items) != 1 || run.Plane != "ephemeral" {
				t.Fatalf("real supported workflow producer failed: error=%+v result=%s", invoked.Error, invoked.Result)
			}
			item := model.RefByID(run.Items[0].ID, model.Natural64(0))
			before := f.conservation(t)
			if _, present := before.Checkpoints["plugin/workflow/default/durable"]; !present {
				t.Fatal("workflow assertion has no actual persisted plugin state")
			}
			// The adopted Option C contract refuses mixed-plane references.
			// It defines no parent/link producer for positive cross-plane child
			// counts or settlement; such proof requires the B26 owner's seam.
			f.refuseB08(t, "tx.apply", "mixed-plane-close", b08Batch(b08CloseOp(durable, "must roll back"), b08CloseOp(item, "must roll back")), "unsupported", "")
			b08SameConservation(t, before, f.conservation(t))
		})
	}
}
