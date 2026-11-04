package s1b05b08

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func b08ForceParams(method string, target model.Ref, force bool, reason string, expect ...model.Precondition) map[string]any {
	if method == "bead.close" {
		params := map[string]any{"id": target, "force": force, "reason": reason}
		if len(expect) != 0 {
			params["expect"] = expect
		}
		return params
	}
	op := b08CloseOp(target, reason)
	op.Close.Force, op.Close.Expect = force, expect
	return b08Batch(op)
}

func TestB08ForceCloseBypassesOnlyPolicyGuards(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, guard := range []ontology.GuardKind{ontology.FieldsRequired, ontology.EvidenceRequired, ontology.NoOpenChildren, ontology.NoOpenBlockers, ontology.GatesResolved} {
				t.Run(engine+"/"+method+"/"+string(guard), func(t *testing.T) {
					doc := b08ContractOntology()
					extra := ontology.Guard{Kind: guard}
					if guard == ontology.FieldsRequired {
						extra.Fields = []string{"description"}
					}
					if guard == ontology.EvidenceRequired || guard == ontology.GatesResolved {
						extra.Kinds = []string{"link"}
						if guard == ontology.GatesResolved {
							extra.Kinds = []string{"review"}
						}
					}
					b08AddCloseGuards(&doc, extra)
					f := newB08CloseFixture(t, engine, doc)
					target := f.createB08(t, "force-policy-target", "issue", nil, nil)
					var peer model.Ref
					if guard == ontology.NoOpenChildren {
						peer = f.createB08(t, "force-open-child", "issue", nil, &target)
					}
					if guard == ontology.NoOpenBlockers {
						peer = f.createB08(t, "force-live-blocker", "issue", nil, nil)
						b08Commit(t, f.callB08(t, "bead.link", "force-blocker-link", map[string]any{"from": target, "to": peer, "kind": "blocks"}))
					}
					f.refuseB08(t, method, "ordinary-policy-refusal", b08ForceParams(method, target, false, ""), "ontology_guard_failed", guard)
					before := f.conservation(t)
					params := b08ForceParams(method, target, true, "first forced close")
					closed := f.callB08(t, method, "forced-policy-close", params)
					commit := b08Commit(t, closed)
					after := f.conservation(t)
					actual := f.document(t, target)
					if actual.Fields["status"] != b08Completed || len(actual.Comments) != 1 || actual.Comments[0].Body != "first forced close" || actual.Lease != nil || commit.Revision.IsZero() {
						t.Fatalf("forced public close did not settle the actual row: %+v", actual)
					}
					if len(after.Versions[target.ID]) != len(before.Versions[target.ID])+1 || len(after.Changes) != len(before.Changes)+1 {
						t.Fatal("forced close did not publish exactly one target version and one changeset")
					}
					if peer.ID != "" && f.document(t, peer).Fields["status"] != b08Unstarted {
						t.Fatal("policy override changed the live child or blocker")
					}
					f.reopenB08(t)
					b08SameConservation(t, after, f.conservation(t))
					replay := f.callB08(t, method, "forced-policy-close", params)
					if replay.Meta["replayed"] != true || !reflect.DeepEqual(b08Commit(t, replay), commit) {
						t.Fatal("forced close did not replay its original public result")
					}
					b08SameConservation(t, after, f.conservation(t))
				})
			}
		}
	}
}

func TestB08ForceCloseCannotBypassTemplatesOrCAS(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				doc := b08ContractOntology()
				// Template refusal is mandatory even if the workflow omitted
				// its optional not_template guard declaration.
				w := doc.Workflows["issue"]
				for i := range w.Transitions {
					var guards []ontology.Guard
					for _, g := range w.Transitions[i].Guards {
						if g.Kind != ontology.NotTemplate {
							guards = append(guards, g)
						}
					}
					w.Transitions[i].Guards = guards
				}
				doc.Workflows["issue"] = w
				f := newB08CloseFixture(t, engine, doc)
				template := f.createB08(t, "force-template", "issue", map[string]any{"template": true}, nil)
				f.refuseB08(t, method, "force-template-refusal", b08ForceParams(method, template, true, "must not close"), "ontology_guard_failed", ontology.NotTemplate)
				target := f.createB08(t, "force-cas-target", "issue", nil, nil)
				var stale model.Revision
				for _, row := range f.conservation(t).Rows {
					if row.Ref.ID == target.ID {
						stale = row.Revision
					}
				}
				if stale.IsZero() {
					t.Fatal("force CAS fixture has no stored revision")
				}
				b08Commit(t, f.callB08(t, "bead.update", "force-advance-cas", map[string]any{"id": target, "fields": map[string]any{"description": "new revision"}}))
				f.refuseB08(t, method, "force-cas-refusal", b08ForceParams(method, target, true, "must not close", model.PreconditionByRevision(target, stale)), "precondition_failed", "")
			})
		}
	}
}

func TestB08ForceCloseDoesNotLeakToAnotherBatchMemberOrSchema(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newB08CloseFixture(t, engine, b08ContractOntology())
			first := f.createB08(t, "force-first-parent", "issue", nil, nil)
			second := f.createB08(t, "force-second-parent", "issue", nil, nil)
			f.createB08(t, "force-first-child", "issue", nil, &first)
			f.createB08(t, "force-second-child", "issue", nil, &second)
			forced := b08CloseOp(first, "must roll back")
			forced.Close.Force = true
			f.refuseB08(t, "tx.apply", "force-late-unforced-refusal", b08Batch(forced, b08CloseOp(second, "must refuse")), "ontology_guard_failed", ontology.NoOpenChildren)
			f.refuseB08(t, "tx.apply", "force-late-schema-refusal", b08Batch(forced,
				model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: second, Fields: map[string]any{"priority": 5}}},
			), "invalid_params", "")
			// Final-image checks remain authoritative after an earlier forced
			// close, even if the later operation itself is an ordinary patch.
			f.refuseB08(t, "tx.apply", "force-late-template-refusal", b08Batch(forced,
				model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: first, Fields: map[string]any{"template": true}}},
			), "ontology_guard_failed", ontology.NotTemplate)
		})
	}
}

func TestB08ForceCloseRetainsForeignAndExpiredLeaseRefusals(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, refusal := range []string{"foreign", "expired"} {
				t.Run(engine+"/"+method+"/"+refusal, func(t *testing.T) {
					f := newB08CloseFixture(t, engine, b08ContractOntology())
					ref := f.createB08(t, "forced-lease-target", "issue", nil, nil)
					b08Commit(t, f.callB08(t, "bead.claim", "forced-claim", map[string]any{"id": ref}))
					agent, code := "foreign-agent", model.ErrorCode("lease_not_held")
					if refusal == "expired" {
						agent, code = actor, "lease_expired"
						f.now = f.document(t, ref).Lease.ExpiresAt
					}
					f.refuseB08As(t, method, "forced-lease-refusal", agent, b08ForceParams(method, ref, true, "must not close"), code, "")
				})
			}
		}
	}
}

func TestB08ForceCloseRetainsMissingTransitionPermissionOnReplayAndFreshSettlement(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "force-permission-target", "issue", nil, nil)
				params := b08ForceParams(method, target, true, "original forced reason")
				b08Commit(t, f.callB08(t, method, "force-permission-original", params))
				f.ontologyOptions = []service.OntologyOption{service.WithTransitionPermissionObserver(nil)}
				f.install(t)
				before := f.conservation(t)
				for attempt := 0; attempt < 2; attempt++ {
					b08RequireRefusal(t, method, f.callB08(t, method, "force-permission-original", params), "ontology_guard_failed", ontology.Permission)
					b08SameConservation(t, before, f.conservation(t))
					// An explicitly missing installed adapter is denial evidence,
					// never installed-positive AZ9 authorization.
					f.refuseB08(t, method, "force-permission-fresh", b08ForceParams(method, target, true, "must retain first reason"), "ontology_guard_failed", ontology.Permission)
					f.reopenB08(t)
				}
			})
		}
	}
}

func TestB08ForceCloseFreshNoopPreservesFirstReasonIncludingEmpty(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, reason := range []string{"", "first reason"} {
				t.Run(engine+"/"+method+"/"+map[bool]string{true: "empty", false: "reason"}[reason == ""], func(t *testing.T) {
					f := newB08CloseFixture(t, engine, b08ContractOntology())
					target := f.createB08(t, "forced-noop-target", "issue", nil, nil)
					b08Commit(t, f.callB08(t, method, "force-first-result", b08ForceParams(method, target, true, reason)))
					before := f.conservation(t)
					original := f.document(t, target)
					b08Commit(t, f.callB08(t, method, "force-fresh-noop", b08ForceParams(method, target, true, "replacement must not be stored")))
					after := f.conservation(t)
					if !reflect.DeepEqual(original, f.document(t, target)) {
						t.Fatal("fresh forced settlement replaced the first document/reason or published a bead version")
					}
					scope := f.scope(method, "force-fresh-noop", actor)
					receipt, found := after.Receipts[scope.identity()]
					if !found || receipt.Changeset.Kind != "" || receipt.ResponsePending {
						t.Fatal("fresh forced settlement did not store exactly a final no-op receipt")
					}
					delete(after.Receipts, scope.identity())
					b08SameConservation(t, before, after)
					after = f.conservation(t)
					f.reopenB08(t)
					b08SameConservation(t, after, f.conservation(t))
				})
			}
		}
	}
}

func TestB08ForceCloseWireTypesAndRequestIdentityAreStrict(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "force-wire-target", "issue", nil, nil)
				for _, force := range []any{nil, "true", 1, map[string]any{}} {
					params := map[string]any{"id": target, "force": force}
					if method == "tx.apply" {
						params = map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "close", "target": target, "force": force}}}}
					}
					f.refuseB08(t, method, "force-invalid-type", params, "invalid_params", "")
				}
				for _, force := range []bool{false, true} {
					label := "force-key-absent"
					if force {
						label = "force-key-present"
					}
					ref := f.createB08(t, label, "issue", nil, nil)
					b08Commit(t, f.callB08(t, method, label, b08ForceParams(method, ref, force, "")))
					before := f.conservation(t)
					b08RequireRefusal(t, method, f.callB08(t, method, label, b08ForceParams(method, ref, !force, "")), "idempotency_key_reused", "")
					b08SameConservation(t, before, f.conservation(t))
				}
				f.refuseB08(t, "bead.update", "force-on-update", map[string]any{"id": target, "force": true, "fields": map[string]any{}}, "invalid_params", "")
				f.refuseB08(t, "tx.apply", "force-on-transaction", map[string]any{"force": true, "changeSet": map[string]any{"ops": []any{map[string]any{"kind": "close", "target": target}}}}, "invalid_params", "")
			})
		}
	}
}

func TestB08ForceCloseAuthenticatedNoAuthorizationRequiresAUser(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				target := f.createB08(t, "force-authn-target", "issue", nil, nil)
				keys := authnapikey.New(authnapikey.Config{})
				admission, err := authn.New(authn.Config{Provider: keys})
				if err != nil {
					t.Fatal(err)
				}
				handler, err := httpjson.New(f.server, admission, httpjson.Config{})
				if err != nil {
					t.Fatal(err)
				}
				params := b08ForceParams(method, target, true, "verified user close")
				agent := model.PrincipalRef{Kind: "agent", ID: "agent:force", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:force"}}
				before := f.conservation(t)
				if _, err := keys.Issue(agent, f.now.Add(time.Hour)); !errors.Is(err, authn.ErrAuthzNotInstalled) {
					t.Fatalf("scoped agent key without authorization: %v", err)
				}
				// P58 refuses agent credentials before the force route. An
				// unscoped service key reaches the real HTTP handler and
				// independently exercises P63's user-only force admission.
				nonuser := model.PrincipalRef{Kind: "service", ID: "service:force"}
				b08RequireRefusal(t, method, f.callB08Authenticated(t, handler, keys, nonuser, method, "force-service-denied", params), "forbidden", "")
				b08SameConservation(t, before, f.conservation(t))
				user := model.PrincipalRef{Kind: "user", ID: "user:force"}
				reply := f.callB08Authenticated(t, handler, keys, user, method, "force-user-no-authz", params)
				b08Commit(t, reply)
				if f.document(t, target).Fields["status"] != b08Completed {
					t.Fatal("authenticated user did not close through the real no-authorization HTTP route")
				}
				// This is a real authenticated no-authorization route. It
				// supplies no installed-positive transition/override grant.
				var result map[string]any
				if json.Unmarshal(reply.Result, &result) != nil || result["revision"] == nil {
					t.Fatal("force close did not return its public commit")
				}
				meta, err := f.store.Meta(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "force-fixed", Authority: model.RefByID("force-authority", model.Natural64(0))}
				config := authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Namespace: ns, DefaultProject: project, Facts: authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns}}}
				installed, err := authzrpc.New(f.ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				acl, err := driver.NewReadFact(authz.ACLDependency(ns).Key)
				if err != nil {
					t.Fatal(err)
				}
				f.extraLocks = append(f.extraLocks, driver.LockKey{Kind: driver.LockReadFact, Fact: acl})
				before = f.conservation(t)
				r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				// Exercise the actual installed chain's distinct action. Its
				// finite catalog has no override permit; no grant is invented.
				err = installed.Checker.AuthorizeGuardOverride(f.ctx, port.AuthorizeReplay, user, []model.ResourceRef{{Kind: "bead", Ref: target}}, r)
				r.Close()
				var denied *model.Error
				if !errors.As(err, &denied) || denied.Data.Code != "forbidden" {
					t.Fatalf("installed override_guard did not deny its ungranted action: %v", err)
				}
				// Changing from the real no-authorization route to an installed
				// chain cannot turn an existing forced receipt into permission.
				b08RequireRefusal(t, method, f.callB08Authenticated(t, installed.Handler, keys, user, method, "force-user-no-authz", params), "forbidden", "")
				b08RequireRefusal(t, method, f.callB08Authenticated(t, installed.Handler, keys, user, method, "force-user-installed-fresh", params), "forbidden", "")
				b08SameConservation(t, before, f.conservation(t))
			})
		}
	}
}
