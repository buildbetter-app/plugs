package s1b05b08

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b08InstalledLifecycle struct {
	f             *b08CloseFixture
	ns            authz.Namespace
	policy        authzrpc.LifecyclePolicy
	policyRef     model.Ref
	keys          *authnapikey.Provider
	by            model.PrincipalRef
	app           *authzrpc.Composition
	policyWritten bool
}

func newB08InstalledLifecycle(t *testing.T, f *b08CloseFixture) *b08InstalledLifecycle {
	p := seedB08Lifecycle(t, f)
	p.compose(t)
	return p
}

// seedB08Lifecycle supplies administrative policy data without constructing a
// service. Production composition tests install only the Runtime's real chain.
func seedB08Lifecycle(t *testing.T, f *b08CloseFixture) *b08InstalledLifecycle {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "b08-native-lifecycle", Authority: model.RefByID("authority", model.Natural64(0))}
	p := &b08InstalledLifecycle{f: f, ns: ns, policyRef: model.RefByID("b08-lifecycle-authority", model.Natural64(0)), by: model.PrincipalRef{Kind: "user", ID: "user:b08-lifecycle"}, keys: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})}
	p.policy.Namespace = ns
	for _, dep := range []authz.Dependency{authz.ACLDependency(ns), authz.ContextDependency(ns)} {
		fact, err := driver.NewReadFact(dep.Key)
		if err != nil {
			t.Fatal(err)
		}
		f.extraLocks = append(f.extraLocks, driver.LockKey{Kind: driver.LockReadFact, Fact: fact})
	}
	grantDep, err := authz.GrantDependency(ns, authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("b08-native-project", model.Natural64(0))})
	if err != nil {
		t.Fatal(err)
	}
	grantSentinel, err := driver.NewSentinel(grantDep.Key)
	if err != nil {
		t.Fatal(err)
	}
	policyLockRef := p.policyRef
	f.extraLocks = append(f.extraLocks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: grantSentinel}, driver.LockKey{Kind: driver.LockRecord, Ref: &policyLockRef})
	for _, from := range []string{"todo", "done"} {
		p.policy.Grants = append(p.policy.Grants, authzrpc.LifecycleGrant{Principal: p.by, Project: model.RefByID("b08-native-project", model.Natural64(0)), Type: "issue", FromStatus: from, ToStatus: "done", Actions: []authz.Action{authz.Modify, authz.Transition, authz.OverrideGuard}, Effect: authz.Allow})
	}
	p.writePolicy(t)
	return p
}

func (p *b08InstalledLifecycle) writePolicy(t *testing.T) {
	t.Helper()
	body, err := json.Marshal(p.policy)
	if err != nil {
		t.Fatal(err)
	}
	op := model.OpCreate
	if p.policyWritten {
		op = model.OpPatch
	}
	if _, err := conformance.Seed(p.f.ctx, p.f.store, p.f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: p.policyRef, Kind: "lifecycle_policy", Op: op, Body: body}}); err != nil {
		t.Fatal(err)
	}
	p.policyWritten = true
}

func (p *b08InstalledLifecycle) compose(t *testing.T) {
	t.Helper()
	facts := authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: p.ns, Projects: []authzfixed.Project{{Ref: model.RefByID("b08-native-project", model.Natural64(0)), Name: project, InstallationID: p.ns.InstallationID, Active: true}}}}
	app, err := authzrpc.New(p.f.ctx, authzrpc.Config{Store: p.f.store, Lease: p.f.lease, Provider: p.keys, Facts: facts, Namespace: p.ns, DefaultProject: project, LifecyclePolicy: &authzrpc.LifecyclePolicyConfig{Ref: p.policyRef}})
	if err != nil || app == nil || len(app.Chain.Rules) != 2 {
		t.Fatalf("native lifecycle installation: app=%+v err=%v", app, err)
	}
	p.app = app
}

func (p *b08InstalledLifecycle) call(t *testing.T, method, label string, params map[string]any) b08Reply {
	t.Helper()
	return p.f.callB08Authenticated(t, p.app.Handler, p.keys, p.by, method, label, params)
}

func (p *b08InstalledLifecycle) revoke(t *testing.T, action authz.Action) {
	t.Helper()
	for i := range p.policy.Grants {
		var keep []authz.Action
		for _, a := range p.policy.Grants[i].Actions {
			if a != action {
				keep = append(keep, a)
			}
		}
		p.policy.Grants[i].Actions = keep
	}
	p.writePolicy(t)
}

// All permits come from the native configured rule's real policy record.
// These calls traverse authentication, public RPC, service and the Writer;
// SQL close/reopen reconstructs the installation and rereads that record.
func TestB08InstalledLifecyclePublicCloseForceAndRevocation(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, force := range []bool{false, true} {
				for _, revoke := range []authz.Action{authz.Modify, authz.Transition, authz.OverrideGuard} {
					if !force && revoke == authz.OverrideGuard {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/force=%t/revoke=%s", engine, method, force, revoke), func(t *testing.T) {
						f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
						target := f.createB08(t, "native-target", "issue", nil, nil)
						if force {
							f.createB08(t, "native-open-child", "issue", nil, &target)
						}
						p := newB08InstalledLifecycle(t, f)
						params := b08ForceParams(method, target, force, "authorized first reason")
						commit := b08Commit(t, p.call(t, method, "native-close", params))
						closed := f.document(t, target)
						if closed.Fields["status"] != "done" || closed.ClosedAt == nil || len(closed.Comments) != 1 || closed.Comments[0].Body != "authorized first reason" || len(commit.Results) == 0 {
							t.Fatalf("authorized close missing durable state: %+v", closed)
						}
						after := f.conservation(t)
						f.reopenB08(t)
						p.compose(t)
						b08SameConservation(t, after, f.conservation(t))
						replay := p.call(t, method, "native-close", params)
						if replay.Meta["replayed"] != true || !reflect.DeepEqual(b08Commit(t, replay), commit) {
							t.Fatal("authorized close lost its exact receipt after reopen")
						}
						b08SameConservation(t, after, f.conservation(t))
						p.revoke(t, revoke)
						beforeRefusal := f.conservation(t)
						for attempt := 0; attempt < 2; attempt++ {
							b08RequireRefusal(t, method, p.call(t, method, "native-close", params), "forbidden", "")
							b08SameConservation(t, beforeRefusal, f.conservation(t))
							code, guard := model.ErrorCode("forbidden"), ontology.GuardKind("")
							if revoke == authz.Transition {
								code, guard = "ontology_guard_failed", ontology.Permission
							}
							b08RequireRefusal(t, method, p.call(t, method, "native-fresh-refused", b08ForceParams(method, target, force, "must not replace")), code, guard)
							b08SameConservation(t, beforeRefusal, f.conservation(t))
							if attempt == 0 {
								f.reopenB08(t)
								p.compose(t)
								b08SameConservation(t, beforeRefusal, f.conservation(t))
							}
						}
					})
				}
			}
		}
	}
}

func TestB08InstalledLifecycleExactAuthorityRefusals(t *testing.T) {
	for _, engine := range testEngines {
		for _, mismatch := range []string{"principal", "project", "type", "source-status", "target-status", "explicit-deny", "empty-grants", "missing-policy", "uncreated-policy", "malformed-policy", "wrong-kind", "wrong-namespace"} {
			t.Run(engine+"/"+mismatch, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
				target := f.createB08(t, "native-refusal-target", "issue", nil, nil)
				p := newB08InstalledLifecycle(t, f)
				for i := range p.policy.Grants {
					g := &p.policy.Grants[i]
					switch mismatch {
					case "principal":
						g.Principal.ID = "user:other"
					case "project":
						g.Project = model.RefByID("different-project", model.Natural64(0))
					case "type":
						g.Type = "task"
					case "source-status":
						g.FromStatus = "in_progress"
					case "target-status":
						g.ToStatus = "failed"
					case "explicit-deny":
						g.Effect = authz.Deny
					}
				}
				if mismatch == "empty-grants" {
					p.policy.Grants = nil
				}
				if mismatch == "wrong-namespace" {
					p.policy.Namespace.InstallationID = "other"
				}
				p.writePolicy(t)
				if mismatch == "missing-policy" {
					if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: p.policyRef, Kind: "lifecycle_policy", Op: model.OpDelete}}); err != nil {
						t.Fatal(err)
					}
				}
				if mismatch == "uncreated-policy" || mismatch == "wrong-kind" {
					p.policyRef = model.RefByID("b08-unobserved-authority", model.Natural64(0))
					if mismatch == "wrong-kind" {
						body, err := json.Marshal(p.policy)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: p.policyRef, Kind: "other_policy", Op: model.OpCreate, Body: body}}); err != nil {
							t.Fatal(err)
						}
					}
					f.extraLocks = append(f.extraLocks, driver.LockKey{Kind: driver.LockRecord, Ref: &p.policyRef})
					p.compose(t)
				}
				if mismatch == "malformed-policy" {
					if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: p.policyRef, Kind: "lifecycle_policy", Op: model.OpPatch, Body: []byte(`{"grants":"invalid"}`)}}); err != nil {
						t.Fatal(err)
					}
				}
				before := f.conservation(t)
				code := model.ErrorCode("forbidden")
				if mismatch == "wrong-namespace" || mismatch == "wrong-kind" || mismatch == "malformed-policy" {
					code = "authz_unavailable"
				}
				for attempt := 0; attempt < 2; attempt++ {
					for _, method := range []string{"bead.close", "tx.apply"} {
						b08RequireRefusal(t, method, p.call(t, method, "native-bound-refusal", b08ForceParams(method, target, false, "must not publish")), code, "")
						b08SameConservation(t, before, f.conservation(t))
					}
					if attempt == 0 {
						f.reopenB08(t)
						p.compose(t)
						b08SameConservation(t, before, f.conservation(t))
					}
				}
			})
		}
	}
}

func TestB08InstalledLifecycleFreshPermissionRefusalDoesNotPublish(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, missing := range []authz.Action{authz.Transition, authz.OverrideGuard} {
				t.Run(engine+"/"+method+"/"+string(missing), func(t *testing.T) {
					f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
					target := f.createB08(t, "native-initial-permission-target", "issue", nil, nil)
					force := missing == authz.OverrideGuard
					if force {
						f.createB08(t, "native-initial-open-child", "issue", nil, &target)
					}
					p := newB08InstalledLifecycle(t, f)
					p.revoke(t, missing)
					before := f.conservation(t)
					params := b08ForceParams(method, target, force, "authorized after repair")
					code, guard := model.ErrorCode("ontology_guard_failed"), ontology.Permission
					if force {
						code, guard = "forbidden", ""
					}
					for attempt := 0; attempt < 2; attempt++ {
						b08RequireRefusal(t, method, p.call(t, method, "native-repaired-key", params), code, guard)
						b08SameConservation(t, before, f.conservation(t))
						if attempt == 0 {
							f.reopenB08(t)
							p.compose(t)
							b08SameConservation(t, before, f.conservation(t))
						}
					}
					for i := range p.policy.Grants {
						p.policy.Grants[i].Actions = append(p.policy.Grants[i].Actions, missing)
					}
					p.writePolicy(t)
					b08Commit(t, p.call(t, method, "native-repaired-key", params))
					if f.document(t, target).Fields["status"] != "done" {
						t.Fatal("repaired native authority did not close target")
					}
					after := f.conservation(t)
					f.reopenB08(t)
					p.compose(t)
					b08SameConservation(t, after, f.conservation(t))
				})
			}
		}
	}
}
