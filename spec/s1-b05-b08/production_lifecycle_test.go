package s1b05b08

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This fixture enters the production composition root. Runtime owns the real
// lease/renewal, shared ontology, checker, Writer and authenticated HTTP boundary.
// Only initial administrative records and the storage location are fixture data.
type b08ProductionLifecycle struct {
	*b08InstalledLifecycle
	runtime *composition.Runtime
	source  *composition.OntologyConfig
}

func newB08ProductionLifecycle(t *testing.T, f *b08CloseFixture) *b08ProductionLifecycle {
	t.Helper()
	return installB08ProductionLifecycle(t, seedB08Lifecycle(t, f), nil)
}

func installB08ProductionLifecycle(t *testing.T, policy *b08InstalledLifecycle, source *composition.OntologyConfig) *b08ProductionLifecycle {
	t.Helper()
	f := policy.f
	p := &b08ProductionLifecycle{b08InstalledLifecycle: policy, source: source}
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	p.open(t)
	t.Cleanup(func() {
		if p.runtime != nil {
			if err := p.runtime.Close(context.Background()); err != nil {
				t.Error(err)
			}
			p.runtime = nil
		}
		f.store, f.lease = nil, nil
	})
	return p
}

func (p *b08ProductionLifecycle) open(t *testing.T) {
	t.Helper()
	facts := authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: p.ns, Projects: []authzfixed.Project{{Ref: model.RefByID("b08-native-project", model.Natural64(0)), Name: project, InstallationID: p.ns.InstallationID, Active: true}}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: p.by.Kind, ID: p.by.ID}, Scope: authz.Scope{Kind: authz.AuthorityScope, Ref: p.ns.Authority}, Role: "admin"}}}}
	runtime, err := composition.Open(p.f.ctx, composition.Config{
		Driver: p.f.engine, DSN: p.f.dsn, Project: project, AuthMode: "authenticated",
		Auth:         authn.Config{Provider: p.keys},
		Authority:    &authzrpc.AuthorityConfig{Facts: facts, Namespace: p.ns, LifecyclePolicy: &authzrpc.LifecyclePolicyConfig{Ref: p.policyRef}, OntologyManagement: p.source != nil},
		Ontology:     p.source,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return p.f.store, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Authority == nil {
		t.Fatal("production runtime omitted installed authority")
	}
	p.runtime = runtime
	p.f.store, p.f.lease, p.f.service = runtime.Store, runtime.Lease, runtime.Service
}

func (p *b08ProductionLifecycle) reopen(t *testing.T) {
	t.Helper()
	if p.f.storage == nil {
		return
	}
	if err := p.runtime.Close(p.f.ctx); err != nil {
		t.Fatal(err)
	}
	p.runtime = nil
	store, err := p.f.storage.Open(p.f.ctx, driver.Config{DSN: p.f.dsn})
	if err != nil {
		t.Fatal(err)
	}
	p.f.store, p.f.lease = store, nil
	p.open(t)
}

func (p *b08ProductionLifecycle) call(t *testing.T, method, label string, params map[string]any) b08Reply {
	t.Helper()
	return p.f.callB08Authenticated(t, p.runtime.HTTPHandler, p.keys, p.by, method, label, params)
}

func TestB08ProductionLifecycleCloseForceAndCurrentReplay(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, force := range []bool{false, true} {
				for _, revoke := range []authz.Action{authz.Modify, authz.Transition, authz.OverrideGuard} {
					if !force && revoke == authz.OverrideGuard {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/force=%t/revoke=%s", engine, method, force, revoke), func(t *testing.T) {
						f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
						target := f.createB08(t, "production-target", "issue", nil, nil)
						if force {
							f.createB08(t, "production-open-child", "issue", nil, &target)
						}
						p := newB08ProductionLifecycle(t, f)
						params := b08ForceParams(method, target, force, "production first reason")
						first := b08Commit(t, p.call(t, method, "production-close", params))
						closed := f.document(t, target)
						if closed.Fields["status"] != "done" || closed.ClosedAt == nil || len(closed.Comments) != 1 || closed.Comments[0].Body != "production first reason" {
							t.Fatalf("production close did not settle actual document: %+v", closed)
						}
						after := f.conservation(t)
						p.reopen(t)
						b08SameConservation(t, after, f.conservation(t))
						replay := p.call(t, method, "production-close", params)
						if replay.Meta["replayed"] != true || !reflect.DeepEqual(b08Commit(t, replay), first) {
							t.Fatal("production reopen lost exact close receipt")
						}
						b08SameConservation(t, after, f.conservation(t))
						p.revoke(t, revoke)
						beforeRefusal := f.conservation(t)
						for attempt := 0; attempt < 2; attempt++ {
							b08RequireRefusal(t, method, p.call(t, method, "production-close", params), "forbidden", "")
							b08SameConservation(t, beforeRefusal, f.conservation(t))
							code, guard := model.ErrorCode("forbidden"), ontology.GuardKind("")
							if revoke == authz.Transition {
								code, guard = "ontology_guard_failed", ontology.Permission
							}
							b08RequireRefusal(t, method, p.call(t, method, "production-fresh-refusal", b08ForceParams(method, target, force, "must not replace")), code, guard)
							b08SameConservation(t, beforeRefusal, f.conservation(t))
							if attempt == 0 {
								p.reopen(t)
								b08SameConservation(t, beforeRefusal, f.conservation(t))
							}
						}
					})
				}
			}
		}
	}
}

func TestB08ProductionLifecycleMissingAuthorityAndRepairedKey(t *testing.T) {
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, missing := range []authz.Action{authz.Modify, authz.Transition, authz.OverrideGuard} {
				t.Run(engine+"/"+method+"/"+string(missing), func(t *testing.T) {
					f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
					target := f.createB08(t, "production-repaired-target", "issue", nil, nil)
					force := missing == authz.OverrideGuard
					if force {
						f.createB08(t, "production-policy-child", "issue", nil, &target)
					}
					p := newB08ProductionLifecycle(t, f)
					p.revoke(t, missing)
					before := f.conservation(t)
					params := b08ForceParams(method, target, force, "production authorized after repair")
					code, guard := model.ErrorCode("forbidden"), ontology.GuardKind("")
					if missing == authz.Transition {
						code, guard = "ontology_guard_failed", ontology.Permission
					}
					for attempt := 0; attempt < 2; attempt++ {
						b08RequireRefusal(t, method, p.call(t, method, "production-repaired-key", params), code, guard)
						b08SameConservation(t, before, f.conservation(t))
						if attempt == 0 {
							p.reopen(t)
							b08SameConservation(t, before, f.conservation(t))
						}
					}
					for i := range p.policy.Grants {
						p.policy.Grants[i].Actions = append(p.policy.Grants[i].Actions, missing)
					}
					p.writePolicy(t)
					b08Commit(t, p.call(t, method, "production-repaired-key", params))
					if f.document(t, target).Fields["status"] != "done" {
						t.Fatal("production repaired key did not close")
					}
					after := f.conservation(t)
					p.reopen(t)
					b08SameConservation(t, after, f.conservation(t))
				})
			}
		}
	}
}

// A non-default slot makes a split checker/Writer ontology observable: a
// private default checker would request ToStatus=done and this exact persisted
// policy would deny it. Both fresh and replay authority use the installed
// source, including after a physical SQL close/reopen.
func TestB08ProductionConfiguredCloseSlotAndReplayRevocation(t *testing.T) {
	doc := ontology.DefaultDocument()
	doc.Statuses["resolved"] = doc.Statuses["done"]
	delete(doc.Statuses, "done")
	w := doc.Workflows["issue"]
	for i, status := range w.Statuses {
		if status == "done" {
			w.Statuses[i] = "resolved"
		}
	}
	w.Slots[ontology.Close] = "resolved"
	for i := range w.Transitions {
		if w.Transitions[i].To == "done" {
			w.Transitions[i].To = "resolved"
		}
	}
	doc.Workflows["issue"] = w
	for _, engine := range testEngines {
		for _, method := range []string{"bead.close", "tx.apply"} {
			for _, force := range []bool{false, true} {
				for _, revoke := range []authz.Action{authz.Modify, authz.Transition, authz.OverrideGuard} {
					if !force && revoke == authz.OverrideGuard {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/force=%t/revoke=%s", engine, method, force, revoke), func(t *testing.T) {
						f := newB08CloseFixture(t, engine, doc)
						target := f.createB08(t, "configured-production-target", "issue", nil, nil)
						if force {
							f.createB08(t, "configured-production-child", "issue", nil, &target)
						}
						policy := seedB08Lifecycle(t, f)
						for i := range policy.policy.Grants {
							g := &policy.policy.Grants[i]
							if g.FromStatus == "done" {
								g.FromStatus = "resolved"
							}
							g.ToStatus = "resolved"
						}
						policy.writePolicy(t)
						ref := model.RefByID("ontology/b08-configured-close", model.Natural64(0))
						body, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
						if err != nil {
							t.Fatal(err)
						}
						if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
							t.Fatal(err)
						}
						f.extraLocks = append(f.extraLocks, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
						p := installB08ProductionLifecycle(t, policy, &composition.OntologyConfig{Source: service.StaticOntologySource(ref)})
						params := b08ForceParams(method, target, force, "configured first reason")
						commit := b08Commit(t, p.call(t, method, "configured-production-close", params))
						closed := f.document(t, target)
						if closed.Fields["status"] != "resolved" || closed.ClosedAt == nil || len(closed.Comments) != 1 || closed.Comments[0].Body != "configured first reason" {
							t.Fatalf("configured close did not use shared ontology: %+v", closed)
						}
						after := f.conservation(t)
						p.reopen(t)
						b08SameConservation(t, after, f.conservation(t))
						replay := p.call(t, method, "configured-production-close", params)
						if replay.Meta["replayed"] != true || !reflect.DeepEqual(b08Commit(t, replay), commit) {
							t.Fatal("configured close replay changed its receipt")
						}
						b08SameConservation(t, after, f.conservation(t))
						p.revoke(t, revoke)
						before := f.conservation(t)
						for attempt := 0; attempt < 2; attempt++ {
							b08RequireRefusal(t, method, p.call(t, method, "configured-production-close", params), "forbidden", "")
							b08SameConservation(t, before, f.conservation(t))
							code, guard := model.ErrorCode("forbidden"), ontology.GuardKind("")
							if revoke == authz.Transition {
								code, guard = "ontology_guard_failed", ontology.Permission
							}
							b08RequireRefusal(t, method, p.call(t, method, "configured-production-fresh-refusal", b08ForceParams(method, target, force, "must not replace")), code, guard)
							b08SameConservation(t, before, f.conservation(t))
							if attempt == 0 {
								p.reopen(t)
								b08SameConservation(t, before, f.conservation(t))
							}
						}
					})
				}
			}
		}
	}
}
