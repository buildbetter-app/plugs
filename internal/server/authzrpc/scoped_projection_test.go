package authzrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// This is deliberately a TEST-ONLY lowering of named scalar edges. There is no
// product compiler or host routing here. Preflight visits every arm: the actual
// fixed Modify policy remains unavailable even when no current team grants exist.
func scopedScalarTestLower(t authz.FilterTemplate, e authz.FilterExpr, keys authzfixed.FactsRows) (driver.SelectionExpr, error) {
	args := make([]driver.SelectionExpr, len(e.Args))
	for i, arg := range e.Args {
		var err error
		args[i], err = scopedScalarTestLower(t, arg, keys)
		if err != nil {
			return driver.SelectionExpr{}, err
		}
	}
	literal := func(c driver.SelectionFactColumn, value string) driver.SelectionFactTest {
		v := driver.SelectionString(value)
		return driver.SelectionFactTest{Column: c, Value: &v}
	}
	exists := func(row driver.DerivedRowKey, kind driver.SelectionFactKind, tests ...driver.SelectionFactTest) driver.SelectionExpr {
		return driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: row, Kind: kind, Tests: tests}}
	}
	boolean := func(b bool) driver.SelectionExpr {
		if b {
			return driver.SelectionExpr{Op: driver.SelectionTrue}
		}
		return driver.SelectionExpr{Op: driver.SelectionFalse}
	}
	switch e.Op {
	case authz.All:
		return driver.SelectionExpr{Op: driver.SelectionAll, Args: args}, nil
	case authz.Any:
		return driver.SelectionExpr{Op: driver.SelectionAny, Args: args}, nil
	case authz.True:
		return boolean(true), nil
	case authz.False:
		return boolean(false), nil
	case authz.PrincipalKind:
		return boolean(t.Request.Caller.Principal.Kind == e.Kind), nil
	case authz.SponsorKind:
		p := t.Request.Caller.Principal.Sponsor
		return boolean(p != nil && p.Kind == e.Kind), nil
	case authz.ActiveProject:
		v := driver.SelectionBool(true)
		return exists(keys.Projects, driver.SelectionProjectFact, driver.SelectionFactTest{Column: driver.SelectionFactName, Candidate: driver.SelectionProject}, driver.SelectionFactTest{Column: driver.SelectionFactActive, Value: &v}), nil
	case authz.GrantExists:
		principal := &t.Request.Caller.Principal
		if e.Subject == authz.SponsorSubject {
			principal = principal.Sponsor
		}
		if principal == nil || principal.Kind != "user" {
			return boolean(false), nil
		}
		tests := []driver.SelectionFactTest{literal(driver.SelectionFactSubject, authzfixed.SelectionSubject(authz.SubjectRef{Kind: principal.Kind, ID: principal.ID})), literal(driver.SelectionFactRole, e.Role)}
		if e.Scope == authz.ProjectBindingScope && t.Request.ResourceKind == authz.Bead {
			tests = append(tests, literal(driver.SelectionFactScopeKind, string(driver.SelectionProjectScope)), driver.SelectionFactTest{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject})
		} else if e.Scope == authz.AuthorityBinding {
			scope, err := authz.ScopeComponent(t.Namespace, authz.Scope{Kind: authz.AuthorityScope, Ref: t.Namespace.Authority})
			if err != nil {
				return driver.SelectionExpr{}, err
			}
			tests = append(tests, literal(driver.SelectionFactScope, scope), literal(driver.SelectionFactScopeKind, string(driver.SelectionAuthorityScope)))
		} else {
			return driver.SelectionExpr{}, driver.ErrSelectionUnavailable
		}
		return exists(keys.Grants, driver.SelectionScopedGrantFact, tests...), nil
	default:
		return driver.SelectionExpr{}, driver.ErrSelectionUnavailable
	}
}

// Reconstruct only public fields, intentionally leaving the private prepared
// pointer out. Match therefore exercises the independent existing interpreter.
func scopedReference(t authz.FilterTemplate) authz.FilterTemplate {
	return authz.FilterTemplate{Namespace: t.Namespace, Request: t.Request, Permit: t.Permit, Forbid: t.Forbid, Restrictions: t.Restrictions}
}

func scopedScalarTemplateTestLower(t authz.FilterTemplate, keys authzfixed.FactsRows) (driver.SelectionExpr, error) {
	// Caveats are implicit in Match, outside its finite expression fields.
	// This test adapter has no caveat compiler: refuse before emitting a plan,
	// including when an explicit permit or principal-kind arm is already false.
	for p := &t.Request.Caller.Principal; p != nil; p = p.Sponsor {
		if len(p.Caveats) != 0 {
			return driver.SelectionExpr{}, driver.ErrSelectionUnavailable
		}
	}
	permit, err := scopedScalarTestLower(t, t.Permit, keys)
	if err != nil {
		return driver.SelectionExpr{}, err
	}
	forbid, err := scopedScalarTestLower(t, t.Forbid, keys)
	if err != nil {
		return driver.SelectionExpr{}, err
	}
	args := []driver.SelectionExpr{permit, {Op: driver.SelectionNot, Args: []driver.SelectionExpr{forbid}}}
	for _, restriction := range t.Restrictions {
		lowered, err := scopedScalarTestLower(t, restriction, keys)
		if err != nil {
			return driver.SelectionExpr{}, err
		}
		args = append(args, lowered)
	}
	return driver.SelectionExpr{Op: driver.SelectionAll, Args: args}, nil
}

func scopedDirectEdge(e authz.FilterExpr) (authz.FilterExpr, bool) {
	if e.Op == authz.GrantExists && e.Scope == authz.ProjectBindingScope {
		return e, true
	}
	for _, arg := range e.Args {
		if found, ok := scopedDirectEdge(arg); ok {
			return found, true
		}
	}
	return authz.FilterExpr{}, false
}

func scopedPlan(t *testing.T, r driver.Reader, filter *authz.PreparedFilter, expression driver.SelectionExpr, keys authzfixed.FactsRows, namespace authz.Namespace, content model.Revision) driver.SelectionPlan {
	t.Helper()
	ctx := context.Background()
	states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &keys.Source}})
	if err != nil || len(states) != 1 || states[0].Derived == nil {
		t.Fatalf("actual complete facts source: %v", err)
	}
	source := driver.SelectionSource{Key: states[0].Key, Revision: states[0].Revision, Present: states[0].Present, Digest: driver.SelectionDigest(states[0].Derived.Value)}
	acl, err := observeACL(ctx, r, namespace)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := filter.Binding()
	if err != nil {
		t.Fatal(err)
	}
	p := driver.SelectionPlan{Incarnation: namespace.Incarnation, Branch: model.Natural64(0), ContentRevision: content, AuthorityRevision: *r.Snapshot().At, ContentCommitted: true, Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: expression, Mode: driver.SelectionPageMode, Page: &driver.SelectionPage{Limit: 100}}
	p.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: binding, Incarnation: p.Incarnation, Branch: p.Branch, AuthorityRevision: p.AuthorityRevision, Sources: []driver.SelectionSource{source, {Key: acl.Key, Revision: acl.Revision, Present: acl.Present}}, Chain: driver.SelectionChainCompiled, Authorization: expression, Host: driver.SelectionHostAbsent})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

type scopedObservedSnapshot struct {
	Snapshot
	observed []driver.LockKey
	failKey  string
}

func (s *scopedObservedSnapshot) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	s.observed = append(s.observed, keys...)
	for _, key := range keys {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return nil, err
		}
		if encoded == s.failKey {
			return nil, errors.New("injected actual authority observation failure")
		}
	}
	return s.Snapshot.Observe(ctx, keys)
}

func scopedRequireUnavailable(t *testing.T, err error) {
	t.Helper()
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "authz_unavailable" || !failure.Data.Retryable {
		t.Fatalf("authority failure became an ordinary denial: %v", err)
	}
}

// Mirror the core legacy-startup fixture's canonical logical root input.
// Graph source metadata alone does not establish ancestor closure. These rows
// have no SelectionIndex; only public startup may prove and annotate them.
func scopedSeedLogicalRoots(t *testing.T, s driver.Store, lease driver.ActiveLease, namespace authz.Namespace, refs []model.Ref) {
	t.Helper()
	ctx := context.Background()
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	var rows []driver.DerivedRow
	for _, ref := range refs {
		row, err := pipeline.AncestorRow(ref.ID, ref.ID, 0)
		if err != nil || row.Index != nil {
			t.Fatal("canonical unannotated logical root:", err)
		}
		rows = append(rows, row)
		key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
		sentinel, err := driver.NewClosureSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		sentinels = append(sentinels, sentinel)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	defer tx.Rollback(ctx)
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, rows); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: namespace.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "scoped-logical-fixture"}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
}

func TestActualScopedProducerAndW2BindingDifferential(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	t.Cleanup(func() { _ = s.Close() })
	lease, err := s.Lease(ctx, "scoped-producer", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, _ := s.Meta(ctx)
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: ref("canonical/authority")}
	base := authzfixed.Facts{Namespace: ns, Teams: []authzfixed.Team{{Ref: ref("same"), InstallationID: "fixed", Active: true}}, Projects: []authzfixed.Project{{Ref: ref("canonical/project"), Name: "web/api", InstallationID: "fixed", Active: true}, {Ref: ref("canonical/other"), Name: "other", InstallationID: "fixed", Active: true}}}
	facts, err := NewMutableFacts(base)
	if err != nil {
		t.Fatal(err)
	}
	composition, err := New(ctx, Config{Store: s, Lease: lease, Provider: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }}), Facts: facts, Namespace: ns, DefaultProject: "web/api"})
	if err != nil {
		t.Fatal(err)
	}
	// Durable authority is an explicit fixture publication, never a side
	// effect required from read-only host construction.
	if err := facts.Replace(base); err != nil {
		t.Fatal(err)
	}
	writer := pipeline.New(s, lease, pipeline.WithOntology(service.NewOntology(nil)))
	var refs []model.Ref
	projects := []string{"web/api", "other", "canonical/project", ""}
	for i, project := range projects {
		// The portable allocator uses slash-free prefixes. These imported
		// registry names intentionally contain slashes and are not allocator
		// profiles. Seed their content through the adopted storage fixture,
		// then publish every tested projection with the real Writer below.
		// Imported updates need a valid status; OpPatch skips create defaults.
		target := ref(fmt.Sprintf("scoped-content-%d", i))
		body, err := json.Marshal(pipeline.Document{ID: target.ID, Type: "issue", Project: project, Fields: map[string]any{"title": "imported source", "priority": 0, "status": "todo"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: target, Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{}}}); err != nil {
			t.Fatal(err)
		}
		result, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "creator"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: target, Fields: map[string]any{"title": "real source"}}}}}, model.WriteMeta{Method: "bead.update", Project: project, IdempotencyKey: fmt.Sprintf("%032x", i+1)})
		if err != nil || len(result.Results) != 1 || result.Results[0].Ref.ID != target.ID {
			t.Fatalf("actual Writer publication project=%q ref=%s: err=%#v results=%+v", project, target.ID, err, result.Results)
		}
		refs = append(refs, target)
	}
	scopedSeedLogicalRoots(t, s, lease, ns, refs)
	checkCoreCoverage := func(stage string, available bool) {
		t.Helper()
		r, err := s.Read(ctx, driver.View{Incarnation: ns.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		binding, err := composition.Checker.BindAuthorization(ctx, r, model.PrincipalRef{Kind: "user", ID: "coverage-reader"}, authz.Read, authz.Bead, "bead.list")
		if err != nil {
			t.Fatal(err)
		}
		filter, err := binding.Filter()
		if err != nil {
			t.Fatal(err)
		}
		probeKeys, _ := authzfixed.SelectionRows(ns)
		expression, err := scopedScalarTemplateTestLower(filter.Description(), probeKeys)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.(driver.SelectionReader).Select(ctx, scopedPlan(t, r, filter, expression, probeKeys, ns, *r.Snapshot().At))
		if !available {
			if !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatalf("%s imported interval unexpectedly admitted: %v", stage, err)
			}
			return
		}
		if err != nil || got.Total != uint64(len(refs)) || len(got.Keys) != len(refs) {
			t.Fatalf("%s SortKey current interval/census: total=%d keys=%d err=%v", stage, got.Total, len(got.Keys), err)
		}
		want, actual := []string{}, []string{}
		for _, ref := range refs {
			want = append(want, ref.ID)
		}
		for _, key := range got.Keys {
			actual = append(actual, key.Ref.ID)
		}
		sort.Strings(want)
		sort.Strings(actual)
		if !reflect.DeepEqual(want, actual) {
			t.Fatalf("%s source census=%v want=%v", stage, actual, want)
		}
	}
	checkCoreCoverage("before public startup", false)
	// Imported ordinary saves leave a temporal coverage gap. With real logical
	// closure present, activate their complete current image, then publish
	// every tested current source again with the real Writer. Backfill must
	// not supply the final source-bound projections exercised below.
	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatalf("activate imported source indexes: %v", err)
	}
	checkCoreCoverage("after public startup", true)
	for i, project := range projects {
		target := refs[i]
		result, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "creator"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: target, Fields: map[string]any{"title": "current source"}}}}}, model.WriteMeta{Method: "bead.update", Project: project, IdempotencyKey: fmt.Sprintf("%032x", i+101)})
		if err != nil || len(result.Results) != 1 || result.Results[0].Ref.ID != target.ID {
			t.Fatalf("post-startup Writer publication project=%q ref=%s: err=%#v results=%+v", project, target.ID, err, result.Results)
		}
	}
	checkCoreCoverage("after final Writer publications", true)
	initial, _ := s.Meta(ctx)
	keys, _ := authzfixed.SelectionRows(ns)
	principal := model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "same"}}
	var stale driver.SelectionPlan
	for bits := 0; bits < 32; bits++ {
		t.Run(fmt.Sprintf("direct_team_activity_%05b", bits), func(t *testing.T) {
			next := base
			next.Projects = append([]authzfixed.Project(nil), base.Projects...)
			next.Teams = append([]authzfixed.Team(nil), base.Teams...)
			next.Projects[0].Active, next.Teams[0].Active = bits&8 != 0, bits&16 != 0
			add := func(subject authz.SubjectRef, kind authz.ScopeKind, target, role string) {
				next.Grants = append(next.Grants, authz.Grant{Subject: subject, Scope: authz.Scope{Kind: kind, Ref: ref(target)}, Role: role})
			}
			if bits&1 != 0 {
				add(authz.SubjectRef{Kind: "user", ID: "same"}, authz.ProjectScope, "canonical/project", "involved")
			}
			if bits&2 != 0 {
				add(authz.SubjectRef{Kind: "user", ID: "same"}, authz.TeamScope, "same", "member")
			}
			if bits&4 != 0 {
				add(authz.SubjectRef{Kind: "team", ID: "same"}, authz.ProjectScope, "canonical/project", "involved")
			}
			if err := facts.Replace(next); err != nil {
				t.Fatal(err)
			}
			r, err := s.Read(ctx, driver.View{Incarnation: ns.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			binding, err := composition.Checker.BindAuthorization(ctx, r, principal, authz.Modify, authz.Bead, "bead.update")
			if err != nil {
				t.Fatal(err)
			}
			filter, err := binding.Filter()
			if err != nil {
				t.Fatal(err)
			}
			description := filter.Description()
			if _, err := scopedScalarTemplateTestLower(description, keys); !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatal("full fixed Modify silently dropped the team alternative")
			}
			unreachable := scopedReference(description)
			unreachable.Permit = authz.And(authz.FilterExpr{Op: authz.False}, description.Permit)
			if _, err := scopedScalarTemplateTestLower(unreachable, keys); !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatal("false arm hid unsupported TeamInvolvement")
			}
			full, err := binding.Check(ctx, refs)
			if err != nil {
				t.Fatal(err)
			}
			edge, found := scopedDirectEdge(description.Permit)
			if !found {
				t.Fatal("actual W2 policy lost direct project grant edge")
			}
			direct := scopedReference(description)
			direct.Permit = authz.And(authz.FilterExpr{Op: authz.PrincipalKind, Kind: "agent"}, authz.FilterExpr{Op: authz.SponsorKind, Kind: "user"}, authz.FilterExpr{Op: authz.ActiveProject}, edge)
			expression, err := scopedScalarTemplateTestLower(direct, keys)
			if err != nil {
				t.Fatal(err)
			}
			// Seal this explicitly extracted TEST subset under its own binding.
			// It never attests that the full W2 Modify policy has been lowered.
			directFilter, err := direct.Prepare()
			if err != nil {
				t.Fatal(err)
			}
			plan := scopedPlan(t, r, directFilter, expression, keys, ns, initial.Head)
			got, err := r.(driver.SelectionReader).Select(ctx, plan)
			if err != nil {
				t.Fatalf("bits=%05b actual producer selection: %v", bits, err)
			}
			var wantIDs, gotIDs []string
			for i, target := range refs {
				resource := authz.Resource{Kind: authz.Bead, Ref: target}
				wantFull, err := scopedReference(description).Match(ctx, binding.slice, resource)
				expectedFull := i == 0 && bits&8 != 0 && (bits&1 != 0 || bits&2 != 0 && bits&4 != 0 && bits&16 != 0)
				if err != nil || wantFull != full[i] || wantFull != expectedFull {
					t.Fatalf("actual W2 binding vs fixed interpreter bits=%05b: %v", bits, err)
				}
				wantDirect, err := direct.Match(ctx, binding.slice, resource)
				expectedDirect := i == 0 && bits&8 != 0 && bits&1 != 0
				if err != nil || wantDirect != expectedDirect {
					t.Fatalf("direct subset differs from explicit registry/activity expectation bits=%05b: %v", bits, err)
				}
				if wantDirect {
					wantIDs = append(wantIDs, target.ID)
				}
			}
			for _, key := range got.Keys {
				gotIDs = append(gotIDs, key.Ref.ID)
			}
			sort.Strings(wantIDs)
			sort.Strings(gotIDs)
			if !reflect.DeepEqual(gotIDs, wantIDs) || got.Total != uint64(len(wantIDs)) {
				t.Fatalf("bits=%05b scoped direct edge=%v want=%v", bits, gotIDs, wantIDs)
			}
			if bits == 9 {
				stale = plan
			} // direct grant + active project; valid NAME!=ID positive.
			if bits == 30 && (!full[0] || len(got.Keys) != 0) {
				t.Fatal("same-team path was incorrectly represented by direct grants")
			}
		})
	}
	current, err := s.Read(ctx, driver.View{Incarnation: ns.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	stale.AuthorityRevision = *current.Snapshot().At
	boundary, err := stale.Boundary.Description()
	if err != nil {
		t.Fatal(err)
	}
	boundary.AuthorityRevision = stale.AuthorityRevision
	acl, err := observeACL(ctx, current, ns)
	if err != nil {
		t.Fatal(err)
	}
	for i, source := range boundary.Sources {
		if source.Key.Kind == driver.LockReadFact {
			boundary.Sources[i] = driver.SelectionSource{Key: acl.Key, Revision: acl.Revision, Present: acl.Present}
		}
	}
	stale.Boundary, err = driver.PrepareSelectionBoundary(boundary)
	if err != nil || stale.Validate() != nil {
		t.Fatalf("stale-source control has an invalid current-authority boundary: %v", err)
	}
	// Authority revision and ACL witness are fresh. ONLY the actual old facts
	// source witness is retained, so this must reach exact-source admission.
	if got, err := current.(driver.SelectionReader).Select(ctx, stale); !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatal("stale actual source recertified historical membership")
	}

	// Uncaveated History's actual fixed policy has a scalar authority grant.
	// Lower its complete W2-bound filter, including forbids and restrictions.
	// Caveated requests retain the actual Check/interpreter path below.
	// It authorizes a History resource; it is not a history-query host adapter.
	var allowedHistory driver.Reader
	for _, admin := range []bool{false, true} {
		next := base
		if admin {
			next.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "same"}, Scope: authz.Scope{Kind: authz.AuthorityScope, Ref: ns.Authority}, Role: "admin"}}
		}
		if err := facts.Replace(next); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name      string
			caller    model.PrincipalRef
			eligible  bool
			checkOnly bool
		}{
			{"user", model.PrincipalRef{Kind: "user", ID: "same"}, true, false},
			{"other_user", model.PrincipalRef{Kind: "user", ID: "different"}, false, false},
			{"service", model.PrincipalRef{Kind: "service", ID: "same"}, false, false},
			{"user_sponsored_agent", model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "same"}}, false, false},
			{"service_sponsored_agent", model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "service", ID: "same"}}, false, false},
			{"allowed_action_caveat", model.PrincipalRef{Kind: "user", ID: "same", Caveats: map[string]any{"actions": []string{string(authz.ImportBulkHistory)}}}, true, true},
			{"empty_action_caveat", model.PrincipalRef{Kind: "user", ID: "same", Caveats: map[string]any{"actions": []string{}}}, false, true},
			{"history_has_no_project", model.PrincipalRef{Kind: "user", ID: "same", Caveats: map[string]any{"projects": []model.Ref{ref("canonical/project")}}}, false, true},
			{"empty_ref_caveat", model.PrincipalRef{Kind: "user", ID: "same", Caveats: map[string]any{"refs": []model.Ref{}}}, false, true},
			{"unknown_caveat", model.PrincipalRef{Kind: "user", ID: "same", Caveats: map[string]any{"unknown": true}}, false, true},
		} {
			caller := tc.caller
			r, err := s.Read(ctx, driver.View{Incarnation: ns.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			binding, err := composition.Checker.BindAuthorization(ctx, r, caller, authz.ImportBulkHistory, authz.History, "")
			if err != nil {
				t.Fatal(err)
			}
			filter, err := binding.Filter()
			if err != nil {
				t.Fatal(err)
			}
			description := filter.Description()
			expression, err := scopedScalarTemplateTestLower(description, keys)
			if tc.checkOnly {
				if !errors.Is(err, driver.ErrSelectionUnavailable) {
					t.Fatalf("History %s silently dropped caveats: %v", tc.name, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			targets := append(append([]model.Ref(nil), refs...), ns.Authority)
			decisions, err := binding.Check(ctx, targets)
			if err != nil || len(decisions) != len(targets) {
				t.Fatalf("actual History W2 binding: %v", err)
			}
			want := admin && tc.eligible
			for i, ref := range targets {
				interpreted, err := scopedReference(description).Match(ctx, binding.slice, authz.Resource{Kind: authz.History, Ref: ref})
				if err != nil || decisions[i] != interpreted || interpreted != want {
					t.Fatalf("actual authority/History decision=%v reference=%v want=%v: %v", decisions[i], interpreted, want, err)
				}
			}
			if tc.checkOnly {
				continue
			}
			plan := scopedPlan(t, r, filter, expression, keys, ns, initial.Head)
			selected, err := r.(driver.SelectionReader).Select(ctx, plan)
			wantTotal := uint64(0)
			if want {
				wantTotal = uint64(len(refs))
				allowedHistory = r
			}
			if err != nil || selected.Total != wantTotal || uint64(len(selected.Keys)) != wantTotal {
				t.Fatalf("actual scoped authority filter=%+v want=%d: %v", selected, wantTotal, err)
			}
			if want {
				for _, mode := range []string{"forbid", "restriction"} {
					limited := scopedReference(description)
					if mode == "forbid" {
						limited.Forbid = authz.FilterExpr{Op: authz.True}
					} else {
						limited.Restrictions = append(limited.Restrictions, authz.FilterExpr{Op: authz.False})
					}
					expr, err := scopedScalarTemplateTestLower(limited, keys)
					if err != nil {
						t.Fatal(err)
					}
					prepared, err := limited.Prepare()
					if err != nil {
						t.Fatal(err)
					}
					if allowed, err := limited.Match(ctx, binding.slice, authz.Resource{Kind: authz.History, Ref: refs[0]}); err != nil || allowed {
						t.Fatalf("History %s failed independent denial: %v", mode, err)
					}
					plan := scopedPlan(t, r, prepared, expr, keys, ns, initial.Head)
					if result, err := r.(driver.SelectionReader).Select(ctx, plan); err != nil || result.Total != 0 || len(result.Keys) != 0 {
						t.Fatalf("History authority bypassed %s: %+v/%v", mode, result, err)
					}
				}
			}
		}
	}
	if allowedHistory == nil {
		t.Fatal("no actual scoped authority positive was exercised")
	}

	reader, err := s.Read(ctx, driver.View{Incarnation: ns.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	user := model.PrincipalRef{Kind: "user", ID: "same"}
	tracked := &scopedObservedSnapshot{Snapshot: reader}
	bound, err := composition.Checker.BindAuthorization(ctx, tracked, user, authz.ImportBulkHistory, authz.History, "")
	if err != nil {
		t.Fatal(err)
	}
	if decisions, err := bound.Check(ctx, []model.Ref{ns.Authority}); err != nil || len(decisions) != 1 || !decisions[0] {
		t.Fatalf("tracked authority positive: %v", err)
	}
	observedBeforeRebind := len(tracked.observed)
	rebound, err := bound.Rebind(ctx, tracked)
	if err != nil || rebound == bound || rebound.slice == bound.slice || len(tracked.observed) <= observedBeforeRebind {
		t.Fatalf("same-pointer/equal-revision retry reused authority observations: %v", err)
	}
	if decisions, err := rebound.Check(ctx, []model.Ref{ns.Authority}); err != nil || len(decisions) != 1 || !decisions[0] {
		t.Fatalf("fresh equal-revision binding lost the authority positive: %v", err)
	}
	seen := map[string]bool{}
	for _, key := range tracked.observed {
		encoded, _ := driver.EncodeLockKey(key)
		seen[encoded] = true
	}
	deps := []authz.Dependency{authz.ContextDependency(ns), authz.ACLDependency(ns), {Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"}}
	grant, err := authz.GrantDependency(ns, authz.Scope{Kind: authz.AuthorityScope, Ref: ns.Authority})
	if err != nil {
		t.Fatal(err)
	}
	deps = append(deps, grant)
	for _, dep := range deps {
		var key driver.LockKey
		switch dep.Kind {
		case "sentinel":
			value, err := driver.NewSentinel(dep.Key)
			if err != nil {
				t.Fatal(err)
			}
			key = driver.LockKey{Kind: driver.LockSentinel, Sentinel: value}
		case "read_fact":
			value, err := driver.NewReadFact(dep.Key)
			if err != nil {
				t.Fatal(err)
			}
			key = driver.LockKey{Kind: driver.LockReadFact, Fact: value}
		default:
			t.Fatalf("unobserved dependency kind %s", dep.Kind)
		}
		encoded, _ := driver.EncodeLockKey(key)
		if !seen[encoded] {
			t.Fatalf("actual bound policy omitted dependency %v", dep)
		}
	}
	sourceKey, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source})
	if !seen[sourceKey] {
		t.Fatal("actual MutableFacts binding omitted its durable source observation")
	}
	filter, err := bound.Filter()
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []func(*authz.Namespace){
		func(n *authz.Namespace) { n.StoreID = "foreign-store" },
		func(n *authz.Namespace) { n.Incarnation = "foreign-incarnation" },
		func(n *authz.Namespace) { n.InstallationID = "foreign-installation" },
		func(n *authz.Namespace) { n.Authority = ref("foreign/authority") },
	} {
		description := scopedReference(filter.Description())
		changed(&description.Namespace)
		tracked.observed = nil
		allow, err := description.Match(ctx, bound.slice, authz.Resource{Kind: authz.History, Ref: ns.Authority})
		var failure *authz.Error
		if allow || !errors.As(err, &failure) || failure.Code != "authz_unavailable" || len(tracked.observed) != 0 {
			t.Fatal("foreign namespace reused actual source authority")
		}
	}
	for _, failed := range []string{sourceKey, func() string {
		value, err := driver.NewSentinel(grant.Key)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: value})
		return encoded
	}()} {
		failing := &scopedObservedSnapshot{Snapshot: reader, failKey: failed}
		binding, err := composition.Checker.BindAuthorization(ctx, failing, user, authz.ImportBulkHistory, authz.History, "")
		if failed == sourceKey {
			if binding != nil {
				t.Fatal("failed source observation produced a binding")
			}
			scopedRequireUnavailable(t, err)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if decisions, err := binding.Check(ctx, []model.Ref{ns.Authority}); decisions != nil {
			t.Fatal("failed grant observation produced ordinary decisions")
		} else {
			scopedRequireUnavailable(t, err)
		}
		f, err := binding.Filter()
		if err != nil {
			t.Fatal(err)
		}
		allow, err := scopedReference(f.Description()).Match(ctx, binding.slice, authz.Resource{Kind: authz.History, Ref: ns.Authority})
		var failure *authz.Error
		if allow || !errors.As(err, &failure) || failure.Code != "authz_unavailable" || !failure.Retryable {
			t.Fatal("reference interpreter swallowed actual grant observation failure")
		}
	}
	if err := facts.Replace(base); err != nil {
		t.Fatal(err)
	}
	// The actual source loader refuses an old pinned authority after replacement.
	if binding, err := composition.Checker.BindAuthorization(ctx, allowedHistory, user, authz.ImportBulkHistory, authz.History, ""); binding != nil {
		t.Fatal("old snapshot was rebound to revoked authority")
	} else {
		scopedRequireUnavailable(t, err)
	}
	fresh, err := s.Read(ctx, driver.View{Incarnation: ns.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	denied, err := composition.Checker.BindAuthorization(ctx, fresh, user, authz.ImportBulkHistory, authz.History, "")
	if err != nil {
		t.Fatal(err)
	}
	if decisions, err := denied.Check(ctx, []model.Ref{ns.Authority}); err != nil || len(decisions) != 1 || decisions[0] {
		t.Fatalf("revoked current History authority=%v/%v", decisions, err)
	}
	deniedFilter, err := denied.Filter()
	if err != nil {
		t.Fatal(err)
	}
	deniedExpr, err := scopedScalarTemplateTestLower(deniedFilter.Description(), keys)
	if err != nil {
		t.Fatal(err)
	}
	deniedPlan := scopedPlan(t, fresh, deniedFilter, deniedExpr, keys, ns, initial.Head)
	if got, err := fresh.(driver.SelectionReader).Select(ctx, deniedPlan); err != nil || got.Total != 0 || len(got.Keys) != 0 {
		t.Fatalf("historical content retained revoked current authority: %+v/%v", got, err)
	}
}
