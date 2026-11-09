package authzrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type bindingFactsSource struct {
	facts authzfixed.Facts
	loads int
}

func (s *bindingFactsSource) Load(ctx context.Context, snapshot Snapshot) (authzfixed.Facts, error) {
	s.loads++
	return (StaticFacts{Facts: s.facts}).Load(ctx, snapshot)
}

func bindingChecker(t *testing.T) (*Checker, *bindingFactsSource, *atomic.Int32, *atomic.Int32, model.PrincipalRef) {
	t.Helper()
	chain, caller, normalizations := preparationFixture(t)
	source := &bindingFactsSource{facts: authzfixed.Facts{Namespace: chain.Namespace}}
	verifications := new(atomic.Int32)
	defaultCaller := DefaultCallerSource(model.Natural64(1))
	checker := &Checker{chain: chain, source: source, caller: func(ctx context.Context, p model.PrincipalRef) (authz.Caller, error) {
		verifications.Add(1)
		return defaultCaller(ctx, p)
	}}
	return checker, source, verifications, normalizations, caller.Principal
}

func TestBoundAuthorizationPreparesActualCheckerOnce(t *testing.T) {
	checker, source, verifications, normalizations, principal := bindingChecker(t)
	snapshot := new(preparationSnapshot)
	binding, err := checker.BindAuthorization(context.Background(), snapshot, principal, authz.Read, authz.Bead, "bead.get")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.observations != 2 {
		t.Fatalf("StaticFacts source observation + Checker ACL bind=%d want=2", snapshot.observations)
	}
	refs := make([]model.Ref, 100)
	for i := range refs {
		refs[i] = model.RefByID(fmt.Sprintf("bead-%03d", i), model.Natural64(0))
	}
	for _, refs := range [][]model.Ref{refs, refs[:1], refs[50:51]} {
		allowed, err := binding.Check(context.Background(), refs)
		if err != nil || len(allowed) != len(refs) {
			t.Fatalf("bound checks=%v/%v", allowed, err)
		}
		for i, ok := range allowed {
			if !ok {
				t.Fatalf("authenticated read %d was denied", i)
			}
		}
	}
	filter, err := binding.Filter()
	if err != nil {
		t.Fatal(err)
	}
	if again, err := binding.Filter(); err != nil || again != filter {
		t.Fatalf("immutable filter not reused: %v", err)
	}
	for _, ref := range refs {
		if ok, err := filter.Match(context.Background(), binding.slice, authz.Resource{Kind: authz.Bead, Ref: ref}); err != nil || !ok {
			t.Fatalf("sealed filter read=%v/%v", ok, err)
		}
	}
	// These are real host phases, not the four reads of a hand-built slice.
	// Admission freezes the raw principal/caveats once. The caller source gets
	// isolated normalized input; subsequent resources perform no raw codec work.
	if verifications.Load() != 1 || source.loads != 1 || normalizations.Load() != 1 || snapshot.observations != 6 {
		t.Fatalf("caller=%d facts=%d raw normalizations=%d total observations=%d; want 1/1/1/6", verifications.Load(), source.loads, normalizations.Load(), snapshot.observations)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = binding.Check(canceled, refs[:1])
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "authz_unavailable" || !failure.Data.Retryable {
		t.Fatalf("elapsed context reused prior success: %v", err)
	}
	// Independent one-off decisions remain independent authenticated bindings.
	// W9/W10 separately adopt the explicit batch seam in their service loops.
	if ok, err := checker.readDecision(context.Background(), snapshot, principal, "bead.get", refs[0]); err != nil || !ok || verifications.Load() != 2 || source.loads != 2 || snapshot.observations != 12 {
		t.Fatalf("one-off binding reused prior authority: %v/%v caller=%d facts=%d reads=%d", ok, err, verifications.Load(), source.loads, snapshot.observations)
	}
}

type bindingRecordSnapshot struct {
	preparationSnapshot
	rows  map[string]driver.Record
	reads int
}

func (s *bindingRecordSnapshot) Get(_ context.Context, refs []model.Ref) ([]driver.Record, error) {
	s.reads += len(refs)
	var rows []driver.Record
	for _, ref := range refs {
		if row, ok := s.rows[ref.ID]; ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (*bindingRecordSnapshot) Snapshot() driver.View { return driver.View{} }
func (*bindingRecordSnapshot) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	return driver.Page[driver.Record]{}, errors.New("authorization must not scan the corpus")
}

func bindingProjectFixture(t *testing.T) (*Checker, *bindingFactsSource, *atomic.Int32, model.PrincipalRef, *bindingRecordSnapshot) {
	t.Helper()
	checker, source, verifications, _, _ := bindingChecker(t)
	project := model.RefByID("project", model.Natural64(0))
	source.facts.Projects = []authzfixed.Project{{Ref: project, Name: "project", InstallationID: checker.chain.Namespace.InstallationID, Active: true}}
	source.facts.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "sponsor"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"}}
	principal := model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor"}}
	snapshot := &bindingRecordSnapshot{rows: map[string]driver.Record{}}
	for _, id := range []string{"allowed", "other"} {
		name := "project"
		if id == "other" {
			name = "unregistered"
		}
		ref := model.RefByID(id, model.Natural64(0))
		snapshot.rows[id] = driver.Record{Ref: ref, Kind: "bead", Body: []byte(fmt.Sprintf(`{"id":%q,"project":%q,"type":"issue","fields":{"title":"item"}}`, id, name))}
	}
	return checker, source, verifications, principal, snapshot
}

func TestBoundAuthorizationRebindsSamePointerAndGenerations(t *testing.T) {
	checker, source, verifications, principal, snapshot := bindingProjectFixture(t)
	ctx := context.Background()
	ref := model.RefByID("allowed", model.Natural64(0))
	binding, err := checker.BindAuthorization(ctx, snapshot, principal, authz.Modify, authz.Bead, "bead.update")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []bool{true, false, true} {
		if i > 0 {
			source.facts.Grants = nil
			if want {
				source.facts.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "sponsor"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("project", model.Natural64(0))}, Role: "involved"}}
			}
			before := snapshot.observations
			binding, err = binding.Rebind(ctx, snapshot)
			if err != nil || snapshot.observations != before+2 {
				t.Fatalf("same-pointer bind %d failed to reload source/ACL: %v observations=%d/%d", i, err, snapshot.observations, before)
			}
		}
		// Same pointer and zero-valued observed revisions are intentional: a
		// source/fact change still requires a fresh Load and authority state.
		allowed, err := binding.Check(ctx, []model.Ref{ref})
		if err != nil || len(allowed) != 1 || allowed[0] != want || verifications.Load() != int32(i+1) || source.loads != i+1 {
			t.Fatalf("fresh bind %d=%v/%v caller=%d loads=%d", i, allowed, err, verifications.Load(), source.loads)
		}
	}
	// An observation failure on the next retry must not become a cache hit.
	snapshot.failObserve = errors.New("new snapshot observation failed")
	_, err = binding.Rebind(ctx, snapshot)
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "authz_unavailable" || !failure.Data.Retryable || verifications.Load() != 4 || source.loads != 4 {
		t.Fatalf("failed retry reused prior authority: %v", err)
	}
}

func TestBoundAuthorizationKeepsResourceAndMethodBindingsSeparate(t *testing.T) {
	checker, _, _, principal, snapshot := bindingProjectFixture(t)
	refs := []model.Ref{model.RefByID("allowed", model.Natural64(0)), model.RefByID("other", model.Natural64(0)), model.RefByKey("invalid")}
	for _, tc := range []struct {
		name   string
		action authz.Action
		kind   authz.ResourceKind
		method string
		want   []bool
	}{
		{"modify", authz.Modify, authz.Bead, "bead.update", []bool{true, false, false}},
		{"read_agent", authz.Read, authz.Bead, "bead.get", []bool{false, false, false}},
		{"unsupported_method", authz.Modify, authz.Bead, "bead.close", []bool{false, false, false}},
		{"wrong_kind", authz.Modify, authz.History, "bead.update", []bool{false, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding, err := checker.BindAuthorization(context.Background(), snapshot, principal, tc.action, tc.kind, tc.method)
			if err != nil {
				t.Fatal(err)
			}
			allowed, err := binding.Check(context.Background(), refs)
			if err != nil || !reflect.DeepEqual(allowed, tc.want) {
				t.Fatalf("bound=%v/%v want=%v", allowed, err, tc.want)
			}
			for i, ref := range refs {
				want, err := checker.decision(context.Background(), snapshot, principal, tc.action, tc.kind, ref, tc.method)
				if err != nil || want != allowed[i] {
					t.Fatalf("one-off parity %d: bound=%v one-off=%v/%v", i, allowed[i], want, err)
				}
			}
		})
	}
}

func TestBoundCloseUsesEachCurrentTransitionAndRebindsPolicy(t *testing.T) {
	checker, _, _, _, snapshot := bindingProjectFixture(t)
	checker.lifecycle = true
	checker.ontology = service.NewOntology(nil)
	principal := model.PrincipalRef{Kind: "user", ID: "closer"}
	policyRef := model.RefByID("lifecycle-policy", model.Natural64(0))
	checker.chain.Rules = append(checker.chain.Rules, &lifecycleRuleSet{namespace: checker.chain.Namespace, ref: policyRef})
	for id, status := range map[string]string{"allowed": "todo", "other": "in_progress"} {
		row := snapshot.rows[id]
		row.Body = []byte(fmt.Sprintf(`{"id":%q,"project":"project","type":"issue","fields":{"title":"item","status":%q}}`, id, status))
		snapshot.rows[id] = row
	}
	policy := LifecyclePolicy{Namespace: checker.chain.Namespace, Grants: []LifecycleGrant{{
		Principal: principal, Project: model.RefByID("project", model.Natural64(0)),
		Type: "issue", FromStatus: "todo", ToStatus: "done", Actions: []authz.Action{authz.Modify}, Effect: authz.Allow,
	}}}
	putPolicy := func() {
		t.Helper()
		body, err := json.Marshal(policy)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.rows[policyRef.ID] = driver.Record{Ref: policyRef, Kind: "lifecycle_policy", Body: body}
	}
	putPolicy()
	ctx := context.Background()
	binding, err := checker.BindAuthorization(ctx, snapshot, principal, authz.Modify, authz.Bead, "bead.close")
	if err != nil {
		t.Fatal(err)
	}
	refs := []model.Ref{model.RefByID("allowed", model.Natural64(0)), model.RefByID("other", model.Natural64(0))}
	if allowed, err := binding.Check(ctx, refs); err != nil || !reflect.DeepEqual(allowed, []bool{true, false}) {
		t.Fatalf("per-target close transition=%v/%v", allowed, err)
	}
	if allowed, err := checker.modifyDecision(ctx, snapshot, principal, "bead.close", refs[0]); err != nil || !allowed {
		t.Fatalf("ordinary close lost its transition: %v/%v", allowed, err)
	}
	policy.Grants = nil
	putPolicy()
	binding, err = binding.Rebind(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := binding.Check(ctx, refs); err != nil || !reflect.DeepEqual(allowed, []bool{false, false}) {
		t.Fatalf("rebind reused revoked close authority=%v/%v", allowed, err)
	}
}

func TestBoundOntologyChecksKeepExactResourceAndCurrentGrant(t *testing.T) {
	checker, source, _, _, _ := bindingChecker(t)
	management, err := authzfixed.NewOntology(checker.chain.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	checker.chain.Rules = append(checker.chain.Rules, management)
	principal := model.PrincipalRef{Kind: "user", ID: "admin"}
	source.facts.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: principal.ID}, Scope: authz.Scope{Kind: authz.AuthorityScope, Ref: checker.chain.Namespace.Authority}, Role: "admin"}}
	ctx := context.Background()
	binding, err := checker.BindAuthorization(ctx, new(preparationSnapshot), principal, authz.ManageOntology, authz.Ontology, "ontology.apply")
	if err != nil {
		t.Fatal(err)
	}
	refs := []model.Ref{model.RefByID("ontology", model.Natural64(0)), model.RefByID("ontology/project", model.Natural64(0)), model.RefByID("ontology-other", model.Natural64(0))}
	if allowed, err := binding.Check(ctx, refs); err != nil || !reflect.DeepEqual(allowed, []bool{true, true, false}) {
		t.Fatalf("prepared ontology resource bounds=%v/%v", allowed, err)
	}
	filter, err := binding.Filter()
	if err != nil {
		t.Fatal(err)
	}
	for i, ref := range refs {
		if allowed, err := filter.Match(ctx, binding.slice, authz.Resource{Kind: authz.Ontology, Ref: ref}); err != nil || allowed != (i < 2) {
			t.Fatalf("prepared ontology filter %s=%v/%v", ref.ID, allowed, err)
		}
	}
	source.facts.Grants = nil
	binding, err = binding.Rebind(ctx, new(preparationSnapshot))
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := binding.Check(ctx, refs); err != nil || !reflect.DeepEqual(allowed, []bool{false, false, false}) {
		t.Fatalf("rebind reused revoked ontology authority=%v/%v", allowed, err)
	}
}

func TestBoundWriterCallbacksReloadAuthorityAtEachInvocation(t *testing.T) {
	checker, source, verifications, principal, snapshot := bindingProjectFixture(t)
	ref := model.RefByID("allowed", model.Natural64(0))
	proposal := port.ProposedChange{Principal: principal, Meta: model.WriteMeta{Method: "bead.update"}, Changes: []port.Change{{Resource: model.ResourceRef{Kind: "bead", Ref: ref}}}}
	for i, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeBefore, port.AuthorizeReplay} {
		if i == 1 {
			source.facts.Grants = nil
		}
		err := checker.Authorize(context.Background(), stage, proposal, snapshot)
		if (err == nil) != (i == 0) || source.loads != i+1 || verifications.Load() != int32(i+1) {
			t.Fatalf("Writer callback %d did not reacquire authority: %v caller=%d loads=%d", i, err, verifications.Load(), source.loads)
		}
	}
	// The actual Writer retry races remain covered by the unchanged
	// TestFixedPermissionsAcrossDrivers/authorization_races integration controls.
}

func TestBoundContextPreservesExactAndNativeCallerEquality(t *testing.T) {
	checker, _, _, _, _ := bindingChecker(t)
	for _, tc := range []struct {
		name, expected, supplied string
		want                     bool
	}{
		{"large_equal", "9007199254740993", "9007199254740993", true},
		{"large_distinct", "9007199254740993", "9007199254740992", false},
		{"lexical_decimal", "1", "1.0", false},
		{"lexical_exponent", "1", "1e0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principal := model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor", Caveats: map[string]any{"exact": json.Number(tc.expected)}}, Caveats: map[string]any{"refs": []model.Ref{model.RefByID("allowed", model.Natural64(0))}}}
			binding, err := checker.BindAuthorization(context.Background(), new(preparationSnapshot), principal, authz.Read, authz.Bead, "bead.get")
			if err != nil {
				t.Fatal(err)
			}
			caller := binding.request.Request().Caller
			caller.Principal = principal // Native Ref map needs the codec fallback.
			caller.Principal.Sponsor.Caveats["exact"] = json.Number(tc.supplied)
			caller.CapturedEpoch = model.Natural("1e0")
			if got, err := binding.slice.CheckContext(context.Background(), caller); err != nil || got != tc.want {
				t.Fatalf("exact/native equality=%v/%v want=%v", got, err, tc.want)
			}
			caller.Principal.Sponsor.ID = "different-sponsor"
			if got, err := binding.slice.CheckContext(context.Background(), caller); err != nil || got {
				t.Fatalf("same receipt admitted different sponsor: %v/%v", got, err)
			}
		})
	}
}

func TestBoundInputIsolationAndFreshCallerFailure(t *testing.T) {
	checker, _, _, _, principal := bindingChecker(t)
	snapshot := new(preparationSnapshot)
	binding, err := checker.BindAuthorization(context.Background(), snapshot, principal, authz.Read, authz.Bead, "bead.get")
	if err != nil {
		t.Fatal(err)
	}
	principal.Caveats["actions"] = []string{}
	ref := model.RefByID("bead", model.Natural64(0))
	if allowed, err := binding.Check(context.Background(), []model.Ref{ref}); err != nil || !allowed[0] {
		t.Fatalf("caller-owned mutation changed a bound request: %v/%v", allowed, err)
	}
	checker.caller = func(_ context.Context, p model.PrincipalRef) (authz.Caller, error) {
		p.ID = "changed-by-source"
		return authz.Caller{Principal: p, VerificationID: "same", CapturedEpoch: model.Natural64(1)}, nil
	}
	if _, err := binding.Rebind(context.Background(), snapshot); err == nil {
		t.Fatal("fresh caller source replaced the authenticated principal")
	}
	checker, _, _, principal, snapshotWithRows := bindingProjectFixture(t)
	var retained model.PrincipalRef
	defaultCaller := checker.caller
	checker.caller = func(ctx context.Context, p model.PrincipalRef) (authz.Caller, error) {
		retained = p
		return defaultCaller(ctx, p)
	}
	owned, err := checker.BindAuthorization(context.Background(), snapshotWithRows, principal, authz.Modify, authz.Bead, "bead.update")
	if err != nil {
		t.Fatal(err)
	}
	retained.Sponsor.ID = "retained-by-source"
	retained.Caveats = map[string]any{"actions": []string{}}
	if allowed, err := owned.Check(context.Background(), []model.Ref{model.RefByID("allowed", model.Natural64(0))}); err != nil || !allowed[0] || principal.Sponsor.ID != "sponsor" {
		t.Fatalf("retained source alias changed bound input: %v/%v", allowed, err)
	}
	checker.caller = func(_ context.Context, p model.PrincipalRef) (authz.Caller, error) {
		p.Sponsor.ID = "changed-through-shared-pointer"
		return authz.Caller{Principal: p, VerificationID: "same", CapturedEpoch: model.Natural64(1)}, nil
	}
	if _, err := checker.BindAuthorization(context.Background(), snapshotWithRows, principal, authz.Modify, authz.Bead, "bead.update"); err == nil || principal.Sponsor.ID != "sponsor" {
		t.Fatalf("caller source changed shared admitted sponsor: error=%v principal=%+v", err, principal.Sponsor)
	}
}

func TestVerificationDerivativeRetainsParsingWithoutAuthority(t *testing.T) {
	checker, _, _, _, principal := bindingChecker(t)
	binding, err := checker.BindAuthorization(context.Background(), new(preparationSnapshot), principal, authz.Read, authz.Bead, "bead.get")
	if err != nil {
		t.Fatal(err)
	}
	original := binding.request.Request()
	baseFilter, err := binding.Filter()
	if err != nil {
		t.Fatal(err)
	}
	baseDigest, err := baseFilter.Binding()
	if err != nil {
		t.Fatal(err)
	}
	resource := authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))}
	for _, tc := range []struct {
		name, receipt string
		epoch         model.Revision
	}{
		{"receipt", "different", original.Caller.CapturedEpoch},
		{"epoch", original.Caller.VerificationID, model.Natural64(2)},
		{"invalid_receipt", "", original.Caller.CapturedEpoch},
		{"invalid_epoch", original.Caller.VerificationID, model.Natural("-1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			derived := binding.request.WithVerification(tc.receipt, tc.epoch)
			copy := derived.Request()
			copy.Caller.Principal.Caveats["actions"].([]any)[0] = "modify"
			if !reflect.DeepEqual(binding.request.Request(), original) || derived.Request().Caller.Principal.Caveats["actions"].([]any)[0] != "read" {
				t.Fatal("derivative or returned input mutated shared parsing")
			}
			ds, err := checker.chain.CheckPrepared(context.Background(), binding.slice, derived.Checks([]authz.Resource{resource}))
			if err != nil || len(ds) != 1 || ds[0].Effect != authz.Deny {
				t.Fatalf("unverified derivative inherited context: %+v/%v", ds, err)
			}
			if tc.epoch.Validate() == nil {
				template, err := checker.chain.CompilePrepared(derived)
				if err != nil {
					t.Fatal(err)
				}
				sealed, err := template.Prepare()
				if err != nil {
					t.Fatal(err)
				}
				if got, err := sealed.Binding(); err != nil || got == baseDigest {
					t.Fatalf("derivative retained parent digest: %q/%v", got, err)
				}
			}
		})
	}
	if allowed, err := binding.Check(context.Background(), []model.Ref{resource.Ref}); err != nil || !allowed[0] {
		t.Fatalf("derivative damaged original bound context: %v/%v", allowed, err)
	}
}

func TestBoundCustomComponentsRetainNestedFreshObservations(t *testing.T) {
	for _, customRule := range []bool{false, true} {
		for _, path := range []string{"prepared_check", "nested_fixed_eval", "opaque_filter"} {
			t.Run(fmt.Sprintf("rules=%t/%s", customRule, path), func(t *testing.T) {
				checker, _, _, _, principal := bindingChecker(t)
				if customRule {
					checker.chain.Rules[0] = preparationCustomRule{checker.chain.Rules[0]}
				} else {
					checker.chain.Links = []authz.Link{preparationCustomLink{}}
				}
				snapshot := new(preparationSnapshot)
				binding, err := checker.BindAuthorization(context.Background(), snapshot, principal, authz.Read, authz.Bead, "bead.get")
				if err != nil {
					t.Fatal(err)
				}
				filter, err := binding.Filter()
				if err != nil {
					t.Fatal(err)
				}
				resource := authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))}
				check := func() error {
					if path == "opaque_filter" {
						_, err := filter.Match(context.Background(), binding.slice, resource)
						return err
					}
					_, err := binding.Check(context.Background(), []model.Ref{resource.Ref})
					return err
				}
				if err := check(); err != nil {
					t.Fatal(err)
				}
				snapshot.observations = 0
				snapshot.failObserve = errors.New("later custom observation failed")
				if path == "nested_fixed_eval" {
					snapshot.failObserveAfter = 4
				}
				if err := check(); err == nil {
					t.Fatal("custom component skipped its required fresh first-touch read")
				}
			})
		}
	}
}

func TestCompilePreparedKeepsFiniteAndCustomEligibilityIsolated(t *testing.T) {
	for _, customFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(customFirst), func(t *testing.T) {
			finite, caller, _ := preparationFixture(t)
			custom := finite
			custom.Links = []authz.Link{preparationCustomLink{}}
			p, err := authz.PrepareRequest(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.get"})
			if err != nil {
				t.Fatal(err)
			}
			chains := []authz.Chain{finite, custom}
			if customFirst {
				chains[0], chains[1] = chains[1], chains[0]
			}
			filters := make([]*authz.PreparedFilter, 2)
			for i, chain := range chains {
				template, err := chain.CompilePrepared(p)
				if err != nil {
					t.Fatal(err)
				}
				filters[i], err = template.Prepare()
				if err != nil {
					t.Fatal(err)
				}
			}
			resource := authz.Resource{Kind: authz.Bead, Ref: model.RefByID("bead", model.Natural64(0))}
			for i, filter := range filters {
				s, snapshot := preparationSlice(finite, caller)
				if allowed, err := filter.Match(context.Background(), s, resource); err != nil || !allowed {
					t.Fatalf("positive filter=%v/%v", allowed, err)
				}
				snapshot.observations = 0
				snapshot.failObserve = errors.New("fresh custom first-touch failed")
				_, err := filter.Match(context.Background(), s, resource)
				isCustom := len(chains[i].Links) != 0
				if (err != nil) != isCustom || (!isCustom && snapshot.observations != 0) {
					t.Fatalf("chain eligibility leaked across compile order: custom=%v error=%v observations=%d", isCustom, err, snapshot.observations)
				}
			}
			// Concurrent compilation of the same immutable input cannot mutate
			// eligibility on that input; both CheckPrepared paths remain usable.
			var wg sync.WaitGroup
			for _, chain := range chains {
				wg.Add(1)
				go func(chain authz.Chain) {
					defer wg.Done()
					if _, err := chain.CompilePrepared(p); err != nil {
						t.Error(err)
					}
				}(chain)
			}
			wg.Wait()
			for _, chain := range chains {
				s, _ := preparationSlice(finite, caller)
				decisions, err := chain.CheckPrepared(context.Background(), s, p.Checks([]authz.Resource{resource}))
				if err != nil || len(decisions) != 1 || decisions[0].Effect != authz.Allow {
					t.Fatalf("prepared Check after concurrent compile: %+v/%v", decisions, err)
				}
			}
		})
	}
}
