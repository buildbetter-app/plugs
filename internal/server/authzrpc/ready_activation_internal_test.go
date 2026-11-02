package authzrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Test exports exercise private routing with the existing external real-engine
// fixtures. They add no production Config, policy, or selector API.
var readyContractPoison = errors.New("ready contract source failure")

type readyDemand struct {
	ctx                                                     context.Context
	events                                                  []string
	records, checks, observes, gets                         int
	failRecord, failCheck, failObserve, failGet, falseCheck int
	cancel                                                  context.CancelFunc
}

func (d *readyDemand) add(ctx context.Context, method string, args, result any, err error) {
	a, _ := json.Marshal(args)
	b, _ := json.Marshal(result)
	d.events = append(d.events, fmt.Sprintf("%s context_same=%t args=%s result=%s error=%v", method, ctx == d.ctx, a, b, err))
}

type readyDemandSnapshot struct {
	Snapshot
	d *readyDemand
}

func (s *readyDemandSnapshot) Get(ctx context.Context, refs []model.Ref) (out []driver.Record, err error) {
	s.d.gets++
	defer func() { s.d.add(ctx, "Get", refs, out, err) }()
	if s.d.gets == s.d.failGet {
		return nil, readyContractPoison
	}
	return s.Snapshot.Get(ctx, refs)
}
func (s *readyDemandSnapshot) Observe(ctx context.Context, keys []driver.LockKey) (out []driver.LockState, err error) {
	s.d.observes++
	defer func() { s.d.add(ctx, "Observe", keys, out, err) }()
	if s.d.observes == s.d.failObserve {
		return nil, readyContractPoison
	}
	return s.Snapshot.Observe(ctx, keys)
}

type readyDemandSlice struct {
	authz.Slice
	d *readyDemand
}

func (s *readyDemandSlice) Record(ctx context.Context, dep authz.Dependency) (err error) {
	s.d.records++
	defer func() { s.d.add(ctx, "Record", dep, nil, err) }()
	if s.d.records == s.d.failRecord {
		return readyContractPoison
	}
	return s.Slice.Record(ctx, dep)
}
func (s *readyDemandSlice) CheckContext(ctx context.Context, c authz.Caller) (ok bool, err error) {
	s.d.checks++
	defer func() { s.d.add(ctx, "CheckContext", c, ok, err) }()
	if s.d.checks == s.d.failCheck {
		return false, readyContractPoison
	}
	ok, err = s.Slice.CheckContext(ctx, c)
	if s.d.checks == s.d.falseCheck {
		ok = false
	}
	if s.d.cancel != nil {
		s.d.cancel()
	}
	return
}
func (s *readyDemandSlice) HasGrant(ctx context.Context, subject authz.SubjectRef, scope authz.Scope, role string) (ok bool, err error) {
	defer func() { s.d.add(ctx, "HasGrant", []any{subject, scope, role}, ok, err) }()
	return s.Slice.HasGrant(ctx, subject, scope, role)
}
func (s *readyDemandSlice) Groups(ctx context.Context, subject authz.SubjectRef) (out []model.Ref, err error) {
	defer func() { s.d.add(ctx, "Groups", subject, out, err) }()
	return s.Slice.Groups(ctx, subject)
}
func (s *readyDemandSlice) TeamActive(ctx context.Context, ref model.Ref) (ok bool, err error) {
	defer func() { s.d.add(ctx, "TeamActive", ref, ok, err) }()
	return s.Slice.TeamActive(ctx, ref)
}
func (s *readyDemandSlice) ProjectActive(ctx context.Context, ref model.Ref) (ok bool, err error) {
	defer func() { s.d.add(ctx, "ProjectActive", ref, ok, err) }()
	return s.Slice.ProjectActive(ctx, ref)
}
func (s *readyDemandSlice) ProjectForBead(ctx context.Context, ref model.Ref) (out authz.ProjectBinding, err error) {
	defer func() { s.d.add(ctx, "ProjectForBead", ref, out, err) }()
	return s.Slice.ProjectForBead(ctx, ref)
}

func ReadyPreparationContract(t *testing.T, c *Composition, by model.PrincipalRef, refs []model.Ref) {
	t.Helper()
	background := context.Background()
	caller, err := c.Checker.callerFor(background, by)
	if err != nil {
		t.Fatal(err)
	}
	base := authz.FilterTemplate{Namespace: c.Namespace, Request: authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.ready"}, Permit: authz.FilterExpr{Op: authz.True}, Forbid: authz.FilterExpr{Op: authz.False}}
	cases := []struct {
		name   string
		mutate func(*authz.FilterTemplate)
		want   []int
	}{
		{"permit", func(*authz.FilterTemplate) {}, []int{0, 1, 2}},
		{"deny", func(t *authz.FilterTemplate) { t.Permit.Op = authz.False }, nil},
		{"restriction_short_circuit", func(t *authz.FilterTemplate) {
			t.Restrictions = []authz.FilterExpr{{Op: authz.False}, {Op: authz.ActiveProject}}
		}, nil},
		{"forbid_before_permit", func(t *authz.FilterTemplate) {
			t.Forbid.Op = authz.True
			t.Permit = authz.FilterExpr{Op: authz.ActiveProject}
		}, nil},
		{"refs_intersection", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Caveats = map[string]any{"refs": []model.Ref{refs[2], refs[0]}}
		}, []int{0, 2}},
		{"refs_empty", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Caveats = map[string]any{"refs": []model.Ref{}}
		}, nil},
		{"refs_null", func(t *authz.FilterTemplate) { t.Request.Caller.Principal.Caveats = map[string]any{"refs": nil} }, nil},
		{"refs_malformed", func(t *authz.FilterTemplate) { t.Request.Caller.Principal.Caveats = map[string]any{"refs": "invalid"} }, nil},
		{"refs_invalid_arm", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Caveats = map[string]any{"refs": []model.Ref{{ID: "bad", Branch: model.Natural64(1)}}}
		}, nil},
		{"actions_read", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Caveats = map[string]any{"actions": []string{"read"}}
		}, []int{0, 1, 2}},
		{"actions_modify", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Caveats = map[string]any{"actions": []string{"modify"}}
		}, nil},
		{"actions_null", func(t *authz.FilterTemplate) { t.Request.Caller.Principal.Caveats = map[string]any{"actions": nil} }, nil},
		{"unknown_caveat", func(t *authz.FilterTemplate) { t.Request.Caller.Principal.Caveats = map[string]any{"unknown": true} }, nil},
		{"invalid_caller", func(t *authz.FilterTemplate) { t.Request.Caller.Principal.ID = "" }, nil},
		{"invalid_kind_storage_bytes", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Kind = "user\x00"
			t.Permit = authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"}
		}, nil},
		{"invalid_sponsor_storage_bytes", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user\x00", ID: "sponsor"}}
			t.Permit = authz.FilterExpr{Op: authz.SponsorKind, Kind: "user"}
		}, nil},
		{"invalid_sponsor", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal.Sponsor = &model.PrincipalRef{Kind: "user", ID: "sponsor"}
		}, nil},
		{"caller_sponsor_intersection", func(t *authz.FilterTemplate) {
			t.Request.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "agent", Caveats: map[string]any{"refs": []model.Ref{refs[0], refs[1]}}, Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor", Caveats: map[string]any{"refs": []model.Ref{refs[1], refs[2]}}}}
		}, []int{1}},
	}
	// Each independent reader is pinned to the same real source. The trace
	// includes exact arguments, outputs/errors and context identity, including
	// per-ref Get/Observe dependencies, not only a driver call count.
	run := func(t *testing.T, template authz.FilterTemplate, replay bool, configure func(*readyDemand)) ([]bool, []string, error) {
		t.Helper()
		ctx, cancel := context.WithCancel(background)
		defer cancel()
		reader, _, closeReader, e := c.Service.current(ctx, nil)
		if e != nil {
			t.Fatal(e)
		}
		defer closeReader()
		s, e := c.Checker.load(ctx, reader, template.Request.Caller)
		if e != nil {
			t.Fatal(e)
		}
		d := &readyDemand{ctx: ctx}
		if configure != nil {
			configure(d)
		}
		s.snapshot = &readyDemandSnapshot{Snapshot: reader, d: d}
		traced := &readyDemandSlice{Slice: s, d: d}
		var bits []bool
		for _, ref := range refs {
			r := authz.Resource{Kind: authz.Bead, Ref: ref}
			var bit bool
			if replay {
				bit, e = template.ReplayReadPreparation(ctx, traced, r)
			} else {
				bit, e = template.Match(ctx, traced, r)
			}
			if e != nil {
				return bits, d.events, e
			}
			bits = append(bits, bit)
		}
		return bits, d.events, nil
	}
	content, err := c.Service.readyComplete(background, by, service.ReadyRequest{Project: "project", Limit: 1000})
	if err != nil || len(content.Items) != len(refs) {
		t.Fatalf("actual demand/control content: %d %v", len(content.Items), err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			template := base
			tc.mutate(&template)
			reference, trace, refErr := run(t, template, false, nil)
			prepared, replayTrace, replayErr := run(t, template, true, nil)
			if fmt.Sprint(refErr) != fmt.Sprint(replayErr) || !reflect.DeepEqual(trace, replayTrace) {
				t.Fatalf("reference demand changed:\nreference=%v %v\nreplay=%v %v", trace, refErr, replayTrace, replayErr)
			}
			if refErr != nil {
				t.Fatal(refErr)
			}
			var got []int
			for i, bit := range reference {
				if bit {
					got = append(got, i)
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("fixture membership oracle: got=%v want=%v", got, tc.want)
			}
			if readySQLCompatible(template, nil) {
				reader, _, closeReader, e := c.Service.current(background, nil)
				if e != nil {
					t.Fatal(e)
				}
				defer closeReader()
				rows := make([]driver.ReadCandidate, len(refs))
				for i, ref := range refs {
					rows[i] = driver.ReadCandidate{Ordinal: i, Ref: ref, Prepared: prepared[i]}
				}
				selection := &driver.ReadSelection{View: reader.Snapshot(), Candidates: rows, Predicate: readyStoragePredicate(template), PrincipalKind: template.Request.Caller.Principal.Kind, Limit: 1000}
				if sponsor := template.Request.Caller.Principal.Sponsor; sponsor != nil {
					selection.SponsorKind = sponsor.Kind
				}
				page, e := reader.Query(background, driver.Query{ReadSelection: selection})
				if e != nil || page.ReadSelection == nil {
					t.Fatalf("real selection: %+v %v", page, e)
				}
				if page.ReadSelection.Total != len(tc.want) || !reflect.DeepEqual(append([]int{}, page.ReadSelection.Ordinals...), append([]int{}, tc.want...)) {
					t.Fatalf("reference/storage mismatch: %+v want=%v", page.ReadSelection, tc.want)
				}
			}
			// Exercise the actual adapter regardless of compatibility routing. A
			// fallback must preserve the complete reference outcome; skipping
			// SQL alone is not evidence of a correct denial or availability.
			original := c.Checker.chain
			c.Checker.chain = authz.Chain{Namespace: c.Namespace, Rules: []authz.RuleSet{readyContractRules{namespace: c.Namespace, permit: template.Permit, forbid: template.Forbid, restrictions: template.Restrictions}}}
			guarded, guardErr := c.Service.filterReadyPage(background, template.Request.Caller.Principal, 1000, nil, content)
			referencePage, referenceErr := c.Service.filterPage(background, template.Request.Caller.Principal, "bead.ready", 1000, nil, content, false)
			c.Checker.chain = original
			// The caller/Compile boundary may reject an invalid principal
			// before Match. Preserve that exact existing refusal as well as
			// Match's direct denial, rather than inventing an empty success.
			if fmt.Sprint(guardErr) != fmt.Sprint(referenceErr) || fmt.Sprintf("%T", guardErr) != fmt.Sprintf("%T", referenceErr) || !reflect.DeepEqual(guarded, referencePage) {
				t.Fatalf("actual guard/reference outcome changed: new=%+v %v reference=%+v %v", guarded, guardErr, referencePage, referenceErr)
			}
			if guardErr != nil {
				if len(tc.want) != 0 || len(guarded.Items) != 0 || guarded.Next != nil {
					t.Fatal("invalid identity disclosed a result")
				}
				t.Logf("READY_REFERENCE_INPUT_REFUSAL case=%s new_error=%v reference_error=%v", tc.name, guardErr, referenceErr)
				return
			}
			if len(guarded.Items) != len(tc.want) || guarded.Next != nil {
				t.Fatalf("actual guard changed complete reference outcome: items=%d want=%v next=%v", len(guarded.Items), tc.want, guarded.Next)
			}
			for i, ordinal := range tc.want {
				if guarded.Items[i].ID != refs[ordinal].ID {
					t.Fatal("actual guard changed authorized order")
				}
			}
			for _, event := range trace {
				if !strings.Contains(event, "context_same=true") {
					t.Fatal("context replaced:", event)
				}
			}
			t.Logf("READY_REFERENCE_DEMAND case=%s resources=%d events=%d reference=%v prepared=%v trace=%v", tc.name, len(refs), len(trace), reference, prepared, trace)
		})
	}
	t.Run("each_later_dependency_error", func(t *testing.T) {
		for _, kind := range []string{"record", "observe", "context"} {
			last := 4
			if kind == "context" {
				last = 1
			}
			for n := 1; n <= last; n++ {
				t.Run(fmt.Sprintf("%s_%d", kind, n), func(t *testing.T) {
					configure := func(d *readyDemand) {
						switch kind {
						case "record":
							d.failRecord = 4 + n
						case "observe":
							d.failObserve = 4 + n
						case "context":
							d.failCheck = 2
						}
					}
					_, a, ae := run(t, base, false, configure)
					_, b, be := run(t, base, true, configure)
					if !errors.Is(ae, readyContractPoison) || !errors.Is(be, readyContractPoison) || !reflect.DeepEqual(a, b) {
						t.Fatalf("later source failure/demand changed: %v %v\n%v\n%v", ae, be, a, b)
					}
					t.Logf("READY_LATER_SOURCE_ERROR kind=%s index=%d events=%d error=%v", kind, n, len(a), ae)
				})
			}
		}
	})
	t.Run("false_later_context", func(t *testing.T) {
		config := func(d *readyDemand) { d.falseCheck = 2 }
		a, at, ae := run(t, base, false, config)
		b, bt, be := run(t, base, true, config)
		if ae != nil || be != nil || !reflect.DeepEqual(a, []bool{true, false, true}) || !reflect.DeepEqual(a, b) || !reflect.DeepEqual(at, bt) {
			t.Fatalf("false context changed: %v %v %v %v", a, b, ae, be)
		}
	})
	t.Run("cancellation_after_preparation", func(t *testing.T) {
		// Cancellation is delivered by the underlying context check. Both paths
		// must still enter the same restriction/forbid/permit evaluation point.
		for _, stage := range []string{"restriction", "forbid", "permit"} {
			t.Run(stage, func(t *testing.T) {
				template := base
				if stage == "restriction" {
					template.Restrictions = []authz.FilterExpr{{Op: authz.True}}
				}
				if stage == "permit" {
					template.Forbid = authz.FilterExpr{Op: authz.Any}
				}
				runCanceled := func(replay bool) ([]string, error) {
					ctx, cancel := context.WithCancel(background)
					defer cancel()
					r, _, closeReader, e := c.Service.current(ctx, nil)
					if e != nil {
						t.Fatal(e)
					}
					defer closeReader()
					s, e := c.Checker.load(ctx, r, caller)
					if e != nil {
						t.Fatal(e)
					}
					d := &readyDemand{ctx: ctx, cancel: cancel}
					s.snapshot = &readyDemandSnapshot{Snapshot: r, d: d}
					traced := &readyDemandSlice{Slice: s, d: d}
					if replay {
						_, e = template.ReplayReadPreparation(ctx, traced, authz.Resource{Kind: authz.Bead, Ref: refs[0]})
					} else {
						_, e = template.Match(ctx, traced, authz.Resource{Kind: authz.Bead, Ref: refs[0]})
					}
					return d.events, e
				}
				a, ae := runCanceled(false)
				b, be := runCanceled(true)
				if !errors.Is(ae, context.Canceled) || !errors.Is(be, context.Canceled) || !reflect.DeepEqual(a, b) {
					t.Fatalf("cancellation changed demand/error: %v %v", ae, be)
				}
			})
		}
	})
}

type readyContractRules struct {
	namespace      authz.Namespace
	permit, forbid authz.FilterExpr
	restrictions   []authz.FilterExpr
}

func (r readyContractRules) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{Namespace: r.namespace, Request: q, Permit: r.permit, Forbid: r.forbid, Restrictions: r.restrictions}, nil
}
func (r readyContractRules) Eval(ctx context.Context, s authz.Slice, qs []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(qs))
	for i, q := range qs {
		template, _ := r.Compile(authz.ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method})
		ok, e := template.Match(ctx, s, q.Resource)
		if e != nil {
			return nil, e
		}
		out[i].Effect = authz.Deny
		if ok {
			out[i].Effect = authz.Allow
		}
	}
	return out, nil
}

// This wrapper delegates all original capabilities. Only named test failures
// and a one-shot post-selection race alter a call, never successful membership.
type readyContractStore struct {
	driver.Store
	mu                  sync.Mutex
	selections, plugins int
	failPlugin          int
	failGet             bool
	afterSelection      func()
	partial             bool
}
type readyContractReader struct {
	driver.Reader
	driver.AllocatorReader
	owner *readyContractStore
}

func (s *readyContractStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, e := s.Store.Read(ctx, v)
	if e != nil {
		return nil, e
	}
	return &readyContractReader{Reader: r, AllocatorReader: r.(driver.AllocatorReader), owner: s}, nil
}
func (r *readyContractReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	r.owner.mu.Lock()
	fail := r.owner.failGet
	r.owner.mu.Unlock()
	if fail {
		return nil, readyContractPoison
	}
	return r.Reader.Get(ctx, refs)
}
func (r *readyContractReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r.owner.mu.Lock()
	fail := false
	for _, key := range keys {
		if key.Kind == driver.LockSentinel && key.Sentinel == driver.SentinelPlugins {
			r.owner.plugins++
			fail = fail || r.owner.plugins == r.owner.failPlugin
		}
	}
	r.owner.mu.Unlock()
	if fail {
		return nil, readyContractPoison
	}
	return r.Reader.Observe(ctx, keys)
}
func (r *readyContractReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	p, e := r.Reader.Query(ctx, q)
	if q.ReadSelection != nil {
		r.owner.mu.Lock()
		r.owner.selections++
		hook := r.owner.afterSelection
		r.owner.afterSelection = nil
		partial := r.owner.partial
		r.owner.mu.Unlock()
		if hook != nil {
			hook()
		}
		if partial && p.ReadSelection != nil && len(p.ReadSelection.Ordinals) > 0 {
			p.ReadSelection.Ordinals = p.ReadSelection.Ordinals[:len(p.ReadSelection.Ordinals)-1]
		}
	}
	return p, e
}

func ReadyReferenceAndFenceContract(t *testing.T, c *Composition, by model.PrincipalRef, replace func(), project string) {
	t.Helper()
	ctx := context.Background()
	originalChain, originalStore := c.Checker.chain, c.Checker.store
	defer func() { c.Checker.chain = originalChain; c.Checker.store = originalStore }()
	page, e := c.Service.readyComplete(ctx, by, service.ReadyRequest{Project: project, Limit: 1000})
	if e != nil || len(page.Items) < 3 {
		t.Fatalf("actual complete reference fixture: %d %v", len(page.Items), e)
	}
	observed := &readyContractStore{Store: originalStore}
	c.Checker.store = observed
	for _, tc := range []struct {
		name  string
		expr  authz.FilterExpr
		allow bool
	}{
		{"active_project", authz.FilterExpr{Op: authz.ActiveProject}, true},
		{"team_involvement", authz.FilterExpr{Op: authz.TeamInvolvement, Subject: authz.CallerSubject}, true},
		{"direct_grant", authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.Checker.chain = authz.Chain{Namespace: c.Namespace, Rules: []authz.RuleSet{readyContractRules{namespace: c.Namespace, permit: tc.expr, forbid: authz.FilterExpr{Op: authz.False}}}}
			got, err := c.Service.filterReadyPage(ctx, by, 2, nil, page)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.allow {
				want = 2
			}
			if len(got.Items) != want || (got.Next != nil) != tc.allow || observed.selections != 0 {
				t.Fatalf("valid relational template lost complete reference behavior: items=%d next=%v selections=%d", len(got.Items), got.Next, observed.selections)
			}
			if tc.allow {
				next, err := c.Service.filterReadyPage(ctx, by, 1000, got.Next, page)
				if err != nil || len(next.Items) != len(page.Items)-2 || next.Next != nil {
					t.Fatalf("reference tail lost: %d %v", len(next.Items), err)
				}
			}
		})
	}
	t.Run("poison_project_short_circuits", func(t *testing.T) {
		observed.failGet = true
		for _, kind := range []string{"restriction", "forbid", "permit_or"} {
			rule := readyContractRules{namespace: c.Namespace, permit: authz.FilterExpr{Op: authz.ActiveProject}, forbid: authz.FilterExpr{Op: authz.False}}
			switch kind {
			case "restriction":
				rule.restrictions = []authz.FilterExpr{{Op: authz.False}}
			case "forbid":
				rule.forbid.Op = authz.True
			case "permit_or":
				rule.permit = authz.Or(authz.FilterExpr{Op: authz.False}, authz.FilterExpr{Op: authz.ActiveProject})
			}
			c.Checker.chain = authz.Chain{Namespace: c.Namespace, Rules: []authz.RuleSet{rule}}
			got, err := c.Service.filterReadyPage(ctx, by, 1, nil, page)
			if kind == "permit_or" {
				if (err == nil || !strings.Contains(err.Error(), readyContractPoison.Error())) || len(got.Items) != 0 {
					t.Fatalf("demanded project error suppressed: %+v %v", got, err)
				}
			} else if err != nil || len(got.Items) != 0 {
				t.Fatalf("short-circuit demanded poison source: %s %v", kind, err)
			}
		}
		// A malformed projects list refuses without touching targetProject. A
		// valid present-empty list still demands the reference project lookup.
		c.Checker.chain = originalChain
		for _, value := range []any{"bad", nil, []model.Ref{}} {
			scoped := by
			scoped.Caveats = map[string]any{"projects": value}
			got, err := c.Service.filterReadyPage(ctx, scoped, 1, nil, page)
			if _, valid := value.([]model.Ref); valid {
				if (err == nil || !strings.Contains(err.Error(), readyContractPoison.Error())) || len(got.Items) != 0 {
					t.Fatal("empty project caveat suppressed reference read error")
				}
			} else if err != nil || len(got.Items) != 0 {
				t.Fatalf("malformed project caveat changed short circuit: %v", err)
			}
		}
		observed.failGet = false
	})
	c.Checker.chain = originalChain
	t.Run("later_resource_error_before_page", func(t *testing.T) {
		observed.plugins, observed.failPlugin, observed.selections = 0, 2, 0
		got, err := c.Service.filterReadyPage(ctx, by, 1, nil, page)
		var modelErr *model.Error
		if err == nil || !errors.As(err, &modelErr) || modelErr.Data.Code != "authz_unavailable" || len(got.Items) != 0 || observed.selections != 0 {
			t.Fatalf("later resource failure became early page success: %+v %v selections=%d", got, err, observed.selections)
		}
		observed.failPlugin = 0
	})
	t.Run("partial_selector_refuses", func(t *testing.T) {
		observed.partial = true
		defer func() { observed.partial = false }()
		got, err := c.Service.filterReadyPage(ctx, by, 1, nil, page)
		if err == nil || len(got.Items) != 0 || got.Next != nil {
			t.Fatalf("partial selector accepted: %+v %v", got, err)
		}
	})
	t.Run("fresh_authority_fence", func(t *testing.T) {
		observed.afterSelection = replace
		got, err := c.Service.filterReadyPage(ctx, by, 1, nil, page)
		if err == nil || len(got.Items) != 0 || got.Next != nil || observed.afterSelection != nil {
			t.Fatalf("current authority race escaped final fence: %+v %v", got, err)
		}
	})
}

type readyCheckOnlyLink struct {
	mu     sync.Mutex
	mode   string
	calls  int
	batch  []authz.CheckRequest
	budget time.Duration
}

func (l *readyCheckOnlyLink) Filter(authz.ListRequest) (authz.FilterExpr, error) {
	return authz.FilterExpr{}, authz.ErrCheckOnly
}
func (l *readyCheckOnlyLink) Check(ctx context.Context, _ authz.Slice, qs []authz.CheckRequest) ([]authz.Step, error) {
	if len(qs) > 0 && qs[0].Method == "bead.ready" {
		l.mu.Lock()
		l.calls++
		l.batch = append([]authz.CheckRequest(nil), qs...)
		deadline, _ := ctx.Deadline()
		l.budget = time.Until(deadline)
		mode := l.mode
		l.mu.Unlock()
		if mode == "deadline" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if mode == "error" {
			return nil, readyContractPoison
		}
		if mode == "cardinality" {
			return nil, nil
		}
		if mode == "effect" {
			out := make([]authz.Step, len(qs))
			for i := range out {
				out[i].Effect = "invalid"
			}
			return out, nil
		}
	}
	out := make([]authz.Step, len(qs))
	for i := range out {
		out[i].Effect = authz.Allow
	}
	return out, nil
}
func ReadyCheckOnlyContract(t *testing.T, c *Composition, by model.PrincipalRef, corpus int) {
	t.Helper()
	ctx := context.Background()
	original := c.Checker.chain
	defer func() { c.Checker.chain = original }()
	full, err := c.Service.readyComplete(ctx, by, service.ReadyRequest{Project: "project", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	above := corpus > listPostfilterCap
	if len(full.Items) != corpus {
		t.Fatalf("real check-only corpus=%d want=%d", len(full.Items), corpus)
	}
	modes := []string{"allow", "cardinality", "effect", "error", "deadline"}
	if corpus == listPostfilterCap {
		modes = []string{"allow"}
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			link := &readyCheckOnlyLink{mode: mode}
			c.Checker.chain = authz.Chain{Namespace: c.Namespace, Links: []authz.Link{link}, Budget: original.Budget}
			if _, err := c.Checker.chain.Compile(authz.ListRequest{Caller: authz.Caller{Principal: by}, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.ready"}); !errors.Is(err, authz.ErrCheckOnly) {
				t.Fatal("fixture is not genuinely check-only", err)
			}
			call, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			started := time.Now()
			got, err := c.Service.filterReadyPage(call, by, 1, nil, full)
			link.mu.Lock()
			calls, batch, budget := link.calls, append([]authz.CheckRequest(nil), link.batch...), link.budget
			link.mu.Unlock()
			t.Logf("READY_GENUINE_CHECK_ONLY corpus=%d mode=%s full_calls=%d actual_batch=%d component_budget=%s elapsed=%s error=%v", len(full.Items), mode, calls, len(batch), budget, time.Since(started), err)
			if above {
				var failure *model.Error
				if !errors.As(err, &failure) || failure.Data.Code != "authz_postfilter_cap" || calls != 0 || len(got.Items) != 0 || got.Next != nil {
					t.Fatalf("genuine full-set cap changed: %+v %v calls=%d", got, err, calls)
				}
				return
			}
			if calls != 1 || len(batch) != corpus {
				t.Fatalf("check-only did not check one complete set: calls=%d size=%d", calls, len(batch))
			}
			for i, q := range batch {
				if q.Resource.Ref.ID != full.Items[i].ID || q.Method != "bead.ready" || q.Action != authz.Read || q.Caller.Principal.ID != by.ID {
					t.Fatal("check-only original request/order changed at", i)
				}
			}
			if budget <= 0 || budget > 50*time.Millisecond {
				t.Fatalf("existing default component ceiling changed: %s", budget)
			}
			if mode == "allow" {
				if err != nil || len(got.Items) != 1 || got.Items[0].ID != full.Items[0].ID || got.Next == nil || got.Next.Offset != 1 {
					t.Fatalf("exact-cap allowed sequence: %+v %v", got, err)
				}
			} else if err == nil || len(got.Items) != 0 || got.Next != nil {
				t.Fatalf("check-only error became partial success: %s %+v %v", mode, got, err)
			}
		})
	}
}

func ReadyProjectReferenceContract(t *testing.T, c *Composition, by model.PrincipalRef, expected map[string][]string, revoke func()) {
	t.Helper()
	ctx := context.Background()
	original, store := c.Checker.chain, c.Checker.store
	defer func() { c.Checker.chain = original; c.Checker.store = store }()
	observed := &readyContractStore{Store: store}
	c.Checker.store = observed
	run := func(t *testing.T, name string, expr authz.FilterExpr, principal model.PrincipalRef, allowed map[string]bool) {
		t.Helper()
		if name == "no_plugins" {
			c.Checker.chain = authz.Chain{Namespace: c.Namespace}
		} else {
			c.Checker.chain = authz.Chain{Namespace: c.Namespace, Rules: []authz.RuleSet{readyContractRules{namespace: c.Namespace, permit: expr, forbid: authz.FilterExpr{Op: authz.False}}}}
		}
		for _, project := range []string{"project", "inactive", "other", "unregistered"} {
			t.Run(project, func(t *testing.T) {
				var got []string
				var position *service.PagePosition
				for turn := 0; turn < 10; turn++ {
					p, err := c.Service.Ready(ctx, principal, service.ReadyRequest{Project: project, Limit: 1, Position: position})
					if err != nil {
						t.Fatal(err)
					}
					for _, item := range p.Items {
						got = append(got, item.ID)
					}
					position = p.Next
					if position == nil {
						break
					}
				}
				var want []string
				if allowed[project] {
					want = expected[project]
				}
				if !reflect.DeepEqual(got, want) || position != nil || observed.selections != 0 {
					t.Fatalf("complete valid-policy reference outcome changed: case=%s got=%v want=%v selections=%d", name, got, want, observed.selections)
				}
			})
		}
	}
	for _, tc := range []struct {
		name    string
		expr    authz.FilterExpr
		allowed map[string]bool
	}{
		{"active", authz.FilterExpr{Op: authz.ActiveProject}, map[string]bool{"project": true, "other": true}},
		{"team", authz.FilterExpr{Op: authz.TeamInvolvement, Subject: authz.CallerSubject}, map[string]bool{"project": true}},
		{"direct", authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"}, map[string]bool{"other": true}},
		{"no_plugins", authz.FilterExpr{Op: authz.True}, map[string]bool{"project": true, "inactive": true, "other": true, "unregistered": true}},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, tc.name, tc.expr, by, tc.allowed) })
	}
	scoped := by
	scoped.Caveats = map[string]any{"projects": []model.Ref{model.RefByID("project", model.Natural64(0)), model.RefByID("inactive", model.Natural64(0))}}
	t.Run("project_caveat_including_inactive", func(t *testing.T) {
		run(t, "project_caveat", authz.FilterExpr{Op: authz.True}, scoped, map[string]bool{"project": true, "inactive": true})
	})
	revoke()
	for _, tc := range []struct {
		name string
		expr authz.FilterExpr
	}{
		{"revoked_team", authz.FilterExpr{Op: authz.TeamInvolvement, Subject: authz.CallerSubject}},
		{"revoked_direct", authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"}},
		{"inactive_current_projects", authz.FilterExpr{Op: authz.ActiveProject}},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, tc.name, tc.expr, by, nil) })
	}
}

func ReadyCompleteContract(t *testing.T, c *Composition, by model.PrincipalRef, project string) service.BeadPage {
	t.Helper()
	page, err := c.Service.readyComplete(context.Background(), by, service.ReadyRequest{Project: project, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return page
}
func ReadyFilterContract(ctx context.Context, c *Composition, by model.PrincipalRef, content service.BeadPage, limit int, position *service.PagePosition) (service.BeadPage, error) {
	return c.Service.filterReadyPage(ctx, by, limit, position, content)
}
