package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func hRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func hBody(id, project, parent, status string) []byte {
	// A known fixture creation instant, independent of seed and version clocks.
	body := map[string]any{"id": id, "type": "issue", "project": project, "createdAt": "2026-11-03T00:00:00Z", "fields": map[string]any{"title": id, "status": status, "priority": 0}}
	if parent != "" {
		body["parent"] = hRef(parent)
	}
	data, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return data
}

type hFixture struct {
	store driver.Store
	lease driver.ActiveLease
	keys  *authnapikey.Provider
	facts *MutableFacts
	base  authzfixed.Facts
	app   *Composition
}

func newHFixture(t *testing.T, enabled bool) *hFixture {
	t.Helper()
	return newHFixtureStore(t, enabled, memory.New())
}

func newHFixtureStore(t *testing.T, enabled bool, store driver.Store) *hFixture {
	t.Helper()
	ctx := context.Background()
	f := &hFixture{store: store}
	t.Cleanup(func() { _ = f.store.Close() })
	var err error
	f.lease, err = f.store.Lease(ctx, "hierarchy-authz", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(ctx) })
	var changes []conformance.FixtureChange
	for _, row := range []struct{ id, project, parent, status string }{
		{"child", "child-project", "old", "todo"},
		{"old", "old-project", "", "todo"},
		{"new", "parent-project", "", "todo"},
		{"visible", "child-project", "new", "todo"},
		{"hidden-one", "secret-project", "new", "secret-status-one"},
		{"hidden-two", "secret-project", "new", "secret-status-two"},
	} {
		changes = append(changes, conformance.FixtureChange{Ref: hRef(row.id), Kind: "bead", Op: model.OpCreate, Body: hBody(row.id, row.project, row.parent, row.status)})
	}
	if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: hRef("authority")}
	f.base.Namespace = ns
	for _, project := range []string{"child-project", "parent-project", "old-project", "secret-project"} {
		f.base.Projects = append(f.base.Projects, authzfixed.Project{Ref: hRef(project), Name: project, InstallationID: "fixed", Active: true})
	}
	for _, project := range []string{"child-project", "parent-project"} {
		f.base.Grants = append(f.base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: hRef(project)}, Role: "involved"})
	}
	f.facts, err = NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	f.app, err = New(ctx, Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: f.facts, Namespace: ns, DefaultProject: "child-project", Hierarchy: enabled})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func hAgent() model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}
}

type hSnapshot struct {
	driver.Reader
	mu          sync.Mutex
	gets        map[string]int
	locks       []driver.LockKey
	failGet     string
	failObserve string
}

func (f *hFixture) snapshot(t *testing.T) *hSnapshot {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return &hSnapshot{Reader: r, gets: map[string]int{}}
}

func (s *hSnapshot) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	s.mu.Lock()
	for _, ref := range refs {
		s.gets[ref.ID]++
		if ref.ID == s.failGet {
			s.mu.Unlock()
			return nil, errors.New("test source unavailable")
		}
	}
	s.mu.Unlock()
	return s.Reader.Get(ctx, refs)
}

func (s *hSnapshot) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	s.mu.Lock()
	s.locks = append(s.locks, keys...)
	for _, key := range keys {
		encoded, err := driver.EncodeLockKey(key)
		if err == nil && encoded == s.failObserve {
			s.mu.Unlock()
			return nil, errors.New("test dependency unavailable")
		}
	}
	s.mu.Unlock()
	return s.Reader.Observe(ctx, keys)
}

func (s *hSnapshot) observed(key driver.LockKey) bool {
	for _, value := range s.locks {
		if reflect.DeepEqual(value, key) {
			return true
		}
	}
	return false
}

func hMove(parent *model.Ref) port.Change {
	destination := ""
	if parent != nil && parent.IsID() {
		destination = parent.ID
	}
	return port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: hRef("child")},
		Op:     model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: hRef("child"), Parent: parent}},
		Before: hBody("child", "child-project", "old", "todo"), After: hBody("child", "child-project", destination, "todo")}
}

func hCreate(id, key, project string, parent *model.Ref, resolvedParent string) port.Change {
	return port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: hRef(id)},
		Op:    model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Key: key, Type: "issue", Project: project, Parent: parent, Fields: map[string]any{"title": id}}},
		After: hBody(id, project, resolvedParent, "todo")}
}

func hProposal(by model.PrincipalRef, method string, changes ...port.Change) port.ProposedChange {
	return port.ProposedChange{Principal: by, Meta: model.WriteMeta{Method: method, Project: "child-project"}, Changes: changes}
}

func requireHCode(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != code {
		t.Fatalf("error = %v, want %s", err, code)
	}
}

func TestHierarchyMoveRequiresChildAndNewParentOnly(t *testing.T) {
	for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter, port.AuthorizeReplay} {
		t.Run(string(stage), func(t *testing.T) {
			f := newHFixture(t, true)
			s := f.snapshot(t)
			parent := hRef("new")
			proposal := hProposal(hAgent(), "bead.reparent", hMove(&parent))
			if stage == port.AuthorizeReplay {
				proposal.Changes[0].Before, proposal.Changes[0].After = nil, nil
			}
			if err := f.app.Checker.Authorize(context.Background(), stage, proposal, s); err != nil {
				t.Fatal(err)
			}
			if s.gets["child"] == 0 || s.gets["new"] == 0 || s.gets["old"] != 0 {
				t.Fatalf("source checks = %v; old parent must require no permission/read", s.gets)
			}
			fact, _ := driver.NewReadFact(authz.ACLDependency(f.base.Namespace).Key)
			if !s.observed(driver.LockKey{Kind: driver.LockReadFact, Fact: fact}) || !s.observed(driver.PluginsSentinelLock) || !s.observed(driver.OntologySentinelLock) {
				t.Fatal("authorization dependencies did not join the supplied snapshot")
			}
		})
	}
	for _, missing := range []string{"child-project", "parent-project"} {
		t.Run("missing-"+missing, func(t *testing.T) {
			f := newHFixture(t, true)
			facts := f.base
			facts.Grants = nil
			for _, grant := range f.base.Grants {
				if grant.Scope.Ref.ID != missing {
					facts.Grants = append(facts.Grants, grant)
				}
			}
			if err := f.facts.Replace(facts); err != nil {
				t.Fatal(err)
			}
			parent := hRef("new")
			err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "bead.reparent", hMove(&parent)), f.snapshot(t))
			requireHCode(t, err, "forbidden")
		})
	}
	f := newHFixture(t, true)
	if err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "bead.reparent", hMove(nil)), f.snapshot(t)); err != nil {
		t.Fatalf("detach required old-parent permission: %v", err)
	}
}

func TestHierarchyOrderedCreatesAndFinalImages(t *testing.T) {
	f := newHFixture(t, true)
	alias, parent := model.RefByKey("epic"), hRef("batch-parent")
	epic := hCreate("batch-parent", "epic", "parent-project", nil, "")
	child := hCreate("batch-child", "", "child-project", &alias, parent.ID)
	proposal := hProposal(hAgent(), "tx.apply", epic, child)
	// Exercise the same detachment boundary as Writer callbacks: absent
	// Before images become explicit JSON null while source misses stay tracked.
	encoded, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &proposal); err != nil {
		t.Fatal(err)
	}
	s := f.snapshot(t)
	if err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, proposal, s); err != nil {
		t.Fatal(err)
	}
	if s.gets[parent.ID] == 0 || s.gets["batch-child"] == 0 {
		t.Fatal("batch-created IDs did not register tracked source misses")
	}
	for _, changes := range [][]port.Change{{child, epic}, {child}, {epic, epic}} {
		err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeBefore, hProposal(hAgent(), "tx.apply", changes...), f.snapshot(t))
		requireHCode(t, err, "forbidden")
	}
	// Writer callbacks use one stored Before and one final After per target,
	// even when a later operation reparents an earlier create.
	first := hCreate("batch-child", "task", "child-project", nil, "new")
	newParent := hRef("new")
	move := port.Change{Resource: first.Resource,
		Op: model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: model.RefByKey("task"), Parent: &newParent}}, After: first.After}
	if err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", first, move), f.snapshot(t)); err != nil {
		t.Fatalf("ordered create/reparent failed: %v", err)
	}
	for _, mutate := range []func(*port.Change){
		func(c *port.Change) { c.After = hBody("batch-child", "parent-project", parent.ID, "todo") },
		func(c *port.Change) { c.After = hBody("batch-child", "child-project", "old", "todo") },
		func(c *port.Change) { c.Before = child.After },
	} {
		bad := child
		mutate(&bad)
		err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", epic, bad), f.snapshot(t))
		requireHCode(t, err, "forbidden")
	}
}

func TestHierarchyFixedPolicyBatchesHundredOperationAuthorization(t *testing.T) {
	f := newHFixture(t, true)
	if !f.app.Checker.canBatchHierarchyChecks() {
		t.Fatal("fixed-policy hierarchy chain did not select aggregate authorization")
	}
	changes := make([]port.Change, 100)
	for i := range changes {
		id := fmt.Sprintf("batch-%03d", i)
		changes[i] = hCreate(id, "", "child-project", nil, "")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := f.app.Checker.AuthorizeHierarchy(ctx, port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", changes...), f.snapshot(t)); err != nil {
		t.Fatalf("100-operation fixed-policy batch exceeded the unchanged authorization deadline: %v", err)
	}
}

func TestHierarchyFixedPolicyCoalescesEquivalentPermissionRequests(t *testing.T) {
	caller := authz.Caller{Principal: hAgent(), VerificationID: "verification", CapturedEpoch: model.Natural64(1)}
	batch := &hierarchyCheckBatch{caller: caller}
	request := authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Project, Ref: hRef("child-project")}, Method: "tx.apply"}
	first := batch.add(request)
	if repeated := batch.add(request); repeated != first {
		t.Fatalf("equivalent fixed-policy request was not coalesced: first=%d repeated=%d", first, repeated)
	}
	if got, want := len(batch.requests), 1; got != want {
		t.Fatalf("coalesced request count = %d, want %d", got, want)
	}
	for _, edit := range []func(*authz.CheckRequest){
		func(q *authz.CheckRequest) { q.Resource.Ref = hRef("parent-project") },
		func(q *authz.CheckRequest) { q.Method = "bead.reparent" },
		func(q *authz.CheckRequest) { q.Caller.VerificationID = "different-verification" },
	} {
		changed := request
		edit(&changed)
		if got := batch.add(changed); got == first {
			t.Fatalf("non-equivalent request reused fixed-policy decision: %+v", changed)
		}
	}
	if got, want := len(batch.requests), 4; got != want {
		t.Fatalf("non-equivalent request count = %d, want %d", got, want)
	}
}

func TestHierarchyFixedPolicyObservesDependenciesOncePerPass(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			var store driver.Store
			if engine == "sqlite" {
				var err error
				store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: t.TempDir() + "/hierarchy.db"})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				store = memory.New()
			}
			f := newHFixtureStore(t, true, store)
			changes := make([]port.Change, 100)
			for i := range changes {
				changes[i] = hCreate(fmt.Sprintf("observed-%03d", i), "", "child-project", nil, "")
			}
			encodeKey := func(key driver.LockKey) string {
				t.Helper()
				encoded, err := driver.EncodeLockKey(key)
				if err != nil {
					t.Fatal(err)
				}
				return encoded
			}
			s := f.snapshot(t)
			started := time.Now()
			if err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", changes...), s); err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, key := range s.locks {
				counts[encodeKey(key)]++
			}
			t.Logf("100-operation authorization: engine=%s elapsed_ms=%.3f source_reads=%d dependency_observations=%d unique_dependencies=%d", engine, float64(time.Since(started))/float64(time.Millisecond), len(s.gets), len(s.locks), len(counts))
			// Preserve the original 205 identities: bead/closure for each
			// resource, context, ACL, plugins, ontology, and project grant.
			expected := map[string]bool{}
			expectKey := func(key driver.LockKey) {
				t.Helper()
				encoded := encodeKey(key)
				if expected[encoded] {
					t.Fatalf("duplicate expected dependency: %s", encoded)
				}
				expected[encoded] = true
			}
			for _, dependency := range []authz.Dependency{authz.ContextDependency(f.base.Namespace), authz.ACLDependency(f.base.Namespace)} {
				fact, err := driver.NewReadFact(dependency.Key)
				if err != nil {
					t.Fatal(err)
				}
				expectKey(driver.LockKey{Kind: driver.LockReadFact, Fact: fact})
			}
			expectKey(driver.PluginsSentinelLock)
			expectKey(driver.OntologySentinelLock)
			grant, err := authz.GrantDependency(f.base.Namespace, authz.Scope{Kind: authz.ProjectScope, Ref: hRef("child-project")})
			if err != nil {
				t.Fatal(err)
			}
			grantSentinel, err := driver.NewSentinel(grant.Key)
			if err != nil {
				t.Fatal(err)
			}
			expectKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: grantSentinel})
			for _, change := range changes {
				for _, prefix := range []string{"bead/", "closure/"} {
					sentinel, err := driver.NewSentinel(prefix + change.Resource.Ref.ID)
					if err != nil {
						t.Fatal(err)
					}
					expectKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
				}
			}
			if got, want := len(expected), 2*len(changes)+5; got != want {
				t.Fatalf("original dependency identities changed: got=%d want=%d", got, want)
			}
			// Facts.Load adds exactly the canonical durable source witness.
			rows, err := authzfixed.SelectionRows(f.base.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			sourceKey := encodeKey(driver.LockKey{Kind: driver.LockDerived, Derived: &rows.Source})
			if expected[sourceKey] {
				t.Fatal("facts source aliases an original dependency")
			}
			if got := counts[sourceKey]; got != 1 {
				t.Errorf("current-namespace facts source observations = %d, want 1", got)
			}
			for key := range expected {
				if counts[key] == 0 {
					t.Errorf("original dependency lost: %s", key)
				}
			}
			acl, _ := driver.NewReadFact(authz.ACLDependency(f.base.Namespace).Key)
			aclKey := encodeKey(driver.LockKey{Kind: driver.LockReadFact, Fact: acl})
			for key, count := range counts {
				if !expected[key] && key != sourceKey {
					t.Errorf("unexpected dependency: %s", key)
				}
				// FactsSource.Load and Checker.load separately fence the ACL.
				// The finite policy may observe each dependency only once more.
				limit := 1
				if key == aclKey {
					limit = 3
				}
				if count > limit {
					t.Errorf("snapshot dependency repeatedly read: key=%s observations=%d limit=%d", key, count, limit)
				}
			}
			if got, want := len(s.gets), len(changes); got != want {
				t.Errorf("source read identities = %d, want %d", got, want)
			}
			for _, change := range changes {
				if s.gets[change.Resource.Ref.ID] != 1 {
					t.Errorf("source absence was not tracked exactly once for %s", change.Resource.Ref.ID)
				}
				for _, prefix := range []string{"bead/", "closure/"} {
					sentinel, _ := driver.NewSentinel(prefix + change.Resource.Ref.ID)
					if !s.observed(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}) {
						t.Errorf("first-touch dependency lost: %s%s", prefix, change.Resource.Ref.ID)
					}
				}
			}
			t.Run("source_unavailable_before_resource_reads", func(t *testing.T) {
				failed := f.snapshot(t)
				failed.failObserve = sourceKey
				err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", changes...), failed)
				requireHCode(t, err, "authz_unavailable")
				if len(failed.gets) != 0 {
					t.Fatalf("resource reads preceded unavailable facts source: %v", failed.gets)
				}
				sourceAttempts := 0
				for _, key := range failed.locks {
					encoded, err := driver.EncodeLockKey(key)
					if err != nil {
						t.Fatal(err)
					}
					if encoded == sourceKey {
						sourceAttempts++
					}
				}
				if sourceAttempts != 1 {
					t.Fatalf("failed facts source observations = %d, want 1", sourceAttempts)
				}
				t.Logf("unavailable facts source: observations=%d resource_reads=%d", sourceAttempts, len(failed.gets))
			})
		})
	}
}

type hCountingLink struct{ calls atomic.Int32 }

func (l *hCountingLink) Check(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Step, error) {
	l.calls.Add(1)
	out := make([]authz.Step, len(requests))
	for i := range out {
		out[i].Effect = authz.NoDecision
	}
	return out, nil
}

func (*hCountingLink) Filter(authz.ListRequest) (authz.FilterExpr, error) {
	return authz.FilterExpr{Op: authz.False}, authz.ErrCheckOnly
}

func TestHierarchyCustomLinksRetainPerOperationFallback(t *testing.T) {
	f := newHFixture(t, true)
	link := &hCountingLink{}
	f.app.Checker.chain.Links = []authz.Link{link}
	if f.app.Checker.canBatchHierarchyChecks() {
		t.Fatal("custom-link hierarchy chain selected aggregate authorization")
	}
	changes := []port.Change{
		hCreate("custom-0", "", "child-project", nil, ""),
		hCreate("custom-1", "", "child-project", nil, ""),
		hCreate("custom-2", "", "child-project", nil, ""),
	}
	s := f.snapshot(t)
	if err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", changes...), s); err != nil {
		t.Fatalf("custom-link fallback refused a valid batch: %v", err)
	}
	if got, want := link.calls.Load(), int32(len(changes)); got != want {
		t.Fatalf("custom link was batched across operations: calls=%d want=%d", got, want)
	}
	// A custom component also keeps fresh first-touch reads per operation.
	pluginKey, _ := driver.EncodeLockKey(driver.PluginsSentinelLock)
	count := 0
	for _, key := range s.locks {
		encoded, _ := driver.EncodeLockKey(key)
		if encoded == pluginKey {
			count++
		}
	}
	if count <= 1 {
		t.Fatal("custom-link fallback reused fixed-only dependency observations")
	}
}

type hCountingRules struct{ calls atomic.Int32 }

func (r *hCountingRules) Eval(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	r.calls.Add(1)
	decisions := make([]authz.Decision, len(requests))
	for i := range decisions {
		decisions[i].Effect = authz.NoDecision
	}
	return decisions, nil
}

func (*hCountingRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func TestHierarchyCustomRulesRetainPerOperationFallback(t *testing.T) {
	f := newHFixture(t, true)
	rules := &hCountingRules{}
	f.app.Checker.chain.Rules = append(f.app.Checker.chain.Rules, rules)
	if f.app.Checker.canBatchHierarchyChecks() {
		t.Fatal("custom rules selected aggregate authorization")
	}
	changes := []port.Change{
		hCreate("custom-rule-0", "", "child-project", nil, ""),
		hCreate("custom-rule-1", "", "child-project", nil, ""),
		hCreate("custom-rule-2", "", "child-project", nil, ""),
	}
	if err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "tx.apply", changes...), f.snapshot(t)); err != nil {
		t.Fatal(err)
	}
	if got, want := rules.calls.Load(), int32(len(changes)); got != want {
		t.Fatalf("custom rules were batched across operations: calls=%d want=%d", got, want)
	}
}

func TestHierarchyFixedPolicyDependencyScopeIsFresh(t *testing.T) {
	f := newHFixture(t, true)
	s := f.snapshot(t)
	parent := hRef("new")
	proposal := hProposal(hAgent(), "bead.reparent", hMove(&parent))
	for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter, port.AuthorizeReplay} {
		if err := f.app.Checker.AuthorizeHierarchy(context.Background(), stage, proposal, s); err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
		// Reusing the Reader, even within the same stage, must not reuse an
		// earlier pass's successful dependency or hide a new source failure.
		s.failObserve, _ = driver.EncodeLockKey(driver.PluginsSentinelLock)
		err := f.app.Checker.AuthorizeHierarchy(context.Background(), stage, proposal, s)
		requireHCode(t, err, "authz_unavailable")
		s.failObserve = ""
	}
}

func TestHierarchyRefusalsDoNotBroadenFixedPolicy(t *testing.T) {
	newParent := hRef("new")
	for _, test := range []struct {
		name string
		edit func(*port.ProposedChange)
	}{
		{"missing-parent", func(p *port.ProposedChange) { r := hRef("absent"); p.Changes[0] = hMove(&r) }},
		{"self-parent", func(p *port.ProposedChange) { r := hRef("child"); p.Changes[0] = hMove(&r) }},
		{"project-move", func(p *port.ProposedChange) { p.Changes[0].Op.Reparent.Project = "parent-project" }},
		{"forged-before", func(p *port.ProposedChange) { p.Changes[0].Before = hBody("child", "parent-project", "old", "todo") }},
		{"forged-project", func(p *port.ProposedChange) { p.Changes[0].After = hBody("child", "parent-project", "new", "todo") }},
		{"unsupported-method", func(p *port.ProposedChange) { p.Meta.Method = "bead.update" }},
		{"unsupported-op", func(p *port.ProposedChange) {
			p.Changes[0].Op = model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: hRef("child")}}
		}},
		{"parent-caveat", func(p *port.ProposedChange) {
			p.Principal.Caveats = map[string]any{"projects": []model.Ref{hRef("child-project")}}
		}},
		{"child-caveat", func(p *port.ProposedChange) {
			p.Principal.Caveats = map[string]any{"projects": []model.Ref{hRef("parent-project")}}
		}},
		{"ref-caveat", func(p *port.ProposedChange) { p.Principal.Caveats = map[string]any{"refs": []model.Ref{hRef("child")}} }},
		{"action-caveat", func(p *port.ProposedChange) { p.Principal.Caveats = map[string]any{"actions": []string{"read"}} }},
		{"sponsor-caveat", func(p *port.ProposedChange) {
			p.Principal.Sponsor.Caveats = map[string]any{"projects": []model.Ref{hRef("child-project")}}
		}},
		{"User-write", func(p *port.ProposedChange) { p.Principal = model.PrincipalRef{Kind: "user", ID: "user:operator"} }},
		{"unsponsored-Agent", func(p *port.ProposedChange) { p.Principal.Sponsor = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHFixture(t, true)
			p := hProposal(hAgent(), "bead.reparent", hMove(&newParent))
			test.edit(&p)
			err := f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, p, f.snapshot(t))
			requireHCode(t, err, "forbidden")
		})
	}
	f := newHFixture(t, false)
	err := f.app.Checker.Authorize(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "bead.reparent", hMove(&newParent)), f.snapshot(t))
	requireHCode(t, err, "forbidden")
	f.app.Checker.hierarchy = true
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"new": true}, action: authz.Modify}}
	err = f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "bead.reparent", hMove(&newParent)), f.snapshot(t))
	requireHCode(t, err, "forbidden")
	f.app.Checker.chain.Links = nil
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"child": true}, action: authz.Modify, method: "bead.reparent"}}
	err = f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "bead.reparent", hMove(&newParent)), f.snapshot(t))
	requireHCode(t, err, "forbidden")
	f.app.Checker.chain.Links = nil
	s := f.snapshot(t)
	s.failGet = "new"
	err = f.app.Checker.AuthorizeHierarchy(context.Background(), port.AuthorizeAfter, hProposal(hAgent(), "bead.reparent", hMove(&newParent)), s)
	requireHCode(t, err, "authz_unavailable")
}

type hDeny struct {
	ids    map[string]bool
	action authz.Action
	method string
}

func (d hDeny) Check(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Step, error) {
	out := make([]authz.Step, len(requests))
	for i, req := range requests {
		out[i].Effect = authz.NoDecision
		if req.Action == d.action && d.ids[req.Resource.Ref.ID] && (d.method == "" || d.method == req.Method) {
			out[i] = authz.Step{Effect: authz.Deny, Reason: "test-specific restriction"}
		}
	}
	return out, nil
}

func (d hDeny) Filter(authz.ListRequest) (authz.FilterExpr, error) {
	return authz.FilterExpr{Op: authz.False}, authz.ErrCheckOnly
}

func TestHierarchyDisclosureAndVisibleOnlyCounts(t *testing.T) {
	f := newHFixture(t, true)
	by := model.PrincipalRef{Kind: "user", ID: "user:viewer"}
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"new": true}, action: authz.Read}}
	s := f.snapshot(t)
	disclosure, err := f.app.Checker.ParentDisclosure(context.Background(), by, hRef("visible"), s)
	if err != nil || !disclosure.Exists || disclosure.Ref != nil {
		t.Fatalf("hidden-parent disclosure = %+v, %v", disclosure, err)
	}
	data, err := json.Marshal(disclosure)
	if err != nil || string(data) != `{"exists":true}` {
		t.Fatalf("hidden parent leaked data: %s", data)
	}
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"visible": true}, action: authz.Read}}
	_, err = f.app.Checker.ParentDisclosure(context.Background(), by, hRef("visible"), f.snapshot(t))
	requireHCode(t, err, "not_found")
	f.app.Checker.chain.Links = nil
	disclosure, err = f.app.Checker.ParentDisclosure(context.Background(), by, hRef("visible"), f.snapshot(t))
	if err != nil || disclosure.Ref == nil || disclosure.Ref.ID != "new" {
		t.Fatal("readable parent reference missing")
	}
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"hidden-one": true, "hidden-two": true}, action: authz.Read}}
	children := []model.Ref{hRef("hidden-one"), hRef("visible"), hRef("hidden-two")}
	visibility, err := f.app.Checker.VisibleChildren(context.Background(), by, hRef("new"), children, f.snapshot(t))
	if err != nil || len(visibility.Refs) != 1 || visibility.Refs[0].ID != "visible" || visibility.Hidden.String() != "2" {
		t.Fatalf("visible-only counts = %+v, %v", visibility, err)
	}
	data, _ = json.Marshal(visibility)
	if strings.Contains(string(data), "hidden-") || strings.Contains(string(data), "secret-") || strings.Contains(string(data), "status") {
		t.Fatalf("hidden child data escaped: %s", data)
	}
	for _, refs := range [][]model.Ref{{hRef("visible"), hRef("visible")}, {hRef("child")}, {hRef("absent")}} {
		_, err := f.app.Checker.VisibleChildren(context.Background(), by, hRef("new"), refs, f.snapshot(t))
		requireHCode(t, err, "authz_unavailable")
	}
	s = f.snapshot(t)
	s.failGet = "hidden-two"
	partial, err := f.app.Checker.VisibleChildren(context.Background(), by, hRef("new"), children, s)
	requireHCode(t, err, "authz_unavailable")
	if len(partial.Refs) != 0 || !partial.Hidden.IsZero() {
		t.Fatal("source failure published a partial or approximate hidden count")
	}
}

func TestHierarchyReadBindingInvalidatesOnRevocation(t *testing.T) {
	f := newHFixture(t, true)
	by := model.PrincipalRef{Kind: "user", ID: "user:viewer"}
	oldSnapshot := f.snapshot(t)
	old, err := f.app.Checker.ReadAuthorityBinding(context.Background(), by, oldSnapshot)
	if err != nil || len(old) != 64 {
		t.Fatal("read authority binding unavailable", err)
	}
	same, err := f.app.Checker.ReadAuthorityBinding(context.Background(), by, f.snapshot(t))
	if err != nil || same != old {
		t.Fatal("unchanged current authority changed its binding")
	}
	other, err := f.app.Checker.ReadAuthorityBinding(context.Background(), hAgent(), f.snapshot(t))
	if err != nil || other == old {
		t.Fatal("different authenticated caller shared a scope binding")
	}
	revoked := f.base
	revoked.Grants = nil
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	fresh, err := f.app.Checker.ReadAuthorityBinding(context.Background(), by, f.snapshot(t))
	if err != nil || fresh == old {
		t.Fatal("ACL revocation did not invalidate continuation authority")
	}
	_, err = f.app.Checker.ReadAuthorityBinding(context.Background(), by, oldSnapshot)
	requireHCode(t, err, "authz_unavailable")
}

// A test restriction, not a new permit: it narrows the installed fixed User
// read rule to current project involvement so revocation can be exercised.
type hInvolvedReads struct{}

func (hInvolvedReads) Check(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Step, error) {
	out := make([]authz.Step, len(requests))
	for i, req := range requests {
		out[i].Effect = authz.NoDecision
		if req.Action != authz.Read {
			continue
		}
		template := authz.FilterTemplate{Namespace: s.Namespace(), Request: authz.ListRequest{Caller: req.Caller, Action: req.Action, ResourceKind: req.Resource.Kind, Method: req.Method},
			Permit: authz.And(authz.FilterExpr{Op: authz.ActiveProject}, authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"}), Forbid: authz.FilterExpr{Op: authz.False}}
		allowed, err := template.Match(ctx, s, req.Resource)
		if err != nil {
			return nil, err
		}
		if !allowed {
			out[i] = authz.Step{Effect: authz.Deny, Reason: "test current project restriction"}
		}
	}
	return out, nil
}

func (hInvolvedReads) Filter(authz.ListRequest) (authz.FilterExpr, error) {
	return authz.FilterExpr{Op: authz.False}, authz.ErrCheckOnly
}

func TestEpicProgressUsesCurrentAuthorityAgainstPinnedContent(t *testing.T) {
	f := newHFixture(t, true)
	f.app.Checker.chain.Links = []authz.Link{hInvolvedReads{}}
	by := model.PrincipalRef{Kind: "user", ID: "user:operator"}
	content := f.snapshot(t)
	children := []model.Ref{hRef("visible"), hRef("hidden-one"), hRef("hidden-two")}
	first, err := f.app.Checker.AuthorizeEpicProgress(context.Background(), by, content, hRef("new"), children)
	if err != nil || len(first.Visible) != 1 || first.Visible[0].ID != "visible" || first.Hidden.String() != "2" || first.PolicyBinding == "" {
		t.Fatalf("current authority result = %+v, %v", first, err)
	}
	revoked := f.base
	revoked.Grants = []authz.Grant{f.base.Grants[1]} // Keep the parent, revoke child-project.
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	second, err := f.app.Checker.AuthorizeEpicProgress(context.Background(), by, content, hRef("new"), children)
	if err != nil || len(second.Visible) != 0 || second.Hidden.String() != "3" || second.PolicyBinding == first.PolicyBinding {
		t.Fatalf("pinned content retained revoked child authority: %+v, %v", second, err)
	}
	if content.Snapshot().At.String() == f.snapshot(t).Snapshot().At.String() {
		t.Fatal("test did not keep the old content snapshot across ACL writes")
	}
	requireHCode(t, f.app.Checker.ValidateReadAuthorityBinding(context.Background(), by, first.PolicyBinding), "authz_unavailable")
	revoked.Grants = nil
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	result, err := f.app.Checker.AuthorizeEpicProgress(context.Background(), by, content, hRef("new"), children)
	requireHCode(t, err, "not_found")
	if len(result.Visible) != 0 || !result.Hidden.IsZero() || result.PolicyBinding != "" {
		t.Fatal("unreadable parent disclosed child progress")
	}
}

type hRevokeDuringRead struct {
	once  sync.Once
	facts *MutableFacts
	next  authzfixed.Facts
	err   error
}

func (r *hRevokeDuringRead) Check(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Step, error) {
	out := make([]authz.Step, len(requests))
	for i, req := range requests {
		out[i].Effect = authz.NoDecision
		if req.Action == authz.Read && req.Resource.Ref.ID == "visible" {
			r.once.Do(func() { r.err = r.facts.Replace(r.next) })
		}
	}
	return out, r.err
}

func (*hRevokeDuringRead) Filter(authz.ListRequest) (authz.FilterExpr, error) {
	return authz.FilterExpr{Op: authz.False}, authz.ErrCheckOnly
}

func TestEpicProgressFinalCheckRefusesMidCountRevocation(t *testing.T) {
	f := newHFixture(t, true)
	revoked := f.base
	revoked.Grants = nil
	f.app.Checker.chain.Links = []authz.Link{&hRevokeDuringRead{facts: f.facts, next: revoked}}
	result, err := f.app.Checker.AuthorizeEpicProgress(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:operator"}, f.snapshot(t), hRef("new"), []model.Ref{hRef("visible")})
	requireHCode(t, err, "authz_unavailable")
	if len(result.Visible) != 0 || !result.Hidden.IsZero() || result.PolicyBinding != "" {
		t.Fatal("mid-count revocation escaped the final fresh authority check")
	}
}

type hReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func (f *hFixture) callRPC(t *testing.T, principal model.PrincipalRef, method string, params map[string]any) hReply {
	t.Helper()
	issued, err := f.keys.Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issued.Token)
	recorder := httptest.NewRecorder()
	f.app.Handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d", recorder.Code)
	}
	var reply hReply
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Error != nil {
		t.Logf("%s response: %s", method, recorder.Body.Bytes())
	}
	return reply
}

func (f *hFixture) createRPC(t *testing.T, key int) hReply {
	t.Helper()
	return f.callRPC(t, hAgent(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "created"},
		"_meta": map[string]any{"project": "child-project", "idempotencyKey": fmt.Sprintf("%032x", key)},
	})
}

func TestHierarchyHTTPParentDisclosure(t *testing.T) {
	f := newHFixture(t, true)
	by := model.PrincipalRef{Kind: "user", ID: "user:reader", Caveats: map[string]any{"projects": []model.Ref{hRef("child-project")}}}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"bead.get", "bead.ready", "history.query"} {
		t.Run(method, func(t *testing.T) {
			params := map[string]any{"_meta": map[string]any{"project": "child-project"}}
			if method == "bead.get" {
				params["id"] = hRef("visible")
			}
			if method == "history.query" {
				params["asOf"] = map[string]any{"revision": meta.Head, "incarnation": meta.Incarnation}
			}
			reply := f.callRPC(t, by, method, params)
			if reply.Error != nil {
				t.Fatalf("authorized child read refused: %+v", reply.Error)
			}
			var result struct {
				Bead  map[string]any   `json:"bead"`
				Items []map[string]any `json:"items"`
			}
			if err := json.Unmarshal(reply.Result, &result); err != nil {
				t.Fatal(err)
			}
			items := result.Items
			if method == "bead.get" {
				items = []map[string]any{result.Bead}
			}
			if len(items) == 0 {
				t.Fatal("visible child disappeared")
			}
			for _, item := range items {
				if item["project"] != "child-project" {
					t.Fatalf("child project changed: %v", item["project"])
				}
				if !reflect.DeepEqual(item["parent"], map[string]any{"exists": true}) {
					t.Fatalf("hidden parent did not remain existence-only: %v", item["parent"])
				}
			}
			if bytes.Contains(reply.Result, []byte(`"new"`)) || bytes.Contains(reply.Result, []byte(`"old"`)) || bytes.Contains(reply.Result, []byte("parent-project")) || bytes.Contains(reply.Result, []byte("old-project")) {
				t.Fatalf("hidden parent content escaped: %s", reply.Result)
			}
		})
	}
	denied := f.callRPC(t, by, "bead.get", map[string]any{"id": hRef("new")})
	if denied.Error == nil || denied.Error.Data.Code != "not_found" || len(denied.Result) != 0 {
		t.Fatal("project caveat did not conceal the denied parent itself")
	}
	// CP7 still permits an unconstrained User to read every bead.
	user := model.PrincipalRef{Kind: "user", ID: "user:reader"}
	for _, id := range []string{"visible", "new"} {
		reply := f.callRPC(t, user, "bead.get", map[string]any{"id": hRef(id)})
		var result struct {
			Bead map[string]any `json:"bead"`
		}
		if reply.Error != nil || json.Unmarshal(reply.Result, &result) != nil {
			t.Fatal("unconstrained User read refused")
		}
		if id == "visible" && result.Bead["parent"] != "new" {
			t.Fatal("readable parent lost its compatible bare ID")
		}
		if id == "new" {
			if _, present := result.Bead["parent"]; present {
				t.Fatal("root disclosed a nonexistent parent")
			}
		}
	}
}

func TestHierarchyOptInAuthenticatedWriterAndReplay(t *testing.T) {
	f := newHFixture(t, false)
	before, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	denied := f.createRPC(t, 101)
	if denied.Error == nil || denied.Error.Data.Code != "forbidden" {
		t.Fatalf("default #18 create no longer refuses: %+v", denied)
	}
	after, _ := f.store.Meta(context.Background())
	if after.Head.String() != before.Head.String() {
		t.Fatal("default refusal changed the head")
	}
	// This trusted opt-in tests the new adapter through the one real HTTP,
	// authentication, service and Writer path. Parented persistence/closure
	// remains EP3's independent implementation, not a test-only replacement.
	f.app.Checker.hierarchy = true
	created := f.createRPC(t, 102)
	if created.Error != nil || len(created.Result) == 0 || created.Meta.Replayed {
		t.Fatalf("opt-in root create failed: %+v", created)
	}
	committed, _ := f.store.Meta(context.Background())
	if committed.Head.String() == before.Head.String() {
		t.Fatal("authorized create did not commit")
	}
	principal, err := driver.PrincipalKey(hAgent())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.snapshot(t).IdempotencyByKey(context.Background(), principal, "", "bead.create", fmt.Sprintf("%032x", 102))
	if err != nil || receipt == nil || len(receipt.Commit.Results) != 1 {
		t.Fatal("authorized create omitted the durable receipt", err)
	}
	rows, err := f.snapshot(t).Get(context.Background(), []model.Ref{receipt.Commit.Results[0].Ref})
	if err != nil || len(rows) != 1 {
		t.Fatal("authorized create did not persist its child", err)
	}
	value, err := hierarchyRaw(rows[0].Ref, rows[0].Revision, rows[0].Body)
	if err != nil || value.Body["project"] != "child-project" {
		t.Fatal("created child lost its explicit own project", err)
	}
	replayed := f.createRPC(t, 102)
	if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(replayed.Result, created.Result) {
		t.Fatalf("current authorized receipt did not replay: %+v", replayed)
	}
	after, _ = f.store.Meta(context.Background())
	if after.Head.String() != committed.Head.String() {
		t.Fatal("replay republished the original write")
	}
	revoked := f.base
	revoked.Grants = nil
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	revocation, _ := f.store.Meta(context.Background())
	denied = f.createRPC(t, 102)
	if denied.Error == nil || denied.Error.Data.Code != "forbidden" || denied.Meta.Replayed {
		t.Fatalf("stored receipt bypassed current revocation: %+v", denied)
	}
	after, _ = f.store.Meta(context.Background())
	if after.Head.String() != revocation.Head.String() {
		t.Fatal("denied replay published a write")
	}
}

func TestParentProjectionPinnedContentAndCurrentAuthority(t *testing.T) {
	f := newHFixture(t, true)
	ctx := context.Background()
	by := model.PrincipalRef{Kind: "user", ID: "user:viewer"}
	content := f.snapshot(t)
	// The current child changes parents after content is pinned. Projection
	// must still describe its historical parent, using only current read grants.
	if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: hRef("child"), Kind: "bead", Op: model.OpPatch, Body: hBody("child", "child-project", "new", "todo")}}); err != nil {
		t.Fatal(err)
	}
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"old": true}, action: authz.Read}}
	result, err := f.app.Checker.AuthorizeParentProjections(ctx, by, content, []model.Ref{hRef("child"), hRef("visible"), hRef("new")})
	if err != nil {
		t.Fatal(err)
	}
	if p := result.Parents["child"]; !p.Exists || p.Ref != nil {
		t.Fatalf("historical hidden parent = %+v", p)
	}
	if p := result.Parents["visible"]; !p.Exists || p.Ref == nil || p.Ref.ID != "new" {
		t.Fatalf("readable parent = %+v", p)
	}
	if p := result.Parents["new"]; p.Exists || p.Ref != nil {
		t.Fatalf("root = %+v", p)
	}
	if err := f.app.Checker.ValidateReadAuthorityBinding(ctx, by, result.PolicyBinding); err != nil {
		t.Fatal(err)
	}
	// Record-dependent custom rules and project caveats must not outlive a
	// current-source change even when the ACL registry itself is unchanged.
	if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: hRef("new"), Kind: "bead", Op: model.OpPatch, Body: hBody("new", "parent-project", "", "done")}}); err != nil {
		t.Fatal(err)
	}
	requireHCode(t, f.app.Checker.ValidateReadAuthorityBinding(ctx, by, result.PolicyBinding), "authz_unavailable")
	for _, refs := range [][]model.Ref{{hRef("visible"), hRef("visible")}, {hRef("absent")}} {
		partial, err := f.app.Checker.AuthorizeParentProjections(ctx, by, content, refs)
		if err == nil || partial.Parents != nil || partial.PolicyBinding != "" {
			t.Fatal("invalid batch published a partial projection", partial, err)
		}
	}
	f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"visible": true}, action: authz.Read}}
	_, _, err = f.app.Checker.AuthorizeParentProjection(ctx, by, content, hRef("visible"))
	requireHCode(t, err, "not_found")
}

func TestParentProjectionFinalCheckRefusesRevocation(t *testing.T) {
	f := newHFixture(t, true)
	revoked := f.base
	revoked.Grants = nil
	f.app.Checker.chain.Links = []authz.Link{&hRevokeDuringRead{facts: f.facts, next: revoked}}
	result, err := f.app.Checker.AuthorizeParentProjections(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:viewer"}, f.snapshot(t), []model.Ref{hRef("visible")})
	requireHCode(t, err, "authz_unavailable")
	if result.Parents != nil || result.PolicyBinding != "" {
		t.Fatal("mid-projection revocation published a partial result")
	}
}

func TestHierarchyHTTPParentCustomForbidAndRefCaveat(t *testing.T) {
	for _, mode := range []string{"custom-forbid", "ref-caveat"} {
		t.Run(mode, func(t *testing.T) {
			f := newHFixture(t, true)
			by := model.PrincipalRef{Kind: "user", ID: "user:reader"}
			if mode == "custom-forbid" {
				f.app.Checker.chain.Links = []authz.Link{hDeny{ids: map[string]bool{"new": true}, action: authz.Read}}
			} else {
				by.Caveats = map[string]any{"refs": []model.Ref{hRef("visible")}}
			}
			reply := f.callRPC(t, by, "bead.get", map[string]any{"id": hRef("visible")})
			if reply.Error != nil {
				t.Fatalf("child read refused: %+v", reply.Error)
			}
			var result struct {
				Bead map[string]any `json:"bead"`
			}
			if err := json.Unmarshal(reply.Result, &result); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Bead["parent"], map[string]any{"exists": true}) || bytes.Contains(reply.Result, []byte(`"new"`)) {
				t.Fatalf("parent content escaped: %s", reply.Result)
			}
		})
	}
}
