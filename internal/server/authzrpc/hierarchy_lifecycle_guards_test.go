package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func hierarchyLifecycleNoReceipt(t *testing.T, f *hFixture, params map[string]any) {
	t.Helper()
	principal, err := driver.PrincipalKey(hAgent())
	if err != nil {
		t.Fatal(err)
	}
	key := params["_meta"].(map[string]any)["idempotencyKey"].(string)
	receipt, err := f.snapshot(t).IdempotencyByKey(context.Background(), principal, "", "tx.apply", key)
	if err != nil || receipt != nil {
		t.Fatalf("refused hierarchy lifecycle receipt=%+v err=%v", receipt, err)
	}
}

func TestHierarchyMixedLifecycleIndependentPermitsAndLateGuardRollback(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, refusal := range []string{"tx.apply", "bead.close", "late-template"} {
			t.Run(engine+"/"+refusal, func(t *testing.T) {
				f := newHierarchyLifecycleFixture(t, engine)
				rules := &hierarchyLifecycleRules{denyTx: refusal == "tx.apply", denyClose: refusal == "bead.close"}
				f.app.Checker.chain.Rules = []authz.RuleSet{rules}
				params := hierarchyLifecycleMixedParams(2100, nil)
				want := model.ErrorCode("forbidden")
				if refusal == "late-template" {
					ops := params["changeSet"].(map[string]any)["ops"].([]any)
					ops[0].(map[string]any)["fields"].(map[string]any)["template"] = true
					want = "ontology_guard_failed"
				}
				head, rows := hierarchyLifecycleState(t, f, hRef("child"))
				count := hierarchyLifecycleCount(t, f)
				reply := f.callRPC(t, hAgent(), "tx.apply", params)
				if reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0 || reply.Meta.Replayed {
					t.Fatalf("refusal=%+v want=%s", reply, want)
				}
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				if hierarchyLifecycleCount(t, f) != count {
					t.Fatal("late refusal published the staged create")
				}
				hierarchyLifecycleNoReceipt(t, f, params)
			})
		}
	}
}

// This installed test policy explicitly permits both methods using current
// membership facts. Production's finite CP7 policy remains unchanged and is
// separately tested below to grant neither tx.apply nor bead.close by default.
type hierarchyLifecycleMembership struct{ fixed *authzfixed.RuleSet }

func (r hierarchyLifecycleMembership) Eval(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	mapped := append([]authz.CheckRequest(nil), requests...)
	for i := range mapped {
		if mapped[i].Method == "tx.apply" || mapped[i].Method == "bead.close" {
			// This installed test policy grants a transition only when the
			// same current project membership also permits the underlying
			// write. The production fixed policy grants neither action here.
			if mapped[i].Action == authz.Transition {
				mapped[i].Action = authz.Modify
			}
			mapped[i].Method = "bead.update"
			if mapped[i].Resource.Kind == authz.Project {
				mapped[i].Method = ""
			}
		}
	}
	return r.fixed.Eval(ctx, s, mapped)
}
func (hierarchyLifecycleMembership) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func TestHierarchyMixedCloseRechecksCurrentMembershipAndPreservesFinitePolicy(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newHierarchyLifecycleFixture(t, engine)
			params := hierarchyLifecycleMixedParams(2110, nil)
			head, rows := hierarchyLifecycleState(t, f, hRef("child"))
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			hierarchyLifecycleNoReceipt(t, f, params)
			fixed, err := authzfixed.New(f.base.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			f.app.Checker.chain.Rules = []authz.RuleSet{hierarchyLifecycleMembership{fixed}}
			first := f.callRPC(t, hAgent(), "tx.apply", params)
			if first.Error != nil || first.Meta.Replayed {
				t.Fatalf("configured membership grant failed: %+v", first)
			}
			for _, project := range []string{"child-project", "parent-project"} {
				facts := f.base
				facts.Grants = nil
				for _, grant := range f.base.Grants {
					if grant.Scope.Ref.ID != project {
						facts.Grants = append(facts.Grants, grant)
					}
				}
				if err := f.facts.Replace(facts); err != nil {
					t.Fatal(err)
				}
				// Replace records its own authority revision; measure the replay
				// against the state AFTER that legitimate fact mutation.
				head, rows = hierarchyLifecycleState(t, f, hRef("child"))
				hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				if err := f.facts.Replace(f.base); err != nil {
					t.Fatal(err)
				}
				head, rows = hierarchyLifecycleState(t, f, hRef("child"))
				replay := f.callRPC(t, hAgent(), "tx.apply", params)
				if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
					t.Fatalf("restored current %s authority replay=%+v", project, replay)
				}
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			}
		})
	}
}

func TestHierarchyMixedCloseExpectationOnlyPrivacyAndReparentedTarget(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, order := range []string{"reparent-close", "close-reparent"} {
			t.Run(engine+"/"+order, func(t *testing.T) {
				f := newHierarchyLifecycleFixture(t, engine)
				rules := &hierarchyLifecycleRules{}
				f.app.Checker.chain.Rules = []authz.RuleSet{rules}
				params := hierarchyLifecycleMixedParams(2120, nil)
				ops := params["changeSet"].(map[string]any)["ops"].([]any)
				close := ops[2].(map[string]any)
				close["target"] = hRef("child")
				if order == "close-reparent" {
					ops[1], ops[2] = ops[2], ops[1]
				}
				hidden := hRef("hidden-one")
				close["expect"] = []model.Precondition{model.PreconditionByRevision(hidden, model.Natural64(0))}
				rules.setRef(hidden.ID, true)
				head, rows := hierarchyLifecycleState(t, f, hRef("child"))
				hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				hierarchyLifecycleNoReceipt(t, f, params)
				rules.setRef(hidden.ID, false)
				stale := f.callRPC(t, hAgent(), "tx.apply", params)
				if stale.Error == nil || stale.Error.Data.Code != "precondition_failed" || stale.Error.Data.Details["reason"] != "revision" || fmt.Sprint(stale.Error.Data.Details["currentRevision"]) != hierarchyLifecycleRevision(t, f, hidden).String() {
					t.Fatalf("allowed stale close expectation=%+v", stale)
				}
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				hierarchyLifecycleNoReceipt(t, f, params)
				close["expect"] = []model.Precondition{model.PreconditionByRevision(hidden, hierarchyLifecycleRevision(t, f, hidden))}
				first := f.callRPC(t, hAgent(), "tx.apply", params)
				if first.Error != nil || first.Meta.Replayed {
					t.Fatalf("reparent then close failed: %+v", first)
				}
				var commit model.Commit
				if err := json.Unmarshal(first.Result, &commit); err != nil || len(commit.Results) != 3 || commit.Results[1].Ref.ID != "child" || commit.Results[2].Ref.ID != "child" {
					t.Fatalf("mixed result=%s err=%v", first.Result, err)
				}
				entries, err := f.snapshot(t).Changesets(context.Background(), nil, 100)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, entry := range entries {
					if entry.Revision.String() != commit.Revision.String() {
						continue
					}
					for index, change := range entry.Changes {
						if change.Ref.ID != "child" {
							continue
						}
						found = true
						parent, status := false, false
						for _, path := range change.ChangedPaths {
							parent = parent || path == "/parent"
							status = status || path == "/fields/status"
						}
						if change.Op != model.OpClose || !parent || !status {
							t.Fatalf("coalesced close lost parent/status history: %+v", change)
						}
						eventFound := false
						for _, event := range entry.Events {
							if event.ChangeIndex.String() == fmt.Sprint(index) && event.Type == model.EventBeadClosed && event.Resource.Ref.ID == "child" && event.Provenance.Method == "tx.apply" && event.Provenance.Slot == "close" {
								eventFound = true
							}
						}
						if !eventFound {
							t.Fatalf("coalesced close lost typed slot provenance: %+v", entry.Events)
						}
					}
				}
				if !found {
					t.Fatal("committed close history missing")
				}
				head, rows = hierarchyLifecycleState(t, f, hRef("child"))
				rules.setRef(hidden.ID, true)
				hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
				rules.setRef(hidden.ID, false)
				replay := f.callRPC(t, hAgent(), "tx.apply", params)
				if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
					t.Fatalf("restored close expectation replay=%+v", replay)
				}
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			})
		}
	}
}

func TestHierarchyLifecycleFinalImagesRemainBoundToHierarchyChecks(t *testing.T) {
	f := newHFixture(t, true)
	f.app.Checker.chain.Rules = []authz.RuleSet{&hierarchyLifecycleRules{}}
	parent := hRef("new")
	move := hMove(&parent)
	close := port.Change{Resource: move.Resource, Op: model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: hRef("child")}}, Before: move.Before, After: move.After}
	for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter} {
		proposal := hProposal(hAgent(), "tx.apply", move, close)
		if err := f.app.Checker.Authorize(context.Background(), stage, proposal, f.snapshot(t)); err != nil {
			t.Fatalf("valid reparent/close images: %v", err)
		}
		for _, forged := range []string{"old", "hidden-one"} {
			proposal.Changes[1].After = hBody("child", "child-project", forged, "todo")
			err := f.app.Checker.Authorize(context.Background(), stage, proposal, f.snapshot(t))
			requireHCode(t, err, "forbidden")
		}
	}
	create := hCreate("fresh", "fresh", "child-project", nil, "")
	close = port.Change{Resource: create.Resource, Op: model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: model.RefByKey("fresh")}}, After: create.After}
	proposal := hProposal(hAgent(), "tx.apply", create, close)
	if err := f.app.Checker.Authorize(context.Background(), port.AuthorizeAfter, proposal, f.snapshot(t)); err != nil {
		t.Fatalf("valid staged create/close images: %v", err)
	}
	for _, field := range []string{"labels", "owner", "assignee", "restricted", "unexpected"} {
		var body map[string]any
		if err := json.Unmarshal(create.After, &body); err != nil {
			t.Fatal(err)
		}
		body[field] = "forged"
		bad, _ := json.Marshal(body)
		proposal.Changes[0].After, proposal.Changes[1].After = bad, bad
		requireHCode(t, f.app.Checker.Authorize(context.Background(), port.AuthorizeAfter, proposal, f.snapshot(t)), "forbidden")
	}
	if !reflect.DeepEqual(create.After, close.After) {
		t.Fatal("authorization mutated the caller's image")
	}
}

func TestHierarchyLifecycleCreateCloseInstantRequiresMatchingTypedClose(t *testing.T) {
	f := newHFixture(t, true)
	f.app.Checker.chain.Rules = []authz.RuleSet{&hierarchyLifecycleRules{}}
	create := hCreate("fresh", "fresh", "child-project", nil, "")
	var body map[string]any
	if err := json.Unmarshal(create.After, &body); err != nil {
		t.Fatal(err)
	}
	body["fields"].(map[string]any)["status"] = "done"
	body["closedAt"] = "2026-11-03T00:01:00Z"
	final, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	create.After = final
	close := port.Change{Resource: create.Resource, Op: model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: model.RefByKey("fresh")}}, After: final}
	for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter} {
		t.Run(string(stage), func(t *testing.T) {
			proposal := hProposal(hAgent(), "tx.apply", create, close)
			if err := f.app.Checker.Authorize(context.Background(), stage, proposal, f.snapshot(t)); err != nil {
				t.Fatalf("valid same-target core close instant refused: %v", err)
			}
			t.Run("different-close-target", func(t *testing.T) {
				other := port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: hRef("child")},
					Op:     model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: hRef("child")}},
					Before: hBody("child", "child-project", "old", "todo"), After: hBody("child", "child-project", "old", "done")}
				requireHCode(t, f.app.Checker.Authorize(context.Background(), stage, hProposal(hAgent(), "tx.apply", create, other), f.snapshot(t)), "forbidden")
			})
			for _, tc := range []struct {
				name, member string
				value        any
			}{
				{"null-instant", "closedAt", nil},
				{"numeric-instant", "closedAt", 1},
				{"malformed-instant", "closedAt", "not-an-instant"},
				{"zero-instant", "closedAt", "0001-01-01T00:00:00Z"},
				{"unknown-member", "unexpected", true},
				{"unaccounted-start", "startedAt", "2026-11-03T00:00:30Z"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var changed map[string]any
					if err := json.Unmarshal(final, &changed); err != nil {
						t.Fatal(err)
					}
					changed[tc.member] = tc.value
					bad, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					proposal := hProposal(hAgent(), "tx.apply", create, close)
					proposal.Changes[0].After, proposal.Changes[1].After = bad, bad
					requireHCode(t, f.app.Checker.Authorize(context.Background(), stage, proposal, f.snapshot(t)), "forbidden")
				})
			}
			if !bytes.Equal(create.After, final) || !bytes.Equal(close.After, final) {
				t.Fatal("authorization changed the complete original final images")
			}
		})
	}
}

// The older authorization fixture deliberately seeds only source records.
// Real parented Writer operations also require a complete derived closure;
// installing those fixture rows does not change any product validation.
func newHierarchyLifecycleFixture(t *testing.T, engine string) *hFixture {
	t.Helper()
	f := newHFixtureStore(t, true, hierarchyLifecycleStore(t, engine))
	ctx := context.Background()
	parents := map[string]string{"child": "old", "old": "", "new": "", "visible": "new", "hidden-one": "new", "hidden-two": "new"}
	var rows []driver.DerivedRow
	var keys []driver.LockKey
	var sentinels []driver.Sentinel
	for id := range parents {
		sentinel, err := driver.NewClosureSentinel(id)
		if err != nil {
			t.Fatal(err)
		}
		sentinels = append(sentinels, sentinel)
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		var depth uint64
		for ancestor := id; ancestor != ""; ancestor = parents[ancestor] {
			row, err := pipeline.AncestorRow(id, ancestor, depth)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
			keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}})
			depth++
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	locks := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.Write(ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
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
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "hierarchy-fixture"}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	stored, err := f.snapshot(t).DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil || !reflect.DeepEqual(stored, rows) {
		t.Fatalf("fixture closure rows=%+v want=%+v err=%v", stored, rows, err)
	}
	return f
}

func TestHierarchyMixedLifecycleRefusesMissingClosure(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newHFixtureStore(t, true, hierarchyLifecycleStore(t, engine))
			f.app.Checker.chain.Rules = []authz.RuleSet{&hierarchyLifecycleRules{}}
			head, rows := hierarchyLifecycleState(t, f, hRef("child"))
			params := hierarchyLifecycleMixedParams(2130, nil)
			reply := f.callRPC(t, hAgent(), "tx.apply", params)
			if reply.Error == nil || reply.Error.Data.Code != "ontology_guard_failed" || len(reply.Result) != 0 || reply.Meta.Replayed {
				t.Fatalf("missing closure refusal=%+v", reply)
			}
			hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
			hierarchyLifecycleNoReceipt(t, f, params)
		})
	}
}

// Match the existing Writer event-context child budget, without changing it.
// Counts are printed on the remote regression so budget refusals stay distinct
// from permission, ontology and deadline failures.
type hierarchyLifecycleBoundedReader struct {
	port.SnapshotReader
	points   int
	unique   map[string]bool
	exceeded bool
}

func (r *hierarchyLifecycleBoundedReader) budget(keys []driver.LockKey) error {
	r.points += len(keys)
	if r.unique == nil {
		r.unique = map[string]bool{}
	}
	for _, key := range keys {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return err
		}
		r.unique[encoded] = true
	}
	if r.points > 32 {
		r.exceeded = true
		return fmt.Errorf("subscriber snapshot read budget exceeded at %d points", r.points)
	}
	return nil
}
func (r *hierarchyLifecycleBoundedReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	keys := make([]driver.LockKey, len(refs))
	for i := range refs {
		keys[i] = driver.LockKey{Kind: driver.LockRecord, Ref: &refs[i]}
	}
	if err := r.budget(keys); err != nil {
		return nil, err
	}
	return r.SnapshotReader.Get(ctx, refs)
}
func (r *hierarchyLifecycleBoundedReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := r.budget(keys); err != nil {
		return nil, err
	}
	return r.SnapshotReader.Observe(ctx, keys)
}

func TestHierarchyLifecycleEventContextRetainsBoundedTrackedReads(t *testing.T) {
	f := newHFixture(t, true)
	fixed, err := authzfixed.New(f.base.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	f.app.Checker.chain.Rules = []authz.RuleSet{hierarchyLifecycleMembership{fixed}}
	parent := hRef("new")
	create := hCreate("fresh", "fresh", "child-project", &parent, "new")
	close := port.Change{Resource: create.Resource, Op: model.Op{Kind: model.OpClose, Close: &model.CloseOp{Kind: model.OpClose, Target: model.RefByKey("fresh")}}, After: create.After}
	proposal := hProposal(hAgent(), "tx.apply", create, hMove(&parent), close)
	for pass := 0; pass < 2; pass++ {
		reader := &hierarchyLifecycleBoundedReader{SnapshotReader: f.snapshot(t)}
		_, err := f.app.Checker.EventContext(context.Background(), proposal, reader)
		t.Logf("event-context pass=%d points=%d unique=%d limit=32 exceeded=%t err=%v", pass, reader.points, len(reader.unique), reader.exceeded, err)
		if err != nil || reader.exceeded {
			t.Fatalf("mixed lifecycle exhausted event-context reads: %v", err)
		}
		for _, key := range []driver.LockKey{{Kind: driver.LockRecord, Ref: &create.Resource.Ref}, {Kind: driver.LockRecord, Ref: &parent}} {
			encoded, _ := driver.EncodeLockKey(key)
			if !reader.unique[encoded] {
				t.Fatalf("pass %d omitted tracked source or miss %s", pass, encoded)
			}
		}
	}
}

func TestHierarchyLifecycleSnapshotReadsStayDetachedAndFresh(t *testing.T) {
	f := newHFixture(t, true)
	source := f.snapshot(t)
	ctx := context.Background()
	memo := &hierarchyLifecycleSnapshot{SnapshotReader: source, reads: map[string][]byte{}}
	child := hRef("child")
	first, err := memo.Get(ctx, []model.Ref{child})
	if err != nil || len(first) != 1 {
		t.Fatalf("initial read=%+v err=%v", first, err)
	}
	original := bytes.Clone(first[0].Body)
	first[0].Body[0] = '!'
	second, err := memo.Get(ctx, []model.Ref{child})
	if err != nil || len(second) != 1 || !bytes.Equal(second[0].Body, original) || source.gets[child.ID] != 1 {
		t.Fatalf("retained read mutated or repeated: rows=%+v reads=%d err=%v", second, source.gets[child.ID], err)
	}
	missing := hRef("absent")
	for i := 0; i < 2; i++ {
		rows, err := memo.Get(ctx, []model.Ref{missing})
		if err != nil || len(rows) != 0 {
			t.Fatalf("miss=%+v err=%v", rows, err)
		}
	}
	if source.gets[missing.ID] != 1 {
		t.Fatal("missing source did not retain its first tracked observation")
	}
	key := driver.LockKey{Kind: driver.LockRecord, Ref: &child}
	state, err := memo.Observe(ctx, []driver.LockKey{key})
	if err != nil || len(state) != 1 || !state[0].Present {
		t.Fatalf("observation=%+v err=%v", state, err)
	}
	state[0].Key.Ref.ID = "corrupted"
	if state[0].Record != nil {
		state[0].Record.Body[0] = '!'
	}
	state, err = memo.Observe(ctx, []driver.LockKey{key})
	if err != nil || len(state) != 1 || state[0].Key.Ref.ID != child.ID || state[0].Record != nil && !bytes.Equal(state[0].Record.Body, original) {
		t.Fatalf("observation was not detached: %+v err=%v", state, err)
	}
	source.failGet = "new"
	if _, err := memo.Get(ctx, []model.Ref{hRef("new")}); err == nil {
		t.Fatal("read failure allowed")
	}
	source.failGet = ""
	if rows, err := memo.Get(ctx, []model.Ref{hRef("new")}); err != nil || len(rows) != 1 {
		t.Fatalf("failed read was cached: %+v err=%v", rows, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := memo.Get(canceled, []model.Ref{child}); err == nil {
		t.Fatal("cached read ignored cancellation")
	}
	fresh := &hierarchyLifecycleSnapshot{SnapshotReader: source, reads: map[string][]byte{}}
	if _, err := fresh.Get(ctx, []model.Ref{child}); err != nil || source.gets[child.ID] != 2 {
		t.Fatalf("new pass reused a prior observation: reads=%d err=%v", source.gets[child.ID], err)
	}
	bounded := &hierarchyLifecycleBoundedReader{SnapshotReader: f.snapshot(t)}
	limited := &hierarchyLifecycleSnapshot{SnapshotReader: bounded, reads: map[string][]byte{}}
	for i := 0; i < 33; i++ {
		_, err := limited.Get(ctx, []model.Ref{hRef(fmt.Sprintf("distinct-miss-%d", i))})
		if (err != nil) != (i == 32) {
			t.Fatalf("distinct read %d changed the original 32-point limit: %v", i+1, err)
		}
	}
	if bounded.points != 33 || len(bounded.unique) != 33 || !bounded.exceeded {
		t.Fatal("distinct dependencies bypassed the wrapped budget")
	}
}
