package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// hierarchyLifecycleRules is a configured policy used only to prove that the
// composed route asks for tx.apply and bead.close independently. It grants
// every other request unless the test explicitly revokes a method or ref.
type hierarchyLifecycleRules struct {
	mu        sync.Mutex
	denyTx    bool
	denyClose bool
	denyRefs  map[string]bool
	calls     map[string]int
}

func (r *hierarchyLifecycleRules) Eval(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	out := make([]authz.Decision, len(requests))
	for i, request := range requests {
		r.calls[request.Method]++
		out[i].Effect = authz.Allow
		switch {
		case r.denyRefs[request.Resource.Ref.ID]:
			out[i] = authz.Decision{Effect: authz.Deny, Reasons: []string{"test hierarchy ref denied"}}
		case request.Method == "tx.apply" && r.denyTx:
			out[i] = authz.Decision{Effect: authz.Deny, Reasons: []string{"test tx.apply denied"}}
		case request.Method == "bead.close" && r.denyClose:
			out[i] = authz.Decision{Effect: authz.Deny, Reasons: []string{"test bead.close denied"}}
		}
	}
	return out, nil
}

func (*hierarchyLifecycleRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func (r *hierarchyLifecycleRules) count(method string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[method]
}

func (r *hierarchyLifecycleRules) setRef(ref string, denied bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.denyRefs == nil {
		r.denyRefs = map[string]bool{}
	}
	r.denyRefs[ref] = denied
}

func hierarchyLifecycleStore(t *testing.T, engine string) driver.Store {
	t.Helper()
	if engine == "sqlite" {
		store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "hierarchy-lifecycle.db")})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	return memory.New()
}

func hierarchyLifecycleRevision(t *testing.T, f *hFixture, ref model.Ref) model.Revision {
	t.Helper()
	rows, err := f.snapshot(t).Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("revision %s: rows=%+v err=%v", ref.ID, rows, err)
	}
	return rows[0].Revision
}

func hierarchyLifecycleCount(t *testing.T, f *hFixture) int {
	t.Helper()
	page, err := f.snapshot(t).Query(context.Background(), driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return len(page.Items)
}

func hierarchyLifecycleState(t *testing.T, f *hFixture, refs ...model.Ref) (model.Revision, map[string]driver.Record) {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]driver.Record{}
	for _, ref := range refs {
		value, err := f.snapshot(t).Get(context.Background(), []model.Ref{ref})
		if err != nil || len(value) != 1 {
			t.Fatalf("state %s: rows=%+v err=%v", ref.ID, value, err)
		}
		rows[ref.ID] = value[0]
	}
	return meta.Head, rows
}

func hierarchyLifecycleSameState(t *testing.T, f *hFixture, head model.Revision, before map[string]driver.Record, refs ...model.Ref) {
	t.Helper()
	afterHead, after := hierarchyLifecycleState(t, f, refs...)
	if afterHead.String() != head.String() || !reflect.DeepEqual(after, before) {
		t.Fatalf("refused hierarchy batch changed state: head %s -> %s before=%+v after=%+v", head, afterHead, before, after)
	}
}

func hierarchyLifecycleCloseParams(key int, ref model.Ref, revision model.Revision) map[string]any {
	expect := model.PreconditionByRevision(ref, revision)
	return map[string]any{
		"changeSet": map[string]any{
			"ops": []any{map[string]any{
				"kind": "close", "target": ref, "reason": "hierarchy close",
				"expect": []model.Precondition{expect},
			}},
			"expect": []model.Precondition{expect},
		},
		"_meta": map[string]any{"project": "child-project", "idempotencyKey": fmt.Sprintf("b21%029x", key)},
	}
}

// The close targets the earlier create by key. This intentionally exercises a
// staged source miss; routing the whole proposal through validateExistingWrite
// would refuse it because the created row has no stored before image yet.
func hierarchyLifecycleMixedParams(key int, extra *model.Precondition) map[string]any {
	changeSet := map[string]any{
		"ops": []any{
			map[string]any{
				"kind": "create", "key": "fresh", "type": "issue", "project": "child-project",
				"parent": hRef("new"), "fields": map[string]any{"title": "fresh"},
			},
			map[string]any{"kind": "reparent", "target": hRef("child"), "parent": hRef("new")},
			map[string]any{"kind": "close", "target": model.RefByKey("fresh"), "reason": "created then closed"},
		},
	}
	if extra != nil {
		changeSet["expect"] = []model.Precondition{*extra}
	}
	return map[string]any{
		"changeSet": changeSet,
		"_meta":     map[string]any{"project": "child-project", "idempotencyKey": fmt.Sprintf("b21%029x", key)},
	}
}

func hierarchyLifecycleForbidden(t *testing.T, reply hReply) {
	t.Helper()
	if reply.Error == nil || reply.Error.Data.Code != "forbidden" || len(reply.Error.Data.Details) != 0 || len(reply.Result) != 0 || reply.Meta.Replayed {
		t.Fatalf("hierarchy refusal=%+v", reply)
	}
}

func TestHierarchyEnabledCloseOnlyUsesOrdinaryCloseAuthorization(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newHierarchyLifecycleFixture(t, engine)
			rules := &hierarchyLifecycleRules{}
			f.app.Checker.chain.Rules = []authz.RuleSet{rules}
			child := hRef("child")
			beforeHead, beforeRows := hierarchyLifecycleState(t, f, child)
			params := hierarchyLifecycleCloseParams(1001, child, hierarchyLifecycleRevision(t, f, child))

			rules.denyTx = true
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
			if rules.count("tx.apply") == 0 || rules.count("bead.close") == 0 {
				t.Fatal("close-only path did not issue independent tx.apply and bead.close checks")
			}
			hierarchyLifecycleSameState(t, f, beforeHead, beforeRows, child)

			rules.denyTx = false
			rules.denyClose = true
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", hierarchyLifecycleCloseParams(1002, child, hierarchyLifecycleRevision(t, f, child))))
			hierarchyLifecycleSameState(t, f, beforeHead, beforeRows, child)

			rules.denyClose = false
			valid := hierarchyLifecycleCloseParams(1003, child, hierarchyLifecycleRevision(t, f, child))
			first := f.callRPC(t, hAgent(), "tx.apply", valid)
			if first.Error != nil || first.Meta.Replayed {
				t.Fatalf("authorized hierarchy close failed: %+v", first)
			}
			var commit model.Commit
			if err := json.Unmarshal(first.Result, &commit); err != nil || len(commit.Results) != 1 {
				t.Fatalf("close result=%s err=%v", first.Result, err)
			}
			committedHead, committedRows := hierarchyLifecycleState(t, f, child)
			if committedHead.String() == beforeHead.String() || committedRows[child.ID].Revision.String() != committedHead.String() {
				t.Fatal("authorized close did not produce one durable revision")
			}

			rules.denyClose = true
			replayDenied := f.callRPC(t, hAgent(), "tx.apply", valid)
			hierarchyLifecycleForbidden(t, replayDenied)
			hierarchyLifecycleSameState(t, f, committedHead, committedRows, child)
			rules.denyClose = false
			replay := f.callRPC(t, hAgent(), "tx.apply", valid)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("authorized current close replay changed response: %+v", replay)
			}
			hierarchyLifecycleSameState(t, f, committedHead, committedRows, child)
		})
	}
}

func TestHierarchyEnabledMixedLifecyclePreservesHierarchyAuthorityAndRollback(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			for _, deniedRef := range []string{"new", "child-project"} {
				f := newHierarchyLifecycleFixture(t, engine)
				rules := &hierarchyLifecycleRules{}
				f.app.Checker.chain.Rules = []authz.RuleSet{rules}
				rules.setRef(deniedRef, true)
				head, rows := hierarchyLifecycleState(t, f, hRef("child"), hRef("visible"))
				count := hierarchyLifecycleCount(t, f)
				hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", hierarchyLifecycleMixedParams(2000+len(deniedRef), nil)))
				hierarchyLifecycleSameState(t, f, head, rows, hRef("child"), hRef("visible"))
				if got := hierarchyLifecycleCount(t, f); got != count {
					t.Fatalf("refused hierarchy batch changed bead count: %d -> %d", count, got)
				}
			}

			f := newHierarchyLifecycleFixture(t, engine)
			rules := &hierarchyLifecycleRules{}
			f.app.Checker.chain.Rules = []authz.RuleSet{rules}
			beforeHead, beforeRows := hierarchyLifecycleState(t, f, hRef("child"), hRef("visible"))
			count := hierarchyLifecycleCount(t, f)
			rules.denyClose = true
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", hierarchyLifecycleMixedParams(2010, nil)))
			hierarchyLifecycleSameState(t, f, beforeHead, beforeRows, hRef("child"), hRef("visible"))
			if got := hierarchyLifecycleCount(t, f); got != count {
				t.Fatalf("close-denied mixed batch changed bead count: %d -> %d", count, got)
			}
			rules.denyClose = false
			mixed := hierarchyLifecycleMixedParams(2011, nil)
			first := f.callRPC(t, hAgent(), "tx.apply", mixed)
			if first.Error != nil || first.Meta.Replayed {
				t.Fatalf("authorized mixed hierarchy lifecycle failed: %+v", first)
			}
			var commit model.Commit
			if err := json.Unmarshal(first.Result, &commit); err != nil || len(commit.Results) != 3 {
				t.Fatalf("mixed result=%s err=%v", first.Result, err)
			}
			for i, result := range commit.Results {
				if result.Revision.String() != commit.Revision.String() {
					t.Fatalf("result %d revision=%s commit=%s", i, result.Revision, commit.Revision)
				}
			}
			if commit.Results[0].Ref.ID != commit.Results[2].Ref.ID || rules.count("tx.apply") == 0 || rules.count("bead.close") == 0 {
				t.Fatal("mixed create/close lost operation order or independent close checks")
			}
			fresh, child := commit.Results[0].Ref, hRef("child")
			freshRow, err := f.snapshot(t).Get(context.Background(), []model.Ref{fresh})
			if err != nil || len(freshRow) != 1 {
				t.Fatalf("created-and-closed row missing: rows=%+v err=%v", freshRow, err)
			}
			var freshBody map[string]any
			if err := json.Unmarshal(freshRow[0].Body, &freshBody); err != nil {
				t.Fatal(err)
			}
			fields, _ := freshBody["fields"].(map[string]any)
			if fields["status"] != "done" {
				t.Fatalf("created-and-closed row status=%v", fields["status"])
			}
			for _, member := range []string{"createdAt", "closedAt"} {
				stamp, ok := freshBody[member].(string)
				at, err := time.Parse(time.RFC3339Nano, stamp)
				if !ok || err != nil || at.IsZero() {
					t.Fatalf("created-and-closed row lost its core %s: %v", member, freshBody[member])
				}
			}
			childRow, err := f.snapshot(t).Get(context.Background(), []model.Ref{child})
			if err != nil || len(childRow) != 1 {
				t.Fatalf("reparented child missing: rows=%+v err=%v", childRow, err)
			}
			var childBody map[string]any
			if err := json.Unmarshal(childRow[0].Body, &childBody); err != nil {
				t.Fatal(err)
			}
			parent, _ := childBody["parent"].(map[string]any)
			if parent["id"] != "new" {
				t.Fatalf("reparented child parent=%v", childBody["parent"])
			}
		})
	}
}

func TestHierarchyEnabledMixedLifecycleAuthorizesExpectationOnlyTargetsBeforeDisclosureAndReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newHierarchyLifecycleFixture(t, engine)
			rules := &hierarchyLifecycleRules{}
			f.app.Checker.chain.Rules = []authz.RuleSet{rules}
			hidden := hRef("hidden-one")
			stale := model.PreconditionByRevision(hidden, model.Natural64(0))
			beforeHead, beforeRows := hierarchyLifecycleState(t, f, hRef("child"), hRef("visible"))
			rules.setRef(hidden.ID, true)
			hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", hierarchyLifecycleMixedParams(2020, &stale)))
			hierarchyLifecycleSameState(t, f, beforeHead, beforeRows, hRef("child"), hRef("visible"))
			rules.setRef(hidden.ID, false)
			allowed := f.callRPC(t, hAgent(), "tx.apply", hierarchyLifecycleMixedParams(2020, &stale))
			if allowed.Error == nil || allowed.Error.Data.Code != "precondition_failed" || allowed.Error.Data.Details["reason"] != "revision" || allowed.Error.Data.Details["currentRevision"] == nil {
				t.Fatalf("authorized stale expectation did not release typed CAS: %+v", allowed)
			}
			hierarchyLifecycleSameState(t, f, beforeHead, beforeRows, hRef("child"), hRef("visible"))

			f = newHierarchyLifecycleFixture(t, engine)
			rules = &hierarchyLifecycleRules{}
			f.app.Checker.chain.Rules = []authz.RuleSet{rules}
			hiddenRevision := hierarchyLifecycleRevision(t, f, hidden)
			validExpectation := model.PreconditionByRevision(hidden, hiddenRevision)
			valid := hierarchyLifecycleMixedParams(2021, &validExpectation)
			first := f.callRPC(t, hAgent(), "tx.apply", valid)
			if first.Error != nil {
				t.Fatalf("mixed request with authorized expectation failed: %+v", first)
			}
			committedHead, _ := hierarchyLifecycleState(t, f, hRef("child"), hRef("visible"))
			rules.setRef(hidden.ID, true)
			replayDenied := f.callRPC(t, hAgent(), "tx.apply", valid)
			hierarchyLifecycleForbidden(t, replayDenied)
			hierarchyLifecycleSameState(t, f, committedHead, map[string]driver.Record{
				"child":   hierarchyLifecycleRecord(t, f, hRef("child")),
				"visible": hierarchyLifecycleRecord(t, f, hRef("visible")),
			}, hRef("child"), hRef("visible"))
			rules.setRef(hidden.ID, false)
			replay := f.callRPC(t, hAgent(), "tx.apply", valid)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("current authorized mixed replay failed: %+v", replay)
			}
		})
	}
}

func hierarchyLifecycleRecord(t *testing.T, f *hFixture, ref model.Ref) driver.Record {
	t.Helper()
	rows, err := f.snapshot(t).Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("record %s: rows=%+v err=%v", ref.ID, rows, err)
	}
	return rows[0]
}
