package gatecontract_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB14PublicGateListingCanonicalFiltersAndLimit(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		installGateCatalog(t, f)
		// Non-gates with await fields, closed gates and templates precede
		// the first checkable gate in the real driver query order.
		gateCreate(f, "issue", "gh:run", map[string]any{"priority": 0})
		closed := gateCreate(f, "gate", "timer", map[string]any{"priority": 0})
		template := gateCreate(f, "gate", "timer", map[string]any{"template": true, "priority": 0})
		pinned := gateCreate(f, "gate", "timer", map[string]any{"priority": 1})
		failed := gateCreate(f, "gate", "gh:run", map[string]any{"priority": 2})
		f.call("bead.fail", map[string]any{"id": failed}, true, "")
		canceled := gateCreate(f, "gate", "gh:pr", map[string]any{"priority": 3})
		f.call("bead.cancel", map[string]any{"id": canceled}, true, "")
		backlog := gateCreate(f, "gate", "opaque", map[string]any{"priority": 4})
		f.call("bead.defer", map[string]any{"id": backlog}, true, "")
		otherCompleted := gateCreate(f, "gate", "opaque", map[string]any{"priority": 4})
		f.call("bead.close", map[string]any{"id": otherCompleted}, true, "")
		// Install literal Beads status names/categories through the public
		// catalog API. Existing slots select them; no alternate lifecycle
		// implementation or passive pinned flag stands in for a native status.
		candidate := gateCatalogDocument()
		candidate.Statuses["pinned"] = ontology.Status{Name: "Pinned", Category: ontology.Frozen}
		candidate.Statuses["closed"] = ontology.Status{Name: "Closed", Category: ontology.Done, Outcome: ontology.Success}
		workflow := candidate.Workflows["issue"]
		workflow.Statuses = append(workflow.Statuses, "pinned", "closed")
		workflow.Slots[ontology.Defer], workflow.Slots[ontology.Close] = "pinned", "closed"
		workflow.Transitions = append(workflow.Transitions, ontology.Rule{To: "pinned"}, ontology.Rule{To: "closed", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}})
		candidate.Workflows["issue"] = workflow
		gateApply(f, gatePlan(f, candidate, []any{}, 2), 2)
		f.call("bead.defer", map[string]any{"id": pinned}, true, "")
		f.call("bead.close", map[string]any{"id": closed}, true, "")
		if f.bead(pinned)["status"] != "pinned" || f.bead(closed)["status"] != "closed" || f.bead(otherCompleted)["status"] != "done" {
			t.Fatal("literal installed status was normalized or not selected")
		}
		before, history := f.meta(), f.historyCount()
		for _, test := range []struct {
			name   string
			params map[string]any
			want   []string
		}{
			{"default-includes-template-pinned-failed-canceled-backlog", map[string]any{}, []string{template, pinned, failed, canceled, otherCompleted, backlog}},
			{"all-admits-closed-alongside-open", map[string]any{"all": true}, []string{template, closed, pinned, failed, canceled, otherCompleted, backlog}},
			{"default-template-consumes-limit", map[string]any{"limit": 1}, []string{template}},
			{"check-template-filter-before-limit", map[string]any{"excludeTemplates": true, "limit": 1}, []string{pinned}},
			{"all-and-exclude-templates", map[string]any{"all": true, "excludeTemplates": true}, []string{closed, pinned, failed, canceled, otherCompleted, backlog}},
		} {
			t.Run(test.name, func(t *testing.T) {
				if got := gateIDs(f.call("gate.list", test.params, false, "")); !reflect.DeepEqual(got, test.want) {
					t.Fatalf("gate.list=%v want=%v", got, test.want)
				}
			})
		}
		for _, limit := range []any{-1, json.Number("9223372036854775808"), 0.5, "1", false} {
			f.call("gate.list", map[string]any{"limit": limit}, false, "invalid_params")
		}
		f.call("gate.list", map[string]any{"limit": json.Number("9223372036854775807")}, false, "")
		if !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
			t.Fatal("listing or refused options wrote state/history")
		}
	})
}

func TestB14PublicGateListingCheckAwaitTypeAfterLimit(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		installGateCatalog(t, f)
		timer := gateCreate(f, "gate", "timer", map[string]any{"priority": 0})
		gh := gateCreate(f, "gate", "gh:run", map[string]any{"priority": 1})
		exact := gateCreate(f, "gate", "opaque", map[string]any{"priority": 2})
		for _, test := range []struct {
			params map[string]any
			want   []string
		}{
			{map[string]any{"excludeTemplates": true, "limit": 1, "awaitType": "gh"}, []string{}},
			{map[string]any{"excludeTemplates": true, "limit": 2, "awaitType": "gh"}, []string{gh}},
			{map[string]any{"awaitType": "gh:run"}, []string{gh}},
			{map[string]any{"awaitType": "opaque"}, []string{exact}},
			{map[string]any{"awaitType": "all"}, []string{timer, gh, exact}},
			{map[string]any{"awaitType": "missing"}, []string{}},
		} {
			if got := gateIDs(f.call("gate.list", test.params, false, "")); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("%v: got=%v want=%v", test.params, got, test.want)
			}
		}
	})
}

// This wrapper preserves the real driver, actual rows and cursor traversal.
// Fault modes are explicit query-port controls, never native recordings.
type gateQueryStore struct {
	driver.Store
	queries int
	failAt  int
	missing bool
}

func (s *gateQueryStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &gateQueryReader{Reader: r, owner: s}, nil
}

type gateQueryReader struct {
	driver.Reader
	owner *gateQueryStore
}

func (r *gateQueryReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.owner.queries++
	if r.owner.failAt == r.owner.queries {
		return driver.Page[driver.Record]{}, &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "named gate query failure"}}
	}
	page, err := r.Reader.Query(ctx, q)
	if err == nil && r.owner.missing {
		page.Items = append([]driver.Record{{Kind: "bead"}, {Kind: "other", Body: []byte(`{}`)}}, page.Items...)
	}
	return page, err
}

func TestB14PublicGateListingDrainsPagesAndPropagatesQueryFailures(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		installGateCatalog(t, f)
		want := []string{}
		for start := 0; start < 1003; start += 500 {
			ops := []any{}
			for n := start; n < start+500 && n < 1003; n++ {
				ops = append(ops, map[string]any{"kind": "create", "type": "gate", "fields": map[string]any{"title": fmt.Sprintf("page gate %04d", n)}})
			}
			commit := f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": ops}}, true, "")
			for i := range ops {
				want = append(want, resultID(commit, i))
			}
		}
		wrapped := &gateQueryStore{Store: f.store}
		bindGateHost(t, f, wrapped, true, false)
		before, history := f.meta(), f.historyCount()
		got := gateIDs(f.call("gate.list", map[string]any{"limit": 0}, false, ""))
		set := append([]string(nil), got...)
		sort.Strings(set)
		if !reflect.DeepEqual(set, want) || wrapped.queries < 2 {
			t.Fatalf("unlimited real pages: rows=%d want=%d queries=%d", len(got), len(want), wrapped.queries)
		}
		t.Logf("actual %s gate query pages=%d rows=%d", f.engine, wrapped.queries, len(got))
		wrapped.queries, wrapped.missing = 0, true
		if retained := gateIDs(f.call("gate.list", map[string]any{}, false, "")); !reflect.DeepEqual(retained, got) {
			t.Fatal("missing rows changed retained query order")
		}
		wrapped.queries, wrapped.failAt = 0, 2
		f.call("gate.list", map[string]any{}, false, "internal_error")
		wrapped.queries, wrapped.failAt = 0, 1
		f.call("gate.list", map[string]any{}, false, "internal_error")
		if !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
			t.Fatal("query failure or missing row handling wrote state/history")
		}
	})
}

func TestB14PublicGateListingBothPlanesAndDurableReopen(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		installGateCatalog(t, f)
		bindGateHost(t, f, f.store, true, true)
		ordinary := gateCreate(f, "gate", "timer", map[string]any{"priority": 4})
		invocation := map[string]any{"template": "b14-gate-list", "project": "local", "mode": "ephemeral", "checklist": []any{
			map[string]any{"label": "open", "title": "ephemeral gate", "type": "gate", "fields": map[string]any{"await_type": "gh:run", "priority": 0}},
			map[string]any{"label": "closed", "title": "closed ephemeral gate", "type": "gate", "fields": map[string]any{"priority": 1}},
			map[string]any{"label": "ordinary", "title": "not a gate", "type": "issue", "fields": map[string]any{"await_type": "gh:run"}},
		}}
		if f.engine == "memory" {
			f.refuse("x.workflow.invoke", invocation, "plugin_durable_state_unavailable")
			if got := gateIDs(f.call("gate.list", map[string]any{}, false, "")); !reflect.DeepEqual(got, []string{ordinary}) {
				t.Fatalf("ordinary memory listing after durable refusal=%v", got)
			}
			t.Log("memory: durable invocation refused without a receipt; ordinary gate listing passed; no ephemeral-positive credit")
			return
		}
		invoked := f.call("x.workflow.invoke", invocation, true, "")
		items := invoked["items"].([]any)
		open, closed := items[0].(map[string]any)["id"].(string), items[1].(map[string]any)["id"].(string)
		f.call("bead.close", map[string]any{"id": closed}, true, "")
		check := func() {
			t.Helper()
			before, history := f.meta(), f.historyCount()
			if got := gateIDs(f.call("gate.list", map[string]any{"_meta": map[string]any{"agent": "gate-contract"}}, false, "")); !reflect.DeepEqual(got, []string{open, ordinary}) {
				t.Fatalf("both planes=%v", got)
			}
			all := gateIDs(f.call("gate.list", map[string]any{"all": true, "_meta": map[string]any{"agent": "gate-contract"}}, false, ""))
			if !reflect.DeepEqual(all, []string{open, closed, ordinary}) {
				t.Fatalf("all both planes=%v", all)
			}
			if !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
				t.Fatal("both-plane read wrote core state/history")
			}
		}
		check()
		if f.engine != "memory" {
			f.close()
			f.open()
			bindGateHost(t, f, f.store, true, true)
			check()
		}
	})
}

func TestB14PublicGateListingGlobalPriorityCreationAndIDOrder(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		installGateCatalog(t, f)
		now := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
		bindGateHost(t, f, f.store, true, true, func() time.Time { return now })
		low := gateCreate(f, "gate", "timer", map[string]any{"priority": 4})
		invoke := func(label string, priority int) string {
			t.Helper()
			result := f.call("x.workflow.invoke", map[string]any{"template": "b14-order-" + label, "project": "local", "mode": "ephemeral", "checklist": []any{map[string]any{"label": label, "title": label, "type": "gate", "fields": map[string]any{"priority": priority}}}}, true, "")
			return result["items"].([]any)[0].(map[string]any)["id"].(string)
		}
		// Every engine proves ordinary priority, creation and ID ordering.
		// SQL engines additionally prove the combined provider/ordinary order.
		high := gateCreate(f, "gate", "timer", map[string]any{"priority": 0})
		now = now.Add(time.Hour)
		older := gateCreate(f, "gate", "timer", map[string]any{"priority": 2})
		now = now.Add(time.Hour)
		newer := gateCreate(f, "gate", "timer", map[string]any{"priority": 2})
		tieA := gateCreate(f, "gate", "timer", map[string]any{"priority": 2})
		tieB := gateCreate(f, "gate", "timer", map[string]any{"priority": 2})
		ties := []string{newer, tieA, tieB}
		sort.Strings(ties)
		want := append([]string{high}, ties...)
		want = append(want, older, low)
		meta := map[string]any{"agent": "gate-contract"}
		check := func(plane string, order []string, boundary int) {
			t.Helper()
			before, history := f.meta(), f.historyCount()
			for _, test := range []struct {
				limit int
				want  []string
			}{{0, order}, {1, order[:1]}, {boundary, order[:boundary]}} {
				if got := gateIDs(f.call("gate.list", map[string]any{"limit": test.limit, "_meta": meta}, false, "")); !reflect.DeepEqual(got, test.want) {
					t.Fatalf("%s order limit%d=%v want=%v", plane, test.limit, got, test.want)
				}
			}
			if !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
				t.Fatal("ordered gate read wrote state/history")
			}
		}
		check("ordinary", want, 4)
		if f.engine == "memory" {
			f.refuse("x.workflow.invoke", map[string]any{"template": "b14-order-refusal", "project": "local", "mode": "ephemeral", "checklist": []any{map[string]any{"label": "unavailable", "title": "unavailable", "type": "gate"}}}, "plugin_durable_state_unavailable")
			check("ordinary-after-durable-refusal", want, 4)
			t.Log("memory: ordinary order positive and durable invocation refusal; no both-plane-positive credit")
			return
		}
		now = now.Add(time.Hour)
		ephemeralHigh := invoke("priority-winner", 0)
		ephemeralNewer := invoke("created-winner", 2)
		crossPlaneTie := gateCreate(f, "gate", "timer", map[string]any{"priority": 2})
		crossPlaneTies := []string{ephemeralNewer, crossPlaneTie}
		sort.Strings(crossPlaneTies)
		combined := append([]string{ephemeralHigh, high}, crossPlaneTies...)
		combined = append(combined, ties...)
		combined = append(combined, older, low)
		check("both-planes", combined, 4)
	})
}

func TestB14PublicGateListingNoQuerierUnsupported(t *testing.T) {
	reg, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(reg, service.New(nil, nil, nil)); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(reg, rpc.Config{DefaultProject: "local", Incarnation: "b14-no-query-source"})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, server: server}
	f.call("gate.list", map[string]any{}, false, "unsupported")
}
