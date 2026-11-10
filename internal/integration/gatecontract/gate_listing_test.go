package gatecontract_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB14PublicGateListingCanonicalFiltersAndLimit(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		// Non-gates with await fields, closed gates and templates precede
		// the first checkable gate in the real driver query order.
		gateCreate(f, "issue", "gh:run", nil)
		closed := gateCreate(f, "gate", "timer", nil)
		f.call("bead.close", map[string]any{"id": closed}, true, "")
		template := gateCreate(f, "gate", "timer", map[string]any{"template": true})
		pinned := gateCreate(f, "gate", "timer", map[string]any{"pinned": true})
		failed := gateCreate(f, "gate", "gh:run", nil)
		f.call("bead.fail", map[string]any{"id": failed}, true, "")
		canceled := gateCreate(f, "gate", "gh:pr", nil)
		f.call("bead.cancel", map[string]any{"id": canceled}, true, "")
		backlog := gateCreate(f, "gate", "opaque", nil)
		f.call("bead.defer", map[string]any{"id": backlog}, true, "")
		before, history := f.meta(), f.historyCount()
		for _, test := range []struct {
			name   string
			params map[string]any
			want   []string
		}{
			{"default-includes-template-pinned-failed-canceled-backlog", map[string]any{}, []string{template, pinned, failed, canceled, backlog}},
			{"all-admits-closed-alongside-open", map[string]any{"all": true}, []string{closed, template, pinned, failed, canceled, backlog}},
			{"default-template-consumes-limit", map[string]any{"limit": 1}, []string{template}},
			{"check-template-filter-before-limit", map[string]any{"excludeTemplates": true, "limit": 1}, []string{pinned}},
			{"all-and-exclude-templates", map[string]any{"all": true, "excludeTemplates": true}, []string{closed, pinned, failed, canceled, backlog}},
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
		timer := gateCreate(f, "gate", "timer", nil)
		gh := gateCreate(f, "gate", "gh:run", nil)
		exact := gateCreate(f, "gate", "opaque", nil)
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
		bindGateHost(t, f, wrapped, false, false)
		before, history := f.meta(), f.historyCount()
		got := gateIDs(f.call("gate.list", map[string]any{"limit": 0}, false, ""))
		if !reflect.DeepEqual(got, want) || wrapped.queries < 2 {
			t.Fatalf("unlimited real pages: rows=%d want=%d queries=%d", len(got), len(want), wrapped.queries)
		}
		t.Logf("actual %s gate query pages=%d rows=%d", f.engine, wrapped.queries, len(got))
		wrapped.queries, wrapped.missing = 0, true
		if got := gateIDs(f.call("gate.list", map[string]any{}, false, "")); !reflect.DeepEqual(got, want) {
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
		bindGateHost(t, f, f.store, false, true)
		ordinary := gateCreate(f, "gate", "timer", nil)
		invoked := f.call("x.workflow.invoke", map[string]any{"template": "b14-gate-list", "project": "local", "mode": "ephemeral", "checklist": []any{
			map[string]any{"label": "open", "title": "ephemeral gate", "type": "gate", "fields": map[string]any{"await_type": "gh:run"}},
			map[string]any{"label": "closed", "title": "closed ephemeral gate", "type": "gate"},
			map[string]any{"label": "ordinary", "title": "not a gate", "type": "issue", "fields": map[string]any{"await_type": "gh:run"}},
		}}, true, "")
		items := invoked["items"].([]any)
		open, closed := items[0].(map[string]any)["id"].(string), items[1].(map[string]any)["id"].(string)
		f.call("bead.close", map[string]any{"id": closed}, true, "")
		check := func() {
			t.Helper()
			before, history := f.meta(), f.historyCount()
			if got := gateIDs(f.call("gate.list", map[string]any{"_meta": map[string]any{"agent": "gate-contract"}}, false, "")); !reflect.DeepEqual(got, []string{ordinary, open}) {
				t.Fatalf("both planes=%v", got)
			}
			all := gateIDs(f.call("gate.list", map[string]any{"all": true, "_meta": map[string]any{"agent": "gate-contract"}}, false, ""))
			if len(all) != 3 || all[0] != ordinary {
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
			bindGateHost(t, f, f.store, false, true)
			check()
		}
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
	server, err := rpc.New(reg, rpc.Config{DefaultProject: "local"})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, server: server}
	f.call("gate.list", map[string]any{}, false, "unsupported")
}
