package gatecontract_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These explicit port faults preserve the actual engine and public RPC. They
// qualify error handling, not native recordings or a second storage engine.
type defaultGateQueryStore struct {
	driver.Store
	missing bool
	fail    bool
}

func TestB14ExplicitIssueOnlyCatalogNeverInjectsDefaultGate(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		seedGateCatalog(t, f, ontology.DefaultDocument())
		check := func() {
			t.Helper()
			before, history := f.meta(), f.historyCount()
			f.call("bead.create", map[string]any{"type": "gate", "fields": map[string]any{"title": "absent from explicit source"}}, true, "invalid_params")
			f.call("ontology.schema", map[string]any{"project": "local", "type": "gate"}, false, "not_found")
			items := f.call("gate.list", map[string]any{}, false, "")["items"].([]any)
			if len(items) != 0 || !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
				t.Fatal("explicit issue-only catalog injected a gate or refusal changed history")
			}
		}
		check()
		if f.engine != "memory" {
			f.close()
			f.open()
			bindGateHost(t, f, f.store, true, false)
			check()
		}
	})
}

func (s *defaultGateQueryStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &defaultGateQueryReader{Reader: r, owner: s}, nil
}

type defaultGateQueryReader struct {
	driver.Reader
	owner *defaultGateQueryStore
}

func (r *defaultGateQueryReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	if r.owner.fail {
		return driver.Page[driver.Record]{}, &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "B14 default gate query fault"}}
	}
	page, err := r.Reader.Query(ctx, query)
	if err == nil && r.owner.missing {
		page.Items = append([]driver.Record{{Kind: "bead"}}, page.Items...)
	}
	return page, err
}

func TestB14DefaultGateMissingRowsAndQueryErrors(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		created := f.call("bead.create", map[string]any{"type": "gate", "fields": map[string]any{"title": "default gate"}}, true, "")
		id := resultID(created, 0)
		wrapped := &defaultGateQueryStore{Store: f.store, missing: true}
		f.store = wrapped
		f.bind()
		before, history := f.meta(), f.historyCount()
		got := f.call("gate.list", map[string]any{}, false, "")["items"].([]any)
		if len(got) != 1 || got[0].(map[string]any)["id"] != id {
			t.Fatalf("missing issue row replaced or hid the real default gate: %v", got)
		}
		wrapped.fail = true
		response := f.call("gate.list", map[string]any{}, false, "internal_error")
		if response["result"] != nil {
			t.Fatal("query failure returned a partial successful result")
		}
		wrapped.fail = false
		if !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
			t.Fatal("default listing or query error changed committed state/history")
		}
	})
}
