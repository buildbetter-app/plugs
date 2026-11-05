package gatecontract_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func targetWrite(producer string, op map[string]any) (string, map[string]any) {
	if producer == "tx.apply" {
		kind := op["kind"]
		op["kind"], op["linkKind"] = "link", kind
		return producer, map[string]any{"changeSet": map[string]any{"ops": []any{op}}}
	}
	return producer, op
}

func TestB14ExplicitExternalWaitsForRetainsUnresolvedAcrossBothProducers(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(producer, func(t *testing.T) {
				source, _ := f.create(map[string]any{"title": "external waiter"}, "")
				method, params := targetWrite(producer, map[string]any{"from": ref(source), "to": ref("missing-spawner"), "targetKind": "external", "kind": "waits-for", "metadata": map[string]any{}})
				first := f.call(method, params, true, "")
				key, revision := f.lastKey, first["revision"]
				assertExternal := func(body map[string]any) {
					t.Helper()
					assertMetadata(t, body, "all-children", false)
					link := body["links"].([]any)[0].(map[string]any)
					if link["targetKind"] != "external" || link["to"].(map[string]any)["id"] != "missing-spawner" {
						t.Fatalf("external identity changed: %v", link)
					}
				}
				assertExternal(f.currentBody(source))
				before := f.meta()
				if replay := f.request(method, params, key, ""); !reflect.DeepEqual(first, replay) || !reflect.DeepEqual(before, f.meta()) {
					t.Fatal("external replay changed response or state")
				}
				f.call(method, params, true, "")
				if !reflect.DeepEqual(before, f.meta()) {
					t.Fatal("external duplicate minted a revision")
				}
				if f.engine != "memory" {
					f.close()
					f.open()
					assertExternal(f.currentBody(source))
					assertExternal(f.body(source, revision))
				}
				f.call("bead.unlink", map[string]any{"from": ref(source), "to": ref("missing-spawner"), "targetKind": "external", "kind": "waits-for"}, true, "")
				assertExternal(f.body(source, revision))
			})
		}
	})
}

func TestB14ExternalTargetShapeRefusalsAcrossBothProducers(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		source, _ := f.create(map[string]any{"title": "external waiter"}, "")
		for _, producer := range []string{"bead.link", "tx.apply"} {
			for _, marker := range []any{"", nil, "workflow", "future", 1} {
				method, params := targetWrite(producer, map[string]any{"from": ref(source), "to": ref("missing-spawner"), "targetKind": marker, "kind": "blocks"})
				f.refuse(method, params, "invalid_params")
			}
			for _, target := range []any{map[string]any{"key": "alias"}, map[string]any{"id": "x", "branch": 1}, ref("")} {
				method, params := targetWrite(producer, map[string]any{"from": ref(source), "to": target, "targetKind": "external", "kind": "blocks"})
				f.refuse(method, params, "invalid_params")
			}
		}
	})
}

func TestB14ExternalCollisionExcludedFromCurrentAndHistoricalSourceIndexes(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		ctx := context.Background()
		source, _ := f.create(map[string]any{"title": "dependent"}, "")
		target, _ := f.create(map[string]any{"title": "same-spelled local target"}, "")
		commit := f.call("bead.link", map[string]any{"from": ref(source), "to": ref(target), "targetKind": "external", "kind": "blocks"}, true, "")
		raw, err := json.Marshal(commit["revision"])
		if err != nil {
			t.Fatal(err)
		}
		var at model.Revision
		if err := json.Unmarshal(raw, &at); err != nil {
			t.Fatal(err)
		}
		f.call("bead.link", map[string]any{"from": ref(source), "to": ref(target), "kind": "blocks"}, true, "")
		if err := pipeline.BuildSourceIndexes(ctx, f.store, f.lease); err != nil {
			t.Fatal(err)
		}
		for _, historical := range []bool{true, false} {
			view := driver.View{Incarnation: f.meta().Incarnation}
			if historical {
				view.At = &at
			}
			r, err := f.store.Read(ctx, view)
			if err != nil {
				t.Fatal(err)
			}
			rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, []model.Ref{model.RefByID(target, model.Natural64(0))}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10)
			r.Close()
			want := 1
			if historical {
				want = 0
			}
			if err != nil || more || len(rows) != want {
				t.Fatalf("historical=%v local inverse rows=%d want=%d more=%v err=%v", historical, len(rows), want, more, err)
			}
		}
	})
}
