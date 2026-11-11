package s1b17b20

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestB18PublicHistoryGetDiffUseCurrentReader(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			writes := newPrecisionClient(t, p.rpc(t))
			at := p.now
			created := success[model.Commit](t, "create", writes.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "old readable content"}}, true))
			ref := created.Results[0].Ref
			p.advance(t)
			updated := success[model.Commit](t, "update", writes.call(t, "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "current readable content"}}, true))
			before := precisionState(t, p, writes, ref, "permission-source")
			check := func(t *testing.T) {
				reads := newPrecisionClient(t, guardedInstantRuntime(t, p))
				selectors := []map[string]any{{"revision": created.Revision, "incarnation": before.Meta.Incarnation}, instantAt(at)}
				for _, selector := range selectors {
					got := success[struct {
						Revision model.Revision `json:"revision"`
						Body struct { Fields map[string]json.RawMessage `json:"fields"` } `json:"body"`
					}](t, "authorized history.get", reads.call(t, "history.get", map[string]any{"ref": ref, "asOf": selector}, false))
					if got.Revision.String() != created.Revision.String() || string(got.Body.Fields["title"]) != `"old readable content"` {
						t.Fatalf("old address substituted content: %+v", got)
					}
				}
				diffParams := map[string]any{"ref": ref, "fromRevision": created.Revision, "toRevision": updated.Revision}
				diff := success[struct { Patch []b18LiteralPatch `json:"patch"` }](t, "authorized history.diff", reads.call(t, "history.diff", diffParams, false))
				if !reflect.DeepEqual(diff.Patch, []b18LiteralPatch{{Op: "replace", Path: "/fields/title", Value: json.RawMessage(`"current readable content"`)}}) {
					t.Fatalf("history.diff did not use exact bodies: %+v", diff)
				}
				// A sequential authenticated caller-caveat change is an ordinary
				// current-reader boundary. No record or policy is edited to simulate it.
				reads.r.principal.Caveats = map[string]any{"projects": []model.Ref{model.RefByID("other", model.Natural64(0))}}
				for _, selector := range selectors {
					denied := reads.call(t, "history.get", map[string]any{"ref": ref, "asOf": selector}, false)
					if denied.Error == nil || denied.Error.Data.Code != "not_found" || len(denied.Result) != 0 {
						t.Fatalf("history.get bypassed current reader: %+v", denied)
					}
					page := success[struct { Items []json.RawMessage `json:"items"` }](t, "current history.query", reads.call(t, "history.query", map[string]any{"asOf": selector}, false))
					if len(page.Items) != 0 { t.Fatal("history.query leaked historical content") }
				}
				denied := reads.call(t, "history.diff", diffParams, false)
				if denied.Error == nil || denied.Error.Data.Code != "not_found" || len(denied.Result) != 0 { t.Fatalf("history.diff bypassed current reader: %+v", denied) }
				reads.r.principal.Caveats = map[string]any{"projects": []model.Ref{model.RefByID("cardb20", model.Natural64(0))}}
				success[json.RawMessage](t, "restored current reader", reads.call(t, "history.get", map[string]any{"ref": ref, "asOf": selectors[0]}, false))
				if !reflect.DeepEqual(before, precisionState(t, p, writes, ref, "permission-invariance")) { t.Fatal("public history changed source or history") }
			}
			t.Run("before_reopen", check)
			if engine != "memory" { p.reopen(t); writes.r = p.rpc(t); t.Run("after_reopen", check) }
		})
	}
}
