package s1b05b08

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func (f *fixture) createWithParent(t *testing.T, title string, parent model.Ref) model.Ref {
	t.Helper()
	got := f.call(t, f.ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}, "parent": parent}, key("create-"+title), actor)
	var commit model.Commit
	if got.Error != nil || json.Unmarshal(got.Result, &commit) != nil || len(commit.Results) != 1 {
		t.Fatalf("parented create refused: %+v", got)
	}
	return commit.Results[0].Ref
}

func TestB06BlockingHierarchyRefusesBothDirectionsAcrossStores(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, representation := range []string{"parent-field", "parent-child-link"} {
				t.Run(representation, func(t *testing.T) {
					for _, direction := range []string{"descendant-to-ancestor", "ancestor-to-descendant"} {
						t.Run(direction, func(t *testing.T) {
							f := newFixture(t, engine)
							root := f.create(t, "root")
							var parent, child model.Ref
							if representation == "parent-field" {
								parent = f.createWithParent(t, "parent", root)
								child = f.createWithParent(t, "child", parent)
							} else {
								parent, child = f.create(t, "parent"), f.create(t, "child")
								for i, edge := range [][2]model.Ref{{parent, root}, {child, parent}} {
									got := f.call(t, f.ctx, "bead.link", map[string]any{"from": edge[0], "to": edge[1], "kind": "parent-child"}, key(string(rune('a'+i))), actor)
									if got.Error != nil {
										t.Fatalf("hierarchy seed refused: %s", got.Error.Data.Code)
									}
								}
							}
							from, to := child, root
							if direction == "ancestor-to-descendant" {
								from, to = root, child
							}
							before := []durableState{f.state(t, root), f.state(t, parent), f.state(t, child)}
							blocked := f.call(t, f.ctx, "bead.link", map[string]any{"from": from, "to": to, "kind": "blocks"}, key("ancestor-blocker"), actor)
							if blocked.Error == nil || blocked.Error.Data.Code != "ontology_guard_failed" {
								t.Fatalf("blocker between ancestor and descendant was not a typed refusal: %+v", blocked)
							}
							if after := []durableState{f.state(t, root), f.state(t, parent), f.state(t, child)}; !reflect.DeepEqual(before, after) {
								t.Fatalf("hierarchy refusal changed records/history: before=%+v after=%+v", before, after)
							}
							// Association annotations remain legal over the same hierarchy.
							associated := f.call(t, f.ctx, "bead.link", map[string]any{"from": from, "to": to, "kind": "relates-to"}, key("ancestor-association"), actor)
							if associated.Error != nil {
								t.Fatalf("association was treated as a blocker: %s", associated.Error.Data.Code)
							}
							f.reopen(t)
							if len(f.document(t, from).Links) == 0 {
								t.Fatal("association did not survive reopen")
							}
						})
					}
				})
			}
		})
	}
}
