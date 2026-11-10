package converterroundtrip

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestImportedSQLWaitsMetadataUsesTypedPredicateAndKeepsSource(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  any
		want *model.LinkMetadata
	}{
		{"absent", nil, nil},
		{"explicit-default", "{}", &model.LinkMetadata{Gate: "all-children"}},
		{"sql-text-any-also", " {\"gate\":\"any-children\",\"also_blocks\":true} \n", &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Use the separately admitted external vocabulary until a real new
			// producer contract binds local kinds. This is parser/planner proof.
			base, err := dumpformat.Read(bytes.NewReader(externalImportBytes(t, " foreign:spawner ", "waits-for")))
			if err != nil {
				t.Fatal(err)
			}
			data := rewriteData(t, func(members map[string][]byte) {
				clear(members)
				members["manifest.json"], _ = base.Member("manifest.json")
				for name := range base.Manifest().Files {
					members[name], _ = base.Member(name)
				}
				changeRecord(t, members, "edge", func(record map[string]any) {
					source(record)["metadata"], body(record)["metadata"] = test.raw, test.raw
				})
			})
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := buildPlanForInspection(archive)
			if err != nil {
				t.Fatal(err)
			}
			edge := archive.Records("edge")[0]
			foundDocument, foundSource := false, false
			for _, row := range plan.records {
				if row.ref.ID == edge.Body["from"] {
					doc, err := pipeline.DecodeDocument(row.body)
					if err != nil || len(doc.Links) != 1 || doc.Links[0].TargetKind != model.LinkTargetExternal || doc.Links[0].To.ID != " foreign:spawner " || !reflect.DeepEqual(doc.Links[0].Metadata, test.want) {
						t.Fatalf("typed waits semantics changed: %+v %v", doc, err)
					}
					foundDocument = true
				}
				if row.ref.ID == "source/edge/"+edge.ID {
					var saved map[string]any
					if err := decodeReverseJSON(row.body, &saved); err != nil || !reflect.DeepEqual(saved["metadata"], test.raw) || !reflect.DeepEqual(saved["source"], map[string]any(edge.Source.Fields)) {
						t.Fatal("raw edge metadata or complete SQL source provenance changed")
					}
					foundSource = true
				}
			}
			if !foundDocument || !foundSource || !bytes.Equal(plan.blobs[archive.SHA256()], data) {
				t.Fatal("typed document, raw source edge, or exact archive envelope lost")
			}
		})
	}
}

// These are independent semantic regressions, not native producer recordings.
// Mixed children and a completed spawner distinguish all, any, also_blocks,
// and the absent-metadata legacy predicate; a kind allowlist cannot pass them.
func TestImportedDependencyDerivedPredicates(t *testing.T) {
	for _, test := range []struct {
		name, kind, status string
		metadata           *model.LinkMetadata
		children           []string
		blocked            bool
	}{
		{"all-mixed", "waits-for", "todo", &model.LinkMetadata{Gate: "all-children"}, []string{"todo", "done"}, true},
		{"any-mixed", "waits-for", "todo", &model.LinkMetadata{Gate: "any-children"}, []string{"todo", "done"}, false},
		{"any-open-only", "waits-for", "todo", &model.LinkMetadata{Gate: "any-children"}, []string{"todo"}, true},
		{"any-also-open-spawner", "waits-for", "todo", &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}, []string{"todo", "done"}, true},
		{"any-also-closed-spawner", "waits-for", "done", &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}, []string{"todo", "done"}, false},
		{"all-closed-spawner-open-child", "waits-for", "done", &model.LinkMetadata{Gate: "all-children"}, []string{"todo"}, true},
		{"legacy-closed-spawner-open-child", "waits-for", "done", nil, []string{"todo"}, false},
		{"legacy-open-spawner-open-child", "waits-for", "todo", nil, []string{"todo"}, true},
		{"legacy-open-spawner-terminal-child", "waits-for", "todo", nil, []string{"failed"}, false},
		{"empty-any", "waits-for", "todo", &model.LinkMetadata{Gate: "any-children"}, nil, false},
		{"empty-all", "waits-for", "todo", &model.LinkMetadata{Gate: "all-children"}, nil, false},
		{"empty-also", "waits-for", "todo", &model.LinkMetadata{Gate: "all-children", AlsoBlocks: true}, nil, true},
		{"condition-success", "conditional-blocks", "done", nil, nil, true},
		{"condition-failure", "conditional-blocks", "failed", nil, nil, false},
		{"condition-canceled", "conditional-blocks", "canceled", nil, nil, true},
		{"condition-open", "conditional-blocks", "todo", nil, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			docs := dependencyDocuments(test.kind, test.status, test.metadata)
			for i, status := range test.children {
				id := "demo-child-" + string(rune('a'+i))
				doc := dependencyDocument(id, status)
				parent := model.RefByID("demo-spawner", model.Natural64(0))
				doc.Parent = &parent
				docs[id] = doc
			}
			var plan restorePlan
			if err := rebuildDerived(&plan, docs); err != nil {
				t.Fatal(err)
			}
			requireDependencyReady(t, plan, "demo-waiter", test.blocked)
		})
	}
}

func dependencyDocument(id, status string) pipeline.Document {
	return pipeline.Document{ID: id, Type: "issue", Project: "demo", Fields: map[string]any{"title": id, "status": status, "priority": json.Number("2")}}
}

func dependencyDocuments(kind, status string, metadata *model.LinkMetadata) map[string]pipeline.Document {
	waiter := dependencyDocument("demo-waiter", "todo")
	waiter.Links = []pipeline.Link{{Kind: kind, To: model.RefByID("demo-spawner", model.Natural64(0)), Metadata: metadata}}
	return map[string]pipeline.Document{waiter.ID: waiter, "demo-spawner": dependencyDocument("demo-spawner", status)}
}

func requireDependencyReady(t *testing.T, plan restorePlan, id string, blocked bool) {
	t.Helper()
	requireDependencyState(t, plan, id, blocked, !blocked)
}

func requireDependencyState(t *testing.T, plan restorePlan, id string, blocked, ready bool) {
	t.Helper()
	found := 0
	for _, row := range plan.derived {
		if row.Table != driver.DerivedReadyState || row.Key != id {
			continue
		}
		found++
		var state struct{ Blocked, Ready bool }
		if err := json.Unmarshal(row.Value, &state); err != nil || state.Blocked != blocked || state.Ready != ready {
			t.Fatalf("derived readiness for %s: %s want blocked=%v ready=%v err=%v", id, row.Value, blocked, ready, err)
		}
	}
	if found != 1 {
		t.Fatalf("readiness census for %s: %d", id, found)
	}
}

func TestImportedDependencyAncestorScheduling(t *testing.T) {
	for _, test := range []struct {
		name, parentStatus, kind, blockerStatus string
		ready                                   bool
	}{
		{"ordinary-open-parent", "todo", "", "todo", true},
		{"backlog-grandparent", "backlog", "", "done", false},
		{"conditional-failure-grandparent", "todo", "conditional-blocks", "failed", true},
		{"conditional-success-grandparent", "todo", "conditional-blocks", "done", false},
		{"waits-grandparent-open-child", "todo", "waits-for", "todo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			docs := dependencyDocuments(test.kind, test.blockerStatus, nil)
			ancestor := docs["demo-waiter"]
			ancestor.Fields["status"] = test.parentStatus
			if test.kind == "" {
				ancestor.Links = nil
			}
			docs[ancestor.ID] = ancestor
			for _, pair := range [][2]string{{"demo-parent", ancestor.ID}, {"demo-descendant", "demo-parent"}, {"demo-openchild", "demo-spawner"}} {
				doc := dependencyDocument(pair[0], "todo")
				parent := model.RefByID(pair[1], model.Natural64(0))
				doc.Parent = &parent
				docs[doc.ID] = doc
			}
			var plan restorePlan
			if err := rebuildDerived(&plan, docs); err != nil {
				t.Fatal(err)
			}
			requireDependencyState(t, plan, "demo-descendant", false, test.ready)
		})
	}
}

func TestImportedDependencyGraphAndTypedExternalIsolation(t *testing.T) {
	for _, kind := range []string{"blocks", "conditional-blocks", "waits-for"} {
		t.Run(kind+"-ancestor-refusal", func(t *testing.T) {
			docs := dependencyDocuments(kind, "done", nil)
			waiter := docs["demo-waiter"]
			parent := model.RefByID("demo-spawner", model.Natural64(0))
			waiter.Parent = &parent
			docs[waiter.ID] = waiter
			if err := rebuildDerived(&restorePlan{}, docs); err == nil {
				t.Fatal("blocking ancestor relationship admitted")
			}
		})
		t.Run(kind+"-external-local-collision", func(t *testing.T) {
			docs := dependencyDocuments(kind, "failed", nil)
			waiter := docs["demo-waiter"]
			waiter.Links[0].TargetKind = model.LinkTargetExternal
			docs[waiter.ID] = waiter
			var plan restorePlan
			if err := rebuildDerived(&plan, docs); err != nil {
				t.Fatal(err)
			}
			requireDependencyReady(t, plan, waiter.ID, true)
		})
	}
	t.Run("conditional-cycle", func(t *testing.T) {
		docs := dependencyDocuments("conditional-blocks", "failed", nil)
		spawner := docs["demo-spawner"]
		spawner.Links = []pipeline.Link{{Kind: "blocks", To: model.RefByID("demo-waiter", model.Natural64(0))}}
		docs[spawner.ID] = spawner
		if err := rebuildDerived(&restorePlan{}, docs); err == nil {
			t.Fatal("conditional scheduling cycle admitted")
		}
	})
	t.Run("external-parent-does-not-expand-local-fanout", func(t *testing.T) {
		docs := dependencyDocuments("waits-for", "todo", &model.LinkMetadata{Gate: "all-children"})
		child := dependencyDocument("demo-external-child", "todo")
		child.Links = []pipeline.Link{{Kind: "parent-child", TargetKind: model.LinkTargetExternal, To: model.RefByID("demo-spawner", model.Natural64(0))}}
		docs[child.ID] = child
		var plan restorePlan
		if err := rebuildDerived(&plan, docs); err != nil {
			t.Fatal(err)
		}
		requireDependencyReady(t, plan, "demo-waiter", false)
	})
}
