package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type importEdge struct{ from, to, kind string }

// These are detached authored regression inputs, never reference recordings.
// Keep the canonical fixture and all retained public capture bytes unchanged.
func hierarchyImportBytes(t *testing.T, edges []importEdge) []byte {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(admittedFixtureBytes(t)))
	if err != nil {
		t.Fatal(err)
	}
	return rewriteData(t, func(members map[string][]byte) {
		clear(members)
		for path := range archive.Manifest().Files {
			members[path], _ = archive.Member(path)
		}
		members["manifest.json"], _ = archive.Member("manifest.json")
		var output bytes.Buffer
		for i, edge := range edges {
			r := archive.Records("edge")[0]
			r.ID = fmt.Sprintf("hierarchy-edge-%d", i)
			r.Source.Fields["id"] = r.ID
			r.Source.Fields["issue_id"], r.Source.Fields["depends_on_issue_id"], r.Source.Fields["type"] = edge.from, edge.to, edge.kind
			r.Body["from"], r.Body["to"], r.Body["type"] = edge.from, edge.to, edge.kind
			output.Write(encode(t, r))
		}
		members["records/edge.ndjson"] = output.Bytes()
		changeManifest(t, members, func(m map[string]any) {
			for _, raw := range m["kind_registry"].([]any) {
				row := raw.(map[string]any)
				if row["name"] == "edge" {
					row["count"] = len(edges)
				}
			}
			for _, raw := range m["sources"].([]any) {
				row := raw.(map[string]any)
				if row["family"] == "dependencies" {
					row["rows"] = len(edges)
				}
			}
		})
	})
}

func assertImportParents(t *testing.T, store driver.Store, parents map[string]string) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	want := map[string]string{}
	for id, parent := range parents {
		rows, err := r.Get(ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatalf("restored bead %s: %v", id, err)
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil || parent == "" && doc.Parent != nil || parent != "" && (doc.Parent == nil || doc.Parent.ID != parent || !doc.Parent.Branch.IsZero()) {
			t.Fatalf("restored parent for %s: %+v, want %q, err=%v", id, doc.Parent, parent, err)
		}
		if len(doc.Links) != 0 {
			t.Fatalf("parent duplicated as generic link: %+v", doc.Links)
		}
		for current, depth := id, 0; current != ""; current, depth = parents[current], depth+1 {
			key := base64.RawURLEncoding.EncodeToString([]byte(id)) + "." + base64.RawURLEncoding.EncodeToString([]byte(current))
			want[key] = fmt.Sprint(depth)
		}
	}
	rows, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, row := range rows {
		got[row.Key] = string(row.Value)
	}
	if !reflect.DeepEqual(want, got) || len(rows) != len(want) {
		t.Fatalf("canonical transitive closure: got %v, want %v", got, want)
	}
}

func TestB27ImportedParentHierarchyAcrossDrivers(t *testing.T) {
	data := hierarchyImportBytes(t, []importEdge{{"demo-7.4", "demo-7", "parent-child"}, {"demo-8", "demo-7.4", "parent-child"}})
	profile, _ := admittedImportProfile(t, data)
	inspection, err := InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil || inspection.DerivedSHA256 != profile.DerivedDigest {
		t.Fatalf("parent inspection: %+v %v", inspection, err)
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var store driver.Store
			var lease driver.ActiveLease
			var path string
			if kind == "sqlite" {
				path = filepath.Join(t.TempDir(), "parent-import.sqlite")
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				lease, err = store.Lease(ctx, "parent-import", driver.LeaseTTL)
				if err != nil {
					_ = store.Close()
					t.Fatal(err)
				}
				defer func() { _ = lease.Release(ctx); _ = store.Close() }()
			} else {
				var cleanup func()
				store, lease, cleanup = openReverseTarget(t, kind)
				defer cleanup()
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			parents := map[string]string{"demo-7": "", "demo-7.4": "demo-7", "demo-8": "demo-7.4"}
			assertImportParents(t, store, parents)
			before, err := ReverseRestored(ctx, store)
			if err != nil || bytes.Count(before.Result.JSONL, []byte(`"type":"parent-child"`)) != 2 {
				t.Fatalf("exact parent reverse: %s %v", before.Result.JSONL, err)
			}
			if kind == "sqlite" {
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				lease, err = store.Lease(ctx, "parent-import-reopen", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				assertImportParents(t, store, parents)
				after, err := ReverseRestored(ctx, store)
				if err != nil || !bytes.Equal(before.Result.JSONL, after.Result.JSONL) {
					t.Fatalf("parent reverse changed after reopen: %v", err)
				}
			}
			writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
			meta := model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: strings.Repeat("d", 32)}
			// Reparenting traverses and guards the restored transitive closure;
			// a scalar-only patch would never discover an incompatible codec.
			_, err = writer.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: model.RefByID("demo-8", model.Natural64(0)), Parent: nil}}}}, meta)
			if err != nil {
				t.Fatalf("ordinary reparent after restore: %v", err)
			}
			parents["demo-8"] = ""
			assertImportParents(t, store, parents)
			state := reverseState(t, store)
			refused, err := ReverseRestored(ctx, store)
			if err == nil || !strings.Contains(err.Error(), "changed_source_relationships") || len(refused.Result.JSONL) != 0 || !reflect.DeepEqual(state, reverseState(t, store)) {
				t.Fatalf("changed parent reverse must refuse without output or mutation: %+v %v", refused, err)
			}
		})
	}
}

func TestB27InvalidImportedHierarchyRefusesBeforeWriteAcrossDrivers(t *testing.T) {
	base := hierarchyImportBytes(t, []importEdge{{"demo-7.4", "demo-7", "parent-child"}})
	profile, _ := admittedImportProfile(t, base)
	tests := []struct {
		name, cause string
		edges       []importEdge
	}{
		{"multiple-parents", "multiple parent edges", []importEdge{{"demo-7.4", "demo-7", "parent-child"}, {"demo-7.4", "demo-8", "parent-child"}}},
		{"parent-cycle", "cycle", []importEdge{{"demo-7.4", "demo-7", "parent-child"}, {"demo-7", "demo-7.4", "parent-child"}}},
		{"mixed-cycle", "cycle", []importEdge{{"demo-7.4", "demo-7", "parent-child"}, {"demo-7", "demo-7.4", "blocks"}}},
		{"ancestor-block", "ancestor and descendant", []importEdge{{"demo-7.4", "demo-7", "parent-child"}, {"demo-7.4", "demo-7", "blocks"}}},
		{"missing-parent", "dangling typed edge target", []importEdge{{"demo-7.4", "demo-missing", "parent-child"}}},
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openReverseTarget(t, kind)
			defer cleanup()
			target := &observedTarget{Store: store}
			before := reverseState(t, store)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					data := hierarchyImportBytes(t, test.edges)
					p := profile
					p.ArchiveDigest = digestBytes(data)
					_, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), target, lease, p)
					if err == nil || !strings.Contains(err.Error(), test.cause) || target.writes != 0 || !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatalf("invalid hierarchy was not atomically refused before writes: %v, writes=%d", err, target.writes)
					}
					if test.name == "missing-parent" {
						var unresolved *dumpformat.UnresolvedDependencyError
						if !errors.As(err, &unresolved) || unresolved.EdgeID != "hierarchy-edge-0" || unresolved.Project != "demo" || unresolved.Source != "demo-7.4" || unresolved.Target != "demo-missing" || unresolved.TargetKind != "depends_on_issue_id" {
							t.Fatalf("missing parent lost its typed endpoint report: %+v", err)
						}
					}
				})
			}
		})
	}
}
