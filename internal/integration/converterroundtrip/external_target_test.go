package converterroundtrip

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is a generated archive contract control, not native producer evidence.
func externalImportBytes(t *testing.T, target, kind string) []byte {
	t.Helper()
	admitted, err := dumpformat.Read(bytes.NewReader(admittedFixtureBytes(t)))
	if err != nil {
		t.Fatal(err)
	}
	return rewriteData(t, func(m map[string][]byte) {
		for name := range m {
			delete(m, name)
		}
		m["manifest.json"], _ = admitted.Member("manifest.json")
		for name := range admitted.Manifest().Files {
			m[name], _ = admitted.Member(name)
		}
		changeRecord(t, m, "edge", func(r map[string]any) {
			source(r)["depends_on_issue_id"] = nil
			source(r)["depends_on_external"] = target
			source(r)["type"], body(r)["type"] = kind, kind
			body(r)["target_kind"], body(r)["to"] = "depends_on_external", target
			if kind == "waits-for" {
				metadata := map[string]any{"gate": "any-children", "also_blocks": true}
				source(r)["metadata"], body(r)["metadata"] = metadata, metadata
			}
		})
	})
}

func TestRestoreAndLiveReverseRetainTypedExternalIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, test := range []struct{ target, kind string }{
				{"external:tracker/1", "blocks"},
				{"demo-7", "waits-for"},
				{"demo-7", "parent-child"},
			} {
				t.Run(test.kind, func(t *testing.T) {
					ctx := context.Background()
					data := externalImportBytes(t, test.target, test.kind)
					profile, archive := admittedImportProfile(t, data)
					store, lease, cleanup := openReverseTarget(t, engine)
					defer cleanup()
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
						t.Fatal(err)
					}
					from := stringValue(archive.Records("edge")[0].Body["from"])
					meta, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					rows, err := r.Get(ctx, []model.Ref{model.RefByID(from, model.Natural64(0))})
					if err != nil || len(rows) != 1 {
						t.Fatalf("restored source: %v", err)
					}
					doc, err := pipeline.DecodeDocument(rows[0].Body)
					if err != nil || doc.Parent != nil || len(doc.Links) != 1 || doc.Links[0].TargetKind != model.LinkTargetExternal || doc.Links[0].To.ID != test.target {
						t.Fatalf("restored typed target=%+v err=%v", doc, err)
					}
					if test.kind == "waits-for" && (doc.Links[0].Metadata == nil || doc.Links[0].Metadata.Gate != "any-children" || !doc.Links[0].Metadata.AlsoBlocks) {
						t.Fatal("waits-for metadata lost")
					}
					r.Close()
					if err := pipeline.BuildSourceIndexes(ctx, store, lease); err != nil {
						t.Fatal(err)
					}
					before := reverseState(t, store)
					reversed, err := ReverseRestored(ctx, store)
					if err != nil || len(reversed.Result.Projection.Edges) != 1 {
						t.Fatalf("live reverse: %v", err)
					}
					edge := reversed.Result.Projection.Edges[0]
					if edge.TargetKind != "depends_on_external" || edge.To != test.target || edge.Type != test.kind {
						t.Fatalf("reverse flattened target: %+v", edge)
					}
					if !bytes.Contains(reversed.Result.JSONL, []byte(`"depends_on_external":"`+test.target+`"`)) {
						t.Fatalf("typed output absent: %s", reversed.Result.JSONL)
					}
					if !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatal("reverse mutated store")
					}
				})
			}
		})
	}
}
