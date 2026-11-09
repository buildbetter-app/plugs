package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A separately versioned detached control, never a native capture or corpus.
// The waiter is unrelated to the spawner hierarchy; the spawner has one open
// and one terminal child, so all/any/also_blocks cannot collapse to one result.
func versionedDependencyBytes(t *testing.T, input []byte, kind string, metadata any, closedSpawner bool) []byte {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	beads := archive.Records("bead")
	newChild := beads[0]
	newChild.ID, newChild.Body, newChild.Source.Fields = "demo-9", cloneRow(newChild.Body), cloneRow(newChild.Source.Fields)
	newChild.Source.Fields["id"] = newChild.ID
	beads = append(beads, newChild)
	for i := range beads {
		row := &beads[i]
		if row.ID != "demo-8" {
			row.Source.Fields["status"], row.Body["status_category"] = "open", "unstarted"
			delete(row.Body, "result")
		}
		if row.ID == "demo-7" && closedSpawner {
			row.Source.Fields["status"], row.Body["status_category"], row.Body["result"] = "closed", "completed", "success"
		}
	}
	edge := archive.Records("edge")[0]
	edge.Body["type"], edge.Source.Fields["type"] = kind, kind
	edge.Body["metadata"], edge.Source.Fields["metadata"] = metadata, metadata
	edge.Body["thread_id"], edge.Source.Fields["thread_id"] = " exact/thread ", " exact/thread "
	if kind == "conditional-blocks" {
		edge.Body["to"], edge.Source.Fields["depends_on_issue_id"] = "demo-8", "demo-8"
	}
	edges := []dumpformat.Record{edge}
	for _, child := range []string{"demo-8", "demo-9"} {
		parent := edge
		parent.ID, parent.Body, parent.Source.Fields = "parent-"+child, cloneRow(edge.Body), cloneRow(edge.Source.Fields)
		parent.Source.Fields["id"] = parent.ID
		parent.Source.Fields["issue_id"], parent.Body["from"] = child, child
		parent.Source.Fields["depends_on_issue_id"], parent.Body["to"] = "demo-7", "demo-7"
		parent.Source.Fields["type"], parent.Body["type"] = "parent-child", "parent-child"
		parent.Source.Fields["metadata"], parent.Body["metadata"] = nil, nil
		edges = append(edges, parent)
	}
	return rewriteData(t, func(members map[string][]byte) {
		clear(members)
		members["manifest.json"], _ = archive.Member("manifest.json")
		for name := range archive.Manifest().Files {
			members[name], _ = archive.Member(name)
		}
		for name, rows := range map[string][]dumpformat.Record{"bead": beads, "edge": edges} {
			sort.Slice(rows, func(i, j int) bool { return rows[i].Project+"\x00"+rows[i].ID < rows[j].Project+"\x00"+rows[j].ID })
			var data bytes.Buffer
			for _, row := range rows {
				data.Write(encode(t, row))
			}
			members["records/"+name+".ndjson"] = data.Bytes()
		}
		changeManifest(t, members, func(m map[string]any) {
			for _, raw := range m["kind_registry"].([]any) {
				entry := raw.(map[string]any)
				if entry["name"] == "edge" {
					entry["version"], entry["count"] = 2, len(edges)
				}
				if entry["name"] == "bead" {
					entry["count"] = len(beads)
				}
			}
			labels := 0
			for _, row := range beads {
				labels += len(row.Body["labels"].([]any))
			}
			for _, raw := range m["sources"].([]any) {
				entry := raw.(map[string]any)
				switch entry["family"] {
				case "issues", "issues.row_lock", "issues.is_blocked":
					entry["rows"] = len(beads)
				case "dependencies":
					entry["rows"] = len(edges)
				case "labels":
					entry["rows"] = labels
				}
			}
		})
	})
}

func requireVersionedDependencyState(t *testing.T, store driver.Store, archive *dumpformat.Archive, ready bool) {
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
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("demo-7.4", model.Natural64(0)), model.RefByID("demo-9", model.Natural64(0))})
	if err != nil || len(rows) != 2 {
		t.Fatalf("actual restored documents missing: %v", err)
	}
	for _, row := range rows {
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		if doc.ID == "demo-9" && (doc.Parent == nil || doc.Parent.ID != "demo-7" || len(doc.Links) != 0) {
			t.Fatal("parent edge was not promoted to canonical hierarchy")
		}
		if doc.ID == "demo-7.4" {
			edge := archive.Records("edge")[0]
			predicate, err := dumpformat.SourceEdgeMetadata(2, edge.ID, stringValue(edge.Body["type"]), edge.Body["metadata"])
			if err != nil || doc.Parent != nil || len(doc.Links) != 1 || doc.Links[0].Kind != edge.Body["type"] || doc.Links[0].To.ID != edge.Body["to"] || doc.Links[0].TargetKind == model.LinkTargetExternal || !reflect.DeepEqual(doc.Links[0].Metadata, predicate) {
				t.Fatalf("typed local source projection changed: %+v %v", doc, err)
			}
		}
	}
	for _, edge := range archive.Records("edge") {
		rows, err := r.Get(ctx, []model.Ref{model.RefByID("source/edge/"+edge.ID, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatalf("source edge missing: %v", err)
		}
		var body map[string]any
		if decodeReverseJSON(rows[0].Body, &body) != nil {
			t.Fatal("invalid source mirror")
		}
		for key, value := range edge.Body {
			if !reflect.DeepEqual(body[key], value) {
				t.Fatalf("source body changed at %s/%s", edge.ID, key)
			}
		}
		if !reflect.DeepEqual(body["source"], map[string]any(edge.Source.Fields)) {
			t.Fatal("full SQL source provenance changed")
		}
	}
	states, err := r.DerivedRows(ctx, driver.DerivedReadyState, "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range states {
		if row.Key == "demo-7.4" {
			found = true
			var state struct{ Blocked, Ready bool }
			if json.Unmarshal(row.Value, &state) != nil || state.Ready != ready || state.Blocked == ready {
				t.Fatalf("stored readiness differs: %s want ready=%v", row.Value, ready)
			}
		}
	}
	if !found || len(states) != len(archive.Records("bead")) {
		t.Fatal("stored ready-state census incomplete")
	}
	raw, err := r.Blob(ctx, driver.Hash(archive.SHA256()))
	if err != nil || !bytes.Equal(raw, archive.TarBytes()) {
		t.Fatal("exact archive provenance lost")
	}
}

func TestVersionedDependencyImportReadinessSourceAndReopen(t *testing.T) {
	for _, test := range []struct {
		name, kind    string
		raw           any
		closed, ready bool
	}{
		{"legacy-nil", "waits-for", nil, true, true},
		{"id-only-nil", "waits-for", `{"_plug_edge_id":"edge-fixture"}`, true, true},
		{"explicit-all", "waits-for", `{}`, true, false},
		{"any-sql-envelope", "waits-for", `{"_plug_edge_id":"edge-fixture","_plug_metadata":" {\"gate\":\"any-children\",\"spawner_id\":\"wrong-target\",\"opaque\":[9007199254740993,1.0,1e2]} "}`, false, true},
		{"also-blocks", "waits-for", map[string]any{"gate": "any-children", "also_blocks": true}, false, false},
		{"conditional-success", "conditional-blocks", `[{"opaque":9007199254740993},1.0,null]`, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := versionedDependencyBytes(t, admittedFixtureBytes(t), test.kind, test.raw, test.closed)
			profile, archive := admittedImportProfile(t, data)
			inspection, err := InspectImport(context.Background(), bytes.NewReader(data))
			if err != nil || inspection.ArchiveSHA256 != profile.ArchiveDigest || inspection.DerivedSHA256 != profile.DerivedDigest {
				t.Fatalf("inspection/profile mismatch: %v", err)
			}
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					store, lease, reopen := openIdentityTarget(t, engine)
					if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile); err != nil {
						t.Fatal(err)
					}
					requireVersionedDependencyState(t, store, archive, test.ready)
					store, _ = reopen()
					requireVersionedDependencyState(t, store, archive, test.ready)
				})
			}
		})
	}
}

func TestVersionedDependencyStagingFailureConservesAllSQLAndReopens(t *testing.T) {
	data := versionedDependencyBytes(t, importedOntologyBytes(t, "queued:active,working:wip,shelved:frozen,finished:done"), "waits-for", `{"gate":"any-children","opaque":9007199254740993}`, false)
	profile, archive := admittedImportProfile(t, data)
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, db, reopen := openDependencySQLTarget(t, engine)
			prepareDependencySQLLocks(t, store, lease, db, engine, archive, profile)
			before := dependencySQLCensus(t, db, engine, false)
			failure, hit := errors.New("injected v2 lineage staging failure"), false
			target := &observedTarget{Store: store, wrap: func(tx driver.WriteTx) driver.WriteTx {
				return &ontologySourceFaultTx{WriteTx: tx, RestoreImportTx: tx.(driver.RestoreImportTx), AllocatorContinuation: tx.(driver.AllocatorContinuation), hit: &hit, err: failure}
			}}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), target, lease, profile); !errors.Is(err, failure) || !hit {
				t.Fatalf("v2 staging fault not reached: %v", err)
			}
			if got := dependencySQLCensus(t, db, engine, false); !reflect.DeepEqual(before, got) {
				t.Fatalf("v2 rollback changed complete SQL census: %s", dependencySQLDifference(before, got))
			}
			stable := dependencySQLCensus(t, db, engine, true)
			store, lease = reopen()
			if !reflect.DeepEqual(stable, dependencySQLCensus(t, db, engine, true)) {
				t.Fatal("staged v2 rows survived physical reopen")
			}
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			requireVersionedDependencyState(t, store, archive, true)
			requireImportedOntology(t, store, archive, profile, report.Restored.Head)
			stable = dependencySQLCensus(t, db, engine, true)
			store, lease = reopen()
			if !reflect.DeepEqual(stable, dependencySQLCensus(t, db, engine, true)) {
				t.Fatal("committed v2 SQL image changed on reopen")
			}
			requireVersionedDependencyState(t, store, archive, true)
			before = dependencySQLCensus(t, db, engine, false)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); !errors.Is(err, ErrTargetNotEmpty) {
				t.Fatalf("nonempty v2 restore changed: %v", err)
			}
			if !reflect.DeepEqual(before, dependencySQLCensus(t, db, engine, false)) {
				t.Fatal("nonempty v2 refusal changed SQL")
			}
		})
	}
}
