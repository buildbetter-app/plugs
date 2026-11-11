package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A separately versioned detached control, never a native capture or corpus.
// The waiter is unrelated to the spawner hierarchy; the spawner has one open
// and one terminal child, so all/any/also_blocks cannot collapse to one result.
func versionedDependencyBytes(t *testing.T, input []byte, kind string, metadata any, closedSpawner bool) []byte {
	return versionedDependencyBytesAtVersion(t, input, kind, metadata, closedSpawner, dumpformat.EdgeVersionTypedDependencies)
}

func versionedDependencyBytesAtVersion(t *testing.T, input []byte, kind string, metadata any, closedSpawner bool, version int) []byte {
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
					entry["version"], entry["count"] = version, len(edges)
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

func localOpaqueDependencyBytes(t *testing.T, input []byte, kind string, metadata any, closedSpawner bool) []byte {
	return versionedDependencyBytesAtVersion(t, input, kind, metadata, closedSpawner, dumpformat.EdgeVersionLocalOpaque)
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
			if doc.Parent != nil || len(doc.Links) != 1 || doc.Links[0].Kind != edge.Body["type"] || doc.Links[0].To.ID != edge.Body["to"] || doc.Links[0].TargetKind == model.LinkTargetExternal {
				t.Fatalf("typed local source projection changed: %+v %v", doc, err)
			}
			requireImportedDependencyMetadata(t, archive, edge, doc.Links[0].Metadata)
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
		{"fixed-tracks-is-inert", "tracks", map[string]any{"opaque": json.Number("9007199254740993")}, false, true},
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

func TestEmptyWaitsGateReadRestoreReverseAndReopenPreserveRawValue(t *testing.T) {
	for _, version := range []int{dumpformat.EdgeVersionTypedDependencies, dumpformat.EdgeVersionLocalOpaque} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			for _, test := range []struct {
				name string
				raw  any
			}{
				{"object", map[string]any{"gate": ""}},
				{"SQL-text", " {\"gate\":\"\",\"opaque\":[9007199254740993,1.0]} \n"},
			} {
				t.Run(test.name, func(t *testing.T) {
					data := versionedDependencyBytesAtVersion(t, admittedFixtureBytes(t), "waits-for", test.raw, true, version)
					profile, archive := admittedImportProfile(t, data)
					edge := archive.Records("edge")[0]
					if !reflect.DeepEqual(edge.Body["metadata"], test.raw) || !reflect.DeepEqual(edge.Source.Fields["metadata"], test.raw) {
						t.Fatal("reader changed the exact empty-gate body/source metadata")
					}
					for _, engine := range []string{"memory", "sqlite", "postgres"} {
						t.Run(engine, func(t *testing.T) {
							store, lease, reopen := openIdentityTarget(t, engine)
							if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile); err != nil {
								t.Fatal(err)
							}
							// One child is still open, so an explicit empty gate is
							// all-children even when its spawner is terminal.
							requireVersionedDependencyState(t, store, archive, false)
							requireDependencyReverse(t, store, archive, profile)
							store, _ = reopen()
							requireVersionedDependencyState(t, store, archive, false)
							requireDependencyReverse(t, store, archive, profile)
						})
					}
				})
			}
		})
	}
}

func requireLocalOpaqueDependencyState(t *testing.T, store driver.Store, archive *dumpformat.Archive) {
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
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("demo-7.4", model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("local opaque owner missing: %v", err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Links) != 1 || doc.Links[0].Kind != "local-opaque" || doc.Links[0].TargetKind == model.LinkTargetExternal || doc.Links[0].To.ID != "demo-7" || doc.Links[0].Metadata.HasPredicate() {
		t.Fatalf("local opaque link was projected as workflow state: %+v", doc.Links)
	}
	edge := archive.Records("edge")[0]
	requireImportedDependencyMetadata(t, archive, edge, doc.Links[0].Metadata)
	sourceRows, err := r.Get(ctx, []model.Ref{model.RefByID("source/edge/"+edge.ID, model.Natural64(0))})
	if err != nil || len(sourceRows) != 1 {
		t.Fatalf("source edge missing: %v", err)
	}
	var body map[string]any
	if decodeReverseJSON(sourceRows[0].Body, &body) != nil {
		t.Fatal("invalid local opaque source mirror")
	}
	for key, value := range edge.Body {
		if !reflect.DeepEqual(body[key], value) {
			t.Fatalf("local opaque body changed at %s: %#v != %#v", key, body[key], value)
		}
	}
	if !reflect.DeepEqual(body["source"], map[string]any(edge.Source.Fields)) {
		t.Fatal("local opaque SQL source fields changed")
	}
	readyRows, err := r.DerivedRows(ctx, driver.DerivedReadyState, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, readyRow := range readyRows {
		if readyRow.Key != "demo-7.4" {
			continue
		}
		var state struct{ Blocked, Ready bool }
		if json.Unmarshal(readyRow.Value, &state) != nil || state.Blocked || !state.Ready {
			t.Fatalf("local opaque edge changed readiness: %s", readyRow.Value)
		}
		return
	}
	t.Fatal("local opaque readiness row missing")
}

func TestLocalOpaqueDependencyImportIsInertAndReopenSafe(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  any
	}{
		{"SQL-NULL", nil},
		{"empty-SQL-object", " {} \n"},
		{"SQL-text", " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} \n"},
		{"object", map[string]any{"large": json.Number("9007199254740993"), "literal": json.Number("1.0"), "gate": "any-children", "also_blocks": true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := localOpaqueDependencyBytes(t, admittedFixtureBytes(t), "local-opaque", test.raw, false)
			profile, archive := admittedImportProfile(t, data)
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					store, lease, reopen := openIdentityTarget(t, engine)
					if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile); err != nil {
						t.Fatal(err)
					}
					requireLocalOpaqueDependencyState(t, store, archive)
					requireDependencyReverse(t, store, archive, profile)
					store, _ = reopen()
					requireLocalOpaqueDependencyState(t, store, archive)
					requireDependencyReverse(t, store, archive, profile)
				})
			}
		})
	}
}

func requireImportedDependencyMetadata(t *testing.T, archive *dumpformat.Archive, edge dumpformat.Record, got *model.LinkMetadata) {
	t.Helper()
	want, err := dumpformat.SourceEdgeMetadata(archive.Manifest().EdgeVersion(), edge.ID, stringValue(edge.Body["type"]), edge.Body["metadata"])
	if err != nil || got == nil {
		t.Fatal("imported dependency metadata missing", err)
	}
	if want == nil {
		if got.HasPredicate() {
			t.Fatal("opaque source fields acquired scheduling semantics")
		}
	} else if got.Gate != want.Gate || got.AlsoBlocks != want.AlsoBlocks {
		t.Fatal("typed source predicate changed")
	}
	if !reflect.DeepEqual(got.NativeExtensions, edge.NativeEdgeTokens) {
		t.Fatal("imported native field tokens differ from the original archive")
	}
}

func requireDependencyReverse(t *testing.T, store driver.Store, archive *dumpformat.Archive, profile driver.RestoreImportProfile) {
	t.Helper()
	before := savedLabelImport(t, store, archive, profile, "")
	got, err := ReverseRestored(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	type dependency struct {
		ID, IssueID, To, External, Kind, Thread, CreatedAt, CreatedBy, Metadata string
	}
	dependencies := map[string]dependency{}
	for _, line := range bytes.Split(bytes.TrimSpace(got.Result.JSONL), []byte{'\n'}) {
		var issue struct {
			ID           string `json:"id"`
			Dependencies []struct {
				ID        string `json:"id"`
				IssueID   string `json:"issue_id"`
				To        string `json:"depends_on_id"`
				External  string `json:"depends_on_external"`
				Kind      string `json:"type"`
				Thread    string `json:"thread_id"`
				CreatedAt string `json:"created_at"`
				CreatedBy string `json:"created_by"`
				Metadata  string `json:"metadata"`
			} `json:"dependencies"`
		}
		if err := json.Unmarshal(line, &issue); err != nil {
			t.Fatal(err)
		}
		for _, edge := range issue.Dependencies {
			if _, duplicate := dependencies[edge.ID]; duplicate || edge.IssueID != issue.ID {
				t.Fatal("reverse duplicated an edge or changed its owner")
			}
			dependencies[edge.ID] = dependency{edge.ID, edge.IssueID, edge.To, edge.External, edge.Kind, edge.Thread, edge.CreatedAt, edge.CreatedBy, edge.Metadata}
		}
	}
	if len(dependencies) != len(archive.Records("edge")) {
		t.Fatal("reverse edge census changed")
	}
	for _, edge := range archive.Records("edge") {
		got, present := dependencies[edge.ID]
		if !present || got.IssueID != edge.Body["from"] || got.To != edge.Body["to"] || got.External != "" || got.Kind != edge.Body["type"] || got.Thread != edge.Body["thread_id"] || got.CreatedAt != edge.Source.Fields["created_at"] || got.CreatedBy != edge.Source.Fields["created_by"] {
			t.Fatalf("reverse dependency identity/target/source fields changed: %+v", got)
		}
		var envelope map[string]any
		if err := decodeReverseJSON([]byte(got.Metadata), &envelope); err != nil || envelope["_plug_edge_id"] != edge.ID {
			t.Fatalf("reverse dependency envelope changed: %s %v", got.Metadata, err)
		}
		payload, present := envelope["_plug_metadata"]
		if present != (edge.Body["metadata"] != nil) || !reflect.DeepEqual(payload, edge.Body["metadata"]) {
			t.Fatalf("reverse changed NULL/object/SQL-text metadata: %#v want %#v", payload, edge.Body["metadata"])
		}
	}
	if !reflect.DeepEqual(before, savedLabelImport(t, store, archive, profile, "")) {
		t.Fatal("reverse changed source records, history, derived rows, blobs or receipt")
	}
}

func editDependencyArchive(t *testing.T, archive *dumpformat.Archive, edit func(map[string][]byte)) []byte {
	t.Helper()
	return rewriteData(t, func(members map[string][]byte) {
		clear(members)
		members["manifest.json"], _ = archive.Member("manifest.json")
		for name := range archive.Manifest().Files {
			members[name], _ = archive.Member(name)
		}
		edit(members)
	})
}

// Detached consumer control only: distinct endpoints prevent an edge from
// borrowing another kind's graph effects. This is never native producer proof.
func fixed19DependencyBytes(t *testing.T, version int) []byte {
	t.Helper()
	input, err := dumpformat.Read(bytes.NewReader(admittedFixtureBytes(t)))
	if err != nil {
		t.Fatal(err)
	}
	kinds := []string{"blocks", "parent-child", "conditional-blocks", "waits-for", "related", "discovered-from", "replies-to", "relates-to", "duplicates", "supersedes", "authored-by", "assigned-to", "approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from"}
	beads := input.Records("bead")
	var edges []dumpformat.Record
	for i, kind := range kinds {
		from, to := fmt.Sprintf("demo-%d", 100+2*i), fmt.Sprintf("demo-%d", 101+2*i)
		for _, id := range []string{from, to} {
			row := beads[0]
			row.ID, row.Body, row.Source.Fields = id, cloneRow(row.Body), cloneRow(row.Source.Fields)
			row.Source.Fields["id"] = id
			row.Body["title"], row.Source.Fields["title"] = kind+" "+id, kind+" "+id
			row.Source.Fields["status"], row.Body["status_category"] = "open", "unstarted"
			delete(row.Body, "result")
			for _, field := range []string{"assignee", "owner", "closed_at", "due_at", "defer_until", "started_at"} {
				row.Body[field], row.Source.Fields[field] = nil, nil
			}
			row.Body["pinned"], row.Source.Fields["pinned"] = false, false
			if kind == "conditional-blocks" && id == to {
				row.Source.Fields["status"], row.Body["status_category"], row.Body["result"] = "closed", "completed", "success"
			}
			beads = append(beads, row)
		}
		raw := any(" {\"gate\":\"not-a-scheduling-gate\",\"also_blocks\":true,\"opaque\":[9007199254740993,1.0,null]} \n")
		if kind == "waits-for" {
			raw = `{"gate":"all-children","also_blocks":true}`
		}
		edge := input.Records("edge")[0]
		edge.ID = fmt.Sprintf("fixed-%02d", i)
		edge.Body = dumpformat.Row{"from": from, "to": to, "target_kind": "depends_on_issue_id", "type": kind, "metadata": raw, "thread_id": " exact/" + kind + " "}
		edge.Source.Fields = dumpformat.Row{
			"id": edge.ID, "issue_id": from, "depends_on_issue_id": to,
			dumpformat.EphemeralDependencyTargetField: nil, "depends_on_external": nil, "type": kind,
			"created_at": "2026-01-01T00:00:00Z", "created_by": " exact source actor ",
			"metadata": raw, "thread_id": edge.Body["thread_id"],
		}
		edges = append(edges, edge)
	}
	return editDependencyArchive(t, input, func(members map[string][]byte) {
		for name, rows := range map[string][]dumpformat.Record{"bead": beads, "edge": edges} {
			sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
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
					entry["version"], entry["count"] = version, len(edges)
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

func TestFixed19TypedRestoreReverseReopenAndNonblockingEffects(t *testing.T) {
	ctx := context.Background()
	for _, version := range []int{dumpformat.EdgeVersionTypedDependencies, dumpformat.EdgeVersionLocalOpaque} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			data := fixed19DependencyBytes(t, version)
			profile, archive := admittedImportProfile(t, data)
			if len(archive.Records("edge")) != 19 {
				t.Fatal("fixed19 control lost a dependency")
			}
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					store, lease, reopen := openIdentityTarget(t, engine)
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
						t.Fatal(err)
					}
					for pass := 0; pass < 2; pass++ {
						requireDependencyReverse(t, store, archive, profile)
						state := savedLabelImport(t, store, archive, profile, "")
						rows := map[string]driver.Record{}
						for _, row := range state.Records {
							rows[row.Ref.ID] = row
						}
						for _, edge := range archive.Records("edge") {
							kind, from, to := stringValue(edge.Body["type"]), stringValue(edge.Body["from"]), stringValue(edge.Body["to"])
							doc, err := pipeline.DecodeDocument(rows[from].Body)
							if err != nil {
								t.Fatal(err)
							}
							if kind == "parent-child" {
								if doc.Parent == nil || doc.Parent.ID != to || len(doc.Links) != 0 {
									t.Fatal("fixed parent-child lost canonical hierarchy")
								}
								requireImportedDependencyMetadata(t, archive, edge, doc.ParentMetadata)
							} else {
								wantKind := kind
								if kind == "related" {
									wantKind = "relates-to"
								}
								if doc.Parent != nil || len(doc.Links) != 1 || doc.Links[0].Kind != wantKind || doc.Links[0].To.ID != to || doc.Links[0].TargetKind == model.LinkTargetExternal || kind != "waits-for" && doc.Links[0].Metadata.HasPredicate() {
									t.Fatalf("fixed kind acquired hierarchy, routing or scheduling metadata: %+v", doc)
								}
								requireImportedDependencyMetadata(t, archive, edge, doc.Links[0].Metadata)
							}
							var mirror map[string]any
							if err := decodeReverseJSON(rows["source/edge/"+edge.ID].Body, &mirror); err != nil || !reflect.DeepEqual(mirror["source"], map[string]any(edge.Source.Fields)) {
								t.Fatalf("fixed dependency lost complete physical source: %v", err)
							}
							for field, value := range edge.Body {
								if !reflect.DeepEqual(mirror[field], value) {
									t.Fatalf("fixed dependency source body changed at %s/%s", edge.ID, field)
								}
							}
							found := false
							for _, row := range state.Derived[driver.DerivedReadyState] {
								if row.Key == from {
									found = true
									var ready struct{ Ready, Blocked bool }
									want := kind != "blocks" && kind != "waits-for" && kind != "conditional-blocks"
									if err := json.Unmarshal(row.Value, &ready); err != nil || ready.Ready != want || ready.Blocked == want {
										t.Fatalf("fixed kind changed ready/blocking effects: %s %s %v", kind, row.Value, err)
									}
								}
							}
							if !found {
								t.Fatal("fixed dependency owner missing from physical ready state")
							}
						}
						if pass == 0 {
							store, lease = reopen()
							if !reflect.DeepEqual(state, savedLabelImport(t, store, archive, profile, "")) {
								t.Fatal("reopen changed records, versions, derived state, source blobs or import receipt")
							}
						}
					}
					ontology := service.NewOntology(nil)
					domain := service.New(store, lease, pipeline.New(store, lease, pipeline.WithOntology(ontology)), service.WithOntology(ontology))
					for i, edge := range archive.Records("edge") {
						switch stringValue(edge.Body["type"]) {
						case "replies-to", "relates-to", "duplicates", "supersedes", "authored-by", "assigned-to", "approved-by", "attests", "until", "caused-by", "validates", "delegated-from":
							// An open target and gate-shaped raw metadata cannot
							// turn a nonblocking known kind into a close guard.
							_, err := domain.Close(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, service.CloseRequest{ID: model.RefByID(stringValue(edge.Body["from"]), model.Natural64(0))}, model.WriteMeta{Project: "demo", Agent: "fixed19-control", AgentLabel: "fixed19-control", IdempotencyKey: fmt.Sprintf("%032x", i+1)})
							if err != nil {
								t.Fatalf("nonblocking fixed kind close-guarded its owner: %s %v", edge.Body["type"], err)
							}
						}
					}
				})
			}
		})
	}
}

func TestTypedTracksReadbackKeepsExpandedReportsAndPureLoopsInert(t *testing.T) {
	ctx := context.Background()
	for _, returningKind := range []string{"tracks", "blocks"} {
		t.Run(returningKind, func(t *testing.T) {
			data := versionedDependencyBytes(t, admittedFixtureBytes(t), "tracks", " {\"large\":9007199254740993,\"literal\":1.0} ", false)
			_, archive := admittedImportProfile(t, data)
			data = editDependencyArchive(t, archive, func(members map[string][]byte) {
				edge := archive.Records("edge")[0]
				edge.ID, edge.Body, edge.Source.Fields = "tracks-return", cloneRow(edge.Body), cloneRow(edge.Source.Fields)
				edge.Source.Fields["id"] = edge.ID
				edge.Body["from"], edge.Source.Fields["issue_id"] = "demo-7", "demo-7"
				edge.Body["to"], edge.Source.Fields["depends_on_issue_id"] = "demo-7.4", "demo-7.4"
				edge.Body["type"], edge.Source.Fields["type"] = returningKind, returningKind
				members["records/edge.ndjson"] = append(members["records/edge.ndjson"], encode(t, edge)...)
				changeManifest(t, members, func(m map[string]any) {
					for _, raw := range m["kind_registry"].([]any) {
						if entry := raw.(map[string]any); entry["name"] == "edge" {
							entry["count"] = len(archive.Records("edge")) + 1
						}
					}
					for _, raw := range m["sources"].([]any) {
						if entry := raw.(map[string]any); entry["family"] == "dependencies" {
							entry["rows"] = len(archive.Records("edge")) + 1
						}
					}
				})
			})
			profile, archive := admittedImportProfile(t, data)
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					store, lease, reopen := openIdentityTarget(t, engine)
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
						t.Fatal(err)
					}
					for pass := 0; pass < 2; pass++ {
						if err := pipeline.BuildSourceIndexes(ctx, store, lease); err != nil {
							t.Fatal(err)
						}
						requireVersionedDependencyState(t, store, archive, true)
						requireDependencyReverse(t, store, archive, profile)
						meta, err := store.Meta(ctx)
						if err != nil {
							t.Fatal(err)
						}
						r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
						if err != nil {
							t.Fatal(err)
						}
						for _, include := range []bool{false, true} {
							report, err := service.ReadCycles(ctx, r, service.CyclesRequest{Project: "demo", IncludeTracks: include, Limit: 100}, nil)
							want := uint64(0)
							if include && returningKind == "blocks" {
								want = 1
								if len(report.Items) != 1 || len(report.Items[0].Path) != 2 || report.Items[0].Path[0].Ref == nil || report.Items[0].Path[1].Ref == nil || report.Items[0].Path[0].Ref.ID != "demo-7" || report.Items[0].Path[1].Ref.ID != "demo-7.4" || report.Items[0].Partial {
									t.Fatalf("tracks-expanded report lost canonical rotation or members: %+v", report)
								}
							}
							if err != nil || report.Truncated || report.CycleCount != model.Natural64(want) || len(report.Items) != int(want) {
								t.Fatalf("tracks changed scheduling-cycle/report semantics: %+v %v", report, err)
							}
						}
						r.Close()
						if pass == 0 {
							store, lease = reopen()
						}
					}
				})
			}
		})
	}
}

type dependencyRollbackTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	reached *bool
	fault   error
}

func (tx dependencyRollbackTx) Commit(context.Context, driver.ChangesetHeader) (model.Changeset, error) {
	*tx.reached = true
	return model.Changeset{}, tx.fault
}

func TestV3DependencyRefusalsAndStagingRollbackConserveTarget(t *testing.T) {
	ctx := context.Background()
	data := localOpaqueDependencyBytes(t, admittedFixtureBytes(t), "local-opaque", " {\"large\":9007199254740993} ", false)
	profile, archive := admittedImportProfile(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, reopen := openIdentityTarget(t, engine)
			for _, test := range []struct {
				name  string
				edit  func(map[string][]byte)
				cause string
			}{
				{"v2-local-opaque", func(m map[string][]byte) {
					changeManifest(t, m, func(manifest map[string]any) {
						for _, raw := range manifest["kind_registry"].([]any) {
							if entry := raw.(map[string]any); entry["name"] == "edge" {
								entry["version"] = 2
							}
						}
					})
				}, "unsupported dependency type"},
				{"v1-fixed-kind", func(m map[string][]byte) {
					changeRecord(t, m, "edge", func(r map[string]any) { body(r)["type"], source(r)["type"] = "duplicates", "duplicates" })
					changeManifest(t, m, func(manifest map[string]any) {
						for _, raw := range manifest["kind_registry"].([]any) {
							if entry := raw.(map[string]any); entry["name"] == "edge" {
								entry["version"] = 1
							}
						}
					})
				}, "unsupported dependency type"},
				{"multi-arm", func(m map[string][]byte) {
					changeRecord(t, m, "edge", func(r map[string]any) { source(r)["depends_on_external"] = "tracker:collision" })
				}, "multiple typed edge targets"},
				{"ephemeral-arm", func(m map[string][]byte) {
					changeRecord(t, m, "edge", func(r map[string]any) {
						source(r)["depends_on_issue_id"], source(r)[dumpformat.EphemeralDependencyTargetField] = nil, "demo-7"
						body(r)["target_kind"] = dumpformat.EphemeralDependencyTargetField
					})
				}, "unsupported or missing edge target"},
				{"dangling", func(m map[string][]byte) {
					changeRecord(t, m, "edge", func(r map[string]any) {
						body(r)["to"], source(r)["depends_on_issue_id"] = "demo-missing", "demo-missing"
					})
				}, "dangling typed edge target"},
				{"metadata-identity", func(m map[string][]byte) {
					changeRecord(t, m, "edge", func(r map[string]any) {
						body(r)["metadata"], source(r)["metadata"] = `{"_plug_edge_id":"wrong"}`, `{"_plug_edge_id":"wrong"}`
					})
				}, "edge metadata envelope identity mismatch"},
				{"staging-rollback", nil, ""},
			} {
				t.Run(test.name, func(t *testing.T) {
					input := data
					if test.edit != nil {
						input = editDependencyArchive(t, archive, test.edit)
					}
					before := snapshot(t, store)
					reached := false
					fault := errors.New("B07 staged import publication refused")
					target := &observedTarget{Store: store, wrap: func(tx driver.WriteTx) driver.WriteTx {
						return dependencyRollbackTx{tx, tx.(driver.RestoreImportTx), tx.(driver.AllocatorContinuation), &reached, fault}
					}}
					_, err := RestoreImportWithLease(ctx, bytes.NewReader(input), target, lease, profile)
					if err == nil || test.edit == nil && (!errors.Is(err, fault) || !reached || target.writes != 1) || test.edit != nil && (target.writes != 0 || reached) {
						t.Fatalf("wrong refusal/staging boundary: err=%v writes=%d staged=%v", err, target.writes, reached)
					}
					if test.cause != "" && !strings.Contains(err.Error(), test.cause) {
						t.Fatalf("lost direct dependency refusal %q: %v", test.cause, err)
					}
					if !reflect.DeepEqual(before, snapshot(t, store)) {
						t.Fatal("refused v3 import changed metadata, records, versions or log")
					}
					meta, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					for _, table := range []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey} {
						rows, err := r.DerivedRows(ctx, table, "")
						if err != nil || len(rows) != 0 {
							t.Fatalf("refused v3 import published %s: %v", table, err)
						}
					}
					hashes := map[string]bool{archive.SHA256(): true}
					for _, file := range archive.Manifest().Files {
						hashes[file.SHA256] = true
					}
					for hash := range hashes {
						if _, err := r.Blob(ctx, driver.Hash(hash)); !errors.Is(err, driver.ErrNotFound) {
							t.Fatalf("refused v3 import published source blob %s: %v", hash, err)
						}
					}
					principal, err := driver.PrincipalKey(profile.Admission.Actor)
					if err != nil {
						t.Fatal(err)
					}
					receipt, err := r.IdempotencyByKey(ctx, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
					if err != nil || receipt != nil {
						t.Fatalf("refused v3 import published receipt: %v", err)
					}
					r.Close()
					store, lease = reopen()
					if !reflect.DeepEqual(before, snapshot(t, store)) {
						t.Fatal("refused v3 import changed state across reopen")
					}
				})
			}
		})
	}
}
