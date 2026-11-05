package plugdump

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type fixtureSource map[string]Table

func (f fixtureSource) Tables(context.Context) ([]string, error) {
	var names []string
	for name := range f {
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return names, nil
}

func (f fixtureSource) ReadTable(_ context.Context, name string) (Table, error) {
	return f[name], nil
}

func fixture(t *testing.T) (fixtureSource, Inventory) {
	t.Helper()
	data, err := os.ReadFile("testdata/source.json")
	if err != nil {
		t.Fatal(err)
	}
	var source fixtureSource
	if err := decodeJSON(data, &source); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile("testdata/inventory.json")
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.ReplaceAll(data, []byte("$FIXTURE_ROOT"), []byte(filepath.ToSlash(t.TempDir())))
	inventory, err := ReadInventory(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return source, inventory
}

func buildFixture(t *testing.T, source Source, inventory Inventory) []byte {
	t.Helper()
	data, err := Build(context.Background(), source, inventory)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func jsonEqual(left, right any) bool {
	a, err := marshal(left)
	if err != nil {
		return false
	}
	b, err := marshal(right)
	return err == nil && bytes.Equal(a, b)
}

func archiveMembers(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	members := map[string][]byte{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		member, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name] = member
	}
	return members
}

func TestDumpDeterministicPreservation(t *testing.T) {
	source, inventory := fixture(t)
	first := buildFixture(t, source, inventory)
	for name, table := range source {
		for i, j := 0, len(table.Rows)-1; i < j; i, j = i+1, j-1 {
			table.Rows[i], table.Rows[j] = table.Rows[j], table.Rows[i]
		}
		for i, j := 0, len(table.Columns)-1; i < j; i, j = i+1, j-1 {
			table.Columns[i], table.Columns[j] = table.Columns[j], table.Columns[i]
		}
		source[name] = table
	}
	second := buildFixture(t, source, inventory)
	if !bytes.Equal(first, second) {
		t.Fatal("equivalent unordered sources produced different archive bytes")
	}
	manifest, err := Validate(bytes.NewReader(first))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Projects[0].LastID != 8 || manifest.Projects[0].ChildCounters["demo-7"] != 4 {
		t.Fatalf("allocator maxima not reconciled: %+v", manifest.Projects)
	}
	if inventory.Projects[0].LastID != 5 || len(inventory.Projects[0].ChildCounters) != 0 {
		t.Fatal("export mutated caller inventory")
	}
	members := archiveMembers(t, first)
	for _, wanted := range []string{"spec-fixture", "Preserve source notes", "fixture-author", "alpha", "zeta", "\"status_category\":\"started\"", "\"result\":\"success\"", "\"custom\":\"kept\""} {
		if !bytes.Contains(members["records/bead.ndjson"], []byte(wanted)) {
			t.Errorf("missing bead content %s", wanted)
		}
	}
	for _, wanted := range []string{"\"depends_on_issue_id\":\"demo-7\"", "thread-fixture", "edge-fixture"} {
		if !bytes.Contains(members["records/edge.ndjson"], []byte(wanted)) {
			t.Errorf("missing typed edge content %s", wanted)
		}
	}
	if !bytes.Contains(members["records/comment.ndjson"], []byte("fixture-commenter")) || !bytes.Contains(members["records/memory.ndjson"], []byte("A complete synthetic memory")) || !bytes.Contains(members["records/beads-archive.ndjson"], []byte("\"$bytes\"")) {
		t.Fatal("comment, memory or available version bytes lost")
	}
	if bytes.Contains(members["records/bead.ndjson"], []byte("row_lock")) || bytes.Contains(members["records/bead.ndjson"], []byte("is_blocked")) || bytes.Contains(members["records/bead.ndjson"], []byte("lease_expires_at")) {
		t.Fatal("operational/derived fields entered bead records")
	}
	secret := []byte("synthetic-secret-never-export-this-value")
	if bytes.Contains(first, secret) || bytes.Contains(first, []byte(digest(secret))) || !bytes.Contains(first, []byte(`"$secret":"config/github.token"`)) {
		t.Fatal("secret bytes/digests leaked or reference missing")
	}
}

func TestDumpSuccessorManifestContracts(t *testing.T) {
	source, inventory := fixture(t)
	data := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.SourceFieldsContract != IssueSourceFieldsContract {
		t.Fatalf("source fields contract=%q", manifest.SourceFieldsContract)
	}
	for _, kind := range manifest.Kinds {
		want := 1
		if kind.Name == "edge" {
			want = EdgeVersionTypedDependencies
		}
		if kind.Version != want {
			t.Fatalf("kind %s version=%d want %d", kind.Name, kind.Version, want)
		}
	}
}

func TestDumpPreservesOpaqueIssueFields(t *testing.T) {
	source, inventory := fixture(t)
	row := source["issues"].Rows[0]
	row["agent_state"] = "state\x00opaque"
	row["await_id"] = "demo-8"
	row["await_type"] = "human"
	row["ephemeral"] = int32(1)
	row["event_kind"] = "opaque-event"
	row["hook_bead"] = "demo-8"
	row["is_template"] = int32(1)
	row["mol_type"] = "opaque-mol"
	row["payload"] = `{"opaque":"x\\u0000y","n":9007199254740993}`
	row["role_bead"] = "demo-8"
	row["role_type"] = "opaque-role"
	row["target"] = "opaque-target"
	row["timeout_ns"] = int64(9007199254740993)
	row["waiters"] = `["demo-8"]`
	row["wisp_type"] = "opaque-wisp"
	row["work_type"] = "opaque-work"

	data := buildFixture(t, source, inventory)
	if _, err := Validate(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	var record Record
	for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, data)["records/bead.ndjson"]), []byte{'\n'}) {
		var candidate Record
		if err := decodeJSON(line, &candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.ID == row["id"] {
			record = candidate
			break
		}
	}
	if record.ID == "" {
		t.Fatal("opaque-field fixture bead was not exported")
	}
	for _, key := range opaqueIssueFields {
		want, err := json.Marshal(row[key])
		if err != nil {
			t.Fatalf("marshal source %s: %v", key, err)
		}
		got, err := json.Marshal(record.Source.Fields[key])
		if err != nil {
			t.Fatalf("marshal archive %s: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("opaque field %s changed: got %s, want %s", key, got, want)
		}
	}
}

func TestDumpStatusMapping(t *testing.T) {
	for status, category := range map[string]string{"open": "unstarted", "in_progress": "started", "hooked": "started", "blocked": "backlog", "deferred": "backlog", "pinned": "backlog", "frozen": "backlog", "closed": "completed"} {
		t.Run(status, func(t *testing.T) {
			source, inventory := fixture(t)
			for _, row := range source["issues"].Rows {
				row["status"] = status
			}
			data := buildFixture(t, source, inventory)
			for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, data)["records/bead.ndjson"]), []byte{'\n'}) {
				var record Record
				if err := json.Unmarshal(line, &record); err != nil {
					t.Fatal(err)
				}
				if record.Body["status_category"] != category {
					t.Fatalf("%s mapped to %v", status, record.Body["status_category"])
				}
			}
		})
	}
}

func TestDumpUnknownIssueTypeRequiresOntologyDirective(t *testing.T) {
	source, inventory := fixture(t)
	source["issues"].Rows[0]["issue_type"] = "decision"
	data, err := Build(context.Background(), source, inventory)
	if err == nil || len(data) != 0 || !strings.Contains(err.Error(), "unsupported issue type; workflow/ontology mapping required") {
		t.Fatalf("unknown issue type did not retain the closed-world refusal: data=%d err=%v", len(data), err)
	}
}

func TestDumpOntologyDirectivePreservesUnknownIssueType(t *testing.T) {
	source, inventory := fixture(t)
	for _, row := range source["issues"].Rows {
		row["issue_type"] = "decision"
	}
	inventory.Ontology = []OntologyDirective{{
		ID: "preserve-decision", Kind: "map_type",
		Target: OntologyTarget{Project: "demo", Type: "decision"},
		From:   "decision", To: "issue",
	}}
	data := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(data))
	if err != nil || len(manifest.Ontology) != 1 || manifest.Ontology[0].ID != "preserve-decision" {
		t.Fatalf("directive evidence missing from manifest: err=%v ontology=%+v", err, manifest.Ontology)
	}
	var records []Record
	for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, data)["records/bead.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
		if record.Body["type"] != "issue" || record.Body["source_type"] != "decision" || record.Source.Fields["issue_type"] != "decision" {
			t.Fatalf("directive changed or dropped source type: %+v", record)
		}
	}
	if len(records) != len(source["issues"].Rows) || records[0].Source.Fields["created_by"] != source["issues"].Rows[0]["created_by"] {
		t.Fatalf("directive lost rows or attribution: got=%d want=%d", len(records), len(source["issues"].Rows))
	}
}

func TestDumpOntologyDirectiveRefusals(t *testing.T) {
	tests := map[string]func(*Inventory){
		"wrong kind": func(i *Inventory) {
			i.Ontology = []OntologyDirective{{ID: "bad", Kind: "map_status", Target: OntologyTarget{Project: "demo", Type: "decision"}, From: "decision", To: "issue"}}
		},
		"wrong target": func(i *Inventory) {
			i.Ontology = []OntologyDirective{{ID: "bad", Kind: "map_type", Target: OntologyTarget{Project: "demo", Type: "decision"}, From: "other", To: "issue"}}
		},
		"unused": func(i *Inventory) {
			i.Ontology = []OntologyDirective{{ID: "unused", Kind: "map_type", Target: OntologyTarget{Project: "demo", Type: "decision"}, From: "decision", To: "issue"}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			source, inventory := fixture(t)
			mutate(&inventory)
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 {
				t.Fatalf("malformed or unused directive was accepted: data=%d err=%v", len(data), err)
			}
		})
	}
}

func TestDumpDependencyKindsPreserveTypedEdges(t *testing.T) {
	source, inventory := fixture(t)
	base := source["dependencies"].Rows[0]
	cases := []struct {
		id         string
		kind       string
		targetKey  string
		target     string
		metadata   any
		targetKind string
	}{
		{id: "edge-blocks", kind: "blocks", targetKey: "depends_on_issue_id", target: "demo-7", metadata: map[string]any{"reason": "ordinary"}, targetKind: "depends_on_issue_id"},
		{id: "edge-related", kind: "related", targetKey: "depends_on_issue_id", target: "demo-8", metadata: map[string]any{"topic": "context"}, targetKind: "depends_on_issue_id"},
		{id: "edge-discovered", kind: "discovered-from", targetKey: "depends_on_external", target: "external:tracker:source", metadata: map[string]any{"source": "opaque"}, targetKind: "depends_on_external"},
		{id: "edge-waits", kind: "waits-for", targetKey: "depends_on_issue_id", target: "demo-7", metadata: map[string]any{"gate": "any-children", "also_blocks": true, "spawner_id": "demo-7"}, targetKind: "depends_on_issue_id"},
		{id: "edge-conditional", kind: "conditional-blocks", targetKey: "depends_on_external", target: "external:tracker:condition", metadata: map[string]any{"branch": "failure", "predicate": "terminal-error"}, targetKind: "depends_on_external"},
		{id: "edge-tracks", kind: "tracks", targetKey: "depends_on_issue_id", target: "demo-7", metadata: map[string]any{"cycle": "expanded"}, targetKind: "depends_on_issue_id"},
	}
	for _, test := range cases {
		row := cloneRow(base)
		row["id"] = test.id
		row["type"] = test.kind
		row["metadata"] = test.metadata
		row["depends_on_issue_id"], row["depends_on_wisp_id"], row["depends_on_external"] = nil, nil, nil
		row[test.targetKey] = test.target
		table := source["dependencies"]
		table.Rows = append(table.Rows, row)
		source["dependencies"] = table
	}
	data := buildFixture(t, source, inventory)
	if _, err := Validate(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	records := map[string]Record{}
	for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, data)["records/edge.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := decodeJSON(line, &record); err != nil {
			t.Fatal(err)
		}
		records[record.ID] = record
	}
	for _, test := range cases {
		record, ok := records[test.id]
		if !ok {
			t.Fatalf("missing %s edge", test.id)
		}
		if record.Body["type"] != test.kind || record.Body["to"] != test.target || record.Body["target_kind"] != test.targetKind {
			t.Fatalf("typed edge %s changed: body=%+v", test.id, record.Body)
		}
		if !jsonEqual(record.Body["metadata"], test.metadata) || !jsonEqual(record.Source.Fields["metadata"], test.metadata) {
			t.Fatalf("metadata for %s changed: body=%#v source=%#v want=%#v", test.id, record.Body["metadata"], record.Source.Fields["metadata"], test.metadata)
		}
		for _, field := range []string{"created_at", "created_by", "thread_id", "issue_id", "type"} {
			if record.Source.Fields[field] != rowFieldForDependencyTest(base, field, test.kind, test.id) {
				t.Fatalf("source field %s for %s changed: got=%#v", field, test.id, record.Source.Fields[field])
			}
		}
	}
}

func rowFieldForDependencyTest(base Row, field, kind, id string) any {
	if field == "type" {
		return kind
	}
	if field == "id" {
		return id
	}
	return base[field]
}

func TestDumpLocalOpaqueDependencyUsesV3AndPreservesStoredRow(t *testing.T) {
	source, inventory := fixture(t)
	base := source["dependencies"].Rows[0]
	row := cloneRow(base)
	row["id"] = "edge-local-opaque"
	row["type"] = "custom-waits-later"
	row["metadata"] = map[string]any{"opaque": "kept", "number": int64(9007199254740993)}
	row["depends_on_issue_id"], row["depends_on_wisp_id"], row["depends_on_external"] = "demo-8", nil, nil
	table := source["dependencies"]
	table.Rows = append(table.Rows, row)
	source["dependencies"] = table

	data := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range manifest.Kinds {
		if kind.Name == "edge" && kind.Version != EdgeVersionLocalOpaque {
			t.Fatalf("opaque edge registry version=%d, want %d", kind.Version, EdgeVersionLocalOpaque)
		}
	}
	var record Record
	for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, data)["records/edge.ndjson"]), []byte{'\n'}) {
		var candidate Record
		if err := decodeJSON(line, &candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.ID == "edge-local-opaque" {
			record = candidate
			break
		}
	}
	if record.ID == "" {
		t.Fatal("local opaque dependency was not exported")
	}
	if record.Body["type"] != row["type"] || record.Body["to"] != row["depends_on_issue_id"] || record.Body["target_kind"] != "depends_on_issue_id" {
		t.Fatalf("opaque edge target/type changed: body=%+v", record.Body)
	}
	if !jsonEqual(record.Body["metadata"], row["metadata"]) || !jsonEqual(record.Source.Fields["metadata"], row["metadata"]) {
		t.Fatalf("opaque metadata changed: body=%#v source=%#v", record.Body["metadata"], record.Source.Fields["metadata"])
	}
	for _, field := range []string{"created_at", "created_by", "thread_id", "issue_id", "type", "depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
		if !reflect.DeepEqual(record.Source.Fields[field], row[field]) {
			t.Fatalf("opaque source field %s changed: got=%#v want=%#v", field, record.Source.Fields[field], row[field])
		}
	}
}

func TestDumpLocalOpaqueDependencyRefusals(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		targetKey string
		target    any
		secondKey string
		second    any
		want      string
	}{
		{name: "empty kind", kind: "", targetKey: "depends_on_issue_id", target: "demo-8", want: "unsupported dependency type"},
		{name: "invalid utf8", kind: string([]byte{0xff}), targetKey: "depends_on_issue_id", target: "demo-8", want: "invalid UTF-8"},
		{name: "over limit", kind: strings.Repeat("x", 33), targetKey: "depends_on_issue_id", target: "demo-8", want: "unsupported dependency type"},
		{name: "unsupported well-known", kind: "duplicates", targetKey: "depends_on_issue_id", target: "demo-8", want: "unsupported dependency type"},
		{name: "external target", kind: "custom-external", targetKey: "depends_on_external", target: "external:tracker:source", want: "unsupported dependency type"},
		{name: "wisp target", kind: "custom-wisp", targetKey: "depends_on_wisp_id", target: "wisp-1", want: "dangling wisp edge target"},
		{name: "multiple targets", kind: "custom-multiple", targetKey: "depends_on_issue_id", target: "demo-8", secondKey: "depends_on_external", second: "external:tracker:source", want: "edge must have exactly one typed target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, inventory := fixture(t)
			row := cloneRow(source["dependencies"].Rows[0])
			row["id"] = "edge-local-opaque-refusal"
			row["type"] = test.kind
			row["metadata"] = map[string]any{"opaque": true}
			row["depends_on_issue_id"], row["depends_on_wisp_id"], row["depends_on_external"] = nil, nil, nil
			row[test.targetKey] = test.target
			if test.secondKey != "" {
				row[test.secondKey] = test.second
			}
			table := source["dependencies"]
			table.Rows = append(table.Rows, row)
			source["dependencies"] = table
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid local opaque edge accepted: data=%d err=%v", len(data), err)
			}
		})
	}
}

func TestDumpFaithfulWispFamiliesPreserveCollidingActiveRows(t *testing.T) {
	source, inventory := fixture(t)
	issue := cloneRow(source["issues"].Rows[0])
	issue["title"] = "durable collision"
	source["issues"].Rows[0]["title"] = issue["title"]
	wisp := cloneRow(issue)
	wisp["title"] = "active collision wisp"
	secondWisp := cloneRow(wisp)
	secondWisp["id"] = "demo-9"
	secondWisp["title"] = "second active wisp"
	wisps := source["wisps"]
	wisps.Rows = append(wisps.Rows, wisp, secondWisp)
	source["wisps"] = wisps

	source["wisp_labels"] = Table{Columns: columnsFor("wisp_labels"), Rows: []Row{{"issue_id": "demo-7", "label": "active"}}}
	source["wisp_comments"] = Table{Columns: columnsFor("wisp_comments"), Rows: []Row{{"id": "wisp-comment", "issue_id": "demo-7", "author": "wisp-author", "text": "wisp text", "created_at": "2026-01-02T00:00:00Z"}}}
	source["wisp_events"] = Table{Columns: columnsFor("wisp_events"), Rows: []Row{{"id": "wisp-event", "issue_id": "demo-7", "event_type": "created", "actor": "wisp-author", "old_value": nil, "new_value": "open", "comment": "wisp event", "created_at": "2026-01-02T00:00:00Z"}}}
	source["wisp_child_counters"] = Table{Columns: columnsFor("wisp_child_counters"), Rows: []Row{{"parent_id": "demo-7", "last_child": 2}}}
	dep := cloneRow(source["dependencies"].Rows[0])
	dep["id"], dep["issue_id"], dep["depends_on_issue_id"] = "wisp-edge", "demo-7", "demo-8"
	dep["depends_on_wisp_id"], dep["depends_on_external"] = nil, nil
	wispToWisp := cloneRow(dep)
	wispToWisp["id"], wispToWisp["depends_on_issue_id"], wispToWisp["depends_on_wisp_id"] = "wisp-edge-wisp", nil, "demo-9"
	wispToWisp["type"] = "waits-for"
	wispToWisp["metadata"] = map[string]any{"gate": "any-children"}
	wispToExternal := cloneRow(dep)
	wispToExternal["id"], wispToExternal["depends_on_issue_id"], wispToExternal["depends_on_wisp_id"], wispToExternal["depends_on_external"] = "wisp-edge-external", nil, nil, "external:tracker:capability"
	durableToWisp := cloneRow(dep)
	durableToWisp["id"], durableToWisp["issue_id"], durableToWisp["depends_on_issue_id"], durableToWisp["depends_on_wisp_id"] = "durable-edge-wisp", "demo-8", nil, "demo-7"
	source["wisp_dependencies"] = Table{Columns: columnsFor("wisp_dependencies"), Rows: []Row{dep, wispToWisp, wispToExternal}}
	durableDependencies := source["dependencies"]
	durableDependencies.Rows = append(durableDependencies.Rows, durableToWisp)
	source["dependencies"] = durableDependencies

	data := buildFixture(t, source, inventory)
	if _, err := Validate(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	members := archiveMembers(t, data)
	var durable, active, wispEdge, wispToWispEdge, wispExternalEdge, durableToWispEdge bool
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/bead.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := decodeJSON(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.ID == "demo-7" && jsonEqual(record.Source.Fields, issue) {
			durable = true
		}
	}
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := decodeJSON(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Source.Table == "wisps" && text(record.Source.Fields, "id") == "demo-7" && jsonEqual(record.Source.Fields, wisp) {
			active = true
		}
		if record.Source.Table == "wisp_labels" || record.Source.Table == "wisp_comments" || record.Source.Table == "wisp_events" || record.Source.Table == "wisp_child_counters" {
			if !record.Body["read_only"].(bool) || !record.Body["redactable"].(bool) {
				t.Fatalf("wisp sidecar lost archive controls: %+v", record.Body)
			}
		}
	}
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/edge.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := decodeJSON(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Source.Table == "wisp_dependencies" {
			switch record.ID {
			case "wisp-edge":
				wispEdge = record.Body["from"] == "demo-7" && record.Body["to"] == "demo-8"
			case "wisp-edge-wisp":
				wispToWispEdge = record.Body["from"] == "demo-7" && record.Body["to"] == "demo-9"
			case "wisp-edge-external":
				wispExternalEdge = record.Body["from"] == "demo-7" && record.Body["to"] == "external:tracker:capability"
			}
		}
		if record.ID == "durable-edge-wisp" && record.Source.Table == "dependencies" && record.Body["from"] == "demo-8" && record.Body["to"] == "demo-7" {
			durableToWispEdge = true
		}
	}
	if !durable || !active || !wispEdge || !wispToWispEdge || !wispExternalEdge || !durableToWispEdge {
		t.Fatalf("faithful active collision lost a source row: durable=%v active=%v wispEdge=%v wispToWisp=%v wispExternal=%v durableToWisp=%v", durable, active, wispEdge, wispToWispEdge, wispExternalEdge, durableToWispEdge)
	}
}

func TestDumpProjectConfigEmptyYAMLObservationIsExplicitObject(t *testing.T) {
	source, inventory := fixture(t)
	root := t.TempDir()
	yamlPath := filepath.Join(root, "config.yaml")
	raw := []byte("# project config remains byte exact\n# no values\n")
	if err := os.WriteFile(yamlPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	observation, err := observeNativePath(yamlPath, 0, &nativeCaptureBudget{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range inventory.External {
		if inventory.External[i].Family != "project-config" {
			continue
		}
		inventory.External[i].State = "json-config"
		if err := os.WriteFile(inventory.External[i].Path, mustJSON(t, map[string]any{"observations": []any{observation}}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	data := buildFixture(t, source, inventory)
	if !bytes.Contains(archiveMembers(t, data)["config/sources.json"], []byte(`"values":{}`)) || !bytes.Contains(archiveMembers(t, data)["config/sources.json"], []byte(fmt.Sprintf("%x", sha256.Sum256(raw)))) {
		t.Fatal("project-config empty YAML did not preserve explicit values, bytes or hash")
	}
	if _, err := Validate(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

func TestDumpProjectConfigExplicitNullYAMLRefuses(t *testing.T) {
	source, inventory := fixture(t)
	root := t.TempDir()
	yamlPath := filepath.Join(root, "config.yaml")
	raw := []byte("null\n")
	if err := os.WriteFile(yamlPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	observation, err := observeNativePath(yamlPath, 0, &nativeCaptureBudget{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range inventory.External {
		if inventory.External[i].Family == "project-config" {
			inventory.External[i].State = "json-config"
			if err := os.WriteFile(inventory.External[i].Path, mustJSON(t, map[string]any{"observations": []any{observation}}), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 || !strings.Contains(err.Error(), "missing parsed values") {
		t.Fatalf("explicit-null project YAML was accepted: data=%d err=%v", len(data), err)
	}
}

func TestDumpProjectConfigDirectoryObservationPreservesEmptyYAMLObject(t *testing.T) {
	source, inventory := fixture(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "config.yaml"), []byte("# directory comment-only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	observation, err := observeNativePath(root, 0, &nativeCaptureBudget{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range inventory.External {
		if inventory.External[i].Family == "project-config" {
			inventory.External[i].State = "json-config"
			if err := os.WriteFile(inventory.External[i].Path, mustJSON(t, map[string]any{"observations": []any{observation}}), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	data := buildFixture(t, source, inventory)
	if !bytes.Contains(archiveMembers(t, data)["config/sources.json"], []byte(`"values":{}`)) {
		t.Fatal("directory project-config lost nested empty YAML object")
	}
}

func TestDumpCatalogUnionIncludesStoredAndYAMLSources(t *testing.T) {
	source, inventory := fixture(t)
	status := source["custom_statuses"]
	status.Rows = append(status.Rows, Row{"name": "review", "category": "active"})
	source["custom_statuses"] = status
	typesTable := source["custom_types"]
	typesTable.Rows = append(typesTable.Rows, Row{"name": "gate"})
	source["custom_types"] = typesTable
	config := source["config"]
	config.Rows = append(config.Rows,
		Row{"key": "status.custom", "value": "review:active,stored_done:done"},
		Row{"key": "types.custom", "value": "gate,stored-kind"},
	)
	source["config"] = config
	inventory.ConfigPolicy["config/status.custom"] = "public"
	inventory.ConfigPolicy["config/types.custom"] = "public"
	root := t.TempDir()
	yamlPath := filepath.Join(root, "config.yaml")
	raw := []byte("status:\n  custom: review:active,yaml_done:done\ntypes:\n  custom: [gate, yaml-kind]\n")
	if err := os.WriteFile(yamlPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	observation, err := observeNativePath(yamlPath, 0, &nativeCaptureBudget{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range inventory.External {
		if inventory.External[i].Family == "project-config" {
			inventory.External[i].State = "json-config"
			if err := os.WriteFile(inventory.External[i].Path, mustJSON(t, map[string]any{"observations": []any{observation}}), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	data := buildFixture(t, source, inventory)
	for _, wanted := range []string{"review", "stored_done", "yaml_done", "gate", "stored-kind", "yaml-kind"} {
		if !bytes.Contains(data, []byte(wanted)) {
			t.Errorf("catalog union value %q missing", wanted)
		}
	}
	if _, err := Validate(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	var statusRow, typeRow bool
	for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, data)["records/beads-archive.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := decodeJSON(line, &record); err != nil {
			t.Fatal(err)
		}
		statusRow = statusRow || record.Source.Table == "custom_statuses"
		typeRow = typeRow || record.Source.Table == "custom_types"
	}
	if !statusRow || !typeRow {
		t.Fatalf("catalog source rows were not archived: status=%v type=%v", statusRow, typeRow)
	}
}

func TestDumpCatalogMalformedAndDuplicateRowsRefuse(t *testing.T) {
	for name, mutate := range map[string]func(fixtureSource){
		"duplicate status rows": func(source fixtureSource) {
			table := source["custom_statuses"]
			table.Rows = []Row{{"name": "review", "category": "active"}, {"name": "review", "category": "active"}}
			source["custom_statuses"] = table
		},
		"empty custom type": func(source fixtureSource) {
			table := source["custom_types"]
			table.Rows = []Row{{"name": ""}}
			source["custom_types"] = table
		},
		"malformed status config": func(source fixtureSource) {
			table := source["config"]
			table.Rows = append(table.Rows, Row{"key": "status.custom", "value": "review:unknown"})
			source["config"] = table
		},
	} {
		t.Run(name, func(t *testing.T) {
			source, inventory := fixture(t)
			mutate(source)
			if name == "malformed status config" {
				inventory.ConfigPolicy["config/status.custom"] = "public"
			}
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 {
				t.Fatalf("malformed catalog accepted: data=%d err=%v", len(data), err)
			}
		})
	}
}

func TestIssueSourceFieldsContractRefusesMissingOpaqueField(t *testing.T) {
	source, _ := fixture(t)
	row := cloneRow(source["issues"].Rows[0])
	delete(row, "payload")
	if _, err := copyIssueSourceFields(row); err == nil || !strings.Contains(err.Error(), "missing source field in issues") {
		t.Fatalf("missing physical source field was accepted: %v", err)
	}
}

func TestDumpDependencyMetadataRefusals(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		metadata any
		want     string
	}{
		{name: "invalid JSON", kind: "related", metadata: "{", want: "metadata is not valid JSON"},
		{name: "waits-for non-object", kind: "waits-for", metadata: []any{"not-an-object"}, want: "waits-for metadata must be a JSON object"},
		{name: "unknown gate", kind: "waits-for", metadata: map[string]any{"gate": "never"}, want: "waits-for gate must be all-children or any-children"},
		{name: "non-boolean also_blocks", kind: "waits-for", metadata: map[string]any{"gate": "all-children", "also_blocks": "true"}, want: "waits-for also_blocks must be boolean"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, inventory := fixture(t)
			row := cloneRow(source["dependencies"].Rows[0])
			row["id"] = "edge-invalid-metadata"
			row["type"] = test.kind
			row["metadata"] = test.metadata
			table := source["dependencies"]
			table.Rows = append(table.Rows, row)
			source["dependencies"] = table
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid dependency metadata accepted: data=%d err=%v", len(data), err)
			}
		})
	}
}

func TestDumpRefusals(t *testing.T) {
	tests := map[string]func(fixtureSource, *Inventory){
		"unknown main":    func(s fixtureSource, _ *Inventory) { s["schema_migrations"].Rows[0]["version"] = 73 },
		"unknown ignored": func(s fixtureSource, _ *Inventory) { s["ignored_schema_migrations"].Rows[0]["version"] = 29 },
		"migration hash":  func(s fixtureSource, _ *Inventory) { s["schema_migrations"].Rows[0]["content_hash"] = "altered" },
		"unknown table":   func(s fixtureSource, _ *Inventory) { s["unaccounted"] = Table{} },
		"missing family":  func(s fixtureSource, _ *Inventory) { delete(s, "wisps") },
		"schema drift": func(s fixtureSource, _ *Inventory) {
			v := s["issues"]
			v.Columns = append(v.Columns, "extra")
			s["issues"] = v
		},
		"unknown row field": func(s fixtureSource, _ *Inventory) { s["issues"].Rows[0]["extra"] = true },
		"duplicate prefix":  func(_ fixtureSource, i *Inventory) { i.Projects = append(i.Projects, i.Projects[0]) },
		"unmerged remote":   func(_ fixtureSource, i *Inventory) { i.RemotesMerged = false },
		"dirty capture":     func(_ fixtureSource, i *Inventory) { i.WorkingSetClean = false },
		"live claim": func(s fixtureSource, _ *Inventory) {
			row := Row{}
			for _, c := range columnsFor("leases") {
				row[c] = nil
			}
			row["holder"] = "fixture-agent"
			v := s["leases"]
			v.Rows = append(v.Rows, row)
			s["leases"] = v
		},
		"unaccounted external":        func(_ fixtureSource, i *Inventory) { i.External = i.External[1:] },
		"unknown external":            func(_ fixtureSource, i *Inventory) { i.External[0].Family = "unrequested" },
		"unsupported external format": func(_ fixtureSource, i *Inventory) { i.External[0].State = "yaml" },
		"custom status":               func(s fixtureSource, _ *Inventory) { s["issues"].Rows[0]["status"] = "review" },
		"workflow":                    func(s fixtureSource, _ *Inventory) { s["issues"].Rows[0]["issue_type"] = "agent" },
		"invalid priority":            func(s fixtureSource, _ *Inventory) { s["issues"].Rows[0]["priority"] = 5 },
		"missing title":               func(s fixtureSource, _ *Inventory) { s["issues"].Rows[0]["title"] = "" },
		"invalid child path":          func(s fixtureSource, _ *Inventory) { s["issues"].Rows[0]["id"] = "demo-7.invalid.4" },
		"dangling edge":               func(s fixtureSource, _ *Inventory) { s["dependencies"].Rows[0]["depends_on_issue_id"] = "demo-999" },
		"wisp target": func(s fixtureSource, _ *Inventory) {
			s["dependencies"].Rows[0]["depends_on_issue_id"] = nil
			s["dependencies"].Rows[0]["depends_on_wisp_id"] = "wisp-1"
		},
		"typed targets": func(s fixtureSource, _ *Inventory) {
			s["dependencies"].Rows[0]["depends_on_external"] = "external:project:capability"
		},
		"duplicate issue": func(s fixtureSource, _ *Inventory) {
			v := s["issues"]
			v.Rows = append(v.Rows, v.Rows[0])
			s["issues"] = v
		},
		"credential public":   func(_ fixtureSource, i *Inventory) { i.ConfigPolicy["config/github.token"] = "public" },
		"unclassified config": func(_ fixtureSource, i *Inventory) { delete(i.ConfigPolicy, "config/kv.memory.fixture") },
	}
	for name, edit := range tests {
		t.Run(name, func(t *testing.T) {
			source, inventory := fixture(t)
			edit(source, &inventory)
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 {
				t.Fatal("unsupported source returned successful or partial archive")
			}
		})
	}
}

func TestDumpExternalConfigAndLosslessCounters(t *testing.T) {
	source, inventory := fixture(t)
	input := &inventory.External[0]
	input.State = "json-config"
	if err := os.WriteFile(input.Path, []byte(`{"custom":{"enabled":true},"github.token":{"$secret":"fixture/github"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	source["issue_counter"].Rows[0]["last_id"] = json.Number("9007199254740993")
	data := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(data))
	if err != nil || manifest.Projects[0].LastID != 9007199254740993 {
		t.Fatal("large allocator counter was narrowed")
	}
	if !bytes.Contains(archiveMembers(t, data)["config/sources.json"], []byte("fixture/github")) {
		t.Fatal("configuration provenance/reference lost")
	}
	input.State = "absent"
	if _, err := Build(context.Background(), source, inventory); err == nil {
		t.Fatal("present requested file was accepted as absent")
	}
	input.State = "json-config"
	if err := os.WriteFile(input.Path, []byte(`{"github.token":"synthetic-do-not-copy"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), source, inventory); err == nil {
		t.Fatal("raw external credential accepted")
	}
}

var dumpSecretTestKeys = []string{
	"service.api-key", "integrations.apikey", "database.passwd", "signing.privatekey",
	"tls.private_key", "ssh.privkey", "ssh.key", "service.keys", "database.pwd",
	"github.pat", "github.auth", "registry.bearer", "tls.cert", "aws.credentials",
	"service.credential", "service.secret", "service.token", "service.password",
	"service.APIKEY", "database.dsn", "legacy.access_keyname",
}

func TestDumpSecretClassificationMatchesConfigPredicate(t *testing.T) {
	source, inventory := fixture(t)
	for _, name := range []string{"config", "metadata"} {
		for _, key := range dumpSecretTestKeys {
			table := source[name]
			table.Rows = append(table.Rows, Row{"key": key, "value": "fixture-classified-value"})
			source[name] = table
			inventory.ConfigPolicy[name+"/"+key] = "secret"
		}
	}
	data := buildFixture(t, source, inventory)
	if bytes.Contains(data, []byte("fixture-classified-value")) {
		t.Fatal("recognized secret value entered the archive")
	}
	for _, name := range []string{"config", "metadata"} {
		for _, key := range dumpSecretTestKeys {
			if !bytes.Contains(data, []byte(`"$secret":"`+name+`/`+key+`"`)) {
				t.Errorf("missing secret reference for %q", name+"/"+key)
			}
		}
	}

	external := &inventory.External[0]
	external.State = "json-config"
	if err := os.WriteFile(external.Path, []byte(`{"nested":{"api-key":{"$secret":"fixture/api-key"},"apikey":{"$secret":"fixture/apikey"},"passwd":{"$secret":"fixture/passwd"},"privatekey":{"$secret":"fixture/privatekey"}},"safe":"fixture-public-value"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	data = buildFixture(t, source, inventory)
	if bytes.Contains(data, []byte("fixture-classified-value")) || !bytes.Contains(data, []byte("fixture/api-key")) || !bytes.Contains(data, []byte("fixture/privatekey")) {
		t.Fatal("nested external secret references were not preserved safely")
	}
	if !bytes.Contains(data, []byte("fixture-public-value")) {
		t.Fatal("nested external public value was lost")
	}
	if err := os.WriteFile(external.Path, []byte(`{"nested":{"api-key":null,"apikey":null,"passwd":null,"privatekey":null}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 {
		t.Fatal("nested external JSON accepted missing credential references")
	}
}

func TestDumpSecretClassificationPolicyRefusals(t *testing.T) {
	for _, key := range dumpSecretTestKeys {
		t.Run(key, func(t *testing.T) {
			for _, name := range []string{"config", "metadata"} {
				source, inventory := fixture(t)
				table := source[name]
				table.Rows = append(table.Rows, Row{"key": key, "value": nil})
				source[name] = table
				inventory.ConfigPolicy[name+"/"+key] = "public"
				if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 || !strings.Contains(err.Error(), "requires explicit public/secret policy") {
					t.Fatalf("%s accepted public policy for a recognized secret key", name)
				}
				record := Record{Kind: "config", ID: name + "/" + key,
					Body:   map[string]any{"classification": "public", "value": nil},
					Source: Provenance{Table: name, Fields: Row{"key": key, "value": nil}}}
				if err := validateRecord(record, nil, map[string]int{}); err == nil || !strings.Contains(err.Error(), "unclassified or raw credential record") {
					t.Fatalf("archive record validation accepted public policy for %s", name)
				}
			}
		})
	}
}

func TestDumpSecretClassificationNestedReferences(t *testing.T) {
	for _, key := range dumpSecretTestKeys {
		t.Run(key, func(t *testing.T) {
			for _, value := range []any{nil, false, map[string]any{"$secret": ""}, map[string]any{"$secret": "fixture/reference", "extra": true}} {
				nested := map[string]any{"nested": []any{map[string]any{key: value}}}
				if err := validateSecretReferences(nested); err == nil {
					t.Fatal("nested recognized key accepted a missing or malformed reference")
				}
			}
			valid := map[string]any{"nested": []any{map[string]any{key: map[string]any{"$secret": "fixture/reference"}}}}
			if err := validateSecretReferences(valid); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, key := range []string{"issue.path", "export.pattern", "commit.author", "sort.keyword", "build.compat"} {
		if err := validateSecretReferences(map[string]any{key: false}); err != nil {
			t.Fatalf("ordinary public key %s was rejected: %v", key, err)
		}
	}
}

func TestDumpLocalMetadataUsesEntryKey(t *testing.T) {
	source, inventory := fixture(t)
	table := source["local_metadata"]
	table.Rows = []Row{{"key": "bd_version", "value": "fixture-version"}}
	source["local_metadata"] = table
	data := buildFixture(t, source, inventory)
	if !bytes.Contains(archiveMembers(t, data)["records/beads-archive.ndjson"], []byte("fixture-version")) {
		t.Fatal("ordinary local metadata was not archived")
	}
	table.Rows[0] = Row{"key": "service.api-key", "value": false}
	source["local_metadata"] = table
	if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 {
		t.Fatal("local metadata accepted a recognized key without a reference")
	}
	table.Rows[0]["value"] = map[string]any{"$secret": "fixture/reference"}
	source["local_metadata"] = table
	buildFixture(t, source, inventory)
}

func TestDumpInvalidArchive(t *testing.T) {
	source, inventory := fixture(t)
	data := buildFixture(t, source, inventory)
	tests := map[string]func(map[string][]byte){
		"checksum":       func(m map[string][]byte) { m["records/bead.ndjson"][10] ^= 1 },
		"missing":        func(m map[string][]byte) { delete(m, "records/edge.ndjson") },
		"unknown member": func(m map[string][]byte) { m["unknown"] = []byte("unaccounted") },
		"manifest schema": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(`"schema":1`), []byte(`"schema":2`), 1)
		},
		"empty source fields contract": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(IssueSourceFieldsContract), []byte(``), 1)
		},
		"unknown source fields contract": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(IssueSourceFieldsContract), []byte("unknown-contract"), 1)
		},
		"registry version": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(`"version":1`), []byte(`"version":2`), 1)
		},
		"unknown registry kind": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(`"name":"bead"`), []byte(`"name":"unknown"`), 1)
		},
		"false verification claim": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(`"a3":"not-run"`), []byte(`"a3":"passed"`), 1)
		},
		"duplicate manifest key": func(m map[string][]byte) {
			m["manifest.json"] = bytes.Replace(m["manifest.json"], []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1)
		},
	}
	for name, edit := range tests {
		t.Run(name, func(t *testing.T) {
			members := archiveMembers(t, data)
			edit(members)
			var output bytes.Buffer
			writer := tar.NewWriter(&output)
			var names []string
			for name := range members {
				if name != "manifest.json" {
					names = append(names, name)
				}
			}
			sort.Strings(names)
			for _, name := range append([]string{"manifest.json"}, names...) {
				if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(members[name])), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Write(members[name]); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Validate(&output); err == nil {
				t.Fatal("invalid archive accepted")
			}
		})
	}
	if _, err := Validate(bytes.NewReader(data[:len(data)-1])); err == nil {
		t.Fatal("truncated archive accepted")
	}
}

type failedSource struct{ fixtureSource }

func (f failedSource) ReadTable(ctx context.Context, name string) (Table, error) {
	if name == "comments" {
		return Table{}, errors.New("fixture read failure")
	}
	return f.fixtureSource.ReadTable(ctx, name)
}

func TestDumpReadFailureAndInventoryParsing(t *testing.T) {
	source, inventory := fixture(t)
	if data, err := Build(context.Background(), failedSource{source}, inventory); err == nil || len(data) != 0 {
		t.Fatal("failed source yielded a partial dump")
	}
	for _, text := range []string{`{"version":"beads-inventory/1","unknown":true}`, `{"version":"a","version":"b"}`, `{} {}`} {
		if _, err := ReadInventory(strings.NewReader(text)); err == nil {
			t.Fatal("unknown/ambiguous inventory accepted")
		}
	}
}

func TestDumpSQLValuePreservesTextAndBinary(t *testing.T) {
	for _, value := range []any{string([]byte{0xff}), []byte{0xff}} {
		if _, err := sqlValue(value, "TEXT"); err == nil {
			t.Fatal("invalid UTF-8 accepted for lossy JSON encoding")
		}
	}
	value, err := sqlValue([]byte{0xff, 0x00}, "LONGBLOB")
	if err != nil || value.(map[string]string)["$bytes"] != "/wA=" {
		t.Fatal("binary bytes were not preserved")
	}
	value, err = sqlValue([]byte(`{"counter":9007199254740993}`), "JSON")
	if err != nil || value.(map[string]any)["counter"] != json.Number("9007199254740993") {
		t.Fatal("SQL JSON integer was narrowed")
	}
}

func TestDumpNaturalAcceptsLosslessIntegerWidths(t *testing.T) {
	const want = uint64(42)
	for name, value := range map[string]any{
		"int":    int(42),
		"int8":   int8(42),
		"int16":  int16(42),
		"int32":  int32(42),
		"int64":  int64(42),
		"uint":   uint(42),
		"uint8":  uint8(42),
		"uint16": uint16(42),
		"uint32": uint32(42),
		"uint64": uint64(42),
		"number": json.Number("42"),
		"string": "42",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := natural(value)
			if err != nil || got != want {
				t.Fatalf("natural(%T) = %d, %v; want %d, nil", value, got, err, want)
			}
		})
	}
	if _, err := natural(int32(-1)); err == nil {
		t.Fatal("natural accepted a negative signed integer")
	}
}

func TestDumpEveryUnsupportedFamily(t *testing.T) {
	for name := range tableColumns {
		action, _ := actionFor(name)
		if action != "refuse-if-nonempty" && name != "leases" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			source, inventory := fixture(t)
			row := Row{}
			for _, column := range columnsFor(name) {
				row[column] = nil
			}
			// Empty credential cells do not constitute a secret value.
			if name == "federation_peers" {
				row["password_encrypted"] = map[string]any{"$secret": "fixture/federation"}
			}
			table := source[name]
			table.Rows = []Row{row}
			source[name] = table
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 || !strings.Contains(err.Error(), "nonempty family "+name) {
				t.Fatalf("family did not reach its explicit refusal: %v", err)
			}
		})
	}
}

func TestDumpMissingHistoryIsReported(t *testing.T) {
	source, inventory := fixture(t)
	table := source["issue_versions"]
	table.Rows = nil
	source["issue_versions"] = table
	if _, err := Build(context.Background(), source, inventory); err == nil {
		t.Fatal("false history availability claim accepted")
	}
	inventory.History = "unavailable"
	data := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(data))
	if err != nil || manifest.History != "unavailable" {
		t.Fatal("missing history not reported")
	}
	if !bytes.Contains(archiveMembers(t, data)["records/beads-archive.ndjson"], []byte("event-fixture")) {
		t.Fatal("event evidence lost when version history unavailable")
	}
}
