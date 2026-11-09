package plugdump

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// This is a mapping fixture, not persisted-native or public-runtime evidence.
func sourcePlaneFixture(t *testing.T, status string) (fixtureSource, Inventory) {
	t.Helper()
	source, inventory := fixture(t)
	inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
	wisp := cloneRow(source["issues"].Rows[0])
	wisp["title"], wisp["status"], wisp["ephemeral"], wisp["no_history"] = "raw active source", status, true, true
	second := cloneRow(wisp)
	second["id"], second["title"] = "demo-9", "second active source"
	source["wisps"] = Table{Columns: columnsFor("wisps"), Rows: []Row{wisp, second}}
	source["custom_statuses"] = Table{Columns: columnsFor("custom_statuses"), Rows: []Row{{"name": "review", "category": "active"}}}
	durable := cloneRow(source["dependencies"].Rows[0])
	durable["id"], durable["issue_id"], durable["type"] = "shared\x00physical-edge", wisp["id"], "waits-for"
	durable["depends_on_issue_id"], durable["depends_on_wisp_id"], durable["depends_on_external"] = source["issues"].Rows[1]["id"], nil, nil
	durable["metadata"], durable["thread_id"] = `{"gate":"","n":9007199254740993}`, "physical-thread"
	active := cloneRow(durable)
	active["depends_on_issue_id"], active["depends_on_wisp_id"] = nil, second["id"]
	active["metadata"] = map[string]any{"gate": "any-children", "also_blocks": true, "n": json.Number("9007199254740993")}
	source["dependencies"] = Table{Columns: columnsFor("dependencies"), Rows: []Row{durable}}
	source["wisp_dependencies"] = Table{Columns: columnsFor("wisp_dependencies"), Rows: []Row{active}}
	return source, inventory
}

func sourcePlaneRecords(t *testing.T, member []byte) []Record {
	t.Helper()
	var records []Record
	decoder := json.NewDecoder(bytes.NewReader(member))
	decoder.UseNumber()
	for {
		var r Record
		if err := decoder.Decode(&r); err == io.EOF {
			return records
		} else if err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
	}
}

// Recompute only USTAR framing and checksums to exercise hostile-input
// validation. Keeping the raw manifest map preserves null/empty marker tests.
func reframeSourcePlaneArchive(t *testing.T, members map[string][]byte) []byte {
	t.Helper()
	var manifest map[string]any
	if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	files := map[string]File{}
	var paths []string
	for path, data := range members {
		if path != "manifest.json" {
			files[path] = File{digest(data), int64(len(data))}
			paths = append(paths, path)
		}
	}
	manifest["files"] = files
	members["manifest.json"] = mustJSON(t, manifest)
	sort.Strings(paths)
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, path := range append([]string{"manifest.json"}, paths...) {
		data := members[path]
		if err := writer.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestSourcePlaneCaptureConservesPhysicalIdentitiesAndRawRows(t *testing.T) {
	source, inventory := sourcePlaneFixture(t, "review")
	data := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(data))
	if err != nil || manifest.SourcePlaneAddressContract != SourcePlaneAddressContract {
		t.Fatalf("source plane marker lost: %+v %v", manifest, err)
	}
	members := archiveMembers(t, data)
	actual := map[string][]Row{}
	edges := sourcePlaneRecords(t, members["records/edge.ndjson"])
	if len(edges) != 2 || edges[0].Source.Table != "dependencies" || edges[1].Source.Table != "wisp_dependencies" || edges[0].ID != edges[1].ID || edges[0].Project != edges[1].Project {
		t.Fatalf("equal physical edge IDs collapsed or reminted: %+v", edges)
	}
	for _, edge := range edges {
		actual[edge.Source.Table] = append(actual[edge.Source.Table], edge.Source.Fields)
		if len(edge.Source.Fields) != 10 || edge.ID != edge.Source.Fields["id"] || edge.Body["from"] != edge.Source.Fields["issue_id"] || edge.Body["to"] != edge.Source.Fields[text(edge.Body, "target_kind")] || edge.Body["type"] != edge.Source.Fields["type"] || edge.Body["thread_id"] != edge.Source.Fields["thread_id"] || !jsonEqual(edge.Body["metadata"], edge.Source.Fields["metadata"]) {
			t.Fatalf("physical edge fields/body changed: %+v", edge)
		}
	}
	for _, record := range sourcePlaneRecords(t, members["records/beads-archive.ndjson"]) {
		actual[record.Source.Table] = append(actual[record.Source.Table], record.Source.Fields)
	}
	for _, table := range []string{"wisps", "custom_statuses", "dependencies", "wisp_dependencies"} {
		want, got := []string{}, []string{}
		for _, row := range source[table].Rows {
			want = append(want, string(mustJSON(t, row)))
		}
		for _, row := range actual[table] {
			got = append(got, string(mustJSON(t, row)))
		}
		sort.Strings(want)
		sort.Strings(got)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("raw %s multiset changed: got=%v want=%v", table, got, want)
		}
	}
	for table, input := range source {
		for i, j := 0, len(input.Rows)-1; i < j; i, j = i+1, j-1 {
			input.Rows[i], input.Rows[j] = input.Rows[j], input.Rows[i]
		}
		source[table] = input
	}
	if again := buildFixture(t, source, inventory); !bytes.Equal(data, again) {
		t.Fatal("source row order changed archive bytes")
	}
}

func TestSourcePlaneCapabilityKeepsLegacyBytesAndRefusals(t *testing.T) {
	source, inventory := fixture(t)
	legacy := archiveMembers(t, buildFixture(t, source, inventory))
	inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
	current := archiveMembers(t, buildFixture(t, source, inventory))
	var manifest map[string]any
	if err := decodeJSON(current["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	delete(manifest, "source_plane_address_contract")
	current["manifest.json"] = mustJSON(t, manifest)
	var legacyManifest map[string]any
	if err := decodeJSON(legacy["manifest.json"], &legacyManifest); err != nil {
		t.Fatal(err)
	}
	legacy["manifest.json"] = mustJSON(t, legacyManifest)
	if !reflect.DeepEqual(legacy, current) {
		t.Fatal("explicit cap changed ordinary source members beyond its marker")
	}
	for _, test := range []struct{ status, want string }{{"open", "duplicate edge record identity"}, {"review", "unsupported wisp status"}} {
		source, inventory := sourcePlaneFixture(t, test.status)
		inventory.SourcePlaneAddressContract = ""
		data, err := Build(context.Background(), source, inventory)
		if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("legacy refusal lost: bytes=%d err=%v want=%s", len(data), err, test.want)
		}
	}
}

func TestSourcePlaneMarkerRequiresExactPresentValue(t *testing.T) {
	for _, value := range []any{nil, "", "beads-source-planes/2", false, json.Number("1"), map[string]any{}} {
		t.Run(string(mustJSON(t, value)), func(t *testing.T) {
			_, inventory := fixture(t)
			baseline := mustJSON(t, inventory)
			if _, err := ReadInventory(bytes.NewReader(baseline)); err != nil {
				t.Fatalf("invalid baseline inventory: %v", err)
			}
			var object map[string]any
			if err := decodeJSON(baseline, &object); err != nil {
				t.Fatal(err)
			}
			object["source_plane_address_contract"] = value
			checkRefusal := func(where string, err error) {
				t.Helper()
				want := "source_plane_address_contract"
				if value == nil {
					want = "unsupported source plane address contract"
				}
				if _, ok := value.(string); ok {
					want = "unsupported source plane address contract"
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("%s did not reach the marker refusal: err=%v want=%s", where, err, want)
				}
			}
			_, err := ReadInventory(bytes.NewReader(mustJSON(t, object)))
			checkRefusal("inventory", err)
			source, inventory := fixture(t)
			archive := buildFixture(t, source, inventory)
			if _, err := Validate(bytes.NewReader(archive)); err != nil {
				t.Fatalf("invalid baseline archive: %v", err)
			}
			members := archiveMembers(t, archive)
			// Decode into a fresh object: reusing the inventory map would retain
			// unrelated inventory keys and cause an earlier unknown-field refusal.
			object = nil
			if err := decodeJSON(members["manifest.json"], &object); err != nil {
				t.Fatal(err)
			}
			object["source_plane_address_contract"] = value
			members["manifest.json"] = mustJSON(t, object)
			_, err = Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members)))
			checkRefusal("manifest", err)
		})
	}
	_, _, err := (SQLSource{}).NativeInventory(context.Background(), NativeCapture{SourcePlaneAddressContract: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "source plane address contract") {
		t.Fatalf("native selector was not rejected before SQL: %v", err)
	}
}

func TestSourcePlaneMarkerRejectsNoncanonicalAliases(t *testing.T) {
	source, inventory := fixture(t)
	baselineInventory := mustJSON(t, inventory)
	baselineArchive := buildFixture(t, source, inventory)
	if _, err := ReadInventory(bytes.NewReader(baselineInventory)); err != nil {
		t.Fatalf("invalid baseline inventory: %v", err)
	}
	if _, err := Validate(bytes.NewReader(baselineArchive)); err != nil {
		t.Fatalf("invalid baseline archive: %v", err)
	}
	for _, alias := range []string{"SOURCE_PLANE_ADDRESS_CONTRACT", "Source_Plane_Address_Contract", "ſource_plane_address_contract"} {
		for _, test := range []struct {
			name   string
			fields map[string]any
		}{
			{"null alias", map[string]any{alias: nil}},
			{"empty alias", map[string]any{alias: ""}},
			{"supported alias", map[string]any{alias: SourcePlaneAddressContract}},
			{"matching alias and canonical", map[string]any{alias: SourcePlaneAddressContract, "source_plane_address_contract": SourcePlaneAddressContract}},
			{"null alias and supported canonical", map[string]any{alias: nil, "source_plane_address_contract": SourcePlaneAddressContract}},
			{"supported alias and empty canonical", map[string]any{alias: SourcePlaneAddressContract, "source_plane_address_contract": ""}},
		} {
			t.Run(alias+"/"+test.name, func(t *testing.T) {
				var object map[string]any
				if err := decodeJSON(baselineInventory, &object); err != nil {
					t.Fatal(err)
				}
				for key, value := range test.fields {
					object[key] = value
				}
				_, err := ReadInventory(bytes.NewReader(mustJSON(t, object)))
				if err == nil || !strings.Contains(err.Error(), "noncanonical source_plane_address_contract key") {
					t.Fatalf("inventory alias did not reach marker refusal: %v", err)
				}
				members := archiveMembers(t, baselineArchive)
				object = nil
				if err := decodeJSON(members["manifest.json"], &object); err != nil {
					t.Fatal(err)
				}
				for key, value := range test.fields {
					object[key] = value
				}
				members["manifest.json"] = mustJSON(t, object)
				_, err = Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members)))
				if err == nil || !strings.Contains(err.Error(), "noncanonical source_plane_address_contract key") {
					t.Fatalf("manifest alias did not reach marker refusal: %v", err)
				}
			})
		}
	}
}

func TestSourcePlaneManifestPublishedSchemaCorrespondence(t *testing.T) {
	data, err := os.ReadFile("manifest.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := decodeJSON(data, &document); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	const schemaURL = "https://plug.invalid/manifest.schema.json"
	if err := compiler.AddResource(schemaURL, document); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile(schemaURL)
	if err != nil {
		t.Fatal(err)
	}
	for _, contract := range []string{"", SourcePlaneAddressContract} {
		name := "legacy"
		if contract != "" {
			name = "capability"
		}
		source, inventory := fixture(t)
		inventory.SourcePlaneAddressContract = contract
		if _, err := ReadInventory(bytes.NewReader(mustJSON(t, inventory))); err != nil {
			t.Fatalf("canonical inventory refused: %v", err)
		}
		archive := buildFixture(t, source, inventory)
		if _, err := Validate(bytes.NewReader(archive)); err != nil {
			t.Fatalf("canonical archive refused: %v", err)
		}
		var manifest map[string]any
		if err := decodeJSON(archiveMembers(t, archive)["manifest.json"], &manifest); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(manifest); err != nil {
			t.Fatalf("emitted manifest violates published schema (contract %q): %v", contract, err)
		}
		if got, present := manifest["source_plane_address_contract"]; (contract == "" && present) || (contract != "" && (!present || got != contract)) {
			t.Fatalf("canonical marker emission changed: contract=%q present=%v value=%v", contract, present, got)
		}
		for _, test := range []struct {
			name, key string
			value     any
		}{
			{"null marker", "source_plane_address_contract", nil},
			{"empty marker", "source_plane_address_contract", ""},
			{"unknown marker", "source_plane_address_contract", "beads-source-planes/2"},
			{"wrong marker type", "source_plane_address_contract", true},
			{"folded marker alias", "SOURCE_PLANE_ADDRESS_CONTRACT", SourcePlaneAddressContract},
			{"unknown member", "undeclared_member", "value"},
			{"unknown existing schema version", "schema", json.Number("2")},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				var changed map[string]any
				if err := decodeJSON(mustJSON(t, manifest), &changed); err != nil {
					t.Fatal(err)
				}
				changed[test.key] = test.value
				if err := schema.Validate(changed); err == nil || !strings.Contains(err.Error(), test.key) {
					t.Fatalf("published schema did not refuse %s at %q: %v", test.name, test.key, err)
				}
			})
		}
	}
}

func TestSourcePlaneFullTupleAndOpaqueRefusals(t *testing.T) {
	for _, test := range []struct {
		name, want string
		change     func(fixtureSource)
	}{
		{"same full edge tuple", "duplicate edge record identity", func(s fixtureSource) {
			table := s["dependencies"]
			row := cloneRow(table.Rows[0])
			row["metadata"] = `{"n":2}`
			table.Rows = append(table.Rows, row)
			s["dependencies"] = table
		}},
		{"duplicate active entity", "duplicate wisp ID", func(s fixtureSource) {
			table := s["wisps"]
			row := cloneRow(table.Rows[0])
			row["title"] = "different raw row, same physical ID"
			table.Rows = append(table.Rows, row)
			s["wisps"] = table
		}},
		{"undeclared active status", "unsupported wisp status", func(s fixtureSource) { s["wisps"].Rows[0]["status"] = "undeclared" }},
		{"duplicate status declaration", "duplicate custom status row", func(s fixtureSource) {
			table := s["custom_statuses"]
			table.Rows = append(table.Rows, Row{"name": "review", "category": "done"})
			s["custom_statuses"] = table
		}},
		{"missing target", "exactly one typed target", func(s fixtureSource) { s["dependencies"].Rows[0]["depends_on_issue_id"] = nil }},
		{"multiple target arms", "exactly one typed target", func(s fixtureSource) { s["dependencies"].Rows[0]["depends_on_wisp_id"] = "demo-9" }},
		{"new fixed kind active source", "unsupported dependency type", func(s fixtureSource) { s["wisp_dependencies"].Rows[0]["type"] = "duplicates" }},
		{"opaque active source", "durable issue source", func(s fixtureSource) {
			row := s["wisp_dependencies"].Rows[0]
			row["type"] = "custom-kind"
			row["depends_on_wisp_id"] = nil
			row["depends_on_issue_id"] = s["issues"].Rows[1]["id"]
		}},
		{"opaque active target", "unsupported dependency type", func(s fixtureSource) {
			row := s["dependencies"].Rows[0]
			row["type"] = "custom-kind"
			row["depends_on_issue_id"] = nil
			row["depends_on_wisp_id"] = "demo-9"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, inventory := sourcePlaneFixture(t, "review")
			test.change(source)
			data, err := Build(context.Background(), source, inventory)
			if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("non-atomic refusal: bytes=%d err=%v want=%s", len(data), err, test.want)
			}
		})
	}
}

func TestSourcePlaneReadbackRefusesReorderedOrForgedOrigins(t *testing.T) {
	for _, test := range []struct {
		name, want string
		change     func(*testing.T, map[string][]byte)
	}{
		{"reordered edges", "unordered typed record", func(t *testing.T, m map[string][]byte) {
			r := sourcePlaneRecords(t, m["records/edge.ndjson"])
			m["records/edge.ndjson"] = append(mustJSON(t, r[1]), mustJSON(t, r[0])...)
		}},
		{"wrong source project", "edge project", func(t *testing.T, m map[string][]byte) {
			r := sourcePlaneRecords(t, m["records/edge.ndjson"])
			r[0].Project = "other"
			sort.Slice(r, func(i, j int) bool { return compareRecords(r[i], r[j], SourcePlaneAddressContract) < 0 })
			m["records/edge.ndjson"] = append(mustJSON(t, r[0]), mustJSON(t, r[1])...)
			var manifest Manifest
			if err := decodeJSON(m["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.Projects = append(manifest.Projects, Project{Prefix: "other", IDMode: "sequential", HashLength: 0, ChildCounters: map[string]uint64{}})
			m["manifest.json"] = mustJSON(t, manifest)
		}},
		{"unbound raw active row", "source archive identity", func(t *testing.T, m map[string][]byte) {
			records := sourcePlaneRecords(t, m["records/beads-archive.ndjson"])
			for i := range records {
				if records[i].Source.Table == "wisps" {
					records[i].Source.Fields["title"] = "forged"
					break
				}
			}
			m["records/beads-archive.ndjson"] = nil
			for _, r := range records {
				m["records/beads-archive.ndjson"] = append(m["records/beads-archive.ndjson"], mustJSON(t, r)...)
			}
		}},
		{"rehashed undeclared active status", "unsupported wisp status", func(t *testing.T, m map[string][]byte) {
			records := sourcePlaneRecords(t, m["records/beads-archive.ndjson"])
			for i := range records {
				if records[i].Source.Table == "wisps" {
					records[i].Source.Fields["status"] = "undeclared"
					records[i].ID = "wisps/" + digest(mustJSON(t, records[i].Source.Fields))
					break
				}
			}
			sort.Slice(records, func(i, j int) bool { return compareRecords(records[i], records[j], SourcePlaneAddressContract) < 0 })
			m["records/beads-archive.ndjson"] = nil
			for _, r := range records {
				m["records/beads-archive.ndjson"] = append(m["records/beads-archive.ndjson"], mustJSON(t, r)...)
			}
		}},
		{"marker cannot override edge version", "requires edge registry version", func(t *testing.T, m map[string][]byte) {
			var manifest Manifest
			if err := decodeJSON(m["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			for i := range manifest.Kinds {
				if manifest.Kinds[i].Name == "edge" {
					manifest.Kinds[i].Version = 1
				}
			}
			m["manifest.json"] = mustJSON(t, manifest)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, inventory := sourcePlaneFixture(t, "review")
			members := archiveMembers(t, buildFixture(t, source, inventory))
			test.change(t, members)
			_, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members)))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("readback admitted forged source: %v want=%s", err, test.want)
			}
		})
	}
}

func TestSourcePlaneActiveStatusUsesStrictStoredAndYAMLUnion(t *testing.T) {
	for _, test := range []struct {
		name string
		yaml []byte
		want string
	}{
		{"YAML-only declaration", []byte("status:\n  custom: [review:active]\n"), ""},
		{"SQL YAML conflict", []byte("status:\n  custom: [review:done]\n"), "conflicting custom status"},
		{"explicit null catalog", []byte("status:\n  custom: null\n"), "custom status"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, inventory := sourcePlaneFixture(t, "review")
			if test.name == "YAML-only declaration" {
				source["custom_statuses"] = Table{Columns: columnsFor("custom_statuses")}
			}
			setCatalogYAML(t, &inventory, test.yaml)
			data, err := Build(context.Background(), source, inventory)
			if test.want != "" {
				if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("catalog refusal: bytes=%d err=%v want=%s", len(data), err, test.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Validate(bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSourcePlaneCaptureCannotPromoteUnrepresentablePortableHistory(t *testing.T) {
	source, inventory := sourcePlaneFixture(t, "review")
	id := strings.Repeat("a", 32)
	port := &historyFixtureSource{fixtureSource: source, log: SourceCommitLog{Head: id, Clean: true, Commits: []SourceCommit{{ID: id, Parents: []string{}, Metadata: Row{"message": "synthetic source-plane history boundary"}}}}, images: map[string]map[string]Table{id: source}}
	data := buildFixture(t, port, inventory)
	manifest, history := sourceCapture(t, data)
	if manifest.History != "available-unverified" || history.Range.Status != "unverified" || history.Range.Complete || history.Range.Reason != "head-image-not-proven" || len(history.Range.Frames) != 0 {
		t.Fatalf("unrepresentable source image acquired history credit: %+v %+v", manifest, history.Range)
	}
	if len(sourcePlaneRecords(t, archiveMembers(t, data)["records/edge.ndjson"])) != 2 || !jsonEqual(history.Events, canonicalSourceEvents(source["events"].Rows)) {
		t.Fatal("source-plane head or event evidence dropped")
	}
}

func statusCategoryFixture(t *testing.T) (fixtureSource, Inventory) {
	t.Helper()
	source, inventory := fixture(t)
	inventory.SourcePlaneAddressContract = SourcePlaneAddressContract
	inventory.StatusCategoryContract = StatusCategoryContract
	source["custom_statuses"] = Table{Columns: columnsFor("custom_statuses"), Rows: []Row{{"name": "review", "category": "wip"}, {"name": "parked", "category": "frozen"}}}
	config := source["config"]
	config.Rows = append(config.Rows, Row{"key": "status.custom", "value": "todo:active,shipped:done"})
	source["config"] = config
	inventory.ConfigPolicy["config/status.custom"] = "public"
	inventory.Projects[0].Statuses = []ProjectStatus{{"blocked", "wip"}, {"closed", "done"}, {"deferred", "frozen"}, {"hooked", "wip"}, {"in_progress", "wip"}, {"open", "active"}, {"parked", "frozen"}, {"pinned", "frozen"}, {"review", "wip"}, {"shipped", "done"}, {"todo", "active"}}
	for i, name := range []string{"todo", "shipped", "review"} {
		source["issues"].Rows[i]["status"] = name
	}
	return source, inventory
}

func TestStatusCategoryContractPreservesNativeNamesAndCategories(t *testing.T) {
	source, inventory := statusCategoryFixture(t)
	before := mustJSON(t, inventory)
	archive := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.StatusCategoryContract != StatusCategoryContract || !reflect.DeepEqual(manifest.Projects[0].Statuses, inventory.Projects[0].Statuses) || !bytes.Equal(before, mustJSON(t, inventory)) {
		t.Fatal("status catalog lost or input inventory mutated")
	}
	want := map[string]string{"todo": "active", "review": "wip", "shipped": "done"}
	for _, record := range sourcePlaneRecords(t, archiveMembers(t, archive)["records/bead.ndjson"]) {
		name := text(record.Source.Fields, "status")
		if record.Body["status_category"] != want[name] || record.Source.Fields["status"] != source["issues"].Rows[map[string]int{"demo-7": 0, "demo-8": 1, "demo-7.4": 2}[record.ID]]["status"] {
			t.Fatal("native status was renamed or category changed")
		}
		if name == "shipped" && record.Body["result"] != "success" {
			t.Fatal("native done category lost result")
		}
	}
	source["issues"].Rows[0]["status"] = "parked"
	frozen := buildFixture(t, source, inventory)
	if _, err := Validate(bytes.NewReader(frozen)); err != nil {
		t.Fatal(err)
	}
	for _, record := range sourcePlaneRecords(t, archiveMembers(t, frozen)["records/bead.ndjson"]) {
		if record.ID == "demo-7" && (record.Body["status_category"] != "frozen" || record.Source.Fields["status"] != "parked") {
			t.Fatal("frozen category lost")
		}
	}
	// An identical spelling on another project may carry a different declared
	// category; record validation must use its own bound project's ontology.
	other := Project{Prefix: "other", Statuses: []ProjectStatus{{Name: "todo", Category: "wip"}}}
	record := Record{Project: "other", Body: Row{"status_category": "wip"}, Source: Provenance{Fields: Row{"status": "todo"}}}
	if err := validateStatusRecord(record, append(manifest.Projects, other)); err != nil {
		t.Fatal(err)
	}
	record.Body["status_category"] = "active"
	if err := validateStatusRecord(record, append(manifest.Projects, other)); err == nil {
		t.Fatal("cross-project category substitution admitted")
	}
}

func TestStatusCategoryContractRefusesUnknownAndForgedDeclarations(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(fixtureSource, *Inventory)
	}{
		{"unknown status", func(s fixtureSource, i *Inventory) { s["issues"].Rows[0]["status"] = "unknown" }},
		{"undeclared frozen spelling", func(s fixtureSource, i *Inventory) { s["issues"].Rows[0]["status"] = "frozen" }},
		{"catalog mismatch", func(s fixtureSource, i *Inventory) {
			i.Projects[0].Statuses[len(i.Projects[0].Statuses)-1].Category = "wip"
		}},
		{"missing builtin", func(s fixtureSource, i *Inventory) { i.Projects[0].Statuses = i.Projects[0].Statuses[1:] }},
		{"unspecified", func(s fixtureSource, i *Inventory) { s["custom_statuses"].Rows[0]["category"] = "unspecified" }},
		{"conflicting union", func(s fixtureSource, i *Inventory) {
			s["config"].Rows[len(s["config"].Rows)-1]["value"] = "review:active,todo:active,shipped:done"
		}},
		{"sorted duplicate", func(s fixtureSource, i *Inventory) {
			i.Projects[0].Statuses = append(i.Projects[0].Statuses, ProjectStatus{"todo", "active"})
		}},
		{"without marker", func(s fixtureSource, i *Inventory) { i.StatusCategoryContract = "" }},
		{"unknown marker", func(s fixtureSource, i *Inventory) { i.StatusCategoryContract = "beads-status-categories/2" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, i := statusCategoryFixture(t)
			test.change(s, &i)
			if data, err := Build(context.Background(), s, i); err == nil || len(data) != 0 {
				t.Fatal("unsupported native status emitted archive")
			}
		})
	}
	source, inventory := statusCategoryFixture(t)
	data := buildFixture(t, source, inventory)
	for _, test := range []struct {
		name   string
		change func(map[string][]byte)
	}{
		{"changed category", func(m map[string][]byte) {
			m["records/bead.ndjson"] = bytes.Replace(m["records/bead.ndjson"], []byte(`"status_category":"active"`), []byte(`"status_category":"wip"`), 1)
		}},
		{"renamed status with mismatched category", func(m map[string][]byte) {
			// Both todo and open are declared active. A self-consistent source
			// archive cannot authenticate one same-category spelling against
			// another. Conservation is checked against SQL in the positive
			// controls; this negative must actually violate the catalog.
			m["records/bead.ndjson"] = bytes.ReplaceAll(m["records/bead.ndjson"], []byte(`"status":"todo"`), []byte(`"status":"review"`))
		}},
		{"done result lost", func(m map[string][]byte) {
			m["records/bead.ndjson"] = bytes.Replace(m["records/bead.ndjson"], []byte(`"result":"success",`), nil, 1)
		}},
		{"marker removed", func(m map[string][]byte) {
			var obj map[string]any
			if err := decodeJSON(m["manifest.json"], &obj); err != nil {
				t.Fatal(err)
			}
			delete(obj, "status_category_contract")
			m["manifest.json"] = mustJSON(t, obj)
		}},
		{"marker and catalogs removed", func(m map[string][]byte) {
			var manifest Manifest
			if err := decodeJSON(m["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.StatusCategoryContract = ""
			manifest.Verification = verificationLimits()
			for i := range manifest.Projects {
				manifest.Projects[i].Statuses = nil
			}
			m["manifest.json"] = mustJSON(t, manifest)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := archiveMembers(t, data)
			test.change(m)
			if _, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, m))); err == nil {
				t.Fatal("tampered status archive admitted")
			}
		})
	}
	for _, value := range []any{nil, "", "beads-status-categories/2", true} {
		var obj map[string]any
		if err := decodeJSON(mustJSON(t, inventory), &obj); err != nil {
			t.Fatal(err)
		}
		obj["status_category_contract"] = value
		if _, err := ReadInventory(bytes.NewReader(mustJSON(t, obj))); err == nil {
			t.Fatal("invalid marker admitted")
		}
	}
	var folded map[string]any
	if err := decodeJSON(mustJSON(t, inventory), &folded); err != nil {
		t.Fatal(err)
	}
	delete(folded, "status_category_contract")
	folded["STATUS_CATEGORY_CONTRACT"] = StatusCategoryContract
	if _, err := ReadInventory(bytes.NewReader(mustJSON(t, folded))); err == nil {
		t.Fatal("folded marker admitted")
	}
}

func TestStatusCategoryManifestPublishedSchemaCorrespondence(t *testing.T) {
	source, inventory := statusCategoryFixture(t)
	data := buildFixture(t, source, inventory)
	var document, manifest any
	schemaBytes, err := os.ReadFile("manifest.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(schemaBytes, &document); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(archiveMembers(t, data)["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	const url = "https://plug.invalid/native-status-manifest.json"
	if err := compiler.AddResource(url, document); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(manifest); err != nil {
		t.Fatal(err)
	}
}

func TestStatusCategoryCatalogIncludesCapturedYAML(t *testing.T) {
	source, inventory := statusCategoryFixture(t)
	path := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(path, []byte("status:\n  custom: yaml_done:done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	observation, err := observeNativePath(path, 0, &nativeCaptureBudget{})
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
	inventory.Projects[0].Statuses = append(inventory.Projects[0].Statuses, ProjectStatus{"yaml_done", "done"})
	source["issues"].Rows[0]["status"] = "yaml_done"
	archive := buildFixture(t, source, inventory)
	m, err := Validate(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	if projectStatusCategory(m.Projects[0], "yaml_done") != "done" {
		t.Fatal("captured native YAML declaration omitted")
	}
	// Removing a declaration from the explicit catalog cannot hide a captured
	// source-layer value, even when the captured bytes/checksums are intact.
	inventory.Projects[0].Statuses = inventory.Projects[0].Statuses[:len(inventory.Projects[0].Statuses)-1]
	if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 {
		t.Fatal("incomplete explicit catalog admitted")
	}
}
