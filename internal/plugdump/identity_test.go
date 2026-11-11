package plugdump

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNamespacedHashIdentityRequiresExactInventory(t *testing.T) {
	project := Project{Prefix: "repro", IDMode: "hash", HashLength: 3, HashNamespaces: []string{"abc"}, ChildCounters: map[string]uint64{}}
	for _, id := range []string{"repro-4de", "repro-abc-4de", "repro-abc-45d", "repro-abc-456", "repro-abc-d4e", "repro-abc-de4", "repro-abc-def", "repro-abc-4de.2.03"} {
		if _, _, _, err := issueIDParts(id, project); err != nil {
			t.Errorf("supported synthetic identity %q: %v", id, err)
		}
	}
	for _, id := range []string{"other-abc-4de", "repro-def-4de", "repro-abc--4de", "repro--4de", "repro-abc-", "repro-abc-4_d", "repro-abc-4de.0", "repro-abc-4de..2", "repro-abc-4de.-2", "repro-abc-4de.x"} {
		if _, _, _, err := issueIDParts(id, project); err == nil {
			t.Errorf("malformed or undeclared synthetic identity admitted: %q", id)
		}
	}
	project.HashLength = 4
	if _, _, _, err := issueIDParts("repro-abc-4de", project); err == nil {
		t.Fatal("namespace bytes inflated the hash-length floor")
	}
	project.HashLength = 3
	project.HashNamespaces = nil
	if _, _, _, err := issueIDParts("repro-abc-4de", project); err == nil {
		t.Fatal("legacy inventory admitted an undeclared namespace")
	}
}

func TestHashNamespaceInventoryRefusals(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `["abc","abc"]`, `["def","abc"]`, `[""]`, `["abc-"]`, `["abc--def"]`, `["4abc"]`, `["` + strings.Repeat("a", 33) + `"]`} {
		t.Run(raw, func(t *testing.T) {
			var p Project
			err := decodeJSON([]byte(`{"prefix":"repro","id_mode":"hash","hash_length":3,"last_id":0,"child_counters":{},"hash_namespaces":`+raw+`}`), &p)
			if err == nil {
				_, err = copyProjects([]Project{p})
			}
			if err == nil {
				t.Fatal("invalid namespace inventory admitted")
			}
		})
	}
	for _, key := range []string{"HASH_NAMESPACES", "Hash_Namespaces", "unknown_namespaces"} {
		var p Project
		if err := decodeJSON([]byte(`{"prefix":"repro","id_mode":"hash","hash_length":3,"last_id":0,"child_counters":{},"`+key+`":["abc"]}`), &p); err == nil {
			t.Errorf("noncanonical key %q admitted", key)
		}
	}
	if _, err := copyProjects([]Project{{Prefix: "repro", IDMode: "sequential", HashNamespaces: []string{"abc"}}}); err == nil {
		t.Fatal("sequential mode admitted hash namespaces")
	}
}

func TestNativeTypeDirectivesAreExplicitAndLossless(t *testing.T) {
	project := Project{Prefix: "repro", IDMode: "hash", HashLength: 3}
	rows := []Row{{"issue_type": "task"}, {"issue_type": "zzzzzzzz"}, {"issue_type": "aaaaa"}, {"issue_type": "zzzzzzzz"}, {"issue_type": "yyyyyyyy"}, {"issue_type": "xxxxxxxxx"}}
	directives, err := nativeTypeDirectives(rows, project)
	if err != nil || len(directives) != 4 {
		t.Fatalf("explicit custom type inventory: %+v %v", directives, err)
	}
	for _, d := range directives {
		if d.Kind != "map_type" || d.Target.Project != "repro" || d.Target.Type != d.From || d.Target.ID != "" || d.To != "issue" {
			t.Fatalf("source type was relabelled: %+v", d)
		}
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	reordered, err := nativeTypeDirectives(rows, project)
	if err != nil || !reflect.DeepEqual(reordered, directives) {
		t.Fatal("native row order changed ontology identity")
	}
	for _, raw := range []any{nil, "", " zzzzzzzz", strings.Repeat("z", 65), string([]byte{0xff})} {
		if _, err := nativeTypeDirectives([]Row{{"issue_type": raw}}, project); err == nil {
			t.Error("invalid native type admitted")
		}
	}
}

func TestNamespacedHashArchivePreservesSourceAndRefusesMissingBindings(t *testing.T) {
	source, inventory := fixture(t)
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	// This changes only detached synthetic fixture identities and references.
	raw = bytes.ReplaceAll(raw, []byte("demo-7"), []byte("demo-abc-4de"))
	if err := decodeJSON(raw, &source); err != nil {
		t.Fatal(err)
	}
	inventory.Projects[0].IDMode = "hash"
	inventory.Projects[0].HashLength = 1
	inventory.Projects[0].HashNamespaces = []string{"abc"}
	inventory.Projects[0].ChildCounters = map[string]uint64{}
	for _, row := range source["issues"].Rows {
		row["issue_type"] = "zzzzzzzz"
	}
	inventory.Ontology, err = nativeTypeDirectives(source["issues"].Rows, inventory.Projects[0])
	if err != nil {
		t.Fatal(err)
	}
	archive := buildFixture(t, source, inventory)
	manifest, err := Validate(bytes.NewReader(archive))
	if err != nil || !reflect.DeepEqual(manifest.Projects[0].HashNamespaces, []string{"abc"}) || manifest.Projects[0].ChildCounters["demo-abc-4de"] != 4 {
		t.Fatalf("namespace/counter inventory changed: %+v %v", manifest.Projects, err)
	}
	var records []Record
	for _, line := range bytes.Split(bytes.TrimSpace(archiveMembers(t, archive)["records/bead.ndjson"]), []byte{'\n'}) {
		var record Record
		if err := decodeJSON(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	for _, record := range records {
		if record.ID != text(record.Source.Fields, "id") || record.Project != "demo" || record.Body["source_type"] != "zzzzzzzz" || text(record.Source.Fields, "issue_type") != "zzzzzzzz" {
			t.Fatalf("source identity or type was changed: %+v", record)
		}
	}
	inventory.Projects[0].HashNamespaces = nil
	if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 {
		t.Fatal("missing namespace binding emitted an archive")
	}
	inventory.Projects[0].HashNamespaces = []string{"abc"}
	inventory.Ontology = nil
	if data, err := Build(context.Background(), source, inventory); err == nil || len(data) != 0 || !strings.Contains(err.Error(), "workflow/ontology mapping required") {
		t.Fatal("missing explicit ontology emitted an archive")
	}
	members := archiveMembers(t, archive)
	manifest.Projects[0].HashNamespaces = nil
	members["manifest.json"] = mustJSON(t, manifest)
	if _, err := Validate(bytes.NewReader(repackCatalogRefusalArchive(t, members))); err == nil {
		t.Fatal("validator admitted an archive with removed namespace binding")
	}
}
