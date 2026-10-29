package plugdump

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
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
