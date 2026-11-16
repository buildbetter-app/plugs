package dumpformat

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestMergeDeterministicAndPreservesEvidence(t *testing.T) {
	a, b := newMergeFixture(t, "alpha"), newMergeFixture(t, "beta")
	// Mix available and unavailable history without changing either claim.
	b.manifest.History = "unavailable"
	var withoutHistory []Record
	for _, record := range b.records["beads-archive"] {
		if record.Source.Table != "issue_versions" {
			withoutHistory = append(withoutHistory, record)
		}
	}
	b.records["beads-archive"] = withoutHistory
	// A shared, identical external input is retained once; distinct source
	// paths and their precedence are preserved even when families are shared.
	b.config[0] = a.config[0]
	withMtimes := func(rows int) func(*Manifest, map[string][]byte) {
		return func(m *Manifest, _ map[string][]byte) {
			for i := range m.Sources {
				if m.Sources[i].Family == "repo_mtimes" {
					m.Sources[i].Rows = rows
				}
			}
		}
	}
	left, right := a.bytes(t, withMtimes(3)), b.bytes(t, withMtimes(4))
	inputs := []*Archive{readMergeArchive(t, left), readMergeArchive(t, right)}
	before := []Manifest{inputs[0].Manifest(), inputs[1].Manifest()}
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	for _, readers := range [][]io.Reader{
		{bytes.NewReader(right), bytes.NewReader(left)},
		{bytes.NewReader(left), bytes.NewReader(right)},
	} {
		again, err := Merge(readers...)
		if err != nil || !bytes.Equal(merged, again) {
			t.Fatalf("merge is not byte deterministic: %v", err)
		}
	}
	fromArchives, err := MergeArchives(inputs...)
	if err != nil || !bytes.Equal(fromArchives, merged) {
		t.Fatalf("validated-archive path differs: %v", err)
	}
	output := readMergeArchive(t, merged)
	m := output.Manifest()
	if m.History != "available-unverified" || !strings.HasPrefix(m.StoreID, "merge-") || m.Machine != "offline-merge" {
		t.Fatalf("merged provenance: %#v", m)
	}
	if !reflect.DeepEqual(m.Projects, append(before[0].Projects, before[1].Projects...)) ||
		!reflect.DeepEqual(m.Verification, before[0].Verification) || !reflect.DeepEqual(m.Warnings, before[0].Warnings) {
		t.Fatal("project allocators or verification disclosures changed")
	}
	for _, kind := range knownKindNames() {
		want := map[string]Record{}
		for _, input := range inputs {
			for _, record := range input.Records(kind) {
				want[recordIdentity(kind, record)] = record
			}
		}
		got := output.Records(kind)
		if len(got) != len(want) {
			t.Fatalf("%s record count=%d want=%d", kind, len(got), len(want))
		}
		for _, record := range got {
			if !equalJSON(record, want[recordIdentity(kind, record)]) {
				t.Fatalf("changed ID/body/history/digest/secret reference: %s %s", kind, record.ID)
			}
		}
	}
	for family, want := range map[string]int{"issues": 6, "labels": 6, "comments": 2, "dependencies": 2, "schema_migrations": 72, "ignored_schema_migrations": 28, "issue_versions": 1, "repo_mtimes": 7} {
		found := false
		for _, source := range m.Sources {
			if source.Family == family {
				found = true
				if source.Rows != want {
					t.Fatalf("%s census=%d want=%d", family, source.Rows, want)
				}
			}
		}
		if !found {
			t.Fatalf("missing census family %s", family)
		}
	}
	wantEntries := map[string]externalEntry{}
	for _, fixture := range []*mergeFixture{a, b} {
		for _, entry := range fixture.config {
			wantEntries[entry.Input.Family+"\x00"+entry.Input.Path] = entry
		}
	}
	config, _ := output.Member("config/sources.json")
	var gotEntries []externalEntry
	if err := decodeJSON(config, &gotEntries); err != nil || len(gotEntries) != len(wantEntries) {
		t.Fatalf("configuration provenance lost: %v", err)
	}
	for _, entry := range gotEntries {
		if !equalJSON(entry, wantEntries[entry.Input.Family+"\x00"+entry.Input.Path]) {
			t.Fatal("configuration source, value, secret reference or precedence changed")
		}
	}
	for i, input := range inputs {
		if !reflect.DeepEqual(input.Manifest(), before[i]) {
			t.Fatal("input manifest mutated")
		}
		for path := range before[i].Files {
			original, _ := readMergeArchive(t, [][]byte{left, right}[i]).Member(path)
			retained, _ := input.Member(path)
			if !bytes.Equal(original, retained) {
				t.Fatalf("input member mutated: %s", path)
			}
		}
	}
}

func TestMergeRefusesPrefixAndGlobalIdentityConflictsAtomically(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		change func(*mergeFixture, *mergeFixture)
	}{
		{name: "duplicate prefix", prefix: "alpha"},
		{name: "ambiguous prefix", prefix: "alpha-child"},
		{name: "global config", prefix: "beta", change: func(a, b *mergeFixture) {
			a.addConfig("config", "common.setting", "first", "public")
			b.addConfig("config", "common.setting", "second", "public")
		}},
		{name: "external config", prefix: "beta", change: func(a, b *mergeFixture) {
			for i := range b.config {
				if b.config[i].Input.Family == "project-config" {
					b.config[i].Input.Path = "/synthetic/alpha/project-config"
				}
			}
		}},
		{name: "comment identity", prefix: "beta", change: func(a, b *mergeFixture) {
			b.records["comment"][0].ID = a.records["comment"][0].ID
			b.records["comment"][0].Source.Fields["id"] = a.records["comment"][0].ID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := newMergeFixture(t, "alpha"), newMergeFixture(t, tc.prefix)
			if tc.change != nil {
				tc.change(a, b)
			}
			left, right := a.bytes(t, nil), b.bytes(t, nil)
			readMergeArchive(t, left)
			readMergeArchive(t, right) // Each input is independently valid.
			assertMergeRefuses(t, bytes.NewReader(left), bytes.NewReader(right))
			assertMergeRefuses(t, bytes.NewReader(right), bytes.NewReader(left))
		})
	}
	archive := newMergeFixture(t, "alpha").bytes(t, nil)
	assertMergeRefuses(t, bytes.NewReader(archive), bytes.NewReader(archive))
}

func TestMergeRefusesUnsupportedOrTamperedInputAtomically(t *testing.T) {
	good := newMergeFixture(t, "alpha").bytes(t, nil)
	cases := map[string]func(*Manifest, map[string][]byte){
		"unknown format":       func(m *Manifest, _ map[string][]byte) { m.Format = "plug-dump/2" },
		"unknown schema":       func(m *Manifest, _ map[string][]byte) { m.Schema++ },
		"unknown main":         func(m *Manifest, _ map[string][]byte) { m.MainVersion++ },
		"unknown ignored":      func(m *Manifest, _ map[string][]byte) { m.IgnoredVersion++ },
		"unknown kind":         func(m *Manifest, _ map[string][]byte) { m.Kinds[0].Name = "unknown-kind" },
		"unknown kind version": func(m *Manifest, _ map[string][]byte) { m.Kinds[0].Version++ },
		"wrong scope":          func(m *Manifest, _ map[string][]byte) { m.Scope = "full-history" },
		"false verification":   func(m *Manifest, _ map[string][]byte) { m.Verification["a3"] = "pass" },
		"bad checksum":         func(_ *Manifest, files map[string][]byte) { files["config/sources.json"][0] = '!' },
		"incompatible disposition": func(m *Manifest, _ map[string][]byte) {
			m.Sources[0].Reason = "different schema interpretation"
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			bad := newMergeFixture(t, "beta").bytes(t, change)
			assertMergeRefuses(t, bytes.NewReader(good), bytes.NewReader(bad))
		})
	}
	assertMergeRefuses(t, bytes.NewReader(good), bytes.NewReader(good[:len(good)-1]))
	assertMergeRefuses(t, bytes.NewReader(good), bytes.NewReader([]byte("malformed")))
	assertMergeRefuses(t, bytes.NewReader(good), failingMergeReader{})
}

func TestMergeRefusesUnmergedSourceDeclarations(t *testing.T) {
	good := newMergeFixture(t, "alpha").bytes(t, nil)
	for _, family := range []string{"dolt-conflicts", "dolt-working-set", "dolt-remotes"} {
		t.Run(family, func(t *testing.T) {
			fixture := newMergeFixture(t, "beta")
			for i := range fixture.config {
				if fixture.config[i].Input.Family == family {
					fixture.config[i].Input.State = "json-config"
					fixture.config[i].Values = map[string]any{"unmerged": true}
				}
			}
			bad := fixture.bytes(t, nil)
			readMergeArchive(t, bad) // Merger adds refusal beyond wire validation.
			assertMergeRefuses(t, bytes.NewReader(good), bytes.NewReader(bad))
		})
	}
}

func TestMergeRefusesConflictingMigrationEvidence(t *testing.T) {
	a, b := newMergeFixture(t, "alpha"), newMergeFixture(t, "beta")
	for i, record := range b.records["beads-archive"] {
		if record.Source.Table == "ignored_schema_migrations" {
			b.records["beads-archive"] = append(b.records["beads-archive"][:i], b.records["beads-archive"][i+1:]...)
			record.Source.Fields["applied_at"] = "2026-02-01T00:00:00Z"
			b.addArchive(t, record.Source.Table, record.Source.Fields)
			break
		}
	}
	left, right := a.bytes(t, nil), b.bytes(t, nil)
	readMergeArchive(t, left)
	readMergeArchive(t, right)
	assertMergeRefuses(t, bytes.NewReader(left), bytes.NewReader(right))
}

func TestMergeEmptyNilAndSingleInput(t *testing.T) {
	assertMergeRefuses(t)
	assertMergeRefuses(t, nil)
	for _, inputs := range [][]*Archive{nil, {nil}, {new(Archive)}} {
		data, err := MergeArchives(inputs...)
		if err == nil || data != nil {
			t.Fatalf("accepted invalid archives: %v", err)
		}
	}
	fixture := newMergeFixture(t, "alpha")
	fixture.manifest.Projects[0].IDMode, fixture.manifest.Projects[0].HashLength = "hash", 1
	data := fixture.bytes(t, nil)
	merged, err := Merge(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readMergeArchive(t, merged).Manifest().Projects, fixture.manifest.Projects) {
		t.Fatal("hash allocator changed")
	}
}

func readMergeArchive(t *testing.T, data []byte) *Archive {
	t.Helper()
	archive, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return archive
}

func assertMergeRefuses(t *testing.T, readers ...io.Reader) {
	t.Helper()
	data, err := Merge(readers...)
	if err == nil || data != nil {
		t.Fatalf("refusal must return an error and nil bytes; got %d bytes, %v", len(data), err)
	}
}

type failingMergeReader struct{}

func (failingMergeReader) Read([]byte) (int, error) { return 0, errors.New("synthetic read failure") }

func TestMergeArchivedSQLObservations(t *testing.T) {
	families := []string{"dolt-conflicts", "dolt-working-set", "dolt-remotes"}
	queries := []string{"SELECT * FROM dolt_conflicts", "SELECT table_name, staged, status FROM dolt_status", "SELECT * FROM dolt_remotes"}
	observation := func(f *mergeFixture, family, query string, table any) {
		for i := range f.config {
			if f.config[i].Input.Family == family {
				f.config[i].Input.State = "json-config"
				f.config[i].Values = map[string]any{"query": query, "table": table}
			}
		}
	}
	for _, nullRows := range []bool{false, true} {
		t.Run(fmt.Sprintf("clean-null-%v", nullRows), func(t *testing.T) {
			a, b := newMergeFixture(t, "alpha"), newMergeFixture(t, "beta")
			for _, f := range []*mergeFixture{a, b} {
				for i, family := range families {
					var rows any = []any{}
					if nullRows {
						rows = nil
					}
					observation(f, family, queries[i], map[string]any{"Columns": []string{"name"}, "Rows": rows})
				}
			}
			merged, err := Merge(bytes.NewReader(a.bytes(t, nil)), bytes.NewReader(b.bytes(t, nil)))
			if err != nil {
				t.Fatal(err)
			}
			output := readMergeArchive(t, merged)
			if len(output.Manifest().Projects) != 2 {
				t.Fatal("two projects not retained")
			}
			for _, source := range output.Manifest().Sources {
				for _, family := range families {
					if source.Family == family && source.Rows != 2 {
						t.Fatalf("configuration key census changed: %#v", source)
					}
				}
			}
		})
	}
	for i, family := range families {
		for name, table := range map[string]any{
			"dirty":             map[string]any{"Columns": []string{"name"}, "Rows": []any{map[string]any{"name": "dirty"}}},
			"missing-table":     nil,
			"missing-rows":      map[string]any{"Columns": []string{"name"}},
			"wrong-rows":        map[string]any{"Columns": []string{"name"}, "Rows": map[string]any{}},
			"ambiguous-rows":    map[string]any{"Columns": []string{"name"}, "Rows": []any{}, "rows": []any{}},
			"duplicate-columns": map[string]any{"Columns": []string{"name", "name"}, "Rows": []any{}},
		} {
			t.Run(family+"/"+name, func(t *testing.T) {
				f := newMergeFixture(t, "alpha")
				observation(f, family, queries[i], table)
				raw := f.bytes(t, nil)
				readMergeArchive(t, raw)
				assertMergeRefuses(t, bytes.NewReader(raw))
			})
		}
	}
}
