package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestNativeCommitSchemaProfileStrictNullableWire(t *testing.T) {
	// These exercise wire shape only. A cursor's presence and supported prefix
	// must still be authenticated against that image's physical tables.
	for _, test := range []struct {
		name, main, ignored string
	}{
		{"observed-pair", "66", "26"},
		{"absent-ignored-table", "39", "null"},
		{"empty-root-shape", "null", "null"},
		{"distinct-zero-shape", "0", "0"},
		{"full-uint64-shape", "18446744073709551615", "9007199254740993"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := `{"format":"beads-commit-schema/1","scope":"immutable-commit-root","main_version":` + test.main + `,"ignored_version":` + test.ignored + `,"absences":[]}`
			var got NativeCommitSchemaProfile
			if err := decodeJSON([]byte(input), &got); err != nil {
				t.Fatal(err)
			}
			output, err := json.Marshal(got)
			if err != nil || string(output) != input || got.Absences == nil {
				t.Fatalf("nullable cursor wire changed: %s, %v", output, err)
			}
		})
	}
}

func TestNativeCommitSchemaProfileRefusesMalformedWire(t *testing.T) {
	const baseline = `{"format":"beads-commit-schema/1","scope":"immutable-commit-root","main_version":39,"ignored_version":null,"absences":[{"family":"events","column":"old_value","reason":"absent-by-source-schema"}]}`
	for _, test := range []struct {
		name, from, to string
	}{
		{"missing-main", `"main_version":39,`, ""},
		{"missing-ignored", `"ignored_version":null,`, ""},
		{"case-main", `"main_version"`, `"Main_version"`},
		{"duplicate-main", `"main_version":39`, `"main_version":39,"main_version":39`},
		{"string-main", `"main_version":39`, `"main_version":"39"`},
		{"negative-main", `"main_version":39`, `"main_version":-1`},
		{"fraction-main", `"main_version":39`, `"main_version":39.0`},
		{"exponent-main", `"main_version":39`, `"main_version":3.9e1`},
		{"overflow-main", `"main_version":39`, `"main_version":18446744073709551616`},
		{"boolean-main", `"main_version":39`, `"main_version":true`},
		{"object-main", `"main_version":39`, `"main_version":{}`},
		{"string-ignored", `"ignored_version":null`, `"ignored_version":"26"`},
		{"negative-ignored", `"ignored_version":null`, `"ignored_version":-1`},
		{"fraction-ignored", `"ignored_version":null`, `"ignored_version":26.0`},
		{"overflow-ignored", `"ignored_version":null`, `"ignored_version":18446744073709551616`},
		{"null-format", `"format":"beads-commit-schema/1"`, `"format":null`},
		{"null-scope", `"scope":"immutable-commit-root"`, `"scope":null`},
		{"null-absences", `"absences":[{"family":"events","column":"old_value","reason":"absent-by-source-schema"}]`, `"absences":null`},
		{"null-absence", `{"family":"events","column":"old_value","reason":"absent-by-source-schema"}`, `null`},
		{"null-column", `"column":"old_value"`, `"column":null`},
		{"empty-column", `"column":"old_value"`, `"column":""`},
		{"guessed-introduction", `"column":"old_value"`, `"column":"old_value","introduced_in":62`},
		{"unknown-field", `"scope":"immutable-commit-root"`, `"scope":"immutable-commit-root","head_version":66`},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := strings.Replace(baseline, test.from, test.to, 1)
			if input == baseline {
				t.Fatal("malformed control did not change its input")
			}
			var got NativeCommitSchemaProfile
			if err := decodeJSON([]byte(input), &got); err == nil {
				t.Fatal("malformed per-image schema accepted")
			}
		})
	}
	for _, input := range []string{"null", "[]", baseline + " {}"} {
		var got NativeCommitSchemaProfile
		if err := decodeJSON([]byte(input), &got); err == nil {
			t.Fatal("malformed per-image schema container accepted")
		}
	}
}

func TestNativeCommitSchemaImageWireRetainsOptionalAnnotation(t *testing.T) {
	const legacy = `{"sequence":1,"commit_id":"original-root","tables":{}}`
	const profile = `{"format":"beads-commit-schema/1","scope":"immutable-commit-root","main_version":39,"ignored_version":null,"absences":[{"family":"events","reason":"absent-by-source-schema"}]}`
	for _, input := range []string{legacy, strings.TrimSuffix(legacy, "}") + `,"schema_profile":` + profile + `}`} {
		var got NativeHistoryImage
		if err := decodeJSON([]byte(input), &got); err != nil {
			t.Fatal(err)
		}
		output, err := json.Marshal(got)
		if err != nil || !bytes.Equal(output, []byte(input)) {
			t.Fatalf("image wire changed: %s, %v", output, err)
		}
		var detached NativeHistoryImage
		if err := decodeJSON(output, &detached); err != nil || !reflect.DeepEqual(got, detached) {
			t.Fatal("image annotation was lost in detached JSON copy", err)
		}
		if input == legacy && got.SchemaProfile != nil {
			t.Fatal("legacy image was assigned a fabricated profile")
		}
	}
	for _, member := range []string{"null", "{}", profile + `,"schema_profile":` + profile} {
		input := strings.TrimSuffix(legacy, "}") + `,"schema_profile":` + member + `}`
		var got NativeHistoryImage
		if err := decodeJSON([]byte(input), &got); err == nil {
			t.Fatal("malformed image annotation accepted")
		}
	}
}

func TestNativeCommitAbsencesObserveOnlyThePinnedSurface(t *testing.T) {
	// This miniature census is explicitly synthetic. It exercises physical
	// observations without claiming producer support for a migration ordinal.
	census := map[string]string{"events": "actor id", "issues": "id title", "new_family": "id value"}
	tables := map[string]NativeTable{
		"events":         {Columns: []string{"id", "legacy_payload"}, Rows: []Row{{"id": json.Number("1"), "legacy_payload": nil}}},
		"issues":         {Columns: []string{"id", "title"}, Rows: []Row{}},
		"retired_family": {Columns: []string{"old_value"}, Rows: []Row{{"old_value": "keep exactly"}, {"old_value": "keep exactly"}}},
	}
	before, err := json.Marshal(tables)
	if err != nil {
		t.Fatal(err)
	}
	want := []NativeCommitSchemaAbsence{
		{Family: "events", Column: "actor", Reason: AbsentBySourceSchema},
		{Family: "new_family", Reason: AbsentBySourceSchema},
	}
	if got := nativeCommitAbsences(tables, census); !reflect.DeepEqual(got, want) {
		t.Fatalf("physical absence census differs: %+v", got)
	}
	after, err := json.Marshal(tables)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("observing absences changed old columns, SQL NULL or duplicate rows", err)
	}
	delete(tables, "events")
	want[0].Column = ""
	if got := nativeCommitAbsences(tables, census); !reflect.DeepEqual(got, want) {
		t.Fatal("missing family was expanded into fabricated column facts", got)
	}
}

func TestNativeCommitCursorChecksPhysicalPrefixWithoutHeadFallback(t *testing.T) {
	const family = "schema_migrations"
	version, zero := uint64(2), uint64(0)
	hashes := map[int]string{1: "public-one", 2: "public-two"}
	good := func() map[string]NativeTable {
		return map[string]NativeTable{family: {Columns: []string{"content_hash", "version"}, Rows: []Row{
			{"content_hash": "public-two", "version": json.Number("2")},
			{"content_hash": "public-one", "version": json.Number("1")},
		}}}
	}
	if err := nativeCommitCursor(good(), family, &version, hashes); err != nil {
		t.Fatal(err)
	}
	if err := nativeCommitCursor(map[string]NativeTable{}, family, nil, hashes); err != nil {
		t.Fatal("physically absent ignored cursor shape refused", err)
	}
	for _, test := range []struct {
		name string
		edit func(map[string]NativeTable)
	}{
		{"missing-table", func(tables map[string]NativeTable) { delete(tables, family) }},
		{"missing-prefix-row", func(tables map[string]NativeTable) {
			table := tables[family]
			table.Rows = table.Rows[:1]
			tables[family] = table
		}},
		{"duplicate-version", func(tables map[string]NativeTable) { tables[family].Rows[1]["version"] = json.Number("2") }},
		{"above-cursor", func(tables map[string]NativeTable) { tables[family].Rows[1]["version"] = json.Number("3") }},
		{"wrong-hash", func(tables map[string]NativeTable) { tables[family].Rows[1]["content_hash"] = "other" }},
		{"zero-version", func(tables map[string]NativeTable) { tables[family].Rows[1]["version"] = json.Number("0") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			tables := good()
			test.edit(tables)
			if err := nativeCommitCursor(tables, family, &version, hashes); err == nil {
				t.Fatal("unauthenticated commit cursor accepted")
			}
		})
	}
	if err := nativeCommitCursor(good(), family, nil, hashes); err == nil {
		t.Fatal("physical table was treated as absent")
	}
	if err := nativeCommitCursor(map[string]NativeTable{family: {Columns: []string{"content_hash", "version"}, Rows: []Row{}}}, family, &zero, hashes); err != nil {
		t.Fatal("explicit empty migration prefix differs from absent cursor", err)
	}
}

func TestNativeCommitSchemaRegistryBindsExactProducerContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/commit-schema-registry-ff8006bf.json")
	if err != nil {
		t.Fatal(err)
	}
	if digest(raw) != "cf71387bc62853dd48aa9e7e96dfe67cf1126cdd47c920758d1d4234b80cd253" {
		t.Fatal("frozen producer registry bytes differ")
	}
	var registry struct {
		Surfaces  map[string][]string       `json:"comparison_surfaces"`
		Hashes    map[string]map[int]string `json:"pinned_prefix_hashes"`
		MainMin   int                       `json:"main_supported_min"`
		MainMax   int                       `json:"main_supported_max"`
		IgnoreMin int                       `json:"ignored_supported_min"`
		IgnoreMax int                       `json:"ignored_supported_max"`
	}
	// This is an independently hash-bound producer document, not archive wire.
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	census := map[string][]string{}
	for name, columns := range nativeCommitSchemaSurfaces() {
		census[name] = strings.Fields(columns)
		sort.Strings(census[name])
	}
	if !reflect.DeepEqual(census, registry.Surfaces) || !reflect.DeepEqual(migrationHashes, registry.Hashes) || registry.MainMin != 0 || registry.MainMax != 72 || registry.IgnoreMin != 0 || registry.IgnoreMax != 28 {
		t.Fatal("consumer schema census or supported prefixes differ from frozen producer")
	}
}

// Declared synthetic annotation control. It does not claim that a Beads
// process produced the existing old-image rows or this constructed prefix.
func nativeCommitAnnotatedFixture(t *testing.T, project string) []byte {
	t.Helper()
	return editNativeFixture(t, native66Fixture(t, project), func(h *NativeHistory) {
		old := h.Images[0].Tables
		prefix := NativeTable{Columns: append([]string{}, h.Current["schema_migrations"].Columns...), Rows: []Row{}}
		for _, row := range h.Current["schema_migrations"].Rows {
			version, err := natural(row["version"])
			if err != nil {
				t.Fatal(err)
			}
			if version <= 39 {
				prefix.Rows = append(prefix.Rows, row)
			}
		}
		old["schema_migrations"] = prefix
		for i := range h.Images {
			main := uint64(66)
			if i == 0 {
				main = 39
			}
			h.Images[i].SchemaProfile = &NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, MainVersion: &main, Absences: nativeCommitAbsences(h.Images[i].Tables, nativeCommitSchemaSurfaces())}
		}
	})
}

func TestNativeCommitSchemaProfilesRetainOwnCursorsAndOriginalBytes(t *testing.T) {
	left, right := nativeCommitAnnotatedFixture(t, "pl"), nativeCommitAnnotatedFixture(t, "qa")
	a := readMergeArchive(t, left)
	h := a.NativeSourceHistories()[0]
	if len(h.Images) != 2 || h.SchemaProfile.MainVersion != 66 || *h.Images[0].SchemaProfile.MainVersion != 39 || *h.Images[1].SchemaProfile.MainVersion != 66 || h.Images[0].SchemaProfile.IgnoredVersion != nil || h.Images[1].SchemaProfile.IgnoredVersion != nil || !h.Complete || len(h.Gaps) != 0 {
		t.Fatal("own cursor facts became head defaults or a history gap")
	}
	if !reflect.DeepEqual(h.Images[0].Tables["events"].Columns, []string{"id", "legacy_payload"}) || len(h.Images[0].Tables["retired_family"].Rows) != 2 {
		t.Fatal("old schema or duplicate physical rows changed")
	}
	*h.Images[0].SchemaProfile.MainVersion = 1
	h.Images[0].SchemaProfile.Absences[0].Family = "changed"
	again := a.NativeSourceHistories()[0]
	if *again.Images[0].SchemaProfile.MainVersion != 39 || again.Images[0].SchemaProfile.Absences[0].Family == "changed" {
		t.Fatal("detached per-image annotations alias original archive")
	}
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	joined := readMergeArchive(t, merged)
	for _, input := range []*Archive{a, readMergeArchive(t, right)} {
		original, _ := input.Member(NativeHistoryMember)
		kept, _ := joined.Member(nativeHistoryPath(input.Manifest().Projects[0].Prefix))
		if !bytes.Equal(original, kept) {
			t.Fatal("assembly rewrote original per-commit annotations")
		}
	}
	legacy := readMergeArchive(t, native66Fixture(t, "zz"))
	for _, image := range legacy.NativeSourceHistories()[0].Images {
		if image.SchemaProfile != nil {
			t.Fatal("legacy fixture was assigned invented cursor facts")
		}
	}
	// Annotation mode is per original source, not an aggregate merge identity.
	if _, err := Merge(bytes.NewReader(left), bytes.NewReader(legacy.TarBytes())); err != nil {
		t.Fatal("independent legacy source mode was collapsed into annotated mode", err)
	}
}

func TestNativeCommitSchemaProfilesRefuseContradictionsAsSchemaFacts(t *testing.T) {
	baseline := nativeCommitAnnotatedFixture(t, "pl")
	for _, test := range []struct {
		name string
		edit func(*NativeHistory)
	}{
		{"mixed-images", func(h *NativeHistory) { h.Images[0].SchemaProfile = nil }},
		{"unknown-format", func(h *NativeHistory) { h.Images[0].SchemaProfile.Format = "unknown" }},
		{"wrong-scope", func(h *NativeHistory) { h.Images[0].SchemaProfile.Scope = "working-root" }},
		{"head-fallback", func(h *NativeHistory) { *h.Images[0].SchemaProfile.MainVersion = 66 }},
		{"unsupported-main", func(h *NativeHistory) { *h.Images[0].SchemaProfile.MainVersion = 73 }},
		{"missing-main", func(h *NativeHistory) { h.Images[0].SchemaProfile.MainVersion = nil }},
		{"inferred-ignored", func(h *NativeHistory) { v := uint64(26); h.Images[0].SchemaProfile.IgnoredVersion = &v }},
		{"unsupported-ignored", func(h *NativeHistory) { v := uint64(29); h.Images[0].SchemaProfile.IgnoredVersion = &v }},
		{"wrong-prefix-hash", func(h *NativeHistory) {
			h.Images[0].Tables["schema_migrations"].Rows[0]["content_hash"] = strings.Repeat("0", 64)
		}},
		{"missing-absence", func(h *NativeHistory) { p := h.Images[0].SchemaProfile; p.Absences = p.Absences[1:] }},
		{"duplicate-absence", func(h *NativeHistory) { p := h.Images[0].SchemaProfile; p.Absences = append(p.Absences, p.Absences[0]) }},
		{"absent-present-family", func(h *NativeHistory) { h.Images[0].SchemaProfile.Absences[0].Family = "config" }},
		{"wrong-reason", func(h *NativeHistory) { h.Images[0].SchemaProfile.Absences[0].Reason = "missing-history" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := editNativeFixture(t, baseline, test.edit)
			_, err := Read(bytes.NewReader(data))
			var refusal NativeCommitSchemaError
			var gap IncompleteHistoryError
			if !errors.As(err, &refusal) || errors.As(err, &gap) || refusal.OrdinalFromHead != 2 || strings.Contains(err.Error(), strings.Repeat("a", 32)) {
				t.Fatal("contradiction did not remain a typed source-schema refusal", err)
			}
		})
	}
	empty := editNativeFixture(t, baseline, func(h *NativeHistory) {
		h.Images[0].Tables = map[string]NativeTable{}
		h.Images[0].SchemaProfile = &NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, Absences: nativeCommitAbsences(h.Images[0].Tables, nativeCommitSchemaSurfaces())}
	})
	if h := readMergeArchive(t, empty).NativeSourceHistories()[0]; h.Images[0].SchemaProfile.MainVersion != nil || len(h.Images[0].Tables) != 0 || len(h.Gaps) != 0 {
		t.Fatal("empty physical root was filled from head or marked as a gap")
	}
	zero := editNativeFixture(t, baseline, func(h *NativeHistory) {
		image := &h.Images[0]
		image.Tables["schema_migrations"] = NativeTable{Columns: []string{"content_hash", "version"}, Rows: []Row{}}
		image.Tables["ignored_schema_migrations"] = NativeTable{Columns: []string{"applied_at", "content_hash", "version"}, Rows: []Row{}}
		main, ignored := uint64(0), uint64(0)
		image.SchemaProfile.MainVersion, image.SchemaProfile.IgnoredVersion = &main, &ignored
		image.SchemaProfile.Absences = nativeCommitAbsences(image.Tables, nativeCommitSchemaSurfaces())
	})
	if p := readMergeArchive(t, zero).NativeSourceHistories()[0].Images[0].SchemaProfile; p.MainVersion == nil || *p.MainVersion != 0 || p.IgnoredVersion == nil || *p.IgnoredVersion != 0 {
		t.Fatal("independent physically present zero prefixes became absent cursors")
	}
}
