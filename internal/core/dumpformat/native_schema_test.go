package dumpformat

import (
	"bytes"
	"errors"
	"reflect"
	"sort"
	"testing"
)

// This is an explicitly constructed synthetic migration-prefix fixture, not
// an archive from a Beads process. Existing old-image values remain literal.
func native66Fixture(t *testing.T, project string) []byte {
	t.Helper()
	members := sourcePlaneReaderMembers(t, nativeSyntheticFixture(t, project))
	var m Manifest
	var h NativeHistory
	if err := decodeJSON(members["manifest.json"], &m); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(members[NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	p, err := NativeSchemaForVersion(66, 26)
	if err != nil {
		t.Fatal(err)
	}
	h.SchemaProfile = &p
	m.MainVersion, m.IgnoredVersion = 66, 26
	transform := func(name string, row Row) bool {
		for _, absence := range p.Absences {
			if absence.Family != name {
				continue
			}
			if absence.Column == "" {
				return false
			}
			delete(row, absence.Column)
		}
		if name == "schema_migrations" || name == "ignored_schema_migrations" {
			if _, present := row["version"]; !present {
				return true
			}
			version, err := natural(row["version"])
			if err != nil {
				t.Fatal(err)
			}
			limit := uint64(66)
			if name == "ignored_schema_migrations" {
				limit = 26
			}
			return version <= limit
		}
		return true
	}
	for _, tables := range append([]map[string]NativeTable{h.Current}, nativeImageTables(h.Images)...) {
		for name, table := range tables {
			if !transform(name, Row{}) {
				delete(tables, name)
				continue
			}
			columns := []string{}
			for _, column := range table.Columns {
				keep := true
				for _, absence := range p.Absences {
					if absence.Family == name && absence.Column == column {
						keep = false
					}
				}
				if keep {
					columns = append(columns, column)
				}
			}
			rows := []Row{}
			for _, row := range table.Rows {
				if transform(name, row) {
					rows = append(rows, row)
				}
			}
			tables[name] = NativeTable{Columns: columns, Rows: rows}
		}
	}
	for i := range m.Kinds {
		kind := &m.Kinds[i]
		rows := []Record{}
		for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var row Record
			if err := decodeJSON(line, &row); err != nil {
				t.Fatal(err)
			}
			if !transform(row.Source.Table, row.Source.Fields) {
				continue
			}
			if row.Kind == "beads-archive" {
				row.ID = row.Source.Table + "/" + digest(fixtureJSON(t, row.Source.Fields))
			}
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool { return compareSourceRecords(rows[i], rows[j], m.SourcePlaneAddressContract) < 0 })
		members[kind.File] = []byte{}
		for _, row := range rows {
			members[kind.File] = append(members[kind.File], fixtureJSON(t, row)...)
		}
		kind.Count = len(rows)
	}
	sources := []Disposition{}
	for _, source := range m.Sources {
		if !transform(source.Family, Row{}) {
			continue
		}
		if table, ok := h.Current[source.Family]; ok {
			source.Rows = len(table.Rows)
		}
		sources = append(sources, source)
	}
	m.Sources = sources
	members["manifest.json"], members[NativeHistoryMember] = fixtureJSON(t, m), fixtureJSON(t, h)
	return nativeFixtureMembers(t, members)
}

func nativeImageTables(images []NativeHistoryImage) []map[string]NativeTable {
	result := []map[string]NativeTable{}
	for _, image := range images {
		result = append(result, image.Tables)
	}
	return result
}

func TestNativeSchema66ExactAbsencesAndSameProfileMerge(t *testing.T) {
	left, right := native66Fixture(t, "pl"), native66Fixture(t, "qa")
	a := readMergeArchive(t, left)
	h := a.NativeSourceHistories()[0]
	want := []NativeSchemaAbsence{
		{Family: "epoch_minted_addresses", IntroducedIn: 72, Reason: AbsentBySourceSchema},
		{Family: "issue_versions", IntroducedIn: 67, Reason: AbsentBySourceSchema},
		{Family: DurableEntityTable, Column: "current_revision", IntroducedIn: 67, Reason: AbsentBySourceSchema},
		{Family: DurableEntityTable, Column: "participation_generation", IntroducedIn: 70, Reason: AbsentBySourceSchema},
		{Family: "store_epoch", IntroducedIn: 67, Reason: AbsentBySourceSchema},
		{Family: EphemeralEntityTable, Column: "current_revision", IntroducedIn: 67, Reason: AbsentBySourceSchema},
		{Family: EphemeralEntityTable, Column: "participation_generation", IntroducedIn: 70, Reason: AbsentBySourceSchema},
	}
	if h.SchemaProfile == nil || !reflect.DeepEqual(h.SchemaProfile.Absences, want) || len(h.Current) != 30 || !h.Complete || len(h.Gaps) != 0 {
		t.Fatal("source schema absence census changed")
	}
	for _, row := range h.Current[DurableEntityTable].Rows {
		if _, present := row["participation_generation"]; present {
			t.Fatal("source absence replaced with a cell")
		}
		if _, present := row["current_revision"]; present {
			t.Fatal("source absence replaced with a cell")
		}
	}
	merged, err := Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	joined := readMergeArchive(t, merged)
	for _, input := range []*Archive{a, readMergeArchive(t, right)} {
		original, _ := input.Member(NativeHistoryMember)
		retained, _ := joined.Member(nativeHistoryPath(input.Manifest().Projects[0].Prefix))
		if !bytes.Equal(original, retained) {
			t.Fatal("same-profile merge changed original source bytes")
		}
	}
	if len(joined.NativeSourceHistories()) != 2 || a.Manifest().MainVersion != 66 || a.Manifest().IgnoredVersion != 26 {
		t.Fatal("source profile changed")
	}
	if data, err := Merge(bytes.NewReader(left), bytes.NewReader(nativeSyntheticFixture(t, "zz"))); err == nil || data != nil {
		t.Fatal("mixed schema profiles accepted")
	}
}

func TestNativeSchema66RefusesContradictedFactsAndKeepsHistoryGaps(t *testing.T) {
	baseline := native66Fixture(t, "pl")
	for _, test := range []struct {
		name string
		edit func(*NativeHistory)
	}{
		{"missing-profile", func(h *NativeHistory) { h.SchemaProfile = nil }},
		{"unsupported-pair", func(h *NativeHistory) { h.SchemaProfile.IgnoredVersion = 27 }},
		{"missing-fact", func(h *NativeHistory) { h.SchemaProfile.Absences = h.SchemaProfile.Absences[1:] }},
		{"invented-fact", func(h *NativeHistory) { h.SchemaProfile.Absences[0].Family = "config" }},
		{"wrong-introduced-version", func(h *NativeHistory) { h.SchemaProfile.Absences[0].IntroducedIn = 67 }},
		{"wrong-reason", func(h *NativeHistory) { h.SchemaProfile.Absences[0].Reason = "empty" }},
		{"required-family", func(h *NativeHistory) { delete(h.Current, "config") }},
		{"required-column", func(h *NativeHistory) {
			table := h.Current[DurableEntityTable]
			table.Columns = table.Columns[1:]
			h.Current[DurableEntityTable] = table
		}},
		{"invented-family", func(h *NativeHistory) { h.Current["issue_versions"] = NativeTable{Columns: []string{}, Rows: []Row{}} }},
		{"cursor-row", func(h *NativeHistory) { h.Current["schema_migrations"].Rows[0]["version"] = "72" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Read(bytes.NewReader(editNativeFixture(t, baseline, test.edit))); err == nil {
				t.Fatal("contradictory source profile accepted")
			}
		})
	}
	members := sourcePlaneReaderMembers(t, baseline)
	var manifest Manifest
	if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.MainVersion = 65
	members["manifest.json"] = fixtureJSON(t, manifest)
	var schema NativeSchemaError
	if _, err := Read(bytes.NewReader(nativeFixtureMembers(t, members))); !errors.As(err, &schema) {
		t.Fatalf("unsupported version lacked typed refusal: %v", err)
	}
	gap := editNativeFixture(t, baseline, func(h *NativeHistory) {
		h.Complete = false
		h.Gaps = []NativeHistoryGap{{Sequence: 1, CommitID: h.Commits[0].ID, Family: "events", Reason: "retained-source-image-not-readable"}}
		h.Images = h.Images[1:]
	})
	var incomplete IncompleteHistoryError
	if data, err := MergeArchives(readMergeArchive(t, gap)); data != nil || !errors.As(err, &incomplete) || incomplete.First != 1 || incomplete.Family != "events" {
		t.Fatalf("schema absence erased named history gap: %v", err)
	}
}
