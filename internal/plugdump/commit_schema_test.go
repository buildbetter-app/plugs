package plugdump

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
)

func commitPrefixTable(lane string, version int) Table {
	rows := []Row{}
	for i := 1; i <= version; i++ {
		rows = append(rows, Row{"version": i, "content_hash": migrationHashes[lane][i]})
	}
	return Table{Columns: []string{"version", "content_hash"}, Rows: rows}
}

func TestCommitSchemaProfilesBindPhysicalAbsenceWithoutHeadProjection(t *testing.T) {
	for _, version := range []int{0, 1, 31, 39, 40, 41, 43, 54, 55, 61, 62, 66, 72} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			tables := map[string]Table{
				"schema_migrations": commitPrefixTable("main", version),
				"dependencies":      {Columns: []string{"issue_id", "depends_on_id", "legacy_extra"}, Rows: []Row{{"issue_id": "public-a", "depends_on_id": nil, "legacy_extra": "old cell"}}},
			}
			before := mustJSON(t, tables)
			p, err := ProfileNativeCommit(tables)
			if err != nil || p.MainVersion == nil || *p.MainVersion != uint64(version) || p.IgnoredVersion != nil {
				t.Fatalf("own cursor lost: %+v %v", p, err)
			}
			if p.Format != NativeCommitSchemaFormat || p.Scope != "immutable-commit-root" {
				t.Fatal("wrong schema scope")
			}
			sawMissing, sawOld := false, false
			for _, a := range p.Absences {
				if a.Family == "provenance_events" && a.Column == "" {
					sawMissing = true
				}
				if a.Family == "dependencies" && a.Column == "depends_on_id" {
					sawOld = true
				}
				if a.Reason != AbsentBySourceSchema {
					t.Fatal("absence became a gap")
				}
			}
			if !sawMissing || sawOld || !bytes.Equal(before, mustJSON(t, tables)) {
				t.Fatal("physical shape/cells changed")
			}
		})
	}
}

func TestCommitSchemaUnknownAndUnauthenticatedPrefixesRefuse(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]Table)
	}{
		{"missing-main", func(s map[string]Table) { delete(s, "schema_migrations") }},
		{"newer-main", func(s map[string]Table) { s["schema_migrations"] = commitPrefixTable("main", 73) }},
		{"bad-main-hash", func(s map[string]Table) {
			s["schema_migrations"].Rows[0]["content_hash"] = "public-not-the-pinned-hash"
		}},
		{"missing-main-step", func(s map[string]Table) { x := s["schema_migrations"]; x.Rows = x.Rows[1:]; s["schema_migrations"] = x }},
		{"newer-ignored", func(s map[string]Table) { s["ignored_schema_migrations"] = commitPrefixTable("ignored", 29) }},
		{"bad-ignored-hash", func(s map[string]Table) { s["ignored_schema_migrations"].Rows[0]["content_hash"] = "bad" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := map[string]Table{"schema_migrations": commitPrefixTable("main", 66), "ignored_schema_migrations": commitPrefixTable("ignored", 26)}
			test.mutate(s)
			if _, err := ProfileNativeCommit(s); err == nil {
				t.Fatal("unsupported/unauthenticated cursor accepted")
			}
		})
	}
	empty, err := ProfileNativeCommit(map[string]Table{})
	if err != nil || empty.MainVersion != nil || empty.IgnoredVersion != nil || len(empty.Absences) != len(tableColumns) {
		t.Fatal("empty successful root not explicit")
	}
	if _, err := ProfileNativeCommit(nil); err == nil {
		t.Fatal("missing read became empty genesis")
	}
}

func TestCommitSchemaCursorAbsentInitialRootsRemainNullAndExact(t *testing.T) {
	ignore := Table{Columns: []string{"ignored", "pattern"}, Rows: []Row{{"ignored": 1, "pattern": "__temp__%"}}}
	tables := map[string]Table{"dolt_ignore": ignore}
	before := mustJSON(t, tables)
	profile, err := ProfileNativeCommit(tables)
	if err != nil || profile.MainVersion != nil || profile.IgnoredVersion != nil || len(profile.Absences) != len(tableColumns) || !bytes.Equal(before, mustJSON(t, tables)) {
		t.Fatal("authenticated cursor-absent initial root changed or acquired a version")
	}
	for _, test := range []struct {
		name   string
		tables map[string]Table
	}{
		{"beads-table", map[string]Table{"issues": {Columns: []string{"id"}, Rows: []Row{}}}},
		{"extra-table", map[string]Table{"dolt_ignore": ignore, "public_extra": {Columns: []string{"id"}, Rows: []Row{}}}},
		{"ignored-cursor", map[string]Table{"dolt_ignore": ignore, "ignored_schema_migrations": commitPrefixTable("ignored", 0)}},
		{"reshaped-ignore", map[string]Table{"dolt_ignore": {Columns: []string{"pattern", "public_extra"}, Rows: []Row{}}}},
		{"duplicate-ignore-column", map[string]Table{"dolt_ignore": {Columns: []string{"pattern", "pattern"}, Rows: []Row{}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ProfileNativeCommit(test.tables)
			var unsupported ErrUnsupportedNativeCommitSchema
			if !errors.As(err, &unsupported) || unsupported.MainVersion != nil {
				t.Fatal("noninitial cursor-absent root accepted or assigned a version")
			}
		})
	}
}

type schemaHistoryFixture struct{ nativeHistoryFixture }

func (schemaHistoryFixture) ProfileNativeSourceImage(image map[string]Table) (NativeCommitSchemaProfile, error) {
	return ProfileNativeCommit(image)
}

func TestCommitSchemaArchiveRejectsMixedForgedAndUnsupportedImages(t *testing.T) {
	s, inventory := nativeHistoryFixtureForTest(t)
	source := schemaHistoryFixture{s}
	data, err := Build(context.Background(), source, inventory)
	if err != nil {
		t.Fatal(err)
	}
	manifest, native := nativeCapture(t, data)
	for _, image := range native.Images {
		if image.SchemaProfile == nil {
			t.Fatal("physical annotation omitted")
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*NativeHistory)
	}{
		{"mixed", func(h *NativeHistory) { h.Images[0].SchemaProfile = nil }},
		{"wrong-version", func(h *NativeHistory) { v := uint64(1); h.Images[0].SchemaProfile.MainVersion = &v }},
		{"omitted-absence", func(h *NativeHistory) { h.Images[0].SchemaProfile.Absences = h.Images[0].SchemaProfile.Absences[1:] }},
		{"forged-empty", func(h *NativeHistory) { h.Images[0].SchemaProfile.MainVersion = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			members := archiveMembers(t, data)
			var changed NativeHistory
			if err := decodeJSON(members[NativeHistoryMember], &changed); err != nil {
				t.Fatal(err)
			}
			test.mutate(&changed)
			members[NativeHistoryMember] = mustJSON(t, changed)
			if err := validateNativeHistory(manifest, members); err == nil {
				t.Fatal("forged per-commit profile accepted")
			}
		})
	}
	oldest := native.Commits[0].ID
	source.images[oldest]["schema_migrations"] = commitPrefixTable("main", 73)
	_, err = Build(context.Background(), source, inventory)
	var failure *NativeCommitFailure
	var unsupported ErrUnsupportedNativeCommitSchema
	if !errors.As(err, &failure) || !errors.As(err, &unsupported) || failure.SchemaVersion == nil || *failure.SchemaVersion != 73 || failure.OrdinalFromHead != uint64(len(native.Commits)) {
		t.Fatalf("typed unknown historical schema/ordinal lost: %T", err)
	}
}
