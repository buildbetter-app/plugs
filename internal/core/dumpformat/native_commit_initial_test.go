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

func TestNativeCommitInitialRootRegistryBindsCorrectedProducerContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/commit-schema-registry-c8ab0ce7.json")
	if err != nil || digest(raw) != "900320651fedf60f29686f928c4febba7513f0cb802c8f85399526d058b3aa44" {
		t.Fatal("corrected frozen producer registry bytes differ", err)
	}
	var registry struct {
		Surfaces    map[string][]string       `json:"comparison_surfaces"`
		Hashes      map[string]map[int]string `json:"pinned_prefix_hashes"`
		InitialRoot string                    `json:"cursor_absent_initial_root"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	census := map[string][]string{}
	for family, columns := range nativeCommitSchemaSurfaces() {
		census[family] = strings.Fields(columns)
		sort.Strings(census[family])
	}
	if registry.InitialRoot == "" || !reflect.DeepEqual(registry.Surfaces, census) || !reflect.DeepEqual(registry.Hashes, migrationHashes) {
		t.Fatal("initial-root correction changed the pinned comparison census or migration prefixes")
	}
}

// Declared synthetic controls for the corrected initial-root admission rule.
// They are not an original multi-version capture or historical schema proof.
func nativeCommitInitialFixture(t *testing.T, tables map[string]NativeTable) []byte {
	t.Helper()
	return editNativeFixture(t, nativeCommitAnnotatedFixture(t, "pl"), func(h *NativeHistory) {
		image := &h.Images[0]
		image.Tables = tables
		image.SchemaProfile = &NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, Absences: nativeCommitAbsences(tables, nativeCommitSchemaSurfaces())}
	})
}

func TestNativeCommitNullCursorsPreserveAuthenticatedInitialRoots(t *testing.T) {
	for _, test := range []struct {
		name   string
		tables map[string]NativeTable
	}{
		{"empty", map[string]NativeTable{}},
		{"empty-ignore-catalog", map[string]NativeTable{"dolt_ignore": {Columns: []string{"ignored", "pattern"}, Rows: []Row{}}}},
		{"populated-ignore-catalog", map[string]NativeTable{"dolt_ignore": {Columns: []string{"ignored", "pattern"}, Rows: []Row{{"ignored": json.Number("1"), "pattern": "public_%"}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := nativeCommitInitialFixture(t, test.tables)
			a := readMergeArchive(t, input)
			h := a.NativeSourceHistories()[0]
			image := h.Images[0]
			if image.SchemaProfile.MainVersion != nil || image.SchemaProfile.IgnoredVersion != nil || !reflect.DeepEqual(image.Tables, test.tables) || !bytes.Equal(a.TarBytes(), input) || !h.Complete || len(h.Gaps) != 0 || *h.Images[1].SchemaProfile.MainVersion != 66 {
				t.Fatal("initial root was assigned version0/head defaults, changed or made a history gap")
			}
		})
	}
}

func TestNativeCommitNullCursorsRefuseNonInitialRoots(t *testing.T) {
	ignore := NativeTable{Columns: []string{"ignored", "pattern"}, Rows: []Row{}}
	for _, test := range []struct {
		name   string
		tables map[string]NativeTable
	}{
		{"beads-table", map[string]NativeTable{"issues": {Columns: []string{"id"}, Rows: []Row{}}}},
		{"other-table", map[string]NativeTable{"old_table": {Columns: []string{"id"}, Rows: []Row{}}}},
		{"ignore-with-beads-table", map[string]NativeTable{"dolt_ignore": ignore, "issues": {Columns: []string{"id"}, Rows: []Row{}}}},
		{"ignore-with-ignored-cursor", map[string]NativeTable{"dolt_ignore": ignore, "ignored_schema_migrations": {Columns: []string{"content_hash", "version"}, Rows: []Row{}}}},
		{"ignore-with-main-cursor", map[string]NativeTable{"dolt_ignore": ignore, "schema_migrations": {Columns: []string{"content_hash", "version"}, Rows: []Row{}}}},
		{"ignore-extra-column", map[string]NativeTable{"dolt_ignore": {Columns: []string{"extra", "ignored", "pattern"}, Rows: []Row{}}}},
		{"ignore-missing-column", map[string]NativeTable{"dolt_ignore": {Columns: []string{"pattern"}, Rows: []Row{}}}},
		{"ignore-renamed-column", map[string]NativeTable{"dolt_ignore": {Columns: []string{"ignored", "renamed"}, Rows: []Row{}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := nativeCommitInitialFixture(t, test.tables)
			_, err := Read(bytes.NewReader(input))
			var refusal NativeCommitSchemaError
			var gap IncompleteHistoryError
			if !errors.As(err, &refusal) || errors.As(err, &gap) || refusal.OrdinalFromHead != 2 || refusal.MainVersion != nil || refusal.IgnoredVersion != nil {
				t.Fatal("cursorless non-initial root must remain a typed schema refusal", err)
			}
		})
	}
	input := editNativeFixture(t, nativeCommitInitialFixture(t, map[string]NativeTable{"dolt_ignore": ignore}), func(h *NativeHistory) {
		zero := uint64(0)
		h.Images[0].SchemaProfile.IgnoredVersion = &zero
	})
	_, err := Read(bytes.NewReader(input))
	var refusal NativeCommitSchemaError
	if !errors.As(err, &refusal) || refusal.OrdinalFromHead != 2 || refusal.MainVersion != nil || refusal.IgnoredVersion == nil || *refusal.IgnoredVersion != 0 {
		t.Fatal("initial root must not acquire an inferred ignored cursor", err)
	}
}
