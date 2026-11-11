package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const NativeCommitSchemaFormat = "beads-commit-schema/1"
const NativeCommitSchemaScope = "immutable-commit-root"

// NativeCommitSchemaAbsence is an observed absence at one immutable SQL root.
// It makes no claim about the migration that introduced a table or column.
type NativeCommitSchemaAbsence struct {
	Family string `json:"family"`
	Column string `json:"column,omitempty"`
	Reason string `json:"reason"`
}

// NativeCommitSchemaProfile binds each image to its own migration cursors.
// Null cursor values are distinct from zero and never inherit from the head.
type NativeCommitSchemaProfile struct {
	Format         string                      `json:"format"`
	Scope          string                      `json:"scope"`
	MainVersion    *uint64                     `json:"main_version"`
	IgnoredVersion *uint64                     `json:"ignored_version"`
	Absences       []NativeCommitSchemaAbsence `json:"absences"`
}

// NativeCommitSchemaError exposes schema coordinates without source IDs,
// commit hashes, cell values or error-message text from the source reader.
type NativeCommitSchemaError struct {
	MainVersion, IgnoredVersion *uint64
	OrdinalFromHead             uint64
	Reason                      string
}

func (e NativeCommitSchemaError) Error() string {
	main, ignored := "null", "null"
	if e.MainVersion != nil {
		main = strconv.FormatUint(*e.MainVersion, 10)
	}
	if e.IgnoredVersion != nil {
		ignored = strconv.FormatUint(*e.IgnoredVersion, 10)
	}
	return fmt.Sprintf("native commit schema %s/%s ordinal %d refused: %s", main, ignored, e.OrdinalFromHead, e.Reason)
}

// Only the two cursor values can be JSON null. Keep this shape private so
// source-facing callers retain nullable uint64 fields rather than untyped data.
type nativeCommitSchemaWire struct {
	Format         string                      `json:"format"`
	Scope          string                      `json:"scope"`
	MainVersion    any                         `json:"main_version"`
	IgnoredVersion any                         `json:"ignored_version"`
	Absences       []NativeCommitSchemaAbsence `json:"absences"`
}

func (p *NativeCommitSchemaProfile) UnmarshalJSON(data []byte) error {
	// The strict decoder rejects null typed members. Only these two cursor
	// slots admit null, so decode them as source values and check their exact
	// integer type here without weakening other nullable-field rules.
	var value nativeCommitSchemaWire
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	cursor := func(raw any) (*uint64, error) {
		if raw == nil {
			return nil, nil
		}
		number, ok := raw.(json.Number)
		if !ok {
			return nil, fmt.Errorf("native commit cursor requires a nullable uint64")
		}
		version, err := strconv.ParseUint(string(number), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("native commit cursor requires a nullable uint64")
		}
		return &version, nil
	}
	main, err := cursor(value.MainVersion)
	if err != nil {
		return err
	}
	ignored, err := cursor(value.IgnoredVersion)
	if err != nil {
		return err
	}
	*p = NativeCommitSchemaProfile{value.Format, value.Scope, main, ignored, value.Absences}
	return nil
}

func (a *NativeCommitSchemaAbsence) UnmarshalJSON(data []byte) error {
	fields, err := nativeObject(data, "family reason", "column")
	if err != nil {
		return err
	}
	if column, present := fields["column"]; present && column == "" {
		return fmt.Errorf("empty native commit absent column")
	}
	type plain NativeCommitSchemaAbsence
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*a = NativeCommitSchemaAbsence(value)
	return nil
}

// nativeCommitAbsences observes the supplied pinned census against one image.
// Extra and historical-only tables/columns stay in the image unchanged.
func nativeCommitAbsences(tables map[string]NativeTable, census map[string]string) []NativeCommitSchemaAbsence {
	result := []NativeCommitSchemaAbsence{}
	for family, columns := range census {
		table, present := tables[family]
		if !present {
			result = append(result, NativeCommitSchemaAbsence{Family: family, Reason: AbsentBySourceSchema})
			continue
		}
		seen := map[string]bool{}
		for _, column := range table.Columns {
			seen[column] = true
		}
		for _, column := range strings.Fields(columns) {
			if !seen[column] {
				result = append(result, NativeCommitSchemaAbsence{Family: family, Column: column, Reason: AbsentBySourceSchema})
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Family != b.Family {
			return a.Family < b.Family
		}
		return a.Column < b.Column
	})
	return result
}

// nativeCommitCursor checks only the physical prefix and the supplied hash
// registry. The producer's supported schema registry is a separate admission
// requirement; a matching prefix alone does not establish schema support.
func nativeCommitCursor(tables map[string]NativeTable, family string, cursor *uint64, hashes map[int]string) error {
	table, present := tables[family]
	if cursor == nil {
		if present {
			return fmt.Errorf("null native commit cursor has a physical table")
		}
		return nil
	}
	if !present || *cursor > uint64(len(hashes)) || len(table.Rows) != int(*cursor) {
		return fmt.Errorf("native commit cursor lacks a complete pinned prefix")
	}
	columns := map[string]bool{}
	for _, name := range table.Columns {
		columns[name] = true
	}
	if !columns["version"] || !columns["content_hash"] {
		return fmt.Errorf("native commit cursor lacks pinned columns")
	}
	seen := map[uint64]bool{}
	for _, row := range table.Rows {
		version, err := natural(row["version"])
		if err != nil || version == 0 || version > *cursor || seen[version] || hashes[int(version)] == "" || row["content_hash"] != hashes[int(version)] {
			return fmt.Errorf("native commit migration prefix differs")
		}
		seen[version] = true
	}
	return nil
}

// Exact comparison census from producer ff8006bfb18316d4a5e8fdadcb9062b8e882fece,
// COMMIT-SCHEMA-REGISTRY.json SHA256
// cf71387bc62853dd48aa9e7e96dfe67cf1126cdd47c920758d1d4234b80cd253.
// It includes removed columns and does not describe any one expected schema.
func nativeCommitSchemaSurfaces() map[string]string {
	census := make(map[string]string, len(tableColumns))
	for family, columns := range tableColumns {
		census[family] = columns
	}
	census["schema_migrations"] += " applied_at"
	for _, family := range []string{DurableEntityTable, EphemeralEntityTable} {
		census[family] += " quality_score crystallizes lease_expires_at heartbeat_at"
	}
	for _, family := range []string{DurableDependencyTable, EphemeralDependencyTable} {
		census[family] += " depends_on_id"
	}
	return census
}

// A pre-migration root may have the native ignore catalog before either
// migration cursor exists. Its full table/column census is still explicit;
// this does not assign a schema version to an arbitrary cursorless root.
func nativeCommitInitialRoot(tables map[string]NativeTable) bool {
	if len(tables) == 0 {
		return true
	}
	ignore, present := tables["dolt_ignore"]
	return len(tables) == 1 && present && equalJSON(ignore.Columns, []string{"ignored", "pattern"})
}

func validateNativeCommitSchemas(h NativeHistory) error {
	annotated := 0
	for _, image := range h.Images {
		if image.SchemaProfile != nil {
			annotated++
		}
	}
	if annotated == 0 {
		// Legacy synthetic and fixed-version archives retain their exact wire.
		return nil
	}
	for _, image := range h.Images {
		if annotated != len(h.Images) {
			return NativeCommitSchemaError{OrdinalFromHead: uint64(len(h.Commits)) - image.Sequence + 1, Reason: "mixed annotated and legacy images"}
		}
		if err := validateNativeCommitSchema(image, uint64(len(h.Commits))); err != nil {
			return err
		}
	}
	return nil
}

func validateNativeCommitSchema(image NativeHistoryImage, count uint64) error {
	census := nativeCommitSchemaSurfaces()
	p := image.SchemaProfile
	refuse := func(reason string) error {
		err := NativeCommitSchemaError{OrdinalFromHead: count - image.Sequence + 1, Reason: reason}
		if p != nil {
			err.MainVersion, err.IgnoredVersion = p.MainVersion, p.IgnoredVersion
		}
		return err
	}
	if p == nil || p.Format != NativeCommitSchemaFormat || p.Scope != NativeCommitSchemaScope || p.Absences == nil {
		return refuse("unsupported per-commit profile shape")
	}
	if p.MainVersion == nil && (!nativeCommitInitialRoot(image.Tables) || p.IgnoredVersion != nil) {
		return refuse("null main cursor requires an initial physical root")
	}
	if err := nativeCommitCursor(image.Tables, "schema_migrations", p.MainVersion, migrationHashes["main"]); err != nil {
		return refuse("main cursor or pinned prefix differs")
	}
	if err := nativeCommitCursor(image.Tables, "ignored_schema_migrations", p.IgnoredVersion, migrationHashes["ignored"]); err != nil {
		return refuse("ignored cursor or pinned prefix differs")
	}
	if !equalJSON(p.Absences, nativeCommitAbsences(image.Tables, census)) {
		return refuse("absence facts differ from observed physical schema")
	}

	return nil
}
