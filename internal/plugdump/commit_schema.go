package plugdump

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const NativeCommitSchemaFormat = "beads-commit-schema/1"

// NativeCommitSchemaProfile describes ONE complete immutable root. Its
// absences refer only to that root's physical schema, not the working set,
// HEAD, portable business history or a promise of historical defaults.
// A null ignored cursor is an observed physical absence, never HEAD's version.
// A null main cursor requires an authenticated empty or dolt_ignore-only
// initial root, with no Beads tables or ignored cursor; it never means version0.
type NativeCommitSchemaProfile struct {
	Format         string                      `json:"format"`
	Scope          string                      `json:"scope"`
	MainVersion    *uint64                     `json:"main_version"`
	IgnoredVersion *uint64                     `json:"ignored_version"`
	Absences       []NativeCommitSchemaAbsence `json:"absences"`
}

type NativeCommitSchemaAbsence struct {
	Family string `json:"family"`
	Column string `json:"column,omitempty"`
	Reason string `json:"reason"`
}

// NativeCommitSchemaReader is an opt-in by the authenticated physical reader.
// Legacy synthetic readers keep their original unannotated image contract.
type NativeCommitSchemaReader interface {
	ProfileNativeSourceImage(map[string]Table) (NativeCommitSchemaProfile, error)
}

type ErrUnsupportedNativeCommitSchema struct {
	MainVersion, IgnoredVersion *uint64
	Cause                       error
}

func (ErrUnsupportedNativeCommitSchema) Error() string   { return "unsupported immutable commit schema" }
func (e ErrUnsupportedNativeCommitSchema) Unwrap() error { return e.Cause }

// NativeCommitFailure carries only code/schema-derived public coordinates.
// OrdinalFromHead is one-based reverse canonical parent-before-child order,
// including merge ancestry. It is never a source hash or timestamp.
type NativeCommitFailure struct {
	SchemaVersion   *uint64
	OrdinalFromHead uint64
	Cause           error
}

func (*NativeCommitFailure) Error() string   { return "native commit capture failed" }
func (e *NativeCommitFailure) Unwrap() error { return e.Cause }

// Keep the genuine cause while filling only coordinates actually observed in
// the current read. Unknown graph/schema coordinates remain explicit null in
// the closed report; HEAD's version is never borrowed for an older commit.
func historyFailureCoordinates(err error, ordinal uint64, version *uint64) error {
	if err == nil {
		return nil
	}
	var previous *NativeCommitFailure
	if errors.As(err, &previous) {
		if previous.OrdinalFromHead != 0 {
			ordinal = previous.OrdinalFromHead
			// These coordinates are one observation. An unknown schema at a
			// known older commit must stay unknown, never inherit HEAD's cursor.
			version = previous.SchemaVersion
		} else if previous.SchemaVersion != nil {
			version = previous.SchemaVersion
		}
		if previous.OrdinalFromHead == ordinal && equalJSON(previous.SchemaVersion, version) {
			return err
		}
	}
	return &NativeCommitFailure{SchemaVersion: version, OrdinalFromHead: ordinal, Cause: err}
}

// NativeCommitSchemaSurfaces is a detached closed PUBLIC comparison census.
// The current pinned census is supplemented by removed shipped columns. This
// is not an expected schema at any version: extra historical tables/columns
// remain in the image, including migration intermediate shapes.
func NativeCommitSchemaSurfaces() map[string][]string {
	result := make(map[string][]string, len(tableColumns))
	for name, columns := range tableColumns {
		result[name] = strings.Fields(columns)
	}
	result["schema_migrations"] = append(result["schema_migrations"], "applied_at")
	for _, family := range []string{"issues", "wisps"} {
		result[family] = append(result[family], "quality_score", "crystallizes", "lease_expires_at", "heartbeat_at")
	}
	for _, family := range []string{"dependencies", "wisp_dependencies"} {
		result[family] = append(result[family], "depends_on_id")
	}
	for _, columns := range result {
		sort.Strings(columns)
	}
	return result
}

// NativeCommitMigrationHashes returns the exact already-pinned registry, not
// a newly inferred main/ignored pairing. Every physically present cursor must
// authenticate its own entire hash prefix, independently of the other lane.
func NativeCommitMigrationHashes() map[string]map[int]string {
	result := map[string]map[int]string{}
	for lane, hashes := range migrationHashes {
		result[lane] = map[int]string{}
		for version, hash := range hashes {
			result[lane][version] = hash
		}
	}
	return result
}

func commitCursor(tables map[string]Table, name string) (*uint64, error) {
	table, present := tables[name]
	if !present {
		return nil, nil
	}
	hasVersion := false
	for _, column := range table.Columns {
		if column == "version" {
			hasVersion = true
		}
	}
	if !hasVersion {
		return nil, fmt.Errorf("immutable cursor lacks numeric version column")
	}
	version := uint64(0)
	for _, row := range table.Rows {
		value, err := natural(row["version"])
		if err != nil {
			return nil, fmt.Errorf("invalid immutable schema cursor")
		}
		if value > version {
			version = value
		}
	}
	return &version, nil
}

func nativeCommitCursors(tables map[string]Table, rootTables int) (NativeCommitSchemaProfile, error) {
	p := NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: "immutable-commit-root", Absences: []NativeCommitSchemaAbsence{}}
	var err error
	p.MainVersion, err = commitCursor(tables, "schema_migrations")
	if err != nil {
		return p, err
	}
	p.IgnoredVersion, err = commitCursor(tables, "ignored_schema_migrations")
	if err != nil {
		return p, err
	}
	unsupported := func() (NativeCommitSchemaProfile, error) {
		return p, ErrUnsupportedNativeCommitSchema{MainVersion: p.MainVersion, IgnoredVersion: p.IgnoredVersion}
	}
	if p.MainVersion == nil {
		// A one-table cursor-absent root is only provisional here. The full
		// observed census must prove it is precisely the dolt_ignore initial
		// root, including its columns, before any profile is returned.
		if p.IgnoredVersion != nil || rootTables > 1 {
			return unsupported()
		}
		return p, nil
	}
	for _, lane := range []string{"main", "ignored"} {
		name, version := "schema_migrations", p.MainVersion
		if lane == "ignored" {
			name, version = "ignored_schema_migrations", p.IgnoredVersion
		}
		if version == nil {
			continue
		}
		if *version > uint64(len(migrationHashes[lane])) {
			return unsupported()
		}
		if err := checkMigrationPrefix(tables[name], lane, int(*version)); err != nil {
			return p, ErrUnsupportedNativeCommitSchema{MainVersion: p.MainVersion, IgnoredVersion: p.IgnoredVersion, Cause: err}
		}
	}
	return p, nil
}

// nativeInitialCommitRoot recognizes the actual pre-migration roots produced
// by the pinned reference: an empty root, or only dolt_ignore with its exact
// two-column physical schema. No numeric migration version or user-table
// schema is invented for these observed cursor-absent roots.
func nativeInitialCommitRoot(tables map[string]Table) bool {
	if len(tables) == 0 {
		return true
	}
	ignore, ok := tables["dolt_ignore"]
	if !ok || len(tables) != 1 || len(ignore.Columns) != 2 {
		return false
	}
	seen := map[string]bool{}
	for _, column := range ignore.Columns {
		seen[column] = true
	}
	return seen["pattern"] && seen["ignored"]
}

// ProfileNativeCommit binds observed physical absence, never a guessed
// migration delta. It does not drop old columns or fabricate absent cells.
func ProfileNativeCommit(tables map[string]Table) (NativeCommitSchemaProfile, error) {
	if tables == nil {
		return NativeCommitSchemaProfile{}, fmt.Errorf("missing immutable census")
	}
	p, err := nativeCommitCursors(tables, len(tables))
	if err != nil {
		return p, err
	}
	if p.MainVersion == nil && !nativeInitialCommitRoot(tables) {
		return p, ErrUnsupportedNativeCommitSchema{MainVersion: p.MainVersion, IgnoredVersion: p.IgnoredVersion}
	}
	surfaces := NativeCommitSchemaSurfaces()
	names := make([]string, 0, len(surfaces))
	for name := range surfaces {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		table, present := tables[name]
		if !present {
			p.Absences = append(p.Absences, NativeCommitSchemaAbsence{Family: name, Reason: AbsentBySourceSchema})
			continue
		}
		columns := map[string]bool{}
		for _, column := range table.Columns {
			columns[column] = true
		}
		for _, column := range surfaces[name] {
			if !columns[column] {
				p.Absences = append(p.Absences, NativeCommitSchemaAbsence{Family: name, Column: column, Reason: AbsentBySourceSchema})
			}
		}
	}
	return p, nil
}

func validateNativeCommitProfile(image NativeHistoryImage) error {
	if image.SchemaProfile == nil {
		return nil
	}
	tables := make(map[string]Table, len(image.Tables))
	for name, table := range image.Tables {
		tables[name] = Table{Columns: table.Columns, Rows: table.Rows}
	}
	observed, err := ProfileNativeCommit(tables)
	if err != nil {
		return err
	}
	if !equalJSON(*image.SchemaProfile, observed) {
		return fmt.Errorf("immutable commit schema profile differs from observed image")
	}
	return nil
}
