package plugdump

import (
	"fmt"
	"sort"
	"strings"
)

const AbsentBySourceSchema = "absent-by-source-schema"

// NativeSchemaAbsence names a public schema surface that did not yet exist.
// It is neither an empty observed table nor missing retained history.
type NativeSchemaAbsence struct {
	Family       string `json:"family"`
	Column       string `json:"column,omitempty"`
	IntroducedIn int    `json:"introduced_in"`
	Reason       string `json:"reason"`
}

// NativeSchemaProfile binds the exact main AND ignored migration prefixes.
// In particular, the ignored twins can add wisp columns independently of main.
type NativeSchemaProfile struct {
	MainVersion    int                   `json:"main_version"`
	IgnoredVersion int                   `json:"ignored_version"`
	Absences       []NativeSchemaAbsence `json:"absences"`
}

type ErrUnsupportedSourceSchema struct {
	MainVersion, IgnoredVersion uint64
}

func (ErrUnsupportedSourceSchema) Error() string { return "unsupported source schema profile" }

// CaptureSchemaSupported checks only the main cursor. Full capture additionally
// authenticates the paired ignored prefix, every migration hash and the schema.
func CaptureSchemaSupported(version uint64) bool { return version == 66 || version == 72 }

func schemaProfile(main, ignored uint64) (NativeSchemaProfile, error) {
	p := NativeSchemaProfile{MainVersion: int(main), IgnoredVersion: int(ignored), Absences: []NativeSchemaAbsence{}}
	switch {
	case main == 72 && ignored == 28:
		return p, nil
	case main == 66 && ignored == 26:
		// Frozen 0067 creates both tables and both current_revision columns;
		// 0070 adds participation_generation on both planes. The ignored
		// twins are 0027 and 0028, both strictly beyond this ignored prefix.
		p.Absences = []NativeSchemaAbsence{
			{Family: "epoch_minted_addresses", IntroducedIn: 72, Reason: AbsentBySourceSchema},
			{Family: "issue_versions", IntroducedIn: 67, Reason: AbsentBySourceSchema},
			{Family: "issues", Column: "current_revision", IntroducedIn: 67, Reason: AbsentBySourceSchema},
			{Family: "issues", Column: "participation_generation", IntroducedIn: 70, Reason: AbsentBySourceSchema},
			{Family: "store_epoch", IntroducedIn: 67, Reason: AbsentBySourceSchema},
			{Family: "wisps", Column: "current_revision", IntroducedIn: 67, Reason: AbsentBySourceSchema},
			{Family: "wisps", Column: "participation_generation", IntroducedIn: 70, Reason: AbsentBySourceSchema},
		}
		return p, nil
	default:
		return NativeSchemaProfile{}, ErrUnsupportedSourceSchema{MainVersion: main, IgnoredVersion: ignored}
	}
}

func (p NativeSchemaProfile) tables() map[string][]string {
	result := make(map[string][]string, len(tableColumns))
	for name, columns := range tableColumns {
		result[name] = strings.Fields(columns)
	}
	for _, absent := range p.Absences {
		if absent.Column == "" {
			delete(result, absent.Family)
			continue
		}
		var columns []string
		for _, column := range result[absent.Family] {
			if column != absent.Column {
				columns = append(columns, column)
			}
		}
		result[absent.Family] = columns
	}
	return result
}

func sourceSchemaProfile(tables map[string]Table) (NativeSchemaProfile, error) {
	versions := map[string]uint64{}
	for _, lane := range []string{"main", "ignored"} {
		name := "schema_migrations"
		if lane == "ignored" {
			name = "ignored_schema_migrations"
		}
		for _, row := range tables[name].Rows {
			version, err := natural(row["version"])
			if err != nil {
				return NativeSchemaProfile{}, fmt.Errorf("invalid %s schema cursor", lane)
			}
			if version > versions[lane] {
				versions[lane] = version
			}
		}
	}
	p, err := schemaProfile(versions["main"], versions["ignored"])
	if err != nil {
		return NativeSchemaProfile{}, err
	}
	if err := checkMigrationPrefix(tables["schema_migrations"], "main", p.MainVersion); err != nil {
		return NativeSchemaProfile{}, err
	}
	if err := checkMigrationPrefix(tables["ignored_schema_migrations"], "ignored", p.IgnoredVersion); err != nil {
		return NativeSchemaProfile{}, err
	}
	return p, nil
}

func validateSchemaProfile(p *NativeSchemaProfile, main, ignored int) (NativeSchemaProfile, error) {
	expected, err := schemaProfile(uint64(main), uint64(ignored))
	if err != nil {
		return NativeSchemaProfile{}, err
	}
	if p == nil {
		if main != 72 {
			return NativeSchemaProfile{}, fmt.Errorf("own-version source requires explicit schema profile")
		}
	} else if !equalJSON(p, expected) {
		return NativeSchemaProfile{}, fmt.Errorf("source schema profile or typed absences mismatch")
	}
	return expected, nil
}

func validateCurrentSchema(tables map[string]Table, p NativeSchemaProfile) error {
	expected := p.tables()
	for name, table := range tables {
		columns, present := expected[name]
		if !present {
			return fmt.Errorf("unaccounted source schema family %s", name)
		}
		actual := append([]string{}, table.Columns...)
		sort.Strings(actual)
		sort.Strings(columns)
		if !equalJSON(actual, columns) {
			return fmt.Errorf("schema drift in %s", name)
		}
		for _, row := range table.Rows {
			if len(row) != len(columns) {
				return fmt.Errorf("missing or unknown source fields in %s", name)
			}
			for _, column := range columns {
				if _, present := row[column]; !present {
					return fmt.Errorf("missing source field in %s", name)
				}
			}
		}
	}
	for name := range expected {
		if _, present := tables[name]; !present {
			return fmt.Errorf("unaccounted table family %s", name)
		}
	}
	return nil
}
