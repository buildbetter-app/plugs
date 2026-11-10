package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const AbsentBySourceSchema = "absent-by-source-schema"

// NativeSchemaAbsence describes a surface that did not exist at the captured
// migration cursors. It is not an empty table, SQL NULL cell or history gap.
type NativeSchemaAbsence struct {
	Family       string `json:"family"`
	Column       string `json:"column,omitempty"`
	IntroducedIn int    `json:"introduced_in"`
	Reason       string `json:"reason"`
}

type NativeSchemaProfile struct {
	MainVersion    int                   `json:"main_version"`
	IgnoredVersion int                   `json:"ignored_version"`
	Absences       []NativeSchemaAbsence `json:"absences"`
}

// NativeSchemaError keeps unsupported cursors and mismatching absence claims
// distinct from missing retained history. No default or migration is attempted.
type NativeSchemaError struct {
	MainVersion, IgnoredVersion int
	Reason                      string
}

func (e NativeSchemaError) Error() string {
	return fmt.Sprintf("native source schema %d/%d refused: %s", e.MainVersion, e.IgnoredVersion, e.Reason)
}

func (p *NativeSchemaProfile) UnmarshalJSON(data []byte) error {
	if _, err := nativeObject(data, "main_version ignored_version absences", ""); err != nil {
		return err
	}
	type plain NativeSchemaProfile
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*p = NativeSchemaProfile(value)
	return nil
}

func (a *NativeSchemaAbsence) UnmarshalJSON(data []byte) error {
	object, err := nativeObject(data, "family introduced_in reason", "column")
	if err != nil {
		return err
	}
	if column, present := object["column"]; present && column == "" {
		return fmt.Errorf("empty native absent column")
	}
	type plain NativeSchemaAbsence
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	*a = NativeSchemaAbsence(value)
	return nil
}

// NativeSchemaForVersion returns detached protocol facts from the pinned main
// and ignored migration series. Historical images keep their observed schemas.
func NativeSchemaForVersion(main, ignored int) (NativeSchemaProfile, error) {
	p := NativeSchemaProfile{MainVersion: main, IgnoredVersion: ignored, Absences: []NativeSchemaAbsence{}}
	switch {
	case main == 72 && ignored == 28:
		return p, nil
	case main == 66 && ignored == 26:
		for _, family := range []string{"issue_versions", "store_epoch", "epoch_minted_addresses"} {
			introduced := 67
			if family == "epoch_minted_addresses" {
				introduced = 72
			}
			p.Absences = append(p.Absences, NativeSchemaAbsence{Family: family, IntroducedIn: introduced, Reason: AbsentBySourceSchema})
		}
		for _, family := range []string{DurableEntityTable, EphemeralEntityTable} {
			p.Absences = append(p.Absences,
				NativeSchemaAbsence{Family: family, Column: "current_revision", IntroducedIn: 67, Reason: AbsentBySourceSchema},
				NativeSchemaAbsence{Family: family, Column: "participation_generation", IntroducedIn: 70, Reason: AbsentBySourceSchema})
		}
		sort.Slice(p.Absences, func(i, j int) bool {
			a, b := p.Absences[i], p.Absences[j]
			if a.Family != b.Family {
				return a.Family < b.Family
			}
			return a.Column < b.Column
		})
		return p, nil
	default:
		return NativeSchemaProfile{}, NativeSchemaError{main, ignored, "unsupported migration cursor pair"}
	}
}

func nativeSchemaColumns(p NativeSchemaProfile) map[string]string {
	columns := make(map[string]string, len(tableColumns))
	for name, value := range tableColumns {
		columns[name] = value
	}
	for _, absence := range p.Absences {
		if absence.Column == "" {
			delete(columns, absence.Family)
			continue
		}
		names := []string{}
		for _, name := range strings.Fields(columns[absence.Family]) {
			if name != absence.Column {
				names = append(names, name)
			}
		}
		columns[absence.Family] = strings.Join(names, " ")
	}
	return columns
}

func sourceTableColumns(main int) map[string]string {
	if main == 66 {
		p, _ := NativeSchemaForVersion(66, 26)
		return nativeSchemaColumns(p)
	}
	return tableColumns
}

func validateNativeSchema(h NativeHistory) (map[string]string, error) {
	p, err := NativeSchemaForVersion(72, 28)
	if h.SchemaProfile != nil {
		p, err = NativeSchemaForVersion(h.SchemaProfile.MainVersion, h.SchemaProfile.IgnoredVersion)
		if err == nil && !equalJSON(p, *h.SchemaProfile) {
			err = NativeSchemaError{p.MainVersion, p.IgnoredVersion, "absence facts differ from exact version profile"}
		}
	}
	if err != nil {
		return nil, err
	}
	if h.SchemaProfile != nil {
		for table, want := range map[string]int{"schema_migrations": p.MainVersion, "ignored_schema_migrations": p.IgnoredVersion} {
			max := uint64(0)
			seen := map[uint64]bool{}
			for _, row := range h.Current[table].Rows {
				version, err := natural(row["version"])
				if err != nil || version == 0 || seen[version] || version > uint64(want) {
					return nil, NativeSchemaError{p.MainVersion, p.IgnoredVersion, "invalid captured migration census: " + table}
				}
				seen[version] = true
				if version > max {
					max = version
				}
			}
			if max != uint64(want) {
				return nil, NativeSchemaError{p.MainVersion, p.IgnoredVersion, "captured migration cursor differs: " + table}
			}
		}
	}
	return nativeSchemaColumns(p), nil
}

// Keep JSON in the wire vocabulary while requiring its exact typed shape.
var _ json.Unmarshaler = (*NativeSchemaProfile)(nil)
