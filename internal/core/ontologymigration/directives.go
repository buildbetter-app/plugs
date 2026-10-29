package ontologymigration

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func directiveShape(d Directive) error {
	if strings.TrimSpace(d.ID) == "" || d.Target.Project == "" || d.Target.Type == "" {
		return fmt.Errorf("directive ID, project and original type are required")
	}
	allowed := map[string]bool{}
	required := []string{}
	switch d.Kind {
	case MapType, MapStatus:
		required = []string{"from", "to"}
		if d.From == d.To {
			return fmt.Errorf("mapping must change its key")
		}
		if d.Kind == MapType && d.From != d.Target.Type {
			return fmt.Errorf("map_type source must equal the original target type")
		}
	case SetField:
		required = []string{"field", "value"}
	case ConvertField:
		required = []string{"field", "conversion"}
	case DropField:
		required = []string{"field"}
	case Grandfather:
		required = []string{"violation", "reason"}
		if d.Target.ID == "" || strings.TrimSpace(d.Reason) == "" || !validDigest(d.Violation) {
			return fmt.Errorf("grandfather requires an exact row, violation digest and reason")
		}
	default:
		return fmt.Errorf("unrecognized directive kind")
	}
	data, _ := json.Marshal(d)
	var members map[string]json.RawMessage
	_ = json.Unmarshal(data, &members)
	for _, key := range required {
		allowed[key] = true
		if _, exists := members[key]; !exists {
			return fmt.Errorf("directive requires %s", key)
		}
	}
	for key := range members {
		if !allowed[key] && key != "id" && key != "kind" && key != "target" {
			return fmt.Errorf("%s is not valid for %s", key, d.Kind)
		}
	}
	if d.Field != "" && (strings.ContainsAny(d.Field, ". /\t\n") || slices.Contains([]string{"id", "project", "type", "status", "priority", "template", "metadata"}, d.Field)) {
		return fmt.Errorf("field must name a custom typed field; kernel fields are immutable")
	}
	return nil
}

func matches(d Directive, row Row) bool {
	return d.Target.Project == row.State.Project && d.Target.Type == row.State.Type && (d.Target.ID == "" || d.Target.ID == row.State.ID) && (d.Kind != MapStatus || d.From == row.State.Status)
}

func member(d Directive) string {
	switch d.Kind {
	case MapType:
		return "type"
	case MapStatus:
		return "status"
	case Grandfather:
		return "violation:" + d.Violation
	default:
		return "fields." + d.Field
	}
}

// applyDirectives always reads selectors from Before. Mapping type first makes
// status and custom-field validation use the resulting project's whole-key
// effective type. No directive renames IDs, changes projects or erases fields
// as a side effect of changing type.
func applyDirectives(req Request, row *RowPlan, directives []Directive, used map[string]bool, refuse func(Refusal)) {
	current := req.Current.Snapshot.Effective(row.Before.State.Project).Document()
	effective := req.Candidate.Snapshot.Effective(row.Before.State.Project)
	candidate := effective.Document()
	seen := map[string]bool{}
	conflict := false
	for _, d := range directives {
		key := member(d)
		if seen[key] {
			refuse(Refusal{Code: "invalid_params", Row: rowKey(row.Before), Directive: d.ID, Message: "conflicting directives target the same row member"})
			conflict = true
		}
		seen[key] = true
	}
	if conflict {
		return
	}
	for _, phase := range []Kind{MapType, MapStatus, SetField, ConvertField, DropField} {
		for _, d := range directives {
			if d.Kind != phase {
				continue
			}
			fail := func(code, message string) {
				refuse(Refusal{Code: model.ErrorCode(code), Row: rowKey(row.Before), Directive: d.ID, Message: message})
			}
			oldType := current.Types[row.Before.State.Type]
			newType, newTypeExists := candidate.Types[row.After.State.Type]
			oldField := oldType.Fields[d.Field]
			newField, newFieldExists := newType.Fields[d.Field]
			switch d.Kind {
			case MapType:
				_, oldExists := current.Types[d.From]
				new, newExists := candidate.Types[d.To]
				if !oldExists || !newExists || new.Archived {
					fail("invalid_params", "type mapping requires a known source and active candidate target")
					continue
				}
				row.After.State.Type = d.To
			case MapStatus:
				if _, exists := current.Statuses[d.From]; !exists || !newTypeExists {
					fail("invalid_params", "status mapping requires known source status and resulting type")
					continue
				}
				target, exists := candidate.Statuses[d.To]
				workflow := candidate.Workflows[newType.Workflow]
				if !exists || target.Archived || workflow.Archived || !slices.Contains(workflow.Statuses, d.To) {
					fail("invalid_params", "status mapping target is outside the active resulting workflow")
					continue
				}
				row.After.State.Status = d.To
			default:
				// Ownership in either ontology forbids all three field directives,
				// including dropping a field whose candidate removed its owner.
				if oldField.Owner != "" || newField.Owner != "" {
					fail("field_owned", "migration cannot set, convert or drop a plugin-owned field")
					continue
				}
				value, present := row.After.State.Fields[d.Field]
				switch d.Kind {
				case SetField:
					if !newFieldExists || (present && equal(value, d.Value)) {
						fail("invalid_params", "set_field requires a candidate field and an actual value change")
						continue
					}
					row.After.State.Fields[d.Field] = d.Value
				case DropField:
					if !present || (newFieldExists && (newField.Required || newField.Default != nil)) {
						fail("invalid_params", "drop_field requires a present field without a candidate requirement/default")
						continue
					}
					delete(row.After.State.Fields, d.Field)
				case ConvertField:
					converted, err := convert(d.Conversion, oldField, newField, value)
					if !present || !newFieldExists || err != nil {
						fail("invalid_params", "unsupported conversion, incompatible schemas or invalid source value")
						continue
					}
					row.After.State.Fields[d.Field] = converted
				}
			}
			used[d.ID] = true
			row.Directives = append(row.Directives, d.ID)
		}
	}
}

// Conversions deliberately retain exact JSON numbers. They never trim, round,
// narrow to a machine integer, parse booleans loosely or flatten collections.
func convert(name string, from, to ontology.Field, value any) (any, error) {
	if name != from.Type+"_to_"+to.Type {
		return nil, fmt.Errorf("conversion does not match field schemas")
	}
	bad := fmt.Errorf("unsupported conversion or source value")
	switch name {
	case "string_to_integer", "string_to_number":
		text, ok := value.(string)
		if !ok || text == "" || text != strings.TrimSpace(text) || !json.Valid([]byte(text)) {
			return nil, bad
		}
		var number any
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		if err := decoder.Decode(&number); err != nil {
			return nil, bad
		}
		if _, ok := number.(json.Number); !ok {
			return nil, bad
		}
		if name == "string_to_integer" && model.Natural(strings.TrimPrefix(text, "-")).Validate() != nil {
			return nil, bad
		}
		return number, nil
	case "integer_to_string", "number_to_string":
		v, ok := value.(json.Number) // Inputs have crossed a lossless JSON copy.
		if !ok {
			return nil, bad
		}
		if name == "integer_to_string" && model.Natural(strings.TrimPrefix(string(v), "-")).Validate() != nil {
			return nil, bad
		}
		return string(v), nil
	case "string_to_boolean":
		if value == "true" {
			return true, nil
		}
		if value == "false" {
			return false, nil
		}
		return nil, bad
	case "boolean_to_string":
		if flag, ok := value.(bool); ok {
			if flag {
				return "true", nil
			}
			return "false", nil
		}
	}
	return nil, bad
}
