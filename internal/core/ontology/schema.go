package ontology

import "sort"

// Schema describes the ontology view of a bead and its typed fields. Metadata,
// links and other core record data belong to their own schemas. A target status
// adds fields_required guards for that target to the field requirements.
func (e Effective) Schema(typ, status string) (map[string]any, []Violation) {
	t, ok := e.document.Types[typ]
	if !ok || t.Archived {
		return nil, invalid("type", "unknown or archived type")
	}
	w := e.document.Workflows[t.Workflow]
	statuses := []string{}
	for _, key := range w.Statuses {
		if s, ok := e.Status(key); ok && !s.Archived {
			statuses = append(statuses, key)
		}
	}
	if status != "" {
		if !contains(statuses, status) {
			return nil, invalid("status", "status is outside the active workflow")
		}
		statuses = []string{status}
	}
	sort.Strings(statuses)
	properties := map[string]any{}
	properties["priority"] = map[string]any{"type": "integer", "minimum": 0, "maximum": 4, "default": DefaultPriority}
	properties["template"] = map[string]any{"type": "boolean", "default": false}
	for name, property := range awaitMemberSchemas() {
		properties[name] = property
	}
	requiredSet := map[string]bool{}
	for _, name := range keys(t.Fields) {
		field := t.Fields[name]
		property := map[string]any{}
		switch field.Type {
		case "enum":
			property["type"] = "string"
			property["enum"] = append([]string{}, field.Values...)
		case "strings":
			property["type"] = "array"
			property["items"] = map[string]any{"type": "string"}
		default:
			property["type"] = field.Type
		}
		if field.Default != nil && field.Owner == "" {
			property["default"] = field.Default
		}
		if field.Owner != "" {
			property["readOnly"] = true
			property["x-plug-owner"] = field.Owner
		}
		properties[name] = property
		if field.Required {
			requiredSet[name] = true
		}
	}
	if status != "" {
		for _, rule := range w.Transitions {
			if rule.To == status {
				for _, guard := range rule.Guards {
					if guard.Kind == FieldsRequired {
						for _, name := range guard.Fields {
							requiredSet[name] = true
						}
					}
				}
			}
		}
	}
	required := keys(requiredSet)
	fields := map[string]any{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) != 0 {
		fields["required"] = required
	}
	statusSchema := map[string]any{"type": "string", "enum": statuses}
	if status == "" {
		statusSchema["default"] = w.Initial
	}
	result := map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "additionalProperties": false,
		"required": []string{"type", "status"},
		"properties": map[string]any{
			"id": map[string]any{"type": "string"}, "project": map[string]any{"type": "string"},
			"type": map[string]any{"type": "string", "const": typ}, "status": statusSchema,
			"template": map[string]any{"type": "boolean"},
			"fields":   fields,
		},
	}
	if len(required) != 0 {
		result["required"] = []string{"type", "status", "fields"}
	}
	// Reuse the JSON-copy boundary so slice/default values cannot alias the snapshot.
	copy, err := copyState(BeadState{Fields: result})
	if err != nil {
		return nil, invalid("schema", "schema cannot be encoded")
	}
	return copy.Fields, nil
}

// Guards returns all applicable guards in document order. Multiple matching
// transition rules are conjunctive: a broad rule cannot bypass a narrow guard.
func (e Effective) Guards(typ, from, to string) ([]Guard, bool) {
	t, ok := e.document.Types[typ]
	if !ok || t.Archived {
		return nil, false
	}
	w := e.document.Workflows[t.Workflow]
	if !contains(w.Statuses, from) || !contains(w.Statuses, to) || from == to {
		return nil, false
	}
	result := []Guard{}
	matched := false
	for _, rule := range w.Transitions {
		if rule.To != to || (len(rule.From) != 0 && !contains(rule.From, from)) {
			continue
		}
		matched = true
		for _, guard := range rule.Guards {
			guard.Fields = append([]string{}, guard.Fields...)
			guard.Kinds = append([]string{}, guard.Kinds...)
			result = append(result, guard)
		}
	}
	return result, matched
}
