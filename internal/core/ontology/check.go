package ontology

import (
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type CheckResult struct {
	Violations []Violation
	Warnings   []model.Warning
}

// Check applies touched-rule semantics to a final proposal. Existing violations
// outside the write's field set are reported as ontology_drifted warnings; they
// cannot be silently repaired or prevent an unrelated write. Create, type and
// project changes validate the whole state. Validate is the strict full check.
func Check(e Effective, before, after *BeadState, touched model.FieldSet, method string, facts GuardFacts) CheckResult {
	result := CheckResult{}
	touched = append(append(model.FieldSet{}, touched...), facts.Touched...)
	if before != nil && after != nil {
		// A caller's incomplete touched set must never hide a newly introduced
		// violation. Add the actual state differences before classifying drift.
		for name, value := range after.Fields {
			old, exists := before.Fields[name]
			if !exists || !sameValue(old, value) {
				touched = append(touched, "fields."+name)
			}
		}
		for name := range before.Fields {
			if _, exists := after.Fields[name]; !exists {
				touched = append(touched, "fields."+name)
			}
		}
		for _, pair := range []struct {
			name          string
			before, after any
		}{
			{"id", before.ID, after.ID}, {"type", before.Type, after.Type}, {"project", before.Project, after.Project},
			{"status", before.Status, after.Status}, {"template", before.Template, after.Template},
		} {
			if !reflect.DeepEqual(pair.before, pair.after) {
				touched = append(touched, pair.name)
			}
		}
	}
	all := before == nil || hasPath(touched, "type") || hasPath(touched, "project")
	if before != nil && after != nil {
		all = all || before.Type != after.Type || before.Project != after.Project
	}
	facts.Touched = append(append(model.FieldSet{}, facts.Touched...), touched...)
	violations := Validate(e, before, after, method, facts)
	for _, violation := range violations {
		if all || violation.Field == "" || hasPath(touched, violation.Field) || violation.Code == "field_owned" || violation.Code == "subscriber_unavailable" || violation.Code == "ontology_use_slot_verb" || violation.Guard != "" || (violation.Field == "type" && hasPath(touched, "fields")) {
			result.Violations = append(result.Violations, violation)
		} else {
			result.Warnings = append(result.Warnings, model.Warning{Code: "ontology_drifted", Message: violation.Message, Details: map[string]any{"field": violation.Field, "code": violation.Code}})
		}
	}
	return result
}

func hasPath(touched model.FieldSet, field string) bool {
	for _, path := range touched {
		if path == "*" || path == field || strings.HasPrefix(field, path+".") || strings.HasPrefix(path, field+".") {
			return true
		}
	}
	return false
}
