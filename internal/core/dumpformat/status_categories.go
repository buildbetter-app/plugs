package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

const NativeStatusOntologyVerification = "native status names/categories retained per project; explicit map_type directives recorded in manifest"

// NativeStatusOntology is the protected source body installed by restore. An
// base contains only shared native builtins; project overlays add exact catalogs.
type NativeStatusOntology struct {
	Base     ontology.Document            `json:"base"`
	Overlays map[string]ontology.Document `json:"overlays"`
}

func nativeBuiltInStatuses() []ProjectStatus {
	return []ProjectStatus{{"blocked", "wip"}, {"closed", "done"}, {"deferred", "frozen"}, {"hooked", "wip"}, {"in_progress", "wip"}, {"open", "active"}, {"pinned", "frozen"}}
}

// UnmarshalJSON refuses aliases and missing members instead of accepting
// encoding/json's case-insensitive field matching for this typed contract.
func (s *ProjectStatus) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if len(fields) != 2 || fields["name"] == nil || fields["category"] == nil {
		return fmt.Errorf("status declaration requires exact name and category members")
	}
	type plain ProjectStatus
	var value plain
	if err := decodeJSON(data, &value); err != nil || value.Name == "" || value.Category == "" {
		return fmt.Errorf("status declaration requires nonempty text name and category")
	}
	*s = ProjectStatus(value)
	return nil
}

func projectStatusCategory(project Project, name string) string {
	for _, status := range project.Statuses {
		if status.Name == name {
			return status.Category
		}
	}
	return ""
}

func validateStatusCategoryMarker(fields map[string]json.RawMessage, manifest Manifest) error {
	for key := range fields {
		if key != "status_category_contract" && strings.EqualFold(key, "status_category_contract") {
			return fmt.Errorf("noncanonical status_category_contract key")
		}
	}
	if _, present := fields["status_category_contract"]; present && manifest.StatusCategoryContract != StatusCategoryContract {
		return fmt.Errorf("unsupported status category contract")
	}
	return validateStatusCategoryContract(manifest)
}

func validateStatusCategoryContract(manifest Manifest) error {
	if manifest.StatusCategoryContract != "" && manifest.StatusCategoryContract != StatusCategoryContract {
		return fmt.Errorf("unsupported status category contract")
	}
	for _, project := range manifest.Projects {
		if manifest.StatusCategoryContract == "" {
			if project.Statuses != nil {
				return fmt.Errorf("project statuses require status category contract")
			}
			continue
		}
		if manifest.SourcePlaneAddressContract != SourcePlaneAddressContract || len(project.Statuses) < 7 || len(project.Statuses) > 57 {
			return fmt.Errorf("native status catalog requires source planes, all native builtins and at most 50 custom statuses")
		}
		for i, status := range project.Statuses {
			if !declarationName.MatchString(status.Name) || len(status.Name) > 64 || i > 0 && status.Name <= project.Statuses[i-1].Name {
				return migrationRefusal("invalid-native-status-catalog", status.Name, project.Prefix)
			}
			switch status.Category {
			case "active", "wip", "done", "frozen":
			default:
				return migrationRefusal("unknown-native-category", status.Category, project.Prefix, status.Name)
			}
		}
		for _, builtin := range nativeBuiltInStatuses() {
			if projectStatusCategory(project, builtin.Name) != builtin.Category {
				return migrationRefusal("native-builtin-conflict", builtin.Name, project.Prefix)
			}
		}
	}
	return nil
}

func validateNativeStatusCatalogConservation(manifest Manifest, records map[string][]Record, config configEntrySource) error {
	custom, err := sourcePlaneCatalogCategoriesFor(records, config, true)
	if err != nil {
		return err
	}
	expected := append([]ProjectStatus{}, nativeBuiltInStatuses()...)
	for name, category := range custom {
		expected = append(expected, ProjectStatus{name, category})
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].Name < expected[j].Name })
	for _, project := range manifest.Projects {
		if !equalJSON(expected, project.Statuses) {
			return migrationRefusal("native-status-catalog-source-mismatch", project.Prefix)
		}
	}
	return nil
}

// NativeStatusOntology returns detached documents derived only from validated
// per-project native catalogs. Source names and literal categories survive.
func (a *Archive) NativeStatusOntology() (NativeStatusOntology, bool) {
	if a == nil || a.manifest.StatusCategoryContract != StatusCategoryContract {
		return NativeStatusOntology{}, false
	}
	result := NativeStatusOntology{Base: nativeStatusDocument(nativeBuiltInStatuses()), Overlays: map[string]ontology.Document{}}
	for _, project := range a.manifest.Projects {
		result.Overlays[project.Prefix] = nativeStatusDocument(project.Statuses)
	}
	return result, true
}

func nativeStatusDocument(statuses []ProjectStatus) ontology.Document {
	doc := ontology.DefaultDocument()
	doc.Statuses = map[string]ontology.Status{}
	workflow := ontology.Workflow{Initial: "open", Slots: map[ontology.Slot]string{ontology.Claim: "in_progress", ontology.Close: "closed", ontology.Reopen: "open", ontology.Defer: "deferred"}}
	var active []string
	for _, status := range statuses {
		if status.Category == "active" {
			active = append(active, status.Name)
		}
	}
	for _, status := range statuses {
		entry := ontology.Status{Name: status.Name, Category: ontology.Category(status.Category)}
		if status.Category == "done" {
			entry.Outcome = ontology.Success
		}
		doc.Statuses[status.Name] = entry
		workflow.Statuses = append(workflow.Statuses, status.Name)
		rule := ontology.Rule{To: status.Name, Guards: []ontology.Guard{{Kind: ontology.Permission}}}
		switch status.Category {
		case "wip":
			rule.From = append([]string{}, active...)
			rule.Guards = []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}
		case "done":
			rule.Guards = []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}
		}
		workflow.Transitions = append(workflow.Transitions, rule)
	}
	doc.Workflows["issue"] = workflow
	doc.Types[ontology.GateType] = ontology.Type{Name: "Gate", Workflow: "issue", Fields: map[string]ontology.Field{
		"title": {Type: "string"}, "description": {Type: "string"},
		"labels": {Type: "strings"}, "blocked": {Type: "boolean"},
	}}
	return doc
}
