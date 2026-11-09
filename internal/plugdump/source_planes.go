package plugdump

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/steveyegge/beads/internal/types"
	"strings"
)

func validateSourcePlaneContract(contract string) error {
	if contract != "" && contract != SourcePlaneAddressContract {
		return fmt.Errorf("unsupported source plane address contract")
	}
	return nil
}

// JSON presence is distinct from a typed caller's legacy zero value: explicit
// null/empty and unknown values must not silently become legacy omission.
func validateSourcePlaneMarker(data []byte, contract string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key := range fields {
		// encoding/json accepts folded matches for tagged struct fields. Such
		// aliases must not turn a present marker into legacy omission or enable
		// the capability without its canonical wire spelling.
		if key != "source_plane_address_contract" && strings.EqualFold(key, "source_plane_address_contract") {
			return fmt.Errorf("noncanonical source_plane_address_contract key %q", key)
		}
	}
	if _, present := fields["source_plane_address_contract"]; present && contract != SourcePlaneAddressContract {
		return fmt.Errorf("unsupported source plane address contract")
	}
	return validateSourcePlaneContract(contract)
}

func compareRecords(a, b Record, contract string) int {
	if contract != SourcePlaneAddressContract || a.Kind != "edge" || b.Kind != "edge" {
		return strings.Compare(recordKey(a), recordKey(b))
	}
	// Compare valid UTF-8 components bytewise, not delimiter-concatenated keys.
	// Raw physical IDs are unchanged, including equal IDs on different planes.
	if c := strings.Compare(a.Project, b.Project); c != 0 {
		return c
	}
	if c := strings.Compare(a.Source.Table, b.Source.Table); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

func validateSourcePlanes(projects []Project, beads, edges []Record, rows map[string][]Row, customStatuses map[string]bool) error {
	m := mapping{projects: projects, beads: map[string]string{}, wisps: map[string]string{}, records: map[string][]Record{}, sourcePlanes: true, customStatuses: customStatuses, statusCategories: len(projects) > 0 && len(projects[0].Statuses) > 0}
	for _, bead := range beads {
		m.beads[bead.ID] = bead.Project
	}
	if err := m.wispRows(rows["wisps"]); err != nil {
		return err
	}
	for _, table := range []string{"wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters"} {
		if err := m.wispAuxiliary(table, rows[table]); err != nil {
			return err
		}
	}
	for _, edge := range edges {
		if err := m.dependency(edge.Source.Table, edge.Source.Fields); err != nil {
			return err
		}
		mapped := m.records["edge"][len(m.records["edge"])-1]
		if edge.Project != mapped.Project {
			return fmt.Errorf("edge project does not match its physical source plane")
		}
	}
	return nil
}

// Native names remain authoritative. The public category function, rather than
// the legacy Plug-body mapper, defines the native built-in behavior.
func nativeBuiltInStatuses() []ProjectStatus {
	statuses := []ProjectStatus{}
	for _, name := range []types.Status{types.StatusBlocked, types.StatusClosed, types.StatusDeferred, types.StatusHooked, types.StatusInProgress, types.StatusOpen, types.StatusPinned} {
		statuses = append(statuses, ProjectStatus{Name: string(name), Category: string(types.BuiltInStatusCategory(name))})
	}
	return statuses
}

func validateStatusCategoryContract(contract string, projects []Project) error {
	if contract != "" && contract != StatusCategoryContract {
		return fmt.Errorf("unsupported status category contract")
	}
	for _, project := range projects {
		if contract == "" {
			if project.Statuses != nil {
				return fmt.Errorf("project statuses require status category contract")
			}
			continue
		}
		if len(project.Statuses) < len(nativeBuiltInStatuses()) || len(project.Statuses) > 57 {
			return fmt.Errorf("project status catalog must include native builtins and at most 50 custom statuses")
		}
		for i, status := range project.Statuses {
			if !validCatalogName(status.Name) || i > 0 && status.Name <= project.Statuses[i-1].Name || !validNativeCategory(status.Category) {
				return fmt.Errorf("project statuses require sorted unique names and explicit native categories")
			}
		}
		for _, status := range nativeBuiltInStatuses() {
			if projectStatusCategory(project, status.Name) != status.Category {
				return fmt.Errorf("project status catalog disagrees with native builtins")
			}
		}
	}
	return nil
}

func validNativeCategory(category string) bool {
	return category == "active" || category == "wip" || category == "done" || category == "frozen"
}

func projectStatusCategory(project Project, name string) string {
	for _, status := range project.Statuses {
		if status.Name == name {
			return status.Category
		}
	}
	return ""
}

func validateStatusCategoryMarker(data []byte, contract string, projects []Project) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key := range fields {
		if key != "status_category_contract" && strings.EqualFold(key, "status_category_contract") {
			return fmt.Errorf("noncanonical status_category_contract key")
		}
	}
	if _, present := fields["status_category_contract"]; present && contract != StatusCategoryContract {
		return fmt.Errorf("unsupported status category contract")
	}
	return validateStatusCategoryContract(contract, projects)
}

func declaredStatusCatalog(statusRows, typeRows []Row, config map[string]any, yamlValues []map[string]any) ([]ProjectStatus, error) {
	categories := map[string]string{}
	if err := readCatalogUnionCategories(statusRows, typeRows, config, yamlValues, nil, categories); err != nil {
		return nil, err
	}
	catalog := nativeBuiltInStatuses()
	for name, category := range categories {
		catalog = append(catalog, ProjectStatus{Name: name, Category: category})
	}
	sort.Slice(catalog, func(i, j int) bool { return catalog[i].Name < catalog[j].Name })
	return catalog, nil
}

func validateProjectStatusCatalogs(projects []Project, statusRows, typeRows []Row, config map[string]any, yamlValues []map[string]any) error {
	catalog, err := declaredStatusCatalog(statusRows, typeRows, config, yamlValues)
	if err != nil {
		return err
	}
	for _, project := range projects {
		if !equalJSON(project.Statuses, catalog) {
			return fmt.Errorf("project status catalog disagrees with captured SQL/config/YAML declarations")
		}
	}
	return nil
}

func validateStatusRecord(record Record, projects []Project) error {
	if len(projects) > 0 && len(projects[0].Statuses) == 0 && validNativeCategory(text(record.Body, "status_category")) {
		return fmt.Errorf("native category body requires status category contract")
	}
	for _, project := range projects {
		if project.Prefix != record.Project || project.Statuses == nil {
			continue
		}
		category := projectStatusCategory(project, text(record.Source.Fields, "status"))
		if category == "" || text(record.Body, "status_category") != category {
			return fmt.Errorf("source status/category disagrees with its project catalog")
		}
		result, present := record.Body["result"]
		if category == "done" && result != "success" || category != "done" && present {
			return fmt.Errorf("source status result disagrees with its native category")
		}
	}
	return nil
}

func verificationLimitsForStatus(contract string) map[string]string {
	limits := verificationLimits()
	if contract == StatusCategoryContract {
		limits["ontology"] = "native status names/categories retained per project; explicit map_type directives recorded in manifest"
	}
	return limits
}
