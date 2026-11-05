package plugdump

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

type mapping struct {
	sourcePlanes   bool
	customStatuses map[string]bool
	projects       []Project
	beads          map[string]string
	wisps          map[string]string
	records        map[string][]Record
	policy         map[string]string
	usedPolicy     map[string]bool
	ontology       []OntologyDirective
	usedOntology   map[string]bool
}

var builtInIssueTypes = map[string]bool{
	"task": true, "bug": true, "feature": true, "epic": true, "chore": true,
}

// wellKnownDependencyKinds is the closed set of names that already have a
// defined Beads/Plug meaning. A name in this set is never silently relabeled
// as an opaque custom edge when the producer does not support it.
var wellKnownDependencyKinds = map[string]struct{}{
	"blocks": {}, "parent-child": {}, "conditional-blocks": {}, "waits-for": {},
	"related": {}, "discovered-from": {}, "replies-to": {}, "relates-to": {},
	"duplicates": {}, "supersedes": {}, "authored-by": {}, "assigned-to": {},
	"approved-by": {}, "attests": {}, "tracks": {}, "until": {}, "caused-by": {},
	"validates": {}, "delegated-from": {},
}

// crossPlaneTypedDependencyKinds retains the existing wisp/external contract.
// The other fixed kinds are admitted only for durable issue-to-issue rows.
var crossPlaneTypedDependencyKinds = map[string]struct{}{
	"blocks": {}, "parent-child": {}, "related": {}, "discovered-from": {},
	"waits-for": {}, "conditional-blocks": {}, "tracks": {},
}

func isValidLocalOpaqueDependencyKind(kind string) bool {
	return kind != "" && utf8.ValidString(kind) && len(kind) <= 32 && func() bool {
		_, known := wellKnownDependencyKinds[kind]
		return !known
	}()
}

func isTypedDependencyKind(kind string) bool {
	_, ok := wellKnownDependencyKinds[kind]
	return ok
}

func supportsTypedDependency(kind, table, targetKind string) bool {
	if _, ok := crossPlaneTypedDependencyKinds[kind]; ok {
		return true
	}
	return isTypedDependencyKind(kind) && table == "dependencies" && targetKind == "depends_on_issue_id"
}

// These issue columns have no lossless Plug issue-body representation. They
// are retained verbatim in source.fields and are never interpreted as Plug
// workflow state by the exporter.
var opaqueIssueFields = [...]string{
	"agent_state", "await_id", "await_type", "ephemeral", "event_kind",
	"hook_bead", "is_template", "mol_type", "payload", "role_bead",
	"role_type", "target", "timeout_ns", "waiters", "wisp_type", "work_type",
}

func copyOntology(input []OntologyDirective, projects []Project) ([]OntologyDirective, error) {
	output := append([]OntologyDirective(nil), input...)
	seenIDs := map[string]bool{}
	seenTargets := map[string]bool{}
	knownProjects := map[string]bool{}
	for _, project := range projects {
		knownProjects[project.Prefix] = true
	}
	for _, directive := range output {
		if strings.TrimSpace(directive.ID) == "" {
			return nil, fmt.Errorf("ontology directive requires an ID")
		}
		if seenIDs[directive.ID] {
			return nil, fmt.Errorf("duplicate ontology directive ID")
		}
		seenIDs[directive.ID] = true
		if directive.Kind != "map_type" {
			return nil, fmt.Errorf("ontology directive %q must use map_type", directive.ID)
		}
		if directive.Target.Project == "" || directive.Target.Type == "" || directive.From == "" || directive.To == "" {
			return nil, fmt.Errorf("ontology directive %q requires project, source and target types", directive.ID)
		}
		if !knownProjects[directive.Target.Project] {
			return nil, fmt.Errorf("ontology directive %q targets an unaccounted project", directive.ID)
		}
		if builtInIssueTypes[directive.Target.Type] {
			return nil, fmt.Errorf("ontology directive %q cannot remap a built-in issue type", directive.ID)
		}
		if directive.From != directive.Target.Type {
			return nil, fmt.Errorf("ontology directive %q source must equal target type", directive.ID)
		}
		if directive.To != "issue" {
			return nil, fmt.Errorf("ontology directive %q must target the Plug issue kind", directive.ID)
		}
		key := directive.Target.Project + "\x00" + directive.Target.Type + "\x00" + directive.Target.ID
		if seenTargets[key] {
			return nil, fmt.Errorf("duplicate ontology directive target")
		}
		seenTargets[key] = true
	}
	sort.Slice(output, func(i, j int) bool {
		a, b := output[i], output[j]
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		if a.Target.Project != b.Target.Project {
			return a.Target.Project < b.Target.Project
		}
		if a.Target.Type != b.Target.Type {
			return a.Target.Type < b.Target.Type
		}
		return a.Target.ID < b.Target.ID
	})
	return output, nil
}

func (m *mapping) ontologyDirective(project, issueType, id string) error {
	if builtInIssueTypes[issueType] {
		return nil
	}
	var matches []OntologyDirective
	for _, directive := range m.ontology {
		if directive.Target.Project == project && directive.Target.Type == issueType && directive.From == issueType && (directive.Target.ID == "" || directive.Target.ID == id) {
			matches = append(matches, directive)
		}
	}
	if len(matches) != 1 {
		// Preserve the original closed-world refusal for callers that did not
		// provide a directive. Details stay out of the stable user-facing error.
		return fmt.Errorf("unsupported issue type; workflow/ontology mapping required")
	}
	m.usedOntology[matches[0].ID] = true
	return nil
}

func (m *mapping) add(kind, id, project, table string, body map[string]any, row Row) {
	m.records[kind] = append(m.records[kind], Record{kind, id, project, body, Provenance{table, cloneRow(row)}})
}

func (m *mapping) project(id string) (*Project, error) {
	for i := range m.projects {
		if strings.HasPrefix(id, m.projects[i].Prefix+"-") {
			return &m.projects[i], nil
		}
	}
	return nil, fmt.Errorf("issue has no inventoried project prefix")
}

func (m *mapping) wispRows(rows []Row) error {
	seen := map[string]bool{}
	for _, row := range rows {
		if err := validateRowUTF8(row); err != nil {
			return fmt.Errorf("wisp row: %w", err)
		}
		id, err := requiredText(row, "id")
		if err != nil {
			return fmt.Errorf("invalid wisp identity")
		}
		project, err := m.project(id)
		if err != nil || seen[id] || m.wisps[id] != "" {
			return fmt.Errorf("unknown project, empty or duplicate wisp ID")
		}
		status, statusErr := requiredText(row, "status")
		if statusErr != nil || !supportedSourceStatus(status) && !(m.sourcePlanes && m.customStatuses[status]) {
			return fmt.Errorf("unsupported wisp status")
		}
		if strings.TrimSpace(text(row, "title")) == "" {
			return fmt.Errorf("wisp requires a title")
		}
		if priority, priorityErr := natural(row["priority"]); priorityErr != nil || priority > 4 {
			return fmt.Errorf("wisp requires a priority from 0 to 4")
		}
		m.wisps[id] = project.Prefix
		seen[id] = true
	}
	return nil
}

var catalogStatusNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

func validCatalogName(name string) bool {
	if name == "" || len(name) > 64 || !utf8.ValidString(name) {
		return false
	}
	return catalogStatusNamePattern.MatchString(name)
}

// validCatalogTypeName follows the canonical custom-type parser: entries are
// trimmed, nonempty UTF-8 values bounded by the VARCHAR(64) source column. A
// custom type is not restricted to the status-name alphabet.
func validCatalogTypeName(name string) bool {
	return name != "" && name == strings.TrimSpace(name) && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64
}

type catalogStatus struct {
	name     string
	category string
}

func validateCatalogUnion(statusRows, typeRows []Row, storedConfig map[string]any, yamlValues []map[string]any) error {
	return readCatalogUnion(statusRows, typeRows, storedConfig, yamlValues, nil)
}

// readCatalogUnion validates the same strict SQL/config/YAML union and may
// return its declared status names for raw active-plane capture. It does not
// select consumer workflow or lifetime semantics.
func readCatalogUnion(statusRows, typeRows []Row, storedConfig map[string]any, yamlValues []map[string]any, customStatuses map[string]bool) error {
	if len(statusRows) > 50 {
		return fmt.Errorf("too many custom statuses")
	}
	statusNames := map[string]bool{"open": true, "in_progress": true, "blocked": true, "deferred": true, "closed": true, "pinned": true, "hooked": true}
	statusCategories := map[string]string{}
	customStatusCount := 0
	addStatus := func(name, category string) error {
		lower := strings.ToLower(name)
		if !validCatalogName(name) || statusNames[lower] && statusCategories[lower] == "" {
			return fmt.Errorf("invalid or colliding custom status %q", name)
		}
		if category != "active" && category != "wip" && category != "done" && category != "frozen" {
			return fmt.Errorf("custom status %q requires an explicit active, wip, done or frozen category", name)
		}
		if existing, ok := statusCategories[lower]; ok {
			if existing == category {
				return nil
			}
			return fmt.Errorf("conflicting custom status declarations %q and %q", name+":"+existing, name+":"+category)
		}
		statusNames[lower] = true
		statusCategories[lower] = category
		customStatusCount++
		if customStatusCount > 50 {
			return fmt.Errorf("too many custom statuses")
		}
		return nil
	}
	seenStatusRows := map[string]bool{}
	for _, row := range statusRows {
		name, category := text(row, "name"), text(row, "category")
		if seenStatusRows[strings.ToLower(name)] {
			return fmt.Errorf("duplicate custom status row %q", name)
		}
		seenStatusRows[strings.ToLower(name)] = true
		if err := addStatus(name, category); err != nil {
			return err
		}
	}
	typeNames := map[string]bool{}
	addType := func(name string) error {
		if !validCatalogTypeName(name) {
			return fmt.Errorf("invalid custom type %q", name)
		}
		if typeNames[name] {
			return nil
		}
		typeNames[name] = true
		return nil
	}
	seenTypeRows := map[string]bool{}
	for _, row := range typeRows {
		name := text(row, "name")
		if seenTypeRows[name] {
			return fmt.Errorf("duplicate custom type row %q", name)
		}
		seenTypeRows[name] = true
		if err := addType(name); err != nil {
			return err
		}
	}
	if err := addCatalogConfig(storedConfig, addStatus, addType); err != nil {
		return err
	}
	for _, values := range yamlValues {
		if err := addCatalogConfig(values, addStatus, addType); err != nil {
			return err
		}
	}
	for name := range statusCategories {
		if customStatuses != nil {
			customStatuses[name] = true
		}
	}
	return nil
}

func addCatalogConfig(values map[string]any, addStatus func(string, string) error, addType func(string) error) error {
	if values == nil {
		return nil
	}
	if raw, ok := values["status.custom"]; ok {
		statuses, err := parseCatalogStatuses(raw)
		if err != nil {
			return err
		}
		for _, status := range statuses {
			if err := addStatus(status.name, status.category); err != nil {
				return err
			}
		}
	}
	if raw, ok := values["types.custom"]; ok {
		typesList, err := parseCatalogTypes(raw)
		if err != nil {
			return err
		}
		for _, name := range typesList {
			if err := addType(name); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"status", "types"} {
		raw, present := values[key]
		if !present {
			continue
		}
		nested, ok := raw.(map[string]any)
		if !ok || nested == nil {
			return fmt.Errorf("%s catalog declaration must be an object", key)
		}
		if key == "status" {
			if raw, ok := nested["custom"]; ok {
				statuses, err := parseCatalogStatuses(raw)
				if err != nil {
					return err
				}
				for _, status := range statuses {
					if err := addStatus(status.name, status.category); err != nil {
						return err
					}
				}
			}
		} else if raw, ok := nested["custom"]; ok {
			typesList, err := parseCatalogTypes(raw)
			if err != nil {
				return err
			}
			for _, name := range typesList {
				if err := addType(name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func parseCatalogStatuses(value any) ([]catalogStatus, error) {
	var parts []string
	switch value := value.(type) {
	case string:
		parts = strings.Split(value, ",")
	case []any:
		for _, item := range value {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("invalid custom status catalog value")
			}
			parts = append(parts, name)
		}
	default:
		return nil, fmt.Errorf("invalid custom status catalog value")
	}
	var result []catalogStatus
	seen := map[string]bool{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid custom status catalog value: empty declaration")
		}
		index := strings.IndexByte(part, ':')
		if index <= 0 || index == len(part)-1 {
			return nil, fmt.Errorf("invalid custom status declaration %q: explicit category required", part)
		}
		name, category := part[:index], part[index+1:]
		if name == "" || category == "" {
			return nil, fmt.Errorf("invalid custom status declaration %q: explicit name and category required", part)
		}
		nameKey := strings.ToLower(name)
		if seen[nameKey] {
			return nil, fmt.Errorf("duplicate custom status catalog declaration %q", name)
		}
		seen[nameKey] = true
		result = append(result, catalogStatus{name: name, category: category})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("invalid custom status catalog value: empty declaration")
	}
	return result, nil
}

func parseCatalogTypes(value any) ([]string, error) {
	var result []string
	switch value := value.(type) {
	case string:
		if strings.HasPrefix(strings.TrimSpace(value), "[") {
			var list []string
			if err := json.Unmarshal([]byte(value), &list); err != nil {
				return nil, fmt.Errorf("invalid custom type catalog value")
			}
			result = list
		} else {
			result = strings.Split(value, ",")
		}
	case []any:
		for _, item := range value {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("invalid custom type catalog value")
			}
			result = append(result, name)
		}
	default:
		return nil, fmt.Errorf("invalid custom type catalog value")
	}
	var cleaned []string
	seen := map[string]bool{}
	for _, name := range result {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("invalid custom type catalog value: empty declaration")
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate custom type catalog declaration %q", name)
		}
		seen[name] = true
		cleaned = append(cleaned, name)
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("invalid custom type catalog value: empty declaration")
	}
	return cleaned, nil
}

func (m *mapping) wispAuxiliary(table string, rows []Row) error {
	seen := map[string]bool{}
	labels := map[string]map[string]bool{}
	for _, row := range rows {
		if err := validateRowUTF8(row); err != nil {
			return fmt.Errorf("%s row: %w", table, err)
		}
		id := text(row, "issue_id")
		if table == "wisp_child_counters" {
			id = text(row, "parent_id")
			if _, err := natural(row["last_child"]); err != nil {
				return fmt.Errorf("invalid wisp child counter")
			}
		}
		if id == "" || m.wisps[id] == "" {
			return fmt.Errorf("dangling wisp %s row", table)
		}
		if !utf8.ValidString(id) {
			return fmt.Errorf("invalid UTF-8 wisp %s row", table)
		}
		if table == "wisp_labels" && text(row, "label") == "" {
			return fmt.Errorf("empty wisp label")
		}
		if m.sourcePlanes && table == "wisp_labels" {
			if labels[id] == nil {
				labels[id] = map[string]bool{}
			}
			label := text(row, "label")
			if labels[id][label] {
				return fmt.Errorf("duplicate wisp label identity")
			}
			labels[id][label] = true
		}
		if m.sourcePlanes && table == "wisp_child_counters" {
			if seen[id] {
				return fmt.Errorf("duplicate wisp child counter identity")
			}
			seen[id] = true
		}
		if table == "wisp_comments" || table == "wisp_events" {
			rowID, err := requiredText(row, "id")
			if err != nil || seen[rowID] {
				return fmt.Errorf("empty or duplicate wisp %s identity", table)
			}
			seen[rowID] = true
		}
	}
	return nil
}

func supportedSourceStatus(status string) bool {
	switch status {
	case "open", "in_progress", "blocked", "deferred", "pinned", "frozen", "hooked", "closed":
		return true
	default:
		return false
	}
}

func requiredText(row Row, key string) (string, error) {
	value, ok := row[key]
	if !ok {
		return "", fmt.Errorf("missing %s", key)
	}
	textValue, ok := value.(string)
	if !ok || textValue == "" || !utf8.ValidString(textValue) {
		return "", fmt.Errorf("invalid %s", key)
	}
	return textValue, nil
}

func validateRowUTF8(row Row) error {
	for key, value := range row {
		if !utf8.ValidString(key) {
			return fmt.Errorf("invalid UTF-8 source field")
		}
		if err := validateSourceValue(value); err != nil {
			return fmt.Errorf("field %s: %w", key, err)
		}
	}
	return nil
}

func validateSourceValue(value any) error {
	switch value := value.(type) {
	case string:
		if !utf8.ValidString(value) {
			return fmt.Errorf("invalid UTF-8 value")
		}
	case json.Number:
		if !utf8.ValidString(string(value)) {
			return fmt.Errorf("invalid UTF-8 number")
		}
	case []byte:
		// SQL BLOBs are represented by the lossless {$bytes:...} envelope before
		// mapping. A raw byte slice cannot be serialized without changing it.
		return fmt.Errorf("raw bytes require a lossless source envelope")
	case map[string]any:
		for key, item := range value {
			if !utf8.ValidString(key) {
				return fmt.Errorf("invalid UTF-8 map key")
			}
			if err := validateSourceValue(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := validateSourceValue(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *mapping) issues(rows []Row) error {
	for _, row := range rows {
		if err := validateRowUTF8(row); err != nil {
			return fmt.Errorf("issue row: %w", err)
		}
		id := text(row, "id")
		project, err := m.project(id)
		if err != nil || id == "" || m.beads[id] != "" {
			return fmt.Errorf("unknown project, empty or duplicate issue ID")
		}
		top, parent, child, err := issueIDParts(id, *project)
		if err != nil {
			return err
		}
		project.LastID = max(project.LastID, top)
		if parent != "" {
			project.ChildCounters[parent] = max(project.ChildCounters[parent], child)
		}
		priority, err := natural(row["priority"])
		if strings.TrimSpace(text(row, "title")) == "" || err != nil || priority > 4 {
			return fmt.Errorf("issue requires a title and priority from 0 to 4")
		}
		category, result := "", ""
		switch text(row, "status") {
		case "open":
			category = "unstarted"
		case "in_progress", "hooked":
			category = "started"
		case "blocked", "deferred", "pinned", "frozen":
			category = "backlog"
		case "closed":
			category, result = "completed", "success"
		default:
			return fmt.Errorf("unsupported source status; explicit overlay mapping required")
		}
		issueType := text(row, "issue_type")
		if err := m.ontologyDirective(project.Prefix, issueType, id); err != nil {
			return err
		}
		if class := text(row, "storage_class"); class != "" && class != "versioned" {
			return fmt.Errorf("unsupported issue storage class")
		}
		fields, err := copyIssueSourceFields(row)
		if err != nil {
			return err
		}
		body := map[string]any{"type": "issue", "source_type": row["issue_type"], "title": row["title"], "status_category": category, "labels": []string{}}
		if result != "" {
			body["result"] = result
		}
		for _, key := range []string{"description", "design", "acceptance_criteria", "notes", "priority", "assignee", "owner", "created_at", "created_by", "updated_at", "closed_at", "due_at", "defer_until", "started_at", "metadata", "spec_id", "external_ref", "pinned"} {
			body[key] = row[key]
		}
		m.beads[id] = project.Prefix
		m.add("bead", id, project.Prefix, "issues", body, fields)
	}
	return nil
}

func copyIssueSourceFields(row Row) (Row, error) {
	if err := validateIssueSourceFields(row); err != nil {
		return nil, err
	}
	fields := cloneRow(row)
	delete(fields, "row_lock")
	delete(fields, "is_blocked")
	return fields, nil
}

// validateIssueSourceFields is the producer half of the native source-fields
// contract. Every physical opaque issue column must be present before mapping;
// the exporter may omit only transient row_lock and derived is_blocked from the
// bead body while retaining all other source fields losslessly.
func validateIssueSourceFields(row Row) error {
	for _, key := range opaqueIssueFields {
		if _, ok := row[key]; !ok {
			return fmt.Errorf("missing source field in issues")
		}
	}
	return nil
}

func (m *mapping) allocator(tables map[string]Table) error {
	seen := map[string]bool{}
	for _, row := range tables["issue_counter"].Rows {
		prefix := text(row, "prefix")
		found := false
		for i := range m.projects {
			if m.projects[i].Prefix == prefix && !seen[prefix] {
				count, err := natural(row["last_id"])
				if err != nil {
					return fmt.Errorf("invalid top-level counter")
				}
				m.projects[i].LastID = max(m.projects[i].LastID, count)
				found, seen[prefix] = true, true
				break
			}
		}
		if !found {
			return fmt.Errorf("duplicate or unaccounted counter prefix")
		}
	}
	for _, row := range tables["child_counters"].Rows {
		parent := text(row, "parent_id")
		project, err := m.project(parent)
		count, countErr := natural(row["last_child"])
		if err != nil || countErr != nil || m.beads[parent] == "" {
			return fmt.Errorf("dangling or invalid child counter")
		}
		project.ChildCounters[parent] = max(project.ChildCounters[parent], count)
	}
	for _, project := range m.projects {
		for parent := range project.ChildCounters {
			if m.beads[parent] != project.Prefix {
				return fmt.Errorf("unresolved child-counter parent")
			}
		}
	}
	return nil
}

func (m *mapping) row(table string, row Row) error {
	if err := validateRowUTF8(row); err != nil {
		return fmt.Errorf("%s row: %w", table, err)
	}
	switch table {
	case "dependencies", "wisp_dependencies":
		if err := m.dependency(table, row); err != nil {
			return err
		}
	case "comments":
		id, owner := text(row, "id"), text(row, "issue_id")
		if id == "" || m.beads[owner] == "" {
			return fmt.Errorf("missing comment identity or dangling owner")
		}
		m.add("comment", id, m.beads[owner], table, map[string]any(row), row)
	case "config", "metadata":
		key := text(row, "key")
		policy := m.policy[table+"/"+key]
		if key == "" || (policy != "secret" && policy != "public") || (secretKey(key) && policy != "secret") {
			return fmt.Errorf("config/metadata key requires explicit public/secret policy; credential digests excluded")
		}
		m.usedPolicy[table+"/"+key] = true
		fields := cloneRow(row)
		if policy == "secret" {
			fields["value"] = map[string]string{"$secret": table + "/" + key}
		}
		kind, id := "config", table+"/"+key
		if strings.HasPrefix(key, "kv.memory.") && table == "config" {
			kind, id = "memory", strings.TrimPrefix(key, "kv.memory.")
			if id == "" {
				return fmt.Errorf("empty memory key")
			}
		}
		m.add(kind, id, "", table, map[string]any{"key": id, "value": fields["value"], "classification": policy}, fields)
	default:
		data, err := marshal(row)
		if err != nil {
			return fmt.Errorf("unserializable source family %s", table)
		}
		m.add("beads-archive", table+"/"+digest(data), "", table, map[string]any{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}, row)
	}
	return nil
}

func (m *mapping) dependency(table string, row Row) error {
	id, err := requiredText(row, "id")
	if err != nil {
		return fmt.Errorf("missing edge identity")
	}
	from, err := requiredText(row, "issue_id")
	if err != nil {
		return fmt.Errorf("missing edge source")
	}
	project := ""
	if table == "dependencies" {
		project = m.beads[from]
	} else {
		project = m.wisps[from]
	}
	if project == "" {
		return fmt.Errorf("dangling or cross-plane edge source")
	}

	target, targetKind, err := m.dependencyTarget(row)
	if err != nil {
		return err
	}
	kind, err := requiredText(row, "type")
	if err != nil || len(kind) > 32 {
		return fmt.Errorf("unsupported dependency type")
	}
	if !supportsTypedDependency(kind, table, targetKind) && !(targetKind == "depends_on_issue_id" && isValidLocalOpaqueDependencyKind(kind)) {
		return fmt.Errorf("unsupported dependency type")
	}
	if m.sourcePlanes && table != "dependencies" && isValidLocalOpaqueDependencyKind(kind) {
		return fmt.Errorf("local opaque dependency requires a durable issue source")
	}
	if err := validateDependencyMetadata(kind, row["metadata"]); err != nil {
		return fmt.Errorf("dependency %s metadata: %w", kind, err)
	}
	m.add("edge", id, project, table, map[string]any{
		"from": from, "to": target, "target_kind": targetKind, "type": row["type"],
		"metadata": row["metadata"], "thread_id": row["thread_id"],
	}, row)
	return nil
}

func (m *mapping) dependencyTarget(row Row) (string, string, error) {
	var target, targetKind string
	for _, key := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
		value, present := row[key]
		if !present || value == nil {
			continue
		}
		if targetKind != "" {
			return "", "", fmt.Errorf("edge must have exactly one typed target")
		}
		candidate, ok := value.(string)
		if !ok || candidate == "" || !utf8.ValidString(candidate) {
			return "", "", fmt.Errorf("empty, wrong-type or invalid UTF-8 edge target")
		}
		target, targetKind = candidate, key
	}
	if targetKind == "" {
		return "", "", fmt.Errorf("edge must have exactly one typed target")
	}
	switch targetKind {
	case "depends_on_issue_id":
		if m.beads[target] == "" {
			return "", "", fmt.Errorf("dangling issue edge target")
		}
	case "depends_on_wisp_id":
		if m.wisps[target] == "" {
			return "", "", fmt.Errorf("dangling wisp edge target")
		}
	case "depends_on_external":
		if !validExternalDependencyTarget(target) {
			return "", "", fmt.Errorf("invalid external edge target")
		}
	default:
		return "", "", fmt.Errorf("unsupported edge target arm")
	}
	return target, targetKind, nil
}

func validExternalDependencyTarget(target string) bool {
	if !utf8.ValidString(target) || !strings.HasPrefix(target, "external:") {
		return false
	}
	parts := strings.SplitN(target, ":", 3)
	return len(parts) == 3 && parts[1] != "" && parts[2] != "" &&
		strings.TrimSpace(parts[1]) == parts[1] && strings.TrimSpace(parts[2]) == parts[2]
}

// validateDependencyMetadata checks the source JSON without rewriting it. The
// exporter keeps the original metadata in both the typed body and provenance;
// this guard only prevents an invalid JSON cell, or a waits-for object with
// malformed gate members, from becoming an archive that cannot be read back.
func validateDependencyMetadata(kind string, value any) error {
	data, err := dependencyMetadataJSON(value)
	if err != nil {
		return err
	}
	if len(data) == 0 || kind != "waits-for" {
		return nil
	}
	var object map[string]any
	if err := decodeJSON(data, &object); err != nil || object == nil {
		return fmt.Errorf("waits-for metadata must be a JSON object")
	}
	if gate, ok := object["gate"]; ok {
		value, ok := gate.(string)
		if !ok || value != "" && value != "all-children" && value != "any-children" {
			return fmt.Errorf("waits-for gate must be all-children or any-children")
		}
	}
	if alsoBlocks, ok := object["also_blocks"]; ok {
		if _, ok := alsoBlocks.(bool); !ok {
			return fmt.Errorf("waits-for also_blocks must be boolean")
		}
	}
	if spawner, ok := object["spawner_id"]; ok {
		if _, ok := spawner.(string); !ok {
			return fmt.Errorf("waits-for spawner_id must be text")
		}
	}
	return nil
}

func dependencyMetadataJSON(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(strings.TrimSpace(string(value))) == 0 {
			return nil, nil
		}
		if !json.Valid(value) {
			return nil, fmt.Errorf("metadata is not valid JSON")
		}
		return append([]byte(nil), value...), nil
	case string:
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		data := []byte(value)
		if !json.Valid(data) {
			return nil, fmt.Errorf("metadata is not valid JSON")
		}
		return data, nil
	case []byte:
		if len(strings.TrimSpace(string(value))) == 0 {
			return nil, nil
		}
		if !json.Valid(value) {
			return nil, fmt.Errorf("metadata is not valid JSON")
		}
		return append([]byte(nil), value...), nil
	default:
		data, err := json.Marshal(value)
		if err != nil || !json.Valid(data) {
			return nil, fmt.Errorf("metadata is not valid JSON")
		}
		return data, nil
	}
}

func (m *mapping) labels(rows []Row) error {
	labels := map[string][]string{}
	seen := map[string]bool{}
	for _, row := range rows {
		id, label := text(row, "issue_id"), text(row, "label")
		if m.beads[id] == "" || label == "" || seen[id+"\x00"+label] {
			return fmt.Errorf("dangling, empty or duplicate label")
		}
		seen[id+"\x00"+label] = true
		labels[id] = append(labels[id], label)
	}
	for i := range m.records["bead"] {
		record := &m.records["bead"][i]
		if values := labels[record.ID]; len(values) > 0 {
			sort.Strings(values)
			record.Body["labels"] = values
		}
	}
	return nil
}
