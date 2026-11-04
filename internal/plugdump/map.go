package plugdump

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type mapping struct {
	projects     []Project
	beads        map[string]string
	records      map[string][]Record
	policy       map[string]string
	usedPolicy   map[string]bool
	ontology     []OntologyDirective
	usedOntology map[string]bool
}

var builtInIssueTypes = map[string]bool{
	"task": true, "bug": true, "feature": true, "epic": true, "chore": true,
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

func (m *mapping) issues(rows []Row) error {
	for _, row := range rows {
		id := text(row, "id")
		project, err := m.project(id)
		if err != nil || id == "" || m.beads[id] != "" {
			return fmt.Errorf("unknown project, empty or duplicate issue ID")
		}
		idBody := strings.TrimPrefix(id, project.Prefix+"-")
		parts := strings.Split(idBody, ".")
		if project.IDMode == "sequential" {
			number, err := natural(parts[0])
			if err != nil || number == 0 {
				return fmt.Errorf("issue ID does not match sequential allocator")
			}
			project.LastID = max(project.LastID, number)
		} else if !hashPattern.MatchString(parts[0]) || len(parts[0]) < project.HashLength {
			return fmt.Errorf("issue ID does not match hash allocator")
		}
		if len(parts) > 1 {
			for _, part := range parts[1:] {
				number, err := natural(part)
				if err != nil || number == 0 {
					return fmt.Errorf("invalid child ID")
				}
			}
			parent := id[:strings.LastIndex(id, ".")]
			number, err := natural(parts[len(parts)-1])
			if err != nil || number == 0 {
				return fmt.Errorf("invalid child ID")
			}
			project.ChildCounters[parent] = max(project.ChildCounters[parent], number)
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
	fields := cloneRow(row)
	for _, key := range opaqueIssueFields {
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("missing source field in issues")
		}
	}
	delete(fields, "row_lock")
	delete(fields, "is_blocked")
	return fields, nil
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
	switch table {
	case "dependencies":
		id, from := text(row, "id"), text(row, "issue_id")
		if id == "" || m.beads[from] == "" {
			return fmt.Errorf("missing edge identity or dangling source")
		}
		var target, targetKind string
		for _, key := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
			if text(row, key) != "" {
				if target != "" {
					return fmt.Errorf("edge must have exactly one typed target")
				}
				target, targetKind = text(row, key), key
			}
		}
		if target == "" || targetKind == "depends_on_wisp_id" || (targetKind == "depends_on_issue_id" && m.beads[target] == "") {
			return fmt.Errorf("missing, wisp or dangling edge target")
		}
		kind := text(row, "type")
		switch kind {
		case "blocks", "parent-child", "related", "discovered-from", "waits-for", "conditional-blocks":
		default:
			return fmt.Errorf("unsupported dependency type")
		}
		if err := validateDependencyMetadata(kind, row["metadata"]); err != nil {
			return fmt.Errorf("dependency %s metadata: %w", kind, err)
		}
		m.add("edge", id, m.beads[from], table, map[string]any{"from": from, "to": target, "target_kind": targetKind, "type": row["type"], "metadata": row["metadata"], "thread_id": row["thread_id"]}, row)
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
