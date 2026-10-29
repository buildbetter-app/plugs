package plugdump

import (
	"fmt"
	"sort"
	"strings"
)

type mapping struct {
	projects   []Project
	beads      map[string]string
	records    map[string][]Record
	policy     map[string]string
	usedPolicy map[string]bool
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
		switch text(row, "issue_type") {
		case "task", "bug", "feature", "epic", "chore":
		default:
			return fmt.Errorf("unsupported issue type; workflow/ontology mapping required")
		}
		// Even in the durable table these flags can identify a promoted wisp.
		// Refuse active workflow semantics rather than pretend they are issues.
		for _, key := range []string{"ephemeral", "is_template"} {
			if active(row[key]) {
				return fmt.Errorf("unsupported issue flag %s", key)
			}
		}
		if class := text(row, "storage_class"); class != "" && class != "versioned" {
			return fmt.Errorf("unsupported issue storage class")
		}
		for _, key := range []string{"await_type", "await_id", "mol_type", "hook_bead", "role_bead", "agent_state", "event_kind"} {
			if text(row, key) != "" {
				return fmt.Errorf("unsupported active workflow field %s", key)
			}
		}
		fields := cloneRow(row)
		delete(fields, "row_lock")
		delete(fields, "is_blocked")
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

func active(value any) bool {
	if value == nil || value == false || value == "" {
		return false
	}
	n, err := natural(value)
	return err != nil || n != 0
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
		switch text(row, "type") {
		case "blocks", "parent-child", "related", "discovered-from":
		default:
			return fmt.Errorf("unsupported dependency type")
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
