package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// UnresolvedDependencyError identifies a source dependency whose endpoint is
// not admitted by the archive or restore plan.  Callers can use errors.As to
// distinguish a typed, state-preserving refusal from malformed source data.
// The source and target values are retained exactly as they appeared in the
// source edge; no endpoint is guessed or substituted.
type UnresolvedDependencyError struct {
	EdgeID     string `json:"edge_id"`
	Project    string `json:"project"`
	Source     string `json:"source"`
	Target     string `json:"target"`
	TargetKind string `json:"target_kind"`
}

func (e *UnresolvedDependencyError) Error() string {
	if e == nil {
		return "unresolved dependency target"
	}
	return fmt.Sprintf("unresolved dependency target: edge %s project %s source %s target %s kind %s", e.EdgeID, e.Project, e.Source, e.Target, e.TargetKind)
}

var hashPattern = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

func validateRecord(record Record, projects []Project, counts map[string]int) error {
	return validateMigratedRecord(record, projects, counts, nil)
}

func validateMigratedRecord(record Record, projects []Project, counts map[string]int, migration *ImportMigration) error {
	table, fields := record.Source.Table, record.Source.Fields
	columns, ok := tableColumns[table]
	if !ok {
		return fmt.Errorf("unknown record source table")
	}
	if migrationAction(table, migration) == "refuse-if-nonempty" || table == "labels" || table == "repo_mtimes" || table == "leases" {
		return fmt.Errorf("unsupported or operational source record")
	}
	expectedKind := "beads-archive"
	switch table {
	case "issues":
		expectedKind = "bead"
	case "dependencies":
		expectedKind = "edge"
	case "comments":
		expectedKind = "comment"
	case "config", "metadata":
		expectedKind = "config"
		if table == "config" && strings.HasPrefix(stringValue(fields["key"]), "kv.memory.") {
			expectedKind = "memory"
		}
	}
	if record.Kind != expectedKind {
		return fmt.Errorf("unsupported kind/source mapping")
	}
	names := []string{}
	for _, column := range strings.Fields(columns) {
		if table == "issues" && (column == "row_lock" || column == "is_blocked") {
			continue
		}
		names = append(names, column)
	}
	if len(names) != len(fields) {
		return fmt.Errorf("source record schema drift")
	}
	for _, column := range names {
		if _, ok := fields[column]; !ok {
			return fmt.Errorf("missing source column")
		}
	}
	if strings.ContainsRune(record.ID, '\x00') || strings.TrimSpace(record.ID) != record.ID {
		return fmt.Errorf("invalid record identity")
	}
	if expectedKind == "bead" || expectedKind == "edge" || expectedKind == "comment" {
		if record.ID != stringValue(fields["id"]) || !projectKnown(record.Project, projects) {
			return fmt.Errorf("record/source identity or project mismatch")
		}
	} else if record.Project != "" {
		return fmt.Errorf("unsupported project binding")
	}

	var body Row
	switch expectedKind {
	case "bead":
		p := findProject(record.Project, projects)
		if err := checkID(record.ID, p); err != nil {
			return err
		}
		priority, err := natural(fields["priority"])
		if strings.TrimSpace(stringValue(fields["title"])) == "" || err != nil || priority > 4 {
			return fmt.Errorf("invalid source title/priority")
		}
		category, result, targetType := "", "", "issue"
		if migration != nil {
			target, ok := migration.Target(record.Project, record.ID)
			status, known := migration.snapshot.Effective(record.Project).Status(target.Status)
			if !ok || !known {
				return migrationRefusal("undeclared-entry", record.ID, record.Project)
			}
			category, result, targetType = string(status.Category), string(status.Outcome), target.Type
		} else {
			switch stringValue(fields["status"]) {
			case "open":
				category = "unstarted"
			case "in_progress", "hooked":
				category = "started"
			case "blocked", "deferred", "pinned", "frozen":
				category = "backlog"
			case "closed":
				category, result = "completed", "success"
			default:
				return fmt.Errorf("unsupported source status")
			}
		}
		if strings.TrimSpace(stringValue(fields["issue_type"])) == "" {
			return fmt.Errorf("missing source type")
		}
		if class := stringValue(fields["storage_class"]); class != "" && class != "versioned" {
			return fmt.Errorf("unsupported storage class")
		}
		// agent_state is a Beads-owned opaque string. It is carried through the
		// converter as metadata, but never interpreted as Plug workflow state.
		if value, present := fields["agent_state"]; present && value != nil {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("agent_state must be text")
			}
		}
		// await_id is a Beads-owned opaque string. It is carried through the
		// converter as metadata, but never interpreted as Plug workflow state.
		if value, present := fields["await_id"]; present && value != nil {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("await_id must be text")
			}
		}
		labels, ok := record.Body["labels"].([]any)
		if !ok {
			return fmt.Errorf("missing bead labels")
		}
		last := ""
		for _, raw := range labels {
			label, ok := raw.(string)
			if !ok || label <= last {
				return fmt.Errorf("invalid, duplicate or unordered labels")
			}
			last = label
		}
		counts["labels"] += len(labels)
		body = Row{"type": targetType, "source_type": fields["issue_type"], "title": fields["title"], "status_category": category, "labels": labels}
		if result != "" {
			body["result"] = result
		}
		for _, key := range []string{"description", "design", "acceptance_criteria", "notes", "priority", "assignee", "owner", "created_at", "created_by", "updated_at", "closed_at", "due_at", "defer_until", "started_at", "metadata", "spec_id", "external_ref", "pinned"} {
			body[key] = fields[key]
		}
	case "edge":
		target, targetKind := "", ""
		for _, key := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
			if value := stringValue(fields[key]); value != "" {
				if target != "" {
					return fmt.Errorf("multiple typed edge targets")
				}
				target, targetKind = value, key
			}
		}
		if target == "" || targetKind == "depends_on_wisp_id" {
			if target != "" && targetKind == "depends_on_wisp_id" {
				return fmt.Errorf("unsupported or missing edge target: %w", &UnresolvedDependencyError{
					EdgeID: record.ID, Project: record.Project, Source: stringValue(fields["issue_id"]), Target: target, TargetKind: targetKind,
				})
			}
			return fmt.Errorf("unsupported or missing edge target")
		}
		switch stringValue(fields["type"]) {
		case "blocks", "parent-child", "related", "discovered-from":
		default:
			kind := stringValue(fields["type"])
			if targetKind != "depends_on_external" || kind == "" || len(kind) > 32 || !utf8.ValidString(kind) {
				return fmt.Errorf("unsupported dependency type")
			}
		}
		body = Row{"from": fields["issue_id"], "to": target, "target_kind": targetKind, "type": fields["type"], "metadata": fields["metadata"], "thread_id": fields["thread_id"]}
	case "comment":
		body = fields
	case "config", "memory":
		key := stringValue(fields["key"])
		id := table + "/" + key
		if expectedKind == "memory" {
			id = strings.TrimPrefix(key, "kv.memory.")
		}
		if key == "" || id == "" || record.ID != id {
			return fmt.Errorf("config/source identity mismatch")
		}
		policy := stringValue(record.Body["classification"])
		if policy != "public" && policy != "secret" || secretKey(key) && policy != "secret" {
			return fmt.Errorf("unclassified or raw credential record")
		}
		if policy == "secret" {
			ref, ok := fields["value"].(map[string]any)
			if !ok || len(ref) != 1 || ref["$secret"] != table+"/"+key {
				return fmt.Errorf("credential requires its exact source reference")
			}
		}
		body = Row{"key": id, "value": fields["value"], "classification": policy}
	default:
		data, err := json.Marshal(fields)
		if err != nil || record.ID != table+"/"+digest(append(data, '\n')) {
			return fmt.Errorf("source archive identity mismatch")
		}
		body = Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}
	}
	if !equalJSON(record.Body, body) {
		return fmt.Errorf("typed body/source mapping mismatch")
	}
	var secretValues any = map[string]any(fields)
	if table == "config" || table == "metadata" || table == "local_metadata" {
		secretValues = map[string]any{stringValue(fields["key"]): fields["value"]}
	}
	if err := validateSecretReferences(secretValues); err != nil {
		return err
	}
	counts[table]++
	return nil
}

func equalJSON(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}
func projectKnown(prefix string, projects []Project) bool {
	return findProject(prefix, projects).Prefix != ""
}
func findProject(prefix string, projects []Project) Project {
	for _, p := range projects {
		if p.Prefix == prefix {
			return p
		}
	}
	return Project{}
}
func natural(value any) (uint64, error) {
	switch v := value.(type) {
	case json.Number:
		return strconv.ParseUint(string(v), 10, 64)
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("expected a lossless nonnegative integer")
	}
}
func active(value any) bool {
	if value == nil || value == false || value == "" {
		return false
	}
	n, err := natural(value)
	return err != nil || n != 0
}

func checkID(id string, p Project) error {
	if !strings.HasPrefix(id, p.Prefix+"-") {
		return fmt.Errorf("source ID/project prefix mismatch")
	}
	parts := strings.Split(strings.TrimPrefix(id, p.Prefix+"-"), ".")
	if p.IDMode == "sequential" {
		n, err := natural(parts[0])
		if err != nil || n == 0 || n > p.LastID {
			return fmt.Errorf("source ID exceeds sequential allocator")
		}
	} else if !hashPattern.MatchString(parts[0]) || len(parts[0]) < p.HashLength {
		return fmt.Errorf("invalid source hash ID")
	}
	for i := 1; i < len(parts); i++ {
		n, err := natural(parts[i])
		if err != nil || n == 0 {
			return fmt.Errorf("invalid source child ID")
		}
		if i == len(parts)-1 {
			parent := id[:strings.LastIndex(id, ".")]
			if p.ChildCounters[parent] < n {
				return fmt.Errorf("source child ID exceeds allocator")
			}
		}
	}
	return nil
}

func validateRelations(projects []Project, records map[string][]Record) error {
	beads := map[string]string{}
	for _, record := range records["bead"] {
		if beads[record.ID] != "" {
			return fmt.Errorf("duplicate source bead ID")
		}
		beads[record.ID] = record.Project
	}
	for _, p := range projects {
		for parent := range p.ChildCounters {
			if beads[parent] != p.Prefix {
				return fmt.Errorf("unresolved child counter parent")
			}
		}
	}
	for _, kind := range []string{"edge", "comment"} {
		seen := map[string]bool{}
		for _, record := range records[kind] {
			owner := stringValue(record.Source.Fields["issue_id"])
			if seen[record.ID] || beads[owner] != record.Project {
				return fmt.Errorf("duplicate identity or dangling relation owner")
			}
			seen[record.ID] = true
			if kind == "edge" && record.Body["target_kind"] == "depends_on_issue_id" && beads[stringValue(record.Body["to"])] == "" {
				return fmt.Errorf("dangling typed edge target: %w", &UnresolvedDependencyError{
					EdgeID: record.ID, Project: record.Project, Source: stringValue(record.Body["from"]), Target: stringValue(record.Body["to"]), TargetKind: stringValue(record.Body["target_kind"]),
				})
			}
		}
	}
	seenTop, seenChild := map[string]bool{}, map[string]bool{}
	for _, record := range records["beads-archive"] {
		fields := record.Source.Fields
		switch record.Source.Table {
		case "issue_counter":
			prefix := stringValue(fields["prefix"])
			p := findProject(prefix, projects)
			count, err := natural(fields["last_id"])
			if p.Prefix == "" || seenTop[prefix] || err != nil || count > p.LastID {
				return fmt.Errorf("invalid source top-level counter")
			}
			seenTop[prefix] = true
		case "child_counters":
			parent := stringValue(fields["parent_id"])
			p := findProject(beads[parent], projects)
			count, err := natural(fields["last_child"])
			if p.Prefix == "" || seenChild[parent] || err != nil || count > p.ChildCounters[parent] {
				return fmt.Errorf("invalid source child counter")
			}
			seenChild[parent] = true
		}
	}
	for lane, table := range map[string]string{"main": "schema_migrations", "ignored": "ignored_schema_migrations"} {
		seen := map[uint64]bool{}
		for _, record := range records["beads-archive"] {
			if record.Source.Table != table {
				continue
			}
			version, err := natural(record.Source.Fields["version"])
			if err != nil || version == 0 || version > uint64(len(migrationHashes[lane])) || seen[version] || record.Source.Fields["content_hash"] != migrationHashes[lane][int(version)] {
				return fmt.Errorf("unknown or altered migration series")
			}
			seen[version] = true
		}
		if len(seen) != len(migrationHashes[lane]) {
			return fmt.Errorf("incomplete migration series")
		}
	}
	return nil
}

func sourceStatusTarget(source string) (string, bool) {
	target, ok := map[string]string{
		"open": "todo", "in_progress": "in_progress", "hooked": "in_progress",
		"blocked": "backlog", "deferred": "backlog", "pinned": "backlog", "frozen": "backlog", "closed": "done",
	}[source]
	return target, ok
}

// Source built-in names differ from legacy import spellings: frozen is a
// legal custom name at the pinned source, even though older imports accepted
// that spelling as backlog without a declaration.
func builtInSourceStatus(source string) bool {
	switch source {
	case "open", "in_progress", "hooked", "blocked", "deferred", "pinned", "closed":
		return true
	}
	return false
}
