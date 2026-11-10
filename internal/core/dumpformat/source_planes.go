package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type sourceRecordIdentity struct{ Kind, Project, Table, ID string }

// The explicit capability compares physical edge components independently.
// In particular NUL within a physical ID is data, never a tuple delimiter.
func compareSourceRecords(a, b Record, contract string) int {
	if a.Source.Identity != nil || b.Source.Identity != nil {
		return strings.Compare(recordKey(a), recordKey(b))
	}
	if contract != SourcePlaneAddressContract || a.Kind != "edge" || b.Kind != "edge" {
		return strings.Compare(recordKey(a), recordKey(b))
	}
	if c := strings.Compare(a.Project, b.Project); c != 0 {
		return c
	}
	if c := strings.Compare(a.Source.Table, b.Source.Table); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

func sourcePlaneArchiveTable(table string) bool {
	switch table {
	case "wisps", EphemeralLabelTable, EphemeralCommentTable, EphemeralEventTable, EphemeralChildCounterTable, "custom_statuses", "custom_types":
		return true
	}
	return false
}

func nativeOperationalTable(table string, familyContract string) bool {
	return familyContract == SourceFamilyContract && (table == "leases" || table == "repo_mtimes")
}

func validateSourcePlaneRecord(r Record, m Manifest, counts map[string]int, statuses map[string]bool) error {
	table, fields := r.Source.Table, r.Source.Fields
	if (table == "leases" || table == "repo_mtimes") && m.SourceFamilyContract != SourceFamilyContract {
		return fmt.Errorf("operational source rows require source family contract")
	}
	columns, ok := tableColumns[table]
	if !ok || (table != DurableDependencyTable && table != EphemeralDependencyTable && len(fields) != len(strings.Fields(columns))) {
		return fmt.Errorf("source plane record schema drift")
	}
	for _, column := range strings.Fields(columns) {
		if _, present := fields[column]; !present {
			return fmt.Errorf("missing source plane column %s", column)
		}
	}
	if table == "dependencies" || table == EphemeralDependencyTable {
		if !IsTypedEdgeVersion(m.EdgeVersion()) || r.Kind != "edge" || !projectKnown(r.Project, m.Projects) {
			return fmt.Errorf("source plane dependency requires typed edge and exact project")
		}
		id, idOK := fields["id"].(string)
		from, fromOK := fields["issue_id"].(string)
		kind, kindOK := fields["type"].(string)
		if !idOK || id == "" || id != r.ID || !fromOK || from == "" || !kindOK || kind == "" || len(kind) > 32 || !utf8.ValidString(id+from+kind) {
			return fmt.Errorf("invalid source plane dependency identity/type")
		}
		target, arm := "", ""
		for _, key := range []string{"depends_on_issue_id", EphemeralDependencyTargetField, "depends_on_external"} {
			if fields[key] == nil {
				continue
			}
			value, ok := fields[key].(string)
			if !ok || value == "" || !utf8.ValidString(value) || arm != "" {
				return fmt.Errorf("source plane dependency requires one exact typed target")
			}
			target, arm = value, key
		}
		if arm == "" {
			return fmt.Errorf("missing source plane dependency target")
		}
		cross := false
		switch kind {
		case "blocks", "parent-child", "related", "discovered-from", "waits-for", "conditional-blocks", "tracks":
			cross = true
		}
		local := table == "dependencies" && arm == "depends_on_issue_id"
		if !cross && !(local && (IsVersionedLocalKnownEdgeType(m.EdgeVersion(), arm, kind) || IsLocalOpaqueEdgeType(m.EdgeVersion(), arm, kind))) {
			return fmt.Errorf("unsupported source plane dependency kind/plane")
		}
		if _, err := SourceEdgeMetadata(m.EdgeVersion(), id, kind, fields["metadata"]); err != nil {
			return fmt.Errorf("source plane dependency metadata: %w", err)
		}
		body := Row{"from": from, "to": target, "target_kind": arm, "type": kind, "metadata": fields["metadata"], "thread_id": fields["thread_id"]}
		if !equalJSON(body, r.Body) {
			return fmt.Errorf("source plane dependency body/source mismatch")
		}
	} else {
		data, err := json.Marshal(fields)
		body := Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}
		if err != nil || r.Kind != "beads-archive" || r.Project != "" || r.ID != table+"/"+digest(append(data, '\n')) || !equalJSON(body, r.Body) {
			return fmt.Errorf("source plane archive identity/body mismatch")
		}
		if table == "wisps" {
			id, idOK := fields["id"].(string)
			status, statusOK := fields["status"].(string)
			priority, err := natural(fields["priority"])
			_, builtIn := sourceStatusTarget(status)
			if m.StatusCategoryContract == StatusCategoryContract {
				project, projectErr := sourcePhysicalProject(id, m.Projects)
				if projectErr != nil || projectStatusCategory(findProject(project, m.Projects), status) == "" {
					return migrationRefusal("unknown-native-status", status, project)
				}
			}
			if !idOK || id == "" || !statusOK || !builtIn && !statuses[status] || strings.TrimSpace(stringValue(fields["title"])) == "" || err != nil || priority > 4 {
				return fmt.Errorf("invalid active source identity/status/title/priority")
			}
		}
	}
	counts[table]++
	return nil
}

func validateSourcePlaneRelations(m Manifest, rows map[string][]Record) error {
	if m.SourcePlaneAddressContract != SourcePlaneAddressContract {
		return validateRelations(m.Projects, rows)
	}
	entities := map[driver.SourceEntityKey]bool{}
	for _, r := range rows["bead"] {
		entities[driver.SourceEntityKey{Project: r.Project, Plane: "issues", ID: r.ID}] = true
	}
	// Physical active IDs carry a source project prefix, never the inert
	// archive row's content-hash ID or an invented workflow target address.
	for _, r := range rows["beads-archive"] {
		if r.Source.Table != "wisps" {
			continue
		}
		id := stringValue(r.Source.Fields["id"])
		project, err := sourcePhysicalProject(id, m.Projects)
		if err != nil {
			return err
		}
		key := driver.SourceEntityKey{Project: project, Plane: "wisps", ID: id}
		if entities[key] {
			return fmt.Errorf("duplicate active source entity")
		}
		entities[key] = true
	}
	seen := map[sourceRecordIdentity]bool{}
	for _, r := range rows["beads-archive"] {
		table, fields := r.Source.Table, r.Source.Fields
		switch table {
		case EphemeralLabelTable, EphemeralCommentTable, EphemeralEventTable, EphemeralChildCounterTable:
			owner := stringValue(fields["issue_id"])
			if table == EphemeralChildCounterTable {
				owner = stringValue(fields["parent_id"])
				if _, err := natural(fields["last_child"]); err != nil {
					return err
				}
			}
			project, err := sourcePhysicalProject(owner, m.Projects)
			if err != nil || !entities[driver.SourceEntityKey{Project: project, Plane: "wisps", ID: owner}] {
				return fmt.Errorf("dangling active auxiliary source owner")
			}
			id := stringValue(fields["id"])
			if table == EphemeralLabelTable {
				id = stringValue(fields["label"])
			} else if table == EphemeralChildCounterTable {
				id = owner
			}
			key := sourceRecordIdentity{Kind: owner, Project: project, Table: table, ID: id}
			// Comment/event IDs are table-wide; labels and counters have
			// explicit owner-based physical identities.
			if table == EphemeralCommentTable || table == EphemeralEventTable {
				key.Kind = ""
				key.Project = ""
			}
			if id == "" || seen[key] {
				return fmt.Errorf("empty or duplicate active auxiliary source identity")
			}
			seen[key] = true
		}
	}
	for _, r := range rows["edge"] {
		plane := "issues"
		if r.Source.Table == EphemeralDependencyTable {
			plane = "wisps"
		}
		if !entities[driver.SourceEntityKey{Project: r.Project, Plane: plane, ID: stringValue(r.Body["from"])}] {
			return fmt.Errorf("dangling source plane dependency owner")
		}
		arm, target := stringValue(r.Body["target_kind"]), stringValue(r.Body["to"])
		if arm == "depends_on_external" {
			continue
		}
		project, err := sourcePhysicalProject(target, m.Projects)
		plane = "issues"
		if arm == EphemeralDependencyTargetField {
			plane = "wisps"
		}
		if err != nil || !entities[driver.SourceEntityKey{Project: project, Plane: plane, ID: target}] {
			return &UnresolvedDependencyError{EdgeID: r.ID, Project: r.Project, Source: stringValue(r.Body["from"]), Target: target, TargetKind: arm}
		}
	}
	// Existing issue comments, migrations and counter bounds remain checked
	// by the same validator; it must not apply naked edge-ID uniqueness to
	// independently addressed physical dependency planes.
	legacy := make(map[string][]Record, len(rows))
	for kind, records := range rows {
		if kind != "edge" {
			legacy[kind] = records
		}
	}
	return validateRelations(m.Projects, legacy)
}

func sourcePhysicalProject(id string, projects []Project) (string, error) {
	for _, p := range projects {
		if strings.HasPrefix(id, p.Prefix+"-") {
			return p.Prefix, nil
		}
	}
	return "", fmt.Errorf("source identity has no inventoried project")
}

// SourcePlaneRecords returns each original nonblank NDJSON line without its
// framing LF. Raw formatting, SQL-text cells, number lexemes and inert wire
// IDs are preserved independently of the detached decoded record views.
func (a *Archive) SourcePlaneRecords() ([]driver.SourcePlaneRecordV1, error) {
	if a == nil || a.native || a.manifest.SourcePlaneAddressContract != SourcePlaneAddressContract {
		return nil, driver.ErrUnsupported
	}
	var result []driver.SourcePlaneRecordV1
	for _, kind := range a.manifest.Kinds {
		data := a.members[kind.File]
		if len(data) == 0 || data[len(data)-1] != '\n' {
			return nil, fmt.Errorf("source plane raw member is missing framing")
		}
		lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
		if len(lines) != kind.Count || len(lines) != len(a.byKind[kind.Name]) {
			return nil, fmt.Errorf("source plane raw member differs from selected image")
		}
		for i, r := range a.byKind[kind.Name] {
			var original Record
			if err := decodeJSON(lines[i], &original); err != nil || !equalJSON(original, r) {
				return nil, fmt.Errorf("source plane raw member differs from selected record")
			}
			result = append(result, driver.SourcePlaneRecordV1{Kind: r.Kind, Project: r.Project, Table: r.Source.Table, ID: r.ID, Raw: bytes.Clone(lines[i])})
		}
	}
	return result, nil
}
