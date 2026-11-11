// Package plugdump retains the independent normalized capture oracle for native tests.
// Production typed writes do not pass through this test-only representation.
package plugdump

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/storage/physicalcapture"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type Table struct {
	Columns []string
	Rows    []Row
}

type SourceCommit struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Metadata Row      `json:"metadata"`
}

type SourceCommitLog struct {
	Head    string
	Clean   bool
	Commits []SourceCommit
}

func (s SQLSource) ReadTable(ctx context.Context, name string) (Table, error) {
	if _, ok := tableColumns[name]; !ok {
		return Table{}, fmt.Errorf("unsupported table")
	}
	// name comes exclusively from the closed census registry above.
	rows, err := s.DB.QueryContext(ctx, "SELECT * FROM `"+name+"`") //nolint:gosec // closed allowlist, no caller SQL
	if err != nil {
		return Table{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return Table{}, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return Table{}, err
	}
	table := Table{Columns: columns}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return Table{}, err
		}
		row := Row{}
		for i, value := range values {
			converted, err := sqlValue(value, columnTypes[i].DatabaseTypeName())
			if err != nil {
				return Table{}, fmt.Errorf("column %s: %w", columns[i], err)
			}
			row[columns[i]] = converted
		}
		table.Rows = append(table.Rows, row)
	}
	return table, rows.Err()
}

func sqlValue(value any, dbType string) (any, error) {
	if value == nil {
		return nil, nil
	}
	if stamp, ok := value.(time.Time); ok {
		return stamp.UTC().Format(time.RFC3339Nano), nil
	}
	if data, ok := value.([]byte); ok {
		if strings.Contains(strings.ToUpper(dbType), "BLOB") || strings.Contains(strings.ToUpper(dbType), "BINARY") {
			return map[string]string{"$bytes": base64.StdEncoding.EncodeToString(data)}, nil
		}
		if !utf8.Valid(data) {
			return nil, fmt.Errorf("non-UTF-8 text refuses lossy conversion")
		}
		value = string(data)
	}
	if text, ok := value.(string); ok {
		if !utf8.ValidString(text) {
			return nil, fmt.Errorf("non-UTF-8 text refuses lossy conversion")
		}
		if !strings.EqualFold(dbType, "JSON") {
			return text, nil
		}
		var decoded any
		if err := decodeJSON([]byte(text), &decoded); err != nil {
			return nil, fmt.Errorf("invalid JSON cell")
		}
		return decoded, nil
	}
	return value, nil
}

func decodeJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON must be UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := checkJSONValue(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	return d.Decode(target)
}

func checkJSONValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON")
	}
	if delim, ok := token.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return fmt.Errorf("invalid JSON object")
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return fmt.Errorf("duplicate or invalid JSON key")
				}
				seen[key] = true
				if err := checkJSONValue(d); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := checkJSONValue(d); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid JSON delimiter")
		}
		_, err = d.Token()
	}
	return err
}

func (s SQLSource) ReadSourceCommits(ctx context.Context) (SourceCommitLog, error) {
	if s.DB == nil {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "missing-scoped-reader"}
	}
	head, err := s.historyTable(ctx, "SELECT DOLT_HASHOF('HEAD')")
	if err != nil || len(head.Rows) != 1 || len(head.Columns) != 1 {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "head-unavailable"}
	}
	headID := historyText(head.Rows[0][head.Columns[0]])
	if !validCommitID(headID) {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "invalid-head"}
	}
	status, err := s.historyTable(ctx, "SELECT table_name, staged, status FROM dolt_status")
	if err != nil {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "working-set-unavailable"}
	}
	log, err := s.historyTable(ctx, "SELECT * FROM dolt_log LIMIT 10001")
	if err != nil || len(log.Rows) == 0 || len(log.Rows) > 10000 {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "commit-census-unavailable"}
	}
	ancestors, err := s.historyTable(ctx, "SELECT commit_hash, parent_hash FROM dolt_commit_ancestors LIMIT 20001")
	if err != nil || len(ancestors.Rows) > 20000 {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "ancestry-unavailable"}
	}
	parents, err := sourceParentCensus(ancestors.Rows)
	if err != nil {
		return SourceCommitLog{}, err
	}
	result := SourceCommitLog{Head: headID, Clean: len(status.Rows) == 0, Commits: []SourceCommit{}}
	for _, row := range log.Rows {
		id := historyText(row["commit_hash"])
		if !validCommitID(id) {
			return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "invalid-commit"}
		}
		metadata := Row{}
		for key, value := range row {
			if key != "commit_hash" {
				metadata[key] = value
			}
		}
		p, observed := parents[id]
		if !observed {
			return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "ancestry-unavailable"}
		}
		p = append([]string{}, p...)
		sort.Strings(p)
		result.Commits = append(result.Commits, SourceCommit{ID: id, Parents: p, Metadata: metadata})
	}
	sort.Slice(result.Commits, func(i, j int) bool { return result.Commits[i].ID < result.Commits[j].ID })
	return result, nil
}

func sourceParentCensus(rows []Row) (map[string][]string, error) {
	parents := map[string][]string{}
	roots, seen := map[string]bool{}, map[string]bool{}
	invalid := func() (map[string][]string, error) {
		return nil, ErrSourceHistoryUnavailable{Reason: "invalid-ancestry"}
	}
	for _, row := range rows {
		id := historyText(row["commit_hash"])
		if !validCommitID(id) {
			return invalid()
		}
		parentValue, present := row["parent_hash"]
		if !present {
			return invalid()
		}
		// The pinned Dolt system table represents a root with one explicit
		// SQL NULL parent row. An absent census row or empty hash is not proof
		// of a root, and a root cannot also have a non-null parent.
		if parentValue == nil {
			if _, observed := parents[id]; observed {
				return invalid()
			}
			parents[id], roots[id] = []string{}, true
			continue
		}
		parent := historyText(parentValue)
		key := id + ":" + parent
		if roots[id] || !validCommitID(parent) || seen[key] {
			return invalid()
		}
		seen[key] = true
		parents[id] = append(parents[id], parent)
	}
	return parents, nil
}

func (s SQLSource) historyTable(ctx context.Context, query string) (Table, error) {
	rows, err := s.DB.QueryContext(ctx, query)
	if err != nil {
		return Table{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return Table{}, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return Table{}, err
	}
	table := Table{Columns: columns, Rows: []Row{}}
	bytesRead := 0
	for rows.Next() {
		if len(table.Rows) >= 100000 {
			return Table{}, ErrSourceHistoryUnavailable{Reason: "row-limit"}
		}
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return Table{}, err
		}
		row := Row{}
		for i, value := range values {
			v, err := sqlValue(value, types[i].DatabaseTypeName())
			if err != nil {
				return Table{}, fmt.Errorf("source-history column %s: %w", columns[i], err)
			}
			row[columns[i]] = v
		}
		data, err := marshal(row)
		if err != nil {
			return Table{}, err
		}
		bytesRead += len(data)
		if bytesRead > MaxArchiveBytes {
			return Table{}, ErrSourceHistoryUnavailable{Reason: "table-size-limit"}
		}
		table.Rows = append(table.Rows, row)
	}
	return table, rows.Err()
}

func validCommitID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'v') {
			return false
		}
	}
	return true
}

type Row map[string]any
type SQLSource struct{ DB Queryer }
type ErrSourceHistoryUnavailable struct{ Reason string }

func (e ErrSourceHistoryUnavailable) Error() string { return "source history unavailable: " + e.Reason }
func historyText(v any) string                      { s, _ := v.(string); return s }
func marshal(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

type PhysicalCommitSource struct {
	SQLSource
	PhysicalDB *sql.DB
}

func (s PhysicalCommitSource) ReadNativeSourceImage(ctx context.Context, id string) (map[string]Table, error) {
	if s.PhysicalDB == nil {
		return nil, fmt.Errorf("physical capture database required")
	}
	conn, err := s.PhysicalDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	physical, err := physicalcapture.ReadCommit(ctx, conn, id)
	if err != nil {
		return nil, err
	}
	if physical.CommitID != id || !physical.ReadOnlyScope {
		return nil, fmt.Errorf("physical commit identity differs")
	}
	image := map[string]Table{}
	for _, source := range physical.Tables {
		table := Table{Columns: []string{}, Rows: []Row{}}
		for _, column := range source.Columns {
			table.Columns = append(table.Columns, column.Name)
		}
		for _, values := range source.Rows {
			if len(values) != len(source.Columns) {
				return nil, fmt.Errorf("physical row width differs")
			}
			row := Row{}
			for i, value := range values {
				converted, err := sqlValue(value, source.Columns[i].DatabaseTypeName)
				if err != nil {
					return nil, err
				}
				row[source.Columns[i].Name] = converted
			}
			table.Rows = append(table.Rows, row)
		}
		if _, duplicate := image[source.Name]; duplicate {
			return nil, fmt.Errorf("duplicate physical table name")
		}
		image[source.Name] = table
	}
	return image, nil
}

var issueColumns = "id content_hash title description design acceptance_criteria notes status priority issue_type assignee estimated_minutes created_at created_by owner updated_at closed_at closed_by_session external_ref spec_id compaction_level compacted_at compacted_at_commit original_size sender ephemeral wisp_type pinned is_template mol_type work_type source_system metadata source_repo close_reason event_kind actor target payload await_type await_id timeout_ns waiters hook_bead role_bead agent_state last_activity role_type rig due_at defer_until no_history started_at is_blocked row_lock storage_class current_revision participation_generation"

var tableColumns = map[string]string{
	"schema_migrations":         "version content_hash",
	"ignored_schema_migrations": "version applied_at content_hash",
	"issues":                    issueColumns,
	"wisps":                     issueColumns,
	"dependencies":              "id issue_id depends_on_issue_id depends_on_wisp_id depends_on_external type created_at created_by metadata thread_id",
	"wisp_dependencies":         "id issue_id depends_on_issue_id depends_on_wisp_id depends_on_external type created_at created_by metadata thread_id",
	"labels":                    "issue_id label",
	"wisp_labels":               "issue_id label",
	"comments":                  "id issue_id author text created_at",
	"wisp_comments":             "id issue_id author text created_at",
	"events":                    "id issue_id event_type actor old_value new_value comment created_at",
	"wisp_events":               "id issue_id event_type actor old_value new_value comment created_at",
	"config":                    "key value",
	"metadata":                  "key value",
	"local_metadata":            "key value",
	"child_counters":            "parent_id last_child",
	"wisp_child_counters":       "parent_id last_child",
	"issue_counter":             "prefix last_id",
	"issue_snapshots":           "id issue_id snapshot_time compaction_level original_size compressed_size original_content archived_events",
	"compaction_snapshots":      "id issue_id compaction_level snapshot_json created_at",
	"repo_mtimes":               "repo_path jsonl_path mtime_ns last_checked",
	"routes":                    "prefix path created_at updated_at",
	"interactions":              "id kind created_at actor issue_id model prompt response error tool_name exit_code parent_id label reason extra",
	"federation_peers":          "name remote_url username password_encrypted sovereignty last_sync created_at updated_at",
	"custom_statuses":           "name category",
	"custom_types":              "name",
	"provenance_events":         "id issue_id kind actor ref ref_kind payload source occurred_at created_at",
	"bd_events_journal":         "seq ts op issue_id issue_json dep_json comment_json actor",
	"bd_events_seq":             "id next_seq",
	"issue_versions":            "issue_id revision epoch durable_state change_actor change_agent change_message change_at removed_at removed_reason attribution_status removed_restriction",
	"store_epoch":               "id epoch bumped_at bumped_reason",
	"epoch_minted_addresses":    "address store_id minted_id minted_epoch minted_at carried_from gone_at_epoch",
	"leases":                    "issue_id holder granted_at lease_expires_at heartbeat_at granted_node",
}

const MaxArchiveBytes = 128 << 20
