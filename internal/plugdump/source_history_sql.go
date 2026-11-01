package plugdump

import (
	"context"
	"fmt"
	"sort"
)

// SQLSource uses only the caller's scoped Queryer. All source-history queries
// therefore share the same read transaction as the exported current image.
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
	parents := map[string][]string{}
	for _, row := range ancestors.Rows {
		id, parent := historyText(row["commit_hash"]), historyText(row["parent_hash"])
		if !validCommitID(id) || !validCommitID(parent) {
			return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "invalid-ancestry"}
		}
		parents[id] = append(parents[id], parent)
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
		p := append([]string{}, parents[id]...)
		sort.Strings(p)
		result.Commits = append(result.Commits, SourceCommit{ID: id, Parents: p, Metadata: metadata})
	}
	sort.Slice(result.Commits, func(i, j int) bool { return result.Commits[i].ID < result.Commits[j].ID })
	return result, nil
}

func (s SQLSource) ReadSourceImage(ctx context.Context, id string) (map[string]Table, error) {
	if !validCommitID(id) || s.DB == nil {
		return nil, ErrSourceHistoryUnavailable{Reason: "invalid-image-request"}
	}
	// The only interpolated input is an exact 32-character base32 Dolt ID.
	names, err := s.historyTable(ctx, "SHOW TABLES AS OF '"+id+"'")
	if err != nil || len(names.Columns) != 1 {
		return nil, ErrSourceHistoryUnavailable{Reason: "historical-schema-unavailable"}
	}
	tables := map[string]Table{}
	for _, row := range names.Rows {
		name := historyText(row[names.Columns[0]])
		if _, ok := tableColumns[name]; !ok {
			return nil, ErrSourceHistoryUnavailable{Reason: "historical-schema-unmappable"}
		}
		if _, ok := tables[name]; ok {
			return nil, ErrSourceHistoryUnavailable{Reason: "duplicate-historical-table"}
		}
		// Table names come only from the closed source census registry.
		table, err := s.historyTable(ctx, "SELECT * FROM `"+name+"` AS OF '"+id+"'")
		if err != nil {
			return nil, ErrSourceHistoryUnavailable{Reason: "historical-image-unavailable"}
		}
		tables[name] = table
	}
	return tables, nil
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
