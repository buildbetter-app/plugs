package plugdump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// This test-only rejected SQL reader retains the original query/error oracle.
// It does not authenticate physical roots or add capability to SQLSource.
type referenceNativeSQLSource struct{ SQLSource }

func (s referenceNativeSQLSource) ReadNativeSourceImage(ctx context.Context, id string) (map[string]Table, error) {
	if !validCommitID(id) || s.DB == nil {
		return nil, ErrSourceHistoryUnavailable{Reason: "invalid-image-request"}
	}
	names, err := s.historyTable(ctx, "SHOW FULL TABLES AS OF '"+id+"'")
	if err != nil || len(names.Columns) != 2 {
		return nil, ErrSourceHistoryUnavailable{Reason: "historical-schema-unavailable"}
	}
	// SHOW TABLES includes nonlocal aliases. Ordinary SELECT AS OF follows
	// those aliases before resolving the immutable root, so it cannot witness
	// the local table's retained rows. Preserve the declarations themselves.
	nonlocal, err := s.historyTable(ctx, "SELECT * FROM dolt_nonlocal_tables AS OF '"+id+"'")
	if err != nil {
		return nil, ErrSourceHistoryUnavailable{Reason: "historical-nonlocal-census-unavailable"}
	}
	if shadowed, err := nativeNonlocalAlias("dolt_nonlocal_tables", nonlocal); err != nil || shadowed {
		return nil, ErrSourceHistoryUnavailable{Reason: "historical-nonlocal-census-shadowed"}
	}
	result := map[string]Table{"dolt_nonlocal_tables": nonlocal}
	seen := map[string]bool{}
	for _, row := range names.Rows {
		name, kind := historyText(row[names.Columns[0]]), historyText(row[names.Columns[1]])
		if kind == "VIEW" {
			continue
		}
		if kind != "BASE TABLE" || !nativeSQLName.MatchString(name) {
			return nil, ErrSourceHistoryUnavailable{Reason: "historical-schema-unmappable"}
		}
		if seen[name] {
			return nil, ErrSourceHistoryUnavailable{Reason: "duplicate-historical-table"}
		}
		seen[name] = true
		alias, err := nativeNonlocalAlias(name, nonlocal)
		if err != nil {
			return nil, err
		}
		if alias {
			// The resolver checks nonlocal rules before recognizing the
			// dolt_history prefix. Its own redirection would use a later schema.
			shadowed, err := nativeNonlocalAlias("dolt_history_"+name, nonlocal)
			if err != nil || shadowed {
				return nil, ErrSourceHistoryUnavailable{Reason: "historical-history-helper-shadowed:" + name}
			}
			table, present, err := s.readNativeLocalHistoryTable(ctx, name, id)
			if err != nil {
				return nil, err
			}
			if present {
				result[name] = table
			}
			continue
		}
		// Names are read from this immutable census and constrained above. This
		// raw evidence port neither executes DDL nor treats an old schema as v72.
		table, err := s.historyTable(ctx, "SELECT * FROM `"+name+"` AS OF '"+id+"'")
		if err != nil {
			return nil, ErrSourceHistoryUnavailable{Reason: "historical-image-unavailable:" + name}
		}
		result[name] = table
	}
	return result, nil
}

func nativeNonlocalAlias(name string, declarations Table) (bool, error) {
	for _, row := range declarations.Rows {
		pattern := text(row, "table_name")
		if pattern == "" || len(pattern) > 128 {
			return false, ErrSourceHistoryUnavailable{Reason: "historical-nonlocal-census-unmappable"}
		}
		// The public Dolt table-pattern grammar: literal characters, ? for one
		// character and * or % for any suffix. No ref is followed by this reader.
		pattern = regexp.QuoteMeta(strings.ToLower(pattern))
		pattern = strings.NewReplacer(`\?`, ".", `\*`, ".*", "%", ".*").Replace(pattern)
		matched, err := regexp.MatchString("^"+pattern+"$", strings.ToLower(name))
		if err != nil {
			return false, ErrSourceHistoryUnavailable{Reason: "historical-nonlocal-census-unmappable"}
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func (s referenceNativeSQLSource) readNativeLocalHistoryTable(ctx context.Context, name, id string) (Table, bool, error) {
	// Pin BOTH the history table's schema and its row partition to this exact
	// commit. Unqualified dolt_history uses the current schema and is lossy for
	// old/dropped columns. This supported SQL table resolves the physical base
	// table at the pinned root without following that base table's aliases.
	// Observed aliases on this auxiliary name refuse at the caller. This does
	// not independently authenticate a redirected declaration relation.
	table, err := s.historyTable(ctx, "SELECT * FROM `dolt_history_"+name+"` AS OF '"+id+"' WHERE commit_hash = '"+id+"'")
	if err != nil {
		var server *mysql.MySQLError
		if errors.As(err, &server) && server.Number == 1146 {
			// Only called for an alias actually declared at this commit. The
			// typed missing physical history table proves alias-only absence;
			// neither an empty table nor current target rows are manufactured.
			return Table{}, false, nil
		}
		return Table{}, false, ErrSourceHistoryUnavailable{Reason: "historical-image-unavailable:" + name}
	}
	metadata := []string{"commit_hash", "committer", "commit_date"}
	if len(table.Columns) <= len(metadata) || !equalJSON(table.Columns[len(table.Columns)-3:], metadata) {
		return Table{}, false, ErrSourceHistoryUnavailable{Reason: "historical-schema-unmappable"}
	}
	for _, column := range table.Columns[:len(table.Columns)-3] {
		for _, reserved := range metadata {
			if strings.EqualFold(column, reserved) {
				return Table{}, false, ErrSourceHistoryUnavailable{Reason: "historical-schema-unmappable"}
			}
		}
	}
	table.Columns = table.Columns[:len(table.Columns)-3]
	for _, row := range table.Rows {
		if text(row, "commit_hash") != id {
			return Table{}, false, ErrSourceHistoryUnavailable{Reason: "historical-commit-partition-mismatch"}
		}
		for _, column := range metadata {
			delete(row, column)
		}
	}
	return table, true, nil
}

// Count calls at SQLSource's actual Queryer boundary, including unexpected
// queries that a SQL mock could reject before matching a registered expectation.
type nativeImageQueryCounter struct {
	Queryer
	queries int
}

func (q *nativeImageQueryCounter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.queries++
	return q.Queryer.QueryContext(ctx, query, args...)
}

func TestSQLNativeHistoryAliasAbsenceRequiresTypedMissingPhysicalTable(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared bool
		readErr  error
		refuse   bool
	}{
		{"declared alias-only", true, &mysql.MySQLError{Number: 1146, Message: "missing physical history table"}, false},
		{"ordinary missing table", false, &mysql.MySQLError{Number: 1146, Message: "missing table"}, true},
		{"alias read permission failure", true, &mysql.MySQLError{Number: 1142, Message: "denied"}, true},
		{"untyped missing-table text", true, fmt.Errorf("table not found"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			id := strings.Repeat("a", 32)
			// Stock SQL must refuse before consulting even a configured SQL mock.
			// The separate test-only reader below retains the original query/error
			// oracle; it cannot supply physical-root capability to the producer.
			stock := &nativeImageQueryCounter{Queryer: db}
			stockImage, stockErr := (SQLSource{DB: stock}).ReadNativeSourceImage(context.Background(), id)
			var unavailable ErrSourceHistoryUnavailable
			if stockImage != nil || !errors.As(stockErr, &unavailable) || unavailable.Reason != "native-physical-root-capability-unavailable:stock-sql" || stock.queries != 0 {
				t.Fatalf("stock SQL physical capability changed: image=%v error=%v queries=%d", stockImage, stockErr, stock.queries)
			}
			mock.ExpectQuery(regexp.QuoteMeta("SHOW FULL TABLES AS OF '" + id + "'")).WillReturnRows(sqlmock.NewRows([]string{"Tables_in_fixture", "Table_type"}).AddRow("wisps", "BASE TABLE"))
			declarations := sqlmock.NewRows([]string{"table_name", "target_ref", "ref_table", "options"})
			if test.declared {
				declarations.AddRow("wisps", "main", nil, "immediate")
			}
			mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM dolt_nonlocal_tables AS OF '" + id + "'")).WillReturnRows(declarations)
			query := "SELECT * FROM `wisps` AS OF '" + id + "'"
			if test.declared {
				query = "SELECT * FROM `dolt_history_wisps` AS OF '" + id + "' WHERE commit_hash = '" + id + "'"
			}
			mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(test.readErr)
			image, err := (referenceNativeSQLSource{SQLSource{DB: db}}).ReadNativeSourceImage(context.Background(), id)
			if test.refuse != (err != nil) {
				t.Fatalf("refuse=%t err=%v", test.refuse, err)
			}
			if !test.refuse {
				if _, exists := image["wisps"]; exists {
					t.Fatal("alias absence fabricated a table")
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
