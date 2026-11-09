package plugdump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/testutil"
)

// The rejected historical SQL route is retained as a manual capability
// reference. It cannot be used by a producer or promoted to physical evidence.
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

func TestSQLNativeHistoryReadsLocalRowsBehindNonlocalAliases(t *testing.T) {
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit pinned Dolt source-history lane")
	}
	server := testutil.StartIsolatedDoltContainerHandle(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%s)/beads_test?parseTime=true", server.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exec := func(query string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(message string) string {
		t.Helper()
		exec("CALL DOLT_ADD('-A')")
		exec("CALL DOLT_COMMIT('-m', '" + message + "', '--author', 'Synthetic source <source@example.test>')")
		var id string
		if err := db.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// Two identical keyless rows, NULL, binary bytes and an old-only column.
	exec("CREATE TABLE archived_rows (id INT, old_column TEXT, opaque LONGBLOB)")
	exec("INSERT INTO archived_rows VALUES (1, 'original', X'00FF'), (1, 'original', X'00FF'), (2, NULL, NULL)")
	before := commit("public retained local rows")
	exec("INSERT INTO dolt_nonlocal_tables (table_name,target_ref,options) VALUES ('archived_rows','main','immediate')")
	exec("INSERT INTO dolt_nonlocal_tables (table_name,target_ref,ref_table,options) VALUES ('only_alias','main','later_rows','immediate')")
	aliased := commit("public nonlocal aliases shadow retained local rows")
	exec("DELETE FROM dolt_nonlocal_tables")
	exec("ALTER TABLE archived_rows DROP COLUMN old_column, ADD COLUMN later_column TEXT")
	exec("UPDATE archived_rows SET later_column='later', opaque=X'11'")
	exec("CREATE TABLE later_rows (id INT)")
	exec("INSERT INTO later_rows VALUES (999)")
	commit("public later schema and target rows must not leak backwards")
	port := referenceNativeSQLSource{SQLSource{DB: db}}
	for _, id := range []string{before, aliased} {
		if id == aliased {
			// Public-only capability evidence: retain the actual server error,
			// rather than guessing its class from the port's named refusal.
			for _, name := range []string{"archived_rows", "only_alias"} {
				rows, queryErr := db.QueryContext(ctx, "SELECT * FROM `dolt_history_"+name+"` AS OF '"+id+"' WHERE commit_hash = '"+id+"'")
				if queryErr != nil {
					t.Logf("PUBLIC_PINNED_HISTORY_CAPABILITY table=%s error_type=%T error=%v", name, queryErr, queryErr)
				} else {
					columns, columnsErr := rows.Columns()
					t.Logf("PUBLIC_PINNED_HISTORY_CAPABILITY table=%s columns=%v error=%v", name, columns, columnsErr)
					rows.Close()
				}
			}
		}
		image, err := port.ReadNativeSourceImage(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		table := image["archived_rows"]
		if !equalJSON(table.Columns, []string{"id", "old_column", "opaque"}) || len(table.Rows) != 3 {
			t.Fatalf("retained old schema/row multiplicity: %+v", table)
		}
		original, nulls := 0, 0
		for _, row := range table.Rows {
			if fmt.Sprint(row["id"]) == "1" && row["old_column"] == "original" && equalJSON(row["opaque"], map[string]string{"$bytes": "AP8="}) {
				original++
			} else if fmt.Sprint(row["id"]) == "2" && row["old_column"] == nil && row["opaque"] == nil {
				nulls++
			} else {
				t.Fatalf("current rows, history metadata or lossy conversion in old image: %+v", row)
			}
		}
		if original != 2 || nulls != 1 {
			t.Fatal("historical duplicate/NULL rows changed")
		}
		if _, present := image["only_alias"]; present {
			t.Fatal("nonlocal target rows or a fabricated empty table became local history")
		}
		if _, present := image["later_rows"]; present {
			t.Fatal("later physical table appeared in an earlier image")
		}
		if id == aliased && len(image["dolt_nonlocal_tables"].Rows) != 2 {
			t.Fatal("the exact historical alias declarations were omitted")
		}
		t.Logf("PINNED_LOCAL_HISTORY commit=%s rows=%d duplicate_rows=%d null_rows=%d declarations=%d", id, len(table.Rows), original, nulls, len(image["dolt_nonlocal_tables"].Rows))
	}
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

func TestSQLNativeHistoryRefusesNonlocalHelperSchemaRedirection(t *testing.T) {
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit pinned Dolt source-history lane")
	}
	for _, pattern := range []string{"dolt_history_archived_rows", "dolt_history_*", "dolt_nonlocal_tables"} {
		t.Run(pattern, func(t *testing.T) {
			server := testutil.StartIsolatedDoltContainerHandle(t)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%s)/beads_test?parseTime=true", server.Port))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			exec := func(query string) {
				t.Helper()
				if _, err := db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			exec("CREATE TABLE archived_rows (id INT, old_column TEXT, opaque LONGBLOB)")
			exec("INSERT INTO archived_rows VALUES (1,'original',X'00FF'),(1,'original',X'00FF'),(2,NULL,NULL)")
			exec("INSERT INTO dolt_nonlocal_tables (table_name,target_ref,options) VALUES ('archived_rows','main','immediate')")
			if _, err := db.ExecContext(ctx, "INSERT INTO dolt_nonlocal_tables (table_name,target_ref,options) VALUES (?,'main','immediate')", pattern); err != nil {
				t.Fatal(err)
			}
			exec("CALL DOLT_ADD('-A')")
			exec("CALL DOLT_COMMIT('-m','public helper alias','--author','Synthetic source <source@example.test>')")
			var id string
			if err := db.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&id); err != nil {
				t.Fatal(err)
			}
			exec("DELETE FROM dolt_nonlocal_tables")
			exec("ALTER TABLE archived_rows DROP COLUMN old_column, ADD COLUMN later_column TEXT")
			exec("UPDATE archived_rows SET later_column='later',opaque=X'11'")
			exec("CALL DOLT_ADD('-A')")
			exec("CALL DOLT_COMMIT('-m','public later schema','--author','Synthetic source <source@example.test>')")
			port := referenceNativeSQLSource{SQLSource{DB: db}}
			declarations, censusErr := port.historyTable(ctx, "SELECT * FROM dolt_nonlocal_tables AS OF '"+id+"'")
			t.Logf("PUBLIC_NONLOCAL_CENSUS_CAPABILITY pattern=%s commit=%s expected_stored_rows=2 observed_rows=%d columns=%v error=%v", pattern, id, len(declarations.Rows), declarations.Columns, censusErr)
			_, err = port.ReadNativeSourceImage(ctx, id)
			reason := "historical-history-helper-shadowed:archived_rows"
			if pattern == "dolt_nonlocal_tables" {
				reason = "historical-nonlocal-census-shadowed"
			}
			if typed, ok := err.(ErrSourceHistoryUnavailable); !ok || typed.Reason != reason {
				t.Fatalf("helper schema redirection must name refusal %s: %v", reason, err)
			}
			t.Logf("PUBLIC_HELPER_ALIAS_REFUSAL pattern=%s commit=%s reason=%s", pattern, id, reason)
		})
	}
}
