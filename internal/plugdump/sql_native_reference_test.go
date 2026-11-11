package plugdump

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/testutil"
)

// The rejected SQL runtime probes retain their original assertions. The shared
// test-only reader and four typed-error mocks also run in the ordinary unit target.
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
