package plugdump

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/testutil"
)

// This public-only spike probes both documented revision-database forms before
// any disposable-copy checkout or private-driver option is attempted. The
// producer's existing controls above are retained byte-for-byte.
func TestNativePhysicalRootRevisionDatabaseControls(t *testing.T) {
	for _, pattern := range []string{"only_alias", "dolt_history_archived_rows", "dolt_history_*", "dolt_nonlocal_tables"} {
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
			if _, err := db.ExecContext(ctx, "INSERT INTO dolt_nonlocal_tables (table_name,target_ref,ref_table,options) VALUES (?,'main','archived_rows','immediate')", pattern); err != nil {
				t.Fatal(err)
			}
			exec("CALL DOLT_ADD('-A')")
			exec("CALL DOLT_COMMIT('-m','public revision database physical root','--author','Synthetic source <source@example.test>')")
			var id string
			if err := db.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&id); err != nil {
				t.Fatal(err)
			}
			exec("DELETE FROM dolt_nonlocal_tables")
			exec("ALTER TABLE archived_rows DROP COLUMN old_column, ADD COLUMN later_column TEXT")
			exec("UPDATE archived_rows SET later_column='later',opaque=X'11'")
			exec("CALL DOLT_ADD('-A')")
			exec("CALL DOLT_COMMIT('-m','public revision later root','--author','Synthetic source <source@example.test>')")
			for _, mode := range []string{"qualified", "use"} {
				t.Run(mode, func(t *testing.T) {
					conn, err := db.Conn(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					prefix := "`beads_test/" + id + "`."
					if mode == "use" {
						rows, err := tx.QueryContext(ctx, "USE `beads_test/"+id+"`")
						if err != nil {
							t.Fatalf("OPTION1_USE error_type=%T error=%v", err, err)
						}
						rows.Close()
						prefix = ""
					}
					port := SQLSource{DB: tx}
					census, censusErr := port.historyTable(ctx, "SELECT * FROM "+prefix+"`dolt_nonlocal_tables`")
					t.Logf("OPTION1_DECLARATIONS mode=%s pattern=%s commit=%s expected=2 columns=%v rows=%v error_type=%T error=%v", mode, pattern, id, census.Columns, census.Rows, censusErr, censusErr)
					if censusErr != nil || !equalJSON(census.Columns, []string{"table_name", "target_ref", "ref_table", "options"}) || len(census.Rows) != 2 {
						t.Errorf("revision database does not return the exact committed declaration census")
					}
					for _, name := range []string{"archived_rows", "dolt_history_archived_rows", "only_alias"} {
						query := "SELECT * FROM " + prefix + "`" + name + "`"
						if name == "dolt_history_archived_rows" {
							query += " WHERE commit_hash='" + id + "'"
						}
						table, readErr := port.historyTable(ctx, query)
						t.Logf("OPTION1_PHYSICAL mode=%s pattern=%s commit=%s table=%s columns=%v rows=%v error_type=%T error=%v", mode, pattern, id, name, table.Columns, table.Rows, readErr, readErr)
						if name == "dolt_history_archived_rows" && pattern == "only_alias" {
							if readErr != nil || !equalJSON(table.Columns, []string{"id", "old_column", "opaque", "commit_hash", "committer", "commit_date"}) || len(table.Rows) != 3 {
								t.Errorf("revision database does not return retained physical schema and row multiplicity")
							}
						}
					}
				})
			}
		})
	}
}
