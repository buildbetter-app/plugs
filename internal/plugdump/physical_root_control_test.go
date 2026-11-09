package plugdump

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/testutil"
)

// These controls are deliberately independent of the archive/mapping fixtures.
// Spike adapters replace only newNativePhysicalRootControlCandidate in their
// isolated worktree. Expectations below never enter the candidate read path.
type nativePhysicalRootControlCandidate struct {
	Name      string
	ReadImage func(context.Context, string) (map[string]Table, error)
	// present=false is typed physical absence, never an arbitrary read error.
	ReadTable func(context.Context, string, string) (table Table, present bool, err error)
}

func newNativePhysicalRootControlCandidate(_ *testing.T, db *sql.DB, _ *testutil.IsolatedDoltContainer) nativePhysicalRootControlCandidate {
	port := SQLSource{DB: db}
	return nativePhysicalRootControlCandidate{
		Name:      "current-as-of-reference",
		ReadImage: port.ReadNativeSourceImage,
		ReadTable: func(ctx context.Context, id, name string) (Table, bool, error) {
			// This reference recognizes only the existing typed MySQL1146 path.
			return port.readNativeLocalHistoryTable(ctx, name, id)
		},
	}
}

func TestSQLNativePhysicalRootControls(t *testing.T) {
	runNativePhysicalRootControls(t, newNativePhysicalRootControlCandidate)
}

func runNativePhysicalRootControls(t *testing.T, factory func(*testing.T, *sql.DB, *testutil.IsolatedDoltContainer) nativePhysicalRootControlCandidate) {
	t.Helper()
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit pinned Dolt physical-root lane")
	}
	for _, scenario := range []struct {
		name, pattern string
	}{
		{"base_alias", ""},
		{"helper_exact", "dolt_history_archived_rows"},
		{"helper_wildcard", "dolt_history_*"},
		{"census_self", "dolt_nonlocal_tables"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			server := testutil.StartIsolatedDoltContainerHandle(t)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%s)/beads_test?parseTime=true", server.Port))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := db.ExecContext(ctx, query, args...); err != nil {
					t.Fatal(err)
				}
			}
			commit := func(message string) string {
				t.Helper()
				exec("CALL DOLT_ADD('-A')")
				exec("CALL DOLT_COMMIT('-m',?,'--author','Synthetic source <source@example.test>')", message)
				var id string
				if err := db.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&id); err != nil {
					t.Fatal(err)
				}
				return id
			}
			exec("CREATE TABLE archived_rows (id INT, old_column TEXT, opaque LONGBLOB)")
			exec("INSERT INTO archived_rows VALUES (1,'original',X'00FF'),(1,'original',X'00FF'),(2,NULL,NULL)")
			before := commit("physical control original schema and rows")
			declarations := Table{Columns: []string{"table_name", "target_ref", "ref_table", "options"}, Rows: []Row{
				{"table_name": "archived_rows", "target_ref": "main", "ref_table": nil, "options": "immediate"},
				{"table_name": "only_alias", "target_ref": "main", "ref_table": "later_rows", "options": "immediate"},
			}}
			if scenario.pattern != "" {
				declarations.Rows = append(declarations.Rows, Row{"table_name": scenario.pattern, "target_ref": "main", "ref_table": nil, "options": "immediate"})
			}
			for _, row := range declarations.Rows {
				exec("INSERT INTO dolt_nonlocal_tables (table_name,target_ref,ref_table,options) VALUES (?,?,?,?)", row["table_name"], row["target_ref"], row["ref_table"], row["options"])
			}
			aliased := commit("physical control retained alias declarations")
			// Later working rows/schema deliberately disagree with both roots.
			exec("DELETE FROM dolt_nonlocal_tables")
			exec("ALTER TABLE archived_rows DROP COLUMN old_column, ADD COLUMN later_column TEXT")
			exec("UPDATE archived_rows SET later_column='later',opaque=X'11'")
			exec("CREATE TABLE later_rows (id INT)")
			exec("INSERT INTO later_rows VALUES (999)")
			later := commit("physical control later working set")
			candidate := factory(t, db, server)
			if candidate.Name == "" || candidate.ReadImage == nil || candidate.ReadTable == nil {
				t.Fatal("candidate needs a name, immutable image reader and typed physical-table reader")
			}
			t.Logf("PHYSICAL_ROOT_CONTROL candidate=%s database=beads_test before=%s aliased=%s later=%s pattern=%s declarations=%d", candidate.Name, before, aliased, later, scenario.pattern, len(declarations.Rows))
			t.Run("unknown_commit_is_error", func(t *testing.T) {
				_, present, err := candidate.ReadTable(ctx, strings.Repeat("z", 32), "archived_rows")
				if err == nil || present {
					t.Fatalf("unknown commit must refuse rather than become physical absence: present=%t error=%v", present, err)
				}
			})
			for _, root := range []struct{ name, id string }{{"before", before}, {"aliased", aliased}} {
				t.Run(root.name, func(t *testing.T) {
					t.Run("image", func(t *testing.T) {
						image, err := candidate.ReadImage(ctx, root.id)
						if err != nil {
							t.Fatal(err)
						}
						t.Run("old_schema_and_exact_cells", func(t *testing.T) { assertNativePhysicalRootRows(t, image["archived_rows"]) })
						t.Run("declarations", func(t *testing.T) {
							got := image["dolt_nonlocal_tables"]
							if root.name == "before" {
								if len(got.Rows) != 0 {
									t.Fatal("later declarations appeared in the earlier physical root")
								}
							} else if !equalNativePhysicalRootTable(got, declarations) {
								t.Fatalf("historical declaration columns/rows changed: got %+v want %+v", got, declarations)
							}
						})
						t.Run("physical_census", func(t *testing.T) {
							for _, name := range []string{"only_alias", "later_rows", "dolt_history_archived_rows"} {
								if _, present := image[name]; present {
									t.Errorf("nonphysical or later table %s appeared in retained root", name)
								}
							}
							for name := range image {
								if name != "archived_rows" && name != "dolt_nonlocal_tables" {
									t.Errorf("unexpected physical table %s", name)
								}
							}
						})
					})
					t.Run("direct_physical_table", func(t *testing.T) {
						table, present, err := candidate.ReadTable(ctx, root.id, "archived_rows")
						if err != nil || !present {
							t.Fatalf("retained physical table unavailable: present=%t error=%v", present, err)
						}
						assertNativePhysicalRootRows(t, table)
					})
					for _, name := range []string{"only_alias", "never_existed"} {
						t.Run("typed_absence_"+name, func(t *testing.T) {
							table, present, err := candidate.ReadTable(ctx, root.id, name)
							if err != nil || present || len(table.Columns) != 0 || len(table.Rows) != 0 {
								t.Fatalf("absence must be a distinct false result with no fabricated schema/rows: present=%t error_type=%T error=%v table=%+v", present, err, err, table)
							}
						})
					}
				})
			}
		})
	}
}

func assertNativePhysicalRootRows(t *testing.T, got Table) {
	t.Helper()
	want := Table{Columns: []string{"id", "old_column", "opaque"}, Rows: []Row{
		{"id": 1, "old_column": "original", "opaque": map[string]string{"$bytes": "AP8="}},
		{"id": 1, "old_column": "original", "opaque": map[string]string{"$bytes": "AP8="}},
		{"id": 2, "old_column": nil, "opaque": nil},
	}}
	if !equalNativePhysicalRootTable(got, want) {
		t.Fatalf("old columns, duplicate multiplicity, NULL or binary cells changed: got %+v want %+v", got, want)
	}
}

func equalNativePhysicalRootTable(a, b Table) bool {
	canonical := func(table Table) string {
		columns := append([]string{}, table.Columns...)
		sort.Strings(columns)
		rows := make([]string, len(table.Rows))
		for i, row := range table.Rows {
			data, err := marshal(row)
			if err != nil {
				return "invalid"
			}
			rows[i] = string(data)
		}
		sort.Strings(rows)
		return strings.Join(columns, "\x00") + "\n" + strings.Join(rows, "\n")
	}
	return canonical(a) == canonical(b)
}
