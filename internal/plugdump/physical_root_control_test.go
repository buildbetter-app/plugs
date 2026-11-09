package plugdump

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/dolthub/driver/v2"
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

func newNativePhysicalRootControlCandidate(t *testing.T, db *sql.DB, server *testutil.IsolatedDoltContainer) nativePhysicalRootControlCandidate {
	// Option 2 is reached only after the exact option-1 runtime RED. Stop this
	// owned public fixture before copying, and never check out its source store.
	setup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var absent int
	if err := db.QueryRowContext(setup, "SELECT count(*) FROM dolt_log WHERE commit_hash = ?", strings.Repeat("a", 32)).Scan(&absent); err != nil || absent != 0 {
		t.Fatalf("valid missing commit census: count=%d error=%v", absent, err)
	}
	code, directory, err := server.Exec(setup, []string{"pwd"})
	if err != nil || code != 0 {
		t.Fatalf("public fixture directory: code=%d error=%v", code, err)
	}
	if err := server.Stop(setup); err != nil {
		t.Fatal(err)
	}
	snapshot, err := os.MkdirTemp("/tmp", "native-physical-option2-snapshot-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(snapshot); err != nil {
			t.Error(err)
		}
		_, err := os.Lstat(snapshot)
		if !os.IsNotExist(err) {
			t.Errorf("option2 public snapshot cleanup: %v", err)
		}
		t.Logf("OPTION2_CLEANUP snapshot_absent=%t", os.IsNotExist(err))
	})
	copyTree := func(src, dst string) error {
		return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			rel, err := filepath.Rel(src, path)
			if err != nil {
				return err
			}
			target := filepath.Join(dst, rel)
			if entry.IsDir() {
				return os.MkdirAll(target, 0700)
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("nonregular public source entry")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, 0600)
		})
	}
	if err := copyTree(filepath.Join(strings.TrimSpace(directory), "beads_test"), filepath.Join(snapshot, "beads_test")); err != nil {
		t.Fatal(err)
	}
	read := func(ctx context.Context, id string) (map[string]Table, error) {
		if !validCommitID(id) {
			return nil, fmt.Errorf("invalid immutable revision")
		}
		root, err := os.MkdirTemp("/tmp", "native-physical-option2-read-")
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := os.RemoveAll(root); err != nil {
				t.Error(err)
			}
			_, err := os.Lstat(root)
			if !os.IsNotExist(err) {
				t.Errorf("option2 read copy cleanup: %v", err)
			}
			t.Logf("OPTION2_CLEANUP read_copy_absent=%t", os.IsNotExist(err))
		}()
		dbdir := filepath.Join(root, "beads_test")
		if err := copyTree(filepath.Join(snapshot, "beads_test"), dbdir); err != nil {
			return nil, err
		}
		code, out, err := server.Exec(ctx, []string{"sh", "-c", `cd "$1" && exec dolt checkout --detach "$2"`, "spike-detached-checkout", dbdir, id})
		t.Logf("OPTION2_DETACHED commit=%s code=%d error_type=%T error=%v output=%s", id, code, err, err, out)
		if err != nil || code != 0 {
			// Probe the other documented-looking commit form as well; an
			// unsupported flag alone must not be called an exhaustive failure.
			plainCode, plainOut, plainErr := server.Exec(ctx, []string{"sh", "-c", `cd "$1" && exec dolt checkout "$2"`, "spike-commit-checkout", dbdir, id})
			t.Logf("OPTION2_COMMIT_FORM commit=%s code=%d error_type=%T error=%v output=%s", id, plainCode, plainErr, plainErr, plainOut)
			return nil, fmt.Errorf("detached checkout unavailable: flag code=%d error=%v; commit form code=%d error=%v", code, err, plainCode, plainErr)
		}
		copyDB, err := sql.Open("dolt", "file://"+root+"?database=beads_test&disable_singleton_cache=true")
		if err != nil {
			return nil, err
		}
		defer copyDB.Close()
		copyDB.SetMaxOpenConns(1)
		port := SQLSource{DB: copyDB}
		declarations, err := port.historyTable(ctx, "SELECT * FROM dolt_nonlocal_tables")
		if err != nil {
			return nil, err
		}
		// Only the disposable working set is changed, without a Dolt commit.
		if _, err := copyDB.ExecContext(ctx, "DELETE FROM dolt_nonlocal_tables"); err != nil {
			return nil, err
		}
		var observed string
		if err := copyDB.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&observed); err != nil {
			return nil, err
		}
		if observed != id {
			return nil, fmt.Errorf("detached immutable identity changed")
		}
		tx, err := copyDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		port = SQLSource{DB: tx}
		names, err := port.historyTable(ctx, "SHOW FULL TABLES")
		if err != nil {
			return nil, err
		}
		if len(names.Columns) != 2 {
			return nil, fmt.Errorf("invalid physical census")
		}
		image := map[string]Table{"dolt_nonlocal_tables": declarations}
		for _, row := range names.Rows {
			name := historyText(row[names.Columns[0]])
			if historyText(row[names.Columns[1]]) == "VIEW" || name == "dolt_nonlocal_tables" {
				continue
			}
			if !nativeSQLName.MatchString(name) {
				return nil, fmt.Errorf("invalid physical table name")
			}
			table, err := port.historyTable(ctx, "SELECT * FROM `"+name+"`")
			if err != nil {
				return nil, err
			}
			image[name] = table
		}
		return image, nil
	}
	candidate := nativePhysicalRootControlCandidate{
		Name:      "disposable-copy-detached-checkout",
		ReadImage: read,
		ReadTable: func(ctx context.Context, id, name string) (Table, bool, error) {
			image, err := read(ctx, id)
			if err != nil {
				return Table{}, false, err
			}
			table, present := image[name]
			return table, present, nil
		},
	}
	t.Run("valid_missing_commit_is_error", func(t *testing.T) {
		id := strings.Repeat("a", 32)
		if _, err := candidate.ReadImage(setup, id); err == nil {
			t.Error("valid missing immutable image did not refuse")
		}
		if _, present, err := candidate.ReadTable(setup, id, "archived_rows"); err == nil || present {
			t.Error("valid missing immutable table root became absence")
		}
	})
	return candidate
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
