package physicalcapture_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	embedded "github.com/dolthub/driver/v2"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
)

var currentOnlyFamilies = []string{
	"bd_events_journal", "bd_events_seq", "events", "ignored_schema_migrations",
	"leases", "local_metadata", "repo_mtimes", "wisp_child_counters",
	"wisp_comments", "wisp_dependencies", "wisp_events", "wisp_labels", "wisps",
}

func workingFixture(t *testing.T) (context.Context, *sql.Conn) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	params := url.Values{"commitname": {"Fixture"}, "commitemail": {"fixture@example.test"}, "disable_singleton_cache": {"true"}, "multistatements": {"true"}}
	cfg, err := embedded.ParseDSN("file://" + dir + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	connector, err := embedded.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db, release := sql.OpenDB(connector), connector.Close
	t.Cleanup(func() { _ = db.Close(); _ = release() })
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	workingExec(t, ctx, conn, "CREATE DATABASE public_working_source")
	workingExec(t, ctx, conn, "USE public_working_source")
	return ctx, conn
}

func workingExec(t *testing.T, ctx context.Context, conn *sql.Conn, query string, args ...any) {
	t.Helper()
	if _, err := conn.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func workingCommit(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	for _, query := range []string{"CALL DOLT_ADD('-A')", "CALL DOLT_COMMIT('--allow-empty','-m','public typed working-root fixture','--author','Fixture <fixture@example.test>')"} {
		if err := schema.DrainCall(ctx, conn, query); err != nil {
			t.Fatal(err)
		}
	}
	head, err := versioncontrolops.CurrentCommit(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func workingRead(t *testing.T, ctx context.Context, conn *sql.Conn, head string) physicalcapture.WorkingImage {
	t.Helper()
	image, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	if image.Format != physicalcapture.WorkingImageFormat || !image.ReadOnlyScope || !image.Complete || image.Database != "public_working_source" || image.HeadCommitID != head || len(image.WorkingRootHash) != 32 {
		t.Fatalf("unauthenticated/incomplete working image: %+v", image)
	}
	return image
}

func workingTable(t *testing.T, image physicalcapture.WorkingImage, name string) physicalcapture.Table {
	t.Helper()
	for _, table := range image.Tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("complete physical census omitted %s", name)
	return physicalcapture.Table{}
}

func workingRowKeys(t *testing.T, rows [][]driver.Value) []string {
	t.Helper()
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, string(data))
	}
	sort.Strings(result)
	return result
}

func TestWorkingRootCompleteTypedCurrentOnlyCensus(t *testing.T) {
	ctx, conn := workingFixture(t)
	workingExec(t, ctx, conn, "CREATE TABLE versioned_rows (id BIGINT PRIMARY KEY, label TEXT)")
	workingExec(t, ctx, conn, "INSERT INTO versioned_rows VALUES (1,'immutable')")
	for _, name := range currentOnlyFamilies {
		// Minimal synthetic schemas deliberately exercise every current-only
		// family name. Actual Beads schema coverage is a separate control below.
		workingExec(t, ctx, conn, "INSERT INTO dolt_ignore (pattern,ignored) VALUES (?,true)", name)
		workingExec(t, ctx, conn, "CREATE TABLE `"+name+"` (id BIGINT, metadata JSON, opaque LONGBLOB)")
		workingExec(t, ctx, conn, "INSERT INTO `"+name+"` VALUES (9007199254740993,JSON_ARRAY(1,'public'),X'00FF'),(1,NULL,NULL),(2,CAST('null' AS JSON),X''),(1,NULL,NULL)")
	}
	head := workingCommit(t, ctx, conn)
	immutable, err := physicalcapture.ReadCommit(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range immutable.Tables {
		for _, name := range currentOnlyFamilies {
			if table.Name == name {
				t.Fatalf("ignored table %s unexpectedly tracked by fixture", name)
			}
		}
	}
	workingExec(t, ctx, conn, "INSERT INTO versioned_rows VALUES (2,'uncommitted working row')")
	// The ordinary SQL oracle is measured before introducing a self-alias.
	var metadata string
	if err := conn.QueryRowContext(ctx, "SELECT metadata FROM events WHERE id=9007199254740993").Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	workingExec(t, ctx, conn, "INSERT INTO dolt_nonlocal_tables VALUES ('dolt_nonlocal_tables','HEAD','versioned_rows',NULL)")
	before := workingRead(t, ctx, conn, head)
	t.Run("all_stored_names_no_aliases", func(t *testing.T) {
		var names []string
		for _, table := range before.Tables {
			names = append(names, table.Name)
		}
		want := append(append([]string{}, currentOnlyFamilies...), "dolt_ignore", "dolt_nonlocal_tables", "versioned_rows")
		sort.Strings(want)
		if !reflect.DeepEqual(names, want) {
			t.Fatalf("physical working census: got %v want %v", names, want)
		}
	})
	for _, name := range currentOnlyFamilies {
		t.Run(name, func(t *testing.T) {
			table := workingTable(t, before, name)
			wantColumns := []physicalcapture.Column{{Name: "id", DatabaseTypeName: "bigint"}, {Name: "metadata", DatabaseTypeName: "json"}, {Name: "opaque", DatabaseTypeName: "longblob"}}
			if !reflect.DeepEqual(table.Columns, wantColumns) {
				t.Fatalf("actual SQL types changed: %+v", table.Columns)
			}
			wantRows := [][]driver.Value{{int64(9007199254740993), metadata, []byte{0, 255}}, {int64(1), nil, nil}, {int64(2), "null", []byte{}}, {int64(1), nil, nil}}
			if !reflect.DeepEqual(workingRowKeys(t, table.Rows), workingRowKeys(t, wantRows)) {
				t.Fatalf("current-only cells, NULL tags or multiplicity changed: %+v", table.Rows)
			}
		})
	}
	t.Run("working_rows_are_not_head_rows", func(t *testing.T) {
		if len(workingTable(t, before, "versioned_rows").Rows) != 2 {
			t.Fatal("working root was replaced by immutable head")
		}
	})
	t.Run("self_alias_declaration_is_physical", func(t *testing.T) {
		if len(workingTable(t, before, "dolt_nonlocal_tables").Rows) != 1 {
			t.Fatal("declaration census used SQL resolver")
		}
	})
	t.Run("valid_missing_head_refuses_no_partial_image", func(t *testing.T) {
		missing := strings.Repeat("a", 32)
		var count int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_log WHERE commit_hash=?", missing).Scan(&count); err != nil || count != 0 {
			t.Fatal("missing exact head was not independently established")
		}
		image, err := physicalcapture.ReadWorkingRoot(ctx, conn, missing)
		if err == nil || !reflect.DeepEqual(image, physicalcapture.WorkingImage{}) {
			t.Fatal("head mismatch accepted or returned partial evidence")
		}
	})
	t.Run("occupied_scope_preserved", func(t *testing.T) {
		workingExec(t, ctx, conn, "START TRANSACTION")
		image, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
		if err == nil || !reflect.DeepEqual(image, physicalcapture.WorkingImage{}) {
			t.Fatal("occupied connection accepted")
		}
		workingExec(t, ctx, conn, "ROLLBACK")
	})
	t.Run("cancelled_capture_has_no_partial_image", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		image, err := physicalcapture.ReadWorkingRoot(cancelled, conn, head)
		if err == nil || !reflect.DeepEqual(image, physicalcapture.WorkingImage{}) {
			t.Fatal("canceled capture returned success or partial evidence")
		}
	})
	t.Run("owned_binary_no_source_mutation", func(t *testing.T) {
		table := workingTable(t, before, "events")
		for _, row := range table.Rows {
			if cell, ok := row[2].([]byte); ok && len(cell) > 0 {
				cell[0] = 7
			}
		}
		after := workingRead(t, ctx, conn, head)
		if before.WorkingRootHash != after.WorkingRootHash {
			t.Fatal("read changed working root")
		}
		for _, row := range workingTable(t, after, "events").Rows {
			if cell, ok := row[2].([]byte); ok && len(cell) > 0 && cell[0] != 0 {
				t.Fatal("borrowed binary buffer changed source")
			}
		}
		actualHead, err := versioncontrolops.CurrentCommit(ctx, conn)
		if err != nil || actualHead != head {
			t.Fatal("capture changed source history")
		}
	})
	t.Run("other_database_cannot_borrow_source_head", func(t *testing.T) {
		workingExec(t, ctx, conn, "CREATE DATABASE public_other_source")
		workingExec(t, ctx, conn, "USE public_other_source")
		defer workingExec(t, ctx, conn, "USE public_working_source")
		workingExec(t, ctx, conn, "CREATE TABLE distinct_source (id BIGINT PRIMARY KEY)")
		otherHead := workingCommit(t, ctx, conn)
		image, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
		if err == nil || !reflect.DeepEqual(image, physicalcapture.WorkingImage{}) {
			t.Fatal("another database accepted original source head")
		}
		image, err = physicalcapture.ReadWorkingRoot(ctx, conn, otherHead)
		if err != nil || image.Database != "public_other_source" || image.HeadCommitID != otherHead || !image.Complete || !image.ReadOnlyScope {
			t.Fatal("observed database identity was replaced by a caller label")
		}
	})
	t.Logf("PUBLIC_TYPED_WORKING_CENSUS families=%d rows_each=4 complete=true current_only_history=false", len(currentOnlyFamilies))
}

func TestWorkingRootActualBeadsSchemaCensus(t *testing.T) {
	for _, version := range []int{66, 72} {
		t.Run(fmt.Sprintf("actual_schema_%d", version), func(t *testing.T) {
			ctx, conn := workingFixture(t)
			if _, err := schema.MigrateUpTo(ctx, conn, version); err != nil {
				t.Fatal(err)
			}
			if version == 72 {
				// A fresh public fixture may apply the real ignored-source pass;
				// capture itself never migrates or invents those tables.
				if _, err := schema.MigrateUp(ctx, conn); err != nil {
					t.Fatal(err)
				}
			}
			head := workingCommit(t, ctx, conn)
			rows, err := conn.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_type='BASE TABLE'")
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(err)
				}
				names = append(names, name)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			before := workingRead(t, ctx, conn, head)
			// Information schema omits stored Dolt system tables. Compare every public
			// user table and explicitly retain the additional physical tables.
			for _, name := range names {
				table := workingTable(t, before, name)
				for _, column := range table.Columns {
					if column.Name == "" || column.DatabaseTypeName == "" {
						t.Fatalf("missing actual column/type in %s", name)
					}
				}
			}
			if version == 72 {
				for _, name := range currentOnlyFamilies {
					_ = workingTable(t, before, name)
				}
			}
			after := workingRead(t, ctx, conn, head)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("working schema/cells/source identity changed during capture")
			}
			t.Logf("PUBLIC_ACTUAL_TYPED_SCHEMA version=%d usertables=%d physicaltables=%d", version, len(names), len(before.Tables))
		})
	}
}
