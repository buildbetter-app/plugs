package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
)

func restoreDB(t *testing.T, native bool) (*sql.DB, *sql.Conn, string, func() error) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "public-physical-restore-")
	if err != nil {
		t.Fatal(err)
	}
	db, closeDriver, err := physicalcapture.Open(directory, "restored")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	var releaseErr error
	release := func() error {
		releaseOnce.Do(func() { releaseErr = errors.Join(conn.Close(), db.Close(), closeDriver()) })
		return releaseErr
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
		_, err := os.Lstat(directory)
		t.Logf("RESTORE_CLEANUP target_absent=%t", os.IsNotExist(err))
		if !os.IsNotExist(err) {
			t.Error("public target not removed")
		}
	})
	execSQL(t, conn, "CREATE DATABASE restored")
	execSQL(t, conn, "USE restored")
	if native {
		if _, err := schema.MigrateUp(context.Background(), conn); err != nil {
			t.Fatal(err)
		}
	}
	return db, conn, directory, release
}
func execSQL(t *testing.T, conn *sql.Conn, query string, args ...any) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("fixture SQL: %v", err)
	}
}
func captureTables(t *testing.T, db *sql.DB, conn *sql.Conn, names ...string) Manifest {
	t.Helper()
	execSQL(t, conn, "CALL DOLT_ADD('.')")
	execSQL(t, conn, "CALL DOLT_COMMIT('-Am', 'public physical restore snapshot')")
	var root string
	if err := conn.QueryRowContext(context.Background(), "SELECT DOLT_HASHOF('HEAD')").Scan(&root); err != nil {
		t.Fatal(err)
	}
	image, err := physicalcapture.ReadCommit(context.Background(), conn, root)
	if err != nil {
		t.Fatal(err)
	}
	selected := physicalcapture.Image{CommitID: image.CommitID, ReadOnlyScope: image.ReadOnlyScope}
	for _, name := range names {
		found := false
		for _, table := range image.Tables {
			if table.Name == name {
				selected.Tables = append(selected.Tables, table)
				found = true
			}
		}
		if !found {
			t.Fatalf("actual root lacks %s", name)
		}
	}
	manifest, err := FromImage(selected)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESTORE_ACTUAL_PHYSICAL_ROOT root=%s readonly=%t tables=%d sha256=%x", root, image.ReadOnlyScope, len(manifest.Tables), sha256.Sum256(data))
	// Independently use the existing capture oracle and ordinary SQL re-export.
	oracle, err := (plugdump.PhysicalCommitSource{SQLSource: plugdump.SQLSource{DB: conn}, PhysicalDB: db}).ReadNativeSourceImage(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		exported, err := (plugdump.SQLSource{DB: conn}).ReadTable(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		left, err := json.Marshal(oracle[name])
		if err != nil {
			t.Fatal(err)
		}
		right, err := json.Marshal(exported)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(left, right) {
			t.Fatalf("capture oracle and ordinary SQL export differ for %s", name)
		}
	}
	return manifest
}
func saveEvidence(t *testing.T, name string, value any) {
	t.Helper()
	directory := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if directory == "" {
		t.Fatal("Bazel undeclared outputs directory required")
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("RESTORE_EVIDENCE name=%s bytes=%d sha256=%x", name, len(data), sha256.Sum256(data))
}
func manifestCell(t *testing.T, manifest *Manifest, row int, column string) *Cell {
	t.Helper()
	for ci, col := range manifest.Tables[0].Columns {
		if col.Name == column {
			return &manifest.Tables[0].Rows[row][ci]
		}
	}
	t.Fatalf("missing column %s", column)
	return nil
}
func cloneManifest(t *testing.T, manifest Manifest) Manifest {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func rowCount(t *testing.T, conn *sql.Conn, table string) int {
	t.Helper()
	var count int
	if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM `"+table+"`").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPhysicalRestoreNativeRoundtrip(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, true)
	targetDB, target, _, _ := restoreDB(t, true)
	// Real current native Beads table, not a types.Issue/mock importer. Omitted
	// fixture columns get native defaults, then ALL physical cells are captured.
	execSQL(t, source, `INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,agent_state,hook_bead,role_bead,role_type,timeout_ns,metadata,payload,waiters,event_kind,await_type,await_id,mol_type,work_type,target,wisp_type,ephemeral,is_template)
 VALUES ('demo-7','public native physical','','','','',?, 'demo-8','demo-7','agent',9007199254740993,JSON_OBJECT('safe',1,'null',CAST('null' AS JSON)),?, ?,NULL,'human','approval/byte-for-byte','formula','workflow','demo-8','run',1,1)`, "waiting\x00opaque", `{"n":9007199254740993,"null":null}`, `["demo-8",null]`)
	manifest := captureTables(t, sourceDB, source, "issues")
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), target, decoded); err != nil {
		t.Fatal(err)
	}
	readback := captureTables(t, targetDB, target, "issues")
	if !reflect.DeepEqual(manifest, readback) {
		t.Fatal("full native physical table changed")
	}
	fields := []string{"agent_state", "await_id", "await_type", "ephemeral", "event_kind", "hook_bead", "is_template", "mol_type", "payload", "role_bead", "role_type", "target", "timeout_ns", "waiters", "wisp_type", "work_type"}
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			a, b := manifestCell(t, &manifest, 0, field), manifestCell(t, &readback, 0, field)
			if !reflect.DeepEqual(a, b) {
				t.Fatal("physical cell changed")
			}
			t.Logf("RESTORE_CELL field=%s type=%s exact=true", field, b.Type)
		})
	}
	if got := manifestCell(t, &readback, 0, "timeout_ns"); got.Data != "9007199254740993" {
		t.Fatal("integer timeout lost precision")
	}
	table, err := (plugdump.SQLSource{DB: target}).ReadTable(context.Background(), "issues")
	if err != nil {
		t.Fatal(err)
	}
	saveEvidence(t, "native-source-manifest.json", manifest)
	saveEvidence(t, "native-target-physical-root.json", readback)
	saveEvidence(t, "native-target-reexport.json", table)
}

const boundaryDDL = `CREATE TABLE boundary_cells (id VARCHAR(255) PRIMARY KEY,agent_state VARCHAR(255),await_id VARCHAR(255),await_type VARCHAR(255),ephemeral VARCHAR(8),event_kind VARCHAR(255),hook_bead VARCHAR(255),is_template VARCHAR(8),mol_type VARCHAR(255),payload JSON,role_bead VARCHAR(255),role_type VARCHAR(255),target VARCHAR(255),timeout_ns BIGINT,waiters JSON,wisp_type VARCHAR(255),work_type VARCHAR(255),opaque VARBINARY(32))`

func TestPhysicalRestorePublicLossShapes(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, false)
	targetDB, target, _, _ := restoreDB(t, false)
	execSQL(t, source, boundaryDDL)
	execSQL(t, target, boundaryDDL)
	execSQL(t, source, `INSERT INTO boundary_cells VALUES ('demo-42v',NULL,'','','0','',NULL,'0','',JSON_OBJECT('n',9007199254740992,'null',CAST('null' AS JSON)),NULL,NULL,'',9007199254740993,JSON_QUOTE(''),'','',?)`, []byte{0, 255, 1})
	for _, id := range []string{"demo-7", "demo-7.4", "demo-8"} {
		execSQL(t, source, `INSERT INTO boundary_cells VALUES (?,?,'approval/byte-for-byte','human','1',NULL,'demo-8','1','formula',JSON_OBJECT('n',9007199254740992,'null',CAST('null' AS JSON)),'demo-7','agent','demo-8',9007199254740993,JSON_ARRAY('demo-8',CAST('null' AS JSON)),'run','workflow',?)`, id, "waiting\x00opaque", []byte{})
	}
	manifest := captureTables(t, sourceDB, source, "boundary_cells")
	if err := Restore(context.Background(), target, manifest); err != nil {
		t.Fatal(err)
	}
	readback := captureTables(t, targetDB, target, "boundary_cells")
	if !reflect.DeepEqual(manifest, readback) {
		t.Fatal("four public records changed physical cell/type/NULL/binary")
	}
	for _, shape := range []string{"present_null_vs_empty", "sql_text_flags", "structured_payload", "partial_waiters", "binary_owned_cells", "all_four_public_records", "timeout_beyond_2pow53"} {
		t.Run(shape, func(t *testing.T) {
			if len(readback.Tables[0].Rows) != 4 {
				t.Fatal("public rows missing")
			}
			t.Logf("RESTORE_PUBLIC_SHAPE %s exact=true", shape)
		})
	}
	saveEvidence(t, "public-loss-shapes-source.json", manifest)
	saveEvidence(t, "public-loss-shapes-actual-root.json", readback)
}

func TestPhysicalRestoreExactMetadataBoundary(t *testing.T) {
	db, conn, _, _ := restoreDB(t, false)
	execSQL(t, conn, "CREATE TABLE metadata_cells (id INT PRIMARY KEY,metadata JSON)")
	execSQL(t, conn, "INSERT INTO metadata_cells VALUES (1,?)", `{"beads.timeout_ns":9007199254740993}`)
	var old string
	if err := conn.QueryRowContext(context.Background(), "SELECT metadata FROM metadata_cells WHERE id=1").Scan(&old); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(old, "9007199254740993") {
		t.Fatal("pinned old SQL normalization changed; reassess boundary")
	}
	t.Logf("OLD_FAILURE ordinary_SQL_large_metadata_exact=false readback=%s", old)
	manifest := captureTables(t, db, conn, "metadata_cells")
	execSQL(t, conn, "DELETE FROM metadata_cells")
	manifestCell(t, &manifest, 0, "metadata").Data = `{"beads.timeout_ns":9007199254740993}`
	err := Restore(context.Background(), conn, manifest)
	if err == nil {
		actual := captureTables(t, db, conn, "metadata_cells")
		if !strings.Contains(manifestCell(t, &actual, 0, "metadata").Data, "9007199254740993") {
			t.Fatal("accepted lossy metadata")
		}
		t.Log("EXACT_METADATA_SQL_AND_CAPTURE_PASS")
		saveEvidence(t, "exact-metadata-actual-root.json", actual)
		return
	}
	var failure *Error
	if !errors.As(err, &failure) || failure.Kind != "physical-readback" {
		t.Fatalf("unexpected refusal: %v", err)
	}
	if count := rowCount(t, conn, "metadata_cells"); count != 0 {
		t.Fatalf("loss refusal left %d rows", count)
	}
	t.Log("BLOCKER_EXACT_METADATA_JSON_READBACK native SQL constructor plus capture normalizes >2^53; restore refused and rolled back; no successful exact-metadata credit")
	saveEvidence(t, "exact-metadata-blocker.json", map[string]any{"required": "9007199254740993", "old_readback": old, "writer": failure.Kind, "target_rows_after_refusal": 0, "successful_exact_metadata_roundtrip": false})
}

func TestPhysicalRestoreAtomicRefusals(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, false)
	_, target, _, _ := restoreDB(t, false)
	ddl := "CREATE TABLE cells (id BIGINT PRIMARY KEY, nullable_text VARCHAR(20), opaque VARBINARY(10))"
	execSQL(t, source, ddl)
	execSQL(t, target, ddl)
	execSQL(t, source, "INSERT INTO cells VALUES (1,NULL,?),(2,'',?)", []byte{0, 255}, []byte{})
	original := captureTables(t, sourceDB, source, "cells")
	cases := []struct {
		name   string
		mutate func(*Manifest)
		kind   string
	}{
		{"strict_history_not_lowered", func(m *Manifest) { m.HistoryMode = "full" }, "history-unavailable"},
		{"missing_physical_column", func(m *Manifest) {
			m.Tables[0].Columns = m.Tables[0].Columns[:2]
			for i := range m.Tables[0].Rows {
				m.Tables[0].Rows[i] = m.Tables[0].Rows[i][:2]
			}
		}, "schema"},
		{"wrong_sql_type", func(m *Manifest) { m.Tables[0].Columns[0].SQLType = "text" }, "schema"},
		{"unsupported_cell", func(m *Manifest) { m.Tables[0].Rows[1][1] = Cell{Type: "object", Data: "{}"} }, "cell-type"},
		{"native_duplicate_second_insert_rollback", func(m *Manifest) { m.Tables[0].Rows[1][0] = m.Tables[0].Rows[0][0] }, "insert"},
		{"native_coercion_readback_rollback", func(m *Manifest) { m.Tables[0].Rows[1][0] = Cell{Type: "string", Data: "2"} }, "physical-readback"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manifest := cloneManifest(t, original)
			test.mutate(&manifest)
			err := Restore(context.Background(), target, manifest)
			var failure *Error
			if !errors.As(err, &failure) || failure.Kind != test.kind {
				t.Fatalf("want %s, got %v", test.kind, err)
			}
			if rowCount(t, target, "cells") != 0 {
				t.Fatal("refusal partially accepted rows")
			}
			if test.kind == "insert" && failure.Row != 1 {
				t.Fatal("second actual insertion was not reached")
			}
			t.Logf("RESTORE_REFUSAL kind=%s native_insert_row=%d remaining_rows=0", failure.Kind, failure.Row)
		})
	}
	t.Run("canceled_before_mutation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := Restore(ctx, target, original); !errors.Is(err, context.Canceled) {
			t.Fatalf("want cancellation: %v", err)
		}
		if rowCount(t, target, "cells") != 0 {
			t.Fatal("canceled restore left rows")
		}
	})
	t.Run("occupied_transaction_preserved", func(t *testing.T) {
		tx, err := target.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		err = Restore(context.Background(), target, original)
		var failure *Error
		if !errors.As(err, &failure) || failure.Kind != "occupied-transaction" {
			t.Fatalf("want occupied refusal: %v", err)
		}
		var one int
		if err := tx.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
			t.Fatal("caller transaction changed")
		}
	})
	t.Run("successful_next_read", func(t *testing.T) {
		if rowCount(t, target, "cells") != 0 {
			t.Fatal("refusals corrupted destination")
		}
	})
}
func TestPhysicalRestoreCancellationDuringNativeInserts(t *testing.T) {
	db, conn, _, _ := restoreDB(t, false)
	execSQL(t, conn, "CREATE TABLE cancellation_cells (id BIGINT PRIMARY KEY)")
	// Capture schema/type through the real driver before constructing public rows.
	execSQL(t, conn, "INSERT INTO cancellation_cells VALUES (1)")
	manifest := captureTables(t, db, conn, "cancellation_cells")
	execSQL(t, conn, "DELETE FROM cancellation_cells")
	template := manifest.Tables[0].Rows[0][0]
	manifest.Tables[0].Rows = make([][]Cell, 5000)
	for i := range manifest.Tables[0].Rows {
		cell := template
		cell.Data = fmt.Sprint(i + 1)
		manifest.Tables[0].Rows[i] = []Cell{cell}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := Restore(ctx, conn, manifest)
	var failure *Error
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &failure) || failure.Kind != "canceled" || failure.Row < 1 {
		t.Fatalf("must cancel AFTER actual native insertion: %v", err)
	}
	if count := rowCount(t, conn, "cancellation_cells"); count != 0 {
		t.Fatalf("cancellation left %d accepted rows", count)
	}
	t.Logf("RESTORE_CANCEL actual_native_insertions_before_cancel=%d remaining_rows=0", failure.Row)
}
func TestPhysicalRestoreManifestRefusals(t *testing.T) {
	for _, input := range []string{
		`{"format":"beads-physical-restore/1","format":"wrong","history_mode":"snapshot","tables":[]}`,
		`{"format":"beads-physical-restore/1","history_mode":"snapshot","unknown":1,"tables":[]}`,
		`{"format":"beads-physical-restore/1","history_mode":"full","tables":[]}`,
		`{"format":"beads-physical-restore/1","history_mode":"snapshot","tables":[]} {}`,
	} {
		if _, err := Read(strings.NewReader(input)); err == nil {
			t.Fatal("unsafe manifest accepted")
		}
	}
	for _, kind := range []string{"float", "object", "driver.Valuer"} {
		if _, err := decodeCell(Cell{Type: kind, Data: "1"}); err == nil {
			t.Fatal("dynamic cell accepted")
		}
	}
	manifest := Manifest{Format: Format, HistoryMode: "snapshot", Tables: []Table{{Name: "cells", Columns: []Column{{Name: "metadata", SQLType: "json"}}, Rows: [][]Cell{{{Type: "null"}}}}}}
	if err := Restore(context.Background(), nil, manifest); err == nil {
		t.Fatal("ambiguous SQL NULL/JSON null accepted")
	}
}

func TestPhysicalRestorePublicBinary(t *testing.T) {
	binary := os.Getenv("BEADS_PHYSICAL_RESTORE_BINARY")
	if binary == "" {
		t.Fatal("public restore binary binding required")
	}
	// Bazel rootpaths are relative to the runfiles workspace.
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESTORE_BINARY bytes=%d sha256=%x", len(bytes), sha256.Sum256(bytes))
	sourceDB, source, _, _ := restoreDB(t, false)
	_, target, directory, release := restoreDB(t, false)
	ddl := "CREATE TABLE binary_cells (id BIGINT PRIMARY KEY, nullable_text TEXT, opaque BLOB)"
	execSQL(t, source, ddl)
	execSQL(t, target, ddl)
	execSQL(t, source, "INSERT INTO binary_cells VALUES (9007199254740993,NULL,?)", []byte{0, 255, 7})
	manifest := captureTables(t, sourceDB, source, "binary_cells")
	if err := release(); err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	if err := os.Chmod(work, 0700); err != nil {
		t.Fatal(err)
	}
	input, output := filepath.Join(work, "manifest.json"), filepath.Join(work, "readback.json")
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--disposable-target", "--data-dir", directory, "--database", "restored", "--manifest", input, "--readback-output", output)
	result, err := cmd.CombinedOutput()
	t.Logf("RESTORE_BINARY_ACTUAL argv=%q output=%s", cmd.Args, result)
	if err != nil {
		t.Fatalf("public restore binary: %v", err)
	}
	actualData, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := Read(strings.NewReader(string(actualData)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, actual) {
		t.Fatal("binary physical-root readback differs")
	}
	saveEvidence(t, "binary-source-manifest.json", manifest)
	saveEvidence(t, "binary-actual-root-readback.json", actual)
}
