package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
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

	embedded "github.com/dolthub/driver/v2"
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
		exported, err := readActualSQLTable(conn, name)
		if name == "issues" {
			exported, err = (plugdump.SQLSource{DB: conn}).ReadTable(context.Background(), name)
		}
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

// Direct ordinary SQL readback for public diagnostic tables. The production
// SQLSource allowlist stays unchanged. This receives no expected cell values.
func readActualSQLTable(conn *sql.Conn, name string) (plugdump.Table, error) {
	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM `"+name+"`")
	if err != nil {
		return plugdump.Table{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return plugdump.Table{}, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return plugdump.Table{}, err
	}
	table := plugdump.Table{Columns: columns, Rows: []plugdump.Row{}}
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return plugdump.Table{}, err
		}
		row := plugdump.Row{}
		for ci, value := range values {
			kind := strings.ToUpper(types[ci].DatabaseTypeName())
			if binary, ok := value.([]byte); ok {
				if strings.Contains(kind, "BLOB") || strings.Contains(kind, "BINARY") {
					value = map[string]string{"$bytes": base64.StdEncoding.EncodeToString(binary)}
				} else {
					value = string(binary)
				}
			}
			if stamp, ok := value.(time.Time); ok {
				value = stamp.UTC().Format(time.RFC3339Nano)
			}
			if text, ok := value.(string); ok && kind == "JSON" {
				decoder := json.NewDecoder(strings.NewReader(text))
				decoder.UseNumber()
				if err := decoder.Decode(&value); err != nil {
					return plugdump.Table{}, err
				}
			}
			row[columns[ci]] = value
		}
		table.Rows = append(table.Rows, row)
	}
	return table, rows.Err()
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

const boundaryDDL = `CREATE TABLE complete_cells (id VARCHAR(255) PRIMARY KEY,agent_state VARCHAR(255),await_id VARCHAR(255),await_type VARCHAR(255),ephemeral VARCHAR(8),event_kind VARCHAR(255),hook_bead VARCHAR(255),is_template VARCHAR(8),mol_type VARCHAR(255),payload JSON,role_bead VARCHAR(255),role_type VARCHAR(255),target VARCHAR(255),timeout_ns BIGINT,waiters JSON,wisp_type VARCHAR(255),work_type VARCHAR(255),opaque VARBINARY(32))`

func TestPhysicalRestorePublicLossShapes(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, false)
	targetDB, target, _, _ := restoreDB(t, false)
	completeDDL := strings.Replace(boundaryDDL, "waiters JSON", "waiters TEXT", 1)
	partialDDL := strings.ReplaceAll(strings.Replace(boundaryDDL, "complete_cells", "partial_cells", 1), "VARCHAR(8)", "TINYINT")
	for _, conn := range []*sql.Conn{source, target} {
		execSQL(t, conn, completeDDL)
		execSQL(t, conn, partialDDL)
	}
	execSQL(t, source, `INSERT INTO complete_cells VALUES ('demo-42v',NULL,'','','0','',NULL,'0','',JSON_OBJECT('n',9007199254740992,'null',CAST('null' AS JSON)),NULL,NULL,'',9007199254740993,'','','',?)`, []byte{0, 255, 1})
	for _, id := range []string{"demo-7", "demo-7.4", "demo-8"} {
		execSQL(t, source, `INSERT INTO partial_cells VALUES (?,?,'approval/byte-for-byte','human',1,NULL,'demo-8',1,'formula',JSON_OBJECT('n',9007199254740992,'null',CAST('null' AS JSON)),'demo-7','agent','demo-8',9007199254740993,JSON_ARRAY('demo-8',CAST('null' AS JSON)),'run','workflow',?)`, id, "waiting\x00opaque", []byte{})
	}
	manifest := captureTables(t, sourceDB, source, "complete_cells", "partial_cells")
	if err := Restore(context.Background(), target, manifest); err != nil {
		t.Fatal(err)
	}
	readback := captureTables(t, targetDB, target, "complete_cells", "partial_cells")
	if !reflect.DeepEqual(manifest, readback) {
		t.Fatal("four public records changed physical cell/type/NULL/binary")
	}
	for _, shape := range []string{"present_null_vs_empty", "sql_text_flags", "structured_payload", "partial_waiters", "binary_owned_cells", "all_four_public_records", "timeout_beyond_2pow53"} {
		t.Run(shape, func(t *testing.T) {
			if len(readback.Tables) != 2 || len(readback.Tables[0].Rows) != 1 || len(readback.Tables[1].Rows) != 3 {
				t.Fatal("public records missing")
			}
			t.Logf("RESTORE_PUBLIC_SHAPE %s exact=true", shape)
		})
	}
	for ti, table := range manifest.Tables {
		for ci, column := range table.Columns {
			if column.Name == "id" || column.Name == "opaque" {
				continue
			}
			t.Run(table.Name+"/"+column.Name, func(t *testing.T) {
				for ri, row := range table.Rows {
					if !reflect.DeepEqual(row[ci], readback.Tables[ti].Rows[ri][ci]) {
						t.Fatal("named physical cell changed")
					}
				}
				t.Logf("RESTORE_PUBLIC_FIELD table=%s field=%s rows=%d exact=true", table.Name, column.Name, len(table.Rows))
			})
		}
	}
	saveEvidence(t, "public-loss-shapes-source.json", manifest)
	saveEvidence(t, "public-loss-shapes-actual-root.json", readback)
}

func TestPhysicalRestoreSQLNullAndJSONNull(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, false)
	targetDB, target, _, _ := restoreDB(t, false)
	ddl := "CREATE TABLE json_cells (id INT PRIMARY KEY,metadata JSON)"
	execSQL(t, source, ddl)
	execSQL(t, target, ddl)
	execSQL(t, source, `INSERT INTO json_cells VALUES (1,NULL),(2,CAST('null' AS JSON)),(3,JSON_OBJECT('nested',CAST('null' AS JSON))),(4,JSON_QUOTE(''))`)
	var sqlNull, jsonNull bool
	if err := source.QueryRowContext(context.Background(), "SELECT metadata IS NULL FROM json_cells WHERE id=1").Scan(&sqlNull); err != nil {
		t.Fatal(err)
	}
	if err := source.QueryRowContext(context.Background(), "SELECT metadata IS NULL FROM json_cells WHERE id=2").Scan(&jsonNull); err != nil {
		t.Fatal(err)
	}
	if !sqlNull || jsonNull {
		t.Fatal("actual SQL fixture does not distinguish SQL NULL/JSON null")
	}
	manifest := captureTables(t, sourceDB, source, "json_cells")
	if cell := manifestCell(t, &manifest, 0, "metadata"); cell.Type != "null" {
		t.Fatal("raw port lost SQL NULL")
	}
	if cell := manifestCell(t, &manifest, 1, "metadata"); cell.Type != "string" || cell.Data != "null" {
		t.Fatal("raw port lost JSON null")
	}
	if err := Restore(context.Background(), target, manifest); err != nil {
		t.Fatal(err)
	}
	actual := captureTables(t, targetDB, target, "json_cells")
	if !reflect.DeepEqual(manifest, actual) {
		t.Fatal("SQL NULL/JSON null/nested null/empty JSON string changed")
	}
	t.Log("RESTORE_NULL_TYPES SQL_NULL=null-tag JSON_null=string-null-tag exact=true")
	saveEvidence(t, "json-null-source-manifest.json", manifest)
	saveEvidence(t, "json-null-actual-root.json", actual)
}

func TestPhysicalRestoreCleanupErrorTree(t *testing.T) {
	refusal, rollback := errors.New("native mismatch"), errors.New("native rollback failure")
	original := &embedded.PhysicalRestoreError{Kind: "rollback-indeterminate", Row: -1, Cause: errors.Join(&embedded.PhysicalRestoreError{Kind: "physical-readback", Table: "cells", Column: "metadata", Row: 1, Cause: refusal}, rollback)}
	translated := translateRestoreError(original)
	var failure *Error
	if !errors.As(translated, &failure) || failure.Kind != "rollback-indeterminate" || !errors.Is(translated, refusal) || !errors.Is(translated, rollback) {
		t.Fatal("facade lost indeterminate cleanup/error tree")
	}
	var leaked *embedded.PhysicalRestoreError
	if errors.As(translated, &leaked) {
		t.Fatal("driver-specific restore error escaped facade")
	}
}

func TestPhysicalRestoreExactMetadataBoundary(t *testing.T) {
	db, conn, _, _ := restoreDB(t, false)
	execSQL(t, conn, "CREATE TABLE metadata_cells (id INT PRIMARY KEY,metadata JSON)")
	const requested = `{"beads.timeout_ns":9007199254740993,"nested":{"array":[9007199254740993,null]}}`
	// Both native representations are RECORDED from actual ordinary SQL. No
	// hand-written rounded result decides native representability.
	execSQL(t, conn, "INSERT INTO metadata_cells VALUES (1,?)", requested)
	var ordinary string
	if err := conn.QueryRowContext(context.Background(), "SELECT metadata FROM metadata_cells").Scan(&ordinary); err != nil {
		t.Fatal(err)
	}
	nativeOrigin := captureTables(t, db, conn, "metadata_cells")
	execSQL(t, conn, "DELETE FROM metadata_cells")
	t.Run("native_origin_successful_roundtrip", func(t *testing.T) {
		if err := Restore(context.Background(), conn, nativeOrigin); err != nil {
			t.Fatal(err)
		}
		actual := captureTables(t, db, conn, "metadata_cells")
		if !reflect.DeepEqual(nativeOrigin, actual) {
			t.Fatal("actual native-origin metadata changed")
		}
		saveEvidence(t, "native-origin-metadata-actual-root.json", actual)
	})
	execSQL(t, conn, "DELETE FROM metadata_cells")
	execSQL(t, conn, `INSERT INTO metadata_cells VALUES (1,JSON_OBJECT('beads.timeout_ns',CAST(? AS SIGNED),'nested',JSON_OBJECT('array',JSON_ARRAY(CAST(? AS SIGNED),CAST('null' AS JSON)))))`, int64(9007199254740993), int64(9007199254740993))
	var constructor string
	if err := conn.QueryRowContext(context.Background(), "SELECT metadata FROM metadata_cells").Scan(&constructor); err != nil {
		t.Fatal(err)
	}
	nativeConstructor := captureTables(t, db, conn, "metadata_cells")
	execSQL(t, conn, "DELETE FROM metadata_cells")
	canonical := func(text string) string {
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	nativeExact := canonical(constructor) == canonical(requested)
	intended := cloneManifest(t, nativeOrigin)
	manifestCell(t, &intended, 0, "metadata").Data = requested
	t.Run("plug_created_value_matches_recorded_native_outcome", func(t *testing.T) {
		err := Restore(context.Background(), conn, intended)
		if nativeExact {
			if err != nil {
				t.Fatalf("native observed exact but writer refused: %v", err)
			}
			actual := captureTables(t, db, conn, "metadata_cells")
			if canonical(manifestCell(t, &actual, 0, "metadata").Data) != canonical(requested) {
				t.Fatal("writer accepted a changed requested value")
			}
			saveEvidence(t, "plug-created-metadata-actual-root.json", actual)
		} else {
			var failure *Error
			if !errors.As(err, &failure) || failure.Kind != "physical-readback" || failure.Table != "metadata_cells" || failure.Column != "metadata" || failure.Row != 0 {
				t.Fatalf("require exact field+row typed native refusal: %v", err)
			}
			if count := rowCount(t, conn, "metadata_cells"); count != 0 {
				t.Fatalf("unrepresentable metadata refusal left %d rows", count)
			}
			t.Log("NATIVE_UNREPRESENTABLE requested exact JSON refused at field=metadata row=0; all writes rolled back")
		}
	})
	saveEvidence(t, "recorded-native-metadata-oracle.json", map[string]any{"requested": json.RawMessage(requested), "ordinary_sql_readback": json.RawMessage(ordinary), "constructor_sql_readback": json.RawMessage(constructor), "constructor_immutable_root": nativeConstructor, "native_represents_requested_exactly": nativeExact, "engine_patch": "NONE; unchanged GMS/native semantics", "native_origin_roundtrip": "required PASS"})
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
			if test.kind == "physical-readback" && (failure.Column != "id" || failure.Row != 1) {
				t.Fatal("actual mismatching field/requested row was not identified")
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

// TIME exercises a native scalar that the current detached capture contract
// cannot carry. Record the actual SQL value/type, then require the real native
// readback error to identify its field/row and roll back the inserted row.
func TestPhysicalRestoreNativeReadbackTypeRefusal(t *testing.T) {
	db, conn, _, _ := restoreDB(t, false)
	execSQL(t, conn, "CREATE TABLE readback_cells (id INT PRIMARY KEY,duration TIME(6))")
	execSQL(t, conn, "INSERT INTO readback_cells VALUES (1,'01:02:03.123456')")
	var native any
	if err := conn.QueryRowContext(context.Background(), "SELECT duration FROM readback_cells").Scan(&native); err != nil {
		t.Fatal(err)
	}
	if _, err := encodeCell(native); err == nil {
		t.Fatalf("native TIME fixture must expose its unsupported detached type; observed %T", native)
	}
	execSQL(t, conn, "DELETE FROM readback_cells")
	manifest := captureTables(t, db, conn, "readback_cells")
	manifest.Tables[0].Rows = [][]Cell{{{Type: "int32", Data: "1"}, {Type: "string", Data: "01:02:03.123456"}}}
	err := Restore(context.Background(), conn, manifest)
	var failure *Error
	if !errors.As(err, &failure) || failure.Kind != "physical-readback" || failure.Table != "readback_cells" || failure.Column != "duration" || failure.Row != 0 {
		t.Fatalf("actual native unsupported-cell readback must name field+row: %v", err)
	}
	if rowCount(t, conn, "readback_cells") != 0 {
		t.Fatal("native readback error left accepted rows")
	}
	t.Logf("RESTORE_NATIVE_READBACK_ERROR sql_value_type=%T field=duration row=0 remaining_rows=0", native)
	saveEvidence(t, "native-readback-type-refusal.json", map[string]any{"ordinary_sql_value_type": fmt.Sprintf("%T", native), "ordinary_sql_value": fmt.Sprint(native), "refusal_kind": failure.Kind, "field": failure.Column, "row": failure.Row, "remaining_rows": 0})
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
	if _, err := decode(manifest); err != nil {
		t.Fatalf("explicit typed SQL NULL refused: %v", err)
	}
	manifest.Tables[0].Rows[0][0] = Cell{Type: "string", Data: "null"}
	if _, err := decode(manifest); err != nil {
		t.Fatalf("explicit typed JSON null refused: %v", err)
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
	if err := os.WriteFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "native-physical-restore"), bytes, 0700); err != nil {
		t.Fatal(err)
	}
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

func TestPhysicalRestoreKeylessAndUnsigned(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, false)
	targetDB, target, _, _ := restoreDB(t, false)
	ddl := "CREATE TABLE keyless_cells (id INT, old_column TEXT, opaque LONGBLOB, exact_unsigned BIGINT UNSIGNED)"
	execSQL(t, source, ddl)
	execSQL(t, target, ddl)
	for i := 0; i < 2; i++ {
		execSQL(t, source, "INSERT INTO keyless_cells VALUES (1,'original',?,18446744073709551615)", []byte{0, 255})
	}
	execSQL(t, source, "INSERT INTO keyless_cells VALUES (2,NULL,NULL,0)")
	manifest := captureTables(t, sourceDB, source, "keyless_cells")
	if err := Restore(context.Background(), target, manifest); err != nil {
		t.Fatal(err)
	}
	actual := captureTables(t, targetDB, target, "keyless_cells")
	if !reflect.DeepEqual(manifest, actual) || rowCount(t, target, "keyless_cells") != 3 {
		t.Fatal("keyless multiplicity/NULL/binary/unsigned max changed")
	}
	t.Log("RESTORE_KEYLESS duplicate_multiplicity=2 rows=3 unsigned_max_exact=true")
	saveEvidence(t, "keyless-unsigned-source.json", manifest)
	saveEvidence(t, "keyless-unsigned-actual-root.json", actual)
}

func TestPhysicalRestoreNonlocalRefusal(t *testing.T) {
	sourceDB, source, _, _ := restoreDB(t, false)
	_, target, _, _ := restoreDB(t, false)
	for _, conn := range []*sql.Conn{source, target} {
		execSQL(t, conn, "CREATE TABLE alias_cells (id INT PRIMARY KEY)")
	}
	execSQL(t, source, "INSERT INTO alias_cells VALUES (1)")
	manifest := captureTables(t, sourceDB, source, "alias_cells")
	execSQL(t, target, "CALL DOLT_ADD('.')")
	execSQL(t, target, "CALL DOLT_COMMIT('-Am','empty public alias target')")
	execSQL(t, target, "INSERT INTO dolt_nonlocal_tables (table_name,target_ref,ref_table,options) VALUES ('alias_cells','HEAD','alias_cells',NULL)")
	err := Restore(context.Background(), target, manifest)
	var failure *Error
	if !errors.As(err, &failure) || failure.Kind != "nonlocal-aliases" || failure.Table != "alias_cells" {
		t.Fatalf("destination alias must refuse: %v", err)
	}
	// Remove only this public fixture declaration, then inspect the local target.
	execSQL(t, target, "DELETE FROM dolt_nonlocal_tables WHERE table_name='alias_cells'")
	if rowCount(t, target, "alias_cells") != 0 {
		t.Fatal("nonlocal refusal wrote to destination")
	}
	t.Log("RESTORE_ALIAS typed_refusal=true local_target_rows=0")
}
