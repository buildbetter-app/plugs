package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/physicalcapture"
)

// Independent expected-value tagging of the original native driver's values.
// This oracle never invokes encodeCell, FromImage, FromWorkingImage, the row
// adapter, reconstruction, Undo, or VerifyUndo. Float widths and SQL NULL remain
// tags; JSON text is not parsed here. Expected files are frozen before writers.
func publicProofCell(t *testing.T, value sqldriver.Value) Cell {
	t.Helper()
	switch v := value.(type) {
	case nil:
		return Cell{Type: "null"}
	case string:
		return Cell{Type: "string", Data: v}
	case []byte:
		return Cell{Type: "bytes", Data: base64.StdEncoding.EncodeToString(v)}
	case bool:
		return Cell{Type: "bool", Data: strconv.FormatBool(v)}
	case time.Time:
		return Cell{Type: "time", Data: v.UTC().Format(time.RFC3339Nano)}
	}
	v := reflect.ValueOf(value)
	kind := v.Kind()
	tag := v.Type().String()
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Cell{Type: tag, Data: strconv.FormatInt(v.Int(), 10)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Cell{Type: tag, Data: strconv.FormatUint(v.Uint(), 10)}
	case reflect.Float32, reflect.Float64:
		number := v.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			t.Fatal("nonfinite native oracle value")
		}
		return Cell{Type: tag, Data: strconv.FormatFloat(number, 'g', -1, v.Type().Bits())}
	default:
		t.Fatalf("unsupported independent native oracle type %T", value)
		return Cell{}
	}
}
func publicProofManifest(t *testing.T, tables []physicalcapture.Table) Manifest {
	t.Helper()
	out := Manifest{Format: Format, HistoryMode: "snapshot", Tables: []Table{}}
	for _, raw := range tables {
		table := Table{Name: raw.Name, Columns: []Column{}, Rows: [][]Cell{}}
		for _, c := range raw.Columns {
			table.Columns = append(table.Columns, Column{Name: c.Name, SQLType: c.DatabaseTypeName})
		}
		for _, row := range raw.Rows {
			cells := []Cell{}
			for _, value := range row {
				cells = append(cells, publicProofCell(t, value))
			}
			table.Rows = append(table.Rows, cells)
		}
		out.Tables = append(out.Tables, table)
	}
	return out
}

// Definition C(M) from reviewed contract R2. No production byte helper is used
// for expected or actual native snapshots, nor any inner Cell.Data decoding.
func publicProofBytes(t *testing.T, manifest Manifest) []byte {
	t.Helper()
	tables := append([]Table{}, manifest.Tables...)
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	for i := range tables {
		encoded := make([]string, len(tables[i].Rows))
		for j, row := range tables[i].Rows {
			data, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			encoded[j] = string(data)
		}
		sort.Strings(encoded)
		tables[i].Rows = make([][]Cell, len(encoded))
		for j, data := range encoded {
			if err := json.Unmarshal([]byte(data), &tables[i].Rows[j]); err != nil {
				t.Fatal(err)
			}
		}
	}
	data, err := json.Marshal(Manifest{Format: manifest.Format, HistoryMode: manifest.HistoryMode, Tables: tables})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func publicProofProjection(manifest Manifest, reserved bool) Manifest {
	out := Manifest{Format: manifest.Format, HistoryMode: manifest.HistoryMode, Tables: []Table{}}
	for _, table := range manifest.Tables {
		if strings.HasPrefix(strings.ToLower(table.Name), "dolt_") == reserved {
			out.Tables = append(out.Tables, table)
		}
	}
	return out
}
func publicProofFile(t *testing.T, name string, data []byte) {
	t.Helper()
	directory := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if directory == "" {
		t.Fatal("Bazel output directory required")
	}
	if err := os.WriteFile(filepath.Join(directory, name), data, 0400); err != nil {
		t.Fatal(err)
	}
	t.Logf("NATIVE_UNDO_BYTE_FILE name=%s bytes=%d sha256=%x", name, len(data), sha256.Sum256(data))
}
func publicProofEqual(t *testing.T, name string, want, got []byte) {
	t.Helper()
	publicProofFile(t, name, got)
	if !bytes.Equal(want, got) {
		offset := 0
		for offset < len(want) && offset < len(got) && want[offset] == got[offset] {
			offset++
		}
		t.Fatalf("exact bytes differ in %s at offset %d; expected=%x actual=%x", name, offset, sha256.Sum256(want), sha256.Sum256(got))
	}
}
func publicProofImages(t *testing.T, conn *sql.Conn) (physicalcapture.Image, physicalcapture.WorkingImage) {
	t.Helper()
	ctx := context.Background()
	head := undoHead(t, conn)
	immutable, err := physicalcapture.ReadCommit(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	working, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	return immutable, working
}

// PK ordinals come from the actual native SQL catalog at the fixture's frozen
// schema. Missing PRIMARY rows mean authenticated keylessness only after full
// query completion, never a column-name heuristic. All fixture edits are DML.
func publicProofPKs(t *testing.T, conn *sql.Conn) map[string]map[string]uint64 {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), `SELECT TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND CONSTRAINT_NAME='PRIMARY' ORDER BY TABLE_NAME,ORDINAL_POSITION`)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]uint64{}
	for rows.Next() {
		var table, column string
		var ordinal uint64
		if err := rows.Scan(&table, &column, &ordinal); err != nil {
			t.Fatal(err)
		}
		if out[table] == nil {
			out[table] = map[string]uint64{}
		}
		if ordinal == 0 {
			t.Fatal("invalid native SQL PRIMARY ordinal")
		}
		out[table][column] = ordinal - 1
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}
	return out
}
func publicProofNormalized(t *testing.T, table physicalcapture.Table) dumpformat.NativeTable {
	t.Helper()
	out := dumpformat.NativeTable{Columns: []string{}, Rows: []dumpformat.Row{}}
	for _, col := range table.Columns {
		out.Columns = append(out.Columns, col.Name)
	}
	sort.Strings(out.Columns)
	for _, cells := range table.Rows {
		fields := dumpformat.Row{}
		for ci, value := range cells {
			switch v := value.(type) {
			case []byte:
				if strings.Contains(strings.ToUpper(table.Columns[ci].DatabaseTypeName), "BLOB") || strings.Contains(strings.ToUpper(table.Columns[ci].DatabaseTypeName), "BINARY") {
					value = map[string]any{"$bytes": base64.StdEncoding.EncodeToString(v)}
				} else {
					value = string(v)
				}
			case time.Time:
				value = v.UTC().Format(time.RFC3339Nano)
			}
			if text, ok := value.(string); ok && strings.EqualFold(table.Columns[ci].DatabaseTypeName, "JSON") {
				d := json.NewDecoder(strings.NewReader(text))
				d.UseNumber()
				if err := d.Decode(&value); err != nil {
					t.Fatal(err)
				}
			}
			fields[table.Columns[ci].Name] = value
		}
		// Convert Go scalars to lossless json.Number, as the old public exporter.
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		var owned dumpformat.Row
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber()
		if err := d.Decode(&owned); err != nil {
			t.Fatal(err)
		}
		out.Rows = append(out.Rows, owned)
	}
	sort.Slice(out.Rows, func(i, j int) bool {
		a, _ := json.Marshal(out.Rows[i])
		b, _ := json.Marshal(out.Rows[j])
		return bytes.Compare(a, b) < 0
	})
	return out
}
func publicProofD3Cell(t *testing.T, value sqldriver.Value) dumpformat.NativeDeltaCell {
	cell := publicProofCell(t, value)
	out := dumpformat.NativeDeltaCell{Kind: cell.Type, Value: cell.Data}
	switch v := value.(type) {
	case nil:
		out.Kind = "sql-null"
	case float32:
		out.Kind = "float32-bits"
		out.Value = strconv.FormatUint(uint64(math.Float32bits(v)), 16)
	case float64:
		out.Kind = "float64-bits"
		out.Value = strconv.FormatUint(math.Float64bits(v), 16)
	}
	return out
}
func publicProofGroup(t *testing.T, table physicalcapture.Table, history, kind string, pks map[string]map[string]uint64) (NativeTableBinding, []driver.Record, []byte) {
	t.Helper()
	normalized := publicProofNormalized(t, table)
	expected := nativeOldEnvelope(t, nativeProofIdentity, table.Name, history, normalized)
	var records []driver.Record
	var err error
	physical := ""
	if kind == dumpformat.NativeRowV1ContentBag {
		records, err = dumpformat.NativeV1TableRecords(nativeProofIdentity, table.Name, history, normalized)
	} else {
		schema := dumpformat.NativeDeltaTableSchema{Name: table.Name, Columns: []dumpformat.NativeDeltaColumn{}}
		for _, c := range table.Columns {
			col := dumpformat.NativeDeltaColumn{Name: c.Name, SQLType: c.DatabaseTypeName}
			if ordinal, ok := pks[table.Name][c.Name]; ok {
				o := ordinal
				col.PrimaryKeyOrdinal = &o
			}
			schema.Columns = append(schema.Columns, col)
		}
		rows := []dumpformat.NativeDeltaRow{}
		occurrences := map[string]uint64{}
		for _, raw := range table.Rows {
			single := table
			single.Rows = [][]sqldriver.Value{raw}
			fields := publicProofNormalized(t, single).Rows[0]
			n, e := json.Marshal(fields)
			if e != nil {
				t.Fatal(e)
			}
			n = append(n, '\n')
			cells := make([]dumpformat.NativeDeltaCell, len(raw))
			for i, v := range raw {
				cells[i] = publicProofD3Cell(t, v)
			}
			key, e := dumpformat.NativeDeltaPrimaryKey(schema, cells)
			if e != nil {
				t.Fatal(e)
			}
			if key == nil {
				payload := dumpformat.NativeDeltaRowPayload(cells, n)
				occ := occurrences[string(payload)]
				key = dumpformat.NativeDeltaBagKey(payload, occ)
				occurrences[string(payload)] = occ + 1
			}
			rows = append(rows, dumpformat.NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(n)})
		}
		sort.Slice(rows, func(i, j int) bool {
			a, _ := base64.StdEncoding.DecodeString(rows[i].Key)
			b, _ := base64.StdEncoding.DecodeString(rows[j].Key)
			return bytes.Compare(a, b) < 0
		})
		physical, err = dumpformat.NativeDeltaTableSHA256(schema, rows)
		if err != nil {
			t.Fatal(err)
		}
		records, err = dumpformat.NativeV2TableRecords(nativeProofIdentity, schema, history, rows, nil)
	}
	if err != nil {
		t.Fatalf("source row encoding table=%s history=%s: %v", table.Name, history, err)
	}
	return nativeBinding(t, nativeProofIdentity, table.Name, history, kind, normalized, records, physical), records, expected
}
func publicProofReadback(t *testing.T, engine, kind string, immutable physicalcapture.Image, working physicalcapture.WorkingImage, pks map[string]map[string]uint64, prefix string) NativeRowReadback {
	t.Helper()
	census := []NativeTableBinding{}
	records := []driver.Record{}
	expected := [][]byte{}
	add := func(table physicalcapture.Table, history string) {
		b, r, e := publicProofGroup(t, table, history, kind, pks)
		census = append(census, b)
		records = append(records, r...)
		expected = append(expected, e)
	}
	versioned := map[string]bool{}
	for _, table := range immutable.Tables {
		versioned[table.Name] = true
		add(table, "retained-native-commit")
	}
	for _, table := range working.Tables {
		history := dumpformat.FamilyUnversioned
		if versioned[table.Name] {
			history = dumpformat.FamilyVersioned
		}
		if strings.HasPrefix(strings.ToLower(table.Name), "dolt_") {
			history = dumpformat.NativeRowOpaqueCurrentHistory
		}
		add(table, history)
	}
	// Expected old-table source envelopes are frozen before reconstruction.
	for i, body := range expected {
		publicProofFile(t, fmt.Sprintf("%s-old-table-%03d.json", prefix, i), body)
	}
	r := nativeStoreReadback(t, engine, records, census)
	actual := r.TableBodies()
	for i, body := range actual {
		if !bytes.Equal(body, expected[i]) {
			t.Fatalf("source table envelope %d differs", i)
		}
	}
	saveEvidence(t, prefix+"-source-census.json", census)
	t.Logf("NATIVE_UNDO_SOURCE_TABLE_BYTES engine=%s identity=%s immutable=%d current=%d records=%d all_exact=true", engine, kind, len(immutable.Tables), len(working.Tables), len(records))
	return r
}

func publicProofSource(t *testing.T) (string, SourceBinding, physicalcapture.Image, physicalcapture.WorkingImage, []physicalcapture.Image) {
	t.Helper()
	_, conn, directory, release := restoreDB(t, true)
	for _, ddl := range []string{
		"CREATE TABLE public_cells (id BIGINT PRIMARY KEY, sql_null JSON, json_null JSON, opaque BLOB, unsigned_value BIGINT UNSIGNED, narrow_value TINYINT, f32 FLOAT, f64 DOUBLE)",
		"CREATE TABLE public_keyless (value TEXT)",
		"CREATE TABLE public_empty (id INT PRIMARY KEY)",
		"CREATE TABLE public_children (id INT PRIMARY KEY, parent_id BIGINT, CONSTRAINT public_fk FOREIGN KEY(parent_id) REFERENCES public_cells(id))",
	} {
		execSQL(t, conn, ddl)
	}
	execSQL(t, conn, `INSERT INTO issues(id,title,description,design,acceptance_criteria,notes,agent_state,hook_bead,role_bead,role_type,timeout_ns,metadata,payload,waiters) VALUES ('public-row-1','original issue','','','','','working','public-row-2','public-row-1','agent',9007199254740993,JSON_OBJECT('native',9007199254740993),'{"opaque":null}','["public-row-2",null]')`)
	execSQL(t, conn, `INSERT INTO wisps(id,title,agent_state,timeout_ns,payload) VALUES ('public-wisp','original ignored wisp','working',9007199254740993,'{"null":null}')`)
	execSQL(t, conn, "INSERT INTO public_cells VALUES(1,NULL,CAST('null' AS JSON),?,18446744073709551615,-12,1.5,2.25)", []byte{0, 255, 7})
	execSQL(t, conn, "INSERT INTO public_children VALUES(1,1)")
	execSQL(t, conn, "INSERT INTO public_keyless VALUES('old'),('old'),(NULL)")
	execSQL(t, conn, "INSERT INTO public_empty VALUES(1)")
	execSQL(t, conn, "CALL DOLT_ADD('.')")
	execSQL(t, conn, "CALL DOLT_COMMIT('-Am','public native byte source first')")
	first := undoHead(t, conn)
	execSQL(t, conn, `INSERT INTO issues(id,title,description,design,acceptance_criteria,notes) VALUES ('public-row-2','second original issue','','','','')`)
	execSQL(t, conn, "CALL DOLT_COMMIT('-Am','public native byte source second')")
	execSQL(t, conn, "CALL DOLT_BRANCH('public-retained-branch')")
	execSQL(t, conn, "CALL DOLT_TAG('public-retained-tag')")
	immutable, working := publicProofImages(t, conn)
	binding, err := BindWorkingSource(immutable, working)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := physicalcapture.ReadCommit(context.Background(), conn, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	return directory, binding, immutable, working, []physicalcapture.Image{prior, immutable}
}

func TestNativeRowUndoIndependentPublicNativeBytes(t *testing.T) {
	for _, mode := range []string{"noedit", "edited"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			prefix := "row-undo-" + mode
			source, binding, sourceImmutable, sourceWorking, selected := publicProofSource(t)
			originalFiles, err := InventoryPreservedStore(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			work := undoWork(t)
			if err := os.Chmod(work, 0700); err != nil {
				t.Fatal(err)
			}
			targets := map[string]string{}
			copies := map[string]CopyReceipt{}
			for _, name := range []string{"expected", "old", "row"} {
				target := filepath.Join(work, name)
				receipt, err := CopyPreservedStore(ctx, source, target)
				if err != nil {
					t.Fatal(err)
				}
				if receipt.SHA256 != originalFiles.SHA256 {
					t.Fatal("preserved copies are not byte-identical")
				}
				targets[name] = target
				copies[name] = receipt
			}
			publicProofFile(t, prefix+"-source-immutable.json", publicProofBytes(t, publicProofManifest(t, sourceImmutable.Tables)))
			publicProofFile(t, prefix+"-source-working.json", publicProofBytes(t, publicProofManifest(t, sourceWorking.Tables)))
			saveEvidence(t, prefix+"-source-binding.json", binding)
			// Independently authored native DML recipe. This is ordinary SQL on the
			// EXPECTED preserved copy, before either old or row adapter/Undo executes.
			expectedConn, closeExpected := undoOpen(t, targets["expected"])
			recipe := []string{}
			if mode == "edited" {
				recipe = []string{
					"UPDATE issues SET title='independent expected issue' WHERE id='public-row-1'",
					"UPDATE wisps SET title='independent expected ignored wisp' WHERE id='public-wisp'",
					"DELETE FROM public_children",
					"DELETE FROM public_cells",
					"INSERT INTO public_cells VALUES(11,NULL,CAST('null' AS JSON),X'00FF08',18446744073709551615,-13,1.5,2.25)",
					"INSERT INTO public_children VALUES(2,11)",
					"DELETE FROM public_empty",
					"DELETE FROM public_keyless",
					"INSERT INTO public_keyless VALUES('current'),('current'),(NULL),('')",
				}
			}
			for _, query := range recipe {
				execSQL(t, expectedConn, query)
			}
			execSQL(t, expectedConn, "CALL DOLT_ADD('.')")
			execSQL(t, expectedConn, "CALL DOLT_COMMIT('--allow-empty','-Am','independent public expected state')")
			expectedImmutable, expectedWorking := publicProofImages(t, expectedConn)
			pks := publicProofPKs(t, expectedConn)
			expectedComplete := publicProofManifest(t, expectedWorking.Tables)
			expectedUser := publicProofProjection(expectedComplete, false)
			expectedCurrentBytes := publicProofBytes(t, expectedUser)
			expectedImmutableBytes := publicProofBytes(t, publicProofManifest(t, expectedImmutable.Tables))
			expectedWorkingBytes := publicProofBytes(t, expectedComplete)
			expectedReserved := publicProofBytes(t, publicProofProjection(expectedComplete, true))
			if len(publicProofProjection(expectedComplete, true).Tables) == 0 {
				t.Fatal("native reserved/view schema evidence absent")
			}
			hasViews := false
			for _, table := range expectedComplete.Tables {
				if table.Name == "dolt_schemas" && len(table.Rows) > 0 {
					hasViews = true
				}
			}
			if !hasViews {
				t.Fatal("genuine native public fixture must retain stored view definitions")
			}
			// Files are complete canonical bytes, not expected hashes/counts. No
			// production writer/conversion helper participated in their derivation.
			publicProofFile(t, prefix+"-EXPECTED-current-users.json", expectedCurrentBytes)
			publicProofFile(t, prefix+"-EXPECTED-reopened-immutable.json", expectedImmutableBytes)
			publicProofFile(t, prefix+"-EXPECTED-reopened-working.json", expectedWorkingBytes)
			publicProofFile(t, prefix+"-EXPECTED-reserved-views.json", expectedReserved)
			saveEvidence(t, prefix+"-expected-native-images.json", map[string]any{"immutable": expectedImmutable, "working": expectedWorking, "primary_key_ordinals_zero_based": pks})
			saveEvidence(t, prefix+"-native-expected-recipe.json", map[string]any{"mode": mode, "ordinary_native_sql": recipe, "commit": "CALL DOLT_COMMIT('--allow-empty','-Am','independent public expected state')", "derived_before_adapters_and_writers": true, "raw_container_bytes_expected_equal": false, "byte_domain": "contractR2 C(M), exact tagged cells, SQL types, duplicate-preserving sorted rows"})
			if err := closeExpected(); err != nil {
				t.Fatal(err)
			}
			expectedInventory, err := InventoryPreservedStore(ctx, targets["expected"])
			if err != nil {
				t.Fatal(err)
			}
			saveEvidence(t, prefix+"-expected-closed-inventory.json", expectedInventory)
			t.Log("NATIVE_UNDO_EXPECTED_FROZEN before_old_adapter=true before_row_adapter=true before_either_writer=true independently_native_authored=true")

			// Both v1 and v2 must reconstruct every selected genuine source commit and
			// the complete current plane. V1 needs no typed witness for this byte proof.
			for i, image := range selected {
				_ = publicProofReadback(t, "memory", dumpformat.NativeRowV1ContentBag, image, expectedWorking, pks, fmt.Sprintf("%s-v1-commit%d", prefix, i))
				if i < len(selected)-1 {
					_ = publicProofReadback(t, "sqlite", dumpformat.NativeRowD3Physical, image, expectedWorking, pks, fmt.Sprintf("%s-v2-commit%d", prefix, i))
				}
			}
			v1 := publicProofReadback(t, "memory", dumpformat.NativeRowV1ContentBag, sourceImmutable, expectedWorking, pks, prefix+"-v1-write-refusal")
			report := []byte(" {\n  \"lost_post_switch_history\": [\"public fixture; actual Plug loss census not supplied\"],\n  \"core_kind_losses\": [], \"namespace_losses\": [], \"warnings\": []\n}\n")
			if _, err := PrepareNativeRowUndo(binding, nativeProofIdentity, v1, report); err == nil {
				t.Fatal("untyped v1 was promoted to physical permission")
			}
			rows := publicProofReadback(t, "sqlite", dumpformat.NativeRowD3Physical, sourceImmutable, expectedWorking, pks, prefix+"-v2-current")
			rowRequest, err := PrepareNativeRowUndo(binding, nativeProofIdentity, rows, report)
			if err != nil {
				t.Fatal(err)
			}
			oldCurrent, err := FromWorkingImage(expectedWorking)
			if err != nil {
				t.Fatal(err)
			}
			oldRequest := UndoRequest{Format: UndoFormat, HistoryMode: "switch-back", Source: binding, Current: publicProofProjection(oldCurrent, false), ReverseReport: bytes.Clone(report), ReverseReportBytes: bytes.Clone(report)}
			for name, request := range map[string]UndoRequest{"old": oldRequest, "row": rowRequest} {
				actual, err := CanonicalPhysicalBytes(request.Current)
				if err != nil {
					t.Fatal(err)
				}
				publicProofEqual(t, prefix+"-"+name+"-current-request.json", expectedCurrentBytes, actual)
				saveEvidence(t, prefix+"-"+name+"-request.json", request)
			}
			publicProofPrepareRefusals(t, binding, rows, report)
			saveEvidence(t, prefix+"-writer-start.json", map[string]any{"expected_files_already_frozen": true, "expected_native_store_closed": true, "expected_inventory": expectedInventory.SHA256, "original_copies": copies, "selected_source_commits": []string{selected[0].CommitID, selected[1].CommitID}})
			// Real consumer: the new row/meta readback has replaced Current and now
			// reaches the unchanged normal Undo storage transaction, not a codec-only
			// oracle or a snapshot-table substitute. Each path has its own source copy.
			for _, path := range []struct {
				name    string
				request UndoRequest
			}{{"old", oldRequest}, {"row", rowRequest}} {
				conn, closeCopy := undoOpen(t, targets[path.name])
				receipt, err := Undo(ctx, conn, path.request)
				if err != nil {
					t.Fatal(err)
				}
				if receipt.CurrentCommits != receipt.OriginalCommits+1 || receipt.DeletedRows == 0 || receipt.WrittenRows == 0 || !receipt.PhysicalReadbackVerified {
					t.Fatalf("missing actual nonempty one-switch-back native transaction: %+v", receipt)
				}
				if !bytes.Equal(receipt.ReverseReport, report) {
					t.Fatal("original reverse report bytes changed")
				}
				if err := closeCopy(); err != nil {
					t.Fatal(err)
				}
				reopened, closeAgain := undoOpen(t, targets[path.name])
				if _, err := VerifyUndo(ctx, reopened, path.request, receipt); err != nil {
					t.Fatal(err)
				}
				receipt.Reopened = true
				actualImmutable, actualWorking := publicProofImages(t, reopened)
				publicProofEqual(t, prefix+"-"+path.name+"-actual-reopened-immutable.json", expectedImmutableBytes, publicProofBytes(t, publicProofManifest(t, actualImmutable.Tables)))
				publicProofEqual(t, prefix+"-"+path.name+"-actual-reopened-working.json", expectedWorkingBytes, publicProofBytes(t, publicProofManifest(t, actualWorking.Tables)))
				publicProofEqual(t, prefix+"-"+path.name+"-actual-reserved-views.json", expectedReserved, publicProofBytes(t, publicProofProjection(publicProofManifest(t, actualWorking.Tables), true)))
				for i, original := range selected {
					observed, err := physicalcapture.ReadCommit(ctx, reopened, original.CommitID)
					if err != nil {
						t.Fatal(err)
					}
					publicProofEqual(t, fmt.Sprintf("%s-%s-preserved-commit%d.json", prefix, path.name, i), publicProofBytes(t, publicProofManifest(t, original.Tables)), publicProofBytes(t, publicProofManifest(t, observed.Tables)))
				}
				publicProofFile(t, prefix+"-"+path.name+"-actual-loss-report.json", receipt.ReverseReport)
				saveEvidence(t, prefix+"-"+path.name+"-native-reopened-images.json", map[string]any{"immutable": actualImmutable, "working": actualWorking})
				saveEvidence(t, prefix+"-"+path.name+"-receipt.json", receipt)
				if err := closeAgain(); err != nil {
					t.Fatal(err)
				}
				t.Logf("NATIVE_UNDO_INDEPENDENT_BYTE_PROOF mode=%s path=%s original=%s switchback=%s immutable_tables=%d working_tables=%d reserved_views_exact=true original_commits=%d new_commits=1 deleted=%d inserted=%d real_reopen=true", mode, path.name, receipt.OriginalCommit, receipt.SwitchBackCommit, len(actualImmutable.Tables), len(actualWorking.Tables), receipt.OriginalCommits, receipt.DeletedRows, receipt.WrittenRows)
			}
			unchanged, err := InventoryPreservedStore(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if unchanged.SHA256 != originalFiles.SHA256 {
				t.Fatal("closed original source bytes changed")
			}
			expectedUnchanged, err := InventoryPreservedStore(ctx, targets["expected"])
			if err != nil {
				t.Fatal(err)
			}
			if expectedUnchanged.SHA256 != expectedInventory.SHA256 {
				t.Fatal("independent expected copy changed after freeze")
			}
			saveEvidence(t, prefix+"-source-inventory-before-after.json", map[string]any{"before": originalFiles, "after": unchanged})
			if status, err := os.ReadFile("/proc/self/status"); err == nil {
				lines := []string{}
				for _, line := range strings.Split(string(status), "\n") {
					if strings.HasPrefix(line, "VmRSS:") || strings.HasPrefix(line, "VmHWM:") {
						lines = append(lines, line)
					}
				}
				saveEvidence(t, prefix+"-process-memory-observation.json", map[string]any{"proc_self": lines, "logical_decoded_budget_proven": false, "pure_reconstruction_accounting": "NOT_SUPPLIED; shared codec allocations unaccounted; RSS is observational only", "decoded_limit": 2147483648, "qualification": "small disposable public fixture only"})
			} else {
				t.Fatal(err)
			}
		})
	}
}

func publicProofPrepareRefusals(t *testing.T, source SourceBinding, readback NativeRowReadback, report []byte) {
	t.Helper()
	clone := func() NativeRowReadback {
		r := NativeRowReadback{tables: append([]nativeRebuiltTable{}, readback.tables...)}
		for i := range r.tables {
			r.tables[i].meta.PhysicalColumns = append([]dumpformat.NativeDeltaColumn{}, r.tables[i].meta.PhysicalColumns...)
			r.tables[i].rows = append([]*dumpformat.NativeDecodedRow{}, r.tables[i].rows...)
		}
		return r
	}
	// Defense at the adapter boundary even if an internal future caller assembles
	// an invalid readback. Public callers cannot mutate its private state.
	tests := []struct {
		name   string
		change func(*NativeRowReadback)
	}{
		{"missing_current_table", func(r *NativeRowReadback) {
			for i, g := range r.tables {
				if g.meta.Table == "public_empty" && g.meta.History != "retained-native-commit" {
					r.tables = append(r.tables[:i:i], r.tables[i+1:]...)
					return
				}
			}
		}},
		{"desired_schema_change", func(r *NativeRowReadback) {
			for i, g := range r.tables {
				if g.meta.Table == "public_cells" && g.meta.History != "retained-native-commit" {
					r.tables[i].meta.PhysicalColumns[0].SQLType = "TEXT"
					return
				}
			}
		}},
		{"changed_view_projection", func(r *NativeRowReadback) {
			for i, g := range r.tables {
				if g.meta.Table == "dolt_schemas" && g.meta.History != "retained-native-commit" {
					row := *g.rows[0]
					row.Cells = append([]dumpformat.NativeDeltaCell{}, row.Cells...)
					row.Cells[0].Value = "changed-view-intent"
					r.tables[i].rows[0] = &row
					return
				}
			}
		}},
		{"unavailable_secret", func(r *NativeRowReadback) {
			for i, g := range r.tables {
				if g.meta.Table == "public_cells" && g.meta.History != "retained-native-commit" {
					row := *g.rows[0]
					row.Cells = append([]dumpformat.NativeDeltaCell{}, row.Cells...)
					row.Cells[0] = dumpformat.NativeDeltaCell{Kind: "secret-reference", Value: "unavailable"}
					r.tables[i].rows[0] = &row
					return
				}
			}
		}},
		{"foreign_current_source", func(r *NativeRowReadback) { r.tables[0].binding.Source.Project = "other" }},
		{"wrong_current_history_family", func(r *NativeRowReadback) {
			for i, g := range r.tables {
				if g.meta.Table == "wisps" && g.meta.History != "retained-native-commit" {
					r.tables[i].meta.History = dumpformat.FamilyVersioned
					return
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := clone()
			test.change(&r)
			_, err := PrepareNativeRowUndo(source, nativeProofIdentity, r, report)
			var failure *Error
			if !errors.As(err, &failure) {
				t.Fatalf("typed pre-SQL refusal missing: %v", err)
			}
			if test.name == "unavailable_secret" && (failure.Table != "public_cells" || failure.Column != "id" || failure.Row != 0) {
				t.Fatalf("lost native field/row refusal: %+v", failure)
			}
		})
	}
}
