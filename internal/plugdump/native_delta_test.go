package plugdump

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

func deltaOrdinal(n uint64) *uint64 { return &n }

// Constants were computed separately from the Go writer with the published
// binary preimages. They bind binary-vs-text framing, physical PK order,
// exact large integer/NULL/BLOB values and schema/table/commit hash domains.
func TestNativeDeltaCanonicalBinaryVectors(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{
		{Name: "a", SQLType: "bigint", PrimaryKeyOrdinal: deltaOrdinal(1)},
		{Name: "b", SQLType: "varbinary(8)", PrimaryKeyOrdinal: deltaOrdinal(0)},
		{Name: "c", SQLType: "text"},
	}}
	cells := []NativeDeltaCell{{Kind: "int64", Value: "9007199254740993"}, {Kind: "bytes", Value: "AP8="}, {Kind: "sql-null"}}
	normal := []byte("{\"a\":9007199254740993,\"b\":{\"$bytes\":\"AP8=\"},\"c\":null}\n")
	if got := NativeDeltaTableSchemaID(table); got != "aff042c96377d88c5c1ca6b55fd442439fc11b21a4aac0fe7adf576cc981d84a" {
		t.Fatal(got)
	}
	schema := NativeDeltaSchema{Profile: NativeCommitSchemaProfile{Format: "beads-native-commit-schema/1", Scope: "full", MainVersion: deltaOrdinal(66), Absences: []NativeCommitSchemaAbsence{}}, Tables: []NativeDeltaTableSchema{table}}
	if got := NativeDeltaSchemaID(schema); got != "a2ed3867daddc72b4cdfa5854d55388292bf3ce5245a475327dd466a7b627d28" {
		t.Fatal(got)
	}
	key, err := NativeDeltaPrimaryKey(table, cells)
	if err != nil || hex.EncodeToString(key) != "706b0000000000000000020000000000000005627974657300000000000000044150383d0000000000000005696e743634000000000000001039303037313939323534373430393933" {
		t.Fatalf("stored PK order lost: %x %v", key, err)
	}
	payload := NativeDeltaRowPayload(cells, normal)
	if got := fmt.Sprintf("%x", sha256.Sum256(payload)); got != "f5414f4fe493371aa2d4ca49d0aa5d625a3e22f929b46afb2ff04baa8c34c7bc" {
		t.Fatal(got)
	}
	if got := base64.StdEncoding.EncodeToString(NativeDeltaBagKey(payload, 2)); got != "YmFnAPVBT0/kkzcaotTKSdCqXWJaPiL5KbRq+y/wS6qMNMe8AAAAAAAAAAI=" {
		t.Fatal(got)
	}
	row := NativeDeltaRow{Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(normal)}
	hash, err := NativeDeltaTableSHA256(table, []NativeDeltaRow{row})
	if err != nil || hash != "22b30a660580a278d7f336e100f6c2e3f5eb3b8feb286df59f72e618e9c90bcb" {
		t.Fatalf("%s %v", hash, err)
	}
	root, err := NativeDeltaRootSHA256(map[string]NativeDeltaTableDigest{"items": {Rows: 1, SHA256: hash}})
	if err != nil || root != "a7d93c8eeb5c71cb93f20c3bd41f08e82ac8381c8f0c6c48284ca2fffe5a71c8" {
		t.Fatalf("%s %v", root, err)
	}
}

// Authenticate every file and reconstruct the concatenated test stream.
// The small payload size ensures a JSON row crosses several physical parts.
func deltaTestStream(t *testing.T, parts *nativeDeltaParts) []byte {
	t.Helper()
	count, last, streamHash, size, err := parts.finish()
	if err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	var previous *string
	for i := uint64(1); i <= count; i++ {
		data, err := os.ReadFile(filepath.Join(parts.directory, fmt.Sprintf("%020d.part", i)))
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(bytes.NewReader(data))
		line, err := r.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var header NativeDeltaPartHeader
		if err := json.Unmarshal(line, &header); err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if header.Format != NativeDeltaPartFormat || header.Sequence != i || !reflect.DeepEqual(header.PreviousPartSHA256, previous) || header.PayloadBytes != uint64(len(payload)) || header.PayloadSHA256 != digest(payload) || header.PayloadBytes == 0 || header.PayloadBytes > parts.limit || i < count && header.PayloadBytes != parts.limit {
			t.Fatalf("part %d does not authenticate exact bytes", i)
		}
		hash := digest(data)
		previous = &hash
		stream.Write(payload)
	}
	if previous == nil || *previous != last || streamHash != digest(stream.Bytes()) || size != uint64(stream.Len()) {
		t.Fatal("final part/stream commitment differs")
	}
	return stream.Bytes()
}

func TestNativeDeltaPartsCrossRecordBoundary(t *testing.T) {
	directory := t.TempDir()
	parts, err := newNativeDeltaParts(filepath.Join(directory, "parts"), directory, 37)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parts.Close() })
	records := []any{NativeDeltaHeader{Type: "header", Projects: []string{}, ConfigPolicy: map[string]string{}}, NativeDeltaRow{Type: "row", Op: "insert", Cells: []NativeDeltaCell{{Kind: "string", Value: "public<>&\n\u2028é"}}, NormalizedJSON: "e30K"}}
	var expected bytes.Buffer
	for _, record := range records {
		if err := parts.record(record); err != nil {
			t.Fatal(err)
		}
		data, err := marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		expected.Write(data)
		measured, err := nativeDeltaJSONSize(record, uint64(len(data)-1))
		if err != nil || measured != uint64(len(data)-1) {
			t.Fatalf("exact-fit record refused or miscounted: %d %v", measured, err)
		}
		if _, err := nativeDeltaJSONSize(record, measured-1); err == nil {
			t.Fatal("record did not refuse one byte below its encoded size")
		}
	}
	if actual := deltaTestStream(t, parts); !bytes.Equal(actual, expected.Bytes()) {
		t.Fatal("cross-part stream changed a record")
	}
}

func deltaTestState(t *testing.T, tables []physicalcapture.Table) *nativeDeltaState {
	t.Helper()
	state, err := nativeDeltaPhysicalStateWithProfile(tables, NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: "fixture", Absences: []NativeCommitSchemaAbsence{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestNativeDeltaEditsSchemaAndKeylessMultiplicity(t *testing.T) {
	columns := []physicalcapture.Column{{Name: "id", DatabaseTypeName: "bigint", PrimaryKeyOrdinal: deltaOrdinal(0)}, {Name: "payload", DatabaseTypeName: "longblob"}}
	before := deltaTestState(t, []physicalcapture.Table{{Name: "keyed", Columns: columns, Rows: [][]driver.Value{{int64(1), []byte{0, 255}}, {int64(2), nil}}}, {Name: "bag", Columns: []physicalcapture.Column{{Name: "value", DatabaseTypeName: "text"}}, Rows: [][]driver.Value{{"duplicate"}, {"duplicate"}, {nil}}}})
	after := deltaTestState(t, []physicalcapture.Table{{Name: "keyed", Columns: columns, Rows: [][]driver.Value{{int64(1), []byte{}}, {int64(3), []byte{255, 0}}}}, {Name: "bag", Columns: []physicalcapture.Column{{Name: "value", DatabaseTypeName: "text"}}, Rows: [][]driver.Value{{nil}, {"duplicate"}}}})
	if before.Root == after.Root || before.Schema.SchemaID != after.Schema.SchemaID {
		t.Fatal("data changes did not retain schema/change root")
	}
	directory := t.TempDir()
	parts, err := newNativeDeltaParts(filepath.Join(directory, "parts"), directory, 113)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parts.Close() })
	e := nativeDeltaEmitter{parts: parts, schemas: map[string]bool{}}
	if err := e.tables(after, before); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(deltaTestStream(t, parts)))
	states := map[string]map[string]NativeDeltaRow{}
	for table, rows := range before.Tables {
		states[table] = map[string]NativeDeltaRow{}
		for _, row := range rows {
			states[table][row.Key] = row
		}
	}
	var table string
	ops := map[string]int{}
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		var kind struct{ Type, Op string }
		if err := json.Unmarshal(raw, &kind); err != nil {
			t.Fatal(err)
		}
		switch kind.Type {
		case "table":
			var open NativeDeltaTable
			if err := json.Unmarshal(raw, &open); err != nil {
				t.Fatal(err)
			}
			if open.Mode != "delta" {
				t.Fatal("same-schema table did not use delta")
			}
			table = open.Name
		case "row":
			var row NativeDeltaRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			_, exists := states[table][row.Key]
			if kind.Op == "insert" && exists || kind.Op != "insert" && !exists {
				t.Fatal("row operation precondition differs")
			}
			ops[kind.Op]++
			if kind.Op == "delete" {
				delete(states[table], row.Key)
			} else {
				row.Op = "insert"
				states[table][row.Key] = row
			}
		}
	}
	if !reflect.DeepEqual(ops, map[string]int{"insert": 1, "update": 1, "delete": 2}) {
		t.Fatalf("wrong row changes: %v", ops)
	}
	for table, rows := range after.Tables {
		if len(states[table]) != len(rows) {
			t.Fatal("multiplicity differs")
		}
		for _, row := range rows {
			if !reflect.DeepEqual(states[table][row.Key], row) {
				t.Fatalf("row reconstruction differs in %s", table)
			}
		}
	}
	changedColumns := append(append([]physicalcapture.Column{}, columns...), physicalcapture.Column{Name: "extra", DatabaseTypeName: "text"})
	changed := deltaTestState(t, []physicalcapture.Table{{Name: "keyed", Columns: changedColumns, Rows: [][]driver.Value{{int64(1), []byte{}, "new schema"}}}})
	if changed.Schema.SchemaID == after.Schema.SchemaID {
		t.Fatal("physical schema change reused old ID")
	}
	directory2 := t.TempDir()
	parts2, err := newNativeDeltaParts(filepath.Join(directory2, "parts"), directory2, 173)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parts2.Close() })
	e2 := nativeDeltaEmitter{parts: parts2, schemas: map[string]bool{}}
	if err := e2.tables(changed, after); err != nil {
		t.Fatal(err)
	}
	data := deltaTestStream(t, parts2)
	if !bytes.Contains(data, []byte(`"type":"drop_table","name":"bag"`)) || !bytes.Contains(data, []byte(`"mode":"replace"`)) || bytes.Contains(data, []byte(`"op":"update"`)) {
		t.Fatal("dropped table or changed-schema replace was lost")
	}
}

func TestNativeDeltaSecretReferencesAndExactJSONSizing(t *testing.T) {
	profile := NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: "fixture", Absences: []NativeCommitSchemaAbsence{{Family: "absent", Reason: "not-created"}}}
	tables := []physicalcapture.Table{{Name: "config", Columns: []physicalcapture.Column{{Name: "key", DatabaseTypeName: "varchar(255)", PrimaryKeyOrdinal: deltaOrdinal(0)}, {Name: "value", DatabaseTypeName: "text"}}, Rows: [][]driver.Value{{"ordinary_setting", "public-synthetic-secret-value"}}}}
	state, err := nativeDeltaPhysicalStateWithProfile(tables, profile, map[string]string{"config/ordinary_setting": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	row := state.Tables["config"][0]
	if row.Cells[1].Kind != "secret-reference" || row.Cells[1].Value != "config/ordinary_setting" {
		t.Fatal("authenticated policy did not select reference")
	}
	data, err := marshal(state)
	if err != nil || bytes.Contains(data, []byte("public-synthetic-secret-value")) {
		t.Fatal("raw secret reached staged state")
	}
	for _, value := range []any{state, profile, map[string]any{"null": nil, "number": json.Number("9007199254740993"), "float": -0.25, "binary": []byte{0, 255}, "escaped": "\x00\xff<>&\u2028\u2029\t\n"}} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		measured, err := nativeDeltaJSONSize(value, uint64(len(data)))
		if err != nil || measured != uint64(len(data)) {
			t.Fatalf("JSON sizing differs: %d %d %v", measured, len(data), err)
		}
	}
	if _, err := nativeDeltaJSONSize(strings.Repeat("public", 1024), 32); err == nil {
		t.Fatal("over-budget logical value admitted")
	}
	inner := &NativeCommitFailure{OrdinalFromHead: 17, SchemaVersion: deltaOrdinal(39), Cause: ErrSourceHistoryUnavailable{Reason: "table-size-limit"}}
	wrapped := historyFailureCoordinates(inner, 1, deltaOrdinal(66))
	var failure *NativeCommitFailure
	if !errors.As(wrapped, &failure) || failure.OrdinalFromHead != 17 || failure.SchemaVersion == nil || *failure.SchemaVersion != 39 {
		t.Fatal("specific older-commit coordinate replaced by caller context")
	}
	inner = &NativeCommitFailure{OrdinalFromHead: 17, Cause: ErrSourceHistoryUnavailable{Reason: "historical-schema-unavailable"}}
	wrapped = historyFailureCoordinates(inner, 1, deltaOrdinal(66))
	if !errors.As(wrapped, &failure) || failure.OrdinalFromHead != 17 || failure.SchemaVersion != nil {
		t.Fatal("unknown older-commit schema borrowed HEAD's version")
	}
}
