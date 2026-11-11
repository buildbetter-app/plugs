package plugdump

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

const (
	NativeDeltaFormat         = "beads-native-delta/2"
	NativeCaptureSetFormat    = "beads-native-capture-set/2"
	NativeDeltaPartFormat     = "beads-native-part/2"
	NativeDeltaEvidenceFormat = "beads-physical-evidence/2"
	NativeDeltaPartBytes      = 64 << 20
	// This is a PER-RECORD decoding bound, never a total stream/part-set bound.
	// Records may cross part boundaries. Legacy portable bounds are unchanged.
	NativeDeltaRecordBytes     = MaxArchiveBytes
	NativeDeltaRootBytes       = 1 << 20
	NativeDeltaPartHeaderBytes = 4096
)

// These structs define the complete v2 JSON wire. Fields without omitempty
// are mandatory, including empty arrays and explicit null pointers. Unknown
// fields, duplicate object keys and trailing JSON values refuse.
type NativeCaptureSet struct {
	Format          string            `json:"format"`
	Contract        string            `json:"contract"`
	StoreID         string            `json:"store_id"`
	Projects        []string          `json:"projects"`
	Head            string            `json:"head"`
	HeadArchive     File              `json:"head_archive"`
	PartCount       uint64            `json:"part_count"`
	FinalPartSHA256 string            `json:"final_part_sha256"`
	StreamSHA256    string            `json:"stream_sha256"`
	StreamBytes     uint64            `json:"stream_bytes"`
	Totals          NativeDeltaTotals `json:"totals"`
}

type NativeDeltaPartHeader struct {
	Format             string  `json:"format"`
	Sequence           uint64  `json:"sequence"`
	PreviousPartSHA256 *string `json:"previous_part_sha256"`
	PayloadBytes       uint64  `json:"payload_bytes"`
	PayloadSHA256      string  `json:"payload_sha256"`
}

type NativeDeltaTotals struct {
	Schemas            uint64 `json:"schemas"`
	Commits            uint64 `json:"commits"`
	TableTransactions  uint64 `json:"table_transactions"`
	RowOperations      uint64 `json:"row_operations"`
	ReconstructedRows  uint64 `json:"reconstructed_rows"`
	ReconstructedCells uint64 `json:"reconstructed_cells"`
	FullImageBytes     uint64 `json:"full_image_bytes"`
}

type NativeDeltaHeader struct {
	Type            string                 `json:"type"` // header
	Format          string                 `json:"format"`
	StoreID         string                 `json:"store_id"`
	Projects        []string               `json:"projects"`
	Head            string                 `json:"head"`
	CommitCount     uint64                 `json:"commit_count"`
	HistoryCoverage *NativeHistoryCoverage `json:"history_coverage"`
	Ignore          NativeTable            `json:"ignore"`
	ConfigPolicy    map[string]string      `json:"config_policy"`
}

type NativeDeltaColumn struct {
	Name              string  `json:"name"`
	SQLType           string  `json:"sql_type"`
	PrimaryKeyOrdinal *uint64 `json:"primary_key_ordinal"`
}

type NativeDeltaTableSchema struct {
	Name    string              `json:"name"`
	Columns []NativeDeltaColumn `json:"columns"`
}

type NativeDeltaSchema struct {
	Type     string                    `json:"type"` // schema
	SchemaID string                    `json:"schema_id"`
	Profile  NativeCommitSchemaProfile `json:"profile"`
	Tables   []NativeDeltaTableSchema  `json:"tables"`
}

type NativeDeltaCommit struct {
	Type               string   `json:"type"` // commit
	Sequence           uint64   `json:"sequence"`
	CommitID           string   `json:"commit_id"`
	Parents            []string `json:"parents"`
	FirstParent        *string  `json:"first_parent"`
	Metadata           Row      `json:"metadata"`
	SchemaID           string   `json:"schema_id"`
	Mode               string   `json:"mode"` // snapshot or delta
	BaseCommit         *string  `json:"base_commit"`
	ExpectedRootSHA256 string   `json:"expected_root_sha256"`
}

type NativeDeltaCheckpoint struct {
	Type               string `json:"type"`      // head-checkpoint or current
	CommitID           string `json:"commit_id"` // actual HEAD, not a new commit
	SchemaID           string `json:"schema_id"`
	ExpectedRootSHA256 string `json:"expected_root_sha256"`
}

type NativeDeltaTable struct {
	Type           string `json:"type"` // table
	Name           string `json:"name"`
	TableSchemaID  string `json:"table_schema_id"`
	Mode           string `json:"mode"` // replace or delta
	ExpectedRows   uint64 `json:"expected_rows"`
	ExpectedSHA256 string `json:"expected_sha256"`
}

type NativeDeltaCell struct {
	Kind  string `json:"kind"`
	Value string `json:"value"` // explicit empty string for SQL NULL/empty values
}

type NativeDeltaRow struct {
	Type           string            `json:"type"` // row
	Op             string            `json:"op"`   // insert or update
	Key            string            `json:"key"`  // padded standard base64 of raw key bytes
	Cells          []NativeDeltaCell `json:"cells"`
	NormalizedJSON string            `json:"normalized_json"` // padded standard base64
}

type NativeDeltaDelete struct {
	Type string `json:"type"` // row
	Op   string `json:"op"`   // delete
	Key  string `json:"key"`
}

type NativeDeltaDropTable struct {
	Type                  string `json:"type"` // drop_table
	Name                  string `json:"name"`
	PreviousTableSchemaID string `json:"previous_table_schema_id"`
}

type NativeDeltaEndTable struct {
	Type          string `json:"type"` // end_table
	RowOperations uint64 `json:"row_operations"`
	Rows          uint64 `json:"rows"`
	SHA256        string `json:"sha256"`
}

type NativeDeltaEndCommit struct {
	Type       string `json:"type"` // end_commit
	Sequence   uint64 `json:"sequence"`
	CommitID   string `json:"commit_id"`
	TableCount uint64 `json:"table_count"`
	RootSHA256 string `json:"root_sha256"`
}

type NativeDeltaEndCheckpoint struct {
	Type       string `json:"type"` // end_head-checkpoint or end_current
	TableCount uint64 `json:"table_count"`
	RootSHA256 string `json:"root_sha256"`
}

type NativeDeltaEnd struct {
	Type              string            `json:"type"` // end
	Totals            NativeDeltaTotals `json:"totals"`
	HeadRootSHA256    string            `json:"head_root_sha256"`
	CurrentRootSHA256 string            `json:"current_root_sha256"`
}

type NativeDeltaWorkingIdentity struct {
	Format          string `json:"format"`
	Database        string `json:"database"`
	HeadCommitID    string `json:"head_commit_id"`
	WorkingRootHash string `json:"working_root_hash"`
	ReadOnlyScope   bool   `json:"read_only_scope"`
	Complete        bool   `json:"complete"`
}

type NativeDeltaEvidence struct {
	Format            string                     `json:"format"`
	CaptureSetSHA256  string                     `json:"capture_set_sha256"`
	StoreID           string                     `json:"store_id"`
	Projects          []string                   `json:"projects"`
	SQLDefaults       string                     `json:"sql_defaults"`
	Working           NativeDeltaWorkingIdentity `json:"working"`
	Totals            NativeDeltaTotals          `json:"totals"`
	HeadRootSHA256    string                     `json:"head_root_sha256"`
	CurrentRootSHA256 string                     `json:"current_root_sha256"`
}

// All B operands are arbitrary raw bytes. Text operands are first encoded as
// UTF-8. Hex/base64 are decoded before B where explicitly specified below.
func nativeDeltaU(out *bytes.Buffer, n uint64) { _ = binary.Write(out, binary.BigEndian, n) }
func nativeDeltaB(out *bytes.Buffer, value []byte) {
	nativeDeltaU(out, uint64(len(value)))
	out.Write(value)
}
func nativeDeltaS(out *bytes.Buffer, value string) { nativeDeltaB(out, []byte(value)) }
func nativeDeltaOptional(out *bytes.Buffer, value *uint64) {
	if value == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	nativeDeltaU(out, *value)
}
func nativeDeltaDigest(domain string, value []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(domain + "\x00"))
	_, _ = h.Write(value)
	return hex.EncodeToString(h.Sum(nil))
}

func NativeDeltaTableSchemaID(table NativeDeltaTableSchema) string {
	var out bytes.Buffer
	nativeDeltaS(&out, table.Name)
	nativeDeltaU(&out, uint64(len(table.Columns)))
	for _, c := range table.Columns {
		nativeDeltaS(&out, c.Name)
		nativeDeltaS(&out, c.SQLType)
		nativeDeltaOptional(&out, c.PrimaryKeyOrdinal)
	}
	return nativeDeltaDigest("beads-native-table-schema/2", out.Bytes())
}

func NativeDeltaSchemaID(schema NativeDeltaSchema) string {
	var out bytes.Buffer
	nativeDeltaS(&out, schema.Profile.Format)
	nativeDeltaS(&out, schema.Profile.Scope)
	nativeDeltaOptional(&out, schema.Profile.MainVersion)
	nativeDeltaOptional(&out, schema.Profile.IgnoredVersion)
	nativeDeltaU(&out, uint64(len(schema.Profile.Absences)))
	for _, a := range schema.Profile.Absences {
		nativeDeltaS(&out, a.Family)
		nativeDeltaS(&out, a.Column)
		nativeDeltaS(&out, a.Reason)
	}
	nativeDeltaU(&out, uint64(len(schema.Tables)))
	for _, t := range schema.Tables {
		nativeDeltaS(&out, t.Name)
		raw, _ := hex.DecodeString(NativeDeltaTableSchemaID(t))
		out.Write(raw)
	}
	return nativeDeltaDigest("beads-native-schema/2", out.Bytes())
}

func NativeDeltaRowPayload(cells []NativeDeltaCell, normalized []byte) []byte {
	var out bytes.Buffer
	nativeDeltaU(&out, uint64(len(cells)))
	for _, cell := range cells {
		nativeDeltaS(&out, cell.Kind)
		nativeDeltaS(&out, cell.Value)
	}
	nativeDeltaB(&out, normalized)
	return out.Bytes()
}

func NativeDeltaPrimaryKey(table NativeDeltaTableSchema, cells []NativeDeltaCell) ([]byte, error) {
	if len(cells) != len(table.Columns) {
		return nil, fmt.Errorf("delta cell width differs")
	}
	keys := map[uint64]NativeDeltaCell{}
	for i, column := range table.Columns {
		if column.PrimaryKeyOrdinal == nil {
			continue
		}
		o := *column.PrimaryKeyOrdinal
		if _, duplicate := keys[o]; duplicate {
			return nil, fmt.Errorf("duplicate primary key ordinal")
		}
		keys[o] = cells[i]
	}
	if len(keys) == 0 {
		return nil, nil
	}
	var out bytes.Buffer
	out.WriteString("pk\x00")
	nativeDeltaU(&out, uint64(len(keys)))
	for i := uint64(0); i < uint64(len(keys)); i++ {
		cell, exists := keys[i]
		if !exists || cell.Kind == "sql-null" || cell.Kind == "secret-reference" {
			return nil, fmt.Errorf("invalid primary key metadata/value")
		}
		nativeDeltaS(&out, cell.Kind)
		nativeDeltaS(&out, cell.Value)
	}
	return out.Bytes(), nil
}

func NativeDeltaBagKey(payload []byte, occurrence uint64) []byte {
	var out bytes.Buffer
	out.WriteString("bag\x00")
	hash := sha256.Sum256(payload)
	out.Write(hash[:])
	nativeDeltaU(&out, occurrence)
	return out.Bytes()
}

// Rows must already be sorted by decoded unsigned key bytes, with no duplicate
// keys. The caller also validates PK/bag identity and typed/normalized coherence.
func NativeDeltaTableSHA256(table NativeDeltaTableSchema, rows []NativeDeltaRow) (string, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("beads-native-table/2\x00"))
	id, _ := hex.DecodeString(NativeDeltaTableSchemaID(table))
	_, _ = h.Write(id)
	var encoded bytes.Buffer
	nativeDeltaU(&encoded, uint64(len(rows)))
	_, _ = h.Write(encoded.Bytes())
	var previous []byte
	for _, row := range rows {
		key, err := base64.StdEncoding.DecodeString(row.Key)
		if err != nil || len(key) == 0 || previous != nil && bytes.Compare(previous, key) >= 0 {
			return "", fmt.Errorf("unordered delta keys")
		}
		normalized, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
		if err != nil {
			return "", err
		}
		encoded.Reset()
		nativeDeltaB(&encoded, key)
		nativeDeltaB(&encoded, NativeDeltaRowPayload(row.Cells, normalized))
		_, _ = h.Write(encoded.Bytes())
		previous = key
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type NativeDeltaTableDigest struct {
	Rows   uint64
	SHA256 string
}

func NativeDeltaRootSHA256(tables map[string]NativeDeltaTableDigest) (string, error) {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	var out bytes.Buffer
	for _, name := range names {
		d := tables[name]
		raw, err := hex.DecodeString(d.SHA256)
		if err != nil || len(raw) != sha256.Size {
			return "", fmt.Errorf("invalid table digest")
		}
		nativeDeltaS(&out, name)
		out.Write(raw)
		nativeDeltaU(&out, d.Rows)
	}
	return nativeDeltaDigest("beads-native-commit/2", out.Bytes()), nil
}
