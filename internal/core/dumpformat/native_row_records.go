package dumpformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	NativeRowFormat       = "plug-native-row/1"
	NativeRowMetaFormat   = "plug-native-meta/1"
	NativeRowV1ContentBag = "v1-content-bag"
	NativeRowD3Physical   = "d3-physical"
	NativeRowOrder        = "normalized-json-bytes/1"
	// NativeRowOpaqueCurrentHistory labels inert authenticated system-table
	// evidence. It never authorizes schema execution or physical restoration.
	NativeRowOpaqueCurrentHistory = "opaque-native-physical"
	nativeRetainedHistory         = "retained-native-commit"
	nativeCurrentPrefix           = "source/native-current/"
)

// NativeRowRecordError is a refusal of record shape, identity or reconstruction.
// It exposes no source cell values. A valid codec object is not proof that its
// caller authenticated the original archive, schema or physical source.
type NativeRowRecordError struct{ Table, Reason string }

func (e *NativeRowRecordError) Error() string {
	return fmt.Sprintf("native row record table=%s: %s", e.Table, e.Reason)
}
func (e *NativeRowRecordError) Unwrap() error { return driver.ErrUnsupported }

func nativeRowRefusal(table, reason string) error {
	return &NativeRowRecordError{Table: table, Reason: reason}
}

// NativeRowMeta is the closed canonical table metadata body. Counts and hashes
// describe represented rows. A filtered current lease projection has its own
// digest; its original physical commitment remains separate source evidence.
type NativeRowMeta struct {
	Format              string              `json:"format"`
	Source              SourceIdentity      `json:"source_identity"`
	Table               string              `json:"table"`
	History             string              `json:"history"`
	IdentityKind        string              `json:"identity_kind"`
	Columns             []string            `json:"columns"`
	SchemaID            string              `json:"schema_id"`
	PhysicalColumns     []NativeDeltaColumn `json:"physical_columns"`
	RowOrder            string              `json:"row_order"`
	RowCount            uint64              `json:"row_count"`
	TableBodySHA256     string              `json:"table_body_sha256"`
	PhysicalTableSHA256 *string             `json:"physical_table_sha256"`
}

type nativeRowBody struct {
	Format       string            `json:"format"`
	Source       SourceIdentity    `json:"source_identity"`
	Table        string            `json:"table"`
	History      string            `json:"history"`
	IdentityKind string            `json:"identity_kind"`
	Key          string            `json:"key"`
	Fields       Row               `json:"fields"`
	Cells        []NativeDeltaCell `json:"cells,omitempty"`
}

// NativeDecodedRow is an owned decoded row body. Cells is nil for v1. The raw
// canonical key is detached; no identity or SQL type is inferred from Fields.
type NativeDecodedRow struct {
	Source                       SourceIdentity
	Table, History, IdentityKind string
	Key                          []byte
	Fields                       Row
	Cells                        []NativeDeltaCell
}

func nativeRowPrefix(source SourceIdentity, table, history, kind string) (string, error) {
	if !validSourceIdentity(source) || !utf8.ValidString(source.StoreID) || !utf8.ValidString(source.Project) || !nativeSQLName.MatchString(table) {
		return "", nativeRowRefusal(table, "invalid source/table identity")
	}
	if kind != NativeRowV1ContentBag && kind != NativeRowD3Physical {
		return "", nativeRowRefusal(table, "unknown identity kind")
	}
	switch history {
	case nativeRetainedHistory:
		return driver.NativeTableHistoryRecordPrefix, nil
	case FamilyVersioned, FamilyUnversioned, NativeRowOpaqueCurrentHistory:
		return nativeCurrentPrefix, nil
	default:
		return "", nativeRowRefusal(table, "unknown history class")
	}
}

func nativeRowRecordRef(source SourceIdentity, table, history, kind string, key []byte, meta bool) (model.Ref, error) {
	prefix, err := nativeRowPrefix(source, table, history, kind)
	if err != nil {
		return model.Ref{}, err
	}
	var tuple bytes.Buffer
	domain, part := NativeRowFormat, "row/"
	if meta {
		domain, part = NativeRowMetaFormat, "meta/"
	}
	nativeDeltaS(&tuple, domain)
	nativeDeltaS(&tuple, source.StoreID)
	nativeDeltaS(&tuple, source.Project)
	nativeDeltaS(&tuple, table)
	nativeDeltaS(&tuple, kind)
	if !meta {
		nativeDeltaB(&tuple, key)
	}
	sum := sha256.Sum256(tuple.Bytes())
	return model.RefByID(prefix+part+hex.EncodeToString(sum[:]), model.Natural64(0)), nil
}

func nativeRowSameRef(actual, expected model.Ref) bool {
	// Ref's private constructor marker and Natural's zero spellings are Go
	// representation details. Compare the validated generic-driver identity.
	return actual.Validate() == nil && actual.IsID() && actual.ID == expected.ID && actual.Key == "" && actual.Branch.IsZero()
}

func nativeV1RowKey(normalized []byte, occurrence uint64) []byte {
	var out bytes.Buffer
	out.WriteString("v1-bag\x00")
	sum := sha256.Sum256(normalized)
	out.Write(sum[:])
	nativeDeltaU(&out, occurrence)
	return out.Bytes()
}

func nativeRowOccurrence(key []byte, prefix string) (uint64, bool) {
	if len(key) != len(prefix)+sha256.Size+8 || !bytes.HasPrefix(key, []byte(prefix)) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(key)-8:]), true
}

func nativeRowMarshal(body any, source SourceIdentity, table, history, kind string, key []byte, meta bool) (driver.Record, error) {
	ref, err := nativeRowRecordRef(source, table, history, kind, key, meta)
	if err != nil {
		return driver.Record{}, err
	}
	data, err := json.Marshal(body)
	if err != nil {
		return driver.Record{}, nativeRowRefusal(table, "nonserializable body")
	}
	return driver.Record{Ref: ref, Kind: "beads-archive", Body: data}, nil
}

// NativeV1RowRecord preserves normalized content-bag identity. The caller must
// supply the canonical duplicate occurrence; complete reconstruction checks it.
func NativeV1RowRecord(source SourceIdentity, table, history string, fields Row, occurrence uint64) (driver.Record, error) {
	if err := nativeRowJSONValue(fields, 0); err != nil {
		return driver.Record{}, nativeRowRefusal(table, "invalid normalized JSON value")
	}
	columns := make([]string, 0, len(fields))
	for name := range fields {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	if err := validateNativeTable(table, NativeTable{Columns: columns, Rows: []Row{fields}}); err != nil {
		return driver.Record{}, nativeRowRefusal(table, "invalid normalized v1 row")
	}
	normalized, err := json.Marshal(fields)
	if err != nil {
		return driver.Record{}, nativeRowRefusal(table, "invalid normalized v1 row")
	}
	key := nativeV1RowKey(normalized, occurrence)
	body := nativeRowBody{Format: NativeRowFormat, Source: source, Table: table, History: history, IdentityKind: NativeRowV1ContentBag, Key: base64.StdEncoding.EncodeToString(key), Fields: fields}
	return nativeRowMarshal(body, source, table, history, NativeRowV1ContentBag, key, false)
}

// NativeV2RowRecord uses the original D3 schema/key/cell preimages. It checks
// shape/coherence, not archive authority or complete keyless multiplicity.
func NativeV2RowRecord(source SourceIdentity, schema NativeDeltaTableSchema, history string, row NativeDeltaRow, policy map[string]string) (driver.Record, error) {
	if err := nativeRowPhysicalSchema(schema); err != nil {
		return driver.Record{}, err
	}
	fields, key, err := nativeRowPhysicalRow(schema, row, policy)
	if err != nil {
		return driver.Record{}, err
	}
	body := nativeRowBody{Format: NativeRowFormat, Source: source, Table: schema.Name, History: history, IdentityKind: NativeRowD3Physical, Key: row.Key, Fields: fields, Cells: row.Cells}
	return nativeRowMarshal(body, source, schema.Name, history, NativeRowD3Physical, key, false)
}

func nativeRowPhysicalRow(schema NativeDeltaTableSchema, row NativeDeltaRow, policy map[string]string) (Row, []byte, error) {
	for _, cell := range row.Cells {
		if !utf8.ValidString(cell.Kind) || !utf8.ValidString(cell.Value) {
			return nil, nil, nativeRowRefusal(schema.Name, "non-UTF8 typed cell")
		}
	}
	fields, key, err := validateNativeDeltaRow(schema, row, policy)
	if err != nil {
		return nil, nil, nativeRowRefusal(schema.Name, "invalid D3 row/key/cells")
	}
	pk, err := NativeDeltaPrimaryKey(schema, row.Cells)
	if err != nil {
		return nil, nil, nativeRowRefusal(schema.Name, "invalid D3 primary key")
	}
	if pk == nil {
		occ, ok := nativeRowOccurrence(key, "bag\x00")
		normalized, decodeErr := nativeDeltaBase64(row.NormalizedJSON)
		if !ok || decodeErr != nil || !bytes.Equal(key, NativeDeltaBagKey(NativeDeltaRowPayload(row.Cells, normalized), occ)) {
			return nil, nil, nativeRowRefusal(schema.Name, "invalid D3 bag key")
		}
	}
	return fields, key, nil
}

// Input rows are already normalized JSON, with lossless decimal tokens. Do
// not silently repair invalid text or accept a custom marshaler/float value
// whose conversion could discard source information.
func nativeRowJSONValue(value any, depth int) error {
	if depth > 128 {
		return nativeRowRefusal("", "JSON nesting exceeds 128")
	}
	switch value := value.(type) {
	case nil, bool:
		return nil
	case json.Number:
		if value != "" {
			if _, err := json.Marshal(value); err == nil {
				return nil
			}
		}
	case string:
		if utf8.ValidString(value) {
			return nil
		}
	case Row:
		return nativeRowJSONValue(map[string]any(value), depth)
	case map[string]any:
		for key, item := range value {
			if !utf8.ValidString(key) {
				return nativeRowRefusal("", "non-UTF8 JSON key")
			}
			if err := nativeRowJSONValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for _, item := range value {
			if err := nativeRowJSONValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return nativeRowRefusal("", "value is not lossless normalized JSON")
}

func nativeRowPhysicalSchema(schema NativeDeltaTableSchema) error {
	if !nativeSQLName.MatchString(schema.Name) || len(schema.Columns) == 0 || len(schema.Columns) > 1024 {
		return nativeRowRefusal(schema.Name, "invalid physical schema")
	}
	names, ordinals := map[string]bool{}, map[uint64]bool{}
	for _, c := range schema.Columns {
		if !nativeSQLName.MatchString(c.Name) || names[c.Name] || c.SQLType == "" || !utf8.ValidString(c.SQLType) {
			return nativeRowRefusal(schema.Name, "invalid physical column")
		}
		names[c.Name] = true
		if c.PrimaryKeyOrdinal != nil {
			o := *c.PrimaryKeyOrdinal
			if ordinals[o] {
				return nativeRowRefusal(schema.Name, "duplicate primary key ordinal")
			}
			ordinals[o] = true
		}
	}
	for i := 0; i < len(ordinals); i++ {
		if !ordinals[uint64(i)] {
			return nativeRowRefusal(schema.Name, "noncontiguous primary key ordinals")
		}
	}
	return nil
}

// NativeMetaRecord encodes small metadata without a per-row identity vector.
// A caller-supplied count/digest becomes usable only after complete source and
// row-set reconciliation; this constructor mints no verification authority.
func NativeMetaRecord(meta NativeRowMeta) (driver.Record, error) {
	if err := validateNativeRowMeta(meta); err != nil {
		return driver.Record{}, err
	}
	return nativeRowMarshal(meta, meta.Source, meta.Table, meta.History, meta.IdentityKind, nil, true)
}

func nativeRowDigest(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func nativeV1ColumnsID(table string, columns []string) string {
	var out bytes.Buffer
	nativeDeltaS(&out, "plug-native-v1-columns/1")
	nativeDeltaS(&out, table)
	nativeDeltaU(&out, uint64(len(columns)))
	for _, c := range columns {
		nativeDeltaS(&out, c)
	}
	sum := sha256.Sum256(out.Bytes())
	return hex.EncodeToString(sum[:])
}

func validateNativeRowMeta(meta NativeRowMeta) error {
	if _, err := nativeRowPrefix(meta.Source, meta.Table, meta.History, meta.IdentityKind); err != nil {
		return err
	}
	if meta.Format != NativeRowMetaFormat || meta.RowOrder != NativeRowOrder || len(meta.Columns) == 0 || len(meta.Columns) > 1024 || !nativeRowDigest(meta.TableBodySHA256) {
		return nativeRowRefusal(meta.Table, "invalid metadata shape/order/digest")
	}
	for i, c := range meta.Columns {
		if !nativeSQLName.MatchString(c) || i > 0 && c <= meta.Columns[i-1] {
			return nativeRowRefusal(meta.Table, "invalid normalized columns")
		}
	}
	if meta.IdentityKind == NativeRowV1ContentBag {
		if meta.PhysicalColumns != nil || meta.PhysicalTableSHA256 != nil || meta.SchemaID != nativeV1ColumnsID(meta.Table, meta.Columns) {
			return nativeRowRefusal(meta.Table, "v1 metadata claims physical authority")
		}
		return nil
	}
	schema := NativeDeltaTableSchema{Name: meta.Table, Columns: meta.PhysicalColumns}
	if err := nativeRowPhysicalSchema(schema); err != nil {
		return err
	}
	columns := make([]string, len(meta.PhysicalColumns))
	for i, c := range meta.PhysicalColumns {
		columns[i] = c.Name
	}
	sort.Strings(columns)
	if !equalJSON(columns, meta.Columns) || meta.SchemaID != NativeDeltaTableSchemaID(schema) || meta.PhysicalTableSHA256 == nil || !nativeRowDigest(*meta.PhysicalTableSHA256) {
		return nativeRowRefusal(meta.Table, "physical metadata/schema differs")
	}
	return nil
}

// Canonical re-encoding rejects missing, duplicate, case-folded and unknown
// fields, whitespace, noncanonical numbers and trailing data. UseNumber retains
// normalized source number spelling without visiting float64.
func nativeRowDecode(data []byte, target any, budgets ...*NativeDeltaBudget) error {
	if len(budgets) != 0 && budgets[0] != nil {
		for _, phase := range []string{"native_record_decoder_input", "native_record_canonical_bytes"} {
			if err := budgets[0].ChargeBytes(phase, uint64(len(data))); err != nil {
				return err
			}
		}
	}
	if !utf8.Valid(data) {
		return nativeRowRefusal("", "non-UTF8 body")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return nativeRowRefusal("", "invalid body shape")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nativeRowRefusal("", "trailing body data")
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(data, canonical) {
		return nativeRowRefusal("", "noncanonical body")
	}
	return nil
}

// DecodeNativeRowRecord returns exactly one of meta/row. It rejects wrong Kind
// before body parsing. Complete namespace enumeration belongs to the caller;
// filtering the query by Kind would hide wrong-kind native-prefix extras.
func DecodeNativeRowRecord(record driver.Record) (*NativeRowMeta, *NativeDecodedRow, error) {
	return decodeNativeRowRecord(record, nil)
}

// DecodeNativeRowRecord admits the decoded body and exact validation candidates
// in this archive's owner. The existing pure codec remains budget-free.
func (a *Archive) DecodeNativeRowRecord(record driver.Record) (*NativeRowMeta, *NativeDecodedRow, error) {
	return decodeNativeRowRecord(record, a.decodedBudget())
}

func decodeNativeRowRecord(record driver.Record, owner *NativeDeltaBudget) (*NativeRowMeta, *NativeDecodedRow, error) {
	var work *NativeDeltaBudget
	if owner != nil {
		work = owner.Scope()
		defer work.Release()
		if err := owner.ChargeBytes("native_record_decoded_body", uint64(len(record.Body))); err != nil {
			return nil, nil, err
		}
	}
	if record.Kind != "beads-archive" {
		return nil, nil, nativeRowRefusal("", "wrong record kind")
	}
	// Canonical bodies put the closed format field first. Selecting its exact
	// prefix avoids allocating an unbounded marker string before admission.
	if bytes.HasPrefix(record.Body, []byte(`{"format":"plug-native-meta/1",`)) {
		var meta NativeRowMeta
		if err := nativeRowDecode(record.Body, &meta, work); err != nil {
			return nil, nil, err
		}
		if work != nil {
			if err := reserveNativeMetaValidation(meta, work); err != nil {
				return nil, nil, err
			}
			if err := reserveNativeRowRef(work, work, meta.Source, meta.Table, meta.History, meta.IdentityKind, 0, true); err != nil {
				return nil, nil, err
			}
		}
		if err := validateNativeRowMeta(meta); err != nil {
			return nil, nil, err
		}
		ref, err := nativeRowRecordRef(meta.Source, meta.Table, meta.History, meta.IdentityKind, nil, true)
		if err != nil || !nativeRowSameRef(record.Ref, ref) {
			return nil, nil, nativeRowRefusal(meta.Table, "metadata Ref differs")
		}
		return &meta, nil, nil
	}
	if !bytes.HasPrefix(record.Body, []byte(`{"format":"plug-native-row/1",`)) {
		return nil, nil, nativeRowRefusal("", "unknown record format")
	}
	var body nativeRowBody
	if err := nativeRowDecode(record.Body, &body, work); err != nil {
		return nil, nil, err
	}
	if work != nil {
		for _, candidate := range []struct {
			phase string
			size  uint64
		}{
			{"native_record_key_decode", uint64(base64.StdEncoding.DecodedLen(len(body.Key)))},
			{"native_record_key_encode_buffer", uint64(len(body.Key))},
			{"native_record_key_encode_text", uint64(len(body.Key))},
		} {
			if err := work.ChargeBytes(candidate.phase, candidate.size); err != nil {
				return nil, nil, err
			}
		}
	}
	key, err := nativeDeltaBase64(body.Key)
	if err != nil || len(key) == 0 || body.Fields == nil || len(body.Fields) == 0 {
		return nil, nil, nativeRowRefusal(body.Table, "missing row/key")
	}
	if body.IdentityKind == NativeRowV1ContentBag && body.Cells != nil || body.IdentityKind == NativeRowD3Physical && len(body.Cells) == 0 {
		return nil, nil, nativeRowRefusal(body.Table, "identity/cell shape differs")
	}
	if work != nil {
		if err := reserveNativeRowRef(work, work, body.Source, body.Table, body.History, body.IdentityKind, len(key), false); err != nil {
			return nil, nil, err
		}
		if err := owner.ChargeBytes("native_record_detached_key", uint64(len(key))); err != nil {
			return nil, nil, err
		}
	}
	ref, err := nativeRowRecordRef(body.Source, body.Table, body.History, body.IdentityKind, key, false)
	if err != nil || !nativeRowSameRef(record.Ref, ref) {
		return nil, nil, nativeRowRefusal(body.Table, "row Ref differs")
	}
	return nil, &NativeDecodedRow{Source: body.Source, Table: body.Table, History: body.History, IdentityKind: body.IdentityKind, Key: append([]byte{}, key...), Fields: body.Fields, Cells: body.Cells}, nil
}

func reserveNativeMetaValidation(meta NativeRowMeta, work *NativeDeltaBudget) error {
	counter := deltaSizeCounter{}
	if meta.IdentityKind == NativeRowV1ContentBag {
		if err := counter.add(uint64(3*8 + len("plug-native-v1-columns/1") + len(meta.Table))); err != nil {
			return err
		}
		for _, name := range meta.Columns {
			if err := counter.add(uint64(8 + len(name))); err != nil {
				return err
			}
		}
	} else {
		if err := counter.add(uint64(16 + len(meta.Table))); err != nil {
			return err
		}
		for _, column := range meta.PhysicalColumns {
			if err := counter.add(uint64(17 + len(column.Name) + len(column.SQLType))); err != nil {
				return err
			}
			if column.PrimaryKeyOrdinal != nil {
				if err := counter.add(8); err != nil {
					return err
				}
			}
		}
		for _, phase := range []string{"native_meta_normalized_columns", "native_meta_physical_columns"} {
			if err := work.ChargeJSON(phase, meta.Columns); err != nil {
				return err
			}
		}
	}
	if err := work.ChargeBytes("native_meta_schema_preimage", counter.n); err != nil {
		return err
	}
	if err := work.ChargeBytes("native_meta_schema_digest", sha256.Size*2); err != nil {
		return err
	}
	return work.ChargeBytes("native_meta_digest_validation", sha256.Size)
}
