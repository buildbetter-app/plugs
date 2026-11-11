package dumpformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeTableBodyBytes is the old native-table body serializer: exact ordered
// envelope, default JSON escaping, lexical json.Number, and no trailing LF.
// It makes no physical SQL or source-authentication claim. Reconstruction and
// its caller must separately validate the complete table and its authority.
func NativeTableBodyBytes(source SourceIdentity, table, history string, value NativeTable) ([]byte, error) {
	if _, err := nativeRowPrefix(source, table, history, NativeRowV1ContentBag); err != nil {
		return nil, err
	}
	if value.Columns == nil || value.Rows == nil {
		return nil, nativeRowRefusal(table, "missing table arrays")
	}
	for _, row := range value.Rows {
		if err := nativeRowJSONValue(row, 0); err != nil {
			return nil, nativeRowRefusal(table, "invalid normalized JSON value")
		}
	}
	data, err := json.Marshal(struct {
		Source  SourceIdentity `json:"source_identity"`
		Table   string         `json:"table"`
		History string         `json:"history"`
		Columns []string       `json:"columns"`
		Rows    []Row          `json:"rows"`
	}{source, table, history, value.Columns, value.Rows})
	if err != nil {
		return nil, nativeRowRefusal(table, "nonserializable table body")
	}
	return data, nil
}

// NativeV1TableRecords lowers one already validated, canonically ordered v1
// table. Empty present tables retain a meta record. No typed witness is needed.
// The returned records are detached, sorted by the driver record-lock key.
func NativeV1TableRecords(source SourceIdentity, table, history string, value NativeTable) ([]driver.Record, error) {
	for _, row := range value.Rows {
		if err := nativeRowJSONValue(row, 0); err != nil {
			return nil, nativeRowRefusal(table, "invalid normalized JSON value")
		}
	}
	if err := validateNativeTable(table, value); err != nil {
		return nil, nativeRowRefusal(table, "invalid normalized v1 table")
	}
	occurrences := map[string]uint64{}
	rows := make([]*NativeDecodedRow, 0, len(value.Rows))
	records := make([]driver.Record, 0, len(value.Rows)+1)
	for _, fields := range value.Rows {
		if err := nativeRowJSONValue(fields, 0); err != nil {
			return nil, nativeRowRefusal(table, "invalid normalized JSON value")
		}
		normalized, err := json.Marshal(fields)
		if err != nil {
			return nil, nativeRowRefusal(table, "nonserializable v1 row")
		}
		occ := occurrences[string(normalized)]
		record, err := NativeV1RowRecord(source, table, history, fields, occ)
		if err != nil {
			return nil, err
		}
		occurrences[string(normalized)] = occ + 1 // bounded by the input slice length
		records = append(records, record)
		rows = append(rows, &NativeDecodedRow{Source: source, Table: table, History: history, IdentityKind: NativeRowV1ContentBag, Key: nativeV1RowKey(normalized, occ), Fields: fields})
	}
	meta, _, err := nativeRowDescribe(source, table, history, NativeRowV1ContentBag, value.Columns, nil, rows, nil)
	if err != nil {
		return nil, err
	}
	return nativeRowFinishRecords(records, meta)
}

// NativeV2TableRecords lowers one complete typed D3 table state. The caller
// authenticates the schema's PK descriptors (including actual keylessness),
// source root and complete membership before using these records. This pure
// codec cannot confer that authority on a caller-supplied schema or digest.
func NativeV2TableRecords(source SourceIdentity, schema NativeDeltaTableSchema, history string, value []NativeDeltaRow, policy map[string]string) ([]driver.Record, error) {
	if err := nativeRowPhysicalSchema(schema); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nativeRowRefusal(schema.Name, "missing physical rows")
	}
	columns := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		columns[i] = c.Name
	}
	sort.Strings(columns)
	rows := make([]*NativeDecodedRow, 0, len(value))
	records := make([]driver.Record, 0, len(value)+1)
	for _, row := range value {
		record, err := NativeV2RowRecord(source, schema, history, row, policy)
		if err != nil {
			return nil, err
		}
		_, decoded, err := DecodeNativeRowRecord(record)
		if err != nil {
			return nil, err
		}
		rows = append(rows, decoded)
		records = append(records, record)
	}
	meta, _, err := nativeRowDescribe(source, schema.Name, history, NativeRowD3Physical, columns, schema.Columns, rows, policy)
	if err != nil {
		return nil, err
	}
	return nativeRowFinishRecords(records, meta)
}

func nativeRowFinishRecords(records []driver.Record, meta NativeRowMeta) ([]driver.Record, error) {
	record, err := NativeMetaRecord(meta)
	if err != nil {
		return nil, err
	}
	records = append(records, record)
	type ordered struct {
		record driver.Record
		key    string
	}
	keys := make([]ordered, len(records))
	for i, r := range records {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref})
		if err != nil {
			return nil, nativeRowRefusal(meta.Table, "invalid record lock key")
		}
		keys[i] = ordered{r, key}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].key < keys[j].key })
	for i, item := range keys {
		if i > 0 && item.key == keys[i-1].key {
			return nil, nativeRowRefusal(meta.Table, "duplicate record Ref")
		}
		records[i] = item.record
	}
	return records, nil
}

// ReconstructNativeRowTable verifies a complete group for exactly one expected
// source/table/plane. It rejects every foreign, extra, malformed or wrong-Kind
// input rather than selecting a valid subset. The caller must first enumerate
// its entire fixed snapshot with Query{}, select native namespaces before
// Kind validation, and independently compare the complete table membership.
// Returned table/meta own their data; no input Body slice is retained.
func ReconstructNativeRowTable(source SourceIdentity, table, history string, records []driver.Record, policy map[string]string) (NativeTable, NativeRowMeta, error) {
	refuse := func(reason string) (NativeTable, NativeRowMeta, error) {
		return NativeTable{}, NativeRowMeta{}, nativeRowRefusal(table, reason)
	}
	if _, err := nativeRowPrefix(source, table, history, NativeRowV1ContentBag); err != nil {
		return NativeTable{}, NativeRowMeta{}, err
	}
	seen := map[string]bool{}
	var meta *NativeRowMeta
	rows := make([]*NativeDecodedRow, 0)
	for _, record := range records {
		if seen[record.Ref.ID] {
			return refuse("duplicate record Ref")
		}
		seen[record.Ref.ID] = true
		m, row, err := DecodeNativeRowRecord(record)
		if err != nil {
			return NativeTable{}, NativeRowMeta{}, err
		}
		if m != nil {
			if meta != nil {
				return refuse("multiple metadata records")
			}
			if m.Source != source || m.Table != table || m.History != history {
				return refuse("foreign metadata identity")
			}
			meta = m
		} else {
			if row.Source != source || row.Table != table || row.History != history {
				return refuse("foreign row identity")
			}
			rows = append(rows, row)
		}
	}
	if meta == nil {
		return refuse("missing metadata record")
	}
	if meta.RowCount != uint64(len(rows)) {
		return refuse("row count differs")
	}
	actual, value, err := nativeRowDescribe(source, table, history, meta.IdentityKind, meta.Columns, meta.PhysicalColumns, rows, policy)
	if err != nil {
		return NativeTable{}, NativeRowMeta{}, err
	}
	if !equalJSON(*meta, actual) {
		return refuse("metadata commitments differ")
	}
	return value, *meta, nil
}

type nativeRowEntry struct {
	row        *NativeDecodedRow
	normalized []byte
	physical   NativeDeltaRow
}

// nativeRowDescribe computes metadata from complete row preimages. Hash-group
// checks retain full payloads: a digest match never silently merges distinct
// values. Duplicate indices are dense independent of incoming record order.
func nativeRowDescribe(source SourceIdentity, table, history, kind string, columns []string, physical []NativeDeltaColumn, rows []*NativeDecodedRow, policy map[string]string) (NativeRowMeta, NativeTable, error) {
	fail := func(reason string) (NativeRowMeta, NativeTable, error) {
		return NativeRowMeta{}, NativeTable{}, nativeRowRefusal(table, reason)
	}
	if _, err := nativeRowPrefix(source, table, history, kind); err != nil {
		return NativeRowMeta{}, NativeTable{}, err
	}
	if len(columns) == 0 || len(columns) > 1024 {
		return fail("invalid normalized column census")
	}
	for i, c := range columns {
		if !nativeSQLName.MatchString(c) || i > 0 && c <= columns[i-1] {
			return fail("invalid normalized columns")
		}
	}
	schema := NativeDeltaTableSchema{Name: table, Columns: physical}
	if kind == NativeRowV1ContentBag {
		if physical != nil {
			return fail("v1 table claims physical columns")
		}
	} else {
		if err := nativeRowPhysicalSchema(schema); err != nil {
			return NativeRowMeta{}, NativeTable{}, err
		}
		physicalNames := make([]string, len(physical))
		for i, c := range physical {
			physicalNames[i] = c.Name
		}
		sort.Strings(physicalNames)
		if !equalJSON(columns, physicalNames) {
			return fail("normalized and physical columns differ")
		}
	}
	type bagGroup struct {
		payload    []byte
		count, max uint64
	}
	bags := map[string]bagGroup{}
	entries := make([]nativeRowEntry, 0, len(rows))
	for _, row := range rows {
		if row.Source != source || row.Table != table || row.History != history || row.IdentityKind != kind {
			return fail("mixed row source/identity kind")
		}
		if err := nativeRowJSONValue(row.Fields, 0); err != nil {
			return fail("invalid normalized JSON value")
		}
		normalized, err := json.Marshal(row.Fields)
		if err != nil {
			return fail("nonserializable normalized row")
		}
		entry := nativeRowEntry{row: row, normalized: normalized}
		var payload []byte
		var occurrence uint64
		bag := false
		if kind == NativeRowV1ContentBag {
			if row.Cells != nil {
				return fail("v1 row claims typed cells")
			}
			if err := validateNativeTable(table, NativeTable{Columns: columns, Rows: []Row{row.Fields}}); err != nil {
				return fail("v1 row/schema differs")
			}
			occ, ok := nativeRowOccurrence(row.Key, "v1-bag\x00")
			if !ok || !bytes.Equal(row.Key, nativeV1RowKey(normalized, occ)) {
				return fail("v1 content key differs")
			}
			payload, occurrence, bag = normalized, occ, true
		} else {
			wire := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(row.Key), Cells: row.Cells, NormalizedJSON: base64.StdEncoding.EncodeToString(append(append([]byte{}, normalized...), '\n'))}
			if _, _, err := nativeRowPhysicalRow(schema, wire, policy); err != nil {
				return NativeRowMeta{}, NativeTable{}, err
			}
			entry.physical = wire
			if occ, ok := nativeRowOccurrence(row.Key, "bag\x00"); ok {
				withLF, _ := nativeDeltaBase64(wire.NormalizedJSON)
				payload, occurrence, bag = NativeDeltaRowPayload(row.Cells, withLF), occ, true
			}
		}
		if bag {
			digest := string(row.Key[:len(row.Key)-8])
			group, exists := bags[digest]
			if exists && !bytes.Equal(group.payload, payload) {
				return fail("row digest collision")
			}
			if !exists {
				group.payload = payload
			}
			group.count++ // bounded by the actual row slice length
			if occurrence > group.max {
				group.max = occurrence
			}
			bags[digest] = group
		}
		entries = append(entries, entry)
	}
	// The physical table digest uses decoded unsigned key order. This is not
	// Ref order, nor the normalized JSON row order of the old table serializer.
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].row.Key, entries[j].row.Key) < 0 })
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].row.Key, entries[i].row.Key) {
			return fail("duplicate canonical row key")
		}
	}
	for _, group := range bags {
		if group.max != group.count-1 {
			return fail("non-dense bag occurrences")
		}
	}
	meta := NativeRowMeta{Format: NativeRowMetaFormat, Source: source, Table: table, History: history, IdentityKind: kind, Columns: columns, PhysicalColumns: physical, RowOrder: NativeRowOrder, RowCount: uint64(len(rows))}
	if kind == NativeRowD3Physical {
		physicalRows := make([]NativeDeltaRow, len(entries))
		for i, entry := range entries {
			physicalRows[i] = entry.physical
		}
		hash, err := NativeDeltaTableSHA256(schema, physicalRows)
		if err != nil {
			return fail("invalid physical table commitment")
		}
		meta.SchemaID, meta.PhysicalTableSHA256 = NativeDeltaTableSchemaID(schema), &hash
	} else {
		meta.SchemaID = nativeV1ColumnsID(table, columns)
	}
	sort.Slice(entries, func(i, j int) bool {
		if c := bytes.Compare(entries[i].normalized, entries[j].normalized); c != 0 {
			return c < 0
		}
		return bytes.Compare(entries[i].row.Key, entries[j].row.Key) < 0
	})
	value := NativeTable{Columns: columns, Rows: make([]Row, len(entries))}
	for i, entry := range entries {
		value.Rows[i] = entry.row.Fields
	}
	body, err := NativeTableBodyBytes(source, table, history, value)
	if err != nil {
		return NativeRowMeta{}, NativeTable{}, err
	}
	hash := sha256.Sum256(body)
	meta.TableBodySHA256 = hex.EncodeToString(hash[:])
	return meta, value, nil
}
