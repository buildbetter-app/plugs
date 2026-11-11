package dumpformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeV1TableRecords lowers a borrowed validated image. It retains detached
// row/meta bodies, not another normalized image. Each named reservation precedes
// allocation; the archive's conversion scope owns the result. The pure codec
// remains the independent unit oracle for this budgeted receiving path.
func (a *Archive) NativeV1TableRecords(source SourceIdentity, table, history string, value NativeTable) ([]driver.Record, error) {
	budget := a.decodedBudget()
	if budget == nil {
		return NativeV1TableRecords(source, table, history, value)
	}
	work := budget.Scope()
	defer work.Release()
	output := budget.Scope()
	defer output.Release()
	if err := work.ChargeBytes("v1_raw_content_key", uint64(len("v1-bag\x00")+sha256.Size+8)); err != nil {
		return nil, err
	}
	if err := work.ChargeBytes("v1_encoded_content_key", uint64(base64.StdEncoding.EncodedLen(len("v1-bag\x00")+sha256.Size+8))); err != nil {
		return nil, err
	}
	var maximum uint64
	for _, fields := range value.Rows {
		n, err := NativeDeltaJSONSize(fields)
		if err != nil {
			return nil, err
		}
		if n-1 > maximum {
			maximum = n - 1
		}
	}
	for _, phase := range []string{"v1_validation_previous", "v1_validation_candidate"} {
		if err := work.ChargeBytes(phase, maximum); err != nil {
			return nil, err
		}
	}
	if err := validateNativeTable(table, value); err != nil {
		return nil, err
	}
	if _, err := nativeRowPrefix(source, table, history, NativeRowV1ContentBag); err != nil {
		return nil, err
	}
	// Digest entries point back to the immutable source row. Equal content is
	// adjacent in canonical order, so duplicate occurrence needs one rolling
	// normalized key. Different content with the same digest must still refuse.
	seen := map[[sha256.Size]byte]int{}
	var previous []byte
	var occurrence uint64
	records := make([]driver.Record, 0, len(value.Rows)+1)
	for i, fields := range value.Rows {
		if err := nativeRowJSONValue(fields, 0); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(fields)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(raw)
		if index, exists := seen[digest]; exists {
			if err := work.ChargeBytes("v1_collision_preimage", uint64(len(raw))); err != nil {
				return nil, err
			}
			other, err := json.Marshal(value.Rows[index])
			work.ReleaseBytes("v1_collision_preimage", uint64(len(raw)))
			if err != nil || !bytes.Equal(raw, other) {
				return nil, nativeRowRefusal(table, "v1 normalized digest collision")
			}
		} else {
			if err := work.ChargeBytes("v1_digest_index", sha256.Size); err != nil {
				return nil, err
			}
			seen[digest] = i
		}
		if bytes.Equal(previous, raw) {
			occurrence++ // bounded by the validated input row count
		} else {
			occurrence = 0
		}
		key := nativeV1RowKey(raw, occurrence)
		body := nativeRowBody{Format: NativeRowFormat, Source: source, Table: table, History: history, IdentityKind: NativeRowV1ContentBag, Key: base64.StdEncoding.EncodeToString(key), Fields: fields}
		n, err := NativeDeltaJSONSize(body)
		if err != nil {
			return nil, err
		}
		if err := output.ChargeBytes("v1_row_bodies", n-1); err != nil {
			return nil, err
		}
		if err := reserveNativeRowRef(output, work, source, table, history, NativeRowV1ContentBag, len(key), false); err != nil {
			return nil, err
		}
		record, err := nativeRowMarshal(body, source, table, history, NativeRowV1ContentBag, key, false)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
		previous = raw
	}
	envelope := struct {
		Source  SourceIdentity `json:"source_identity"`
		Table   string         `json:"table"`
		History string         `json:"history"`
		Columns []string       `json:"columns"`
		Rows    []Row          `json:"rows"`
	}{source, table, history, value.Columns, value.Rows}
	n, err := NativeDeltaJSONSize(envelope)
	if err != nil {
		return nil, err
	}
	if err := work.ChargeBytes("v1_table_digest_preimage", n-1); err != nil {
		return nil, err
	}
	old, err := NativeTableBodyBytes(source, table, history, value)
	if err != nil {
		return nil, err
	}
	digest := digest(old)
	old = nil
	work.ReleaseBytes("v1_table_digest_preimage", n-1)
	// The v1 schema commitment is a binary S/S/U/S... preimage, not JSON.
	// NativeMetaRecord validates it a second time after its first result exists.
	columnsSize := deltaSizeCounter{}
	if err := columnsSize.add(uint64(3*8 + len("plug-native-v1-columns/1") + len(table))); err != nil {
		return nil, err
	}
	for _, column := range value.Columns {
		if err := columnsSize.add(uint64(8 + len(column))); err != nil {
			return nil, err
		}
	}
	if err := work.ChargeBytes("v1_schema_id_preimage", columnsSize.n); err != nil {
		return nil, err
	}
	for _, phase := range []string{"v1_table_digest_text", "v1_schema_id_text", "v1_schema_validation_digest"} {
		if err := work.ChargeBytes(phase, sha256.Size*2); err != nil {
			return nil, err
		}
	}
	meta := NativeRowMeta{Format: NativeRowMetaFormat, Source: source, Table: table, History: history, IdentityKind: NativeRowV1ContentBag, Columns: value.Columns, SchemaID: nativeV1ColumnsID(table, value.Columns), RowOrder: NativeRowOrder, RowCount: uint64(len(value.Rows)), TableBodySHA256: digest}
	n, err = NativeDeltaJSONSize(meta)
	if err != nil {
		return nil, err
	}
	if err := output.ChargeBytes("v1_meta_bodies", n-1); err != nil {
		return nil, err
	}
	if err := reserveNativeRowRef(output, work, source, table, history, NativeRowV1ContentBag, 0, true); err != nil {
		return nil, err
	}
	record, err := NativeMetaRecord(meta)
	if err != nil {
		return nil, err
	}
	records = append(records, record)
	sort.Slice(records, func(i, j int) bool { return records[i].Ref.ID < records[j].Ref.ID })
	work.Release()
	output.Retain()
	return records, nil
}

func reserveNativeRowRef(output, scratch *NativeDeltaBudget, source SourceIdentity, table, history, kind string, keyBytes int, meta bool) error {
	prefix, err := nativeRowPrefix(source, table, history, kind)
	if err != nil {
		return err
	}
	part, domain := "row/", NativeRowFormat
	if meta {
		part, domain = "meta/", NativeRowMetaFormat
	}
	if err := output.ChargeBytes("lowered_record_refs", uint64(len(prefix)+len(part)+sha256.Size*2)); err != nil {
		return err
	}
	n := uint64(5*8 + len(domain) + len(source.StoreID) + len(source.Project) + len(table) + len(kind))
	if !meta {
		n += uint64(8 + keyBytes)
	}
	// Ref generation is sequential. Reuse one reservation for the longest
	// preimage encountered in this scope, instead of summing discarded keys.
	if n > scratch.categories["record_ref_preimage"] {
		old := scratch.categories["record_ref_preimage"]
		if err := scratch.ChargeBytes("record_ref_preimage", n-old); err != nil {
			return err
		}
	}
	return nil
}
