package dumpformat

import (
	"encoding/base64"
	"fmt"
	"io"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// validateNativeDeltaRowAccounted inventories the logical copies made by the
// existing typed-cell validator. Each reservation names a live representation;
// it is not an RSS multiplier. The returned fields and decoded key belong to
// owner; validation scratch ends when validateNativeDeltaRow returns.
func validateNativeDeltaRowAccounted(table NativeDeltaTableSchema, row NativeDeltaRow, policy map[string]string, owner *NativeDeltaBudget) (Row, []byte, error) {
	raw := uint64(base64.StdEncoding.DecodedLen(len(row.NormalizedJSON)))
	work := owner.Scope()
	defer work.Release()
	if err := work.ChargeBytes("normalized_admission_decode", raw); err != nil {
		return nil, nil, err
	}
	normalized, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
	if err != nil {
		return nil, nil, err
	}
	counter := deltaSizeCounter{}
	if err := counter.raw(normalized); err != nil {
		return nil, nil, err
	}
	canonical := counter.n + 1
	if err := work.ChargeBytes("normalized_raw", raw); err != nil {
		return nil, nil, err
	}
	for _, phase := range []string{"normalized_validation_input", "normalized_shape_input", "normalized_typed_input"} {
		if err := work.ChargeBytes(phase, raw); err != nil {
			return nil, nil, err
		}
	}
	for _, phase := range []string{"normalized_validation_tokens", "normalized_shape", "normalized_canonical"} {
		if err := work.ChargeBytes(phase, canonical); err != nil {
			return nil, nil, err
		}
	}
	if err := reserveNativeDeltaKey(row.Key, owner, work); err != nil {
		return nil, nil, err
	}
	if err := work.ChargeBytes("physical_key_preimage", uint64(base64.StdEncoding.DecodedLen(len(row.Key)))); err != nil {
		return nil, nil, err
	}
	// Only one converted cell exists at a time. Canonical normalized-row bytes
	// bound that cell's JSON representation, including escaped text and binary.
	// JSON SQL cells additionally hold their original lexical cell input.
	var cellInput uint64
	for _, cell := range row.Cells {
		if uint64(len(cell.Value)) > cellInput {
			cellInput = uint64(len(cell.Value))
		}
	}
	for _, phase := range []string{"cell_lexical_input", "cell_validation_input", "cell_shape_input", "cell_typed_input"} {
		if err := work.ChargeBytes(phase, cellInput); err != nil {
			return nil, nil, err
		}
	}
	for _, phase := range []string{"cell_validation_tokens", "cell_shape", "cell_value"} {
		if err := work.ChargeBytes(phase, canonical); err != nil {
			return nil, nil, err
		}
	}
	// Reserve the escaping-independent canonical source size before decoding.
	// Canonical normalized JSON is a required validator precondition.
	if err := owner.ChargeBytes("normalized_fields", canonical); err != nil {
		return nil, nil, err
	}
	return validateNativeDeltaRow(table, row, policy)
}

// DecodeString allocates DecodedLen bytes even when padding makes the returned
// slice shorter. Canonical re-encoding separately allocates an encoded buffer
// and its string copy. Only the decoded buffer survives validation.
func reserveNativeDeltaKey(encoded string, owner, scratch *NativeDeltaBudget) error {
	if err := owner.ChargeBytes("physical_key_decode", uint64(base64.StdEncoding.DecodedLen(len(encoded)))); err != nil {
		return err
	}
	for _, phase := range []string{"physical_key_reencode_buffer", "physical_key_reencode_text"} {
		if err := scratch.ChargeBytes(phase, uint64(len(encoded))); err != nil {
			return err
		}
	}
	return nil
}

func nativeDeltaKeyAccounted(encoded string, owner *NativeDeltaBudget) ([]byte, error) {
	candidate := owner.Scope()
	defer candidate.Release()
	scratch := candidate.Scope()
	defer scratch.Release()
	if err := reserveNativeDeltaKey(encoded, candidate, scratch); err != nil {
		return nil, err
	}
	key, err := nativeDeltaBase64(encoded)
	if err != nil {
		return nil, err
	}
	scratch.Release()
	candidate.Retain()
	return key, nil
}

// Map strings have a distinct lifetime and representation from decoded keys.
// The caller releases a replaced/deleted key or transfers a newly kept key.
func nativeDeltaMapKeyAccounted(key []byte, owner *NativeDeltaBudget) (string, error) {
	if err := owner.ChargeBytes("decoded_row_map_keys", uint64(len(key))); err != nil {
		return "", err
	}
	return string(key), nil
}

// The driver defines generic Ref ordering. Admit the returned encoded key
// and its temporary preimage separately; never use D3/body order in its place.
func nativeDeltaRecordKeyAccounted(ref model.Ref, owner *NativeDeltaBudget) (string, error) {
	n := uint64(len("0:key:")) + uint64(len(ref.Key))
	if ref.IsID() {
		n = uint64(len("0:id::")) + uint64(len(ref.ID)) + uint64(len(ref.Branch.String()))
	}
	if err := owner.ChargeBytes("record_order_keys", n); err != nil {
		return "", err
	}
	scratch := owner.Scope()
	defer scratch.Release()
	if err := scratch.ChargeBytes("record_order_key_preimage", n-2); err != nil {
		return "", err
	}
	return driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
}

func sortNativeDeltaRecords(records []driver.Record, budget *NativeDeltaBudget) error {
	scratch := budget.Scope()
	defer scratch.Release()
	type entry struct {
		record driver.Record
		key    string
	}
	entries := make([]entry, 0, len(records))
	for _, record := range records {
		key, err := nativeDeltaRecordKeyAccounted(record.Ref, scratch)
		if err != nil {
			return err
		}
		entries = append(entries, entry{record, key})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	for i, entry := range entries {
		records[i] = entry.record
	}
	return nil
}

// nativeDeltaReadBounded reserves each byte-buffer capacity before allocating.
// During growth both old and new buffers remain admitted until copying ends.
func nativeDeltaReadBounded(input io.Reader, limit uint64, budget *NativeDeltaBudget, phase string) ([]byte, error) {
	var data []byte
	for {
		if len(data) == cap(data) {
			capacity := uint64(cap(data)) * 2
			if capacity < 512 {
				capacity = 512
			}
			if capacity > limit+1 {
				capacity = limit + 1
			}
			if uint64(cap(data)) == capacity {
				return nil, fmt.Errorf("native delta input exceeds %d bytes", limit)
			}
			if err := budget.ChargeBytes(phase, capacity); err != nil {
				return nil, err
			}
			next := make([]byte, len(data), int(capacity))
			copy(next, data)
			if cap(data) != 0 {
				budget.ReleaseBytes(phase, uint64(cap(data)))
			}
			data = next
		}
		n, err := input.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if uint64(len(data)) > limit {
			return nil, fmt.Errorf("native delta input exceeds %d bytes", limit)
		}
		if err == io.EOF {
			return data, nil
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
}
