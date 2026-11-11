package dumpformat

import (
	"encoding/base64"
	"fmt"
	"io"
)

// reserveNativeDeltaRowValidation inventories the logical copies made by the
// existing typed-cell validator. Each reservation names a live representation;
// it is not an RSS multiplier. The returned fields reservation belongs to the
// caller; validation scratch ends when validateNativeDeltaRow returns.
func validateNativeDeltaRowAccounted(table NativeDeltaTableSchema, row NativeDeltaRow, policy map[string]string, owner *NativeDeltaBudget) (Row, []byte, error) {
	raw := uint64(base64.StdEncoding.DecodedLen(len(row.NormalizedJSON)))
	key := uint64(base64.StdEncoding.DecodedLen(len(row.Key)))
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
	for _, phase := range []string{"physical_key_decode", "physical_key_reencode", "physical_key_preimage"} {
		if err := work.ChargeBytes(phase, key); err != nil {
			return nil, nil, err
		}
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
