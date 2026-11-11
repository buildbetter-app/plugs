package dumpformat

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeDeltaTableRecords lowers one complete authenticated typed callback
// table, optionally projecting current rows using the bound restore clock. It
// does not confer source authority on caller-authored state. Reservations live
// in this archive's accounting scope until its owner drops the returned set.
func (a *Archive) NativeDeltaTableRecords(state NativeDeltaTableState, history string, keep func(Row) (bool, error)) ([]driver.Record, NativeTable, error) {
	if a == nil || a.delta == nil || state.Complete {
		return nil, NativeTable{}, fmt.Errorf("native delta typed table unavailable")
	}
	owner := a.delta.budget.Scope()
	defer owner.Release()
	scratch := owner.Scope()
	defer scratch.Release()
	r := nativeDeltaReconstructor{header: NativeDeltaHeader{StoreID: a.delta.root.StoreID, Projects: a.delta.root.Projects, ConfigPolicy: state.ConfigPolicy}}
	rows := state.Rows
	normalized := state.Normalized
	if keep != nil {
		if err := scratch.ChargeJSON("projected_typed_rows", rows); err != nil {
			return nil, NativeTable{}, err
		}
		rows = make([]NativeDeltaRow, 0, len(state.Rows))
		normalized = NativeTable{Columns: state.Normalized.Columns, Rows: []Row{}}
		for _, row := range state.Rows {
			candidate := scratch.Scope()
			fields, _, err := validateNativeDeltaRowAccounted(state.Schema, row, state.ConfigPolicy, candidate)
			if err != nil {
				candidate.Release()
				return nil, NativeTable{}, err
			}
			candidate.ReleaseBytes("physical_key_decode", candidate.ReservedBytes("physical_key_decode"))
			retain, err := keep(fields)
			if err == nil && retain {
				rows = append(rows, row)
				normalized.Rows = append(normalized.Rows, fields)
				// These are the same decoded fields, not a second JSON tree.
				candidate.RetainTo(owner)
			}
			candidate.Release()
			if err != nil {
				return nil, NativeTable{}, err
			}
		}
		if err := sortNativeDeltaNormalizedRows(normalized.Rows, scratch); err != nil {
			return nil, NativeTable{}, err
		}
	}
	records := make([]driver.Record, 0, len(rows)+1)
	for _, row := range rows {
		record, err := r.rowRecord(state.Schema, row, history, owner)
		if err != nil {
			return nil, NativeTable{}, err
		}
		records = append(records, record)
	}
	if err := reserveNativeDeltaTableHash(rows, scratch); err != nil {
		return nil, NativeTable{}, err
	}
	hash, err := NativeDeltaTableSHA256(state.Schema, rows)
	if err != nil {
		return nil, NativeTable{}, err
	}
	table := deltaStagedTable{schema: state.Schema, digest: NativeDeltaTableDigest{Rows: uint64(len(rows)), SHA256: hash}}
	meta, err := r.rowMeta(table, normalized, history, owner)
	if err != nil {
		return nil, NativeTable{}, err
	}
	records = append(records, meta)
	if err := sortNativeDeltaRecords(records, scratch); err != nil {
		return nil, NativeTable{}, err
	}
	scratch.Release()
	owner.Retain()
	return records, normalized, nil
}

func reserveNativeDeltaTableHash(rows []NativeDeltaRow, budget *NativeDeltaBudget) error {
	var key, raw, payload uint64
	for _, row := range rows {
		k, n := uint64(base64.StdEncoding.DecodedLen(len(row.Key))), uint64(base64.StdEncoding.DecodedLen(len(row.NormalizedJSON)))
		p := uint64(16) + n // U(cell count), B(normalized)
		for _, cell := range row.Cells {
			if err := deltaAddCount(&p, 16+uint64(len(cell.Kind))+uint64(len(cell.Value))); err != nil {
				return err
			}
		}
		if k > key {
			key = k
		}
		if n > raw {
			raw = n
		}
		if p > payload {
			payload = p
		}
	}
	for _, item := range []struct {
		phase string
		n     uint64
	}{{"table_hash_previous_key", key}, {"table_hash_key", key}, {"table_hash_normalized", raw}, {"table_hash_payload", payload}, {"table_hash_preimage", 16 + key + payload}} {
		if err := budget.ChargeBytes(item.phase, item.n); err != nil {
			return err
		}
	}
	return nil
}

func sortNativeDeltaNormalizedRows(rows []Row, budget *NativeDeltaBudget) error {
	scratch := budget.Scope()
	defer scratch.Release()
	type entry struct {
		fields Row
		raw    []byte
	}
	values := make([]entry, 0, len(rows))
	for _, fields := range rows {
		n, err := NativeDeltaJSONSize(fields)
		if err != nil {
			return err
		}
		if err := scratch.ChargeBytes("normalized_sort_keys", n-1); err != nil {
			return err
		}
		raw, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		values = append(values, entry{fields, raw})
	}
	sort.SliceStable(values, func(i, j int) bool { return bytes.Compare(values[i].raw, values[j].raw) < 0 })
	for i, v := range values {
		rows[i] = v.fields
	}
	return nil
}
