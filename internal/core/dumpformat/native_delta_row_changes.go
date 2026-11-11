package dumpformat

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeDeltaRowCommit is an ephemeral authenticated commit candidate. Changes
// own detached canonical bodies. A callback may retain them only after reserving
// its retained batch bytes; the entire result remains private until verified EOF.
type NativeDeltaRowCommit struct {
	Sequence uint64
	CommitID string
	Changes  []driver.SourceHistoryChange
}

type nativeDeltaRowCallbacks struct {
	commit func(NativeDeltaRowCommit) error
	// Each complete checkpoint table is presented once in name order. Schema,
	// rows and policy are borrowed until the callback returns. No SQL is run.
	checkpoint      func(string, NativeDeltaTableSchema, []NativeDeltaRow, map[string]string) error
	tableState      func(NativeDeltaTableState) error
	checkpointsOnly bool
}

// NativeDeltaTableState presents one authenticated table for independent
// readback. Complete terminates a state and has no table payload. Values are
// borrowed until callback return; no source effect is public before final EOF.
type NativeDeltaTableState struct {
	Kind         string
	Sequence     uint64
	CommitID     string
	Complete     bool
	Schema       NativeDeltaTableSchema
	Rows         []NativeDeltaRow
	Normalized   NativeTable
	ConfigPolicy map[string]string
}

func nativeDeltaSameRow(left, right NativeDeltaRow) bool {
	if left.Key != right.Key || left.NormalizedJSON != right.NormalizedJSON || len(left.Cells) != len(right.Cells) {
		return false
	}
	for i, cell := range left.Cells {
		if cell != right.Cells[i] {
			return false
		}
	}
	return true
}

func (r *nativeDeltaReconstructor) rowRecord(schema NativeDeltaTableSchema, row NativeDeltaRow, history string, work *NativeDeltaBudget) (driver.Record, error) {
	scratch := work.Scope()
	defer scratch.Release()
	fields, key, err := validateNativeDeltaRowAccounted(schema, row, r.header.ConfigPolicy, scratch)
	if err != nil {
		return driver.Record{}, err
	}
	source := SourceIdentity{StoreID: r.header.StoreID, Project: r.header.Projects[0]}
	body := nativeRowBody{Format: NativeRowFormat, Source: source, Table: schema.Name, History: history, IdentityKind: NativeRowD3Physical, Key: row.Key, Fields: fields, Cells: row.Cells}
	n, err := NativeDeltaJSONSize(body)
	if err != nil {
		return driver.Record{}, err
	}
	if err := work.ChargeBytes("row_record_candidates", n-1); err != nil {
		return driver.Record{}, err
	}
	if err := reserveNativeRowRef(work, scratch, source, schema.Name, history, NativeRowD3Physical, len(key), false); err != nil {
		return driver.Record{}, err
	}
	return nativeRowMarshal(body, source, schema.Name, history, NativeRowD3Physical, key, false)
}

func (r *nativeDeltaReconstructor) rowMeta(table deltaStagedTable, normalized NativeTable, history string, work *NativeDeltaBudget) (driver.Record, error) {
	source := SourceIdentity{StoreID: r.header.StoreID, Project: r.header.Projects[0]}
	envelope := struct {
		Source  SourceIdentity `json:"source_identity"`
		Table   string         `json:"table"`
		History string         `json:"history"`
		Columns []string       `json:"columns"`
		Rows    []Row          `json:"rows"`
	}{source, table.schema.Name, history, normalized.Columns, normalized.Rows}
	n, err := NativeDeltaJSONSize(envelope)
	if err != nil {
		return driver.Record{}, err
	}
	scratch := work.Scope()
	if err := scratch.ChargeBytes("table_body_digest_scratch", n-1); err != nil {
		scratch.Release()
		return driver.Record{}, err
	}
	body, err := NativeTableBodyBytes(source, table.schema.Name, history, normalized)
	if err != nil {
		scratch.Release()
		return driver.Record{}, err
	}
	bodyDigest := digest(body)
	body = nil
	scratch.Release()
	physical := table.digest.SHA256
	meta := NativeRowMeta{Format: NativeRowMetaFormat, Source: source, Table: table.schema.Name, History: history, IdentityKind: NativeRowD3Physical, Columns: normalized.Columns, SchemaID: NativeDeltaTableSchemaID(table.schema), PhysicalColumns: table.schema.Columns, RowOrder: NativeRowOrder, RowCount: table.digest.Rows, TableBodySHA256: bodyDigest, PhysicalTableSHA256: &physical}
	n, err = NativeDeltaJSONSize(meta)
	if err != nil {
		return driver.Record{}, err
	}
	if err := work.ChargeBytes("meta_record_candidates", n-1); err != nil {
		return driver.Record{}, err
	}
	keyScratch := work.Scope()
	defer keyScratch.Release()
	if err := reserveNativeRowRef(work, keyScratch, source, table.schema.Name, history, NativeRowD3Physical, 0, true); err != nil {
		return driver.Record{}, err
	}
	return NativeMetaRecord(meta)
}

func nativeDeltaOrderChanges(changes []driver.SourceHistoryChange, budget *NativeDeltaBudget) error {
	scratch := budget.Scope()
	defer scratch.Release()
	keys := make(map[string]string, len(changes))
	for _, change := range changes {
		record := change.After
		if record == nil {
			record = change.Before
		}
		if record == nil {
			return fmt.Errorf("native delta empty row change")
		}
		key, err := nativeDeltaRecordKeyAccounted(record.Ref, scratch)
		if err != nil {
			return err
		}
		if _, exists := keys[record.Ref.ID]; exists {
			return fmt.Errorf("native delta repeated row change Ref")
		}
		keys[record.Ref.ID] = key
	}
	sort.Slice(changes, func(i, j int) bool {
		ref := func(c driver.SourceHistoryChange) string {
			if c.After != nil {
				return c.After.Ref.ID
			}
			return c.Before.Ref.ID
		}
		return keys[ref(changes[i])] < keys[ref(changes[j])]
	})
	return nil
}

func (r *nativeDeltaReconstructor) emitRowChanges(state deltaStagedState, image NativeHistoryImage, touched []string) error {
	if r.rowCallbacks == nil || r.rowCallbacks.commit == nil {
		return nil
	}
	if len(r.header.Projects) != 1 {
		return fmt.Errorf("native row changes require one source project")
	}
	work := r.budget.Scope()
	defer work.Release()
	changes := []driver.SourceHistoryChange{}
	for _, name := range touched {
		err := func() error {
			tableWork := work.Scope()
			defer tableWork.Release()
			before, existed := r.previousTables[name]
			after, present := state.tables[name]
			left, err := r.readRows(before, tableWork)
			if err != nil {
				return err
			}
			right, err := r.readRows(after, tableWork)
			if err != nil {
				return err
			}
			keys := map[string]bool{}
			for key := range left {
				keys[key] = true
			}
			for key := range right {
				keys[key] = true
			}
			for key := range keys {
				old, hadOld := left[key]
				next, hasNext := right[key]
				if hadOld && hasNext && nativeDeltaSameRow(old.row, next.row) {
					continue
				}
				if hadOld && hasNext {
					pk, err := NativeDeltaPrimaryKey(after.schema, next.row.Cells)
					if err != nil {
						return err
					}
					if pk == nil {
						return fmt.Errorf("native delta keyless payload digest collision across states")
					}
				}
				change := driver.SourceHistoryChange{}
				if hadOld {
					record, err := r.rowRecord(before.schema, old.row, nativeRetainedHistory, work)
					if err != nil {
						return err
					}
					change.Before = &record
				}
				if hasNext {
					record, err := r.rowRecord(after.schema, next.row, nativeRetainedHistory, work)
					if err != nil {
						return err
					}
					change.After = &record
				}
				changes = append(changes, change)
			}
			oldMeta, hadMeta := r.rowMetadata[name]
			if hadMeta != existed {
				return fmt.Errorf("native delta rolling metadata census differs")
			}
			change := driver.SourceHistoryChange{}
			if hadMeta {
				change.Before = &oldMeta
			}
			if present {
				record, err := r.rowMeta(after, image.Tables[name], nativeRetainedHistory, work)
				if err != nil {
					return err
				}
				if hadMeta && bytes.Equal(oldMeta.Body, record.Body) {
					return nil
				}
				size, err := NativeDeltaJSONSize(record)
				if err != nil {
					return err
				}
				if err := r.retainedBudget.ChargeBytes("rolling_row_metadata", size); err != nil {
					return err
				}
				change.After = &record
				r.rowMetadata[name] = record
			} else {
				delete(r.rowMetadata, name)
			}
			if hadMeta {
				if err := work.ChargeJSON("meta_before_candidates", oldMeta); err != nil {
					return err
				}
				size, err := NativeDeltaJSONSize(oldMeta)
				if err != nil {
					return err
				}
				r.retainedBudget.ReleaseBytes("rolling_row_metadata", size)
			}
			changes = append(changes, change)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	if err := nativeDeltaOrderChanges(changes, work); err != nil {
		return err
	}
	return r.rowCallbacks.commit(NativeDeltaRowCommit{Sequence: image.Sequence, CommitID: image.CommitID, Changes: changes})
}

func (r *nativeDeltaReconstructor) emitRowCheckpoint(kind string, state deltaStagedState, image NativeHistoryImage) error {
	if r.rowCallbacks == nil || r.rowCallbacks.checkpoint == nil && r.rowCallbacks.tableState == nil {
		return nil
	}
	if kind == "commit" && r.rowCallbacks.checkpointsOnly {
		return nil
	}
	names := make([]string, 0, len(state.tables))
	for name := range state.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		work := r.budget.Scope()
		table := state.tables[name]
		rows, err := r.readRows(table, work)
		if err == nil {
			keys := make([]string, 0, len(rows))
			for key := range rows {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			ordered := make([]NativeDeltaRow, 0, len(rows))
			for _, key := range keys {
				row := rows[key].row
				if err = work.ChargeJSON("callback_typed_rows", row); err != nil {
					break
				}
				row.Cells = append([]NativeDeltaCell{}, row.Cells...)
				ordered = append(ordered, row)
			}
			if err != nil {
				work.Release()
				return err
			}
			if err = work.ChargeJSON("callback_schema", table.schema); err != nil {
				work.Release()
				return err
			}
			if err = work.ChargeJSON("callback_policy", r.header.ConfigPolicy); err != nil {
				work.Release()
				return err
			}
			schema := table.schema
			schema.Columns = append([]NativeDeltaColumn{}, schema.Columns...)
			for i := range schema.Columns {
				schema.Columns[i].PrimaryKeyOrdinal = cloneNativeDeltaCursor(schema.Columns[i].PrimaryKeyOrdinal)
			}
			policy := map[string]string{}
			for k, v := range r.header.ConfigPolicy {
				policy[k] = v
			}
			if kind != "commit" && r.rowCallbacks.checkpoint != nil {
				err = r.rowCallbacks.checkpoint(kind, schema, ordered, policy)
			}
			if err == nil && r.rowCallbacks.tableState != nil {
				if err = work.ChargeJSON("callback_normalized_table", image.Tables[name]); err == nil {
					detached := cloneNativeDeltaTable(image.Tables[name])
					err = r.rowCallbacks.tableState(NativeDeltaTableState{Kind: kind, Sequence: image.Sequence, CommitID: image.CommitID, Schema: schema, Rows: ordered, Normalized: detached, ConfigPolicy: policy})
				}
			}
		}
		work.Release()
		if err != nil {
			return err
		}
	}
	if r.rowCallbacks.tableState != nil {
		return r.rowCallbacks.tableState(NativeDeltaTableState{Kind: kind, Sequence: image.Sequence, CommitID: image.CommitID, Complete: true})
	}
	return nil
}

// VisitNativeDeltaRowChanges reauthenticates the complete original capture.
// Row operations are compared to the preceding imported state, including branch
// switches. It never stores full per-commit normalized images or row record maps.
// Checkpoint arguments are detached; callbacks cannot alter source authority.
// Candidate effects must remain private until terminal verification returns nil.
func (a *Archive) VisitNativeDeltaRowChanges(ctx context.Context, commit func(NativeDeltaRowCommit) error, checkpoint func(string, NativeDeltaTableSchema, []NativeDeltaRow, map[string]string) error) error {
	if commit == nil && checkpoint == nil {
		return fmt.Errorf("native delta row callbacks unavailable")
	}
	return a.visitNativeDelta(ctx, nil, &nativeDeltaRowCallbacks{commit: commit, checkpoint: checkpoint})
}

// VisitNativeDeltaTableStates supplies complete typed source table sets for
// independent expectations. It never derives expected membership from stored
// receiver records or the receiver's emitted change list.
func (a *Archive) VisitNativeDeltaTableStates(ctx context.Context, visit func(NativeDeltaTableState) error) error {
	if visit == nil {
		return fmt.Errorf("native delta table state visitor unavailable")
	}
	return a.visitNativeDelta(ctx, nil, &nativeDeltaRowCallbacks{tableState: visit})
}
