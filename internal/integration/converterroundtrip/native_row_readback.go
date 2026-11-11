package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeRowGroup struct {
	Source  dumpformat.SourceIdentity
	Table   string
	History string
}

// queryNativeRowGroups deliberately has no Kind filter. Every selected native
// namespace record is mandatory input, including malformed/wrong-Kind extras.
func queryNativeRowGroups(ctx context.Context, reader driver.Reader, incarnation string, at model.Revision, archive *dumpformat.Archive) (map[nativeRowGroup][]driver.Record, error) {
	output, releaseOutput := archive.NativeDeltaAccountingScope()
	defer releaseOutput()
	outputBudget := dumpformat.NativeDeltaBudgetFromContext(output.WithNativeDeltaAccounting(context.Background()))
	bookkeeping, releaseBookkeeping := output.NativeDeltaAccountingScope()
	defer releaseBookkeeping()
	groups := map[nativeRowGroup][]driver.Record{}
	seen, cursors := map[string]bool{}, map[string]bool{}
	var cursor *driver.Cursor
	var cursorBytes uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := reader.Query(ctx, driver.Query{Cursor: cursor, Limit: 256, AsOf: &at})
		if err != nil {
			return nil, err
		}
		if cursorBytes != 0 {
			bookkeeping.ReleaseNativeDeltaBytes("readback_retained_cursor", cursorBytes)
			cursorBytes = 0
		}
		if page.AsOf.String() != at.String() {
			return nil, fmt.Errorf("native row query snapshot differs")
		}
		err = func() error {
			pageOwner, releasePage := output.NativeDeltaAccountingScope()
			defer releasePage()
			// Query owns the returned allocation. Admit the delivered page before
			// decoding any item; each native candidate then transfers its existing
			// reservation out of the page without double-counting an aliased Body.
			candidates := make([]*dumpformat.Archive, len(page.Items))
			for i, record := range page.Items {
				candidate, release := pageOwner.NativeDeltaAccountingScope()
				defer release()
				if err := candidate.ChargeNativeDeltaJSON("readback_native_records", record); err != nil {
					return err
				}
				candidates[i] = candidate
			}
			for i, record := range page.Items {
				if err := ctx.Err(); err != nil {
					return err
				}
				branch := record.Ref.Branch.String()
				n := uint64(len("0:key:") + len(record.Ref.Key))
				if record.Ref.IsID() {
					n = uint64(len("0:id::") + len(record.Ref.ID) + len(branch))
				}
				if err := bookkeeping.ChargeNativeDeltaBytes("readback_seen_keys", n); err != nil {
					return err
				}
				if err := pageOwner.ChargeNativeDeltaBytes("readback_lock_key_preimage", n-2); err != nil {
					return err
				}
				key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
				if err != nil || seen[key] {
					return fmt.Errorf("native row query repeats/invalidates Ref")
				}
				seen[key] = true
				pageOwner.ReleaseNativeDeltaBytes("readback_lock_key_preimage", n-2)
				if !strings.HasPrefix(record.Ref.ID, driver.NativeTableHistoryRecordPrefix) && !strings.HasPrefix(record.Ref.ID, "source/native-current/") {
					continue
				}
				parseOwner, releaseParse := pageOwner.NativeDeltaAccountingScope()
				meta, row, err := parseOwner.DecodeNativeRowRecord(record)
				if err != nil {
					releaseParse()
					return err
				}
				var group nativeRowGroup
				if meta != nil {
					group = nativeRowGroup{meta.Source, meta.Table, meta.History}
				} else {
					group = nativeRowGroup{row.Source, row.Table, row.History}
				}
				if _, exists := groups[group]; !exists {
					if err := output.ChargeNativeDeltaJSON("readback_group_keys", group); err != nil {
						releaseParse()
						return err
					}
				}
				groups[group] = append(groups[group], record)
				releaseParse()
				if outputBudget != nil {
					candidateBudget := dumpformat.NativeDeltaBudgetFromContext(candidates[i].WithNativeDeltaAccounting(context.Background()))
					candidateBudget.RetainTo(outputBudget)
				}
			}
			if page.NextCursor == nil {
				return nil
			}
			if len(page.Items) == 0 || page.NextCursor.Incarnation != incarnation || page.NextCursor.Revision.String() != at.String() || page.NextCursor.Method != "query" {
				return fmt.Errorf("native row query cursor binding differs")
			}
			encoded, err := marshalNativePayload(pageOwner, "readback_cursor_encoding", page.NextCursor)
			if err != nil {
				return err
			}
			if err := bookkeeping.ChargeNativeDeltaBytes("readback_cursor_keys", uint64(len(encoded))); err != nil {
				return err
			}
			if cursors[string(encoded)] {
				return fmt.Errorf("native row query cursor repeats")
			}
			cursors[string(encoded)] = true
			n, err := dumpformat.NativeDeltaJSONSize(page.NextCursor)
			if err != nil {
				return err
			}
			if err := bookkeeping.ChargeNativeDeltaBytes("readback_retained_cursor", n); err != nil {
				return err
			}
			cursorBytes = n
			cursor = page.NextCursor
			return nil
		}()
		if err != nil {
			return nil, err
		}
		if page.NextCursor == nil {
			releaseBookkeeping()
			if outputBudget != nil {
				outputBudget.Retain()
			}
			return groups, nil
		}
	}
}

func verifyNativeRowGroup(group nativeRowGroup, got, expected []driver.Record, table dumpformat.NativeTable, policy map[string]string, accounting ...*dumpformat.Archive) error {
	var archive *dumpformat.Archive
	if len(accounting) != 0 {
		archive = accounting[0]
	}
	work, release := archive.NativeDeltaAccountingScope()
	defer release()

	if len(got) != len(expected) {
		return fmt.Errorf("native row/meta census differs for %s", group.Table)
	}
	byRef := make(map[string]driver.Record, len(expected))
	for _, record := range expected {
		if _, exists := byRef[record.Ref.ID]; exists {
			return fmt.Errorf("native source repeats expected Ref")
		}
		byRef[record.Ref.ID] = record
	}
	for _, actual := range got {
		want, exists := byRef[actual.Ref.ID]
		if !exists || actual.Kind != want.Kind || !bytes.Equal(actual.Body, want.Body) {
			return fmt.Errorf("native row source-derived Ref/body differs for %s", group.Table)
		}
		delete(byRef, actual.Ref.ID)
	}
	// Exact source-derived Ref/body membership above already proves the typed
	// keys, dense duplicates and commitments. Reconstruct only the actual
	// normalized payload here: no second whole typed-table materialization.
	type normalizedEntry struct {
		row      dumpformat.Row
		raw, key []byte
	}
	entries := make([]normalizedEntry, 0, len(table.Rows))
	var columns []string
	metas := 0
	for _, record := range got {
		meta, row, err := work.DecodeNativeRowRecord(record)
		if err != nil {
			return err
		}
		if meta != nil {
			if meta.Source != group.Source || meta.Table != group.Table || meta.History != group.History {
				return fmt.Errorf("native metadata group differs")
			}
			columns, metas = meta.Columns, metas+1
			continue
		}
		if row.Source != group.Source || row.Table != group.Table || row.History != group.History {
			return fmt.Errorf("native row group differs")
		}
		n, err := dumpformat.NativeDeltaJSONSize(row.Fields)
		if err != nil {
			return err
		}
		if err := work.ChargeNativeDeltaBytes("readback_normalized_sort_keys", n-1); err != nil {
			return err
		}
		raw, err := json.Marshal(row.Fields)
		if err != nil {
			return err
		}
		entries = append(entries, normalizedEntry{row.Fields, raw, row.Key})
	}
	if metas != 1 || len(entries) != len(table.Rows) {
		return fmt.Errorf("native reconstructed table census differs")
	}
	sort.Slice(entries, func(i, j int) bool {
		if order := bytes.Compare(entries[i].raw, entries[j].raw); order != 0 {
			return order < 0
		}
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	value := dumpformat.NativeTable{Columns: columns, Rows: make([]dumpformat.Row, len(entries))}
	for i, entry := range entries {
		value.Rows[i] = entry.row
	}
	actualEnvelope := struct {
		Source  dumpformat.SourceIdentity `json:"source_identity"`
		Table   string                    `json:"table"`
		History string                    `json:"history"`
		Columns []string                  `json:"columns"`
		Rows    []dumpformat.Row          `json:"rows"`
	}{group.Source, group.Table, group.History, value.Columns, value.Rows}
	for _, item := range []struct {
		phase string
		value any
	}{
		{"readback_actual_table_bytes", actualEnvelope},
		{"readback_expected_table_bytes", struct {
			Source  dumpformat.SourceIdentity `json:"source_identity"`
			Table   string                    `json:"table"`
			History string                    `json:"history"`
			Columns []string                  `json:"columns"`
			Rows    []dumpformat.Row          `json:"rows"`
		}{group.Source, group.Table, group.History, table.Columns, table.Rows}},
	} {
		n, err := dumpformat.NativeDeltaJSONSize(item.value)
		if err != nil {
			return err
		}
		if err := work.ChargeNativeDeltaBytes(item.phase, n-1); err != nil {
			return err
		}
	}
	actual, err := dumpformat.NativeTableBodyBytes(group.Source, group.Table, group.History, value)
	if err != nil {
		return err
	}
	want, err := dumpformat.NativeTableBodyBytes(group.Source, group.Table, group.History, table)
	if err != nil || !bytes.Equal(actual, want) {
		return fmt.Errorf("native reconstructed table bytes differ for %s", group.Table)
	}
	return nil
}

// VerifyNativeCaptureSetReadback compares every mapped immutable state and the
// final HEAD/current attachment to independent authenticated typed source rows.
// It exhausts one fixed unfiltered snapshot before selecting native namespaces.
func VerifyNativeCaptureSetReadback(ctx context.Context, store driver.Store, archive *dumpformat.Archive, report Report) error {
	if !archive.HasNativeDelta() {
		return driver.ErrUnsupported
	}
	archive, release := archive.NativeDeltaAccountingScope()
	defer release()
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return err
	}
	if len(metadata) != 1 || len(metadata[0].Projects) != 1 {
		return driver.ErrUnsupported
	}
	identity := dumpformat.SourceIdentity{StoreID: metadata[0].StoreID, Project: metadata[0].Projects[0]}
	finalAt := report.Restored.Head
	reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &finalAt})
	if err != nil {
		return err
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID(importBindingRecordID, model.Natural64(0))})
	closeErr := reader.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	var evidence driver.HistoryImportEvidence
	var binding importBindingEvidence
	for _, record := range rows {
		if record.Ref.ID == driver.OfflineHistoryEvidenceID {
			if err := json.Unmarshal(record.Body, &evidence); err != nil {
				return err
			}
		}
		if record.Ref.ID == importBindingRecordID {
			if err := json.Unmarshal(record.Body, &binding); err != nil {
				return err
			}
		}
	}
	if evidence.Source != driver.NativeTableHistorySource || len(evidence.Mapping) != metadata[0].ImageCount || binding.Profile.ArchiveDigest != archive.SHA256() {
		return fmt.Errorf("native row source mapping/binding differs")
	}
	when, err := time.Parse(time.RFC3339Nano, binding.Profile.HistoryTime)
	if err != nil {
		return err
	}
	mapping := map[uint64]driver.SourceImportRevision{}
	for _, entry := range evidence.Mapping {
		if entry.SourceStoreID != identity.StoreID || entry.Project != identity.Project || mapping[entry.SourceSequence].SourceSequence != 0 {
			return fmt.Errorf("native row mapping source/repetition differs")
		}
		mapping[entry.SourceSequence] = entry
	}
	var currentClasses map[string]string
	err = archive.VisitNativeSourceHistories(ctx, func(scope dumpformat.NativeHistory) error {
		if currentClasses != nil {
			return driver.ErrUnsupported
		}
		var err error
		currentClasses, _, err = nativeCurrentClasses(scope)
		return err
	})
	if err != nil {
		return err
	}
	var groups map[nativeRowGroup][]driver.Record
	var snapshot *dumpformat.Archive
	var releaseSnapshot func()
	defer func() {
		if releaseSnapshot != nil {
			releaseSnapshot()
		}
	}()
	start := func(at model.Revision) error {
		snapshot, releaseSnapshot = archive.NativeDeltaAccountingScope()
		r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
		if err != nil {
			return err
		}
		groups, err = queryNativeRowGroups(ctx, r, report.Restored.Incarnation, at, snapshot)
		closeErr := r.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	finish := func() error {
		if len(groups) != 0 {
			return fmt.Errorf("native row snapshot has extra/orphan table groups")
		}
		releaseSnapshot()
		releaseSnapshot = nil
		groups = nil
		return nil
	}
	finalStarted := false
	err = archive.VisitNativeDeltaTableStates(ctx, func(state dumpformat.NativeDeltaTableState) error {
		if state.Kind == "commit" && groups == nil {
			entry, exists := mapping[state.Sequence]
			if !exists || len(entry.EventIDs) != 0 || len(entry.CommitIDs) != 1 || entry.CommitIDs[0] != state.CommitID {
				return fmt.Errorf("native row exact commit mapping differs")
			}
			if err := start(entry.Revision); err != nil {
				return err
			}
		} else if state.Kind != "commit" && !finalStarted {
			if err := start(finalAt); err != nil {
				return err
			}
			finalStarted = true
		}
		if state.Complete {
			if state.Kind == "head-checkpoint" {
				return nil
			}
			return finish()
		}
		history := "retained-native-commit"
		var keep func(dumpformat.Row) (bool, error)
		if state.Kind == "current" {
			history = currentClasses[state.Schema.Name]
			if history == "" {
				return fmt.Errorf("native current source family missing disposition")
			}
			if state.Schema.Name == "leases" {
				keep = func(fields dumpformat.Row) (bool, error) { return nativeLeaseUnexpired(fields, when) }
			}
		}
		tableScope, release := snapshot.NativeDeltaAccountingScope()
		defer release()
		expected, table, err := tableScope.NativeDeltaTableRecords(state, history, keep)
		if err != nil {
			return err
		}
		group := nativeRowGroup{identity, state.Schema.Name, history}
		if err := verifyNativeRowGroup(group, groups[group], expected, table, state.ConfigPolicy, tableScope); err != nil {
			return err
		}
		actual := groups[group]
		delete(groups, group)
		for _, record := range actual {
			n, err := dumpformat.NativeDeltaJSONSize(record)
			if err != nil {
				return err
			}
			snapshot.ReleaseNativeDeltaBytes("readback_native_records", n)
		}
		return nil
	})
	return err
}
