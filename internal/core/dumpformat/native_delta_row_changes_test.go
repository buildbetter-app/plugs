package dumpformat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativeDeltaRecordOrderingCountsGenericKeysAndRefusesBeforeMutation(t *testing.T) {
	input := []driver.Record{{Ref: model.RefByID("native/b", model.Natural64(2))}, {Ref: model.RefByID("native/a", model.Natural64(10))}, {Ref: model.RefByID("native/a", model.Natural64(2))}}
	expected := append([]driver.Record{}, input...)
	oracle := func(record driver.Record) string {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	sort.Slice(expected, func(i, j int) bool { return oracle(expected[i]) < oracle(expected[j]) })
	for _, ceiling := range []uint64{0, 1} {
		root := NewNativeDeltaBudget(ceiling)
		actual := append([]driver.Record{}, input...)
		err := sortNativeDeltaRecords(actual, root)
		var refusal *NativeDeltaDecodedLimitError
		if ceiling == 0 {
			if err != nil || !reflect.DeepEqual(actual, expected) || root.Usage().CumulativeCharges["record_order_keys"] != uint64(len(input)) {
				t.Fatal("generic driver ordering changed", actual, err, root.Usage())
			}
		} else if !errors.As(err, &refusal) || !reflect.DeepEqual(actual, input) || refusal.MeasuredBytes <= ceiling || refusal.Overflow {
			t.Fatal("sort allocated/published an unadmitted ordering", actual, err)
		}
		if root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("sort key index outlived sorting", root.Usage())
		}
	}
}

func TestNativeDeltaRowsApplyActualPreviousStateAndOldTableBytes(t *testing.T) {
	f := deltaReconstructionFixture(t)
	budget := NewNativeDeltaBudget(0)
	returned := budget.Scope()
	stored := map[string]driver.Record{}
	counts := []int{}
	callbacks := &nativeDeltaRowCallbacks{commit: func(commit NativeDeltaRowCommit) error {
		counts = append(counts, len(commit.Changes))
		previous := ""
		for _, change := range commit.Changes {
			record := change.After
			if record == nil {
				record = change.Before
			}
			if record == nil {
				t.Fatal("empty change")
			}
			key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
			if err != nil || key <= previous {
				t.Fatal("change order is not strict record-lock order", err)
			}
			previous = key
			old, present := stored[record.Ref.ID]
			if change.Before == nil {
				if present {
					t.Fatal("insert did not require absence")
				}
			} else if !present || old.Kind != change.Before.Kind || !bytes.Equal(old.Body, change.Before.Body) {
				t.Fatal("before differs from actual previous imported state")
			}
			if change.After == nil {
				delete(stored, record.Ref.ID)
			} else {
				stored[record.Ref.ID] = *change.After
			}
		}
		return nil
	}}
	seen := 0
	h, _, err := reconstructNativeDeltaCallbacks(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, returned, func(image NativeHistoryImage, _ []string) error {
		expected := f.expected[seen]
		seen++
		groups := map[string][]driver.Record{}
		for _, record := range stored {
			meta, row, err := DecodeNativeRowRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			name := ""
			if meta != nil {
				name = meta.Table
			} else {
				name = row.Table
			}
			groups[name] = append(groups[name], record)
		}
		if len(groups) != len(expected.Tables) {
			t.Fatal("complete table membership differs")
		}
		source := SourceIdentity{StoreID: f.root.StoreID, Project: f.root.Projects[0]}
		for name, want := range expected.Tables {
			actual, meta, err := ReconstructNativeRowTable(source, name, nativeRetainedHistory, groups[name], nil)
			if err != nil || meta.IdentityKind != NativeRowD3Physical {
				t.Fatal("typed reconstructed table differs", err)
			}
			// Independently spell the predecessor envelope and json.Marshal oracle.
			envelope := func(table NativeTable) []byte {
				data, err := json.Marshal(struct {
					Source  SourceIdentity `json:"source_identity"`
					Table   string         `json:"table"`
					History string         `json:"history"`
					Columns []string       `json:"columns"`
					Rows    []Row          `json:"rows"`
				}{source, name, nativeRetainedHistory, table.Columns, table.Rows})
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			if !bytes.Equal(envelope(actual), envelope(want)) {
				t.Fatal("old ordered envelope bytes differ", image.Sequence, name)
			}
		}
		return nil
	}, callbacks, false)
	if err != nil || len(h.Images) != 1 || seen != 4 || !reflect.DeepEqual(counts, []int{5, 3, 5, 2}) {
		t.Fatal("branch/keyless incremental changes differ", counts, err)
	}
	h = NativeHistory{}
	returned.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("replay reservations leaked")
	}
}

func TestNativeDeltaRowCandidatesRequireTerminalAndCancellation(t *testing.T) {
	f := deltaReconstructionFixture(t)
	raw := f.stream.Bytes()
	terminal := bytes.LastIndex(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'}) + 1
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-terminal", true: "cancelled"}[cancelled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seen := 0
			input := raw[:terminal]
			if cancelled {
				input = raw
			}
			budget := NewNativeDeltaBudget(0)
			h, _, err := reconstructNativeDeltaCallbacks(ctx, bytes.NewReader(input), f.root, budget, nil, &nativeDeltaRowCallbacks{commit: func(NativeDeltaRowCommit) error {
				seen++
				if cancelled {
					cancel()
				}
				return nil
			}}, false)
			if err == nil || seen == 0 || len(h.Images) != 0 || budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("partial row candidates became usable or leaked", err)
			}
			if cancelled && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation identity lost", err)
			}
		})
	}
}

func TestNativeDeltaTypedTableLoweringUsesSameCanonicalRecords(t *testing.T) {
	f := deltaReconstructionFixture(t)
	archive := &Archive{delta: &nativeDeltaAttachment{root: f.root, budget: NewNativeDeltaBudget(0)}}
	callbacks := &nativeDeltaRowCallbacks{tableState: func(state NativeDeltaTableState) error {
		if state.Complete {
			return nil
		}
		scoped, release := archive.NativeDeltaAccountingScope()
		defer release()
		actual, normalized, err := scoped.NativeDeltaTableRecords(state, nativeRetainedHistory, nil)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := NativeV2TableRecords(SourceIdentity{f.root.StoreID, f.root.Projects[0]}, state.Schema, nativeRetainedHistory, state.Rows, state.ConfigPolicy)
		if err != nil || !reflect.DeepEqual(actual, expected) || !reflect.DeepEqual(normalized, state.Normalized) {
			t.Fatal("budgeted typed lowering changed codec bytes", err)
		}
		return nil
	}}
	returned := archive.delta.budget.Scope()
	if _, _, err := reconstructNativeDeltaCallbacks(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, returned, nil, callbacks, false); err != nil {
		t.Fatal(err)
	}
	returned.Release()
	if archive.delta.budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("typed lowering scope leaked")
	}
}

func TestNativeDeltaUnchangedLargeRowsDoNotGrowHistoryBatches(t *testing.T) {
	schema := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	wire := NativeDeltaSchema{Type: "schema", Profile: NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, MainVersion: deltaTestUint(66), Absences: []NativeCommitSchemaAbsence{}}, Tables: []NativeDeltaTableSchema{schema}}
	wire.SchemaID = NativeDeltaSchemaID(wire)
	ids := []string{}
	for _, c := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		ids = append(ids, strings.Repeat(c, 32))
	}
	f := &deltaTestFixture{t: t, root: NativeCaptureSet{Format: NativeCaptureSetFormat, Contract: NativeDeltaFormat, StoreID: "public-store", Projects: []string{"public"}, Head: ids[len(ids)-1]}}
	f.write(NativeDeltaHeader{Type: "header", Format: NativeDeltaFormat, StoreID: f.root.StoreID, Projects: f.root.Projects, Head: f.root.Head, CommitCount: uint64(len(ids)), Ignore: NativeTable{Columns: []string{"ignored", "pattern"}, Rows: []Row{}}, ConfigPolicy: map[string]string{}})
	f.schema(wire)
	row := deltaTestRow(t, schema, "1", strings.Repeat("x", 1<<20), 0)
	for i, id := range ids {
		parents, mode, tableMode, ops := []string{}, "snapshot", "replace", []any{row}
		if i > 0 {
			parents = []string{ids[i-1]}
			mode = "delta"
			tableMode = "delta"
			ops = []any{}
		}
		f.transaction("commit", uint64(i+1), id, wire, parents, mode, []deltaTestTransaction{{schema, tableMode, []NativeDeltaRow{row}, ops}})
	}
	for _, kind := range []string{"head-checkpoint", "current"} {
		f.transaction(kind, 0, f.root.Head, wire, nil, "", []deltaTestTransaction{{schema, "replace", []NativeDeltaRow{row}, []any{row}}})
	}
	f.write(NativeDeltaEnd{Type: "end", Totals: f.totals, HeadRootSHA256: f.lastRoot, CurrentRootSHA256: f.lastRoot})
	f.root.Totals = f.totals
	counts := []int{}
	budget := NewNativeDeltaBudget(0)
	returned := budget.Scope()
	_, proof, err := reconstructNativeDeltaCallbacks(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, returned, nil, &nativeDeltaRowCallbacks{commit: func(c NativeDeltaRowCommit) error { counts = append(counts, len(c.Changes)); return nil }}, false)
	if err != nil || len(counts) != len(ids) || counts[0] != 2 {
		t.Fatal(counts, err)
	}
	for _, count := range counts[1:] {
		if count != 0 {
			t.Fatal("unchanged row or meta was rewritten", counts)
		}
	}
	var cumulative uint64
	for _, n := range proof.Usage.CumulativeCategories {
		cumulative += n
	}
	returned.Release()
	if cumulative <= proof.Usage.PeakAccountedDecodedBytes || budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("live and cumulative accounting were conflated")
	}
}
