package dumpformat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestNativeDeltaRetainedEmptyDeltaAuthenticatesOriginalTable(t *testing.T) {
	schema := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	row := deltaTestRow(t, schema, "1", "original", 0)
	hash, err := NativeDeltaTableSHA256(schema, []NativeDeltaRow{row})
	if err != nil {
		t.Fatal(err)
	}
	root := NewNativeDeltaBudget(0)
	owner := root.Scope()
	defer owner.Release()
	cache := &deltaDecodedCache{ctx: context.Background(), budget: owner.Scope()}
	var input bytes.Buffer
	write := func(value any) {
		t.Helper()
		if err := json.NewEncoder(&input).Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	write(row)
	write(NativeDeltaEndTable{Type: "end_table", RowOperations: 1, Rows: 1, SHA256: hash})
	r := nativeDeltaReconstructor{ctx: context.Background(), budget: owner, decoded: cache, input: newNativeDeltaRecordReader(context.Background(), &input, owner)}
	open := NativeDeltaTable{Type: "table", Name: schema.Name, TableSchemaID: NativeDeltaTableSchemaID(schema), Mode: "replace", ExpectedRows: 1, ExpectedSHA256: hash}
	base, table, err := r.table(open, schema, deltaStagedTable{}, true)
	if err != nil || base.decoded == nil || table.Rows[0]["value"] != "original" {
		t.Fatal("verified retained baseline unavailable", err)
	}
	defer cache.releaseTable(base.decoded)
	r.input.Close()
	// An attacker changes both the empty delta's opening and closing digest.
	// No row is available to revalidate: the original verified digest must bind it.
	for _, forged := range []bool{false, true} {
		input.Reset()
		open.Mode = "delta"
		open.ExpectedSHA256 = hash
		if forged {
			open.ExpectedSHA256 = strings.Repeat("0", 64)
		}
		write(NativeDeltaEndTable{Type: "end_table", Rows: 1, SHA256: open.ExpectedSHA256})
		r.input = newNativeDeltaRecordReader(context.Background(), &input, owner)
		actual, normalized, err := r.table(open, schema, base, false)
		r.input.Close()
		if forged {
			if err == nil || actual.decoded != nil {
				t.Fatal("forged empty delta digest admitted")
			}
		} else {
			if err != nil || !reflect.DeepEqual(normalized, table) {
				t.Fatal("unchanged authenticated table differs", err)
			}
			cache.releaseTable(actual.decoded)
		}
	}
}

func TestNativeDeltaRetainedBranchesConserveBudgetAndImageBytes(t *testing.T) {
	f := deltaReconstructionFixture(t)
	var peak uint64
	for _, bounded := range []bool{false, true} {
		limit := uint64(0)
		if bounded {
			limit = peak - 1
		}
		budget := NewNativeDeltaBudget(limit)
		owner := budget.Scope()
		seen := 0
		h, proof, err := reconstructNativeDeltaVisit(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, owner, func(image NativeHistoryImage, _ []string) error {
			actual, err := json.Marshal(image)
			if err != nil {
				return err
			}
			want, err := json.Marshal(f.expected[seen])
			if err != nil {
				return err
			}
			if !bytes.Equal(actual, want) {
				t.Fatal("late first-parent branch changed canonical image bytes", seen)
			}
			seen++
			return nil
		}, false)
		if !bounded {
			if err != nil || seen != 4 || len(h.Images) != 1 || proof.Totals.FullImageBytes != f.totals.FullImageBytes {
				t.Fatal("retained branch reconstruction differs", err)
			}
			peak = budget.Usage().PeakAccountedDecodedBytes
			for _, phase := range []string{"retained_delta_rows", "retained_row_index", "retained_table_views", "retained_row_hash_preimages", "retained_row_ownership", "retained_table_ownership"} {
				if budget.Usage().CumulativeCharges[phase] == 0 {
					t.Fatal("retained representation lacks admission", phase)
				}
			}
		} else {
			var refusal *NativeDeltaDecodedLimitError
			if !errors.As(err, &refusal) || refusal.MeasuredBytes != peak || refusal.Overflow || len(h.Images) != 0 || refusal.Usage.CurrentCategories["retained_delta_rows"] == 0 || refusal.Usage.CurrentCategories["retained_row_index"] == 0 {
				t.Fatal("live shared state was not included in typed admission", peak, err)
			}
		}
		h = NativeHistory{}
		owner.Release()
		if budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("retained branch admission did not unwind", budget.Usage())
		}
	}
}

func TestNativeDeltaRetainedRowsReleaseAtAuthenticatedFrontier(t *testing.T) {
	f := deltaReconstructionFixture(t)
	// Authenticate the actual complete graph, then replay it with the same
	// trusted frontier used by archive visitors. A future branch must survive
	// its sibling, and the immediately previous emitted state remains live.
	firstBudget := NewNativeDeltaBudget(0)
	firstOwner := firstBudget.Scope()
	history, _, err := reconstructNativeDelta(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, firstOwner)
	if err != nil {
		t.Fatal(err)
	}
	budget := NewNativeDeltaBudget(0)
	owner := budget.Scope()
	seen := 0
	_, _, err = reconstructNativeDeltaCallbacks(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, owner, func(image NativeHistoryImage, _ []string) error {
		if !reflect.DeepEqual(image, f.expected[seen]) {
			t.Fatal("shared branch expired before actual last use", seen)
		}
		seen++
		return nil
	}, nil, false, history.Commits)
	if err != nil || seen != 4 {
		t.Fatal("authenticated frontier replay differs", seen, err)
	}
	owner.Release()
	firstOwner.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 || firstBudget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("frontier owners did not unwind")
	}
}

func TestNativeDeltaRetainedCandidatesReleaseOnCancellationAndBadEOF(t *testing.T) {
	f := deltaReconstructionFixture(t)
	for _, cancelAfterCommit := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		budget := NewNativeDeltaBudget(0)
		input := append(bytes.Clone(f.stream.Bytes()), []byte("{}\n")...)
		seen := 0
		h, _, err := reconstructNativeDeltaVisit(ctx, bytes.NewReader(input), f.root, budget, func(NativeHistoryImage, []string) error {
			seen++
			if cancelAfterCommit {
				cancel()
			}
			return nil
		}, false)
		cancel()
		if err == nil || seen == 0 || len(h.Images) != 0 || budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("unauthenticated retained result escaped or leaked", err)
		}
		if cancelAfterCommit && !errors.Is(err, context.Canceled) {
			t.Fatal("retained cancellation identity lost", err)
		}
	}
}

func TestNativeDeltaTableCallbackCannotMutateRetainedFirstParent(t *testing.T) {
	f := deltaReconstructionFixture(t)
	budget := NewNativeDeltaBudget(0)
	owner := budget.Scope()
	seen := 0
	_, _, err := reconstructNativeDeltaCallbacks(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, owner, func(image NativeHistoryImage, _ []string) error {
		if !reflect.DeepEqual(image, f.expected[seen]) {
			t.Fatal("table callback mutated retained source authority", seen)
		}
		seen++
		return nil
	}, &nativeDeltaRowCallbacks{tableState: func(state NativeDeltaTableState) error {
		if state.Complete {
			return nil
		}
		for _, row := range state.Normalized.Rows {
			row["value"] = "callback-only"
		}
		for _, row := range state.Rows {
			for i := range row.Cells {
				row.Cells[i].Value = "callback-only"
			}
		}
		return nil
	}}, false)
	if err != nil || seen != 4 {
		t.Fatal("detached callback changed reconstruction", seen, err)
	}
	owner.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("callback copy retained an owner")
	}
}
