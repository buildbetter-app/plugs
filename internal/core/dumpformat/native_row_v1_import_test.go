package dumpformat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestNativeV1BudgetedLoweringMatchesIndependentPureCodec(t *testing.T) {
	one := rowTestFields(t, `{"id":9007199254740993,"value":"<>&\u2028"}`)
	other := rowTestFields(t, `{"id":-0,"value":null}`)
	for _, table := range []NativeTable{
		{Columns: []string{"id", "value"}, Rows: []Row{}},
		rowTestTable(t, one, one, other),
	} {
		root := NewNativeDeltaBudget(0)
		scope := root.Scope()
		archive := &Archive{nativeBudget: scope}
		got, err := archive.NativeV1TableRecords(rowTestSource, "issues", nativeRetainedHistory, table)
		if err != nil {
			t.Fatal(err)
		}
		want, err := NativeV1TableRecords(rowTestSource, "issues", nativeRetainedHistory, table)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("budgeted v1 changed Ref/body or duplicates", err)
		}
		rowTestOracle(t, rowTestSource, "issues", nativeRetainedHistory, got, table)
		if root.Usage().CurrentCategories["v1_table_digest_preimage"] != 0 || root.Usage().CurrentCategories["v1_validation_previous"] != 0 || root.Usage().CurrentCategories["v1_meta_bodies"] == 0 {
			t.Fatal("scratch or output owner lost", root.Usage())
		}
		scope.Release()
		if root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("v1 output was not released")
		}
	}
}

func TestNativeV1BudgetRefusalRollsBackEveryCandidate(t *testing.T) {
	table := rowTestTable(t, rowTestFields(t, `{"id":1,"value":"a"}`), rowTestFields(t, `{"id":1,"value":"a"}`))
	measured := NewNativeDeltaBudget(0)
	candidate := measured.Scope()
	_, err := (&Archive{nativeBudget: candidate}).NativeV1TableRecords(rowTestSource, "issues", nativeRetainedHistory, table)
	if err != nil {
		t.Fatal(err)
	}
	peak := measured.Usage().PeakAccountedDecodedBytes
	candidate.Release()
	// Each boundary is a distinct refusal control; no aggregate can escape
	// either an early scratch failure or a late output-body reservation.
	for _, ceiling := range []uint64{1, 80, peak / 2, peak - 1} {
		root := NewNativeDeltaBudget(ceiling)
		owner := root.Scope()
		records, err := (&Archive{nativeBudget: owner}).NativeV1TableRecords(rowTestSource, "issues", nativeRetainedHistory, table)
		var refusal *NativeDeltaDecodedLimitError
		if !errors.As(err, &refusal) || records != nil || refusal.MeasuredBytes <= ceiling || root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatalf("candidate retained at %d: %#v %+v", ceiling, err, root.Usage())
		}
		owner.Release()
	}
}

func TestNativeSourceVisitorBorrowsOrderedViewsAndCancels(t *testing.T) {
	row := Row{"id": json.Number("1")}
	root := NewNativeDeltaBudget(0)
	archive := &Archive{nativeBudget: root, nativeHistories: []NativeHistory{
		{StoreID: "z", Projects: []string{"b"}},
		{StoreID: "a", Projects: []string{"a"}, Images: []NativeHistoryImage{{Tables: map[string]NativeTable{"issues": {Columns: []string{"id"}, Rows: []Row{row}}}}}},
	}}
	var order []string
	if err := archive.VisitNativeSourceHistories(context.Background(), func(scope NativeHistory) error {
		order = append(order, scope.StoreID)
		if scope.StoreID == "a" && reflect.ValueOf(scope.Images[0].Tables["issues"].Rows[0]).Pointer() != reflect.ValueOf(row).Pointer() {
			t.Fatal("visitor cloned full row")
		}
		return nil
	}); err != nil || !reflect.DeepEqual(order, []string{"a", "z"}) {
		t.Fatal(order, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	visited := false
	if err := archive.VisitNativeSourceHistories(ctx, func(NativeHistory) error { visited = true; return nil }); !errors.Is(err, context.Canceled) || visited {
		t.Fatal("canceled visitor exposed state", err)
	}
	before, _ := json.Marshal(row)
	after, _ := json.Marshal(archive.nativeHistories[1].Images[0].Tables["issues"].Rows[0])
	if !bytes.Equal(before, after) || root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("visitor changed/retained source")
	}
}
