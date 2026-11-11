package dumpformat

import (
	"errors"
	"math"
	"testing"
)

func TestNativeDeltaDecodedBudgetRefusesCandidateBeforeRetention(t *testing.T) {
	b := NewNativeDeltaBudget(10)
	if err := b.ChargeJSON("schema", map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	// Canonical JSON is {"a":1} followed by a newline: eight bytes.
	if err := b.ChargeBytes("head", 2); err != nil {
		t.Fatal(err)
	}
	err := b.ChargeBytes("batch", 1)
	var limit *NativeDeltaDecodedLimitError
	if !errors.As(err, &limit) || limit.MeasuredBytes != 11 || limit.LimitBytes != 10 || limit.Overflow {
		t.Fatalf("wrong typed refusal: %#v", err)
	}
	usage := b.Usage()
	if usage.PeakAccountedDecodedBytes != 10 || usage.Categories["batch"] != 0 {
		t.Fatalf("refused candidate retained: %+v", usage)
	}
	usage.Categories["head"] = 100
	if b.Usage().Categories["head"] != 2 {
		t.Fatal("usage aliases budget")
	}
}

func TestNativeDeltaDecodedBudgetOverflowIsExplicit(t *testing.T) {
	b := NewNativeDeltaBudget(math.MaxUint64)
	if err := b.ChargeBytes("first", math.MaxUint64-1); err != nil {
		t.Fatal(err)
	}
	var limit *NativeDeltaDecodedLimitError
	if err := b.ChargeBytes("next", 2); !errors.As(err, &limit) || !limit.Overflow || limit.MeasuredBytes != math.MaxUint64 {
		t.Fatalf("wrong overflow refusal: %#v", err)
	}
}

func TestNativeDeltaDecodedBudgetReleasesWorkButKeepsSimultaneousPeak(t *testing.T) {
	b := NewNativeDeltaBudget(100)
	if err := b.ChargeBytes("retained_history", 20); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		work := b.Scope()
		rows := work.Scope()
		if err := work.ChargeBytes("rolling_state", 30); err != nil {
			t.Fatal(err)
		}
		if err := rows.ChargeBytes("decoded_rows", 40); err != nil {
			t.Fatal(err)
		}
		rows.Release()
		if err := work.ChargeBytes("offline_batches", 45); err != nil {
			t.Fatal(err)
		}
		work.Release()
	}
	usage := b.Usage()
	if usage.PeakAccountedDecodedBytes != 95 || usage.CurrentAccountedDecodedBytes != 20 || usage.PeakRetainedBatchBytes != 45 || usage.CumulativeCategories["rolling_state"] != 600 || usage.CumulativeCharges["decoded_rows"] != 20 || usage.CumulativeOverflow {
		t.Fatalf("wrong lifetime accounting: %+v", usage)
	}
	var peak, live uint64
	for _, n := range usage.Categories {
		peak += n
	}
	for _, n := range usage.CurrentCategories {
		live += n
	}
	if peak != 95 || live != 20 || usage.Charges["offline_batches"] != 1 {
		t.Fatal("simultaneous snapshots do not reconcile", usage)
	}
	usage.CurrentCategories["retained_history"] = 0
	if b.Usage().CurrentCategories["retained_history"] != 20 {
		t.Fatal("current usage aliases budget")
	}
	work := b.Scope()
	if err := work.ChargeBytes("rolling_state", 70); err != nil {
		t.Fatal(err)
	}
	var refused *NativeDeltaDecodedLimitError
	if err := work.ChargeBytes("candidate", 11); !errors.As(err, &refused) || refused.AdmittedBytes != 90 || refused.MeasuredBytes != 101 || refused.Usage.CurrentAccountedDecodedBytes != 90 || refused.Usage.PeakAccountedDecodedBytes != 95 {
		t.Fatalf("refusal used cumulative or peak bytes: %#v", err)
	}
	work.Release()
	if b.Usage().CurrentAccountedDecodedBytes != 20 {
		t.Fatal("refused candidate leaked a reservation")
	}
}

func TestNativeDeltaDecodedBudgetReplacesRollingReservation(t *testing.T) {
	b := NewNativeDeltaBudget(100)
	work := b.Scope()
	if err := work.ChargeBytes("table", 40); err != nil {
		t.Fatal(err)
	}
	if err := work.ChargeBytes("table", 50); err != nil {
		t.Fatal(err)
	}
	work.ReleaseBytes("table", 40)
	if b.Usage().CurrentAccountedDecodedBytes != 50 || b.Usage().PeakAccountedDecodedBytes != 90 {
		t.Fatal("rolling replacement lost simultaneous peak")
	}
	work.Release()
	work.Release()
	if b.Usage().CurrentAccountedDecodedBytes != 0 || b.Usage().CumulativeCategories["table"] != 90 {
		t.Fatal("release was not idempotent or erased diagnostics")
	}
}

func TestNativeDeltaJSONCandidateRefusalLeavesTargetAndParentUnchanged(t *testing.T) {
	budget := NewNativeDeltaBudget(64)
	var target NativeDeltaDelete
	raw := []byte(`{"type":"row","op":"delete","key":"cGs="}`)
	var limit *NativeDeltaDecodedLimitError
	if err := decodeNativeDeltaJSONWithBudget(raw, &target, budget); !errors.As(err, &limit) || target.Type != "" || budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("JSON allocated/published an unadmitted target", err)
	}
}

func TestNativeDeltaJSONSuccessfulTargetTransfersBeforeScratchEnds(t *testing.T) {
	root := NewNativeDeltaBudget(0)
	owner := root.Scope()
	var target NativeDeltaDelete
	raw := []byte(`{"type":"row","op":"delete","key":"cGs="}`)
	admittedBeforeDecode := false
	root.Observe(func(event NativeDeltaAllocationEvent) {
		if event.Action == "reserve" && event.Phase == "json_typed_candidate" {
			admittedBeforeDecode = target.Type == ""
		}
	})
	if err := decodeNativeDeltaJSONWithBudget(raw, &target, owner); err != nil {
		t.Fatal(err)
	}
	n, err := NativeDeltaJSONSize(target)
	if err != nil || !admittedBeforeDecode || target.Key != "cGs=" || root.Usage().CurrentAccountedDecodedBytes != n || owner.ReservedBytes("json_typed_candidate") != n {
		t.Fatal("returned typed target lost its reservation", target, root.Usage(), n, err)
	}
	owner.Release()
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("typed target did not end with its caller owner")
	}
}

func TestNativeDeltaBudgetTransfersOnlySurvivingOutput(t *testing.T) {
	root := NewNativeDeltaBudget(100)
	operation := root.Scope()
	output := operation.Scope()
	scratch := output.Scope()
	if err := output.ChargeBytes("output", 30); err != nil {
		t.Fatal(err)
	}
	if err := scratch.ChargeBytes("scratch", 40); err != nil {
		t.Fatal(err)
	}
	scratch.Release()
	output.Retain()
	output.Release()
	usage := root.Usage()
	if usage.CurrentAccountedDecodedBytes != 30 || usage.PeakAccountedDecodedBytes != 70 || usage.CurrentCategories["scratch"] != 0 || usage.CurrentCharges["output"] != 1 {
		t.Fatal("transfer lost ownership", usage)
	}
	if err := output.ChargeBytes("late", 1); err == nil {
		t.Fatal("transferred scope reused")
	}
	operation.Release()
	if root.Usage().CurrentAccountedDecodedBytes != 0 || root.Usage().CumulativeCategories["scratch"] != 40 {
		t.Fatal("parent did not release transferred output", root.Usage())
	}
}

func TestNativeDeltaBudgetTransfersPageBodyWithoutDuplicateCharge(t *testing.T) {
	root := NewNativeDeltaBudget(100)
	operation := root.Scope()
	page := operation.Scope()
	record := page.Scope()
	if err := record.ChargeBytes("body", 80); err != nil {
		t.Fatal(err)
	}
	record.RetainTo(operation)
	page.Release()
	if root.Usage().CurrentAccountedDecodedBytes != 80 || root.Usage().PeakAccountedDecodedBytes != 80 || root.Usage().CumulativeCategories["body"] != 80 {
		t.Fatal("page transfer duplicated/dropped Body", root.Usage())
	}
	operation.Release()
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("transferred page body leaked")
	}
}

func TestNativeDeltaBudgetFailureUnwindsUnfinishedChildren(t *testing.T) {
	root := NewNativeDeltaBudget(100)
	owner := root.Scope()
	child := owner.Scope()
	if err := child.ChargeBytes("unfinished", 80); err != nil {
		t.Fatal(err)
	}
	owner.Release()
	child.Release()
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("unfinished child escaped", root.Usage())
	}
	if err := child.ChargeBytes("late", 1); err == nil {
		t.Fatal("released child reused")
	}
}
