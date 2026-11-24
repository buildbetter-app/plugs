package converterroundtrip

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSourceInspectionResourceIdentityThroughRecoveryAndReverse(t *testing.T) {
	data := sourceReverseFixture(t, true)
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := sourceReverseProfile(t, data, store, lease, true)
	report, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile)
	if err != nil {
		t.Fatal("establish actual committed source import", err)
	}
	if report.Archive != nil {
		defer report.Archive.ReleaseNativeDecoded()
	}
	before := savedLabelImport(t, store, report.Archive, profile, "")
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	namespace := report.SourcePlanes.Providers[0].Provider.Namespace
	beforeCheckpoint, err := capability.LoadCheckpoint(context.Background(), namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"recovery", "reverse"} {
		for _, fault := range []string{"cancel", "budget"} {
			t.Run(route+"/"+fault, func(t *testing.T) {
				budget := dumpformat.NewNativeDeltaBudget(0)
				owner := budget.Scope()
				retained := owner.Scope()
				ctx, cancel := context.WithCancel(dumpformat.WithNativeDeltaBudget(context.Background(), owner))
				defer cancel()
				armed := false
				budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
					if event.Action != "reserve" || event.Phase != "workflow_reverse_indexes" || armed {
						return
					}
					armed = true
					if fault == "cancel" {
						cancel()
						return
					}
					usage := budget.Usage()
					if err := retained.ChargeBytes("test_retained_caller_work", usage.LimitBytes-usage.CurrentAccountedDecodedBytes-(sha256.Size*2-1)); err != nil {
						t.Fatal(err)
					}
				})
				var callErr error
				if route == "recovery" {
					recovered, e := RecoverImport(ctx, store, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
					if recovered.Archive != nil {
						recovered.Archive.ReleaseNativeDecoded()
					}
					callErr = e
				} else {
					_, callErr = ReverseRestored(ctx, store)
				}
				if !armed {
					t.Fatal("call never reached admitted inspection", callErr)
				}
				var refusal *dumpformat.NativeDeltaDecodedLimitError
				var reverseRefusal ReverseRefusal
				if fault == "cancel" && !errors.Is(callErr, context.Canceled) || fault == "budget" && (!errors.As(callErr, &refusal) || refusal.Phase != "workflow_commitment_digest") {
					t.Fatal("resource identity lost through public boundary", callErr)
				}
				if errors.Is(callErr, driver.ErrConflict) || errors.Is(callErr, driver.ErrUnsupported) || errors.As(callErr, &reverseRefusal) {
					t.Fatal("resource refusal misreported as source corruption", callErr)
				}
				retained.Release()
				for phase, n := range budget.Usage().CurrentCategories {
					if strings.HasPrefix(phase, "workflow_reverse_") && n != 0 {
						t.Fatal("failed inspection owner survived caller", phase, n)
					}
				}
				owner.Release()
				if budget.Usage().CurrentAccountedDecodedBytes != 0 {
					t.Fatal("failed caller retained decoded reservations", budget.Usage())
				}
				afterCheckpoint, err := capability.LoadCheckpoint(context.Background(), namespace)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(beforeCheckpoint, afterCheckpoint) {
					t.Fatal("failed inspection changed workflow checkpoint")
				}
				if !reflect.DeepEqual(before, savedLabelImport(t, store, report.Archive, profile, "")) {
					t.Fatal("failed inspection changed committed store")
				}
			})
		}
	}
}

func TestSourceInspectionFailureKeepsResourceIdentityAndMalformedMapping(t *testing.T) {
	limit := &dumpformat.NativeDeltaDecodedLimitError{Phase: "workflow_commitment_digest", LimitBytes: 1, MeasuredBytes: 2}
	for _, fallback := range []error{driver.ErrConflict, ReverseRefusal{"invalid_current_source_image"}} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded, limit} {
			wrapped := fmt.Errorf("late inspection: %w", cause)
			got := sourceInspectionFailure(wrapped, fallback)
			if got != wrapped || !errors.Is(got, cause) {
				t.Fatal("resource identity flattened", got)
			}
		}
		semantic := fmt.Errorf("%w: invalid reverse entity origin", driver.ErrUnsupported)
		if got := sourceInspectionFailure(semantic, fallback); got != fallback {
			t.Fatal("malformed-image mapping changed", got)
		}
	}
}

func TestSourceReverseSuccessfulInspectionReleasesScratch(t *testing.T) {
	data := sourceReverseFixture(t, true)
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := sourceReverseProfile(t, data, store, lease, true)
	report, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Archive.ReleaseNativeDecoded()

	budget := dumpformat.NewNativeDeltaBudget(0)
	caller := budget.Scope()
	defer caller.Release()
	if err := caller.ChargeBytes("test_live_caller", 37); err != nil {
		t.Fatal(err)
	}
	baseline := budget.Usage()
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), caller)
	for attempt := 0; attempt < 2; attempt++ {
		reversed, err := ReverseRestored(ctx, store)
		if err != nil || len(reversed.Result.JSONL) == 0 {
			t.Fatal("reverse committed source", err)
		}
		usage := budget.Usage()
		if usage.CumulativeCharges["workflow_reverse_indexes"] == 0 {
			t.Fatal("inspection did not run", usage)
		}
		// The full independently established caller baseline must survive exactly,
		// with the returned conversion still live. No parent release hides residue.
		assertReceivingCurrentUsage(t, baseline, usage)
		if len(reversed.Result.JSONL) == 0 {
			t.Fatal("returned reverse output lost")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	inspections := 0
	budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
		if event.Action == "reserve" && event.Phase == "workflow_reverse_indexes" {
			inspections++
			if inspections == 3 {
				cancel()
			} // two recovery inspectors, then reverse
		}
	})
	_, err = ReverseRestored(canceled, store)
	budget.Observe(nil)
	if inspections != 3 || !errors.Is(err, context.Canceled) {
		t.Fatal("did not reach post-recovery error", inspections, err)
	}
	assertReceivingCurrentUsage(t, baseline, budget.Usage())
	caller.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("caller baseline cleanup leak")
	}
}

// Resource refusals from the retained read must not imply a receipt conflict.
func TestNativeDeltaRecoveryPreservesResourceRefusals(t *testing.T) {
	for _, limit := range []interface {
		error
		Code() string
	}{
		&driver.ReverseJSONLimitError{Resource: "value_bytes", Measured: 2, Limit: 1},
		&dumpformat.NativeDeltaDecodedLimitError{Phase: "head_decoded_shapes", MeasuredBytes: 2, LimitBytes: 1},
		&dumpformat.NativeDeltaRecordLimitError{MeasuredBytes: 2, LimitBytes: 1},
	} {
		t.Run(limit.Code(), func(t *testing.T) {
			cause := errors.Join(fmt.Errorf("read retained capture: %w", limit), nil)
			recovered := nativeDeltaRecoveryError(cause)
			converted := sourceInspectionFailure(recovered, ReverseRefusal{"source_receipt_archive_or_origin_mismatch"})
			for _, got := range []error{recovered, converted, retainedReverseReadFailure(cause)} {
				var mismatch driver.ErrRestoreBindingMismatch
				if got != cause || !errors.Is(got, limit) || errors.Is(got, driver.ErrConflict) || errors.As(got, &mismatch) {
					t.Fatalf("resource refusal became a binding conflict: %v", got)
				}
			}
		})
	}
	var mismatch driver.ErrRestoreBindingMismatch
	err := nativeDeltaRecoveryError(errors.New("malformed retained capture"))
	if !errors.As(err, &mismatch) || mismatch.Field != "nativeDelta" || !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("malformed evidence lost its binding failure: %v", err)
	}
}
