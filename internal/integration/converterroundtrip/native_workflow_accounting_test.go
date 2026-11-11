package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func TestNativeWorkflowOptionalAccountingPreservesNoBudgetBytesAndCloneOwnership(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, _ := openIdentityTarget(t, engine)
			data := jointSourceFixture(t)
			_, plan := jointUnboundProfileWithPlan(t, data, store, lease, nil)
			plan.Providers[0].LogicalImageSHA256 = strings.Repeat("0", 64)
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			records, err := archive.SourcePlaneRecords()
			if err != nil {
				t.Fatal(err)
			}
			original, _ := json.Marshal(records)
			// Existing provider API, existing fixture and absence of a context budget.
			plain, err := (&workflowplugin.Provider{}).PrepareSourceRestore(context.Background(), plan, records)
			if err != nil {
				t.Fatal("unchanged no-budget preparation", err)
			}
			expected := plain.LogicalImage()
			encoded, err := driver.EncodeSourcePlanePlanV1(plan)
			if err != nil {
				t.Fatal(err)
			}
			counted, err := dumpformat.NativeSourcePlanePlanSize(plan)
			if err != nil || counted != uint64(len(encoded)) {
				t.Fatal("binary plan domain differs", counted, len(encoded), err)
			}

			root := dumpformat.NewNativeDeltaBudget(0)
			var trace []dumpformat.NativeDeltaAllocationEvent
			root.Observe(func(event dumpformat.NativeDeltaAllocationEvent) { trace = append(trace, event) })
			owner := root.Scope()
			prepared, err := (&workflowplugin.Provider{}).PrepareSourceRestore(dumpformat.WithNativeDeltaBudget(context.Background(), owner), plan, records)
			if err != nil {
				t.Fatal(err)
			}
			preparationPeak := root.Usage().PeakAccountedDecodedBytes
			n := owner.ReservedBytes("workflow_prepared_logical")
			if n != uint64(len(expected)) || root.Usage().CurrentAccountedDecodedBytes != n {
				t.Fatal("prepared output/scratch ownership differs", root.Usage())
			}
			if err := owner.ChargeBytes("workflow_logical_image_clone", n); err != nil {
				t.Fatal(err)
			}
			actual := prepared.LogicalImage()
			if !bytes.Equal(actual, expected) || plain.SHA256() != prepared.SHA256() {
				t.Fatal("optional budget changed exact logical bytes")
			}
			// The caller owns the accessor clone. Mutating it cannot alter preparation.
			saved := actual[0]
			actual[0] ^= 1
			if prepared.SHA256() != plain.SHA256() {
				t.Fatal("accessor stopped detaching")
			}
			actual[0] = saved
			owner.ReleaseBytes("workflow_prepared_logical", n)
			if root.Usage().CurrentAccountedDecodedBytes != n {
				t.Fatal("clone owner was lost")
			}
			owner.Release()
			if root.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("caller clone leaked")
			}
			after, _ := json.Marshal(records)
			if !bytes.Equal(after, original) {
				t.Fatal("input records mutated")
			}
			for _, phase := range []string{"workflow_wire_decoded", "workflow_wire_raw_clone", "workflow_origin_raw_clone", "workflow_prepared_logical", "workflow_logical_image_clone"} {
				if root.Usage().CumulativeCharges[phase] == 0 {
					t.Fatal("missing allocation domain", phase)
				}
			}
			traces, _ := json.Marshal(trace)
			t.Logf("workflow_allocation_ownership_trace=%s", traces)

			peak := preparationPeak
			for _, ceiling := range []uint64{1, peak - 1} {
				failed := dumpformat.NewNativeDeltaBudget(ceiling)
				op := failed.Scope()
				result, err := (&workflowplugin.Provider{}).PrepareSourceRestore(dumpformat.WithNativeDeltaBudget(context.Background(), op), plan, records)
				var refusal *dumpformat.NativeDeltaDecodedLimitError
				if !errors.As(err, &refusal) || refusal.Overflow || refusal.MeasuredBytes <= ceiling || result.SHA256() != hashBytes(nil) || failed.Usage().CurrentAccountedDecodedBytes != 0 {
					t.Fatalf("unwind/refusal failed: %#v %+v", err, failed.Usage())
				}
				op.Release()
				t.Logf("workflow_refusal=%+v", refusal)
			}
			canceled, cancel := context.WithCancel(context.Background())
			failed := dumpformat.NewNativeDeltaBudget(0)
			op := failed.Scope()
			failed.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
				if event.Action == "reserve" && event.Phase == "workflow_wire_raw_clone" {
					cancel()
				}
			})
			_, err = (&workflowplugin.Provider{}).PrepareSourceRestore(dumpformat.WithNativeDeltaBudget(canceled, op), plan, records)
			if !errors.Is(err, context.Canceled) || failed.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("mid-allocation cancellation retained a candidate", err, failed.Usage())
			}
			op.Release()
			broken := append([]driver.SourcePlaneRecordV1{}, records...)
			broken[0].Raw = append(append([]byte{}, broken[0].Raw...), ' ')
			failed = dumpformat.NewNativeDeltaBudget(0)
			op = failed.Scope()
			_, err = (&workflowplugin.Provider{}).PrepareSourceRestore(dumpformat.WithNativeDeltaBudget(context.Background(), op), plan, broken)
			if err == nil || failed.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("origin failure did not unwind", err, failed.Usage())
			}
			op.Release()
			if !reflect.DeepEqual(plan.Providers[0].LogicalImageSHA256, strings.Repeat("0", 64)) {
				t.Fatal("plan mutated")
			}
		})
	}
}

func TestNativeWorkflowAccountingPublishesAndReopensExactLogicalBytes(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			data := jointSourceFixture(t)
			store, lease, reopen := openIdentityTarget(t, engine)
			profile, plan := jointUnboundProfileWithPlan(t, data, store, lease, nil)
			root := dumpformat.NewNativeDeltaBudget(0)
			owner := root.Scope()
			defer owner.Release()
			trace := []dumpformat.NativeDeltaAllocationEvent{}
			root.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
				if strings.HasPrefix(event.Phase, "workflow_") || event.Action == "transfer" {
					trace = append(trace, event)
				}
			})
			ctx := dumpformat.WithNativeDeltaBudget(context.Background(), owner)
			profile, err := BindSourceImport(ctx, bytes.NewReader(data), profile, plan)
			if err != nil {
				t.Fatal("budgeted binding", err)
			}
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal("budgeted publication", err)
			}
			if report.SourcePlanes == nil || report.Receipt == nil {
				t.Fatal("joint receipt absent")
			}
			binding := report.SourcePlanes.Providers[0]
			cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			before, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
			if err != nil || driver.ValidateSourcePlaneInitialCheckpoint(before, binding) != nil {
				t.Fatal("exact initial bytes not bound", err)
			}
			store, lease = reopen()
			cap, err = driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			after, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("checkpoint bytes changed on reopen", err)
			}
			recovered, err := RecoverImport(ctx, store, profile, Verification{profile.ArchiveDigest, profile.DerivedDigest})
			if err != nil || !reflect.DeepEqual(report.Receipt, recovered.Receipt) {
				t.Fatal("reopened receipt differs", err)
			}
			state, err := pluginhost.NewSourceRuntimeState(ctx, cap, lease, binding)
			if err != nil {
				t.Fatal(err)
			}
			provider, err := workflowplugin.NewSourceRuntime(state, binding)
			if err != nil {
				t.Fatal(err)
			}
			item, err := provider.Get(ctx, profile.Admission.Actor, model.RefByID("workflow:item/restored", model.Natural64(0)), model.WriteMeta{Project: "demo"})
			if err != nil || item.Title != "Exact active collision" {
				t.Fatal("reopened active source payload differs", item, err)
			}
			owner.Release()
			if root.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("completed owner leaked", root.Usage())
			}
			events, _ := json.Marshal(trace)
			t.Logf("workflow_publication_reopen_trace=%s usage=%+v test_process_memory=%v", events, root.Usage(), nativeTestProcessMemory())
		})
	}
}
