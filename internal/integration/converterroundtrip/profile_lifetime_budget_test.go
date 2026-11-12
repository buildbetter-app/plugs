package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Add only the actual retained profile output to a pre-binding baseline.
// Prepared caches, wire buffers and planner scratch remain excluded so an
// exact comparison still detects their survival after the operation.
func receivingUsageWithProfile(t *testing.T, baseline dumpformat.NativeDeltaDecodedUsage, profile driver.RestoreImportProfile) dumpformat.NativeDeltaDecodedUsage {
	t.Helper()
	canonicalBytes := uint64(0)
	if profile.SourcePlanes != nil {
		canonicalBytes = uint64(len(profile.SourcePlanes.CanonicalPlan))
	}
	profile.SourcePlanes = nil
	scalarBytes, err := dumpformat.NativeDeltaJSONSize(profile)
	if err != nil {
		t.Fatal(err)
	}
	copyCounts := func(counts map[string]uint64) map[string]uint64 {
		copy := make(map[string]uint64, len(counts)+2)
		for phase, count := range counts {
			copy[phase] = count
		}
		return copy
	}
	baseline.CurrentCategories = copyCounts(baseline.CurrentCategories)
	baseline.CurrentCharges = copyCounts(baseline.CurrentCharges)
	add := func(phase string, size uint64) {
		baseline.CurrentAccountedDecodedBytes += size
		baseline.CurrentCategories[phase] += size
		baseline.CurrentCharges[phase]++
	}
	add("profile_scalar_output", scalarBytes-1)
	if canonicalBytes != 0 {
		add("profile_canonical_output", canonicalBytes)
	}
	return baseline
}

func assertReceivingCurrentUsage(t *testing.T, want, got dumpformat.NativeDeltaDecodedUsage) {
	t.Helper()
	if got.CurrentAccountedDecodedBytes != want.CurrentAccountedDecodedBytes || !reflect.DeepEqual(got.CurrentCategories, want.CurrentCategories) || !reflect.DeepEqual(got.CurrentCharges, want.CurrentCharges) {
		t.Fatal("live receiving ownership differs", want, got)
	}
}

func TestHistoryBindingOutputStaysChargedThroughParentClone(t *testing.T) {
	ledger := newReceivingLedger(t)
	archive := ledger.archive()
	defer archive.ReleaseNativeDecoded()
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := receivingPublicProfile(t, archive, store, lease)
	baseline, _ := archive.NativeDecodedUsage()
	work, releaseWork := archive.NativeDeltaAccountingScope()
	defer releaseWork()
	bound, err := bindHistoryArchive(work.WithNativeDeltaAccounting(context.Background()), work, profile, nativeFixtureTime)
	if err != nil {
		t.Fatal(err)
	}
	if bound.HistoryDigest != profile.HistoryDigest || bound.SourcePlanes == nil || !bytes.Equal(bound.SourcePlanes.CanonicalPlan, profile.SourcePlanes.CanonicalPlan) {
		t.Fatal("history binding changed the admitted profile")
	}
	wantBound := receivingUsageWithProfile(t, baseline, bound)
	held, _ := archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, wantBound, held)

	// This is the real caller's next step: validate and clone while the
	// intermediate output is still held, then end its incoming work owner.
	retained, releaseRetained, err := nativeprojection.DetachedProfile(archive, bound)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseRetained()
	wantBoth := receivingUsageWithProfile(t, wantBound, retained)
	held, _ = archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, wantBoth, held)
	bound.SourcePlanes.CanonicalPlan[0] ^= 1
	if !bytes.Equal(retained.SourcePlanes.CanonicalPlan, profile.SourcePlanes.CanonicalPlan) {
		t.Fatal("parent output aliases the intermediate profile")
	}
	bound = driver.RestoreImportProfile{}
	releaseWork()
	held, _ = archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, receivingUsageWithProfile(t, baseline, retained), held)
	runtime.KeepAlive(retained)
	retained = driver.RestoreImportProfile{}
	releaseRetained()
	held, _ = archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, baseline, held)
	profile = driver.RestoreImportProfile{}
	archive.ReleaseNativeDecoded()
	held, _ = archive.NativeDecodedUsage()
	if held.CurrentAccountedDecodedBytes != 0 || len(held.CurrentCategories) != 0 || len(held.CurrentCharges) != 0 {
		t.Fatal("history handoff final ledger is not zero", held)
	}
	runtime.KeepAlive(ledger)
}

// Exercise real detachment, the shared receiving ledger and the Go heap. Input
// ownership is established before the baseline; only the returned output is
// measured. The margin allows allocator rounding and small JSON/runtime state,
// not a canonical-plan-sized overreservation or an unaccounted output copy.
func TestProfileDetachedLedgerMatchesLiveHeapAndReleasesToZero(t *testing.T) {
	const resultEnv = "PLUG_PROFILE_HEAP_RESULT"
	type measurement struct {
		RawPlan, Held, BeforeHeap, AfterHeap      uint64
		BeforeGC, AfterGC                         uint32
		HeapDelta, Difference                     int64
		Margin                                    int64
		FinalBytes, FinalCategories, FinalCharges uint64
	}
	resultPath := os.Getenv(resultEnv)
	if resultPath == "" {
		// A process-wide heap delta must not include caches or background work
		// left by other tests in this shard. Run this control exactly once in
		// its own copy of the same test binary, with the same real fixture.
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		resultPath = filepath.Join(t.TempDir(), "profile-heap.json")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, self, "-test.run=^TestProfileDetachedLedgerMatchesLiveHeapAndReleasesToZero$", "-test.v", "-test.count=1", "-test.timeout=90s")
		// The child runs the named control instead of inheriting the parent's
		// shard selection or writing over the parent's XML/shard marker.
		for _, value := range os.Environ() {
			key, _, _ := strings.Cut(value, "=")
			switch key {
			case resultEnv, "TEST_TOTAL_SHARDS", "TEST_SHARD_INDEX", "TEST_SHARD_STATUS_FILE", "TESTBRIDGE_TEST_ONLY", "XML_OUTPUT_FILE":
				continue
			}
			command.Env = append(command.Env, value)
		}
		command.Env = append(command.Env, resultEnv+"="+resultPath, "TEST_TOTAL_SHARDS=1", "TEST_SHARD_INDEX=0")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated profile heap control failed: %v\n%s", err, output)
		}
		data, err := os.ReadFile(resultPath)
		if err != nil {
			t.Fatalf("isolated profile heap control did not produce a measurement: %v\n%s", err, output)
		}
		var measured measurement
		if err := json.Unmarshal(data, &measured); err != nil {
			t.Fatal(err)
		}
		if measured.RawPlan < 2<<20 || measured.Held < measured.RawPlan || measured.Margin != 256<<10 || measured.Difference > measured.Margin || measured.Difference < -measured.Margin || measured.AfterGC < measured.BeforeGC+2 || measured.FinalBytes != 0 || measured.FinalCategories != 0 || measured.FinalCharges != 0 {
			t.Fatal("isolated profile heap measurement is incomplete", measured)
		}
		t.Logf("PROFILE_LEDGER_HEAP raw_plan=%d held=%d heap_delta=%d margin=%d final_bytes=%d before_heap=%d after_heap=%d before_gc=%d after_gc=%d final_categories=%d final_counts=%d isolated=true", measured.RawPlan, measured.Held, measured.HeapDelta, measured.Margin, measured.FinalBytes, measured.BeforeHeap, measured.AfterHeap, measured.BeforeGC, measured.AfterGC, measured.FinalCategories, measured.FinalCharges)
		return
	}
	ledger := newReceivingLedger(t)
	archive := ledger.archive()
	defer archive.ReleaseNativeDecoded()
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(),
		Target:     driver.SourcePlaneTargetV1{StoreID: "target", Incarnation: "target-incarnation", RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: strings.Repeat("x", 2<<20), Authority: "trusted-explicit/1", PrincipalJSON: []byte(`{"kind":"user","id":"owner"}`)}},
		Providers:  []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: driver.CheckpointLifetimeProcess, LogicalImageSize: 2, LogicalImageSHA256: hashBytes([]byte("{}")), OriginAdapter: "beads-workflow-origins/1"}},
	}
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: "profile-ledger-live-heap", ArchiveDigest: archive.SHA256(), DerivedDigest: strings.Repeat("c", 64), SourcePlanes: &binding}
	// Warm serializer/type caches without retaining its detached output.
	if _, err := profile.Detached(); err != nil {
		t.Fatal(err)
	}
	// One collection can move a sync.Pool's cached buffers into its victim
	// generation, still counted in HeapAlloc until the next collection. Drain
	// both generations before either sample so old scratch cannot subtract
	// from the returned output's measured live bytes.
	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	runtime.KeepAlive(profile)
	runtime.KeepAlive(plan)
	runtime.KeepAlive(binding)
	baseline, _ := archive.NativeDecodedUsage()
	detached, release, err := nativeprojection.DetachedProfile(archive, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(detached)
	runtime.KeepAlive(profile)
	runtime.KeepAlive(plan)
	runtime.KeepAlive(binding)
	live, _ := archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, receivingUsageWithProfile(t, baseline, detached), live)
	held := live.CurrentAccountedDecodedBytes - baseline.CurrentAccountedDecodedBytes
	heapDelta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const margin = 256 << 10
	difference := int64(held) - heapDelta
	if held < 2<<20 || difference > margin || difference < -margin {
		t.Fatalf("live ledger/heap mismatch: held=%d heap_delta=%d difference=%d margin=%d", held, heapDelta, difference, margin)
	}
	if !bytes.Equal(detached.SourcePlanes.CanonicalPlan, profile.SourcePlanes.CanonicalPlan) {
		t.Fatal("detachment changed raw plan")
	}
	detached.SourcePlanes.CanonicalPlan[0] ^= 1
	if bytes.Equal(detached.SourcePlanes.CanonicalPlan, profile.SourcePlanes.CanonicalPlan) {
		t.Fatal("returned plan aliases caller bytes")
	}
	runtime.KeepAlive(detached)
	runtime.KeepAlive(profile)
	runtime.KeepAlive(plan)
	detached = driver.RestoreImportProfile{}
	release()
	done, _ := archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, baseline, done)
	archive.ReleaseNativeDecoded()
	final, _ := archive.NativeDecodedUsage()
	if final.CurrentAccountedDecodedBytes != 0 || len(final.CurrentCategories) != 0 || len(final.CurrentCharges) != 0 {
		t.Fatal("final receiving ledger is not zero", final)
	}
	measured := measurement{RawPlan: uint64(len(binding.CanonicalPlan)), Held: held, BeforeHeap: before.HeapAlloc, AfterHeap: after.HeapAlloc, BeforeGC: before.NumGC, AfterGC: after.NumGC, HeapDelta: heapDelta, Difference: difference, Margin: margin, FinalBytes: final.CurrentAccountedDecodedBytes, FinalCategories: uint64(len(final.CurrentCategories)), FinalCharges: uint64(len(final.CurrentCharges))}
	data, err := json.Marshal(measured)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(ledger)
}

func TestDigestOnlyReceivingPlannerReleasesActualImageBeforeVerifier(t *testing.T) {
	ledger := newReceivingLedger(t)
	archive := ledger.archive()
	defer archive.ReleaseNativeDecoded()
	// Admission and preparation are actual frozen public data; this test does
	// not substitute a second mapper, fake planner or synthetic allocation loop.
	if _, err := archive.PrepareNativeDelta(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The existing public profile helper binds one admitted mapping/clock.
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := receivingPublicProfile(t, archive, store, lease)
	baseline, _ := archive.NativeDecodedUsage()
	for range 2 {
		derived, err := sourceDerivedCommitment(context.Background(), archive, &profile, nativeFixtureTime)
		if err != nil || derived != profile.DerivedDigest {
			t.Fatal("same planner digest differs", derived, err)
		}
		after, _ := archive.NativeDecodedUsage()
		if after.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(after.CurrentCategories, baseline.CurrentCategories) {
			t.Fatal("digest-only planner survived return", after.CurrentCategories, baseline.CurrentCategories)
		}
	}
}
