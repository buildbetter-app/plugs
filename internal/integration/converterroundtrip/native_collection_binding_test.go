package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

// Independent intent for the exported ownership handle. Source I/O remains
// the same observed real physical originals used by the receiving tests.
func physicalCollectionIntent(t *testing.T, ctx context.Context, l *joinedOriginalLedger, store driver.Store, lease driver.ActiveLease) (*historyverify.NativeCollection, driver.RestoreImportProfile, driver.SourcePlanePlanV1) {
	t.Helper()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(l.binding)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "physical-collection-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64)}, SourceStoreID: driver.StoreID("merge-" + hashBytes(raw)), Method: "offline.restore.import", IdempotencyKey: "physical-native-prepared-handle", ArchiveDigest: hashBytes(raw), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	target := driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	provider := driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: target.Lifetime, OriginAdapter: "beads-workflow-origins/1"}
	issuer, err := historyverify.NewNativeCollection(ctx, l.binding, historyverify.NativeCollectionAdmission{Profile: profile, Target: target, Provider: provider}, when)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = issuer.Close() })
	for _, source := range l.sources() {
		if err := issuer.Append(ctx, source); err != nil {
			t.Fatal(err)
		}
	}
	a, err := issuer.FinishCurrent()
	if err != nil {
		t.Fatal(err)
	}
	plan := physicalCollectionSourcePlan(t, a, store, lease)
	return issuer, profile, plan
}

func TestNativeCollectionPreparedHandleReceivesOnceAndReleasesOwner(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
	store, initial, _ := openIdentityTarget(t, "memory")
	if err := initial.Release(ctx); err != nil {
		t.Fatal(err)
	}
	ctx, lease, release, err := HoldRestoreLease(ctx, store, "collection-prepared-handle")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	l := newJoinedOriginalLedger(t)
	issuer, profile, plan := physicalCollectionIntent(t, ctx, l, store, lease)
	prepared, err := BindNativeCaptureCollectionImport(ctx, issuer, profile, plan)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	bound, err := prepared.Profile()
	if err != nil {
		t.Fatal(err)
	}
	// Independent retained-evidence oracle: a compact manifest does not hash
	// itself, but the actual final plan must retain it for receipt recovery.
	owner := issuer.Owner()
	manifest, exists, err := owner.MemberChecked("manifest.json")
	if err != nil || !exists {
		t.Fatal("authenticated compact manifest unavailable", err)
	}
	image, err := prepared.prepared.plan.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var evidence archiveEvidence
	assertJoinedPhysicalPlanHistories(t, image, l.binding)
	for _, row := range image.Records {
		if row.Ref.ID == "restore/archive" {
			if err := json.Unmarshal(row.Body, &evidence); err != nil {
				t.Fatal(err)
			}
		}
	}
	file, exists := evidence.Members["manifest.json"]
	if !exists || file.SHA256 != hashBytes(manifest) || file.Size != int64(len(manifest)) || !bytes.Equal(image.Blobs[file.SHA256], manifest) || evidence.ArchiveSHA256 != bound.ArchiveDigest {
		t.Fatal("actual plan omitted or changed retained compact manifest")
	}
	if _, self := owner.Manifest().Files["manifest.json"]; self {
		t.Fatal("source manifest acquired a self commitment")
	}
	manifest, image, evidence, owner = nil, nativeprojection.Snapshot{}, archiveEvidence{}, nil
	observed := observeJoinedStore(t, store, l)
	report, err := prepared.RestoreWithLease(ctx, observed, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Close()
	// This actual receipt formerly hit the unknown model.Commit counter guard.
	retained := report
	retained.Archive = nil
	encodedResult, encodeErr := json.Marshal(retained)
	encodedReceipt, receiptErr := json.Marshal(report.Receipt)
	countedReceipt, countErr := dumpformat.NativeDeltaJSONSize(report.Receipt)
	usage, tracked := report.Archive.NativeDecodedUsage()
	if encodeErr != nil || receiptErr != nil || countErr != nil || !tracked || usage.CurrentCategories["receiving_retained_result"] != uint64(len(encodedResult)+1) || usage.CurrentCharges["receiving_retained_result"] != 1 || countedReceipt != uint64(len(encodedReceipt)+1) {
		t.Fatal("actual returned receipt/report charge differs from marshaled wire", encodeErr, receiptErr, countErr, usage, len(encodedResult), countedReceipt, len(encodedReceipt))
	}
	assertJoinedPhysicalHistories(t, report.NativeHistories, l.binding)
	if observed.writes != 1 || report.Receipt == nil || report.Restored.Head != model.Natural64(195) {
		t.Fatal("prepared receive changed session cardinality", observed.writes, report.Restored)
	}
	l.assertTraversals(t, 0, 1, true)
	if _, err := prepared.RestoreWithLease(ctx, observed, lease); !errors.Is(err, driver.ErrConflict) {
		t.Fatal("consumed preparation reused", err)
	}
	if observed.writes != 1 {
		t.Fatal("consumed handle opened another writer")
	}
	l.phase = "prepared handle independent readback"
	if err := VerifyNativeCaptureCollectionReadback(ctx, observed, l.binding, l.sources(), bound, report); err != nil {
		t.Fatal(err)
	}
	l.assertTraversals(t, 0, 2, true)
	bound, profile, plan = driver.RestoreImportProfile{}, driver.RestoreImportProfile{}, driver.SourcePlanePlanV1{}
	if err := report.Close(); err != nil {
		t.Fatal(err)
	}
	if report.Archive != nil || report.SourcePlanes != nil || report.NativeFamilies != nil || report.NativeSchemas != nil || report.NativeHistories != nil || report.Receipt != nil || report.NativeDeltaHistory != nil || report.GraphCertification != nil {
		t.Fatal("closed report retained owned views")
	}
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("prepared operation retained reservations", budget.Usage())
	}
	if err := report.Close(); err != nil || budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("repeated report close changed ownership", err, budget.Usage())
	}
}

func TestNativeCollectionPreparedHandleAdmitsCanonicalBindingBeforeAllocation(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
	store, initial, _ := openIdentityTarget(t, "memory")
	if err := initial.Release(ctx); err != nil {
		t.Fatal(err)
	}
	ctx, lease, release, err := HoldRestoreLease(ctx, store, "collection-binding-admission")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	l := newJoinedOriginalLedger(t)
	issuer, profile, plan := physicalCollectionIntent(t, ctx, l, store, lease)
	plan.Providers = append([]driver.SourcePlaneProviderV1(nil), plan.Providers...)
	plan.Providers[0].LogicalImageSHA256 = strings.Repeat("0", 64)
	n, err := driver.SourcePlanePlanEncodedSizeV1(plan)
	if err != nil || n == 0 {
		t.Fatal(err)
	}
	// Compare the sizing path to the real codec, never to a second encoder.
	actual, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil || uint64(len(actual.CanonicalPlan)) != n {
		t.Fatal("codec measurement differs", err, n)
	}
	actual = driver.SourcePlaneImportBindingV1{}
	usage := budget.Usage()
	held := usage.LimitBytes - usage.CurrentAccountedDecodedBytes - n + 1
	// Scalar admission fault; this is deliberately not an RSS observation.
	if err := budget.ChargeBytes("invented_binding_limit_control", held); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, store)
	prepared, err := BindNativeCaptureCollectionImport(ctx, issuer, profile, plan)
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if prepared != nil || !errors.As(err, &limit) || limit.Phase != "collection_host_plan_encoding" || limit.RejectedChargeBytes != n {
		t.Fatal("binding allocated before required admission", err)
	}
	if issuer.Owner() != nil || budget.Usage().CurrentAccountedDecodedBytes != held {
		t.Fatal("refusal did not close transferred issuer", budget.Usage())
	}
	if !reflect.DeepEqual(before, snapshot(t, store)) {
		t.Fatal("binding refusal wrote destination")
	}
	budget.ReleaseBytes("invented_binding_limit_control", held)
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("refused binding retained bytes", budget.Usage())
	}
}
