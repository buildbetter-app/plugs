package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

type failedPostCommitSnapshot struct {
	driver.Store
	driver.WorkflowCheckpointStore
	driver.SourcePlaneSnapshotStore
	writes int
}

func (s *failedPostCommitSnapshot) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}
func (s *failedPostCommitSnapshot) Persistent() bool {
	persistent, ok := s.Store.(driver.PersistentCheckpointStore)
	return ok && persistent.Persistent()
}
func (s *failedPostCommitSnapshot) ReadSourcePlaneSnapshot(ctx context.Context, view driver.View, namespaces []string) (driver.SourcePlaneSnapshot, error) {
	meta, err := s.Store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	if !meta.Head.IsZero() {
		return nil, errors.Join(driver.ErrConflict, context.Canceled)
	}
	return s.SourcePlaneSnapshotStore.ReadSourcePlaneSnapshot(ctx, view, namespaces)
}

func TestNativeReceivingPostCommitFailureRetainsReceiptAndNonConflictWireCode(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			archive := publicNativeDeltaArchive(t)
			defer archive.ReleaseNativeDecoded()
			ctx := archive.WithNativeDeltaAccounting(context.Background())
			store, initial, reopen := openIdentityTarget(t, engine)
			if err := initial.Release(ctx); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "post-commit-readback")
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			profile := nativeStreamingProfile(t, archive, store, lease)
			observed := &failedPostCommitSnapshot{Store: store, WorkflowCheckpointStore: store.(driver.WorkflowCheckpointStore), SourcePlaneSnapshotStore: store.(driver.SourcePlaneSnapshotStore)}
			report, err := RestoreNativeCaptureSetImportWithLease(op, archive, observed, lease, profile)
			if err == nil || errors.Is(err, driver.ErrConflict) || errors.Is(err, context.Canceled) {
				t.Fatal("committed import reported as rejected", err)
			}
			if observed.writes != 1 || report.Restored.Head.IsZero() || !strings.Contains(err.Error(), "native import committed at "+report.Restored.Head.String()) {
				t.Fatal("missing committed revision or unexpected replay", report.Restored, err, observed.writes)
			}
			catalog, catalogErr := wire.DefaultCatalog()
			if catalogErr != nil {
				t.Fatal(catalogErr)
			}
			encoded, encodeErr := json.Marshal(catalog.MapError(err))
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			var response wire.Error
			if decodeErr := json.Unmarshal(encoded, &response); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if response.Data.Code != "internal_error" || response.Data.Code == "write_conflict" || response.Data.Code == "conflict" {
				t.Fatal("client sees a retryable conflict", string(encoded))
			}
			_, keyAndReason, found := strings.Cut(err.Error(), "receipt key ")
			quotedKey, _, hasReason := strings.Cut(keyAndReason, "; post-commit verification failed:")
			key, keyErr := strconv.Unquote(quotedKey)
			if !found || !hasReason || keyErr != nil || key != profile.IdempotencyKey {
				t.Fatal("message lost original receipt key", err, keyErr)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			store, _ = reopen()
			recoveryProfile := profile
			recoveryProfile.IdempotencyKey = key
			recovered, recoveryErr := RecoverImport(ctx, store, recoveryProfile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if recoveryErr != nil || recovered.Receipt == nil || recovered.Receipt.Commit.Revision != report.Restored.Head || observed.writes != 1 {
				t.Fatal("message key did not resolve the actual committed result without replay", recoveryErr, observed.writes)
			}
		})
	}
}

// The public small capture exercises the actual commit and evidence verification,
// rather than pretending a generic budget.Release is a receiving handoff.
func TestNativeReceivingPostCommitReservationHandoff(t *testing.T) {
	var measuredPeak, counterfactualPeak uint64
	for _, bounded := range []bool{false, true} {
		limit := uint64(0)
		if bounded {
			limit = measuredPeak
		}
		budget := dumpformat.NewNativeDeltaBudget(limit)
		archive := publicNativeDeltaArchive(t, budget)
		store, lease, _ := openIdentityTarget(t, "memory")
		profile := nativeStreamingProfile(t, archive, store, lease)
		baseline := budget.Usage()
		armed, inspecting, checkedPeak := false, false, false
		var released, inspectorPeak, handoff, logicalBytes uint64
		var firstUsage dumpformat.NativeDeltaDecodedUsage
		budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
			if event.Action == "reserve" && event.Phase == "receiving_retained_result" {
				armed = true
			}
			if armed && !inspecting && event.Action == "release" {
				released += event.Bytes
			}
			if event.Action != "reserve" || !strings.HasPrefix(event.Phase, "workflow_reverse_") {
				return
			}
			if !inspecting {
				inspecting = true
				handoff, logicalBytes = event.Held-event.Bytes, event.Bytes
				firstUsage = budget.Usage()
			}
			if event.Phase == "workflow_reverse_canonical" && !checkedPeak {
				checkedPeak = true
				if event.Held != handoff+4*logicalBytes {
					t.Fatal("inspection peak differs from measured payloads plus retained bytes", event, handoff, logicalBytes)
				}
			}
			if event.Held > inspectorPeak {
				inspectorPeak = event.Held
			}
		})
		report, err := restoreArchiveWithProfile(archive.WithNativeDeltaAccounting(context.Background()), archive, store, lease, &profile, nil)
		budget.Observe(nil)
		if err != nil || report.Receipt == nil || !inspecting {
			t.Fatal("actual known-commit handoff", err, inspecting)
		}
		retained := report
		retained.Archive = nil
		retainedBytes, e := dumpformat.NativeDeltaJSONSize(retained)
		if e != nil {
			t.Fatal(e)
		}
		// Compare complete maps, including baseline keys absent from the actual
		// map. Expected sizes come from the returned report and measured input,
		// never from the observed category being checked.
		baseline.CurrentAccountedDecodedBytes += 2*logicalBytes + retainedBytes
		baseline.CurrentCategories["workflow_logical_image_clone"] += logicalBytes
		baseline.CurrentCharges["workflow_logical_image_clone"]++
		baseline.CurrentCategories["receiving_retained_result"] += retainedBytes
		baseline.CurrentCharges["receiving_retained_result"]++
		baseline.CurrentCategories["workflow_reverse_input"] += logicalBytes
		baseline.CurrentCharges["workflow_reverse_input"]++
		assertReceivingCurrentUsage(t, baseline, firstUsage)
		if released == 0 || handoff != firstUsage.CurrentAccountedDecodedBytes-logicalBytes {
			t.Fatal("inexact consumed handoff", released, handoff, firstUsage)
		}
		if !bounded {
			measuredPeak = budget.Usage().PeakAccountedDecodedBytes
			counterfactualPeak = inspectorPeak + released
			if measuredPeak >= dumpformat.NativeDeltaDefaultDecodedLimit || counterfactualPeak <= measuredPeak {
				t.Fatal("small capture did not distinguish old simultaneous ownership", measuredPeak, counterfactualPeak, released)
			}
		} else if budget.Usage().PeakAccountedDecodedBytes > limit || inspectorPeak+released <= limit {
			t.Fatal("changed measured peak or budget", budget.Usage(), counterfactualPeak)
		}
		if budget.Usage().CurrentCategories["receiving_retained_result"] == 0 {
			t.Fatal("result lost its reservation")
		}
		archive.ReleaseNativeDecoded()
		if budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("result owner cleanup", budget.Usage())
		}
		t.Logf("bounded=%v limit=%d inspection_peak=%d dead_before_inspection=%d former_peak=%d", bounded, budget.Usage().LimitBytes, inspectorPeak, released, counterfactualPeak)
	}
}

func TestNativeReceivingPreparationCloseErrorKeepsReturnedResultCharged(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	archive := publicNativeDeltaArchive(t, budget)
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := nativeStreamingProfile(t, archive, store, lease)
	logicalOwner, releaseLogical := archive.NativeDeltaAccountingScope()
	prepared, err := prepareSourceArchive(logicalOwner.WithNativeDeltaAccounting(ctx), logicalOwner, profile, true)
	if err != nil {
		t.Fatal(err)
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := nativeprojection.BuildWithProfile(archive, &profile, when, ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := historyverify.PrepareNativeArchive(ctx, archive, profile, when)
	if err != nil {
		t.Fatal(err)
	}
	closeFailure := errors.New("injected postcommit preparation close failure")
	report, err := restorePreparedNativeReceivingStream(ctx, archive, store, lease, profile, plan, prepared.LogicalImage(), stream, archive, func() error { return closeFailure })
	if err == nil || !strings.Contains(err.Error(), closeFailure.Error()) || errors.Is(err, driver.ErrConflict) || report.Restored.Head.IsZero() || report.Receipt == nil || len(report.NativeFamilies) == 0 {
		t.Fatal("lost known durable result on cleanup error", err, report.Restored)
	}
	retained := report
	retained.Archive = nil
	n, sizeErr := dumpformat.NativeDeltaJSONSize(retained)
	if sizeErr != nil || budget.Usage().CurrentCategories["receiving_retained_result"] != n || budget.Usage().CurrentCharges["receiving_retained_result"] != 1 {
		t.Fatal("cleanup error dropped returned payload owner", sizeErr, budget.Usage(), n)
	}
	// The actual closed Plan copy must drop every private payload reference.
	payload := reflect.ValueOf(plan).FieldByName("planPayload").Elem()
	for _, name := range []string{"image", "archive", "profile", "sourcePlanes", "release"} {
		if !payload.FieldByName(name).IsNil() {
			t.Fatal("closed Plan retains private payload", name)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	releaseLogical()
	archive.ReleaseNativeDecoded()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("cleanup-error result owner leak", budget.Usage())
	}
}

func TestNativeReceivingRetainedResultRefusalDropsUnownedReport(t *testing.T) {
	var limit uint64
	for _, bounded := range []bool{false, true} {
		budget := dumpformat.NewNativeDeltaBudget(limit)
		var priorPeak uint64
		admitted := false
		if !bounded {
			budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
				if event.Action != "reserve" || admitted {
					return
				}
				if event.Phase == "receiving_retained_result" {
					admitted = true
					// Choose inside the measured nonempty interval, including
					// construction/profile peaks observed before the import.
					if event.Held <= priorPeak+1 {
						t.Fatal("no result-only refusal interval", priorPeak, event)
					}
					limit = priorPeak + (event.Held-priorPeak)/2
					if limit <= priorPeak || limit >= dumpformat.NativeDeltaDefaultDecodedLimit {
						t.Fatal("fixture does not isolate tiny result admission", limit, priorPeak, event)
					}
					return
				}
				if event.Held > priorPeak {
					priorPeak = event.Held
				}
			})
		}
		archive := publicNativeDeltaArchive(t, budget)
		ctx := archive.WithNativeDeltaAccounting(context.Background())
		store, lease, _ := openIdentityTarget(t, "memory")
		profile := nativeStreamingProfile(t, archive, store, lease)
		report, err := RestoreNativeCaptureSetImportWithLease(ctx, archive, store, lease, profile)
		budget.Observe(nil)
		if !bounded {
			if err != nil || !admitted || report.Receipt == nil {
				t.Fatal("measure actual returned report", err, admitted)
			}
		} else {
			if err == nil || !strings.Contains(err.Error(), "phase=receiving_retained_result") || !strings.Contains(err.Error(), "native import committed at ") || errors.Is(err, driver.ErrConflict) {
				t.Fatal("did not refuse exactly postcommit result admission", err)
			}
			if report.NativeFamilies != nil || report.NativeSchemas != nil || report.NativeHistories != nil || report.SourcePlanes != nil || report.Receipt != nil || report.Restored.Head.IsZero() {
				t.Fatal("refused result returned unowned payload or lost committed identity", report.Restored)
			}
			if budget.Usage().CurrentCategories["receiving_retained_result"] != 0 || budget.Usage().CurrentCharges["receiving_retained_result"] != 0 {
				t.Fatal("refused result retained a reservation", budget.Usage())
			}
		}
		archive.ReleaseNativeDecoded()
		if budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("unwound refusal retained outstanding budget", budget.Usage())
		}
		t.Logf("bounded=%v tiny_limit=%d", bounded, limit)
	}
}

func TestNativeCollectionAdmittedInspectionErrorDropsUnownedReceipt(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx, cancel := context.WithCancel(dumpformat.WithNativeDeltaBudget(context.Background(), budget))
	defer cancel()
	ledger := newJoinedOriginalLedger(t)
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := joinedPhysicalProfile(t, ctx, ledger, store, lease)
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("constructor retained import work", budget.Usage())
	}
	admitted := false
	budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
		if event.Action == "reserve" && event.Phase == "receiving_retained_result" {
			admitted = true
			cancel()
		}
	})
	report, err := RestoreNativeCaptureCollectionImportWithLease(ctx, ledger.binding, ledger.sources(), store, lease, profile)
	budget.Observe(nil)
	if !admitted || err == nil || !strings.Contains(err.Error(), "native import committed at ") || errors.Is(err, driver.ErrConflict) || errors.Is(err, context.Canceled) || report.Restored.Head.IsZero() {
		t.Fatal("did not reach honest post-admission inspection failure", admitted, err, report.Restored)
	}
	if report.Archive != nil || report.collection != nil || report.Receipt != nil || report.NativeFamilies != nil || report.NativeSchemas != nil || report.NativeHistories != nil || report.SourcePlanes != nil {
		t.Fatal("collection error retained unowned report payload")
	}
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("failed collection retained outstanding budget", budget.Usage())
	}
	meta, metaErr := store.Meta(context.Background())
	if metaErr != nil || meta.Head != report.Restored.Head {
		t.Fatal("error report lost the durable committed identity", meta, metaErr, report.Restored)
	}
}

// Recovery must not leave readback-only state on the original public archive.
// Its preparation is deliberately released before independent source readback.
func TestNativeRecoveryPreparedReadbackFlagRemainsPrivate(t *testing.T) {
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	store, lease, _ := openIdentityTarget(t, "memory")
	profile := nativeStreamingProfile(t, archive, store, lease)
	report, err := RestoreNativeCaptureSetImportWithLease(ctx, archive, store, lease, profile)
	if err != nil {
		t.Fatal(err)
	}
	delta := reflect.ValueOf(archive).Elem().FieldByName("delta").Elem()
	cache := delta.FieldByName("prepared")
	cachePresent := !cache.IsNil() && !cache.Elem().FieldByName("view").IsNil()
	flag := delta.FieldByName("usePreparedReadback").Bool()
	if flag || cachePresent {
		t.Fatal("recovery leaked prepared state to public root", flag, cachePresent)
	}
	if err := VerifyNativeCaptureSetReadback(ctx, store, archive, report); err != nil {
		t.Fatal("independent readback after recovery", err)
	}
	// Standalone recovery owns a NEW source archive backed by its pinned reader.
	// That reader is closed when RecoverImport returns; the charged prepared cache
	// must service readback without consulting its now-closed Blob method.
	before, _ := archive.NativeDecodedUsage()
	recovered, err := RecoverImport(ctx, store, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
	if err != nil || recovered.Archive == nil || recovered.Archive == archive {
		t.Fatal("owned recovery archive missing", err)
	}
	owned := reflect.ValueOf(recovered.Archive).Elem().FieldByName("delta").Elem()
	prepared := owned.FieldByName("prepared")
	if !owned.FieldByName("usePreparedReadback").Bool() || prepared.IsNil() || prepared.Elem().FieldByName("view").IsNil() {
		t.Fatal("owned returned archive lost prepared mode/cache")
	}
	during, _ := archive.NativeDecodedUsage()
	if during.CurrentAccountedDecodedBytes <= before.CurrentAccountedDecodedBytes {
		t.Fatal("owned cache has no live accounting", before, during)
	}
	if err := VerifyNativeCaptureSetReadback(ctx, store, recovered.Archive, recovered); err != nil {
		t.Fatal("owned archive readback after pinned reader close", err)
	}
	recovered.Archive.ReleaseNativeDecoded()
	after, _ := archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, before, after)
	if delta.FieldByName("usePreparedReadback").Bool() {
		t.Fatal("standalone recovery mutated supplied public root")
	}
}

func TestNativeReceivingPreHandoffErrorDropsPublicationViews(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	archive := publicNativeDeltaArchive(t, budget)
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	store, lease, _ := openIdentityTarget(t, "memory")
	baseline, _ := archive.NativeDecodedUsage()
	profile := nativeStreamingProfile(t, archive, store, lease)
	baseline = receivingUsageWithProfile(t, baseline, profile)
	failed := &failedPostCommitSnapshot{Store: store, WorkflowCheckpointStore: store.(driver.WorkflowCheckpointStore), SourcePlaneSnapshotStore: store.(driver.SourcePlaneSnapshotStore)}
	report, err := RestoreNativeCaptureSetImportWithLease(ctx, archive, failed, lease, profile)
	if err == nil || !strings.Contains(err.Error(), "native import committed at ") || report.Restored.Head.IsZero() {
		t.Fatal("lost known durable outcome", err)
	}
	if report.NativeFamilies != nil || report.NativeSchemas != nil || report.NativeHistories != nil || report.SourcePlanes != nil || report.Receipt != nil {
		t.Fatal("pre-handoff error returned unowned views")
	}
	held, _ := archive.NativeDecodedUsage()
	assertReceivingCurrentUsage(t, baseline, held)
	archive.ReleaseNativeDecoded()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("pre-handoff error owner leak", budget.Usage())
	}
}

// The actual engine publishes and claims its real opaque proof. Only its return
// is faulted; PreparedOfflineHistoryStream.Commit must resolve that uncertainty.
type committedIndeterminateStage struct {
	driver.OfflineHistoryStreamStage
	published *int
}

func (s *committedIndeterminateStage) Publish(ctx context.Context, proof driver.OfflineHistoryStreamPublication, header driver.ChangesetHeader) (model.Changeset, error) {
	result, err := s.OfflineHistoryStreamStage.Publish(ctx, proof, header)
	if err != nil {
		return result, err
	}
	*s.published++
	return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
}

func TestNativeReceivingActualCommittedIndeterminateRetainsAndClosesResult(t *testing.T) {
	for _, shape := range []string{"single", "collection"} {
		t.Run(shape, func(t *testing.T) {
			budget := dumpformat.NewNativeDeltaBudget(0)
			ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
			store, lease, _ := openIdentityTarget(t, "memory")
			observed := nativeStreamObservedStore(t, store)
			published, released := 0, 0
			factory := func(tx driver.WriteTx, proof historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error) {
				stage, err := nativeReceivingStage(tx, proof)
				if err != nil {
					return nil, err
				}
				return &committedIndeterminateStage{OfflineHistoryStreamStage: stage, published: &published}, nil
			}
			budget.Observe(func(e dumpformat.NativeDeltaAllocationEvent) {
				if e.Action == "release" && e.Phase == "receiving_retained_result" {
					released++
				}
			})
			var report Report
			var err error
			var closeResult func() error
			var expected dumpformat.NativeDeltaDecodedUsage
			if shape == "single" {
				archive := publicNativeDeltaArchive(t, budget)
				baseline, _ := archive.NativeDecodedUsage()
				profile := nativeStreamingProfile(t, archive, store, lease)
				expected = receivingUsageWithProfile(t, baseline, profile)
				logical, releaseLogical := archive.NativeDeltaAccountingScope()
				defer releaseLogical()
				prepared, e := prepareSourceArchive(logical.WithNativeDeltaAccounting(ctx), logical, profile, true)
				if e != nil {
					t.Fatal(e)
				}
				when, e := time.Parse(time.RFC3339Nano, profile.HistoryTime)
				if e != nil {
					t.Fatal(e)
				}
				plan, e := nativeprojection.BuildWithProfile(archive, &profile, when, archive.WithNativeDeltaAccounting(ctx))
				if e != nil {
					t.Fatal(e)
				}
				defer plan.Close()
				stream, e := historyverify.PrepareNativeArchive(archive.WithNativeDeltaAccounting(ctx), archive, profile, when)
				if e != nil {
					t.Fatal(e)
				}
				defer stream.Close()
				report, err = restorePreparedNativeReceivingStream(ctx, archive, observed, lease, profile, plan, prepared.LogicalImage(), stream, archive, stream.Close, factory)
				releaseLogical()
				archive.ClearNativeDeltaPreparation()
				if err == nil {
					err = VerifyNativeCaptureSetReadback(ctx, observed, report.Archive, report)
				}
				closeResult = func() error { archive.ReleaseNativeDecoded(); return nil }
			} else {
				ledger := newJoinedOriginalLedger(t)
				profile := joinedPhysicalProfile(t, ctx, ledger, store, lease)
				prepared, e := prepareNativeCaptureCollection(ctx, ledger.binding, ledger.sources(), profile, 0)
				if e != nil {
					t.Fatal(e)
				}
				report, err = restorePreparedNativeCaptureCollection(ctx, prepared, observed, lease, factory)
				if err == nil {
					err = VerifyNativeCaptureCollectionReadback(ctx, observed, ledger.binding, ledger.sources(), profile, report)
				}
				closeResult = report.Close
			}
			if err != nil || !report.Recovered || report.Receipt == nil || report.Archive == nil || published != 1 || observed.writes != 1 {
				t.Fatal("real committed uncertainty did not recover exactly once", shape, err, published, observed.writes)
			}
			retained := report
			retained.Archive = nil
			wireBytes, err := json.Marshal(retained)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(len(wireBytes) + 1)
			usage := budget.Usage()
			if usage.CurrentCategories["receiving_retained_result"] != want || usage.CurrentCharges["receiving_retained_result"] != 1 || released != 0 {
				t.Fatal("recovered payload is not fully charged", want, usage, released)
			}
			if shape == "single" {
				expected.CurrentAccountedDecodedBytes += want
				expected.CurrentCategories["receiving_retained_result"] += want
				expected.CurrentCharges["receiving_retained_result"]++
				assertReceivingCurrentUsage(t, expected, usage)
			}
			if err := closeResult(); err != nil {
				t.Fatal(err)
			}
			if budget.Usage().CurrentAccountedDecodedBytes != 0 || released != 1 {
				t.Fatal("recovered result did not close exactly once", released, budget.Usage())
			}
			if err := closeResult(); err != nil || budget.Usage().CurrentAccountedDecodedBytes != 0 || released != 1 {
				t.Fatal("repeated close changed result accounting", err, released, budget.Usage())
			}
			if shape == "collection" && (report.Archive != nil || report.Receipt != nil || report.NativeFamilies != nil || report.collection != nil) {
				t.Fatal("closed recovered collection retains payload")
			}
			budget.Observe(nil)
		})
	}
}
