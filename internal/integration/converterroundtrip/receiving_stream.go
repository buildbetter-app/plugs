package converterroundtrip

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func nativeReceivingStreamDigest(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile, when time.Time) (_ string, err error) {
	stream, err := historyverify.PrepareNativeArchive(ctx, archive, profile, when)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, stream.Close()) }()
	return stream.Digest(), nil
}
func nativeReceivingRequestHash(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (string, error) {
	return profile.RequestHashWithAccounting(nativeprojection.ProfileAccounting(archive))
}
func nativeReceivingStage(tx driver.WriteTx, proof historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error) {
	for _, newStage := range []func(driver.WriteTx, historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error){memory.NewOfflineHistoryStreamStage, sqlite.NewOfflineHistoryStreamStage, postgres.NewOfflineHistoryStreamStage} {
		stage, err := newStage(tx, proof)
		if err == nil {
			return stage, nil
		}
		if !errors.Is(err, driver.ErrUnsupported) {
			return nil, err
		}
	}
	return nil, driver.ErrUnsupported
}

// restoreNativeReceivingStream is the production admitted native receiving
// route. The verifier owns the transcript and checkpoint; this host publishes
// the SAME actual planner image under complete locks and the engine's port.
func restoreNativeReceivingStream(ctx context.Context, archive *dumpformat.Archive, target driver.Store, suppliedLease driver.ActiveLease, profile driver.RestoreImportProfile, plan nativeprojection.Plan, sourceLogical []byte) (report Report, restoreErr error) {
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil || when.IsZero() || sourceLogical == nil {
		return report, driver.ErrConflict
	}
	stream, err := historyverify.PrepareNativeArchive(ctx, archive, profile, when)
	if err != nil {
		return report, err
	}
	defer func() { restoreErr = errors.Join(restoreErr, stream.Close()) }()
	return restorePreparedNativeReceivingStream(ctx, archive, target, suppliedLease, profile, plan, sourceLogical, stream)
}

// Both native input shapes publish through this same row-granular receiver,
// lock set, transaction, stage and original-key receipt. The collection issuer
// supplies a whole verified stream/Plan, never a second receiving algorithm.
func restorePreparedNativeReceivingStream(ctx context.Context, archive *dumpformat.Archive, target driver.Store, suppliedLease driver.ActiveLease, profile driver.RestoreImportProfile, plan nativeprojection.Plan, sourceLogical []byte, stream *historyverify.NativeArchiveStream) (report Report, restoreErr error) {
	report.Archive = archive
	meta, err := target.Meta(ctx)
	report.Target = meta
	if err != nil {
		return report, err
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil || when.IsZero() || sourceLogical == nil {
		return report, driver.ErrConflict
	}
	image, err := plan.Snapshot()
	if err != nil {
		return report, err
	}
	report.NativeFamilies, report.NativeSchemas, report.NativeHistories = image.NativeFamilies, image.NativeSchemas, image.NativeHistories
	report.IdempotencyKey = profile.IdempotencyKey
	report.RequestHash, err = nativeReceivingRequestHash(archive, profile)
	if err != nil {
		return report, err
	}
	lease := suppliedLease
	if lease == nil {
		var release func() error
		ctx, lease, release, err = HoldRestoreLease(ctx, target, "plug-converter-restore")
		if err != nil {
			return report, err
		}
		defer func() { restoreErr = errors.Join(restoreErr, release()) }()
	}
	capability, err := driver.RequireWorkflowCheckpointStore(target, lease)
	if err != nil {
		return report, err
	}
	request := driver.OfflineHistoryStreamRequest{Current: meta, Profile: profile, Header: stream.Header(), When: when, Lifetime: capability.WorkflowCheckpointLifetime(), Limits: historyverify.NativeArchiveLimits()}
	proof, err := stream.Seal(ctx, request, plan)
	if err != nil {
		return report, err
	}
	requests, err := stream.LockRequests(ctx, plan, archive)
	if err != nil {
		return report, err
	}
	// The detached image owns the borrowed bodies. These separate publication
	// arrays also retain record/change/result/lock-key slots and per-record paths.
	// Admit them before acquiring a writer or allocating any publication arrays.
	if err = archive.ChargeNativeDeltaBytes("receiving_publication_slots", uint64(len(image.Records)+2)*1024+uint64(len(requests))*256); err != nil {
		return report, err
	}
	if err = archive.ChargeNativeDeltaBytes("receiving_result_revisions", uint64(len(image.Records))*32); err != nil {
		return report, err
	}
	revisions := make([]model.Revision, len(image.Records))
	for i, record := range image.Records {
		revisions[i] = stream.RecordRevision(record.Ref)
	}
	tx, err := target.Write(ctx, lease)
	if err != nil {
		return report, err
	}
	defer func() { restoreErr = errors.Join(restoreErr, tx.Close()) }()
	if _, err = tx.Lock(ctx, requests); err != nil {
		return report, err
	}
	if _, err = tx.Head(ctx); err != nil {
		return report, err
	}
	stage, err := nativeReceivingStage(tx, proof)
	if err != nil {
		return report, err
	}
	source, err := stream.OpenReceivingSource(ctx)
	if err != nil {
		return report, err
	}
	prepared, err := driver.PrepareOfflineHistoryStream(ctx, request, source, stage)
	if err != nil {
		return report, err
	}
	defer func() { restoreErr = errors.Join(restoreErr, prepared.Close()) }()
	complete, err := prepared.Completion()
	if err != nil {
		return report, err
	}
	restored := complete.Meta
	continuation, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		return report, driver.ErrUnsupported
	}
	if err = continuation.RestoreAllocators(ctx, image.Allocators); err != nil {
		return report, err
	}
	binding, err := prepareSourcePublication(ctx, capability, lease, tx, profile, restored, sourceLogical)
	if err != nil {
		return report, err
	}
	body, err := nativeprojection.MarshalNativePayload(archive, "receiving_source_publication", binding)
	if err != nil {
		return report, err
	}
	image.Records = append(image.Records, driver.Record{Ref: model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0)), Kind: "instance_config", Body: body})
	report.SourcePlanes = &binding
	changes := make([]model.ChangeRecord, 0, len(image.Records)+1)
	results := make([]model.OpResult, 0, len(image.Records))
	for i, record := range image.Records {
		if err = tx.Put(ctx, record); err != nil {
			return report, err
		}
		revision := model.Natural64(0)
		if i < len(revisions) {
			revision = revisions[i]
		}
		if revision.IsZero() {
			revision = restored.Head
		}
		results = append(results, model.OpResult{Ref: record.Ref, Revision: revision, Record: record.Body})
		changes = append(changes, model.ChangeRecord{Ref: record.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}})
	}
	changes = append(changes, model.ChangeRecord{Ref: model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}})
	for _, row := range image.Derived {
		if err = tx.PutDerived(ctx, row.Table, nil, []driver.DerivedRow{{Key: row.Key, Value: row.Value}}); err != nil {
			return report, err
		}
	}
	for hash, data := range image.Blobs {
		actual, e := tx.PutBlob(ctx, data)
		if e != nil {
			return report, e
		}
		if string(actual) != hash {
			return report, fmt.Errorf("restore blob digest mismatch")
		}
	}
	if archive.HasNativeCaptureCollection() {
		// Each callback buffer is separately charged by the verifier. The writer
		// retains its own transaction clone through commit/abort, admitted first.
		err = stream.VisitOriginalFiles(ctx, func(_ string, data []byte) error {
			if e := archive.ChargeNativeDeltaBytes("receiving_collection_original_blobs", uint64(len(data))); e != nil {
				return e
			}
			actual, e := tx.PutBlob(ctx, data)
			if e != nil {
				return e
			}
			if string(actual) != hashBytes(data) {
				return fmt.Errorf("native capture blob digest differs")
			}
			return nil
		})
	} else {
		var capture *dumpformat.NativeDeltaPrepared
		capture, err = archive.PrepareNativeDelta(ctx)
		if err == nil {
			err = capture.Visit(ctx, dumpformat.NativeDeltaVisitor{File: func(_ string, data []byte) error {
				actual, e := tx.PutBlob(ctx, data)
				if e != nil {
					return e
				}
				if string(actual) != hashBytes(data) {
					return fmt.Errorf("native capture blob digest differs")
				}
				return nil
			}})
		}
	}
	if err != nil {
		return report, err
	}
	if err = tx.BumpSentinels(ctx, image.Sentinels); err != nil {
		return report, err
	}
	commit, err := prepared.Commit(ctx, driver.ChangesetHeader{Incarnation: restored.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: profile.Admission.Actor, Chain: []string{}, Method: profile.Method, AgentLabel: profile.AgentLabel(), IdempotencyKey: report.IdempotencyKey, RequestHash: report.RequestHash, Response: model.Commit{Revision: restored.Head, Results: results, Warnings: []model.Warning{}}, Changes: changes, LockKeys: lockKeys(requests)})
	check := Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}
	if err != nil {
		recovered, lookupErr := recoverNativeReceivingIndeterminate(ctx, target, profile, check, err)
		if lookupErr != nil {
			return report, lookupErr
		}
		if recovered.collection == nil {
			recovered.Archive = archive
		}
		recovered.Target = meta
		recovered.Recovered = true
		return recovered, nil
	}
	if commit.Kind != model.ChangesetRestore || commit.Incarnation != restored.Incarnation || commit.Revision != restored.Head {
		return report, fmt.Errorf("restore commit did not retain the completed stream")
	}
	report.Restored = restored
	receiverView, err := receivePlan(archive, plan, nil)
	if err != nil {
		return report, err
	}
	recovered, err := recoverSourceImport(ctx, target, profile, check, &receivingEvidence{archive: archive, plan: &receiverView, logical: sourceLogical, stream: stream, requestHash: report.RequestHash})
	if err != nil {
		return report, err
	}
	report.Receipt = recovered.Receipt
	return report, nil
}

// recoverNativeReceivingIndeterminate resolves only the actual original key.
// The consumed stream is never replayed; cancellation cannot turn an uncertain
// durable outcome into a determinate rollback or a new receiving transaction.
func recoverNativeReceivingIndeterminate(ctx context.Context, target driver.Store, profile driver.RestoreImportProfile, check Verification, cause error) (Report, error) {
	var uncertain driver.ErrIndeterminate
	var uncertainPointer *driver.ErrIndeterminate
	if !errors.As(cause, &uncertain) && !errors.As(cause, &uncertainPointer) {
		return Report{}, cause
	}
	key := uncertain.IdempotencyKey
	if uncertainPointer != nil {
		key = uncertainPointer.IdempotencyKey
	}
	if key != profile.IdempotencyKey {
		return Report{}, cause
	}
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	recovered, err := RecoverImport(recoveryCtx, target, profile, check)
	if err != nil {
		return Report{}, cause
	}
	return recovered, nil
}
