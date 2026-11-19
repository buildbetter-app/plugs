package converterroundtrip

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
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
func restoreNativeReceivingStream(ctx context.Context, archive *dumpformat.Archive, target driver.Store, suppliedLease driver.ActiveLease, profile driver.RestoreImportProfile, plan nativeprojection.Plan, sourceLogical []byte, retainedArchive *dumpformat.Archive, releasePlan func()) (report Report, restoreErr error) {
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil || when.IsZero() || sourceLogical == nil {
		return report, driver.ErrConflict
	}
	stream, err := historyverify.PrepareNativeArchive(ctx, archive, profile, when)
	if err != nil {
		return report, err
	}
	defer func() { restoreErr = errors.Join(restoreErr, stream.Close()) }()
	return restorePreparedNativeReceivingStream(ctx, archive, target, suppliedLease, profile, plan, sourceLogical, stream, retainedArchive, func() error {
		if err := stream.Close(); err != nil {
			return err
		}
		releasePlan()
		return nil
	})
}

// Both native input shapes publish through this same row-granular receiver,
// lock set, transaction, stage and original-key receipt. The collection issuer
// supplies a whole verified stream/Plan, never a second receiving algorithm.
func restorePreparedNativeReceivingStream(ctx context.Context, archive *dumpformat.Archive, target driver.Store, suppliedLease driver.ActiveLease, profile driver.RestoreImportProfile, plan nativeprojection.Plan, sourceLogical []byte, stream *historyverify.NativeArchiveStream, retainedArchive *dumpformat.Archive, releasePrepared func() error, stageFactories ...func(driver.WriteTx, historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error)) (report Report, restoreErr error) {
	recoveryArchive := retainedArchive
	outputOwned := false
	defer func() {
		if restoreErr != nil && !outputOwned {
			// Publication snapshots are transient until the exact result is
			// admitted. Preserve durable identity, never unowned payload views.
			report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes, report.Receipt = nil, nil, nil, nil, nil
		}
	}()
	archive, releasePublication := archive.NativeDeltaAccountingScope()
	defer releasePublication()
	ctx = archive.WithNativeDeltaAccounting(ctx)
	report.Archive = retainedArchive
	meta, err := target.Meta(ctx)
	report.Target = meta
	if err != nil {
		return report, err
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil || when.IsZero() || sourceLogical == nil {
		return report, driver.ErrConflict
	}
	image, err := plan.SnapshotWithAccounting(archive)
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
	tx, err := target.Write(ctx, lease)
	if err != nil {
		return report, err
	}
	defer func() { restoreErr = errors.Join(restoreErr, tx.Close()) }()
	certifyGraph, err := prepareImportGraph(ctx, tx, image.Records, archive)
	if err != nil {
		return report, err
	}
	var facts []driver.ReadFact
	if certifyGraph {
		facts = []driver.ReadFact{pipeline.GraphValidityReadFact}
	}
	request := driver.OfflineHistoryStreamRequest{Current: meta, Profile: profile, Header: stream.Header(), When: when, Lifetime: capability.WorkflowCheckpointLifetime(), Limits: historyverify.NativeArchiveLimits()}
	proof, err := stream.Seal(ctx, request, plan, facts...)
	if err != nil {
		return report, err
	}
	requests, err := stream.LockRequests(ctx, plan, archive)
	if err != nil {
		return report, err
	}
	if certifyGraph {
		requests = importGraphLocks(requests)
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
	if _, err = tx.Lock(ctx, requests); err != nil {
		return report, err
	}
	if _, err = tx.Head(ctx); err != nil {
		return report, err
	}
	newStage := nativeReceivingStage
	if len(stageFactories) != 0 {
		newStage = stageFactories[0]
	}
	stage, err := newStage(tx, proof)
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
		if err = putImportRecord(ctx, tx, record, certifyGraph); err != nil {
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
	if certifyGraph {
		if err = tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
			return report, err
		}
		if err = tx.BumpReadFacts(ctx, facts); err != nil {
			return report, err
		}
	}
	commit, err := prepared.Commit(ctx, driver.ChangesetHeader{Incarnation: restored.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: profile.Admission.Actor, Chain: []string{}, Method: profile.Method, AgentLabel: profile.AgentLabel(), IdempotencyKey: report.IdempotencyKey, RequestHash: report.RequestHash, Response: model.Commit{Revision: restored.Head, Results: results, Warnings: []model.Warning{}}, Changes: changes, LockKeys: lockKeys(requests)})
	check := Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}
	commitCause := err
	if commitCause != nil {
		if !nativeReceivingIndeterminateMatches(commitCause, profile.IdempotencyKey) {
			return report, commitCause
		}
		// Resolve the original durable key through the SAME retained prepared
		// evidence and result handoff. No reconstructed source owner is created
		// beneath the publication scope, and no consumed stream is replayed.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		report.Recovered = true
	} else if commit.Kind != model.ChangesetRestore || commit.Incarnation != restored.Incarnation || commit.Revision != restored.Head {
		return report, fmt.Errorf("restore commit did not retain the completed stream")
	}
	report.Restored = restored
	report.GraphCertification = importGraphReport(certifyGraph, restored)
	receiverView, err := receivePlan(archive, plan, nil)
	if err != nil {
		if commitCause != nil {
			report.Restored = driver.Meta{}
			return report, commitCause
		}
		return report, nativeReceivingPostCommitError(report, err)
	}
	evidence := &receivingEvidence{archive: recoveryArchive, plan: &receiverView, logical: sourceLogical, stream: stream, requestHash: report.RequestHash}
	// Receipt/history/family verification is the last consumer of the prepared
	// plan and publication image. It must precede their ownership handoff.
	evidenceVerified := false
	evidence.beforeInspection = func(inspectCtx context.Context, recovered *Report) (context.Context, error) {
		evidenceVerified = true
		// Retain only fields exported by the result. The logical input is still
		// owned by the caller's preparation, independently of these dead buffers.
		output, releaseOutput := retainedArchive.NativeDeltaAccountingScope()
		defer releaseOutput()
		retained := report
		retained.Archive, retained.Receipt = nil, recovered.Receipt
		// The verification view is consumed; the public result already has its
		// independently detached family views. Do not keep a second dead copy.
		recovered.NativeFamilies, recovered.NativeSchemas, recovered.NativeHistories, recovered.NativeDeltaHistory = nil, nil, nil, nil
		if e := output.ChargeNativeDeltaJSON("receiving_retained_result", retained); e != nil {
			report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes = nil, nil, nil, nil
			return inspectCtx, e
		}
		// Once admitted, this populated known-commit report owns its retained
		// bytes even if writer/stream cleanup fails before inspection.
		report.Receipt = recovered.Receipt
		if budget := dumpformat.NativeDeltaBudgetFromContext(output.WithNativeDeltaAccounting(inspectCtx)); budget != nil {
			budget.Retain()
		}
		outputOwned = true
		if e := tx.Close(); e != nil {
			return inspectCtx, e
		}
		image, results, changes, requests, revisions = nativeprojection.Snapshot{}, nil, nil, nil, nil
		receiverView = restorePlan{}
		evidence.plan, evidence.stream = nil, nil
		commit = model.Changeset{}
		body = nil
		plan.Close()
		releasePublication()
		if e := releasePrepared(); e != nil {
			return inspectCtx, e
		}
		return recoveryArchive.WithNativeDeltaAccounting(inspectCtx), nil
	}
	recovered, err := recoverSourceImport(ctx, target, profile, check, evidence)
	if err != nil {
		if commitCause != nil && !evidenceVerified {
			report.Restored = driver.Meta{}
			return report, commitCause
		}
		return report, nativeReceivingPostCommitError(report, err)
	}
	report.Receipt = recovered.Receipt
	return report, nil
}

// recoverNativeReceivingIndeterminate resolves only the actual original key.
// The consumed stream is never replayed; cancellation cannot turn an uncertain
// durable outcome into a determinate rollback or a new receiving transaction.
func nativeReceivingIndeterminateMatches(cause error, key string) bool {
	var value driver.ErrIndeterminate
	var pointer *driver.ErrIndeterminate
	if errors.As(cause, &value) {
		return value.IdempotencyKey == key
	}
	return errors.As(cause, &pointer) && pointer.IdempotencyKey == key
}

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
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	recovered, err := RecoverImport(recoveryCtx, target, profile, check)
	if err != nil {
		return Report{}, cause
	}
	return recovered, nil
}

// Publication already succeeded; a verification failure must not classify the
// durable import as a conflict or invite replay under another receipt key.
func nativeReceivingPostCommitError(report Report, cause error) error {
	return fmt.Errorf("native import committed at %s; receipt key %q; post-commit verification failed: %v", report.Restored.Head, report.IdempotencyKey, cause)
}
