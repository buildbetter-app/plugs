package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const NativeFamilyReportID = nativeprojection.NativeFamilyReportID

type NativeFamilyReport = nativeprojection.NativeFamilyReport
type nativeFamilyEvidence = nativeprojection.NativeFamilyEvidence
type NativeHistoryReport = nativeprojection.NativeHistoryReport

func lowerNativeTableHistory(archive *dumpformat.Archive, profile driver.RestoreImportProfile, contexts ...context.Context) (driver.OfflineHistory, error) {
	if profile.SourcePlanes == nil {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	if _, delta := archive.NativeDeltaRoot(); delta {
		if profile.HistoryMode == driver.RestoreHistoryModePartial && profile.NativeDeltaHistory != nil {
			return lowerNativeDeltaPartialHistory(nativeOperationContext(contexts), archive, profile)
		}
		if profile.HistoryMode != driver.RestoreHistoryModeFull {
			return driver.OfflineHistory{}, driver.ErrUnsupported
		}
		return lowerNativeDeltaRowHistory(nativeOperationContext(contexts), archive, profile)
	}
	if profile.HistoryMode != driver.RestoreHistoryModeFull {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	if !archive.HasNativeSourceHistories() {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	history := driver.OfflineHistory{Source: driver.NativeTableHistorySource, Verified: true, Complete: true, First: 1, Baseline: []driver.Record{}, Gaps: []driver.SourceHistoryGap{}, Batches: []driver.SourceHistoryBatch{}}
	ctx := nativeOperationContext(contexts)
	err := archive.VisitNativeSourceHistories(ctx, func(scope dumpformat.NativeHistory) error {
		if err := nativePolicyRefusal(profile.HistoryMode, archive, scope); err != nil {
			return err
		}
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			return err
		}
		if scope.HistoryCoverage != nil {
			history.Complete = false
			history.NativeBoundaries = append(history.NativeBoundaries, driver.NativeHistoryBoundary{SourceStoreID: identity.StoreID, Project: identity.Project, VerifiedCommitCount: uint64(len(scope.Images)), BoundarySHA256: dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)})
		}
		before := []driver.Record{}
		_, releaseBefore := archive.NativeDeltaAccountingScope()
		defer func() { releaseBefore() }()
		for _, image := range scope.Images {
			if err := ctx.Err(); err != nil {
				return err
			}
			afterOwner, releaseAfter := archive.NativeDeltaAccountingScope()
			after, err := nativeImageRecords(identity, image.Tables, afterOwner)
			if err != nil {
				releaseAfter()
				return err
			}
			changes, err := nativeHistoryDelta(archive, before, after)
			if err != nil {
				releaseAfter()
				return err
			}
			batch := driver.SourceHistoryBatch{SourceStoreID: identity.StoreID, Project: identity.Project, SourceSequence: image.Sequence, Sequence: history.Head + 1, CommitIDs: []string{image.CommitID}, EventIDs: []string{}, Changes: changes}
			// Both rolling images remain admitted until the detached changed
			// bodies have their retained-batch reservation.
			if err := archive.ChargeNativeDeltaJSON("offline_batches", batch); err != nil {
				releaseAfter()
				return err
			}
			history.Head++
			history.Batches = append(history.Batches, batch)
			releaseBefore()
			before, releaseBefore = after, releaseAfter
		}
		return nil
	})
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	return history, nil
}

// Recovery recomputes accounting from the authenticated original envelope and
// the admitted restore clock. Neither a stored report nor a caller's new clock
// can change which source leases were eligible in the committed import.
func recoverNativeFamilies(ctx context.Context, reader driver.Reader, profile driver.RestoreImportProfile, evidence archiveEvidence, receipt *driver.IdempotencyRecord) ([]NativeFamilyReport, []NativeSchemaReport, []NativeHistoryReport, *driver.NativeDeltaHistoryEvidence, error) {
	if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull && profile.NativeDeltaHistory == nil {
		return nil, nil, nil, nil, nil
	}
	raw, err := reader.Blob(ctx, driver.Hash(evidence.ArchiveSHA256))
	if err != nil || hashBytes(raw) != profile.ArchiveDigest {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	archive, collection, err := readRetainedReceivingArchive(ctx, reader, raw, evidence, profile)
	if err != nil {
		return nil, nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	if collection != nil {
		defer collection.close()
		plan, err := receivePlan(archive, collection.plan, nil)
		if err != nil {
			return nil, nil, nil, nil, nativeDeltaRecoveryError(err)
		}
		return recoverNativeFamiliesArchive(ctx, reader, profile, receipt, archive, &plan)
	}
	defer archive.ReleaseNativeDecoded()
	return recoverNativeFamiliesArchive(ctx, reader, profile, receipt, archive, nil)
}

func recoverNativeFamiliesArchive(ctx context.Context, reader driver.Reader, profile driver.RestoreImportProfile, receipt *driver.IdempotencyRecord, archive *dumpformat.Archive, prepared *restorePlan) ([]NativeFamilyReport, []NativeSchemaReport, []NativeHistoryReport, *driver.NativeDeltaHistoryEvidence, error) {
	archive, releaseArchive := archive.NativeDeltaAccountingScope()
	defer releaseArchive()
	ctx = archive.WithNativeDeltaAccounting(ctx)
	if !archive.HasNativeSourceHistories() {
		return nil, nil, nil, nil, nil
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	if prepared == nil {
		if archive.HasNativeCaptureCollection() {
			return nil, nil, nil, nil, driver.ErrConflict
		}
		plan, planErr := buildPlanWithProfile(archive, &profile, when, ctx)
		if planErr != nil {
			return nil, nil, nil, nil, nativeDeltaRecoveryError(planErr)
		}
		prepared = &plan
	}
	reports, schemas, histories := prepared.nativeFamilies, prepared.nativeSchemas, prepared.nativeHistories
	policy := prepared.nativeDeltaHistory
	if profile.NativeDeltaHistory != nil && profile.HistoryMode == driver.RestoreHistoryModeSnapshot {
		markers, markerErr := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
		want, encodeErr := json.Marshal(nativeSnapshotHistoryEvidence(policy))
		if markerErr != nil || encodeErr != nil || len(markers) != 1 || markers[0].Kind != "beads-archive" || !bytes.Equal(markers[0].Body, want) {
			return nil, nil, nil, nil, driver.ErrConflict
		}
		var original json.RawMessage
		for _, result := range receipt.Commit.Results {
			if result.Ref.ID == driver.OfflineHistoryEvidenceID {
				original = result.Record
			}
		}
		if !bytes.Equal(original, want) {
			return nil, nil, nil, nil, driver.ErrConflict
		}
	}
	want, err := json.Marshal(nativeFamilyEvidence{Contract: archive.Manifest().SourceFamilyContract, RestoreTime: when.UTC().Format(time.RFC3339Nano), Families: reports, Schemas: schemas, Histories: histories, NativeDeltaHistory: policy})
	if err != nil {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	var committed json.RawMessage
	for _, result := range receipt.Commit.Results {
		if result.Ref.ID == NativeFamilyReportID {
			committed = result.Record
		}
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, want) || !bytes.Equal(committed, want) {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	return reports, schemas, histories, policy, nil
}
