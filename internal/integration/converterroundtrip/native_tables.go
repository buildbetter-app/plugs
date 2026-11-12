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
	if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	if _, delta := archive.NativeDeltaRoot(); delta {
		return lowerNativeDeltaRowHistory(nativeOperationContext(contexts), archive, profile)
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
func recoverNativeFamilies(ctx context.Context, reader driver.Reader, profile driver.RestoreImportProfile, evidence archiveEvidence, receipt *driver.IdempotencyRecord) ([]NativeFamilyReport, []NativeSchemaReport, []NativeHistoryReport, error) {
	if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull {
		return nil, nil, nil, nil
	}
	raw, err := reader.Blob(ctx, driver.Hash(evidence.ArchiveSHA256))
	if err != nil || hashBytes(raw) != profile.ArchiveDigest {
		return nil, nil, nil, driver.ErrConflict
	}
	archive, err := readRetainedImportArchive(ctx, reader, raw, evidence)
	if err != nil {
		return nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	defer archive.ReleaseNativeDecoded()
	archive, releaseArchive := archive.NativeDeltaAccountingScope()
	defer releaseArchive()
	ctx = archive.WithNativeDeltaAccounting(ctx)
	if !archive.HasNativeSourceHistories() {
		return nil, nil, nil, nil
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		return nil, nil, nil, driver.ErrConflict
	}
	_, reports, err := nativeCurrentRecords(archive, when, ctx)
	if err != nil {
		return nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	var schemas []NativeSchemaReport
	if archive.Manifest().MainVersion != 72 {
		plan, planErr := buildPlanWithProfile(archive, &profile, when, ctx)
		if planErr != nil {
			return nil, nil, nil, nativeDeltaRecoveryError(planErr)
		}
		schemas = plan.nativeSchemas
	}
	histories, err := nativeHistoryAccounting(archive)
	if err != nil {
		return nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	want, err := json.Marshal(nativeFamilyEvidence{Contract: archive.Manifest().SourceFamilyContract, RestoreTime: when.UTC().Format(time.RFC3339Nano), Families: reports, Schemas: schemas, Histories: histories})
	if err != nil {
		return nil, nil, nil, driver.ErrConflict
	}
	var committed json.RawMessage
	for _, result := range receipt.Commit.Results {
		if result.Ref.ID == NativeFamilyReportID {
			committed = result.Record
		}
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, want) || !bytes.Equal(committed, want) {
		return nil, nil, nil, driver.ErrConflict
	}
	return reports, schemas, histories, nil
}
