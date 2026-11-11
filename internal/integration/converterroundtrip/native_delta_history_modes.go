package converterroundtrip

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeDeltaHistoryEvidence(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (*driver.NativeDeltaHistoryEvidence, error) {
	if profile.NativeDeltaHistory == nil {
		if profile.HistoryMode == driver.RestoreHistoryModeFull {
			return nil, nil
		}
		return nil, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	if !archive.HasNativeDelta() || profile.NativeDeltaHistory.Validate(profile.HistoryMode) != nil || profile.ArchiveDigest != archive.SHA256() {
		return nil, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return nil, err
	}
	if len(metadata) != 1 || len(metadata[0].Projects) != 1 || metadata[0].StoreID != string(profile.SourceStoreID) {
		return nil, driver.ErrUnsupported
	}
	scope := metadata[0]
	head, ok := archive.NativeDeltaHistoryPoint(uint64(scope.ImageCount))
	headRoot, currentRoot, rootsOK := archive.NativeDeltaCheckpointRoots()
	if !ok || !rootsOK || head.RootSHA256 != headRoot {
		return nil, fmt.Errorf("authenticated native head commitment differs")
	}
	e := &driver.NativeDeltaHistoryEvidence{Format: driver.NativeDeltaHistoryEvidenceFormat, Mode: profile.HistoryMode, ArchiveSHA256: archive.SHA256(), SourceStoreID: scope.StoreID, Project: scope.Projects[0], HeadSequence: head.Sequence, HeadCommitID: head.CommitID, HeadRootSHA256: headRoot, CurrentRootSHA256: currentRoot}
	if scope.HistoryCoverage != nil {
		e.UnavailablePrefixSHA256 = dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)
	}
	if profile.HistoryMode == driver.RestoreHistoryModePartial {
		first, ok := archive.NativeDeltaHistoryPoint(profile.NativeDeltaHistory.FirstSequence)
		if !ok {
			return nil, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonNonContiguous}
		}
		e.FirstSequence, e.FirstCommitID, e.FirstRootSHA256 = first.Sequence, first.CommitID, first.RootSHA256
		if first.Sequence > 1 {
			baseline, ok := archive.NativeDeltaHistoryPoint(first.Sequence - 1)
			if !ok {
				return nil, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonNonContiguous}
			}
			e.BaselineSequence, e.BaselineCommitID, e.BaselineRootSHA256 = baseline.Sequence, baseline.CommitID, baseline.RootSHA256
			e.OmittedPrefix = &driver.SourceHistoryGap{First: 1, Last: first.Sequence - 1, Reason: "operator-selected-floor"}
		}
	}
	if err := e.Validate(profile); err != nil {
		return nil, err
	}
	if err := archive.ChargeNativeDeltaJSON("native_history_policy_evidence", e); err != nil {
		return nil, err
	}
	return e, nil
}

func lowerNativeDeltaPartialHistory(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile) (driver.OfflineHistory, error) {
	evidence, err := nativeDeltaHistoryEvidence(archive, profile)
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	history := driver.OfflineHistory{Source: driver.NativeTableHistorySource, Verified: true, Complete: metadata[0].Complete && evidence.FirstSequence == 1, First: evidence.FirstSequence, Head: evidence.HeadSequence, Baseline: []driver.Record{}, Gaps: []driver.SourceHistoryGap{}, Batches: []driver.SourceHistoryBatch{}, NativeDeltaHistory: evidence}
	if evidence.UnavailablePrefixSHA256 != "" {
		history.NativeBoundaries = []driver.NativeHistoryBoundary{{SourceStoreID: evidence.SourceStoreID, Project: evidence.Project, VerifiedCommitCount: evidence.HeadSequence - evidence.FirstSequence + 1, BoundarySHA256: evidence.UnavailablePrefixSHA256}}
	}
	if evidence.FirstSequence > 1 {
		history.BaselineVerified = true
		history.Gaps = append(history.Gaps, *evidence.OmittedPrefix)
		// Materialize exactly one authenticated predecessor. It becomes only the
		// local floor, with no source mapping or invented pre-capture commits.
		err := archive.VisitNativeDeltaTableStates(ctx, func(state dumpformat.NativeDeltaTableState) error {
			if state.Kind != "commit" || state.Sequence != evidence.BaselineSequence || state.Complete {
				return nil
			}
			records, _, err := archive.NativeDeltaTableRecords(state, "retained-native-commit", nil)
			if err == nil {
				history.Baseline = append(history.Baseline, records...)
			}
			return err
		})
		if err != nil {
			return driver.OfflineHistory{}, err
		}
	}
	var observed uint64
	err = archive.VisitNativeDeltaRowChanges(ctx, func(commit dumpformat.NativeDeltaRowCommit) error {
		observed++
		if commit.Sequence != observed {
			return fmt.Errorf("native partial source sequence differs")
		}
		if commit.Sequence < history.First {
			return nil
		}
		batch := driver.SourceHistoryBatch{SourceStoreID: evidence.SourceStoreID, Project: evidence.Project, SourceSequence: commit.Sequence, Sequence: commit.Sequence, CommitIDs: []string{commit.CommitID}, EventIDs: []string{}, Changes: commit.Changes}
		if err := archive.ChargeNativeDeltaJSON("offline_batches", batch); err != nil {
			return err
		}
		history.Batches = append(history.Batches, batch)
		return nil
	}, nil)
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	if observed != history.Head || uint64(len(history.Batches)) != history.Head-history.First+1 {
		return driver.OfflineHistory{}, fmt.Errorf("native partial commit census differs")
	}
	return history, nil
}

func nativeSnapshotHistoryEvidence(e *driver.NativeDeltaHistoryEvidence) driver.HistoryImportEvidence {
	return driver.HistoryImportEvidence{Source: driver.NativeTableHistorySource, Mode: driver.RestoreHistoryModeSnapshot, Complete: false, Floor: model.Natural64(1), Gaps: []driver.SourceHistoryGap{}, Mapping: []driver.SourceImportRevision{}, NativeDeltaHistory: e}
}

func sameNativeHistoryEvidence(left, right *driver.NativeDeltaHistoryEvidence) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && string(a) == string(b)
}
