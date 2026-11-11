package nativeprojection

import (
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeDeltaHistoryEvidence derives the selected mode provenance from the authenticated archive.
func NativeDeltaHistoryEvidence(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (*driver.NativeDeltaHistoryEvidence, error) {
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

// NativeSnapshotHistoryEvidence marks the current-only import without source revision claims.
func NativeSnapshotHistoryEvidence(e *driver.NativeDeltaHistoryEvidence) driver.HistoryImportEvidence {
	return driver.HistoryImportEvidence{Source: driver.NativeTableHistorySource, Mode: driver.RestoreHistoryModeSnapshot, Complete: false, Floor: model.Natural64(1), Gaps: []driver.SourceHistoryGap{}, Mapping: []driver.SourceImportRevision{}, NativeDeltaHistory: e}
}
