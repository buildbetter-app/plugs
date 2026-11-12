package converterroundtrip

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeDeltaHistoryEvidence(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (*driver.NativeDeltaHistoryEvidence, error) {
	return nativeprojection.NativeDeltaHistoryEvidence(archive, profile)
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
	return nativeprojection.NativeSnapshotHistoryEvidence(e)
}

func sameNativeHistoryEvidence(left, right *driver.NativeDeltaHistoryEvidence) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && string(a) == string(b)
}
