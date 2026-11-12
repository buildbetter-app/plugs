package converterroundtrip

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func lowerNativeDeltaRowHistory(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile) (driver.OfflineHistory, error) {
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	if len(metadata) != 1 || len(metadata[0].Projects) != 1 {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	scope := metadata[0]
	history := driver.OfflineHistory{Source: driver.NativeTableHistorySource, Verified: true, Complete: scope.Complete, First: 1, Baseline: []driver.Record{}, Gaps: []driver.SourceHistoryGap{}, Batches: []driver.SourceHistoryBatch{}}
	if scope.HistoryCoverage != nil {
		history.NativeBoundaries = []driver.NativeHistoryBoundary{{SourceStoreID: scope.StoreID, Project: scope.Projects[0], VerifiedCommitCount: uint64(scope.ImageCount), BoundarySHA256: dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)}}
	}
	err = archive.VisitNativeDeltaRowChanges(ctx, func(commit dumpformat.NativeDeltaRowCommit) error {
		if commit.Sequence != history.Head+1 {
			return fmt.Errorf("native row source sequence differs")
		}
		batch := driver.SourceHistoryBatch{SourceStoreID: scope.StoreID, Project: scope.Projects[0], SourceSequence: commit.Sequence, Sequence: commit.Sequence, CommitIDs: []string{commit.CommitID}, EventIDs: []string{}, Changes: commit.Changes}
		// The decoder owns candidate bodies until callback return. Reserve the
		// retained batch before extending the private lowered history.
		if err := archive.ChargeNativeDeltaJSON("offline_batches", batch); err != nil {
			return err
		}
		history.Batches = append(history.Batches, batch)
		history.Head = commit.Sequence
		return nil
	}, nil)
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	if history.Head != uint64(scope.ImageCount) {
		return driver.OfflineHistory{}, fmt.Errorf("native row commit census differs")
	}
	return history, nil
}
