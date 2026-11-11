package plugdump

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
)

func buildNativeDeltaHead(ctx context.Context, src Source, inventory Inventory, log SourceCommitLog, current, head map[string]NativeTable, ignore NativeTable) ([]byte, Manifest, *NativeHistoryCoverage, error) {
	if inventory.SourceFamilyContract != SourceFamilyContract {
		return nil, Manifest{}, nil, fmt.Errorf("native delta requires source family contract")
	}
	inventory.NativeHistoryContract = NativeDeltaFormat
	archive, err := build(ctx, src, inventory, &nativeDeltaHead{Log: log, Current: current, Head: head, Ignore: ignore})
	if err != nil {
		return nil, Manifest{}, nil, err
	}
	manifest, err := ValidateNativeDeltaHead(bytes.NewReader(archive))
	if err != nil {
		return nil, Manifest{}, nil, err
	}
	var coverage *NativeHistoryCoverage
	if len(log.Boundaries) > 0 {
		reader := tar.NewReader(bytes.NewReader(archive))
		var sourceBytes []byte
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, Manifest{}, nil, err
			}
			if header.Name == SourceHistoryMember {
				sourceBytes, err = io.ReadAll(io.LimitReader(reader, MaxArchiveBytes+1))
				if err != nil || len(sourceBytes) > MaxArchiveBytes {
					return nil, Manifest{}, nil, fmt.Errorf("native delta events attachment limit")
				}
				break
			}
		}
		var history SourceHistory
		if err := decodeJSON(sourceBytes, &history); err != nil {
			return nil, Manifest{}, nil, err
		}
		commits, err := historyCommitOrder(log)
		if err != nil {
			return nil, Manifest{}, nil, err
		}
		coverage = &NativeHistoryCoverage{Format: NativeHistoryCoverageFormat, Status: "verified-to-boundary", VerifiedCommitCount: uint64(len(commits)), Boundaries: log.Boundaries, EventsAttachment: NativeEventsAttachment{Member: SourceHistoryMember, SHA256: digest(sourceBytes), Rows: uint64(len(history.Events))}}
	}
	return archive, manifest, coverage, nil
}
