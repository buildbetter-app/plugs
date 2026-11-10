package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

func archiveNativeHistory(archive []byte) (plugdump.NativeHistory, error) {
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if err != nil {
			return plugdump.NativeHistory{}, err
		}
		if header.Name == plugdump.NativeHistoryMember {
			var native plugdump.NativeHistory
			decoder := json.NewDecoder(reader)
			decoder.UseNumber()
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&native); err != nil {
				return plugdump.NativeHistory{}, err
			}
			return native, nil
		}
	}
}

// The evidence uses the original physical reader and typed current-root port.
// It retains SQL types and exact native string occurrences outside NativeTable;
// no names-only reconstruction or current-to-immutable graft is involved.
func readPhysicalEvidence(ctx context.Context, db *sql.DB, native plugdump.NativeHistory, original physicalcapture.WorkingImage, archive []byte) (result []byte, resultErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, conn.Close()) }()
	tables, err := physicalcapture.EvidenceTables(original.Tables)
	if err != nil {
		return nil, err
	}
	evidence := physicalcapture.Evidence{Format: "beads-physical-evidence/1", ArchiveSHA256: fmt.Sprintf("%x", sha256.Sum256(archive)), StoreID: native.StoreID, Projects: append([]string{}, native.Projects...), SQLDefaults: "not-captured",
		Working: physicalcapture.EvidenceWorkingRoot{Format: original.Format, Database: original.Database, HeadCommitID: original.HeadCommitID, WorkingRootHash: original.WorkingRootHash, ReadOnlyScope: original.ReadOnlyScope, Complete: original.Complete, Tables: tables}, Images: []physicalcapture.EvidenceImage{}}
	if !native.Complete || len(native.Gaps) != 0 {
		return nil, fmt.Errorf("typed physical evidence requires complete native history")
	}
	for _, commit := range native.Commits {
		image, err := physicalcapture.ReadCommit(ctx, conn, commit.ID)
		if err != nil {
			return nil, err
		}
		tables, err := physicalcapture.EvidenceTables(image.Tables)
		if err != nil {
			return nil, err
		}
		evidence.Images = append(evidence.Images, physicalcapture.EvidenceImage{CommitID: image.CommitID, ReadOnlyScope: image.ReadOnlyScope, Tables: tables})
	}
	final, err := physicalcapture.ReadWorkingRoot(ctx, conn, native.Head)
	if err != nil {
		return nil, err
	}
	if final.Database != original.Database || final.WorkingRootHash != original.WorkingRootHash || final.HeadCommitID != original.HeadCommitID || !final.ReadOnlyScope || !final.Complete {
		return nil, fmt.Errorf("physical evidence source root changed")
	}
	data, err := physicalcapture.MarshalEvidence(evidence)
	if err != nil {
		return nil, err
	}
	if len(data) > plugdump.MaxArchiveBytes {
		return nil, fmt.Errorf("physical evidence size limit")
	}
	return data, nil
}
