package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

func runNativeDelta(ctx context.Context, db *sql.DB, conn *sql.Conn, tx *sql.Tx, closeDriver func() error, source observedNativeSource, inventory plugdump.Inventory, working physicalcapture.WorkingImage, originalLog plugdump.SourceCommitLog, output string, ownedOutputs *[]string) (resultErr error) {
	setDirectory := filepath.Join(output, "capture-set")
	if err := os.Mkdir(setDirectory, 0700); err != nil {
		return err
	}
	*ownedOutputs = append(*ownedOutputs, setDirectory)
	prepared, err := plugdump.PrepareNativeDelta(ctx, source, inventory, working, originalLog, setDirectory)
	if err != nil {
		return err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, prepared.Abort())
		}
	}()
	finalLog, err := source.ReadSourceCommits(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(originalLog, finalLog) {
		return plugdump.ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
	}
	if err := prepared.VerifyIgnore(ctx, source); err != nil {
		return err
	}
	evidenceConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	finalWorking, readErr := physicalcapture.ReadWorkingRoot(ctx, evidenceConn, originalLog.Head)
	if err := errors.Join(readErr, evidenceConn.Close()); err != nil {
		return err
	}
	if !finalWorking.ReadOnlyScope || !finalWorking.Complete || finalWorking.Database != working.Database || finalWorking.HeadCommitID != working.HeadCommitID || finalWorking.WorkingRootHash != working.WorkingRootHash {
		return plugdump.ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
	}
	inventory.NativeHistoryContract = plugdump.NativeDeltaFormat
	inventoryBytes, err := plugdump.MarshalNativeInventory(inventory)
	if err != nil {
		return err
	}
	evidence := plugdump.NativeDeltaEvidence{Format: plugdump.NativeDeltaEvidenceFormat, CaptureSetSHA256: fmt.Sprintf("%x", sha256.Sum256(prepared.SetBytes)), StoreID: prepared.Set.StoreID, Projects: prepared.Set.Projects, SQLDefaults: "not-captured", Working: plugdump.NativeDeltaWorkingIdentity{Format: working.Format, Database: working.Database, HeadCommitID: working.HeadCommitID, WorkingRootHash: working.WorkingRootHash, ReadOnlyScope: working.ReadOnlyScope, Complete: working.Complete}, Totals: prepared.Set.Totals, HeadRootSHA256: prepared.HeadRootSHA256, CurrentRootSHA256: prepared.CurrentRootSHA256}
	evidenceBytes, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	// No file containing a receivable set root exists while source handles live.
	if err := tx.Rollback(); err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	if err := closeDriver(); err != nil {
		return err
	}
	for _, file := range []struct {
		name string
		data []byte
	}{{"inventory.json", inventoryBytes}, {"capture-set/head.tar", prepared.HeadArchive}, {"physical-evidence.json", evidenceBytes}, {"capture-set/capture-set.json", prepared.SetBytes}} {
		path := filepath.Join(output, file.name)
		*ownedOutputs = append(*ownedOutputs, path)
		write := writeExclusive
		if file.name == "capture-set/capture-set.json" {
			write = writeNativeDeltaRoot
		}
		if err := write(path, file.data); err != nil {
			return err
		}
	}
	counts := map[string]int{}
	for _, source := range prepared.Manifest.Sources {
		counts[source.Family] = source.Rows
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"capture": "published-validated", "native_history_contract": plugdump.NativeDeltaFormat, "source_family_contract": prepared.Manifest.SourceFamilyContract, "families": counts, "verified_commit_count": prepared.Set.Totals.Commits, "full_image_bytes": prepared.Set.Totals.FullImageBytes, "capture_set_sha256": evidence.CaptureSetSHA256, "scope": "capture-only; receiver reconstruction/import separate"})
}

// Stage outside the three-entry set directory, sync and close the complete
// bytes, then create the final name atomically and exclusively. The hard link
// stays on the same filesystem because the staging directory is its parent.
// Readers never see an open/partly written final root, and an existing final
// pathname is never overwritten. Legacy writeExclusive is unchanged.
func writeNativeDeltaRoot(path string, data []byte) (resultErr error) {
	file, err := os.CreateTemp(filepath.Dir(filepath.Dir(path)), ".native-capture-root-*")
	if err != nil {
		return err
	}
	staged := file.Name()
	closed := false
	defer func() {
		if !closed {
			resultErr = errors.Join(resultErr, file.Close())
		}
		resultErr = errors.Join(resultErr, os.Remove(staged))
	}()
	if n, err := file.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return err
	}
	err = file.Close()
	closed = true
	if err != nil {
		return err
	}
	return os.Link(staged, path)
}
