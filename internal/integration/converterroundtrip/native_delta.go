package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Keep legacy conflict classification while allowing callers to emit the exact
// measured v2 refusal when same-operation recovery exhausts its shared budget.
func nativeDeltaRecoveryError(cause error) error {
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if errors.As(cause, &limit) {
		return errors.Join(driver.ErrConflict, cause)
	}
	return driver.ErrConflict
}

// nativeDeltaBatchSize counts the exact SourceHistoryBatch JSON+newline before
// nativeTableRecord marshals full table bodies. Record.Body is a base64 JSON
// string. Only bounded scalar identities are encoded while computing its size.
func nativeDeltaBatchSize(sequence uint64, identity dumpformat.SourceIdentity, image dumpformat.NativeHistoryImage, before []driver.Record, beforeTables map[string]dumpformat.NativeTable) (uint64, error) {
	batch := driver.SourceHistoryBatch{SourceStoreID: identity.StoreID, Project: identity.Project, SourceSequence: image.Sequence, Sequence: sequence, CommitIDs: []string{image.CommitID}, EventIDs: []string{}, Changes: []driver.SourceHistoryChange{}}
	size, err := dumpformat.NativeDeltaJSONSize(batch)
	if err != nil {
		return 0, err
	}
	add := func(n uint64) error {
		if n > math.MaxUint64-size {
			return fmt.Errorf("native delta lowered batch size overflow")
		}
		size += n
		return nil
	}
	old := map[string]driver.Record{}
	for _, record := range before {
		old[record.Ref.ID] = record
	}
	changes := 0
	addChange := func(left *driver.Record, rightSize uint64, hasRight bool) error {
		if changes > 0 {
			if err := add(1); err != nil {
				return err
			}
		}
		changes++
		if err := add(2); err != nil {
			return err
		} // object braces
		if left != nil {
			n, err := dumpformat.NativeDeltaJSONSize(*left)
			if err != nil {
				return err
			}
			if err := add(uint64(len(`"before":`)) + n - 1); err != nil {
				return err
			}
		}
		if hasRight {
			if left != nil {
				if err := add(1); err != nil {
					return err
				}
			}
			if err := add(uint64(len(`"after":`)) + rightSize); err != nil {
				return err
			}
		}
		return nil
	}
	for name, table := range image.Tables {
		ref, err := nativeTableRef(identity, name, "retained-native-commit")
		if err != nil {
			return 0, err
		}
		previous, present := old[ref.ID]
		delete(old, ref.ID)
		bodySize, err := dumpformat.NativeDeltaJSONSize(nativeTableBody{Source: identity, Table: name, History: "retained-native-commit", Columns: table.Columns, Rows: table.Rows})
		if err != nil {
			return 0, err
		}
		bodySize-- // nativeTableRecord uses Marshal, without a newline
		if bodySize > (math.MaxUint64-2)/4*3-2 {
			return 0, fmt.Errorf("native delta lowered body size overflow")
		}
		encodedSize := 2 + 4*((bodySize+2)/3)
		skeletonSize, err := dumpformat.NativeDeltaJSONSize(driver.Record{Ref: ref, Kind: "beads-archive"})
		if err != nil {
			return 0, err
		}
		recordSize := skeletonSize - 1 - 4 + encodedSize // replace null Body, remove newline
		if present && reflect.DeepEqual(beforeTables[name], table) {
			continue
		}
		var left *driver.Record
		if present {
			left = &previous
		}
		if err := addChange(left, recordSize, true); err != nil {
			return 0, err
		}
	}
	for _, previous := range old {
		if err := addChange(&previous, 0, false); err != nil {
			return 0, err
		}
	}
	return size, nil
}

func chargeNativeDeltaBatch(archive *dumpformat.Archive, sequence uint64, identity dumpformat.SourceIdentity, image dumpformat.NativeHistoryImage, before []driver.Record, beforeTables map[string]dumpformat.NativeTable) error {
	if _, ok := archive.NativeDeltaRoot(); !ok {
		return nil
	}
	size, err := nativeDeltaBatchSize(sequence, identity, image, before, beforeTables)
	if err != nil {
		return err
	}
	return archive.ChargeNativeDeltaBytes("offline_batches", size)
}

func chargeNativeDeltaEnvelope(archive *dumpformat.Archive, history driver.OfflineHistory) error {
	if _, ok := archive.NativeDeltaRoot(); !ok {
		return nil
	}
	for _, part := range []struct {
		name  string
		value any
	}{{"history_baseline", history.Baseline}, {"history_gaps", history.Gaps}, {"history_boundaries", history.NativeBoundaries}} {
		if err := archive.ChargeNativeDeltaJSON(part.name, part.value); err != nil {
			return err
		}
	}
	// Payloads are charged above and in offline_batches. Count their empty
	// container punctuation in the envelope, without a second payload copy.
	history.Batches = []driver.SourceHistoryBatch{}
	history.Baseline = []driver.Record{}
	history.Gaps = []driver.SourceHistoryGap{}
	history.NativeBoundaries = nil
	return archive.ChargeNativeDeltaJSON("history_envelope", history)
}

// BindNativeCaptureSetImport consumes an already completely verified set. The
// host still supplies admission, the policy and actual target identity/lifetime.
// It shares the existing mapping, history digest and transaction semantics.
func BindNativeCaptureSetImport(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile, plan driver.SourcePlanePlanV1) (driver.RestoreImportProfile, error) {
	if _, ok := archive.NativeDeltaRoot(); !ok {
		return driver.RestoreImportProfile{}, driver.ErrUnsupported
	}
	ctx = archive.WithNativeDeltaAccounting(ctx)
	return bindSourceArchive(ctx, archive, profile, plan)
}

func RestoreNativeCaptureSetImportWithLease(ctx context.Context, archive *dumpformat.Archive, target driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile) (Report, error) {
	if _, ok := archive.NativeDeltaRoot(); !ok {
		return Report{}, driver.ErrUnsupported
	}
	ctx = archive.WithNativeDeltaAccounting(ctx)
	bound, err := profile.Detached()
	if err != nil {
		return Report{}, err
	}
	return restoreArchiveWithProfile(ctx, archive, target, lease, &bound, nil)
}

func retainNativeDeltaArchive(plan *restorePlan, archive *dumpformat.Archive, add func(model.Ref, string, any) error) error {
	manifest := archive.Manifest()
	evidence := archiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: derivedProfile(plan), SourceStore: manifest.StoreID, Members: manifest.Files, NativeCaptureFiles: map[string]dumpformat.File{}}
	if proof, ok := archive.NativeDeltaVerification(); ok {
		evidence.NativeDecodedLimitBytes = proof.Usage.LimitBytes
	}
	raw, ok := archive.Member("manifest.json")
	if !ok {
		return fmt.Errorf("native delta manifest missing")
	}
	evidence.Members["manifest.json"] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
	plan.blobs = map[string][]byte{}
	for name, expected := range evidence.Members {
		data, ok := archive.Member(name)
		if !ok || hashBytes(data) != expected.SHA256 || int64(len(data)) != expected.Size {
			return fmt.Errorf("native delta head member changed")
		}
		plan.blobs[expected.SHA256] = data
	}
	if err := archive.VisitNativeCaptureFiles(context.Background(), func(name string, data []byte) error {
		file := dumpformat.File{SHA256: hashBytes(data), Size: int64(len(data))}
		entry := struct {
			Name string          `json:"name"`
			File dumpformat.File `json:"file"`
		}{name, file}
		if err := archive.ChargeNativeDeltaJSON("retained_capture_ledger", entry); err != nil {
			return err
		}
		evidence.NativeCaptureFiles[name] = file
		return nil
	}); err != nil {
		return err
	}
	plan.nativeCapture = archive
	return add(model.RefByID("restore/archive", model.Natural64(0)), "restore-archive", evidence)
}

func readRetainedImportArchive(ctx context.Context, reader driver.Reader, raw []byte, evidence archiveEvidence) (*dumpformat.Archive, error) {
	var format struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(raw, &format); err != nil || format.Format != dumpformat.NativeCaptureSetFormat {
		return dumpformat.Read(bytes.NewReader(raw))
	}
	if evidence.NativeCaptureFiles == nil {
		rows, err := reader.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
		if err != nil || len(rows) != 1 || json.Unmarshal(rows[0].Body, &evidence) != nil {
			return nil, driver.ErrConflict
		}
	}
	if evidence.ArchiveSHA256 != hashBytes(raw) {
		return nil, driver.ErrConflict
	}
	file, ok := evidence.NativeCaptureFiles["head.tar"]
	if !ok {
		return nil, driver.ErrConflict
	}
	head, err := reader.Blob(ctx, driver.Hash(file.SHA256))
	if err != nil || int64(len(head)) != file.Size || hashBytes(head) != file.SHA256 {
		return nil, driver.ErrConflict
	}
	return dumpformat.ReadNativeCaptureSet(ctx, bytes.NewReader(raw), bytes.NewReader(head), func(ctx context.Context, n uint64) (io.ReadCloser, error) {
		name := fmt.Sprintf("parts/%020d.part", n)
		file, ok := evidence.NativeCaptureFiles[name]
		if !ok {
			return nil, driver.ErrConflict
		}
		data, err := reader.Blob(ctx, driver.Hash(file.SHA256))
		if err != nil || int64(len(data)) != file.Size || hashBytes(data) != file.SHA256 {
			return nil, driver.ErrConflict
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}, dumpformat.NativeCaptureSetOptions{DecodedLimitBytes: evidence.NativeDecodedLimitBytes, DecodedLimitOrigin: "retained recovery option"})
}

// VerifyNativeCaptureSetReadback uses independently supplied public originals
// to check every imported per-commit table at its actual import revision.
func VerifyNativeCaptureSetReadback(ctx context.Context, store driver.Store, archive *dumpformat.Archive, report Report) error {
	if _, ok := archive.NativeDeltaRoot(); !ok {
		return driver.ErrUnsupported
	}
	sequence := uint64(0)
	scopes, err := archive.NativeSourceHistoriesChecked()
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			return err
		}
		for _, image := range scope.Images {
			sequence++
			at := model.Natural64(sequence)
			reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
			if err != nil {
				return err
			}
			records, err := nativeImageRecords(identity, image.Tables)
			if err != nil {
				_ = reader.Close()
				return err
			}
			refs := make([]model.Ref, len(records))
			for i := range records {
				refs[i] = records[i].Ref
			}
			got, err := reader.Get(ctx, refs)
			closeErr := reader.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			byID := map[string]driver.Record{}
			for _, record := range got {
				byID[record.Ref.ID] = record
			}
			for _, want := range records {
				actual, ok := byID[want.Ref.ID]
				if !ok || actual.Kind != want.Kind || !bytes.Equal(actual.Body, want.Body) {
					return fmt.Errorf("native delta committed table differs at sequence %d", sequence)
				}
			}
		}
	}
	return nil
}
