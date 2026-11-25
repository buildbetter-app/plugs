package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeDeltaRecoveryError(cause error) error {
	if cause == nil {
		return driver.ErrRestoreBindingMismatch{Field: "nativeDelta"}
	}
	var commitConflict driver.ErrRestoreCommitConflict
	var fenced driver.ErrFenced
	if errors.As(cause, &commitConflict) || errors.As(cause, &fenced) || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	var bindingMismatch driver.ErrRestoreBindingMismatch
	if errors.As(cause, &bindingMismatch) {
		return cause
	}
	if isNativeDeltaResourceLimit(cause) {
		return cause
	}
	return errors.Join(driver.ErrRestoreBindingMismatch{Field: "nativeDelta"}, cause)
}

func isNativeDeltaResourceLimit(err error) bool {
	var jsonLimit *driver.ReverseJSONLimitError
	var decodedLimit *dumpformat.NativeDeltaDecodedLimitError
	var recordLimit *dumpformat.NativeDeltaRecordLimitError
	return errors.As(err, &jsonLimit) || errors.As(err, &decodedLimit) || errors.As(err, &recordLimit)
}

// BindNativeCaptureSetImport consumes an already completely verified set. The
// host still supplies admission, the policy and actual target identity/lifetime.
// It shares the existing mapping, history digest and transaction semantics.
func BindNativeCaptureSetImport(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile, plan driver.SourcePlanePlanV1) (driver.RestoreImportProfile, error) {
	if !archive.HasNativeDelta() {
		return driver.RestoreImportProfile{}, driver.ErrUnsupported
	}
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	bound, err := bindSourceArchive(work.WithNativeDeltaAccounting(ctx), work, profile, plan)
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	// The returned binding belongs to the retained archive, separately from
	// the shorter preparation scope. Its final raw plan is copied once here.
	return nativeDetachedProfile(archive, bound)
}

func RestoreNativeCaptureSetImportWithLease(ctx context.Context, archive *dumpformat.Archive, target driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile) (Report, error) {
	if !archive.HasNativeDelta() {
		return Report{}, driver.ErrUnsupported
	}
	// Profile detachment belongs to this call, not to the retained source.
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	ctx = work.WithNativeDeltaAccounting(ctx)
	bound, err := nativeDetachedProfile(work, profile)
	if err != nil {
		return Report{}, err
	}
	report, err := restoreArchiveWithProfile(ctx, archive, target, lease, &bound, nil)
	if report.Archive != nil && report.Archive.SHA256() == archive.SHA256() {
		report.Archive = archive
	}
	return report, err
}

func readRetainedImportArchive(ctx context.Context, reader driver.Reader, raw []byte, evidence archiveEvidence) (*dumpformat.Archive, error) {
	var format struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(raw, &format); err != nil || format.Format != dumpformat.NativeCaptureSetFormat {
		return dumpformat.ReadWithNativeDeltaAccounting(ctx, bytes.NewReader(raw))
	}
	if evidence.NativeCaptureFiles == nil {
		rows, err := driver.GetNativeHeadRecords(ctx, reader, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || json.Unmarshal(rows[0].Body, &evidence) != nil {
			return nil, driver.ErrRestoreBindingMismatch{Field: "receipt.restoreArchive"}
		}
	}
	return readRetainedCaptureArchive(ctx, reader, raw, evidence, dumpformat.NativeCaptureSetOptions{
		DecodedLimitOrigin: "retained recovery option", PrepareReceiving: true,
	})
}

func readRetainedCaptureArchive(ctx context.Context, reader driver.Reader, raw []byte, evidence archiveEvidence, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
	rootFile := dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
	if evidence.ArchiveSHA256 != rootFile.SHA256 || evidence.NativeCaptureFiles["capture-set.json"] != rootFile {
		return nil, driver.ErrRestoreBindingMismatch{Field: "archiveDigest"}
	}
	var root dumpformat.NativeCaptureSet
	if err := json.Unmarshal(raw, &root); err != nil || len(evidence.NativeCaptureFiles) < 2 ||
		root.PartCount != uint64(len(evidence.NativeCaptureFiles)-2) {
		return nil, driver.ErrRestoreBindingMismatch{Field: "capture.fileCensus"}
	}
	file, ok := evidence.NativeCaptureFiles["head.tar"]
	if !ok || file != root.HeadArchive {
		return nil, driver.ErrRestoreBindingMismatch{Field: "capture.head"}
	}
	options.DecodedLimitBytes = evidence.NativeDecodedLimitBytes
	if streams, ok := reader.(driver.BlobStreamReader); ok && driver.RetainedSourceRead(ctx) {
		open := func(ctx context.Context, file dumpformat.File, max uint64) (io.ReadCloser, error) {
			return openRetainedCaptureBlob(ctx, streams, dumpformat.NativeCaptureFile{SHA256: file.SHA256, Bytes: uint64(file.Size)}, max)
		}
		head, err := open(ctx, file, dumpformat.MaxArchiveBytes)
		if err != nil {
			return nil, err
		}
		options.PrepareReceiving, options.SpoolOriginals = true, true
		archive, err := dumpformat.ReadNativeCaptureSet(ctx, bytes.NewReader(raw), head, func(ctx context.Context, n uint64) (io.ReadCloser, error) {
			name := fmt.Sprintf("parts/%020d.part", n)
			file, ok := evidence.NativeCaptureFiles[name]
			if !ok {
				return nil, driver.ErrRestoreBindingMismatch{Field: "capture.partIndex"}
			}
			return open(ctx, file, dumpformat.NativeDeltaPartBytes+dumpformat.NativeDeltaPartHeaderBytes)
		}, options)
		if err := errors.Join(err, head.Close()); err != nil {
			if archive != nil {
				archive.DiscardNativeCaptureSet()
			}
			return nil, err
		}
		return archive, nil
	}
	head, err := reader.Blob(ctx, driver.Hash(file.SHA256))
	if err != nil {
		return nil, err
	}
	if int64(len(head)) != file.Size || hashBytes(head) != file.SHA256 {
		return nil, driver.ErrRestoreBindingMismatch{Field: "capture.head"}
	}
	return dumpformat.ReadNativeCaptureSet(ctx, bytes.NewReader(raw), bytes.NewReader(head), func(ctx context.Context, n uint64) (io.ReadCloser, error) {
		name := fmt.Sprintf("parts/%020d.part", n)
		file, ok := evidence.NativeCaptureFiles[name]
		if !ok {
			return nil, driver.ErrRestoreBindingMismatch{Field: "capture.partIndex"}
		}
		data, err := reader.Blob(ctx, driver.Hash(file.SHA256))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != file.Size || hashBytes(data) != file.SHA256 {
			return nil, driver.ErrRestoreBindingMismatch{Field: "capture.partDigest"}
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}, options)
}

// historyDelta's two indexes retain one driver ordering string per input Ref.
// Their union and ordered slice borrow those same strings. Reserve both index
// domains and the longest sequential preimage before invoking the unchanged
// diff; the caller's rolling-image owners continue to own the borrowed bodies.
func nativeHistoryDelta(archive *dumpformat.Archive, before, after []driver.Record) ([]driver.SourceHistoryChange, error) {
	scratch, release := archive.NativeDeltaAccountingScope()
	defer release()
	var maximum uint64
	for _, records := range [][]driver.Record{before, after} {
		for _, record := range records {
			n := uint64(len("0:key:")) + uint64(len(record.Ref.Key))
			if record.Ref.IsID() {
				n = uint64(len("0:id::")) + uint64(len(record.Ref.ID)) + uint64(len(record.Ref.Branch.String()))
			}
			if err := scratch.ChargeNativeDeltaBytes("history_diff_index_keys", n); err != nil {
				return nil, err
			}
			if n-2 > maximum {
				maximum = n - 2
			}
		}
	}
	if err := scratch.ChargeNativeDeltaBytes("history_diff_key_preimage", maximum); err != nil {
		return nil, err
	}
	return historyDelta(before, after), nil
}

// The unchanged driver digest serializes the complete lowered request. Admit
// that transient exact JSON buffer before calling it, then drop it after hash.
func nativeHistoryDigest(archive *dumpformat.Archive, history driver.OfflineHistory) (string, error) {
	scratch, release := archive.NativeDeltaAccountingScope()
	defer release()
	n, err := dumpformat.NativeDeltaJSONSize(history)
	if err != nil {
		return "", err
	}
	if err := scratch.ChargeNativeDeltaBytes("offline_history_digest_preimage", n-1); err != nil {
		return "", err
	}
	return driver.OfflineHistoryDigest(history)
}

func nativeDetachedProfile(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (driver.RestoreImportProfile, error) {
	detached, _, err := nativeprojection.DetachedProfile(archive, profile)
	return detached, err
}
