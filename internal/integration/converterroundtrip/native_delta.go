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
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeDeltaRecoveryError(cause error) error {
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if errors.As(cause, &limit) {
		return errors.Join(driver.ErrConflict, cause)
	}
	return driver.ErrConflict
}

// BindNativeCaptureSetImport consumes an already completely verified set. The
// host still supplies admission, the policy and actual target identity/lifetime.
// It shares the existing mapping, history digest and transaction semantics.
func BindNativeCaptureSetImport(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile, plan driver.SourcePlanePlanV1) (driver.RestoreImportProfile, error) {
	if !archive.HasNativeDelta() {
		return driver.RestoreImportProfile{}, driver.ErrUnsupported
	}
	archive, release := archive.NativeDeltaAccountingScope()
	defer release()
	ctx = archive.WithNativeDeltaAccounting(ctx)
	return bindSourceArchive(ctx, archive, profile, plan)
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
	report, err := restoreArchiveWithProfile(ctx, work, target, lease, &bound, nil)
	if report.Archive != nil && report.Archive.SHA256() == archive.SHA256() {
		report.Archive = archive
	}
	return report, err
}

func retainNativeDeltaArchive(plan *restorePlan, archive *dumpformat.Archive, add func(model.Ref, string, any) error) error {
	manifest := archive.Manifest()
	evidence := archiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: derivedProfile(plan), SourceStore: manifest.StoreID, Members: manifest.Files, NativeCaptureFiles: map[string]dumpformat.File{}}
	if proof, ok := archive.NativeDeltaVerification(); ok {
		evidence.NativeDecodedLimitBytes = proof.Usage.LimitBytes
	}
	raw, ok, err := archive.MemberChecked("manifest.json")
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("native delta manifest missing")
	}
	evidence.Members["manifest.json"] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
	plan.blobs = map[string][]byte{}
	for name, expected := range evidence.Members {
		data, ok, err := archive.MemberChecked(name)
		if err != nil {
			return err
		}
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
		return dumpformat.ReadWithNativeDeltaAccounting(ctx, bytes.NewReader(raw))
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

func nativeOperationContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 && contexts[0] != nil {
		return contexts[0]
	}
	return context.Background()
}

// marshalNativePayload admits the exact canonical byte candidate before the
// standard serializer allocates it. The owning archive scope releases it.
func marshalNativePayload(archive *dumpformat.Archive, phase string, value any) ([]byte, error) {
	n, err := dumpformat.NativeDeltaJSONSize(value)
	if err != nil {
		return nil, err
	}
	if err := archive.ChargeNativeDeltaBytes(phase, n-1); err != nil {
		return nil, err
	}
	return json.Marshal(value)
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

// The unchanged plan codec validates a detached binary buffer and a decoded
// candidate while checking its canonical re-encoding. The caller retains only
// the resulting plan; both validation buffers end at this boundary.
func nativeSourcePlan(archive *dumpformat.Archive, binding driver.SourcePlaneImportBindingV1) (driver.SourcePlanePlanV1, error) {
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	n := uint64(len(binding.CanonicalPlan))
	if err := work.ChargeNativeDeltaBytes("source_plan_binary_detach", n); err != nil {
		return driver.SourcePlanePlanV1{}, err
	}
	if err := work.ChargeNativeDeltaBytes("source_plan_canonical_check", n); err != nil {
		return driver.SourcePlanePlanV1{}, err
	}
	if err := archive.ChargeNativeDeltaBytes("source_plan_decoded", n); err != nil {
		return driver.SourcePlanePlanV1{}, err
	}
	return binding.Plan()
}

func nativeDetachedProfile(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (driver.RestoreImportProfile, error) {
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	mode, err := driver.ParseRestoreHistoryMode(string(profile.HistoryMode))
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	profile.HistoryMode = mode
	n, err := dumpformat.NativeDeltaJSONSize(profile)
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	if err := work.ChargeNativeDeltaBytes("restore_profile_json", n-1); err != nil {
		return driver.RestoreImportProfile{}, err
	}
	if profile.SourcePlanes != nil {
		n := uint64(len(profile.SourcePlanes.CanonicalPlan))
		if err := work.ChargeNativeDeltaBytes("profile_source_plan_detach", n); err != nil {
			return driver.RestoreImportProfile{}, err
		}
		if err := work.ChargeNativeDeltaBytes("profile_source_plan_decode", n); err != nil {
			return driver.RestoreImportProfile{}, err
		}
		if err := work.ChargeNativeDeltaBytes("profile_source_plan_canonical_check", n); err != nil {
			return driver.RestoreImportProfile{}, err
		}
	}
	if err := archive.ChargeNativeDeltaJSON("detached_restore_profile", profile); err != nil {
		return driver.RestoreImportProfile{}, err
	}
	return profile.Detached()
}
