package nativeprojection

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func retainNativeDeltaArchive(plan *restorePlan, archive *dumpformat.Archive, add func(model.Ref, string, any) error) error {
	manifest := archive.Manifest()
	evidence := ArchiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: derivedProfile(plan), SourceStore: manifest.StoreID, Members: manifest.Files, NativeCaptureFiles: map[string]dumpformat.File{}}
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
	evidence.Members["manifest.json"] = dumpformat.File{SHA256: HashBytes(raw), Size: int64(len(raw))}
	plan.blobs = map[string][]byte{}
	for name, expected := range evidence.Members {
		data, ok, err := archive.MemberChecked(name)
		if err != nil {
			return err
		}
		if !ok || HashBytes(data) != expected.SHA256 || int64(len(data)) != expected.Size {
			return fmt.Errorf("native delta head member changed")
		}
		plan.blobs[expected.SHA256] = data
	}
	prepared, err := archive.PrepareNativeDelta(context.Background())
	if err != nil {
		return err
	}
	ledgerOwner := dumpformat.NativeDeltaBudgetFromContext(archive.WithNativeDeltaAccounting(context.Background())).Scope()
	defer ledgerOwner.Release()
	files, err := prepared.NativeCaptureFileLedger(ledgerOwner)
	if err != nil {
		return err
	}
	for _, captured := range files {
		file := dumpformat.File{SHA256: captured.SHA256, Size: int64(captured.Bytes)}
		entry := struct {
			Name string          `json:"name"`
			File dumpformat.File `json:"file"`
		}{captured.Name, file}
		if err := archive.ChargeNativeDeltaJSON("retained_capture_ledger", entry); err != nil {
			return err
		}
		evidence.NativeCaptureFiles[captured.Name] = file
	}
	plan.nativeCapture = archive
	return add(model.RefByID("restore/archive", model.Natural64(0)), "restore-archive", evidence)
}

func NativeOperationContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 && contexts[0] != nil {
		return contexts[0]
	}
	return context.Background()
}

// MarshalNativePayload admits the exact canonical byte candidate before the
// standard serializer allocates it. The owning archive scope releases it.
func MarshalNativePayload(archive *dumpformat.Archive, phase string, value any) ([]byte, error) {
	n, err := dumpformat.NativeDeltaJSONSize(value)
	if err != nil {
		return nil, err
	}
	if err := archive.ChargeNativeDeltaBytes(phase, n-1); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// NativeSourcePlan borrows canonical bytes while decoding one admitted output.
// Existing callers retain it in their archive owner; short-lived callers pass a
// child archive and release that owner when the decoded plan is no longer held.
func NativeSourcePlan(archive *dumpformat.Archive, binding driver.SourcePlaneImportBindingV1) (driver.SourcePlanePlanV1, error) {
	plan, _, err := binding.PlanWithAccounting(ProfileAccounting(archive))
	return plan, err
}
