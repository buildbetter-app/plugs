package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// RecoverBoundSourceImport discovers only the protected binding, then verifies
// the original receipt, retained archive and current joint namespace through
// RecoverImport. The returned profile is detached data, never caller authority.
func RecoverBoundSourceImport(ctx context.Context, target driver.Store) (driver.RestoreImportProfile, Report, bool, error) {
	meta, err := target.Meta(ctx)
	if err != nil {
		return driver.RestoreImportProfile{}, Report{}, false, err
	}
	r, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return driver.RestoreImportProfile{}, Report{}, false, err
	}
	rows, readErr := r.Get(ctx, []model.Ref{model.RefByID(importBindingRecordID, model.Natural64(0))})
	closeErr := r.Close()
	if readErr != nil {
		return driver.RestoreImportProfile{}, Report{}, false, readErr
	}
	if closeErr != nil {
		return driver.RestoreImportProfile{}, Report{}, false, closeErr
	}
	if len(rows) == 0 {
		return driver.RestoreImportProfile{}, Report{}, false, nil
	}
	var binding importBindingEvidence
	if len(rows) != 1 || rows[0].Kind != "instance_config" || json.Unmarshal(rows[0].Body, &binding) != nil {
		return driver.RestoreImportProfile{}, Report{}, false, driver.ErrConflict
	}
	profile, err := binding.Profile.Detached()
	if err != nil {
		return driver.RestoreImportProfile{}, Report{}, false, err
	}
	if profile.SourcePlanes == nil {
		return driver.RestoreImportProfile{}, Report{}, false, nil
	}
	request, err := profile.RequestHash()
	if err != nil || request != binding.RequestHash {
		return driver.RestoreImportProfile{}, Report{}, false, driver.ErrConflict
	}
	report, err := RecoverImport(ctx, target, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
	return profile, report, true, err
}

func recoverSourceImport(ctx context.Context, target driver.Store, profile driver.RestoreImportProfile, check Verification) (Report, error) {
	capability, ok := target.(driver.SourcePlaneSnapshotStore)
	if !ok {
		return Report{}, driver.ErrUnsupported
	}
	bound, err := profile.Detached()
	if err != nil || bound.SourcePlanes == nil || check.ArchiveSHA256 != bound.ArchiveDigest || check.DerivedSHA256 != bound.DerivedDigest {
		return Report{}, driver.ErrConflict
	}
	plan, err := bound.SourcePlanes.Plan()
	if err != nil || len(plan.Providers) != 1 {
		return Report{}, driver.ErrConflict
	}
	request, err := bound.RequestHash()
	if err != nil {
		return Report{}, err
	}
	meta, err := target.Meta(ctx)
	if err != nil {
		return Report{}, err
	}
	r, err := capability.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: meta.Incarnation}, []string{plan.Providers[0].Namespace})
	if err != nil {
		return Report{}, err
	}
	defer r.Close()
	meta.Head = *r.Snapshot().At
	report, err := recoverImportReader(ctx, r, meta, bound, check, request)
	if err != nil {
		return Report{}, err
	}
	var binding driver.SourcePlaneCommitBindingV1
	var receiptBytes []byte
	for _, result := range report.Receipt.Commit.Results {
		if result.Ref.ID == driver.SourcePlaneCommitRecordID {
			if receiptBytes != nil || !result.Ref.Branch.IsZero() || result.Revision.String() != "1" {
				return Report{}, driver.ErrConflict
			}
			receiptBytes = result.Record
		}
	}
	if len(receiptBytes) == 0 || json.Unmarshal(receiptBytes, &binding) != nil {
		return Report{}, driver.ErrConflict
	}
	initial := driver.Meta{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Head: model.Natural64(1), HistoryFloor: model.Natural64(1)}
	if err := binding.Validate(bound, initial); err != nil {
		return Report{}, driver.ErrConflict
	}
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, receiptBytes) {
		return Report{}, driver.ErrConflict
	}
	cp, err := r.SourcePlaneCheckpoint(plan.Providers[0].Namespace)
	if err != nil {
		return Report{}, err
	}
	// Origin is immutable; generation and the live logical image may advance.
	inspector := pluginhost.NewDurableState(nil, nil, "workflow", "default").(driver.SourcePlaneOriginReader)
	origin, current, err := inspector.InspectSourcePlaneOrigin(cp)
	if err != nil || origin != binding.Providers[0].Origin || cp.Generation < origin.InitialGeneration {
		return Report{}, driver.ErrConflict
	}
	if cp.Generation == origin.InitialGeneration {
		if err := driver.ValidateSourcePlaneInitialCheckpoint(cp, binding.Providers[0]); err != nil {
			return Report{}, driver.ErrConflict
		}
	}
	// Re-verify the exact retained source and immutable provider origin map.
	// Comparing the schema's original source member permits ordinary mutations
	// while refusing a namespace that retained its origin tag but lost raw data.
	data, err := r.Blob(ctx, driver.Hash(bound.ArchiveDigest))
	if err != nil || hashBytes(data) != bound.ArchiveDigest {
		return Report{}, driver.ErrConflict
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		return Report{}, driver.ErrConflict
	}
	prepared, err := prepareSourceArchive(ctx, archive, bound, true)
	if err != nil {
		return Report{}, driver.ErrConflict
	}
	var initialImage, currentImage map[string]json.RawMessage
	if json.Unmarshal(prepared.LogicalImage(), &initialImage) != nil || json.Unmarshal(current, &currentImage) != nil {
		return Report{}, driver.ErrConflict
	}
	var originalSource, currentSource any
	if decodeReverseJSON(initialImage["source"], &originalSource) != nil || decodeReverseJSON(currentImage["source"], &currentSource) != nil || !reflect.DeepEqual(originalSource, currentSource) {
		return Report{}, driver.ErrConflict
	}
	report.Archive, report.SourcePlanes = archive, &binding
	return report, nil
}
