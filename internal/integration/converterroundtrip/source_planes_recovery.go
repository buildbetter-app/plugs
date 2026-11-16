package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
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

func recoverSourceImport(ctx context.Context, target driver.Store, profile driver.RestoreImportProfile, check Verification, prepared ...*receivingEvidence) (Report, error) {
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
	report, _, err := recoverSourceImportReader(ctx, r, meta, bound, check, plan, request, prepared...)
	return report, err
}

// The caller owns one pinned core/namespace reader. All retained evidence and
// current origin checks use that same observation; this helper never reopens it.
func recoverSourceImportReader(ctx context.Context, r driver.SourcePlaneSnapshot, meta driver.Meta, bound driver.RestoreImportProfile, check Verification, plan driver.SourcePlanePlanV1, request string, prepared ...*receivingEvidence) (Report, []byte, error) {
	if r.Snapshot().At == nil || r.Snapshot().Incarnation != meta.Incarnation {
		return Report{}, nil, driver.ErrConflict
	}
	meta.Head = *r.Snapshot().At
	var source *receivingEvidence
	if len(prepared) != 0 {
		source = prepared[0]
	}
	owned := source == nil
	if owned {
		// recoverImportReader authenticates the receipt and exact current marker
		// before filling this private slot from its original admitted evidence.
		source = &receivingEvidence{requestHash: request}
	}
	keepArchive := false
	defer func() {
		if owned && !keepArchive && source.collection != nil {
			_ = source.collection.close()
			source.collection, source.archive, source.stream, source.plan, source.logical = nil, nil, nil, nil, nil
		} else if owned && source.archive != nil && source.collection == nil {
			source.archive.ClearNativeDeltaPreparation()
			if !keepArchive {
				source.archive.ReleaseNativeDecoded()
			}
		}
	}()
	if source == nil || source.requestHash != request {
		return Report{}, nil, driver.ErrConflict
	}
	report, err := recoverImportReader(ctx, r, meta, bound, check, request, source)
	if err != nil {
		return Report{}, nil, err
	}
	var binding driver.SourcePlaneCommitBindingV1
	var receiptBytes []byte
	for _, result := range report.Receipt.Commit.Results {
		if result.Ref.ID == driver.SourcePlaneCommitRecordID {
			if receiptBytes != nil || !result.Ref.Branch.IsZero() || result.Revision != report.Receipt.Commit.Revision {
				return Report{}, nil, driver.ErrConflict
			}
			receiptBytes = result.Record
		}
	}
	if len(receiptBytes) == 0 || json.Unmarshal(receiptBytes, &binding) != nil {
		return Report{}, nil, driver.ErrConflict
	}
	floor := model.Natural64(1)
	if bound.HistoryMode == driver.RestoreHistoryModeFull ||
		bound.HistoryMode == driver.RestoreHistoryModePartial && bound.NativeDeltaHistory != nil && bound.NativeDeltaHistory.FirstSequence == 1 {
		floor = model.Natural64(0)
	}
	initial := driver.Meta{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Head: report.Receipt.Commit.Revision, HistoryFloor: floor}
	if err := binding.Validate(bound, initial); err != nil {
		return Report{}, nil, driver.ErrConflict
	}
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, receiptBytes) {
		return Report{}, nil, driver.ErrConflict
	}
	cp, err := r.SourcePlaneCheckpoint(plan.Providers[0].Namespace)
	if err != nil {
		return Report{}, nil, err
	}
	// Origin is immutable; generation and the live logical image may advance.
	inspector := pluginhost.NewDurableState(nil, nil, "workflow", "default").(driver.SourcePlaneOriginReader)
	origin, current, err := inspector.InspectSourcePlaneOrigin(cp)
	if err != nil || origin != binding.Providers[0].Origin || cp.Generation < origin.InitialGeneration {
		return Report{}, nil, driver.ErrConflict
	}
	if cp.Generation == origin.InitialGeneration {
		if err := driver.ValidateSourcePlaneInitialCheckpoint(cp, binding.Providers[0]); err != nil {
			return Report{}, nil, driver.ErrConflict
		}
	}
	// Re-verify the exact retained source and immutable provider origin map.
	// Comparing the schema's original source member permits ordinary mutations
	// while refusing a namespace that retained its origin tag but lost raw data.
	archive := source.archive
	// Recovery may run after the original native capture reader was closed.
	// Reuse the archive's authenticated prepared view for readback rather than
	// reopening that closed reader.
	archive.UseNativeDeltaPreparedReadback()
	// The report retains the source archive, while prepared logical bytes and
	// reverse inspection candidates end at this comparison boundary.
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	ctx = work.WithNativeDeltaAccounting(ctx)
	logical := source.logical
	if logical == nil {
		image, err := prepareSourceArchive(ctx, work, bound, true)
		if err != nil {
			return Report{}, nil, nativeDeltaRecoveryError(err)
		}
		logical = image.LogicalImage()
	}
	initialImage, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil {
		return Report{}, nil, driver.ErrConflict
	}
	currentImage, err := workflowplugin.InspectSourceReverseImage(current)
	if err != nil || initialImage.OriginSHA256 == "" || initialImage.OriginSHA256 != currentImage.OriginSHA256 {
		return Report{}, nil, driver.ErrConflict
	}
	report.Archive, report.SourcePlanes, report.collection = archive, &binding, source.collection
	keepArchive = true
	return report, current, nil
}
