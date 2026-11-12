package converterroundtrip

import (
	"context"
	"errors"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

// PreparedNativeCaptureCollectionImport owns a verifier-issued collection and
// actual immutable Plan. It can be received once through the shared receiver;
// no caller snapshot, transcript, profile or new key can replace its binding.
type PreparedNativeCaptureCollectionImport struct{ prepared *nativeCollectionReceiving }

// BindNativeCaptureCollectionImport transfers issuer ownership, including on
// error. The host supplies its complete explicit plan and fixed admission.
// Provider bytes and the final derived/history digests are verified outputs.
// Issuer must already have authenticated both sources and finished current.
// Binding and receiving reuse that preparation without another source pass.
func BindNativeCaptureCollectionImport(ctx context.Context, issuer *historyverify.NativeCollection, profile driver.RestoreImportProfile, plan driver.SourcePlanePlanV1) (_ *PreparedNativeCaptureCollectionImport, err error) {
	if issuer == nil {
		return nil, driver.ErrUnsupported
	}
	p := &nativeCollectionReceiving{issuer: issuer, archive: issuer.Owner()}
	defer func() {
		if err != nil {
			err = errors.Join(err, p.close())
		}
	}()
	if p.archive == nil || !p.archive.HasNativeCaptureCollection() || profile.SourcePlanes != nil || len(plan.Providers) != 1 {
		return nil, driver.ErrUnsupported
	}
	ctx = p.archive.WithNativeDeltaAccounting(ctx)
	work, release := p.archive.NativeDeltaAccountingScope()
	defer release()
	// Both provisional canonical bindings are transient. The issuer detaches
	// its final profile before these strings/buffers cease and this scope ends.
	bind := func(plan driver.SourcePlanePlanV1) (driver.SourcePlaneImportBindingV1, error) {
		n, err := driver.SourcePlanePlanEncodedSizeV1(plan)
		if err != nil {
			return driver.SourcePlaneImportBindingV1{}, err
		}
		if err := work.ChargeNativeDeltaBytes("collection_host_plan_encoding", n); err != nil {
			return driver.SourcePlaneImportBindingV1{}, err
		}
		if err := work.ChargeNativeDeltaJSON("collection_host_plan_validation", plan); err != nil {
			return driver.SourcePlaneImportBindingV1{}, err
		}
		binding, err := driver.NewSourcePlaneImportBindingV1(plan)
		if err == nil && uint64(len(binding.CanonicalPlan)) != n {
			return driver.SourcePlaneImportBindingV1{}, driver.ErrConflict
		}
		return binding, err
	}
	wantImage, wantSize := plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize
	if wantImage == "" && wantSize != 0 {
		return nil, driver.ErrConflict
	}
	plan.Providers = append([]driver.SourcePlaneProviderV1(nil), plan.Providers...)
	if wantImage == "" {
		plan.Providers[0].LogicalImageSHA256 = strings.Repeat("0", 64)
	}
	binding, err := bind(plan)
	if err != nil {
		return nil, err
	}
	profile.SourcePlanes = &binding
	wantDerived := profile.DerivedDigest
	if profile.DerivedDigest == "" {
		profile.DerivedDigest = strings.Repeat("0", 64)
	}
	image, err := prepareSourceArchive(ctx, p.archive, profile, false)
	if err != nil {
		return nil, err
	}
	p.logical = image.LogicalImage()
	if wantImage != "" && (wantImage != image.SHA256() || wantSize != uint64(len(p.logical))) {
		return nil, driver.ErrConflict
	}
	plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = image.SHA256(), uint64(len(p.logical))
	profile.SourcePlanes = nil
	binding = driver.SourcePlaneImportBindingV1{}
	binding, err = bind(plan)
	if err != nil {
		return nil, err
	}
	profile.SourcePlanes, profile.DerivedDigest = &binding, wantDerived
	p.stream, p.plan, p.profile, err = issuer.Bind(ctx, profile)
	profile.SourcePlanes = nil
	binding = driver.SourcePlaneImportBindingV1{}
	if err != nil {
		return nil, err
	}
	return &PreparedNativeCaptureCollectionImport{prepared: p}, nil
}

// Profile returns an admitted detached copy for receipt/readback verification.
// The caller must discard the copy before closing the preparation/report.
func (p *PreparedNativeCaptureCollectionImport) Profile() (driver.RestoreImportProfile, error) {
	if p == nil || p.prepared == nil {
		return driver.RestoreImportProfile{}, driver.ErrConflict
	}
	return nativeDetachedProfile(p.prepared.archive, p.prepared.profile)
}

func (p *PreparedNativeCaptureCollectionImport) Close() error {
	if p == nil || p.prepared == nil {
		return nil
	}
	owned := p.prepared
	p.prepared = nil
	return owned.close()
}

// RestoreWithLease consumes the prepared handle even on refusal. Successful
// ownership moves into Report.Close; the publication algorithm is unchanged.
func (p *PreparedNativeCaptureCollectionImport) RestoreWithLease(ctx context.Context, target driver.Store, lease driver.ActiveLease) (Report, error) {
	if p == nil || p.prepared == nil {
		return Report{}, driver.ErrConflict
	}
	owned := p.prepared
	p.prepared = nil
	if target == nil {
		return Report{}, errors.Join(driver.ErrUnsupported, owned.close())
	}
	return restorePreparedNativeCaptureCollection(ctx, owned, target, lease)
}
