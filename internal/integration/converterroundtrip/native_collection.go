package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

type nativeCollectionReceiving struct {
	issuer  *historyverify.NativeCollection
	archive *dumpformat.Archive
	stream  *historyverify.NativeArchiveStream
	plan    nativeprojection.Plan
	profile driver.RestoreImportProfile
	logical []byte
}

func (p *nativeCollectionReceiving) close() error {
	if p == nil {
		return nil
	}
	issuer := p.issuer
	*p = nativeCollectionReceiving{}
	if issuer != nil {
		return issuer.Close()
	}
	return nil
}

// Close releases a collection report's private spool and current candidates.
// The report's collection-owned views must not be borrowed after this call.
// Legacy single-archive reports retain their existing explicit ownership API.
func (r *Report) Close() error {
	if r == nil || r.collection == nil {
		return nil
	}
	owned := r.collection
	r.collection, r.Archive, r.NativeFamilies, r.NativeSchemas, r.NativeHistories, r.SourcePlanes = nil, nil, nil, nil, nil, nil
	r.Receipt, r.NativeDeltaHistory, r.GraphCertification = nil, nil, nil
	return owned.close()
}

func nativeCollectionBudget(ctx context.Context, limit uint64) (context.Context, *dumpformat.NativeDeltaBudget, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("native collection requires live context")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	budget := dumpformat.NativeDeltaBudgetFromContext(ctx)
	if budget == nil {
		if limit > dumpformat.NativeDeltaDefaultDecodedLimit {
			return nil, nil, fmt.Errorf("native collection decoded ceiling differs")
		}
		budget = dumpformat.NewNativeDeltaBudget(limit)
	} else {
		admitted := budget.Usage().LimitBytes
		if admitted > dumpformat.NativeDeltaDefaultDecodedLimit || limit != 0 && admitted != limit {
			return nil, nil, fmt.Errorf("native collection decoded ceiling differs")
		}
	}
	return dumpformat.WithNativeDeltaBudget(ctx, budget), budget, nil
}

// Preparation uses only the existing builtin source adapter and planner. The
// host supplies the complete admitted final profile; no source actor or row
// can choose authority, mappings, key, target or policy here.
func prepareNativeCaptureCollection(ctx context.Context, binding dumpformat.NativeJoinedBinding, sources []historyverify.NativeCaptureSetOpener, profile driver.RestoreImportProfile, limit uint64) (_ *nativeCollectionReceiving, err error) {
	ctx, budget, err := nativeCollectionBudget(ctx, limit)
	if err != nil {
		return nil, err
	}
	if len(sources) != 2 || profile.SourcePlanes == nil {
		return nil, driver.ErrUnsupported
	}
	validation := budget.Scope()
	defer validation.Release()
	reserve := func(phase string, size uint64) (func(), error) {
		candidate := validation.Scope()
		if err := candidate.ChargeBytes(phase, size); err != nil {
			candidate.Release()
			return nil, err
		}
		return candidate.Release, nil
	}
	accounting := driver.RestoreImportProfileAccounting{Bytes: reserve, JSON: func(phase string, value any) (func(), error) {
		n, err := dumpformat.NativeDeltaJSONSize(value)
		if err != nil {
			return nil, err
		}
		return reserve(phase, n-1)
	}}
	bound, releaseBound, err := profile.DetachedWithAccounting(accounting)
	if err != nil {
		return nil, err
	}
	defer func() { bound = driver.RestoreImportProfile{}; releaseBound() }()
	plan, releasePlan, err := bound.SourcePlanes.PlanWithAccounting(accounting)
	if err != nil {
		return nil, err
	}
	defer func() { plan = driver.SourcePlanePlanV1{}; releasePlan() }()
	if len(plan.Providers) != 1 {
		return nil, driver.ErrUnsupported
	}
	when, err := time.Parse(time.RFC3339Nano, bound.HistoryTime)
	if err != nil || when.IsZero() || bound.HistoryMode != driver.RestoreHistoryModeFull || bound.HistoryDigest == "" {
		return nil, driver.ErrUnsupported
	}
	admission := bound
	admission.SourcePlanes, admission.HistoryDigest = nil, ""
	issuer, err := historyverify.NewNativeCollection(ctx, binding, historyverify.NativeCollectionAdmission{Profile: admission, Target: plan.Target, Provider: plan.Providers[0]}, when)
	if err != nil {
		return nil, err
	}
	// The issuer owns independent target/provider strings; no decoded plan
	// fields escape this last use into the retained collection.
	plan = driver.SourcePlanePlanV1{}
	releasePlan()
	p := &nativeCollectionReceiving{issuer: issuer}
	defer func() {
		if err != nil {
			err = errors.Join(err, p.close())
		}
	}()
	for _, open := range sources {
		if err = issuer.Append(ctx, open); err != nil {
			return nil, err
		}
	}
	p.archive, err = issuer.FinishCurrent()
	if err != nil {
		return nil, err
	}
	// This verifies the actual original/field/route dispositions and exact
	// builtin logical bytes before the verifier binds the final provider image.
	image, err := prepareSourceArchive(p.archive.WithNativeDeltaAccounting(ctx), p.archive, bound, true)
	if err != nil {
		return nil, err
	}
	p.logical = image.LogicalImage()
	p.stream, p.plan, p.profile, err = issuer.Bind(ctx, bound)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// RestoreNativeCaptureCollectionImportWithLease feeds exactly two freshly
// authenticated native sets into the SAME row receiver as a single set. Every
// source is checked before that receiver obtains a writer or creates a stage.
// Close the returned Report to discard its operation-owned current/spool data.
func RestoreNativeCaptureCollectionImportWithLease(ctx context.Context, binding dumpformat.NativeJoinedBinding, sources []historyverify.NativeCaptureSetOpener, target driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile) (report Report, err error) {
	if target == nil {
		return report, driver.ErrUnsupported
	}
	p, err := prepareNativeCaptureCollection(ctx, binding, sources, profile, 0)
	if err != nil {
		return report, err
	}
	return restorePreparedNativeCaptureCollection(ctx, p, target, lease)
}

func restorePreparedNativeCaptureCollection(ctx context.Context, p *nativeCollectionReceiving, target driver.Store, lease driver.ActiveLease, stageFactories ...func(driver.WriteTx, historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error)) (report Report, err error) {
	keep := false
	defer func() {
		if !keep {
			if report.collection == nil {
				report.Archive, report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes, report.Receipt = nil, nil, nil, nil, nil, nil
			}
			err = errors.Join(err, p.close())
		}
	}()
	work, release := p.archive.NativeDeltaAccountingScope()
	defer release()
	ctx = work.WithNativeDeltaAccounting(ctx)
	report, err = restorePreparedNativeReceivingStream(ctx, work, target, lease, p.profile, p.plan, p.logical, p.stream, p.archive, func() error { release(); return nil }, stageFactories...)
	if err != nil {
		return report, err
	}
	if report.collection == nil {
		report.Archive, report.collection, keep = p.archive, p, true
	}
	return report, nil
}

func readRetainedReceivingArchive(ctx context.Context, reader driver.Reader, raw []byte, evidence archiveEvidence, profile driver.RestoreImportProfile) (*dumpformat.Archive, *nativeCollectionReceiving, error) {
	var format struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(raw, &format); err != nil || format.Format != dumpformat.NativeJoinedInputsFormatV2 {
		a, err := readRetainedImportArchive(ctx, reader, raw, evidence)
		return a, nil, err
	}
	if hashBytes(raw) != evidence.ArchiveSHA256 || evidence.ArchiveSHA256 != profile.ArchiveDigest || evidence.NativeCaptureFiles == nil || evidence.NativeDecodedLimitBytes == 0 {
		return nil, nil, driver.ErrRestoreBindingMismatch{Field: "archiveDigest"}
	}
	ctx, budget, err := nativeCollectionBudget(ctx, evidence.NativeDecodedLimitBytes)
	if err != nil {
		return nil, nil, err
	}
	work := budget.Scope()
	defer work.Release()
	if err = work.ChargeBytes("collection_retained_binding", uint64(len(raw))*8+1024); err != nil {
		return nil, nil, err
	}
	var binding dumpformat.NativeJoinedBinding
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&binding); err != nil || len(binding.Sources) != 2 {
		return nil, nil, driver.ErrRestoreBindingMismatch{Field: "collection.binding"}
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return nil, nil, driver.ErrRestoreBindingMismatch{Field: "collection.trailing"}
	}
	count := 0
	for ordinal, source := range binding.Sources {
		for _, file := range source.Files {
			if err = work.ChargeBytes("collection_retained_ledger_validation", uint64(len(file.Name))+256); err != nil {
				return nil, nil, err
			}
			if file.Bytes > math.MaxInt64 || evidence.NativeCaptureFiles[dumpformat.NativeCollectionFileName(ordinal, file.Name)] != (dumpformat.File{SHA256: file.SHA256, Size: int64(file.Bytes)}) {
				return nil, nil, driver.ErrRestoreBindingMismatch{Field: "collection.fileLedger"}
			}
			count++
		}
	}
	if count != len(evidence.NativeCaptureFiles) {
		return nil, nil, driver.ErrRestoreBindingMismatch{Field: "collection.fileLedger"}
	}
	openers := make([]historyverify.NativeCaptureSetOpener, 0, 2)
	for _, source := range binding.Sources {
		source := source
		openers = append(openers, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
			return readRetainedNativeCollectionSource(ctx, reader, source, options)
		})
	}
	p, err := prepareNativeCaptureCollection(ctx, binding, openers, profile, evidence.NativeDecodedLimitBytes)
	if err != nil {
		return nil, nil, err
	}
	return p.archive, p, nil
}

type retainedCollectionPart struct {
	*bytes.Reader
	owner *dumpformat.NativeDeltaBudget
}

func (p *retainedCollectionPart) Close() error {
	p.Reader = nil
	if p.owner != nil {
		p.owner.Release()
		p.owner = nil
	}
	return nil
}

func readRetainedNativeCollectionSource(ctx context.Context, reader driver.Reader, source dumpformat.NativeJoinedInput, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
	if len(source.Files) < 3 {
		return nil, driver.ErrRestoreBindingMismatch{Field: "collection.files"}
	}
	budget := dumpformat.NativeDeltaBudgetFromContext(ctx)
	if budget == nil {
		return nil, driver.ErrUnsupported
	}
	owner := budget.Scope()
	defer owner.Release()
	read := func(file dumpformat.NativeCaptureFile, scope *dumpformat.NativeDeltaBudget) ([]byte, error) {
		if file.Bytes == 0 || file.Bytes > math.MaxInt64 {
			return nil, driver.ErrRestoreBindingMismatch{Field: "collection.fileSize"}
		}
		if err := scope.ChargeBytes("collection_retained_original_buffer", file.Bytes); err != nil {
			return nil, err
		}
		raw, err := reader.Blob(ctx, driver.Hash(file.SHA256))
		if err != nil || uint64(len(raw)) != file.Bytes || hashBytes(raw) != file.SHA256 {
			return nil, driver.ErrRestoreBindingMismatch{Field: "collection.fileDigest"}
		}
		return raw, nil
	}
	root, err := read(source.Files[0], owner)
	if err != nil {
		return nil, err
	}
	head, err := read(source.Files[1], owner)
	if err != nil {
		return nil, err
	}
	return dumpformat.ReadNativeCaptureSet(ctx, bytes.NewReader(root), bytes.NewReader(head), func(_ context.Context, n uint64) (io.ReadCloser, error) {
		if n == 0 || n > uint64(len(source.Files)-2) {
			return nil, driver.ErrRestoreBindingMismatch{Field: "collection.partIndex"}
		}
		part := budget.Scope()
		raw, err := read(source.Files[n+1], part)
		if err != nil {
			part.Release()
			return nil, err
		}
		return &retainedCollectionPart{Reader: bytes.NewReader(raw), owner: part}, nil
	}, options)
}
