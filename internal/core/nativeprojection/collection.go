package nativeprojection

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type collectionCurrent struct {
	records  []driver.Record
	families []NativeFamilyReport
}

// NativeCapturePlanner reuses the ordinary receiving planner with a compact
// collection current leaf. It cannot mint a verifier checkpoint or publish.
// Append detaches its surviving candidates from a single authenticated source;
// no source Archive or full history preparation is retained.
type NativeCapturePlanner struct {
	current    collectionCurrent
	collection *dumpformat.NativeCaptureCollection
	when       time.Time
	finished   bool
}

func NewNativeCapturePlanner(ctx context.Context, binding dumpformat.NativeJoinedBinding, when time.Time) (*NativeCapturePlanner, error) {
	if when.IsZero() {
		return nil, fmt.Errorf("collection planner requires fixed restore clock")
	}
	c, err := dumpformat.NewNativeCaptureCollection(ctx, binding)
	if err != nil {
		return nil, err
	}
	return &NativeCapturePlanner{collection: c, when: when}, nil
}

func (p *NativeCapturePlanner) Owner() *dumpformat.Archive {
	if p == nil || p.collection == nil {
		return nil
	}
	return p.collection.Owner()
}

func (p *NativeCapturePlanner) Append(ctx context.Context, source *dumpformat.Archive) error {
	if p == nil || p.finished || p.collection == nil {
		return fmt.Errorf("collection planner unavailable")
	}
	if err := p.collection.Append(ctx, source); err != nil {
		return err
	}
	work, release := source.NativeDeltaAccountingScope()
	defer release()
	rows, families, err := NativeDeltaCurrentRecords(ctx, work, p.when)
	if err != nil {
		return err
	}
	owner := p.Owner()
	if err := owner.ChargeNativeDeltaJSON("collection_current_rows", rows); err != nil {
		return err
	}
	if err := owner.ChargeNativeDeltaBytes("collection_current_slots", uint64(len(rows))*256); err != nil {
		return err
	}
	for _, row := range rows {
		row.Body = bytes.Clone(row.Body)
		row.Ref.ID, row.Ref.Key, row.Kind = strings.Clone(row.Ref.ID), strings.Clone(row.Ref.Key), strings.Clone(row.Kind)
		p.current.records = append(p.current.records, row)
	}
	if err := owner.ChargeNativeDeltaJSON("collection_family_reports", families); err != nil {
		return err
	}
	p.current.families = append(p.current.families, families...)
	return ctx.Err()
}

// FinishCurrent closes source admission. This current Archive is only the
// authenticated source leaf; callers cannot use it to construct a transcript.
func (p *NativeCapturePlanner) FinishCurrent() (*dumpformat.Archive, error) {
	if p == nil || p.finished || p.collection == nil {
		return nil, fmt.Errorf("collection planner consumed")
	}
	a, err := p.collection.Finish()
	if err != nil {
		return nil, err
	}
	p.finished = true
	return a, nil
}

// Build binds one actual joint current image to the FINAL full profile. The
// verifier's separate collection issuer checks its fixed admission and rejects
// any late source/policy/provider/clock/profile substitution before sealing.
func (p *NativeCapturePlanner) Build(ctx context.Context, profile driver.RestoreImportProfile) (Plan, error) {
	if p == nil || !p.finished || p.Owner() == nil || !p.Owner().HasNativeCaptureCollection() || profile.HistoryTime != p.when.UTC().Format(time.RFC3339Nano) || profile.ArchiveDigest != p.Owner().SHA256() || string(profile.SourceStoreID) != p.Owner().Manifest().StoreID {
		return Plan{}, fmt.Errorf("collection planner final binding differs")
	}
	owner, release := p.Owner().NativeDeltaAccountingScope()
	image, err := buildPlanModeWithCurrent(owner, &profile, p.when, planImport, &p.current, ctx)
	plan, err := finishPlan(owner, image, &profile, p.when, err)
	if err != nil {
		release()
		return Plan{}, err
	}
	plan.release = release
	return plan, nil
}

// DerivedDigest computes only the deterministic rebuilt current digest. The
// trial image has no authority and all of its allocations cease before its
// child scope is released. It never revisits an original capture file.
func (p *NativeCapturePlanner) DerivedDigest(ctx context.Context, profile driver.RestoreImportProfile) (string, error) {
	if p == nil || !p.finished || p.Owner() == nil {
		return "", fmt.Errorf("collection planner is incomplete")
	}
	work, release := p.Owner().NativeDeltaAccountingScope()
	defer release()
	image, err := buildPlanModeWithCurrent(work, &profile, p.when, planImport, &p.current, ctx)
	if err != nil {
		return "", err
	}
	digest := image.derivedDigest
	image = restorePlan{}
	return digest, nil
}

func (p *NativeCapturePlanner) Close() {
	if p == nil {
		return
	}
	p.current = collectionCurrent{}
	if p.collection != nil {
		p.collection.Close()
		p.collection = nil
	}
}
