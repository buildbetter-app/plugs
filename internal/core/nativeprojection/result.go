// Package nativeprojection owns the deterministic receiving planner. It has no
// target-store, verifier-checkpoint or publication capability.
package nativeprojection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Plan is an immutable result of the actual receiving planner. Its zero value
// is invalid. Only Build, BuildWithProfile and BuildForInspection issue one;
// detached snapshots and caller-supplied records cannot be promoted to a Plan.
// A Plan authenticates no streaming transcript and mints no checkpoint.
type Plan struct {
	image         *restorePlan
	archive       *dumpformat.Archive
	archiveSHA256 string
	profile       []byte
	sourcePlanes  *driver.SourcePlaneImportBindingV1
	when          time.Time
}

// Snapshot is a detached publication image, never a constructor or proof.
// Changing it cannot change the Plan from which it was read.
type Snapshot struct {
	Records            []driver.Record
	Derived            []DerivedImage
	DerivedDigest      string
	Blobs              map[string][]byte
	Allocators         []driver.OfflineAllocator
	Sentinels          []driver.Sentinel
	NativeFamilies     []NativeFamilyReport
	NativeSchemas      []NativeSchemaReport
	NativeHistories    []NativeHistoryReport
	NativeDeltaHistory *driver.NativeDeltaHistoryEvidence
}

func Build(archive *dumpformat.Archive) (Plan, error) {
	if archive == nil {
		return Plan{}, fmt.Errorf("receiving planner requires an archive")
	}
	image, err := buildPlan(archive)
	return finishPlan(archive, image, nil, time.Time{}, err)
}

func BuildWithProfile(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, when time.Time, contexts ...context.Context) (Plan, error) {
	if archive == nil {
		return Plan{}, fmt.Errorf("receiving planner requires an archive")
	}
	image, err := buildPlanWithProfile(archive, profile, when, contexts...)
	return finishPlan(archive, image, profile, when, err)
}

func BuildForInspection(archive *dumpformat.Archive) (Plan, error) {
	if archive == nil {
		return Plan{}, fmt.Errorf("receiving planner requires an archive")
	}
	image, err := buildPlanForInspection(archive)
	return finishPlan(archive, image, nil, time.Time{}, err)
}

func finishPlan(archive *dumpformat.Archive, image restorePlan, profile *driver.RestoreImportProfile, when time.Time, err error) (Plan, error) {
	if err != nil {
		return Plan{}, err
	}
	var bound []byte
	var planes *driver.SourcePlaneImportBindingV1
	if profile != nil {
		// Scalar JSON preserves the existing principal/caveat normalization.
		// Canonical bytes have their own byte domain, without base64 expansion.
		scalar := *profile
		scalar.SourcePlanes = nil
		bound, err = MarshalNativePayload(archive, "planner_profile_binding", scalar)
		if err != nil {
			return Plan{}, err
		}
		if profile.SourcePlanes != nil {
			if err := archive.ChargeNativeDeltaBytes("planner_profile_plan", uint64(len(profile.SourcePlanes.CanonicalPlan))); err != nil {
				return Plan{}, err
			}
			value := *profile.SourcePlanes
			value.CanonicalPlan = bytes.Clone(value.CanonicalPlan)
			planes = &value
		}
	}
	return Plan{image: &image, archive: archive, archiveSHA256: archive.SHA256(), profile: bound, sourcePlanes: planes, when: when}, nil
}

func (p Plan) Built() bool           { return p.image != nil }
func (p Plan) ArchiveSHA256() string { return p.archiveSHA256 }
func (p Plan) DerivedSHA256() string {
	if !p.Built() {
		return ""
	}
	return p.image.derivedDigest
}
func (p Plan) HasNativeCapture() bool { return p.Built() && p.image.nativeCapture != nil }

// RequestBinding decodes the complete supplied wire profile and restore clock. A legacy
// or inspection plan has no import binding; the bool preserves that distinction.
func (p Plan) RequestBinding() (driver.RestoreImportProfile, time.Time, bool, error) {
	profile, when, bound, _, err := p.RequestBindingWithAccounting(p.archive)
	return profile, when, bound, err
}

// RequestBindingWithAccounting gives the caller explicit ownership of scalar
// normalization and one raw-plan copy; validation scratch ends before return.
func (p Plan) RequestBindingWithAccounting(owner *dumpformat.Archive) (driver.RestoreImportProfile, time.Time, bool, func(), error) {
	if !p.Built() {
		return driver.RestoreImportProfile{}, time.Time{}, false, nil, fmt.Errorf("unbuilt receiving plan")
	}
	if p.profile == nil {
		return driver.RestoreImportProfile{}, p.when, false, func() {}, nil
	}
	output, release := owner.NativeDeltaAccountingScope()
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	if err := output.ChargeNativeDeltaBytes("planner_profile_readback", uint64(len(p.profile))); err != nil {
		return driver.RestoreImportProfile{}, time.Time{}, false, nil, err
	}
	scratch, done := owner.NativeDeltaAccountingScope()
	defer done()
	if err := scratch.ChargeNativeDeltaBytes("planner_profile_decoder", uint64(len(p.profile))); err != nil {
		return driver.RestoreImportProfile{}, time.Time{}, false, nil, err
	}
	var profile driver.RestoreImportProfile
	decoder := json.NewDecoder(bytes.NewReader(p.profile))
	decoder.UseNumber()
	if err := decoder.Decode(&profile); err != nil {
		return driver.RestoreImportProfile{}, time.Time{}, false, nil, err
	}
	if p.sourcePlanes != nil {
		_, checked, err := p.sourcePlanes.PlanWithAccounting(ProfileAccounting(scratch))
		if err != nil {
			return driver.RestoreImportProfile{}, time.Time{}, false, nil, err
		}
		checked()
		if err := output.ChargeNativeDeltaBytes("planner_profile_plan_readback", uint64(len(p.sourcePlanes.CanonicalPlan))); err != nil {
			return driver.RestoreImportProfile{}, time.Time{}, false, nil, err
		}
		value := *p.sourcePlanes
		value.CanonicalPlan = bytes.Clone(value.CanonicalPlan)
		profile.SourcePlanes = &value
	}
	success = true
	return profile, p.when, true, release, nil
}

func (p Plan) Snapshot() (Snapshot, error) { return p.SnapshotWithAccounting(p.archive) }

// SnapshotWithAccounting reserves raw copied payloads and actual slice element
// sizes. The supplied owner must live through consumption of the entire image.
func (p Plan) SnapshotWithAccounting(owner *dumpformat.Archive) (Snapshot, error) {
	if !p.Built() {
		return Snapshot{}, fmt.Errorf("unbuilt receiving plan")
	}
	// Charge before creating even the record reference slice.
	if err := owner.ChargeNativeDeltaBytes("planner_snapshot_record_slots", uint64(len(p.image.records))*uint64(reflect.TypeFor[driver.Record]().Size())); err != nil {
		return Snapshot{}, err
	}
	view := Snapshot{Derived: p.image.derived, DerivedDigest: p.image.derivedDigest, Blobs: p.image.blobs, Allocators: p.image.allocators, Sentinels: p.image.sentinels, NativeFamilies: p.image.nativeFamilies, NativeSchemas: p.image.nativeSchemas, NativeHistories: p.image.nativeHistories, NativeDeltaHistory: p.image.nativeDeltaHistory}
	copiedBytes := snapshotCopyBytes(p.image, view) + pointerCopyBytes(view.NativeDeltaHistory)
	if view.NativeDeltaHistory != nil {
		copiedBytes += pointerCopyBytes(view.NativeDeltaHistory.OmittedPrefix)
	}
	if err := owner.ChargeNativeDeltaBytes("planner_detached_snapshot", copiedBytes); err != nil {
		return Snapshot{}, err
	}
	if p.image.records != nil {
		view.Records = make([]driver.Record, len(p.image.records))
		for i, record := range p.image.records {
			view.Records[i] = driver.Record{Ref: record.ref, Kind: record.kind, Body: record.body}
		}
	}
	return detachSnapshot(view), nil
}

// These copies preserve nil/empty membership and the original payload bytes.
// References retain their private scalar selection tag through value copies.
func cloneSlice[T any](input []T) []T {
	if input == nil {
		return nil
	}
	output := make([]T, len(input))
	copy(output, input)
	return output
}

func clonePointer[T any](input *T) *T {
	if input == nil {
		return nil
	}
	output := *input
	return &output
}

func detachSnapshot(view Snapshot) Snapshot {
	// Records already has a private slice reserved before its construction.
	for i := range view.Records {
		view.Records[i].Body = cloneSlice(view.Records[i].Body)
	}
	view.Derived = cloneSlice(view.Derived)
	for i := range view.Derived {
		view.Derived[i].Value = cloneSlice(view.Derived[i].Value)
	}
	if view.Blobs != nil {
		blobs := make(map[string][]byte, len(view.Blobs))
		for key, body := range view.Blobs {
			blobs[key] = cloneSlice(body)
		}
		view.Blobs = blobs
	}
	view.Allocators = cloneSlice(view.Allocators)
	for i := range view.Allocators {
		view.Allocators[i] = view.Allocators[i].Clone()
	}
	view.Sentinels = cloneSlice(view.Sentinels)
	view.NativeFamilies = cloneSlice(view.NativeFamilies)
	view.NativeSchemas = cloneSlice(view.NativeSchemas)
	for i := range view.NativeSchemas {
		report := &view.NativeSchemas[i]
		report.Profile.Absences = cloneSlice(report.Profile.Absences)
		report.PlugDefaults = cloneSlice(report.PlugDefaults)
		for j := range report.PlugDefaults {
			report.PlugDefaults[j].Value = cloneSlice(report.PlugDefaults[j].Value)
		}
		report.TargetOmissions = cloneSlice(report.TargetOmissions)
	}
	view.NativeHistories = cloneSlice(view.NativeHistories)
	for i := range view.NativeHistories {
		coverage := clonePointer(view.NativeHistories[i].Coverage)
		if coverage != nil {
			coverage.Boundaries = cloneSlice(coverage.Boundaries)
			for j := range coverage.Boundaries {
				boundary := &coverage.Boundaries[j]
				boundary.ChildCommitIDs = cloneSlice(boundary.ChildCommitIDs)
				boundary.CommitSchemaVersion = clonePointer(boundary.CommitSchemaVersion)
				boundary.SQLDoltCodes = cloneSlice(boundary.SQLDoltCodes)
			}
		}
		view.NativeHistories[i].Coverage = coverage
	}
	view.NativeDeltaHistory = clonePointer(view.NativeDeltaHistory)
	if view.NativeDeltaHistory != nil {
		view.NativeDeltaHistory.OmittedPrefix = clonePointer(view.NativeDeltaHistory.OmittedPrefix)
	}
	return view
}

// Source routing readback returns detached lookup maps for the existing
// reverse adapter. They cannot construct a receiving Plan. Each new byte
// candidate is reserved before the copy, in the same accounting owner.
func (p SourceCoreProjection) Entities() (map[driver.SourceEntityKey]driver.SourcePlaneEntityV1, error) {
	for key, value := range p.entities {
		if err := chargeLookup(p.accounting, key, value); err != nil {
			return nil, err
		}
	}
	if p.entities == nil {
		return nil, nil
	}
	out := make(map[driver.SourceEntityKey]driver.SourcePlaneEntityV1, len(p.entities))
	for key, value := range p.entities {
		value.Fields = cloneSlice(value.Fields)
		out[key] = value
	}
	return out, nil
}
func (p SourceCoreProjection) Relations() (map[driver.SourceEdgeKey]driver.SourcePlaneRelationV1, error) {
	for key, value := range p.relations {
		if err := chargeLookup(p.accounting, key, value); err != nil {
			return nil, err
		}
	}
	if p.relations == nil {
		return nil, nil
	}
	out := make(map[driver.SourceEdgeKey]driver.SourcePlaneRelationV1, len(p.relations))
	for key, value := range p.relations {
		value.To = clonePointer(value.To)
		value.External = clonePointer(value.External)
		value.PredicateJSON = cloneSlice(value.PredicateJSON)
		value.Fields = cloneSlice(value.Fields)
		out[key] = value
	}
	return out, nil
}
func (p SourceCoreProjection) InertRelations() (map[driver.SourceEdgeKey]bool, error) {
	for key, value := range p.inertRelations {
		if err := chargeLookup(p.accounting, key, value); err != nil {
			return nil, err
		}
	}
	if p.inertRelations == nil {
		return nil, nil
	}
	out := make(map[driver.SourceEdgeKey]bool, len(p.inertRelations))
	for key, value := range p.inertRelations {
		out[key] = value
	}
	return out, nil
}
func chargeLookup(archive *dumpformat.Archive, key, value any) error {
	return archive.ChargeNativeDeltaJSON("planner_source_lookup_detach", struct{ Key, Value any }{key, value})
}
