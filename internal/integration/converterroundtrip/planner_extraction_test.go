package converterroundtrip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These cases exercise the new immutable boundary with the existing original
// public archives. The unchanged receiving tests retain their independent
// source-cell, source-byte, derived-golden and physical readback oracles.
func TestReceivingPlannerDetachedSnapshotPublicFixtures(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("portable-complete-%t", complete), func(t *testing.T) {
			data, archive := publicSourceFixture(t, complete)
			profile, _ := admittedImportProfile(t, data)
			assertDetachedReceivingPlan(t, archive, profile)
		})
	}
	for _, version := range []int{66, 72} {
		t.Run(fmt.Sprintf("native-own-version-%d", version), func(t *testing.T) {
			archive, data := actualOwnVersionArchive(t, version)
			store, lease, cleanup := openReverseTarget(t, "memory")
			defer cleanup()
			profile := actualOwnVersionProfile(t, archive, data, store, lease)
			assertDetachedReceivingPlan(t, archive, profile)
		})
	}
}

func assertDetachedReceivingPlan(t *testing.T, archive *dumpformat.Archive, profile driver.RestoreImportProfile) {
	t.Helper()
	when := nativeFixtureTime
	if profile.HistoryTime != "" {
		var err error
		when, err = time.Parse(time.RFC3339Nano, profile.HistoryTime)
		if err != nil { t.Fatal(err) }
	}
	plan, err := nativeprojection.BuildWithProfile(archive, &profile, when)
	if err != nil { t.Fatal(err) }
	if !plan.Built() || plan.ArchiveSHA256() != archive.SHA256() { t.Fatal("actual planner did not bind original archive") }
	first, err := plan.Snapshot()
	if err != nil { t.Fatal(err) }
	original, err := json.Marshal(first)
	if err != nil { t.Fatal(err) }
	view, err := buildPlanWithProfile(archive, &profile, when)
	if err != nil { t.Fatal(err) }
	if len(first.Records) != len(view.records) || first.DerivedDigest != view.derivedDigest || !reflect.DeepEqual(first.Derived, view.derived) || !reflect.DeepEqual(first.Blobs, view.blobs) || !reflect.DeepEqual(first.Allocators, view.allocators) || !reflect.DeepEqual(first.Sentinels, view.sentinels) || !reflect.DeepEqual(first.NativeFamilies, view.nativeFamilies) || !reflect.DeepEqual(first.NativeSchemas, view.nativeSchemas) || !reflect.DeepEqual(first.NativeHistories, view.nativeHistories) {
		t.Fatal("receiver publication view changed actual planner output")
	}
	for i, record := range first.Records {
		if record.Ref != view.records[i].ref || record.Kind != view.records[i].kind || !bytes.Equal(record.Body, view.records[i].body) {
			t.Fatal("receiver changed a planned record identity, order or byte")
		}
	}
	bound, clock, present, err := plan.RequestBinding()
	if err != nil || !present || !clock.Equal(when) || !reflect.DeepEqual(bound, profile) { t.Fatal("full original request/clock binding differs", err) }
	bindingBytes, err := json.Marshal(bound)
	if err != nil { t.Fatal(err) }
	// Mutate every present reference-bearing publication domain, both the
	// caller's original request and its detached request readback.
	for i := range first.Records {
		first.Records[i].Ref = model.RefByID("changed", model.Natural64(0))
		if len(first.Records[i].Body) > 0 { first.Records[i].Body[0] ^= 1 }
	}
	for i := range first.Derived {
		if len(first.Derived[i].Value) > 0 { first.Derived[i].Value[0] ^= 1 }
	}
	for key, body := range first.Blobs {
		if len(body) > 0 { body[0] ^= 1 }
		delete(first.Blobs, key)
	}
	for i := range first.Allocators {
		first.Allocators[i].Prefix = "changed"
		if first.Allocators[i].Children == nil { first.Allocators[i].Children = map[string]uint64{} }
		first.Allocators[i].Children["changed"] = 999
	}
	if len(first.Sentinels) > 0 { first.Sentinels[0] = driver.Sentinel("changed") }
	if len(first.NativeFamilies) > 0 { first.NativeFamilies[0].Family = "changed" }
	for i := range first.NativeSchemas {
		for j := range first.NativeSchemas[i].Profile.Absences { first.NativeSchemas[i].Profile.Absences[j].Column = "changed" }
		for j := range first.NativeSchemas[i].PlugDefaults {
			body := first.NativeSchemas[i].PlugDefaults[j].Value
			if len(body) > 0 { body[0] ^= 1 }
		}
		if len(first.NativeSchemas[i].TargetOmissions) > 0 { first.NativeSchemas[i].TargetOmissions[0].Column = "changed" }
	}
	for i := range first.NativeHistories {
		if c := first.NativeHistories[i].Coverage; c != nil && len(c.Boundaries) > 0 { c.Boundaries[0].Reason = "changed" }
	}
	profile.Admission.Actor.ID = "changed"
	bound.Admission.Actor.ID = "changed"
	if profile.SourcePlanes != nil && len(profile.SourcePlanes.CanonicalPlan) > 0 { profile.SourcePlanes.CanonicalPlan[0] ^= 1 }
	if bound.SourcePlanes != nil && len(bound.SourcePlanes.CanonicalPlan) > 0 { bound.SourcePlanes.CanonicalPlan[0] ^= 1 }
	second, err := plan.Snapshot()
	if err != nil { t.Fatal(err) }
	after, err := json.Marshal(second)
	if err != nil || !bytes.Equal(original, after) { t.Fatal("caller mutation escaped detached snapshot", err) }
	bound, clock, present, err = plan.RequestBinding()
	afterBinding, encodeErr := json.Marshal(bound)
	if err != nil || encodeErr != nil || !present || !clock.Equal(when) || !bytes.Equal(bindingBytes, afterBinding) { t.Fatal("caller mutation escaped bound request/clock", err, encodeErr) }
}

func TestReceivingPlannerRejectsUnbuiltAndRetainsLegacyGolden(t *testing.T) {
	var zero nativeprojection.Plan
	if zero.Built() || zero.ArchiveSHA256() != "" || zero.DerivedSHA256() != "" || zero.HasNativeCapture() { t.Fatal("zero result acquired planner authority") }
	if _, err := zero.Snapshot(); err == nil { t.Fatal("zero snapshot accepted") }
	if _, _, _, err := zero.RequestBinding(); err == nil { t.Fatal("zero binding accepted") }
	if _, err := nativeprojection.Build(nil); err == nil { t.Fatal("nil archive accepted") }
	archive, err := dumpformat.Read(bytes.NewReader(supportedBytes(t, nil)))
	if err != nil { t.Fatal(err) }
	plan, err := nativeprojection.Build(archive)
	if err != nil { t.Fatal(err) }
	if plan.DerivedSHA256() != supportedDerived { t.Fatal("unchanged independent legacy derived golden differs") }
	if _, _, present, err := plan.RequestBinding(); err != nil || present { t.Fatal("legacy plan acquired an import request", err) }
}
