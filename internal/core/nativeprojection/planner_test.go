package nativeprojection

import (
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestProducerAndReverseShareOpaqueMetadataKeySet(t *testing.T) {
	entity := driver.SourceEntityKey{Project: "ops", Plane: dumpformat.DurableEntityTable, ID: "ops-1"}
	plan := driver.SourcePlanePlanV1{
		Dispositions: []driver.SourcePlaneDispositionV1{{
			Key: "task", Fields: []driver.SourcePlaneFieldRuleV1{
				{Source: "id", Target: "id", Disposition: "exact-map"},
				{Source: "started_at", Target: "beads.started_at", Disposition: "opaque-metadata"},
				{Source: "notes", Target: "beads.notes", Disposition: "opaque-metadata"},
			},
		}},
		Entities: []driver.SourcePlaneEntityV1{{Source: entity, DispositionKey: "task"}},
	}
	want := map[driver.SourceEntityKey]map[string]string{entity: {
		"started_at": "beads.started_at",
		"notes":      "beads.notes",
	}}
	if got := OpaqueMetadataTargets(plan); !reflect.DeepEqual(got, want) {
		t.Fatalf("producer/reverse opaque metadata key set diverged: got=%v want=%v", got, want)
	}
}

func closeTimeRecord(value any) dumpformat.Record {
	return dumpformat.Record{
		Kind: "bead",
		ID:   "pl-closed",
		Source: dumpformat.Provenance{Table: dumpformat.DurableEntityTable, Fields: map[string]any{
			"closed_at": value,
		}},
	}
}

func TestSourceCloseTimePreservesExactUTCInstant(t *testing.T) {
	got, err := SourceCloseTime(closeTimeRecord("2026-11-15T03:04:05.123456789+02:00"))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 15, 1, 4, 5, 123456789, time.UTC)
	if got == nil || !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("closed_at=%v, want exact UTC %s", got, want.Format(time.RFC3339Nano))
	}
}

func TestSourceCloseTimeRejectsInvalidStamp(t *testing.T) {
	if _, err := SourceCloseTime(closeTimeRecord("not-a-timestamp")); err == nil {
		t.Fatal("invalid closed_at stamp was accepted")
	}
}

func TestSourceCloseTimeLeavesAbsentFieldUnknown(t *testing.T) {
	if got, err := SourceCloseTime(closeTimeRecord(nil)); err != nil || got != nil {
		t.Fatalf("absent closed_at=%v err=%v, want nil", got, err)
	}
}

func TestClosedPlanCopiesDropEveryPrivatePayloadReference(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(4096)
	retained, prepared := budget.Scope(), budget.Scope()
	if err := retained.ChargeBytes("returned_result", 31); err != nil {
		t.Fatal(err)
	}
	profile, canonical := []byte("profile"), []byte("canonical-plan")
	if err := prepared.ChargeBytes("planner_profile_binding", uint64(len(profile))); err != nil {
		t.Fatal(err)
	}
	if err := prepared.ChargeBytes("planner_profile_plan", uint64(len(canonical))); err != nil {
		t.Fatal(err)
	}
	plan := Plan{&planPayload{image: &restorePlan{}, archive: &dumpformat.Archive{}, profile: profile, sourcePlanes: &driver.SourcePlaneImportBindingV1{CanonicalPlan: canonical}, release: prepared.Release}}
	copy := plan
	if !copy.Built() || budget.Usage().CurrentAccountedDecodedBytes != uint64(31+len(profile)+len(canonical)) {
		t.Fatal("missing preclose ownership", budget.Usage())
	}
	plan.Close()
	for _, closed := range []Plan{plan, copy} {
		if closed.Built() || closed.image != nil || closed.archive != nil || closed.profile != nil || closed.sourcePlanes != nil || closed.release != nil {
			t.Fatal("closed copy retains an uncharged payload", closed)
		}
		if _, err := closed.Snapshot(); err == nil {
			t.Fatal("closed copy still usable")
		}
		closed.Close()
	}
	if budget.Usage().CurrentAccountedDecodedBytes != 31 || budget.Usage().CurrentCategories["returned_result"] != 31 {
		t.Fatal("close did not conserve exactly the retained set", budget.Usage())
	}
	retained.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("result owner cleanup", budget.Usage())
	}
}
