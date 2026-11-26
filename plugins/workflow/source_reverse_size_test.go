package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func reverseSizeCommitment(data []byte, presence string) driver.SourcePlaneBytesV1 {
	hash := sha256.Sum256(data)
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: hex.EncodeToString(hash[:])}
}

// One retained original record expands through the existing []byte JSON wire.
// This exercises the real preparation and inspection with no scale-sized graph.
func reverseSizeFixture(t *testing.T, payloadBytes int) (driver.SourcePlanePlanV1, []driver.SourcePlaneRecordV1) {
	t.Helper()
	payload := strings.Repeat("x", payloadBytes)
	raw, err := json.Marshal(map[string]any{
		"kind": "beads-archive", "id": "config", "project": "p", "body": map[string]any{},
		"source": map[string]any{"table": "config", "fields": map[string]any{"payload": payload}},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1,
		SourceStoreID: "source", ArchiveSHA256: strings.Repeat("a", 64),
		Target:    driver.SourcePlaneTargetV1{StoreID: "target", Incarnation: "initial", RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Origins:   []driver.SourcePlaneOriginV1{{Project: "p", Table: "config", ID: "config", Action: "inert-origin", Raw: reverseSizeCommitment(raw, "object"), Fields: []driver.SourcePlaneFieldV1{{Name: "payload", Value: reverseSizeCommitment([]byte(payload), "text")}}, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}}},
		Providers: []driver.SourcePlaneProviderV1{{Alias: Alias, Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: driver.CheckpointLifetimeProcess, LogicalImageSHA256: strings.Repeat("b", 64), OriginAdapter: "beads-workflow-origins/1"}},
	}
	return plan, []driver.SourcePlaneRecordV1{{Kind: "beads-archive", Project: "p", Table: "config", ID: "config", Raw: raw}}
}

func TestSourceReverseExpandedImageUsesSeparateAdmissionBounds(t *testing.T) {
	plan, records := reverseSizeFixture(t, 49<<20)
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal("admit small encoded plan", err)
	}
	if len(binding.CanonicalPlan) >= driver.SourcePlaneMaxPlanBytes || len(records[0].Raw) >= driver.SourcePlaneMaxPlanBytes {
		t.Fatal("fixture exceeds an individual bound")
	}
	budget := dumpformat.NewNativeDeltaBudget(2 << 30)
	owner := budget.Scope()
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), owner)
	prepared, err := new(Provider).PrepareSourceRestore(ctx, plan, records)
	if err != nil {
		t.Fatal("prepare admitted plan and raw record", err)
	}
	logical := prepared.LogicalImage()
	if len(logical) <= driver.SourcePlaneMaxPlanBytes {
		t.Fatal("fixture did not cross expanded-image boundary", len(logical))
	}
	inspected, err := InspectSourceReverseImageContext(ctx, logical)
	if err != nil {
		t.Fatal("inspect independently admitted parts after preparation", len(logical), err)
	}
	if inspected.OriginSHA256 == "" || len(inspected.Entities) != 0 || len(inspected.Relations) != 0 {
		t.Fatal("lost inert source provenance", inspected.OriginSHA256)
	}
	t.Logf("encoded_plan=%d raw_records=%d expanded_logical=%d individual_limit=%d", len(binding.CanonicalPlan), len(records[0].Raw), len(logical), driver.SourcePlaneMaxPlanBytes)
	owner.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("prepared owner leaked budget", budget.Usage())
	}
	// Removing the aggregate guard must not bypass the admitted decoded budget.
	small := dumpformat.NewNativeDeltaBudget(1)
	smallOwner := small.Scope()
	_, err = new(Provider).PrepareSourceRestore(dumpformat.WithNativeDeltaBudget(context.Background(), smallOwner), plan, records)
	smallOwner.Release()
	var refusal *dumpformat.NativeDeltaDecodedLimitError
	if !errors.As(err, &refusal) || small.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("lost admitted decoded limit or cleanup", err, small.Usage())
	}
}

func TestSourceReverseOversizedPlanStillRefusedAtAdmission(t *testing.T) {
	plan, _ := reverseSizeFixture(t, 1)
	// This declaration is semantically valid but its actual canonical encoding is
	// over the unchanged limit, independently of any expanded provider image.
	plan.Principals = []driver.SourcePlanePrincipalV1{{Key: "origin", Source: strings.Repeat("s", driver.SourcePlaneMaxPlanBytes), Authority: "origin-only", UnverifiedOriginOnly: true}}
	if err := plan.Validate(); err != nil {
		t.Fatal("fixture must reach encoded-size admission", err)
	}
	_, err := driver.NewSourcePlaneImportBindingV1(plan)
	if !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), "plan byte limit") {
		t.Fatal("oversized encoded plan admitted", err)
	}
}

func TestSourceReverseInspectionAdmitsMeasuredExpandedCurrentImage(t *testing.T) {
	plan, records := reverseSizeFixture(t, 49<<20)
	budget := dumpformat.NewNativeDeltaBudget(0)
	owner := budget.Scope()
	defer owner.Release()
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), owner)
	prepared, err := new(Provider).PrepareSourceRestore(ctx, plan, records)
	if err != nil {
		t.Fatal(err)
	}
	var value persisted
	if err := json.Unmarshal(prepared.LogicalImage(), &value); err != nil {
		t.Fatal(err)
	}
	value.Audit = []auditState{{Event: ""}}
	empty, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	const observedExpandedBytes = 78485042
	value.Audit[0].Event = strings.Repeat("x", observedExpandedBytes-len(empty))
	current, err := json.Marshal(value)
	if err != nil || len(current) != observedExpandedBytes {
		t.Fatal("measured-size current fixture", len(current), err)
	}
	initial, err := InspectSourceReverseImageContext(ctx, prepared.LogicalImage())
	if err != nil {
		t.Fatal("initial expanded image", err)
	}
	inspected, err := InspectSourceReverseImageContext(ctx, current)
	if err != nil || inspected.OriginSHA256 != initial.OriginSHA256 {
		t.Fatal("admitted mutable current image changed immutable origin", err)
	}
	start := time.Now()
	digest, err := SourceOriginDigest(ctx, current)
	elapsed := time.Since(start)
	if err != nil || digest != initial.OriginSHA256 {
		t.Fatal("fast digest differed from full archive inspection", err)
	}
	t.Logf("immutable digest for %d checkpoint image bytes took %s", len(current), elapsed)
	if budget.Usage().CurrentCategories["workflow_reverse_result"] == 0 {
		t.Fatal("returned inspection lost its owner", budget.Usage())
	}
	owner.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("current inspection leaked temporary or result ownership", budget.Usage())
	}
	t.Logf("expanded_current=%d raw_records=%d plan_remains_admitted=true", len(current), len(records[0].Raw))
}

func TestSourceReverseInspectionRejectsOversizedSectionsBeforeDecode(t *testing.T) {
	for _, section := range []string{"receipts", "audit", "records"} {
		t.Run(section, func(t *testing.T) {
			var raw []byte
			switch section {
			case "receipts":
				raw = []byte(`{"runs":{},"items":{},"receipts":{"result":"` + strings.Repeat("x", driver.SourcePlaneMaxPlanBytes) + `"}}`)
			case "audit":
				raw = []byte(`{"runs":{},"items":{},"receipts":{},"audit":["` + strings.Repeat("x", driver.SourcePlaneMaxPlanBytes) + `"]}`)
			case "records":
				raw = []byte(`{"source":{"records":[{"Raw":"` + strings.Repeat("A", (driver.SourcePlaneMaxPlanBytes/3+1)*4) + `"}]}}`)
			}
			budget := dumpformat.NewNativeDeltaBudget(0)
			owner := budget.Scope()
			defer owner.Release()
			calls := 0
			_, err := inspectSourceReverseImage(dumpformat.WithNativeDeltaBudget(context.Background(), owner), raw, func([]byte, any) error { calls++; return nil })
			if err == nil || calls != 0 || !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("oversized section reached decode", section, calls, err)
			}
			if section == "records" && !strings.Contains(err.Error(), "source record byte limit") || section != "records" && !strings.Contains(err.Error(), "mutable section byte limit") {
				t.Fatal("wrong predecode boundary", err)
			}
			if budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("predecode refusal retained reservations", budget.Usage())
			}
		})
	}
}

func TestSourceReverseInspectionTinyCurrentBudgetAndMalformedCleanup(t *testing.T) {
	value := persisted{Runs: map[string]runState{}, Items: map[string]itemState{}, Receipts: map[string]receiptState{"current": {Result: json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`)}}, Source: &sourceState{Schema: SourceStateSchema}}
	current, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	small := dumpformat.NewNativeDeltaBudget(1)
	owner := small.Scope()
	calls := 0
	_, err = inspectSourceReverseImage(dumpformat.WithNativeDeltaBudget(context.Background(), owner), current, func([]byte, any) error { calls++; return nil })
	var refusal *dumpformat.NativeDeltaDecodedLimitError
	if !errors.As(err, &refusal) || calls != 0 || refusal.Phase != "workflow_reverse_input" || refusal.RejectedChargeBytes != uint64(len(current)) || small.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("current budget did not refuse before decode", calls, err, small.Usage())
	}
	owner.Release()
	for _, raw := range [][]byte{[]byte(`{"runs":{},"items":{},"receipts":{},"audit":[invalid]}`), []byte(`{"runs":{},"source":{"records":[{"Raw":"unterminated}]}}`), append([]byte(" "), current...)} {
		budget := dumpformat.NewNativeDeltaBudget(0)
		owner := budget.Scope()
		_, err := InspectSourceReverseImageContext(dumpformat.WithNativeDeltaBudget(context.Background(), owner), raw)
		if err == nil || budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("malformed/noncanonical inspection leaked or succeeded", err, budget.Usage())
		}
		owner.Release()
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = InspectSourceReverseImageContext(canceled, current)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("lost inspection cancellation", err)
	}
}

func reverseInspectionEntityFixture(t *testing.T) []byte {
	t.Helper()
	raw := []byte(`{"kind":"bead","id":"p-1","project":"p","body":{},"source":{"table":"issues","fields":{"id":"p-1"}}}`)
	value := persisted{Runs: map[string]runState{}, Items: map[string]itemState{}, Receipts: map[string]receiptState{}, Source: &sourceState{
		Schema:   SourceStateSchema,
		Entities: []driver.SourcePlaneEntityV1{{Source: driver.SourceEntityKey{Project: "p", Plane: "issues", ID: "p-1"}, TargetKind: "bead", TargetID: "p-1", Origin: reverseSizeCommitment(raw, "object"), Fields: []driver.SourcePlaneFieldV1{{Name: "id", Value: reverseSizeCommitment([]byte("p-1"), "text")}}}},
		Records:  []driver.SourcePlaneRecordV1{{Kind: "bead", Project: "p", Table: "issues", ID: "p-1", Raw: raw}},
	}}
	logical, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return logical
}

func TestSourceReverseInspectionLateCancellationAndBudgetIdentity(t *testing.T) {
	logical := reverseInspectionEntityFixture(t)
	if _, err := InspectSourceReverseImage(logical); err != nil {
		t.Fatal("valid small entity image", err)
	}
	for _, phase := range []string{"workflow_reverse_indexes", "workflow_commitment_digest", "workflow_source_text"} {
		t.Run("cancel-"+phase, func(t *testing.T) {
			budget := dumpformat.NewNativeDeltaBudget(0)
			owner := budget.Scope()
			ctx, cancel := context.WithCancel(dumpformat.WithNativeDeltaBudget(context.Background(), owner))
			defer cancel()
			armed, fired := false, false
			budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
				if event.Action != "reserve" {
					return
				}
				if event.Phase == "workflow_reverse_indexes" {
					armed = true
				}
				if armed && !fired && event.Phase == phase {
					fired = true
					cancel()
				}
			})
			_, err := InspectSourceReverseImageContext(ctx, logical)
			if !fired || !errors.Is(err, context.Canceled) || errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("late cancellation flattened", phase, fired, err)
			}
			if budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("failed inspection retained owners", budget.Usage())
			}
			owner.Release()
		})
	}
	t.Run("late-shared-budget", func(t *testing.T) {
		budget := dumpformat.NewNativeDeltaBudget(1 << 20)
		owner := budget.Scope()
		retained := owner.Scope()
		armed := false
		budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
			if event.Action != "reserve" || event.Phase != "workflow_reverse_indexes" || armed {
				return
			}
			armed = true
			usage := budget.Usage()
			// Other retained caller work leaves one byte less than the next digest.
			if err := retained.ChargeBytes("test_retained_caller_work", usage.LimitBytes-usage.CurrentAccountedDecodedBytes-(sha256.Size*2-1)); err != nil {
				t.Fatal(err)
			}
		})
		_, err := InspectSourceReverseImageContext(dumpformat.WithNativeDeltaBudget(context.Background(), owner), logical)
		var refusal *dumpformat.NativeDeltaDecodedLimitError
		if !armed || !errors.As(err, &refusal) || refusal.Phase != "workflow_commitment_digest" || errors.Is(err, driver.ErrUnsupported) {
			t.Fatal("late measured refusal flattened", err)
		}
		retained.Release()
		if budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("late refusal leaked inspection owners", budget.Usage())
		}
		owner.Release()
	})
}
