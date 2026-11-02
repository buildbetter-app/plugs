package compaction_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/service"
)

func TestB12DeterministicSelectionBeforeRevisionAndNonAtomicCalls(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			failSecond := true
			config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
				calls.Add(1)
				if in.Ref.ID == "b12-0002" && failSecond {
					return "", fmt.Errorf("synthetic second-item failure")
				}
				// Provider input must be detached from the retained original.
				in.Text["description"] = "provider-mutated input"
				return "summary " + in.Ref.ID, nil
			})}
			f := newFixture(t, engine, service.WithCompaction(config))
			text := strings.Repeat("Permanent per-item source. ", 40)
			a := f.create("select-a", text)
			closeA := f.call("bead.close", "close-a", map[string]any{"id": a}, "")
			b := f.create("select-b", text)
			f.call("bead.close", "close-b", map[string]any{"id": b}, "")
			f.clock = f.clock.Add(time.Hour)
			c := f.create("select-c", text)
			closeC := f.call("bead.close", "close-c", map[string]any{"id": c}, "")
			open := f.create("select-open", text)
			f.clock = f.clock.Add(30 * 24 * time.Hour)
			before := f.meta()
			// Inclusive boundary must still exclude all rows when it precedes close.
			f.call("admin.compact", "before-first", map[string]any{"beforeRevision": 1}, "compaction_unavailable")
			f.assertSameHead(before)
			f.assertNoReceipt("admin.compact", "before-first")
			dry := f.call("admin.compact", "select-preview", map[string]any{"dryRun": true}, "")
			assertCommitTarget(t, dry, a)
			if calls.Load() != 0 {
				t.Fatal("preview generated a summary")
			}
			f.assertSameHead(before)
			f.assertNoReceipt("admin.compact", "select-preview")
			first := f.call("admin.compact", "compact-a", map[string]any{"beforeRevision": closeA["revision"]}, "")
			assertCommitTarget(t, first, a)
			stable := f.meta()
			f.call("admin.compact", "compact-b", map[string]any{}, "compaction_unavailable")
			f.assertSameHead(stable)
			f.assertNoReceipt("admin.compact", "compact-b")
			assertHistoricalText(t, f, a, closeA["revision"], text)
			assertTextAndTier(t, f, a, "summary "+a["id"].(string), 1)
			got := f.call("bead.get", "b-unchanged", map[string]any{"id": b}, "")["bead"].(map[string]any)
			if got["fields"].(map[string]any)["description"] != text || len(f.versions(b)) != 2 {
				t.Fatal("failed second item mutated its source")
			}
			failSecond = false
			second := f.call("admin.compact", "compact-b", map[string]any{}, "")
			assertCommitTarget(t, second, b)
			third := f.call("admin.compact", "compact-c", map[string]any{"beforeRevision": closeC["revision"]}, "")
			assertCommitTarget(t, third, c)
			stable = f.meta()
			f.call("admin.compact", "no-more", map[string]any{}, "compaction_unavailable")
			f.assertSameHead(stable)
			f.assertNoReceipt("admin.compact", "no-more")
			if len(f.versions(open)) != 1 || calls.Load() != 4 {
				t.Fatalf("open row compacted or unexpected provider calls=%d", calls.Load())
			}
		})
	}
}

func TestB12EligibilityAndThresholdConfiguration(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, scenario := range []string{"open", "recent", "missing-close-time", "invalid-close-time", "future-close-time", "already-tier-two", "bad-tier", "zero-threshold", "negative-threshold", "unknown-tier", "positive-threshold"} {
				t.Run(scenario, func(t *testing.T) {
					var calls atomic.Int32
					config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) { calls.Add(1); return "small", nil })}
					switch scenario {
					case "zero-threshold":
						config.ThresholdDays = map[int]int{1: 0}
					case "negative-threshold":
						config.ThresholdDays = map[int]int{1: -1}
					case "unknown-tier":
						config.ThresholdDays = map[int]int{3: 30}
					case "positive-threshold":
						config.ThresholdDays = map[int]int{1: 7}
					}
					f := newFixture(t, engine, service.WithCompaction(config))
					f.extendedOntology = true
					if scenario == "missing-close-time" {
						f.initialStatus = "done"
					}
					f.bind()
					fields := map[string]any{"title": "Eligibility", "description": strings.Repeat("Long retained source. ", 20)}
					made := f.call("bead.create", "eligible-create", map[string]any{"type": "issue", "fields": fields}, "")
					ref := made["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
					if scenario != "open" && scenario != "missing-close-time" {
						f.call("bead.close", "eligible-close", map[string]any{"id": ref}, "")
					}
					changes := map[string]any{}
					switch scenario {
					case "invalid-close-time":
						changes["closed_at"] = "not-a-timestamp"
					case "future-close-time":
						changes["closed_at"] = f.clock.Add(60 * 24 * time.Hour).Format(time.RFC3339Nano)
					case "already-tier-two":
						changes["compaction_level"] = 2
					case "bad-tier":
						changes["compaction_level"] = 3
					}
					if len(changes) > 0 {
						f.call("bead.update", "eligible-invalid-fields", map[string]any{"id": ref, "fields": changes}, "")
					}
					age := 30 * 24 * time.Hour
					if scenario == "recent" {
						age -= time.Nanosecond
					}
					if scenario == "positive-threshold" {
						age = 7 * 24 * time.Hour
					}
					f.clock = f.clock.Add(age)
					before := f.meta()
					versions := f.versions(ref)
					expected := "compaction_unavailable"
					if scenario == "positive-threshold" {
						expected = ""
					}
					result := f.call("admin.compact", "eligibility", map[string]any{}, expected)
					if expected == "" {
						assertCommitTarget(t, result, ref)
						assertTextAndTier(t, f, ref, "small", 1)
						return
					}
					f.assertSameHead(before)
					f.assertNoReceipt("admin.compact", "eligibility")
					if calls.Load() != 0 || !reflect.DeepEqual(versions, f.versions(ref)) {
						t.Fatal("ineligible row ran provider or changed permanent history")
					}
				})
			}
		})
	}
}

func TestB12SuppliedSummaryValidationAndStagedFailureRollback(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, scenario := range []string{"same-size", "larger", "invalid-utf8", "provider-error", "put-failure", "commit-failure"} {
				t.Run(scenario, func(t *testing.T) {
					text := strings.Repeat("Original source λ. ", 40)
					config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) {
						switch scenario {
						case "same-size":
							return text, nil
						case "larger":
							return text + "x", nil
						case "invalid-utf8":
							return string([]byte{0xff}), nil
						case "provider-error":
							return "", fmt.Errorf("synthetic provider failure")
						}
						return "small", nil
					})}
					f := newFixture(t, engine, service.WithCompaction(config))
					ref := f.create("rollback-create", text)
					closed := f.call("bead.close", "rollback-close", map[string]any{"id": ref}, "")
					f.clock = f.clock.Add(30 * 24 * time.Hour)
					if scenario == "put-failure" || scenario == "commit-failure" {
						f.store = &observedStore{Store: f.store, failPut: scenario == "put-failure", failCommit: scenario == "commit-failure"}
						f.bind()
					}
					before := f.meta()
					versions := f.versions(ref)
					expected := "compaction_unavailable"
					if strings.HasSuffix(scenario, "failure") {
						expected = "internal_error"
					}
					f.call("admin.compact", "rollback-attempt", map[string]any{}, expected)
					f.assertSameHead(before)
					f.assertNoReceipt("admin.compact", "rollback-attempt")
					if !reflect.DeepEqual(versions, f.versions(ref)) {
						t.Fatal("failure published a version")
					}
					assertHistoricalText(t, f, ref, closed["revision"], text)
					got := f.call("bead.get", "rollback-current", map[string]any{"id": ref}, "")["bead"].(map[string]any)
					if got["fields"].(map[string]any)["description"] != text || len(got["metadata"].(map[string]any)) != 0 {
						t.Fatalf("failure lost source or published state: %v", got["metadata"])
					}
				})
			}
		})
	}
}
