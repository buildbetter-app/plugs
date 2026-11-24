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

// P47: admin.compact's optional exclusive afterId cursor (Nikhil chose (a),
// 2026-11-24). Selection order is closed time, then record ID; the cursor
// resumes strictly after the named closed record's position, refusals name the
// selected record and write nothing, and afterId is part of the operation.

func p47Engines() []string { return []string{"memory", "sqlite", "postgres"} }

// p47Fixture builds a, b, c closed at the same time and d closed earlier, so
// the order is d (earlier close despite the larger ID), then a, b, c by ID.
func p47Fixture(t *testing.T, engine string, summary func(service.CompactionInput) (string, error)) (*fixture, *atomic.Int32, map[string]map[string]any) {
	t.Helper()
	calls := &atomic.Int32{}
	config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
		calls.Add(1)
		return summary(in)
	})}
	f := newFixture(t, engine, service.WithCompaction(config))
	text := strings.Repeat("P47 permanent source text. ", 30)
	refs := map[string]map[string]any{}
	for _, name := range []string{"a", "b", "c", "d"} {
		refs[name] = f.create("p47-create-"+name, text+name)
	}
	f.call("bead.close", "p47-close-d", map[string]any{"id": refs["d"]}, "")
	f.clock = f.clock.Add(time.Hour)
	for _, name := range []string{"a", "b", "c"} {
		f.call("bead.close", "p47-close-"+name, map[string]any{"id": refs[name]}, "")
	}
	open := f.create("p47-create-open", text+"open")
	refs["open"] = open
	f.clock = f.clock.Add(31 * 24 * time.Hour)
	return f, calls, refs
}

func p47Small(service.CompactionInput) (string, error) { return "P47 summary.", nil }

func p47ID(ref map[string]any) string { return ref["id"].(string) }

// p47Refusal asserts a refusal with the given code that writes nothing and
// returns its error data details.
func p47Refusal(t *testing.T, f *fixture, key, code string, params map[string]any) map[string]any {
	t.Helper()
	before := f.meta()
	response := f.call("admin.compact", key, params, code)
	f.assertSameHead(before)
	f.assertNoReceipt("admin.compact", key)
	details, _ := response["error"].(map[string]any)["data"].(map[string]any)["details"].(map[string]any)
	return details
}

func TestP47AfterIDEqualClosedAtOrdersByID(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			before := f.meta()
			cursor := map[string]any{"dryRun": true}
			for i, want := range []string{"d", "a", "b", "c"} {
				dry := f.call("admin.compact", fmt.Sprintf("p47-order-%d", i), cursor, "")
				assertCommitTarget(t, dry, refs[want])
				if dry["wouldCommit"] != true {
					t.Fatalf("dry run %d did not preview: %v", i, dry)
				}
				cursor = map[string]any{"dryRun": true, "afterId": p47ID(refs[want])}
			}
			p47Refusal(t, f, "p47-order-end", "compaction_unavailable", cursor)
			f.assertSameHead(before)
			if calls.Load() != 0 {
				t.Fatalf("dry runs called the provider %d times", calls.Load())
			}
			t.Logf("P47_ORDER engine=%s order=d,a,b,c equal_closed_at=a,b,c by_id=true", engine)
		})
	}
}

func TestP47AfterIDPastEndRefusesWithoutWrite(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			// The last record in the order is still eligible, but the cursor
			// is exclusive: nothing lies strictly after it.
			p47Refusal(t, f, "p47-past-last", "compaction_unavailable", map[string]any{"dryRun": false, "afterId": p47ID(refs["c"])})
			if calls.Load() != 0 {
				t.Fatal("past-end refusal called the provider")
			}
			for _, name := range []string{"d", "a", "b"} {
				f.call("admin.compact", "p47-drain-"+name, map[string]any{"dryRun": false}, "")
			}
			f.call("admin.compact", "p47-drain-c", map[string]any{"dryRun": false, "afterId": p47ID(refs["b"])}, "")
			details := p47Refusal(t, f, "p47-past-end", "compaction_unavailable", map[string]any{"dryRun": false, "afterId": p47ID(refs["c"])})
			if details["id"] != nil {
				t.Fatalf("a refusal with no selected record named %v", details["id"])
			}
			if calls.Load() != 4 {
				t.Fatalf("provider calls=%d want 4", calls.Load())
			}
		})
	}
}

func TestP47AfterIDIdempotencyKeyMismatchRefused(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			params := map[string]any{"dryRun": false, "afterId": p47ID(refs["a"])}
			first := f.call("admin.compact", "p47-key", params, "")
			assertCommitTarget(t, first, refs["b"])
			stable := f.meta()
			again := f.call("admin.compact", "p47-key", params, "")
			if !reflect.DeepEqual(first, again) {
				t.Fatalf("same key and afterId did not replay: %v %v", first, again)
			}
			assertReplayed(t, f)
			f.assertSameHead(stable)
			for label, changed := range map[string]map[string]any{
				"other-cursor": {"dryRun": false, "afterId": p47ID(refs["b"])},
				"no-cursor":    {"dryRun": false},
			} {
				f.call("admin.compact", "p47-key", changed, "idempotency_key_reused")
				f.assertSameHead(stable)
				t.Logf("P47_KEY engine=%s changed=%s refused=idempotency_key_reused", engine, label)
			}
			// A committed call without a cursor also refuses a later cursor.
			f.call("admin.compact", "p47-key-plain", map[string]any{"dryRun": false}, "")
			stable = f.meta()
			f.call("admin.compact", "p47-key-plain", map[string]any{"dryRun": false, "afterId": p47ID(refs["d"])}, "idempotency_key_reused")
			f.assertSameHead(stable)
			if calls.Load() != 2 {
				t.Fatalf("provider calls=%d want 2 (replays and refusals never summarize)", calls.Load())
			}
		})
	}
}

func TestP47RefusalNamesRecordAndWritesNothing(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			var ids map[string]string
			f, calls, refs := p47Fixture(t, engine, func(in service.CompactionInput) (string, error) {
				switch in.Ref.ID {
				case ids["a"]:
					return in.Text["description"] + " longer", nil
				case ids["b"]:
					return "", fmt.Errorf("synthetic provider failure")
				}
				return "P47 summary.", nil
			})
			ids = map[string]string{"a": p47ID(refs["a"]), "b": p47ID(refs["b"])}
			f.call("admin.compact", "p47-name-d", map[string]any{"dryRun": false}, "")
			versionsA, versionsB := f.versions(refs["a"]), f.versions(refs["b"])
			notSmaller := p47Refusal(t, f, "p47-name-a", "compaction_unavailable", map[string]any{"dryRun": false})
			if notSmaller["id"] != ids["a"] || fmt.Sprint(notSmaller["tier"]) != "1" {
				t.Fatalf("not-smaller refusal named %v", notSmaller)
			}
			providerFailed := p47Refusal(t, f, "p47-name-b", "compaction_unavailable", map[string]any{"dryRun": false, "afterId": ids["a"]})
			if providerFailed["id"] != ids["b"] {
				t.Fatalf("provider-failure refusal named %v", providerFailed)
			}
			if !reflect.DeepEqual(versionsA, f.versions(refs["a"])) || !reflect.DeepEqual(versionsB, f.versions(refs["b"])) {
				t.Fatal("a refusal wrote a version")
			}
			dry := f.call("admin.compact", "p47-name-dry", map[string]any{"dryRun": true, "afterId": ids["b"]}, "")
			assertCommitTarget(t, dry, refs["c"])
			f.call("admin.compact", "p47-name-c", map[string]any{"dryRun": false, "afterId": ids["b"]}, "")
			if calls.Load() != 4 {
				t.Fatalf("provider calls=%d want 4", calls.Load())
			}
			t.Logf("P47_REFUSAL engine=%s not_smaller=%v provider_error=%v writes=0", engine, notSmaller["id"], providerFailed["id"])
		})
	}
}

func TestP47AfterIDCursorResolution(t *testing.T) {
	for _, engine := range p47Engines() {
		t.Run(engine, func(t *testing.T) {
			f, calls, refs := p47Fixture(t, engine, p47Small)
			for label, cursor := range map[string]any{
				"unknown": "p47-no-such-record", "open": p47ID(refs["open"]),
				"empty": "", "padded": " " + p47ID(refs["a"]), "not-a-string": 7,
			} {
				p47Refusal(t, f, "p47-cursor-"+label, "invalid_params", map[string]any{"dryRun": false, "afterId": cursor})
			}
			// A compacted cursor is no longer eligible but keeps its position.
			f.call("admin.compact", "p47-cursor-compact-d", map[string]any{"dryRun": false}, "")
			f.call("admin.compact", "p47-cursor-compact-a", map[string]any{"dryRun": false}, "")
			dry := f.call("admin.compact", "p47-cursor-ineligible", map[string]any{"dryRun": true, "afterId": p47ID(refs["a"])}, "")
			assertCommitTarget(t, dry, refs["b"])
			if calls.Load() != 2 {
				t.Fatalf("provider calls=%d want 2", calls.Load())
			}
		})
	}
}
