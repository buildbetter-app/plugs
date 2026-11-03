package compaction_test

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/service"
)

func TestB12ExistingImportedLevelAdvancesAndReverses(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			authority := &presenceAuthority{}
			config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
				if in.Tier != 2 {
					t.Errorf("expected prior level1 to select tier2; tier=%d", in.Tier)
				}
				return "tier two", nil
			}), VerifyPresence: authority.verify}
			f := newFixture(t, engine, service.WithCompaction(config))
			f.extendedOntology = true
			f.bind()
			text := strings.Repeat("Imported retained level-one text. ", 60)
			made := f.call("bead.create", "imported-level-create", map[string]any{"type": "issue", "fields": map[string]any{"title": "level one", "description": text, "compaction_level": 1, "closed_at": f.clock.Format(time.RFC3339Nano)}}, "")
			ref := made["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
			closed := f.call("bead.close", "imported-level-close", map[string]any{"id": ref}, "")
			f.clock = f.clock.Add(90*24*time.Hour - time.Nanosecond)
			before := f.meta()
			f.call("admin.compact", "imported-level-recent", map[string]any{}, "compaction_unavailable")
			f.assertSameHead(before)
			f.assertNoReceipt("admin.compact", "imported-level-recent")
			f.clock = f.clock.Add(time.Nanosecond)
			compact := f.call("admin.compact", "imported-level-tier2", map[string]any{}, "")
			fields := f.call("bead.get", "imported-level-get", map[string]any{"id": ref}, "")["bead"].(map[string]any)["fields"].(map[string]any)
			if fields["compaction_level"].(interface{ String() string }).String() != "2" {
				t.Fatalf("existing public level did not advance: %v", fields["compaction_level"])
			}
			params := map[string]any{"ref": ref, "baseRevision": compact["revision"], "expect": []any{}}
			params["humanPresence"] = authority.issue(f, params, "imported-level-revert")
			f.call("history.revert", "imported-level-revert", params, "")
			fields = f.call("bead.get", "imported-level-restored", map[string]any{"id": ref}, "")["bead"].(map[string]any)["fields"].(map[string]any)
			if fields["compaction_level"].(interface{ String() string }).String() != "1" || fields["description"] != text {
				t.Fatal("imported level/text did not reverse")
			}
			assertHistoricalText(t, f, ref, closed["revision"], text)
			if engine != "memory" {
				f.close()
				f.open()
				assertHistoricalText(t, f, ref, compact["revision"], "tier two")
			}
		})
	}
}

func TestB12MinimalReceiptReplayWithoutProviderAndMetaDryRun(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) { calls.Add(1); return "summary", nil })}
			f := newFixture(t, engine, service.WithCompaction(config))
			ref := f.create("minimal-create", strings.Repeat("retained source ", 80))
			f.call("bead.close", "minimal-close", map[string]any{"id": ref}, "")
			f.clock = f.clock.Add(30 * 24 * time.Hour)
			before := f.meta()
			f.call("admin.compact", "meta-dry", map[string]any{"dryRun": false, "_meta": map[string]any{"dryRun": true}}, "")
			f.assertSameHead(before)
			f.assertNoReceipt("admin.compact", "meta-dry")
			if calls.Load() != 0 {
				t.Fatal("transport meta dryRun generated a summary")
			}
			params := map[string]any{"_meta": map[string]any{"return": "minimal"}}
			first := f.call("admin.compact", "minimal-compact", params, "")
			for _, value := range first["results"].([]any) {
				if value.(map[string]any)["record"] != nil {
					t.Fatal("minimal receipt included a record")
				}
			}
			before = f.meta()
			f.options = []service.Option{service.WithCompaction(service.CompactionConfig{})}
			if engine != "memory" {
				f.close()
				f.open()
			} else {
				f.bind()
			}
			replay := f.call("admin.compact", "minimal-compact", params, "")
			if !reflect.DeepEqual(first, replay) || calls.Load() != 1 {
				t.Fatalf("minimal replay required provider or changed response: calls=%d", calls.Load())
			}
			assertReplayed(t, f)
			f.assertSameHead(before)
			f.call("admin.compact", "minimal-compact", map[string]any{}, "idempotency_key_reused")
			f.assertSameHead(before)
		})
	}
}
