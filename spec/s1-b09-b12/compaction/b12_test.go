package compaction_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// This is an actual catalogued RPC call after ordinary public create/close,
// not an authored unsupported marker or a fake successful compaction handler.
func TestB12MissingSummaryProviderRefusesWithoutPublication(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := f.create("create-original", strings.Repeat("Original immutable text. ", 400))
			f.call("bead.close", "close-original", map[string]any{"id": ref}, "")
			before := f.call("bead.get", "get-before", map[string]any{"id": ref}, "")
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			f.call("admin.compact", "missing-provider", map[string]any{"beforeRevision": meta.Head, "dryRun": false}, "compaction_unavailable")
			f.assertSameHead(meta)
			f.assertNoReceipt("admin.compact", "missing-provider")
			after := f.call("bead.get", "get-after", map[string]any{"id": ref}, "")
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("unavailable compaction changed original: before=%v after=%v", before, after)
			}
		})
	}
}

func TestB12PublicTierBoundariesHistoryReplayReversalAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var summaries atomic.Int32
			authority := &presenceAuthority{}
			intermediate := strings.Repeat("Tier one supplied summary. ", 60)
			config := service.CompactionConfig{
				Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
					summaries.Add(1)
					if in.Tier == 1 {
						return intermediate, nil
					}
					if in.Tier != 2 {
						return "", fmt.Errorf("unexpected tier %d", in.Tier)
					}
					return "Tier two summary.", nil
				}),
				VerifyPresence: authority.verify,
			}
			f := newFixture(t, engine, service.WithCompaction(config))
			original := strings.Repeat("Original long Unicode text λ. ", 400)
			ref := f.create("original", original)
			closed := f.call("bead.close", "close", map[string]any{"id": ref}, "")
			boundary := closed["revision"]
			closedAt := f.clock
			params := map[string]any{"beforeRevision": revisionAfter(t, boundary), "dryRun": false}
			f.clock = closedAt.Add(30*24*time.Hour - time.Nanosecond)
			before := f.meta()
			f.call("admin.compact", "too-recent", params, "compaction_unavailable")
			f.assertSameHead(before)
			f.clock = closedAt.Add(30 * 24 * time.Hour)
			dry := f.call("admin.compact", "dry-run", map[string]any{"beforeRevision": revisionAfter(t, boundary), "dryRun": true}, "")
			assertCommitTarget(t, dry, ref)
			if dry["wouldCommit"] != true || summaries.Load() != 0 {
				t.Fatalf("dry run called summary or lost candidate: %v calls=%d", dry, summaries.Load())
			}
			f.assertSameHead(before)
			f.assertNoReceipt("admin.compact", "dry-run")
			first := f.call("admin.compact", "tier-one", params, "")
			assertCommitTarget(t, first, ref)
			assertKind(t, f, first["revision"], model.ChangesetWrite)
			if summaries.Load() != 1 {
				t.Fatalf("summaries=%d", summaries.Load())
			}
			assertTextAndTier(t, f, ref, intermediate, 1)
			assertHistoricalText(t, f, ref, boundary, original)
			if len(f.versions(ref)) != 3 {
				t.Fatalf("compaction did not add exactly one permanent version: %+v", f.versions(ref))
			}
			if engine != "memory" {
				f.close()
				f.open()
			}
			stable := f.meta()
			f.clock = closedAt.Add(89 * 24 * time.Hour)
			replayed := f.call("admin.compact", "tier-one", params, "")
			if !reflect.DeepEqual(first, replayed) || summaries.Load() != 1 {
				t.Fatalf("compaction replay recomputed or changed: first=%v replay=%v calls=%d", first, replayed, summaries.Load())
			}
			f.assertSameHead(stable)
			assertReplayed(t, f)
			changedBoundary := revisionAfter(t, first["revision"])
			if reflect.DeepEqual(changedBoundary, params["beforeRevision"]) {
				t.Fatal("changed-boundary fixture did not change the request")
			}
			f.call("admin.compact", "tier-one", map[string]any{"beforeRevision": changedBoundary, "dryRun": false}, "idempotency_key_reused")
			f.assertSameHead(stable)
			f.clock = closedAt.Add(90*24*time.Hour - time.Nanosecond)
			f.call("admin.compact", "tier-two-recent", map[string]any{"dryRun": false}, "compaction_unavailable")
			f.assertSameHead(stable)
			f.clock = closedAt.Add(90 * 24 * time.Hour)
			second := f.call("admin.compact", "tier-two", map[string]any{"dryRun": false}, "")
			assertTextAndTier(t, f, ref, "Tier two summary.", 2)
			assertHistoricalText(t, f, ref, first["revision"], intermediate)
			revert := map[string]any{"ref": ref, "baseRevision": second["revision"], "expect": []any{}}
			proof := authority.issue(f, revert, "restore-tier-two")
			revert["humanPresence"] = proof
			before = f.meta()
			f.call("history.revert", "missing-proof", map[string]any{"ref": ref, "baseRevision": second["revision"], "expect": []any{}}, "invalid_params")
			f.assertNoReceipt("history.revert", "missing-proof")
			badProof := map[string]any{"mode": "presence_check", "challenge": "b12-issued-challenge", "proof": "forged"}
			f.call("history.revert", "forged-revert", map[string]any{"ref": ref, "baseRevision": second["revision"], "expect": []any{}, "humanPresence": badProof}, "human_presence_required")
			f.assertSameHead(before)
			f.assertNoReceipt("history.revert", "forged-revert")
			restored := f.call("history.revert", "restore-tier-two", revert, "")
			assertCommitTarget(t, restored, ref)
			assertKind(t, f, restored["revision"], model.ChangesetRevert)
			assertTextAndTier(t, f, ref, intermediate, 1)
			assertHistoricalText(t, f, ref, second["revision"], "Tier two summary.")
			if len(f.versions(ref)) != 5 {
				t.Fatalf("reversal removed audit or wrote multiple versions: %d", len(f.versions(ref)))
			}
			stable = f.meta()
			again := f.call("history.revert", "restore-tier-two", revert, "")
			if !reflect.DeepEqual(restored, again) {
				t.Fatal("revert replay changed response")
			}
			f.assertSameHead(stable)
			assertReplayed(t, f)
			f.call("bead.update", "unrelated-title", map[string]any{"id": ref, "fields": map[string]any{"title": "Keep this newer title"}}, "")
			revertOne := map[string]any{"ref": ref, "baseRevision": first["revision"], "expect": []any{}}
			revertOne["humanPresence"] = authority.issue(f, revertOne, "restore-tier-one")
			f.call("history.revert", "restore-tier-one", revertOne, "")
			got := f.call("bead.get", "get-final", map[string]any{"id": ref}, "")["bead"].(map[string]any)
			fields := got["fields"].(map[string]any)
			if fields["description"] != original || fields["title"] != "Keep this newer title" {
				t.Fatalf("reversal clobbered unrelated data or lost original: %v", fields)
			}
			if metadata, _ := got["metadata"].(map[string]any); metadata["plug.compaction"] != nil {
				t.Fatalf("tier one restore retained active compaction state: %v", metadata)
			}
			if engine != "memory" {
				f.close()
				f.open()
				assertHistoricalText(t, f, ref, boundary, original)
			}
			if authority.calls.Load() < 4 {
				t.Fatalf("D32 not checked on every revert, including replay: %d", authority.calls.Load())
			}
		})
	}
}

func assertCommitTarget(t *testing.T, commit, ref map[string]any) {
	t.Helper()
	rows := commit["results"].([]any)
	if len(rows) == 0 {
		t.Fatal("compaction returned no selected record")
	}
	for _, value := range rows {
		actual := value.(map[string]any)["ref"].(map[string]any)
		wantRef, gotRef := decodedRef(t, ref), decodedRef(t, actual)
		if gotRef.ID != wantRef.ID || gotRef.Branch.String() != wantRef.Branch.String() {
			t.Fatalf("one call affected another record: got=%v want=%v", actual, ref)
		}
	}
}

func assertTextAndTier(t *testing.T, f *fixture, ref map[string]any, text string, tier int) {
	t.Helper()
	bead := f.call("bead.get", fmt.Sprintf("get-tier-%d-%d", tier, len(f.trace)), map[string]any{"id": ref}, "")["bead"].(map[string]any)
	if bead["fields"].(map[string]any)["description"] != text {
		t.Fatal("summary was not published verbatim")
	}
	state := bead["metadata"].(map[string]any)["plug.compaction"].(map[string]any)
	if state["tier"] != json.Number(fmt.Sprint(tier)) {
		t.Fatalf("compaction tier=%v want %d", state, tier)
	}
}

func assertHistoricalText(t *testing.T, f *fixture, ref map[string]any, revision any, text string) {
	t.Helper()
	version := f.call("history.get", fmt.Sprintf("history-%v-%d", revision, len(f.trace)), map[string]any{"ref": ref, "asOf": map[string]any{"revision": revision}}, "")
	if version["body"].(map[string]any)["fields"].(map[string]any)["description"] != text {
		t.Fatalf("permanent original history lost at %v", revision)
	}
}

func assertReplayed(t *testing.T, f *fixture) {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal(f.trace[len(f.trace)-1], &response); err != nil {
		t.Fatal(err)
	}
	meta, _ := response["_meta"].(map[string]any)
	if meta["replayed"] != true {
		t.Fatalf("missing exact replay metadata: %s", f.trace[len(f.trace)-1])
	}
}

func decodedRef(t *testing.T, value map[string]any) model.Ref {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var ref model.Ref
	if err = json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	return ref
}
