package compaction_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/service"
)

func TestB12ProjectScopeAndReversalPublicationFailure(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, stage := range []string{"put", "commit"} {
				t.Run(stage, func(t *testing.T) {
					authority := &presenceAuthority{}
					config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) { return "summary", nil }), VerifyPresence: authority.verify}
					f := newFixture(t, engine, service.WithCompaction(config))
					text := strings.Repeat("Retained project source. ", 120)
					other := f.call("bead.create", "other-project-create", map[string]any{"type": "issue", "fields": map[string]any{"title": "other project", "description": text}, "_meta": map[string]any{"project": "other"}}, "")
					otherRef := other["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
					f.call("bead.close", "other-project-close", map[string]any{"id": otherRef, "_meta": map[string]any{"project": "other"}}, "")
					f.clock = f.clock.Add(time.Hour)
					ref := f.create("publication-create", text)
					closed := f.call("bead.close", "publication-close", map[string]any{"id": ref}, "")
					f.clock = f.clock.Add(30 * 24 * time.Hour)
					compact := f.call("admin.compact", "publication-compact", map[string]any{}, "")
					assertCommitTarget(t, compact, ref)
					if len(f.versions(otherRef)) != 2 {
						t.Fatal("compacted another project")
					}
					params := map[string]any{"ref": ref, "baseRevision": compact["revision"], "expect": []any{}}
					params["humanPresence"] = authority.issue(f, params, "publication-revert")
					observed := &observedStore{Store: f.store, failPut: stage == "put", failCommit: stage == "commit"}
					f.store = observed
					f.bind()
					before := f.meta()
					versions := f.versions(ref)
					log := f.changesets()
					f.call("history.revert", "publication-revert", params, "internal_error")
					wantPutFailures, wantCommitFailures := 0, 0
					if stage == "put" {
						wantPutFailures = 1
					} else {
						wantCommitFailures = 1
					}
					if observed.putFailures != wantPutFailures || observed.commitFailures != wantCommitFailures {
						t.Fatalf("injected publication faults: put=%d commit=%d; want put=%d commit=%d", observed.putFailures, observed.commitFailures, wantPutFailures, wantCommitFailures)
					}
					f.assertSameHead(before)
					f.assertNoReceipt("history.revert", "publication-revert")
					if !reflect.DeepEqual(versions, f.versions(ref)) || !reflect.DeepEqual(log, f.changesets()) {
						t.Fatal("failed reversal published permanent state")
					}
					assertTextAndTier(t, f, ref, "summary", 1)
					assertHistoricalText(t, f, ref, closed["revision"], text)
					// Retrying the failed operation after the fault is removed is a new
					// real attempt, not a fabricated successful replay of failed state.
					observed.failPut, observed.failCommit = false, false
					f.call("history.revert", "publication-revert", params, "")
					assertHistoricalText(t, f, ref, compact["revision"], "summary")
				})
			}
		})
	}
}
