package compaction_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

type authorizeFunc func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error

func (f authorizeFunc) Authorize(c context.Context, s port.AuthorizationStage, p port.ProposedChange, r port.SnapshotReader) error {
	return f(c, s, p, r)
}

func TestB12CompactionCASAndConcurrentStaleSelection(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			t.Run("provider-races-write", func(t *testing.T) {
				var f *fixture
				var ref map[string]any
				config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) {
					f.call("bead.update", "racing-user-write", map[string]any{"id": ref, "fields": map[string]any{"description": "newer user text"}}, "")
					return "small", nil
				})}
				f = newFixture(t, engine, service.WithCompaction(config))
				ref = f.create("cas-create", strings.Repeat("old text ", 100))
				f.call("bead.close", "cas-close", map[string]any{"id": ref}, "")
				f.clock = f.clock.Add(30 * 24 * time.Hour)
				f.call("admin.compact", "stale-summary", map[string]any{}, "precondition_failed")
				f.assertNoReceipt("admin.compact", "stale-summary")
				if len(f.versions(ref)) != 3 {
					t.Fatal("stale summary published a version")
				}
				got := f.call("bead.get", "cas-get", map[string]any{"id": ref}, "")["bead"].(map[string]any)
				if got["fields"].(map[string]any)["description"] != "newer user text" {
					t.Fatal("stale compaction overwrote current text")
				}
			})
			t.Run("two-stale-previews", func(t *testing.T) {
				arrivals := make(chan struct{}, 2)
				release := make(chan struct{})
				config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) {
					arrivals <- struct{}{}
					<-release
					return "one summary", nil
				})}
				f := newFixture(t, engine, service.WithCompaction(config))
				ref := f.create("concurrent-create", strings.Repeat("source ", 100))
				f.call("bead.close", "concurrent-close", map[string]any{"id": ref}, "")
				f.clock = f.clock.Add(30 * 24 * time.Hour)
				type result struct {
					out port.WriteOutcome
					err error
					key string
				}
				results := make(chan result, 2)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for _, key := range []string{"parallel-a", "parallel-b"} {
					go func(key string) {
						out, err := f.domain.CompactWithOutcome(ctx, anonymous, service.CompactionRequest{}, writeMeta("admin.compact", key))
						results <- result{out, err, key}
					}(key)
				}
				for range 2 {
					select {
					case <-arrivals:
					case <-ctx.Done():
						close(release)
						t.Fatal("both real provider previews were not reached")
					}
				}
				close(release)
				wins, refusals := 0, 0
				for range 2 {
					got := <-results
					if got.err == nil {
						wins++
						if got.out.Replayed {
							t.Fatal("distinct-key race reported replay")
						}
						continue
					}
					var typed *model.Error
					if !errors.As(got.err, &typed) || (typed.Data.Code != "precondition_failed" && typed.Data.Code != "write_conflict") {
						t.Fatalf("unexpected stale-preview failure: %v", got.err)
					}
					refusals++
					f.assertNoReceipt("admin.compact", got.key)
				}
				if wins != 1 || refusals != 1 || len(f.versions(ref)) != 3 {
					t.Fatalf("wins=%d refusals=%d versions=%d", wins, refusals, len(f.versions(ref)))
				}
				assertTextAndTier(t, f, ref, "one summary", 1)
			})
		})
	}
}

func TestB12WriterAuthorizationBeforeProviderAndOnReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			var denied atomic.Bool
			var replayChecks atomic.Int32
			authority := &presenceAuthority{}
			config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) { calls.Add(1); return "summary", nil }), VerifyPresence: authority.verify}
			f := newFixture(t, engine, service.WithCompaction(config))
			ref := f.create("auth-create", strings.Repeat("retained text ", 100))
			f.call("bead.close", "auth-close", map[string]any{"id": ref}, "")
			f.clock = f.clock.Add(30 * 24 * time.Hour)
			f.pipelineOptions = []pipeline.Option{pipeline.WithAuthorizer(authorizeFunc(func(_ context.Context, stage port.AuthorizationStage, p port.ProposedChange, _ port.SnapshotReader) error {
				if stage == port.AuthorizeReplay {
					replayChecks.Add(1)
				}
				if denied.Load() && (p.Meta.Method == "admin.compact" || p.Meta.Method == "history.revert") {
					return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
				return nil
			}))}
			f.bind()
			denied.Store(true)
			before := f.meta()
			f.call("admin.compact", "denied-compact", map[string]any{}, "forbidden")
			f.assertSameHead(before)
			f.assertNoReceipt("admin.compact", "denied-compact")
			if calls.Load() != 0 {
				t.Fatal("denied caller disclosed original to provider")
			}
			denied.Store(false)
			compact := f.call("admin.compact", "allowed-compact", map[string]any{}, "")
			denied.Store(true)
			before = f.meta()
			f.call("admin.compact", "allowed-compact", map[string]any{}, "forbidden")
			f.assertSameHead(before)
			if calls.Load() != 1 || replayChecks.Load() != 1 {
				t.Fatalf("replay reran provider or missed current Writer authority: calls=%d replay=%d", calls.Load(), replayChecks.Load())
			}
			params := map[string]any{"ref": ref, "baseRevision": compact["revision"], "expect": []any{}}
			params["humanPresence"] = authority.issue(f, params, "denied-revert")
			f.call("history.revert", "denied-revert", params, "forbidden")
			f.assertSameHead(before)
			f.assertNoReceipt("history.revert", "denied-revert")
			denied.Store(false)
			restored := f.call("history.revert", "allowed-revert", params, "")
			assertKind(t, f, restored["revision"], model.ChangesetRevert)
			denied.Store(true)
			before = f.meta()
			f.call("history.revert", "allowed-revert", params, "forbidden")
			f.assertSameHead(before)
		})
	}
}

func TestB12RevertAllTextPermanentHistoryProofCASAndProtectedRecords(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			original := map[string]any{"title": "title survives", "description": strings.Repeat("description λ ", 700), "design": strings.Repeat("design ", 900), "notes": "notes", "acceptance_criteria": "acceptance"}
			authority := &presenceAuthority{}
			config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
				for _, name := range []string{"description", "design", "notes", "acceptance_criteria"} {
					if in.Text[name] != original[name] {
						return "", fmt.Errorf("missing supplied input %s", name)
					}
				}
				return "summary", nil
			}), VerifyPresence: authority.verify}
			f := newFixture(t, engine, service.WithCompaction(config))
			f.extendedOntology = true
			f.bind()
			made := f.call("bead.create", "all-text-create", map[string]any{"type": "issue", "fields": original}, "")
			ref := made["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
			closed := f.call("bead.close", "all-text-close", map[string]any{"id": ref}, "")
			f.clock = f.clock.Add(30 * 24 * time.Hour)
			compact := f.call("admin.compact", "all-text-compact", map[string]any{}, "")
			got := f.call("bead.get", "all-text-compacted", map[string]any{"id": ref}, "")["bead"].(map[string]any)["fields"].(map[string]any)
			for _, name := range []string{"design", "notes", "acceptance_criteria"} {
				if got[name] != "" {
					t.Fatalf("%s was not compacted", name)
				}
			}
			diff := f.call("history.diff", "all-text-diff", map[string]any{"ref": ref, "fromRevision": closed["revision"], "toRevision": compact["revision"]}, "")
			for _, name := range []string{"description", "design", "notes", "acceptance_criteria"} {
				found := false
				for _, p := range diff["changedPaths"].([]any) {
					if p == "/fields/"+name {
						found = true
					}
				}
				if !found {
					t.Fatalf("permanent diff omitted %s", name)
				}
			}
			params := map[string]any{"ref": ref, "baseRevision": compact["revision"], "expect": []any{}}
			params["humanPresence"] = authority.issue(f, params, "all-text-revert")
			// A proof issued for the real record cannot authorize a different base.
			wrong := map[string]any{"ref": ref, "baseRevision": closed["revision"], "expect": []any{}, "humanPresence": params["humanPresence"]}
			before := f.meta()
			f.call("history.revert", "changed-proof-binding", wrong, "human_presence_required")
			f.assertSameHead(before)
			f.assertNoReceipt("history.revert", "changed-proof-binding")
			for _, id := range []string{"ontology/project", "plugins/current", "plugin_lock/active", "instance_config/plugin"} {
				protected := map[string]any{"ref": map[string]any{"id": id}, "baseRevision": compact["revision"], "expect": []any{}}
				protected["humanPresence"] = authority.issue(f, protected, "protected-"+id)
				f.call("history.revert", "protected-"+id, protected, "protected_record")
				f.assertSameHead(before)
				f.assertNoReceipt("history.revert", "protected-"+id)
			}
			f.call("bead.update", "edit-compacted-field", map[string]any{"id": ref, "fields": map[string]any{"notes": "newer notes"}}, "")
			before = f.meta()
			failed := f.call("history.revert", "field-conflict", params, "precondition_failed")
			details := failed["error"].(map[string]any)["data"].(map[string]any)["details"].(map[string]any)
			if details["reason"] != "revert" {
				t.Fatalf("wrong CAS reason: %v", details)
			}
			// The revert refusal keeps the refusing guard address that every
			// compare-and-set refusal carries; its guards all name the target.
			if guard, ok := details["ref"].(map[string]any); !ok || guard["id"] != ref["id"] {
				t.Fatalf("revert CAS refusal lost its refusing guard ref: %v", details)
			}
			f.assertSameHead(before)
			f.assertNoReceipt("history.revert", "field-conflict")
			f.call("bead.update", "return-compacted-field", map[string]any{"id": ref, "fields": map[string]any{"notes": "", "title": "newer title"}}, "")
			// Meta dryRun is exercised through the direct public domain port.
			req := service.CompactionRevertRequest{}
			raw, _ := json.Marshal(params)
			if err := json.Unmarshal(raw, &req); err != nil {
				t.Fatal(err)
			}
			dryMeta := writeMeta("history.revert", "revert-dry")
			dryMeta.DryRun = true
			digest, err := service.CompactionRevertDigest(req, dryMeta, f.meta().Incarnation)
			if err != nil {
				t.Fatal(err)
			}
			authority.mu.Lock()
			authority.issued[digest] = "dry-issued"
			authority.mu.Unlock()
			req.HumanPresence = map[string]any{"mode": "presence_check", "challenge": digest, "proof": "dry-issued"}
			before = f.meta()
			dry, err := f.domain.RevertCompactionWithOutcome(context.Background(), anonymous, req, dryMeta)
			if err != nil || !dry.Commit.WouldCommit {
				t.Fatalf("revert dry=%+v err=%v", dry, err)
			}
			f.assertSameHead(before)
			f.assertNoReceipt("history.revert", "revert-dry")
			restored := f.call("history.revert", "all-text-revert", params, "")
			assertKind(t, f, restored["revision"], model.ChangesetRevert)
			got = f.call("bead.get", "all-text-restored", map[string]any{"id": ref}, "")["bead"].(map[string]any)["fields"].(map[string]any)
			for _, name := range []string{"description", "design", "notes", "acceptance_criteria"} {
				if !reflect.DeepEqual(got[name], original[name]) {
					t.Fatalf("%s was not restored verbatim", name)
				}
			}
			if got["title"] != "newer title" {
				t.Fatal("revert clobbered unrelated current title")
			}
			assertHistoricalText(t, f, ref, closed["revision"], original["description"].(string))
			assertHistoricalText(t, f, ref, compact["revision"], "summary")
			if engine != "memory" {
				f.close()
				f.open()
				assertHistoricalText(t, f, ref, closed["revision"], original["description"].(string))
				assertHistoricalText(t, f, ref, restored["revision"], original["description"].(string))
			}
		})
	}
}
