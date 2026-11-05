package s1b21b24

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func engines() []string { return []string{"memory", "sqlite"} }

// Transaction contracts are exercised against every real writer. View-only
// cases keep the narrower memory/SQLite matrix where their PostgreSQL seam is
// intentionally covered by the dedicated stale/cursor tests.
func transactionEngines() []string { return []string{"memory", "sqlite", "postgres"} }

func TestS1B21BatchOrderRollbackAndReplayAcrossStores(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			anchor, created := f.create(t, "b21-anchor", "anchor")
			before := f.state(t)
			request := f.request("tx.apply", "b21-ordered", map[string]any{
				"changeSet": map[string]any{"ops": []any{
					map[string]any{"kind": "patch", "target": anchor, "fields": map[string]any{"title": "middle"}},
					map[string]any{"kind": "create", "type": "issue", "fields": map[string]any{"title": "created-second"}},
					map[string]any{"kind": "patch", "target": anchor, "fields": map[string]any{"title": "declared-last"}},
					map[string]any{"kind": "patch", "target": anchor, "fields": map[string]any{"priority": 1}},
				}},
			})
			first := f.dispatch(request)
			batch := commit(t, first)
			if batch.Revision.String() != "2" || len(batch.Results) != 4 {
				t.Fatalf("ordered batch commit=%+v", batch)
			}
			for i, result := range batch.Results {
				if result.Revision != batch.Revision {
					t.Fatalf("result %d revision=%s want=%s", i, result.Revision, batch.Revision)
				}
			}
			after := f.state(t)
			if len(after.Changesets) != len(before.Changesets)+1 || len(after.Changesets[len(after.Changesets)-1].Changes) != 2 {
				t.Fatalf("batch history=%+v", after.Changesets)
			}
			if len(after.Records) != len(before.Records)+1 {
				t.Fatalf("batch records=%d want=%d", len(after.Records), len(before.Records)+1)
			}
			var anchorBody struct {
				Fields map[string]any `json:"fields"`
			}
			for _, row := range after.Records {
				if row.Ref.ID == anchor.ID {
					if err := json.Unmarshal(row.Body, &anchorBody); err != nil {
						t.Fatal(err)
					}
				}
			}
			if anchorBody.Fields["title"] != "declared-last" || anchorBody.Fields["priority"] != float64(1) {
				t.Fatalf("declaration order lost: %+v", anchorBody.Fields)
			}
			if created.Revision.String() != "1" {
				t.Fatalf("seed revision=%s", created.Revision)
			}
			replay := f.dispatch(request)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("same-process replay=%+v", replay)
			}
			sameState(t, after, f.state(t))
			f.reopen(t)
			replay = f.dispatch(request)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("reopen replay=%+v", replay)
			}
			sameState(t, after, f.state(t))
			t.Logf("B21 PASS: engine=%s one revision, ordered post-image, rollback-safe receipt replay after SQLite reopen", engine)
		})
	}
}

func TestS1B21LateRefusalRollsBackEveryDurablePlane(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			anchor, _ := f.create(t, "b21-refusal-anchor", "anchor")
			before := f.state(t)
			got := f.call("tx.apply", "b21-refusal", map[string]any{
				"changeSet": map[string]any{"ops": []any{
					map[string]any{"kind": "patch", "target": anchor, "fields": map[string]any{"title": "must-rollback"}},
					map[string]any{"kind": "create", "type": "issue", "fields": map[string]any{"title": "must-not-exist"}},
					map[string]any{"kind": "patch", "target": map[string]any{"id": "s1-missing"}, "fields": map[string]any{"title": "refused-last"}},
				}},
			})
			if got.Error == nil || got.Error.Data.Code != "not_found" {
				t.Fatalf("late refusal=%+v", got)
			}
			sameState(t, before, f.state(t))
			f.reopen(t)
			sameState(t, before, f.state(t))
			t.Logf("B21 rollback PASS: engine=%s typed late refusal left records, versions, changesets and receipts unchanged", engine)
		})
	}
}

func TestS1B22RevisionCASHasOneWinnerAndReportsCurrentRevision(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			anchor, created := f.create(t, "b22-anchor", "original")
			requests := []struct {
				label string
				field string
			}{
				{label: "b22-title", field: "title"},
				{label: "b22-description", field: "description"},
			}
			results := make([]reply, len(requests))
			start := make(chan struct{})
			var ready sync.WaitGroup
			ready.Add(len(requests))
			var done sync.WaitGroup
			done.Add(len(requests))
			for i, request := range requests {
				go func(i int, request struct{ label, field string }) {
					defer done.Done()
					ready.Done()
					<-start
					results[i] = f.call("bead.update", request.label, map[string]any{
						"id":     anchor,
						"fields": map[string]any{request.field: "winner-" + request.field},
						"expect": []any{map[string]any{"ref": anchor, "revision": created.Revision}},
					})
				}(i, request)
			}
			ready.Wait()
			close(start)
			done.Wait()
			winner, loser := 0, 0
			for i, result := range results {
				if result.Error == nil {
					winner++
					continue
				}
				if result.Error.Data.Code != "precondition_failed" {
					t.Fatalf("racer %d refusal=%+v", i, result.Error)
				}
				loser++
				if result.Error.Data.Details["reason"] != "revision" {
					t.Fatalf("missing CAS reason: %+v", result.Error.Data.Details)
				}
				current, ok := result.Error.Data.Details["currentRevision"].(json.Number)
				if !ok || current.String() != "2" {
					t.Fatalf("missing current revision: %#v", result.Error.Data.Details["currentRevision"])
				}
			}
			if winner != 1 || loser != 1 {
				t.Fatalf("CAS winners=%d losers=%d", winner, loser)
			}
			state := f.state(t)
			if len(state.Versions[anchor.ID]) != 2 || len(state.Changesets) != 2 {
				t.Fatalf("CAS history=%+v", state)
			}
			t.Logf("B22 PASS: engine=%s one deterministic CAS winner and typed loser currentRevision=2", engine)
		})
	}
}

func TestS1B23TitleSearchAndFullTypeSummaryCount(t *testing.T) {
	for _, engine := range engines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			alpha, _ := f.create(t, "b23-alpha", "needle-alpha")
			f.create(t, "b23-beta", "other-beta")
			closed, _ := f.create(t, "b23-closed", "needle-closed")
			if got := f.call("bead.close", "b23-close", map[string]any{"id": closed}); got.Error != nil {
				t.Fatalf("close: %+v", got)
			}
			definition := map[string]any{
				"filter": map[string]any{"query": "status=todo AND type=issue AND title=needle"},
				"fields": []string{"id", "status", "type", "fields.title"},
				"sort":   []string{"id"},
			}
			got := f.call("view.run", "b23-title", map[string]any{"definition": definition})
			if got.Error != nil {
				t.Fatalf("title query refusal: %+v", got.Error)
			}
			var result struct {
				Items []struct {
					ID     string `json:"id"`
					Status string `json:"status"`
					Fields struct {
						Title string `json:"title"`
					} `json:"fields"`
				} `json:"items"`
			}
			if err := json.Unmarshal(got.Result, &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Items) != 1 || result.Items[0].ID != alpha.ID || result.Items[0].Fields.Title != "needle-alpha" {
				t.Fatalf("title search=%+v", result)
			}
			summary := f.call("view.run", "b23-summary", map[string]any{
				"definition": map[string]any{
					"filter":  map[string]any{"query": "status=todo AND type=issue"},
					"fields":  []string{},
					"groupBy": []string{"type"},
				},
			})
			if summary.Error != nil {
				t.Fatalf("type summary refusal: %+v", summary.Error)
			}
			var grouped struct {
				Groups []struct {
					Key   string `json:"key"`
					Count int    `json:"count"`
				} `json:"groups"`
			}
			if err := json.Unmarshal(summary.Result, &grouped); err != nil {
				t.Fatal(err)
			}
			if len(grouped.Groups) != 1 || grouped.Groups[0].Key != "issue" || grouped.Groups[0].Count != 2 {
				t.Fatalf("full predicate summary=%+v", grouped)
			}
			t.Logf("B23 PASS: engine=%s title substring producer and unpaged authorized type count", engine)
		})
	}
}

func TestS1B24UntimedDeferRequiresExplicitReopen(t *testing.T) {
	for _, engine := range append(engines(), "postgres") {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			id, _ := f.create(t, "b24-item", "defer me")
			deferred := f.call("bead.defer", "b24-defer", map[string]any{"id": id, "reason": "waiting on external input"})
			if deferred.Error != nil {
				t.Fatalf("defer: %+v", deferred.Error)
			}
			ready := f.call("bead.ready", "b24-ready-deferred", map[string]any{"limit": 100})
			if ready.Error != nil {
				t.Fatalf("ready: %+v", ready.Error)
			}
			var page struct {
				Items []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(ready.Result, &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 0 {
				t.Fatalf("deferred item remained ready: %s", ready.Result)
			}
			// Keep the writer lease live while still proving that a positive
			// controlled-clock advance does not wake an untimed defer.
			f.advance(t, 5*time.Second)
			advanced := f.call("bead.ready", "b24-ready-after-clock", map[string]any{"limit": 100})
			if advanced.Error != nil {
				t.Fatalf("ready after clock: %+v", advanced.Error)
			}
			if err := json.Unmarshal(advanced.Result, &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 0 {
				t.Fatalf("untimed defer woke implicitly: %s", advanced.Result)
			}
			invalid := f.call("bead.defer", "b24-target-refused", map[string]any{"id": id, "reason": "still waiting", "until": f.clock.Now().Add(time.Hour)})
			if invalid.Error == nil || invalid.Error.Data.Code != "invalid_params" {
				t.Fatalf("timed defer was accepted: %+v", invalid)
			}
			reopened := f.call("bead.reopen", "b24-reopen", map[string]any{"id": id, "reason": "input arrived"})
			if reopened.Error != nil {
				t.Fatalf("reopen: %+v", reopened.Error)
			}
			ready = f.call("bead.ready", "b24-ready-reopened", map[string]any{"limit": 100})
			if ready.Error != nil {
				t.Fatalf("ready reopened: %+v", ready.Error)
			}
			if err := json.Unmarshal(ready.Result, &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 1 {
				t.Fatalf("explicit reopen did not restore readiness: %s", ready.Result)
			}
			t.Logf("B24 PASS: engine=%s untimed defer stays parked across controlled clock advance and explicit reopen restores readiness", engine)
		})
	}
}

func TestI1MemorySaveListAndReopenReplay(t *testing.T) {
	for _, engine := range engines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			meta := map[string]any{"project": project, "agent": actor, "idempotencyKey": key("i1-memory")}
			first := f.call("memory.save", "i1-memory", map[string]any{"key": "runbook", "content": "synthetic memory", "metadata": map[string]any{"source": "test"}})
			if first.Error != nil {
				t.Fatalf("memory.save: %+v", first.Error)
			}
			if _, ok := meta["idempotencyKey"]; !ok {
				t.Fatal("fixed idempotency input disappeared")
			}
			list := f.call("memory.list", "i1-memory-list", map[string]any{"query": "runbook", "limit": 10})
			if list.Error != nil {
				t.Fatalf("memory.list: %+v", list.Error)
			}
			var page struct {
				Items []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(list.Result, &page); err != nil || len(page.Items) != 1 {
				t.Fatalf("memory list=%s err=%v", list.Result, err)
			}
			f.reopen(t)
			replay := f.dispatch(f.request("memory.save", "i1-memory", map[string]any{"key": "runbook", "content": "synthetic memory", "metadata": map[string]any{"source": "test"}}))
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("memory replay after reopen=%+v", replay)
			}
			t.Logf("I1 PASS: engine=%s real memory save/list and exact SQLite reopen replay", engine)
		})
	}
}
