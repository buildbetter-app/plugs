package s1b21b24

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

type b22Post struct {
	mu      sync.Mutex
	changes []port.CommittedChange
}

func (p *b22Post) After(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
	view := reader.Snapshot()
	if view.At == nil || *view.At != change.Revision {
		return fmt.Errorf("B22 subscriber snapshot is not pinned to its commit")
	}
	for _, item := range change.Changes {
		rows, err := reader.Get(ctx, []model.Ref{item.Resource.Ref})
		if err != nil || len(rows) != 1 || rows[0].Revision != change.Revision || !bytes.Equal(rows[0].Body, item.After) {
			return fmt.Errorf("B22 subscriber final image differs from committed row: %v", err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.changes = append(p.changes, change)
	return nil
}

func (p *b22Post) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.changes)
}

type b22Validator func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (v b22Validator) Validate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	return v(ctx, p, r)
}

// Read every physical application table and every column, including expired
// receipts, tombstones, blobs, indexes, checkpoints and empty relations. A
// repeatable read keeps the independently opened SQL census consistent.
type b22Relation struct {
	Columns []string
	Rows    []json.RawMessage
}

func b22Physical(t *testing.T, f *fixture, label string) map[string]b22Relation {
	t.Helper()
	if f.engine == "memory" {
		return nil
	}
	name, dsn := "sqlite", f.path
	if f.engine == "postgres" {
		name, dsn = "pgx", f.dsn
	}
	db, err := sql.Open(name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(f.ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	if f.engine == "postgres" {
		query = `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='plug_postgres' ORDER BY tablename`
	}
	rows, err := tx.QueryContext(f.ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil || len(names) == 0 {
		t.Fatalf("B22 physical table inventory: %v / %v", names, err)
	}
	out := map[string]b22Relation{}
	for _, name := range names {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if f.engine == "postgres" {
			quoted = `plug_postgres.` + quoted
		}
		rows, err := tx.QueryContext(f.ctx, "SELECT * FROM "+quoted)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		relation := b22Relation{Columns: columns, Rows: []json.RawMessage{}}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			relation.Rows = append(relation.Rows, encoded)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Slice(relation.Rows, func(i, j int) bool { return bytes.Compare(relation.Rows[i], relation.Rows[j]) < 0 })
		out[name] = relation
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	b22Evidence(t, label+"-physical", out)
	return out
}

func b22Evidence(t *testing.T, label string, value any) {
	t.Helper()
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" {
		return
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	name := strings.ReplaceAll(t.Name(), "/", "_") + "-" + label + ".json"
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func b22SamePhysical(t *testing.T, before, after map[string]b22Relation, reopened bool) {
	t.Helper()
	// Only an explicit release/new acquisition changes lease ownership. Every
	// other relation, including expired receipt bytes, remains exact.
	copyRelations := func(input map[string]b22Relation) map[string]b22Relation {
		if input == nil {
			return nil
		}
		out := map[string]b22Relation{}
		for name, value := range input {
			if reopened && (name == "lease" || name == "active_lease") {
				continue
			}
			out[name] = value
		}
		return out
	}
	if !reflect.DeepEqual(copyRelations(before), copyRelations(after)) {
		t.Fatal("B22 physical application rows/columns changed; inspect named before/after census outputs")
	}
}

func TestS1B22GuardedReceiptExactExpiryRechecksCASAndReopens(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			post := &b22Post{}
			f := newFixture(t, engine, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-expiry", Subscriber: post}))
			target, seed := f.create(t, "b22-expiry-seed", "original")
			params := map[string]any{"id": target, "fields": map[string]any{"description": "committed"}, "expect": []model.Precondition{model.PreconditionByRevision(target, seed.Revision)}}
			request := f.request("bead.update", "b22-expiry-winner", params)
			first := f.dispatch(request)
			winner := commit(t, first)
			if first.Meta.Replayed || winner.Revision != model.Natural64(2) || post.count() != 2 {
				t.Fatalf("B22 first CAS did not publish exactly once: %+v posts=%d", first, post.count())
			}
			receipt := f.casReceipt(t, "bead.update", "b22-expiry-winner")
			if receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, winner) || receipt.Changeset.Revision != winner.Revision {
				t.Fatal("B22 first CAS receipt is not the full final Commit/changeset")
			}
			b22Evidence(t, "original-receipt", receipt)
			tick := time.Nanosecond
			if engine == "postgres" {
				tick = time.Microsecond
			}
			f.advance(t, driver.IdempotencyWindow-tick)
			if engine == "postgres" && !testclock.PostgresNow(t, f.dsn).Equal(f.clock.Now()) {
				t.Fatal("B22 application and actual SQL receipt clocks differ")
			}
			before := f.state(t)
			physical := b22Physical(t, f, "before-expiry")
			for _, reopen := range []bool{false, true} {
				if reopen {
					f.reopen(t)
				}
				replayed := f.dispatch(request)
				if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(replayed.Result, first.Result) {
					t.Fatalf("B22 receipt was not replayed immediately before expiry: %+v", replayed)
				}
				sameState(t, before, f.state(t))
				b22SamePhysical(t, physical, b22Physical(t, f, fmt.Sprintf("before-expiry-reopen-%v", reopen)), reopen)
			}
			f.advance(t, tick)
			for _, boundary := range []string{"exactly", "after"} {
				if boundary == "after" {
					f.advance(t, tick)
				}
				before := f.state(t)
				physical := b22Physical(t, f, boundary+"-before")
				if f.casReceipt(t, "bead.update", "b22-expiry-winner") != nil {
					t.Fatal("B22 expired receipt is still admitted for replay")
				}
				for _, reopen := range []bool{false, true} {
					if reopen {
						f.reopen(t)
					}
					refused := f.dispatch(request)
					requireCASReason(t, refused, "revision", "2")
					sameState(t, before, f.state(t))
					b22SamePhysical(t, physical, b22Physical(t, f, fmt.Sprintf("%s-reopen-%v", boundary, reopen)), reopen)
					if f.casReceipt(t, "bead.update", "b22-expiry-winner") != nil || post.count() != 2 {
						t.Fatal("B22 stale expired winner published a replacement receipt or callback")
					}
				}
			}
			params["expect"] = []model.Precondition{model.PreconditionByRevision(target, winner.Revision)}
			params["fields"] = map[string]any{"description": "corrected after expiry"}
			correctedRequest := f.request("bead.update", "b22-expiry-winner", params)
			corrected := f.dispatch(correctedRequest)
			if corrected.Meta.Replayed || commit(t, corrected).Revision != model.Natural64(3) || post.count() != 3 {
				t.Fatalf("B22 expired/refused key was not reusable with a current guard: %+v", corrected)
			}
			after := f.state(t)
			physical = b22Physical(t, f, "corrected")
			f.reopen(t)
			replayed := f.dispatch(correctedRequest)
			if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(replayed.Result, corrected.Result) || post.count() != 3 {
				t.Fatalf("B22 corrected full receipt did not survive reopen: %+v", replayed)
			}
			sameState(t, after, f.state(t))
			b22SamePhysical(t, physical, b22Physical(t, f, "corrected-reopened"), true)
			t.Logf("B22 %s before/exact/after 24h, real SQL clock, full stored replay, stale CAS, reusable key, physical census and reopen PASS", engine)
		})
	}
}

func TestS1B22SourceReadGuardRetriesBoundAndConservesRefusal(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, continual := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continual-%v", engine, continual), func(t *testing.T) {
				const label = "b22-observed-source"
				var f *fixture
				var source model.Ref
				var calls atomic.Int32
				var lastSourceState storedState
				var lastPhysical map[string]b22Relation
				post := &b22Post{}
				validator := b22Validator(func(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
					if p.RequestID != key(label) {
						return port.Verdict{Allow: true}, nil
					}
					rows, err := r.Get(ctx, []model.Ref{source})
					if err != nil || len(rows) != 1 {
						return port.Verdict{}, fmt.Errorf("B22 watched source read: %v", err)
					}
					call := calls.Add(1)
					b22Evidence(t, fmt.Sprintf("source-read-%d", call), rows)
					if continual || call == 1 {
						// A distinct committed request races this pinned source read.
						// The tested callback itself receives no write capability.
						commit(t, f.call("bead.update", fmt.Sprintf("b22-source-interference-%d", call), map[string]any{"id": source, "fields": map[string]any{"description": fmt.Sprint(call)}}))
						lastSourceState = f.state(t)
						lastPhysical = b22Physical(t, f, fmt.Sprintf("source-committed-%d", call))
					}
					return port.Verdict{Allow: true}, nil
				})
				f = newFixture(t, engine, pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b22-source-read", Validator: validator}), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-source-post", Subscriber: post}))
				source, _ = f.create(t, "b22-source-seed", "source")
				target, _ := f.create(t, "b22-target-seed", "target")
				request := f.request("bead.update", label, map[string]any{"id": target, "fields": map[string]any{"description": "accepted against fresh source"}})
				result := f.dispatch(request)
				if continual {
					if result.Error == nil || result.Error.Data.Code != "write_conflict" || !result.Error.Data.Retryable || result.Meta.Replayed || len(result.Result) != 0 || calls.Load() != 4 {
						t.Fatalf("B22 exhausted source guard did not stop after three retries: %+v calls=%d", result, calls.Load())
					}
					sameState(t, lastSourceState, f.state(t))
					b22SamePhysical(t, lastPhysical, b22Physical(t, f, "exhausted"), false)
					if f.casReceipt(t, "bead.update", label) != nil || post.count() != 6 || len(f.state(t).Versions[target.ID]) != 1 {
						t.Fatal("B22 exhausted source guard published a target version/receipt/callback")
					}
				} else {
					c := commit(t, result)
					if calls.Load() != 2 || c.Revision != model.Natural64(4) || post.count() != 4 || len(f.state(t).Versions[target.ID]) != 2 {
						t.Fatalf("B22 source guard did not restart once against latest state: %+v calls=%d posts=%d", c, calls.Load(), post.count())
					}
					before := f.state(t)
					replay := f.dispatch(request)
					if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(result.Result, replay.Result) || calls.Load() != 2 || post.count() != 4 {
						t.Fatal("B22 source guard replay reran a callback or changed its stored Commit")
					}
					sameState(t, before, f.state(t))
				}
				before := f.state(t)
				physical := b22Physical(t, f, "terminal")
				f.reopen(t)
				sameState(t, before, f.state(t))
				b22SamePhysical(t, physical, b22Physical(t, f, "terminal-reopened"), true)
				if !continual {
					replay := f.dispatch(request)
					if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(result.Result, replay.Result) || calls.Load() != 2 || post.count() != 4 {
						t.Fatal("B22 source guard reopened receipt reran callbacks")
					}
					sameState(t, before, f.state(t))
				}
				t.Logf("B22 %s continual=%v source guard callback attempts=%d, exact conservation and reopen PASS", engine, continual, calls.Load())
			})
		}
	}
}

func TestS1B22StatusAssigneeAndUnwatchedRevisionGuards(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			post := &b22Post{}
			f := newFixture(t, engine, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-field-guards", Subscriber: post}))
			declared := ontology.DefaultDocument()
			typ := declared.Types["issue"]
			typ.Fields["assignee"] = ontology.Field{Type: "string"}
			declared.Types["issue"] = typ
			compiled, problems := ontology.Compile(declared, nil, nil)
			if len(problems) != 0 {
				t.Fatalf("B22 declared assignee field does not compile: %+v", problems)
			}
			f.ontology.Refresh(compiled)
			target, seed := f.create(t, "b22-field-seed", "target")
			// Assignee is ordinary content here. No claim, heartbeat, reclaim or
			// B05 operation is used to establish the guarded stored field.
			assigned := commit(t, f.call("bead.update", "b22-set-assignee", map[string]any{"id": target, "fields": map[string]any{"assignee": "worker-a"}}))
			if assigned.Revision != model.Natural64(2) {
				t.Fatal("B22 assignee setup did not change the record token")
			}
			for _, tc := range []struct {
				name, reason, current string
				expect                model.Precondition
			}{
				{"status", "hash", "", model.PreconditionByHash(target, "fields.status", casHash(`"done"`))},
				{"assignee", "hash", "", model.PreconditionByHash(target, "fields.assignee", casHash(`"worker-b"`))},
				{"outside-watched-title", "revision", "2", model.PreconditionByRevision(target, seed.Revision)},
			} {
				before := f.state(t)
				physical := b22Physical(t, f, tc.name+"-before")
				label := "b22-field-refused-" + tc.name
				got := f.call("bead.close", label, map[string]any{"id": target, "reason": "must not be appended", "expect": []model.Precondition{tc.expect}})
				requireCASReason(t, got, tc.reason, tc.current)
				sameState(t, before, f.state(t))
				b22SamePhysical(t, physical, b22Physical(t, f, tc.name+"-after"), false)
				if post.count() != 2 || f.casReceipt(t, "bead.close", label) != nil {
					t.Fatal("B22 field/revision refusal published a reason, post callback or receipt")
				}
			}
			request := f.request("bead.close", "b22-current-fields", map[string]any{"id": target, "reason": "guarded close", "expect": []model.Precondition{
				model.PreconditionByHash(target, "fields.status", casHash(`"todo"`)),
				model.PreconditionByHash(target, "fields.assignee", casHash(`"worker-a"`)),
				model.PreconditionByRevision(target, assigned.Revision),
			}})
			first := f.dispatch(request)
			closed := commit(t, first)
			if closed.Revision != model.Natural64(3) || len(closed.Results) != 2 || post.count() != 3 {
				t.Fatalf("B22 current field/revision guards did not close once with both ordered results: %+v", first)
			}
			doc := closeDocument(t, f.state(t), target)
			if doc.Fields["status"] != "done" || doc.Fields["assignee"] != "worker-a" || doc.Fields["title"] != "target" || len(doc.Comments) != 1 || doc.Comments[0].Body != "guarded close" {
				t.Fatalf("B22 guarded close lost holder, unwatched content or exact reason: %+v", doc)
			}
			before := f.state(t)
			physical := b22Physical(t, f, "closed-before-stale-noop")
			stale := f.call("bead.close", "b22-stale-before-noop", map[string]any{"id": target, "expect": []model.Precondition{model.PreconditionByRevision(target, assigned.Revision)}})
			requireCASReason(t, stale, "revision", "3")
			sameState(t, before, f.state(t))
			b22SamePhysical(t, physical, b22Physical(t, f, "closed-after-stale-noop"), false)
			if f.casReceipt(t, "bead.close", "b22-stale-before-noop") != nil {
				t.Fatal("B22 stale otherwise-idempotent close stored a receipt")
			}
			noopRequest := f.request("bead.close", "b22-no-version-noop", map[string]any{"id": target})
			noop := f.dispatch(noopRequest)
			noopCommit := commit(t, noop)
			if noop.Meta.Replayed || noopCommit.Revision != closed.Revision || post.count() != 3 {
				t.Fatalf("B22 omitted-version close was not a receipt-only no-op: %+v", noop)
			}
			sameMutationState(t, before, f.state(t))
			receipt := f.casReceipt(t, "bead.close", "b22-no-version-noop")
			if receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != "" || !reflect.DeepEqual(receipt.Commit, noopCommit) {
				t.Fatal("B22 no-op did not persist its exact final response without a changeset")
			}
			// A later unrelated mutation cannot rewrite either stored response.
			f.create(t, "b22-unrelated-head", "later unrelated record")
			after := f.state(t)
			physical = b22Physical(t, f, "final")
			f.reopen(t)
			for _, tc := range []struct {
				request []byte
				result  json.RawMessage
			}{{request, first.Result}, {noopRequest, noop.Result}} {
				replay := f.dispatch(tc.request)
				if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, tc.result) || post.count() != 4 {
					t.Fatalf("B22 final receipt changed revision/results after unrelated head and reopen: %+v", replay)
				}
				sameState(t, after, f.state(t))
			}
			b22SamePhysical(t, physical, b22Physical(t, f, "final-reopened"), true)
			t.Logf("B22 %s status/assignee hashes, current and omitted revisions, unwatched-field invalidation, stale-before-noop and exact reopen receipts PASS", engine)
		})
	}
}

func TestS1B22LifecycleCASRacesBindWinnerEventsAndReopen(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, mixed := range []bool{false, true} {
			name, racers := "close-close", 16
			if mixed {
				name, racers = "update-close", 8
			}
			t.Run(engine+"/"+name, func(t *testing.T) {
				post := &b22Post{}
				// Ordinary event plans require no conditional ontology decision in
				// this graph. The real adapter's nil locator fails closed if one
				// is unexpectedly requested; no protected source is fabricated.
				f := newFixture(t, engine,
					pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)),
					pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-race", Subscriber: post}))
				target, seed := f.create(t, "b22-race-seed", "original")
				requests, results, methods := make([][]byte, racers), make([]reply, racers), make([]string, racers)
				for i := range requests {
					params := map[string]any{"id": target, "expect": []model.Precondition{model.PreconditionByRevision(target, seed.Revision)}}
					methods[i] = "bead.close"
					params["reason"] = fmt.Sprintf("winning close %d", i)
					if mixed && i%2 == 0 {
						methods[i] = "bead.update"
						delete(params, "reason")
						params["fields"] = map[string]any{"description": fmt.Sprintf("winning update %d", i)}
					}
					requests[i] = f.request(methods[i], fmt.Sprintf("b22-race-%d", i), params)
				}
				start := make(chan struct{})
				var ready, done sync.WaitGroup
				ready.Add(racers)
				done.Add(racers)
				for i := range requests {
					go func(i int) {
						defer done.Done()
						ready.Done()
						<-start
						results[i] = f.dispatch(requests[i])
					}(i)
				}
				ready.Wait()
				close(start)
				done.Wait()
				winner := -1
				for i, result := range results {
					if result.Error != nil {
						requireCASReason(t, result, "revision", "2")
						if f.casReceipt(t, methods[i], fmt.Sprintf("b22-race-%d", i)) != nil {
							t.Fatal("B22 losing racer published a receipt")
						}
						continue
					}
					if winner != -1 || result.Meta.Replayed || commit(t, result).Revision != model.Natural64(2) {
						t.Fatalf("B22 lifecycle race did not have one fresh revision-2 winner: %+v", results)
					}
					winner = i
				}
				if winner == -1 || post.count() != 2 {
					t.Fatalf("B22 lifecycle race missing its sole winner/post: winner=%d posts=%d", winner, post.count())
				}
				c := commit(t, results[winner])
				after := f.state(t)
				if len(after.Records) != 1 || len(after.Changesets) != 2 || len(after.Changesets[1].Changes) != 1 || len(after.Versions[target.ID]) != 2 || len(after.Receipts) != 2 {
					t.Fatal("B22 lifecycle race published extra row/version/changeset/receipt")
				}
				doc := closeDocument(t, after, target)
				if doc.Lease != nil || doc.Fields["title"] != "original" {
					t.Fatal("B22 lifecycle race changed a lease or unrelated field")
				}
				wantType, wantSlot, wantResults := model.EventBeadClosed, "close", 2
				if methods[winner] == "bead.update" {
					wantType, wantSlot, wantResults = model.EventBeadUpdated, "", 1
					if doc.Fields["status"] != "todo" || doc.Fields["description"] != fmt.Sprintf("winning update %d", winner) || len(doc.Comments) != 0 || doc.ClosedAt != nil {
						t.Fatalf("B22 mixed update winner includes a losing close/change: %+v", doc)
					}
				} else if doc.Fields["status"] != "done" || doc.Fields["description"] != nil || len(doc.Comments) != 1 || doc.Comments[0].Body != fmt.Sprintf("winning close %d", winner) || doc.ClosedAt == nil {
					t.Fatalf("B22 close winner includes a losing update/reason: %+v", doc)
				}
				if len(c.Results) != wantResults {
					t.Fatal("B22 lifecycle winner lost ordered operation results")
				}
				for _, result := range c.Results {
					if result.Ref.ID != target.ID || result.Revision != c.Revision || !bytes.Equal(result.Record, after.Records[0].Body) {
						t.Fatal("B22 lifecycle winner result is not its exact committed final image")
					}
				}
				events := after.Changesets[1].Events
				if len(events) != 1 || events[0].Type != wantType || events[0].Resource.Ref.ID != target.ID || events[0].Index != model.Natural64(0) || events[0].ChangeIndex != model.Natural64(0) || events[0].Provenance.Method != methods[winner] || events[0].Provenance.Slot != wantSlot || events[0].Provenance.Cause.Root != key(fmt.Sprintf("b22-race-%d", winner)) {
					t.Fatalf("B22 lifecycle winner actual event descriptor differs: %+v", events)
				}
				b22Evidence(t, "winner", map[string]any{"index": winner, "method": methods[winner], "commit": c, "changeset": after.Changesets[1], "callback": post.changes[1]})
				physical := b22Physical(t, f, "winner-before-replay")
				for _, reopen := range []bool{false, true} {
					if reopen {
						f.reopen(t)
					}
					for i, request := range requests {
						got := f.dispatch(request)
						if i == winner {
							if got.Error != nil || !got.Meta.Replayed || !bytes.Equal(got.Result, results[i].Result) {
								t.Fatalf("B22 lifecycle winner full receipt changed on reopen=%v: %+v", reopen, got)
							}
						} else {
							requireCASReason(t, got, "revision", "2")
						}
						sameState(t, after, f.state(t))
					}
					if post.count() != 2 {
						t.Fatal("B22 lifecycle retry/replay invoked a second winner callback")
					}
					b22SamePhysical(t, physical, b22Physical(t, f, fmt.Sprintf("replayed-reopen-%v", reopen)), reopen)
				}
				t.Logf("B22 %s %s actual winner=%d method=%s racers=%d sole event/version/changeset/receipt/post and full losing-state/reopen conservation PASS", engine, name, winner, methods[winner], racers)
			})
		}
	}
}
