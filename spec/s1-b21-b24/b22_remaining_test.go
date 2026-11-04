package s1b21b24

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

func b22CASRefusal(t *testing.T, got reply, target model.Ref, reason, current string) {
	t.Helper()
	requireCASReason(t, got, reason, current)
	actual, err := json.Marshal(got.Error.Data.Details["ref"])
	want, wantErr := json.Marshal(target)
	if err != nil || wantErr != nil || !bytes.Equal(actual, want) {
		t.Fatalf("B22 refusing address=%s want independently selected guard=%s: %v/%v", actual, want, err, wantErr)
	}
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
					b22CASRefusal(t, refused, target, "revision", "2")
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
				type pause struct {
					call     int32
					observed model.Revision
					resume   chan struct{}
				}
				observed := make(chan pause)
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
						// Retain the pipeline's original bounded callback context
						// and tracked reader. Only the external test client writes.
						p := pause{call: call, observed: rows[0].Revision, resume: make(chan struct{})}
						select {
						case observed <- p:
						case <-ctx.Done():
							return port.Verdict{}, ctx.Err()
						}
						select {
						case <-p.resume:
						case <-ctx.Done():
							return port.Verdict{}, ctx.Err()
						}
					}
					return port.Verdict{Allow: true}, nil
				})
				f = newFixture(t, engine, pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b22-source-read", Validator: validator}), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-source-post", Subscriber: post}))
				source, _ = f.create(t, "b22-source-seed", "source")
				target, _ := f.create(t, "b22-target-seed", "target")
				request := f.request("bead.update", label, map[string]any{"id": target, "fields": map[string]any{"description": "accepted against fresh source"}})
				finished := make(chan reply, 1)
				go func() { finished <- f.dispatch(request) }()
				interferences := 1
				if continual {
					interferences = 4
				}
				for i := 1; i <= interferences; i++ {
					var paused pause
					select {
					case paused = <-observed:
					case got := <-finished:
						t.Fatalf("B22 target finished before public interference %d: %+v", i, got)
					case <-time.After(10 * time.Second):
						t.Fatal("B22 validator did not reach its bounded source-read barrier")
					}
					interference := commit(t, f.call("bead.update", fmt.Sprintf("b22-source-interference-%d", i), map[string]any{"id": source, "fields": map[string]any{"description": fmt.Sprint(i)}}))
					oldRevision, oldErr := paused.observed.Uint64()
					newRevision, newErr := interference.Revision.Uint64()
					if paused.call != int32(i) || oldErr != nil || newErr != nil || newRevision <= oldRevision {
						t.Fatal("B22 public writer did not commit after the pinned source read")
					}
					b22Evidence(t, fmt.Sprintf("external-interference-%d", i), map[string]any{"observed": paused.observed, "public_commit": interference})
					lastSourceState = f.state(t)
					lastPhysical = b22Physical(t, f, fmt.Sprintf("source-committed-%d", i))
					close(paused.resume)
				}
				var result reply
				select {
				case result = <-finished:
				case <-time.After(10 * time.Second):
					t.Fatal("B22 target did not finish after external writer barriers")
				}
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
				b22CASRefusal(t, got, target, tc.reason, tc.current)
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
			b22CASRefusal(t, stale, target, "revision", "3")
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
						b22CASRefusal(t, result, target, "revision", "2")
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
							b22CASRefusal(t, got, target, "revision", "2")
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

func TestS1B22LateSQLPublicationFailureConservesEveryTableAndReopens(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			post := &b22Post{}
			f := newFixture(t, engine, pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-late-sql", Subscriber: post}))
			target, seed := f.create(t, "b22-sql-target", "original")
			other, _ := f.create(t, "b22-sql-other", "unrelated")
			const label = "b22-late-sql-recovery"
			params := batchParams(
				map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"title": "committed after recovery", "description": strings.Repeat("b22", 2048)}},
				map[string]any{"kind": "setMeta", "target": target, "key": "plugin.b22", "value": map[string]any{"atomic": true}},
				map[string]any{"kind": "comment", "target": target, "body": "one recovery comment"},
				map[string]any{"kind": "link", "from": target, "to": other, "linkKind": "relates-to"},
			)
			params["changeSet"].(map[string]any)["expect"] = []model.Precondition{model.PreconditionByRevision(target, seed.Revision)}
			request := f.request("tx.apply", label, params)
			encoded, err := json.Marshal(params["changeSet"])
			if err != nil {
				t.Fatal(err)
			}
			var cs model.ChangeSet
			if err := json.Unmarshal(encoded, &cs); err != nil {
				t.Fatal(err)
			}
			name, dsn := "sqlite", f.path
			recordKey, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &target})
			if err != nil {
				t.Fatal(err)
			}
			derivedKey, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "ready/" + target.ID}})
			if err != nil {
				t.Fatal(err)
			}
			install := ""
			if engine == "sqlite" {
				db, err := sql.Open(name, dsn)
				if err != nil {
					t.Fatal(err)
				}
				var original []byte
				err = db.QueryRowContext(f.ctx, `SELECT data FROM records WHERE key=?`, recordKey).Scan(&original)
				closeErr := db.Close()
				if err != nil || closeErr != nil || len(original) == 0 {
					t.Fatalf("B22 original SQLite row: %v/%v", err, closeErr)
				}
				// Bind the actual native key and preimage bytes, then require a
				// changed row joined to its new version and the exact derived key.
				install = fmt.Sprintf(`CREATE TRIGGER b22_reject_changeset BEFORE INSERT ON changesets BEGIN
 SELECT RAISE(ABORT,'B22_LATE_STATE_NOT_STAGED')
 WHERE NOT EXISTS(SELECT 1 FROM records r JOIN versions v ON v.key=r.key AND v.rev=NEW.rev
 WHERE r.key='%s' AND hex(r.data)<>'%X')
 OR NOT EXISTS(SELECT 1 FROM key_history WHERE rev=NEW.rev AND key='%s')
 OR COALESCE(json_array_length(CAST(NEW.data AS TEXT),'$.events'),0)<>1;
 SELECT RAISE(ABORT,'B22_LATE_SQL_REFUSAL'); END;`, strings.ReplaceAll(recordKey, "'", "''"), original, strings.ReplaceAll(derivedKey, "'", "''"))
			}
			remove := `DROP TRIGGER b22_reject_changeset`
			if engine == "postgres" {
				name, dsn = "pgx", f.dsn
				install = fmt.Sprintf(`CREATE SEQUENCE public.b22_late_sql_hit;
CREATE FUNCTION public.b22_reject_changeset() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM plug_postgres.records r JOIN plug_postgres.versions v
 ON r.key=v.key AND r.revision=v.revision WHERE r.revision=NEW.revision AND r.key=decode('%x','hex'))
 OR NOT EXISTS(SELECT 1 FROM plug_postgres.states WHERE revision=NEW.revision AND key=decode('%x','hex'))
 OR COALESCE(jsonb_array_length(convert_from(NEW.value,'UTF8')::jsonb->'events'),0)<>1
 THEN RAISE EXCEPTION 'B22_LATE_STATE_NOT_STAGED'; END IF;
 PERFORM nextval('public.b22_late_sql_hit');
 RAISE EXCEPTION 'B22_LATE_SQL_REFUSAL';
END$$;
CREATE TRIGGER b22_reject_changeset BEFORE INSERT ON plug_postgres.changesets
FOR EACH ROW EXECUTE FUNCTION public.b22_reject_changeset();`, []byte(recordKey), []byte(derivedKey))
				remove = `DROP TRIGGER b22_reject_changeset ON plug_postgres.changesets; DROP FUNCTION public.b22_reject_changeset(); DROP SEQUENCE public.b22_late_sql_hit;`
			}
			execute := func(statement string) {
				t.Helper()
				db, err := sql.Open(name, dsn)
				if err != nil {
					t.Fatal(err)
				}
				_, execErr := db.ExecContext(f.ctx, statement)
				closeErr := db.Close()
				if execErr != nil || closeErr != nil {
					t.Fatalf("B22 owned SQL setup/removal: %v / %v", execErr, closeErr)
				}
			}
			execute(install)
			b22Evidence(t, "fault-installation", map[string]any{"sql": install, "exit": "success"})
			before := f.state(t)
			physical := b22Physical(t, f, "fault-before")
			// Preserve the actual engine exception before the RPC catalog maps
			// it. The marker can only be raised after the SQL-side staging checks.
			outcome, fault := f.writer.ApplyWithOutcome(f.ctx, anonymous, cs, model.WriteMeta{Method: "tx.apply", Project: project, AgentLabel: actor, IdempotencyKey: key(label)})
			if fault == nil || !strings.Contains(fault.Error(), "B22_LATE_SQL_REFUSAL") || strings.Contains(fault.Error(), "B22_LATE_STATE_NOT_STAGED") || outcome.Replayed || !outcome.Commit.Revision.IsZero() || len(outcome.Commit.Results) != 0 {
				t.Fatalf("B22 actual late SQL fault not reached with empty outcome: %+v / %v", outcome, fault)
			}
			b22Evidence(t, "actual-sql-error", map[string]any{"error": fault.Error(), "revision": outcome.Commit.Revision, "result_count": len(outcome.Commit.Results), "replayed": outcome.Replayed})
			sameState(t, before, f.state(t))
			b22SamePhysical(t, physical, b22Physical(t, f, "fault-direct-after"), false)
			got := f.dispatch(request)
			if got.Error == nil || got.Error.Data.Code != "internal_error" || got.Meta.Replayed || len(got.Result) != 0 {
				t.Fatalf("B22 raw SQL fault changed its established RPC error plane: %+v", got)
			}
			sameState(t, before, f.state(t))
			b22SamePhysical(t, physical, b22Physical(t, f, "fault-rpc-after"), false)
			if post.count() != 2 || f.casReceipt(t, "tx.apply", label) != nil {
				t.Fatal("B22 SQL refusal ran post or stored a successful receipt")
			}
			if engine == "postgres" {
				db, err := sql.Open(name, dsn)
				if err != nil {
					t.Fatal(err)
				}
				var hits int64
				var called bool
				err = db.QueryRowContext(f.ctx, `SELECT last_value,is_called FROM public.b22_late_sql_hit`).Scan(&hits, &called)
				closeErr := db.Close()
				if err != nil || closeErr != nil || hits != 2 || !called {
					t.Fatalf("B22 actual PostgreSQL fault hits=%d called=%v error=%v/%v", hits, called, err, closeErr)
				}
				b22Evidence(t, "actual-sql-hit-count", map[string]any{"hits": hits, "is_called": called})
			}
			f.reopen(t)
			sameState(t, before, f.state(t))
			b22SamePhysical(t, physical, b22Physical(t, f, "fault-all-pools-reopened"), true)
			if f.casReceipt(t, "tx.apply", label) != nil {
				t.Fatal("B22 refused receipt became visible after real store close/open")
			}
			execute(remove)
			first := f.dispatch(request)
			c := commit(t, first)
			if first.Meta.Replayed || c.Revision != model.Natural64(3) || len(c.Results) != 4 || post.count() != 3 {
				t.Fatalf("B22 same-key recovery did not commit once with full results: %+v", first)
			}
			after := f.state(t)
			doc := closeDocument(t, after, target)
			if doc.Fields["title"] != "committed after recovery" || len(doc.Comments) != 1 || doc.Comments[0].Body != "one recovery comment" || len(doc.Links) != 1 || doc.Links[0].To.ID != other.ID || string(doc.Metadata["plugin.b22"]) != `{"atomic":true}` || len(after.Versions[target.ID]) != 2 || len(after.Versions[other.ID]) != 1 || len(after.Changesets) != 3 || len(after.Changesets[2].Changes) != 1 {
				t.Fatalf("B22 recovered transaction is not one complete relational mutation: %+v", doc)
			}
			events := after.Changesets[2].Events
			if len(events) != 1 || events[0].Resource.Ref.ID != target.ID || events[0].Type != model.EventBeadLinked || events[0].Provenance.Method != "tx.apply" || events[0].Provenance.Cause.Root != key(label) || events[0].ChangeIndex != model.Natural64(0) {
				t.Fatalf("B22 recovered staged event does not bind the actual relational transaction: %+v", events)
			}
			b22Evidence(t, "recovered-event", events)
			physical = b22Physical(t, f, "recovered")
			f.reopen(t)
			replay := f.dispatch(request)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) || post.count() != 3 {
				t.Fatalf("B22 recovered full stored Commit did not survive second reopen: %+v", replay)
			}
			sameState(t, after, f.state(t))
			b22SamePhysical(t, physical, b22Physical(t, f, "recovered-reopened"), true)
			t.Logf("B22 %s actual staged record/version/derived SQL refusal, full physical conservation, raw/RPC error planes, same-key relational recovery and two real reopens PASS", engine)
		})
	}
}

func TestS1B22RefusalIdentifiesGuardAddressOutsideMutation(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			var hidden atomic.Bool
			var guard model.Ref
			f := newFixture(t, engine, pipeline.WithAuthorizer(closeAuthorizer(func(_ context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
				if stage == port.AuthorizeBefore && hidden.Load() {
					for _, expected := range proposal.Expect {
						if expected.Ref.ID == guard.ID {
							return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
						}
					}
				}
				return nil
			})))
			target, _ := f.create(t, "b22-address-target", "mutation target")
			var current model.Commit
			guard, current = f.create(t, "b22-address-guard", "guard holder")
			for _, tc := range []struct {
				name, reason, revision string
				expect                 model.Precondition
			}{
				{"revision", "revision", current.Revision.String(), model.PreconditionByRevision(guard, model.Natural64(0))},
				{"status", "hash", "", model.PreconditionByHash(guard, "fields.status", casHash(`"done"`))},
				{"missing-metadata", "hash", "", model.PreconditionByHash(guard, "metadata.plugin.absent", casHash(`null`))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					before := f.state(t)
					physical := b22Physical(t, f, "before")
					for _, ordered := range []bool{false, true} {
						label := fmt.Sprintf("b22-address-%s-%v", tc.name, ordered)
						params := batchParams(map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"title": "must not commit"}})
						if ordered {
							params = batchParams(map[string]any{"kind": "close", "target": target, "reason": "must not append", "expect": []model.Precondition{tc.expect}})
						} else {
							params["changeSet"].(map[string]any)["expect"] = []model.Precondition{tc.expect}
						}
						hidden.Store(true)
						denied := f.call("tx.apply", label, params)
						if denied.Error == nil || denied.Error.Data.Code != "forbidden" || len(denied.Error.Data.Details) != 0 || len(denied.Result) != 0 || denied.Meta.Replayed {
							t.Fatalf("B22 denied guard disclosed its address/revision/refusal: %+v", denied)
						}
						sameState(t, before, f.state(t))
						hidden.Store(false)
						refused := f.call("tx.apply", label, params)
						b22CASRefusal(t, refused, guard, tc.reason, tc.revision)
						b22Evidence(t, fmt.Sprintf("ordered-%v-refusal", ordered), refused)
						sameState(t, before, f.state(t))
						if f.casReceipt(t, "tx.apply", label) != nil {
							t.Fatal("B22 refusing guard stored a receipt for a different mutation target")
						}
					}
					b22SamePhysical(t, physical, b22Physical(t, f, "after"), false)
				})
			}
		})
	}
}

// This fault boundary preserves the real Reader and typed source writer. It
// either refuses a staged transaction before publication, or loses only the
// acknowledgment after the native driver has committed its durable receipt.
type b22TransactionFaultStore struct {
	driver.Store
	driver.CheckpointStore
	mode    string
	commits atomic.Int32
	staged  atomic.Int32
	durable atomic.Int32
}

type b22TransactionFaultTx struct {
	driver.WriteTx
	store *b22TransactionFaultStore
}

func (s *b22TransactionFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &b22TransactionFaultTx{WriteTx: tx, store: s}, nil
}

func (s *b22TransactionFaultStore) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	return s.Store.(driver.ResponseFinalizer).FinalizeResponse(ctx, lease, principal, agent, method, key, revision, warnings)
}

func (tx *b22TransactionFaultTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	err := tx.WriteTx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, parent, links)
	if err == nil {
		tx.store.staged.Add(1)
	}
	return err
}

func (tx *b22TransactionFaultTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	call := tx.store.commits.Add(1)
	if tx.store.mode == "retry-exhausted" || tx.store.mode == "retry-once" && call == 1 {
		return model.Changeset{}, driver.ErrConflict
	}
	committed, err := tx.WriteTx.Commit(ctx, header)
	if err == nil {
		tx.store.durable.Add(1)
		if tx.store.mode == "lost-commit-ack" {
			return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
		}
	}
	return committed, err
}

func TestS1B22SerializationRetryExhaustionAndLostCommitAck(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, mode := range []string{"retry-once", "retry-exhausted", "lost-commit-ack"} {
			t.Run(engine+"/"+mode, func(t *testing.T) {
				f := newFixture(t, engine)
				target, _ := f.create(t, "b22-transaction-fault-target", "original")
				fault := &b22TransactionFaultStore{Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), mode: mode}
				post := &b22Post{}
				var validations atomic.Int32
				options := []pipeline.Option{pipeline.WithOntology(f.ontology), pipeline.WithClock(f.clock.Now), pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b22-retry-validate", Validator: b22Validator(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
					validations.Add(1)
					return port.Verdict{Allow: true}, nil
				})})}
				// A lost commit acknowledgment is resolved only by its already
				// final receipt. No post callback is invented after publication.
				if mode != "lost-commit-ack" {
					options = append(options, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-retry-post", Subscriber: post}))
				}
				writer := pipeline.New(fault, f.lease, options...)
				cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: target, Fields: map[string]any{"description": "single durable transaction"}}}}}
				meta := model.WriteMeta{Method: "tx.apply", Project: project, AgentLabel: actor, IdempotencyKey: key("b22-transaction-fault")}
				before := f.state(t)
				physical := b22Physical(t, f, "before")
				out, err := writer.ApplyWithOutcome(f.ctx, anonymous, cs, meta)
				observed := map[string]any{"revision": out.Commit.Revision, "result_count": len(out.Commit.Results), "replayed": out.Replayed, "error": fmt.Sprint(err), "attempts": fault.commits.Load(), "staged": fault.staged.Load(), "durable": fault.durable.Load(), "validations": validations.Load(), "posts": post.count()}
				if err == nil {
					observed["commit"] = out.Commit
				}
				b22Evidence(t, "outcome", observed)
				if mode == "retry-exhausted" {
					var typed *model.Error
					if !errors.As(err, &typed) || typed.Data.Code != "write_conflict" || !typed.Data.Retryable || !out.Commit.Revision.IsZero() || out.Replayed || fault.commits.Load() != 4 || fault.staged.Load() != 4 || validations.Load() != 4 || fault.durable.Load() != 0 || post.count() != 0 {
						t.Fatalf("B22 serialization exhaustion leaked a callback/commit or lost its typed refusal: %+v / %v", out, err)
					}
					sameState(t, before, f.state(t))
					b22SamePhysical(t, physical, b22Physical(t, f, "exhausted"), false)
					fault.mode = "recovery"
					out, err = writer.ApplyWithOutcome(f.ctx, anonymous, cs, meta)
				}
				wantAttempts, wantValidations, wantPosts := int32(2), int32(2), 1
				if mode == "retry-exhausted" {
					wantAttempts, wantValidations = 5, 5
				} else if mode == "lost-commit-ack" {
					wantAttempts, wantValidations, wantPosts = 1, 1, 0
				}
				if err != nil || out.Replayed != (mode == "lost-commit-ack") || out.Commit.Revision != model.Natural64(2) || len(out.Commit.Results) != 1 || fault.commits.Load() != wantAttempts || fault.staged.Load() != wantAttempts || validations.Load() != wantValidations || fault.durable.Load() != 1 || post.count() != wantPosts {
					t.Fatalf("B22 retry/indeterminate resolution did not produce one durable outcome: %+v / %v", out, err)
				}
				after := f.state(t)
				receipt := f.casReceipt(t, "tx.apply", "b22-transaction-fault")
				if len(after.Changesets) != 2 || len(after.Versions[target.ID]) != 2 || receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, out.Commit) {
					t.Fatal("B22 retry/indeterminate recovery published duplicate history/receipts")
				}
				physical = b22Physical(t, f, "final")
				f.reopen(t)
				replayed, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, cs, meta)
				if err != nil || !replayed.Replayed || !reflect.DeepEqual(out.Commit, replayed.Commit) || validations.Load() != wantValidations || post.count() != wantPosts {
					t.Fatalf("B22 durable full outcome changed after real SQL reopen: %+v / %v", replayed, err)
				}
				sameState(t, after, f.state(t))
				b22SamePhysical(t, physical, b22Physical(t, f, "reopened"), true)
			})
		}
	}
}

type b22ClaimReadFault struct {
	driver.Store
	driver.CheckpointStore
	target   model.Ref
	phase    string
	cause    error
	enabled  atomic.Bool
	scans    atomic.Int32
	hydrates atomic.Int32
	writes   atomic.Int32
	hydrated *driver.Record
	view     *driver.View
}

type b22ClaimFaultReader struct {
	driver.Reader
	fault *b22ClaimReadFault
}

func (s *b22ClaimReadFault) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &b22ClaimFaultReader{Reader: r, fault: s}, nil
}

func (s *b22ClaimReadFault) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	return s.Store.Write(ctx, lease)
}

func (s *b22ClaimReadFault) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	return s.Store.(driver.ResponseFinalizer).FinalizeResponse(ctx, lease, principal, agent, method, key, revision, warnings)
}

func (r *b22ClaimFaultReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, query)
	if err == nil && query.Filter["kind"] == "bead" {
		r.fault.scans.Add(1)
		if r.fault.enabled.Load() && r.fault.phase == "selection-enumeration" {
			return driver.Page[driver.Record]{}, r.fault.cause
		}
	}
	return page, err
}

func (r *b22ClaimFaultReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states, err := r.Reader.Observe(ctx, keys)
	if err == nil {
		for _, state := range states {
			// snapshotReader.Get hydrates directly from Observe's complete
			// live Record on all three drivers; Reader.Get is only a fallback.
			if state.Key.Kind == driver.LockRecord && state.Key.Ref != nil && reflect.DeepEqual(*state.Key.Ref, r.fault.target) && state.Present && state.Record != nil {
				r.fault.hydrates.Add(1)
				if r.fault.enabled.Load() && r.fault.phase == "candidate-hydration" {
					row := *state.Record
					row.Body = bytes.Clone(row.Body)
					view := r.Snapshot()
					r.fault.hydrated, r.fault.view = &row, &view
					return nil, r.fault.cause
				}
			}
		}
	}
	return states, err
}

func TestS1B22ClaimSelectionAndHydrationFailurePublishNothing(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, phase := range []string{"selection-enumeration", "candidate-hydration"} {
			t.Run(engine+"/"+phase, func(t *testing.T) {
				f := newFixture(t, engine)
				target, _ := f.create(t, "b22-claim-read-target", "must remain unclaimed")
				fault := &b22ClaimReadFault{Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), target: target, phase: phase, cause: errors.New("B22_REAL_CLAIM_READ_REFUSAL")}
				fault.enabled.Store(true)
				post := &b22Post{}
				writer := pipeline.New(fault, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.clock.Now), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b22-read-post", Subscriber: post}))
				svc := service.New(fault, f.lease, writer, service.WithOntology(f.ontology), service.WithClock(f.clock.Now))
				req := service.ClaimNextRequest{Project: project}
				meta := model.WriteMeta{Method: "bead.claimNext", Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: key("b22-claim-read")}
				before := f.state(t)
				physical := b22Physical(t, f, "before")
				out, err := svc.ClaimNextWithOutcome(f.ctx, anonymous, req, meta)
				if out != nil || !errors.Is(err, fault.cause) || fault.scans.Load() == 0 || phase == "candidate-hydration" && fault.hydrates.Load() == 0 || fault.writes.Load() != 0 || post.count() != 0 {
					t.Fatalf("B22 real claim read failure did not stop before publication: %+v / %v scans=%d hydrates=%d writes=%d posts=%d", out, err, fault.scans.Load(), fault.hydrates.Load(), fault.writes.Load(), post.count())
				}
				if phase == "candidate-hydration" {
					var selected *driver.Record
					for i := range before.Records {
						if reflect.DeepEqual(before.Records[i].Ref, target) {
							selected = &before.Records[i]
						}
					}
					if selected == nil || !reflect.DeepEqual(fault.hydrated, selected) || fault.view == nil || fault.view.At == nil || *fault.view.At != before.Meta.Head || fault.view.Incarnation != before.Meta.Incarnation {
						t.Fatal("B22 hydration fault did not bind the actual selected row and pinned snapshot")
					}
				}
				// Plug enumerates ready membership instead of issuing the old
				// backend Count, then hydrates before admitting a writer. Both
				// actual failures must leave the entire claim state untouched.
				b22Evidence(t, "read-refusal", map[string]any{"error": err.Error(), "phase": phase, "scans": fault.scans.Load(), "hydrates": fault.hydrates.Load(), "writes": fault.writes.Load(), "posts": post.count(), "hydrated": fault.hydrated, "view": fault.view})
				sameState(t, before, f.state(t))
				b22SamePhysical(t, physical, b22Physical(t, f, "refused"), false)
				if f.casReceipt(t, "bead.claimNext", "b22-claim-read") != nil {
					t.Fatal("B22 failed claim read stored a receipt")
				}
				fault.enabled.Store(false)
				out, err = svc.ClaimNextWithOutcome(f.ctx, anonymous, req, meta)
				if err != nil || out == nil || out.NullResult || out.Replayed || out.Commit.Revision != model.Natural64(2) || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != target.ID || post.count() != 1 || fault.writes.Load() != 1 {
					t.Fatalf("B22 failed claim key was not reusable after read recovery: %+v / %v", out, err)
				}
				after := f.state(t)
				receipt := f.casReceipt(t, "bead.claimNext", "b22-claim-read")
				if len(after.Versions[target.ID]) != 2 || len(after.Changesets) != 2 || receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, out.Commit) || closeDocument(t, after, target).Lease == nil {
					t.Fatal("B22 read recovery did not publish one complete claim")
				}
				physical = b22Physical(t, f, "recovered")
				f.reopen(t)
				replay, err := f.service.ClaimNextWithOutcome(f.ctx, anonymous, req, meta)
				if err != nil || replay == nil || !replay.Replayed || !reflect.DeepEqual(out.Commit, replay.Commit) || post.count() != 1 {
					t.Fatalf("B22 full claim receipt changed after SQL reopen: %+v / %v", replay, err)
				}
				sameState(t, after, f.state(t))
				b22SamePhysical(t, physical, b22Physical(t, f, "reopened"), true)
			})
		}
	}
}
