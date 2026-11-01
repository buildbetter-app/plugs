package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// These I1 cases use only direct domain calls and real local stores: no
// listener, transport, plugin host, external service or outbound connection.
// The shared recovery fixture supplies both memory and a disposable SQLite DB.
type i1State struct {
	View     driver.View
	Records  []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
	Receipt  *driver.IdempotencyRecord
}

func captureI1State(ctx context.Context, store driver.Store, key model.WriteMeta) (i1State, error) {
	state := i1State{Versions: map[string][]driver.Version{}}
	m, err := store.Meta(ctx)
	if err != nil {
		return state, err
	}
	r, err := store.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		return state, err
	}
	defer r.Close()
	state.View = r.Snapshot()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		return state, fmt.Errorf("incomplete record census: %v", err)
	}
	state.Records = page.Items
	for _, record := range state.Records {
		versions, err := r.Versions(ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			return state, fmt.Errorf("incomplete versions for %s: %v", record.Ref.ID, err)
		}
		state.Versions[record.Ref.ID] = versions.Items
	}
	state.Log, err = r.Changesets(ctx, nil, 1000)
	if err != nil {
		return state, err
	}
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		return state, err
	}
	state.Receipt, err = r.IdempotencyByKey(ctx, principal, key.AgentLabel, key.Method, key.IdempotencyKey)
	return state, err
}

func i1StoredState(t *testing.T, f *recoveryFixture, key model.WriteMeta) i1State {
	t.Helper()
	state, err := captureI1State(context.Background(), f.store, key)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func sameI1State(t *testing.T, before, after i1State) {
	t.Helper()
	if !bytes.Equal(mustJSON(t, before), mustJSON(t, after)) {
		t.Fatal("records, versions, changesets, snapshot or request receipt changed")
	}
}

func reopenI1SQLite(t *testing.T, f *recoveryFixture) {
	t.Helper()
	if f.dsn == "" {
		return
	}
	ctx := context.Background()
	if err := f.lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: f.dsn})
	if err != nil {
		t.Fatal(err)
	}
	f.start(t)
}

func TestI1ThousandOperationBoundaryAcrossLocalStores(t *testing.T) {
	recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
		ctx := context.Background()
		o := f.svc.ontology
		writer, metrics := pipeline.NewWithTelemetry(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
		f.svc = New(f.store, f.lease, writer, WithOntology(o), WithClock(func() time.Time { return f.now }))
		key := meta(t)
		key.Method = "tx.apply"
		cs := model.ChangeSet{Ops: make([]model.Op, 1000)}
		for i := range cs.Ops {
			cs.Ops[i] = model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
				Key: fmt.Sprintf("row-%d", i), Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("row %d", i)},
			}}
		}
		extra := model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: "row-1000", Type: "issue", Fields: map[string]any{"title": "row 1000"}}}
		oversized := model.ChangeSet{Ops: append(append([]model.Op{}, cs.Ops...), extra)}
		before := i1StoredState(t, f, key)
		_, err := f.svc.ApplyWithOutcome(ctx, anonymous, oversized, key)
		wantCode(t, err, "invalid_params")
		sameI1State(t, before, i1StoredState(t, f, key))

		// A refusal must leave the same key usable for the supported boundary.
		out, err := f.svc.ApplyWithOutcome(ctx, anonymous, cs, key)
		phaseMillis := map[pipeline.Phase]float64{}
		for phase, results := range metrics.Snapshot().LaneMillis {
			for _, millis := range results {
				phaseMillis[phase] += millis
			}
		}
		t.Logf("1000-op pipeline phase times (ms): %v", phaseMillis)
		if err != nil {
			t.Fatalf("1000-op batch: %v", err)
		}
		if out.Replayed || out.Commit.Revision.String() != "1" || len(out.Commit.Results) != 1000 {
			t.Fatalf("boundary commit revision=%s results=%d replay=%v", out.Commit.Revision, len(out.Commit.Results), out.Replayed)
		}
		ids := map[string]bool{}
		for i, result := range out.Commit.Results {
			var doc pipeline.Document
			if err := json.Unmarshal(result.Record, &doc); err != nil {
				t.Fatal(err)
			}
			if !result.Ref.IsID() || ids[result.Ref.ID] || result.Revision.String() != "1" || doc.ID != result.Ref.ID || doc.Fields["title"] != fmt.Sprintf("row %d", i) {
				t.Fatalf("batch ID, order or final image failed at result %d", i)
			}
			ids[result.Ref.ID] = true
		}
		committed := i1StoredState(t, f, key)
		if len(committed.Records) != 1000 || len(committed.Log) != 1 || len(committed.Log[0].Changes) != 1000 || committed.Receipt == nil || committed.Receipt.ResponsePending {
			t.Fatal("boundary batch did not publish exactly 1000 rows in one finalized changeset")
		}
		for id, versions := range committed.Versions {
			if !ids[id] || len(versions) != 1 || versions[0].Revision.String() != "1" || !versions[0].Base || versions[0].Deleted {
				t.Fatalf("boundary history mismatch for %s: %d versions", id, len(versions))
			}
		}
		reopenI1SQLite(t, f)
		sameI1State(t, committed, i1StoredState(t, f, key))
		replayed, err := f.svc.ApplyWithOutcome(ctx, anonymous, cs, key)
		if err != nil || !replayed.Replayed || !bytes.Equal(mustJSON(t, out.Commit), mustJSON(t, replayed.Commit)) {
			t.Fatalf("1000-op replay changed after reopen: %v", err)
		}
		sameI1State(t, committed, i1StoredState(t, f, key))
		t.Log("I1 boundary PASS: 1001 refused atomically; 1000 real IDs/rows/base versions, one changeset; exact replay survives SQLite reopen")
	})
}

func TestI1LifecycleRechecksConcurrentBlockerAcrossLocalStores(t *testing.T) {
	for _, method := range []string{"bead.claim", "bead.close"} {
		t.Run(method, func(t *testing.T) {
			recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
				ctx := context.Background()
				create := func(title string) model.Ref {
					t.Helper()
					commit, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": title}}, meta(t))
					if err != nil || len(commit.Results) != 1 {
						t.Fatalf("create %s: %v", title, err)
					}
					return commit.Results[0].Ref
				}
				blocker, dependent := create("blocker"), create("dependent")
				if _, err := f.svc.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: dependent, To: blocker, LinkKind: "blocks"}}}}, meta(t)); err != nil {
					t.Fatal(err)
				}
				closeBlocker := func() {
					t.Helper()
					if _, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: blocker}, meta(t)); err != nil {
						t.Fatal(err)
					}
				}
				closeBlocker()
				ready, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
				if err != nil || len(ready.Items) != 1 || ready.Items[0].ID != dependent.ID {
					t.Fatalf("unblocked ready=%+v err=%v", ready, err)
				}
				key, reopenKey := meta(t), meta(t)
				key.Method = method
				var validations atomic.Int32
				var interfered i1State
				post := &failedPost{}
				writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.svc.ontology), pipeline.WithClock(func() time.Time { return f.now }),
					pipeline.WithValidators(pipeline.ValidatorBinding{ID: "interleave-host-write", Validator: recoveryValidator(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
						if validations.Add(1) == 1 {
							// Core guards already allowed the old snapshot. The independent
							// host writer reopens its blocker before this proposal commits.
							if _, err := f.svc.Reopen(ctx, anonymous, ReopenRequest{ID: blocker}, reopenKey); err != nil {
								return port.Verdict{}, err
							}
							var err error
							interfered, err = captureI1State(ctx, f.store, key)
							if err != nil {
								return port.Verdict{}, err
							}
						}
						return port.Verdict{Allow: true}, nil
					})}), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "unavailable-sink", Subscriber: post}))
				guarded := New(f.store, f.lease, writer, WithOntology(f.svc.ontology), WithClock(func() time.Time { return f.now }))
				call := func(svc *Service) (port.WriteOutcome, error) {
					if method == "bead.claim" {
						return svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: dependent}, key)
					}
					return svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: dependent, Reason: "completed"}, key)
				}
				_, err = call(guarded)
				wantCode(t, err, "ontology_guard_failed")
				if validations.Load() != 1 || post.calls != 0 || interfered.View.At == nil || interfered.Receipt != nil {
					t.Fatal("stale guard reached callbacks again, ran post, or acquired a receipt")
				}
				sameI1State(t, interfered, i1StoredState(t, f, key))
				ready, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
				if err != nil || len(ready.Items) != 1 || ready.Items[0].ID != blocker.ID {
					t.Fatalf("reopened blocker readiness=%+v err=%v", ready, err)
				}

				// Once the real blocker is satisfied, that refused key can commit.
				closeBlocker()
				out, err := call(guarded)
				if err != nil || out.Replayed || len(out.Commit.Warnings) != 1 || out.Commit.Warnings[0].Code != "post_subscriber_failed" || post.calls != 1 || validations.Load() != 2 {
					t.Fatalf("corrected guarded write=%+v err=%v", out, err)
				}
				if method == "bead.claim" && (out.Commit.LeaseExpiresAt == nil || !out.Commit.LeaseExpiresAt.Equal(f.now.Add(5*time.Minute)) || out.Commit.LeaseTtlMs == nil || out.Commit.LeaseTtlMs.String() != "300000") {
					t.Fatal("corrected claim lost its bounded lease")
				}
				committed := i1StoredState(t, f, key)
				if committed.Receipt == nil || committed.Receipt.ResponsePending || len(committed.Receipt.Commit.Warnings) != 1 {
					t.Fatal("committed warning was not finalized durably")
				}
				reopenI1SQLite(t, f)
				sameI1State(t, committed, i1StoredState(t, f, key))
				replayService := guarded
				if f.dsn != "" {
					replayService = f.svc
				}
				replay, err := call(replayService)
				if err != nil || !replay.Replayed || !bytes.Equal(mustJSON(t, out.Commit), mustJSON(t, replay.Commit)) || post.calls != 1 || validations.Load() != 2 {
					t.Fatalf("warning replay reran callbacks or changed bytes: %v", err)
				}
				sameI1State(t, committed, i1StoredState(t, f, key))
				t.Log("I1 guard PASS: concurrent blocker reopens, stale claim/close refuses with no state/receipt/post; same key commits after unblock and replays its warning after SQLite reopen")
			})
		})
	}
}
