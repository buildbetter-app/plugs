package s1relations_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b28Notices struct {
	mu        sync.Mutex
	items     []port.PostFailureNotice
	isolation string
	release   <-chan struct{}
}

func (n *b28Notices) NotifyPostFailure(ctx context.Context, notice port.PostFailureNotice) {
	n.mu.Lock()
	n.items = append(n.items, notice)
	n.mu.Unlock()
	switch n.isolation {
	case "panic":
		panic("controlled observer failure")
	case "deadline":
		<-ctx.Done()
	case "blocked":
		<-n.release
	}
}

func (n *b28Notices) snapshot() []port.PostFailureNotice {
	n.mu.Lock()
	defer n.mu.Unlock()
	result := append([]port.PostFailureNotice(nil), n.items...)
	sort.Slice(result, func(i, j int) bool { return result[i].Binding < result[j].Binding })
	return result
}

func TestB28PublicPostFailureAlertsArePerBindingSanitizedAndReplayFree(t *testing.T) {
	for _, isolation := range []string{"ordinary", "panic", "deadline", "blocked"} {
		t.Run(isolation, func(t *testing.T) {
			stores(t, func(t *testing.T, kind string) {
				release := make(chan struct{})
				defer close(release)
				notices := &b28Notices{isolation: isolation, release: release}
				var fail, refuse atomic.Bool
				fail.Store(true)
				var failedCalls, healthyCalls, readbacks atomic.Int32
				failing := b28AlertPost(func(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
					failedCalls.Add(1)
					if reader.Snapshot().At == nil || *reader.Snapshot().At != change.Revision || len(change.Changes) == 0 {
						return errors.New("callback lacked committed snapshot")
					}
					refs := make([]model.Ref, len(change.Changes))
					for i, item := range change.Changes {
						refs[i] = item.Resource.Ref
					}
					rows, err := reader.Get(ctx, refs)
					if err != nil || len(rows) != len(refs) {
						return errors.New("callback could not read all committed changes")
					}
					for _, row := range rows {
						doc, err := pipeline.DecodeDocument(row.Body)
						if err != nil || doc.Fields["title"] == nil {
							return errors.New("callback did not read the committed title")
						}
					}
					readbacks.Add(1)
					if fail.Load() {
						return errors.New("controlled destination detail must stay private")
					}
					return nil
				})
				f := newFixture(t, kind, []string{"b28-notice", "b28-notice-peer"},
					pipeline.WithPostFailureObserver(notices),
					pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b28/closed", Validator: b28AlertValidator(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
						if refuse.Load() {
							return port.Verdict{}, errors.New("controlled pre failure")
						}
						return port.Verdict{Allow: true}, nil
					})}),
					pipeline.WithPostSubscribers(
						pipeline.PostBinding{ID: "b28/failing-a", Subscriber: failing},
						pipeline.PostBinding{ID: "b28/failing-b", Subscriber: failing},
						pipeline.PostBinding{ID: "b28/healthy", Subscriber: b28AlertPost(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
							healthyCalls.Add(1)
							return nil
						})},
					),
				)
				params := map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpCreate, Create: &model.CreateOp{Key: "first", Type: "issue", Fields: map[string]any{"title": "exact committed notice value"}}},
					{Kind: model.OpCreate, Create: &model.CreateOp{Key: "second", Type: "issue", Fields: map[string]any{"title": "second committed notice value"}}},
				}}}
				started := time.Now()
				first := f.call("tx.apply", "b28-notice-create", params)
				if elapsed := time.Since(started); elapsed > 3*time.Second {
					t.Fatalf("observer did not remain bounded: %s", elapsed)
				}
				if first.Error != nil || first.Meta.Replayed {
					t.Fatalf("post failure became RPC refusal: %+v", first)
				}
				var commit model.Commit
				if err := json.Unmarshal(first.Result, &commit); err != nil {
					t.Fatal(err)
				}
				if commit.Revision.String() != "1" || len(commit.Results) != 2 || len(commit.Warnings) != 2 || failedCalls.Load() != 2 || healthyCalls.Load() != 1 || readbacks.Load() != 2 || f.document("b28-notice").Fields["title"] != "exact committed notice value" || f.document("b28-notice-peer").Fields["title"] != "second committed notice value" {
					t.Fatalf("committed alert path=%+v", commit)
				}
				meta, err := f.store.Meta(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				observed := notices.snapshot()
				if len(observed) != 2 {
					t.Fatalf("per-binding notice count=%d want2", len(observed))
				}
				for i, binding := range []string{"b28/failing-a", "b28/failing-b"} {
					warning, notice := commit.Warnings[i], observed[i]
					if warning.Code != "post_subscriber_failed" || warning.Details["binding"] != binding || fmt.Sprint(warning.Details["revision"]) != "1" || notice.Binding != binding || notice.Incarnation != meta.Incarnation || notice.Revision != commit.Revision || notice.Reason != "post_subscriber_failed" || notice.FailedAt != fixedTime || !notice.DeadLettered {
						t.Fatalf("warning=%+v notice=%+v", warning, notice)
					}
					stored, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, pipeline.PostDeadLetterKey(meta.Incarnation, commit.Revision, binding))
					var dead pipeline.PostDeadLetter
					if err != nil || stored.Generation != 1 || json.Unmarshal(stored.Payload, &dead) != nil || dead.Binding != binding || dead.Revision != commit.Revision || dead.Incarnation != meta.Incarnation || dead.FailedAt != fixedTime || dead.Reason != "post_subscriber_failed" {
						t.Fatalf("notice lacks actual durable deadletter: %+v %+v %v", stored, dead, err)
					}
				}
				healthyDead, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, pipeline.PostDeadLetterKey(meta.Incarnation, commit.Revision, "b28/healthy"))
				if err != nil || healthyDead.Generation != 0 || len(healthyDead.Payload) != 0 {
					t.Fatalf("healthy binding has failure metadata: %+v %v", healthyDead, err)
				}
				if bytes.Contains(first.Result, []byte("controlled")) || bytes.Contains(jsonBytes(t, observed), []byte("controlled")) {
					t.Fatal("callback or observer detail leaked")
				}
				receipt, image := f.receipt("tx.apply", "b28-notice-create"), f.image()
				if receipt == nil || receipt.ResponsePending {
					t.Fatal("committed warning result not durable")
				}
				equalJSON(t, commit, receipt.Commit)
				f.reopen()
				for _, binding := range []string{"b28/failing-a", "b28/failing-b"} {
					stored, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, pipeline.PostDeadLetterKey(meta.Incarnation, commit.Revision, binding))
					var dead pipeline.PostDeadLetter
					if err != nil || stored.Generation != 1 || json.Unmarshal(stored.Payload, &dead) != nil || dead.Binding != binding || dead.Revision != commit.Revision || dead.Incarnation != meta.Incarnation || dead.FailedAt != fixedTime || dead.Reason != "post_subscriber_failed" {
						t.Fatalf("reopen changed deadletter: %+v %+v %v", stored, dead, err)
					}
				}
				replay := f.call("tx.apply", "b28-notice-create", params)
				if replay.Error != nil || !replay.Meta.Replayed || failedCalls.Load() != 2 || healthyCalls.Load() != 1 || len(notices.snapshot()) != 2 || !bytes.Equal(image, f.image()) || f.document("b28-notice").Fields["title"] != "exact committed notice value" || f.document("b28-notice-peer").Fields["title"] != "second committed notice value" {
					t.Fatalf("reopen/replay redelivered: %+v", replay)
				}
				equalJSON(t, first.Result, replay.Result)
				equalReceipt(t, receipt, f.receipt("tx.apply", "b28-notice-create"))
				fail.Store(false)
				update := map[string]any{"id": ref("b28-notice"), "fields": map[string]any{"title": "healthy current value"}}
				healthy := f.write("bead.update", "b28-notice-healthy", update)
				if healthy.Revision.String() != "2" || len(healthy.Warnings) != 0 || failedCalls.Load() != 4 || healthyCalls.Load() != 2 || readbacks.Load() != 4 || len(notices.snapshot()) != 2 {
					t.Fatalf("healthy callback emitted failure alert: %+v", healthy)
				}
				image = f.image()
				noop := f.write("bead.update", "b28-notice-noop", update)
				refuse.Store(true)
				refused := f.call("bead.update", "b28-notice-refusal", map[string]any{"id": ref("b28-notice"), "fields": map[string]any{"title": "refused"}})
				if noop.Revision.String() != "2" || len(noop.Warnings) != 0 || refused.Error == nil || refused.Error.Data.Code != "subscriber_unavailable" || len(refused.Result) != 0 || f.receipt("bead.update", "b28-notice-refusal") != nil || failedCalls.Load() != 4 || healthyCalls.Load() != 2 || len(notices.snapshot()) != 2 || !bytes.Equal(image, f.image()) {
					t.Fatal("no-op/pre refusal emitted a post alert or committed effects")
				}
				f.reopen()
				if !bytes.Equal(image, f.image()) || f.document("b28-notice").Fields["title"] != "healthy current value" {
					t.Fatal("SQL reopen changed healthy current/refusal state")
				}
			})
		})
	}
}

func TestB28PublicFailOpenDoesNotBypassExplicitValidatorRejection(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		notices := &b28Notices{}
		var posts atomic.Int32
		f := newFixture(t, kind, []string{"b28-rejected"}, pipeline.WithPostFailureObserver(notices),
			pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b28/reject", FailOpen: true, Validator: b28AlertValidator(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
				return port.Verdict{Allow: false, Violations: []model.Violation{{Guard: "b28/ordinary", Message: "independent rejection"}}}, nil
			})}),
			pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b28/post", Subscriber: b28AlertPost(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}),
		)
		before := f.image()
		got := f.call("bead.create", "b28-explicit-rejection", map[string]any{"type": "issue", "fields": map[string]any{"title": "must reject"}})
		if got.Error == nil || got.Error.Data.Code != "ontology_guard_failed" || len(got.Result) != 0 || posts.Load() != 0 || len(notices.snapshot()) != 0 || f.receipt("bead.create", "b28-explicit-rejection") != nil || !bytes.Equal(before, f.image()) {
			t.Fatalf("fail-open bypassed rejecting Verdict: %+v", got)
		}
		f.reopen()
		if !bytes.Equal(before, f.image()) {
			t.Fatal("rejected Verdict persisted state")
		}
	})
}

// These adapters belong only to the dedicated alert target; the shared public
// fixture and the full-card acceptance bodies remain unchanged.
type b28AlertPost func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (fn b28AlertPost) After(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
	return fn(ctx, change, reader)
}

type b28AlertValidator func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (fn b28AlertValidator) Validate(ctx context.Context, change port.ProposedChange, reader port.SnapshotReader) (port.Verdict, error) {
	return fn(ctx, change, reader)
}

// Refuse only after the real driver staged the ordinary update. This fixture
// changes no claim, authorization, concurrency or compare-and-set behavior.
type b28AlertCommitRefusal struct {
	driver.Store
	driver.CheckpointStore
	driver.ResponseFinalizer
	staged atomic.Bool
}

type b28AlertCommitRefusalTx struct {
	driver.WriteTx
	driver.GraphSourceWriter
	driver.CheckpointGuardTx
	owner *b28AlertCommitRefusal
}

func (s *b28AlertCommitRefusal) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	// Retain the real ordinary graph/checkpoint capabilities. Only Commit is
	// replaced, after the driver's actual staging has completed.
	return &b28AlertCommitRefusalTx{WriteTx: tx, GraphSourceWriter: tx.(driver.GraphSourceWriter), CheckpointGuardTx: tx.(driver.CheckpointGuardTx), owner: s}, nil
}

func (tx *b28AlertCommitRefusalTx) Commit(ctx context.Context, _ driver.ChangesetHeader) (model.Changeset, error) {
	rows, err := tx.WriteTx.Get(ctx, []model.Ref{ref("b28-alert-rollback")})
	if err != nil || len(rows) != 1 {
		return model.Changeset{}, errors.New("staged rollback control row unavailable")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || doc.Fields["title"] != "staged value must roll back" {
		return model.Changeset{}, errors.New("driver did not stage rollback control title")
	}
	tx.owner.staged.Store(true)
	return model.Changeset{}, &model.Error{Message: "controlled ordinary commit refusal", Data: model.ErrorData{Code: "internal_error"}}
}

func TestB28AlertSuppressedAfterRealStagingRollback(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		notices := &b28Notices{}
		var posts atomic.Int32
		var failing atomic.Bool
		f := newFixture(t, kind, []string{"b28-alert-rollback"}, pipeline.WithPostFailureObserver(notices),
			pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b28/rollback", Subscriber: b28AlertPost(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
				posts.Add(1)
				if failing.Load() {
					return errors.New("rollback must never deliver this failure")
				}
				return nil
			})}))
		seed := f.write("bead.create", "b28-alert-rollback-seed", map[string]any{"type": "issue", "fields": map[string]any{"title": "original committed value"}})
		if seed.Revision.String() != "1" || posts.Load() != 1 || len(notices.snapshot()) != 0 {
			t.Fatal("ordinary rollback seed failed")
		}
		before := f.image()
		failing.Store(true)
		fault := &b28AlertCommitRefusal{Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), ResponseFinalizer: f.store.(driver.ResponseFinalizer)}
		f.store = fault
		f.bind()
		got := f.call("bead.update", "b28-alert-rollback-refused", map[string]any{"id": ref("b28-alert-rollback"), "fields": map[string]any{"title": "staged value must roll back"}})
		if got.Error == nil || got.Error.Data.Code != "internal_error" || len(got.Result) != 0 || !fault.staged.Load() || posts.Load() != 1 || len(notices.snapshot()) != 0 || f.receipt("bead.update", "b28-alert-rollback-refused") != nil || !bytes.Equal(before, f.image()) {
			t.Fatalf("staged rollback emitted a notice or changed state: %+v staged=%v posts=%d", got, fault.staged.Load(), posts.Load())
		}
		f.reopen()
		if !bytes.Equal(before, f.image()) || f.document("b28-alert-rollback").Fields["title"] != "original committed value" || posts.Load() != 1 || len(notices.snapshot()) != 0 || f.receipt("bead.update", "b28-alert-rollback-refused") != nil {
			t.Fatal("reopen changed rollback state, receipt or notices")
		}
		meta, err := f.store.Meta(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, revision := range []model.Revision{model.Natural64(1), model.Natural64(2)} {
			dead, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, pipeline.PostDeadLetterKey(meta.Incarnation, revision, "b28/rollback"))
			if err != nil || dead.Generation != 0 || len(dead.Payload) != 0 {
				t.Fatalf("rollback created failure metadata: %+v %v", dead, err)
			}
		}
		t.Log("PUBLIC_ALERT_ROLLBACK_REACHED: real staged title refused; graph/head/version/event/receipt conserved; no callback, deadletter or notice")
	})
}

func TestB28AlertUnconfiguredPortPreservesCommittedWarning(t *testing.T) {
	for _, explicitNil := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicitNil=%v", explicitNil), func(t *testing.T) {
			stores(t, func(t *testing.T, kind string) {
				var posts atomic.Int32
				options := []pipeline.Option{pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b28/unconfigured", Subscriber: b28AlertPost(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
					posts.Add(1)
					return errors.New("private unconfigured destination failure")
				})})}
				if explicitNil {
					options = append(options, pipeline.WithPostFailureObserver(nil))
				}
				f := newFixture(t, kind, []string{"b28-alert-unconfigured"}, options...)
				params := map[string]any{"type": "issue", "fields": map[string]any{"title": "existing behavior remains"}}
				first := f.call("bead.create", "b28-alert-unconfigured-create", params)
				var commit model.Commit
				if first.Error != nil || json.Unmarshal(first.Result, &commit) != nil || commit.Revision.String() != "1" || len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" || posts.Load() != 1 || f.document("b28-alert-unconfigured").Fields["title"] != "existing behavior remains" {
					t.Fatalf("unconfigured observer changed committed warning: %+v %+v", first, commit)
				}
				meta, err := f.store.Meta(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				key := pipeline.PostDeadLetterKey(meta.Incarnation, commit.Revision, "b28/unconfigured")
				dead, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, key)
				var failure pipeline.PostDeadLetter
				if err != nil || dead.Generation != 1 || json.Unmarshal(dead.Payload, &failure) != nil || failure.Binding != "b28/unconfigured" || failure.Revision != commit.Revision || failure.Incarnation != meta.Incarnation || failure.Reason != "post_subscriber_failed" {
					t.Fatalf("unconfigured observer changed deadletter: %+v %+v %v", dead, failure, err)
				}
				image, receipt := f.image(), f.receipt("bead.create", "b28-alert-unconfigured-create")
				if receipt == nil || receipt.ResponsePending {
					t.Fatal("unconfigured post warning has no finalized receipt")
				}
				equalJSON(t, commit, receipt.Commit)
				f.reopen()
				replay := f.call("bead.create", "b28-alert-unconfigured-create", params)
				if replay.Error != nil || !replay.Meta.Replayed || posts.Load() != 1 || !bytes.Equal(image, f.image()) {
					t.Fatalf("unconfigured replay redelivered: %+v", replay)
				}
				equalJSON(t, first.Result, replay.Result)
				equalReceipt(t, receipt, f.receipt("bead.create", "b28-alert-unconfigured-create"))
				reloaded, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, key)
				if err != nil || reloaded.Generation != dead.Generation || !bytes.Equal(reloaded.Payload, dead.Payload) {
					t.Fatalf("unconfigured reopen changed failure metadata: %+v %v", reloaded, err)
				}
			})
		})
	}
}
