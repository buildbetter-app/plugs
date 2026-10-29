package postphase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type acceptingOntology struct{}

func (acceptingOntology) Normalize(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, nil
}

func (acceptingOntology) Validate(_ context.Context, proposal port.ProposedChange, _ port.SnapshotReader) error {
	for _, change := range proposal.Changes {
		if change.Op.Kind == model.OpDelete {
			continue
		}
		document, err := pipeline.DecodeDocument(change.After)
		if err != nil {
			return err
		}
		if title, ok := document.Fields["title"].(string); !ok || title == "" {
			return errors.New("postphase fixture requires a title")
		}
	}
	return nil
}

type postCallback func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error

// nativePost is a real compiled-in SDK plugin. Keeping the manifest here makes
// the acceptance package exercise the same manifest, host, adapter and writer
// boundaries that a server composition root uses.
func nativePost(alias string, callback postCallback) sdk.Native {
	manifest := pluginapi.PluginManifest{
		ID:        "app.example." + alias,
		Alias:     alias,
		Version:   "1.0.0",
		PluginAPI: pluginapi.APIVersion,
		Runtime:   "native",
		Provides: []pluginapi.Provision{{
			Kind:          "subscriber",
			Name:          "observe",
			Phases:        []pluginapi.Phase{pluginapi.PhasePost},
			Reads:         []string{"fields.title", "fields.normalized"},
			MaxPointReads: 4,
			MaxQueries:    1,
		}},
		Requires: pluginapi.Requirements{Methods: []string{"tx.apply"}},
	}
	return sdk.Native{
		Instance: "default",
		Manifest: manifest,
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{Posts: map[string]pluginapi.PostSubscriber{
				"observe": sdk.AfterFunc(func(ctx context.Context, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
					return callback(ctx, host, change, reader)
				}),
			}}, nil
		},
	}
}

type registry struct{}

func (registry) Effect(method string) (pluginhost.Effect, bool) {
	if method == "tx.apply" {
		return pluginhost.Write, true
	}
	return "", false
}

func (registry) Call(context.Context, pluginapi.PrincipalRef, string, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

type fixture struct {
	ctx       context.Context
	store     *memory.Store
	lease     driver.ActiveLease
	principal model.PrincipalRef
	ids       atomic.Uint64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := memory.NewWithOptions(memory.Options{
		StoreID:     "postphase-store",
		Incarnation: "postphase-incarnation",
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "postphase-owner", time.Hour)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	t.Cleanup(func() { _ = store.Close() })
	return &fixture{ctx: ctx, store: store, lease: lease, principal: model.PrincipalRef{Kind: "user", ID: "postphase-test"}}
}

func (f *fixture) host(t *testing.T, natives ...sdk.Native) *pluginhost.Host {
	return f.hostWithTimeout(t, 0, natives...)
}

func (f *fixture) hostWithTimeout(t *testing.T, timeout time.Duration, natives ...sdk.Native) *pluginhost.Host {
	t.Helper()
	bindings := make([]pluginhost.Binding, 0, len(natives))
	for _, native := range natives {
		bindings = append(bindings, pluginhost.Binding{
			ID:         "post-" + native.Manifest.Alias,
			Alias:      native.Manifest.Alias,
			Instance:   native.Instance,
			Subscriber: "observe",
			Phase:      pluginapi.PhasePost,
			Reads:      []string{"fields.title", "fields.normalized"},
			Timeout:    timeout,
		})
	}
	host, err := pluginhost.New(pluginhost.Config{Natives: natives, Bindings: bindings, Dispatcher: registry{}})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func (f *fixture) writer(host *pluginhost.Host, store driver.Store, lease driver.ActiveLease) *pipeline.Writer {
	return pipeline.New(store, lease,
		pipeline.WithOntology(acceptingOntology{}),
		pipeline.WithPostSubscribers(host.Posts()...),
		pipeline.WithIDGenerator(func() (string, error) {
			return fmt.Sprintf("pl-postphase-%d", f.ids.Add(1)), nil
		}),
	)
}

func create(title, key string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Key: key, Type: "issue", Fields: map[string]any{"title": title},
	}}}}
}

func patch(ref model.Ref, title string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{
		Target: ref, Fields: map[string]any{"title": title},
	}}}}
}

func writeMeta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%032x", n), Project: "postphase"}
}

func receipt(t *testing.T, store driver.Store, principal model.PrincipalRef, meta model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	principalKey, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	value, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(context.Background(), driver.View{Incarnation: value.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	entry, err := reader.IdempotencyByKey(context.Background(), principalKey, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil {
		t.Fatalf("missing receipt for %s", meta.IdempotencyKey)
	}
	return entry
}

func head(t *testing.T, store interface {
	Meta(context.Context) (driver.Meta, error)
}) model.Revision {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return meta.Head
}

func hostErrorCode(err error) string {
	var hostErr *pluginhost.Error
	if errors.As(err, &hostErr) {
		return hostErr.Code
	}
	return ""
}

func TestNativePostRunsAtCommittedRevisionInParallelAndFinalizesReceipt(t *testing.T) {
	f := newFixture(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	observed := make(chan error, 2)
	var calls atomic.Int32
	observe := func(label string) postCallback {
		return func(ctx context.Context, host pluginapi.Host, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
			calls.Add(1)
			var check error
			if change.Revision.String() != "1" {
				check = fmt.Errorf("%s revision=%s", label, change.Revision)
			}
			page, err := reader.Query(ctx, pluginapi.Query{Limit: 10})
			if check == nil && (err != nil || page.AsOf.String() != change.Revision.String() || len(page.Items) != 1) {
				check = fmt.Errorf("%s pinned query asOf=%s items=%d err=%v", label, page.AsOf, len(page.Items), err)
			}
			if check == nil {
				if change.Changes[0].SetBy["fields.title"] != "request" {
					check = fmt.Errorf("%s lost request provenance: %+v", label, change.Changes[0].SetBy)
				}
			}
			if check == nil {
				records, err := reader.Get(ctx, []pluginapi.Ref{change.Changes[0].Resource.Ref})
				if err != nil || len(records) != 1 || records[0].Revision.String() != change.Revision.String() {
					check = fmt.Errorf("%s committed read revision=%s records=%d err=%v", label, func() pluginapi.Revision {
						if len(records) == 0 {
							return ""
						}
						return records[0].Revision
					}(), len(records), err)
				}
			}
			if label == "a" && check == nil {
				if err := host.Call(ctx, "tx.apply", map[string]any{"id": "ignored"}, nil); hostErrorCode(err) != pluginhost.ErrReentrant {
					check = fmt.Errorf("graph write was not rejected: %v", err)
				}
			}
			if label == "a" && check == nil {
				if _, err := host.HTTP().Get("https://example.com"); hostErrorCode(err) != pluginhost.ErrEgress {
					check = fmt.Errorf("undeclared egress was not rejected: %v", err)
				}
			}
			observed <- check
			started <- label
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	host := f.host(t, nativePost("alpha", observe("a")), nativePost("beta", observe("b")))
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(1)
	resultCh := make(chan struct {
		commit model.Commit
		err    error
	}, 1)
	go func() {
		commit, err := writer.Apply(f.ctx, f.principal, create("hello", "first"), meta)
		resultCh <- struct {
			commit model.Commit
			err    error
		}{commit, err}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-f.ctx.Done():
			t.Fatal("native post bindings did not start in parallel")
		}
	}
	close(release)
	result := <-resultCh
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.commit.Revision.String() != "1" || len(result.commit.Warnings) != 0 || calls.Load() != 2 {
		t.Fatalf("commit=%+v calls=%d", result.commit, calls.Load())
	}
	for i := 0; i < 2; i++ {
		if err := <-observed; err != nil {
			t.Fatal(err)
		}
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending || entry.Commit.Revision.String() != "1" || len(entry.Commit.Warnings) != 0 {
		t.Fatalf("receipt=%+v", entry)
	}
	if got := head(t, f.store); got.String() != "1" {
		t.Fatalf("post callbacks changed graph head=%s", got)
	}
	t.Logf("native post proof: revision=%s parallel_bindings=2 receipt_pending=%t calls=%d", result.commit.Revision, entry.ResponsePending, calls.Load())
}

func TestNativePostFailureIsDurableWarningAndReplayDoesNotRepeatSideEffects(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	host := f.host(t, nativePost("failed", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		calls.Add(1)
		return errors.New("destination unavailable")
	}))
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(2)
	first, err := writer.Apply(f.ctx, f.principal, create("warning", "warning"), meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Warnings) != 1 || first.Warnings[0].Code != "post_subscriber_failed" || calls.Load() != 1 {
		t.Fatalf("first=%+v calls=%d", first, calls.Load())
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending || len(entry.Commit.Warnings) != 1 || entry.Commit.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("final warning was not retained=%+v", entry)
	}
	replayed, err := writer.Apply(f.ctx, f.principal, create("warning", "warning"), meta)
	if err != nil {
		t.Fatal(err)
	}
	if !reflectCommitEqual(first, replayed) || calls.Load() != 1 {
		t.Fatalf("replay=%+v first=%+v calls=%d", replayed, first, calls.Load())
	}
	t.Logf("failed post proof: committed_revision=%s warning=%s receipt_pending=%t replay_calls=%d", first.Revision, first.Warnings[0].Code, entry.ResponsePending, calls.Load())
}

func TestNativePostBudgetAndNoopDryRunSuppressSideEffects(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	var callbackDeadline atomic.Int64
	started := make(chan struct{})
	host := f.hostWithTimeout(t, time.Second, nativePost("bounded", func(ctx context.Context, _ pluginapi.Host, _ pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
		if deadline, ok := ctx.Deadline(); ok {
			callbackDeadline.Store(deadline.UnixNano())
		}
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}))
	writer := f.writer(host, f.store, f.lease)
	first, err := writer.Apply(f.ctx, f.principal, create("budget", "budget"), writeMeta(7))
	if err != nil || len(first.Warnings) != 1 || calls.Load() != 1 {
		t.Fatalf("bounded post result=%+v err=%v calls=%d", first, err, calls.Load())
	}
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal("bounded post callback did not run")
	}
	deadline := time.Unix(0, callbackDeadline.Load())
	if callbackDeadline.Load() == 0 || time.Until(deadline) > time.Second {
		t.Fatalf("callback exceeded one-second host budget: deadline=%s", deadline)
	}
	if entry := receipt(t, f.store, f.principal, writeMeta(7)); entry.ResponsePending {
		t.Fatalf("bounded post left pending receipt=%+v", entry)
	}
	ref := first.Results[0].Ref
	noopMeta := writeMeta(8)
	noop, err := writer.Apply(f.ctx, f.principal, patch(ref, "budget"), noopMeta)
	if err != nil || noop.Revision.String() != first.Revision.String() || calls.Load() != 1 {
		t.Fatalf("no-op reran post or changed revision=%+v err=%v calls=%d", noop, err, calls.Load())
	}
	dryMeta := writeMeta(9)
	dry, err := writer.Apply(f.ctx, f.principal, patch(ref, "would-change"), func() model.WriteMeta {
		value := dryMeta
		value.DryRun = true
		return value
	}())
	if err != nil || !dry.WouldCommit || dry.Revision.String() != first.Revision.String() || calls.Load() != 1 {
		t.Fatalf("dry-run published or ran post=%+v err=%v calls=%d", dry, err, calls.Load())
	}
	if got := head(t, f.store); got.String() != first.Revision.String() {
		t.Fatalf("dry-run changed head=%s", got)
	}
	t.Logf("budget/no-op proof: callback_deadline=%s warning=%s no_op_revision=%s dry_run=%t post_calls=%d", deadline, first.Warnings[0].Code, noop.Revision, dry.WouldCommit, calls.Load())
}

func TestCallerCancellationAfterCommitStillReturnsFinalizedNativePostReceipt(t *testing.T) {
	f := newFixture(t)
	started := make(chan struct{})
	var cancel context.CancelFunc
	var postRevision atomic.Value
	host := f.host(t, nativePost("cancel", func(ctx context.Context, _ pluginapi.Host, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
		postRevision.Store(change.Revision.String())
		if _, err := reader.Query(ctx, pluginapi.Query{Limit: 1}); err != nil {
			return err
		}
		close(started)
		cancel()
		return nil
	}))
	writer := f.writer(host, f.store, f.lease)
	ctx, cancelCall := context.WithCancel(f.ctx)
	cancel = cancelCall
	meta := writeMeta(3)
	commit, err := writer.Apply(ctx, f.principal, create("cancel", "cancel"), meta)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled || commit.Revision.String() != "1" || postRevision.Load() != "1" {
		t.Fatalf("cancelled caller result=%+v ctx=%v post=%v", commit, ctx.Err(), postRevision.Load())
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending {
		t.Fatalf("caller cancellation left pending receipt=%+v", entry)
	}
	t.Logf("caller-cancel proof: caller=%v committed_revision=%s finalized=%t", ctx.Err(), commit.Revision, !entry.ResponsePending)
}

type faultyFinalizerStore struct {
	*memory.Store
	fail atomic.Bool
}

func (s *faultyFinalizerStore) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	if s.fail.Load() {
		return errors.New("simulated finalizer transport failure")
	}
	return s.Store.FinalizeResponse(ctx, lease, principal, agent, method, key, revision, warnings)
}

func TestPendingReceiptSurvivesFinalizerFailureAndTakeoverFinalizesWithoutPostReplay(t *testing.T) {
	f := newFixture(t)
	faulty := &faultyFinalizerStore{Store: f.store}
	faulty.fail.Store(true)
	var posts atomic.Int32
	host := f.host(t, nativePost("pending", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		posts.Add(1)
		return nil
	}))
	writer := f.writer(host, faulty, f.lease)
	meta := writeMeta(4)
	_, err := writer.Apply(f.ctx, f.principal, create("pending", "pending"), meta)
	if err == nil {
		t.Fatal("unresolved finalizer failure returned success")
	}
	var pendingErr *model.Error
	if !errors.As(err, &pendingErr) || pendingErr.Data.Code != "busy" {
		t.Fatalf("finalizer error=%v", err)
	}
	pending := receipt(t, f.store, f.principal, meta)
	if !pending.ResponsePending || posts.Load() != 1 || pending.Commit.Revision.String() != "1" {
		t.Fatalf("pending receipt=%+v posts=%d", pending, posts.Load())
	}
	retained, err := f.store.Read(f.ctx, driver.View{Incarnation: "postphase-incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retained.Close() })
	oldLease := f.lease
	if err := oldLease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	newLease, err := f.store.Lease(f.ctx, "postphase-takeover", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.lease = newLease
	t.Cleanup(func() { _ = newLease.Release(context.Background()) })
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "recovered"}}
	if err := f.store.FinalizeResponse(f.ctx, oldLease, "user:postphase-test", "", meta.Method, meta.IdempotencyKey, pending.Commit.Revision, warnings); err == nil {
		t.Fatal("old lease finalized pending receipt")
	} else {
		var fenced driver.ErrFenced
		if !errors.As(err, &fenced) {
			t.Fatalf("old lease finalization error=%v", err)
		}
	}
	faulty.fail.Store(false)
	if err := f.store.FinalizeResponse(f.ctx, newLease, "user:postphase-test", "", meta.Method, meta.IdempotencyKey, pending.Commit.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	writer = f.writer(host, faulty, newLease)
	replayed, err := writer.Apply(f.ctx, f.principal, create("pending", "pending"), meta)
	if err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 || len(replayed.Warnings) != 1 || replayed.Warnings[0].Message != "recovered" {
		t.Fatalf("takeover replay=%+v posts=%d", replayed, posts.Load())
	}
	noPostWriter := pipeline.New(f.store, newLease, pipeline.WithOntology(acceptingOntology{}), pipeline.WithIDGenerator(func() (string, error) {
		return "pl-postphase-takeover", nil
	}))
	if _, err := noPostWriter.Apply(f.ctx, f.principal, create("after-takeover", "after-takeover"), writeMeta(6)); err != nil {
		t.Fatal(err)
	}
	if retained.Snapshot().At.String() != "1" {
		t.Fatalf("retained reader moved to %s", retained.Snapshot().At)
	}
	if got := head(t, f.store); got.String() != "2" {
		t.Fatalf("takeover write did not advance current head=%s", got)
	}
	page, err := retained.Query(f.ctx, driver.Query{})
	if err != nil || len(page.Items) != 1 || page.Items[0].Revision.String() != "1" {
		t.Fatalf("retained reader observed current state page=%+v err=%v", page, err)
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending || len(entry.Commit.Warnings) != 1 {
		t.Fatalf("takeover receipt=%+v", entry)
	}
	t.Logf("takeover proof: old_epoch_fenced=true pending_revision=%s finalized=true retained_reader=%s posts=%d", pending.Commit.Revision, entry.Commit.Revision, posts.Load())
}

func TestDispatcherReplaysIdenticalDeliveryKeyAfterCheckpointFailure(t *testing.T) {
	f := newFixture(t)
	host := f.host(t, nativePost("writer", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		return nil
	}))
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(5)
	commit, err := writer.Apply(f.ctx, f.principal, create("durable", "durable"), meta)
	if err != nil {
		t.Fatal(err)
	}
	metaInfo, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: coreevents.AllowAll{}}
	checkpoints, err := serverevents.NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	var late []bool
	var eventTypes []model.DurableEventType
	var resources []string
	var provenance []map[string]string
	var sideEffects atomic.Int32
	binding := coreevents.Binding{ID: "durable-post", Class: coreevents.Post, Principal: f.principal, Handler: postHandler(func(_ context.Context, item coreevents.Delivery) error {
		keys = append(keys, item.Key.String())
		late = append(late, item.Late)
		if item.Event.Durable != nil {
			eventTypes = append(eventTypes, item.Event.Durable.Type)
			resources = append(resources, item.Event.Durable.Resource)
			provenance = append(provenance, item.Event.Durable.SetBy)
		}
		sideEffects.Add(1)
		if len(keys) == 1 {
			return errors.New("checkpoint write lost after side effect")
		}
		return nil
	})}
	dispatcher := &coreevents.Dispatcher{Reader: reader, Checkpoints: checkpoints, Active: f.lease}
	warnings := dispatcher.RunPost(f.ctx, []coreevents.Binding{binding}, commit.Revision)
	if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("checkpoint failure warning=%+v", warnings)
	}
	state, generation, err := checkpoints.Load(f.ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	if generation != 1 || state.Position.Revision.String() != "0" || state.Failure == nil || !state.Failure.DeadLetter {
		t.Fatalf("failed delivery state=%+v generation=%d", state, generation)
	}
	// A new dispatcher owns no in-process callback state. Its replay comes from
	// the retained changeset and checkpoint only, and must be marked late.
	newCheckpoints, err := serverevents.NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	second := &coreevents.Dispatcher{Reader: reader, Checkpoints: newCheckpoints, Active: f.lease}
	binding.Handler = postHandler(func(_ context.Context, item coreevents.Delivery) error {
		keys = append(keys, item.Key.String())
		late = append(late, item.Late)
		if item.Event.Durable != nil {
			eventTypes = append(eventTypes, item.Event.Durable.Type)
			resources = append(resources, item.Event.Durable.Resource)
			provenance = append(provenance, item.Event.Durable.SetBy)
		}
		sideEffects.Add(1)
		return nil
	})
	progress, err := second.Step(f.ctx, binding)
	if err != nil || !progress.Advanced {
		t.Fatalf("late replay progress=%+v err=%v", progress, err)
	}
	if len(keys) != 2 || keys[0] != keys[1] || late[0] || !late[1] || sideEffects.Load() != 2 {
		t.Fatalf("delivery identity keys=%v late=%v effects=%d", keys, late, sideEffects.Load())
	}
	wantResource := "bead/" + commit.Results[0].Ref.ID
	if len(eventTypes) != 2 || eventTypes[0] != model.EventBeadCreated || eventTypes[1] != model.EventBeadCreated || resources[0] != wantResource || provenance[0]["fields/title"] != "request" {
		t.Fatalf("durable event record/provenance types=%v resources=%v provenance=%v", eventTypes, resources, provenance)
	}
	state, generation, err = newCheckpoints.Load(f.ctx, binding.ID)
	if err != nil || generation != 2 || state.Failure != nil || state.Position.Revision.String() != commit.Revision.String() {
		t.Fatalf("recovered checkpoint=%+v generation=%d err=%v", state, generation, err)
	}
	encoded, err := json.Marshal(state)
	if err != nil || strings.Contains(string(encoded), "durable") || strings.Contains(string(encoded), "title") {
		t.Fatalf("checkpoint retained event payload: %s", encoded)
	}
	if metaInfo.Head.String() != commit.Revision.String() || head(t, f.store).String() != commit.Revision.String() {
		t.Fatalf("delivery changed graph head before=%s after=%s", metaInfo.Head, head(t, f.store))
	}
	t.Logf("dispatcher proof: key=%s late_replay=%t checkpoint_generation=%d head=%s", keys[0], late[1], generation, head(t, f.store))
}

type postHandler func(context.Context, coreevents.Delivery) error

func (h postHandler) Deliver(ctx context.Context, item coreevents.Delivery) error {
	return h(ctx, item)
}

func reflectCommitEqual(a, b model.Commit) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && string(left) == string(right)
}

var _ driver.ResponseFinalizer = (*faultyFinalizerStore)(nil)
var _ pluginhost.Dispatcher = registry{}
