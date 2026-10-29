package postphase

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

type postHandler func(context.Context, coreevents.Delivery) error

func (h postHandler) Deliver(ctx context.Context, item coreevents.Delivery) error {
	return h(ctx, item)
}

func (f *fixture) dispatcher(t *testing.T) (*coreevents.Dispatcher, *serverevents.Checkpoints) {
	t.Helper()
	checkpoints, err := serverevents.NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	reader := &coreevents.Reader{
		Log:           serverevents.Log{Store: f.store},
		Materializer:  serverevents.HistoryMaterializer{Store: f.store},
		Authorization: coreevents.AllowAll{},
	}
	return &coreevents.Dispatcher{Reader: reader, Checkpoints: checkpoints, Active: f.lease}, checkpoints
}

var checkpointFault = errors.New("injected checkpoint-save transport failure")

type checkpointSaveFault struct {
	coreevents.Checkpoints
	persist bool
	calls   atomic.Int32
}

func (s *checkpointSaveFault) Save(ctx context.Context, binding string, generation uint64, state coreevents.State) (uint64, error) {
	if s.calls.Add(1) == 1 {
		if s.persist {
			if _, err := s.Checkpoints.Save(ctx, binding, generation, state); err != nil {
				return generation, err
			}
		}
		return generation, checkpointFault
	}
	return s.Checkpoints.Save(ctx, binding, generation, state)
}

type deliverySink struct {
	mu       sync.Mutex
	attempts []coreevents.Delivery
	effects  map[string]bool
}

func (s *deliverySink) handle(_ context.Context, item coreevents.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, item)
	if s.effects == nil {
		s.effects = map[string]bool{}
	}
	// This is the external consumer's idempotent effect, not a replacement
	// checkpoint. Losing the save may repeat delivery but must reuse its key.
	s.effects[item.Key.String()] = true
	return nil
}

func TestDispatcherCheckpointFaultReconstructsDeliveryFromNativeWriterLog(t *testing.T) {
	for _, mode := range []struct {
		name    string
		persist bool
	}{{"save_rejected", false}, {"save_persisted_ack_lost", true}} {
		t.Run(mode.name, func(t *testing.T) {
			f := newFixture(t)
			var posts atomic.Int32
			host := f.host(t, nativePost("eventsource", func(_ context.Context, _ pluginapi.Host, c pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
				posts.Add(1)
				if len(c.Changes) != 2 {
					return errors.New("native did not observe atomic two-record batch")
				}
				return nil
			}))
			meta := writeMeta(30)
			cs := create("durable-first", "first")
			cs.Ops = append(cs.Ops, create("durable-second", "second").Ops...)
			commit := mustApply(t, f, f.writer(host, f.store, f.lease), cs, meta)
			entryBefore := receipt(t, f.store, f.principal, meta)
			if len(commit.Warnings) != 0 || len(entryBefore.Changeset.Events) != 2 || posts.Load() != 1 {
				t.Fatalf("native source commit=%+v log=%+v posts=%d", commit, entryBefore.Changeset, posts.Load())
			}
			dispatcher, checkpoints := f.dispatcher(t)
			fault := &checkpointSaveFault{Checkpoints: checkpoints, persist: mode.persist}
			dispatcher.Checkpoints = fault
			sink := &deliverySink{}
			binding := coreevents.Binding{ID: "checkpoint-post", Class: coreevents.Post, Principal: f.principal, Handler: postHandler(sink.handle)}
			warnings := dispatcher.RunPost(f.ctx, []coreevents.Binding{binding}, commit.Revision)
			if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" || fault.calls.Load() != 1 || len(sink.attempts) != 1 {
				t.Fatalf("save fault warnings=%+v save_calls=%d deliveries=%d", warnings, fault.calls.Load(), len(sink.attempts))
			}
			state, generation, err := checkpoints.Load(f.ctx, binding.ID)
			if err != nil || state.Failure != nil {
				t.Fatalf("checkpoint fault was misreported as handler failure: state=%+v err=%v", state, err)
			}
			if mode.persist {
				if generation != 1 || state.Position.Revision.String() != "1" || state.Position.Index.String() != "0" {
					t.Fatalf("lost acknowledgement did not retain actual save=%+v generation=%d", state, generation)
				}
			} else if generation != 0 {
				t.Fatalf("rejected checkpoint persisted generation=%d", generation)
			}
			old := f.lease
			if err := old.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			lease, err := f.store.Lease(f.ctx, "event-takeover", time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			f.lease = lease
			// The previous adapter still holds its old epoch, even if handed an
			// otherwise valid candidate state and generation after takeover.
			candidate := coreevents.State{Class: coreevents.Post, Position: sink.attempts[0].Key.Position}
			if _, err := checkpoints.Save(f.ctx, binding.ID, generation, candidate); err == nil {
				t.Fatal("old epoch saved checkpoint after takeover")
			} else {
				var fenced driver.ErrFenced
				if !errors.As(err, &fenced) {
					t.Fatalf("old checkpoint epoch error=%v", err)
				}
			}
			// All adapters and dispatcher state are new. Only the actual store
			// log/history/checkpoint and the consumer's external effects survive.
			recovered, newCheckpoints := f.dispatcher(t)
			progress, err := recovered.Step(f.ctx, binding)
			if err != nil || !progress.Advanced {
				t.Fatalf("recovery progress=%+v err=%v", progress, err)
			}
			wantAttempts := 3
			if mode.persist {
				wantAttempts = 2
			}
			if len(sink.attempts) != wantAttempts || len(sink.effects) != 2 {
				t.Fatalf("at-least-once effects attempts=%d unique=%d", len(sink.attempts), len(sink.effects))
			}
			if sink.attempts[0].Late {
				t.Fatal("fresh delivery was marked late")
			}
			for index, item := range sink.attempts {
				if index > 0 && !item.Late {
					t.Fatal("reconstructed delivery was not marked late")
				}
				assertCreatedEvent(t, f, item, commit, meta)
			}
			if !mode.persist {
				assertJSONEqual(t, sink.attempts[0].Key, sink.attempts[1].Key)
				assertJSONEqual(t, sink.attempts[0].Event, sink.attempts[1].Event)
			}
			state, generation, err = newCheckpoints.Load(f.ctx, binding.ID)
			if err != nil || generation != 2 || state.Failure != nil || state.Position.Revision.String() != "1" || state.Position.Index.String() != "1" {
				t.Fatalf("final checkpoint=%+v generation=%d err=%v", state, generation, err)
			}
			assertCheckpointMetadataOnly(t, f, binding.ID, state)
			progress, err = recovered.Step(f.ctx, binding)
			if err != nil || progress.Advanced || len(sink.attempts) != wantAttempts {
				t.Fatalf("caught-up dispatcher repeated delivery: %+v err=%v", progress, err)
			}
			assertJSONEqual(t, entryBefore, receipt(t, f.store, f.principal, meta))
			if head(t, f.store).String() != "1" || posts.Load() != 1 {
				t.Fatal("dispatcher replay changed graph or reran native callback")
			}
			t.Logf("checkpoint proof: mode=%s first_key=%s attempts=%d unique_effects=2 generation=2 late recovery; old epoch fenced; receipt and graph unchanged", mode.name, sink.attempts[0].Key, wantAttempts)
		})
	}
}

func assertCreatedEvent(t *testing.T, f *fixture, item coreevents.Delivery, commit model.Commit, meta model.WriteMeta) {
	t.Helper()
	index, err := item.Key.Index.Uint64()
	if err != nil || index > 1 || item.Key.Binding != "checkpoint-post" || item.Key.Incarnation != "postphase-incarnation" || item.Key.Revision.String() != commit.Revision.String() {
		t.Fatalf("delivery identity=%+v err=%v", item.Key, err)
	}
	event := item.Event.Durable
	if event == nil || event.Type != model.EventBeadCreated || event.Resource != "bead/"+commit.Results[index].Ref.ID || item.Partition != event.Resource {
		t.Fatalf("canonical event=%+v", item)
	}
	assertJSONEqual(t, f.principal, event.Principal)
	if event.SetBy["fields.title"] != "request" || event.Cause.Root != meta.IdempotencyKey || len(event.Before) != 0 {
		t.Fatalf("event provenance/images=%+v", event)
	}
	wantTitle := []string{"durable-first", "durable-second"}[index]
	if err := assertTitle(jsonBytes(t, event.After), wantTitle); err != nil {
		t.Fatal(err)
	}
}

func assertCheckpointMetadataOnly(t *testing.T, f *fixture, binding string, state coreevents.State) {
	t.Helper()
	raw, err := f.store.LoadCheckpoint(f.ctx, "events:"+binding)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw.Payload) != string(jsonBytes(t, state)) || strings.Contains(string(raw.Payload), "durable-first") || strings.Contains(string(raw.Payload), "title") || strings.Contains(string(raw.Payload), "after") {
		t.Fatalf("checkpoint retained payload instead of metadata=%s", raw.Payload)
	}
}

type noticeObserver struct{ notices chan coreevents.Notice }

func (o noticeObserver) Notify(_ context.Context, notice coreevents.Notice) { o.notices <- notice }

func TestDispatcherPostFailurePersistsDeadLetterAndEmitsNoticeBeforeLateRetry(t *testing.T) {
	f := newFixture(t)
	host := f.host(t, nativePost("dlqsource", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		return nil
	}))
	meta := writeMeta(31)
	commit := mustApply(t, f, f.writer(host, f.store, f.lease), create("deadletter-source", "deadletter"), meta)
	dispatcher, checkpoints := f.dispatcher(t)
	notices := make(chan coreevents.Notice, 2)
	dispatcher.Observer = noticeObserver{notices}
	sink := &deliverySink{}
	binding := coreevents.Binding{ID: "failure-post", Class: coreevents.Post, Principal: f.principal, Handler: postHandler(func(ctx context.Context, item coreevents.Delivery) error {
		_ = sink.handle(ctx, item)
		return errors.New("actual event consumer failure")
	})}
	warnings := dispatcher.RunPost(f.ctx, []coreevents.Binding{binding}, commit.Revision)
	if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("dispatcher post failure=%+v", warnings)
	}
	state, generation, err := checkpoints.Load(f.ctx, binding.ID)
	if err != nil || generation != 1 || state.Failure == nil || !state.Failure.DeadLetter || state.Failure.Attempts != 1 || state.Position.Revision.String() != "0" {
		t.Fatalf("dead-letter state=%+v generation=%d err=%v", state, generation, err)
	}
	assertJSONEqual(t, sink.attempts[0].Key, state.Failure.Key)
	notice := await(t, f.ctx, notices)
	if notice.Binding != binding.ID || notice.Reason != "delivery_failed" || notice.Class != coreevents.Post {
		t.Fatalf("dead-letter notice=%+v", notice)
	}
	assertJSONEqual(t, state.Failure, notice.Failure)
	assertCheckpointMetadataOnly(t, f, binding.ID, state)
	recovered, newCheckpoints := f.dispatcher(t)
	binding.Handler = postHandler(sink.handle)
	if progress, err := recovered.Step(f.ctx, binding); err != nil || !progress.Advanced {
		t.Fatalf("dead-letter replay progress=%+v err=%v", progress, err)
	}
	if len(sink.attempts) != 2 || len(sink.effects) != 1 || sink.attempts[0].Late || !sink.attempts[1].Late {
		t.Fatalf("dead-letter replay attempts=%+v effects=%d", sink.attempts, len(sink.effects))
	}
	assertJSONEqual(t, sink.attempts[0].Key, sink.attempts[1].Key)
	assertJSONEqual(t, sink.attempts[0].Event, sink.attempts[1].Event)
	state, generation, err = newCheckpoints.Load(f.ctx, binding.ID)
	if err != nil || generation != 2 || state.Failure != nil || head(t, f.store).String() != "1" {
		t.Fatalf("retry checkpoint=%+v generation=%d err=%v", state, generation, err)
	}
	t.Log("dispatcher-only failure proof: persisted dead-letter metadata and observer notice; new dispatcher retries same key/event late; no native breaker/alert-sink composition claimed")
}

type graphEvidence struct {
	Meta       driver.Meta
	Records    driver.Page[driver.Record]
	Versions   driver.Page[driver.Version]
	Changesets []model.Changeset
	Checkpoint driver.Checkpoint
}

func graphSnapshot(t *testing.T, f *fixture, ref model.Ref, binding string) graphEvidence {
	t.Helper()
	var value graphEvidence
	var err error
	reader := openReader(t, f)
	if value.Meta, err = f.store.Meta(f.ctx); err != nil {
		t.Fatal(err)
	}
	if value.Records, err = reader.Query(f.ctx, driver.Query{}); err != nil {
		t.Fatal(err)
	}
	if value.Versions, err = reader.Versions(f.ctx, ref, driver.PageReq{}); err != nil {
		t.Fatal(err)
	}
	if value.Changesets, err = reader.Changesets(f.ctx, nil, 100); err != nil {
		t.Fatal(err)
	}
	if value.Checkpoint, err = f.store.LoadCheckpoint(f.ctx, "events:"+binding); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestNoopDryRunAndRejectedWriteEmitNoPostLogVersionOrDelivery(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	host := f.host(t, nativePost("suppression", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		calls.Add(1)
		return nil
	}))
	writer := f.writer(host, f.store, f.lease)
	first := mustApply(t, f, writer, create("unchanged", "unchanged"), writeMeta(32))
	dispatcher, _ := f.dispatcher(t)
	sink := &deliverySink{}
	binding := coreevents.Binding{ID: "suppression-post", Class: coreevents.Post, Principal: f.principal, Handler: postHandler(sink.handle)}
	if warnings := dispatcher.RunPost(f.ctx, []coreevents.Binding{binding}, first.Revision); len(warnings) != 0 {
		t.Fatalf("initial delivery warnings=%+v", warnings)
	}
	ref := first.Results[0].Ref
	before := graphSnapshot(t, f, ref, binding.ID)
	noopMeta := writeMeta(33)
	noop := mustApply(t, f, writer, patch(ref, "unchanged"), noopMeta)
	if noop.Revision.String() != "1" || noop.WouldCommit || len(noop.Warnings) != 0 {
		t.Fatalf("no-op response=%+v", noop)
	}
	entry := receipt(t, f.store, f.principal, noopMeta)
	if entry.ResponsePending || len(entry.Changeset.Changes) != 0 {
		t.Fatalf("no-op receipt=%+v", entry)
	}
	assertJSONEqual(t, noop, entry.Commit)
	assertJSONEqual(t, noop, mustApply(t, f, writer, patch(ref, "unchanged"), noopMeta))
	principal, err := driver.PrincipalKey(f.principal)
	if err != nil {
		t.Fatal(err)
	}
	if log, err := openReader(t, f).ChangesetByKey(f.ctx, principal, "", noopMeta.Method, noopMeta.IdempotencyKey); err != nil || log != nil {
		t.Fatalf("no-op published changeset=%+v err=%v", log, err)
	}
	assertJSONEqual(t, before, graphSnapshot(t, f, ref, binding.ID))
	dryMeta := writeMeta(34)
	dryMeta.DryRun = true
	dry := mustApply(t, f, writer, patch(ref, "would-change"), dryMeta)
	if !dry.WouldCommit || dry.Revision.String() != "1" || lookupReceipt(t, f.store, f.principal, dryMeta) != nil {
		t.Fatalf("dry-run response/receipt=%+v", dry)
	}
	assertJSONEqual(t, before, graphSnapshot(t, f, ref, binding.ID))
	invalid := create("invalid", "invalid")
	invalid.Ops[0].Create.Type = "unknown-type"
	invalidMeta := writeMeta(35)
	if _, err := writer.Apply(f.ctx, f.principal, invalid, invalidMeta); err == nil {
		t.Fatal("real ontology accepted unknown type")
	}
	if lookupReceipt(t, f.store, f.principal, invalidMeta) != nil {
		t.Fatal("rejected write left receipt")
	}
	if progress, err := dispatcher.Step(f.ctx, binding); err != nil || progress.Advanced || len(sink.attempts) != 1 || calls.Load() != 1 {
		t.Fatalf("suppressed write emitted callback/delivery: progress=%+v err=%v events=%d native_calls=%d", progress, err, len(sink.attempts), calls.Load())
	}
	assertJSONEqual(t, before, graphSnapshot(t, f, ref, binding.ID))
	t.Log("suppression proof: no-op has final receipt only; dry-run/rejection have none; graph, log, versions, checkpoint and event/native callback counts unchanged")
}
