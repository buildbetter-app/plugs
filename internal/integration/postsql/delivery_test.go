package postsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

type handler func(context.Context, coreevents.Delivery) error

func (h handler) Deliver(ctx context.Context, d coreevents.Delivery) error { return h(ctx, d) }

func (f *fixture) dispatcher(t *testing.T) (*coreevents.Dispatcher, *serverevents.Checkpoints) {
	t.Helper()
	checkpoints, err := serverevents.NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	reader := &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: coreevents.AllowAll{}}
	return &coreevents.Dispatcher{Reader: reader, Checkpoints: checkpoints, Active: f.lease}, checkpoints
}

func prime(t *testing.T, f *fixture, d *coreevents.Dispatcher, b coreevents.Binding) {
	t.Helper()
	// Consume the real protected-source fixture through the dispatcher. The
	// bead-created filter skips its callback but durably advances its cursor.
	progress, err := d.Step(f.ctx, b)
	if err != nil || !progress.Advanced || progress.Delivered != 0 || progress.Position.Revision.String() != "1" {
		t.Fatalf("dispatcher failed to consume source fixture: progress=%+v err=%v", progress, err)
	}
}

func deliveries(t *testing.T, journal string) []coreevents.Delivery {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(effects(t, journal)))
	var result []coreevents.Delivery
	for {
		var value coreevents.Delivery
		err := decoder.Decode(&value)
		if err == io.EOF {
			return result
		}
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
}

func assertDelivery(t *testing.T, f *fixture, item coreevents.Delivery, binding string, commit model.Commit, m model.WriteMeta) {
	t.Helper()
	i, err := item.Key.Index.Uint64()
	if err != nil || i > 1 || item.Key.Binding != binding || item.Key.Revision.String() != commit.Revision.String() || item.Key.Incarnation != f.meta(t).Incarnation {
		t.Fatalf("delivery key lost committed identity: %+v err=%v", item.Key, err)
	}
	e := item.Event.Durable
	if e == nil || e.Type != model.EventBeadCreated || e.Resource != "bead/"+commit.Results[i].Ref.ID || item.Partition != e.Resource || e.Revision.String() != commit.Revision.String() || e.Index.String() != item.Key.Index.String() {
		t.Fatalf("delivery was not reconstructed from committed SQL history: %+v", item)
	}
	same(t, writerPrincipal, e.Principal)
	if len(e.Before) != 0 || e.Cause.Root != m.IdempotencyKey || e.SetBy["fields.title"] != "request" {
		t.Fatalf("delivery lost audit provenance or create before-image: %+v", e)
	}
	title(t, encoded(t, e.After), []string{"dispatch-first", "dispatch-second"}[i])
}

func checkpoint(t *testing.T, f *fixture, c *serverevents.Checkpoints, binding string, generation uint64, revision, index string) coreevents.State {
	t.Helper()
	state, got, err := c.Load(f.ctx, binding)
	if err != nil || got != generation || state.Class != coreevents.Post || state.Position.Revision.String() != revision || state.Position.Index.String() != index {
		t.Fatalf("SQL checkpoint: state=%+v generation=%d want=%d err=%v", state, got, generation, err)
	}
	raw, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, "events:"+binding)
	if err != nil || raw.Generation != generation || !bytes.Equal(raw.Payload, encoded(t, state)) {
		t.Fatalf("event adapter did not persist actual SQL metadata: %+v err=%v", raw, err)
	}
	for _, payload := range []string{"dispatch-first", "dispatch-second", `"before"`, `"after"`, `"fields"`} {
		if bytes.Contains(raw.Payload, []byte(payload)) {
			t.Fatalf("checkpoint stored event images: %s", raw.Payload)
		}
	}
	return state
}

func nativeSource(t *testing.T, f *fixture, journal string) (model.Commit, model.WriteMeta, *driver.IdempotencyRecord) {
	t.Helper()
	host := nativeHost(t, post{"source", func(_ context.Context, c pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
		return appendEffect(journal, c)
	}})
	m := meta(401)
	out := apply(t, f, f.writer(host, nil), create("dispatch-first", "dispatch-second"), m)
	entry := f.receipt(t, m)
	if out.Replayed || len(out.Commit.Warnings) != 0 || entry.ResponsePending || len(entry.Changeset.Events) != 2 || out.Commit.Revision.String() != "2" {
		t.Fatalf("native source failed to publish a real two-event SQL plan: %+v entry=%+v", out, entry)
	}
	return out.Commit, m, entry
}

func caughtUp(t *testing.T, f *fixture, d *coreevents.Dispatcher, b coreevents.Binding, journal string) {
	t.Helper()
	before := effects(t, journal)
	// The later title edit is real history but outside this binding's filter.
	progress, err := d.Step(f.ctx, b)
	if err != nil || !progress.Advanced || progress.Delivered != 0 || progress.Position.Revision.String() != "3" {
		t.Fatalf("filtered later event failed to advance: %+v err=%v", progress, err)
	}
	progress, err = d.Step(f.ctx, b)
	if err != nil || progress.Advanced || progress.Delivered != 0 {
		t.Fatalf("caught-up dispatcher repeated an event: %+v err=%v", progress, err)
	}
	same(t, before, effects(t, journal))
}

func TestSQLPostCheckpointRejectionAndLostAckRecoverAfterReopen(t *testing.T) {
	for _, mode := range []string{"sql_rejected", "persisted_ack_lost"} {
		t.Run(mode, func(t *testing.T) {
			engines(t, func(t *testing.T, e engine) {
				runCheckpointRecovery(t, e, mode)
			})
		})
	}
}

func runCheckpointRecovery(t *testing.T, e engine, mode string) {
	t.Helper()
	f := newFixture(t, e)
	nativeJournal, deliveryJournal := filepath.Join(t.TempDir(), "native.ndjson"), filepath.Join(t.TempDir(), "deliveries.ndjson")
	binding := coreevents.Binding{ID: "sql-post", Class: coreevents.Post, Principal: writerPrincipal, Filter: coreevents.Filter{Types: []string{string(model.EventBeadCreated)}}, Handler: handler(func(_ context.Context, item coreevents.Delivery) error { return appendEffect(deliveryJournal, item) })}
	d, c := f.dispatcher(t)
	prime(t, f, d, binding)
	checkpoint(t, f, c, binding.ID, 1, "1", "0")
	commit, m, receipt := nativeSource(t, f, nativeJournal)
	var removeFault = func() {}
	var backing coreevents.Checkpoints = c
	if mode == "sql_rejected" {
		removeFault = rejectSQLUpdate(t, f, "checkpoint")
	} else {
		backing = &lostCheckpointAck{Checkpoints: c}
	}
	capture := &checkpointCapture{Checkpoints: backing}
	d.Checkpoints = capture
	warnings := d.RunPost(f.ctx, []coreevents.Binding{binding}, commit.Revision)
	if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" || capture.calls != 1 || capture.err == nil {
		t.Fatalf("checkpoint fault did not fail at the real save: warnings=%+v calls=%d err=%v", warnings, capture.calls, capture.err)
	}
	if mode == "sql_rejected" && !strings.Contains(capture.err.Error(), "post_sql_rejected") {
		t.Fatalf("checkpoint rejection did not reach SQL: %v", capture.err)
	}
	first := deliveries(t, deliveryJournal)
	if len(first) != 1 || first[0].Late {
		t.Fatalf("fresh post delivery=%+v", first)
	}
	assertDelivery(t, f, first[0], binding.ID, commit, m)
	generation, rev := uint64(1), "1"
	if mode == "persisted_ack_lost" {
		generation, rev = 2, "2"
	}
	state := checkpoint(t, f, c, binding.ID, generation, rev, "0")
	if state.Failure != nil {
		t.Fatal("checkpoint publication fault was mislabeled as handler dead-letter")
	}
	removeFault()
	oldEpoch := f.lease.Epoch()
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	var err error
	f.lease, err = f.store.Lease(f.ctx, "post-sql-next-holder", driver.LeaseTTL)
	if err != nil || f.lease.Epoch() <= oldEpoch {
		t.Fatalf("lease handoff failed: %v", err)
	}
	var fenced driver.ErrFenced
	if _, err := c.Save(f.ctx, binding.ID, generation, coreevents.State{Class: coreevents.Post, Position: first[0].Key.Position}); !errors.As(err, &fenced) {
		t.Fatalf("old event adapter advanced SQL checkpoint after handoff: %v", err)
	}
	same(t, state, checkpoint(t, f, c, binding.ID, generation, rev, "0"))
	apply(t, f, f.writer(nil, nil), patch(commit.Results[0].Ref, "newer-head"), meta(402))
	refs := []model.Ref{commit.Results[0].Ref, commit.Results[1].Ref}
	before, nativeBefore := f.graph(t, refs...), effects(t, nativeJournal)
	f.reopen(t)
	recovered, newCheckpoints := f.dispatcher(t)
	same(t, state, checkpoint(t, f, newCheckpoints, binding.ID, generation, rev, "0"))
	progress, err := recovered.Step(f.ctx, binding)
	if err != nil || !progress.Advanced {
		t.Fatalf("reopened SQL dispatcher failed: %+v err=%v", progress, err)
	}
	all := deliveries(t, deliveryJournal)
	want := 3
	if mode == "persisted_ack_lost" {
		want = 2
	}
	if len(all) != want {
		t.Fatalf("delivery attempts=%d want=%d", len(all), want)
	}
	for i, item := range all {
		assertDelivery(t, f, item, binding.ID, commit, m)
		if i > 0 && !item.Late {
			t.Fatal("reopened recovery did not mark delivery late")
		}
	}
	if mode == "sql_rejected" {
		same(t, all[0].Key, all[1].Key)
		same(t, all[0].Event, all[1].Event)
	} else if all[1].Key.Index.String() != "1" {
		t.Fatal("durable acknowledged position was not respected after lost ack")
	}
	state = checkpoint(t, f, newCheckpoints, binding.ID, 3, "2", "1")
	if state.Failure != nil {
		t.Fatal("successful recovery retained a failure")
	}
	caughtUp(t, f, recovered, binding, deliveryJournal)
	same(t, before, f.graph(t, refs...))
	same(t, receipt, f.receipt(t, m))
	same(t, nativeBefore, effects(t, nativeJournal))
	t.Logf("SQL checkpoint proof: %s; native Writer log -> real history materializer -> post; attempts=%d, stable keys/payloads, old epoch fenced, late handle-reopen recovery and no graph/native-response changes", mode, want)
}

type noticeObserver struct{ notices chan coreevents.Notice }

func (o noticeObserver) Notify(_ context.Context, n coreevents.Notice) { o.notices <- n }

func TestSQLPostDeadLetterAndNoticeSurviveReopenBeforeLateRetry(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		nativeJournal, deliveryJournal := filepath.Join(t.TempDir(), "native.ndjson"), filepath.Join(t.TempDir(), "failed-deliveries.ndjson")
		binding := coreevents.Binding{ID: "sql-post-failure", Class: coreevents.Post, Principal: writerPrincipal, Filter: coreevents.Filter{Types: []string{string(model.EventBeadCreated)}}, Handler: handler(func(_ context.Context, item coreevents.Delivery) error {
			if err := appendEffect(deliveryJournal, item); err != nil {
				return err
			}
			return errors.New("disposable event destination failed after its external effect")
		})}
		d, c := f.dispatcher(t)
		prime(t, f, d, binding)
		commit, m, receipt := nativeSource(t, f, nativeJournal)
		notices := make(chan coreevents.Notice, 2)
		d.Observer = noticeObserver{notices}
		warnings := d.RunPost(f.ctx, []coreevents.Binding{binding}, commit.Revision)
		if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
			t.Fatalf("post handler failure did not return a committed-result warning: %+v", warnings)
		}
		failed := checkpoint(t, f, c, binding.ID, 2, "1", "0")
		if failed.Failure == nil || !failed.Failure.DeadLetter || failed.Failure.Attempts != 1 || failed.Halted || failed.Failure.FirstAt.IsZero() {
			t.Fatalf("post failure metadata was not durably dead-lettered: %+v", failed)
		}
		first := deliveries(t, deliveryJournal)
		if len(first) != 1 || first[0].Late {
			t.Fatalf("initial failed delivery=%+v", first)
		}
		same(t, first[0].Key, failed.Failure.Key)
		notice := await(t, f.ctx, notices)
		if notice.Binding != binding.ID || notice.Class != coreevents.Post || notice.Reason != "delivery_failed" {
			t.Fatalf("missing actual alert/breaker observer notice: %+v", notice)
		}
		same(t, failed.Failure, notice.Failure)
		// Dispatcher warnings are a separate existing API. They are not
		// fabricated into the native Writer's already finalized receipt.
		same(t, receipt, f.receipt(t, m))
		apply(t, f, f.writer(nil, nil), patch(commit.Results[0].Ref, "newer-head"), meta(402))
		refs := []model.Ref{commit.Results[0].Ref, commit.Results[1].Ref}
		before, nativeBefore := f.graph(t, refs...), effects(t, nativeJournal)
		f.reopen(t)
		recovered, newCheckpoints := f.dispatcher(t)
		same(t, failed, checkpoint(t, f, newCheckpoints, binding.ID, 2, "1", "0"))
		binding.Handler = handler(func(_ context.Context, item coreevents.Delivery) error { return appendEffect(deliveryJournal, item) })
		progress, err := recovered.Step(f.ctx, binding)
		if err != nil || !progress.Advanced || progress.Delivered != 2 {
			t.Fatalf("dead-letter retry failed to consume the real changeset: %+v err=%v", progress, err)
		}
		all := deliveries(t, deliveryJournal)
		if len(all) != 3 {
			t.Fatalf("at-least-once post effects=%d want=3", len(all))
		}
		same(t, all[0].Key, all[1].Key)
		same(t, all[0].Event, all[1].Event)
		for i, item := range all {
			assertDelivery(t, f, item, binding.ID, commit, m)
			if i > 0 && !item.Late {
				t.Fatal("dead-letter recovery delivery was not marked late")
			}
		}
		final := checkpoint(t, f, newCheckpoints, binding.ID, 4, "2", "1")
		if final.Failure != nil || final.Halted {
			t.Fatalf("successful retry retained failed state: %+v", final)
		}
		caughtUp(t, f, recovered, binding, deliveryJournal)
		same(t, before, f.graph(t, refs...))
		same(t, receipt, f.receipt(t, m))
		same(t, nativeBefore, effects(t, nativeJournal))
		t.Log("SQL dead-letter proof: real post handler failure persists key/attempt/time and emits observer notice; reopened late retry repeats the same external effect/key and historical payload; native receipt and graph remain unchanged")
	})
}
