package cp7latency_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Only Store.Write is intercepted; its returned transaction is the concrete
// SQL transaction. Preserve every optional store capability used by this
// Writer, including quiet heartbeat and durable response finalization.
type qualificationWriteStore struct {
	driver.Store
	driver.CheckpointStore
	driver.RecordGuardedCheckpointStore
	driver.ResponseFinalizer
	beforeWrite func()
}

func (s *qualificationWriteStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if hook := s.beforeWrite; hook != nil {
		s.beforeWrite = nil
		hook()
	}
	return s.Store.Write(ctx, lease)
}

func TestCommittedCandidateBeforeTerminalHeadPreventsDurableNull(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 1)
			done := document(1)
			done.Fields["status"] = "done"
			encoded, err := json.Marshal(done)
			if err != nil {
				t.Fatal(err)
			}
			f.patchSource(t, "terminal_empty", beadRef(1), encoded, []string{"/fields/status"})
			ctx := context.Background()
			p := f.agent(1, false)
			live, err := f.app.Service.Ready(ctx, f.user(1), service.ReadyRequest{Project: projectName, Limit: 100})
			if err != nil || len(live.Items) != 0 {
				t.Fatalf("live empty control: %+v %v", live, err)
			}
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			inserted := false
			store := &qualificationWriteStore{Store: f.store.Store,
				CheckpointStore:              f.store.Store.(driver.CheckpointStore),
				RecordGuardedCheckpointStore: f.store.Store.(driver.RecordGuardedCheckpointStore),
				ResponseFinalizer:            f.store.Store.(driver.ResponseFinalizer)}
			store.beforeWrite = func() {
				// A distinct committed source mutation lands after the empty
				// live walk, before the terminal transaction obtains Head.
				f.patchSource(t, "terminal_candidate", beadRef(1), body(t, 1), []string{"/fields/status"})
				inserted = true
			}
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, f.lease, pipeline.WithOntology(ontology), pipeline.WithAuthorizer(f.app.Checker), pipeline.WithEventContext(f.app.Checker))
			domain := service.New(f.store.Store, f.lease, writer, service.WithOntology(ontology))
			meta := model.WriteMeta{Method: "bead.claimNext", Project: projectName, Return: "full", IdempotencyKey: requestKey("terminal-race", 1, "empty", 0)}
			probes := 0
			out, err := writer.ApplyClaimNextEmpty(ctx, p, model.ChangeSet{Ops: []model.Op{}}, meta, func(ctx context.Context, r driver.Reader) (bool, error) {
				probes++
				if _, ok := r.(driver.WriteTx); !ok {
					t.Fatal("terminal probe lost concrete transaction")
				}
				if _, ok := r.(driver.FreshSelectionExistsReader); !ok {
					t.Fatal("terminal probe hid fresh EXISTS capability")
				}
				items, err := domain.ReadyCandidates(ctx, service.ReadyRequest{Project: projectName}, r)
				if err != nil {
					return false, err
				}
				if len(items) != 1 || items[0].ID != beadRef(1).ID {
					t.Fatalf("post-Head probe missed committed candidate: %+v", items)
				}
				return true, nil
			})
			if !inserted || probes != 1 || !errors.Is(err, port.ErrClaimNextReady) || !out.Commit.Revision.IsZero() {
				t.Fatalf("stale null was not released: %+v %v probes=%d", out, err, probes)
			}
			after, err := f.store.Meta(ctx)
			old, _ := before.Head.Uint64()
			if err != nil || after.Head.String() != model.Natural64(old+1).String() {
				t.Fatalf("terminal attempt advanced head beyond competing mutation: %v", err)
			}
			r := f.reader(t, nil)
			principal, _ := driver.PrincipalKey(p)
			receipt, err := r.IdempotencyByKey(ctx, principal, "", "bead.claimNext", meta.IdempotencyKey)
			r.Close()
			if err != nil || receipt != nil {
				t.Fatalf("stale terminal receipt persisted: %+v %v", receipt, err)
			}
			// Reusing the uncommitted key through the complete guarded service
			// now claims the actual candidate and saves a one-row receipt.
			claimed, err := f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
			if err != nil {
				t.Fatal(err)
			}
			f.checkClaim(t, p, claimed, meta, 1)
			t.Logf("CP7_TERMINAL_INSERT engine=%s live_empty=true committed_candidate_before_head=true after_head_actual_probe=true stale_receipt_absent=true guarded_retry_claimed=true", engine)
		})
	}
}

func TestPublicHeartbeatKeepsGraphReceiptAndQueueEligibilitySeparate(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			ctx := context.Background()
			p := f.agent(1, false)
			meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: requestKey("heartbeat-control", 1, "claim", 0)}
			out, err := f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
			if err != nil {
				t.Fatal(err)
			}
			f.checkClaim(t, p, out, meta, 1)
			ref := out.Commit.Results[0].Ref
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r := f.reader(t, nil)
			prior, err := r.Get(ctx, []model.Ref{ref})
			r.Close()
			if err != nil || len(prior) != 1 {
				t.Fatalf("claim source: %v", err)
			}
			beatMeta := model.WriteMeta{Project: projectName, IdempotencyKey: requestKey("heartbeat-control", 1, "beat", 0)}
			beat, err := f.app.Service.HeartbeatWithOutcome(ctx, p, service.HeartbeatRequest{ID: ref}, beatMeta)
			if err != nil || beat.Replayed || beat.LeaseExpiresAt.IsZero() {
				t.Fatalf("real heartbeat: %+v %v", beat, err)
			}
			replay, err := f.app.Service.HeartbeatWithOutcome(ctx, p, service.HeartbeatRequest{ID: ref}, beatMeta)
			if err != nil || !replay.Replayed || !replay.LeaseExpiresAt.Equal(beat.LeaseExpiresAt) {
				t.Fatalf("quiet heartbeat replay changed expiry: %+v %v", replay, err)
			}
			after, err := f.store.Meta(ctx)
			if err != nil || after.Head.String() != before.Head.String() {
				t.Fatalf("heartbeat advanced graph: %v", err)
			}
			r = f.reader(t, nil)
			current, err := r.Get(ctx, []model.Ref{ref})
			r.Close()
			if err != nil || len(current) != 1 || current[0].Revision.String() != prior[0].Revision.String() || !bytes.Equal(current[0].Body, prior[0].Body) {
				t.Fatalf("heartbeat changed graph source/version: %v", err)
			}
			page, err := f.app.Service.Ready(ctx, f.user(1), service.ReadyRequest{Project: projectName, Limit: 1000})
			if err != nil {
				t.Fatal(err)
			}
			for _, bead := range page.Items {
				if bead.ID == ref.ID {
					t.Fatal("heartbeat-held candidate admitted by Ready")
				}
			}
			ontology := service.NewOntology(nil)
			domain := service.New(f.store.Store, f.lease, pipeline.New(f.store.Store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(time.Now)), service.WithOntology(ontology), service.WithClock(time.Now))
			queue, err := domain.ClaimNextReadyPage(ctx, p, service.ReadyRequest{Project: projectName, Limit: 1000})
			if err != nil || len(queue.Items) != len(page.Items) {
				t.Fatalf("queue/readiness heartbeat membership drift: %+v %v", queue, err)
			}
			for i, bead := range queue.Items {
				if bead.ID != page.Items[i].ID {
					t.Fatal("queue changed complete heartbeat membership/order")
				}
			}
			t.Logf("CP7_PUBLIC_HEARTBEAT engine=%s durable_graph_unchanged=true quiet_replay=true ready_and_queue_agree=true graph_revision=%s", engine, after.Head)
		})
	}
}
