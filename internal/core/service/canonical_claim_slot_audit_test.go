package service

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAuditedPublicLifecycleMethodsKeepCanonicalSlots(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		method, before, after, slot string
		event                       model.DurableEventType
	}{
		{"bead.claim", "todo", "in_progress", "claim", model.EventBeadClaimed},
		{"bead.claimNext", "todo", "in_progress", "claim", model.EventBeadClaimed},
		{"bead.close", "in_progress", "done", "close", model.EventBeadClosed},
		{"bead.fail", "in_progress", "failed", "fail", model.EventBeadFailed},
		{"bead.cancel", "in_progress", "canceled", "cancel", model.EventBeadCanceled},
		{"bead.reopen", "done", "todo", "reopen", model.EventBeadReopened},
		{"bead.defer", "todo", "backlog", "defer", model.EventBeadDeferred},
		{"bead.update", "todo", "todo", "", model.EventBeadUpdated},
		{"bead.release", "todo", "todo", "", model.EventBeadReleased},
	} {
		t.Run(tc.method, func(t *testing.T) {
			source, compiled := auditSource(t, nil)
			sourceRef := model.Ref{ID: "ontology/canonical-claim-slot"}
			normalizer := NewOntology(compiled.Effective)
			f := setup(t, pipeline.WithOntology(normalizer), pipeline.WithOntologyAudit(NewOntologyAudit(StaticOntologySource(sourceRef))))
			f.svc = New(f.store, f.lease, f.writer, WithOntology(normalizer), WithClock(func() time.Time { return f.now }))
			body, err := MarshalOntologySource(source)
			if err != nil {
				t.Fatal(err)
			}
			id := model.Ref{ID: "pl-canonical-claim-slot"}
			// This happy-path fixture is created at the declared fixture clock;
			// an unknown legacy creation instant remains a ready refusal.
			bead, err := json.Marshal(pipeline.Document{ID: id.ID, Project: "local", Type: "issue", CreatedAt: &f.now, Fields: map[string]any{"title": "canonical claim slot", "status": tc.before}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.now}, []conformance.FixtureChange{
				{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body},
				{Ref: id, Kind: "bead", Op: model.OpCreate, Body: bead},
			}); err != nil {
				t.Fatal(err)
			}
			if tc.method == "bead.release" {
				if _, err := f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, meta(t)); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			m := meta(t)
			invoke := func() (port.WriteOutcome, error) {
				switch tc.method {
				case "bead.claim":
					return f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, m)
				case "bead.claimNext":
					out, err := f.svc.ClaimNextWithOutcome(ctx, anonymous, ClaimNextRequest{Project: m.Project}, m)
					if err != nil {
						return port.WriteOutcome{}, err
					}
					if out == nil {
						t.Fatal("claimNext returned no outcome")
					}
					return *out, nil
				case "bead.close":
					return f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id}, m)
				case "bead.fail":
					return f.svc.FailWithOutcome(ctx, anonymous, FailRequest{ID: id}, m)
				case "bead.cancel":
					return f.svc.CancelWithOutcome(ctx, anonymous, CancelRequest{ID: id}, m)
				case "bead.reopen":
					return f.svc.ReopenWithOutcome(ctx, anonymous, ReopenRequest{ID: id}, m)
				case "bead.defer":
					return f.svc.DeferWithOutcome(ctx, anonymous, DeferRequest{ID: id}, m)
				case "bead.update":
					return f.svc.UpdateWithOutcome(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "updated title"}}, m)
				case "bead.release":
					return f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id}, m)
				default:
					t.Fatalf("unhandled public method %q", tc.method)
					return port.WriteOutcome{}, nil
				}
			}
			out, err := invoke()
			if err != nil || out.Replayed || out.NullResult || len(out.Commit.Results) != 1 || !sameResolvedRef(out.Commit.Results[0].Ref, id) {
				t.Fatalf("public audited write=%+v err=%v", out, err)
			}
			after, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			oldHead, err := before.Head.Uint64()
			if err != nil || after.Head.String() != model.Natural64(oldHead+1).String() || out.Commit.Revision.String() != after.Head.String() {
				t.Fatalf("write did not advance exactly one durable revision: before=%+v after=%+v err=%v", before, after, err)
			}
			got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
			if err != nil || got.Bead.Status != tc.after || got.Bead.Revision.String() != after.Head.String() {
				t.Fatalf("committed bead=%+v err=%v", got, err)
			}
			if tc.slot == "claim" {
				if got.Lease == nil || !reflect.DeepEqual(got.Lease.Principal, anonymous) || got.Lease.Agent != m.Agent || got.Lease.Epoch.String() != model.Natural64(f.lease.Epoch()).String() || got.Lease.TTL != 5*time.Minute || !got.Lease.ExpiresAt.Equal(f.now.Add(5*time.Minute)) || out.Commit.LeaseExpiresAt == nil || !out.Commit.LeaseExpiresAt.Equal(got.Lease.ExpiresAt) {
					t.Fatalf("claim did not retain its actual durable lease: %+v", got.Lease)
				}
			} else if got.Lease != nil {
				t.Fatalf("non-claim retained a lease: %+v", got.Lease)
			}
			reader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			entries, err := reader.Changesets(ctx, nil, 20)
			if err != nil {
				t.Fatal(err)
			}
			var entry *model.Changeset
			for i := range entries {
				if entries[i].Revision.String() == out.Commit.Revision.String() {
					entry = &entries[i]
				}
			}
			if entry == nil || len(entry.Changes) != 1 || !sameResolvedRef(entry.Changes[0].Ref, id) || entry.Changes[0].Op != model.OpPatch || entry.Changes[0].FromRevision.String() != before.Head.String() || len(entry.Events) != 1 || entry.IdempotencyKey != m.IdempotencyKey || !reflect.DeepEqual(entry.Principal, anonymous) {
				t.Fatalf("incomplete durable audited write: %+v", entry)
			}
			event := entry.Events[0]
			if err := entry.ValidateEvents(); err != nil {
				t.Fatal(err)
			}
			if event.Type != tc.event || event.Index.String() != "0" || event.ChangeIndex.String() != "0" || event.Resource.Kind != "bead" || !sameResolvedRef(event.Resource.Ref, id) || event.Details != nil || event.Provenance.Method != tc.method || event.Provenance.Slot != tc.slot {
				t.Fatalf("public method/canonical slot/event target changed: %+v", event)
			}
			field := "fields.status"
			if tc.method == "bead.update" {
				field = "fields.title"
			} else if tc.method == "bead.release" {
				field = "lease"
			}
			if event.Provenance.SetBy[field] != "request" || event.Provenance.Cause.Root != m.IdempotencyKey || event.Provenance.Cause.Depth.String() != "0" || event.Provenance.Cause.Chain == nil || len(event.Provenance.Cause.Chain) != 0 || event.Provenance.Chain == nil || len(event.Provenance.Chain) != 0 || len(entry.Chain) != 0 {
				t.Fatalf("trusted provenance changed: %+v", event.Provenance)
			}
			principal, err := driver.PrincipalKey(anonymous)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, tc.method, m.IdempotencyKey)
			if err != nil || receipt == nil || receipt.ResponsePending || len(receipt.RequestHash) != 64 || !reflect.DeepEqual(receipt.Commit, out.Commit) || !reflect.DeepEqual(receipt.Changeset, *entry) {
				t.Fatalf("public-method-scoped finalized receipt=%+v err=%v", receipt, err)
			}
			otherMethod := "bead.claimNext"
			if tc.method == otherMethod {
				otherMethod = "bead.claim"
			}
			other, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, otherMethod, m.IdempotencyKey)
			if err != nil || other != nil {
				t.Fatalf("receipt leaked into method %q: %+v err=%v", otherMethod, other, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			replayed, err := invoke()
			if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, out.Commit) {
				t.Fatalf("exact public request did not replay its durable receipt: %+v err=%v", replayed, err)
			}
			replayedMeta, err := f.store.Meta(ctx)
			if err != nil || !reflect.DeepEqual(replayedMeta, after) {
				t.Fatalf("receipt replay changed store metadata: %+v err=%v", replayedMeta, err)
			}
			replayedReader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer replayedReader.Close()
			retained, err := replayedReader.IdempotencyByKey(ctx, principal, m.AgentLabel, tc.method, m.IdempotencyKey)
			if err != nil || !reflect.DeepEqual(retained, receipt) {
				t.Fatalf("replay changed the request hash, event plan or receipt: %+v err=%v", retained, err)
			}
		})
	}
}
