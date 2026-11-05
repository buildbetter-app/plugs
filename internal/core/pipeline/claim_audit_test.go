package pipeline

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type claimAuditOntology struct{ auditSlotOntology }

func (o claimAuditOntology) Normalize(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	if p.Meta.Method == "bead.claimNext" {
		p.Meta.Method = "bead.claim"
	}
	return o.auditSlotOntology.Normalize(ctx, p, r)
}

func TestClaimAuditDoesNotRestartForUnrelatedPublication(t *testing.T) {
	for _, method := range []string{"bead.claim", "bead.claimNext"} {
		t.Run(method, func(t *testing.T) {
			h, blocker, dependents := auditFixture(t, 1, "blocker-project")
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			w := auditWriter(h, WithOntology(claimAuditOntology{}), WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
				decision, err := fixtureAudit(ctx, c, r)
				if calls.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return port.ConditionalDecision{}, ctx.Err()
					}
				}
				return decision, err
			})))
			cs, m := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, slotMeta(740, method)
			result := make(chan applyResult, 1)
			go func() {
				commit, err := w.Apply(context.Background(), by, cs, m)
				result <- applyResult{commit, err}
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("claim did not reach its trusted conditional decision")
			}
			// This changes the global graph revision after the claim's snapshot.
			// The dependent's incoming edge stays present and its title changes.
			_, unrelatedErr := h.writer().Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(dependents[0], map[string]any{"title": "concurrent"})}}, meta(741))
			close(release)
			if unrelatedErr != nil {
				t.Fatal(unrelatedErr)
			}
			first := awaitResult(t, result)
			if first.err != nil || first.commit.Revision.String() != "3" || calls.Load() != 1 {
				t.Fatalf("unrelated publication restarted or failed claim: result=%+v decisions=%d", first, calls.Load())
			}
			entry := auditEntry(t, h, first.commit.Revision)
			if len(entry.Changes) != 1 || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadClaimed || entry.Events[0].Resource.Ref.ID != blocker.ID || entry.Events[0].Provenance.Method != method {
				t.Fatalf("claim lost its sole ordinary event: %+v", entry)
			}
			r := h.read(t)
			versions, versionErr := r.Versions(context.Background(), blocker, driver.PageReq{})
			rows, readErr := r.Get(context.Background(), dependents)
			if versionErr != nil || len(versions.Items) != 2 || readErr != nil || len(rows) != 1 {
				t.Fatalf("claim/dependent conservation: versions=%+v rows=%+v errors=%v/%v", versions, rows, versionErr, readErr)
			}
			doc, err := DecodeDocument(rows[0].Body)
			if err != nil || doc.Fields["title"] != "concurrent" || len(doc.Links) != 1 || doc.Links[0].To.ID != blocker.ID {
				t.Fatalf("claim changed the dependent: doc=%+v err=%v", doc, err)
			}
			again, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
			stored := receipt(t, h, m)
			if err != nil || !again.Replayed || !reflect.DeepEqual(first.commit, again.Commit) || stored.ResponsePending || !reflect.DeepEqual(stored.Commit, first.commit) || calls.Load() != 1 || h.read(t).Snapshot().At.String() != "3" {
				t.Fatalf("claim replay changed publication: outcome=%+v err=%v decisions=%d", again, err, calls.Load())
			}
		})
	}
}

func TestClaimAuditInvalidDecisionRetainsDependentGuard(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalid-pin", true: "hook-error"}[broken], func(t *testing.T) {
			h, blocker, _ := auditFixture(t, 1, "blocker-project")
			var calls atomic.Int32
			injected := errors.New("conditional decision unavailable")
			w := auditWriter(h, WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
				calls.Add(1)
				if broken {
					return port.ConditionalDecision{}, injected
				}
				decision, err := fixtureAudit(ctx, c, r)
				decision.Ontology.Revision = model.Natural64(0)
				return decision, err
			})))
			m := slotMeta(742, "bead.claim")
			_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, m)
			if broken {
				if !errors.Is(err, injected) {
					t.Fatalf("claim hid conditional hook failure: %v", err)
				}
			} else {
				code(t, err, "invalid_params")
			}
			if calls.Load() != 1 {
				t.Fatalf("claim repeated its conditional hook: %d", calls.Load())
			}
			assertAuditUnpublished(t, h, blocker, model.Natural64(1), m)
		})
	}
}
