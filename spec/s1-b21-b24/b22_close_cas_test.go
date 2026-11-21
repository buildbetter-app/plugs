package s1b21b24

import (
	"bytes"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// TestS1B22CloseRefusesStaleExpectedRevision is the B22 line item carried
// from B08: a close that names an out-of-date expected revision is refused
// with the typed CAS outcome and publishes nothing. The concurrent close/close
// and update/close forms are TestS1B22LifecycleCASRacesBindWinnerEventsAndReopen;
// this case pins the sequential form on both public close surfaces, the
// refused-key reuse, and the card edge that the expectation is checked before
// an otherwise repeated close.
func TestS1B22CloseRefusesStaleExpectedRevision(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newFixture(t, engine)
				target, created := f.create(t, "b22-close-stale-seed", "close guard")
				moved := commit(t, f.call("bead.update", "b22-close-stale-move", map[string]any{
					"id": target, "fields": map[string]any{"description": "moved past the caller's revision"},
				}))
				if created.Revision.String() != "1" || moved.Revision.String() != "2" {
					t.Fatalf("B22 stale-close seed revisions = %s, %s; want 1, 2", created.Revision, moved.Revision)
				}
				params := func(expected model.Natural, reason string) map[string]any {
					expect := model.PreconditionByRevision(target, expected)
					if method == "tx.apply" {
						return batchParams(closeOp(target, reason, expect))
					}
					return map[string]any{"id": target, "reason": reason, "expect": []model.Precondition{expect}}
				}

				before := f.state(t)
				if doc := closeDocument(t, before, target); doc.Fields["status"] != "todo" || doc.ClosedAt != nil || len(doc.Comments) != 0 {
					t.Fatalf("B22 stale-close target is not open before the refusal: %+v", doc)
				}
				for _, reopen := range []bool{false, true} {
					if reopen {
						f.reopen(t)
					}
					stale := f.call(method, "b22-close-stale", params(created.Revision, "stale close"))
					b22CASRefusal(t, stale, target, "revision", "2")
					sameState(t, before, f.state(t))
					if f.casReceipt(t, method, "b22-close-stale") != nil {
						t.Fatalf("B22 stale close stored a receipt (reopen=%v)", reopen)
					}
				}

				current := f.call(method, "b22-close-current", params(moved.Revision, "current close"))
				closed := commit(t, current)
				if current.Meta.Replayed || closed.Revision.String() != "3" {
					t.Fatalf("B22 current-revision close did not commit revision 3: %+v", closed)
				}
				after := f.state(t)
				doc := closeDocument(t, after, target)
				if doc.Fields["status"] != "done" || doc.ClosedAt == nil || len(doc.Comments) != 1 || doc.Comments[0].Body != "current close" {
					t.Fatalf("B22 current-revision close image differs: %+v", doc)
				}
				if len(after.Changesets) != 3 || len(after.Versions[target.ID]) != 3 || f.casReceipt(t, method, "b22-close-current") == nil {
					t.Fatal("B22 current-revision close did not publish exactly one changeset, version and receipt")
				}

				repeat := f.call(method, "b22-close-repeat", params(moved.Revision, "repeat close"))
				b22CASRefusal(t, repeat, target, "revision", "3")
				sameState(t, after, f.state(t))
				if f.casReceipt(t, method, "b22-close-repeat") != nil {
					t.Fatal("B22 stale repeat close stored a receipt")
				}

				f.reopen(t)
				replay := f.call(method, "b22-close-current", params(moved.Revision, "current close"))
				if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, current.Result) {
					t.Fatalf("B22 current-revision close replay differs after reopen: %+v", replay)
				}
				sameState(t, after, f.state(t))
			})
		}
	}
}
