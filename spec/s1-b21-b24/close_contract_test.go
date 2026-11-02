package s1b21b24

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func closeOp(target model.Ref, reason string, expect ...model.Precondition) map[string]any {
	op := map[string]any{"kind": "close", "target": target, "reason": reason}
	if len(expect) != 0 {
		op["expect"] = expect
	}
	return op
}
func batchParams(ops ...any) map[string]any {
	return map[string]any{"changeSet": map[string]any{"ops": ops}}
}
func closeDocument(t *testing.T, state storedState, ref model.Ref) pipeline.Document {
	t.Helper()
	for _, row := range state.Records {
		if row.Ref.ID == ref.ID {
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			return doc
		}
	}
	t.Fatalf("missing close target %s", ref.ID)
	return pipeline.Document{}
}
func closeRefusal(t *testing.T, f *fixture, label string, params map[string]any, want string) {
	t.Helper()
	before := f.state(t)
	got := f.call("tx.apply", label, params)
	if got.Error == nil || got.Error.Data.Code != want || len(got.Result) != 0 || got.Meta.Replayed {
		t.Fatalf("atomic close refusal=%+v want=%s", got, want)
	}
	sameState(t, before, f.state(t))
	if f.casReceipt(t, "tx.apply", label) != nil {
		t.Fatal("refused batch stored a receipt")
	}
}

func TestS1B21AtomicCloseCreatePatchReasonAndReopen(t *testing.T) {
	for _, engine := range engines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			anchor, _ := f.create(t, "close-anchor", "anchor")
			params := batchParams(
				map[string]any{"kind": "create", "key": "new", "type": "issue", "fields": map[string]any{"title": "new"}},
				map[string]any{"kind": "patch", "target": map[string]any{"key": "new"}, "fields": map[string]any{"title": "updated in request"}},
				closeOp(model.RefByKey("new"), "created then closed", model.PreconditionByHash(model.RefByKey("new"), "fields.title", casHash(`"updated in request"`))),
				closeOp(anchor, "existing close", model.PreconditionByRevision(anchor, model.Natural64(1))),
				map[string]any{"kind": "patch", "target": anchor, "fields": map[string]any{"description": "after close"}},
			)
			original, _ := json.Marshal(params)
			request := f.request("tx.apply", "close-atomic", params)
			first := f.dispatch(request)
			c := commit(t, first)
			afterParams, _ := json.Marshal(params)
			if !bytes.Equal(original, afterParams) {
				t.Fatal("batch modified caller request")
			}
			if c.Revision.String() != "2" || len(c.Results) != 5 {
				t.Fatalf("one close revision/results: %+v", c)
			}
			for i, r := range c.Results {
				if r.Revision != c.Revision {
					t.Fatalf("result %d got revision=%s", i, r.Revision)
				}
			}
			created := c.Results[0].Ref
			if c.Results[1].Ref.ID != created.ID || c.Results[2].Ref.ID != created.ID || c.Results[3].Ref.ID != anchor.ID || c.Results[4].Ref.ID != anchor.ID {
				t.Fatal("close operation result order changed")
			}
			after := f.state(t)
			a, b := closeDocument(t, after, anchor), closeDocument(t, after, created)
			if a.Fields["status"] != "done" || b.Fields["status"] != "done" || b.Fields["title"] != "updated in request" || a.Fields["description"] != "after close" {
				t.Fatalf("wrong ordered images: %+v %+v", a, b)
			}
			if len(a.Comments) != 1 || a.Comments[0].Body != "existing close" || len(b.Comments) != 1 || b.Comments[0].Body != "created then closed" {
				t.Fatalf("close reason lost/duplicated: %+v %+v", a.Comments, b.Comments)
			}
			if len(after.Changesets) != 2 || len(after.Changesets[1].Changes) != 2 || len(after.Versions[anchor.ID]) != 2 || len(after.Versions[created.ID]) != 1 {
				t.Fatalf("close batch was not one durable history entry: %+v", after)
			}
			found := false
			for _, e := range after.Changesets[1].Events {
				if e.Resource.Ref.ID == anchor.ID {
					found = true
					if e.Type != model.EventBeadClosed || e.Provenance.Method != "tx.apply" || e.Provenance.Slot != "close" {
						t.Fatalf("per-op lifecycle provenance: %+v", e)
					}
				}
			}
			if !found {
				t.Fatal("close batch omitted durable close event")
			}
			replay := f.dispatch(request)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("close replay: %+v", replay)
			}
			sameState(t, after, f.state(t))
			f.reopen(t)
			replay = f.dispatch(request)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("close reopen replay: %+v", replay)
			}
			sameState(t, after, f.state(t))
			t.Logf("B21 atomic close: %s public create/patch/close/patch, aliases, prefix hash, reason, one history/revision and stored reopen replay", engine)
		})
	}
}

func TestS1B21CloseGuardsUseOperationPrefix(t *testing.T) {
	for _, engine := range engines() {
		for _, order := range []string{"close-before-unlink", "unlink-before-close", "close-blocker-first"} {
			t.Run(engine+"/"+order, func(t *testing.T) {
				f := newFixture(t, engine)
				blocker, _ := f.create(t, "close-blocker", "blocker")
				blocked, _ := f.create(t, "close-blocked", "blocked")
				commit(t, f.call("bead.link", "close-edge", map[string]any{"from": blocked, "to": blocker, "kind": "blocks"}))
				unlink := map[string]any{"kind": "unlink", "from": blocked, "to": blocker, "linkKind": "blocks"}
				if order == "close-before-unlink" {
					closeRefusal(t, f, "prefix-refused", batchParams(closeOp(blocked, "refused"), unlink), "ontology_guard_failed")
					return
				}
				var params map[string]any
				if order == "unlink-before-close" {
					params = batchParams(unlink, closeOp(blocked, "unblocked"))
				} else {
					params = batchParams(closeOp(blocker, "first"), closeOp(blocked, "second"))
				}
				c := commit(t, f.call("tx.apply", "prefix-accepted", params))
				if c.Revision.String() != "4" || len(c.Results) != 2 || closeDocument(t, f.state(t), blocked).Fields["status"] != "done" {
					t.Fatalf("prefix close not committed atomically: %+v", c)
				}
				t.Logf("B21 close prefix guard: %s/%s", engine, order)
			})
		}
	}
}

func TestS1B21CloseLateRefusalsLeaveNoHistoryOrReceipt(t *testing.T) {
	for _, engine := range engines() {
		for _, kind := range []string{"template", "hash", "revision-after-write", "status-patch", "forward-alias", "unknown-force"} {
			t.Run(engine+"/"+kind, func(t *testing.T) {
				f := newFixture(t, engine)
				target, _ := f.create(t, "refused-target", "target")
				var ops []any
				want := "invalid_params"
				switch kind {
				case "template":
					commit(t, f.call("bead.update", "make-template", map[string]any{"id": target, "fields": map[string]any{"template": true}}))
					ops = []any{map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"title": "must rollback"}}, closeOp(target, "must not append")}
					want = "ontology_guard_failed"
				case "hash":
					ops = []any{map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"title": "prefix"}}, closeOp(target, "must not append", model.PreconditionByHash(target, "fields.title", casHash(`"target"`)))}
					want = "precondition_failed"
				case "revision-after-write":
					ops = []any{map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"title": "prefix"}}, closeOp(target, "must not append", model.PreconditionByRevision(target, model.Natural64(1)))}
					want = "precondition_failed"
				case "status-patch":
					ops = []any{closeOp(target, "must rollback"), map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"status": "done"}}}
					want = "ontology_use_slot_verb"
				case "forward-alias":
					ops = []any{closeOp(model.RefByKey("later"), ""), map[string]any{"kind": "create", "key": "later", "type": "issue", "fields": map[string]any{"title": "later"}}}
				case "unknown-force":
					ops = []any{map[string]any{"kind": "close", "target": target, "force": true}}
				}
				closeRefusal(t, f, "refused-"+kind, batchParams(ops...), want)
			})
		}
	}
}

func TestS1B21RepeatedCloseNoopAndLeaseSettlement(t *testing.T) {
	for _, engine := range engines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			target, _ := f.create(t, "claim-target", "target")
			commit(t, f.call("bead.claim", "close-claim", map[string]any{"id": target}))
			c := commit(t, f.call("tx.apply", "close-claimed", batchParams(closeOp(target, ""))))
			if c.Revision.String() != "3" || closeDocument(t, f.state(t), target).Lease != nil {
				t.Fatal("atomic close did not clear the existing claim")
			}
			before := f.state(t)
			first := f.call("tx.apply", "close-noop", batchParams(closeOp(target, ""), closeOp(target, "")))
			noop := commit(t, first)
			if noop.Revision.String() != "3" || len(noop.Results) != 2 {
				t.Fatalf("repeat close is not a no-op: %+v", noop)
			}
			sameState(t, before, f.state(t))
			f.reopen(t)
			replay := f.call("tx.apply", "close-noop", batchParams(closeOp(target, ""), closeOp(target, "")))
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatalf("no-op close receipt changed on reopen: %+v", replay)
			}
			sameState(t, before, f.state(t))
		})
	}
}
