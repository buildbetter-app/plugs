package cardb20

import (
	"testing"
	"time"

	"encoding/json"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Binding17:19/17:24 names Plug's different-reason comment deviation. A stored
// first-reason identity is independent of comments and survives SQL reopen.
func TestB21PublicRepeatCloseReasonIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			c := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("a", "reason subject")}}))
			ref := c.Results[0].Ref
			b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: "unrelated before close"}}}}))
			f.now = f.now.Add(time.Hour)
			firstAt := f.now
			firstKey := f.key()
			firstReply := f.call("bead.close", firstKey, map[string]any{"id": ref, "reason": "original reason"})
			first := b21Commit(t, firstReply)
			if len(first.Results) != 2 {
				t.Fatal("named close retains its historical two-result lowering")
			}
			b21Reason(t, f.doc(ref), "original reason", firstAt, []string{"unrelated before close", "original reason"})
			f.publicFields(ref, map[string]any{"title": "reason subject", "status": "done"})
			f.now = f.now.Add(time.Hour)
			before := f.state()
			posts := f.posts.count()
			same := b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": ref, "reason": "original reason"}))
			b21Equal(t, before.Head, same.Revision, "named identical close no revision")
			b21Equal(t, before, f.state(), "named identical reason complete conservation")
			explicit := "done"
			identical := b21Close(ref)
			identical.Close.Reason, identical.Close.TargetStatus = "original reason", &explicit
			b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{identical, b21Close(ref)}}))
			b21Equal(t, before, f.state(), "typed identical and empty explicit/default no-op batch")
			if f.posts.count() != posts {
				t.Fatal("no-op close delivered post")
			}
			f.now = f.now.Add(time.Hour)
			different := b21Close(ref)
			different.Close.Reason = "different settlement reason"
			different.Close.TargetStatus = &explicit
			changed := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{different}}))
			b21Equal(t, b21Next(t, before.Head), changed.Revision, "different reason one revision")
			b21Reason(t, f.doc(ref), "original reason", firstAt, []string{"unrelated before close", "original reason", "different settlement reason"})
			after := f.state()
			f.now = f.now.Add(time.Hour)
			b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{identical}}))
			b21Equal(t, after, f.state(), "same first reason remains no-op after a different later comment")
			b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: "original reason"}}}}))
			b21Reason(t, f.doc(ref), "original reason", firstAt, []string{"unrelated before close", "original reason", "different settlement reason", "original reason"})
			if engine != "memory" {
				f.reopen()
				replayed := f.call("bead.close", firstKey, map[string]any{"id": ref, "reason": "original reason"})
				if !replayed.Meta.Replayed || string(replayed.Result) != string(firstReply.Result) {
					t.Fatal("SQL reopened named close lost stored two-result response bytes")
				}
				b21Reason(t, f.doc(ref), "original reason", firstAt, []string{"unrelated before close", "original reason", "different settlement reason", "original reason"})
				after = f.state()
				b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": ref, "reason": "original reason", "targetStatus": "done"}))
				b21Equal(t, after, f.state(), "reopened named identical reason no-op")
			}
			b21Commit(t, f.call("bead.reopen", f.key(), map[string]any{"id": ref}))
			if d := f.doc(ref); d.CloseReason != nil || d.ClosedAt != nil {
				t.Fatalf("reopen retained prior close identity %+v", d)
			}
			f.now = f.now.Add(time.Hour)
			nextAt := f.now
			b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Close(ref)}}))
			d := f.doc(ref)
			if d.CloseReason == nil || *d.CloseReason != "" || d.ClosedAt == nil || !d.ClosedAt.Equal(nextAt) {
				t.Fatalf("new cycle records exact empty reason/time %+v", d)
			}
			emptyState := f.state()
			f.now = f.now.Add(time.Hour)
			b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": ref, "reason": ""}))
			b21Equal(t, emptyState, f.state(), "known empty first reason repeat no-op")
			if engine != "memory" {
				f.reopen()
				d = f.doc(ref)
				if d.CloseReason == nil || *d.CloseReason != "" || !d.ClosedAt.Equal(nextAt) {
					t.Fatal("known empty identity lost on SQL reopen")
				}
			}
			b21Retain(t, engine+"-first-close-reason", f.doc(ref))
		})
	}
}

func b21Reason(t *testing.T, d pipeline.Document, reason string, at time.Time, comments []string) {
	t.Helper()
	if d.CloseReason == nil || *d.CloseReason != reason || d.ClosedAt == nil || !d.ClosedAt.Equal(at) || len(d.Comments) != len(comments) {
		t.Fatalf("exact stored first reason/time/comments %+v", d)
	}
	for i, body := range comments {
		if d.Comments[i].Body != body {
			t.Fatalf("comment index%d %+v", i, d.Comments)
		}
	}
}

func TestB21PublicLegacyClosedReasonDoesNotInferOrBackfill(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			ref := model.RefByID("b21-legacy-unknown", model.Natural64(0))
			at := f.now.Add(-time.Hour)
			d := pipeline.Document{ID: ref.ID, Type: "issue", Project: b21Project, Fields: map[string]any{"title": "legacy", "status": "done"}, ClosedAt: &at, Comments: []pipeline.Comment{{Body: "arbitrary old comment", Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Time: at}}}
			body, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
				t.Fatal(err)
			}
			state := f.state()
			b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Close(ref)}}))
			b21Equal(t, state, f.state(), "legacy empty reason pure no-op without backfill")
			f.now = f.now.Add(time.Hour)
			repeat := b21Close(ref)
			repeat.Close.Reason = "arbitrary old comment"
			c := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{repeat}}))
			b21Equal(t, b21Next(t, state.Head), c.Revision, "legacy nonempty reason comment revision")
			got := f.doc(ref)
			if got.CloseReason != nil || len(got.Comments) != 2 || got.Comments[1].Body != "arbitrary old comment" || !got.ClosedAt.Equal(at) {
				t.Fatalf("legacy inferred or rewrote identity %+v", got)
			}
			if engine != "memory" {
				f.reopen()
				got = f.doc(ref)
				if got.CloseReason != nil || len(got.Comments) != 2 || !got.ClosedAt.Equal(at) {
					t.Fatal("legacy reopen invented identity")
				}
			}
		})
	}
}

// Exact terminal selection still changes the selected status and evaluates its
// guards. It must not fabricate legacy unknown reason/time or duplicate a known
// first reason merely because the successful terminal key differs.
func TestB21PublicExactTerminalCloseKeepsFirstIdentityAndLegacyAbsence(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			document := ontology.DefaultDocument()
			document.Statuses["done-alt"] = ontology.Status{Name: "Alternate completed", Category: ontology.Done, Outcome: ontology.Success}
			flow := document.Workflows["issue"]
			flow.Statuses = append(flow.Statuses, "done-alt")
			flow.Transitions = append(flow.Transitions, ontology.Rule{To: "done-alt", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}})
			document.Workflows["issue"] = flow
			f := newB21Ontology(t, engine, document)
			// Admit all legacy fixtures before ordinary public writes enable the
			// driver's certified-publication metadata boundary.
			for _, prior := range []struct{ id, status string }{
				{"b21-legacy-failed", "failed"}, {"b21-legacy-canceled", "canceled"},
				{"b21-legacy-no-time-typed", "done"}, {"b21-legacy-no-time-named", "done"},
			} {
				legacy := model.RefByID(prior.id, model.Natural64(0))
				d := pipeline.Document{ID: prior.id, Type: "issue", Project: b21Project, Fields: map[string]any{"title": "legacy " + prior.status, "status": prior.status}}
				body, err := json.Marshal(d)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: legacy, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
					t.Fatal(err)
				}
			}
			ref := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("known", "known first reason")}})).Results[0].Ref
			firstAt := f.now
			b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": ref, "reason": "first reason"}))
			f.now = f.now.Add(time.Hour)
			alternate := "done-alt"
			op := b21Close(ref)
			op.Close.TargetStatus, op.Close.Reason = &alternate, "first reason"
			before := f.state()
			changed := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{op}}))
			b21Equal(t, b21Next(t, before.Head), changed.Revision, "exact different terminal still selects status")
			b21Reason(t, f.doc(ref), "first reason", firstAt, []string{"first reason"})
			f.publicFields(ref, map[string]any{"status": "done-alt", "title": "known first reason"})
			f.now = f.now.Add(time.Hour)
			b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": ref, "targetStatus": "done", "reason": "first reason"}))
			b21Reason(t, f.doc(ref), "first reason", firstAt, []string{"first reason"})

			// A legacy failure/cancellation without any prior close identity/time is
			// not a successful close. Preserve the existing genuine-close path.
			for _, previous := range []string{"failed", "canceled"} {
				id := "b21-legacy-" + previous
				legacy := model.RefByID(id, model.Natural64(0))
				f.now = f.now.Add(time.Hour)
				close := b21Close(legacy)
				close.Close.Reason = "actual successful close"
				b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{close}}))
				b21Reason(t, f.doc(legacy), "actual successful close", f.now, []string{"actual successful close"})
			}

			// Exercise real failed/canceled public lifecycle rows, which carry
			// populated terminal times, before their first successful close.
			for _, method := range []string{"bead.fail", "bead.cancel"} {
				row := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("subject", method)}})).Results[0].Ref
				f.now = f.now.Add(time.Hour)
				terminalAt := f.now
				b21Commit(t, f.call(method, f.key(), map[string]any{"id": row, "reason": "earlier terminal reason"}))
				if got := f.doc(row); got.ClosedAt == nil || !got.ClosedAt.Equal(terminalAt) || got.CloseReason != nil {
					t.Fatalf("public failure/cancellation fixture %+v", got)
				}
				f.now = f.now.Add(time.Hour)
				b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": row, "reason": "first successful reason"}))
				b21Reason(t, f.doc(row), "first successful reason", f.now, []string{"earlier terminal reason", "first successful reason"})
			}

			for _, named := range []bool{false, true} {
				id := "b21-legacy-no-time-typed"
				if named {
					id = "b21-legacy-no-time-named"
				}
				legacy := model.RefByID(id, model.Natural64(0))
				f.now = f.now.Add(time.Hour)
				before = f.state()
				if named {
					changed = b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": legacy, "targetStatus": alternate, "reason": "legacy settlement"}))
				} else {
					op := b21Close(legacy)
					op.Close.TargetStatus, op.Close.Reason = &alternate, "legacy settlement"
					changed = b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{op}}))
				}
				b21Equal(t, b21Next(t, before.Head), changed.Revision, "legacy settlement is one revision")
				assertLegacy := func(status string, comments int) {
					t.Helper()
					got := f.doc(legacy)
					if got.CloseReason != nil || got.ClosedAt != nil || got.Fields["status"] != status || len(got.Comments) != comments || comments > 0 && got.Comments[0].Body != "legacy settlement" {
						t.Fatalf("legacy terminal selection fabricated identity/time: %+v", got)
					}
					f.publicFields(legacy, map[string]any{"status": status, "title": "legacy done"})
				}
				assertLegacy(alternate, 1)
				f.now = f.now.Add(time.Hour)
				b21Commit(t, f.call("bead.close", f.key(), map[string]any{"id": legacy, "targetStatus": "done"}))
				assertLegacy("done", 1)
				state := f.state()
				b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Close(legacy)}}))
				b21Equal(t, state, f.state(), "legacy same target empty remains no-op")
				if engine != "memory" {
					f.reopen()
					assertLegacy("done", 1)
				}
			}
		})
	}
}
