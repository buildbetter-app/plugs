package cardb20

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Every write enters HTTP -> registry -> Service -> the actual Writer. No
// importer, protected fixture, concurrent request or alternate writer is used.
func TestB21PublicBatchAcceptance(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			t.Run("declaration-order-backward-keys-create-update-close", func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				before := f.state()
				key := f.key()
				cs := model.ChangeSet{Ops: []model.Op{
					b21Create("first", "first created"),
					b21Create("second", "second created"),
					b21Patch(model.RefByKey("first"), map[string]any{"title": "first patched", "description": "preserved verbatim Ω"}),
					b21Patch(model.RefByKey("second"), map[string]any{"title": "second patched"}),
					b21Close(model.RefByKey("first")),
				}}
				reply := f.apply(key, cs)
				c := b21Commit(t, reply)
				if reply.Meta.Replayed || len(c.Results) != 5 || c.Revision != b21Next(t, before.Head) {
					t.Fatalf("ordered batch %+v replay=%v", c, reply.Meta.Replayed)
				}
				first, second := c.Results[0].Ref, c.Results[1].Ref
				if !first.IsID() || !second.IsID() || first == second {
					t.Fatalf("distinct minted key bindings %+v", c.Results)
				}
				for i, want := range []model.Ref{first, second, first, second, first} {
					b21Equal(t, want, c.Results[i].Ref, fmt.Sprintf("result index %d", i))
					b21Equal(t, c.Revision, c.Results[i].Revision, "single revision per indexed result")
				}
				a, b := f.doc(first), f.doc(second)
				if a.Fields["title"] != "first patched" || a.Fields["description"] != "preserved verbatim Ω" || a.Fields["status"] != "done" || b.Fields["title"] != "second patched" || b.Fields["status"] != "todo" {
					t.Fatalf("independent exact final images first=%+v second=%+v", a, b)
				}
				f.publicFields(first, map[string]any{"title": "first patched", "description": "preserved verbatim Ω", "status": "done"})
				f.publicFields(second, map[string]any{"title": "second patched", "status": "todo"})
				after := f.state()
				if len(after.Log) != len(before.Log)+1 || len(after.Log[len(after.Log)-1].Changes) != 2 || len(after.Versions[first.ID]) != 1 || len(after.Versions[second.ID]) != 1 {
					t.Fatalf("one delta revision, one version per row %+v", after)
				}
				receipt := f.receipt(key)
				if receipt == nil || receipt.ResponsePending {
					t.Fatal("missing complete receipt")
				}
				b21Equal(t, c, receipt.Commit, "stored exact commit")
				calls := f.posts.count()
				state := f.state()
				replay := f.apply(key, cs)
				b21Equal(t, c, b21Commit(t, replay), "exact public replay commit")
				if !bytes.Equal(reply.Result, replay.Result) || !replay.Meta.Replayed || f.posts.count() != calls {
					t.Fatal("replay must not call post subscriber")
				}
				b21Equal(t, state, f.state(), "replay durable conservation")
				if engine != "memory" {
					f.reopen()
					b21Equal(t, c, f.receipt(key).Commit, "reopened receipt")
					replay = f.apply(key, cs)
					b21Equal(t, c, b21Commit(t, replay), "reopened replay")
					if !bytes.Equal(reply.Result, replay.Result) || !replay.Meta.Replayed || f.posts.count() != calls {
						t.Fatal("SQL replay must not fire post")
					}
					b21Equal(t, state, f.state(), "reopened replay state")
					if f.doc(first).Fields["status"] != "done" {
						t.Fatal("closed value lost on SQL reopen")
					}
					f.publicFields(first, map[string]any{"title": "first patched", "description": "preserved verbatim Ω", "status": "done"})
				}
				b21Retain(t, engine+"-ordered-commit", map[string]any{"commit": c, "request": cs, "first": a, "second": b, "state": state, "post_calls": calls})
			})
			t.Run("typed-later-key-and-final-item-refusal-conserve-all", func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				seeded := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("anchor", "unchanged anchor")}})).Results[0].Ref
				cases := []struct {
					name string
					ops  []model.Op
					code model.ErrorCode
				}{
					{"declared-later", []model.Op{b21Patch(seeded, map[string]any{"title": "must roll back"}), b21Close(model.RefByKey("later")), b21Create("later", "not published")}, "invalid_params"},
					{"unknown-key", []model.Op{b21Create("first", "must roll back"), b21Close(model.RefByKey("absent"))}, "invalid_params"},
					{"final-item-not-found", []model.Op{b21Create("first", "must roll back"), b21Patch(seeded, map[string]any{"description": "must roll back"}), b21Close(model.RefByID("b21-no-such-row", model.Natural64(0)))}, "not_found"},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						prior := f.t
						f.t = t
						defer func() { f.t = prior }()
						before := f.state()
						posts := f.posts.count()
						key := f.key()
						r := f.apply(key, model.ChangeSet{Ops: tc.ops})
						b21Code(t, r, tc.code)
						b21Equal(t, before, f.state(), "refused batch complete durable state")
						if f.receipt(key) != nil || f.posts.count() != posts {
							t.Fatal("refused batch published receipt/post")
						}
						if f.doc(seeded).Fields["title"] != "unchanged anchor" {
							t.Fatal("failed batch changed exact anchor")
						}
						f.publicFields(seeded, map[string]any{"title": "unchanged anchor", "status": "todo"})
						if engine != "memory" {
							f.reopen()
							b21Equal(t, before, f.state(), "refusal persisted across SQL reopen")
							if f.receipt(key) != nil {
								t.Fatal("refused receipt survived reopen")
							}
						}
					})
				}
			})
			t.Run("1000-bound-and-1001-refusal", func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				c := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("bound", "bound start")}}))
				ref := c.Results[0].Ref
				ops := make([]model.Op, 1000)
				for i := range ops {
					ops[i] = b21Patch(ref, map[string]any{"title": fmt.Sprintf("position %04d", i)})
				}
				before := f.state()
				c = b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: ops}))
				if len(c.Results) != 1000 || c.Revision != b21Next(t, before.Head) {
					t.Fatalf("1000 boundary commit %+v", c)
				}
				for i, result := range c.Results {
					if result.Ref != ref || result.Revision != c.Revision {
						t.Fatalf("boundary result index%d %+v", i, result)
					}
				}
				if f.doc(ref).Fields["title"] != "position 0999" {
					t.Fatal("1000 operations not applied in declaration order")
				}
				state := f.state()
				ops = append(ops, b21Patch(ref, map[string]any{"title": "refused 1001"}))
				key := f.key()
				b21Code(t, f.call("tx.apply", key, map[string]any{"changeSet": map[string]any{"ops": ops}}), "invalid_params")
				b21Equal(t, state, f.state(), "1001 no partial state")
				if f.receipt(key) != nil {
					t.Fatal("1001 receipt")
				}
				if engine != "memory" {
					f.reopen()
					b21Equal(t, state, f.state(), "boundary SQL reopen")
					f.publicFields(ref, map[string]any{"title": "position 0999", "status": "todo"})
				}
			})
			t.Run("row-delta-and-noop-history-cardinality", func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				c := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("waiter", "waiter"), b21Create("spawner", "spawner"), b21Link(model.RefByKey("waiter"), model.RefByKey("spawner"), "all-children")}}))
				waiter, spawner := c.Results[0].Ref, c.Results[1].Ref
				before := f.state()
				posts := f.posts.count()
				c = b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Link(waiter, spawner, "any-children")}}))
				after := f.state()
				if c.Revision != b21Next(t, before.Head) || len(after.Log) != len(before.Log)+1 || len(after.Versions[waiter.ID]) != len(before.Versions[waiter.ID])+1 || f.posts.count() != posts+1 {
					t.Fatal("real metadata row delta must mint exactly one durable revision/version/post")
				}
				links := f.doc(waiter).Links
				if len(links) != 1 || links[0].Metadata == nil || links[0].Metadata.Gate != "any-children" {
					t.Fatalf("exact rewritten row metadata %+v", links)
				}
				// Plug OpResult has no Changed field. Its no-op oracle is the actual row
				// delta; native's Changed=false counterexample is tested on its own wire.
				posts = f.posts.count()
				key := f.key()
				noop := model.ChangeSet{Ops: []model.Op{b21Link(waiter, spawner, "any-children"), b21Patch(waiter, map[string]any{"title": "waiter"})}}
				c = b21Commit(t, f.apply(key, noop))
				if c.Revision != after.Head {
					t.Fatalf("no-op revision=%s head=%s", c.Revision, after.Head)
				}
				b21Equal(t, after, f.state(), "no-op has no durable history/version")
				if f.posts.count() != posts {
					t.Fatal("no-op post")
				}
				if engine != "memory" {
					f.reopen()
					b21Equal(t, after, f.state(), "metadata/noop SQL reopen")
					links = f.doc(waiter).Links
					if len(links) != 1 || links[0].Metadata.Gate != "any-children" {
						t.Fatal("metadata rewrite lost on reopen")
					}
					f.publicFields(waiter, map[string]any{"title": "waiter", "status": "todo"})
				}
			})
			t.Run("indexed-close-results-and-idempotent-close-no-history", func(t *testing.T) {
				prior := f.t
				f.t = t
				defer func() { f.t = prior }()
				c := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("a", "close A"), b21Create("b", "close B")}}))
				a, b := c.Results[0].Ref, c.Results[1].Ref
				before := f.state()
				postsBeforeRefusal := f.posts.count()
				refusedKey := f.key()
				refused := model.ChangeSet{Ops: []model.Op{b21Close(b), b21Close(model.RefByID("b21-missing-close", model.Natural64(0))), b21Close(a), b21Close(b)}}
				b21Code(t, f.apply(refusedKey, refused), "not_found")
				b21Equal(t, before, f.state(), "Plug atomic close refusal has no native survivor commit")
				if f.receipt(refusedKey) != nil || f.posts.count() != postsBeforeRefusal {
					t.Fatal("refused indexed close published receipt/post")
				}
				if engine != "memory" {
					f.reopen()
					b21Equal(t, before, f.state(), "atomic close refusal SQL reopen")
				}
				firstA, firstB := b21Close(a), b21Close(b)
				firstA.Close.Reason, firstB.Close.Reason = "first A", "first B"
				duplicateB := b21Close(b)
				duplicateB.Close.Reason = "first B"
				c = b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{firstB, firstA, duplicateB}}))
				if len(c.Results) != 3 || c.Revision != b21Next(t, before.Head) {
					t.Fatal("close commit shape")
				}
				for i, want := range []model.Ref{b, a, b} {
					b21Equal(t, want, c.Results[i].Ref, "close result index")
				}
				if f.doc(a).Fields["status"] != "done" || f.doc(b).Fields["status"] != "done" {
					t.Fatal("exact close readback")
				}
				for _, expected := range []struct {
					ref    model.Ref
					reason string
				}{{a, "first A"}, {b, "first B"}} {
					comments := f.doc(expected.ref).Comments
					if len(comments) != 1 || comments[0].Body != expected.reason {
						t.Fatalf("first close reason must survive duplicate: %+v", comments)
					}
				}
				after := f.state()
				posts := f.posts.count()
				repeatA, repeatB := b21Close(a), b21Close(b)
				repeatA.Close.Reason, repeatB.Close.Reason = "first A", "first B"
				c = b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{repeatA, repeatB}}))
				b21Equal(t, after.Head, c.Revision, "all-idempotent close revision")
				b21Equal(t, after, f.state(), "all-idempotent close no history")
				if f.posts.count() != posts {
					t.Fatal("all-idempotent close emitted post")
				}
				if engine != "memory" {
					f.reopen()
					b21Equal(t, after, f.state(), "close SQL reopen")
					f.publicFields(a, map[string]any{"title": "close A", "status": "done"})
					f.publicFields(b, map[string]any{"title": "close B", "status": "done"})
				}
			})
		})
	}
}
func mustB21JSON(value any) string { b, _ := json.Marshal(value); return string(b) }

func b21Next(t *testing.T, revision model.Revision) model.Revision {
	t.Helper()
	n, e := revision.Uint64()
	if e != nil || n == ^uint64(0) {
		t.Fatal("invalid finite test revision", e)
	}
	return model.Natural64(n + 1)
}
