package extended

import (
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func TestB06ConcurrentOppositeBlockingEdgesHaveOneWinner(t *testing.T) {
	f := newFixture(t, []string{"pl-a", "pl-b"}, nil)
	f.apply(t, "nodes", create("a", project, map[string]any{"title": "a"}),
		create("b", project, map[string]any{"title": "b"}))
	type outcome struct {
		from, to, kind, label string
		commit                model.Commit
		err                   error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, attempt := range []outcome{
		{from: "pl-a", to: "pl-b", kind: "blocks", label: "a-to-b"},
		{from: "pl-b", to: "pl-a", kind: "conditional-blocks", label: "b-to-a"},
	} {
		wg.Add(1)
		go func(v outcome) {
			defer wg.Done()
			<-start
			v.commit, v.err = f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
				edge(model.OpLink, v.from, v.to, v.kind),
			}}, f.meta(v.label, "tx.apply"))
			results <- v
		}(attempt)
	}
	close(start)
	wg.Wait()
	close(results)
	wins, refusals := 0, 0
	var winner outcome
	for v := range results {
		if v.err == nil {
			wins++
			winner = v
		} else {
			typed(t, v.err, "ontology_guard_failed")
			refusals++
			f.noReceipt(t, v.label, "tx.apply")
		}
	}
	if wins != 1 || refusals != 1 || winner.commit.Revision.String() != "2" {
		t.Fatalf("cycle race: %d wins, %d refusals, %+v", wins, refusals, winner)
	}
	after := f.image(t)
	if len(after.Log) != 2 || len(after.Log[1].Changes) != 1 || after.Log[1].Changes[0].Ref.ID != winner.from {
		t.Fatalf("losing edge published history: %+v", after.Log)
	}
	sameJSON(t, after.document(t, winner.from).Links, []pipeline.Link{{To: ref(winner.to), Kind: winner.kind}})
	if len(after.document(t, winner.to).Links) != 0 || after.row(t, winner.to).Revision.String() != "1" ||
		len(after.Versions[winner.to]) != 1 || len(after.Versions[winner.from]) != 2 {
		t.Fatalf("loser or read-only endpoint was written: %+v", after)
	}
}

func TestB06BlockingCyclesRollbackTheWholeBatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ab, bc, ca string
	}{
		{"ordinary", "blocks", "blocks", "blocks"},
		{"conditional", "conditional-blocks", "conditional-blocks", "conditional-blocks"},
		{"mixed", "blocks", "conditional-blocks", "blocks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, []string{"pl-a", "pl-b", "pl-c", "pl-rollback"}, nil)
			f.apply(t, "nodes", create("a", project, map[string]any{"title": "a"}),
				create("b", project, map[string]any{"title": "b"}),
				create("c", project, map[string]any{"title": "c"}))
			landed := f.apply(t, "acyclic", edge(model.OpLink, "pl-a", "pl-b", tc.ab),
				edge(model.OpLink, "pl-b", "pl-c", tc.bc))
			before := f.image(t)
			if landed.Revision.String() != "2" || before.row(t, "pl-c").Revision.String() != "1" {
				t.Fatalf("acyclic insertion rewrote its read-only endpoint: %+v", landed)
			}
			sameJSON(t, before.document(t, "pl-a").Links, []pipeline.Link{{To: ref("pl-b"), Kind: tc.ab}})
			sameJSON(t, before.document(t, "pl-b").Links, []pipeline.Link{{To: ref("pl-c"), Kind: tc.bc}})
			_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
				patch("pl-a", map[string]any{"title": "must roll back"}),
				create("temporary", project, map[string]any{"title": "must disappear"}),
				edge(model.OpLink, "pl-c", "pl-a", tc.ca),
			}}, f.meta("cycle-refused", "tx.apply"))
			typed(t, err, "ontology_guard_failed")
			f.unchanged(t, before)
			f.noReceipt(t, "cycle-refused", "tx.apply")

			// A repeated edge is a real no-op: no extra version, revision,
			// changeset or graph-sentinel bump, even with a fresh request key.
			noop := f.apply(t, "repeat-edge", edge(model.OpLink, "pl-a", "pl-b", tc.ab))
			if noop.Revision.String() != "2" || len(noop.Results) != 1 || noop.Results[0].Revision.String() != "2" {
				t.Fatalf("duplicate edge lost original row revision: %+v", noop)
			}
			f.unchanged(t, before)
			receipt := f.receipt(t, "repeat-edge", "tx.apply")
			if receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != "" || receipt.Commit.Revision.String() != "2" {
				t.Fatalf("duplicate edge lacks a final no-op receipt: %+v", receipt)
			}
		})
	}
}

func TestB06SelfEdgesRefuseWithoutPublishingEarlierOperations(t *testing.T) {
	for _, kind := range []string{"blocks", "conditional-blocks"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t, []string{"pl-self"}, nil)
			f.apply(t, "node", create("node", project, map[string]any{"title": "original"}))
			before := f.image(t)
			_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
				patch("pl-self", map[string]any{"title": "must roll back"}),
				edge(model.OpLink, "pl-self", "pl-self", kind),
			}}, f.meta("self-refused", "tx.apply"))
			typed(t, err, "ontology_guard_failed")
			f.unchanged(t, before)
			f.noReceipt(t, "self-refused", "tx.apply")
		})
	}
}

func TestB06NonblockingLoopAndAtomicFinalGraph(t *testing.T) {
	f := newFixture(t, []string{"pl-a", "pl-b", "pl-c"}, nil)
	f.apply(t, "nodes", create("a", project, map[string]any{"title": "a"}),
		create("b", project, map[string]any{"title": "b"}),
		create("c", project, map[string]any{"title": "c"}))
	f.apply(t, "nonblocking-loop", edge(model.OpLink, "pl-a", "pl-b", "relates-to"),
		edge(model.OpLink, "pl-b", "pl-a", "relates-to"))
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-a", "pl-b", "pl-c")
	f.apply(t, "blocking", edge(model.OpLink, "pl-a", "pl-c", "blocks"))
	// Although the first operation temporarily closes a cycle, the same
	// atomic request removes its other edge. Only the final graph is guarded.
	commit := f.apply(t, "reverse-atomically", edge(model.OpLink, "pl-c", "pl-a", "conditional-blocks"),
		edge(model.OpUnlink, "pl-a", "pl-c", "blocks"))
	if commit.Revision.String() != "4" || len(commit.Results) != 2 {
		t.Fatalf("final acyclic graph must commit once: %+v", commit)
	}
	after := f.image(t)
	sameJSON(t, after.document(t, "pl-a").Links, []pipeline.Link{{To: ref("pl-b"), Kind: "relates-to"}})
	sameJSON(t, after.document(t, "pl-b").Links, []pipeline.Link{
		{To: ref("pl-a"), Kind: "relates-to"},
	})
	sameJSON(t, after.document(t, "pl-c").Links, []pipeline.Link{{To: ref("pl-a"), Kind: "conditional-blocks"}})
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-a", "pl-b")
}
