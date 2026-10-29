package graphacceptance_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func TestB06B07B08GraphCardsAcrossRealEngines(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newGraphFixture(t, engine)

			// Every ordinary edge below is created through the actual Writer. The
			// public bead.link/unlink methods are intentionally unbound at this
			// baseline, so this direct boundary is called out in the test log.
			root := f.create(t, "diamond root")
			left := f.create(t, "diamond left")
			right := f.create(t, "diamond right")
			leaf := f.create(t, "diamond leaf")
			ordinaryBlocker := f.create(t, "ordinary blocker")
			ordinaryDependent := f.create(t, "ordinary dependent")
			conditionalBlocker := f.create(t, "conditional blocker")
			conditionalDependent := f.create(t, "conditional dependent")
			related := f.create(t, "association only")

			f.apply(t, "tx.apply", model.ChangeSet{Ops: []model.Op{
				linkOp(left, root, "blocks"),
				linkOp(right, root, "blocks"),
				linkOp(leaf, left, "blocks"),
				linkOp(leaf, right, "blocks"),
				linkOp(ordinaryDependent, ordinaryBlocker, "blocks"),
				linkOp(conditionalDependent, conditionalBlocker, "conditional-blocks"),
				linkOp(related, root, "relates-to"),
			}})

			// Ordinary and conditional blockers suppress only their dependents;
			// an association edge has no readiness effect.
			requireReady(t, f, root, ordinaryBlocker, conditionalBlocker, related)

			beforeRefusal := f.state(t)
			meta, err := f.applyErr(t, "bead.close", model.ChangeSet{Ops: []model.Op{patchOp(ordinaryDependent, nil)}})
			wantErrorCode(t, err, "ontology_guard_failed")
			requireSameState(t, beforeRefusal, f.state(t))
			f.requireNoReceipt(t, meta)

			// Closing the root settles the two diamond branches, but the leaf
			// remains blocked by the other side and the independent ordinary
			// dependent remains blocked by its own blocker.
			f.closeBead(t, root)
			requireReady(t, f, left, right, ordinaryBlocker, conditionalBlocker, related)
			f.closeBead(t, left)
			requireReady(t, f, right, ordinaryBlocker, conditionalBlocker, related)
			f.closeBead(t, right)
			requireReady(t, f, leaf, ordinaryBlocker, conditionalBlocker, related)

			f.closeBead(t, ordinaryBlocker)
			requireReady(t, f, leaf, ordinaryDependent, conditionalBlocker, related)

			// A conditional edge is satisfied only by a completed failure. The
			// fail slot is not RPC-bound, so use the real Writer with its named
			// lifecycle method rather than patching status directly.
			beforeConditionalRefusal := f.state(t)
			meta, err = f.applyErr(t, "bead.close", model.ChangeSet{Ops: []model.Op{patchOp(conditionalDependent, nil)}})
			wantErrorCode(t, err, "ontology_guard_failed")
			requireSameState(t, beforeConditionalRefusal, f.state(t))
			f.requireNoReceipt(t, meta)
			f.lifecycle(t, "bead.fail", conditionalBlocker)
			requireReady(t, f, leaf, ordinaryDependent, conditionalDependent, related)

			// A completed success still does not satisfy a conditional edge.
			successBlocker := f.create(t, "completed success blocker")
			unmetDependent := f.create(t, "condition remains unmet")
			f.apply(t, "tx.apply", model.ChangeSet{Ops: []model.Op{linkOp(unmetDependent, successBlocker, "conditional-blocks")}})
			requireReady(t, f, leaf, ordinaryDependent, conditionalDependent, related, successBlocker)
			f.closeBead(t, successBlocker)
			requireReady(t, f, leaf, ordinaryDependent, conditionalDependent, related)

			t.Logf("B06/B07/B08 %s: multi-bead diamond, ordinary close settlement, conditional failure/success distinction and association readiness passed", engine)
		})
	}
}

func TestB06CycleRefusalAndFailureConservationAcrossRealEngines(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newGraphFixture(t, engine)
			first := f.create(t, "cycle first")
			second := f.create(t, "cycle second")
			f.apply(t, "tx.apply", model.ChangeSet{Ops: []model.Op{linkOp(first, second, "blocks")}})

			before := f.state(t)
			meta, err := f.applyErr(t, "tx.apply", model.ChangeSet{Ops: []model.Op{
				patchOp(first, map[string]any{"title": "must roll back"}),
				createOp("rolled-back", "must disappear"),
				linkOp(second, first, "blocks"),
			}})
			wantErrorCode(t, err, "ontology_guard_failed")
			requireSameState(t, before, f.state(t))
			f.requireNoReceipt(t, meta)

			before = f.state(t)
			meta, err = f.applyErr(t, "tx.apply", model.ChangeSet{Ops: []model.Op{linkOp(first, first, "blocks")}})
			wantErrorCode(t, err, "ontology_guard_failed")
			requireSameState(t, before, f.state(t))
			f.requireNoReceipt(t, meta)

			t.Logf("B06 %s: self and two-node scheduling cycles refused without graph, sentinel, log or version drift", engine)
		})
	}
}

func TestB07B21AliasesAndMixedBatchGuardsAcrossRealEngines(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newGraphFixture(t, engine)
			blocker := f.create(t, "existing blocker")

			// Direct Writer mixed batch: the first create establishes an alias,
			// and the later link resolves that backward alias to the actual ID.
			meta := f.meta("tx.apply")
			commit, err := f.writer.Apply(f.ctx, graphAnonymous, model.ChangeSet{Ops: []model.Op{
				createOp("batch-dependent", "created in mixed batch"),
				linkOp(model.RefByKey("batch-dependent"), blocker, "blocks"),
			}}, meta)
			if err != nil || len(commit.Results) != 2 || commit.Results[0].Ref.ID == "" || commit.Results[0].Ref.ID != commit.Results[1].Ref.ID {
				t.Fatalf("backward alias mixed batch: commit=%+v err=%v", commit, err)
			}
			batchDependent := commit.Results[0].Ref
			if commit.Revision.String() != "2" || commit.Results[0].Revision.String() != "2" || commit.Results[1].Revision.String() != "2" {
				t.Fatalf("mixed batch did not commit at one revision: %+v", commit)
			}
			if got := f.document(t, batchDependent, nil); len(got.Links) != 1 || got.Links[0].To.ID != blocker.ID || got.Links[0].Kind != "blocks" {
				t.Fatalf("mixed batch did not persist its edge: %+v", got.Links)
			}

			before := f.state(t)
			forwardMeta, err := f.applyErr(t, "tx.apply", model.ChangeSet{Ops: []model.Op{
				linkOp(model.RefByKey("declared-later"), blocker, "blocks"),
				createOp("declared-later", "forward alias must refuse"),
			}})
			wantErrorCode(t, err, "invalid_params")
			requireSameState(t, before, f.state(t))
			f.requireNoReceipt(t, forwardMeta)

			// A named lifecycle slot accepts only its one target patch and an
			// optional same-target reason comment. A graph op in that batch is a
			// typed refusal before any state or edge work is published.
			before = f.state(t)
			slotMeta, err := f.applyErr(t, "bead.close", model.ChangeSet{Ops: []model.Op{
				patchOp(batchDependent, nil),
				linkOp(batchDependent, blocker, "relates-to"),
			}})
			wantErrorCode(t, err, "invalid_params")
			requireSameState(t, before, f.state(t))
			f.requireNoReceipt(t, slotMeta)

			requireReady(t, f, blocker)
			t.Logf("B07/B21 %s: backward alias and graph batch landed atomically; forward alias and mixed lifecycle batch refused", engine)
		})
	}
}

func TestB07SupportedEdgeHistoryReplayAcrossRealEngines(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newGraphFixture(t, engine)
			blocker := f.create(t, "history blocker")
			dependent := f.create(t, "history dependent")
			linked := f.apply(t, "tx.apply", model.ChangeSet{Ops: []model.Op{linkOp(dependent, blocker, "blocks")}})
			unlinked := f.apply(t, "tx.apply", model.ChangeSet{Ops: []model.Op{unlinkOp(dependent, blocker, "blocks")}})

			checks := []struct {
				at     model.Revision
				linked bool
				label  string
			}{
				{at: model.Natural64(2), label: "before link"},
				{at: linked.Revision, linked: true, label: "after link"},
				{at: unlinked.Revision, label: "after unlink"},
			}
			checkHistory := func() {
				for _, tc := range checks {
					doc := f.document(t, dependent, &tc.at)
					if tc.linked {
						if len(doc.Links) != 1 || doc.Links[0].To.ID != blocker.ID || doc.Links[0].Kind != "blocks" {
							t.Fatalf("%s historical edge missing: %+v", tc.label, doc.Links)
						}
					} else if len(doc.Links) != 0 {
						t.Fatalf("%s unexpected historical edge: %+v", tc.label, doc.Links)
					}
					// The service projection exposes the bead revision, not Links;
					// edge replay is checked in the driver's real document above.
					bead := f.historyRevision(t, dependent, tc.at)
					if bead.Revision.String() != tc.at.String() {
						t.Fatalf("%s service history revision=%s want=%s", tc.label, bead.Revision, tc.at)
					}
				}
			}
			checkHistory()

			if engine == "sqlite" {
				f.reopen(t)
				checkHistory()
			}
			state := f.state(t)
			if len(state.Log) != 4 || len(state.Versions[dependent.ID]) != 3 {
				t.Fatalf("edge history minted unexpected graph history: log=%d versions=%d", len(state.Log), len(state.Versions[dependent.ID]))
			}
			t.Logf("B07 %s: blocks edge replayed before/after link and unlink through the real driver; service history preserved corresponding revisions", engine)
		})
	}
}

func TestB06CompetingLinkRaceAcrossRealEngines(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newGraphFixture(t, engine)
			first := f.create(t, "race first")
			second := f.create(t, "race second")
			before := f.state(t)
			barrier := &linkPreparationBarrier{release: make(chan struct{}), bases: map[string][]string{}}
			racingWriter := f.newWriter(pipeline.WithValidators(pipeline.ValidatorBinding{ID: "link-preparation-barrier", Validator: barrier}))
			metas := []model.WriteMeta{f.meta("tx.apply"), f.meta("tx.apply")}
			changes := []model.ChangeSet{
				{Ops: []model.Op{linkOp(first, second, "blocks")}},
				{Ops: []model.Op{linkOp(second, first, "conditional-blocks")}},
			}

			type result struct {
				index  int
				commit model.Commit
				err    error
			}
			start := make(chan struct{})
			out := make(chan result, len(changes))
			for i := range changes {
				go func(i int) {
					<-start
					commit, err := racingWriter.Apply(f.ctx, graphAnonymous, changes[i], metas[i])
					out <- result{index: i, commit: commit, err: err}
				}(i)
			}
			close(start)
			results := []result{<-out, <-out}
			wins, refusals := 0, 0
			winner := -1
			for _, result := range results {
				if result.err == nil {
					wins++
					winner = result.index
					if result.commit.Revision.String() != "3" || len(result.commit.Results) != 1 {
						t.Fatalf("race winner published unexpected commit: %+v", result.commit)
					}
					continue
				}
				wantErrorCode(t, result.err, "ontology_guard_failed")
				f.requireNoReceipt(t, metas[result.index])
				refusals++
			}
			if wins != 1 || refusals != 1 {
				t.Fatalf("competing links wins=%d refusals=%d results=%+v", wins, refusals, results)
			}

			edges := []string{}
			for _, ref := range []model.Ref{first, second} {
				doc := f.document(t, ref, nil)
				for _, link := range doc.Links {
					edges = append(edges, ref.ID+"->"+link.To.ID+":"+link.Kind)
				}
			}
			sort.Strings(edges)
			winnerEdge := changes[winner].Ops[0].Link
			if len(edges) != 1 || edges[0] != winnerEdge.From.ID+"->"+winnerEdge.To.ID+":"+winnerEdge.LinkKind {
				t.Fatalf("competing link race graph does not match its winner: %v", edges)
			}
			state := f.state(t)
			if state.Meta.Head.String() != "3" || len(state.Log) != 3 || len(state.Log[2].Changes) != 1 || state.Log[2].Changes[0].Ref.ID != winnerEdge.From.ID || len(state.Versions[winnerEdge.From.ID]) != 2 || len(state.Versions[winnerEdge.To.ID]) != 1 || !reflect.DeepEqual(state.Records[winnerEdge.To.ID], before.Records[winnerEdge.To.ID]) {
				t.Fatalf("competing link race changed history unexpectedly: log=%d versions=%+v", len(state.Log), state.Versions)
			}
			barrier.mu.Lock()
			bases := barrier.bases
			barrier.mu.Unlock()
			for i, meta := range metas {
				want := []string{"2"}
				if i != winner {
					want = append(want, "3")
				}
				if !reflect.DeepEqual(bases[meta.IdempotencyKey], want) {
					t.Fatalf("race attempt %d snapshots=%v want=%v", i, bases[meta.IdempotencyKey], want)
				}
			}
			t.Logf("B06 %s: both opposing links prepared at revision 2; winner committed at 3; loser retried at 3 and refused its cycle without a receipt", engine)
		})
	}
}
