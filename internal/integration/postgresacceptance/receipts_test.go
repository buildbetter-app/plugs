package postgresacceptance

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func create(t *testing.T, h *host, requestKey, title string, revision uint64) model.Commit {
	t.Helper()
	return committed(t, h.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title, "description": "original"}}, key(requestKey)), revision)
}

func replay(t *testing.T, got reply, original []byte) {
	t.Helper()
	if got.Error != nil || !got.replayed() || !bytes.Equal(got.Result, original) {
		t.Fatalf("durable replay differs: got=%s error=%v replay=%v want=%s", got.Result, got.Error, got.replayed(), original)
	}
}

func TestB28DurableWarningReceiptAcrossHandlesAndReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-a")
	a, b := openStore(t, dsn), openStore(t, dsn)
	post := &warningPost{}
	options := pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "synthetic-destination", Subscriber: post})
	h := startHost(t, a, sequence, trace, options)
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": "initial"}}
	first := h.call(t, "bead.create", params, key("durable-create"))
	commit := committed(t, first, 1)
	if len(commit.Results) != 1 || commit.Results[0].Ref.ID != "pl-a" || len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" || post.calls.Load() != 1 {
		t.Fatalf("post failure did not return a committed warning: %+v calls=%d", commit, post.calls.Load())
	}
	stored := receipt(t, b, "bead.create", key("durable-create"), agent)
	if stored == nil || stored.ResponsePending || !reflect.DeepEqual(stored.Commit, commit) {
		t.Fatalf("independent pool did not observe finalized durable outcome: %+v", stored)
	}
	update := map[string]any{"id": ref("pl-a"), "fields": map[string]any{"title": "later"}}
	updated := h.call(t, "bead.update", update, key("durable-update"))
	committed(t, updated, 2)
	before := graph(t, b)
	ledger(t, before, 2, map[string]int{"pl-a": 2})
	oldEpoch := h.lease.Epoch()
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	// The second pool was opened before either write. It now acquires its own
	// lease and constructs a fresh service/Writer, with no in-memory receipts.
	h = startHost(t, b, sequence, trace, options)
	if h.lease.Epoch() <= oldEpoch {
		t.Fatal("new independent handle did not advance its lease epoch")
	}
	replay(t, h.call(t, "bead.create", params, key("durable-create")), first.Result)
	refused(t, h.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "different"}}, key("durable-create")), "idempotency_key_reused")
	unchanged(t, before, graph(t, b))
	if sequence.used.Load() != 1 || post.calls.Load() != 2 {
		t.Fatalf("replay/refusal reran ID generation or posts: ids=%d posts=%d", sequence.used.Load(), post.calls.Load())
	}
	if got := receipt(t, b, "bead.create", key("durable-create"), agent); !reflect.DeepEqual(got, stored) {
		t.Fatalf("first receipt changed after later writes/replay: %+v", got)
	}
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	c := openStore(t, dsn)
	h = startHost(t, c, sequence, trace, options)
	replay(t, h.call(t, "bead.update", update, key("durable-update")), updated.Result)
	unchanged(t, before, graph(t, c))
	if post.calls.Load() != 2 || post.badSnapshot.Load() || sequence.used.Load() != 1 {
		t.Fatal("reopened replay reran a subscriber or generated an ID")
	}
	current := get(t, h, "pl-a", nil)
	if current.ID != "pl-a" || current.Revision.String() != "2" || current.Fields["title"] != "later" {
		t.Fatalf("reopened current image: %+v", current)
	}
	t.Log("B28 bounded: HTTP warning result is finalized in PostgreSQL, identical across independent handles and all-pool close/reopen; no duplicate write/post")
}

func TestB22CASNoopAndReplayRetainOriginalRevision(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-a", "pl-b")
	h := startHost(t, openStore(t, dsn), sequence, trace)
	create(t, h, "cas-a", "alpha", 1)
	create(t, h, "cas-b", "other", 2)
	update := map[string]any{
		"id": ref("pl-a"), "fields": map[string]any{"title": "beta"},
		"expect": []model.Precondition{model.PreconditionByRevision(ref("pl-a"), model.Natural64(1))},
	}
	committed(t, h.call(t, "bead.update", update, key("cas-current")), 3)
	before := graph(t, h.store)
	refused(t, h.call(t, "bead.update", update, key("cas-stale")), "precondition_failed")
	unchanged(t, before, graph(t, h.store))
	if receipt(t, h.store, "bead.update", key("cas-stale"), agent) != nil {
		t.Fatal("stale CAS stored a successful receipt")
	}
	// A field hash from the original description survives a disjoint title
	// change. A whole-record revision at that point would be stale.
	hash, err := pipeline.FieldHash("original")
	if err != nil {
		t.Fatal(err)
	}
	fieldCAS := map[string]any{"id": ref("pl-a"), "fields": map[string]any{"description": "new description"}, "expect": []model.Precondition{model.PreconditionByHash(ref("pl-a"), "fields.description", hash)}}
	committed(t, h.call(t, "bead.update", fieldCAS, key("field-current")), 4)
	before = graph(t, h.store)
	refused(t, h.call(t, "bead.update", fieldCAS, key("field-stale")), "precondition_failed")
	unchanged(t, before, graph(t, h.store))
	committed(t, h.call(t, "bead.update", map[string]any{"id": ref("pl-b"), "fields": map[string]any{"title": "other 2"}}, key("other-2")), 5)
	before = graph(t, h.store)
	noopParams := map[string]any{"id": ref("pl-a"), "fields": map[string]any{"title": "beta"}}
	noop := h.call(t, "bead.update", noopParams, key("no-op"))
	result := committed(t, noop, 5)
	if len(result.Results) != 1 || result.Results[0].Revision.String() != "4" {
		t.Fatalf("no-op replaced record revision with graph head: %+v", result)
	}
	unchanged(t, before, graph(t, h.store))
	noOpReceipt := receipt(t, h.store, "bead.update", key("no-op"), agent)
	if noOpReceipt == nil || noOpReceipt.ResponsePending || noOpReceipt.Changeset.Kind != "" || !reflect.DeepEqual(noOpReceipt.Commit, result) {
		t.Fatalf("no-op receipt was not final graph-free metadata: %+v", noOpReceipt)
	}
	committed(t, h.call(t, "bead.update", map[string]any{"id": ref("pl-b"), "fields": map[string]any{"title": "other 3"}}, key("other-3")), 6)
	before = graph(t, h.store)
	replay(t, h.call(t, "bead.update", noopParams, key("no-op")), noop.Result)
	unchanged(t, before, graph(t, h.store))
	ledger(t, before, 6, map[string]int{"pl-a": 3, "pl-b": 3})
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace)
	replay(t, h.call(t, "bead.update", noopParams, key("no-op")), noop.Result)
	unchanged(t, before, graph(t, h.store))
	if got := receipt(t, h.store, "bead.update", key("no-op"), agent); !reflect.DeepEqual(noOpReceipt, got) {
		t.Fatal("reopen changed the no-op receipt")
	}
	t.Log("B22/B18 bounded: current record CAS and disjoint field CAS accept; stale guards refuse atomically; durable no-op retains head=5/result=4 even at head=6 after reopen")
}

func TestB22ConcurrentCASHasOneWinner(t *testing.T) {
	h := startHost(t, openStore(t, freshDatabase(t)), newIDs("pl-a"), newTrace(t))
	create(t, h, "race-create", "original title", 1)
	start := make(chan struct{})
	type outcome struct {
		index int
		reply reply
		err   error
	}
	results := make(chan outcome, 2)
	params := []map[string]any{
		{"id": ref("pl-a"), "fields": map[string]any{"title": "winning title"}, "expect": []model.Precondition{model.PreconditionByRevision(ref("pl-a"), model.Natural64(1))}},
		{"id": ref("pl-a"), "fields": map[string]any{"description": "winning description"}, "expect": []model.Precondition{model.PreconditionByRevision(ref("pl-a"), model.Natural64(1))}},
	}
	for i := range params {
		go func(i int) {
			<-start
			r, err := h.exchange("bead.update", params[i], key(fmt.Sprintf("cas-race-%d", i)), agent)
			results <- outcome{i, r, err}
		}(i)
	}
	close(start)
	winner, success, failure := -1, 0, 0
	var won reply
	for range params {
		out := <-results
		if out.err != nil {
			t.Fatal(out.err)
		}
		if out.reply.Error == nil {
			committed(t, out.reply, 2)
			winner, won, success = out.index, out.reply, success+1
		} else {
			refused(t, out.reply, "precondition_failed")
			failure++
		}
	}
	if success != 1 || failure != 1 {
		t.Fatalf("concurrent CAS successes=%d refusals=%d", success, failure)
	}
	state := graph(t, h.store)
	ledger(t, state, 2, map[string]int{"pl-a": 2})
	if len(state.Records) != 1 {
		t.Fatalf("race created extra records: %+v", state.Records)
	}
	doc, err := pipeline.DecodeDocument(state.Records[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	wantTitle, wantDescription := "original title", "original"
	if winner == 0 {
		wantTitle = "winning title"
	} else {
		wantDescription = "winning description"
	}
	if doc.Fields["title"] != wantTitle || doc.Fields["description"] != wantDescription {
		t.Fatalf("losing disjoint update leaked: %+v", doc)
	}
	loser := 1 - winner
	if receipt(t, h.store, "bead.update", key(fmt.Sprintf("cas-race-%d", loser)), agent) != nil {
		t.Fatal("losing concurrent CAS left a receipt")
	}
	replay(t, h.call(t, "bead.update", params[winner], key(fmt.Sprintf("cas-race-%d", winner))), won.Result)
	unchanged(t, state, graph(t, h.store))
	// The winner must be present in SQL, not just in its HTTP response.
	r := reader(t, h.store)
	defer r.Close()
	rows, err := r.Get(context.Background(), []model.Ref{ref("pl-a")})
	if err != nil || len(rows) != 1 || rows[0].Revision.String() != "2" {
		t.Fatalf("CAS winner not durable: %+v %v", rows, err)
	}
	t.Log("B22 bounded: two simultaneous HTTP disjoint updates with revision 1 produce one revision-2 commit and one typed refusal, with no losing data/version/log/receipt")
}
