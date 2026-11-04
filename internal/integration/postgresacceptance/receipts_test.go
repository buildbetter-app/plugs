package postgresacceptance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
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
	fieldMiss := h.call(t, "bead.update", fieldCAS, key("field-stale"))
	refused(t, fieldMiss, "precondition_failed")
	actualRef, refErr := json.Marshal(fieldMiss.Error.Data.Details["ref"])
	expectedRef, _ := json.Marshal(ref("pl-a"))
	if fieldMiss.Error.Data.Details["reason"] != "hash" || len(fieldMiss.Error.Data.Details) != 2 || refErr != nil || !bytes.Equal(actualRef, expectedRef) {
		t.Fatalf("B22 PostgreSQL field CAS must identify exactly its hash reason and resolved target: %+v", fieldMiss.Error.Data.Details)
	}
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

func metadataSet(target model.Ref, name, raw string) model.Op {
	return model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: target, Key: name, Value: json.RawMessage(raw)}}
}

func metadataSHA(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func metadataMeta(label string) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: project, AgentLabel: agent, IdempotencyKey: key(label)}
}

func seedMetadataAllocator(t *testing.T, h *host) {
	t.Helper()
	lock, err := driver.AllocatorLock(project)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.store.Write(h.ctx, h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(h.ctx, driver.LockSet{{Key: lock, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(h.ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.(driver.AllocatorContinuation).RestoreAllocators(h.ctx, []driver.OfflineAllocator{{Prefix: project, Mode: driver.AllocatorSequential, LastID: 8}}); err != nil {
		t.Fatal(err)
	}
	meta, err := h.store.Meta(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(h.ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: actor, LockKeys: []driver.LockKey{lock}}); err != nil {
		t.Fatal(err)
	}
}

// Read every durable graph/event/predicate/allocator/receipt row through a
// separate real SQL connection. Lease heartbeat rows are excluded.
func metadataSQLState(t *testing.T, db *sql.DB) map[string][][]string {
	t.Helper()
	state := map[string][][]string{}
	for _, table := range []string{"meta", "records", "versions", "states", "blobs", "changesets", "receipts", "checkpoints", "core_sessions", "allocator_states"} {
		rows, err := db.Query("SELECT * FROM plug_postgres." + table + " ORDER BY 1, 2")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			row := make([]string, len(values))
			for i, value := range values {
				row[i] = fmt.Sprintf("%T:%v", value, value)
			}
			state[table] = append(state[table], row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func TestB17MetadataCASRealPostgresReceiptsAndReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("unused-random-id")
	post := &warningPost{}
	options := pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "metadata-post", Subscriber: post})
	h := startHost(t, openStore(t, dsn), sequence, trace, options)
	other := openStore(t, dsn) // opened before writes, independent pool/readbacks
	seedMetadataAllocator(t, h)
	created := create(t, h, "metadata-create", "metadata", 2)
	target := created.Results[0].Ref
	const literal = "plugin.literal.dot/~key"
	seed := model.ChangeSet{Ops: []model.Op{
		metadataSet(target, literal, `{"b":2,"a":1}`), metadataSet(target, "number", `1.0`),
		metadataSet(target, "large", `9007199254740993`), metadataSet(target, "array", `[1,2]`),
		metadataSet(target, "string", `"1"`), metadataSet(target, "sibling", `{"kept":true}`),
	}}
	if _, err := h.writer.ApplyWithOutcome(h.ctx, actor, seed, metadataMeta("metadata-seed")); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before, sqlBefore, posts := graph(t, other), metadataSQLState(t, db), post.calls.Load()
	var refusedKeys []string
	for i, miss := range []struct{ field, canonical string }{
		{"metadata." + literal, `null`}, {"metadata." + literal, `{"a":1,"b":3}`},
		{"metadata.number", `1`}, {"metadata.number", `"1"`}, {"metadata.large", `9007199254740992`},
		{"metadata.array", `[2,1]`}, {"metadata.string", `1`}, {"metadata.missing", `null`},
	} {
		requestKey := key(fmt.Sprint("metadata-refused-", i))
		refusedKeys = append(refusedKeys, requestKey)
		params := map[string]any{"id": target, "fields": map[string]any{"title": "must not publish"}, "expect": []model.Precondition{model.PreconditionByHash(target, miss.field, metadataSHA(miss.canonical))}}
		r := h.call(t, "bead.update", params, requestKey)
		refused(t, r, "precondition_failed")
		actualRef, refErr := json.Marshal(r.Error.Data.Details["ref"])
		expectedRef, _ := json.Marshal(target)
		if r.Error.Data.Details["reason"] != "hash" || len(r.Error.Data.Details) != 2 || refErr != nil || !bytes.Equal(actualRef, expectedRef) {
			t.Fatalf("PostgreSQL metadata CAS must identify exactly its hash reason and resolved target: %+v", r.Error.Data.Details)
		}
		if r.Error.Data.Retryable || receipt(t, other, "bead.update", requestKey, agent) != nil || post.calls.Load() != posts || !reflect.DeepEqual(sqlBefore, metadataSQLState(t, db)) {
			t.Fatal("PostgreSQL metadata refusal mutated SQL/receipt/post state or was retryable")
		}
		unchanged(t, before, graph(t, other))
	}
	committed(t, h.call(t, "bead.update", map[string]any{"id": target, "fields": map[string]any{"description": "object matched"}, "expect": []model.Precondition{model.PreconditionByHash(target, "metadata."+literal, "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777")}}, key("metadata-object-match")), 4)
	committed(t, h.call(t, "bead.update", map[string]any{"id": target, "fields": map[string]any{"title": "number matched"}, "expect": []model.Precondition{model.PreconditionByHash(target, "metadata.number", "d0ff5974b6aa52cf562bea5921840c032a860a91a3512f7fe8f768f6bbe005f6")}}, key("metadata-number-match")), 5)
	before = graph(t, other)
	swap := model.ChangeSet{Ops: []model.Op{metadataSet(target, literal, `{"next":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(target, "metadata."+literal, metadataSHA(`{"a":1,"b":2}`))}}
	if _, err := h.writer.ApplyWithOutcome(h.ctx, actor, swap, metadataMeta("metadata-swap")); err != nil {
		t.Fatal(err)
	}
	after := graph(t, other)
	doc, err := pipeline.DecodeDocument(after.Records[0].Body)
	if err != nil || string(doc.Metadata[literal]) != `{"next":true}` || string(doc.Metadata["sibling"]) != `{"kept":true}` || string(doc.Metadata["number"]) != `1.0` || len(after.Changesets) != len(before.Changesets)+1 || len(after.Versions[target.ID]) != len(before.Versions[target.ID])+1 {
		t.Fatalf("PostgreSQL metadata swap/history/sibling/pre-image=%+v %v", doc, err)
	}
	before, sqlBefore, posts = after, metadataSQLState(t, db), post.calls.Load()
	failedMeta := metadataMeta("metadata-late-batch-miss")
	failedBatch := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: target, Fields: map[string]any{"title": "staged"}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Key: "discard", Type: "issue", Fields: map[string]any{"title": "discard"}}},
		metadataSet(target, literal, `"post-image"`),
	}, Expect: []model.Precondition{model.PreconditionByHash(target, "metadata."+literal, metadataSHA(`"post-image"`))}}
	_, err = h.writer.ApplyWithOutcome(h.ctx, actor, failedBatch, failedMeta)
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "precondition_failed" || typed.Data.Retryable {
		t.Fatalf("PostgreSQL late CAS mismatch=%v", err)
	}
	unchanged(t, before, graph(t, other))
	if !reflect.DeepEqual(sqlBefore, metadataSQLState(t, db)) || receipt(t, other, "tx.apply", failedMeta.IdempotencyKey, agent) != nil || post.calls.Load() != posts || sequence.used.Load() != 0 {
		t.Fatal("late metadata batch miss consumed graph/allocator/receipt/post/ID state")
	}
	noopMeta := metadataMeta("metadata-noop")
	noop := model.ChangeSet{Ops: []model.Op{metadataSet(target, literal, `{"next":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(target, "metadata."+literal, metadataSHA(`{"next":true}`))}}
	out, err := h.writer.ApplyWithOutcome(h.ctx, actor, noop, noopMeta)
	if err != nil || out.Replayed || out.Commit.Revision.String() != before.Meta.Head.String() {
		t.Fatalf("PostgreSQL metadata no-op=%+v %v", out, err)
	}
	stored := receipt(t, other, "tx.apply", noopMeta.IdempotencyKey, agent)
	if stored == nil || stored.ResponsePending || stored.Changeset.Kind != "" || !reflect.DeepEqual(stored.Commit, out.Commit) || post.calls.Load() != posts {
		t.Fatalf("metadata no-op receipt/post=%+v posts=%d", stored, post.calls.Load())
	}
	unchanged(t, before, graph(t, other))
	httpNoop := map[string]any{"id": target, "fields": map[string]any{"title": "number matched"}, "expect": noop.Expect}
	firstHTTP := h.call(t, "bead.update", httpNoop, key("metadata-http-noop"))
	committed(t, firstHTTP, 6)
	unchanged(t, before, graph(t, other))
	if post.calls.Load() != posts {
		t.Fatal("metadata-guarded HTTP no-op ran post subscribers")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace, options)
	again, err := h.writer.ApplyWithOutcome(h.ctx, actor, noop, noopMeta)
	if err != nil || !again.Replayed || !reflect.DeepEqual(again.Commit, out.Commit) || !reflect.DeepEqual(stored, receipt(t, h.store, "tx.apply", noopMeta.IdempotencyKey, agent)) || post.calls.Load() != posts {
		t.Fatalf("PostgreSQL metadata no-op/replay changed after all-pool reopen: %+v %v", again, err)
	}
	replay(t, h.call(t, "bead.update", httpNoop, key("metadata-http-noop")), firstHTTP.Result)
	for _, requestKey := range refusedKeys {
		if receipt(t, h.store, "bead.update", requestKey, agent) != nil {
			t.Fatal("refused HTTP metadata key appeared after reopen")
		}
	}
	if receipt(t, h.store, "tx.apply", failedMeta.IdempotencyKey, agent) != nil {
		t.Fatal("failed metadata batch key appeared after reopen")
	}
	unchanged(t, before, graph(t, h.store))
	t.Log("B17: real PostgreSQL/HTTP metadata vectors, no false null/type/order/large-number acceptance, full-row atomic refusal and finalized no-op replay across all-pool close/reopen")
}
