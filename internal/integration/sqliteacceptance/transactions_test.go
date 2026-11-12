package sqliteacceptance_test

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func createOp(alias, title string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: alias, Type: "issue", Fields: map[string]any{"title": title}}}
}

func patchOp(ref model.Ref, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}}
}

func TestB21B22AtomicBatchRefusalsAndOrderedRecovery(t *testing.T) {
	f := newFixture(t)
	ref, _, _ := f.create(t, "unchanged")
	before := f.state(t)
	m := writeMeta(t)
	bad := model.ChangeSet{Ops: []model.Op{
		patchOp(ref, map[string]any{"title": "must roll back"}),
		createOp("transient", "must not exist"),
		patchOp(model.RefByID("missing", model.Natural64(0)), map[string]any{"title": "missing"}),
	}}
	_, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, bad, m)
	domainCode(t, err, "not_found")
	requireSameState(t, before, f.state(t))
	if f.receipt(t, "tx.apply", "owner", m.IdempotencyKey) != nil {
		t.Fatal("failed batch published a receipt")
	}
	forward := model.ChangeSet{Ops: []model.Op{patchOp(model.RefByKey("later"), map[string]any{"title": "forward"}), createOp("later", "refused")}}
	_, err = f.writer.ApplyWithOutcome(f.ctx, anonymous, forward, writeMeta(t))
	domainCode(t, err, "invalid_params")
	requireSameState(t, before, f.state(t))
	invalid := model.ChangeSet{Ops: []model.Op{createOp("transient", "must not exist"), patchOp(ref, map[string]any{"title": "must roll back", "unknown": true})}}
	_, err = f.writer.ApplyWithOutcome(f.ctx, anonymous, invalid, writeMeta(t))
	domainCode(t, err, "ontology_unknown_field")
	requireSameState(t, before, f.state(t))
	// A failed key is reusable. Backward aliases resolve to actual minted IDs;
	// every operation lands in one revision and each changed row gets one version.
	good := model.ChangeSet{Ops: []model.Op{
		createOp("first", "created first"),
		patchOp(model.RefByKey("first"), map[string]any{"description": "backward alias"}),
		createOp("second", "created second"),
	}}
	outcome, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, good, m)
	if err != nil || outcome.Replayed || outcome.Commit.Revision.String() != "2" || len(outcome.Commit.Results) != 3 {
		t.Fatalf("atomic recovery=%+v %v", outcome, err)
	}
	results := outcome.Commit.Results
	if results[0].Ref.ID == "" || results[0].Ref.ID != results[1].Ref.ID || results[0].Ref.ID == results[2].Ref.ID {
		t.Fatalf("backward alias did not preserve identity: %+v", results)
	}
	first := f.get(t, results[0].Ref, nil)
	if first.Fields["title"] != "created first" || first.Fields["description"] != "backward alias" || first.Revision.String() != "2" {
		t.Fatalf("declaration order lost: %+v", first)
	}
	if f.get(t, ref, nil).Fields["title"] != "unchanged" {
		t.Fatal("refused patch leaked into the recovery transaction")
	}
	after := f.state(t)
	if len(after.Log) != 2 || len(after.Log[1].Changes) != 2 || len(after.Versions[results[0].Ref.ID]) != 1 || len(after.Versions[results[2].Ref.ID]) != 1 || len(after.Versions[ref.ID]) != 1 {
		t.Fatalf("batch did not publish one changeset and one version per changed row: %+v", after)
	}
	f.reopen(t)
	requireSameState(t, after, f.state(t))
	replay, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, good, m)
	if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, outcome.Commit) {
		t.Fatalf("batch receipt did not survive reopen: %+v %v", replay, err)
	}
	requireSameState(t, after, f.state(t))
	t.Log("B21/B22: late missing target, forward alias and ontology refusals changed no durable state; corrected same-key batch landed in declaration order as one revision and replayed after reopen")
}

func TestB15B25ScalarUpdatePreservesCommentsAndMetadata(t *testing.T) {
	f := newFixture(t)
	ref, _, _ := f.create(t, "content")
	metadata := json.RawMessage(`{"nested":{"kept":true},"number":7}`)
	cs := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: "first comment"}},
		{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: "acceptance.content", Value: metadata}},
	}}
	if _, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, cs, writeMeta(t)); err != nil {
		t.Fatal(err)
	}
	params := writeParams(t, map[string]any{"id": ref, "body": "second comment"})
	commented, firstReply := f.commit(t, "bead.comment", params)
	replayed, replayReply := f.commit(t, "bead.comment", params)
	if replayReply.Meta == nil || !replayReply.Meta.Replayed || !bytes.Equal(firstReply.Result, replayReply.Result) || !reflect.DeepEqual(commented, replayed) {
		t.Fatal("comment retry changed its stored result")
	}
	updated, _ := f.commit(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "scalar only"}}))
	var doc struct {
		Fields   map[string]any             `json:"fields"`
		Metadata map[string]json.RawMessage `json:"metadata"`
		Comments []struct {
			Body      string             `json:"body"`
			Principal model.PrincipalRef `json:"principal"`
			Time      time.Time          `json:"time"`
		} `json:"comments"`
	}
	if len(updated.Results) != 1 || json.Unmarshal(updated.Results[0].Record, &doc) != nil {
		t.Fatal("missing externally returned document")
	}
	if doc.Fields["title"] != "scalar only" || !bytes.Equal(doc.Metadata["acceptance.content"], metadata) || len(doc.Comments) != 2 || doc.Comments[0].Body != "first comment" || doc.Comments[1].Body != "second comment" || !doc.Comments[1].Time.After(doc.Comments[0].Time) {
		t.Fatalf("scalar update clobbered content/order or retry duplicated a comment: %+v", doc)
	}
	for _, comment := range doc.Comments {
		if !reflect.DeepEqual(comment.Principal, anonymous) {
			t.Fatal("comment lost the effective principal")
		}
	}
	before := f.state(t)
	f.reopen(t)
	requireSameState(t, before, f.state(t))
	if len(before.Log) != 4 || len(before.Versions[ref.ID]) != 4 {
		t.Fatal("comment replay minted a version or changeset")
	}
	t.Log("B15/B25: ordered attributed comments and nested metadata survived a scalar HTTP update, replay and real SQL reopen")
}

func TestB22ConcurrentCompareAndSetHasOneWinner(t *testing.T) {
	f := newFixture(t)
	ref, created, _ := f.create(t, "original title")
	params := []map[string]any{
		writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "winner title"}, "expect": []model.Precondition{model.PreconditionByRevision(ref, created.Revision)}}),
		writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"description": "winner description"}, "expect": []model.Precondition{model.PreconditionByRevision(ref, created.Revision)}}),
	}
	responses := concurrentCalls(t, f, "bead.update", params)
	winner := -1
	for i, r := range responses {
		if r.Error != nil {
			f.code(t, r, "precondition_failed")
			if f.receipt(t, "bead.update", "owner", params[i]["_meta"].(map[string]any)["idempotencyKey"].(string)) != nil {
				t.Fatal("losing CAS wrote a receipt")
			}
			continue
		}
		var committed model.Commit
		decodeResult(t, r, &committed)
		if winner != -1 || committed.Revision.String() != "2" || r.Meta != nil {
			t.Fatal("concurrent CAS admitted more than one winner")
		}
		winner = i
	}
	if winner == -1 {
		t.Fatal("concurrent CAS had no winner")
	}
	got := f.get(t, ref, nil)
	wantTitle, wantDescription := "original title", "original"
	if winner == 0 {
		wantTitle = "winner title"
	} else {
		wantDescription = "winner description"
	}
	state := f.state(t)
	if got.Fields["title"] != wantTitle || got.Fields["description"] != wantDescription || got.Revision.String() != "2" || len(state.Log) != 2 || len(state.Versions[ref.ID]) != 2 {
		t.Fatalf("losing disjoint field write leaked: %+v", got)
	}
	f.reopen(t)
	requireSameState(t, state, f.state(t))
	t.Log("B22: concurrent HTTP updates using the same current revision produced one commit, one typed refusal and no losing field/history/receipt")
}

func TestB22ConcurrentIdempotencyCreateReplaysOneDurableResult(t *testing.T) {
	f := newFixture(t)
	params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "one create"}})
	responses := concurrentCalls(t, f, "bead.create", []map[string]any{params, params})
	replays := 0
	for _, r := range responses {
		var c model.Commit
		decodeResult(t, r, &c)
		if c.Revision.String() != "1" || len(c.Results) != 1 || c.Results[0].Ref.ID == "" {
			t.Fatalf("concurrent keyed create=%+v", c)
		}
		if r.Meta != nil && r.Meta.Replayed {
			replays++
		}
	}
	state := f.state(t)
	if replays != 1 || !bytes.Equal(responses[0].Result, responses[1].Result) || len(state.Records) != 1 || len(state.Log) != 1 || len(state.Versions[state.Records[0].Ref.ID]) != 1 {
		t.Fatal("same-key concurrency did not return one exact durable result")
	}
	f.reopen(t)
	_, replay := f.commit(t, "bead.create", params)
	if replay.Meta == nil || !replay.Meta.Replayed || !bytes.Equal(responses[0].Result, replay.Result) {
		t.Fatal("concurrent create receipt changed after reopen")
	}
	requireSameState(t, state, f.state(t))
	t.Log("B22/G4: two simultaneous same-key HTTP creates returned one ID/revision/result, exactly one transient replay, one version and one changeset across restart")
}

func concurrentCalls(t *testing.T, f *fixture, method string, params []map[string]any) []reply {
	t.Helper()
	type outcome struct {
		index int
		reply reply
		err   error
	}
	start, done := make(chan struct{}), make(chan outcome, len(params))
	for i, p := range params {
		go func() {
			<-start
			r, err := f.invoke(method, p)
			done <- outcome{i, r, err}
		}()
	}
	close(start)
	responses := make([]reply, len(params))
	for range params {
		out := <-done
		if out.err != nil {
			t.Fatal(out.err)
		}
		responses[out.index] = out.reply
	}
	return responses
}

func TestB20ChangesetPagesAreOrderedAndSinceExclusive(t *testing.T) {
	f := newFixture(t)
	for _, title := range []string{"one", "two", "three", "four", "five"} {
		f.create(t, title)
	}
	f.reopen(t)
	r := f.reader(t)
	defer r.Close()
	var cursor *driver.Cursor
	var revisions []string
	for page := 0; page < 4; page++ {
		entries, err := r.Changesets(f.ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		if len(entries) > 2 {
			t.Fatal("changeset page exceeded its row limit")
		}
		for _, entry := range entries {
			revisions = append(revisions, entry.Revision.String())
			if len(entry.Changes) != 1 || entry.Incarnation != f.meta(t).Incarnation {
				t.Fatal("changeset was split or lost its incarnation")
			}
		}
		cursor = &driver.Cursor{Incarnation: f.meta(t).Incarnation, Revision: entries[len(entries)-1].Revision}
	}
	if !reflect.DeepEqual(revisions, []string{"1", "2", "3", "4", "5"}) || f.meta(t).Head.String() != "5" {
		t.Fatalf("ordered since-exclusive pages=%v", revisions)
	}
	t.Log("B20 changeset projection only: actual HTTP writes yielded complete ordered since-exclusive 2/2/1 pages after SQL reopen; durable event delivery is outside this slice")
}

func metadataSet(ref model.Ref, key, raw string) model.Op {
	return model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: key, Value: json.RawMessage(raw)}}
}

func metadataSHA(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func seedMetadataAllocator(t *testing.T, f *fixture) {
	t.Helper()
	lock, err := driver.AllocatorLock(project)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.Write(f.ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(f.ctx, driver.LockSet{{Key: lock, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(f.ctx); err != nil {
		t.Fatal(err)
	}
	const source driver.StoreID = "b17-metadata-allocator-source"
	restored, err := tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(f.ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	value := driver.OfflineAllocator{Prefix: project, Mode: driver.AllocatorSequential, LastID: 8}
	if err = tx.(driver.AllocatorContinuation).RestoreAllocators(f.ctx, []driver.OfflineAllocator{value}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(struct {
		Source driver.StoreID
		Value  driver.OfflineAllocator
	}{source, value})
	if err != nil {
		t.Fatal(err)
	}
	hash := metadataSHA(string(payload))
	principal := model.PrincipalRef{Kind: "system", ID: "plug-converter"}
	key := "b17-metadata-allocator-" + hash
	if _, err = tx.Commit(f.ctx, driver.ChangesetHeader{Incarnation: restored.Incarnation, Kind: model.ChangesetRestore, Principal: principal, Method: "offline.restore", IdempotencyKey: key, RequestHash: hash, Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}, LockKeys: []driver.LockKey{lock}}); err != nil {
		t.Fatal(err)
	}
	if got := f.meta(t); !reflect.DeepEqual(got, restored) || got.StoreID != source || got.Head.String() != "1" || got.HistoryFloor.String() != "1" {
		t.Fatalf("allocator seed did not publish the prepared offline lifetime: %+v", got)
	}
	r := f.reader(t)
	defer r.Close()
	stored, err := r.(driver.AllocatorReader).LoadAllocator(f.ctx, project)
	if err != nil || !reflect.DeepEqual(stored, value) {
		t.Fatalf("allocator seed was not durable: %+v %v", stored, err)
	}
	principalKey, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(f.ctx, principalKey, "", "offline.restore", key)
	if err != nil || receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Commit.Revision.String() != "1" {
		t.Fatalf("allocator seed lacks its final offline receipt: %+v %v", receipt, err)
	}
}

// The generic fixture predates offline initialization and asks for history
// from zero. Keep this metadata fixture on the public current-floor read path,
// retaining all record/version/log comparisons and the strict truncation guard.
func metadataSQLiteGraph(t *testing.T, f *fixture) durableState {
	t.Helper()
	state := durableState{Meta: f.meta(t), Versions: map[string][]driver.Version{}}
	r := f.reader(t)
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("record query incomplete: %+v %v", page, err)
	}
	state.Records = page.Items
	for _, record := range state.Records {
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("version query incomplete: %+v %v", versions, err)
		}
		state.Versions[record.Ref.ID] = versions.Items
	}
	_, err = r.Changesets(f.ctx, nil, 1000)
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor.String() != state.Meta.HistoryFloor.String() {
		t.Fatalf("restored metadata history must reject the discarded prefix: %v", err)
	}
	state.Log, err = r.Changesets(f.ctx, &driver.Cursor{Incarnation: state.Meta.Incarnation, Revision: state.Meta.HistoryFloor, Method: "changesets"}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// Inspect every persistent graph, predicate, allocator and receipt row through
// an independent SQL connection. Lease heartbeats are intentionally excluded.
func metadataSQLState(t *testing.T, db *sql.DB) map[string][][]string {
	t.Helper()
	state := map[string][][]string{}
	for _, table := range []string{"meta", "objects", "records", "versions", "key_history", "blobs", "changesets", "replays", "checkpoints", "core_sessions", "allocator_states"} {
		rows, err := db.Query("SELECT * FROM " + table + " ORDER BY 1, 2")
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

func TestB17MetadataCASRealSQLiteReceiptsAndReopen(t *testing.T) {
	f := newFixture(t)
	// This seeds only the allocator to measure aborted-batch consumption. It
	// does not bypass or qualify archive restoration.
	seedMetadataAllocator(t, f)
	// The prepared restore changed incarnation; bind the real HTTP service to
	// that committed lifetime before issuing its first application request.
	f.reopen(t)
	ref, _, _ := f.create(t, "metadata")
	const literal = "plugin.literal.dot/~key"
	seed := model.ChangeSet{Ops: []model.Op{
		metadataSet(ref, literal, `{"b":2,"a":1}`), metadataSet(ref, "number", `1.0`),
		metadataSet(ref, "large", `9007199254740993`), metadataSet(ref, "array", `[1,2]`),
		metadataSet(ref, "string", `"1"`), metadataSet(ref, "sibling", `{"kept":true}`),
	}}
	if _, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, seed, writeMeta(t)); err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "file", Path: f.path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before, sqlBefore := metadataSQLiteGraph(t, f), metadataSQLState(t, db)
	var refusedKeys []string
	for _, miss := range []struct{ field, canonical string }{
		{"metadata." + literal, `null`}, {"metadata." + literal, `{"a":1,"b":3}`},
		{"metadata.number", `1`}, {"metadata.number", `"1"`}, {"metadata.large", `9007199254740992`},
		{"metadata.array", `[2,1]`}, {"metadata.string", `1`}, {"metadata.missing", `null`},
	} {
		params := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "must not publish"}, "expect": []model.Precondition{model.PreconditionByHash(ref, miss.field, metadataSHA(miss.canonical))}})
		r := f.call(t, "bead.update", params)
		f.code(t, r, "precondition_failed")
		if r.Error.Data.Retryable {
			t.Fatal("metadata hash miss was retryable")
		}
		requestKey := params["_meta"].(map[string]any)["idempotencyKey"].(string)
		refusedKeys = append(refusedKeys, requestKey)
		if f.receipt(t, "bead.update", "owner", requestKey) != nil || !reflect.DeepEqual(sqlBefore, metadataSQLState(t, db)) {
			t.Fatal("HTTP metadata refusal changed durable SQL rows or stored a receipt")
		}
		requireSameState(t, before, metadataSQLiteGraph(t, f))
	}
	// Real HTTP service lowering uses the fixed metadata expectation seam.
	f.commit(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"description": "object matched"}, "expect": []model.Precondition{model.PreconditionByHash(ref, "metadata."+literal, "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777")}}))
	f.commit(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "number matched"}, "expect": []model.Precondition{model.PreconditionByHash(ref, "metadata.number", "d0ff5974b6aa52cf562bea5921840c032a860a91a3512f7fe8f768f6bbe005f6")}}))
	before = metadataSQLiteGraph(t, f)
	swap := model.ChangeSet{Ops: []model.Op{metadataSet(ref, literal, `{"next":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata."+literal, metadataSHA(`{"a":1,"b":2}`))}}
	if _, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, swap, writeMeta(t)); err != nil {
		t.Fatal(err)
	}
	after := metadataSQLiteGraph(t, f)
	doc, err := pipeline.DecodeDocument(after.Records[0].Body)
	if err != nil || string(doc.Metadata[literal]) != `{"next":true}` || string(doc.Metadata["sibling"]) != `{"kept":true}` || string(doc.Metadata["number"]) != `1.0` || len(after.Log) != len(before.Log)+1 || len(after.Versions[ref.ID]) != len(before.Versions[ref.ID])+1 {
		t.Fatalf("SQLite metadata swap/history/sibling/pre-image=%+v %v", doc, err)
	}
	before, sqlBefore = after, metadataSQLState(t, db)
	failedMeta := writeMeta(t)
	failedBatch := model.ChangeSet{Ops: []model.Op{patchOp(ref, map[string]any{"title": "staged"}), createOp("discard", "discard"), metadataSet(ref, literal, `"post-image"`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata."+literal, metadataSHA(`"post-image"`))}}
	_, err = f.writer.ApplyWithOutcome(f.ctx, anonymous, failedBatch, failedMeta)
	domainCode(t, err, "precondition_failed")
	requireSameState(t, before, metadataSQLiteGraph(t, f))
	if !reflect.DeepEqual(sqlBefore, metadataSQLState(t, db)) || f.receipt(t, "tx.apply", "owner", failedMeta.IdempotencyKey) != nil {
		t.Fatal("late metadata pre-image miss consumed graph/allocator/receipt state")
	}
	noopMeta := writeMeta(t)
	noop := model.ChangeSet{Ops: []model.Op{metadataSet(ref, literal, `{"next":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata."+literal, metadataSHA(`{"next":true}`))}}
	out, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, noop, noopMeta)
	if err != nil || out.Replayed || out.Commit.Revision.String() != before.Meta.Head.String() {
		t.Fatalf("SQLite metadata no-op=%+v %v", out, err)
	}
	requireSameState(t, before, metadataSQLiteGraph(t, f))
	receipt := f.receipt(t, "tx.apply", "owner", noopMeta.IdempotencyKey)
	if receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != "" || !reflect.DeepEqual(receipt.Commit, out.Commit) {
		t.Fatalf("metadata no-op receipt=%+v", receipt)
	}
	// The admitted HTTP update path also exposes transient replay metadata for
	// a metadata-guarded no-op; no additional binder is introduced here.
	httpNoop := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "number matched"}, "expect": noop.Expect})
	_, firstHTTP := f.commit(t, "bead.update", httpNoop)
	if firstHTTP.Meta != nil {
		t.Fatal("first metadata-guarded HTTP no-op was labeled replay")
	}
	requireSameState(t, before, metadataSQLiteGraph(t, f))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	requireSameState(t, before, metadataSQLiteGraph(t, f))
	again, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, noop, noopMeta)
	if err != nil || !again.Replayed || !reflect.DeepEqual(again.Commit, out.Commit) || !reflect.DeepEqual(receipt, f.receipt(t, "tx.apply", "owner", noopMeta.IdempotencyKey)) {
		t.Fatalf("SQLite metadata no-op lost durable replay: %+v %v", again, err)
	}
	_, replayHTTP := f.commit(t, "bead.update", httpNoop)
	if replayHTTP.Meta == nil || !replayHTTP.Meta.Replayed || !bytes.Equal(firstHTTP.Result, replayHTTP.Result) {
		t.Fatal("metadata-guarded HTTP no-op lost wire replay metadata after reopen")
	}
	for _, requestKey := range refusedKeys {
		if f.receipt(t, "bead.update", "owner", requestKey) != nil {
			t.Fatal("refused metadata key appeared after real reopen")
		}
	}
	if f.receipt(t, "tx.apply", "owner", failedMeta.IdempotencyKey) != nil {
		t.Fatal("failed batch receipt appeared after reopen")
	}
	requireSameState(t, before, metadataSQLiteGraph(t, f))
	t.Log("B17: real SQLite/HTTP canonical metadata CAS, exact null/type/order/large-number refusals, atomic allocator rollback and durable no-op replay survive close/reopen")
}
