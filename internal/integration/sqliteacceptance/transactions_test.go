package sqliteacceptance_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
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
