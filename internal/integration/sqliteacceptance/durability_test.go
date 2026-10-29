package sqliteacceptance_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestB18B22DurableCASNoopAndReceiptsAfterReopen(t *testing.T) {
	f := newFixture(t)
	ref, created, createParams := f.create(t, "initial")
	if created.Revision.String() != "1" {
		t.Fatalf("first revision=%s", created.Revision)
	}
	if wal, err := os.Stat(f.path + "-wal"); err != nil || !wal.Mode().IsRegular() || wal.Size() == 0 {
		t.Fatalf("missing real populated WAL: %v %v", wal, err)
	}
	pinned := f.reader(t)
	defer pinned.Close()
	updateParams := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "updated"}, "expect": []model.Precondition{model.PreconditionByRevision(ref, created.Revision)}})
	updated, updateReply := f.commit(t, "bead.update", updateParams)
	if updated.Revision.String() != "2" || updated.Results[0].Revision.String() != "2" {
		t.Fatalf("CAS did not publish one revision: %+v", updated)
	}
	before := f.state(t)
	stale := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "updated", "description": "must not land"}, "expect": []model.Precondition{model.PreconditionByRevision(ref, created.Revision)}})
	f.code(t, f.call(t, "bead.update", stale), "precondition_failed")
	requireSameState(t, before, f.state(t))
	if f.receipt(t, "bead.update", "owner", stale["_meta"].(map[string]any)["idempotencyKey"].(string)) != nil {
		t.Fatal("refused CAS consumed an idempotency key")
	}
	_, _, _ = f.create(t, "unrelated")
	noopParams := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "updated"}})
	before = f.state(t)
	noop, noopReply := f.commit(t, "bead.update", noopParams)
	if noop.Revision.String() != "3" || noop.Results[0].Revision.String() != "2" || noopReply.Meta != nil {
		t.Fatalf("first no-op lost head/record revision distinction: %+v", noop)
	}
	requireSameState(t, before, f.state(t))
	// Compute the expected field token independently from the Writer helper.
	hash := sha256.Sum256([]byte(`"updated"`))
	fieldCAS := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"description": "field CAS"}, "expect": []model.Precondition{model.PreconditionByHash(ref, "fields.title", hex.EncodeToString(hash[:]))}})
	fieldCommit, _ := f.commit(t, "bead.update", fieldCAS)
	if fieldCommit.Revision.String() != "4" {
		t.Fatalf("field CAS revision=%s", fieldCommit.Revision)
	}
	rows, err := pinned.Get(f.ctx, []model.Ref{ref})
	var original struct {
		Fields map[string]any `json:"fields"`
	}
	if err != nil || len(rows) != 1 || json.Unmarshal(rows[0].Body, &original) != nil || original.Fields["title"] != "initial" || rows[0].Revision.String() != "1" {
		t.Fatalf("WAL reader lost its pre-update snapshot: %+v %v", rows, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	before = f.state(t)
	f.reopen(t)
	requireSameState(t, before, f.state(t))
	current := f.get(t, ref, nil)
	if current.ID != ref.ID || current.Revision.String() != "4" || current.Fields["title"] != "updated" || current.Fields["description"] != "field CAS" {
		t.Fatalf("current state did not survive reopen: %+v", current)
	}
	old := f.get(t, ref, &asOf{Revision: created.Revision, Incarnation: before.Meta.Incarnation})
	if old.ID != ref.ID || old.Revision.String() != "1" || old.Fields["title"] != "initial" {
		t.Fatalf("historical state did not survive reopen: %+v", old)
	}
	for _, tc := range []struct {
		method string
		params map[string]any
		commit model.Commit
		raw    json.RawMessage
	}{
		{"bead.create", createParams, created, nil},
		{"bead.update", updateParams, updated, updateReply.Result},
		{"bead.update", noopParams, noop, noopReply.Result},
	} {
		got, response := f.commit(t, tc.method, tc.params)
		if response.Meta == nil || !response.Meta.Replayed || !reflect.DeepEqual(got, tc.commit) || tc.raw != nil && !bytes.Equal(response.Result, tc.raw) {
			t.Fatalf("reopen replay changed %s receipt: %+v", tc.method, got)
		}
		receipt := f.receipt(t, tc.method, "owner", tc.params["_meta"].(map[string]any)["idempotencyKey"].(string))
		if receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, got) {
			t.Fatalf("replay differs from final durable receipt: %+v", receipt)
		}
	}
	createParams["fields"].(map[string]any)["title"] = "same key, changed request"
	f.code(t, f.call(t, "bead.create", createParams), "idempotency_key_reused")
	requireSameState(t, before, f.state(t))
	if len(before.Records) != 2 || len(before.Log) != 4 || len(before.Versions[ref.ID]) != 3 {
		t.Fatalf("refusal/no-op/replay minted history: %+v", before)
	}
	for i, entry := range before.Log {
		if entry.Revision.String() != model.Natural64(uint64(i+1)).String() {
			t.Fatalf("changeset revisions are not gapless: %+v", before.Log)
		}
	}
	t.Log("B18/B22: real HTTP create, revision/field CAS, stale refusal, no-op receipt, pinned WAL read and exact replay survived SQL close/reopen")
}

func TestB18B23PinnedHistoryPagesAcrossReopen(t *testing.T) {
	f := newFixture(t)
	want := map[string]bead{}
	for _, title := range []string{"first", "second", "third", "fourth"} {
		ref, _, _ := f.create(t, title)
		want[ref.ID] = f.get(t, ref, nil)
	}
	at := asOf{Revision: f.meta(t).Head, Incarnation: f.meta(t).Incarnation}
	params := map[string]any{"asOf": at, "limit": 1, "_meta": map[string]any{"agent": "owner"}}
	var page beadPage
	decodeResult(t, f.call(t, "history.query", params), &page)
	if len(page.Items) != 1 || page.NextCursor == "" || !reflect.DeepEqual(page.AsOf, at) {
		t.Fatalf("missing first history page: %+v", page)
	}
	firstCursor := page.NextCursor
	seen := append([]bead(nil), page.Items...)
	// Change every current row and add another after issuing the cursor.
	for id := range want {
		f.commit(t, "bead.update", writeParams(t, map[string]any{"id": model.RefByID(id, model.Natural64(0)), "fields": map[string]any{"title": "later"}}))
	}
	late, _, _ := f.create(t, "outside snapshot")
	f.reopen(t)
	for pages := 1; page.NextCursor != ""; pages++ {
		if pages >= len(want) {
			t.Fatal("history cursor failed to terminate")
		}
		params["cursor"] = page.NextCursor
		page = beadPage{}
		decodeResult(t, f.call(t, "history.query", params), &page)
		if len(page.Items) != 1 || !reflect.DeepEqual(page.AsOf, at) {
			t.Fatalf("history page moved snapshot: %+v", page)
		}
		seen = append(seen, page.Items...)
	}
	ids := make([]string, 0, len(seen))
	for _, item := range seen {
		if expected, exists := want[item.ID]; !exists || !reflect.DeepEqual(item, expected) {
			t.Fatalf("history repeated/changed/invented a row: %+v", item)
		}
		delete(want, item.ID)
		ids = append(ids, item.ID)
	}
	if len(want) != 0 || !sort.StringsAreSorted(ids) {
		t.Fatalf("history lost a row or changed order: ids=%v missing=%v", ids, want)
	}
	f.code(t, f.call(t, "bead.get", map[string]any{"id": late, "asOf": at}), "not_found")
	before := f.state(t)
	for _, bad := range []map[string]any{
		{"asOf": at, "limit": 1, "cursor": firstCursor, "filter": map[string]any{"types": []string{"issue"}}, "_meta": map[string]any{"agent": "owner"}},
		{"asOf": at, "limit": 1, "cursor": firstCursor, "_meta": map[string]any{"agent": "foreign"}},
		{"asOf": asOf{Revision: before.Meta.Head, Incarnation: at.Incarnation}, "limit": 1, "cursor": firstCursor, "_meta": map[string]any{"agent": "owner"}},
		{"asOf": at, "limit": 1, "cursor": "!" + firstCursor, "_meta": map[string]any{"agent": "owner"}},
	} {
		f.code(t, f.call(t, "history.query", bad), "cursor_invalid")
	}
	f.code(t, f.call(t, "bead.ready", map[string]any{"limit": 1, "cursor": firstCursor, "_meta": map[string]any{"agent": "owner"}}), "cursor_invalid")
	f.code(t, f.call(t, "bead.get", map[string]any{"id": late, "asOf": asOf{Revision: at.Revision, Incarnation: "another-store"}}), "incarnation_changed")
	requireSameState(t, before, f.state(t))
	t.Log("B18/B23: four ordered HTTP history pages retained their exact snapshot across writes and restart; changed filter/agent/asOf/method, malformed token and wrong incarnation refused")
}
