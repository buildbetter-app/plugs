package restoreworkload_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func TestRestoredSQLiteLifecyclePersistsAcrossPhaseReopens(t *testing.T) {
	ctx := context.Background()
	source := openThinSliceLocal(t, "memory", "", "reopen-source", false)
	blocker := thinSliceCreate(t, source.Client, "reopen blocker", 1)
	dependent := thinSliceCreate(t, source.Client, "reopen dependent", 2)
	refs := []model.Ref{blocker, dependent}
	link := model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{
		Kind: model.OpLink, From: dependent, To: blocker, LinkKind: "blocks",
	}}}}
	if _, err := source.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": link}, thinSliceKey(3)); err != nil {
		t.Fatal(err)
	}
	sourceReady := readyPage(t, source.Client)
	original := map[string]pipeline.Document{}
	for _, ref := range refs {
		original[ref.ID] = thinSliceHistoryDocument(t, source.Client, ref, "3", sourceReady.incarnation)
	}
	data, archive := captureHeadDump(t, source.Client)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "restored-lifecycle.db")
	target := openThinSliceLocal(t, "sqlite", path, "reopen-worker", true)
	incarnation := restoreHead(t, target.Client, data, archive)
	if incarnation == "" || incarnation == sourceReady.incarnation {
		t.Fatalf("restore reused source incarnation: %q", incarnation)
	}
	if ready := readyPage(t, target.Client); ready.revision != "1" || ready.incarnation != incarnation || !sameSet(ready.ids, []string{blocker.ID}) {
		t.Fatalf("restored ready=%+v", ready)
	}
	for _, ref := range refs {
		if got := thinSliceHistoryDocument(t, target.Client, ref, "1", incarnation); !reflect.DeepEqual(got, original[ref.ID]) {
			t.Fatalf("first restore changed %s: got=%+v want=%+v", ref.ID, got, original[ref.ID])
		}
	}

	type receipt struct {
		method string
		params map[string]any
		key    string
		result client.Result
	}
	var receipts []receipt
	write := func(method string, params map[string]any, key int, revision string) model.Commit {
		t.Helper()
		commit, result := thinSliceCommit(t, target.Client, method, params, thinSliceKey(key))
		if commit.Revision.String() != revision || result.Replayed {
			t.Fatalf("%s revision=%s replayed=%t want fresh revision=%s", method, commit.Revision, result.Replayed, revision)
		}
		receipts = append(receipts, receipt{method, params, thinSliceKey(key), result})
		return commit
	}
	replay := func() {
		t.Helper()
		for _, saved := range receipts {
			got, err := target.Client.Call(ctx, saved.method, saved.params, saved.key)
			if err != nil || !got.Replayed || !bytes.Equal(got.JSON, saved.result.JSON) {
				t.Fatalf("durable %s replay=%s replayed=%t err=%v want=%s", saved.method, got.JSON, got.Replayed, err, saved.result.JSON)
			}
		}
	}

	claimed := write("bead.claimNext", map[string]any{"filter": map[string]any{"types": []string{"issue"}}}, 10, "2")
	if len(claimed.Results) != 1 || claimed.Results[0].Ref.ID != blocker.ID || claimed.LeaseExpiresAt == nil || claimed.LeaseTtlMs == nil {
		t.Fatalf("restored claim=%+v", claimed)
	}
	write("bead.update", map[string]any{"id": blocker, "fields": map[string]any{"title": "reopen blocker updated"}}, 11, "3")
	write("bead.comment", map[string]any{"id": blocker, "body": "comment before close/reopen"}, 12, "4")
	commented := thinSliceHistoryDocument(t, target.Client, blocker, "4", incarnation)
	if commented.Fields["title"] != "reopen blocker updated" || commented.Fields["status"] != "in_progress" || commented.Lease == nil || !commented.Lease.ExpiresAt.Equal(*claimed.LeaseExpiresAt) || commented.Lease.TtlMs.String() != claimed.LeaseTtlMs.String() || commented.Lease.Agent != "reopen-worker" {
		t.Fatalf("pre-reopen update/claim=%+v", commented)
	}
	if len(commented.Comments) != 1 || commented.Comments[0].Body != "comment before close/reopen" || commented.Comments[0].Time.IsZero() || commented.Comments[0].Principal.Kind == "" || commented.Comments[0].Principal.ID == "" {
		t.Fatalf("pre-reopen comment=%+v", commented.Comments)
	}
	if got := thinSliceHistoryDocument(t, target.Client, dependent, "4", incarnation); !reflect.DeepEqual(got, original[dependent.ID]) || len(got.Links) != 1 || got.Links[0].To != blocker || got.Links[0].Kind != "blocks" {
		t.Fatalf("pre-reopen dependent changed: %+v", got)
	}

	// Save the actual current responses and every retained history envelope
	// before closing. A newly opened driver and a post-reopen self-comparison
	// alone would not detect missing comments, version metadata or a lost claim.
	beforeGuard := captureLifecycleReads(t, target.Client, refs, 4, incarnation)
	closeDependent := map[string]any{"id": dependent, "reason": "dependent completed after reopen"}
	_, err := target.Client.Call(ctx, "bead.close", closeDependent, thinSliceKey(13))
	requireCode(t, err, "ontology_guard_failed")
	assertLifecycleReads(t, target.Client, refs, 4, incarnation, beforeGuard)
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target = openThinSliceLocal(t, "sqlite", path, "reopen-worker", true)
	assertLifecycleReads(t, target.Client, refs, 4, incarnation, beforeGuard)
	replay()
	_, err = target.Client.Call(ctx, "bead.close", closeDependent, thinSliceKey(13))
	requireCode(t, err, "ontology_guard_failed")
	assertLifecycleReads(t, target.Client, refs, 4, incarnation, beforeGuard)
	t.Log("SQLite phase reopen at head=4: claim/update/comment, exact current/history envelopes, dependency guard and keyed warning receipts persisted")

	changeSet := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{
		Target: blocker, Fields: map[string]any{"description": "continued after durable reopen"},
	}}}}
	write("tx.apply", map[string]any{"changeSet": changeSet}, 14, "5")
	write("bead.close", map[string]any{"id": blocker, "reason": "blocker completed after reopen"}, 15, "6")
	if ready := readyPage(t, target.Client); ready.revision != "6" || ready.incarnation != incarnation || !sameSet(ready.ids, []string{dependent.ID}) {
		t.Fatalf("blocker close did not release dependent: %+v", ready)
	}
	closedBlocker := thinSliceHistoryDocument(t, target.Client, blocker, "6", incarnation)
	if closedBlocker.Fields["status"] != "done" || closedBlocker.Lease != nil || closedBlocker.Fields["title"] != "reopen blocker updated" || closedBlocker.Fields["description"] != "continued after durable reopen" || len(closedBlocker.Comments) != 2 || !reflect.DeepEqual(closedBlocker.Comments[0], commented.Comments[0]) || closedBlocker.Comments[1].Body != "blocker completed after reopen" {
		t.Fatalf("continued blocker close lost fields/comment/claim settlement: %+v", closedBlocker)
	}
	beforeSecondReopen := captureLifecycleReads(t, target.Client, refs, 6, incarnation)
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target = openThinSliceLocal(t, "sqlite", path, "reopen-worker", true)
	assertLifecycleReads(t, target.Client, refs, 6, incarnation, beforeSecondReopen)
	replay()
	assertLifecycleReads(t, target.Client, refs, 6, incarnation, beforeSecondReopen)
	t.Log("SQLite phase reopen at head=6: completed blocker and exact history persisted; dependent alone remains ready")

	// The identical request/key refused at head 4 can now commit, proving the
	// refusal did not leave a durable receipt and the persisted guard is lifted.
	write("bead.close", closeDependent, 13, "7")
	if ready := readyPage(t, target.Client); ready.revision != "7" || ready.incarnation != incarnation || len(ready.ids) != 0 {
		t.Fatalf("completed lifecycle ready=%+v", ready)
	}
	closed := map[string]pipeline.Document{}
	for _, ref := range refs {
		document := thinSliceHistoryDocument(t, target.Client, ref, "7", incarnation)
		if document.Fields["status"] != "done" || document.Lease != nil {
			t.Fatalf("completed document=%+v", document)
		}
		for _, comment := range document.Comments {
			if comment.Time.IsZero() || comment.Principal.Kind == "" || comment.Principal.ID == "" {
				t.Fatalf("completed comment lost attribution/time: %+v", comment)
			}
		}
		closed[ref.ID] = document
	}
	if !reflect.DeepEqual(closed[blocker.ID], closedBlocker) {
		t.Fatalf("dependent close changed completed blocker: %+v", closed[blocker.ID])
	}
	closedDependent := closed[dependent.ID]
	if len(closedDependent.Comments) != 1 || closedDependent.Comments[0].Body != closeDependent["reason"] || !reflect.DeepEqual(closedDependent.Links, original[dependent.ID].Links) {
		t.Fatalf("completed dependent lost close reason/dependency: %+v", closedDependent)
	}
	beforeReplay := captureLifecycleReads(t, target.Client, refs, 7, incarnation)
	replay()
	assertLifecycleReads(t, target.Client, refs, 7, incarnation, beforeReplay)

	mutatedData, mutatedArchive := captureHeadDump(t, target.Client)
	manifest := mutatedArchive.Manifest()
	if !mutatedArchive.Native() || manifest.History != "head" || manifest.NativeHead != "7" || manifest.NativeIncarnation != incarnation || manifest.NativeStoreID != archive.Manifest().NativeStoreID || manifest.StoreID != archive.Manifest().StoreID || manifest.RecordCount < 2 || manifest.ChangesetCount != 0 {
		t.Fatalf("completed reopened archive identity/scope=%+v", manifest)
	}
	assertLifecycleReads(t, target.Client, refs, 7, incarnation, beforeReplay)
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	// One second-restore destination is sufficient for this persistence gap;
	// PR310 owns the independent memory/SQLite source/destination matrix.
	roundtrip := openThinSliceLocal(t, "memory", "", "reopen-roundtrip", false)
	secondIncarnation := restoreHead(t, roundtrip.Client, mutatedData, mutatedArchive)
	if secondIncarnation == "" || secondIncarnation == incarnation || secondIncarnation == sourceReady.incarnation {
		t.Fatalf("second restore reused incarnation=%q", secondIncarnation)
	}
	if ready := readyPage(t, roundtrip.Client); ready.revision != "1" || ready.incarnation != secondIncarnation || len(ready.ids) != 0 {
		t.Fatalf("second restore ready=%+v", ready)
	}
	for _, ref := range refs {
		if got := thinSliceHistoryDocument(t, roundtrip.Client, ref, "1", secondIncarnation); !reflect.DeepEqual(got, closed[ref.ID]) {
			t.Fatalf("second restore changed %s: got=%+v want=%+v", ref.ID, got, closed[ref.ID])
		}
	}
	if page := historyPage(t, roundtrip.Client, blocker.ID, "1", secondIncarnation); len(page) != 1 || page[0].title != "reopen blocker updated" || page[0].revision != "1" {
		t.Fatalf("second restore head history=%+v", page)
	}
	for _, refusal := range []struct {
		revision    int
		incarnation string
		code        string
	}{{0, secondIncarnation, "cursor_expired"}, {7, incarnation, "incarnation_changed"}} {
		_, err := roundtrip.Client.Call(ctx, "history.query", map[string]any{
			"asOf": map[string]any{"revision": refusal.revision, "incarnation": refusal.incarnation}, "limit": 10,
		}, "")
		requireCode(t, err, refusal.code)
	}
	_, readback := captureHeadDump(t, roundtrip.Client)
	if got := readback.Manifest(); got.NativeHead != "1" || got.NativeIncarnation != secondIncarnation || got.NativeStoreID != manifest.NativeStoreID || got.StoreID != manifest.StoreID || got.RecordCount != manifest.RecordCount {
		t.Fatalf("second restore census/identity=%+v want=%+v", got, manifest)
	}
	if err := roundtrip.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("I2 restored SQLite lifecycle: ready -> claim/update/comment -> guarded refusal -> close/reopen(head=4) -> tx.apply/blocker close -> close/reopen(head=6) -> dependent close -> dump(head=7) -> second restore(head=1); exact documents/comments/dependency/history; test-presence-verifier=restore-workload")
}

func captureLifecycleReads(t *testing.T, c *client.Client, refs []model.Ref, head int, incarnation string) map[string]string {
	t.Helper()
	saved := map[string]string{}
	read := func(name, method string, params map[string]any) {
		t.Helper()
		result, err := c.Call(context.Background(), method, params, "")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		saved[name] = string(result.JSON)
	}
	read("ready", "bead.ready", map[string]any{"limit": 100})
	for _, ref := range refs {
		read("current/"+ref.ID, "bead.get", map[string]any{"id": ref})
		for revision := 1; revision <= head; revision++ {
			asOf := map[string]any{"revision": revision, "incarnation": incarnation}
			// Preserve the complete envelope: ref, selected version revision,
			// base/patch, principal, createdAt and body, not only decoded fields.
			read(fmt.Sprintf("history.get/%s/%d", ref.ID, revision), "history.get", map[string]any{"ref": ref, "asOf": asOf})
			read(fmt.Sprintf("history.query/%s/%d", ref.ID, revision), "history.query", map[string]any{
				"asOf": asOf, "filter": map[string]any{"resources": []string{ref.ID}}, "limit": 100,
			})
		}
	}
	return saved
}

func assertLifecycleReads(t *testing.T, c *client.Client, refs []model.Ref, head int, incarnation string, want map[string]string) {
	t.Helper()
	got := captureLifecycleReads(t, c, refs, head, incarnation)
	for name, expected := range want {
		if !bytes.Equal([]byte(got[name]), []byte(expected)) {
			t.Fatalf("persisted %s changed: got=%s want=%s", name, got[name], expected)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("persisted read census changed: got=%d want=%d", len(got), len(want))
	}
}
