package rpc_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type failingPost struct{ calls int }

func (p *failingPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls++
	return errors.New("test subscriber unavailable")
}

func realServiceServer(t *testing.T) (*rpc.Server, *failingPost, time.Time, uint64) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "rpc-integration", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	ontology := service.NewOntology(nil)
	post := &failingPost{}
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithClock(clock), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "test-post", Subscriber: post}))
	domain := service.New(store, lease, writer, service.WithOntology(ontology), service.WithClock(clock))
	// This assignment also checks the actual pure service port at compile time.
	var svc rpc.Service = domain
	config := pagingConfig()
	config.Incarnation = meta.Incarnation
	return pagingServer(t, svc, config), post, now, lease.Epoch()
}

func writeParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{"idempotencyKey": hex.EncodeToString(key[:]), "agent": "rpc-test"}
	return params
}

func createdRef(t *testing.T, commit map[string]any) map[string]any {
	t.Helper()
	rows := commit["results"].([]any)
	if len(rows) != 1 {
		t.Fatalf("missing create result: %v", commit)
	}
	return rows[0].(map[string]any)["ref"].(map[string]any)
}

func writeResult(t *testing.T, s *rpc.Server, method string, params map[string]any, replayed bool) map[string]any {
	t.Helper()
	reply := call(t, s, context.Background(), method, paramsJSON(t, params))
	meta, valid := reply["_meta"].(map[string]any)
	_, present := reply["_meta"]
	if (replayed && (!valid || meta["replayed"] != true)) || (!replayed && present) {
		t.Fatalf("wrong replay metadata for %s (want %t): %v", method, replayed, reply)
	}
	return result(t, reply)
}

func TestRealWriteReplayMetadataPreservesResultBytes(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	head := func() any { return result(t, call(t, s, ctx, "bead.ready", `{}`))["asOf"].(map[string]any)["revision"] }
	var ref map[string]any
	for _, method := range []string{"bead.create", "bead.claim", "bead.update", "bead.comment", "bead.close"} {
		t.Run(method, func(t *testing.T) {
			params := map[string]any{"id": ref}
			switch method {
			case "bead.create":
				params = map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}
			case "bead.update":
				params["fields"] = map[string]any{"title": "Changed"}
			case "bead.comment":
				params["body"] = "Replay evidence"
			case "bead.close":
				params["reason"] = "Completed"
			}
			writeParams(t, params)
			if method == "bead.claim" || method == "bead.update" {
				params["_meta"].(map[string]any)["return"] = "minimal"
			}
			var original json.RawMessage
			var postCalls int
			var revision any
			for i := range 2 {
				raw, err := inprocess.New(s).RoundTrip(ctx, request(method, paramsJSON(t, params)))
				if err != nil {
					t.Fatal(err)
				}
				var reply map[string]json.RawMessage
				if err := json.Unmarshal(raw, &reply); err != nil {
					t.Fatal(err)
				}
				if len(reply["error"]) != 0 || len(reply["result"]) == 0 {
					t.Fatalf("write failed: %s", raw)
				}
				if i == 0 {
					if len(reply["_meta"]) != 0 {
						t.Fatalf("first committed/finalized response marked replay: %s", raw)
					}
					original, postCalls, revision = reply["result"], post.calls, head()
					if method == "bead.create" {
						v, err := wire.DecodeJSON(original)
						if err != nil {
							t.Fatal(err)
						}
						ref = createdRef(t, v.(map[string]any))
					}
				} else if string(reply["_meta"]) != `{"replayed":true}` || !bytes.Equal(original, reply["result"]) || head() != revision || post.calls != postCalls {
					t.Fatalf("replay changed stored result bytes/head/post effects or omitted metadata: %s", raw)
				}
			}
		})
	}
}

func TestRealServiceLifecycleThroughInprocess(t *testing.T) {
	s, post, now, epoch := realServiceServer(t)
	ctx := context.Background()
	createParams := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Original", "priority": 1}})
	created := result(t, call(t, s, ctx, "bead.create", paramsJSON(t, createParams)))
	ref := createdRef(t, created)
	warnings := created["warnings"].([]any)
	if len(warnings) != 1 || warnings[0].(map[string]any)["code"] != "post_subscriber_failed" {
		t.Fatal("committed warning was not visible at byte boundary", created)
	}
	replay := result(t, call(t, s, ctx, "bead.create", paramsJSON(t, createParams)))
	if !reflect.DeepEqual(created, replay) || post.calls != 1 {
		t.Fatal("replay changed result or reran post subscriber", replay, post.calls)
	}
	ready := result(t, call(t, s, ctx, "bead.ready", `{"limit":1}`))
	if len(ready["items"].([]any)) != 1 || ready["items"].([]any)[0].(map[string]any)["id"] != ref["id"] {
		t.Fatal("real create did not reach ready", ready)
	}
	claimParams := writeParams(t, map[string]any{"id": ref})
	claimParams["_meta"].(map[string]any)["return"] = "minimal"
	claim := result(t, call(t, s, ctx, "bead.claim", paramsJSON(t, claimParams)))
	if claim["leaseTtlMs"] != json.Number("300000") || claim["leaseExpiresAt"] != now.Add(5*time.Minute).Format(time.RFC3339Nano) || claim["commit"] != nil {
		t.Fatal("real claim was not flattened with durable lease values", claim)
	}
	postCalls := post.calls
	replay = result(t, call(t, s, ctx, "bead.claim", paramsJSON(t, claimParams)))
	if !reflect.DeepEqual(claim, replay) || post.calls != postCalls {
		t.Fatal("claim replay changed durable result", replay)
	}
	got := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))
	lease := got["lease"].(map[string]any)
	if lease["ttlMs"] != json.Number("300000") || lease["epoch"] != json.Number(model.Natural64(epoch).String()) || lease["holder"] != "anonymous" {
		t.Fatal("stored lease wire values changed", lease)
	}
	update := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Updated"}, "expect": []any{map[string]any{"ref": ref, "revision": claim["revision"]}}})
	result(t, call(t, s, ctx, "bead.update", paramsJSON(t, update)))
	stale := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Stale"}, "expect": update["expect"]})
	requireCode(t, call(t, s, ctx, "bead.update", paramsJSON(t, stale)), "precondition_failed")
	result(t, call(t, s, ctx, "bead.comment", paramsJSON(t, writeParams(t, map[string]any{"id": ref, "body": "Recorded evidence"}))))
	closeParams := writeParams(t, map[string]any{"id": ref, "reason": "Done with evidence"})
	closed := result(t, call(t, s, ctx, "bead.close", paramsJSON(t, closeParams)))
	postCalls = post.calls
	replay = result(t, call(t, s, ctx, "bead.close", paramsJSON(t, closeParams)))
	if !reflect.DeepEqual(closed, replay) || post.calls != postCalls {
		t.Fatal("close replay changed the committed response", replay)
	}
	got = result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))
	if got["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "Updated" || got["bead"].(map[string]any)["revision"] != closed["revision"] {
		t.Fatal("final persisted state not returned by get", got)
	}
	ready = result(t, call(t, s, ctx, "bead.ready", `{"limit":1}`))
	if len(ready["items"].([]any)) != 0 {
		t.Fatal("closed bead remained ready", ready)
	}
	history := result(t, call(t, s, ctx, "history.query", paramsJSON(t, map[string]any{"asOf": map[string]any{"revision": created["revision"]}, "limit": 1})))
	if history["items"].([]any)[0].(map[string]any)["fields"].(map[string]any)["title"] != "Original" {
		t.Fatal("history returned current values", history)
	}
}

func TestRealServiceNamedLifecycleRPCsUseCatalogAndSharedWriter(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{
		"type": "issue", "fields": map[string]any{"title": "named lifecycle"},
	}), false)
	ref := createdRef(t, created)

	methods := result(t, call(t, s, ctx, "rpc.methods", `{}`))["methods"].([]any)
	for _, want := range []string{"bead.close", "bead.reopen", "bead.defer", "bead.cancel", "bead.fail"} {
		found := false
		for _, method := range methods {
			if method == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("cataloged lifecycle method %s was not bound/discovered: %v", want, methods)
		}
	}
	openrpc, err := s.OpenRPC(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(openrpc, &document); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"bead.reopen", "bead.defer", "bead.cancel", "bead.fail"} {
		found := false
		for _, method := range document.Methods {
			if method.Name == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("generated OpenRPC omitted %s", want)
		}
	}

	callSlot := func(method, reason string) map[string]any {
		t.Helper()
		params := map[string]any{"id": ref}
		if reason != "" {
			params["reason"] = reason
		}
		return writeResult(t, s, method, writeParams(t, params), false)
	}
	checkStatus := func(want string) {
		t.Helper()
		got := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))
		if bead := got["bead"].(map[string]any); bead["status"] != want {
			t.Fatalf("lifecycle status=%v want=%s", bead["status"], want)
		}
	}
	for _, step := range []struct {
		method string
		reason string
		status string
	}{
		{"bead.defer", "waiting", "backlog"},
		{"bead.reopen", "retrying", "todo"},
		{"bead.claim", "", "in_progress"},
		{"bead.fail", "failed", "failed"},
		{"bead.reopen", "retry after failure", "todo"},
		{"bead.cancel", "canceled", "canceled"},
		{"bead.reopen", "reconsidered", "todo"},
		{"bead.claim", "", "in_progress"},
		{"bead.close", "done", "done"},
		{"bead.reopen", "follow-up", "todo"},
	} {
		commit := callSlot(step.method, step.reason)
		if commit["revision"] == nil {
			t.Fatalf("lifecycle %s returned no commit: %v", step.method, commit)
		}
		checkStatus(step.status)
	}

	// Repeating the exact keyed failure returns the stored result and replay
	// metadata without invoking a second Writer commit.
	failParams := writeParams(t, map[string]any{"id": ref, "reason": "terminal"})
	first := writeResult(t, s, "bead.fail", failParams, false)
	replayed := writeResult(t, s, "bead.fail", failParams, true)
	if !reflect.DeepEqual(first, replayed) {
		t.Fatalf("lifecycle replay changed result: first=%v replay=%v", first, replayed)
	}
	checkStatus("failed")

	// A slot cannot transition to its current target. The refusal is typed and
	// does not publish a new revision or receipt.
	bad := writeParams(t, map[string]any{"id": ref})
	requireCode(t, call(t, s, ctx, "bead.fail", paramsJSON(t, bad)), "invalid_params")
	history := result(t, call(t, s, ctx, "history.query", paramsJSON(t, map[string]any{
		"asOf":   map[string]any{"revision": first["revision"]},
		"filter": map[string]any{"resources": []string{ref["id"].(string)}},
	})))
	if len(history["items"].([]any)) != 1 || history["items"].([]any)[0].(map[string]any)["status"] != "failed" {
		t.Fatalf("history did not expose final lifecycle image: %v", history)
	}
}

func TestRealServiceLiveReadyAndPinnedHistoryAcrossWrites(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	var ids []string
	for priority, title := range []string{"first", "pending-close", "third", "pending-update"} {
		created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": title, "priority": priority}}), false)
		ids = append(ids, createdRef(t, created)["id"].(string))
	}
	params := map[string]any{"limit": 1}
	first := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	at := first["asOf"].(map[string]any)
	if first["items"].([]any)[0].(map[string]any)["id"] != ids[0] {
		t.Fatal("Ready lost priority order", first)
	}
	// Close a row that has not been delivered. A frozen continuation would
	// return it next; live Ready must instead evaluate the current eligible set.
	later := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "later", "priority": 4}}), false)
	laterID := createdRef(t, later)["id"].(string)
	closed := writeResult(t, s, "bead.close", writeParams(t, map[string]any{"id": map[string]any{"id": ids[1]}, "reason": "Closed between pages"}), false)
	params["cursor"] = first["nextCursor"]
	page := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	if len(page["items"].([]any)) != 1 || page["items"].([]any)[0].(map[string]any)["id"] != ids[2] || page["asOf"].(map[string]any)["revision"] != closed["revision"] {
		t.Fatal("Ready returned the old pending row or rejected the live revision", page)
	}
	updated := writeResult(t, s, "bead.update", writeParams(t, map[string]any{"id": map[string]any{"id": ids[3]}, "fields": map[string]any{"title": "Changed between pages"}}), false)
	params["cursor"] = page["nextCursor"]
	page = result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	bead := page["items"].([]any)[0].(map[string]any)
	if bead["id"] != ids[3] || bead["fields"].(map[string]any)["title"] != "Changed between pages" || page["asOf"].(map[string]any)["revision"] != updated["revision"] {
		t.Fatal("Ready did not read the pending row's current value", page)
	}
	params["cursor"] = page["nextCursor"]
	page = result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	if len(page["items"].([]any)) != 1 || page["items"].([]any)[0].(map[string]any)["id"] != laterID || page["nextCursor"] != nil {
		t.Fatal("Ready did not traverse the later live insert", page)
	}
	params = map[string]any{"limit": 1, "asOf": at}
	var historyIDs []string
	for i := range 4 {
		page = result(t, call(t, s, ctx, "history.query", paramsJSON(t, params)))
		if len(page["items"].([]any)) != 1 || !reflect.DeepEqual(page["asOf"], at) {
			t.Fatal("real history lost its snapshot", page)
		}
		bead := page["items"].([]any)[0].(map[string]any)
		historyIDs = append(historyIDs, bead["id"].(string))
		if bead["id"] == ids[3] && bead["fields"].(map[string]any)["title"] != "pending-update" {
			t.Fatal("History returned the later pending-row update", page)
		}
		if i < 3 {
			params["cursor"] = page["nextCursor"]
		} else if page["nextCursor"] != nil {
			t.Fatal("history continued beyond pinned items")
		}
	}
	sort.Strings(ids)
	if !reflect.DeepEqual(historyIDs, ids) {
		t.Fatal("real history repeated, skipped or included a later item", historyIDs, ids)
	}
}

func TestRealServiceLiveReadyAcceptsEmptyShrunkenTail(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	var refs []map[string]any
	for priority := range 2 {
		created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "tail", "priority": priority}}), false)
		refs = append(refs, createdRef(t, created))
	}
	first := result(t, call(t, s, ctx, "bead.ready", `{"limit":1}`))
	var last map[string]any
	for _, ref := range refs {
		last = writeResult(t, s, "bead.close", writeParams(t, map[string]any{"id": ref, "reason": "Shrink list"}), false)
	}
	page := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, map[string]any{"limit": 1, "cursor": first["nextCursor"]})))
	if len(page["items"].([]any)) != 0 || page["nextCursor"] != nil || page["asOf"].(map[string]any)["revision"] != last["revision"] {
		t.Fatal("valid live continuation did not terminate after list shrink", page)
	}
}

func TestRealServiceNoopReceiptsPreserveWireRevisionsAndLease(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	head := func() any {
		return result(t, call(t, s, ctx, "bead.ready", `{}`))["asOf"].(map[string]any)["revision"]
	}
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}), false)
	ref := createdRef(t, created)
	claim := writeResult(t, s, "bead.claim", writeParams(t, map[string]any{"id": ref}), false)
	lease := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))["lease"]
	postCalls := post.calls
	sameClaimParams := writeParams(t, map[string]any{"id": ref})
	sameClaimParams["_meta"].(map[string]any)["return"] = "minimal"
	sameClaim := writeResult(t, s, "bead.claim", sameClaimParams, false)
	if sameClaim["revision"] != claim["revision"] || sameClaim["leaseTtlMs"] != claim["leaseTtlMs"] || sameClaim["leaseExpiresAt"] != claim["leaseExpiresAt"] || head() != claim["revision"] || post.calls != postCalls {
		t.Fatal("fresh-key same-owner claim changed head, lease or subscriber effects", sameClaim)
	}
	if got := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))["lease"]; !reflect.DeepEqual(got, lease) {
		t.Fatal("same-owner claim changed the stored lease", got, lease)
	}
	// A no-op receipt reports current head while retaining the older record's
	// own revision. Wire conversion must preserve both independently.
	other := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Other"}}), false)
	postCalls = post.calls
	noopParams := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Original"}})
	noop := writeResult(t, s, "bead.update", noopParams, false)
	rows := noop["results"].([]any)
	if noop["revision"] != other["revision"] || len(rows) != 1 || rows[0].(map[string]any)["revision"] != claim["revision"] || head() != other["revision"] || post.calls != postCalls {
		t.Fatal("no-op changed head or narrowed record/receipt revisions", noop)
	}
	updated := writeResult(t, s, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Changed"}}), false)
	postCalls = post.calls
	replay := writeResult(t, s, "bead.update", noopParams, true)
	if !reflect.DeepEqual(replay, noop) || head() != updated["revision"] || post.calls != postCalls {
		t.Fatal("no-op replay lost its exact stored response or advanced head", replay)
	}
	replay = writeResult(t, s, "bead.claim", sameClaimParams, true)
	if !reflect.DeepEqual(replay, sameClaim) || head() != updated["revision"] || post.calls != postCalls {
		t.Fatal("same-owner claim replay changed its receipt or current head", replay)
	}
}

func TestRealDryRunDoesNotBecomeReceiptReplay(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}), false)
	ref := createdRef(t, created)
	params := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Previewed"}})
	meta := params["_meta"].(map[string]any)
	meta["dryRun"] = true
	postCalls := post.calls
	for range 2 {
		preview := writeResult(t, s, "bead.update", params, false)
		page := result(t, call(t, s, ctx, "bead.ready", `{}`))
		bead := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))["bead"].(map[string]any)
		if preview["wouldCommit"] != true || page["asOf"].(map[string]any)["revision"] != created["revision"] || bead["fields"].(map[string]any)["title"] != "Original" || post.calls != postCalls {
			t.Fatal("dry-run changed durable state or post effects", preview, bead)
		}
	}
	// The preview did not reserve a receipt: the same key can now execute the
	// real request, and only the retry of that execution is marked replayed.
	delete(meta, "dryRun")
	committed := writeResult(t, s, "bead.update", params, false)
	if committed["revision"] == created["revision"] || post.calls != postCalls+1 {
		t.Fatal("real execution after dry-run did not commit once", committed)
	}
	replayed := writeResult(t, s, "bead.update", params, true)
	if !reflect.DeepEqual(replayed, committed) || post.calls != postCalls+1 {
		t.Fatal("real receipt after dry-run did not replay exactly", replayed)
	}
}
