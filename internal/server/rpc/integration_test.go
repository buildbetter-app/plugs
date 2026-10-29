package rpc_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
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

func TestRealServiceSignedPagingAcrossWrites(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	var ids []string
	for _, priority := range []int{3, 1, 1} {
		created := result(t, call(t, s, ctx, "bead.create", paramsJSON(t, writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "page", "priority": priority}}))))
		ids = append(ids, createdRef(t, created)["id"].(string))
	}
	// Ready order is priority then stable ID; history order is stable ID.
	expectedReady := append([]string(nil), ids[1:]...)
	sort.Strings(expectedReady)
	expectedReady = append(expectedReady, ids[0])
	params := map[string]any{"limit": 1}
	first := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	at := first["asOf"].(map[string]any)
	// A later write must not enter the service's pinned continuation snapshot.
	result(t, call(t, s, ctx, "bead.create", paramsJSON(t, writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "later", "priority": 0}}))))
	var readyIDs []string
	page := first
	for i := range 3 {
		if len(page["items"].([]any)) != 1 || !reflect.DeepEqual(page["asOf"], at) {
			t.Fatal("real ready limit or pinned asOf changed", page)
		}
		readyIDs = append(readyIDs, page["items"].([]any)[0].(map[string]any)["id"].(string))
		if i < 2 {
			params["cursor"] = page["nextCursor"]
			page = result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
		} else if page["nextCursor"] != nil {
			t.Fatal("ready continued beyond pinned items")
		}
	}
	if !reflect.DeepEqual(readyIDs, expectedReady) {
		t.Fatal("real ready ordering or traversal changed", readyIDs, expectedReady)
	}
	params = map[string]any{"limit": 1, "asOf": at}
	var historyIDs []string
	for i := range 3 {
		page = result(t, call(t, s, ctx, "history.query", paramsJSON(t, params)))
		if len(page["items"].([]any)) != 1 || !reflect.DeepEqual(page["asOf"], at) {
			t.Fatal("real history lost its snapshot", page)
		}
		historyIDs = append(historyIDs, page["items"].([]any)[0].(map[string]any)["id"].(string))
		if i < 2 {
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
