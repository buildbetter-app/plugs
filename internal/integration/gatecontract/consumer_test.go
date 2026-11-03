package gatecontract_test

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/port"
)

func assertConsumer(t *testing.T, f *fixture, waiter, spawner string, want bool) {
	t.Helper()
	before := f.meta()
	f.ready(waiter, want)
	result := f.call("view.explain", map[string]any{"viewId": "b14-consumer", "itemId": waiter}, false, "")
	verdict := result["directBlockers"].(map[string]any)
	edges := verdict["edges"].([]any)
	if len(edges) != 1 {
		t.Fatalf("expected one direct fanout verdict: %v", result)
	}
	edge := edges[0].(map[string]any)
	state := "unsatisfied"
	if want {
		state = "satisfied"
	}
	if edge["kind"] != "waits-for" || edge["state"] != state || edge["blocker"].(map[string]any)["id"] != spawner {
		t.Fatalf("authenticated %s: bead.ready=%v but view.explain=%v", consumerPolicyProfile, want, result)
	}
	if !reflect.DeepEqual(before, f.meta()) {
		t.Fatal("readiness/explanation wrote content")
	}
}

func TestB14AuthenticatedWaitsForConsumerFanoutAndReopen(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		h := newConsumerHost(t, f)
		spawner, _ := f.create(map[string]any{"title": "spawner"}, "")
		type waiter struct {
			id       string
			metadata map[string]any
			empty    bool
			mixed    bool
			closed   bool
		}
		waiters := []waiter{
			{metadata: nil, empty: true, mixed: false, closed: true},
			{metadata: map[string]any{}, empty: true, mixed: false, closed: true},
			{metadata: map[string]any{"gate": "all-children"}, empty: true, mixed: false, closed: true},
			{metadata: map[string]any{"gate": "any-children"}, empty: true, mixed: true, closed: true},
			{metadata: map[string]any{"gate": "all-children", "also_blocks": true}, empty: false, mixed: false, closed: false},
			{metadata: map[string]any{"gate": "any-children", "also_blocks": true}, empty: false, mixed: false, closed: false},
		}
		for i := range waiters {
			waiters[i].id, _ = f.create(map[string]any{"title": "waiter"}, "")
			f.link(waiters[i].id, spawner, waiters[i].metadata, waiters[i].metadata != nil)
			assertConsumer(t, f, waiters[i].id, spawner, waiters[i].empty)
		}
		openChild, _ := f.create(map[string]any{"title": "direct child"}, spawner)
		commit := f.call("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "linked child, own ontology"}, "_meta": map[string]any{"project": "children"}}, true, "")
		child := resultID(commit, 0)
		f.call("bead.link", map[string]any{"from": ref(child), "to": ref(spawner), "kind": "parent-child"}, true, "")
		for _, waiter := range waiters {
			assertConsumer(t, f, waiter.id, spawner, false)
		}
		f.call("bead.close", map[string]any{"id": child}, true, "")
		if f.bead(child)["status"] != "accepted" {
			t.Fatal("child did not use its own project close slot")
		}
		for _, waiter := range waiters {
			assertConsumer(t, f, waiter.id, spawner, waiter.mixed)
		}
		// Close and reopen the physical SQL store without reseeding source,
		// links, metadata, grants or receipts; rebind the declared same profile.
		if f.engine != "memory" {
			before := f.meta()
			f.close()
			f.open()
			h.bind()
			if !reflect.DeepEqual(before, f.meta()) {
				t.Fatal("physical reopen changed content")
			}
			for _, waiter := range waiters {
				assertConsumer(t, f, waiter.id, spawner, waiter.mixed)
			}
		}
		f.call("bead.close", map[string]any{"id": openChild}, true, "")
		for _, waiter := range waiters {
			assertConsumer(t, f, waiter.id, spawner, waiter.closed)
		}
		f.call("bead.close", map[string]any{"id": spawner}, true, "")
		for _, waiter := range waiters {
			assertConsumer(t, f, waiter.id, spawner, true)
		}
		// A terminal spawner does not erase explicit child predicates. Only
		// absent metadata keeps the historical terminal-spawner fallback.
		f.call("bead.reopen", map[string]any{"id": openChild}, true, "")
		for i, waiter := range waiters {
			assertConsumer(t, f, waiter.id, spawner, i == 0 || waiter.metadata["gate"] == "any-children")
		}
	})
}

func TestB14AuthenticatedMetadataCASReplayRefusalHistoryAndUnlink(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		h := newConsumerHost(t, f)
		waiter, _ := f.create(map[string]any{"title": "waiter"}, "")
		spawner, _ := f.create(map[string]any{"title": "spawner"}, "")
		initialHistory := f.historyCount()
		params := map[string]any{"from": ref(waiter), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{"gate": "any-children"}}
		first := f.call("bead.link", params, true, "")
		key, revision := f.lastKey, first["revision"]
		if f.historyCount() != initialHistory+1 {
			t.Fatal("authenticated metadata link did not publish one changeset")
		}
		assertMetadata(t, f.body(waiter, revision), "any-children", false)
		before := f.meta()
		h.policy.stages = nil
		if replay := f.request("bead.link", params, key, ""); !reflect.DeepEqual(first, replay) || !reflect.DeepEqual(before, f.meta()) || !reflect.DeepEqual(h.policy.stages, []port.AuthorizationStage{port.AuthorizeReplay}) {
			t.Fatal("authenticated receipt replay changed response, state or authority stages")
		}
		// Current grants, not receipt possession, control replay.
		h.policy.grants["user:operator"]["local"] = false
		f.request("bead.link", params, key, "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("revoked policy replay changed content")
		}
		h.policy.grants["user:operator"]["local"] = true
		f.link(waiter, spawner, nil, false)
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("omitted metadata duplicate changed content")
		}
		changed := f.call("bead.link", map[string]any{"from": ref(waiter), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{"also_blocks": true}, "expect": expect(waiter, revision)}, true, "")
		assertMetadata(t, f.currentBody(waiter), "all-children", true)
		assertMetadata(t, f.body(waiter, revision), "any-children", false)
		f.refuse("bead.link", map[string]any{"from": ref(waiter), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{}, "expect": expect(waiter, revision)}, "precondition_failed")
		if f.engine != "memory" {
			before = f.meta()
			f.close()
			f.open()
			h.bind()
			if !reflect.DeepEqual(before, f.meta()) {
				t.Fatal("SQL reopen changed metadata history")
			}
			assertMetadata(t, f.body(waiter, revision), "any-children", false)
			assertMetadata(t, f.body(waiter, changed["revision"]), "all-children", true)
		}
		assertConsumer(t, f, waiter, spawner, false)
		// A denied project must remain denied at either endpoint and through
		// the typed transaction, even when an earlier op would change a title.
		operator := h.token
		h.token = h.creator
		hidden := resultID(f.call("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "hidden"}, "_meta": map[string]any{"project": "hidden"}}, true, ""), 0)
		h.token = operator
		for _, endpoints := range [][2]string{{waiter, hidden}, {hidden, spawner}} {
			f.refuse("bead.link", map[string]any{"from": ref(endpoints[0]), "to": ref(endpoints[1]), "kind": "waits-for", "metadata": map[string]any{"gate": "any-children"}}, "forbidden")
		}
		f.refuse("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "denied project"}, "_meta": map[string]any{"project": "hidden"}}, "forbidden")
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "patch", "target": ref(waiter), "fields": map[string]any{"title": "must roll back"}},
			map[string]any{"kind": "link", "from": ref(waiter), "to": ref(hidden), "linkKind": "waits-for", "metadata": map[string]any{"gate": "any-children"}},
		}}}, "forbidden")
		if f.bead(waiter)["fields"].(map[string]any)["title"] != "waiter" {
			t.Fatal("denied transaction leaked earlier patch")
		}
		// Missing/invalid transport credentials cannot reach the Writer.
		before = f.meta()
		for _, token := range []string{"", "test-invalid-b14-credential"} {
			h.token = token
			f.call("bead.link", params, true, "unauthenticated")
		}
		h.token = operator
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("unauthenticated request changed content")
		}
		beforeCount := f.historyCount()
		applied := f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "link", "from": ref(waiter), "to": ref(spawner), "linkKind": "waits-for", "metadata": map[string]any{"gate": "any-children"}},
		}}}, true, "")
		if f.historyCount() != beforeCount+1 || applied["revision"] == changed["revision"] {
			t.Fatal("authenticated tx.apply metadata rewrite was not one revision")
		}
		assertConsumer(t, f, waiter, spawner, true)
		unlink := map[string]any{"from": ref(waiter), "to": ref(spawner), "kind": "waits-for"}
		removed := f.call("bead.unlink", unlink, true, "")
		unlinkKey := f.lastKey
		before = f.meta()
		if replay := f.request("bead.unlink", unlink, unlinkKey, ""); !reflect.DeepEqual(removed, replay) || !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("authenticated unlink replay changed content")
		}
		if links, ok := f.currentBody(waiter)["links"].([]any); ok && len(links) != 0 {
			t.Fatal("public unlink retained metadata edge")
		}
		assertMetadata(t, f.body(waiter, revision), "any-children", false)
	})
}
