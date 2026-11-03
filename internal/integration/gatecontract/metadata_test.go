package gatecontract_test

import (
	"reflect"
	"testing"
)

func TestB14PublicMetadataCASHistoryReplayAndReopen(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		source, _ := f.create(map[string]any{"title": "waiter"}, "")
		spawner, endpointRevision := f.create(map[string]any{"title": "spawner"}, "")
		beforeHistory := f.historyCount()
		params := map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{"gate": "any-children", "also_blocks": true}}
		first := f.call("bead.link", params, true, "")
		firstKey, firstRevision := f.lastKey, first["revision"]
		assertMetadata(t, f.body(source, firstRevision), "any-children", true)
		if f.bead(spawner)["revision"] != endpointRevision || f.historyCount() != beforeHistory+1 {
			t.Fatal("link changed endpoint or published more than one history entry")
		}
		before := f.meta()
		replay := f.request("bead.link", params, firstKey, "")
		if !reflect.DeepEqual(replay, first) || !reflect.DeepEqual(before, f.meta()) || f.lastResponse["_meta"].(map[string]any)["replayed"] != true {
			t.Fatalf("metadata replay changed response/state: %v", f.lastResponse)
		}
		// A fresh duplicate without metadata must retain the predicate and version.
		f.link(source, spawner, nil, false)
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("omitted metadata re-add minted a version")
		}
		assertMetadata(t, f.currentBody(source), "any-children", true)
		f.link(source, spawner, map[string]any{"gate": "any-children", "also_blocks": true}, true)
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("normalized equal metadata re-add minted a version")
		}
		changed := f.call("bead.link", map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{}, "expect": expect(source, firstRevision)}, true, "")
		if changed["revision"] == firstRevision || f.historyCount() != beforeHistory+2 {
			t.Fatal("metadata rewrite did not publish exactly one changed version/history entry")
		}
		assertMetadata(t, f.currentBody(source), "all-children", false)
		assertMetadata(t, f.body(source, firstRevision), "any-children", true)
		before = f.meta()
		f.link(source, spawner, map[string]any{"gate": "all-children", "also_blocks": false}, true)
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("explicit normalized default was not a no-op")
		}
		f.refuse("bead.link", map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{"gate": "any-children"}, "expect": expect(source, firstRevision)}, "precondition_failed")
		if f.engine != "memory" {
			f.close()
			f.open()
			if !reflect.DeepEqual(before, f.meta()) {
				t.Fatal("SQL reopen changed content metadata")
			}
			assertMetadata(t, f.currentBody(source), "all-children", false)
			assertMetadata(t, f.body(source, firstRevision), "any-children", true)
		}
		f.call("bead.unlink", map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for"}, true, "")
		if links, ok := f.currentBody(source)["links"].([]any); ok && len(links) != 0 {
			t.Fatalf("unlink retained metadata edge: %v", links)
		}
	})
}

func assertMetadata(t *testing.T, body map[string]any, gate string, also bool) {
	t.Helper()
	links, ok := body["links"].([]any)
	if !ok || len(links) != 1 {
		t.Fatalf("expected one persisted link: %v", body)
	}
	link := links[0].(map[string]any)
	metadata, ok := link["metadata"].(map[string]any)
	if !ok || metadata["gate"] != gate {
		t.Fatalf("persisted metadata: %v want gate=%s", link, gate)
	}
	got, _ := metadata["also_blocks"].(bool)
	if got != also {
		t.Fatalf("persisted also_blocks=%v want %v: %v", got, also, link)
	}
}

func TestB14PublicMetadataOrderedAtomicChangeSet(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		before := f.historyCount()
		commit := f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "create", "key": "spawner", "type": "issue", "fields": map[string]any{"title": "spawner"}},
			map[string]any{"kind": "create", "key": "waiter", "type": "issue", "fields": map[string]any{"title": "waiter"}},
			map[string]any{"kind": "link", "from": map[string]any{"key": "waiter"}, "to": map[string]any{"key": "spawner"}, "linkKind": "waits-for", "metadata": map[string]any{"gate": "all-children", "also_blocks": true}},
			map[string]any{"kind": "link", "from": map[string]any{"key": "waiter"}, "to": map[string]any{"key": "spawner"}, "linkKind": "waits-for", "metadata": map[string]any{"gate": "any-children"}},
		}}}, true, "")
		waiter := resultID(commit, 1)
		if f.historyCount() != before+1 {
			t.Fatal("ordered metadata batch was not one atomic history entry")
		}
		assertMetadata(t, f.currentBody(waiter), "any-children", false)
		first := f.bead(waiter)["revision"]
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "patch", "target": ref(waiter), "fields": map[string]any{"title": "must roll back"}},
			map[string]any{"kind": "link", "from": ref(waiter), "to": ref(resultID(commit, 0)), "linkKind": "waits-for", "metadata": map[string]any{"gate": "unknown"}},
		}}}, "invalid_params")
		if f.bead(waiter)["revision"] != first || f.bead(waiter)["fields"].(map[string]any)["title"] != "waiter" {
			t.Fatal("invalid metadata batch leaked prior mutation")
		}
		assertMetadata(t, f.currentBody(waiter), "any-children", false)
	})
}

func TestB14PublicMetadataStrictRefusalsLeaveNoWrite(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		source, _ := f.create(map[string]any{"title": "waiter"}, "")
		spawner, _ := f.create(map[string]any{"title": "spawner"}, "")
		for _, bad := range []any{nil, "any-children", []any{}, map[string]any{"gate": ""}, map[string]any{"gate": nil}, map[string]any{"gate": "any"}, map[string]any{"also_blocks": "true"}, map[string]any{"also_blocks": nil}, map[string]any{"spawner_id": spawner}, map[string]any{"Gate": "any-children"}} {
			f.refuse("bead.link", map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": bad}, "invalid_params")
			f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "link", "from": ref(source), "to": ref(spawner), "linkKind": "waits-for", "metadata": bad}}}}, "invalid_params")
		}
		for _, kind := range []string{"blocks", "relates-to", "custom"} {
			f.refuse("bead.link", map[string]any{"from": ref(source), "to": ref(spawner), "kind": kind, "metadata": map[string]any{}}, "invalid_params")
		}
		f.link(source, spawner, map[string]any{}, true)
		f.refuse("bead.unlink", map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{}}, "invalid_params")
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "unlink", "from": ref(source), "to": ref(spawner), "linkKind": "waits-for", "metadata": map[string]any{}}}}}, "invalid_params")
		f.refuse("bead.link", map[string]any{"from": ref(source), "to": ref("missing-spawner"), "kind": "waits-for", "metadata": map[string]any{}}, "not_found")
		f.refuse("bead.link", map[string]any{"from": ref(source), "to": ref(source), "kind": "waits-for", "metadata": map[string]any{}}, "ontology_guard_failed")
		before := f.meta()
		f.call("bead.link", map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{"also_blocks": true}}, false, "invalid_params")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("missing idempotency key wrote metadata")
		}
	})
}
