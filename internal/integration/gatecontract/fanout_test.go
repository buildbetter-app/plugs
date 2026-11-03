package gatecontract_test

import "testing"

func TestB14PublicWaitsForEmptyAndMixedChildSemantics(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		spawner, _ := f.create(map[string]any{"title": "spawner"}, "")
		for _, metadata := range []map[string]any{{}, {"gate": "all-children"}, {"gate": "any-children"}} {
			waiter, _ := f.create(map[string]any{"title": "empty fanout"}, "")
			f.link(waiter, spawner, metadata, true)
			f.ready(waiter, true)
			f.call("bead.close", map[string]any{"id": waiter}, true, "")
		}
		openChild, _ := f.create(map[string]any{"title": "direct child"}, spawner)
		closedChild, _ := f.create(map[string]any{"title": "linked child"}, "")
		f.call("bead.link", map[string]any{"from": ref(closedChild), "to": ref(spawner), "kind": "parent-child"}, true, "")
		f.call("bead.close", map[string]any{"id": closedChild}, true, "")
		all, _ := f.create(map[string]any{"title": "all children"}, "")
		anyWaiter, _ := f.create(map[string]any{"title": "any child"}, "")
		both, _ := f.create(map[string]any{"title": "any plus spawner"}, "")
		legacy, _ := f.create(map[string]any{"title": "legacy all fallback"}, "")
		f.link(all, spawner, map[string]any{}, true)
		f.link(anyWaiter, spawner, map[string]any{"gate": "any-children"}, true)
		f.link(both, spawner, map[string]any{"gate": "any-children", "also_blocks": true}, true)
		f.link(legacy, spawner, nil, false)
		for _, id := range []string{all, both, legacy} {
			f.ready(id, false)
			f.refuse("bead.close", map[string]any{"id": id}, "ontology_guard_failed")
		}
		f.ready(anyWaiter, true)
		f.call("bead.close", map[string]any{"id": anyWaiter}, true, "")
		f.call("bead.close", map[string]any{"id": openChild}, true, "")
		f.ready(all, true)
		f.ready(legacy, true)
		f.ready(both, false)
		f.call("bead.close", map[string]any{"id": all}, true, "")
		f.call("bead.close", map[string]any{"id": spawner}, true, "")
		f.ready(both, true)
		f.call("bead.close", map[string]any{"id": both}, true, "")
	})
}

func TestB14PublicWaitsForAlsoBlocksOverridesEmptyAndAny(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		spawner, _ := f.create(map[string]any{"title": "empty spawner"}, "")
		waiter, _ := f.create(map[string]any{"title": "also blocks"}, "")
		f.link(waiter, spawner, map[string]any{"gate": "any-children", "also_blocks": true}, true)
		f.ready(waiter, false)
		f.refuse("bead.close", map[string]any{"id": waiter}, "ontology_guard_failed")
		f.call("bead.close", map[string]any{"id": spawner}, true, "")
		f.ready(waiter, true)
		f.call("bead.close", map[string]any{"id": waiter}, true, "")
	})
}

func TestB14PublicWaitsForMetadataSurvivesUnrelatedWrites(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		source, _ := f.create(map[string]any{"title": "preservation"}, "")
		spawner, _ := f.create(map[string]any{"title": "spawner"}, "")
		f.link(source, spawner, map[string]any{"gate": "any-children", "also_blocks": true}, true)
		f.call("bead.update", map[string]any{"id": source, "fields": map[string]any{"description": "other field"}}, true, "")
		f.call("bead.comment", map[string]any{"id": source, "body": "preserve edge predicate"}, true, "")
		f.call("bead.cancel", map[string]any{"id": source}, true, "")
		f.call("bead.reopen", map[string]any{"id": source}, true, "")
		assertMetadata(t, f.currentBody(source), "any-children", true)
		f.ready(source, false)
	})
}
