package postgresacceptance

import "testing"

func TestUnsupportedCardRemainderRefusesExplicitly(t *testing.T) {
	h := startHost(t, openStore(t, freshDatabase(t)), newIDs("pl-a"), newTrace(t))
	create(t, h, "unsupported-seed", "supported anchor", 1)
	before := graph(t, h.store)
	cases := []struct {
		name, method, code string
		params             map[string]any
	}{
		{"B21-wire-batch", "tx.apply", "invalid_params", map[string]any{"ops": []any{}}},
		{"B08-wire-reopen", "bead.reopen", "method_not_found", map[string]any{"id": ref("pl-a")}},
		{"B18-wire-versions", "history.versions", "method_not_found", map[string]any{"ref": ref("pl-a")}},
		{"B01-saved-view", "bead.ready", "invalid_params", map[string]any{"viewId": "not-installed"}},
		{"B18-query-language", "history.query", "invalid_params", map[string]any{"asOf": map[string]any{"revision": 1}, "filter": map[string]any{"query": "status:todo"}}},
		{"B01-parented-create", "bead.create", "invalid_params", map[string]any{"type": "issue", "fields": map[string]any{"title": "child"}, "parent": ref("pl-a")}},
		{"B08-direct-status-patch", "bead.update", "ontology_use_slot_verb", map[string]any{"id": ref("pl-a"), "fields": map[string]any{"status": "done"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requestKey := key("unsupported-" + tc.name)
			refused(t, h.call(t, tc.method, tc.params, requestKey), tc.code)
			unchanged(t, before, graph(t, h.store))
			if receipt(t, h.store, tc.method, requestKey, agent) != nil {
				t.Fatal("unsupported operation published a success receipt")
			}
		})
	}
	t.Log("unsupported remainders stay explicit typed refusals; direct Writer coverage does not imply an installed RPC method")
}
