package b13memory_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const explicitKey = "  MiXeD Key/Ω café ✓  "
const originalBody = "  VerBaTiM value\n東京\t🌱  "
const replacedBody = "  CURRENT text café 東京\n"
const hiddenBody = "B13-hidden-secret-content"

func key(label string) string {
	sum := sha256.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])
}

// Independent public/persisted value projection; no product memory decoder.
type memoryValue struct {
	ID       string         `json:"id"`
	Key      string         `json:"key"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Revision model.Revision `json:"revision"`
}

func memoryRow(t *testing.T, raw []byte) memoryValue {
	t.Helper()
	fields := decode[map[string]json.RawMessage](t, raw)
	for _, name := range []string{"id", "key", "content"} {
		if value, exists := fields[name]; !exists || bytes.Equal(value, []byte("null")) {
			t.Fatalf("memory omitted required %s (empty is present): %s", name, raw)
		}
	}
	return decode[memoryValue](t, raw)
}

func memoryRows(t *testing.T, rows []json.RawMessage) []memoryValue {
	t.Helper()
	if rows == nil {
		t.Fatal("memory collection is missing/null rather than an explicit array")
	}
	out := make([]memoryValue, 0, len(rows))
	for _, row := range rows {
		out = append(out, memoryRow(t, row))
	}
	return out
}

func (f *fixture) verifyConnected(want map[string]memoryValue, beadID string) {
	f.t.Helper()
	before := f.state()
	expected := make([]memoryValue, 0, len(want))
	for id, value := range want {
		raw := result(f.t, f.call("memory.get", map[string]any{"id": id}, ""))
		if got := memoryRow(f.t, raw); !reflect.DeepEqual(got, value) {
			f.t.Fatalf("public memory.get changed value: got=%+v want=%+v", got, value)
		}
		row := stored(f.t, before, id)
		persisted := memoryRow(f.t, row.Body)
		persisted.Revision = row.Revision
		if row.Kind != "memory" || !reflect.DeepEqual(persisted, value) {
			f.t.Fatalf("public value differs from persisted core record: row=%+v want=%+v", row, value)
		}
		expected = append(expected, value)
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].Key != expected[j].Key {
			return expected[i].Key < expected[j].Key
		}
		return expected[i].ID < expected[j].ID
	})
	// Read every public page, including the nonnil empty page after deletion.
	seen, cursor := make([]memoryValue, 0), ""
	for pages := 0; ; pages++ {
		if pages > len(expected)+1 {
			f.t.Fatal("memory pagination did not terminate")
		}
		params := map[string]any{"limit": 2}
		if cursor != "" {
			params["cursor"] = cursor
		}
		page := decode[struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"nextCursor"`
		}](f.t, result(f.t, f.call("memory.list", params, "")))
		// The admitted MemoryPage exposes items/nextCursor; each item's
		// revision is checked against the independently pinned record above.
		seen = append(seen, memoryRows(f.t, page.Items)...)
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) == 0 || page.NextCursor == cursor {
			f.t.Fatal("memory cursor did not advance")
		}
		cursor = page.NextCursor
	}
	brief := decode[struct {
		Memories []json.RawMessage `json:"memories"`
		ReadyTop []struct {
			ID string `json:"id"`
		} `json:"readyTop"`
	}](f.t, result(f.t, f.call("agent.brief", map[string]any{"budgetTokens": 1000}, "")))
	if got := memoryRows(f.t, brief.Memories); !reflect.DeepEqual(got, expected) || !reflect.DeepEqual(seen, expected) {
		f.t.Fatalf("P85 memory.get/list/brief disagree with literal+persisted values: list=%+v brief=%+v want=%+v", seen, got, expected)
	}
	if beadID == "" && len(brief.ReadyTop) != 0 || beadID != "" && (len(brief.ReadyTop) != 1 || brief.ReadyTop[0].ID != beadID) {
		f.t.Fatalf("core memories entered the brief's ordinary-bead plane: %+v", brief.ReadyTop)
	}
	requireSame(f.t, before, f.state())
}

func (f *fixture) search(query string, ids ...string) {
	f.t.Helper()
	page := decode[struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor string            `json:"nextCursor"`
	}](f.t, result(f.t, f.call("memory.list", map[string]any{"query": query, "limit": 100}, "")))
	got := []string{}
	for _, row := range memoryRows(f.t, page.Items) {
		got = append(got, row.ID)
	}
	if ids == nil {
		ids = []string{}
	}
	if page.NextCursor != "" || !reflect.DeepEqual(got, ids) {
		f.t.Fatalf("case-insensitive key-or-value search %q: got=%v want=%v", query, got, ids)
	}
}

func (f *fixture) seedExistingRows() model.Revision {
	f.t.Helper()
	// Explicitly authored storage fixtures, not public save or native/reference
	// evidence. Existing empty memory must be readable while new blank saves
	// stay refused. Config bodies deliberately look like matching memory rows.
	changes := []conformance.FixtureChange{
		{Ref: ref("b13-empty"), Kind: "memory", Op: model.OpCreate, Body: encode(f.t, map[string]any{"id": "b13-empty", "key": "empty legacy ✓", "content": ""})},
		{Ref: ref("b13-setting"), Kind: "config", Op: model.OpCreate, Body: encode(f.t, map[string]any{"id": "b13-setting", "key": explicitKey, "content": originalBody, "value": "settings must survive memory forget"})},
		{Ref: ref("b13-generic-setting"), Kind: "config", Op: model.OpCreate, Body: encode(f.t, map[string]any{"id": "b13-generic-setting", "key": "kv.memory.config-only-decoy", "content": "config-only-decoy", "value": "generic setting"})},
	}
	set, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, changes)
	if err != nil {
		f.t.Fatal(err)
	}
	return set.Revision
}

func (f *fixture) preserveOtherPlanes(before durableState, beadID string) {
	f.t.Helper()
	after := f.state()
	for _, id := range []string{"b13-setting", "b13-generic-setting", beadID} {
		if !reflect.DeepEqual(stored(f.t, before, id), stored(f.t, after, id)) || !reflect.DeepEqual(before.Versions[id], after.Versions[id]) {
			f.t.Fatalf("memory operation changed setting/ordinary bead %s", id)
		}
	}
}

func TestB13PublicMemoryBriefLifecycle(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.verifyConnected(map[string]memoryValue{}, "")
			seedRevision := f.seedExistingRows()
			want := map[string]memoryValue{"b13-empty": {ID: "b13-empty", Key: "empty legacy ✓", Content: "", Revision: seedRevision}}
			f.verifyConnected(want, "")
			f.refuse("memory.get", map[string]any{"id": "b13-missing"}, "", "not_found")
			created, _ := f.accept("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": explicitKey}}, key("bead-create"), model.OpCreate)
			beadID := created.Results[0].Ref.ID
			planes := f.state()

			metadata := map[string]any{"literal.dot/✓": " kept ", "nested": map[string]any{"source": "public"}}
			firstParams := map[string]any{"key": explicitKey, "content": originalBody, "metadata": metadata}
			first, firstReply := f.accept("memory.save", firstParams, key("save-first"), model.OpCreate)
			id := first.Results[0].Ref.ID
			want[id] = memoryValue{ID: id, Key: explicitKey, Content: originalBody, Metadata: metadata, Revision: first.Revision}
			derived, _ := f.accept("memory.save", map[string]any{"content": "Keep these exact\nwords."}, key("save-derived"), model.OpCreate)
			derivedID := derived.Results[0].Ref.ID
			want[derivedID] = memoryValue{ID: derivedID, Key: "keep-these-exact-words", Content: "Keep these exact\nwords.", Revision: derived.Revision}
			hidden, _ := f.accept("memory.save", map[string]any{"key": "hidden", "content": hiddenBody}, key("save-hidden"), model.OpCreate)
			hiddenID := hidden.Results[0].Ref.ID
			f.verifyConnected(want, beadID)
			f.search("mIXeD kEY/ω CAFÉ", id)
			f.search("vErBaTiM VaLuE", id)
			f.search("東京", id)
			f.search("config-only-decoy")
			f.search("HIDDEN")
			f.search("absent-query")
			f.refuse("memory.get", map[string]any{"id": hiddenID}, "", "not_found")
			f.refuse("memory.get", map[string]any{"id": beadID}, "", "not_found")
			f.refuse("memory.get", map[string]any{"id": "b13-setting"}, "", "not_found")
			f.refuse("bead.get", map[string]any{"id": ref(id)}, "", "not_found")
			f.refuse("bead.update", map[string]any{"id": ref(id), "fields": map[string]any{"title": "wrong-plane write"}}, key("bead-update-memory"), "invalid_params")

			replaceParams := map[string]any{"key": explicitKey, "content": replacedBody, "metadata": metadata}
			replaced, replaceReply := f.accept("memory.save", replaceParams, key("save-replace"), model.OpPatch)
			if replaced.Results[0].Ref.ID != id {
				t.Fatal("same-key replacement allocated a different memory ID")
			}
			want[id] = memoryValue{ID: id, Key: explicitKey, Content: replacedBody, Metadata: metadata, Revision: replaced.Revision}
			f.verifyConnected(want, beadID)
			f.search("current TEXT", id)
			f.search("verbatim value")
			historyParams := map[string]any{"ref": ref(id), "asOf": map[string]any{"revision": first.Revision}}
			history := decode[struct {
				Body json.RawMessage `json:"body"`
			}](t, result(t, f.call("history.get", historyParams, "")))
			if got := memoryRow(t, history.Body); got.Key != explicitKey || got.Content != originalBody || !reflect.DeepEqual(got.Metadata, metadata) {
				t.Fatalf("replacement lost the historical literal body: %+v", got)
			}
			// Explicit keys admit content that cannot supply its own ASCII slug.
			neighbor, _ := f.accept("memory.save", map[string]any{"key": explicitKey + "suffix", "content": "日本語 !!!"}, key("save-neighbor"), model.OpCreate)
			neighborID := neighbor.Results[0].Ref.ID
			want[neighborID] = memoryValue{ID: neighborID, Key: explicitKey + "suffix", Content: "日本語 !!!", Revision: neighbor.Revision}

			// Refusal cases run against the populated connected state. The oracle
			// checks all earlier receipts plus the new refused scope each time.
			for _, tc := range []struct {
				name   string
				params map[string]any
			}{
				{"empty-content", map[string]any{"content": ""}},
				{"empty-existing-row", map[string]any{"key": "empty legacy ✓", "content": ""}},
				{"blank-content", map[string]any{"key": explicitKey, "content": " \t\n"}},
				{"underivable", map[string]any{"content": "!!!"}},
				{"unicode-underivable", map[string]any{"content": "日本語"}},
				{"blank-key", map[string]any{"key": " \t", "content": "valid content"}},
				{"wrong-key-type", map[string]any{"key": 123, "content": "valid content"}},
				{"wrong-metadata-type", map[string]any{"key": explicitKey, "content": "must not land", "metadata": []string{"array"}}},
			} {
				f.refuse("memory.save", tc.params, key(tc.name), "invalid_params")
			}
			f.refuse("memory.save", map[string]any{"key": explicitKey, "content": "must not land"}, "", "invalid_params")
			f.refuse("memory.save", map[string]any{"key": explicitKey, "content": "stale must not land", "expect": []model.Precondition{model.PreconditionByRevision(ref(id), first.Revision)}}, key("stale-replace"), "precondition_failed")
			f.refuse("memory.save", map[string]any{"key": explicitKey, "content": "changed request under old key"}, key("save-first"), "idempotency_key_reused")
			for _, target := range []string{"b13-missing", "b13-public", beadID, "b13-setting", "b13-generic-setting"} {
				f.refuse("memory.delete", map[string]any{"id": target}, key("refused-delete-"+target), "not_found")
			}
			f.refuse("memory.delete", map[string]any{"id": ref(id)}, key("wrong-memory-id-shape"), "invalid_params")
			f.policyError = true
			f.refuse("agent.brief", map[string]any{}, "", "authz_unavailable")
			f.refuse("memory.list", map[string]any{}, "", "authz_unavailable")
			f.policyError = false
			f.verifyConnected(want, beadID)
			f.preserveOtherPlanes(planes, beadID)

			f.reopen()
			f.verifyConnected(want, beadID)
			// Replay an old save after replacement: it returns the old commit,
			// while all current public reads must retain the replacement.
			f.replay("memory.save", firstParams, key("save-first"), firstReply)
			f.replay("memory.save", replaceParams, key("save-replace"), replaceReply)
			f.verifyConnected(want, beadID)
			deleteParams := map[string]any{"id": id}
			_, deleteReply := f.accept("memory.delete", deleteParams, key("delete-exact"), model.OpDelete)
			delete(want, id)
			f.accept("memory.delete", map[string]any{"id": "b13-empty"}, key("delete-empty"), model.OpDelete)
			delete(want, "b13-empty")
			f.refuse("memory.get", map[string]any{"id": id}, "", "not_found")
			f.refuse("memory.get", map[string]any{"id": "b13-empty"}, "", "not_found")
			f.refuse("memory.delete", deleteParams, key("delete-absent-again"), "not_found")
			f.verifyConnected(want, beadID)
			f.preserveOtherPlanes(planes, beadID)
			f.reopen()
			f.replay("memory.delete", deleteParams, key("delete-exact"), deleteReply)
			f.replay("memory.save", firstParams, key("save-first"), firstReply)
			f.verifyConnected(want, beadID)
			for _, target := range []string{derivedID, neighborID, hiddenID} {
				f.accept("memory.delete", map[string]any{"id": target}, key("delete-rest-"+target), model.OpDelete)
				delete(want, target)
			}
			f.verifyConnected(want, beadID) // explicit [] in both list and brief
			recreated, _ := f.accept("memory.save", firstParams, key("recreate"), model.OpCreate)
			newID := recreated.Results[0].Ref.ID
			if newID == id {
				t.Fatal("forget/recreate resurrected the old identity")
			}
			want[newID] = memoryValue{ID: newID, Key: explicitKey, Content: originalBody, Metadata: metadata, Revision: recreated.Revision}
			f.refuse("history.get", historyParams, "", "not_found")
			f.refuse("memory.get", map[string]any{"id": id}, "", "not_found")
			f.reopen()
			f.verifyConnected(want, beadID)
			f.preserveOtherPlanes(planes, beadID)
			t.Logf("B13 connected public memory/P85 assertions complete for %s; native/reference and full-card qualification excluded", engine)
		})
	}
}
