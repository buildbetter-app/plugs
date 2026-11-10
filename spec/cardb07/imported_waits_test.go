package cardb20

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB07ImportedOpaqueWaitsReadiness(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b07ImportedWaits(t, engine, b07OpaqueInputs()) })
	}
}

// The imported closed spawner/open child image is an explicit authored control.
// Ordinary close still refuses a spawner with an open child.
func b07ImportedWaits(t *testing.T, engine string, tokens map[string]string) {
	h := b07NewHost(t, engine)
	raw := b07OpaqueArchive(t, nil, func(members map[string][]byte, manifest map[string]any) {
		var beads bytes.Buffer
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/bead.ndjson"]), []byte{'\n'}) {
			record, body, source, fields := b07SourceParts(t, line)
			id := b07Text(t, record["id"])
			body["status_category"], fields["status"] = json.RawMessage(`"unstarted"`), json.RawMessage(`"open"`)
			delete(body, "result")
			if id == "demo-7" {
				body["status_category"], fields["status"], body["result"] = json.RawMessage(`"completed"`), json.RawMessage(`"closed"`), json.RawMessage(`"success"`)
			}
			beads.Write(b07SourceBytes(t, record, body, source, fields))
			beads.WriteByte('\n')
		}
		members["records/bead.ndjson"] = beads.Bytes()
		record, body, source, fields := b07SourceParts(t, members["records/edge.ndjson"])
		body["type"], fields["type"] = json.RawMessage(`"waits-for"`), json.RawMessage(`"waits-for"`)
		for _, field := range []string{"metadata", "thread_id"} {
			body[field], fields[field] = json.RawMessage(`null`), json.RawMessage(`null`)
		}
		var edges bytes.Buffer
		edges.Write(b07SourceBytes(t, record, body, source, fields))
		edges.WriteByte('\n')
		record["id"], fields["id"] = json.RawMessage(`"parent-demo-8"`), json.RawMessage(`"parent-demo-8"`)
		body["from"], fields["issue_id"] = json.RawMessage(`"demo-8"`), json.RawMessage(`"demo-8"`)
		body["type"], fields["type"] = json.RawMessage(`"parent-child"`), json.RawMessage(`"parent-child"`)
		edges.Write(b07SourceBytes(t, record, body, source, fields))
		edges.WriteByte('\n')
		members["records/edge.ndjson"] = edges.Bytes()
		for _, item := range manifest["kind_registry"].([]any) {
			entry := item.(map[string]any)
			if entry["name"] == "edge" {
				entry["count"] = 2
			}
		}
		for _, item := range manifest["sources"].([]any) {
			entry := item.(map[string]any)
			if entry["family"] == "dependencies" {
				entry["rows"] = 2
			}
		}
	})
	b07RestoreArchive(t, h, raw)
	waiter, spawner := model.RefByID("demo-7.4", model.Natural64(0)), model.RefByID("demo-7", model.Natural64(0))
	ready := []string{"demo-7.4", "demo-8"}
	assert := func(want []string) {
		t.Helper()
		h.requireReady(want)
		s := h.snapshot()
		found := false
		for _, row := range s.Derived[driver.DerivedReadyState] {
			if row.Key != waiter.ID {
				continue
			}
			found = true
			var state struct{ Ready, Blocked bool }
			if json.Unmarshal(row.Value, &state) != nil || state.Ready != b07Has(want, waiter.ID) || state.Blocked == state.Ready {
				t.Fatalf("stored/public waits readiness differs: %s", row.Value)
			}
		}
		if !found {
			t.Fatal("imported waiter derived state absent")
		}
	}
	m := h.metadata(waiter, spawner)
	if m == nil || m.HasPredicate() || m.NativeExtensions["beads.metadata"] != "null" || m.NativeExtensions["beads.thread_id"] != "null" {
		t.Fatal("native null fields must remain opaque and predicate-free")
	}
	assert(ready)
	h.reopen()
	assert(ready)
	params := map[string]any{"from": waiter, "to": spawner, "kind": "waits-for", "metadata": map[string]any{"nativeExtensions": tokens}}
	b07Success(t, h.call("bead.link", "imported-waits-opaque", params))
	m = h.metadata(waiter, spawner)
	for key, value := range tokens {
		if m.NativeExtensions[key] != value {
			t.Fatalf("imported opaque update changed %s", key)
		}
	}
	if m.HasPredicate() {
		t.Fatal("opaque update introduced a waits predicate")
	}
	assert(ready)
	h.reopen()
	assert(ready)
	// Restore-derived rows above describe the imported image. Ordinary writes
	// evaluate readiness from the live graph; they do not refresh that restore
	// table. Check the explicit predicate through the public reader and guard.
	params["metadata"] = map[string]any{"gate": "all-children"}
	b07Success(t, h.call("bead.link", "imported-waits-explicit", params))
	h.requireReady([]string{"demo-8"})
	h.reopen()
	h.requireReady([]string{"demo-8"})
	before := h.snapshot()
	refused := h.call("bead.close", "imported-waits-guard", map[string]any{"id": waiter})
	if refused.Error == nil || refused.Error.Data.Code != "ontology_guard_failed" {
		t.Fatal("explicit waits predicate did not guard close")
	}
	h.same(before)
	if !reflect.DeepEqual(h.metadata(waiter, spawner).NativeExtensions, m.NativeExtensions) {
		t.Fatal("explicit typed predicate erased opaque tokens")
	}
	b07JSON(t, filepath.Join(h.root, "imported-waits-outcomes.json"), map[string]any{"source": "authored closed-spawner/open-child archive", "archive_sha256": b07Hash(raw), "legacy_ready": ready, "opaque_ready": ready, "explicit_predicate_ready": []string{"demo-8"}, "native_history_credit": false})
}

func b07SourceParts(t *testing.T, raw []byte) (record, body, source, fields map[string]json.RawMessage) {
	t.Helper()
	if json.Unmarshal(raw, &record) != nil || json.Unmarshal(record["body"], &body) != nil || json.Unmarshal(record["source"], &source) != nil || json.Unmarshal(source["fields"], &fields) != nil {
		t.Fatal("invalid authored source record")
	}
	return
}

func b07SourceBytes(t *testing.T, record, body, source, fields map[string]json.RawMessage) []byte {
	t.Helper()
	record["body"] = b07RawObject(t, body)
	source["fields"] = b07RawObject(t, fields)
	record["source"] = b07RawObject(t, source)
	return b07RawObject(t, record)
}
