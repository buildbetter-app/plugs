package cardb20

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func b07OpaqueInputs() map[string]string {
	return map[string]string{
		"beads.thread_id":    `"escaped \\ \u03b1 \"tail\"  "`,
		"beads.future_empty": `""`, "beads.future_null": `null`,
		"beads.future_number": `9007199254740993`, "beads.future_exponent": `1.00e+02`,
		"beads.future_object": `{ "n": 1.0, "a": [ null, "\u0061", { "space": "  " } ] }`,
	}
}
func TestB07OpaqueEdgeTokensAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			b07OpaquePublic(t, engine, b07OpaqueInputs())
			b07OpaqueImport(t, engine, b07OpaqueInputs())
		})
	}
}
func (h *b07Host) metadata(from, to model.Ref) *model.LinkMetadata {
	h.t.Helper()
	s := h.snapshot()
	for _, row := range s.Rows {
		if row.Ref.ID != from.ID {
			continue
		}
		doc, e := pipeline.DecodeDocument(row.Body)
		if e != nil {
			h.t.Fatal(e)
		}
		for _, edge := range doc.Links {
			if edge.To.ID == to.ID {
				return edge.Metadata
			}
		}
	}
	h.t.Fatal("metadata edge absent")
	return nil
}
func b07OpaquePublic(t *testing.T, engine string, native map[string]string) {
	h := b07NewHost(t, engine)
	from, to := h.create("opaque-source", nil), h.create("opaque-target", nil)
	tokens := b07OpaqueInputs()
	for key, value := range native {
		tokens[key] = value
	}
	ready := h.ready()
	params := map[string]any{"from": from, "to": to, "kind": "custom kind", "metadata": map[string]any{"nativeExtensions": tokens}}
	first := h.call("bead.link", "opaque-add", params)
	b07Success(t, first)
	h.requireReady(ready)
	assert := func() {
		t.Helper()
		m := h.metadata(from, to)
		if m == nil || m.HasPredicate() || !reflect.DeepEqual(m.NativeExtensions, tokens) {
			t.Fatalf("opaque bytes/keyset changed: %+v", m)
		}
	}
	assert()
	before := h.snapshot()
	h.quiet(before, "opaque-repeat", h.call("bead.link", "opaque-repeat", params))
	h.reopen()
	assert()
	before = h.snapshot()
	replay := h.call("bead.link", "opaque-add", params)
	if replay.Error != nil || replay.Meta["replayed"] != true || !bytes.Equal(first.Result, replay.Result) {
		t.Fatal("opaque original response did not replay")
	}
	h.same(before)
	delete(params, "metadata")
	before = h.snapshot()
	h.quiet(before, "opaque-omitted", h.call("bead.link", "opaque-omitted", params))
	assert()
	params["kind"] = "blocks"
	before = h.snapshot()
	bad := h.call("bead.link", "opaque-retype", params)
	if bad.Error == nil || bad.Error.Data.Code != "ontology_guard_failed" {
		t.Fatal("opaque retype not refused")
	}
	h.same(before)
	assert()
	params["kind"] = "custom kind"
	tokens["beads.thread_id"] = `null`
	tokens["beads.future_number"] = `9007199254740995`
	params["metadata"] = map[string]any{"nativeExtensions": tokens}
	b07Success(t, h.call("bead.link", "opaque-change", params))
	h.requireReady(ready)
	h.reopen()
	assert()
	for _, field := range []string{"id", "issue_id", "depends_on_id", "depends_on_issue_id", "depends_on_wisp_id", "depends_on_external", "type", "created_at", "created_by"} {
		before := h.snapshot()
		refusal := h.call("bead.link", "opaque-represented-"+field, map[string]any{"from": from, "to": to, "kind": "custom kind", "metadata": map[string]any{"nativeExtensions": map[string]string{"beads." + field: `"cannot-override"`}}})
		if refusal.Error == nil || refusal.Error.Data.Code != "invalid_params" || !strings.Contains(refusal.Error.Message+refusal.Error.Data.Hint, "nativeExtensions") {
			t.Fatal("represented edge field was not refused", field, refusal.Error)
		}
		h.same(before)
	}
	for _, test := range []struct {
		name  string
		value any
	}{{"truncated", `{"x":`}, {"trailing", `null true`}, {"empty", ""}, {"wrong_wire_type", map[string]any{"x": 1}}} {
		t.Run(test.name, func(t *testing.T) {
			old := h.t
			h.t = t
			defer func() { h.t = old }()
			before := h.snapshot()
			direct := h.call("bead.link", "malformed-direct-"+test.name, map[string]any{"from": from, "to": to, "kind": "custom kind", "metadata": map[string]any{"nativeExtensions": map[string]any{"beads.future_invalid": test.value}}})
			if direct.Error == nil || direct.Error.Data.Code != "invalid_params" || !strings.Contains(direct.Error.Message+direct.Error.Data.Hint, "nativeExtensions") {
				t.Fatalf("field-naming direct malformed refusal missing: %+v", direct.Error)
			}
			h.same(before)
			ops := []any{map[string]any{"kind": "patch", "target": from, "fields": map[string]any{"title": "must-not-publish"}}, map[string]any{"kind": "link", "from": from, "to": to, "linkKind": "custom kind", "metadata": map[string]any{"nativeExtensions": map[string]any{"beads.future_invalid": test.value}}}}
			r := h.call("tx.apply", "malformed-"+test.name, map[string]any{"changeSet": map[string]any{"ops": ops}})
			path := "nativeExtensions"
			if test.name == "wrong_wire_type" {
				// The outer change-set union rejects this arm before model decoding.
				// The direct route above separately proves the exact field error.
				path = "params.changeSet.ops[1]"
			}
			if r.Error == nil || r.Error.Data.Code != "invalid_params" || !strings.Contains(r.Error.Message+r.Error.Data.Hint, path) {
				t.Fatalf("field-naming malformed refusal missing: %+v", r.Error)
			}
			h.same(before)
			h.requireReady(ready)
		})
	}
	b07JSON(t, filepath.Join(h.root, "opaque-token-outcomes.json"), map[string]any{"source": "explicit authored controls plus supplied captured native tokens", "tokens": tokens, "round_trip": "RPC Writer store reopen replay", "native_history_credit": false})
}

// The archive is a declared authored shape control. Captured native tokens may
// be supplied to this real import/reverse route, but it invents no native history.
func b07OpaqueArchive(t *testing.T, tokens map[string]string, configure ...func(map[string][]byte, map[string]any)) []byte {
	t.Helper()
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	raw, e := os.ReadFile(filepath.Join(root, "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar"))
	if e != nil {
		t.Fatal(e)
	}
	members := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, e := r.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(r)
		if e != nil {
			t.Fatal(e)
		}
		members[hdr.Name] = data
	}
	var manifest map[string]any
	if e := json.Unmarshal(members["manifest.json"], &manifest); e != nil {
		t.Fatal(e)
	}
	manifest["history"] = "unavailable"
	project := manifest["projects"].([]any)[0].(map[string]any)
	project["id_mode"] = "hash"
	project["hash_length"] = 1
	kept := [][]byte{}
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
		var record struct {
			Source struct {
				Table string `json:"table"`
			} `json:"source"`
		}
		if json.Unmarshal(line, &record) != nil {
			t.Fatal("invalid declared fixture")
		}
		if record.Source.Table != "issue_versions" {
			kept = append(kept, line)
		}
	}
	members["records/beads-archive.ndjson"] = append(bytes.Join(kept, []byte{'\n'}), '\n')
	for _, v := range manifest["sources"].([]any) {
		s := v.(map[string]any)
		if s["family"] == "issue_versions" {
			s["rows"] = 0
		}
	}
	for _, v := range manifest["kind_registry"].([]any) {
		k := v.(map[string]any)
		if k["name"] == "beads-archive" {
			k["count"] = len(kept)
		}
		if k["name"] == "edge" {
			k["version"] = 3
		}
	}
	// Work with raw members so even exponent and escaped-string spellings are
	// authored directly into the archive, before the production reader sees it.
	var record, body, source, fields map[string]json.RawMessage
	if json.Unmarshal(members["records/edge.ndjson"], &record) != nil || json.Unmarshal(record["body"], &body) != nil || json.Unmarshal(record["source"], &source) != nil || json.Unmarshal(source["fields"], &fields) != nil {
		t.Fatal("invalid edge fixture")
	}
	body["type"], fields["type"] = json.RawMessage(`"custom kind"`), json.RawMessage(`"custom kind"`)
	for key, token := range tokens {
		field := strings.TrimPrefix(key, "beads.")
		fields[field] = json.RawMessage(token)
		if field == "thread_id" {
			body[field] = json.RawMessage(token)
		}
	}
	fields["metadata"] = json.RawMessage(` { "extra": "kept", "number": 1.00e+02 } `)
	body["metadata"] = fields["metadata"]
	record["body"] = b07RawObject(t, body)
	source["fields"] = b07RawObject(t, fields)
	record["source"] = b07RawObject(t, source)
	members["records/edge.ndjson"] = append(b07RawObject(t, record), '\n')
	for _, apply := range configure {
		apply(members, manifest)
	}
	files := manifest["files"].(map[string]any)
	for path, data := range members {
		if path != "manifest.json" {
			files[path] = map[string]any{"sha256": b07Hash(data), "size": len(data)}
		}
	}
	members["manifest.json"], e = json.Marshal(manifest)
	if e != nil {
		t.Fatal(e)
	}
	members["manifest.json"] = append(members["manifest.json"], '\n')
	paths := []string{}
	for path := range members {
		if path != "manifest.json" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	paths = append([]string{"manifest.json"}, paths...)
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, path := range paths {
		data := members[path]
		if e := w.WriteHeader(&tar.Header{Name: path, Mode: 0600, Typeflag: tar.TypeReg, Size: int64(len(data)), Format: tar.FormatUSTAR}); e != nil {
			t.Fatal(e)
		}
		if _, e := w.Write(data); e != nil {
			t.Fatal(e)
		}
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}
func b07RawObject(t *testing.T, fields map[string]json.RawMessage) []byte {
	t.Helper()
	keys := []string{}
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		name, _ := json.Marshal(key)
		b.Write(name)
		b.WriteByte(':')
		b.Write(fields[key])
	}
	b.WriteByte('}')
	return b.Bytes()
}
func b07OpaqueImport(t *testing.T, engine string, tokens map[string]string) {
	h := b07NewHost(t, engine)
	raw := b07OpaqueArchive(t, tokens)
	b07RestoreArchive(t, h, raw)
	archive, e := dumpformat.Read(bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	edge := archive.Records("edge")[0]
	want, e := dumpformat.NativeEdgeExtensions(edge)
	if e != nil {
		t.Fatal(e)
	}
	from, to := model.RefByID(edge.Body["from"].(string), model.Natural64(0)), model.RefByID(edge.Body["to"].(string), model.Natural64(0))
	m := h.metadata(from, to)
	if m == nil || !reflect.DeepEqual(m.NativeExtensions, want) {
		t.Fatal("import dropped or normalized edge native tokens")
	}
	h.reopen()
	m = h.metadata(from, to)
	if !reflect.DeepEqual(m.NativeExtensions, want) {
		t.Fatal("physical reopen changed edge tokens")
	}
	before := h.snapshot()
	h.quiet(before, "imported-readd", h.call("bead.link", "imported-readd", map[string]any{"from": from, "to": to, "kind": "custom kind"}))
	h.reopen()
	before = h.snapshot()
	reversed, e := converterroundtrip.ReverseRestoredWithOptions(h.ctx, h.runtime.Store, reverseconverter.Options{All: true})
	if e != nil {
		t.Fatal(e)
	}
	h.same(before)
	if e := os.WriteFile(filepath.Join(h.root, "reverse.jsonl"), reversed.Result.JSONL, 0600); e != nil {
		t.Fatal(e)
	}
	found := false
	decoder := json.NewDecoder(bytes.NewReader(reversed.Result.JSONL))
	for {
		var row struct {
			ID           string                       `json:"id"`
			Dependencies []map[string]json.RawMessage `json:"dependencies"`
		}
		e := decoder.Decode(&row)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if row.ID != from.ID {
			continue
		}
		for _, dep := range row.Dependencies {
			if b07Text(t, dep["depends_on_id"]) != to.ID {
				continue
			}
			found = true
			for key, token := range want {
				field := strings.TrimPrefix(key, "beads.")
				got := dep[field]
				if field == "metadata" {
					var envelope map[string]json.RawMessage
					if e := json.Unmarshal([]byte(b07Text(t, dep["metadata"])), &envelope); e != nil {
						t.Fatal(e)
					}
					got = envelope["_plug_metadata"]
				}
				if string(got) != token {
					t.Fatalf("reverse token %s changed: %q != %q", key, got, token)
				}
			}
		}
	}
	if !found {
		t.Fatal("reverse edge missing")
	}
	b07JSON(t, filepath.Join(h.root, "import-reverse-outcomes.json"), map[string]any{"archive_sha256": archive.SHA256(), "reverse_sha256": reversed.Result.Digest, "tokens": want, "source": "authored archive with supplied raw tokens; no source history"})
}

func b07RestoreArchive(t *testing.T, h *b07Host, raw []byte) {
	t.Helper()
	h.config.Project = "demo"
	if e := os.WriteFile(filepath.Join(h.root, "authored-import.tar"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	inspected, e := converterroundtrip.InspectImport(h.ctx, bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b07-public-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: inspected.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: inspected.ArchiveSHA256, DerivedDigest: inspected.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot}
	if _, e := converterroundtrip.RestoreImportWithLease(h.ctx, bytes.NewReader(raw), h.runtime.Store, h.runtime.Lease, profile); e != nil {
		t.Fatal(e)
	}
}
