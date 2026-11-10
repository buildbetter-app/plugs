package dumpformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMemoryArchivePreservesVerbatimKeyAndTrimmedNeighbor(t *testing.T) {
	fixture := newMergeFixture(t, "pl")
	const key = "  Source Key/Ω e\u0301  "
	const value = " \tVerbatim\n東京 e\u0301  "
	neighbor := strings.TrimSpace(key)
	fixture.addConfig("config", "kv.memory."+key, value, "public")
	fixture.addConfig("config", "kv.memory."+neighbor, "distinct trimmed neighbor", "public")
	archive, err := Read(bytes.NewReader(fixture.bytes(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{key: value, neighbor: "distinct trimmed neighbor"}
	seen := map[string]bool{}
	for _, row := range archive.Records("memory") {
		value, ok := want[row.ID]
		if !ok {
			continue // The existing fixture memory remains separate.
		}
		if seen[row.ID] || row.Body["key"] != row.ID || row.Body["value"] != value || row.Source.Fields["key"] != "kv.memory."+row.ID || row.Source.Fields["value"] != value {
			t.Fatalf("memory identity or source value changed: %+v", row)
		}
		seen[row.ID] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("verbatim key aliased its trimmed neighbor: %+v", seen)
	}
}

func TestMemoryArchiveIdentityExceptionsStayNarrow(t *testing.T) {
	for _, test := range []struct{ name, key string }{
		{"empty", ""}, {"ascii-blank", " \t "}, {"unicode-blank", "\u2003\u00a0"}, {"nul", "key\x00tail"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMergeFixture(t, "pl")
			fixture.addConfig("config", "kv.memory."+test.key, "public value", "public")
			if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err == nil {
				t.Fatal("invalid memory key admitted")
			}
		})
	}
	t.Run("source-key-mismatch", func(t *testing.T) {
		fixture := newMergeFixture(t, "pl")
		fixture.addConfig("config", "kv.memory. spaced Ω ", "public value", "public")
		row := &fixture.records["memory"][len(fixture.records["memory"])-1]
		row.ID, row.Body["key"] = "spaced Ω", "spaced Ω"
		if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err == nil {
			t.Fatal("trimmed alias admitted over exact source key")
		}
	})
	t.Run("ordinary-config-remains-strict", func(t *testing.T) {
		fixture := newMergeFixture(t, "pl")
		fixture.addConfig("config", "setting \t", "public value", "public")
		if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err == nil {
			t.Fatal("padded non-memory identity admitted")
		}
	})
}

func TestExternalDependencyArchiveKeepsTypedVocabulary(t *testing.T) {
	for _, test := range []struct {
		name, targetKind, kind string
		admitted               bool
	}{
		{"local-blocks", "depends_on_issue_id", "blocks", true},
		{"local-waits-for-stays-unsupported", "depends_on_issue_id", "waits-for", false},
		{"external-waits-for", "depends_on_external", "waits-for", true},
		{"external-conditional", "depends_on_external", "conditional-blocks", true},
		{"external-opaque-kind", "depends_on_external", strings.Repeat("x", 32), true},
		{"external-empty-kind", "depends_on_external", "", false},
		{"external-overlong-kind", "depends_on_external", strings.Repeat("x", 33), false},
		{"external-overlong-utf8-kind", "depends_on_external", strings.Repeat("é", 17), false},
		{"workflow-stays-unsupported", "depends_on_wisp_id", "blocks", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMergeFixture(t, "pl")
			edge := &fixture.records["edge"][0]
			for _, arm := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
				edge.Source.Fields[arm] = nil
			}
			target := "pl-2"
			if test.targetKind == "depends_on_external" {
				target = " external:tracker/1 "
			}
			edge.Source.Fields[test.targetKind] = target
			edge.Source.Fields["type"] = test.kind
			edge.Body["target_kind"], edge.Body["to"], edge.Body["type"] = test.targetKind, target, test.kind
			archive, err := Read(bytes.NewReader(fixture.bytes(t, nil)))
			if !test.admitted {
				if err == nil {
					t.Fatal("unsupported typed dependency admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := archive.Records("edge")[0]
			if got.Body["target_kind"] != test.targetKind || got.Body["to"] != target || got.Body["type"] != test.kind {
				t.Fatalf("typed external identity changed: %+v", got.Body)
			}
		})
	}
}

func TestIssueAwaitIDIsOpaqueTextOnly(t *testing.T) {
	fixture := newMergeFixture(t, "pl")
	fixture.records["bead"][0].Source.Fields["await_id"] = "opaque-await/v1\x00"
	if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err != nil {
		t.Fatalf("opaque await_id was refused: %v", err)
	}

	fixture.records["bead"][0].Source.Fields["await_id"] = json.Number("1")
	if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err == nil || !strings.Contains(err.Error(), "await_id must be text") {
		t.Fatalf("non-text await_id was accepted: %v", err)
	}
}

func TestOpaqueIssueFieldsAcceptActiveValuesWithoutInterpretation(t *testing.T) {
	fixture := newMergeFixture(t, "pl")
	values := map[string]any{
		"agent_state": "agent-state",
		"await_id":    "await-id",
		"await_type":  "human",
		"ephemeral":   json.Number("1"),
		"event_kind":  nil,
		"hook_bead":   "pl-2",
		"is_template": json.Number("1"),
		"mol_type":    "formula",
		"payload":     map[string]any{"state": "waiting"},
		"role_bead":   "pl-3",
		"role_type":   "agent",
		"target":      "pl-4",
		"timeout_ns":  json.Number("9007199254740993"),
		"waiters":     []any{"pl-2", nil},
		"wisp_type":   "run",
		"work_type":   "workflow",
	}
	for field, value := range values {
		fixture.records["bead"][0].Source.Fields[field] = value
	}
	if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err != nil {
		t.Fatalf("opaque issue fields were interpreted or refused: %v", err)
	}
}

func TestDetachedValuesPreserveNestedNullAndNumber(t *testing.T) {
	a := &Archive{byKind: map[string][]Record{"bead": {{Body: Row{"nested": map[string]any{"null": nil, "number": json.Number("9007199254740993"), "array": []any{"original"}}}}}}}
	got := a.Records("bead")
	nested := got[0].Body["nested"].(map[string]any)
	if nested["null"] != nil || nested["number"] != json.Number("9007199254740993") {
		t.Fatalf("values=%#v", nested)
	}
	nested["number"] = json.Number("1")
	nested["array"].([]any)[0] = "changed"
	if reflect.DeepEqual(got, a.Records("bead")) {
		t.Fatal("accessor did not detach nested JSON")
	}
}

func TestTypedJSONRequiresExactPresentNonNullMembers(t *testing.T) {
	for _, data := range []string{`{}`, `{"SHA256":"x","size":1}`, `{"sha256":"x","size":null}`, `{"sha256":"x","size":1,"size":2}`, `{"sha256":"x","size":1} {}`, `{"sha256":"x","size":1,"extra":0}`} {
		var value File
		if err := decodeJSON([]byte(data), &value); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	var value File
	if err := decodeJSON([]byte(`{"sha256":"x","size":1}`), &value); err != nil {
		t.Fatal(err)
	}
}

func TestJSONRejectsInvalidUnicodeEscapesAndExcessiveNesting(t *testing.T) {
	for _, data := range []string{`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`} {
		var value any
		if err := decodeJSON([]byte(data), &value); err == nil {
			t.Fatalf("accepted invalid Unicode escape %s", data)
		}
	}
	var value any
	if err := decodeJSON([]byte(`"\ud83d\ude42"`), &value); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON([]byte(strings.Repeat("[", 130)+"0"+strings.Repeat("]", 130)), &value); err == nil {
		t.Fatal("unbounded nesting accepted")
	}
}

func TestCredentialReferencesMatchProducerPredicate(t *testing.T) {
	for _, key := range []string{"github.api_key", "github.api-key", "github.apikey", "github.secret", "github.token", "github.password", "github.passwd", "github.credential", "github.private_key", "github.privatekey", "github.privkey", "github.pwd", "github.pat", "github.auth", "github.bearer", "github.cert", "github.key", "github.keys", "database.dsn", "cloud.access_key", "GITHUB.TOKEN"} {
		t.Run(key, func(t *testing.T) {
			if !secretKey(key) {
				t.Fatal("missed producer credential key")
			}
			if err := validateSecretReferences(map[string]any{"nested": []any{map[string]any{key: "raw-or-digest"}}}); err == nil {
				t.Fatal("raw nested credential accepted")
			}
			if err := validateSecretReferences(map[string]any{key: map[string]any{"$secret": "synthetic/reference"}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, key := range []string{"project_id", "commit.author", "issue.path", "export.pattern", "sort.keyword"} {
		if secretKey(key) {
			t.Fatalf("public near miss %s classified as credential", key)
		}
	}
	for _, ref := range []any{map[string]any{"$secret": ""}, map[string]any{"$secret": nil}, map[string]any{"$secret": "valid", "extra": true}} {
		if err := validateSecretReferences(ref); err == nil {
			t.Fatal("invalid reference accepted")
		}
	}
}

// The wire envelope never treats a missing legacy member as an empty image.
// Its semantic admission is checked by the native manager; corrupt members
// and checksums are rejected here before that boundary.
func TestNativeAllocatorEnvelopeRequiresInventoriedBoundBytes(t *testing.T) {
	raw := []byte(`{"version":"plug-native-allocator/1","store_id":"source","incarnation":"source-inc","at":0,"coverage_floor":0,"projects":[],"parents":[]}`)
	members := map[string][]byte{"manifest.json": nil, "config/sources.json": []byte("{}\n"), "records/native.ndjson": nil, "versions.ndjson": nil, "changesets.ndjson": nil, NativeAllocatorMember: raw}
	paths := []string{"manifest.json", NativeAllocatorMember, "changesets.ndjson", "config/sources.json", "records/native.ndjson", "versions.ndjson"}
	manifest := Manifest{Format: Format, Schema: 1, Scope: "plug-native-store", History: "head", NativeStoreID: "source", NativeIncarnation: "source-inc", NativeHead: "0", NativeAllocatorProfile: NativeAllocatorVersion, NativeAllocatorDigest: digest(raw), Files: map[string]File{}}
	bind := func() {
		h := sha256.New()
		manifest.Files = map[string]File{}
		for _, path := range paths[1:] {
			b := members[path]
			manifest.Files[path] = File{SHA256: digest(b), Size: int64(len(b))}
			h.Write([]byte(path))
			h.Write(b)
		}
		manifest.NativeDigest = hex.EncodeToString(h.Sum(nil))
	}
	bind()
	if err := validateNativeArchive(manifest, members, paths); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"member", "file", "profile-digest", "native-digest"} {
		t.Run(mutation, func(t *testing.T) {
			bind()
			switch mutation {
			case "member":
				members[NativeAllocatorMember] = append(append([]byte{}, raw...), ' ')
			case "file":
				manifest.Files[NativeAllocatorMember] = File{SHA256: digest(raw), Size: int64(len(raw) + 1)}
			case "profile-digest":
				manifest.NativeAllocatorDigest = strings.Repeat("0", 64)
			case "native-digest":
				manifest.NativeDigest = strings.Repeat("0", 64)
			}
			if err := validateNativeArchive(manifest, members, paths); err == nil {
				t.Fatal("altered allocator binding accepted")
			}
			members[NativeAllocatorMember] = raw
			manifest.NativeAllocatorDigest = digest(raw)
		})
	}
	delete(members, NativeAllocatorMember)
	paths = append(paths[:1], paths[2:]...)
	bind()
	manifest.NativeAllocatorProfile = ""
	manifest.NativeAllocatorDigest = ""
	if err := validateNativeArchive(manifest, members, paths); err != nil {
		t.Fatalf("historical legacy evidence unreadable: %v", err)
	}
}

func TestNativeAllocatorEnvelopeRejectsMalformedInventory(t *testing.T) {
	valid := `{"version":"plug-native-allocator/1","store_id":"source","incarnation":"source-inc","at":0,"coverage_floor":0,"projects":[],"parents":[]}`
	for name, raw := range map[string]string{
		"missing-parents":  strings.Replace(valid, `,"parents":[]`, "", 1),
		"null-projects":    strings.Replace(valid, `"projects":[]`, `"projects":null`, 1),
		"null-at":          strings.Replace(valid, `"at":0`, `"at":null`, 1),
		"unknown-field":    strings.Replace(valid, `"at":0`, `"extra":1,"at":0`, 1),
		"duplicate-root":   strings.Replace(valid, `"at":0`, `"at":0,"at":1`, 1),
		"duplicate-nested": strings.Replace(valid, `"projects":[]`, `"projects":[{"prefix":"a","prefix":"b"}]`, 1),
		"trailing":         valid + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateNativeAllocatorMember([]byte(raw)); err == nil {
				t.Fatalf("accepted malformed payload %s", raw)
			}
		})
	}
	if err := validateNativeAllocatorMember([]byte(valid)); err != nil {
		t.Fatal(err)
	}
}

func TestNativeAllocatorMemberCannotEnterConverterArchive(t *testing.T) {
	fixture := newMergeFixture(t, "alpha")
	data := fixture.bytes(t, func(manifest *Manifest, members map[string][]byte) {
		raw := []byte(`{"version":"plug-native-allocator/1","store_id":"source","incarnation":"source-inc","at":0,"coverage_floor":0,"projects":[],"parents":[]}`)
		members[NativeAllocatorMember] = raw
		manifest.Files[NativeAllocatorMember] = File{SHA256: digest(raw), Size: int64(len(raw))}
	})
	if _, err := Read(bytes.NewReader(data)); err == nil {
		t.Fatal("converter archive admitted native-only allocator member")
	}
}
