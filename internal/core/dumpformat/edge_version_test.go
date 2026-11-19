package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// These are detached contract controls, not accepted native producer output.
func edgeVersionFixture(t *testing.T, f *mergeFixture, version int) []byte {
	t.Helper()
	// This fixture's encoder is its original capture boundary. Build the
	// expected cache independently from the two explicitly authored columns.
	for i := range f.records["edge"] {
		edge := &f.records["edge"][i]
		edge.NativeEdgeTokens = map[string]string{}
		for _, key := range []string{"metadata", "thread_id"} {
			if value, present := edge.Source.Fields[key]; present {
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				edge.NativeEdgeTokens["beads."+key] = string(raw)
			}
		}
		if len(edge.NativeEdgeTokens) == 0 {
			edge.NativeEdgeTokens = nil
		}
	}
	return f.bytes(t, func(m *Manifest, _ map[string][]byte) {
		for i := range m.Kinds {
			if m.Kinds[i].Name == "edge" {
				m.Kinds[i].Version = version
			}
		}
	})
}

func TestFixed19LocalKindsKeepTypedVersionAndCompleteSource(t *testing.T) {
	// Literal contract census, independent of the implementation's vocabulary.
	kinds := []string{"blocks", "parent-child", "conditional-blocks", "waits-for", "related", "discovered-from", "replies-to", "relates-to", "duplicates", "supersedes", "authored-by", "assigned-to", "approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from"}
	for _, kind := range kinds {
		for _, version := range []int{EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
			t.Run(fmt.Sprintf("%s/v%d", kind, version), func(t *testing.T) {
				f := newMergeFixture(t, "pl")
				edge := &f.records["edge"][0]
				raw := " {\"gate\":\"all-children\",\"also_blocks\":true,\"opaque\":[9007199254740993,1.0,null]} \n"
				edge.Source.Fields = Row{
					"id": edge.ID, "issue_id": edge.Body["from"], "depends_on_issue_id": edge.Body["to"],
					EphemeralDependencyTargetField: nil, "depends_on_external": nil, "type": kind,
					"created_at": "2026-01-01T00:00:00Z", "created_by": " source actor ",
					"metadata": raw, "thread_id": " exact/thread ",
				}
				edge.Body["type"], edge.Body["metadata"], edge.Body["thread_id"] = kind, raw, " exact/thread "
				data := edgeVersionFixture(t, f, version)
				archive, err := Read(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				if archive.Manifest().EdgeVersion() != version || !reflect.DeepEqual(archive.Records("edge")[0], *edge) || !bytes.Equal(archive.TarBytes(), data) {
					t.Fatal("fixed kind lost version, identity, endpoint, raw metadata, thread or one of ten source columns")
				}
				if !IsVersionedLocalKnownEdgeType(version, "depends_on_issue_id", kind) || IsLocalOpaqueEdgeType(version, "depends_on_issue_id", kind) || IsVersionedLocalKnownEdgeType(version, EphemeralDependencyTargetField, kind) || IsVersionedLocalKnownEdgeType(version, "depends_on_external", kind) {
					t.Fatal("fixed kind crossed typed/local/opaque boundaries")
				}
				if kind != "blocks" && kind != "parent-child" && kind != "related" && kind != "discovered-from" {
					if _, err := Read(bytes.NewReader(edgeVersionFixture(t, f, 1))); err == nil {
						t.Fatal("legacy v1 admitted a newly typed local kind")
					}
				}
			})
		}
	}
}

func TestEdgeV2ReaderSeparatelyBindsLocalSemanticsAndExactSource(t *testing.T) {
	for _, kind := range []string{"waits-for", "conditional-blocks"} {
		t.Run(kind, func(t *testing.T) {
			f := newMergeFixture(t, "pl")
			edge := &f.records["edge"][0]
			raw := " {\"gate\":\"any-children\",\"also_blocks\":true,\"spawner_id\":\"not-the-target\",\"opaque\":[9007199254740993,1.0,1e2,null]} \n"
			edge.Body["type"], edge.Source.Fields["type"] = kind, kind
			edge.Body["metadata"], edge.Source.Fields["metadata"] = raw, raw
			edge.Body["thread_id"], edge.Source.Fields["thread_id"] = " source/thread ", " source/thread "
			if _, err := Read(bytes.NewReader(edgeVersionFixture(t, f, 1))); err == nil {
				t.Fatal("frozen v1 profile admitted local successor relation")
			}
			data := edgeVersionFixture(t, f, EdgeVersionTypedDependencies)
			archive, err := Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if archive.Manifest().EdgeVersion() != 2 || !reflect.DeepEqual(archive.Records("edge")[0], *edge) || !bytes.Equal(archive.TarBytes(), data) {
				t.Fatal("v2 marker, typed endpoint, thread, raw metadata or complete source changed")
			}
			if _, err := Read(bytes.NewReader(edgeVersionFixture(t, f, 4))); err == nil {
				t.Fatal("unknown successor version admitted")
			}
		})
	}
}

func TestTracksIsAFixedTypedEdgeAcrossV2AndV3(t *testing.T) {
	for _, version := range []int{EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			fixture := newMergeFixture(t, "pl")
			edge := &fixture.records["edge"][0]
			raw := " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} "
			edge.Body["type"], edge.Source.Fields["type"] = "tracks", "tracks"
			edge.Body["metadata"], edge.Source.Fields["metadata"] = raw, raw
			edge.Body["thread_id"], edge.Source.Fields["thread_id"] = " exact/tracks ", " exact/tracks "
			data := edgeVersionFixture(t, fixture, version)
			archive, err := Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(archive.Records("edge")[0], *edge) || !bytes.Equal(archive.TarBytes(), data) {
				t.Fatal("tracks marker, metadata, thread or source fields changed")
			}
			if IsLocalOpaqueEdgeType(version, "depends_on_issue_id", "tracks") {
				t.Fatal("tracks was admitted through the local-opaque predicate")
			}
			if !IsVersionedLocalKnownEdgeType(version, "depends_on_issue_id", "tracks") {
				t.Fatal("tracks was not admitted as a fixed typed edge")
			}
		})
	}
	fixture := newMergeFixture(t, "pl")
	edge := &fixture.records["edge"][0]
	edge.Body["type"], edge.Source.Fields["type"] = "tracks", "tracks"
	if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, 1))); err == nil {
		t.Fatal("legacy v1 local tracks relation was admitted")
	}
	fixture = newMergeFixture(t, "pl")
	edge = &fixture.records["edge"][0]
	edge.Body["type"], edge.Source.Fields["type"] = "tracks", "tracks"
	edge.Source.Fields["depends_on_issue_id"], edge.Source.Fields["depends_on_external"] = nil, " external:tracker/1 "
	edge.Body["to"], edge.Body["target_kind"] = " external:tracker/1 ", "depends_on_external"
	for _, version := range []int{1, EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
		if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, version))); err != nil {
			t.Fatalf("external tracks behavior changed at v%d: %v", version, err)
		}
	}
}

func TestEdgeV2ReaderRetainsTypedRefusalsAndSourceEquality(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Record)
	}{
		{"local-custom", func(r *Record) { r.Body["type"], r.Source.Fields["type"] = "custom", "custom" }},
		{"workflow-target", func(r *Record) {
			setFixtureWorkflowTarget(r, "pl-2")
		}},
		{"dangling", func(r *Record) { r.Source.Fields["depends_on_issue_id"], r.Body["to"] = "pl-absent", "pl-absent" }},
		{"multiple-arms", func(r *Record) { r.Source.Fields["depends_on_external"] = "external:1" }},
		{"wrong-arm-type", func(r *Record) { r.Source.Fields["depends_on_external"] = json.Number("1") }},
		{"metadata-mirror", func(r *Record) { r.Body["metadata"] = map[string]any{} }},
		{"thread-mirror", func(r *Record) { r.Body["thread_id"] = "changed" }},
		{"bad-predicate", func(r *Record) {
			r.Body["metadata"], r.Source.Fields["metadata"] = `{"also_blocks":null}`, `{"also_blocks":null}`
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newMergeFixture(t, "pl")
			edge := &f.records["edge"][0]
			edge.Body["type"], edge.Source.Fields["type"] = "waits-for", "waits-for"
			test.edit(edge)
			if _, err := Read(bytes.NewReader(edgeVersionFixture(t, f, 2))); err == nil {
				t.Fatal("unsupported or inconsistent typed source admitted")
			}
		})
	}
	for _, kind := range []string{"waits-for", "conditional-blocks", "custom"} {
		f := newMergeFixture(t, "pl")
		edge := &f.records["edge"][0]
		edge.Body["type"], edge.Source.Fields["type"] = kind, kind
		edge.Source.Fields["depends_on_issue_id"] = nil
		edge.Source.Fields["depends_on_external"] = " pl-2 "
		edge.Body["target_kind"], edge.Body["to"] = "depends_on_external", " pl-2 "
		archive, err := Read(bytes.NewReader(edgeVersionFixture(t, f, 2)))
		if err != nil || !reflect.DeepEqual(archive.Records("edge")[0], *edge) {
			t.Fatalf("external arm changed: %s %v", kind, err)
		}
	}
	f := newMergeFixture(t, "pl")
	if _, err := Read(bytes.NewReader(f.bytes(t, func(m *Manifest, _ map[string][]byte) { m.Kinds[0].Version = 2 }))); err == nil {
		t.Fatal("non-edge kind silently upgraded")
	}
}

func TestEdgeV2HistoryAndMergePreserveExplicitVersion(t *testing.T) {
	f, history := sourceHistoryFixture(t, false)
	for _, frame := range history.Range.Frames {
		for _, row := range frame.Records {
			if row.Kind == "edge" {
				row.Body["type"], row.Source.Fields["type"] = "waits-for", "waits-for"
			}
		}
	}
	data := f.bytes(t, func(m *Manifest, files map[string][]byte) {
		m.History = "verified"
		for i := range m.Kinds {
			if m.Kinds[i].Name == "edge" {
				m.Kinds[i].Version = 2
			}
		}
		files[SourceHistoryMember] = fixtureJSON(t, history)
		m.Files[SourceHistoryMember] = File{Size: int64(len(files[SourceHistoryMember])), SHA256: digest(files[SourceHistoryMember])}
	})
	archive, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	image, err := archive.HistoryImage(history.Range.Head)
	if err != nil || image.Manifest().EdgeVersion() != 2 || !reflect.DeepEqual(image.Records("edge"), archive.Records("edge")) {
		t.Fatalf("history lost successor context: %v", err)
	}
	left, right := newMergeFixture(t, "left"), newMergeFixture(t, "right")
	for _, input := range []*mergeFixture{left, right} {
		input.records["edge"][0].Body["type"], input.records["edge"][0].Source.Fields["type"] = "conditional-blocks", "conditional-blocks"
	}
	merged, err := Merge(bytes.NewReader(edgeVersionFixture(t, left, 2)), bytes.NewReader(edgeVersionFixture(t, right, 2)))
	if err != nil {
		t.Fatal(err)
	}
	readback, err := Read(bytes.NewReader(merged))
	if err != nil || readback.Manifest().EdgeVersion() != 2 || len(readback.Records("edge")) != 2 {
		t.Fatalf("merge downgraded explicit edge context: %v", err)
	}
	right.records["edge"][0].Body["type"], right.records["edge"][0].Source.Fields["type"] = "blocks", "blocks"
	if _, err := Merge(bytes.NewReader(edgeVersionFixture(t, left, 2)), bytes.NewReader(edgeVersionFixture(t, right, 1))); err == nil {
		t.Fatal("mixed edge contracts silently merged")
	}
}

func TestEdgeV2MetadataProjectsPredicateWithoutChangingRawValue(t *testing.T) {
	for _, test := range []struct {
		name         string
		raw          any
		gate         string
		also, absent bool
	}{
		{"sql-null", nil, "", false, true},
		{"id-only", `{"_plug_edge_id":"edge-1"}`, "", false, true},
		{"empty", `{}`, "all-children", false, false},
		{"object", map[string]any{"gate": "any-children", "also_blocks": true, "spawner_id": "foreign", "opaque": json.Number("9007199254740993")}, "any-children", true, false},
		{"sql-text-envelope", `{"_plug_edge_id":"edge-1","_plug_metadata":" {\"gate\":\"any-children\",\"n\":1.0} "}`, "any-children", false, false},
		{"object-envelope", map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{}}, "all-children", false, false},
		{"opaque-string", `{"unknown":"{\"_plug_edge_id\":\"literal\"}","_plug_custom":true}`, "all-children", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, _ := json.Marshal(test.raw)
			got, err := SourceEdgeMetadata(2, "edge-1", "waits-for", test.raw)
			if err != nil || (got == nil) != test.absent || got != nil && (got.Gate != test.gate || got.AlsoBlocks != test.also) {
				t.Fatalf("predicate changed: %+v %v", got, err)
			}
			after, _ := json.Marshal(test.raw)
			if !bytes.Equal(before, after) {
				t.Fatal("raw source mutated")
			}
		})
	}
	for _, raw := range []any{`[9007199254740993,1.0,1e2,null]`, []any{json.Number("1.0"), nil}, `"opaque"`, `null`, false} {
		if predicate, err := SourceEdgeMetadata(2, "edge-1", "conditional-blocks", raw); err != nil || predicate != nil {
			t.Fatalf("conditional opaque JSON changed: %v", err)
		}
	}
}

func TestVersionedEmptyWaitsGatePreservesRawValueAndCanonicalPredicate(t *testing.T) {
	for _, version := range []int{EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			for _, test := range []struct {
				name       string
				raw, value any
			}{
				{"object", map[string]any{"gate": ""}, map[string]any{"gate": ""}},
				{"SQL-text", " {\"gate\":\"\",\"opaque\":[9007199254740993,1.0]} \n", " {\"gate\":\"\",\"opaque\":[9007199254740993,1.0]} \n"},
				{"object-envelope", map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{"gate": ""}}, map[string]any{"gate": ""}},
				{"SQL-text-envelope", `{"_plug_edge_id":"edge-1","_plug_metadata":" {\"gate\":\"\",\"n\":1.0} "}`, " {\"gate\":\"\",\"n\":1.0} "},
			} {
				t.Run(test.name, func(t *testing.T) {
					before, _ := json.Marshal(test.raw)
					got, err := InspectSourceEdgeMetadata(version, "edge-1", "waits-for", test.raw)
					if err != nil || got.Predicate == nil || got.Predicate.Gate != "all-children" || got.Predicate.AlsoBlocks {
						t.Fatalf("empty gate changed the canonical explicit predicate: %+v %v", got, err)
					}
					after, _ := json.Marshal(test.raw)
					if !bytes.Equal(before, after) {
						t.Fatal("empty gate raw source representation changed")
					}
					if !reflect.DeepEqual(got.Value, test.value) {
						t.Fatalf("empty gate unwrapped value changed: %#v want %#v", got.Value, test.value)
					}
				})
			}
		})
	}
	if _, err := SourceEdgeMetadata(1, "edge-1", "waits-for", `{"gate":""}`); err == nil {
		t.Fatal("legacy v1 empty-gate refusal changed")
	}
}

func TestVersionedMetadataRejectsMalformedOrAmbiguousEnvelopes(t *testing.T) {
	for _, raw := range []string{
		`{"gate":null}`, `{"gate":" "}`, `{"gate":"any"}`, `{"gate":1}`, `{"gate":false}`, `{"also_blocks":null}`, `{"also_blocks":1}`, `{"spawner_id":null}`, `{"spawner_id":1}`,
		`null`, `[]`, `"{}"`, `{`, `{} {}`, `{"x":1,"x":2}`, `{"x":{"n":1,"n":2}}`, `{"x":"\ud800"}`,
		`{"_plug_edge_id":"other"}`, `{"_plug_edge_id":""}`, `{"_plug_edge_id":1}`, `{"_plug_metadata":{}}`,
		`{"_plug_edge_id":"edge-1","gate":"any-children"}`, `{"_plug_edge_id":"edge-1","_plug_metadata":null}`,
		`{"_plug_edge_id":"edge-1","_plug_edge_id":"edge-1"}`, `{"_plug_edge_id":"edge-1","_plug_metadata":{"_plug_edge_id":"edge-1"}}`,
		`{"opaque":[{"_plug_metadata":{}}]}`, `{"_plug_edge_id":"edge-1","_plug_metadata":"{\"_plug_metadata\":{}}"}`,
	} {
		for _, version := range []int{EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
			if got, err := SourceEdgeMetadata(version, "edge-1", "waits-for", raw); err == nil {
				t.Fatalf("invalid v%d metadata admitted: %s => %+v", version, raw, got)
			}
		}
	}
}

func TestEdgeV2InspectionRetainsUnwrappedSQLTextAndEnvelopeIdentity(t *testing.T) {
	const payload = " {\"gate\":\"any-children\",\"n\":1.0} "
	for _, raw := range []any{payload, map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": payload}} {
		got, err := InspectSourceEdgeMetadata(2, "edge-1", "waits-for", raw)
		_, wrapped := raw.(map[string]any)
		if err != nil || got.Envelope != wrapped || got.Value != payload || got.Predicate == nil || got.Predicate.Gate != "any-children" {
			t.Fatalf("raw envelope inspection changed: %+v %v", got, err)
		}
	}
	raw := map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{"unknown": []any{json.Number("1.0")}}}
	got, err := InspectSourceEdgeMetadata(2, "edge-1", "conditional-blocks", raw)
	if err != nil || !got.Envelope || got.Predicate != nil {
		t.Fatalf("conditional envelope changed: %+v %v", got, err)
	}
	got.Value.(map[string]any)["unknown"].([]any)[0] = "mutated"
	if raw["_plug_metadata"].(map[string]any)["unknown"].([]any)[0] != json.Number("1.0") {
		t.Fatal("unwrapped value aliases original raw source")
	}
	// An opaque conditional string is not a second SQL parsing boundary.
	if _, err := InspectSourceEdgeMetadata(2, "edge-1", "conditional-blocks", map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": "literal text"}); err != nil {
		t.Fatal(err)
	}
}

func TestEdgeMetadataIdentityErrorRetainsExactRejectedEnvelope(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		value     any
		present   bool
	}{
		{"different-string", `{"_plug_edge_id":"other"}`, "other", true},
		{"empty-string", `{"_plug_edge_id":""}`, "", true},
		{"absent", `{"_plug_metadata":{}}`, nil, false},
		{"null", `{"_plug_edge_id":null}`, nil, true},
		{"wide-number", `{"_plug_edge_id":9007199254740993}`, json.Number("9007199254740993"), true},
		{"number-spelling", `{"_plug_edge_id":9.007199254740993e+15}`, json.Number("9.007199254740993e+15"), true},
		{"boolean", `{"_plug_edge_id":false}`, false, true},
		{"array", `{"_plug_edge_id":[9007199254740993,null]}`, []any{json.Number("9007199254740993"), nil}, true},
		{"object", `{"_plug_edge_id":{"n":9007199254740993,"null":null}}`, map[string]any{"n": json.Number("9007199254740993"), "null": nil}, true},
	} {
		for _, version := range []int{EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
			t.Run(fmt.Sprintf("%s/v%d", test.name, version), func(t *testing.T) {
				var detached any
				if err := decodeJSON([]byte(test.raw), &detached); err != nil {
					t.Fatal(err)
				}
				for _, raw := range []any{test.raw, detached} {
					projection, err := InspectSourceEdgeMetadata(version, "edge-1", "waits-for", raw)
					var identity *EdgeMetadataIdentityError
					if !errors.As(fmt.Errorf("retained readback: %w", err), &identity) || err.Error() != "edge metadata envelope identity mismatch" || !reflect.DeepEqual(projection, EdgeMetadataProjection{}) {
						t.Fatalf("identity refusal lost its type, text or empty projection: %+v %v", projection, err)
					}
					if identity.SourceTable != "" || identity.EdgeID != "edge-1" || identity.EnvelopePresent != test.present || !reflect.DeepEqual(identity.EnvelopeID, test.value) {
						t.Fatalf("rejected identity changed: %+v", identity)
					}
					predicate, err := SourceEdgeMetadata(version, "edge-1", "waits-for", raw)
					var predicateError *EdgeMetadataIdentityError
					if predicate != nil || !errors.As(err, &predicateError) || !reflect.DeepEqual(predicateError, identity) {
						t.Fatal("predicate API concealed or accepted the rejected identity")
					}
				}
			})
		}
	}
}

func TestEdgeMetadataIdentityErrorDoesNotClassifyOtherRefusals(t *testing.T) {
	for _, raw := range []string{
		`{"_plug_edge_id":"edge-1","_plug_metadata":null}`,
		`{"_plug_edge_id":"edge-1","extra":true}`,
		`{"_plug_edge_id":"edge-1","_plug_edge_id":"other"}`,
		`{"gate":false}`,
	} {
		_, err := InspectSourceEdgeMetadata(EdgeVersionTypedDependencies, "edge-1", "waits-for", raw)
		var identity *EdgeMetadataIdentityError
		if err == nil || errors.As(err, &identity) {
			t.Fatalf("other malformed metadata acquired an identity diagnostic: %s %v", raw, err)
		}
	}
}

func TestEdgeMetadataIdentityErrorSurvivesPortableReaderWrapping(t *testing.T) {
	for _, version := range []int{EdgeVersionTypedDependencies, EdgeVersionLocalOpaque} {
		f := newMergeFixture(t, "pl")
		edge := &f.records["edge"][0]
		raw := fmt.Sprintf(`{"_plug_edge_id":%q,"_plug_metadata":{"n":9007199254740993}}`, edge.ID)
		edge.Body["metadata"], edge.Source.Fields["metadata"] = raw, raw
		data := edgeVersionFixture(t, f, version)
		archive, err := Read(bytes.NewReader(data))
		if err != nil || !bytes.Equal(archive.TarBytes(), data) || archive.Records("edge")[0].Source.Fields["metadata"] != raw {
			t.Fatalf("matching identity or exact source changed: %v", err)
		}
		raw = `{"_plug_edge_id":9007199254740993}`
		edge.Body["metadata"], edge.Source.Fields["metadata"] = raw, raw
		archive, err = Read(bytes.NewReader(edgeVersionFixture(t, f, version)))
		var identity *EdgeMetadataIdentityError
		if archive != nil || !errors.As(err, &identity) || err.Error() != "edge "+edge.ID+" metadata: edge metadata envelope identity mismatch" {
			t.Fatalf("portable reader accepted or concealed the identity refusal: %v", err)
		}
		if identity.SourceTable != DurableDependencyTable || identity.EdgeID != edge.ID || !identity.EnvelopePresent || identity.EnvelopeID != json.Number("9007199254740993") {
			t.Fatalf("portable reader lost exact source context: %+v", identity)
		}
	}
}

func TestEdgeV3ReaderAdmitsOnlyDurableLocalOpaqueKinds(t *testing.T) {
	fixture := newMergeFixture(t, "pl")
	edge := &fixture.records["edge"][0]
	raw := " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} "
	edge.Body["type"], edge.Source.Fields["type"] = "local-opaque", "local-opaque"
	edge.Body["metadata"], edge.Source.Fields["metadata"] = raw, raw
	edge.Body["thread_id"], edge.Source.Fields["thread_id"] = " exact/thread ", " exact/thread "
	v3 := edgeVersionFixture(t, fixture, EdgeVersionLocalOpaque)
	archive, err := Read(bytes.NewReader(v3))
	if err != nil {
		t.Fatal(err)
	}
	if archive.Manifest().EdgeVersion() != EdgeVersionLocalOpaque || !reflect.DeepEqual(archive.Records("edge")[0], *edge) || !bytes.Equal(archive.TarBytes(), v3) {
		t.Fatal("v3 local opaque row lost its marker, raw metadata or source fields")
	}
	if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, EdgeVersionTypedDependencies))); err == nil {
		t.Fatal("v2 admitted a local opaque dependency")
	}
	for _, kind := range []string{"replies-to", "duplicates", "delegated-from"} {
		fixture := newMergeFixture(t, "pl")
		edge := &fixture.records["edge"][0]
		edge.Body["type"], edge.Source.Fields["type"] = kind, kind
		if IsLocalOpaqueEdgeType(EdgeVersionLocalOpaque, "depends_on_issue_id", kind) {
			t.Fatalf("canonical kind %q was relabeled as opaque", kind)
		}
		if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, 1))); err == nil {
			t.Fatalf("legacy v1 admitted newly typed fixed kind %q", kind)
		}
	}
	for _, test := range []struct {
		name string
		edit func(*Record)
	}{
		{"empty-kind", func(r *Record) { r.Body["type"], r.Source.Fields["type"] = "", "" }},
		{"overlong-kind", func(r *Record) {
			kind := strings.Repeat("x", 33)
			r.Body["type"], r.Source.Fields["type"] = kind, kind
		}},
		{"wrong-arm-type", func(r *Record) { r.Source.Fields["depends_on_issue_id"] = json.Number("1") }},
		{"ephemeral-arm", func(r *Record) { setFixtureWorkflowTarget(r, "pl-2") }},
		{"dangling", func(r *Record) {
			r.Source.Fields["depends_on_issue_id"], r.Body["to"] = "pl-missing", "pl-missing"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMergeFixture(t, "pl")
			fixture.records["edge"][0].Body["type"], fixture.records["edge"][0].Source.Fields["type"] = "local-opaque", "local-opaque"
			test.edit(&fixture.records["edge"][0])
			if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, EdgeVersionLocalOpaque))); err == nil {
				t.Fatal("invalid v3 local opaque row was admitted")
			}
		})
	}
	fixture = newMergeFixture(t, "pl")
	edge = &fixture.records["edge"][0]
	edge.Source.Fields["depends_on_issue_id"], edge.Source.Fields["depends_on_external"] = nil, " external:tracker/1 "
	edge.Body["to"], edge.Body["target_kind"], edge.Body["type"] = " external:tracker/1 ", "depends_on_external", "external-opaque"
	edge.Source.Fields["type"] = "external-opaque"
	if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, EdgeVersionLocalOpaque))); err != nil {
		t.Fatalf("v3 changed external-arm behavior: %v", err)
	}
}

func TestEdgeV3MetadataIsOpaqueAndDoesNotProjectScheduling(t *testing.T) {
	raw := " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} "
	before := raw
	projection, err := InspectSourceEdgeMetadata(EdgeVersionLocalOpaque, "edge-1", "local-opaque", raw)
	if err != nil || projection.Predicate != nil || projection.Value != raw {
		t.Fatalf("local opaque metadata acquired a predicate or changed value: %+v %v", projection, err)
	}
	if raw != before {
		t.Fatal("local opaque metadata input changed")
	}
	predicate, err := SourceEdgeMetadata(EdgeVersionLocalOpaque, "edge-1", "waits-for", map[string]any{"gate": "any-children", "also_blocks": true})
	if err != nil || predicate == nil || predicate.Gate != "any-children" || !predicate.AlsoBlocks {
		t.Fatalf("v3 did not retain v2 waits projection: %+v %v", predicate, err)
	}
	if _, err := SourceEdgeMetadata(4, "edge-1", "local-opaque", nil); err == nil {
		t.Fatal("unknown edge metadata version admitted")
	}
}

func TestEdgeV3HistoryKeepsLocalOpaqueRowsInEveryImage(t *testing.T) {
	fixture, history := sourceHistoryFixture(t, false)
	edge := &fixture.records["edge"][0]
	edge.Body["type"], edge.Source.Fields["type"] = "local-opaque", "local-opaque"
	edge.Body["metadata"], edge.Source.Fields["metadata"] = " {\"literal\":1.0} ", " {\"literal\":1.0} "
	data := fixture.bytes(t, func(manifest *Manifest, files map[string][]byte) {
		manifest.History = "verified"
		for i := range manifest.Kinds {
			if manifest.Kinds[i].Name == "edge" {
				manifest.Kinds[i].Version = EdgeVersionLocalOpaque
			}
		}
		files[SourceHistoryMember] = fixtureJSON(t, history)
		manifest.Files[SourceHistoryMember] = File{Size: int64(len(files[SourceHistoryMember])), SHA256: digest(files[SourceHistoryMember])}
	})
	archive, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{history.Range.First, history.Range.Head} {
		image, err := archive.HistoryImage(sequence)
		if err != nil || image.Manifest().EdgeVersion() != EdgeVersionLocalOpaque || !reflect.DeepEqual(image.Records("edge"), archive.Records("edge")) {
			t.Fatalf("history lost v3 local opaque source at %d: %v", sequence, err)
		}
	}
}

func TestEdgeVersionMergeChecksEveryInputWhenAnInputHasNoEdges(t *testing.T) {
	empty, legacy, successor, opaque := newMergeFixture(t, "empty"), newMergeFixture(t, "legacy"), newMergeFixture(t, "successor"), newMergeFixture(t, "opaque")
	delete(empty.records, "edge")
	successor.records["edge"][0].Body["type"], successor.records["edge"][0].Source.Fields["type"] = "waits-for", "waits-for"
	opaque.records["edge"][0].Body["type"], opaque.records["edge"][0].Source.Fields["type"] = "local-opaque", "local-opaque"
	noEdges := readMergeArchive(t, empty.bytes(t, nil))
	v1 := readMergeArchive(t, edgeVersionFixture(t, legacy, 1))
	v2 := readMergeArchive(t, edgeVersionFixture(t, successor, 2))
	v3 := readMergeArchive(t, edgeVersionFixture(t, opaque, 3))
	// MergeArchives sorts internally. Vary an irrelevant source store ID until
	// the actual edge-free input sorts first, to exercise the causal old gap.
	for i := 0; ; i++ {
		if archiveKey(noEdges) < archiveKey(v1) && archiveKey(noEdges) < archiveKey(v2) && archiveKey(noEdges) < archiveKey(v3) {
			break
		}
		if i == 10000 {
			t.Fatal("could not construct deterministic edge-free-first merge control")
		}
		empty.manifest.StoreID += "x"
		noEdges = readMergeArchive(t, empty.bytes(t, nil))
	}
	for _, pair := range [][2]*Archive{{v1, v2}, {v1, v3}, {v2, v3}} {
		if output, err := MergeArchives(noEdges, pair[0], pair[1]); err == nil || len(output) != 0 {
			t.Fatal("later overlapping edge versions escaped conflict checking or published partial output")
		}
	}
	for _, successor := range []*Archive{v2, v3} {
		merged, err := MergeArchives(noEdges, successor)
		if err != nil {
			t.Fatal(err)
		}
		readback := readMergeArchive(t, merged)
		if readback.Manifest().EdgeVersion() != successor.Manifest().EdgeVersion() || !reflect.DeepEqual(readback.Records("edge"), successor.Records("edge")) {
			t.Fatal("later-only edge version or source image was downgraded")
		}
	}
}
