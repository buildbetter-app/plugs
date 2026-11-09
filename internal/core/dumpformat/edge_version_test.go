package dumpformat

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// Shared helper controls credited to semantic author3ae at5a01cc638.
// These detached controls do not represent accepted native producer output.
func TestEdgeV2ReaderKeepsFrozenExternalCustomRefusal(t *testing.T) {
	for _, version := range []int{1, EdgeVersionTypedDependencies} {
		fixture := newMergeFixture(t, "pl")
		edge := &fixture.records["edge"][0]
		edge.Source.Fields["depends_on_issue_id"] = nil
		edge.Source.Fields["depends_on_external"] = "external:pl-2"
		edge.Body["target_kind"], edge.Body["to"] = "depends_on_external", "external:pl-2"
		if _, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, version))); err != nil {
			t.Fatalf("existing typed external source arm changed under version %d: %v", version, err)
		}
		edge.Body["type"], edge.Source.Fields["type"] = "custom", "custom"
		_, err := Read(bytes.NewReader(edgeVersionFixture(t, fixture, version)))
		if version == EdgeVersionTypedDependencies && err == nil {
			t.Fatalf("version %d relaxed this source's frozen external custom refusal", version)
		}
		if version == 1 && err != nil {
			t.Fatalf("main's v1 external custom vocabulary was refused: %v", err)
		}
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

func TestEdgeV2MetadataRejectsMalformedOrAmbiguousEnvelopes(t *testing.T) {
	for _, raw := range []string{
		`{"gate":null}`, `{"gate":""}`, `{"gate":"any"}`, `{"gate":1}`, `{"also_blocks":null}`, `{"also_blocks":1}`, `{"spawner_id":null}`, `{"spawner_id":1}`,
		`null`, `[]`, `"{}"`, `{`, `{} {}`, `{"x":1,"x":2}`, `{"x":{"n":1,"n":2}}`, `{"x":"\ud800"}`,
		`{"_plug_edge_id":"other"}`, `{"_plug_edge_id":""}`, `{"_plug_edge_id":1}`, `{"_plug_metadata":{}}`,
		`{"_plug_edge_id":"edge-1","gate":"any-children"}`, `{"_plug_edge_id":"edge-1","_plug_metadata":null}`,
		`{"_plug_edge_id":"edge-1","_plug_edge_id":"edge-1"}`, `{"_plug_edge_id":"edge-1","_plug_metadata":{"_plug_edge_id":"edge-1"}}`,
		`{"opaque":[{"_plug_metadata":{}}]}`, `{"_plug_edge_id":"edge-1","_plug_metadata":"{\"_plug_metadata\":{}}"}`,
	} {
		if got, err := SourceEdgeMetadata(2, "edge-1", "waits-for", raw); err == nil {
			t.Fatalf("invalid metadata admitted: %s => %+v", raw, got)
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

func edgeVersionFixture(t *testing.T, f *mergeFixture, version int) []byte {
	t.Helper()
	return f.bytes(t, func(m *Manifest, _ map[string][]byte) {
		for i := range m.Kinds {
			if m.Kinds[i].Name == "edge" {
				m.Kinds[i].Version = version
			}
		}
	})
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
			if _, err := Read(bytes.NewReader(edgeVersionFixture(t, f, 3))); err == nil {
				t.Fatal("unknown successor version admitted")
			}
		})
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
