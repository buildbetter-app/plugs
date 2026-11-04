package dumpformat

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// These are detached contract controls, not accepted native producer output.
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

func TestEdgeV2ReaderRetainsTypedRefusalsAndSourceEquality(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Record)
	}{
		{"local-custom", func(r *Record) { r.Body["type"], r.Source.Fields["type"] = "custom", "custom" }},
		{"workflow-target", func(r *Record) {
			r.Source.Fields["depends_on_issue_id"] = nil
			r.Source.Fields["depends_on_wisp_id"] = "pl-2"
			r.Body["target_kind"] = "depends_on_wisp_id"
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

func TestEdgeVersionMergeChecksEveryInputWhenAnInputHasNoEdges(t *testing.T) {
	empty, legacy, successor := newMergeFixture(t, "empty"), newMergeFixture(t, "legacy"), newMergeFixture(t, "successor")
	delete(empty.records, "edge")
	successor.records["edge"][0].Body["type"], successor.records["edge"][0].Source.Fields["type"] = "waits-for", "waits-for"
	noEdges := readMergeArchive(t, empty.bytes(t, nil))
	v1 := readMergeArchive(t, edgeVersionFixture(t, legacy, 1))
	v2 := readMergeArchive(t, edgeVersionFixture(t, successor, 2))
	// MergeArchives sorts internally. Vary an irrelevant source store ID until
	// the actual edge-free input sorts first, to exercise the causal old gap.
	for i := 0; ; i++ {
		if archiveKey(noEdges) < archiveKey(v1) && archiveKey(noEdges) < archiveKey(v2) {
			break
		}
		if i == 10000 {
			t.Fatal("could not construct deterministic edge-free-first merge control")
		}
		empty.manifest.StoreID += "x"
		noEdges = readMergeArchive(t, empty.bytes(t, nil))
	}
	if _, err := MergeArchives(noEdges, v1, v2); err == nil {
		t.Fatal("later overlapping edge versions escaped conflict checking")
	}
	merged, err := MergeArchives(noEdges, v2)
	if err != nil {
		t.Fatal(err)
	}
	readback := readMergeArchive(t, merged)
	if readback.Manifest().EdgeVersion() != 2 || !reflect.DeepEqual(readback.Records("edge"), v2.Records("edge")) {
		t.Fatal("later-only edge version or source image was downgraded")
	}
}
