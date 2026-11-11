package dumpformat

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// Disposable wire controls bind the actual bc32707d producer contract. They
// are neither stored producer output nor public/native runtime evidence.
func sourcePlaneReaderFixture(t *testing.T, prefix string) *mergeFixture {
	t.Helper()
	f := newMergeFixture(t, prefix)
	f.manifest.SourcePlaneAddressContract = SourcePlaneAddressContract
	active := Row{}
	for _, column := range strings.Fields(issueColumns) {
		active[column] = nil
	}
	active["id"], active["title"], active["status"], active["issue_type"] = prefix+"-1", "active exact source", "review", "task"
	active["priority"], active["ephemeral"], active["no_history"] = json.Number("2"), true, true
	active["metadata"] = " {\"wide\":9007199254740993,\"n\":1.0,\"null\":null} "
	f.addArchive(t, EphemeralEntityTable, active)
	f.addArchive(t, "custom_statuses", Row{"name": "review", "category": "active"})
	f.addArchive(t, "custom_types", Row{"name": "Exact custom type"})
	for _, input := range []struct {
		table string
		row   Row
	}{
		{EphemeralLabelTable, Row{"issue_id": prefix + "-1", "label": "raw-label"}},
		{EphemeralCommentTable, Row{"id": prefix + "-comment-raw", "issue_id": prefix + "-1", "author": "unverified author", "text": "raw comment", "created_at": "2026-11-05T00:00:00Z"}},
		{EphemeralEventTable, Row{"id": prefix + "-event-raw", "issue_id": prefix + "-1", "event_type": "raw-event", "actor": "unverified author", "old_value": nil, "new_value": "raw", "comment": "inert", "created_at": "2026-11-05T00:00:00Z"}},
		{EphemeralChildCounterTable, Row{"parent_id": prefix + "-1", "last_child": json.Number("7")}},
	} {
		f.addArchive(t, input.table, input.row)
	}
	raw := " {\"gate\":\"any-children\",\"also_blocks\":true,\"wide\":9007199254740993,\"n\":1.0} "
	fields := Row{"id": "same\x00edge", "issue_id": prefix + "-1", "depends_on_issue_id": nil, EphemeralDependencyTargetField: prefix + "-1", "depends_on_external": nil, "type": "waits-for", "created_at": "2026-11-05T00:00:00Z", "created_by": "raw author", "metadata": raw, "thread_id": " raw thread "}
	f.records["edge"] = []Record{{Kind: "edge", ID: "same\x00edge", Project: prefix, Source: Provenance{Table: DurableDependencyTable, Fields: fields}, Body: Row{"from": prefix + "-1", "to": prefix + "-1", "target_kind": EphemeralDependencyTargetField, "type": "waits-for", "metadata": raw, "thread_id": " raw thread "}}}
	other := f.records["edge"][0]
	other.Source = Provenance{Table: EphemeralDependencyTable, Fields: Row{}}
	for key, value := range fields {
		other.Source.Fields[key] = value
	}
	other.Body = Row{}
	for key, value := range f.records["edge"][0].Body {
		other.Body[key] = value
	}
	other.Source.Fields["depends_on_issue_id"], other.Source.Fields[EphemeralDependencyTargetField] = prefix+"-2", nil
	other.Body["to"], other.Body["target_kind"] = prefix+"-2", "depends_on_issue_id"
	f.records["edge"] = append(f.records["edge"], other)
	return f
}

func sourcePlaneReaderBytes(t *testing.T, f *mergeFixture) []byte {
	t.Helper()
	return f.bytes(t, func(m *Manifest, members map[string][]byte) {
		// Explicit dependency-plane order, independent of the reader comparator.
		members["records/edge.ndjson"] = []byte(" \t")
		for _, r := range f.records["edge"] {
			members["records/edge.ndjson"] = append(members["records/edge.ndjson"], fixtureJSON(t, r)...)
		}
		for i := range m.Kinds {
			if m.Kinds[i].Name == "edge" {
				m.Kinds[i].Version = EdgeVersionLocalOpaque
			}
		}
		for i := range m.Sources {
			switch m.Sources[i].Family {
			case EphemeralEntityTable, EphemeralDependencyTable, EphemeralLabelTable, EphemeralCommentTable, EphemeralEventTable, EphemeralChildCounterTable, "custom_statuses", "custom_types":
				m.Sources[i].Action = "archive"
			}
		}
		for name, data := range members {
			m.Files[name] = File{SHA256: digest(data), Size: int64(len(data))}
		}
	})
}

func sourcePlaneReaderMembers(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	result := map[string][]byte{}
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return result
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		result[header.Name] = body
	}
}

func TestSourcePlaneReaderConservesBothPhysicalPlanesAndRawRecords(t *testing.T) {
	f := sourcePlaneReaderFixture(t, "pl")
	data := sourcePlaneReaderBytes(t, f)
	a, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	wantEdges := append([]Record(nil), f.records["edge"]...)
	for i := range wantEdges {
		wantEdges[i].NativeEdgeTokens = map[string]string{}
		for _, field := range []string{"metadata", "thread_id"} {
			raw, err := json.Marshal(wantEdges[i].Source.Fields[field])
			if err != nil {
				t.Fatal(err)
			}
			wantEdges[i].NativeEdgeTokens["beads."+field] = string(raw)
		}
	}
	if !bytes.Equal(data, a.TarBytes()) || !reflect.DeepEqual(a.Records("edge"), wantEdges) {
		t.Fatal("physical edges, TEN fields, raw cells or archive bytes changed")
	}
	edges := a.Records("edge")
	if len(edges) != 2 || edges[0].ID != edges[1].ID || edges[0].Source.Table != DurableDependencyTable || edges[1].Source.Table != EphemeralDependencyTable {
		t.Fatal("cross-plane equal identity collapsed")
	}
	raw, err := a.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	expected := 0
	for _, kind := range a.Manifest().Kinds {
		member, _ := a.Member(kind.File)
		lines := bytes.Split(member[:len(member)-1], []byte{'\n'})
		for _, line := range lines {
			if !bytes.Equal(raw[expected].Raw, line) {
				t.Fatal("raw NDJSON remarshal or normalization")
			}
			expected++
		}
	}
	if len(raw) != expected {
		t.Fatal("raw record census changed")
	}
	raw[0].Raw[0] = '!'
	again, err := a.SourcePlaneRecords()
	if err != nil || again[0].Raw[0] == '!' {
		t.Fatal("raw caller bytes alias archive")
	}
	// A derived historical image cannot claim unrelated original member bytes.
	image := *a
	image.byKind = map[string][]Record{}
	for kind, rows := range a.byKind {
		image.byKind[kind] = append([]Record(nil), rows...)
	}
	image.byKind["edge"] = image.byKind["edge"][:1]
	if _, err := image.SourcePlaneRecords(); err == nil {
		t.Fatal("selected image borrowed original raw record census")
	}
}

func TestSourcePlaneReaderRefusalsReachExactWireAndRelations(t *testing.T) {
	tests := []struct {
		name, reason string
		edit         func(*mergeFixture)
	}{
		{"same-full-tuple", "duplicate or unordered", func(f *mergeFixture) {
			f.records["edge"] = append([]Record{f.records["edge"][0]}, f.records["edge"]...)
		}},
		{"unordered-plane", "duplicate or unordered", func(f *mergeFixture) {
			f.records["edge"][0], f.records["edge"][1] = f.records["edge"][1], f.records["edge"][0]
		}},
		{"TEN-field-missing", "missing source plane column thread_id", func(f *mergeFixture) { delete(f.records["edge"][0].Source.Fields, "thread_id") }},
		{"body-mirror", "body/source mismatch", func(f *mergeFixture) { f.records["edge"][0].Body["thread_id"] = "changed" }},
		{"missing-active-target", "unresolved dependency", func(f *mergeFixture) {
			f.records["edge"][0].Body["to"] = "pl-9"
			f.records["edge"][0].Source.Fields[EphemeralDependencyTargetField] = "pl-9"
		}},
		{"cross-plane-local-kind", "kind/plane", func(f *mergeFixture) {
			for i := range f.records["edge"] {
				f.records["edge"][i].Body["type"] = "relates-to"
				f.records["edge"][i].Source.Fields["type"] = "relates-to"
			}
		}},
		{"undeclared-active-status", "identity/status", func(f *mergeFixture) {
			for i, r := range f.records["beads-archive"] {
				if r.Source.Table == "custom_statuses" {
					f.records["beads-archive"] = append(f.records["beads-archive"][:i], f.records["beads-archive"][i+1:]...)
					break
				}
			}
		}},
		{"duplicate-active-comment", "auxiliary source identity", func(f *mergeFixture) {
			for _, r := range f.records["beads-archive"] {
				if r.Source.Table == EphemeralCommentTable {
					copy := r
					copy.Source.Fields = Row{}
					for k, v := range r.Source.Fields {
						copy.Source.Fields[k] = v
					}
					copy.Source.Fields["text"] = "different bytes same physical ID"
					encoded := fixtureJSON(t, copy.Source.Fields)
					copy.ID = copy.Source.Table + "/" + digest(encoded)
					f.records["beads-archive"] = append(f.records["beads-archive"], copy)
					break
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := sourcePlaneReaderFixture(t, "pl")
			if _, err := Read(bytes.NewReader(sourcePlaneReaderBytes(t, f))); err != nil {
				t.Fatal("invalid positive baseline", err)
			}
			test.edit(f)
			if _, err := Read(bytes.NewReader(sourcePlaneReaderBytes(t, f))); err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("refusal=%v want=%s", err, test.reason)
			}
		})
	}
}

func TestSourcePlaneReaderMarkerRequiresCanonicalPresenceAndValue(t *testing.T) {
	legacy := newMergeFixture(t, "pl").bytes(t, nil)
	if _, err := Read(bytes.NewReader(legacy)); err != nil {
		t.Fatal(err)
	}
	supported := sourcePlaneReaderBytes(t, sourcePlaneReaderFixture(t, "pl"))
	if _, err := Read(bytes.NewReader(supported)); err != nil {
		t.Fatal("invalid supported marker baseline", err)
	}
	for _, key := range []string{"source_plane_address_contract", "SOURCE_PLANE_ADDRESS_CONTRACT", "Source_Plane_Address_Contract", "ſource_plane_address_contract"} {
		for _, value := range []any{nil, "", "beads-source-planes/2", false, json.Number("1"), SourcePlaneAddressContract} {
			t.Run(key+"/"+string(fixtureJSON(t, value)), func(t *testing.T) {
				members := sourcePlaneReaderMembers(t, supported)
				var manifest map[string]any
				if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
					t.Fatal(err)
				}
				manifest[key] = value
				members["manifest.json"] = fixtureJSON(t, manifest)
				_, err := Read(bytes.NewReader(fixtureTar(t, members)))
				if key == "source_plane_address_contract" && value == SourcePlaneAddressContract {
					if err != nil {
						t.Fatal("exact supported marker refused", err)
					}
					return
				}
				if err == nil {
					t.Fatal("present invalid/aliased marker collapsed to legacy")
				}
				if key != "source_plane_address_contract" && !strings.Contains(err.Error(), "unknown typed JSON member") {
					t.Fatal("alias did not reach exact shape refusal", err)
				}
			})
		}
	}
	members := sourcePlaneReaderMembers(t, supported)
	var manifest map[string]any
	if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest["source_plane_address_contract"], manifest["SOURCE_PLANE_ADDRESS_CONTRACT"] = SourcePlaneAddressContract, SourcePlaneAddressContract
	members["manifest.json"] = fixtureJSON(t, manifest)
	if _, err := Read(bytes.NewReader(fixtureTar(t, members))); err == nil {
		t.Fatal("canonical/folded ambiguity accepted")
	}
}

func TestSourcePlaneReaderCatalogUsesOnlyStoredAndCapturedYAMLUnion(t *testing.T) {
	f := sourcePlaneReaderFixture(t, "pl")
	raw := []byte("status:\n  custom: review:active\n")
	observation := map[string]any{"path": "/synthetic/catalog.yaml", "state": "file", "bytes": map[string]any{"$bytes": base64.StdEncoding.EncodeToString(raw)}, "sha256": digest(raw), "size": json.Number(strconv.Itoa(len(raw))), "values": map[string]any{"status": map[string]any{"custom": "review:active"}}}
	for i := range f.config {
		if f.config[i].Input.Family == "project-config" {
			f.config[i].Values = map[string]any{"observations": []any{observation}}
		}
		if f.config[i].Input.Family == "store-files-and-quiescence" {
			f.config[i].Input.State = "json-config"
			f.config[i].Values = map[string]any{"observations": []any{map[string]any{"path": "/synthetic/store", "state": "directory", "contents": "not-read"}}}
		}
	}
	if _, err := Read(bytes.NewReader(sourcePlaneReaderBytes(t, f))); err != nil {
		t.Fatal("valid SQL/YAML union and opaque engine census", err)
	}
	observation["values"] = map[string]any{"status": map[string]any{"custom": "review:done"}}
	if _, err := Read(bytes.NewReader(sourcePlaneReaderBytes(t, f))); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatal("SQL/YAML category conflict accepted", err)
	}
	observation["path"] = "/synthetic/catalog.json"
	if _, err := Read(bytes.NewReader(sourcePlaneReaderBytes(t, f))); err != nil {
		t.Fatal("JSON source observations invented catalog semantics", err)
	}
	observation["path"] = "/synthetic/catalog.yaml"
	delete(observation, "values")
	if _, err := Read(bytes.NewReader(sourcePlaneReaderBytes(t, f))); err == nil || !strings.Contains(err.Error(), "missing parsed configuration") {
		t.Fatal("malformed actual file-family YAML accepted", err)
	}
}

func TestSourcePlaneMergeRetainsIndependentPhysicalEdgeIdentity(t *testing.T) {
	a := sourcePlaneReaderBytes(t, sourcePlaneReaderFixture(t, "aa"))
	b := sourcePlaneReaderBytes(t, sourcePlaneReaderFixture(t, "zz"))
	result, err := Merge(bytes.NewReader(a), bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	merged, err := Read(bytes.NewReader(result))
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Records("edge")) != 4 || merged.Manifest().SourcePlaneAddressContract != SourcePlaneAddressContract {
		t.Fatal("merged source plane marker/tuples lost")
	}
	reverse, err := Merge(bytes.NewReader(b), bytes.NewReader(a))
	if err != nil || !bytes.Equal(result, reverse) {
		t.Fatal("cap merge order changed output", err)
	}
	legacy := newMergeFixture(t, "yy").bytes(t, nil)
	if data, err := Merge(bytes.NewReader(a), bytes.NewReader(legacy)); err == nil || len(data) != 0 {
		t.Fatal("mixed cap/legacy merge accepted")
	}
}
