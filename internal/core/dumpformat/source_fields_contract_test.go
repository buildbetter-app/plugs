package dumpformat

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// Contract controls only; native physical SQL import is a separate capability.
func TestSourceFieldsContractPreservesPhysicalValuesWithoutEdgeUpgrade(t *testing.T) {
	f := newMergeFixture(t, "pl")
	fields := f.records["bead"][0].Source.Fields
	fields["timeout_ns"] = json.Number("9007199254740993")
	fields["payload"] = map[string]any{"n": json.Number("9007199254740992"), "decimal": json.Number("1.0"), "null": nil}
	f.manifest.SourceFieldsContract = IssueSourceFieldsContract
	archive, err := Read(bytes.NewReader(f.bytes(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if archive.Manifest().SourceFieldsContract != IssueSourceFieldsContract || archive.Manifest().EdgeVersion() != 1 {
		t.Fatal("independent source and edge contracts were conflated")
	}
	actual := archive.Records("bead")[0].Source.Fields
	for _, key := range OpaqueIssueFields() {
		value, present := actual[key]
		if !present || !reflect.DeepEqual(value, fields[key]) {
			t.Fatalf("physical value/presence changed at %s", key)
		}
	}
	f.records["edge"][0].Body["type"], f.records["edge"][0].Source.Fields["type"] = "waits-for", "waits-for"
	if _, err := Read(bytes.NewReader(f.bytes(t, nil))); err == nil {
		t.Fatal("source-fields marker enabled v1 local dependency")
	}
}

func TestSourceFieldsContractRequiresExactMarkerAndCompleteColumns(t *testing.T) {
	for _, marker := range []any{"", "beads-issues-source-fields/2", nil, json.Number("1"), map[string]any{}} {
		f := newMergeFixture(t, "pl")
		data := f.bytes(t, func(m *Manifest, _ map[string][]byte) { m.SourceFieldsContract = IssueSourceFieldsContract })
		members := map[string][]byte{}
		archive, err := Read(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		for path := range archive.Manifest().Files {
			members[path], _ = archive.Member(path)
		}
		manifest, _ := archive.Member("manifest.json")
		var raw map[string]any
		if err := decodeJSON(manifest, &raw); err != nil {
			t.Fatal(err)
		}
		raw["source_fields_contract"] = marker
		members["manifest.json"] = fixtureJSON(t, raw)
		if _, err := Read(bytes.NewReader(fixtureTar(t, members))); err == nil {
			t.Fatalf("invalid optional marker admitted: %#v", marker)
		}
	}
	for _, key := range OpaqueIssueFields() {
		f := newMergeFixture(t, "pl")
		f.manifest.SourceFieldsContract = IssueSourceFieldsContract
		delete(f.records["bead"][0].Source.Fields, key)
		if _, err := Read(bytes.NewReader(f.bytes(t, nil))); err == nil {
			t.Fatalf("missing physical source column admitted: %s", key)
		}
	}
	legacy := newMergeFixture(t, "legacy")
	archive, err := Read(bytes.NewReader(legacy.bytes(t, nil)))
	if err != nil || archive.Manifest().SourceFieldsContract != "" {
		t.Fatalf("old omitted profile changed: %v", err)
	}
}

func TestSourceFieldsContractMergePreservesExactAgreement(t *testing.T) {
	left, right := newMergeFixture(t, "left"), newMergeFixture(t, "right")
	left.manifest.SourceFieldsContract = IssueSourceFieldsContract
	if _, err := Merge(bytes.NewReader(left.bytes(t, nil)), bytes.NewReader(right.bytes(t, nil))); err == nil {
		t.Fatal("mixed source-field contracts silently merged")
	}
	right.manifest.SourceFieldsContract = IssueSourceFieldsContract
	data, err := Merge(bytes.NewReader(left.bytes(t, nil)), bytes.NewReader(right.bytes(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	archive, err := Read(bytes.NewReader(data))
	if err != nil || archive.Manifest().SourceFieldsContract != IssueSourceFieldsContract {
		t.Fatalf("merged physical contract lost: %v", err)
	}
}
