package dumpformat

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
)

func TestSourcePlaneSidecarsRetainAbsentNullAndExactObservationBytes(t *testing.T) {
	f := sourcePlaneReaderFixture(t, "sidecar")
	for i := range f.config {
		if f.config[i].Input.State == "json-config" {
			f.config[i].Values["observation"] = map[string]any{"number": json.Number("9007199254740993"), "decimal": json.Number("1.0"), "explicit_null": nil, "raw": " bytes with whitespace ", "mode": "captured", "hash": "original-source-hash"}
		}
	}
	data := sourcePlaneReaderBytes(t, f)
	a, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	origins, err := a.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := a.Member("config/sources.json")
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	if len(origins) != len(entries) || len(origins) == 0 {
		t.Fatal("incomplete sidecar census")
	}
	absent, present := false, false
	for _, entry := range entries {
		var fields map[string]json.RawMessage
		var declared externalEntry
		if err := json.Unmarshal(entry, &fields); err != nil {
			t.Fatal(err)
		}
		if err := decodeJSON(entry, &declared); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, origin := range origins {
			if origin.Path != declared.Input.Path || origin.Layer != declared.Input.Family {
				continue
			}
			found = true
			if origin.Raw.SHA256 != digest(entry) || origin.Raw.Size != uint64(len(entry)) || origin.Table != "config/sources.json" || origin.ID != declared.Input.Path || origin.Mode != declared.Input.State || origin.Action != "origin-only" || !reflect.DeepEqual(origin.Precedence, []string{strconv.Itoa(declared.Input.Precedence)}) {
				t.Fatal("captured sidecar identity or bytes changed")
			}
			values, ok := fields["values"]
			if !ok {
				absent = true
				if origin.Parsed.Presence != "absent" || origin.Parsed.Size != 0 || origin.Parsed.SHA256 != "" {
					t.Fatal("absent values invented")
				}
			} else {
				present = true
				if origin.Parsed.Presence != "object" || origin.Parsed.SHA256 != digest(values) || origin.Parsed.Size != uint64(len(values)) || !bytes.Contains(values, []byte("9007199254740993")) || !bytes.Contains(values, []byte("1.0")) || !bytes.Contains(values, []byte(`"explicit_null":null`)) {
					t.Fatal("captured parsed values lost number or null fidelity")
				}
			}
		}
		if !found {
			t.Fatal("missing captured sidecar")
		}
	}
	if !absent || !present {
		t.Fatal("invalid absent/present baseline")
	}
	origins[0].Precedence[0] = "changed caller copy"
	if len(origins[0].Fields) > 0 {
		origins[0].Fields[0].Name = "changed"
	}
	again, err := a.SourcePlaneSidecarOrigins()
	if err != nil || reflect.DeepEqual(origins, again) {
		t.Fatal("sidecar accessor aliased caller", err)
	}
	legacy, err := Read(bytes.NewReader(newMergeFixture(t, "legacy").bytes(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.SourcePlaneSidecarOrigins(); err == nil {
		t.Fatal("legacy absence enabled source planes")
	}
	if _, err := (*Archive)(nil).SourcePlaneSidecarOrigins(); err == nil {
		t.Fatal("nil archive enabled source planes")
	}
}
