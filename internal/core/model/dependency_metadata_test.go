package model

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDependencyMetadataRetainsEqualNativeText(t *testing.T) {
	for _, test := range []struct {
		name, stored, incoming string
		equal                  bool
	}{
		{"object order and numeric form", `{ "a":1.0,"b":[true,null,"x"] }`, `{"b":[true,null,"x"],"a":1e0}`, true},
		{"fraction", `{"n":0.0100}`, `{"n":1e-2}`, true},
		{"negative zero", `{"n":-0.0}`, `{"n":0e999999999}`, true},
		{"overflow byte fallback", `{"n":1e999999999}`, `{"n":10e999999998}`, false},
		{"overflow identical", `{"n":1e999999999}`, `{"n":1e999999999}`, true},
		{"wide equivalent", `{"n":9007199254740993}`, `{"n":9007199254740993.0}`, true},
		{"native wide rounding", `{"n":9007199254740993}`, `{"n":9007199254740992}`, true},
		{"wide distinct", `{"n":9007199254740993}`, `{"n":9007199254740994}`, false},
		{"duplicate byte fallback", `{"x":1,"x":2}`, `{"x":2}`, false},
		{"duplicate identical", `{"x":1,"x":2}`, `{"x":1,"x":2}`, true},
		{"number versus string", `{"n":1}`, `{"n":"1"}`, false},
		{"array order", `[1,2]`, `[2,1]`, false},
		{"missing versus null", `{}`, `{"n":null}`, false},
		{"invalid equal", `not json`, `not json`, true},
		{"invalid unequal", `not json`, `not  json`, false},
		{"invalid versus valid", `{"n":`, `{"n":1}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stored, _ := json.Marshal(test.stored)
			incoming, _ := json.Marshal(test.incoming)
			before := LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": string(stored), "beads.thread_id": `"  exact thread  "`, "beads.future": `{ "n":9007199254740993 }`}}
			request := LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": string(incoming)}}
			got := request.Retaining(&before)
			want := before.Clone()
			if !test.equal {
				want.NativeExtensions["beads.metadata"] = string(incoming)
			}
			if !reflect.DeepEqual(got, want) || dependencyMetadataTokenEqual(string(stored), string(incoming)) != test.equal || dependencyMetadataTokenEqual(string(incoming), string(stored)) != test.equal {
				t.Fatalf("metadata equality or exact retained token differs: %+v want=%+v", got, want)
			}
			got.NativeExtensions["beads.future"] = `null`
			if before.NativeExtensions["beads.future"] != `{ "n":9007199254740993 }` || request.NativeExtensions["beads.metadata"] != string(incoming) {
				t.Fatal("metadata retention mutated its inputs")
			}
		})
	}
}

func TestDependencyMetadataPhysicalValueAndOpaqueTokensStayDistinct(t *testing.T) {
	for _, pair := range [][2]string{{`null`, `"null"`}, {`""`, `"{}"`}, {`{}`, `"{}"`}, {`"null"`, `"{}"`}} {
		if dependencyMetadataTokenEqual(pair[0], pair[1]) {
			t.Fatalf("physical metadata values collapsed: %v", pair)
		}
	}
	previous := LinkMetadata{NativeExtensions: map[string]string{"beads.future": `{ "n":1.0 }`, "beads.metadata": `null`}}
	incoming := LinkMetadata{NativeExtensions: map[string]string{"beads.future": `{"n":1}`, "beads.metadata": `"{}"`}}
	if got := incoming.Retaining(&previous); !reflect.DeepEqual(got, incoming) {
		t.Fatal("semantic metadata rule reinterpreted another opaque field or null")
	}
}
