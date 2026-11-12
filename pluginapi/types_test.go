package pluginapi

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestWireDomains(t *testing.T) {
	for _, input := range []string{`0`, `1.0`, `1e0`, `18446744073709551616`, `1e1000000000`} {
		var value Natural
		if err := json.Unmarshal([]byte(input), &value); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var again Natural
		if err = json.Unmarshal(data, &again); err != nil || again != value {
			t.Fatalf("roundtrip %s: %s, %v", input, data, err)
		}
	}
	if _, err := Natural("18446744073709551616").Uint64(); err == nil {
		t.Fatal("overflow accepted")
	}
	for _, input := range []string{`-1`, `1.1`, `null`, `"1"`} {
		var value Natural
		if json.Unmarshal([]byte(input), &value) == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	for _, input := range []string{`{"id":""}`, `{"key":""}`, `{"id":"pl-a","branch":1e30}`} {
		var value Ref
		if err := json.Unmarshal([]byte(input), &value); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var again Ref
		if err := json.Unmarshal(data, &again); err != nil || again.IsID() != value.IsID() {
			t.Fatalf("ref roundtrip: %s %v", data, err)
		}
	}
	for _, input := range []string{`{}`, `{"id":"a","key":"b"}`, `{"key":"a","branch":0}`, `{"ID":"a"}`, `{"id":null}`, `{"id":"a","extra":1}`} {
		var value Ref
		if json.Unmarshal([]byte(input), &value) == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	for _, input := range []string{`{"kind":"user","id":"u","sponsor":null}`, `{"kind":"plugin","id":"test/default","caveats":{"limit":9007199254740993}}`} {
		var p PrincipalRef
		if err := json.Unmarshal([]byte(input), &p); err != nil {
			t.Fatal(err)
		}
		if _, err := json.Marshal(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, input := range []string{`{"kind":"root","id":"u"}`, `{"kind":"user"}`, `{"kind":"user","id":null}`} {
		var p PrincipalRef
		if json.Unmarshal([]byte(input), &p) == nil {
			t.Fatalf("accepted principal %s", input)
		}
	}
}

func TestCursorAfterIsOptionalAndRoundTrips(t *testing.T) {
	legacy := Cursor{Incarnation: "inc", Revision: Natural64(7), Index: Natural64(3), Method: "query", FilterDigest: "digest"}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"after"`) {
		t.Fatalf("empty cursor after changed legacy wire shape: %s", raw)
	}
	withAfter := legacy
	withAfter.After = "opaque/key"
	raw, err = json.Marshal(withAfter)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Cursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.After != withAfter.After {
		t.Fatalf("cursor after did not round-trip: got %q want %q", decoded.After, withAfter.After)
	}
}

func TestFlatOp(t *testing.T) {
	input := `{"kind":"patch","target":{"id":"pl-a"},"fields":{"priority":9007199254740993}}`
	var op Op
	if err := json.Unmarshal([]byte(input), &op); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(op)
	if err != nil || string(data) != input {
		t.Fatalf("flat op: %s %v", data, err)
	}
	op.Kind = "create"
	if _, err := json.Marshal(op); err == nil {
		t.Fatal("mismatched operation accepted")
	}
}

func fixtureManifest() PluginManifest {
	return PluginManifest{ID: "app.example.test", Alias: "test", Version: "1.0.0", PluginAPI: APIVersion, Runtime: "native", Provides: []Provision{
		{Kind: "subscriber", Name: "rank", Phases: []Phase{PhaseMutate}, Reads: []string{"status"}, Owns: []string{"fields.rank"}},
		{Kind: "methods", Prefix: "x.test", Methods: map[string]MethodGrant{"rank": {Permission: "write", Resource: "params.id"}}},
	}}
}

func TestManifestValidation(t *testing.T) {
	manifest := Manifest{ManifestVersion: 1, Plugins: []PluginManifest{fixtureManifest()}}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeManifest(data); err != nil {
		t.Fatal(err)
	}
	wasi := fixtureManifest()
	wasi.Runtime = RuntimeWASI
	if err := wasi.Validate(); err != nil {
		t.Fatalf("wasip1 manifest rejected: %v", err)
	}
	if _, err := DecodeManifest([]byte(strings.Replace(string(data), `"runtime":"native"`, `"runtime":"native","shell":"bad"`, 1))); err == nil {
		t.Fatal("unknown manifest field accepted")
	}
	cases := []struct {
		name   string
		change func(*PluginManifest)
	}{
		{"prefix", func(p *PluginManifest) { p.Provides[1].Prefix = "bead" }},
		{"permission", func(p *PluginManifest) { p.Provides[1].Methods["rank"] = MethodGrant{Resource: "params.id"} }},
		{"resource", func(p *PluginManifest) { p.Provides[1].Methods["rank"] = MethodGrant{Permission: "write"} }},
		{"points", func(p *PluginManifest) { p.Provides[0].MaxPointReads = 33 }},
		{"queries", func(p *PluginManifest) { p.Provides[0].MaxQueries = 5 }},
		{"foreign metadata", func(p *PluginManifest) { p.Provides[0].Owns = []string{"meta:other/*"} }},
		{"phase", func(p *PluginManifest) { p.Provides[0].Phases = []Phase{"listener"} }},
		{"runtime", func(p *PluginManifest) { p.Runtime = "wasm" }},
		{"api", func(p *PluginManifest) { p.PluginAPI = "2.0" }},
		{"egress", func(p *PluginManifest) { p.Requires.Egress = []string{"*.example.com:443"} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := fixtureManifest()
			c.change(&p)
			if p.Validate() == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
	manifest.Plugins = append(manifest.Plugins, fixtureManifest())
	if manifest.Validate() == nil {
		t.Fatal("duplicate alias accepted")
	}
}
