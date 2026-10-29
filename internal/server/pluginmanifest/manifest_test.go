package pluginmanifest_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/pluginapi"
)

func TestActualPublicManifestValidationRefusesUnsupportedDeclarations(t *testing.T) {
	cases := []struct {
		name   string
		change func(*pluginapi.Manifest)
	}{
		{"manifest version", func(m *pluginapi.Manifest) { m.ManifestVersion = 2 }},
		{"plugin API major", func(m *pluginapi.Manifest) { m.Plugins[0].PluginAPI = "2.0" }},
		{"plugin API future minor", func(m *pluginapi.Manifest) { m.Plugins[0].PluginAPI = "1.1" }},
		{"version", func(m *pluginapi.Manifest) { m.Plugins[0].Version = "latest" }},
		{"wasm", func(m *pluginapi.Manifest) { m.Plugins[0].Runtime = "wasm" }},
		{"authz kind", func(m *pluginapi.Manifest) { m.Plugins[0].Provides[0].Kind = "authz" }},
		{"phase", func(m *pluginapi.Manifest) { m.Plugins[0].Provides[0].Phases = []pluginapi.Phase{"async"} }},
		{"field", func(m *pluginapi.Manifest) { m.Plugins[0].Provides[0].Reads = []string{"fields.*.private"} }},
		{"foreign metadata", func(m *pluginapi.Manifest) { m.Plugins[0].Provides[0].Owns = []string{"meta:other/*"} }},
		{"point budget", func(m *pluginapi.Manifest) { m.Plugins[0].Provides[0].MaxPointReads = 33 }},
		{"query budget", func(m *pluginapi.Manifest) { m.Plugins[0].Provides[0].MaxQueries = 5 }},
		{"prefix", func(m *pluginapi.Manifest) { m.Plugins[1].Provides[0].Prefix = "x.other" }},
		{"permission", func(m *pluginapi.Manifest) {
			m.Plugins[1].Provides[0].Methods["echo"] = pluginapi.MethodGrant{Resource: "params.id"}
		}},
		{"resource", func(m *pluginapi.Manifest) {
			m.Plugins[1].Provides[0].Methods["echo"] = pluginapi.MethodGrant{Permission: "read", Resource: "id"}
		}},
		{"method grant", func(m *pluginapi.Manifest) { m.Plugins[1].Requires.Methods = []string{"*"} }},
		{"egress grant", func(m *pluginapi.Manifest) { m.Plugins[1].Requires.Egress = []string{"*.example:443"} }},
		{"secrets", func(m *pluginapi.Manifest) { m.Plugins[1].Requires.Secrets = []string{"api-key"} }},
		{"duplicate alias", func(m *pluginapi.Manifest) {
			p := m.Plugins[0]
			p.ID = "app.example.other"
			m.Plugins = append(m.Plugins, p)
		}},
		{"duplicate id", func(m *pluginapi.Manifest) {
			p := m.Plugins[0]
			p.Alias = "other"
			m.Plugins = append(m.Plugins, p)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := checkout(t)
			manifest, err := load(t, source).Manifest()
			if err != nil {
				t.Fatal(err)
			}
			tc.change(&manifest)
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			write(t, source, "plug-plugin.json", data)
			source = commit(t, source)
			_, err = pluginmanifest.Load(context.Background(), source)
			requireCode(t, err, "manifest_invalid")
		})
	}
}

func TestStrictJSONRefusesUnknownDuplicateNullAndShellFields(t *testing.T) {
	baseManifest, err := sourceFiles.ReadFile("fixture/plug-plugin.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(baseManifest, &doc); err != nil {
		t.Fatal(err)
	}
	compact, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	base := string(compact)
	cases := map[string]string{
		"unknown root":         strings.Replace(base, `"manifest_version":1`, `"manifest_version":1,"unknown":true`, 1),
		"unknown nested":       strings.Replace(base, `"alias":"normalize"`, `"alias":"normalize","config":{"schema":"../outside"}`, 1),
		"case mismatch":        strings.Replace(base, `"plugin_api":"1.0"`, `"Plugin_API":"1.0"`, 1),
		"duplicate root":       strings.Replace(base, `"manifest_version":1`, `"manifest_version":9,"manifest_version":1`, 1),
		"duplicate grant":      strings.Replace(base, `"permission":"read"`, `"permission":"write","permission":"read"`, 1),
		"null runtime":         strings.Replace(base, `"runtime":"native"`, `"runtime":null`, 1),
		"runtime build object": strings.Replace(base, `"runtime":"native"`, `"runtime":{"wasm":{"build":{"go":"../outside"}}}`, 1),
		"shell entrypoint":     strings.Replace(base, `"runtime":"native"`, `"runtime":"native","entrypoint":"sh -c anything"`, 1),
		"trailing value":       base + `{}`,
		"plain YAML":           "manifest_version: 1\nplugins: []\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if data == base {
				t.Fatal("negative fixture did not change source")
			}
			source := checkout(t)
			write(t, source, "plug-plugin.json", []byte(data))
			source = commit(t, source)
			_, err := pluginmanifest.Load(context.Background(), source)
			requireCode(t, err, "manifest_invalid")
		})
	}
}

func TestManifestMissingAmbiguousAndNestedRefuse(t *testing.T) {
	for _, kind := range []string{"missing", "ambiguous", "nested"} {
		t.Run(kind, func(t *testing.T) {
			source := checkout(t)
			data, err := os.ReadFile(filepath.Join(source.Root, "plug-plugin.json"))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing":
				if err := os.Remove(filepath.Join(source.Root, "plug-plugin.json")); err != nil {
					t.Fatal(err)
				}
			case "ambiguous":
				write(t, source, "plug-plugin.yaml", data)
			case "nested":
				if err := os.Mkdir(filepath.Join(source.Root, "nested"), 0o755); err != nil {
					t.Fatal(err)
				}
				write(t, source, "nested/manifest.json", data)
				source.ManifestFile = "nested/manifest.json"
			}
			source = commit(t, source)
			_, err = pluginmanifest.Load(context.Background(), source)
			if kind == "nested" {
				requireCode(t, err, "containment")
			} else {
				requireCode(t, err, "manifest_invalid")
			}
		})
	}
}
