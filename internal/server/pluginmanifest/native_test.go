package pluginmanifest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

func hostCode(t *testing.T, err error, code string) {
	t.Helper()
	var failure *pluginhost.Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("got %v, want native host refusal %s", err, code)
	}
}

func TestNativeAdapterRequiresExactDeclaredGrants(t *testing.T) {
	bundle := load(t, checkout(t))
	cases := []struct {
		name   string
		change func([]sdk.Native)
	}{
		{"reads", func(n []sdk.Native) {
			n[0].Manifest.Provides[0].Reads = append(n[0].Manifest.Provides[0].Reads, "fields.private")
		}},
		{"owns", func(n []sdk.Native) {
			n[0].Manifest.Provides[0].Owns = append(n[0].Manifest.Provides[0].Owns, "fields.private")
		}},
		{"point reads", func(n []sdk.Native) { n[0].Manifest.Provides[0].MaxPointReads = 3 }},
		{"queries", func(n []sdk.Native) { n[0].Manifest.Provides[0].MaxQueries = 2 }},
		{"phases", func(n []sdk.Native) {
			n[0].Manifest.Provides[0].Phases = append(n[0].Manifest.Provides[0].Phases, pluginapi.PhaseValidate)
		}},
		{"API methods", func(n []sdk.Native) {
			n[1].Manifest.Requires.Methods = append(n[1].Manifest.Requires.Methods, "bead.ready")
		}},
		{"egress", func(n []sdk.Native) {
			n[1].Manifest.Requires.Egress = append(n[1].Manifest.Requires.Egress, "other.example:443")
		}},
		{"permission", func(n []sdk.Native) {
			n[1].Manifest.Provides[0].Methods["echo"] = pluginapi.MethodGrant{Permission: "write", Resource: "params.id"}
		}},
		{"resource", func(n []sdk.Native) {
			n[1].Manifest.Provides[0].Methods["echo"] = pluginapi.MethodGrant{Permission: "read", Resource: "params.other"}
		}},
		{"version", func(n []sdk.Native) { n[0].Manifest.Version = "1.0.1" }},
		{"narrowed grants also require exact reviewed declaration", func(n []sdk.Native) { n[0].Manifest.Provides[0].Reads = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			implementations := natives(t)
			tc.change(implementations)
			_, err := bundle.Natives(context.Background(), implementations)
			requireCode(t, err, "grant_mismatch")
		})
	}
}

func TestNativeAdapterMatchesCompletePluginsAndExactInstances(t *testing.T) {
	bundle := load(t, checkout(t))
	for _, name := range []string{"missing", "unknown", "nil constructor", "empty instance", "path instance", "duplicate instance", "alias collision", "id collision", "method instance collision"} {
		t.Run(name, func(t *testing.T) {
			implementations := natives(t)
			switch name {
			case "missing":
				implementations = implementations[:1]
			case "unknown":
				implementations[0].Manifest.ID = "app.example.unknown"
			case "nil constructor":
				implementations[0].New = nil
			case "empty instance":
				implementations[0].Instance = ""
			case "path instance":
				implementations[0].Instance = "default/child"
			case "duplicate instance":
				implementations = append(implementations, implementations[0])
			case "alias collision":
				other := implementations[0]
				other.Manifest.ID = "app.example.other"
				other.Instance = "other"
				implementations = append(implementations, other)
			case "id collision":
				other := implementations[0]
				other.Manifest.Alias = "other"
				implementations = append(implementations, other)
			case "method instance collision":
				other := implementations[1]
				other.Instance = "other"
				implementations = append(implementations, other)
			}
			if _, err := bundle.Natives(context.Background(), implementations); err == nil {
				t.Fatal("invalid descriptor set accepted")
			}
		})
	}
	implementations := natives(t)
	other := implementations[0]
	other.Instance = "second"
	implementations = append(implementations, other)
	descriptors, err := bundle.Natives(context.Background(), implementations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pluginhost.New(pluginhost.Config{Natives: descriptors}); err != nil {
		t.Fatalf("valid independent subscriber instances refused: %v", err)
	}
	// Separate bundle adapter results still collide at composition time.
	a, err := bundle.Natives(context.Background(), natives(t))
	if err != nil {
		t.Fatal(err)
	}
	b, err := bundle.Natives(context.Background(), natives(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := pluginmanifest.ValidateNatives(append(a, b...)); err == nil {
		t.Fatal("cross-bundle alias/instance collision accepted")
	}
}

func TestActualHostRefusesMissingAndUndeclaredRegistrations(t *testing.T) {
	bundle := load(t, checkout(t))
	for _, name := range []string{"missing mutator", "missing validator", "missing method", "extra mutator", "foreign prefix", "permission mismatch"} {
		t.Run(name, func(t *testing.T) {
			implementations := natives(t)
			index := 0
			want := pluginhost.ErrManifest
			if name == "missing method" || name == "foreign prefix" || name == "permission mismatch" {
				index, want = 1, pluginhost.ErrMethod
			}
			constructor := implementations[index].New
			implementations[index].New = func(host pluginapi.Host) (sdk.Registration, error) {
				registration, err := constructor(host)
				if err != nil {
					return registration, err
				}
				switch name {
				case "missing mutator":
					delete(registration.Mutators, "title")
				case "missing validator":
					delete(registration.Validators, "check")
				case "missing method":
					registration.Methods = registration.Methods[:1]
				case "extra mutator":
					registration.Mutators["undeclared"] = registration.Mutators["title"]
				case "foreign prefix":
					registration.Methods[0].Descriptor.Name = "x.foreign.echo"
				case "permission mismatch":
					registration.Methods[0].Descriptor.Permission = "write"
				}
				return registration, nil
			}
			descriptors, err := bundle.Natives(context.Background(), implementations)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pluginhost.New(pluginhost.Config{Natives: descriptors})
			hostCode(t, err, want)
		})
	}
}

func TestAdapterRejectsValidSourceManifestGrantDrift(t *testing.T) {
	source := checkout(t)
	manifest, err := load(t, source).Manifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.Plugins[0].Provides[0].Reads = append(manifest.Plugins[0].Provides[0].Reads, "fields.private")
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	write(t, source, "plug-plugin.json", data)
	source = commit(t, source)
	bundle := load(t, source)
	_, err = bundle.Natives(context.Background(), natives(t))
	requireCode(t, err, "grant_mismatch")
}
