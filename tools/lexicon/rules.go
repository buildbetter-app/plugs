package lexicon

import (
	"embed"
	"fmt"
	"strings"
)

// The reviewed per-area files are also the compiled acceptance boundary for
// decoded protocol strings. Editing a runtime config cannot broaden a path,
// value set, or AST/JSON tuple beyond the exact rules built into this checker.
//
//go:embed rules.d/*.toml
var areaRules embed.FS

var (
	canonicalRules                                           Config
	externalProtocolSource, nativeManifestProtocolSource     string
	b01NativeGoSource, b01NativeJSONSource                   string
	b03CatalogJSONSource, b03CatalogGoSource                 string
	nativeRecordingLiteralSource, nativeViewProtocolSource   string
	externalEnvironmentDigest                                string
	physicalProtocolDigests, nativeManifestProtocolDigests   map[string]bool
	nativeRecordingLiteralDigests, nativeViewProtocolDigests map[string]bool
)

func init() {
	entries, err := areaRules.ReadDir("rules.d")
	if err != nil {
		panic(err)
	}
	sources := make([]configSource, 0, len(entries))
	for _, entry := range entries {
		data, err := areaRules.ReadFile("rules.d/" + entry.Name())
		if err != nil {
			panic(err)
		}
		sources = append(sources, configSource{name: rulesDirectory + "/" + entry.Name(), data: data})
	}
	canonicalRules, err = parseConfigSources(sources, false)
	if err != nil {
		panic(fmt.Errorf("compiled area rules: %w", err))
	}
	externalProtocolSource = canonicalRules.ExternalProtocolString.Path
	nativeManifestProtocolSource = canonicalRules.NativeManifestStrings.Path
	b01NativeGoSource = canonicalSite(canonicalRules.NativeFieldLiterals, "ready-types").Path
	b01NativeJSONSource = canonicalSite(canonicalRules.NativeFieldLiterals, "native-type-binding-json").Path
	b03CatalogJSONSource = canonicalSite(canonicalRules.NativeFieldLiterals, "catalog-sql-tables").Path
	b03CatalogGoSource = canonicalSite(canonicalRules.B03NativeGoLiterals, "catalog-go-tables").Path
	nativeRecordingLiteralSource = canonicalRules.NativeRecordingLiterals.Path
	nativeViewProtocolSource = canonicalRules.NativeViewStrings.Path
	externalEnvironmentDigest = protocolLiteralDigest(canonicalRules.ExternalProtocolString.Value)
	physicalProtocolDigests = literalDigests(canonicalRules.ExternalProtocolString.PhysicalValues)
	nativeManifestProtocolDigests = literalDigests(canonicalRules.NativeManifestStrings.Values)
	nativeRecordingLiteralDigests = literalDigests(canonicalRules.NativeRecordingLiterals.Values)
	nativeViewProtocolDigests = literalDigests(canonicalRules.NativeViewStrings.Values)
}

func canonicalSite(sites []NativeLiteralSite, id string) NativeLiteralSite {
	for _, site := range sites {
		if site.ID == id {
			return site
		}
	}
	panic("missing compiled protocol site " + id)
}

func literalDigests(values []string) map[string]bool {
	digests := make(map[string]bool, len(values))
	for _, value := range values {
		digests[protocolLiteralDigest(value)] = true
	}
	return digests
}

func siteDigests(sites []NativeLiteralSite) map[string]string {
	digests := make(map[string]string, len(sites))
	for _, site := range sites {
		fields := append([]string{site.Path, site.Field}, site.Values...)
		digests[site.ID] = protocolLiteralDigest(strings.Join(fields, "\x00"))
	}
	return digests
}
