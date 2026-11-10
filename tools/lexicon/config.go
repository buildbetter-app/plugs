package lexicon

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Term is one lexicon rule.
type Term struct {
	Matches []string
	Pattern string
	Class   string
	Allow   map[rune]bool
}

// GoStringAllowance admits exact decoded external-protocol strings at one path.
type GoStringAllowance struct {
	Path           string
	Value          string
	PhysicalValues []string
}

// GoConstantAllowance binds one complete decoded value to a named declaration.
type GoConstantAllowance struct {
	Path, Name, Value string
}

// GoStringSetAllowance binds a finite decoded protocol set at one source path.
type GoStringSetAllowance struct {
	Path   string
	Values []string
}

// NativeControlAllowance binds a complete finite set of decoded Go string
// literals to one exact native source path. No family or prefix matching is
// supported, and imports are never admitted by this allowance.
type NativeControlAllowance struct {
	Path   string
	Values []string
}

// NativeLiteralSite binds a complete decoded vector to one source field.
// Go sites require their actual function/argv context; JSON requires its field.
type NativeLiteralSite struct {
	ID, Path, Field string
	Values          []string
}

// Config is the small TOML subset used by the repository lexicon.
type Config struct {
	SourcePlaneConstants    []GoConstantAllowance
	ruleSources             map[string][32]byte
	Terms                   []Term
	Provenance              []string
	ExternalProtocolString  GoStringAllowance
	NativeManifestStrings   GoStringSetAllowance
	NativeFieldLiterals     []NativeLiteralSite
	B03NativeGoLiterals     []NativeLiteralSite
	NativeRecordingLiterals NativeControlAllowance
	NativeViewStrings       NativeControlAllowance
}

const rulesDirectory = "tools/lexicon/rules.d"

type configSource struct {
	name string
	data []byte
}

// LoadConfig accepts a sorted directory of area rules or a single legacy
// document. Files are parsed separately, then validated as one configuration.
func LoadConfig(path string) (Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Config{}, fmt.Errorf("read lexicon: %w", err)
	}
	if !info.IsDir() {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read lexicon: %w", err)
		}
		return parseConfigSources([]configSource{{name: path, data: data}}, true)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return Config{}, fmt.Errorf("read lexicon rules: %w", err)
	}
	var sources []configSource
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".toml") {
			return Config{}, fmt.Errorf("invalid lexicon rule entry %q", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			return Config{}, fmt.Errorf("read lexicon rule %s: %w", entry.Name(), err)
		}
		sources = append(sources, configSource{name: rulesDirectory + "/" + entry.Name(), data: data})
	}
	return parseConfigSources(sources, true)
}

// configFromFiles uses the logical source paths, since hermetic inventory
// artifacts are flat and do not preserve a filesystem directory hierarchy.
func configFromFiles(files map[string][]byte) (Config, error) {
	var sources []configSource
	for name, data := range files {
		if !strings.HasPrefix(name, rulesDirectory+"/") {
			continue
		}
		rest := strings.TrimPrefix(name, rulesDirectory+"/")
		if strings.Contains(rest, "/") || !strings.HasSuffix(rest, ".toml") || !validSourcePath(name) {
			return Config{}, fmt.Errorf("invalid lexicon rule path %q", name)
		}
		sources = append(sources, configSource{name: name, data: data})
	}
	return parseConfigSources(sources, true)
}

func parseConfigSources(sources []configSource, validateBindings bool) (Config, error) {
	sort.Slice(sources, func(i, j int) bool { return sources[i].name < sources[j].name })
	var cfg Config
	var externalSection, externalPath, externalValue, externalPhysical bool
	var manifestSection, manifestPath, manifestValues bool
	var recordingSection, recordingPath, recordingValues bool
	var viewSection, viewPath, viewValues bool
	for _, source := range sources {
		if !utf8.Valid(source.data) {
			return Config{}, fmt.Errorf("lexicon %s is not valid UTF-8", source.name)
		}
		section := ""
		var current *Term
		var nativeSite *NativeLiteralSite
		var nativeFields map[string]bool
		var sourcePlaneFields map[string]bool
		var sourcePlaneConstant *GoConstantAllowance
		for _, line := range strings.Split(string(source.data), "\n") {
			line = strings.TrimSpace(strings.SplitN(line, "#", 2)[0])
			if line == "" {
				continue
			}
			switch line {
			case "[[term]]":
				cfg.Terms = append(cfg.Terms, Term{})
				current = &cfg.Terms[len(cfg.Terms)-1]
				section = "term"
				continue
			case "[zones]":
				section = "zones"
				current = nil
				continue
			case "[external_protocol_string]":
				if externalSection {
					return Config{}, errors.New("duplicate external protocol string section")
				}
				externalSection = true
				section = "external_protocol_string"
				current = nil
				continue
			case "[native_recording_literals]":
				if recordingSection {
					return Config{}, errors.New("duplicate native recording literal section")
				}
				recordingSection = true
				section = "native_recording_literals"
				current = nil
				continue
			case "[native_view_strings]":
				if viewSection {
					return Config{}, errors.New("duplicate native view string section")
				}
				viewSection = true
				section = "native_view_strings"
				current = nil
				continue
			case "[native_manifest_strings]":
				if manifestSection {
					return Config{}, errors.New("duplicate native manifest string section")
				}
				manifestSection = true
				section = "native_manifest_strings"
				current = nil
				continue
			case "[[source_plane_constant]]":
				cfg.SourcePlaneConstants = append(cfg.SourcePlaneConstants, GoConstantAllowance{})
				sourcePlaneConstant = &cfg.SourcePlaneConstants[len(cfg.SourcePlaneConstants)-1]
				sourcePlaneFields = make(map[string]bool)
				section, current = "source_plane_constant", nil
				continue
			case "[[native_field_literal]]":
				cfg.NativeFieldLiterals = append(cfg.NativeFieldLiterals, NativeLiteralSite{})
				nativeSite = &cfg.NativeFieldLiterals[len(cfg.NativeFieldLiterals)-1]
				nativeFields = make(map[string]bool)
				section, current = "native_field_literal", nil
				continue
			case "[[b03_native_go_literal]]":
				cfg.B03NativeGoLiterals = append(cfg.B03NativeGoLiterals, NativeLiteralSite{})
				nativeSite = &cfg.B03NativeGoLiterals[len(cfg.B03NativeGoLiterals)-1]
				nativeFields = make(map[string]bool)
				section, current = "b03_native_go_literal", nil
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			if !ok {
				return Config{}, fmt.Errorf("invalid lexicon line %q", line)
			}
			key, value = strings.TrimSpace(key), strings.TrimSpace(value)
			if section == "source_plane_constant" {
				if sourcePlaneFields[key] {
					return Config{}, fmt.Errorf("duplicate source plane constant field: %s", key)
				}
				sourcePlaneFields[key] = true
				decoded, err := strconv.Unquote(value)
				if err != nil || !strings.HasPrefix(value, "\"") {
					return Config{}, errors.New("source plane constant fields require quoted scalars")
				}
				switch key {
				case "path":
					sourcePlaneConstant.Path = decoded
				case "name":
					sourcePlaneConstant.Name = decoded
				case "value":
					sourcePlaneConstant.Value = decoded
				default:
					return Config{}, fmt.Errorf("unknown source plane constant field: %s", key)
				}
				continue
			}
			if section == "native_field_literal" || section == "b03_native_go_literal" {
				if nativeFields[key] {
					return Config{}, fmt.Errorf("duplicate native field literal field: %s", key)
				}
				nativeFields[key] = true
				switch key {
				case "id", "path", "field":
					decoded, err := strconv.Unquote(value)
					if err != nil || !strings.HasPrefix(value, "\"") {
						return Config{}, errors.New("native field literal fields require a quoted scalar")
					}
					switch key {
					case "id":
						nativeSite.ID = decoded
					case "path":
						nativeSite.Path = decoded
					case "field":
						nativeSite.Field = decoded
					}
				case "values":
					// JSON's quoted-array subset also preserves commas inside argv.
					if !strings.HasPrefix(value, "[") || json.Unmarshal([]byte(value), &nativeSite.Values) != nil || len(nativeSite.Values) == 0 {
						return Config{}, errors.New("native field literal values require a nonempty quoted array")
					}
				default:
					return Config{}, fmt.Errorf("unknown native field literal field: %s", key)
				}
				continue
			}
			values, err := quotedValues(value)
			if err != nil {
				return Config{}, fmt.Errorf("lexicon %s: %w", key, err)
			}
			if section == "zones" && key == "provenance" {
				cfg.Provenance = append(cfg.Provenance, values...)
				continue
			}
			if section == "native_manifest_strings" {
				switch key {
				case "path":
					if manifestPath || !strings.HasPrefix(value, "\"") || len(values) != 1 || (validateBindings && values[0] != nativeManifestProtocolSource) {
						return Config{}, errors.New("native manifest strings require their exact source path")
					}
					manifestPath = true
					cfg.NativeManifestStrings.Path = values[0]
				case "values":
					if manifestValues || !strings.HasPrefix(value, "[") || (validateBindings && len(values) != len(nativeManifestProtocolDigests)) {
						return Config{}, errors.New("native manifest strings require one complete exact literal set")
					}
					manifestValues = true
					seen := make(map[string]bool)
					for _, value := range values {
						digest := protocolLiteralDigest(value)
						if (validateBindings && !nativeManifestProtocolDigests[digest]) || seen[digest] {
							return Config{}, errors.New("native manifest string is unlisted or duplicated")
						}
						seen[digest] = true
					}
					cfg.NativeManifestStrings.Values = values
				default:
					return Config{}, fmt.Errorf("unknown native manifest string key: %s", key)
				}
				continue
			}
			if section == "external_protocol_string" {
				if key != "physical_values" && len(values) != 1 {
					return Config{}, errors.New("external protocol string fields require one value")
				}
				switch key {
				case "path":
					if externalPath {
						return Config{}, errors.New("duplicate external protocol string path")
					}
					externalPath = true
					cfg.ExternalProtocolString.Path = values[0]
				case "value":
					if externalValue {
						return Config{}, errors.New("duplicate external protocol string value")
					}
					externalValue = true
					cfg.ExternalProtocolString.Value = values[0]
				case "physical_values":
					if externalPhysical || (validateBindings && len(values) != len(physicalProtocolDigests)) {
						return Config{}, errors.New("physical protocol strings require one complete exact literal set")
					}
					externalPhysical = true
					seen := make(map[string]bool)
					for _, value := range values {
						digest := protocolLiteralDigest(value)
						if (validateBindings && !physicalProtocolDigests[digest]) || seen[digest] {
							return Config{}, errors.New("physical protocol string is unlisted or duplicated")
						}
						seen[digest] = true
					}
					cfg.ExternalProtocolString.PhysicalValues = values
				default:
					return Config{}, fmt.Errorf("unknown external protocol string key: %s", key)
				}
				continue
			}
			if section == "native_recording_literals" || section == "native_view_strings" {
				allowance := &cfg.NativeRecordingLiterals
				source := nativeRecordingLiteralSource
				digests := nativeRecordingLiteralDigests
				pathSeen, valuesSeen := &recordingPath, &recordingValues
				if section == "native_view_strings" {
					allowance = &cfg.NativeViewStrings
					source = nativeViewProtocolSource
					digests = nativeViewProtocolDigests
					pathSeen, valuesSeen = &viewPath, &viewValues
				}
				switch key {
				case "path":
					if *pathSeen || !strings.HasPrefix(value, "\"") || len(values) != 1 || (validateBindings && values[0] != source) {
						return Config{}, errors.New("native control literals require their exact source path")
					}
					*pathSeen = true
					allowance.Path = values[0]
				case "values":
					if *valuesSeen || !strings.HasPrefix(value, "[") || (validateBindings && len(values) != len(digests)) {
						return Config{}, errors.New("native control literals require one complete exact set")
					}
					*valuesSeen = true
					seen := make(map[string]bool)
					for _, literal := range values {
						digest := protocolLiteralDigest(literal)
						if (validateBindings && !digests[digest]) || seen[digest] {
							return Config{}, errors.New("native control literal is unlisted or duplicated")
						}
						seen[digest] = true
					}
					allowance.Values = values
				default:
					return Config{}, fmt.Errorf("unknown native control literal key: %s", key)
				}
				continue
			}
			if current == nil {
				return Config{}, fmt.Errorf("lexicon key outside a section: %s", key)
			}
			switch key {
			case "match":
				current.Matches = values
			case "regex":
				if len(values) != 1 {
					return Config{}, errors.New("regex must have one value")
				}
				current.Pattern = values[0]
			case "class":
				if len(values) != 1 {
					return Config{}, errors.New("class must have one value")
				}
				current.Class = values[0]
			case "allow":
				current.Allow = make(map[rune]bool)
				for _, value := range values {
					for _, r := range value {
						current.Allow[r] = true
					}
				}
			default:
				return Config{}, fmt.Errorf("unknown lexicon key: %s", key)
			}
		}
	}
	if len(cfg.Terms) == 0 || len(cfg.Provenance) == 0 {
		return Config{}, errors.New("lexicon has no terms or provenance zones")
	}
	for i := range cfg.Terms {
		if cfg.Terms[i].Class == "" || (len(cfg.Terms[i].Matches) == 0 && cfg.Terms[i].Pattern == "") {
			return Config{}, fmt.Errorf("lexicon term %d is incomplete", i)
		}
	}
	if validateBindings && externalSection && (!externalPath || !externalValue || cfg.ExternalProtocolString.Path != externalProtocolSource || protocolLiteralDigest(cfg.ExternalProtocolString.Value) != externalEnvironmentDigest) {
		return Config{}, errors.New("external protocol string must bind the exact source path and environment literal")
	}
	if recordingSection && (!recordingPath || !recordingValues) || viewSection && (!viewPath || !viewValues) {
		return Config{}, errors.New("native control literals lack their exact path or complete set")
	}
	if manifestSection && (!manifestPath || !manifestValues) {
		return Config{}, errors.New("native manifest strings lack their exact path or literal set")
	}
	if validateBindings && len(cfg.SourcePlaneConstants) != 0 && !validSourcePlaneConstants(cfg.SourcePlaneConstants) {
		return Config{}, errors.New("source plane constants require both exact path, name and value bindings")
	}
	if validateBindings && len(cfg.NativeFieldLiterals) != 0 && !validNativeFieldLiterals(cfg.NativeFieldLiterals) {
		return Config{}, errors.New("native field literals require all four exact path/field/value sites")
	}
	if validateBindings && len(cfg.B03NativeGoLiterals) != 0 && !validB03NativeGoLiterals(cfg.B03NativeGoLiterals) {
		return Config{}, errors.New("B03 Go literals require both exact path/field/value sites")
	}
	cfg.ruleSources = make(map[string][32]byte)
	for _, source := range sources {
		if strings.HasPrefix(source.name, rulesDirectory+"/") {
			cfg.ruleSources[source.name] = sha256.Sum256(source.data)
		}
	}
	return cfg, nil
}
