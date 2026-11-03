package pluginapi

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

const APIVersion = "1.0"

const (
	RuntimeNative = "native"
	RuntimeWASI   = "wasip1"
)

type Phase string

const (
	PhaseMutate   Phase = "mutate"
	PhaseValidate Phase = "validate"
	PhasePost     Phase = "post"
)

// Manifest is the JSON equivalent of the embedded plug-plugin manifest. A
// native package may supply several plugins; the host loads explicit instances.
type Manifest struct {
	ManifestVersion int              `json:"manifest_version"`
	Plugins         []PluginManifest `json:"plugins"`
}

type PluginManifest struct {
	ID        string       `json:"id"`
	Alias     string       `json:"alias"`
	Version   string       `json:"version"`
	PluginAPI string       `json:"plugin_api"`
	Runtime   string       `json:"runtime"`
	Provides  []Provision  `json:"provides"`
	Requires  Requirements `json:"requires,omitempty"`
}

type Provision struct {
	Kind          string                 `json:"kind"`
	Name          string                 `json:"name,omitempty"`
	Phases        []Phase                `json:"phases,omitempty"`
	Reads         []string               `json:"reads,omitempty"`
	Owns          []string               `json:"owns,omitempty"`
	MaxPointReads int                    `json:"max_point_reads,omitempty"`
	MaxQueries    int                    `json:"max_queries,omitempty"`
	Prefix        string                 `json:"prefix,omitempty"`
	Methods       map[string]MethodGrant `json:"methods,omitempty"`
}

type MethodGrant struct {
	Permission string `json:"permission"`
	Resource   string `json:"resource"`
}

type Requirements struct {
	// Core API calls are explicitly granted by exact method name. Synchronous
	// callbacks still read through their pinned SnapshotReader only.
	Methods []string `json:"methods,omitempty"`
	Egress  []string `json:"egress,omitempty"`
	Secrets []string `json:"secrets,omitempty"`
	// State names the restart contract a plugin expects from Host.
	State StateRequirement `json:"state,omitempty"`
}

type StateRequirement string

const (
	StateProcessLocal StateRequirement = "process-local"
	StateDurable      StateRequirement = "durable"
)

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
var methodPart = regexp.MustCompile(`^[a-z][A-Za-z0-9_-]*$`)
var fieldPart = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`)

func DecodeManifest(data []byte) (Manifest, error) {
	var value Manifest
	if err := decodeStrict(data, &value, "manifest_version", "plugins"); err != nil {
		return Manifest{}, err
	}
	return value, value.Validate()
}

func (m Manifest) Validate() error {
	if m.ManifestVersion != 1 || len(m.Plugins) == 0 {
		return fmt.Errorf("manifest requires version 1 and at least one plugin")
	}
	aliases, ids := map[string]bool{}, map[string]bool{}
	for _, plugin := range m.Plugins {
		if err := plugin.Validate(); err != nil {
			return fmt.Errorf("plugin %q: %w", plugin.ID, err)
		}
		if aliases[plugin.Alias] || ids[plugin.ID] {
			return fmt.Errorf("duplicate plugin id or alias")
		}
		aliases[plugin.Alias], ids[plugin.ID] = true, true
	}
	return nil
}

func ValidName(name string) bool { return namePattern.MatchString(name) }

func ValidMethod(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if !methodPart.MatchString(part) {
			return false
		}
	}
	return true
}

func (p PluginManifest) Validate() error {
	if p.ID == "" || !ValidName(p.Alias) || !versionPattern.MatchString(p.Version) {
		return fmt.Errorf("id, alias and semantic version are required")
	}
	// This host currently implements 1.0 only. Refuse newer API requirements
	// explicitly instead of accepting a manifest the host cannot implement.
	if p.PluginAPI != APIVersion || p.Runtime != RuntimeNative && p.Runtime != RuntimeWASI {
		return fmt.Errorf("unsupported plugin_api or runtime (want 1.0, native or wasip1)")
	}
	if len(p.Provides) == 0 {
		return fmt.Errorf("provides must not be empty")
	}
	names, methodNames := map[string]bool{}, map[string]bool{}
	for _, provision := range p.Provides {
		switch provision.Kind {
		case "subscriber":
			if !ValidName(provision.Name) || names[provision.Name] || len(provision.Phases) == 0 {
				return fmt.Errorf("subscriber requires a unique name and phases")
			}
			names[provision.Name] = true
			if provision.Prefix != "" || len(provision.Methods) != 0 {
				return fmt.Errorf("subscriber cannot provide methods")
			}
			if provision.MaxPointReads < 0 || provision.MaxPointReads > 32 || provision.MaxQueries < 0 || provision.MaxQueries > 4 {
				return fmt.Errorf("read budgets exceed 32 point reads or 4 queries")
			}
			phases := map[Phase]bool{}
			for _, phase := range provision.Phases {
				if phase != PhaseMutate && phase != PhaseValidate && phase != PhasePost || phases[phase] {
					return fmt.Errorf("unsupported or duplicate phase %q", phase)
				}
				phases[phase] = true
			}
			if len(provision.Owns) > 0 && !phases[PhaseMutate] {
				return fmt.Errorf("only mutators may own fields")
			}
			for _, fields := range [][]string{provision.Reads, provision.Owns} {
				seen := map[string]bool{}
				for _, path := range fields {
					if !ValidField(path, true) || seen[path] {
						return fmt.Errorf("invalid or duplicate field %q", path)
					}
					seen[path] = true
				}
			}
			for _, path := range provision.Owns {
				if strings.HasPrefix(path, "meta:") && !strings.HasPrefix(path, "meta:"+p.Alias+"/") {
					return fmt.Errorf("metadata ownership must use plugin alias")
				}
			}
		case "methods":
			if provision.Prefix != "x."+p.Alias || len(provision.Methods) == 0 {
				return fmt.Errorf("method prefix must be x.%s", p.Alias)
			}
			if provision.Name != "" || len(provision.Phases)+len(provision.Reads)+len(provision.Owns) != 0 || provision.MaxPointReads != 0 || provision.MaxQueries != 0 {
				return fmt.Errorf("method provision contains subscriber fields")
			}
			for method, grant := range provision.Methods {
				if !ValidMethod(provision.Prefix+"."+method) || methodNames[method] || grant.Permission == "" || !strings.HasPrefix(grant.Resource, "params.") || !ValidField(strings.TrimPrefix(grant.Resource, "params."), false) {
					return fmt.Errorf("method %q requires a unique name, permission and resource parameter", method)
				}
				methodNames[method] = true
			}
		default:
			return fmt.Errorf("unsupported provision kind %q", provision.Kind)
		}
	}
	for _, method := range p.Requires.Methods {
		if !ValidMethod(method) {
			return fmt.Errorf("invalid required method %q", method)
		}
	}
	for _, endpoint := range p.Requires.Egress {
		host, port, err := net.SplitHostPort(endpoint)
		n, e := strconv.Atoi(port)
		if err != nil || e != nil || n < 1 || n > 65535 || host == "" || strings.ContainsAny(host, "/*@ ") {
			return fmt.Errorf("egress requires exact host:port")
		}
	}
	if len(p.Requires.Secrets) > 0 {
		return fmt.Errorf("secret injection is not implemented by this native host")
	}
	if p.Requires.State != "" && p.Requires.State != StateProcessLocal && p.Requires.State != StateDurable {
		return fmt.Errorf("unsupported state requirement %q", p.Requires.State)
	}
	return nil
}

// ValidField accepts dotted field paths and alias-qualified metadata paths.
// Wildcards are suffix-only and are permitted in grants, never in patches.
func ValidField(path string, wildcard bool) bool {
	if strings.HasPrefix(path, "meta:") {
		parts := strings.SplitN(strings.TrimPrefix(path, "meta:"), "/", 2)
		if len(parts) != 2 || !ValidName(parts[0]) || parts[1] == "" {
			return false
		}
		if wildcard && parts[1] == "*" {
			return true
		}
		for _, part := range strings.Split(parts[1], "/") {
			if !fieldPart.MatchString(part) {
				return false
			}
		}
		return true
	}
	parts := strings.Split(path, ".")
	for i, part := range parts {
		if wildcard && part == "*" && i == len(parts)-1 && i > 0 {
			continue
		}
		if !fieldPart.MatchString(part) {
			return false
		}
	}
	return len(parts) > 0
}

// AllowsField compares grants structurally; fields.foo never grants fields.foobar.
func AllowsField(grant, path string) bool {
	if grant == path {
		return true
	}
	if strings.HasSuffix(grant, ".*") {
		return strings.HasPrefix(path, strings.TrimSuffix(grant, "*"))
	}
	if strings.HasSuffix(grant, "/*") {
		return strings.HasPrefix(path, strings.TrimSuffix(grant, "*"))
	}
	return false
}

// CloneJSON copies a wire value, preserving JSON numbers without float64
// conversion. Native callbacks never share mutable maps with the write lane.
func CloneJSON[T any](value T) (T, error) {
	var out T
	data, err := json.Marshal(value)
	if err == nil {
		err = decodeJSON(data, &out)
	}
	return out, err
}
