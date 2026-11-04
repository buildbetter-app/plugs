package composition

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

const maxConfigBytes = 1 << 20

// LoadConfigFile reads the small, deliberately bounded server configuration.
// It accepts the documented flat keys and one nested map level without pulling
// a general YAML implementation into the server binary. Values are never
// included in parse errors because DSNs may contain credentials.
func LoadConfigFile(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, errors.New("composition: config path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("composition: read config: %w", err)
	}
	if len(data) > maxConfigBytes {
		return Config{}, errors.New("composition: config exceeds 1 MiB")
	}
	return ParseConfig(data)
}

// ParseConfig parses the supported .plug/config.yaml subset. The parser is
// intentionally strict: aliases that map to one setting cannot be repeated,
// unknown keys are refused, and flow collections/tags/anchors are not
// accepted. This keeps startup behavior reviewable and secret-free.
func ParseConfig(data []byte) (Config, error) {
	values, err := parseConfigMap(data)
	if err != nil {
		return Config{}, err
	}
	config := Config{
		Driver:   "memory",
		Project:  DefaultProject,
		Address:  DefaultAddress,
		UI:       true,
		AuthMode: "anonymous",
	}
	// The local administrator may select an existing protected source identity.
	// The file supplies no executable binding, source body or authorization;
	// Open validates the durable source before admitting requests.
	for key, raw := range values {
		switch key {
		case "driver":
			config.Driver, err = configString(raw, key)
		case "dsn":
			config.DSN, err = configString(raw, key)
		case "address":
			config.Address, err = configString(raw, key)
		case "project":
			config.Project, err = configString(raw, key)
		case "ui":
			config.UI, err = configBool(raw, key)
		case "auth.mode":
			config.AuthMode, err = configString(raw, key)
		case "ontology.source":
			var id string
			id, err = configString(raw, key)
			if err == nil {
				ref := model.RefByID(id, model.Natural64(0))
				err = validateOntologySourceRef(ref)
				if err == nil {
					config.Ontology = &OntologyConfig{Source: service.StaticOntologySource(ref)}
				}
			}
		default:
			// parseConfigMap canonicalizes and validates all keys. Keep this
			// guard in case a future alias is added without a field mapping.
			return Config{}, fmt.Errorf("composition: unsupported config key %q", key)
		}
		if err != nil {
			return Config{}, err
		}
	}
	if strings.TrimSpace(config.Project) == "" {
		return Config{}, errors.New("composition: project must not be empty")
	}
	if err := validateAddress(config.Address); err != nil {
		return Config{}, err
	}
	return normalizeConfig(config)
}

type configValue struct {
	text string
	kind configValueKind
}

type configValueKind uint8

const (
	configStringValue configValueKind = iota + 1
	configBoolValue
)

func configString(value configValue, key string) (string, error) {
	if value.kind != configStringValue {
		return "", fmt.Errorf("composition: config key %q must be a string", key)
	}
	return value.text, nil
}

func configBool(value configValue, key string) (bool, error) {
	if value.kind != configBoolValue {
		return false, fmt.Errorf("composition: config key %q must be true or false", key)
	}
	return value.text == "true", nil
}

func parseConfigMap(data []byte) (map[string]configValue, error) {
	lines := bytes.Split(bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n")), []byte("\n"))
	values := make(map[string]configValue)
	sections := make(map[string]bool)
	sectionValues := make(map[string]bool)
	section := ""
	sectionIndent := -1
	for number, line := range lines {
		lineNumber := number + 1
		if number == 0 {
			line = bytes.TrimPrefix(line, []byte{0xef, 0xbb, 0xbf})
		}
		if bytes.IndexByte(line, '\t') >= 0 {
			return nil, fmt.Errorf("composition: config line %d uses tabs", lineNumber)
		}
		line = stripComment(line)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		trimmed := bytes.TrimSpace(line)
		if bytes.Equal(trimmed, []byte("---")) || bytes.Equal(trimmed, []byte("...")) {
			if bytes.Equal(trimmed, []byte("...")) {
				section = ""
				sectionIndent = -1
			}
			continue
		}
		indent := len(line) - len(bytes.TrimLeft(line, " "))
		keyRaw, valueRaw, ok := bytes.Cut(bytes.TrimSpace(line), []byte(":"))
		if !ok {
			return nil, fmt.Errorf("composition: malformed config line %d", lineNumber)
		}
		key := string(keyRaw)
		if !validConfigKey(key) {
			return nil, fmt.Errorf("composition: invalid config key on line %d", lineNumber)
		}
		valueRaw = bytes.TrimSpace(valueRaw)
		if indent == 0 {
			section = ""
			sectionIndent = -1
			if len(valueRaw) == 0 {
				if !validSection(key) {
					return nil, fmt.Errorf("composition: unsupported config section %q", key)
				}
				if sections[key] {
					return nil, fmt.Errorf("composition: duplicate config section %q", key)
				}
				sections[key] = true
				section, sectionIndent = key, indent
				continue
			}
		} else {
			if section == "" || indent <= sectionIndent {
				return nil, fmt.Errorf("composition: invalid indentation on line %d", lineNumber)
			}
			key = section + "." + key
		}
		canonical, ok := canonicalConfigKey(key)
		if !ok {
			return nil, fmt.Errorf("composition: unsupported config key %q", key)
		}
		if _, exists := values[canonical]; exists {
			return nil, fmt.Errorf("composition: duplicate config key %q", canonical)
		}
		value, err := parseScalar(valueRaw, lineNumber)
		if err != nil {
			return nil, err
		}
		values[canonical] = value
		if section != "" {
			sectionValues[section] = true
		}
	}
	for name := range sections {
		if !sectionValues[name] {
			return nil, fmt.Errorf("composition: config section %q is empty", name)
		}
	}
	return values, nil
}

func validSection(key string) bool {
	return key == "storage" || key == "server" || key == "auth" || key == "ontology"
}

func canonicalConfigKey(key string) (string, bool) {
	switch key {
	case "driver", "storage.driver":
		return "driver", true
	case "dsn", "storage.dsn":
		return "dsn", true
	case "listen", "address", "server.listen", "server.address":
		return "address", true
	case "project", "server.project":
		return "project", true
	case "ui", "server.ui":
		return "ui", true
	case "auth.mode":
		return "auth.mode", true
	case "ontology.source":
		return "ontology.source", true
	default:
		return "", false
	}
}

func validConfigKey(key string) bool {
	if key == "" || strings.TrimSpace(key) != key {
		return false
	}
	for _, part := range strings.Split(key, ".") {
		if part == "" {
			return false
		}
		for _, r := range part {
			if !(r == '-' || r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
				return false
			}
		}
	}
	return true
}

func parseScalar(raw []byte, line int) (configValue, error) {
	if len(raw) == 0 || bytes.HasPrefix(raw, []byte("{")) || bytes.HasPrefix(raw, []byte("[")) || bytes.HasPrefix(raw, []byte("!")) || bytes.HasPrefix(raw, []byte("&")) || bytes.HasPrefix(raw, []byte("*")) {
		return configValue{}, fmt.Errorf("composition: malformed config value on line %d", line)
	}
	text := string(raw)
	quoted := false
	if strings.HasPrefix(text, "\"") {
		quoted = true
		if !strings.HasSuffix(text, "\"") {
			return configValue{}, fmt.Errorf("composition: malformed quoted value on line %d", line)
		}
		decoded, err := strconv.Unquote(text)
		if err != nil {
			return configValue{}, fmt.Errorf("composition: malformed quoted value on line %d", line)
		}
		text = decoded
	} else if strings.HasPrefix(text, "'") {
		quoted = true
		if len(text) < 2 || !strings.HasSuffix(text, "'") {
			return configValue{}, fmt.Errorf("composition: malformed quoted value on line %d", line)
		}
		text = strings.ReplaceAll(text[1:len(text)-1], "''", "'")
	} else if strings.ContainsAny(text, "{}[]") {
		return configValue{}, fmt.Errorf("composition: malformed config value on line %d", line)
	}
	if !quoted && (text == "true" || text == "false") {
		return configValue{text: text, kind: configBoolValue}, nil
	}
	if strings.TrimSpace(text) != text || strings.ContainsAny(text, "\r\n\x00") {
		return configValue{}, fmt.Errorf("composition: malformed config value on line %d", line)
	}
	return configValue{text: text, kind: configStringValue}, nil
}

func stripComment(line []byte) []byte {
	quoted := byte(0)
	escaped := false
	for i := 0; i < len(line); i++ {
		value := line[i]
		if quoted == '"' {
			if escaped {
				escaped = false
				continue
			}
			if value == '\\' {
				escaped = true
				continue
			}
			if value == quoted {
				quoted = 0
			}
			continue
		}
		if quoted == '\'' {
			if value == '\'' {
				if i+1 < len(line) && line[i+1] == '\'' {
					i++
					continue
				}
				quoted = 0
			}
			continue
		}
		if value == '\'' || value == '"' {
			quoted = value
			continue
		}
		if value == '#' {
			return line[:i]
		}
	}
	return line
}
