package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	localConfigPath = ".plug/config.yaml"
	defaultSQLiteDB = ".plug/plug.db"
)

// fileConfig is deliberately smaller than the server configuration. The CLI
// only consumes connection selection and the local SQLite file; accepting a
// second, partially understood configuration language would make a typo look
// like a successful local run.
type fileConfig struct {
	server     string
	serverSet  bool
	storage    StorageConfig
	storageSet bool
}

// resolveFileConfig loads the workspace-local client configuration. A
// missing file is the normal default and selects a SQLite file below .plug.
// Present files must be a supported, unambiguous configuration.
func resolveFileConfig() (fileConfig, error) {
	root, err := os.Getwd()
	if err != nil {
		return fileConfig{}, fmt.Errorf("determine workspace directory: %w", err)
	}
	path := filepath.Join(root, localConfigPath)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return fileConfig{storage: StorageConfig{Driver: "sqlite", DSN: filepath.Join(root, defaultSQLiteDB)}}, nil
	}
	if err != nil {
		return fileConfig{}, fmt.Errorf("read %s: %w", localConfigPath, err)
	}
	config, err := parseFileConfig(string(data))
	if err != nil {
		return fileConfig{}, fmt.Errorf("load %s: %w", localConfigPath, err)
	}
	if config.storageSet {
		config.storage.DSN, err = resolveStoragePath(root, config.storage.DSN)
		if err != nil {
			return fileConfig{}, fmt.Errorf("load %s: %w", localConfigPath, err)
		}
	}
	return config, nil
}

// SQLiteFactory opens the persistent local store used by the CLI. The driver
// owns SQLite validation and locking; this factory owns only the workspace
// directory lifecycle needed before the driver can open its file.
func SQLiteFactory(ctx context.Context, cfg StorageConfig) (driver.Store, func() error, error) {
	if cfg.Driver != "sqlite" {
		return nil, nil, fmt.Errorf("storage driver %q is not supported for local CLI storage", cfg.Driver)
	}
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, nil, errors.New("sqlite storage path must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	path, err := filepath.Abs(cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve SQLite path: %w", err)
	}
	if strings.ContainsRune(path, '\x00') {
		return nil, nil, errors.New("SQLite path contains NUL")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create SQLite parent directory: %w", err)
	}
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

// parseFileConfig parses the intentionally small YAML subset accepted by the
// CLI. It handles mappings and quoted/plain scalar values, while rejecting
// YAML features whose interpretation would be surprising for a local store.
func parseFileConfig(text string) (fileConfig, error) {
	var out fileConfig
	seen := map[string]bool{}
	storageSeen := map[string]bool{}
	storageMap := false
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	for lineNumber, raw := range lines {
		if strings.ContainsRune(raw, '\t') {
			return fileConfig{}, fmt.Errorf("line %d: tabs are not supported", lineNumber+1)
		}
		withoutComment, err := stripYAMLComment(raw)
		if err != nil {
			return fileConfig{}, fmt.Errorf("line %d: %w", lineNumber+1, err)
		}
		if strings.TrimSpace(withoutComment) == "" {
			continue
		}
		indent := len(withoutComment) - len(strings.TrimLeft(withoutComment, " "))
		if indent != 0 && indent != 2 {
			return fileConfig{}, fmt.Errorf("line %d: indentation must be zero or two spaces", lineNumber+1)
		}
		content := strings.TrimSpace(withoutComment)
		key, rawValue, ok := strings.Cut(content, ":")
		if !ok {
			return fileConfig{}, fmt.Errorf("line %d: expected key: value", lineNumber+1)
		}
		key = strings.TrimSpace(key)
		if !validConfigKey(key) {
			return fileConfig{}, fmt.Errorf("line %d: invalid configuration key %q", lineNumber+1, key)
		}
		value, err := parseConfigScalar(strings.TrimSpace(rawValue))
		if err != nil {
			return fileConfig{}, fmt.Errorf("line %d: %s: %w", lineNumber+1, key, err)
		}
		if indent == 0 {
			if seen[key] {
				return fileConfig{}, fmt.Errorf("line %d: duplicate configuration key %q", lineNumber+1, key)
			}
			seen[key] = true
			switch key {
			case "server":
				if value == "" {
					return fileConfig{}, fmt.Errorf("line %d: server must not be empty", lineNumber+1)
				}
				out.server, out.serverSet = value, true
			case "storage":
				if value == "" {
					if out.storageSet || seen["driver"] || seen["path"] {
						return fileConfig{}, fmt.Errorf("line %d: storage is ambiguous with top-level driver/path", lineNumber+1)
					}
					storageMap = true
					continue
				}
				if storageMap {
					return fileConfig{}, fmt.Errorf("line %d: storage cannot be both a scalar and a mapping", lineNumber+1)
				}
				out.storage = StorageConfig{Driver: value}
				out.storageSet = true
			case "driver", "path":
				if storageMap || seen["storage"] {
					return fileConfig{}, fmt.Errorf("line %d: %q is ambiguous with storage", lineNumber+1, key)
				}
				if key == "driver" {
					out.storage.Driver = value
				} else {
					out.storage.DSN = value
				}
				out.storageSet = true
			default:
				return fileConfig{}, fmt.Errorf("line %d: unsupported configuration key %q", lineNumber+1, key)
			}
			continue
		}

		if !storageMap || !seen["storage"] {
			return fileConfig{}, fmt.Errorf("line %d: nested configuration must be under storage", lineNumber+1)
		}
		if storageSeen[key] {
			return fileConfig{}, fmt.Errorf("line %d: duplicate storage key %q", lineNumber+1, key)
		}
		storageSeen[key] = true
		switch key {
		case "driver":
			out.storage.Driver = value
			out.storageSet = true
		case "path":
			out.storage.DSN = value
			out.storageSet = true
		default:
			return fileConfig{}, fmt.Errorf("line %d: unsupported storage key %q", lineNumber+1, key)
		}
	}
	if out.serverSet && out.storageSet {
		return fileConfig{}, errors.New("server and storage configuration are ambiguous")
	}
	if !out.serverSet && !out.storageSet {
		return fileConfig{}, errors.New("configuration must select server or SQLite storage")
	}
	if out.storageSet {
		if out.storage.Driver == "" {
			out.storage.Driver = "sqlite"
		}
		if out.storage.Driver != "sqlite" {
			return fileConfig{}, fmt.Errorf("storage driver %q is unsupported; local CLI supports sqlite only", out.storage.Driver)
		}
		if out.storage.DSN == "" {
			out.storage.DSN = defaultSQLiteDB
		}
		if strings.HasPrefix(out.storage.DSN, "file:") || strings.ContainsRune(out.storage.DSN, '\x00') {
			return fileConfig{}, errors.New("storage path must be a local filesystem path")
		}
	}
	return out, nil
}

func resolveStoragePath(root, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("storage path must not be empty")
	}
	if strings.HasPrefix(path, "file:") || strings.ContainsRune(path, '\x00') {
		return "", errors.New("storage path must be a local filesystem path")
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	return filepath.Join(root, filepath.Clean(path)), nil
}

func validConfigKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		if (i == 0 && !(unicode.IsLower(r) || r == '_')) || (i > 0 && !(unicode.IsLower(r) || unicode.IsDigit(r) || r == '_' || r == '-')) {
			return false
		}
	}
	return true
}

func parseConfigScalar(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if strings.HasPrefix(raw, "[") || strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "|") || strings.HasPrefix(raw, ">") || strings.HasPrefix(raw, "&") || strings.HasPrefix(raw, "*") || strings.HasPrefix(raw, "!") {
		return "", errors.New("lists, maps and YAML tags are unsupported")
	}
	if raw[0] == '\'' {
		if len(raw) < 2 || raw[len(raw)-1] != '\'' {
			return "", errors.New("unterminated single-quoted scalar")
		}
		value := raw[1 : len(raw)-1]
		if strings.ContainsRune(value, '\n') {
			return "", errors.New("multiline scalars are unsupported")
		}
		return strings.ReplaceAll(value, "''", "'"), nil
	}
	if raw[0] == '"' {
		value, err := strconv.Unquote(raw)
		if err != nil {
			return "", fmt.Errorf("invalid quoted scalar: %w", err)
		}
		return value, nil
	}
	if strings.ContainsAny(raw, "{}[]") || strings.ContainsRune(raw, '\n') {
		return "", errors.New("unsupported scalar syntax")
	}
	if raw == "null" || raw == "Null" || raw == "NULL" || raw == "~" {
		return "", errors.New("null values are unsupported")
	}
	return strings.TrimSpace(raw), nil
}

func stripYAMLComment(line string) (string, error) {
	var quote rune
	var previous rune
	for i, r := range line {
		switch {
		case quote == 0 && (r == '\'' || r == '"'):
			quote = r
		case quote != 0 && r == quote:
			quote = 0
		case quote == 0 && r == '#' && (i == 0 || unicode.IsSpace(previous)):
			return line[:i], nil
		}
		previous = r
	}
	if quote != 0 {
		return "", errors.New("unterminated quoted scalar")
	}
	return line, nil
}
