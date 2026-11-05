package dumpformat

import (
	"encoding/base64"
	"path"
	"strings"
)

// Native capture retains both parsed source values and the exact byte envelope.
// Binding parsed YAML to those bytes is the producer's capture responsibility;
// this reader checks the envelope and consumes its declared parsed values.
func declarationsFromSource(values map[string]any, family, origin string) ([]ConfigDeclaration, error) {
	return declarationsFromSourceWith(values, family, origin, declarationsFromValues, false)
}

func declarationsFromSourceWith(values map[string]any, family, origin string, project func(map[string]any, string) ([]ConfigDeclaration, error), yamlOnly bool) ([]ConfigDeclaration, error) {
	result, err := project(values, origin)
	if err != nil {
		return nil, err
	}
	observations, wrapped := values["observations"]
	if !wrapped {
		return result, nil
	}
	if len(result) != 0 {
		return nil, migrationRefusal("malformed-declaration", "observations", origin, "mixed direct and captured declarations")
	}
	rows, ok := observations.([]any)
	if !ok {
		return nil, migrationRefusal("malformed-declaration", "observations", origin)
	}
	seen := map[string]bool{}
	var visit func(any, int) error
	visit = func(value any, depth int) error {
		row, ok := value.(map[string]any)
		if !ok || depth > 8 {
			return migrationRefusal("malformed-declaration", "observation", origin)
		}
		name := stringValue(row["path"])
		if !path.IsAbs(name) || strings.ContainsRune(name, '\x00') || seen[name] {
			return migrationRefusal("malformed-declaration", name, origin)
		}
		seen[name] = true
		entry := origin + "@" + name
		switch stringValue(row["state"]) {
		case "absent":
			if _, ok := row["values"]; ok {
				return migrationRefusal("malformed-declaration", name, entry)
			}
		case "directory":
			// Native store locations are retained as opaque provenance, never
			// enumerated configuration. The same marker in a config family
			// cannot replace a captured directory or its parsed file entries.
			if contents, marked := row["contents"]; marked {
				if family != "store-files-and-quiescence" || contents != "not-read" {
					return migrationRefusal("malformed-declaration", name, entry, "opaque marker is not captured configuration")
				}
				for _, field := range []string{"entries", "values", "bytes", "sha256", "size"} {
					if _, present := row[field]; present {
						return migrationRefusal("malformed-declaration", name, entry, "opaque store directory has captured contents")
					}
				}
				return nil
			}
			for _, field := range []string{"values", "bytes", "sha256", "size"} {
				if _, present := row[field]; present {
					return migrationRefusal("malformed-declaration", name, entry, "directory has file capture fields")
				}
			}
			children, ok := row["entries"].([]any)
			if !ok {
				return migrationRefusal("malformed-declaration", name, entry)
			}
			for _, child := range children {
				if err := visit(child, depth+1); err != nil {
					return err
				}
			}
		case "file":
			raw, ok := row["bytes"].(map[string]any)
			if !ok || len(raw) != 1 {
				return migrationRefusal("malformed-declaration", name, entry)
			}
			encoded, ok := raw["$bytes"].(string)
			data, err := base64.StdEncoding.Strict().DecodeString(encoded)
			size, sizeErr := natural(row["size"])
			if !ok || err != nil || sizeErr != nil || size != uint64(len(data)) || digest(data) != stringValue(row["sha256"]) {
				return migrationRefusal("malformed-declaration", name, entry)
			}
			parsed, present := row["values"]
			ext := strings.ToLower(path.Ext(name))
			if yamlOnly && ext != ".yaml" && ext != ".yml" {
				return nil
			}
			if !present && ext != ".yaml" && ext != ".yml" && ext != ".json" {
				return nil
			}
			object, ok := parsed.(map[string]any)
			if !ok {
				return migrationRefusal("malformed-declaration", name, entry, "missing parsed configuration")
			}
			decls, err := project(object, entry)
			if err != nil {
				return err
			}
			result = append(result, decls...)
		default:
			return migrationRefusal("malformed-declaration", name, entry)
		}
		return nil
	}
	for _, row := range rows {
		if err := visit(row, 0); err != nil {
			return nil, err
		}
	}
	return result, nil
}
