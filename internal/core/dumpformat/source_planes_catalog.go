package dumpformat

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// This validates raw catalog conservation only. No catalog category chooses
// a workflow status, target kind, principal or lifetime for an active entity.
func validateSourcePlaneCatalog(rows map[string][]Record, config []byte) (map[string]bool, error) {
	categories, err := sourcePlaneCatalogCategories(rows, config)
	if err != nil {
		return nil, err
	}
	statuses := make(map[string]bool, len(categories))
	for name := range categories {
		statuses[name] = true
	}
	return statuses, nil
}

func sourcePlaneCatalogCategories(rows map[string][]Record, config []byte) (map[string]string, error) {
	return sourcePlaneCatalogCategoriesFor(rows, config, false)
}

func sourcePlaneCatalogCategoriesFor(rows map[string][]Record, config []byte, native bool) (map[string]string, error) {
	var declarations []ConfigDeclaration
	seenStatus, seenType := map[string]bool{}, map[string]bool{}
	stored := map[string]any{}
	for _, kind := range sortedRecordKinds(rows) {
		for _, r := range rows[kind] {
			fields := r.Source.Fields
			switch r.Source.Table {
			case "custom_statuses", "custom_types":
				name, ok := fields["name"].(string)
				if !ok {
					return nil, fmt.Errorf("catalog name must be text")
				}
				decl := ConfigDeclaration{Kind: "type", Name: name, Origin: r.Source.Table + "/" + r.ID}
				seen, key := seenType, name
				if r.Source.Table == "custom_statuses" {
					var ok bool
					decl.Kind, seen, key = "status", seenStatus, strings.ToLower(name)
					decl.Category, ok = fields["category"].(string)
					if !ok {
						return nil, fmt.Errorf("catalog status category must be text")
					}
				}
				if seen[key] {
					return nil, fmt.Errorf("duplicate physical catalog row")
				}
				seen[key] = true
				declarations = append(declarations, decl)
			case "config":
				key := stringValue(fields["key"])
				if key == "status.custom" || key == "types.custom" {
					if _, exists := stored[key]; exists {
						return nil, fmt.Errorf("duplicate stored catalog declaration")
					}
					stored[key] = fields["value"]
				}
			}
		}
	}
	decls, err := sourcePlaneCatalogValues(stored, "stored-config")
	if err != nil {
		return nil, err
	}
	declarations = append(declarations, decls...)
	if err := streamConfigEntries(config, func(entry externalEntry, _ []byte) error {
		if entry.Input.State == "absent" {
			return nil
		}
		origin := entry.Input.Family + ":" + entry.Input.Path
		// Match the producer's native YAML projection: direct external JSON
		// and opaque engine census directories remain retained source bytes.
		// Stored declarations are read from their actual SQL config records.
		if !sourcePlaneFileFamily(entry.Input.Family) {
			return nil
		}
		if _, captured := entry.Values["observations"]; !captured {
			return nil
		}
		decls, err = declarationsFromSourceWith(entry.Values, entry.Input.Family, origin, sourcePlaneCatalogValues, true)
		if err != nil {
			return err
		}
		declarations = append(declarations, decls...)
		return nil
	}); err != nil {
		return nil, err
	}
	statuses := map[string]bool{}
	categories := map[string]string{}
	for _, decl := range declarations {
		if decl.Kind == "type" {
			if decl.Name == "" || decl.Name != strings.TrimSpace(decl.Name) || !utf8.ValidString(decl.Name) || utf8.RuneCountInString(decl.Name) > 64 {
				return nil, fmt.Errorf("invalid raw catalog type")
			}
			continue
		}
		builtin := builtInSourceStatus(decl.Name)
		if native {
			builtin = false
			for _, status := range nativeBuiltInStatuses() {
				builtin = builtin || decl.Name == status.Name
			}
		}
		if !declarationName.MatchString(decl.Name) || len(decl.Name) > 64 || builtin {
			return nil, fmt.Errorf("invalid raw catalog status")
		}
		switch decl.Category {
		case "active", "wip", "done", "frozen":
		default:
			return nil, fmt.Errorf("raw catalog status requires explicit category")
		}
		if old, ok := categories[decl.Name]; ok && old != decl.Category {
			return nil, fmt.Errorf("conflicting raw catalog status categories")
		}
		categories[decl.Name], statuses[decl.Name] = decl.Category, true
	}
	if len(statuses) > 50 {
		return nil, fmt.Errorf("too many raw catalog statuses")
	}
	return categories, nil
}

func sourcePlaneCatalogValues(values map[string]any, origin string) ([]ConfigDeclaration, error) {
	var result []ConfigDeclaration
	for _, part := range []struct{ section, kind string }{{"status", "status"}, {"types", "type"}} {
		key := part.section + ".custom"
		var inputs []any
		if value, present := values[key]; present {
			inputs = append(inputs, value)
		}
		if nested, present := values[part.section]; present {
			object, ok := nested.(map[string]any)
			if !ok || object == nil {
				return nil, fmt.Errorf("raw catalog section must be an object")
			}
			if value, present := object["custom"]; present {
				inputs = append(inputs, value)
			}
		}
		for _, input := range inputs {
			names, err := declarationList(input, part.kind, key, origin)
			if err != nil || len(names) == 0 {
				return nil, fmt.Errorf("invalid empty raw catalog declaration: %s", key)
			}
			seen := map[string]bool{}
			for _, value := range names {
				decl := ConfigDeclaration{Kind: part.kind, Name: strings.TrimSpace(value), Origin: origin + "#" + key}
				identity := decl.Name
				if part.kind == "status" {
					name, category, present := strings.Cut(decl.Name, ":")
					if !present || name == "" || category == "" {
						return nil, fmt.Errorf("raw status declaration needs name and category")
					}
					decl.Name, decl.Category, identity = name, category, strings.ToLower(name)
				}
				if seen[identity] {
					return nil, fmt.Errorf("duplicate raw catalog declaration")
				}
				seen[identity] = true
				result = append(result, decl)
			}
		}
	}
	return result, nil
}
