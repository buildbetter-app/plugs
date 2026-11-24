package dumpformat

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

// StrictUnionOntologyVerification selects the explicit, strict beads-compat
// import migration. It describes import semantics, not the companion runtime.
const StrictUnionOntologyVerification = "beads-compat strict configuration union/1; explicit map_status/map_type directives recorded in manifest"

// MigrationRefusal identifies the declaration or directive needing operator
// correction. No partially compiled migration is returned with a refusal.
type MigrationRefusal struct {
	Code    string
	Entry   string
	Origins []string
}

func (r *MigrationRefusal) Error() string {
	return fmt.Sprintf("ontology migration %s: %s [%s]", r.Code, r.Entry, strings.Join(r.Origins, ", "))
}

func migrationRefusal(code, entry string, origins ...string) error {
	origins = append([]string{}, origins...)
	sort.Strings(origins)
	return &MigrationRefusal{Code: code, Entry: entry, Origins: origins}
}

// ConfigDeclaration retains every independently declared entry, including
// compatible duplicates and entries resolved by an explicit mapping.
type ConfigDeclaration struct {
	Kind     string
	Name     string
	Category string
	Origin   string
}

type ImportTarget struct {
	Type   string
	Status string
}

// ImportMigration owns a compiled ontology and the exact admitted head image.
// Its methods expose detached descriptions, never mutable compilation inputs.
type ImportMigration struct {
	snapshot     ontology.Snapshot
	declarations []ConfigDeclaration
	targets      map[string]ImportTarget
}

func (m *ImportMigration) Document() ontology.Document {
	return m.snapshot.Effective("").Document()
}

func (m *ImportMigration) Declarations() []ConfigDeclaration {
	return append([]ConfigDeclaration{}, m.declarations...)
}

func (m *ImportMigration) Target(project, id string) (ImportTarget, bool) {
	target, ok := m.targets[project+"\x00"+id]
	return target, ok
}

var declarationName = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// collectConfigDeclarations reads the raw layers separately. It deliberately
// never consults precedence or the companion's effective DB-first view.
func collectConfigDeclarations(records map[string][]Record, config []byte) ([]ConfigDeclaration, error) {
	var result []ConfigDeclaration
	for _, kind := range sortedRecordKinds(records) {
		for _, r := range records[kind] {
			origin := r.Source.Table + "/" + r.ID
			switch r.Source.Table {
			case "custom_statuses", "custom_types":
				decl := ConfigDeclaration{Kind: "type", Name: stringValue(r.Source.Fields["name"]), Origin: origin}
				if r.Source.Table == "custom_statuses" {
					decl.Kind, decl.Category = "status", stringValue(r.Source.Fields["category"])
					if _, ok := r.Source.Fields["category"].(string); !ok {
						return nil, migrationRefusal("malformed-declaration", decl.Name, origin)
					}
				}
				result = append(result, decl)
			case "config":
				key := stringValue(r.Source.Fields["key"])
				if key == "status.custom" || key == "types.custom" {
					decls, err := declarationsFromValues(map[string]any{key: r.Source.Fields["value"]}, origin)
					if err != nil {
						return nil, err
					}
					result = append(result, decls...)
				}
			}
		}
	}
	if err := streamConfigEntries(config, func(entry externalEntry, _ []byte) error {
		if entry.Input.State == "absent" {
			return nil
		}
		origin := entry.Input.Family + ":" + entry.Input.Path
		decls, err := declarationsFromSource(entry.Values, entry.Input.Family, origin)
		if err != nil {
			return err
		}
		result = append(result, decls...)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		return a.Kind+"\x00"+a.Name+"\x00"+a.Origin < b.Kind+"\x00"+b.Name+"\x00"+b.Origin
	})
	seen := map[string]string{}
	statusNames := map[string]bool{}
	for _, decl := range result {
		if !declarationName.MatchString(decl.Name) || len(decl.Name) > 128 ||
			decl.Kind == "status" && builtInSourceStatus(decl.Name) || decl.Kind == "type" && builtInIssueType(decl.Name) {
			return nil, migrationRefusal("malformed-declaration", decl.Name, decl.Origin)
		}
		if decl.Kind == "status" {
			switch decl.Category {
			case "active", "wip", "done", "frozen", "", "unspecified":
			default:
				return nil, migrationRefusal("malformed-declaration", decl.Name, decl.Origin)
			}
			statusNames[decl.Name] = true
		}
		layer := decl.Origin
		if strings.HasPrefix(layer, "custom_statuses/") {
			layer = "custom_statuses"
		} else if strings.HasPrefix(layer, "custom_types/") {
			layer = "custom_types"
		}
		key := decl.Kind + "\x00" + decl.Name + "\x00" + layer
		if prior, duplicate := seen[key]; duplicate {
			return nil, migrationRefusal("duplicate-declaration", decl.Name, prior, decl.Origin)
		}
		seen[key] = decl.Origin
	}
	if len(statusNames) > 50 {
		return nil, migrationRefusal("malformed-declaration", "status.custom", "more than 50 status names")
	}
	return result, nil
}

func declarationsFromValues(values map[string]any, origin string) ([]ConfigDeclaration, error) {
	var result []ConfigDeclaration
	for _, field := range []struct{ section, kind string }{{"status", "status"}, {"types", "type"}} {
		key := field.section + ".custom"
		value, present := values[key]
		if nested, ok := values[field.section]; ok {
			object, ok := nested.(map[string]any)
			if !ok {
				return nil, migrationRefusal("malformed-declaration", key, origin)
			}
			if custom, exists := object["custom"]; exists {
				if present {
					return nil, migrationRefusal("malformed-declaration", key, origin, "both dotted and nested declarations")
				}
				value, present = custom, true
			}
		}
		if !present {
			continue
		}
		names, err := declarationList(value, field.kind, key, origin)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			decl := ConfigDeclaration{Kind: field.kind, Name: strings.TrimSpace(name), Origin: origin + "#" + key}
			if field.kind == "status" {
				if before, after, found := strings.Cut(decl.Name, ":"); found {
					decl.Name, decl.Category = before, after
					if after == "" {
						return nil, migrationRefusal("malformed-declaration", name, decl.Origin)
					}
				}
			}
			result = append(result, decl)
		}
	}
	return result, nil
}

func declarationList(value any, kind, entry, origin string) ([]string, error) {
	var values []any
	switch v := value.(type) {
	case string:
		v = strings.TrimSpace(v)
		if v == "" {
			return nil, nil
		}
		if kind == "type" && strings.HasPrefix(v, "[") {
			if err := decodeJSON([]byte(v), &values); err != nil {
				return nil, migrationRefusal("malformed-declaration", entry, origin)
			}
		} else {
			for _, part := range strings.Split(v, ",") {
				values = append(values, strings.TrimSpace(part))
			}
		}
	case []any:
		values = v
	default:
		return nil, migrationRefusal("malformed-declaration", entry, origin)
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		name, ok := value.(string)
		if !ok || strings.TrimSpace(name) == "" {
			return nil, migrationRefusal("malformed-declaration", entry, origin)
		}
		result = append(result, name)
	}
	return result, nil
}

func sortedRecordKinds(records map[string][]Record) []string {
	var kinds []string
	for kind := range records {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func validateMigrationDirectives(directives []OntologyDirective, projects []Project) error {
	ids := map[string]bool{}
	for i, d := range directives {
		if strings.TrimSpace(d.ID) == "" || ids[d.ID] {
			return migrationRefusal("duplicate-directive", d.ID)
		}
		ids[d.ID] = true
		if !projectKnown(d.Target.Project, projects) || d.Target.ID != "" || !declarationName.MatchString(d.From) {
			return migrationRefusal("malformed-declaration", d.ID, d.Target.Project, d.From)
		}
		switch d.Kind {
		case "map_status":
			if d.Target.Type != "issue" || builtInSourceStatus(d.From) {
				return migrationRefusal("malformed-declaration", d.ID, d.From)
			}
			if _, ok := ontology.DefaultDocument().Statuses[d.To]; !ok {
				return migrationRefusal("unknown-target", d.ID, d.To)
			}
		case "map_type":
			if d.Target.Type != d.From || builtInIssueType(d.From) {
				return migrationRefusal("malformed-declaration", d.ID, d.From)
			}
			if d.To != "issue" {
				return migrationRefusal("unknown-target", d.ID, d.To)
			}
		default:
			return migrationRefusal("malformed-declaration", d.ID, d.Kind)
		}
		for _, prior := range directives[:i] {
			if prior.Kind == d.Kind && prior.Target.Project == d.Target.Project && prior.From == d.From {
				return migrationRefusal("overlapping-directive", d.From, prior.ID, d.ID)
			}
		}
	}
	return nil
}

func compileImportMigration(manifest Manifest, records map[string][]Record, config []byte, hasHistory bool) (*ImportMigration, error) {
	decls, err := collectConfigDeclarations(records, config)
	if err != nil {
		return nil, err
	}
	strict := manifest.Verification["ontology"] == StrictUnionOntologyVerification
	if !strict {
		if len(decls) != 0 {
			return nil, migrationRefusal("migration-required", decls[0].Name, decls[0].Origin)
		}
		return nil, nil
	}
	if len(manifest.Projects) != 1 {
		return nil, migrationRefusal("project-scope", "configuration union", "one explicit source project required")
	}
	if hasHistory {
		return nil, migrationRefusal("history-scope", SourceHistoryMember, "strict ontology source-history integration is not admitted")
	}
	if err := validateMigrationDirectives(manifest.Ontology, manifest.Projects); err != nil {
		return nil, err
	}
	byName := map[string][]ConfigDeclaration{}
	for _, decl := range decls {
		key := decl.Kind + "/" + decl.Name
		byName[key] = append(byName[key], decl)
	}
	var keys []string
	for key := range byName {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	doc := ontology.DefaultDocument()
	statusTemplates := map[string]string{}
	typeNames := map[string]bool{}
	used := map[string]bool{}
	for _, key := range keys {
		entries := byName[key]
		first := entries[0]
		origins := make([]string, len(entries))
		for i, entry := range entries {
			origins[i] = entry.Origin + "=" + entry.Category
		}
		var directive *OntologyDirective
		for i := range manifest.Ontology {
			d := &manifest.Ontology[i]
			if d.Kind == "map_"+first.Kind && d.From == first.Name {
				directive = d
				used[d.ID] = true
			}
		}
		if first.Kind == "type" {
			if directive == nil {
				return nil, migrationRefusal("mapping-required", first.Name, origins...)
			}
			typ := doc.Types["issue"]
			typ.Name = first.Name
			doc.Types[first.Name] = typ
			typeNames[first.Name] = true
			continue
		}
		template := ""
		if directive != nil {
			template = directive.To
		} else {
			category := first.Category
			for _, entry := range entries {
				if entry.Category == "" || entry.Category == "unspecified" {
					return nil, migrationRefusal("category-required", first.Name, origins...)
				}
				if entry.Category != category {
					return nil, migrationRefusal("category-conflict", first.Name, origins...)
				}
			}
			// P22's beads-compat preset, with strict refusal for unspecified.
			template = map[string]string{"active": "todo", "wip": "in_progress", "done": "done", "frozen": "backlog"}[category]
		}
		status, exists := doc.Statuses[template]
		if !exists {
			return nil, migrationRefusal("unknown-target", first.Name, template)
		}
		if prior, exists := doc.Statuses[first.Name]; exists && (prior.Category != status.Category || prior.Outcome != status.Outcome) {
			return nil, migrationRefusal("ontology-conflict", first.Name, origins...)
		}
		status.Name = first.Name
		doc.Statuses[first.Name] = status
		statusTemplates[first.Name] = template
	}
	for _, d := range manifest.Ontology {
		if !used[d.ID] {
			return nil, migrationRefusal("unused-directive", d.ID, d.From)
		}
	}
	doc.Workflows["issue"] = unionWorkflow(doc.Workflows["issue"], statusTemplates)
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		return nil, migrationRefusal("ontology-conflict", problems[0].Path, problems[0].Message)
	}
	migration := &ImportMigration{snapshot: snapshot, declarations: decls, targets: map[string]ImportTarget{}}
	for _, r := range records["bead"] {
		statusName, typeName := stringValue(r.Source.Fields["status"]), stringValue(r.Source.Fields["issue_type"])
		status, _ := sourceStatusTarget(statusName)
		if _, declared := statusTemplates[statusName]; declared {
			status = statusName
		} else if !builtInSourceStatus(statusName) {
			return nil, migrationRefusal("undeclared-entry", statusName, r.Project+"/"+r.ID)
		}
		typ := "issue"
		if !builtInIssueType(typeName) {
			if !typeNames[typeName] {
				return nil, migrationRefusal("undeclared-entry", typeName, r.Project+"/"+r.ID)
			}
			typ = typeName
		}
		migration.targets[recordKey(r)] = ImportTarget{Type: typ, Status: status}
	}
	return migration, nil
}

// Aliases inherit the selected template's transitions and guards. Slots keep
// their built-in targets, and explicit From sets include corresponding aliases.
func unionWorkflow(workflow ontology.Workflow, templates map[string]string) ontology.Workflow {
	var aliases []string
	for alias := range templates {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	builtins := map[string]bool{}
	for _, key := range workflow.Statuses {
		builtins[key] = true
	}
	var rules []ontology.Rule
	for _, rule := range workflow.Transitions {
		expanded := rule
		expanded.From = append([]string(nil), rule.From...)
		for _, alias := range aliases {
			for _, from := range rule.From {
				if !builtins[alias] && templates[alias] == from {
					expanded.From = append(expanded.From, alias)
				}
			}
		}
		rules = append(rules, expanded)
		for _, alias := range aliases {
			if !builtins[alias] && templates[alias] == rule.To {
				incoming := expanded
				incoming.To = alias
				rules = append(rules, incoming)
			}
		}
	}
	for _, alias := range aliases {
		if !builtins[alias] {
			workflow.Statuses = append(workflow.Statuses, alias)
		}
	}
	workflow.Transitions = rules
	return workflow
}
