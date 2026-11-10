package dumpformat

import (
	"fmt"
	"path"
	"strings"
)

type externalEntry struct {
	Identity *SourceIdentity `json:"identity,omitempty"`
	Input    struct {
		Family     string `json:"family"`
		Path       string `json:"path"`
		State      string `json:"state"`
		Precedence int    `json:"precedence"`
	} `json:"input"`
	Values map[string]any `json:"values,omitempty"`
}

func validateConfigProvenance(data []byte) error {
	var entries []externalEntry
	if err := decodeJSON(data, &entries); err != nil {
		return fmt.Errorf("invalid configuration provenance: %w", err)
	}
	allowed, seen, families := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, family := range externalFamilies {
		allowed[family] = true
	}
	var previous *externalEntry
	for i := range entries {
		entry := &entries[i]
		input := entry.Input
		key := externalEntryKey(*entry)
		if !allowed[input.Family] || !path.IsAbs(input.Path) || strings.ContainsRune(input.Path, '\x00') || seen[key] || input.Precedence < 0 {
			return fmt.Errorf("unknown, duplicate or inconsistent configuration input")
		}
		if previous != nil {
			if compareExternalEntries(*previous, *entry) >= 0 {
				return fmt.Errorf("unordered configuration inputs")
			}
		}
		switch input.State {
		case "absent":
			if entry.Values != nil {
				return fmt.Errorf("absent configuration has values")
			}
		case "json-config":
			if entry.Values == nil {
				return fmt.Errorf("missing configuration values")
			}
		default:
			return fmt.Errorf("unsupported configuration state")
		}
		if err := validateSecretReferences(entry.Values); err != nil {
			return err
		}
		seen[key], families[input.Family] = true, true
		previous = entry
	}
	if len(families) != len(allowed) {
		return fmt.Errorf("unaccounted configuration provenance family")
	}
	return nil
}

func validateDispositions(sources []Disposition, counts map[string]int, config []byte) error {
	return validateMigratedDispositions(sources, counts, config, nil)
}

func migrationAction(table string, migration *ImportMigration) string {
	if migration != nil && (table == "custom_statuses" || table == "custom_types") {
		return "carry"
	}
	return actionFor(table)
}

func validateMigratedDispositions(sources []Disposition, counts map[string]int, config []byte, migration *ImportMigration) error {
	return validateBoundDispositions(sources, counts, config, migration, false)
}

func validateBoundDispositions(sources []Disposition, counts map[string]int, config []byte, migration *ImportMigration, planes bool, familyContracts ...string) error {
	return validateBoundDispositionsForVersion(sources, counts, config, migration, planes, 72, familyContracts...)
}

func validateBoundDispositionsForVersion(sources []Disposition, counts map[string]int, config []byte, migration *ImportMigration, planes bool, mainVersion int, familyContracts ...string) error {
	columns := sourceTableColumns(mainVersion)
	native := len(familyContracts) != 0 && familyContracts[0] == SourceFamilyContract
	var entries []externalEntry
	if err := decodeJSON(config, &entries); err != nil {
		return err
	}
	// Multiple paths in a source family are legal. Match the complete multiset,
	// including zero-row and absent entries, rather than silently coalescing it.
	expected := map[Disposition]int{}
	for _, entry := range entries {
		d := Disposition{Family: entry.Input.Family}
		if entry.Input.State == "absent" {
			d.Action, d.Reason = "manifest/rebuild", "requested source absent at declared path"
		} else {
			d.Rows, d.Action, d.Reason = len(entry.Values), "archive", "explicit JSON config; secrets are references; precedence retained, effective config not inferred"
		}
		expected[d]++
	}
	seen := map[string]bool{}
	lastFamily, lastReason := "", ""
	for _, source := range sources {
		if source.Identity != nil || !native && source.History != "" {
			return fmt.Errorf("source disposition requires explicit scoped native contract")
		}
		if source.Rows < 0 || source.Reason == "" || source.Family < lastFamily || source.Family == lastFamily && source.Reason < lastReason {
			return fmt.Errorf("invalid or unordered source disposition")
		}
		lastFamily, lastReason = source.Family, source.Reason
		if _, table := columns[source.Family]; table {
			action := migrationAction(source.Family, migration)
			if planes {
				if source.Family == EphemeralDependencyTable {
					action = "archive"
				} else if sourcePlaneArchiveTable(source.Family) {
					action = "archive"
				}
			}
			if native && (source.Family == "leases" || source.Family == "repo_mtimes") {
				action = "archive"
			}
			if native && source.History != FamilyVersioned && source.History != FamilyUnversioned {
				return fmt.Errorf("missing native family history designation")
			}
			if seen[source.Family] || source.Action != action || (source.Family != "repo_mtimes" || native) && source.Rows != counts[source.Family] || (source.Action == "refuse-if-nonempty" || source.Family == "leases" && !native) && source.Rows != 0 {
				return fmt.Errorf("source disposition/action/count mismatch")
			}
			seen[source.Family] = true
		} else if source.Family == "issues.row_lock" || source.Family == "issues.is_blocked" {
			if seen[source.Family] || source.Action != "manifest/rebuild" || source.Rows != counts["issues"] {
				return fmt.Errorf("invalid excluded-field disposition")
			}
			seen[source.Family] = true
		} else {
			if expected[source] == 0 {
				return fmt.Errorf("unaccounted or duplicate external disposition")
			}
			expected[source]--
		}
	}
	if len(seen) != len(columns)+2 {
		return fmt.Errorf("unaccounted database disposition")
	}
	for _, count := range expected {
		if count != 0 {
			return fmt.Errorf("missing external disposition")
		}
	}
	return nil
}

// This predicate matches the producer's configuration classification at
// 10953a8536c3a90b7cf54896d2ed5513b48a8e99. It examines keys, never values.
func secretKey(key string) bool {
	lower := strings.ToLower(key)
	for _, part := range []string{"api_key", "api-key", "apikey", "secret", "token", "password", "passwd", "credential", "private_key", "privatekey", "privkey", "dsn", "access_key"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	for _, part := range strings.FieldsFunc(lower, func(r rune) bool { return r == '.' || r == '_' || r == '-' }) {
		switch part {
		case "key", "keys", "apikey", "pwd", "pat", "auth", "bearer", "cert", "credential", "credentials", "secret", "token", "password":
			return true
		}
	}
	return false
}

func validateSecretReferences(value any) error {
	switch value := value.(type) {
	case map[string]any:
		if ref, ok := value["$secret"]; ok {
			name, ok := ref.(string)
			if !ok || name == "" || len(value) != 1 {
				return fmt.Errorf("invalid secret reference")
			}
			return nil
		}
		for key, item := range value {
			if secretKey(key) {
				ref, ok := item.(map[string]any)
				if !ok || len(ref) != 1 || ref["$secret"] == nil {
					return fmt.Errorf("credential values/digests require secret references")
				}
			}
			if err := validateSecretReferences(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := validateSecretReferences(item); err != nil {
				return err
			}
		}
	}
	return nil
}
