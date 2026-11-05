package plugdump

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// A namespace is the additional native IDPrefix used before a generated hash.
// It is distinct from the project prefix and never selects a new project or
// changes the allocator used for future IDs. Empty segments are not admitted.
var hashNamespacePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*(?:-[a-zA-Z0-9_]+)*$`)

func validHashNamespace(namespace string) bool {
	return len(namespace) <= 32 && hashNamespacePattern.MatchString(namespace)
}

// UnmarshalJSON keeps omission backward compatible, while an explicitly
// present extension must use its canonical name and a nonempty array.
func (p *Project) UnmarshalJSON(data []byte) error {
	type plain Project
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key := range fields {
		if key != "hash_namespaces" && strings.EqualFold(key, "hash_namespaces") {
			return fmt.Errorf("noncanonical hash_namespaces key")
		}
	}
	if _, present := fields["hash_namespaces"]; present && len(value.HashNamespaces) == 0 {
		return fmt.Errorf("hash_namespaces requires a nonempty array")
	}
	*p = Project(value)
	return nil
}

// splitHashRoot recognizes the native prefix/namespace/hash shape without
// authorizing a namespace. Callers must separately bind it to the inventory.
func splitHashRoot(root string) (namespace, hash string, ok bool) {
	if hashPattern.MatchString(root) {
		return "", root, true
	}
	separator := strings.LastIndexByte(root, '-')
	if separator < 1 {
		return "", "", false
	}
	namespace, hash = root[:separator], root[separator+1:]
	return namespace, hash, validHashNamespace(namespace) && hashPattern.MatchString(hash)
}

// issueIDParts validates the original ID and returns only its observed counter
// contributions. Hash length applies to the hash token, never namespace bytes.
func issueIDParts(id string, project Project) (top uint64, parent string, child uint64, err error) {
	if !strings.HasPrefix(id, project.Prefix+"-") {
		return 0, "", 0, fmt.Errorf("source ID/project prefix mismatch")
	}
	parts := strings.Split(strings.TrimPrefix(id, project.Prefix+"-"), ".")
	if project.IDMode == "sequential" {
		top, err = natural(parts[0])
		if err != nil || top == 0 {
			return 0, "", 0, fmt.Errorf("issue ID does not match sequential allocator")
		}
	} else {
		namespace, hash, ok := splitHashRoot(parts[0])
		declared := namespace == ""
		for _, candidate := range project.HashNamespaces {
			declared = declared || namespace == candidate
		}
		if !ok || !declared || len(hash) < project.HashLength {
			return 0, "", 0, fmt.Errorf("issue ID does not match hash allocator")
		}
	}
	for _, part := range parts[1:] {
		child, err = natural(part)
		if err != nil || child == 0 {
			return 0, "", 0, fmt.Errorf("invalid child ID")
		}
	}
	if len(parts) > 1 {
		parent = id[:strings.LastIndexByte(id, '.')]
	}
	return top, parent, child, nil
}
