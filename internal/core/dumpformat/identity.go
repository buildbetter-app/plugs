package dumpformat

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// A native hash namespace is an observed ID segment, not another project
// prefix. It is declared by the source allocator inventory and therefore must
// never be inferred from an otherwise unknown ID.
var hashNamespacePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*(?:-[a-zA-Z0-9_]+)*$`)

func validHashNamespace(namespace string) bool {
	return len(namespace) <= 32 && hashNamespacePattern.MatchString(namespace)
}

// UnmarshalJSON keeps omission backward compatible while refusing an empty,
// null, aliased or unknown namespace member. Older readers use strict JSON
// decoding and therefore refuse this extension rather than ignoring it.
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
		if key != "statuses" && strings.EqualFold(key, "statuses") {
			return fmt.Errorf("noncanonical statuses key")
		}
	}
	if _, present := fields["statuses"]; present && len(value.Statuses) == 0 {
		return fmt.Errorf("statuses requires a nonempty array")
	}
	if _, present := fields["hash_namespaces"]; present && len(value.HashNamespaces) == 0 {
		return fmt.Errorf("hash_namespaces requires a nonempty array")
	}
	*p = Project(value)
	return nil
}

// splitHashRoot recognizes a bare hash or the namespace-hash root shape. It
// does not authorize a namespace; callers must bind the returned namespace to
// the project's exact declared inventory.
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

// ValidSourceID checks the source allocator grammar while retaining the exact
// source string. Child counter bounds are checked by the archive validator's
// checkID; this helper is also used by the import planner after that archive
// validation has already admitted the source rows.
func ValidSourceID(id string, project Project) bool {
	prefix := project.Prefix + "-"
	if !strings.HasPrefix(id, prefix) || strings.ContainsRune(id, '\x00') {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(id, prefix), ".")
	if len(parts) == 0 || parts[0] == "" {
		return false
	}
	if project.IDMode == "sequential" {
		n, err := natural(parts[0])
		if err != nil || n == 0 {
			return false
		}
	} else if project.IDMode == "hash" {
		namespace, hash, ok := splitHashRoot(parts[0])
		declared := namespace == ""
		for _, candidate := range project.HashNamespaces {
			if namespace == candidate {
				declared = true
				break
			}
		}
		if !ok || !declared || len(hash) < project.HashLength {
			return false
		}
	} else {
		return false
	}
	for _, child := range parts[1:] {
		n, err := natural(child)
		if err != nil || n == 0 {
			return false
		}
	}
	return true
}
