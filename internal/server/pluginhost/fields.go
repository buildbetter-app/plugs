package pluginhost

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/pluginapi"
)

func lookup(body map[string]any, path string) (any, bool) {
	if strings.HasPrefix(path, "meta:") {
		metadata, ok := body["metadata"].(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := metadata[strings.TrimPrefix(path, "meta:")]
		return value, ok
	}
	var current any = body
	for _, part := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}
func setPath(body map[string]any, path string, value any) {
	if strings.HasPrefix(path, "meta:") {
		metadata, ok := body["metadata"].(map[string]any)
		if !ok {
			metadata = map[string]any{}
			body["metadata"] = metadata
		}
		metadata[strings.TrimPrefix(path, "meta:")] = value
		return
	}
	parts := strings.Split(path, ".")
	current := body
	for _, part := range parts[:len(parts)-1] {
		child, ok := current[part].(map[string]any)
		if !ok {
			child = map[string]any{}
			current[part] = child
		}
		current = child
	}
	current[parts[len(parts)-1]] = value
}

func mask(body json.RawMessage, grants []string) (json.RawMessage, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var object map[string]any
	if err := decode(body, &object); err != nil || object == nil {
		return nil, fmt.Errorf("record body must be an object")
	}
	out := map[string]any{}
	for _, grant := range grants {
		if strings.HasSuffix(grant, ".*") {
			parent := strings.TrimSuffix(grant, ".*")
			if value, ok := lookup(object, parent); ok {
				setPath(out, parent, value)
			}
			continue
		}
		if strings.HasSuffix(grant, "/*") {
			metadata, _ := object["metadata"].(map[string]any)
			prefix := strings.TrimSuffix(strings.TrimPrefix(grant, "meta:"), "*")
			for key, value := range metadata {
				if strings.HasPrefix(key, prefix) {
					setPath(out, "meta:"+key, value)
				}
			}
			continue
		}
		if value, ok := lookup(object, grant); ok {
			setPath(out, grant, value)
		}
	}
	return json.Marshal(out)
}

func maskChange(change pluginapi.Change, grants []string) (pluginapi.Change, error) {
	var err error
	change.Before, err = mask(change.Before, grants)
	if err != nil {
		return change, err
	}
	change.After, err = mask(change.After, grants)
	if err != nil {
		return change, err
	}
	provenance := map[string]string{}
	for path, by := range change.SetBy {
		if granted(grants, path) {
			provenance[path] = by
		}
	}
	change.SetBy = provenance
	// Operation fields can contain the same sensitive data as the post-image.
	// Keep only addressing information and the granted data projection.
	var op map[string]any
	if err = decode(change.Op.Payload, &op); err != nil {
		return change, err
	}
	projection := map[string]any{"kind": change.Op.Kind}
	for _, key := range []string{"target"} {
		if value, ok := op[key]; ok {
			projection[key] = value
		}
	}
	if fields, ok := op["fields"]; ok {
		raw, e := json.Marshal(map[string]any{"fields": fields})
		if e != nil {
			return change, e
		}
		raw, e = mask(raw, grants)
		if e != nil {
			return change, e
		}
		var filtered map[string]any
		if e = decode(raw, &filtered); e != nil {
			return change, e
		}
		projection["fields"] = map[string]any{}
		if v, ok := filtered["fields"]; ok {
			projection["fields"] = v
		}
	}
	if change.Op.Kind == "comment" && granted(grants, "comments") {
		projection["body"] = op["body"]
	}
	if change.Op.Kind == "setMeta" {
		if key, ok := op["key"].(string); ok && granted(grants, "meta:"+key) {
			projection["key"], projection["value"] = key, op["value"]
		} else {
			delete(projection, "key")
		}
	}
	change.Op.Payload, err = json.Marshal(projection)
	return change, err
}
func granted(grants []string, path string) bool {
	for _, grant := range grants {
		if pluginapi.AllowsField(grant, path) {
			return true
		}
	}
	return false
}

func resourceKey(resource pluginapi.ResourceRef) (string, error) {
	if err := resource.Ref.Validate(); err != nil {
		return "", err
	}
	arm, value := "key", resource.Ref.Key
	if resource.Ref.IsID() {
		arm, value = "id", resource.Ref.ID
	}
	data, err := json.Marshal([]string{resource.Kind, arm, value, resource.Ref.Branch.String()})
	return string(data), err
}

func validatePatches(binding Binding, proposal pluginapi.ProposedChange, patches []pluginapi.Patch) error {
	resources := map[string]bool{}
	for _, change := range proposal.Changes {
		if len(change.After) > 0 && change.Op.Kind != "delete" {
			key, err := resourceKey(change.Resource)
			if err != nil {
				return err
			}
			resources[key] = true
		}
	}
	seen := map[string][]string{}
	for _, patch := range patches {
		key, err := resourceKey(patch.Resource)
		if err != nil {
			return err
		}
		if !resources[key] {
			return failure(ErrPatch, "patch targets a resource outside the proposal")
		}
		for path, value := range patch.Paths {
			if !pluginapi.ValidField(path, false) || !strings.HasPrefix(path, "fields.") || !granted(binding.Owns, path) || !json.Valid(value) {
				return failure(ErrPatch, "undeclared or unsupported field %s", path)
			}
			for _, previous := range seen[key] {
				if overlap(path, previous) {
					return failure(ErrPatch, "overlapping patch paths")
				}
			}
			seen[key] = append(seen[key], path)
		}
	}
	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
