package reverseconverter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// marshalTokenObject is deliberately not json.Marshaler: json.Marshal compacts
// Marshaler output. Here native field token bytes are copied only after strict
// validation, with deterministic ordering of the surrounding object.
func marshalTokenObject(fields map[string]json.RawMessage) ([]byte, error) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range keys {
		if !json.Valid(fields[key]) {
			return nil, fmt.Errorf("invalid native field %q", key)
		}
		if i != 0 {
			out.WriteByte(',')
		}
		name, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		out.Write(name)
		out.WriteByte(':')
		out.Write(fields[key])
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

func marshalNativeDependency(dependency dependencyLine) ([]byte, error) {
	if err := model.ValidateNativeExtensions(dependency.NativeExtensions); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(dependency)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	for key, token := range dependency.NativeExtensions {
		field := strings.TrimPrefix(key, "beads.")
		// Metadata is emitted from the source payload by convertEdge. Native
		// extensions never add a Plug identity envelope to the Beads cell.
		if field == "metadata" {
			continue
		}
		fields[field] = json.RawMessage(token)
	}
	if dependency.MetadataNull {
		fields["metadata"] = json.RawMessage("null")
	}
	return marshalTokenObject(fields)
}

func marshalNativeIssue(issue issueLine) ([]byte, error) {
	dependencies := issue.Dependencies
	issue.Dependencies = nil
	raw, err := json.Marshal(issue)
	if err != nil {
		return nil, err
	}
	if len(dependencies) == 0 {
		return raw, nil
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	var array bytes.Buffer
	array.WriteByte('[')
	for i, dependency := range dependencies {
		if i != 0 {
			array.WriteByte(',')
		}
		raw, err = marshalNativeDependency(dependency)
		if err != nil {
			return nil, err
		}
		array.Write(raw)
	}
	array.WriteByte(']')
	fields["dependencies"] = array.Bytes()
	return marshalTokenObject(fields)
}
