package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// NativeEdgeField reports source fields with no ordinary Plug edge slot.
// The classification is open to future fields, not an allowlist of thread IDs.
func NativeEdgeField(field string) bool {
	switch field {
	case "id", "issue_id", "depends_on_issue_id", EphemeralDependencyTargetField, "depends_on_external", "type", "created_at", "created_by":
		return false
	default:
		return true
	}
}

// CaptureNativeEdgeTokens reads actual source JSON tokens before ordinary map
// decoding can erase string escapes, object spacing or number spellings.
// Callers must first validate the complete record using the archive grammar.
func CaptureNativeEdgeTokens(data []byte) (map[string]string, error) {
	var raw struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for key, token := range raw.Source.Fields {
		if NativeEdgeField(key) {
			out["beads."+key] = string(token)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	if err := model.ValidateNativeExtensions(out); err != nil {
		return nil, err
	}
	return out, nil
}

// NativeEdgeExtensions uses captured tokens when available. Programmatic
// records have no original token stream; their JSON encoding is the declared
// capture boundary, never evidence of an earlier lexical representation.
func NativeEdgeExtensions(record Record) (map[string]string, error) {
	out := map[string]string{}
	for key, value := range record.Source.Fields {
		if !NativeEdgeField(key) {
			continue
		}
		name := "beads." + key
		token, ok := record.NativeEdgeTokens[name]
		if !ok {
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			token = string(raw)
		}
		var decoded any
		if err := decodeJSON([]byte(token), &decoded); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if !equalJSON(decoded, value) {
			return nil, fmt.Errorf("%s token/source mismatch", name)
		}
		out[name] = token
	}
	for key := range record.NativeEdgeTokens {
		if _, ok := out[key]; !ok {
			return nil, fmt.Errorf("%s token lacks source field", key)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, model.ValidateNativeExtensions(out)
}

// SourceLinkMetadata combines the typed scheduling projection with detached
// opaque source tokens. Opaque-only metadata deliberately has no predicate.
func SourceLinkMetadata(version int, record Record, kind string) (*model.LinkMetadata, error) {
	predicate, err := SourceEdgeMetadata(version, record.ID, kind, record.Body["metadata"])
	if err != nil {
		return nil, err
	}
	opaque, err := NativeEdgeExtensions(record)
	if err != nil {
		return nil, err
	}
	if len(opaque) == 0 {
		return predicate, nil
	}
	value := model.LinkMetadata{NativeExtensions: maps.Clone(opaque)}
	if predicate != nil {
		value.Gate, value.AlsoBlocks = predicate.Gate, predicate.AlsoBlocks
	}
	return &value, nil
}

// DecodeNativeEdgeToken returns a detached projection solely for validation or
// native export. The caller retains the token separately as the authority.
func DecodeNativeEdgeToken(token string) (any, error) {
	var value any
	if err := decodeJSON([]byte(token), &value); err != nil {
		return nil, err
	}
	return value, nil
}

// EqualNativeEdgeTokenValue compares projections without changing token bytes.
func EqualNativeEdgeTokenValue(token string, value any) bool {
	decoded, err := DecodeNativeEdgeToken(token)
	if err != nil {
		return false
	}
	a, err := json.Marshal(decoded)
	if err != nil {
		return false
	}
	b, err := json.Marshal(value)
	return err == nil && bytes.Equal(a, b)
}
