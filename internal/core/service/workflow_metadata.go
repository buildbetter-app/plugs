package service

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// workflowMetadata lowers the ordinary literal-key metadata operation without
// sending its JSON numbers through float64 or replacing unrelated keys.
func workflowMetadata(current map[string]any, op model.SetMetaOp) (map[string]any, bool, error) {
	if op.Key == "" || strings.HasPrefix(op.Key, "_plug") || strings.HasPrefix(op.Key, "core.") {
		return nil, false, failure("protected_record", "reserved core metadata cannot be changed")
	}
	value, err := decodeJSONValue(op.Value)
	if err != nil {
		return nil, false, failure("invalid_params", "metadata requires valid JSON")
	}
	previous, present := current[op.Key]
	if value == nil && !present {
		return current, false, nil
	}
	if value != nil && present {
		before, beforeErr := json.Marshal(previous)
		after, afterErr := json.Marshal(value)
		if beforeErr != nil || afterErr != nil {
			return nil, false, failure("invalid_params", "metadata comparison requires valid JSON")
		}
		if bytes.Equal(before, after) {
			return current, false, nil
		}
	}
	next := make(map[string]any, len(current)+1)
	for key, sibling := range current {
		next[key] = sibling
	}
	if value == nil {
		delete(next, op.Key)
	} else {
		next[op.Key] = value
	}
	return next, true, nil
}
