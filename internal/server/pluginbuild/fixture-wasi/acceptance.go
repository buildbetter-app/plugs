package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// These modes belong only to the source-built acceptance fixture. They enter
// through the same normalize callback and env.plug.call import as its positive
// masked mutate/validate/post path; they add no host or SDK operation.
func acceptanceMutation(payload changePayload, title string) uint64 {
	var resource struct {
		Ref struct {
			ID string `json:"id"`
		} `json:"ref"`
	}
	if json.Unmarshal(payload.Changes[0].Resource, &resource) != nil || resource.Ref.ID == "" {
		return response(map[string]any{"error": "acceptance callback requires a record"})
	}
	mode := strings.TrimPrefix(title, "acceptance:")
	var err error
	switch mode {
	case "undeclared":
		_, err = acceptanceRead("query", map[string]any{
			"filter": map[string]any{"fields.private": "host-only"}, "limit": 1,
		})
	case "foreign-branch":
		_, err = acceptanceRead("get", map[string]any{
			"refs": []map[string]any{{"id": resource.Ref.ID, "branch": 1}},
		})
	case "foreign-cursor":
		_, err = acceptanceRead("query", map[string]any{"limit": 1, "cursor": map[string]any{
			"incarnation": "foreign-acceptance-store", "revision": 1, "index": 0,
			"method": "bead.list", "filterDigest": "", "principalDigest": "", "ontologyRevision": 0,
		}})
	case "expired":
		// The test holds the real tracked reader until this callback expires.
		// A result from an expired import must never reach the guest.
		_, err = readRecords(resource.Ref.ID)
	case "bounds", "oversized":
		// Establish that this ordinary callback has a usable scoped reader
		// before issuing the invalid ABI request. Missing-reader refusal must
		// not be able to satisfy the bounds assertion.
		records, readErr := readRecords(resource.Ref.ID)
		if readErr != nil || len(records) != 1 {
			return response(map[string]any{"error": fmt.Sprintf("acceptance pre-read: %v", readErr)})
		}
		if readErr := validateReadRecords(records); readErr != nil {
			return response(map[string]any{"error": readErr.Error()})
		}
		if mode == "bounds" {
			// The host caps the complete module at 64 MiB. UINT32_MAX with
			// length one is outside every admitted module's linear memory.
			_ = snapshotCall(^uint32(0), 1)
		} else {
			_ = snapshotCall(plugAlloc(1), (1<<20)+1)
		}
		return response(map[string]any{"error": "invalid acceptance buffer unexpectedly returned"})
	default:
		if !strings.HasPrefix(mode, "watch:") {
			return response(map[string]any{"error": "unknown acceptance mode"})
		}
		// Read a record other than the write target. Only this guest import
		// introduces its point-read fact into the Writer's commit guard.
		records, readErr := readRecords(strings.TrimPrefix(mode, "watch:"))
		if readErr != nil || len(records) != 1 {
			return response(map[string]any{"error": fmt.Sprintf("acceptance watched read: %v", readErr)})
		}
		if readErr := validateReadRecords(records); readErr != nil {
			return response(map[string]any{"error": readErr.Error()})
		}
		var body struct {
			Fields map[string]string `json:"fields"`
		}
		if readErr := json.Unmarshal(records[0].Body, &body); readErr != nil {
			return response(map[string]any{"error": readErr.Error()})
		}
		value, _ := json.Marshal(strings.ToUpper(body.Fields["title"]))
		return response(map[string]any{"patches": []any{map[string]any{
			"resource": payload.Changes[0].Resource,
			"paths":    map[string]json.RawMessage{"fields.normalized": value},
		}}})
	}
	if err == nil {
		return response(map[string]any{"error": "acceptance " + mode + " unexpectedly succeeded"})
	}
	return response(map[string]any{"error": "acceptance " + mode + " refused: " + err.Error()})
}

func acceptanceRead(method string, params any) (json.RawMessage, error) {
	data, err := json.Marshal(map[string]any{"op": "read", "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	ptr := plugAlloc(uint32(len(data)))
	if ptr < base || uint64(ptr-base) >= uint64(len(arena)) || uint32(len(data)) > uint32(len(arena))-(ptr-base) {
		return nil, fmt.Errorf("acceptance request allocation failed")
	}
	mu.Lock()
	copy(arena[ptr-base:ptr-base+uint32(len(data))], data)
	mu.Unlock()
	packed := snapshotCall(ptr, uint32(len(data)))
	resultPtr, resultLen := uint32(packed>>32), uint32(packed)
	if resultPtr < base || uint64(resultPtr-base) >= uint64(len(arena)) || resultLen > uint32(len(arena))-(resultPtr-base) {
		return nil, fmt.Errorf("acceptance response outside guest memory")
	}
	mu.Lock()
	result := append([]byte(nil), arena[resultPtr-base:resultPtr-base+resultLen]...)
	mu.Unlock()
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if err := json.Unmarshal(result, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != "" {
		return nil, fmt.Errorf("snapshot read refused: %s", envelope.Error)
	}
	return envelope.Result, nil
}
