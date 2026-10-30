package main

import (
	"encoding/json"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// A bounded linear-memory arena keeps the ABI pointer stable across calls.
// The host caps the module's entire memory at 64 MiB; this fixture uses 2 MiB.
var (
	arena [2 << 20]byte
	next  uint32 = 1024
	base  uint32
	mu    sync.Mutex
)

func init() {
	base = uint32(uintptr(unsafe.Pointer(&arena[0])))
}

type request struct {
	Op      string          `json:"op"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

type changePayload struct {
	Changes []struct {
		Resource json.RawMessage `json:"resource"`
		After    json.RawMessage `json:"after"`
	} `json:"changes"`
}

//go:wasmexport plug_alloc
func plugAlloc(size uint32) uint32 {
	mu.Lock()
	defer mu.Unlock()
	if size == 0 || size > uint32(len(arena))-next {
		return 0
	}
	ptr := base + next
	next += size
	return ptr
}

//go:wasmexport plug_handle
func plugHandle(ptr, size uint32) uint64 {
	mu.Lock()
	if ptr < base || ptr-base >= uint32(len(arena)) || size > uint32(len(arena))-(ptr-base) {
		mu.Unlock()
		return response(map[string]any{"error": "request outside memory"})
	}
	input := append([]byte(nil), arena[ptr-base:ptr-base+size]...)
	mu.Unlock()
	var call request
	if err := json.Unmarshal(input, &call); err != nil {
		return response(map[string]any{"error": "invalid request"})
	}
	switch call.Op {
	case "mutate":
		return mutate(call.Payload)
	case "validate":
		return validate(call.Payload)
	case "method":
		if call.Name == "x.wasi.probe" {
			return probe(call.Payload)
		}
		return response(map[string]any{"result": json.RawMessage(call.Payload)})
	default:
		return response(map[string]any{"error": "unsupported operation"})
	}
}

func probe(raw json.RawMessage) uint64 {
	var params struct {
		Path    string `json:"path"`
		Address string `json:"address"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return response(map[string]any{"error": "invalid probe"})
	}
	_, readErr := os.ReadFile(params.Path)
	writeErr := os.WriteFile(params.Path, []byte("guest wrote here"), 0o600)
	createErr := os.WriteFile(params.Path+".new", []byte("guest wrote here"), 0o600)
	conn, dialErr := net.DialTimeout("tcp", params.Address, 100*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
	}
	return response(map[string]any{"result": map[string]any{
		"read_denied": readErr != nil, "write_denied": writeErr != nil,
		"create_denied": createErr != nil, "dial_denied": dialErr != nil,
		"environment_empty": os.Getenv("PLUG_WASI_PROBE") == "",
	}})
}

func mutate(raw json.RawMessage) uint64 {
	var payload changePayload
	if json.Unmarshal(raw, &payload) != nil || len(payload.Changes) == 0 {
		return response(map[string]any{"error": "invalid change"})
	}
	var after struct {
		Fields map[string]any `json:"fields"`
	}
	if json.Unmarshal(payload.Changes[0].After, &after) != nil {
		return response(map[string]any{"error": "invalid after"})
	}
	title, _ := after.Fields["title"].(string)
	value, _ := json.Marshal(strings.ToUpper(title))
	path := "fields.normalized"
	if title == "private" {
		path = "fields.private"
	}
	patch := map[string]any{
		"resource": json.RawMessage(payload.Changes[0].Resource),
		"paths":    map[string]json.RawMessage{path: value},
	}
	return response(map[string]any{"patches": []any{patch}})
}

func validate(raw json.RawMessage) uint64 {
	var payload changePayload
	if json.Unmarshal(raw, &payload) != nil {
		return response(map[string]any{"error": "invalid change"})
	}
	for _, item := range payload.Changes {
		var after struct {
			Fields map[string]any `json:"fields"`
		}
		if json.Unmarshal(item.After, &after) == nil {
			if title, _ := after.Fields["title"].(string); title == "reject" {
				return response(map[string]any{"verdict": map[string]any{"allow": false, "violations": []any{map[string]any{"guard": "wasi-check", "message": "WASI validator rejected"}}}})
			}
		}
	}
	return response(map[string]any{"verdict": map[string]any{"allow": true}})
}

func response(value map[string]any) uint64 {
	data, _ := json.Marshal(value)
	ptr := plugAlloc(uint32(len(data)))
	if ptr == 0 {
		return 0
	}
	mu.Lock()
	copy(arena[ptr-base:ptr-base+uint32(len(data))], data)
	mu.Unlock()
	return uint64(ptr)<<32 | uint64(len(data))
}

func main() {
}
