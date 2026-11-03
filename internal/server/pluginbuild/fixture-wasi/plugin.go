//go:build wasip1

package main

import (
	"encoding/json"
	"fmt"
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
	arena      [2 << 20]byte
	next       uint32 = 1024
	stateCalls uint32
	base       uint32
	mu         sync.Mutex
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

type committedPayload struct {
	Revision json.Number `json:"revision"`
	Changes  []struct {
		Resource json.RawMessage `json:"resource"`
	} `json:"changes"`
}

// The host import is the internal E5 callback bridge. Its request and result
// are JSON envelopes documented by pluginhost; this fixture deliberately uses
// only the typed SnapshotReader get operation.
//
//go:wasmimport env plug.call
func snapshotCall(ptr, size uint32) uint64

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
	case "post":
		return post(call.Payload)
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
	if title == "guest-exit-canceled" {
		// WASI proc_exit(-1) uses wazero's reserved context-canceled code.
		// The live caller has not canceled, so the host must retain a guest
		// failure rather than treating this as trusted owner retirement.
		os.Exit(-1)
	}
	if strings.HasPrefix(title, "timer-callback:") {
		// This is an actual guest callback-owned WASI nanosleep. The host-side
		// test observes entry through the callback wait context seam and then
		// cancels or retires the invocation without changing production bounds.
		time.Sleep(time.Second)
	}
	if strings.HasPrefix(title, "acceptance:") {
		return acceptanceMutation(payload, title)
	}
	if strings.HasPrefix(title, "pool-state:") {
		var resource struct {
			Ref struct {
				ID string `json:"id"`
			} `json:"ref"`
		}
		if json.Unmarshal(payload.Changes[0].Resource, &resource) == nil && resource.Ref.ID != "" {
			if _, err := readRecords(resource.Ref.ID); err != nil {
				return response(map[string]any{"error": err.Error()})
			}
		}
		mu.Lock()
		stateCalls++
		calls := stateCalls
		mu.Unlock()
		value, _ := json.Marshal(fmt.Sprintf("%s#%d", strings.ToUpper(title), calls))
		return response(map[string]any{"patches": []any{map[string]any{
			"resource": json.RawMessage(payload.Changes[0].Resource),
			"paths":    map[string]json.RawMessage{"fields.normalized": value},
		}}})
	}
	if len(payload.Changes[0].Resource) != 0 {
		var resource struct {
			Ref struct {
				ID string `json:"id"`
			} `json:"ref"`
		}
		if json.Unmarshal(payload.Changes[0].Resource, &resource) == nil && resource.Ref.ID != "" {
			// A missing record is expected for a create at its pinned base
			// revision. Existing records must be readable and revision-pinned.
			if records, err := readRecords(resource.Ref.ID); err != nil {
				return response(map[string]any{"error": err.Error()})
			} else if err := validateReadRecords(records); err != nil {
				return response(map[string]any{"error": err.Error()})
			}
		}
	}
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
		var resource struct {
			Ref struct {
				ID string `json:"id"`
			} `json:"ref"`
		}
		if json.Unmarshal(item.Resource, &resource) == nil && resource.Ref.ID != "" {
			if records, err := readRecords(resource.Ref.ID); err != nil {
				return response(map[string]any{"error": err.Error()})
			} else if err := validateReadRecords(records); err != nil {
				return response(map[string]any{"error": err.Error()})
			}
		}
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

func post(raw json.RawMessage) uint64 {
	var payload committedPayload
	if json.Unmarshal(raw, &payload) != nil || len(payload.Changes) == 0 || payload.Revision == "" {
		return response(map[string]any{"error": "invalid committed change"})
	}
	var resource struct {
		Ref struct {
			ID string `json:"id"`
		} `json:"ref"`
	}
	if json.Unmarshal(payload.Changes[0].Resource, &resource) != nil || resource.Ref.ID == "" {
		return response(map[string]any{"error": "invalid committed resource"})
	}
	records, err := readRecords(resource.Ref.ID)
	if err != nil {
		return response(map[string]any{"error": err.Error()})
	}
	if len(records) != 1 || records[0].Revision.String() != payload.Revision.String() {
		return response(map[string]any{"error": fmt.Sprintf("post read revision=%q want=%q", firstRevision(records), payload.Revision.String())})
	}
	if err := checkScopedRecord(records[0]); err != nil {
		return response(map[string]any{"error": err.Error()})
	}
	return response(map[string]any{"result": map[string]any{"observedRevision": records[0].Revision}})
}

type readRecord struct {
	Revision json.Number     `json:"revision"`
	Body     json.RawMessage `json:"body"`
}

func checkScopedRecord(record readRecord) error {
	var body struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(record.Body, &body); err != nil {
		return err
	}
	if _, hidden := body.Fields["private"]; hidden {
		return fmt.Errorf("scoped read leaked undeclared private field")
	}
	if _, present := body.Fields["title"]; !present {
		return fmt.Errorf("scoped read omitted declared title field")
	}
	return nil
}

func validateReadRecords(records []readRecord) error {
	if len(records) == 0 {
		return nil
	}
	if records[0].Revision == "" {
		return fmt.Errorf("read record omitted revision")
	}
	return checkScopedRecord(records[0])
}

func firstRevision(records []readRecord) string {
	if len(records) == 0 {
		return ""
	}
	return records[0].Revision.String()
}

func readRecords(id string) ([]readRecord, error) {
	request, err := json.Marshal(map[string]any{
		"op":     "read",
		"method": "get",
		"params": map[string]any{"refs": []map[string]string{{"id": id}}},
	})
	if err != nil {
		return nil, err
	}
	ptr := plugAlloc(uint32(len(request)))
	if ptr < base || uint64(ptr-base) >= uint64(len(arena)) || uint32(len(request)) > uint32(len(arena))-(ptr-base) {
		return nil, fmt.Errorf("guest request allocation failed")
	}
	mu.Lock()
	copy(arena[ptr-base:ptr-base+uint32(len(request))], request)
	mu.Unlock()
	packed := snapshotCall(ptr, uint32(len(request)))
	resultPtr, resultLen := uint32(packed>>32), uint32(packed)
	if resultPtr < base || uint64(resultPtr-base) >= uint64(len(arena)) || resultLen > uint32(len(arena))-(resultPtr-base) {
		return nil, fmt.Errorf("host response is outside guest memory")
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
	var records []readRecord
	if err := json.Unmarshal(envelope.Result, &records); err != nil {
		return nil, err
	}
	return records, nil
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
