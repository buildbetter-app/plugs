// Package main is a real source-built guest for the local construction test.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"
)

var arena [2 << 20]byte
var next uint32 = 1024
var base = uint32(uintptr(unsafe.Pointer(&arena[0])))
var calls int
var refusedReads int

const bulkObserverID = "00000000-0000-4000-8000-000000000999"

// newLocalFixture seeds the two ordinary records at revision 1; seedBulkReady
// appends this dedicated observer in the second changeset at revision 2.
const bulkObserverRevision = "2"

//go:wasmimport env plug.call
func snapshotCall(ptr, size uint32) uint64

//go:wasmexport plug_alloc
func allocate(size uint32) uint32 {
	if size == 0 || size > uint32(len(arena))-next {
		return 0
	}
	ptr := base + next
	next += size
	return ptr
}

func encode(value any) uint64 {
	data, _ := json.Marshal(value)
	ptr := allocate(uint32(len(data)))
	if ptr == 0 {
		return 0
	}
	copy(arena[ptr-base:ptr-base+uint32(len(data))], data)
	return uint64(ptr)<<32 | uint64(len(data))
}

//go:wasmexport plug_handle
func handle(ptr, size uint32) uint64 {
	if ptr < base || uint64(ptr-base)+uint64(size) > uint64(len(arena)) {
		return encode(map[string]any{"error": "invalid input"})
	}
	var request struct {
		Op      string `json:"op"`
		Payload struct {
			BaseRevision json.Number `json:"baseRevision"`
			Principal    struct {
				Kind    string `json:"kind"`
				ID      string `json:"id"`
				Sponsor struct {
					ID string `json:"id"`
				} `json:"sponsor"`
			} `json:"principal"`
			Changes []struct {
				Resource json.RawMessage `json:"resource"`
				After    struct {
					Fields map[string]any `json:"fields"`
				} `json:"after"`
			} `json:"changes"`
		} `json:"payload"`
	}
	if json.Unmarshal(arena[ptr-base:ptr-base+size], &request) != nil || request.Op != "mutate" || len(request.Payload.Changes) != 1 {
		return encode(map[string]any{"error": "expected one actual mutation"})
	}
	calls++
	p := request.Payload.Principal
	if p.Kind != "agent" || p.Sponsor.ID != "user:sponsor" {
		return encode(map[string]any{"error": "authenticated caller changed"})
	}
	title, _ := request.Payload.Changes[0].After.Fields["title"].(string)
	readID := "00000000-0000-4000-8000-000000000002"
	expectedReadRevision := "1"
	if strings.HasPrefix(p.ID, "agent:bulk-") {
		readID = bulkObserverID
		expectedReadRevision = bulkObserverRevision
	}
	method, params := "get", any(map[string]any{"refs": []any{map[string]string{"id": readID}}})
	if title == "refuse-read" {
		method, params = "query", map[string]any{"filter": map[string]string{"fields.description": "hidden"}, "limit": 1}
	}
	read, _ := json.Marshal(map[string]any{"op": "read", "method": method, "params": params})
	readPtr := allocate(uint32(len(read)))
	if readPtr == 0 {
		return encode(map[string]any{"error": "allocation failed"})
	}
	copy(arena[readPtr-base:readPtr-base+uint32(len(read))], read)
	packed := snapshotCall(readPtr, uint32(len(read)))
	outPtr, outLen := uint32(packed>>32), uint32(packed)
	if outPtr < base || uint64(outPtr-base)+uint64(outLen) > uint64(len(arena)) {
		return encode(map[string]any{"error": "invalid read response"})
	}
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if json.Unmarshal(arena[outPtr-base:outPtr-base+outLen], &reply) != nil {
		return encode(map[string]any{"error": "invalid scoped read envelope"})
	}
	if title == "refuse-read" {
		if strings.Contains(reply.Error, "plugin_read_out_of_scope") && strings.Contains(reply.Error, "query field fields.description is not declared") {
			refusedReads++
			return encode(map[string]any{"error": "scoped guest read refused"})
		}
		// A missing or different refusal must commit a visible sentinel rather
		// than satisfy the test's expected failure. The next successful call
		// also attests the guest observed this exact host refusal.
		return encode(map[string]any{"patches": []any{map[string]any{
			"resource": request.Payload.Changes[0].Resource,
			"paths":    map[string]string{"fields.title": "UNDECLARED-READ-ACCEPTED"},
		}}})
	}
	var rows []struct {
		Revision json.Number `json:"revision"`
		Body     struct {
			Fields map[string]any `json:"fields"`
		} `json:"body"`
	}
	if reply.Error != "" || json.Unmarshal(reply.Result, &rows) != nil || len(rows) != 1 {
		return encode(map[string]any{"error": "scoped guest read refused"})
	}
	row := rows[0]
	value, ok := row.Body.Fields["title"].(string)
	if !ok || string(row.Revision) != expectedReadRevision || request.Payload.BaseRevision == "" || len(row.Body.Fields) != 1 {
		return encode(map[string]any{"error": "guest read was not masked and pinned"})
	}
	derived := fmt.Sprintf("%s|%s|read=%s|base=%s|call=%d|refused=%d", strings.ToUpper(title), value, row.Revision, request.Payload.BaseRevision, calls, refusedReads)
	return encode(map[string]any{"patches": []any{map[string]any{
		"resource": request.Payload.Changes[0].Resource,
		"paths":    map[string]string{"fields.title": derived},
	}}})
}

func main() {}
