// Package runtime contains the executable E5 runtime and concurrency spike.
//
// The package deliberately does not install a production plugin host.  It
// records the narrow candidate boundary that the host can test before a
// mode-2 implementation is admitted.
package runtime

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	// ABIName identifies the candidate described by the E5 report.
	ABIName = "plug-wasi-jsonrpc-v1-candidate"

	// MaxMessageBytes is the per-call request/response cap used by the spike.
	// The production value remains a design decision after this harness.
	MaxMessageBytes = 1 << 20

	// CandidateMemoryBytes is the one-page memory used by the tiny executable
	// fixture.  It is intentionally smaller than the production pool ceiling.
	CandidateMemoryBytes = 64 << 10

	allocOffset = 4096
)

// RequiredExports is the complete function export set.  A memory export is
// also required so the host can copy one framed JSON-RPC message.
var RequiredExports = [...]string{"plug_alloc", "plug_handle"}

// RequiredImport is the one host call imported by the candidate.
var RequiredImport = struct {
	Module string
	Name   string
}{Module: "env", Name: "plug.call"}

// PackResult encodes a guest-owned pointer and byte length in the i64 result
// of plug_handle and plug.call.  The high word is the pointer and the low
// word is the length.  Zero length is valid and never means a trap.
func PackResult(ptr, length uint32) uint64 {
	return uint64(ptr)<<32 | uint64(length)
}

// UnpackResult decodes the result of either ABI function.
func UnpackResult(value uint64) (ptr, length uint32) {
	return uint32(value >> 32), uint32(value)
}

// ValidateMessage applies the candidate's framing rules before a message
// crosses the host boundary.  JSON-RPC validation remains the caller's
// responsibility; this only prevents ambiguous or unbounded byte ownership.
func ValidateMessage(message []byte) error {
	if len(message) == 0 {
		return fmt.Errorf("runtime ABI: empty message")
	}
	if len(message) > MaxMessageBytes {
		return fmt.Errorf("runtime ABI: message exceeds %d bytes", MaxMessageBytes)
	}
	if !json.Valid(message) {
		return fmt.Errorf("runtime ABI: message is not valid JSON")
	}
	return nil
}

// CandidateModule returns a minimal valid WebAssembly module implementing the
// two-export candidate.  It has one 64 KiB memory, no WASI imports, and one
// env.plug.call import.  The allocator reserves a fixed scratch region; the
// host replaces this fixture with a real guest allocator during the builder
// work that follows the spike.
func CandidateModule() []byte {
	var module []byte
	module = append(module, 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00)
	module = appendSection(module, 1, section(
		0x03,
		0x60, 0x01, 0x7f, 0x01, 0x7f, // alloc: (i32) -> i32
		0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7e, // handle: (i32, i32) -> i64
		0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7e, // plug.call: (i32, i32) -> i64
	))
	module = appendSection(module, 2, section(
		0x01,
		stringValue("env"), stringValue("plug.call"), 0x00, 0x02,
	))
	module = appendSection(module, 3, section(0x02, 0x00, 0x01))
	module = appendSection(module, 5, section(0x01, 0x00, 0x01))
	module = appendSection(module, 7, section(
		0x03,
		stringValue("memory"), 0x02, 0x00,
		stringValue("plug_alloc"), 0x00, 0x01,
		stringValue("plug_handle"), 0x00, 0x02,
	))
	allocBody := []byte{0x00, 0x41, 0x80, 0x20, 0x0b}
	handleBody := []byte{0x00, 0x20, 0x00, 0x20, 0x01, 0x10, 0x00, 0x0b}
	module = appendSection(module, 10, section(
		0x02,
		byteValue(allocBody),
		byteValue(handleBody),
	))
	return module
}

func section(parts ...any) []byte {
	var result []byte
	for _, part := range parts {
		switch value := part.(type) {
		case byte:
			result = append(result, value)
		case int:
			result = append(result, byte(value))
		case []byte:
			result = append(result, value...)
		default:
			panic(fmt.Sprintf("runtime ABI: unsupported section part %T", part))
		}
	}
	return result
}

func appendSection(module []byte, id byte, payload []byte) []byte {
	module = append(module, id)
	module = append(module, leb128(uint32(len(payload)))...)
	return append(module, payload...)
}

func stringValue(value string) []byte {
	result := leb128(uint32(len(value)))
	return append(result, []byte(value)...)
}

func byteValue(value []byte) []byte {
	result := leb128(uint32(len(value)))
	return append(result, value...)
}

func leb128(value uint32) []byte {
	var result []byte
	for {
		part := byte(value & 0x7f)
		value >>= 7
		if value != 0 {
			part |= 0x80
		}
		result = append(result, part)
		if value == 0 {
			return result
		}
	}
}

// EncodeFrame is the candidate's host-side framing helper.  The length is
// little-endian and includes only the JSON bytes; a caller must still apply
// ValidateMessage to the payload before sending it.
func EncodeFrame(message []byte) ([]byte, error) {
	if err := ValidateMessage(message); err != nil {
		return nil, err
	}
	frame := make([]byte, 4+len(message))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(message)))
	copy(frame[4:], message)
	return frame, nil
}

// DecodeFrame decodes one complete frame and rejects trailing bytes.
func DecodeFrame(frame []byte) ([]byte, error) {
	if len(frame) < 4 {
		return nil, fmt.Errorf("runtime ABI: truncated frame header")
	}
	length := binary.LittleEndian.Uint32(frame[:4])
	if length > MaxMessageBytes {
		return nil, fmt.Errorf("runtime ABI: frame exceeds %d bytes", MaxMessageBytes)
	}
	if uint64(length)+4 != uint64(len(frame)) {
		return nil, fmt.Errorf("runtime ABI: frame length mismatch")
	}
	message := frame[4:]
	if err := ValidateMessage(message); err != nil {
		return nil, err
	}
	return append([]byte(nil), message...), nil
}
