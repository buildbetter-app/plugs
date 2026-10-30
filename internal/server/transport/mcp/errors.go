package mcp

import (
	"fmt"
)

// ErrorKind identifies an MCP transport failure without exposing reader,
// authentication or writer diagnostics to the peer.
type ErrorKind string

const (
	ErrorAdmission ErrorKind = "admission_failed"
	ErrorRead      ErrorKind = "read_failed"
	ErrorWrite     ErrorKind = "write_failed"
	ErrorCanceled  ErrorKind = "canceled"
)

// Error is returned by the handler only for transport-level failures. Canonical
// RPC errors are encoded in the JSON-RPC response or tool result and are not
// returned as arbitrary Go errors.
type Error struct {
	Kind  ErrorKind
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "mcp transport error"
	}
	switch e.Kind {
	case ErrorAdmission:
		return "mcp admission failed"
	case ErrorRead:
		return "mcp request read failed"
	case ErrorWrite:
		return "mcp response write failed"
	case ErrorCanceled:
		return "mcp request canceled"
	default:
		return "mcp transport failed"
	}
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func invalidConfig(message string) error { return fmt.Errorf("mcp: %s", message) }
