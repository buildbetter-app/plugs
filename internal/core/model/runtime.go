package model

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Principal is the effective principal stamped by authentication.
// The wire representation remains the canonical PrincipalRef shape.
type Principal = PrincipalRef

// Record is the schema-shaped core read value. Driver-specific read options,
// cursors, and transaction state belong to the storage driver package.
type Record struct {
	Ref      Ref            `json:"ref"`
	Kind     string         `json:"kind"`
	Revision Revision       `json:"revision"`
	Body     map[string]any `json:"body"`
}

// ErrorData is the catalog metadata carried by a transport error.
// A transport owns the outer JSON-RPC error object and integer mapping.
type ErrorData struct {
	Code         ErrorCode      `json:"code"`
	Retryable    bool           `json:"retryable"`
	RetryAfterMs *Natural       `json:"retryAfterMs,omitempty"`
	Hint         string         `json:"hint,omitempty"`
	TraceID      string         `json:"traceId,omitempty"`
	Details      map[string]any `json:"details,omitempty"`
}

// Error carries a typed core failure without a transport dependency.
// Use errors.As to recover its catalog data after wrapping.
type Error struct {
	Message string
	Data    ErrorData
	// NotRetryable marks one failure that a caller must not repeat unchanged.
	// Transports clear the code's catalog-default retryable flag for it; it can
	// only downgrade retryability, never grant it.
	NotRetryable bool
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return string(e.Data.Code)
}

func (d ErrorData) MarshalJSON() ([]byte, error) {
	if err := d.Code.Validate(); err != nil {
		return nil, err
	}
	type wire ErrorData
	return json.Marshal(wire(d))
}
func (d *ErrorData) UnmarshalJSON(data []byte) error {
	type wire ErrorData
	var v wire
	if err := decodeStrict(data, &v, "code", "retryable"); err != nil {
		return err
	}
	if err := v.Code.Validate(); err != nil {
		return err
	}
	*d = ErrorData(v)
	return nil
}

// Int64 checks the engine-sized conversion; it never silently narrows.
func (v Natural) Int64() (int64, error) {
	s, err := v.Canonical()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("natural does not fit int64: %w", err)
	}
	return n, nil
}
