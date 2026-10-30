package stdio

import (
	"errors"
	"fmt"
)

// ErrorKind identifies a transport failure without exposing arbitrary reader
// or writer text to a JSON-RPC peer.
type ErrorKind string

const (
	ErrorFrameTooLarge ErrorKind = "frame_too_large"
	ErrorRead          ErrorKind = "read_failed"
	ErrorWrite         ErrorKind = "write_failed"
	ErrorCanceled      ErrorKind = "canceled"
)

var errFrameTooLarge = errors.New("stdio frame exceeds the configured limit")

// ErrFrameTooLarge is the sentinel wrapped by an oversize frame error.
var ErrFrameTooLarge = errFrameTooLarge

// Error is returned for stream failures. Framing failures that can be
// reported to a peer are also emitted as a canonical JSON-RPC error before
// Serve returns or continues with the next frame.
type Error struct {
	Kind  ErrorKind
	Limit int
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "stdio transport error"
	}
	switch e.Kind {
	case ErrorFrameTooLarge:
		return fmt.Sprintf("stdio frame exceeds %d bytes", e.Limit)
	case ErrorRead:
		return "stdio input read failed"
	case ErrorWrite:
		return "stdio output write failed"
	case ErrorCanceled:
		return "stdio transport canceled"
	default:
		return "stdio transport failed"
	}
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == errFrameTooLarge {
		return e.Kind == ErrorFrameTooLarge
	}
	return errors.Is(e.Cause, target)
}

// IsFrameTooLarge reports whether err is the typed oversize-frame refusal.
func IsFrameTooLarge(err error) bool { return errors.Is(err, errFrameTooLarge) }
