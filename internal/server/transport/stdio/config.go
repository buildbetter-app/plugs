// Package stdio carries canonical JSON-RPC messages over newline-delimited
// standard input and output. It owns framing and stream lifetime only; method
// validation, authentication context and errors remain with the dispatcher.
package stdio

import (
	"fmt"
	"time"
)

const (
	// DefaultMaxFrameBytes bounds one JSON-RPC request, excluding its newline.
	DefaultMaxFrameBytes = 1 << 20
	// DefaultRequestTimeout bounds cooperative dispatch of one frame.
	DefaultRequestTimeout = 30 * time.Second
)

// Config bounds one stdio stream. Zero values select the defaults; negative
// values are rejected so a caller cannot accidentally disable framing bounds.
type Config struct {
	MaxFrameBytes  int
	RequestTimeout time.Duration
}

func (c Config) defaults() (Config, error) {
	if c.MaxFrameBytes < 0 || c.RequestTimeout < 0 {
		return Config{}, fmt.Errorf("stdio: limits and timeout must not be negative")
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}
	return c, nil
}
