// Package websocket carries the RPC server's bytes over an admitted
// WebSocket connection. It owns framing and connection lifetime only; RPC
// method binding, authentication and watch state remain in their packages.
package websocket

import (
	"fmt"
	"time"
)

const Path = "/rpc/v1/ws"

const (
	defaultMaxMessageBytes  int64 = 1 << 20
	defaultMaxFrameBytes    int64 = 1 << 20
	defaultMaxPendingReads        = 64
	defaultMaxPendingWrites       = 64
	defaultRequestTimeout         = 30 * time.Second
	defaultReadTimeout            = 60 * time.Second
	defaultWriteTimeout           = 35 * time.Second
)

// Config bounds frames, queued requests and queued responses. A connection is
// closed with a bounded WebSocket close code when either queue is full; a
// caller cannot make the server retain unbounded work for a slow peer.
// Zero fields select defaults; negative values are configuration errors.
type Config struct {
	Path             string
	MaxMessageBytes  int64
	MaxFrameBytes    int64
	MaxPendingReads  int
	MaxPendingWrites int
	RequestTimeout   time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
}

func (c Config) defaults() (Config, error) {
	if c.Path == "" {
		c.Path = Path
	}
	if c.MaxMessageBytes < 0 || c.MaxFrameBytes < 0 || c.MaxPendingReads < 0 ||
		c.MaxPendingWrites < 0 || c.RequestTimeout < 0 || c.ReadTimeout < 0 ||
		c.WriteTimeout < 0 {
		return Config{}, fmt.Errorf("websocket: limits and timeouts must be nonnegative")
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = defaultMaxMessageBytes
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = defaultMaxFrameBytes
	}
	if c.MaxMessageBytes < 1 || c.MaxMessageBytes > 64<<20 {
		return Config{}, fmt.Errorf("websocket: MaxMessageBytes must be between 1 and 67108864")
	}
	if c.MaxFrameBytes < 1 || c.MaxFrameBytes > c.MaxMessageBytes {
		return Config{}, fmt.Errorf("websocket: MaxFrameBytes must be between 1 and MaxMessageBytes")
	}
	if c.MaxPendingReads == 0 {
		c.MaxPendingReads = defaultMaxPendingReads
	}
	if c.MaxPendingWrites == 0 {
		c.MaxPendingWrites = defaultMaxPendingWrites
	}
	if c.MaxPendingReads < 1 || c.MaxPendingWrites < 1 {
		return Config{}, fmt.Errorf("websocket: pending queue sizes must be positive")
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = defaultRequestTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	return c, nil
}
