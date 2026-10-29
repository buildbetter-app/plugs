// Package mcp adapts the canonical Plug JSON-RPC registry to the MCP
// streamable HTTP request/response shape. It owns MCP framing and tool
// projection only; authentication, authorization, method validation and
// domain behavior remain in the existing ports.
package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	// Path is the one MCP endpoint exposed by this adapter.
	Path = "/mcp"
	// DefaultMaxBodyBytes bounds one MCP POST body.
	DefaultMaxBodyBytes int64 = 1 << 20
	// DefaultRequestTimeout bounds admission, body reads and dispatch.
	DefaultRequestTimeout = 30 * time.Second
	// DefaultProtocolVersion is the contract version used when a client does
	// not negotiate one of the versions understood by this adapter.
	DefaultProtocolVersion = "2025-03-26"
)

// NotificationSink receives server-to-client JSON-RPC notifications that are
// discovered while serving a request. A streamable-HTTP composition root can
// put the bytes on its SSE/session stream; the JSON POST adapter also includes
// a pending notification in the next JSON response when no sink is installed.
// The sink must not mutate the supplied bytes.
type NotificationSink func(context.Context, model.PrincipalRef, []byte)

// Config bounds one MCP handler. Zero values select defaults. The handler is
// stateless: callers that need a long-lived MCP session may keep using the
// same admitted HTTP connection, while every request still gets a fresh
// canonical method visibility check.
type Config struct {
	MaxBodyBytes          int64
	MaxConcurrentRequests int
	RequestTimeout        time.Duration
	ReadTimeout           time.Duration
	ProtocolVersion       string
	ServerName            string
	OnNotification        NotificationSink
}

func (c Config) defaults() (Config, error) {
	if c.MaxBodyBytes < 0 || c.MaxConcurrentRequests < 0 || c.RequestTimeout < 0 || c.ReadTimeout < 0 {
		return Config{}, fmt.Errorf("mcp: limits and timeout must not be negative")
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.MaxConcurrentRequests == 0 {
		c.MaxConcurrentRequests = 64
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultRequestTimeout
	}
	if c.ProtocolVersion == "" {
		c.ProtocolVersion = DefaultProtocolVersion
	}
	if c.ServerName == "" {
		c.ServerName = "plug"
	}
	return c, nil
}
