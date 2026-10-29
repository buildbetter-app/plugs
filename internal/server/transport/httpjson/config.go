// Package httpjson carries the RPC server's bytes over admitted HTTP requests.
package httpjson

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const Path = "/rpc/v1"

// Config bounds request buffering, concurrent work and connection lifetimes.
// Zero fields select defaults; negative values are configuration errors.
// RequestTimeout covers admission, body reading and cooperative dispatch.
// The listener applies connection timeouts too. A reused Handler needs an HTTP
// server that supports ResponseController deadlines, as net/http.Server does.
type Config struct {
	MaxBodyBytes          int64
	MaxConcurrentRequests int
	MaxHeaderBytes        int
	RequestTimeout        time.Duration
	ReadHeaderTimeout     time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	IdleTimeout           time.Duration
	// SessionBridge attaches a trusted collaboration session lifetime to an
	// admitted RPC request. The composition root supplies this seam; the
	// transport never treats a client principal or session header as proof on
	// its own.
	SessionBridge func(context.Context, model.PrincipalRef, string, string) (context.Context, error)
}

func (c Config) defaults() (Config, error) {
	if c.MaxBodyBytes < 0 || c.MaxConcurrentRequests < 0 || c.MaxHeaderBytes < 0 ||
		c.RequestTimeout < 0 || c.ReadHeaderTimeout < 0 || c.ReadTimeout < 0 ||
		c.WriteTimeout < 0 || c.IdleTimeout < 0 {
		return Config{}, fmt.Errorf("httpjson: limits and timeouts must be positive")
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.MaxConcurrentRequests == 0 {
		c.MaxConcurrentRequests = 64
	}
	if c.MaxHeaderBytes == 0 {
		c.MaxHeaderBytes = 16 << 10
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.ReadHeaderTimeout == 0 {
		c.ReadHeaderTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 35 * time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	return c, nil
}
