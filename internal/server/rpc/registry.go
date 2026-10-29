// Package rpc implements the shared strict JSON-RPC dispatcher and service
// bindings. All transports call Dispatch with the same request bytes.
package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

type Call struct {
	Method    string
	Params    json.RawMessage
	Meta      wire.Meta
	Principal model.PrincipalRef
	paging    *pageCodec
}

type Handler func(context.Context, Call) (any, error)

type binding struct {
	desc    wire.MethodDesc
	handler Handler
}

type Registry struct {
	mu       sync.RWMutex
	catalog  *wire.Catalog
	bindings map[string]binding
}

func NewRegistry() (*Registry, error) {
	c, err := wire.DefaultCatalog()
	if err != nil {
		return nil, err
	}
	return &Registry{catalog: c, bindings: map[string]binding{}}, nil
}

// Register binds an actual handler to its authoritative descriptor. An unbound
// inventory method is unsupported and never appears in runtime discovery.
// Register all handlers before New; New snapshots the registry.
func (r *Registry) Register(name string, handler Handler) error {
	d, ok := r.catalog.Descriptor(name)
	if !ok {
		return fmt.Errorf("method outside bounded inventory: %s", name)
	}
	if d.Effect == "event" {
		return fmt.Errorf("server notification is not callable: %s", name)
	}
	if handler == nil {
		return fmt.Errorf("nil handler for %s", name)
	}
	if err := d.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.bindings[name]; exists {
		return fmt.Errorf("duplicate handler: %s", name)
	}
	r.bindings[name] = binding{d, handler}
	return nil
}

func (r *Registry) Descriptors() []wire.MethodDesc {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]wire.MethodDesc, 0, len(r.bindings))
	for name := range r.bindings {
		d, _ := r.catalog.Descriptor(name)
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *Registry) Catalog() *wire.Catalog { return r.catalog }

func (r *Registry) snapshot() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := &Registry{catalog: r.catalog, bindings: map[string]binding{}}
	for n, b := range r.bindings {
		copy.bindings[n] = b
	}
	return copy
}
