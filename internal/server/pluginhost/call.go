package pluginhost

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/pluginapi"
)

type invocation struct {
	ctx    context.Context
	phase  pluginapi.Phase
	reader pluginapi.SnapshotReader
}

// invocationKey authenticates the context supplied to a durable operation.
// A constructor may retain its facade, but an operation must carry the token
// of this instance's current call, never a previous call or a replacement root.
type invocationKey struct{}

// Native callbacks are serialized per instance. A timed-out, refused, or
// otherwise failed callback disposes its guest; the pool may replace that
// member only after its callback has unwound.
func (i *instance) run(ctx context.Context, phase pluginapi.Phase, timeout time.Duration, reader pluginapi.SnapshotReader, fn func(context.Context) error) error {
	if i.isDisposed() {
		return failure(ErrInactive, "host instance is disposed")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		i.dispose()
		return err
	}
	select {
	case i.gate <- struct{}{}:
	case <-ctx.Done():
		i.dispose()
		return ctx.Err()
	}
	if i.isDisposed() {
		<-i.gate
		return failure(ErrInactive, "host instance is disposed")
	}
	if err := ctx.Err(); err != nil {
		<-i.gate
		i.dispose()
		return err
	}
	if scoped, ok := reader.(*scopedReader); ok {
		scoped.ctx = ctx
	}
	call := &invocation{phase: phase, reader: reader}
	ctx = context.WithValue(ctx, invocationKey{}, call)
	call.ctx = ctx
	i.mu.Lock()
	i.active = call
	i.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("native callback panic: %v", recovered)
			}
			i.mu.Lock()
			i.active = nil
			i.mu.Unlock()
			<-i.gate
			result <- err
		}()
		err = fn(ctx)
	}()
	select {
	case err := <-result:
		if ctx.Err() != nil {
			i.dispose()
			return ctx.Err()
		}
		if err != nil {
			i.dispose()
		}
		return err
	case <-ctx.Done():
		i.dispose()
		return ctx.Err()
	}
}

// waitIdle waits until the per-instance callback gate is released. It is used
// by instance pools after a deadline returns while an uncooperative callback
// is still unwinding in the background.
func (i *instance) waitIdle() {
	i.gate <- struct{}{}
	<-i.gate
}

func (i *instance) current() (*invocation, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.active == nil {
		return nil, failure(ErrInactive, "host is available only during a host-driven call")
	}
	if err := i.active.ctx.Err(); err != nil {
		return nil, err
	}
	return i.active, nil
}
func (c *invocation) hot() bool {
	return c.phase == pluginapi.PhaseMutate || c.phase == pluginapi.PhaseValidate
}

type capabilities struct {
	instance  *instance
	onDispose func()
}

var _ pluginapi.Host = (*capabilities)(nil)

func (c *capabilities) Call(ctx context.Context, method string, params, result any) error {
	call, err := c.instance.current()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	// Never classify methods by a name prefix. The server registry supplies
	// the effect; absent methods cannot dispatch, even when declared.
	effect, known := Read, false
	if c.instance.owner.dispatcher != nil {
		effect, known = c.instance.owner.dispatcher.Effect(method)
	}
	if call.hot() {
		if known && effect == Write {
			return failure(ErrHotWrite, "%s", method)
		}
		// RPC-shaped reads need the registry's serializer and opaque cursors.
		// Until that adapter exists, refuse them instead of returning a record
		// array under a core RPC method's different result schema.
		return failure(ErrMethod, "synchronous callbacks read through SnapshotReader; Host.Call RPC reads are unsupported")
	}
	if call.phase == pluginapi.PhasePost {
		if known && effect == Write {
			return failure(ErrReentrant, "%s", method)
		}
		return failure(ErrMethod, "post callbacks read through their committed SnapshotReader")
	}
	if !known || effect != Read && effect != Write {
		return failure(ErrMethod, "unknown method %s", method)
	}
	if effect == Write {
		return failure(ErrMethod, "outbound graph writes require the registry's field-grant adapter, which is not implemented")
	}
	if !contains(c.instance.manifest.Requires.Methods, method) {
		return failure(ErrMethod, "undeclared method %s", method)
	}
	response, err := c.instance.owner.dispatcher.Call(call.ctx, pluginapi.PrincipalRef{Kind: "plugin", ID: c.instance.manifest.ID + "/" + c.instance.name}, method, data)
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	return decode(response, result)
}

func (c *capabilities) State() pluginapi.StateStore          { return &stateAccess{c.instance} }
func (c *capabilities) DurableState() pluginapi.DurableState { return &durableStateAccess{c.instance} }
func (c *capabilities) HTTP() *http.Client {
	return &http.Client{Transport: &egressTransport{instance: c.instance}, Timeout: time.Second, CheckRedirect: func(_ *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return failure(ErrEgress, "too many redirects")
		}
		return nil
	}}
}
func (c *capabilities) Secret(ctx context.Context, name string) (pluginapi.SecretValue, error) {
	call, err := c.instance.current()
	if err != nil {
		return pluginapi.SecretValue{}, err
	}
	if call.hot() {
		return pluginapi.SecretValue{}, failure(ErrHotEgress, "secret injection")
	}
	return pluginapi.SecretValue{}, failure(ErrEgress, "secret injection is unsupported")
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// Invoke runs a declared native extension. The embedding server authorizes
// its permission/resource before dispatch; schemas remain registry-owned.
func (h *Host) Invoke(ctx context.Context, name string, params json.RawMessage) (json.RawMessage, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, failure(ErrInactive, "plugin host is closed")
	}
	m, ok := h.methods[name]
	h.mu.Unlock()
	if !ok {
		return nil, failure(ErrMethod, "undeclared method %s", name)
	}
	var value map[string]any
	if err := decode(params, &value); err != nil || value == nil {
		return nil, failure(ErrMethod, "params must be an object")
	}
	resource, ok := lookup(value, strings.TrimPrefix(m.grant.Resource, "params."))
	if !ok {
		return nil, failure(ErrMethod, "missing declared resource parameter")
	}
	var ref pluginapi.Ref
	if id, ok := resource.(string); ok {
		ref = pluginapi.RefByID(id, "")
	} else {
		data, err := json.Marshal(resource)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &ref); err != nil {
			return nil, err
		}
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	refs, err := methodResources(m.Descriptor, value, ref)
	if err != nil {
		return nil, err
	}
	if h.authorize != nil {
		if err := h.authorize(ctx, m.grant.Permission, refs); err != nil {
			return nil, err
		}
	}
	var response json.RawMessage
	err = m.instance.run(ctx, "method", time.Second, nil, func(ctx context.Context) error {
		data, err := m.Handle(ctx, append(json.RawMessage(nil), params...))
		if err != nil {
			return err
		}
		if !json.Valid(data) {
			return fmt.Errorf("native method returned invalid JSON")
		}
		response = append(json.RawMessage(nil), data...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func methodResources(method pluginapi.Method, params any, required pluginapi.Ref) (refs []pluginapi.Ref, err error) {
	defer func() {
		if recover() != nil {
			err = failure(ErrMethod, "method resource extractor rejected params")
		}
	}()
	refs = method.Resource(params)
	key, _ := resourceKey(pluginapi.ResourceRef{Kind: "bead", Ref: required})
	present := false
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		actual, _ := resourceKey(pluginapi.ResourceRef{Kind: "bead", Ref: ref})
		if key == actual {
			present = true
		}
	}
	if !present {
		return nil, failure(ErrMethod, "resource extractor omitted the manifest resource")
	}
	return refs, nil
}
