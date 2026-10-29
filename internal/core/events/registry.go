package events

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrProjectionDuplicate is returned when a binding id is already owned by
	// a registered projection. Binding ids are the durable checkpoint keys, so
	// silently replacing one would deliver a different side effect under an old
	// checkpoint.
	ErrProjectionDuplicate = errors.New("projection binding already registered")
	// ErrProjectionNotFound is returned when a registered projection is looked
	// up or removed by an unknown id.
	ErrProjectionNotFound = errors.New("projection binding not found")
)

// ProjectionRegistry owns the effective core projection bindings. It is a
// small composition seam: the server supplies the registry's bindings and the
// Dispatcher remains responsible for durable replay, lease fencing, halt and
// stall behavior. The registry never stores delivery state in memory.
//
// Registration is intentionally projection-only. Listeners and post
// subscribers have different acknowledgement contracts and continue to use
// Dispatcher.Step/RunPost directly.
type ProjectionRegistry struct {
	mu       sync.RWMutex
	bindings map[string]Binding
}

// Registry is kept as a short name for composition roots that register all
// core-owned projections. ProjectionRegistry is the descriptive name used by
// package documentation.
type Registry = ProjectionRegistry

func NewProjectionRegistry() *ProjectionRegistry {
	return &ProjectionRegistry{bindings: make(map[string]Binding)}
}

func NewRegistry() *ProjectionRegistry { return NewProjectionRegistry() }

// Register adds one effective projection binding. A binding is copied before
// it is retained, so later changes to caller-owned filter slices cannot change
// delivery selection while a dispatcher is running.
func (r *ProjectionRegistry) Register(b Binding) error {
	return r.RegisterProjection(b)
}

func (r *ProjectionRegistry) RegisterProjection(b Binding) error {
	if r == nil {
		return errors.New("nil projection registry")
	}
	if b.Class != Projection {
		return fmt.Errorf("projection registration requires class %q", Projection)
	}
	if err := validateBinding(b); err != nil {
		return err
	}
	copy := cloneBinding(b)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bindings == nil {
		r.bindings = make(map[string]Binding)
	}
	if _, exists := r.bindings[copy.ID]; exists {
		return fmt.Errorf("%w: %s", ErrProjectionDuplicate, copy.ID)
	}
	r.bindings[copy.ID] = copy
	return nil
}

func (r *ProjectionRegistry) Remove(id string) error {
	if r == nil {
		return errors.New("nil projection registry")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.bindings[id]; !exists {
		return fmt.Errorf("%w: %s", ErrProjectionNotFound, id)
	}
	delete(r.bindings, id)
	return nil
}

// Get returns a detached binding. The bool distinguishes an absent binding
// from a zero-valued binding and avoids turning a typo into a new checkpoint.
func (r *ProjectionRegistry) Get(id string) (Binding, bool) {
	if r == nil {
		return Binding{}, false
	}
	r.mu.RLock()
	b, ok := r.bindings[id]
	r.mu.RUnlock()
	if !ok {
		return Binding{}, false
	}
	return cloneBinding(b), true
}

// Bindings returns a stable id-sorted snapshot. Stable ordering makes a
// scheduler's observable progress and error reporting deterministic when
// several projections share one active lease.
func (r *ProjectionRegistry) Bindings() []Binding {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	result := make([]Binding, 0, len(r.bindings))
	for _, binding := range r.bindings {
		result = append(result, cloneBinding(binding))
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// Step runs one registered projection through the existing Dispatcher. It is
// the only operation that advances a projection checkpoint; callers cannot
// acknowledge by mutating registry state.
func (r *ProjectionRegistry) Step(ctx context.Context, dispatcher *Dispatcher, id string) (Progress, error) {
	var progress Progress
	if dispatcher == nil {
		return progress, errors.New("projection dispatcher is required")
	}
	binding, ok := r.Get(id)
	if !ok {
		return progress, fmt.Errorf("%w: %s", ErrProjectionNotFound, id)
	}
	return dispatcher.Step(ctx, binding)
}

// StepAll makes one bounded pass over every registered projection. It keeps
// processing later bindings after an earlier failure, while returning the
// first error in deterministic registration order. A halted or stalled
// projection is an operational error and is therefore returned to the caller;
// its durable state remains untouched by this method.
func (r *ProjectionRegistry) StepAll(ctx context.Context, dispatcher *Dispatcher) (map[string]Progress, error) {
	if dispatcher == nil {
		return nil, errors.New("projection dispatcher is required")
	}
	progress := make(map[string]Progress)
	var first error
	for _, binding := range r.Bindings() {
		value, err := dispatcher.Step(ctx, binding)
		progress[binding.ID] = value
		if err != nil && first == nil {
			first = fmt.Errorf("projection %s: %w", binding.ID, err)
		}
	}
	return progress, first
}

func cloneBinding(b Binding) Binding {
	b.Filter.Types = append([]string(nil), b.Filter.Types...)
	b.Filter.Resources = append([]string(nil), b.Filter.Resources...)
	return b
}
