package authz

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// PreparedRequest owns immutable JSON-normalized admission inputs. It contains
// no grants, resource decisions or snapshot facts. Request returns an isolated
// copy for components; the original caller and component inputs cannot mutate
// the material used by another check.
type PreparedRequest struct {
	request ListRequest
	caveats []map[string]preparedCaveat
}

type preparedCaveat struct {
	decoded   bool
	canonical bool
	values    map[string]struct{}
}

func PrepareRequest(r ListRequest) (*PreparedRequest, error) {
	r, err := FreezeList(r)
	if err != nil {
		return nil, err
	}
	p := &PreparedRequest{request: r}
	for principal := &r.Caller.Principal; principal != nil; principal = principal.Sponsor {
		caveats := make(map[string]preparedCaveat, len(principal.Caveats))
		for key, value := range principal.Caveats {
			caveats[key] = prepareCaveat(key, value)
		}
		p.caveats = append(p.caveats, caveats)
	}
	return p, nil
}

func prepareCaveat(key string, value any) preparedCaveat {
	out := preparedCaveat{canonical: true, values: map[string]struct{}{}}
	data, err := json.Marshal(value)
	if err != nil || string(data) == "null" {
		return out
	}
	switch key {
	case "actions":
		var actions []string
		if json.Unmarshal(data, &actions) != nil {
			return out
		}
		for _, action := range actions {
			out.values[action] = struct{}{}
		}
	case "projects", "refs":
		var refs []model.Ref
		if json.Unmarshal(data, &refs) != nil {
			return out
		}
		for _, ref := range refs {
			if CanonicalRef(ref) != nil {
				out.canonical = false
			}
			out.values[ref.ID] = struct{}{}
		}
	default:
		return out
	}
	out.decoded = true
	return out
}

// Request copies the normalized JSON tree without encoding/decoding it again.
func (p *PreparedRequest) Request() ListRequest {
	r := p.request
	r.Caller.Principal = copyPrincipal(r.Caller.Principal)
	return r
}

func copyPrincipal(p model.PrincipalRef) model.PrincipalRef {
	if p.Sponsor != nil {
		sponsor := copyPrincipal(*p.Sponsor)
		p.Sponsor = &sponsor
	}
	if p.Caveats != nil {
		p.Caveats = copyJSON(p.Caveats).(map[string]any)
	}
	return p
}

func copyJSON(value any) any {
	switch value := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, item := range value {
			out[key] = copyJSON(item)
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = copyJSON(item)
		}
		return out
	default:
		// PrincipalRef's strict codec has already reduced values to immutable
		// JSON scalars, including exact json.Number values.
		return value
	}
}

// Filter attaches immutable parsing without changing the exported description.
// Match still validates that description and falls back to its reference path
// if a caller explicitly changes the request binding after compilation.
func (p *PreparedRequest) Filter(t FilterTemplate) (FilterTemplate, error) {
	if !reflect.DeepEqual(t.Request, p.request) {
		return FilterTemplate{}, errors.New("prepared filter changed request binding")
	}
	t.Request = p.Request()
	t.prepared = p
	return t, t.Validate()
}

// PreparedCompiler and PreparedRuleSet are optional finite-component seams.
// Their opaque input exposes only isolated request copies, never mutable access
// to the chain's authenticated binding. Ordinary components retain their ports.
type PreparedCompiler interface {
	CompilePrepared(*PreparedRequest) (FilterTemplate, error)
}

type PreparedRuleSet interface {
	EvalPrepared(context.Context, Slice, *PreparedChecks) ([]Decision, error)
}

// Preparation belongs to one request's immutable authority snapshot. It may
// share common observations and context validation inside that snapshot only.
// A fresh request, Writer retry or authority snapshot MUST own a fresh value.
// Resource facts, grants, restrictions and elapsed context status are not cached.
type Preparation struct {
	mu       sync.Mutex
	observed map[Namespace]bool
	contexts map[preparedContext]bool
}

type preparedContext struct {
	request   *PreparedRequest
	namespace Namespace
}

type preparationSource interface {
	AuthorizationPreparation() *Preparation
}

func snapshotPreparation(s Slice) *Preparation {
	if source, ok := s.(preparationSource); ok {
		return source.AuthorizationPreparation()
	}
	return nil
}

func (p *PreparedRequest) prepare(ctx context.Context, s Slice, n Namespace, resource Resource, state *Preparation) (bool, error) {
	// Keep the reference interpreter's failure order, especially resource
	// validation before any recorded dependency or host context validation.
	if err := ctx.Err(); err != nil {
		return false, unavailable(err)
	}
	if s == nil || !sameNamespace(s.Namespace(), n) {
		return false, unavailable(errors.New("authorization namespace mismatch"))
	}
	if err := n.Validate(); err != nil {
		return false, unavailable(err)
	}
	c := p.request.Caller
	principal := c.Principal
	if !ValidID(c.VerificationID) || c.CapturedEpoch.Validate() != nil || !ValidID(principal.ID) || !validPrincipalKind(principal.Kind) {
		return false, nil
	}
	if principal.Sponsor != nil && (principal.Kind != "agent" || principal.Sponsor.Sponsor != nil || !ValidID(principal.Sponsor.ID) || (principal.Sponsor.Kind != "user" && principal.Sponsor.Kind != "service")) {
		return false, nil
	}
	if resource.Kind != Bead && resource.Kind != Project && resource.Kind != History {
		return false, nil
	}
	if CanonicalRef(resource.Ref) != nil || (resource.Kind == Bead && strings.Contains(resource.Ref.ID, "/")) {
		return false, nil
	}
	if state != nil {
		state.mu.Lock()
		defer state.mu.Unlock()
		if ok, found := state.contexts[preparedContext{p, n}]; found {
			return ok, nil
		}
	}
	if state == nil || !state.observed[n] {
		for _, dep := range []Dependency{ContextDependency(n), ACLDependency(n), {Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"}} {
			if err := s.Record(ctx, dep); err != nil {
				return false, unavailable(err)
			}
		}
		if state != nil {
			if state.observed == nil {
				state.observed = map[Namespace]bool{}
			}
			state.observed[n] = true
		}
	}
	ok, err := s.CheckContext(ctx, c)
	if err != nil {
		return false, unavailable(err)
	}
	if state != nil {
		if state.contexts == nil {
			state.contexts = map[preparedContext]bool{}
		}
		state.contexts[preparedContext{p, n}] = ok
	}
	return ok, nil
}

func (p *PreparedRequest) caveatsAllow(ctx context.Context, s Slice, resource Resource) (bool, error) {
	for _, caveats := range p.caveats {
		for key, caveat := range caveats {
			if !caveat.decoded {
				return false, nil
			}
			target := string(p.request.Action)
			if key != "actions" {
				target = resource.Ref.ID
				if key == "projects" {
					project, found, err := targetProject(ctx, s, resource)
					if err != nil {
						return false, unavailable(err)
					}
					if !found {
						return false, nil
					}
					target = project.ID
				}
			}
			// Even a present-empty project restriction resolves its target first.
			// All Ref elements were validated, including those after a match.
			if !caveat.canonical {
				return false, nil
			}
			if _, found := caveat.values[target]; !found {
				return false, nil
			}
		}
	}
	return true, nil
}

// PreparedChecks keeps heterogeneous batches separate by the entire immutable
// list binding. Construction is lazy so standalone finite Eval retains its
// first-request error order; Chain freezes the complete batch before fact reads.
type PreparedChecks struct {
	raw         []CheckRequest
	bindings    []*PreparedRequest
	groups      map[preparedBatchKey][]preparedBatchBinding
	preparation *Preparation
}

type preparedBatchKey struct {
	verification, epoch string
	action              Action
	kind                ResourceKind
	method              string
}

type preparedBatchBinding struct {
	raw      ListRequest
	prepared *PreparedRequest
}

func PrepareChecks(requests []CheckRequest) *PreparedChecks {
	return &PreparedChecks{raw: append([]CheckRequest(nil), requests...), bindings: make([]*PreparedRequest, len(requests)), groups: map[preparedBatchKey][]preparedBatchBinding{}}
}

func (b *PreparedChecks) Len() int { return len(b.raw) }

func (b *PreparedChecks) Binding(index int) (*PreparedRequest, error) {
	if index < 0 || index >= len(b.raw) {
		return nil, errors.New("prepared check index out of range")
	}
	if p := b.bindings[index]; p != nil {
		return p, nil
	}
	q := b.raw[index]
	r := ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method}
	key := preparedBatchKey{q.Caller.VerificationID, q.Caller.CapturedEpoch.String(), q.Action, q.Resource.Kind, q.Method}
	for _, group := range b.groups[key] {
		// Verification receipts alone are not equality. Shared native maps
		// compare cheaply, while distinct callers with the same receipt still
		// require their full principal/sponsor/caveat binding to agree.
		if reflect.DeepEqual(r, group.raw) {
			b.bindings[index] = group.prepared
			return group.prepared, nil
		}
	}
	p, err := PrepareRequest(r)
	if err != nil {
		return nil, err
	}
	b.groups[key] = append(b.groups[key], preparedBatchBinding{r, p})
	b.bindings[index] = p
	return p, nil
}

func (b *PreparedChecks) state(s Slice) *Preparation {
	if b.preparation == nil {
		b.preparation = snapshotPreparation(s)
		if b.preparation == nil {
			b.preparation = &Preparation{}
		}
	}
	return b.preparation
}

func (b *PreparedChecks) frozen() ([]CheckRequest, error) {
	out := make([]CheckRequest, b.Len())
	for i, q := range b.raw {
		p, err := b.Binding(i)
		if err != nil {
			return nil, err
		}
		q.Caller = p.request.Caller
		out[i] = q
	}
	return out, nil
}

func (b *PreparedChecks) isolated() []CheckRequest {
	out := make([]CheckRequest, b.Len())
	for i, q := range b.raw {
		q.Caller = b.bindings[i].Request().Caller
		out[i] = q
	}
	return out
}

func (b *PreparedChecks) Match(ctx context.Context, s Slice, index int, t FilterTemplate) (bool, error) {
	p, err := b.Binding(index)
	if err != nil {
		return false, err
	}
	if err := t.Validate(); err != nil {
		return false, unavailable(err)
	}
	if !reflect.DeepEqual(t.Request, p.request) {
		return false, unavailable(errors.New("prepared check changed request binding"))
	}
	if t.NoPlugins {
		return t.Match(ctx, s, b.raw[index].Resource)
	}
	return t.matchPrepared(ctx, s, b.raw[index].Resource, p, b.state(s))
}
