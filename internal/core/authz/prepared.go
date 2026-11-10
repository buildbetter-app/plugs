package authz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// PreparedRequest owns immutable JSON-normalized admission inputs. It contains
// no grants, resource decisions or snapshot facts. Request returns an isolated
// copy for components; the original caller and component inputs cannot mutate
// the material used by another check.
type PreparedRequest struct {
	request           ListRequest
	caveats           []map[string]preparedCaveat
	freshObservations bool
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

// WithVerification returns a new immutable binding with the host's receipt and
// epoch. It shares frozen parsing only; it neither verifies those values nor
// carries authority observations. The authenticated principal cannot change.
func (p *PreparedRequest) WithVerification(id string, epoch model.Revision) *PreparedRequest {
	owned := *p
	owned.request.Caller.VerificationID = id
	owned.request.Caller.CapturedEpoch = epoch
	return &owned
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

// PreparedFilter owns a finite chain description and immutable request parsing.
// It contains no authority observations or decisions. It describes only this
// chain; extra host read restrictions require separate trusted composition.
type PreparedFilter struct {
	template   FilterTemplate
	digestOnce sync.Once
	digest     string
	digestErr  error
}

// Prepare seals the entire description once. Public FilterTemplate.Match keeps
// its mutation checks; the opaque form needs no per-resource comparison against
// a caller-owned description. No-plugin admission retains its early behavior.
func (t FilterTemplate) Prepare() (*PreparedFilter, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if t.NoPlugins {
		if scoped(CheckRequest{Caller: t.Request.Caller}) {
			return nil, &Error{Code: "authz_not_installed"}
		}
		t.Request.Caller.Principal = copyPrincipal(t.Request.Caller.Principal)
		t.prepared = nil
	} else {
		p := t.prepared
		if p == nil || !reflect.DeepEqual(t.Request, p.request) {
			var err error
			p, err = PrepareRequest(t.Request)
			if err != nil {
				return nil, err
			}
			// A public, unattached template used fresh observations on every
			// Match before sealing; attaching parsing must not change that.
			p.freshObservations = true
		}
		t.Request = p.Request()
		t.prepared = p
	}
	t.Permit = copyExpr(t.Permit)
	t.Forbid = copyExpr(t.Forbid)
	t.Restrictions = copyExprs(t.Restrictions)
	return &PreparedFilter{template: t}, nil
}

func copyExpr(f FilterExpr) FilterExpr {
	f.Args = copyExprs(f.Args)
	return f
}

func copyExprs(expressions []FilterExpr) []FilterExpr {
	if expressions == nil {
		return nil
	}
	out := make([]FilterExpr, len(expressions))
	for i, f := range expressions {
		out[i] = copyExpr(f)
	}
	return out
}

// Description returns an isolated request and recursive AST. Mutating it cannot
// change this filter, its digest, or another caller's description.
func (f *PreparedFilter) Description() FilterTemplate {
	t := f.template
	t.Request.Caller.Principal = copyPrincipal(t.Request.Caller.Principal)
	t.Permit = copyExpr(t.Permit)
	t.Forbid = copyExpr(t.Forbid)
	t.Restrictions = copyExprs(t.Restrictions)
	return t
}

// Binding is a version-1 SHA256 binding over the full immutable description.
// encoding/json sorts normalized object keys and retains exact json.Number
// lexemes in principal/sponsor caveats. This is a nonsecret policy/request
// identity, not a certificate of current authority or complete host coverage.
// Serialization is lazy so no-plugin Match does not newly decode ignored input.
func (f *PreparedFilter) Binding() (string, error) {
	if f == nil {
		return "", errors.New("prepared filter is nil")
	}
	f.digestOnce.Do(func() {
		data, err := json.Marshal(struct {
			Version int            `json:"version"`
			Filter  FilterTemplate `json:"filter"`
		}{1, f.template})
		if err != nil {
			f.digestErr = err
			return
		}
		digest := sha256.Sum256(append([]byte("plug.authz.prepared-filter/1\x00"), data...))
		f.digest = hex.EncodeToString(digest[:])
	})
	return f.digest, f.digestErr
}

// Match reads resource facts from the supplied current authority snapshot. A
// fresh Slice must supply fresh Preparation; no authority state lives here.
func (f *PreparedFilter) Match(ctx context.Context, s Slice, resource Resource) (bool, error) {
	if f == nil {
		return false, unavailable(errors.New("prepared filter is nil"))
	}
	t := f.template
	if t.NoPlugins {
		return true, nil
	}
	return t.matchPrepared(ctx, s, resource, t.prepared, snapshotPreparation(ctx, s))
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

// Custom component invocations must retain their first-touch reads, including
// when the component delegates to the finite Eval port with the same context.
type freshPreparationKey struct{}

func snapshotPreparation(ctx context.Context, s Slice) *Preparation {
	if fresh, _ := ctx.Value(freshPreparationKey{}).(bool); fresh {
		return nil
	}
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
	if CanonicalRef(resource.Ref) != nil || (resource.Kind == Bead && !validBeadReference(s, resource.Ref)) {
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
// list binding. Construction captures mutable inputs immediately. Preparation
// failures are reported at their original index, preserving standalone Eval's
// first-request error order; Chain checks all indices before reading facts.
type PreparedChecks struct {
	raw      []CheckRequest
	bindings []*PreparedRequest
	errors   []error
	scoped   []bool
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
	err      error
}

func PrepareChecks(requests []CheckRequest) *PreparedChecks {
	b := &PreparedChecks{raw: append([]CheckRequest(nil), requests...), bindings: make([]*PreparedRequest, len(requests)), errors: make([]error, len(requests)), scoped: make([]bool, len(requests))}
	groups := map[preparedBatchKey][]preparedBatchBinding{}
	for i, q := range b.raw {
		b.scoped[i] = scoped(q)
		r := ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method}
		key := preparedBatchKey{q.Caller.VerificationID, q.Caller.CapturedEpoch.String(), q.Action, q.Resource.Kind, q.Method}
		var binding *preparedBatchBinding
		for j := range groups[key] {
			group := &groups[key][j]
			// Verification receipts alone are not equality. Shared native maps
			// compare cheaply, while different principals/sponsors/caveats with
			// the same receipt still require the complete binding to agree.
			if reflect.DeepEqual(r, group.raw) {
				binding = group
				break
			}
		}
		if binding == nil {
			p, err := PrepareRequest(r)
			group := preparedBatchBinding{r, p, err}
			groups[key] = append(groups[key], group)
			binding = &group
		}
		b.bindings[i], b.errors[i] = binding.prepared, binding.err
		b.raw[i].Caller = Caller{} // Retain no caller-owned mutable JSON tree.
	}
	return b
}

// Checks supplies resource targets for this exact immutable action/kind/method
// binding. It does not normalize the caller again or retain any Slice. A target
// of another kind is an indexed binding error, never silently retagged.
func (p *PreparedRequest) Checks(resources []Resource) *PreparedChecks {
	b := &PreparedChecks{raw: make([]CheckRequest, len(resources)), bindings: make([]*PreparedRequest, len(resources)), errors: make([]error, len(resources)), scoped: make([]bool, len(resources))}
	for i, resource := range resources {
		if p == nil {
			b.errors[i] = errors.New("prepared request is nil")
			continue
		}
		b.raw[i] = CheckRequest{Action: p.request.Action, Resource: resource, Method: p.request.Method}
		b.bindings[i] = p
		b.scoped[i] = scoped(CheckRequest{Caller: p.request.Caller})
		if resource.Kind != p.request.ResourceKind {
			b.errors[i] = errors.New("prepared check changed resource kind")
		}
	}
	return b
}

func (b *PreparedChecks) Len() int { return len(b.raw) }

func (b *PreparedChecks) Binding(index int) (*PreparedRequest, error) {
	if index < 0 || index >= len(b.raw) {
		return nil, errors.New("prepared check index out of range")
	}
	return b.bindings[index], b.errors[index]
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
	// Reusable parsing must never retain a prior authority snapshot's state.
	// Generic slices without an immutable request-owned provider revalidate.
	return t.matchPrepared(ctx, s, b.raw[index].Resource, p, snapshotPreparation(ctx, s))
}
