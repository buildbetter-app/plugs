package authzrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Checker is the one host adapter used by both reads and Writer callbacks.
// It never accepts authorization facts from a request; the chain and source
// are fixed at composition time and all source reads use the supplied snapshot.
type Checker struct {
	chain     authz.Chain
	source    FactsSource
	caller    CallerSource
	store     driver.Store
	lease     driver.ActiveLease
	hierarchy bool
	now       func() time.Time
}

var _ port.Authorizer = (*Checker)(nil)
var _ port.EventContextSource = (*Checker)(nil)

func (c *Checker) callerFor(ctx context.Context, p model.PrincipalRef) (authz.Caller, error) {
	if c == nil || c.caller == nil {
		return authz.Caller{}, fmt.Errorf("authorization caller source is unavailable")
	}
	value, err := c.caller(ctx, p)
	if err != nil {
		return authz.Caller{}, err
	}
	if !samePrincipal(value.Principal, p) {
		return authz.Caller{}, fmt.Errorf("authorization caller source changed the authenticated principal")
	}
	return value, nil
}

func (c *Checker) load(ctx context.Context, snapshot Snapshot, expected authz.Caller) (*slice, error) {
	if c == nil || c.source == nil || snapshot == nil {
		return nil, fmt.Errorf("authorization snapshot source is unavailable")
	}
	facts, err := c.source.Load(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if err := facts.Validate(); err != nil {
		return nil, err
	}
	if facts.Namespace != c.chain.Namespace {
		return nil, fmt.Errorf("authorization facts namespace changed")
	}
	state, err := observeACL(ctx, snapshot, facts.Namespace)
	if err != nil {
		return nil, err
	}
	return &slice{
		snapshot: snapshot, facts: facts, expected: expected,
		authorizationRevision: state.Revision, authorizationBound: true,
	}, nil
}

// BoundAuthorization is one authenticated action/kind/method bound to one
// immutable authority snapshot. Callers explicitly Rebind on a fresh snapshot,
// Writer stage or retry, even when revision values or snapshot pointers agree.
// Its finite filter covers the chain only; host-specific read restrictions and
// a final fresh authority fence remain the trusted caller's responsibility.
type BoundAuthorization struct {
	checker    *Checker
	slice      *slice
	request    *authz.PreparedRequest
	kind       authz.ResourceKind
	filterOnce sync.Once
	filter     *authz.PreparedFilter
	filterErr  error
}

// BindAuthorization prepares the real host boundary once for a resource loop.
// Every call reacquires the authenticated caller, owned facts, tracked ACL and
// a new snapshot-local Preparation. It never accepts a request-supplied Caller.
func (c *Checker) BindAuthorization(ctx context.Context, snapshot Snapshot, principal model.PrincipalRef, action authz.Action, kind authz.ResourceKind, method string) (*BoundAuthorization, error) {
	admission, err := authz.PrepareRequest(authz.ListRequest{Caller: authz.Caller{Principal: principal}, Action: action, ResourceKind: kind, Method: method})
	if err != nil {
		return nil, authzError(err)
	}
	// The caller source receives its own copy. Compare its output with the
	// separately owned admission, so mutating a shared sponsor/caveat map cannot
	// replace the authenticated principal while appearing equal to itself.
	caller, err := c.callerFor(ctx, admission.Request().Caller.Principal)
	if err != nil {
		return nil, authzError(err)
	}
	if !samePrincipal(caller.Principal, admission.Request().Caller.Principal) {
		return nil, authzError(fmt.Errorf("authorization caller source changed the authenticated principal"))
	}
	prepared := admission.WithVerification(caller.VerificationID, caller.CapturedEpoch)
	caller = prepared.Request().Caller
	s, err := c.load(ctx, snapshot, caller)
	if err != nil {
		return nil, &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	// The expected caller is an owned normalized value too. Equal prepared
	// inputs need no second JSON codec pass during context checks. Other input
	// representations retain the existing exact samePrincipal comparison.
	s.expectedNormalized = true
	return &BoundAuthorization{checker: c, slice: s, request: prepared, kind: kind}, nil
}

// Rebind always reloads authority, including a new caller-source verification.
// Immutable parsing from the prior binding never carries observations forward.
func (b *BoundAuthorization) Rebind(ctx context.Context, snapshot Snapshot) (*BoundAuthorization, error) {
	if b == nil || b.request == nil || b.checker == nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	r := b.request.Request()
	return b.checker.BindAuthorization(ctx, snapshot, r.Caller.Principal, r.Action, r.ResourceKind, r.Method)
}

// Check evaluates these resource targets without renormalizing their common
// principal/caveats. Per-resource grants, restrictions and source facts remain
// live reads, and custom components retain fresh observation/error behavior.
func (b *BoundAuthorization) Check(ctx context.Context, refs []model.Ref) ([]bool, error) {
	if b == nil || b.request == nil || b.checker == nil || b.slice == nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	resources := make([]authz.Resource, len(refs))
	for i, ref := range refs {
		resources[i] = authz.Resource{Kind: b.kind, Ref: ref}
	}
	checkSlice := &projectBatchSlice{slice: b.slice}
	decisions, err := b.checker.chain.CheckPrepared(ctx, checkSlice, b.request.Checks(resources))
	if err != nil {
		return nil, authzError(err)
	}
	if len(decisions) != len(refs) {
		return nil, &model.Error{Message: "authorization returned an invalid decision set", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	allowed := make([]bool, len(refs))
	for i, decision := range decisions {
		allowed[i] = decision.Effect == authz.Allow
	}
	return allowed, nil
}

// Filter seals the same immutable finite chain description once per binding.
// ErrCheckOnly and other compile errors keep their existing typed host mapping.
func (b *BoundAuthorization) Filter() (*authz.PreparedFilter, error) {
	if b == nil || b.request == nil || b.checker == nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	b.filterOnce.Do(func() {
		template, err := b.checker.chain.CompilePrepared(b.request)
		if err == nil {
			b.filter, err = template.Prepare()
		}
		b.filterErr = authzError(err)
	})
	return b.filter, b.filterErr
}

func (c *Checker) decision(ctx context.Context, snapshot Snapshot, principal model.PrincipalRef, action authz.Action, kind authz.ResourceKind, ref model.Ref, method string) (bool, error) {
	binding, err := c.BindAuthorization(ctx, snapshot, principal, action, kind, method)
	if err != nil {
		return false, err
	}
	allowed, err := binding.Check(ctx, []model.Ref{ref})
	if err != nil {
		return false, err
	}
	return allowed[0], nil
}

// Authorize implements the Writer boundary. Before and after both check the
// stored target through the fixed chain. After additionally applies the
// adopted existing-bead write adapter, so project/parent/ownership changes
// cannot be smuggled through a permitted update. Replay rechecks the current
// chain only and never trusts a receipt as a grant.
func (c *Checker) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	heartbeat := proposal.Meta.Method == "bead.heartbeat"
	claimMaintenance := heartbeat || proposal.Meta.Method == "bead.release"
	if claimMaintenance {
		// These protected service seams maintain the caller's existing claim.
		// Only the finite fixed component maps to its existing modify predicate;
		// every other policy and the Writer retain the actual method.
		if proposal.Principal.Kind != "agent" {
			return forbidden("public claim maintenance requires an Agent")
		}
	}
	if c != nil && c.hierarchy && !claimMaintenance {
		switch hierarchyRouteFor(proposal) {
		case hierarchyDirect:
			return c.AuthorizeHierarchy(ctx, stage, proposal, reader)
		case hierarchyLifecycle:
			return c.AuthorizeHierarchyLifecycle(ctx, stage, proposal, reader)
		case hierarchyUnsupported:
			return forbidden("unsupported hierarchy operation composition")
		}
	}
	if reader == nil {
		return &model.Error{Message: "authorization snapshot unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	caller, err := c.callerFor(ctx, proposal.Principal)
	if err != nil {
		return authzError(err)
	}
	s, err := c.load(ctx, reader, caller)
	if err != nil {
		return &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	requests := make([]authz.CheckRequest, 0, len(proposal.Changes)+len(proposal.Expect)+1)
	for _, change := range proposal.Changes {
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: change.Resource.Ref}, Method: proposal.Meta.Method})
	}
	changeCount := len(proposal.Changes)
	for _, expect := range proposal.Expect {
		if err := authz.CanonicalRef(expect.Ref); err != nil {
			return forbidden("invalid expectation target")
		}
		// Expectations disclose the pre-image only as part of an already
		// admitted write. Use the write permit, not a user read permit: CP7
		// agents may write their sponsor's project while user reads are a
		// separate surface.
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: expect.Ref}, Method: proposal.Meta.Method})
	}
	// A permit for the enclosing transaction is not a close permit. Keep
	// both requests so either an outer tx.apply forbid or a per-op close
	// refusal remains authoritative, including receipt replay.
	for _, change := range proposal.Changes {
		if change.Op.Kind == model.OpClose {
			if proposal.Meta.Method != "tx.apply" {
				return forbidden("close operation requires tx.apply")
			}
			requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: change.Resource.Ref}, Method: "bead.close"})
		}
	}
	if changeCount == 0 {
		if proposal.Meta.Method != "bead.claimNext" {
			return forbidden("authorization requires a concrete resource")
		}
		project, ok := s.projectByName(proposal.Meta.Project)
		if !ok {
			return forbidden("claimNext project is not authorized")
		}
		// An empty claimNext is a durable null receipt, not a graph write. It
		// still needs the requested project permit so an unauthorized caller
		// cannot mint a replayable receipt for a different queue.
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Project, Ref: project}, Method: ""})
	}
	chain := c.chain
	var checkSlice authz.Slice = s
	if claimMaintenance {
		chain = c.claimMaintenanceChain()
		checkSlice = &claimMaintenanceSlice{Slice: s, recorded: make(map[authz.Dependency]struct{})}
	}
	decisions, err := chain.Check(ctx, checkSlice, requests)
	if err != nil {
		return authzError(err)
	}
	if len(decisions) != len(requests) {
		return &model.Error{Message: "authorization returned an invalid decision set", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	for i, decision := range decisions {
		if decision.Effect != authz.Allow {
			return forbidden(strings.Join(decision.Reasons, ","))
		}
		if stage == port.AuthorizeAfter && i < changeCount && !heartbeat {
			if err := c.validateExistingWrite(ctx, reader, proposal, proposal.Changes[i], caller); err != nil {
				return err
			}
		}
		if heartbeat && i < changeCount {
			// A heartbeat has no proposed graph image. Validate the actual
			// unchanged stored image after membership admission, including on
			// replay. Check the current held lease before the Writer can return
			// a previously published receipt for a now-invalid claim.
			change := proposal.Changes[i]
			rows, err := reader.Get(ctx, []model.Ref{change.Resource.Ref})
			if err != nil {
				return &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
			}
			if len(rows) != 1 {
				return forbidden("heartbeat requires an existing authorized bead")
			}
			change.Before, change.After = rows[0].Body, rows[0].Body
			if err := c.validateExistingWrite(ctx, reader, proposal, change, caller); err != nil {
				return err
			}
			if err := c.validateHeartbeatClaim(ctx, proposal.Principal, change.Resource.Ref, rows[0]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Checker) validateExistingWrite(ctx context.Context, reader Snapshot, proposal port.ProposedChange, change port.Change, caller authz.Caller) error {
	if change.Before == nil || change.After == nil {
		return forbidden("fixed authorization supports existing bead writes only")
	}
	rows, err := reader.Get(ctx, []model.Ref{change.Resource.Ref})
	if err != nil {
		return &model.Error{Message: "authorization source unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if len(rows) != 1 {
		return forbidden("bead disappeared during authorization")
	}
	actual, err := recordImage(rows[0], rows[0].Body)
	if err != nil {
		return forbidden("invalid stored bead image")
	}
	before, err := recordImage(rows[0], change.Before)
	if err != nil {
		return forbidden("invalid stored bead image")
	}
	if !reflect.DeepEqual(actual.Body, before.Body) {
		return forbidden("stored bead image changed during authorization")
	}
	after, err := recordImage(rows[0], change.After)
	if err != nil {
		return forbidden("invalid proposed bead image")
	}
	method := proposal.Meta.Method
	if method == "bead.heartbeat" || method == "bead.release" {
		// The chain already evaluated the actual protected method. This
		// existing-image guard checks only immutable permission assignment;
		// holder, expiry and quiet publication remain Writer responsibilities.
		method = "bead.update"
	}
	if method == "tx.apply" && change.Op.Kind == model.OpClose {
		// Policy already checked both tx.apply and bead.close. This existing
		// image adapter only checks immutable permission-bearing assignment.
		method = "bead.update"
	}
	if err := authz.ValidateBeadWrite(method, before, after, proposal.Principal, caller, []authz.Action{authz.Modify}); err != nil {
		return forbidden(err.Error())
	}
	return nil
}

func recordImage(row driver.Record, raw []byte) (model.Record, error) {
	var body map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil {
		return model.Record{}, err
	}
	return model.Record{Ref: row.Ref, Kind: row.Kind, Revision: row.Revision, Body: body}, nil
}

// EventContext checks the same after-image authorization before publishing
// provenance. It therefore supplies a real chain marker to authorized notices
// rather than accepting a caller or fixture-provided grant annotation.
func (c *Checker) EventContext(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader) (port.EventContext, error) {
	if err := c.Authorize(ctx, port.AuthorizeAfter, proposal, reader); err != nil {
		return port.EventContext{}, err
	}
	return port.EventContext{Cause: model.EventCause{Root: proposal.RequestID, Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{"authz/fixed"}}, nil
}

func (c *Checker) readDecision(ctx context.Context, snapshot Snapshot, p model.PrincipalRef, method string, ref model.Ref) (bool, error) {
	action, kind := methodAction(method)
	if method == "history.query" {
		// History is visible only under the current bead read permit. The
		// resource is still the historical bead, never a caller-selected
		// authority or project scope.
		kind = authz.Bead
		method = "bead.get"
	}
	if action != authz.Read {
		return false, fmt.Errorf("method %s is not a read", method)
	}
	return c.decision(ctx, snapshot, p, action, kind, ref, method)
}

func (c *Checker) modifyDecision(ctx context.Context, snapshot Snapshot, p model.PrincipalRef, method string, ref model.Ref) (bool, error) {
	return c.decision(ctx, snapshot, p, authz.Modify, authz.Bead, ref, method)
}

// Visible keeps discovery from advertising methods that the fixed component
// can never permit. Resource-level authorization still happens in the service
// and Writer boundaries, so this is only a non-leaking method inventory.
func (c *Checker) Visible(_ context.Context, p model.PrincipalRef, d wire.MethodDesc) (bool, error) {
	if d.Permission == "none" || d.Name == "server.hello" || strings.HasPrefix(d.Name, "rpc.") {
		return true, nil
	}
	if d.Permission == "read" {
		return p.Kind == "user", nil
	}
	if d.Permission == "write" {
		switch d.Name {
		case "bead.claim", "bead.claimNext", "bead.update", "bead.heartbeat", "bead.release":
			return p.Kind == "agent" && p.Sponsor != nil && p.Sponsor.Kind == "user", nil
		case "x.workflow.invoke":
			return p.Kind == "user" || p.Kind == "agent" && p.Sponsor != nil && p.Sponsor.Kind == "user", nil
		default:
			return false, nil
		}
	}
	return false, nil
}
