package authzrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

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

func (c *Checker) decision(ctx context.Context, snapshot Snapshot, principal model.PrincipalRef, action authz.Action, kind authz.ResourceKind, ref model.Ref, method string) (bool, error) {
	caller, err := c.callerFor(ctx, principal)
	if err != nil {
		return false, authzError(err)
	}
	s, err := c.load(ctx, snapshot, caller)
	if err != nil {
		return false, &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	decisions, err := c.chain.Check(ctx, s, []authz.CheckRequest{{Caller: caller, Action: action, Resource: authz.Resource{Kind: kind, Ref: ref}, Method: method}})
	if err != nil {
		return false, authzError(err)
	}
	if len(decisions) != 1 {
		return false, &model.Error{Message: "authorization returned an invalid decision", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	return decisions[0].Effect == authz.Allow, nil
}

// Authorize implements the Writer boundary. Before and after both check the
// stored target through the fixed chain. After additionally applies the
// adopted existing-bead write adapter, so project/parent/ownership changes
// cannot be smuggled through a permitted update. Replay rechecks the current
// chain only and never trusts a receipt as a grant.
func (c *Checker) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	if c != nil && c.hierarchy {
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
	decisions, err := c.chain.Check(ctx, s, requests)
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
		if stage == port.AuthorizeAfter && i < changeCount {
			if err := c.validateExistingWrite(ctx, reader, proposal, proposal.Changes[i], caller); err != nil {
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
		case "bead.claim", "bead.claimNext", "bead.update":
			return p.Kind == "agent" && p.Sponsor != nil && p.Sponsor.Kind == "user", nil
		default:
			return false, nil
		}
	}
	return false, nil
}
