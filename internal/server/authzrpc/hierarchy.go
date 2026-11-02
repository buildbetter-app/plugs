package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
)

type hierarchyRoute uint8

const (
	hierarchyOrdinary hierarchyRoute = iota
	hierarchyDirect
	hierarchyLifecycle
	hierarchyUnsupported
)

func hierarchyMethod(method string) bool {
	return method == "bead.create" || method == "bead.reparent" || method == "tx.apply"
}

func hierarchyRouteFor(proposal port.ProposedChange) hierarchyRoute {
	switch proposal.Meta.Method {
	case "bead.create", "bead.reparent":
		return hierarchyDirect
	case "tx.apply":
		var hierarchy, close, other bool
		for _, change := range proposal.Changes {
			switch change.Op.Kind {
			case model.OpCreate, model.OpReparent:
				hierarchy = true
			case model.OpClose:
				close = true
			case model.OpPatch, model.OpComment, model.OpLink, model.OpUnlink, model.OpDelete, model.OpSetMeta:
				other = true
			default:
				return hierarchyUnsupported
			}
		}
		if hierarchy && other {
			return hierarchyUnsupported
		}
		if hierarchy && close {
			return hierarchyLifecycle
		}
		if hierarchy {
			return hierarchyDirect
		}
		return hierarchyOrdinary
	default:
		return hierarchyOrdinary
	}
}

// AuthorizeHierarchy is the opt-in EP2 Writer seam. The ordered operation and
// final images are trusted Writer output, not caller-supplied grants. All
// source records, misses, registry bindings and ACL dependencies are read from
// reader, which MUST be the Writer's tracked snapshot. The same seam runs
// before/after callbacks and against current facts on receipt replay.
//
// This adapter authorizes only create/reparent batches. The ordinary #18
// adapter remains unchanged. Ontology, child guards, cycles, alias allocation,
// closure bounds and atomic publication remain the hierarchy Writer's work.
func (c *Checker) AuthorizeHierarchy(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	return c.authorizeHierarchy(ctx, stage, proposal, reader, false)
}

// AuthorizeHierarchyLifecycle composes the existing hierarchy create/reparent
// adapter with the typed close slot. It keeps hierarchy parent/project checks
// and adds an independent bead.close decision for each close operation.
func (c *Checker) AuthorizeHierarchyLifecycle(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	return c.authorizeHierarchy(ctx, stage, proposal, reader, true)
}

func (c *Checker) authorizeHierarchy(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader, allowClose bool) error {
	if reader == nil {
		return hierarchyUnavailable()
	}
	if stage != port.AuthorizeBefore && stage != port.AuthorizeAfter && stage != port.AuthorizeReplay {
		return forbidden("unsupported hierarchy authorization stage")
	}
	if !hierarchyMethod(proposal.Meta.Method) || len(proposal.Changes) == 0 || len(proposal.Changes) > 1000 {
		return forbidden("unsupported hierarchy write")
	}
	if allowClose && proposal.Meta.Method != "tx.apply" {
		return forbidden("hierarchy lifecycle requires tx.apply")
	}
	if proposal.Meta.Method != "tx.apply" && (len(proposal.Changes) != 1 ||
		(proposal.Meta.Method == "bead.create" && proposal.Changes[0].Op.Kind != model.OpCreate) ||
		(proposal.Meta.Method == "bead.reparent" && proposal.Changes[0].Op.Kind != model.OpReparent)) {
		return forbidden("hierarchy method and operation differ")
	}
	caller, err := c.callerFor(ctx, proposal.Principal)
	if err != nil {
		return authzError(err)
	}
	base, err := c.load(ctx, reader, caller)
	if err != nil {
		return hierarchyUnavailable()
	}
	s := &hierarchySlice{slice: base, staged: map[string]model.Record{}}
	var batch *hierarchyCheckBatch
	if c.canBatchHierarchyChecks() {
		batch = &hierarchyCheckBatch{caller: caller}
		s.observed = map[authz.Dependency]bool{}
	}
	aliases := map[string]model.Ref{}
	stored := map[string]*model.Record{}
	final := map[string]model.Record{}
	states := map[string]model.Record{}
	// Expectation-only references are part of the disclosed pre-image even
	// when they are absent from Changes. Check them through the same write
	// permit before hierarchy processing can release any stale refusal.
	if err := c.authorizeExpectations(ctx, s, caller, proposal); err != nil {
		return err
	}

	// Fetch through the tracked reader even for creates: absence is a guarded
	// fact. A staged mapping is introduced only after that create is allowed.
	load := func(ref model.Ref) (*model.Record, error) {
		if value, ok := stored[ref.ID]; ok {
			return value, nil
		}
		value, err := hierarchySource(ctx, reader, ref)
		if err == nil {
			stored[ref.ID] = value
		}
		return value, err
	}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" || hierarchyRef(change.Resource.Ref) != nil {
			return forbidden("invalid hierarchy target")
		}
		if _, err := json.Marshal(change.Op); err != nil {
			return forbidden("invalid hierarchy operation")
		}
		if change.Op.Kind != model.OpCreate && change.Op.Kind != model.OpReparent && (!allowClose || change.Op.Kind != model.OpClose) {
			return forbidden("unmapped hierarchy operation")
		}
		ref := change.Resource.Ref
		source, err := load(ref)
		if err != nil {
			return err
		}
		if stage == port.AuthorizeReplay {
			if source == nil {
				return forbidden("hierarchy write not authorized")
			}
			if change.Op.Kind == model.OpClose {
				if err := c.hierarchyReplayClose(ctx, s, caller, proposal, change, *source, aliases, batch); err != nil {
					return err
				}
			} else if err := c.hierarchyReplay(ctx, s, caller, proposal, change, *source, aliases, load, batch); err != nil {
				return err
			}
			continue
		}
		if change.Op.Kind == model.OpClose {
			if err := c.authorizeHierarchyClose(ctx, s, caller, proposal, change, source, states, load, batch); err != nil {
				return err
			}
			continue
		}
		// Writer detaches proposals through JSON: a nil RawMessage becomes
		// the explicit JSON null image. Both encode the same tracked miss;
		// neither may disguise an existing source record.
		beforeImage := bytes.TrimSpace(change.Before)
		hasBefore := len(beforeImage) != 0 && !bytes.Equal(beforeImage, []byte("null"))
		if (source != nil) != hasBefore {
			return forbidden("hierarchy source mismatch")
		}
		if source != nil {
			before, err := hierarchyRaw(ref, source.Revision, change.Before)
			if err != nil || !reflect.DeepEqual(before.Body, source.Body) {
				return forbidden("hierarchy source mismatch")
			}
		}
		after, err := hierarchyRaw(ref, model.Natural64(0), change.After)
		if err != nil {
			return forbidden("invalid hierarchy final image")
		}
		if previous, ok := final[ref.ID]; ok && !reflect.DeepEqual(previous.Body, after.Body) {
			return forbidden("inconsistent hierarchy final images")
		}
		final[ref.ID] = after
		parent, err := hierarchyParent(change.Op, aliases)
		if err != nil {
			return err
		}
		staged := withHierarchyParent(after, parent)
		var before *model.Record
		switch change.Op.Kind {
		case model.OpCreate:
			if source != nil {
				return forbidden("hierarchy create source exists")
			}
			if _, exists := states[ref.ID]; exists {
				return forbidden("duplicate hierarchy create")
			}
			if !hierarchyCreateMatches(change.Op.Create, proposal.Meta, staged) {
				return forbidden("hierarchy create assignment mismatch")
			}
		case model.OpReparent:
			target, err := hierarchyResolve(change.Op.Reparent.Target, aliases)
			if err != nil || target.ID != ref.ID {
				return forbidden("hierarchy target mismatch")
			}
			value, ok := states[ref.ID]
			if !ok {
				if source == nil {
					return forbidden("hierarchy write not authorized")
				}
				value = *source
			}
			before = &value
			if change.Op.Reparent.Project != "" && change.Op.Reparent.Project != value.Body["project"] {
				return forbidden("hierarchy project moves are unsupported")
			}
		}
		target, err := authz.ValidateHierarchyWrite(change.Op.Kind, before, staged, proposal.Principal, caller)
		if err != nil {
			return forbidden("invalid hierarchy write image (" + string(change.Op.Kind) + ")")
		}
		// Only this validated, tracked-missing new child may use a staged
		// project binding. Existing beads always keep their stored assignment.
		if change.Op.Kind == model.OpCreate {
			s.staged[ref.ID] = staged
		}
		if err := c.checkHierarchyRights(ctx, s, caller, target, change.Op.Kind == model.OpCreate, proposal.Meta.Method, load, batch); err != nil {
			return err
		}
		states[ref.ID] = staged
		if change.Op.Kind == model.OpCreate && change.Op.Create.Key != "" {
			if _, exists := aliases[change.Op.Create.Key]; exists {
				return forbidden("duplicate hierarchy alias")
			}
			aliases[change.Op.Create.Key] = ref
		}
	}
	if batch != nil {
		if err := c.checkHierarchyRightsBatch(ctx, s, batch); err != nil {
			return err
		}
	}
	for id, state := range states {
		actual, err := authz.HierarchyImage(final[id])
		expected, expectedErr := authz.HierarchyImage(state)
		if err != nil || expectedErr != nil || !sameHierarchyParent(actual.Parent, expected.Parent) {
			return forbidden("hierarchy final parent mismatch")
		}
	}
	return nil
}

func (c *Checker) authorizeHierarchyClose(ctx context.Context, s *hierarchySlice, caller authz.Caller, proposal port.ProposedChange, change port.Change, source *model.Record, states map[string]model.Record, load func(model.Ref) (*model.Record, error), batch *hierarchyCheckBatch) error {
	ref := change.Resource.Ref
	before := model.Record{}
	if source != nil {
		before = *source
	} else if staged, ok := states[ref.ID]; ok {
		before = staged
	} else {
		return forbidden("hierarchy close requires a bound source")
	}
	beforeImage, err := hierarchyRaw(ref, before.Revision, change.Before)
	if err != nil || !reflect.DeepEqual(beforeImage.Body, before.Body) {
		return forbidden("hierarchy close source mismatch")
	}
	after, err := hierarchyRaw(ref, model.Natural64(0), change.After)
	if err != nil {
		return forbidden("invalid hierarchy close image")
	}
	if source != nil {
		if err := authz.ValidateBeadWrite("bead.update", beforeImage, after, proposal.Principal, caller, []authz.Action{authz.Modify}); err != nil {
			return forbidden("invalid hierarchy close image")
		}
	} else if !validStagedHierarchyClose(beforeImage, after, proposal.Principal, caller) {
		return forbidden("invalid hierarchy close image")
	}
	target, err := authz.HierarchyImage(after)
	if err != nil {
		return forbidden("invalid hierarchy close image")
	}
	return c.checkHierarchyCloseRights(ctx, s, caller, target, proposal.Meta.Method, load, batch)
}

func validStagedHierarchyClose(before, after model.Record, setter model.PrincipalRef, caller authz.Caller) bool {
	if caller.Principal.Kind != "agent" || !reflect.DeepEqual(setter, caller.Principal) || before.Kind != "bead" || after.Kind != "bead" || before.Ref.ID != after.Ref.ID {
		return false
	}
	if _, err := authz.HierarchyImage(before); err != nil {
		return false
	}
	if _, err := authz.HierarchyImage(after); err != nil {
		return false
	}
	for _, key := range []string{"project", "parent", "owner", "assignee", "type", "labels", "restricted"} {
		old, had := before.Body[key]
		next, has := after.Body[key]
		if had != has || !reflect.DeepEqual(old, next) {
			return false
		}
	}
	return true
}

func (c *Checker) checkHierarchyCloseRights(ctx context.Context, s *hierarchySlice, caller authz.Caller, target authz.HierarchyTarget, method string, load func(model.Ref) (*model.Record, error), batch *hierarchyCheckBatch) error {
	_ = load // close has no parent/project dependency; create already checked those bindings.
	requests := []authz.CheckRequest{
		{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: target.Ref}, Method: method},
		{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: target.Ref}, Method: "bead.close"},
	}
	if batch != nil {
		indices := make([]int, 0, len(requests))
		for _, request := range requests {
			indices = append(indices, batch.add(request))
		}
		batch.groups = append(batch.groups, hierarchyCheckGroup{indices: indices})
		return nil
	}
	return c.checkHierarchyRequests(ctx, s, requests)
}

func (c *Checker) hierarchyReplayClose(ctx context.Context, s *hierarchySlice, caller authz.Caller, proposal port.ProposedChange, change port.Change, source model.Record, aliases map[string]model.Ref, batch *hierarchyCheckBatch) error {
	if change.Op.Close == nil {
		return forbidden("invalid hierarchy close operation")
	}
	if target, err := hierarchyResolve(change.Op.Close.Target, aliases); err != nil || target.ID != source.Ref.ID {
		// Replay aliases are represented by the resolved resource ref in the
		// proposal; an ID target must still agree with the stored result.
		if change.Op.Close.Target.IsID() && change.Op.Close.Target.ID == source.Ref.ID {
			// continue
		} else {
			return forbidden("hierarchy replay close target mismatch")
		}
	}
	if _, err := authz.HierarchyImage(source); err != nil {
		return forbidden("hierarchy replay close source invalid")
	}
	return c.checkHierarchyCloseRights(ctx, s, caller, authz.HierarchyTarget{Ref: source.Ref}, proposal.Meta.Method, nil, batch)
}

func (c *Checker) authorizeExpectations(ctx context.Context, s authz.Slice, caller authz.Caller, proposal port.ProposedChange) error {
	if len(proposal.Expect) == 0 {
		return nil
	}
	requests := make([]authz.CheckRequest, 0, len(proposal.Expect))
	for _, expect := range proposal.Expect {
		if err := authz.CanonicalRef(expect.Ref); err != nil {
			return forbidden("invalid expectation target")
		}
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: expect.Ref}, Method: proposal.Meta.Method})
	}
	chain := c.chain
	chain.Rules = make([]authz.RuleSet, len(c.chain.Rules))
	for i, rules := range c.chain.Rules {
		chain.Rules[i] = rules
		if fixed, ok := rules.(*authzfixed.RuleSet); ok {
			chain.Rules[i] = hierarchyFixedRules{fixed}
		}
	}
	decisions, err := chain.Check(ctx, s, requests)
	if err != nil {
		return authzError(err)
	}
	if len(decisions) != len(requests) {
		return hierarchyUnavailable()
	}
	for _, decision := range decisions {
		if decision.Effect != authz.Allow {
			return forbidden(strings.Join(decision.Reasons, ","))
		}
	}
	return nil
}

// hierarchySlice supplies project assignments only for earlier validated
// creates. The underlying Record calls still observe their persistent misses.
type hierarchySlice struct {
	*slice
	staged   map[string]model.Record
	observed map[authz.Dependency]bool
}

// The finite fixed policy repeatedly records the same dependencies during
// Chain preparation and predicate evaluation. Retain their first successful
// tracked read for this authorization invocation only. The Writer cannot
// publish between these reads; a later Before/After/Replay invocation creates
// a fresh slice. This stores no decisions or grants, and custom components
// retain their original reads through the nil-map fallback.
func (s *hierarchySlice) Record(ctx context.Context, dep authz.Dependency) error {
	if s.observed == nil {
		return s.slice.Record(ctx, dep)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.observed[dep] {
		return nil
	}
	if err := s.slice.Record(ctx, dep); err != nil {
		return err
	}
	s.observed[dep] = true
	return nil
}

func (s *hierarchySlice) ProjectForBead(ctx context.Context, ref model.Ref) (authz.ProjectBinding, error) {
	if value, ok := s.staged[ref.ID]; ok {
		name, _ := value.Body["project"].(string)
		project, found := s.projectByName(name)
		return authz.ProjectBinding{Name: name, Ref: project, Found: found}, nil
	}
	return s.slice.ProjectForBead(ctx, ref)
}

type hierarchyCheckGroup struct {
	indices []int
}

// hierarchyCheckKey identifies the complete fixed-policy input other than the
// caller. AuthorizeHierarchy obtains one caller before constructing a batch
// and every request in that batch carries that same caller. Ref values have
// already passed the canonical hierarchy checks, so ID and branch are the
// immutable resource identity used by the finite policy.
type hierarchyCheckKey struct {
	action authz.Action
	kind   authz.ResourceKind
	id     string
	branch string
	method string
}

type hierarchyCheckBatch struct {
	requests []authz.CheckRequest
	groups   []hierarchyCheckGroup
	caller   authz.Caller
	unique   map[hierarchyCheckKey]int
}

func (b *hierarchyCheckBatch) add(request authz.CheckRequest) int {
	if b.unique == nil {
		b.unique = make(map[hierarchyCheckKey]int)
	}
	key := hierarchyCheckKey{
		action: request.Action,
		kind:   request.Resource.Kind,
		id:     request.Resource.Ref.ID,
		branch: request.Resource.Ref.Branch.String(),
		method: request.Method,
	}
	// A caller is fixed for the entire guarded Writer pass. Keep the
	// comparison here as a fail-closed guard for future callers of this
	// helper: equivalent resources from another principal must be evaluated
	// independently rather than sharing a decision.
	if reflect.DeepEqual(request.Caller, b.caller) {
		if index, ok := b.unique[key]; ok {
			return index
		}
	}
	index := len(b.requests)
	b.requests = append(b.requests, request)
	if reflect.DeepEqual(request.Caller, b.caller) {
		b.unique[key] = index
	}
	return index
}

func (c *Checker) canBatchHierarchyChecks() bool {
	if c == nil || len(c.chain.Links) != 0 || len(c.chain.Rules) == 0 {
		return false
	}
	for _, rules := range c.chain.Rules {
		fixed, ok := rules.(*authzfixed.RuleSet)
		if !ok || fixed == nil {
			return false
		}
	}
	return true
}

func (c *Checker) checkHierarchyRights(ctx context.Context, s *hierarchySlice, caller authz.Caller, target authz.HierarchyTarget, create bool, method string, load func(model.Ref) (*model.Record, error), batch *hierarchyCheckBatch) error {
	requests := []authz.CheckRequest{{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: target.Ref}, Method: method}}
	if create {
		project, found := s.projectByName(target.Project)
		if !found {
			return forbidden("hierarchy write not authorized")
		}
		// Project modify is the fixed create authority; the child check above
		// also preserves ref caveats and per-bead code-link restrictions.
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Project, Ref: project}, Method: method})
	}
	if target.Parent != nil {
		parent, staged := s.staged[target.Parent.ID]
		if !staged {
			stored, err := load(*target.Parent)
			if err != nil {
				return err
			}
			if stored == nil {
				return forbidden("hierarchy write not authorized")
			}
			parent = *stored
		}
		if _, err := authz.HierarchyImage(parent); err != nil {
			return forbidden("hierarchy write not authorized")
		}
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: *target.Parent}, Method: method})
	}
	if batch != nil {
		indices := make([]int, 0, len(requests))
		for _, request := range requests {
			indices = append(indices, batch.add(request))
		}
		batch.groups = append(batch.groups, hierarchyCheckGroup{indices: indices})
		return nil
	}
	return c.checkHierarchyRequests(ctx, s, requests)
}

func (c *Checker) checkHierarchyRightsBatch(ctx context.Context, s *hierarchySlice, batch *hierarchyCheckBatch) error {
	decisions, err := c.checkHierarchyDecisions(ctx, s, batch.requests)
	if err != nil {
		return err
	}
	for _, group := range batch.groups {
		if len(group.indices) == 0 {
			return hierarchyUnavailable()
		}
		for _, index := range group.indices {
			if index < 0 || index >= len(decisions) {
				return hierarchyUnavailable()
			}
			if decisions[index].Effect != authz.Allow {
				return forbidden("hierarchy write not authorized")
			}
		}
	}
	return nil
}

func (c *Checker) checkHierarchyRequests(ctx context.Context, s *hierarchySlice, requests []authz.CheckRequest) error {
	decisions, err := c.checkHierarchyDecisions(ctx, s, requests)
	if err != nil {
		return err
	}
	for _, decision := range decisions {
		if decision.Effect != authz.Allow {
			return forbidden("hierarchy write not authorized")
		}
	}
	return nil
}

func (c *Checker) checkHierarchyDecisions(ctx context.Context, s *hierarchySlice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	// Translate only the adopted finite policy's membership predicates. Code
	// links and every other rule set still receive the ORIGINAL method, so a
	// forbid on reparent/tx.apply cannot be bypassed as a synthetic update.
	chain := c.hierarchyChain()
	decisions, err := chain.Check(ctx, s, requests)
	if err != nil {
		return nil, authzError(err)
	}
	if len(decisions) != len(requests) {
		return nil, hierarchyUnavailable()
	}
	return decisions, nil
}

func (c *Checker) hierarchyChain() authz.Chain {
	chain := c.chain
	chain.Rules = make([]authz.RuleSet, len(c.chain.Rules))
	for i, rules := range c.chain.Rules {
		chain.Rules[i] = rules
		if fixed, ok := rules.(*authzfixed.RuleSet); ok {
			chain.Rules[i] = hierarchyFixedRules{fixed}
		}
	}
	return chain
}

func (c *Checker) hierarchyReplay(ctx context.Context, s *hierarchySlice, caller authz.Caller, proposal port.ProposedChange, change port.Change, source model.Record, aliases map[string]model.Ref, load func(model.Ref) (*model.Record, error), batch *hierarchyCheckBatch) error {
	parent, err := hierarchyParent(change.Op, aliases)
	if err != nil {
		return err
	}
	after := withHierarchyParent(source, parent)
	var before *model.Record
	if change.Op.Kind == model.OpReparent {
		target, err := hierarchyResolve(change.Op.Reparent.Target, aliases)
		if err != nil || target.ID != source.Ref.ID || (change.Op.Reparent.Project != "" && change.Op.Reparent.Project != source.Body["project"]) {
			return forbidden("hierarchy replay assignment mismatch")
		}
		before = &source
	} else if !hierarchyCreateMatches(change.Op.Create, proposal.Meta, after) {
		return forbidden("hierarchy replay assignment mismatch")
	}
	// Current creates may have acquired ordinary comments/leases/metadata since
	// the receipt. Replay validates their source but does not treat that as a
	// fresh creation or reapply the original final image.
	target, err := authz.HierarchyImage(after)
	if err != nil || caller.Principal.Kind != "agent" || caller.Principal.Sponsor == nil || caller.Principal.Sponsor.Kind != "user" {
		return forbidden("hierarchy write not authorized")
	}
	if before != nil {
		if _, err := authz.ValidateHierarchyWrite(model.OpReparent, before, after, proposal.Principal, caller); err != nil {
			return forbidden("hierarchy write not authorized")
		}
	}
	if err := c.checkHierarchyRights(ctx, s, caller, target, change.Op.Kind == model.OpCreate, proposal.Meta.Method, load, batch); err != nil {
		return err
	}
	if change.Op.Kind == model.OpCreate && change.Op.Create.Key != "" {
		if _, exists := aliases[change.Op.Create.Key]; exists {
			return forbidden("duplicate hierarchy replay alias")
		}
		aliases[change.Op.Create.Key] = source.Ref
	}
	return nil
}

type hierarchyFixedRules struct{ fixed *authzfixed.RuleSet }

func (r hierarchyFixedRules) Eval(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	mapped := append([]authz.CheckRequest(nil), requests...)
	for i := range mapped {
		mapped[i].Method = "bead.update"
		if mapped[i].Resource.Kind == authz.Project {
			mapped[i].Method = ""
		}
	}
	return r.fixed.Eval(ctx, s, mapped)
}

func (r hierarchyFixedRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	// This transaction-only adapter must never create a list/discovery permit.
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func hierarchyCreateMatches(op *model.CreateOp, meta model.WriteMeta, after model.Record) bool {
	project := op.Project
	if project == "" {
		project = meta.Project
	}
	return project != "" && after.Body["project"] == project && after.Body["type"] == op.Type
}

func hierarchyParent(op model.Op, aliases map[string]model.Ref) (*model.Ref, error) {
	var input *model.Ref
	if op.Kind == model.OpCreate {
		input = op.Create.Parent
	} else {
		input = op.Reparent.Parent
	}
	if input == nil {
		return nil, nil
	}
	ref, err := hierarchyResolve(*input, aliases)
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

func hierarchyResolve(ref model.Ref, aliases map[string]model.Ref) (model.Ref, error) {
	if ref.Validate() != nil {
		return model.Ref{}, forbidden("invalid hierarchy reference")
	}
	if !ref.IsID() {
		var found bool
		ref, found = aliases[ref.Key]
		if !found {
			return model.Ref{}, forbidden("hierarchy alias must name an earlier create")
		}
	}
	if hierarchyRef(ref) != nil {
		return model.Ref{}, forbidden("invalid hierarchy reference")
	}
	return ref, nil
}

func withHierarchyParent(record model.Record, parent *model.Ref) model.Record {
	body := make(map[string]any, len(record.Body)+1)
	for key, value := range record.Body {
		body[key] = value
	}
	delete(body, "parent")
	if parent != nil {
		body["parent"] = *parent
	}
	record.Body = body
	return record
}

func sameHierarchyParent(a, b *model.Ref) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && a.ID == b.ID)
}

func hierarchyRef(ref model.Ref) error {
	if err := authz.CanonicalRef(ref); err != nil {
		return err
	}
	for _, c := range ref.ID {
		if c == '/' || c == '\x00' {
			return fmt.Errorf("invalid hierarchy record ID")
		}
	}
	return nil
}

func hierarchyUnavailable() error {
	return &model.Error{Message: "hierarchy authorization source unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
}
