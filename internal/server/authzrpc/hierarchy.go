package authzrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func hierarchyMethod(method string) bool {
	return method == "bead.create" || method == "bead.reparent" || method == "tx.apply"
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
	if reader == nil {
		return hierarchyUnavailable()
	}
	if stage != port.AuthorizeBefore && stage != port.AuthorizeAfter && stage != port.AuthorizeReplay {
		return forbidden("unsupported hierarchy authorization stage")
	}
	if !hierarchyMethod(proposal.Meta.Method) || len(proposal.Changes) == 0 || len(proposal.Changes) > 1000 {
		return forbidden("unsupported hierarchy write")
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
	aliases := map[string]model.Ref{}
	stored := map[string]*model.Record{}
	final := map[string]model.Record{}
	states := map[string]model.Record{}

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
		if change.Op.Kind != model.OpCreate && change.Op.Kind != model.OpReparent {
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
			// A receipt is never a grant. Current child assignment and the
			// original requested destination are checked; an old parent is not.
			if err := c.hierarchyReplay(ctx, s, caller, proposal, change, *source, aliases, load); err != nil {
				return err
			}
			continue
		}
		if (source == nil) != (len(change.Before) == 0) {
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
			return forbidden("invalid hierarchy write image")
		}
		// Only this validated, tracked-missing new child may use a staged
		// project binding. Existing beads always keep their stored assignment.
		if change.Op.Kind == model.OpCreate {
			s.staged[ref.ID] = staged
		}
		if err := c.checkHierarchyRights(ctx, s, caller, target, change.Op.Kind == model.OpCreate, load); err != nil {
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
	for id, state := range states {
		actual, err := authz.HierarchyImage(final[id])
		expected, expectedErr := authz.HierarchyImage(state)
		if err != nil || expectedErr != nil || !sameHierarchyParent(actual.Parent, expected.Parent) {
			return forbidden("hierarchy final parent mismatch")
		}
	}
	return nil
}

// hierarchySlice supplies project assignments only for earlier validated
// creates. The underlying Record calls still observe their persistent misses.
type hierarchySlice struct {
	*slice
	staged map[string]model.Record
}

func (s *hierarchySlice) ProjectForBead(ctx context.Context, ref model.Ref) (authz.ProjectBinding, error) {
	if value, ok := s.staged[ref.ID]; ok {
		name, _ := value.Body["project"].(string)
		project, found := s.projectByName(name)
		return authz.ProjectBinding{Name: name, Ref: project, Found: found}, nil
	}
	return s.slice.ProjectForBead(ctx, ref)
}

func (c *Checker) checkHierarchyRights(ctx context.Context, s *hierarchySlice, caller authz.Caller, target authz.HierarchyTarget, create bool, load func(model.Ref) (*model.Record, error)) error {
	requests := []authz.CheckRequest{{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: target.Ref}, Method: "bead.update"}}
	if create {
		project, found := s.projectByName(target.Project)
		if !found {
			return forbidden("hierarchy write not authorized")
		}
		// Project modify is the fixed create authority; the child check above
		// also preserves ref caveats and per-bead code-link restrictions.
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Project, Ref: project}})
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
		requests = append(requests, authz.CheckRequest{Caller: caller, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: *target.Parent}, Method: "bead.update"})
	}
	decisions, err := c.chain.Check(ctx, s, requests)
	if err != nil {
		return authzError(err)
	}
	if len(decisions) != len(requests) {
		return hierarchyUnavailable()
	}
	for _, decision := range decisions {
		if decision.Effect != authz.Allow {
			return forbidden("hierarchy write not authorized")
		}
	}
	return nil
}

func (c *Checker) hierarchyReplay(ctx context.Context, s *hierarchySlice, caller authz.Caller, proposal port.ProposedChange, change port.Change, source model.Record, aliases map[string]model.Ref, load func(model.Ref) (*model.Record, error)) error {
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
	if err := c.checkHierarchyRights(ctx, s, caller, target, change.Op.Kind == model.OpCreate, load); err != nil {
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
