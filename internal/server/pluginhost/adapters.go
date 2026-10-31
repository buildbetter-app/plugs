package pluginhost

import (
	"context"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// Subscriber supplies independently typed core ports. The server composition
// root attaches these to pipeline bindings; core never imports this package.
type Subscriber struct {
	Binding   Binding
	Mutator   port.Mutator
	Validator port.Validator
	Post      port.PostSubscriber
}

func (h *Host) Subscribers() []Subscriber {
	result := make([]Subscriber, 0, len(h.bindings))
	for _, binding := range h.bindings {
		copy := binding.Binding
		copy.Reads = append([]string(nil), copy.Reads...)
		copy.Owns = append([]string(nil), copy.Owns...)
		subscriber := Subscriber{Binding: copy}
		adapter := &subscriberAdapter{binding}
		switch binding.Phase {
		case pluginapi.PhaseMutate:
			subscriber.Mutator = adapter
		case pluginapi.PhaseValidate:
			subscriber.Validator = adapter
		case pluginapi.PhasePost:
			subscriber.Post = adapter
		}
		result = append(result, subscriber)
	}
	return result
}

// Mutators supplies pipeline bindings with detached configuration. Expand read
// grants to concrete owned paths because pipeline dataflow uses exact paths.
func (h *Host) Mutators() []pipeline.MutatorBinding {
	result := []pipeline.MutatorBinding{}
	for _, s := range h.Subscribers() {
		if s.Mutator == nil {
			continue
		}
		reads := map[string]bool{}
		for _, read := range s.Binding.Reads {
			reads[read] = true
			for _, owner := range h.bindings {
				if owner.Phase != pluginapi.PhaseMutate {
					continue
				}
				for _, path := range owner.Owns {
					if overlap(read, path) {
						reads[path] = true
					}
				}
			}
		}
		paths := make([]string, 0, len(reads))
		for path := range reads {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		result = append(result, pipeline.MutatorBinding{ID: s.Binding.ID, Reads: paths, Owns: s.Binding.Owns, FailOpen: s.Binding.FailOpen, Mutator: s.Mutator})
	}
	return result
}

func (h *Host) Validators() []pipeline.ValidatorBinding {
	result := []pipeline.ValidatorBinding{}
	for _, s := range h.Subscribers() {
		if s.Validator != nil {
			result = append(result, pipeline.ValidatorBinding{ID: s.Binding.ID, FailOpen: s.Binding.FailOpen, Validator: s.Validator})
		}
	}
	return result
}

func (h *Host) Posts() []pipeline.PostBinding {
	result := []pipeline.PostBinding{}
	for _, s := range h.Subscribers() {
		if s.Post != nil {
			result = append(result, pipeline.PostBinding{ID: s.Binding.ID, Subscriber: s.Post})
		}
	}
	return result
}

type subscriberAdapter struct{ binding *bound }

func (a *subscriberAdapter) Mutate(ctx context.Context, c port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return a.mutate(ctx, a.binding.Timeout, c, r)
}

func (a *subscriberAdapter) MutateWithTimeout(ctx context.Context, timeout time.Duration, c port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return a.mutate(ctx, minTimeout(a.binding.Timeout, timeout), c, r)
}

func (a *subscriberAdapter) mutate(ctx context.Context, timeout time.Duration, c port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	original, err := toPublicProposal(c, a.binding.Binding)
	if err != nil || len(original.Changes) == 0 {
		return nil, err
	}
	proposal, err := maskProposal(original, a.binding)
	if err != nil {
		return nil, err
	}
	reader, err := a.reader(ctx, r, c.BaseRevision)
	if err != nil {
		return nil, err
	}
	var patches []pluginapi.Patch
	err = a.binding.pool.run(ctx, pluginapi.PhaseMutate, timeout, reader, func(instance *instance, ctx context.Context) error {
		var err error
		patches, err = instance.registration.Mutators[a.binding.Subscriber].Mutate(ctx, proposal, reader)
		if err != nil {
			return err
		}
		if err = validatePatches(a.binding.Binding, original, patches); err != nil {
			return err
		}
		// A plugin must not retain an output map that core later consumes.
		patches, err = pluginapi.CloneJSON(patches)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := make([]port.Patch, len(patches))
	for j, patch := range patches {
		ref, err := toCoreResource(patch.Resource)
		if err != nil {
			return nil, err
		}
		out[j] = port.Patch{Resource: ref, Paths: patch.Paths}
	}
	return out, nil
}

func (a *subscriberAdapter) Validate(ctx context.Context, c port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	return a.validate(ctx, a.binding.Timeout, c, r)
}

func (a *subscriberAdapter) ValidateWithTimeout(ctx context.Context, timeout time.Duration, c port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	return a.validate(ctx, minTimeout(a.binding.Timeout, timeout), c, r)
}

func (a *subscriberAdapter) validate(ctx context.Context, timeout time.Duration, c port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	original, err := toPublicProposal(c, a.binding.Binding)
	if err != nil {
		return port.Verdict{}, err
	}
	if len(original.Changes) == 0 {
		return port.Verdict{Allow: true}, nil
	}
	proposal, err := maskProposal(original, a.binding)
	if err != nil {
		return port.Verdict{}, err
	}
	reader, err := a.reader(ctx, r, c.BaseRevision)
	if err != nil {
		return port.Verdict{}, err
	}
	var verdict pluginapi.Verdict
	err = a.binding.pool.run(ctx, pluginapi.PhaseValidate, timeout, reader, func(instance *instance, ctx context.Context) error {
		var err error
		verdict, err = instance.registration.Validators[a.binding.Subscriber].Validate(ctx, proposal, reader)
		if err == nil {
			verdict, err = pluginapi.CloneJSON(verdict)
		}
		return err
	})
	if err != nil {
		return port.Verdict{}, err
	}
	out := port.Verdict{Allow: verdict.Allow}
	for _, v := range verdict.Violations {
		out.Violations = append(out.Violations, model.Violation{Guard: v.Guard, Message: v.Message, Details: v.Details})
	}
	return out, nil
}

func (a *subscriberAdapter) After(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
	return a.after(ctx, a.binding.Timeout, c, r)
}

func (a *subscriberAdapter) AfterWithTimeout(ctx context.Context, timeout time.Duration, c port.CommittedChange, r port.SnapshotReader) error {
	return a.after(ctx, minTimeout(a.binding.Timeout, timeout), c, r)
}

func (a *subscriberAdapter) after(ctx context.Context, timeout time.Duration, c port.CommittedChange, r port.SnapshotReader) error {
	proposal, err := toPublicProposal(port.ProposedChange{RequestID: c.RequestID, Principal: c.Principal, BaseRevision: c.Revision, Changes: c.Changes}, a.binding.Binding)
	if err != nil || len(proposal.Changes) == 0 {
		return err
	}
	proposal, err = maskProposal(proposal, a.binding)
	if err != nil {
		return err
	}
	reader, err := a.reader(ctx, r, c.Revision)
	if err != nil {
		return err
	}
	committed := pluginapi.CommittedChange{RequestID: proposal.RequestID, Principal: proposal.Principal, Revision: proposal.BaseRevision, Changes: proposal.Changes}
	return a.binding.pool.run(ctx, pluginapi.PhasePost, timeout, reader, func(instance *instance, ctx context.Context) error {
		return instance.registration.Posts[a.binding.Subscriber].After(ctx, committed, reader)
	})
}

func minTimeout(binding, outer time.Duration) time.Duration {
	if outer <= 0 || outer >= binding {
		return binding
	}
	return outer
}

func (a *subscriberAdapter) reader(ctx context.Context, r port.SnapshotReader, revision model.Revision) (*scopedReader, error) {
	if r == nil {
		return nil, failure(ErrRead, "snapshot reader is required")
	}
	snapshot := r.Snapshot()
	if snapshot.At == nil || snapshot.At.String() != revision.String() {
		return nil, failure(ErrRead, "snapshot does not match callback revision")
	}
	return &scopedReader{ctx: ctx, base: &coreReader{reader: r, revision: revision}, reads: append(append([]string(nil), a.binding.Reads...), a.binding.Owns...), maxPoints: a.binding.MaxPointReads, maxQueries: a.binding.MaxQueries}, nil
}

func toPublicProposal(c port.ProposedChange, binding Binding) (pluginapi.ProposedChange, error) {
	var principal pluginapi.PrincipalRef
	if err := assign(c.Principal, &principal); err != nil {
		return pluginapi.ProposedChange{}, err
	}
	out := pluginapi.ProposedChange{RequestID: c.RequestID, Principal: principal, BaseRevision: pluginapi.Revision(c.BaseRevision), DryRun: c.DryRun, Changes: []pluginapi.Change{}}
	for _, change := range c.Changes {
		if binding.Project != "" || binding.Type != "" {
			body := change.After
			if len(body) == 0 {
				body = change.Before
			}
			var object map[string]any
			if err := decode(body, &object); err != nil {
				return out, err
			}
			if binding.Project != "" && object["project"] != binding.Project || binding.Type != "" && object["type"] != binding.Type {
				continue
			}
		}
		var resource pluginapi.ResourceRef
		if err := assign(change.Resource, &resource); err != nil {
			return out, err
		}
		var op pluginapi.Op
		if err := assign(change.Op, &op); err != nil {
			return out, err
		}
		out.Changes = append(out.Changes, pluginapi.Change{Resource: resource, Op: op, Before: copyImage(change.Before), After: copyImage(change.After), SetBy: change.SetBy})
	}
	return pluginapi.CloneJSON(out)
}

func maskProposal(c pluginapi.ProposedChange, b *bound) (pluginapi.ProposedChange, error) {
	// Keep callback edits separate from the original proposal used to
	// validate returned patch targets, including nested mutable values.
	c, err := pluginapi.CloneJSON(c)
	if err != nil {
		return c, err
	}
	grants := append(append([]string(nil), b.Reads...), b.Owns...)
	for j := range c.Changes {
		change, err := maskChange(c.Changes[j], grants)
		if err != nil {
			return c, err
		}
		c.Changes[j] = change
	}
	return c, nil
}
func toCoreResource(resource pluginapi.ResourceRef) (model.ResourceRef, error) {
	var result model.ResourceRef
	err := assign(resource, &result)
	return result, err
}

var _ port.Mutator = (*subscriberAdapter)(nil)
var _ port.Validator = (*subscriberAdapter)(nil)
var _ port.PostSubscriber = (*subscriberAdapter)(nil)
