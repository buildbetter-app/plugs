package pipeline

import (
	"context"
	"errors"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type workflowGuardKey struct{}

// WithWorkflowGuard installs a trusted provider read for this ordinary write.
// The Writer invokes it after receipt replay and pre-image authorization, then
// checks its checkpoint generation in the same transaction as publication.
func WithWorkflowGuard(ctx context.Context, guard func(context.Context, port.ProposedChange) (driver.CheckpointExpectation, error)) context.Context {
	return context.WithValue(ctx, workflowGuardKey{}, guard)
}

func (w *Writer) checkWorkflowGuard(ctx context.Context, p *prepared) error {
	guard, _ := ctx.Value(workflowGuardKey{}).(func(context.Context, port.ProposedChange) (driver.CheckpointExpectation, error))
	if guard == nil {
		return nil
	}
	expectation, err := guard(ctx, p.proposal)
	if errors.Is(err, driver.ErrCheckpointConflict) {
		return errStale
	}
	if err != nil || expectation.Key == "" {
		return err
	}
	if err := driver.ValidateCheckpointKey(expectation.Key); err != nil {
		return err
	}
	meta, err := w.store.Meta(ctx)
	if err != nil {
		return err
	}
	if meta.StoreID != expectation.StoreID || meta.Incarnation != expectation.Incarnation || expectation.Incarnation != p.snapshot.Snapshot().Incarnation {
		return driver.ErrConflict
	}
	if p.checkpointReads == nil {
		p.checkpointReads = make(map[string]uint64)
	}
	if prior, ok := p.checkpointReads[expectation.Key]; ok && prior != expectation.Expected {
		return errStale
	}
	p.checkpointReads[expectation.Key] = expectation.Expected
	return nil
}

type checkpointMutationKey struct{}

// ApplyWithCheckpoint is a trusted service entry, never a public ChangeSet
// operation. The namespace image and ordinary writer proposal commit together.
func (w *Writer) ApplyWithCheckpoint(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, mutation driver.CheckpointMutation) (model.Commit, error) {
	if meta.DryRun {
		return model.Commit{}, failure("invalid_params", "workflow moves do not support dryRun")
	}
	if err := driver.ValidateCheckpointKey(mutation.Key); err != nil {
		return model.Commit{}, failure("invalid_params", err.Error())
	}
	current, err := w.store.Meta(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	if current.StoreID != mutation.StoreID || current.Incarnation != mutation.Incarnation {
		return model.Commit{}, driver.ErrConflict
	}
	mutation.Payload = append([]byte(nil), mutation.Payload...)
	return w.Apply(context.WithValue(ctx, checkpointMutationKey{}, mutation), by, cs, meta)
}

type workflowCommentsKey struct{}

// ApplyWorkflowMove preserves original ephemeral comment authors and times.
// Its complete post-image enters ordinary authorization before publication.
func (w *Writer) ApplyWorkflowMove(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, mutation driver.CheckpointMutation, comments map[string][]Comment) (model.Commit, error) {
	if meta.Method != "x.workflow.run.promote" && meta.Method != "x.workflow.run.demote" {
		return model.Commit{}, failure("invalid_params", "invalid workflow move method")
	}
	detached, err := clone(comments)
	if err != nil {
		return model.Commit{}, err
	}
	return w.ApplyWithCheckpoint(context.WithValue(ctx, workflowCommentsKey{}, detached), by, cs, meta, mutation)
}
func (p *prepared) preserveWorkflowComments(ctx context.Context) error {
	comments, _ := ctx.Value(workflowCommentsKey{}).(map[string][]Comment)
	if len(comments) == 0 {
		return nil
	}
	for id, values := range comments {
		pending := p.writes[id]
		if pending == nil || pending.deleted {
			return failure("invalid_params", "workflow comments require a live proposed target")
		}
		for _, comment := range values {
			if err := comment.Validate(); err != nil {
				return err
			}
		}
		pending.doc.Comments = append(pending.doc.Comments, values...)
		pending.setBy["comments"] = "workflow.move"
	}
	return p.refresh()
}
