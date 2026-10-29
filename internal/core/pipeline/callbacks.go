package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type callbackKey struct{}

func decode(data []byte, into any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(into); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}

func clone[T any](v T) (T, error) {
	var result T
	data, err := json.Marshal(v)
	if err != nil {
		return result, err
	}
	err = decode(data, &result)
	return result, err
}

// Native callbacks receive a deadline and a context that forbids synchronous
// reentrant graph writes. An uncooperative native callback cannot hold a lane.
func invoke[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, callbackKey{}, true), timeout)
	defer cancel()
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		var result outcome
		defer func() {
			if recover() != nil {
				result.err = fmt.Errorf("subscriber panicked")
			}
			done <- result
		}()
		result.value, result.err = fn(ctx)
	}()
	select {
	case result := <-done:
		return result.value, result.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (w *Writer) pre(ctx context.Context, p *prepared) error {
	if w.cfg.ontology == nil {
		return failure("invalid_params", "pipeline requires an ontology hook for bead writes")
	}
	if err := w.authorize(ctx, port.AuthorizeBefore, p.proposal, p.snapshot); err != nil {
		return err
	}
	for _, binding := range w.cfg.mutators {
		proposal, err := clone(p.proposal)
		if err != nil {
			return err
		}
		patches, err := invoke(ctx, 250*time.Millisecond, func(call context.Context) ([]port.Patch, error) {
			return binding.Mutator.Mutate(call, proposal, p.snapshot.child())
		})
		if err != nil {
			if binding.FailOpen {
				p.warnings = append(p.warnings, skipped(binding.ID))
				continue
			}
			return failure("subscriber_unavailable", "pre mutator "+binding.ID+" failed")
		}
		patches, err = clone(patches)
		if err != nil {
			return failure("subscriber_unavailable", "pre mutator returned an invalid result")
		}
		if err := p.patches(patches, binding.ID, append([]string{}, binding.Owns...)); err != nil {
			return err
		}
		p.mutators = append(p.mutators, binding.ID)
	}
	proposal, err := clone(p.proposal)
	if err != nil {
		return err
	}
	patches, err := w.cfg.ontology.Normalize(ctx, proposal, p.snapshot)
	if err != nil {
		return err
	}
	if err := p.patches(patches, "request", nil); err != nil {
		return err
	}
	proposal, err = clone(p.proposal)
	if err != nil {
		return err
	}
	if err := w.cfg.ontology.Validate(ctx, proposal, p.snapshot); err != nil {
		return err
	}
	if err := w.authorize(ctx, port.AuthorizeAfter, p.proposal, p.snapshot); err != nil {
		return err
	}
	type validation struct {
		verdict port.Verdict
		err     error
	}
	results := make([]chan validation, len(w.cfg.validators))
	for i, binding := range w.cfg.validators {
		proposal, err := clone(p.proposal)
		if err != nil {
			return err
		}
		done := make(chan validation, 1)
		results[i] = done
		go func(binding ValidatorBinding) {
			v, e := invoke(ctx, 500*time.Millisecond, func(call context.Context) (port.Verdict, error) {
				return binding.Validator.Validate(call, proposal, p.snapshot.child())
			})
			done <- validation{v, e}
		}(binding)
	}
	for i, binding := range w.cfg.validators {
		result := <-results[i]
		if result.err != nil {
			if binding.FailOpen {
				p.warnings = append(p.warnings, skipped(binding.ID))
				continue
			}
			return failure("subscriber_unavailable", "pre validator "+binding.ID+" failed")
		}
		if !result.verdict.Allow {
			return &model.Error{Message: "pre validator rejected the write", Data: model.ErrorData{Code: "ontology_guard_failed", Details: map[string]any{"binding": binding.ID, "violations": result.verdict.Violations}}}
		}
	}
	return ctx.Err()
}

func skipped(id string) model.Warning {
	return model.Warning{Code: "subscriber_skipped", Message: "fail-open pre subscriber skipped", Details: map[string]any{"binding": id}}
}

func (w *Writer) authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader *snapshotReader) error {
	if w.cfg.authorizer == nil {
		return nil
	}
	proposal, err := clone(proposal)
	if err != nil {
		return err
	}
	_, err = invoke(ctx, 100*time.Millisecond, func(call context.Context) (struct{}, error) {
		return struct{}{}, w.cfg.authorizer.Authorize(call, stage, proposal, reader)
	})
	if err == context.DeadlineExceeded {
		return failure("authz_unavailable", "authorization deadline exceeded")
	}
	return err
}

func (w *Writer) post(ctx context.Context, p *prepared, commit model.Commit) []model.Warning {
	if len(w.cfg.posts) == 0 {
		return nil
	}
	// The caller may disconnect immediately after commit; committed callbacks
	// still receive their bounded window and read precisely the commit revision.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	view := p.snapshot.Snapshot()
	view.At = &commit.Revision
	reader, readErr := w.store.Read(ctx, view)
	results := make([]chan error, len(w.cfg.posts))
	for i, binding := range w.cfg.posts {
		done := make(chan error, 1)
		results[i] = done
		if readErr != nil {
			done <- readErr
			continue
		}
		changes, err := clone(p.proposal.Changes)
		if err != nil {
			done <- err
			continue
		}
		change := port.CommittedChange{RequestID: p.proposal.RequestID, Principal: p.proposal.Principal, Revision: commit.Revision, Meta: p.proposal.Meta, Changes: changes}
		go func(binding PostBinding) {
			snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}, bounded: true}
			_, err := invoke(ctx, 500*time.Millisecond, func(call context.Context) (struct{}, error) {
				return struct{}{}, binding.Subscriber.After(call, change, snapshot)
			})
			done <- err
		}(binding)
	}
	warnings := []model.Warning{}
	for i, binding := range w.cfg.posts {
		if err := <-results[i]; err != nil {
			warnings = append(warnings, model.Warning{Code: "post_subscriber_failed", Message: "post subscriber failed after commit", Details: map[string]any{"binding": binding.ID, "revision": commit.Revision}})
		}
	}
	return warnings
}
