package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type callbackBusy interface {
	CallbackBusy() bool
}

func callbackFailure(ctx context.Context, kind, id string, err error) error {
	// The preparation/caller context can expire while a callback is queued or
	// running. Preserve that parent error for admission handling; an independent
	// callback execution deadline still means the subscriber was unavailable.
	if parentErr := ctx.Err(); parentErr != nil {
		return parentErr
	}
	var busy callbackBusy
	if errors.As(err, &busy) && busy.CallbackBusy() {
		return failure("busy", "write pre phases exceeded their deadline")
	}
	return failure("subscriber_unavailable", "pre "+kind+" "+id+" failed")
}

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

// invokeTimedPost keeps the host-owned queue/execution budget while retaining
// the same panic boundary as the legacy callback path. A timed adapter is still
// an untrusted post subscriber: a panic is a post failure, not a process exit.
func invokeTimedPost(ctx context.Context, timeout time.Duration, fn func(context.Context, time.Duration) error) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("subscriber panicked")
		}
	}()
	return fn(ctx, timeout)
}

func (w *Writer) pre(ctx context.Context, p *prepared) error {
	if err := w.authorize(ctx, port.AuthorizeBefore, p.proposal, p.snapshot); err != nil {
		return err
	}
	// A stale expectation can carry a current revision token. Release it only
	// after the pre-image visibility decision, before any callback or ontology
	// code can observe the proposed write.
	if p.refusal != nil {
		return p.refusal
	}
	if err := w.checkWorkflowGuard(ctx, p); err != nil {
		return err
	}
	if w.cfg.ontology == nil {
		return failure("invalid_params", "pipeline requires an ontology hook for bead writes")
	}
	for _, binding := range w.cfg.mutators {
		proposal, err := clone(p.proposal)
		if err != nil {
			return err
		}
		var patches []port.Patch
		if timed, ok := binding.Mutator.(port.TimedMutator); ok {
			patches, err = timed.MutateWithTimeout(ctx, 250*time.Millisecond, proposal, p.snapshot.child())
		} else {
			patches, err = invoke(ctx, 250*time.Millisecond, func(call context.Context) ([]port.Patch, error) {
				return binding.Mutator.Mutate(call, proposal, p.snapshot.child())
			})
		}
		if err != nil {
			if binding.FailOpen {
				p.warnings = append(p.warnings, skipped(binding.ID))
				continue
			}
			return callbackFailure(ctx, "mutator", binding.ID, err)
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
	if p.release != nil {
		// Recovery enters the canonical reopen slot. The public operation remains
		// bead.release, so this translation is trusted pipeline context only.
		proposal.Meta.Method = "bead.reopen"
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
	if p.release != nil {
		proposal.Meta.Method = "bead.reopen"
	}
	if p.closes != nil {
		ordered, ok := w.cfg.ontology.(orderedCloseOntology)
		if !ok {
			return failure("invalid_params", "ontology does not support ordered close validation")
		}
		images, err := clone(p.ordered)
		if err != nil {
			return err
		}
		if err := ordered.ValidateOrdered(ctx, images, proposal, p.snapshot); err != nil {
			return err
		}
	} else if err := w.cfg.ontology.Validate(ctx, proposal, p.snapshot); err != nil {
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
			var v port.Verdict
			var e error
			if timed, ok := binding.Validator.(port.TimedValidator); ok {
				v, e = timed.ValidateWithTimeout(ctx, 500*time.Millisecond, proposal, p.snapshot.child())
			} else {
				v, e = invoke(ctx, 500*time.Millisecond, func(call context.Context) (port.Verdict, error) {
					return binding.Validator.Validate(call, proposal, p.snapshot.child())
				})
			}
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
			return callbackFailure(ctx, "validator", binding.ID, result.err)
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
	if proposal.Meta.Method == "bead.release" && proposal.Principal.Kind == "system" && proposal.Principal.ID == "core" {
		// Only ApplyClaimRelease can admit this method. Core recovery is
		// invariant work, including receipt replay, and never enters the chain.
		return nil
	}
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

func (w *Writer) post(ctx context.Context, p *prepared, commit model.Commit) ([]model.Warning, error) {
	if len(w.cfg.posts) == 0 {
		return nil, nil
	}
	// The caller may disconnect immediately after commit; committed callbacks
	// still receive their bounded window and read precisely the commit revision.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	view := p.snapshot.Snapshot()
	view.At = &commit.Revision
	reader, readErr := w.store.Read(ctx, view)
	if reader != nil {
		defer reader.Close()
	}
	results := make([]chan error, len(w.cfg.posts))
	for i, binding := range w.cfg.posts {
		done := make(chan error, 1)
		results[i] = done
		if readErr != nil {
			done <- readErr
			continue
		}
		change, err := clone(port.CommittedChange{RequestID: p.proposal.RequestID, Principal: p.proposal.Principal, Revision: commit.Revision, Meta: p.proposal.Meta, Changes: p.proposal.Changes})
		if err != nil {
			done <- err
			continue
		}
		go func(binding PostBinding) {
			snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}, bounded: true}
			var err error
			if timed, ok := binding.Subscriber.(port.TimedPostSubscriber); ok {
				err = invokeTimedPost(ctx, 500*time.Millisecond, func(call context.Context, timeout time.Duration) error {
					return timed.AfterWithTimeout(call, timeout, change, snapshot)
				})
			} else {
				_, err = invoke(ctx, 500*time.Millisecond, func(call context.Context) (struct{}, error) {
					return struct{}{}, binding.Subscriber.After(call, change, snapshot)
				})
			}
			done <- err
		}(binding)
	}
	warnings := []model.Warning{}
	failures := make(chan error, len(w.cfg.posts))
	var persisted sync.WaitGroup
	for i, binding := range w.cfg.posts {
		if err := <-results[i]; err != nil {
			warnings = append(warnings, model.Warning{Code: "post_subscriber_failed", Message: "post subscriber failed after commit", Details: map[string]any{"binding": binding.ID, "revision": commit.Revision}})
			persisted.Add(1)
			go func(id string) {
				defer persisted.Done()
				if err := w.deadLetterPost(ctx, view.Incarnation, commit.Revision, id); err != nil {
					failures <- err
				}
			}(binding.ID)
		}
	}
	persisted.Wait()
	select {
	case err := <-failures:
		return warnings, err
	default:
		return warnings, nil
	}
}
