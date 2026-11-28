package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const responsePollInterval = 10 * time.Millisecond

// resolvePublication only reads a fresh durable receipt. An absent or
// mismatched receipt cannot justify another attempt in this invocation.
func (w *Writer) resolvePublication(ctx context.Context, principal string, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, hash, attemptID string, unknown driver.ErrIndeterminate) (port.WriteOutcome, bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	started := time.Now()
	reader, err := w.reader(ctx)
	w.observePhaseSince(ctx, PhaseAdmission, started, err)
	if err != nil {
		return port.WriteOutcome{}, false, unknown
	}
	defer reader.Close()
	started = time.Now()
	entry, err := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
	if err != nil || entry == nil || entry.RequestHash != hash {
		w.observePhaseSince(ctx, PhaseReplay, started, unknown)
		return port.WriteOutcome{}, false, unknown
	}
	// Only this invocation's token proves it published. Its existing write
	// authorization covers returning its own result, even after a lost ack.
	if entry.AttemptID == attemptID {
		if entry.ResponsePending {
			err := responseUnavailable(meta, entry.Commit.Revision, true)
			w.observePhaseSince(ctx, PhaseReplay, started, err)
			return port.WriteOutcome{}, true, err
		}
		commit, err := clone(entry.Commit)
		w.observePhaseSince(ctx, PhaseReplay, started, err)
		if err != nil {
			return port.WriteOutcome{}, true, responseUnavailable(meta, entry.Commit.Revision, false)
		}
		return port.WriteOutcome{Commit: commit}, true, nil
	}

	// A concurrent winner's receipt is a separate disclosure. If after the
	// attempted COMMIT that result cannot be returned, keep the uncertainty.
	commit, err := w.replayEntry(ctx, reader, entry, principal, by, cs, meta, hash)
	w.observePhaseSince(ctx, PhaseReplay, started, err)
	if err != nil {
		return port.WriteOutcome{}, false, unknown
	}
	return port.WriteOutcome{Commit: commit, Replayed: true}, false, nil
}

func replayOutcomeError(err error) error {
	// Preserve current access refusals. Write guards cannot deny an already
	// committed change when the caller is requesting its durable response.
	var typed *model.Error
	if errors.As(err, &typed) {
		switch typed.Data.Code {
		case "ontology_guard_failed", "write_conflict", "precondition_failed", "ontology_plan_stale", "stale_snapshot":
		default:
			return err
		}
	} else if !errors.Is(err, errStale) && !errors.Is(err, driver.ErrConflict) && !errors.Is(err, driver.ErrCheckpointConflict) {
		return err
	}
	return &model.Error{Data: model.ErrorData{
		Code: "busy", Retryable: true, Hint: "Retry the same request and idempotency key to read its stored response.",
	}}
}

type responsePauseHookKey struct{}

func withResponsePauseHook(ctx context.Context, hook func()) context.Context {
	return context.WithValue(ctx, responsePauseHookKey{}, hook)
}

func responseUnavailable(meta model.WriteMeta, revision model.Revision, pending bool) error {
	return &model.Error{Message: "write committed; durable response is not yet available", Data: model.ErrorData{
		Code: "busy", Retryable: true,
		Hint:    "Retry the same request and idempotency key; the graph write is already committed.",
		Details: map[string]any{"committedRevision": revision, "idempotencyKey": meta.IdempotencyKey, "responsePending": pending},
	}}
}

func responsePause(ctx context.Context) error {
	if hook, ok := ctx.Value(responsePauseHookKey{}).(func()); ok {
		hook()
	}
	timer := time.NewTimer(responsePollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// waitResponse never prepares a write or runs a subscriber. Receipt state is
// immutable within each reader, so every poll owns a fresh, promptly closed
// snapshot, including when the graph head did not change during finalization.
func (w *Writer) waitResponse(ctx context.Context, principal string, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, hash string, revision model.Revision) (model.Commit, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		if err := responsePause(ctx); err != nil {
			return model.Commit{}, responseUnavailable(meta, revision, true)
		}
		reader, err := w.reader(ctx)
		if err != nil {
			return model.Commit{}, responseUnavailable(meta, revision, true)
		}
		entry, err := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
		if err != nil || entry == nil || entry.Commit.Revision.String() != revision.String() {
			_ = reader.Close()
			return model.Commit{}, responseUnavailable(meta, revision, true)
		}
		if entry.RequestHash != hash {
			_ = reader.Close()
			return model.Commit{}, failure("idempotency_key_reused", "idempotency key was used with different parameters")
		}
		if entry.ResponsePending {
			_ = reader.Close()
			continue
		}
		result, err := w.replayReceipt(ctx, reader, entry, by, cs, meta)
		_ = reader.Close()
		return result, err
	}
}

func (w *Writer) loadResponse(ctx context.Context, principal string, meta model.WriteMeta, hash string, revision model.Revision) (*driver.IdempotencyRecord, error) {
	reader, err := w.reader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	entry, err := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
	if err != nil || entry == nil {
		return nil, responseUnavailable(meta, revision, true)
	}
	if entry.RequestHash != hash || entry.Commit.Revision.String() != revision.String() {
		return nil, responseUnavailable(meta, revision, true)
	}
	return entry, nil
}

// Even a response with no post phase comes from its published receipt. Dry runs
// are the sole successful path without a receipt and bypass this function.
func (w *Writer) durableResponse(ctx context.Context, principal string, meta model.WriteMeta, hash string, revision model.Revision) (model.Commit, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	entry, err := w.loadResponse(ctx, principal, meta, hash, revision)
	if err != nil || entry.ResponsePending {
		return model.Commit{}, responseUnavailable(meta, revision, err == nil && entry.ResponsePending)
	}
	return clone(entry.Commit)
}

// Retry only the already known post outcome. A failed or lost CAS response
// cannot justify another graph write, another callback or unpersisted success.
// Exhaustion leaves the durable pending receipt recoverable by an active holder.
func (w *Writer) finalizeResponse(ctx context.Context, principal string, meta model.WriteMeta, hash string, revision model.Revision, warnings []model.Warning) (model.Commit, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	finalizer := w.store.(driver.ResponseFinalizer)
	for attempt := 0; attempt < 4; attempt++ {
		finalErr := finalizer.FinalizeResponse(ctx, w.lease, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey, revision, warnings)
		// A finalizer can commit its CAS and lose the acknowledgment. Only a
		// fresh durable lookup determines whether a success may be returned.
		if entry, err := w.loadResponse(ctx, principal, meta, hash, revision); err == nil && !entry.ResponsePending {
			return clone(entry.Commit)
		}
		code, _, mapped := driver.ErrorMapping(finalErr)
		if ctx.Err() != nil || mapped && code == "lease_not_held" || attempt == 3 {
			break
		}
		if err := responsePause(ctx); err != nil {
			break
		}
	}
	return model.Commit{}, responseUnavailable(meta, revision, true)
}
