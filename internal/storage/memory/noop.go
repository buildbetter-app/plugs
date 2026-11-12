package memory

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// CommitNoop publishes an optional receipt and staged unversioned checkpoints.
// The complete guard locks and Head still apply; graph mutation is forbidden.
func (t *transaction) CommitNoop(ctx context.Context, header driver.ChangesetHeader) (model.Commit, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return model.Commit{}, err
	}
	if !t.hasHead || s.head != t {
		return model.Commit{}, fmt.Errorf("memory: no-op commit requires Head")
	}
	if err := driver.ValidateHeaderLocks(header.LockKeys, t.acquired); err != nil {
		return model.Commit{}, err
	}
	if err := branchZero(header.Branch); err != nil {
		return model.Commit{}, err
	}
	if header.Incarnation != s.state.meta.Incarnation {
		return model.Commit{}, driver.ErrIncarnationChanged{From: header.Incarnation, To: s.state.meta.Incarnation}
	}
	principal, err := driver.PrincipalKey(header.Principal)
	if err != nil {
		return model.Commit{}, err
	}
	if err := header.Kind.Validate(); err != nil {
		return model.Commit{}, err
	}
	if t.restore != nil || header.ResponsePending {
		return model.Commit{}, fmt.Errorf("memory: no-op response cannot be pending")
	}
	if header.Events != nil {
		return model.Commit{}, fmt.Errorf("memory: no-op commit cannot contain an event plan")
	}
	if len(header.Changes) != 0 || len(t.records) != 0 || len(t.blobs) != 0 || len(t.derived) != 0 || len(t.bumps) != 0 {
		return model.Commit{}, fmt.Errorf("memory: no-op commit cannot contain staged mutations")
	}
	id := normalizedScope(principal, header.AgentLabel, header.Method, header.IdempotencyKey)
	if header.IdempotencyKey != "" {
		if err := validateReplayScope(header, principal); err != nil {
			return model.Commit{}, err
		}
		if previous, exists := s.state.replays[id]; exists && s.now().Before(previous.at.Add(driver.IdempotencyWindow)) {
			if previous.value.RequestHash != header.RequestHash || previous.value.ResponsePending {
				return model.Commit{}, driver.ErrConflict
			}
			response, err := cloneJSON(previous.value.Commit)
			if err != nil {
				return model.Commit{}, err
			}
			t.finish()
			return response, nil
		}
	}
	if err := t.checkCheckpointGuards(); err != nil {
		return model.Commit{}, err
	}
	response := header.Response
	response.Revision = s.state.meta.Head
	if response.Results == nil {
		response.Results = []model.OpResult{}
	}
	if response.Warnings == nil {
		response.Warnings = []model.Warning{}
	}
	response, err = cloneJSON(response)
	if err != nil {
		return model.Commit{}, err
	}
	// The returned value and durable value must not share nested response data.
	durable, err := cloneJSON(response)
	if err != nil {
		return model.Commit{}, err
	}
	next := s.state
	if header.IdempotencyKey != "" {
		next = s.state.fork()
		next.replays[id] = replay{value: driver.IdempotencyRecord{RequestHash: header.RequestHash, Commit: durable}, at: s.now()}
	}
	checkpoints := t.prepareCheckpointPublication()
	if err := t.check(ctx); err != nil {
		return model.Commit{}, err
	}
	if err := s.withLeasePublication(t.lease, func() {
		s.state = next
		s.checkpoints = checkpoints
		t.finish()
	}); err != nil {
		return model.Commit{}, err
	}
	return response, nil
}

func validateReplayScope(header driver.ChangesetHeader, principal string) error {
	if !canonicalKey(header.IdempotencyKey) || !canonicalKey(header.Method) || !canonicalKey(header.RequestHash) {
		return fmt.Errorf("memory: idempotent write requires canonical key, method and request hash")
	}
	if strings.HasPrefix(principal, "anonymous:") && header.AgentLabel != "" && !canonicalKey(header.AgentLabel) {
		return fmt.Errorf("memory: invalid anonymous agent label")
	}
	return nil
}

var _ driver.NoopCommitter = (*transaction)(nil)
