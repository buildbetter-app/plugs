package memory

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) PrepareOfflineHistory(ctx context.Context, p driver.RestoreImportProfile, h driver.OfflineHistory, when time.Time) (driver.Meta, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	binding, err := driver.PrepareOfflineHistoryBinding(s.state.meta, p, h, when)
	if err != nil {
		return driver.Meta{}, err
	}
	if _, err = t.prepareRestore(ctx, p.SourceStoreID); err != nil {
		return driver.Meta{}, err
	}
	meta := binding.Meta()
	t.restore = &meta
	t.offlineHistory = binding
	return meta, nil
}
func (t *transaction) CommitOfflineHistory(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	zero := model.Changeset{}
	if err := t.check(ctx); err != nil {
		return zero, err
	}
	if !t.hasHead || s.head != t || t.offlineHistory == nil {
		return zero, fmt.Errorf("history commit requires prepared offline capability and Head")
	}
	if err := s.restoreEmpty(); err != nil {
		return zero, err
	}
	if err := t.validateChanges(h.Changes); err != nil {
		return zero, err
	}
	if err := t.validatePredicates(); err != nil {
		return zero, err
	}
	tail := []driver.Record{}
	for _, r := range t.records {
		if r == nil {
			return zero, fmt.Errorf("history final image cannot stage a deletion")
		}
		tail = append(tail, *r)
	}
	bumps := []driver.LockKey{}
	for _, k := range t.bumps {
		bumps = append(bumps, k)
	}

	if t.sourcePlaneImport != nil {
		if err := t.sourcePlaneImport.Validate(h, t.checkpointWrites, t.records); err != nil {
			return zero, err
		}
	} else if len(t.checkpointWrites)+len(t.checkpointGuards) != 0 {
		return zero, driver.ErrUnsupported
	}
	// Finalize is the expensive, detached history build. Keep the transaction's
	// staged maps unavailable to callers while it runs, then reacquire Store.mu
	// for the final state construction and lease-fenced publication.
	base := s.state
	offline := t.offlineHistory
	locks := append(driver.LockSet(nil), t.acquired...)
	hook := s.beforeCommitHook
	t.committing = true
	defer func() {
		if t.committing {
			t.finish()
		}
	}()
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	plan, err := offline.Finalize(h, tail, locks, bumps)
	s.mu.Lock()
	if err != nil {
		// A refused plan leaves the prepared transaction available for corrected
		// staging and another commit, as it did before detached preparation.
		t.committing = false
		return zero, err
	}
	if err := t.checkCommit(ctx); err != nil {
		return zero, err
	}
	if s.state != base || s.head != t {
		return zero, driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict}
	}
	if err := t.checkCheckpointGuards(); err != nil {
		return zero, err
	}
	next := s.state.fork()
	for i, row := range plan.Rows {
		r := copyRecord(row.Record)
		id, _ := recordKey(r.Ref)
		next.kinds[id] = r.Kind
		state := driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref}, Revision: r.Revision, Present: !row.Deleted}
		if row.Deleted {
			delete(next.records, id)
		} else {
			next.records[id] = r
			state.Record = &r
		}
		next.versions[id] = append(next.versions[id], plan.Versions[i])
		next.publishKey(id, state)
	}
	for id, value := range t.derived {
		value.Revision = plan.Meta.Head
		next.publishKey(id, value)
	}
	next.publishDerivedKeys(t.derived)
	for id, key := range t.bumps {
		next.publishKey(id, driver.LockState{Key: key, Revision: plan.Meta.Head, Present: true})
	}
	for prefix, a := range t.allocators {
		next.allocators[prefix] = a.Clone()
	}
	head, _ := plan.Meta.Head.Uint64()
	for hash, data := range t.blobs {
		next.blobs[hash] = blob{bytes: bytes.Clone(data), revision: head}
	}
	next.meta = plan.Meta
	if err = t.publishSourceIndexes(next); err != nil {
		return zero, err
	}
	next.log = plan.Entries
	response := h.Response
	if response.Results == nil {
		response.Results = []model.OpResult{}
	}
	if response.Warnings == nil {
		response.Warnings = []model.Warning{}
	}
	response.Revision = plan.Meta.Head
	response, err = cloneJSON(response)
	if err != nil {
		return zero, err
	}
	entry := plan.Entries[len(plan.Entries)-1]
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return zero, err
	}
	key := normalizedScope(principal, h.AgentLabel, h.Method, h.IdempotencyKey)
	next.replays[key] = replay{value: driver.IdempotencyRecord{AttemptID: h.AttemptID, RequestHash: h.RequestHash, Commit: response, Changeset: entry}, at: s.now()}
	if err = t.checkCommit(ctx); err != nil {
		return zero, err
	}
	if err := next.publishAllocatorImage(s.state, true); err != nil {
		return zero, err
	}
	checkpoints := t.prepareCheckpointPublication()
	if err = t.checkCommit(ctx); err != nil {
		return zero, err
	}
	if err := s.withLeasePublication(t.lease, func() {
		s.state = next
		s.checkpoints = checkpoints
		t.finish()
	}); err != nil {
		return zero, err
	}
	return cloneJSON(entry)
}

var _ driver.OfflineHistoryTx = (*transaction)(nil)
