package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historystream"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

type historyStreamStage struct {
	tx      *transaction
	proof   historyverify.Checkpoint
	binding driver.OfflineHistoryStreamBinding
	private *historystream.Stage
}

// NewOfflineHistoryStreamStage attaches an engine stage to an existing locked
// writer. The verifier-issued seal is mandatory; only layer-3 receiving will
// issue production seals. The ordinary Commit path cannot publish this writer.
func NewOfflineHistoryStreamStage(tx driver.WriteTx, proof historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error) {
	t, ok := tx.(*transaction)
	if !ok {
		return nil, fmt.Errorf("%w: memory stream requires its own writer", driver.ErrUnsupported)
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.closed || t.stream != nil || t.restore != nil {
		return nil, driver.ErrTxClosed
	}
	return &historyStreamStage{tx: t, proof: proof}, nil
}

func (s *historyStreamStage) close() error {
	if s.private == nil {
		return nil
	}
	return s.private.Close()
}
func (s *historyStreamStage) Abort() error { return errors.Join(s.tx.Close(), s.close()) }

func (s *historyStreamStage) Begin(ctx context.Context, b driver.OfflineHistoryStreamBinding) error {
	t := s.tx
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.stream != nil || s.private != nil {
		return driver.ErrRestoreBindingMismatch{Field: "stream.state"}
	}
	if b.Lifetime != driver.CheckpointLifetimeProcess {
		return driver.ErrRestoreBindingMismatch{Field: "stream.lifetime"}
	}
	if b.Current != t.store.state.meta {
		return driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict}
	}
	private, err := historystream.New(ctx, b, s.proof)
	if err != nil {
		return err
	}
	s.private = private
	if _, err = t.prepareRestore(ctx, b.Profile.SourceStoreID); err != nil {
		return err
	}
	s.binding = b
	t.stream = s
	meta := b.Meta
	t.restore = &meta
	profile, err := b.Profile.Detached()
	if err != nil {
		return err
	}
	t.importRestore = &profile
	if profile.SourcePlanes != nil {
		t.sourcePlaneImport = driver.NewSourcePlaneImportPublication(profile, meta)
	}
	return nil
}

func (s *historyStreamStage) Stage(ctx context.Context, r model.Revision, f driver.OfflineHistoryStreamFrame) error {
	t := s.tx
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if s.private == nil {
		return driver.ErrRestoreBindingMismatch{Field: "stream.state"}
	}
	return s.private.Stage(ctx, r, f)
}

func (s *historyStreamStage) Publish(ctx context.Context, proof driver.OfflineHistoryStreamPublication, h driver.ChangesetHeader) (model.Changeset, error) {
	zero := model.Changeset{}
	if _, err := proof.Claim(s.binding); err != nil {
		return zero, err
	}
	t := s.tx
	store := t.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return zero, err
	}
	if s.private == nil || !t.hasHead || t.restore == nil {
		return zero, driver.ErrRestoreBindingMismatch{Field: "stream.state"}
	}
	if s.binding.Lifetime != driver.CheckpointLifetimeProcess {
		return zero, driver.ErrRestoreBindingMismatch{Field: "stream.lifetime"}
	}
	if *t.restore != s.binding.Meta {
		return zero, driver.ErrRestoreBindingMismatch{Field: "stream.meta"}
	}
	if store.head != t || store.state.meta != s.binding.Current {
		return zero, driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict}
	}
	if err := store.restoreEmpty(); err != nil {
		return zero, err
	}
	if err := t.validatePredicates(); err != nil {
		return zero, err
	}
	if t.sourcePlaneImport != nil {
		if err := t.sourcePlaneImport.Validate(h, t.checkpointWrites, t.records); err != nil {
			return zero, err
		}
	} else if len(t.checkpointWrites)+len(t.checkpointGuards) != 0 {
		return zero, driver.ErrUnsupported
	}
	if err := t.checkCheckpointGuards(); err != nil {
		return zero, err
	}
	if err := s.private.CheckAuxiliary(historyverify.Auxiliary{Derived: t.derived, Allocators: t.allocators, Parents: t.parentCounters, Blobs: t.blobs, Bumps: t.bumps}); err != nil {
		return zero, err
	}
	bumps := make([]driver.LockKey, 0, len(t.bumps))
	for _, key := range t.bumps {
		bumps = append(bumps, key)
	}
	if err := s.private.Finalize(ctx, h, t.records, t.acquired, bumps); err != nil {
		return zero, err
	}
	next := store.state.fork()
	// These are the memory engine's durable-in-process output structures, not a
	// second history plan. Bodies are read once into their final immutable home.
	err := s.private.Rows(ctx, func(row historystream.Row) error {
		if err := t.check(ctx); err != nil {
			return err
		}
		r := driver.Record{Ref: row.Ref, Kind: row.Kind, Revision: row.Revision}
		if !row.Deleted {
			body, err := io.ReadAll(historystream.ContextReader(ctx, s.private.Reader(row.Image)))
			if err != nil {
				return err
			}
			r.Body = body
		}
		id, err := recordKey(r.Ref)
		if err != nil {
			return err
		}
		next.kinds[id] = r.Kind
		state := driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref}, Revision: r.Revision, Present: !row.Deleted}
		if row.Deleted {
			delete(next.records, id)
		} else {
			next.records[id] = r
			state.Record = &r
		}
		v := driver.Version{Ref: r.Ref, Revision: r.Revision, Base: true, Body: r.Body, Deleted: row.Deleted, CreatedAt: s.binding.When, Principal: s.binding.Profile.Admission.Actor}
		next.versions[id] = append(next.versions[id], v)
		next.publishKey(id, state)
		return nil
	})
	if err != nil {
		return zero, err
	}
	for id, value := range t.derived {
		value.Revision = s.binding.Meta.Head
		next.publishKey(id, value)
	}
	next.publishDerivedKeys(t.derived)
	for id, key := range t.bumps {
		next.publishKey(id, driver.LockState{Key: key, Revision: s.binding.Meta.Head, Present: true})
	}
	for prefix, a := range t.allocators {
		next.allocators[prefix] = a.Clone()
	}
	for key, value := range t.parentCounters {
		next.parentCounters[key] = value
	}
	head, _ := s.binding.Meta.Head.Uint64()
	for hash, data := range t.blobs {
		next.blobs[hash] = blob{bytes: bytes.Clone(data), revision: head}
	}
	next.meta = s.binding.Meta
	if err = t.publishSourceIndexes(next); err != nil {
		return zero, err
	}
	next.log = nil
	var final model.Changeset
	if err = s.private.Entries(ctx, func(_ model.Revision, image historystream.Image) error {
		var entry model.Changeset
		if err := json.NewDecoder(historystream.ContextReader(ctx, s.private.Reader(image))).Decode(&entry); err != nil {
			return err
		}
		next.log = append(next.log, entry)
		final = entry
		return nil
	}); err != nil {
		return zero, err
	}
	response := h.Response
	if response.Results == nil {
		response.Results = []model.OpResult{}
	}
	if response.Warnings == nil {
		response.Warnings = []model.Warning{}
	}
	response.Revision = next.meta.Head
	response, err = cloneJSON(response)
	if err != nil {
		return zero, err
	}
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return zero, err
	}
	key := normalizedScope(principal, h.AgentLabel, h.Method, h.IdempotencyKey)
	next.replays[key] = replay{value: driver.IdempotencyRecord{AttemptID: h.AttemptID, RequestHash: h.RequestHash, Commit: response, Changeset: final}, at: store.now()}
	if err = next.publishAllocatorImage(store.state, true); err != nil {
		return zero, err
	}
	checkpoints := t.prepareCheckpointPublication()
	if err = s.close(); err != nil {
		return zero, err
	}
	if err = t.check(ctx); err != nil {
		return zero, err
	}
	if err = store.withLeasePublication(t.lease, func() {
		store.state = next
		store.checkpoints = checkpoints
		t.finish()
	}); err != nil {
		return zero, err
	}
	return cloneJSON(final)
}

var _ driver.OfflineHistoryStreamStage = (*historyStreamStage)(nil)
