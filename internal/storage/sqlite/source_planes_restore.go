package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The caller already owns the complete lock set and Head. Core-only and
// offline-history preparation deliberately keep their separate refusals.
func (t *transaction) PrepareSourcePlaneImport(ctx context.Context, profile driver.RestoreImportProfile) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if len(t.checkpointWrites)+len(t.checkpointGuards) != 0 || t.offlineHistory != nil {
		return driver.Meta{}, driver.ErrUnsupported
	}
	bound, err := driver.PrepareSourcePlaneImportProfile(profile, t.meta, driver.CheckpointLifetimeRestart)
	if err != nil {
		return driver.Meta{}, err
	}
	meta, err := t.prepareRestore(ctx, bound.SourceStoreID)
	if err != nil {
		return driver.Meta{}, err
	}
	t.importRestore = &bound
	t.sourcePlaneImport = driver.NewSourcePlaneImportPublication(bound, meta)
	return meta, nil
}

// PrepareSourcePlaneHistory retains the complete plan and replay payload until
// CommitOfflineHistory validates and publishes namespaces and core together.
func (t *transaction) PrepareSourcePlaneHistory(ctx context.Context, profile driver.RestoreImportProfile, history driver.OfflineHistory, when time.Time) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if t.offlineHistory != nil || t.sourcePlaneImport != nil || len(t.checkpointWrites)+len(t.checkpointGuards) != 0 {
		return driver.Meta{}, driver.ErrUnsupported
	}
	binding, err := driver.PrepareSourcePlaneHistoryBinding(t.meta, profile, history, when, driver.CheckpointLifetimeRestart)
	if err != nil {
		return driver.Meta{}, err
	}
	if _, err = t.prepareRestore(ctx, profile.SourceStoreID); err != nil {
		return driver.Meta{}, err
	}
	bound, err := profile.Detached()
	if err != nil {
		return driver.Meta{}, err
	}
	meta := binding.Meta()
	t.restore, t.importRestore, t.offlineHistory = &meta, &bound, binding
	t.sourcePlaneImport = driver.NewSourcePlaneImportPublication(bound, meta)
	return meta, nil
}

var _ driver.SourcePlaneHistoryRestoreTx = (*transaction)(nil)

func (t *transaction) BindSourcePlaneImport(ctx context.Context, binding driver.SourcePlaneCommitBindingV1) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	return t.sourcePlaneImport.Bind(binding)
}

var _ driver.SourcePlaneRestoreTx = (*transaction)(nil)

// ReadSourcePlaneSnapshot observes namespaces inside the very same SQL read
// transaction that pins the core. Historical core cannot be paired with a
// current namespace. Recovery needs no writer, lease acquisition or receipt.
func (s *Store) ReadSourcePlaneSnapshot(ctx context.Context, view driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	return s.readCheckpointSnapshot(ctx, view, keys, false)
}

func (s *Store) ReadCheckpointInventory(ctx context.Context, view driver.View) (driver.CheckpointInventory, error) {
	return s.readCheckpointSnapshot(ctx, view, nil, true)
}

func (s *Store) readCheckpointSnapshot(ctx context.Context, view driver.View, keys []string, all bool) (*sourcePlaneSnapshot, error) {
	if len(keys) > driver.SourcePlaneMaxListEntries {
		return nil, driver.ErrUnsupported
	}
	for i, key := range keys {
		if driver.ValidateCheckpointKey(key) != nil || i > 0 && keys[i-1] >= key {
			return nil, driver.ErrCheckpointConflict
		}
	}
	base, err := s.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	r := base.(*reader)
	keep := false
	defer func() {
		if !keep {
			_ = r.Close()
		}
	}()
	r.mu.Lock()
	checkpoints, err := r.sourceCheckpoints(ctx, keys, all)
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	keep = true
	return &sourcePlaneSnapshot{reader: r, checkpoints: checkpoints}, nil
}
func (r *reader) sourceCheckpoints(ctx context.Context, keys []string, all bool) (map[string]driver.Checkpoint, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	head, err := r.meta.Head.Uint64()
	if err != nil || r.at != head {
		return nil, driver.ErrCheckpointConflict
	}
	if all {
		rows, err := r.tx.QueryContext(ctx, "SELECT key FROM checkpoints ORDER BY key LIMIT ?", driver.SourcePlaneMaxListEntries+1)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if driver.ValidateCheckpointKey(key) != nil || len(keys) == driver.SourcePlaneMaxListEntries {
				_ = rows.Close()
				return nil, driver.ErrUnsupported
			}
			keys = append(keys, key)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	checkpoints := make(map[string]driver.Checkpoint, len(keys))
	var totalBytes uint64
	bounded := driver.RetainedSourceRead(ctx)
	for index, key := range keys {
		workflow := bounded && key == driver.SourcePlaneWorkflowNamespaceV1
		limit := driver.SourcePlaneCheckpointEnvelopeBytes()
		if all && !workflow {
			limit = driver.SourcePlaneMaxPlanBytes - totalBytes
		}
		cp, err := r.sourceCheckpoint(ctx, key, limit, uint64(index))
		if err != nil {
			return nil, err
		}
		if all && !workflow {
			totalBytes += uint64(len(cp.Payload))
			if totalBytes > driver.SourcePlaneMaxPlanBytes {
				return nil, driver.ErrUnsupported
			}
		}
		checkpoints[key] = cp
	}
	return checkpoints, nil
}
func (r *reader) sourceCheckpoint(ctx context.Context, key string, limit, index uint64) (driver.Checkpoint, error) {
	if !driver.RetainedSourceRead(ctx) {
		return loadCheckpoint(ctx, r.tx, key)
	}
	var cp driver.Checkpoint
	var generation string
	var size uint64
	err := r.tx.QueryRowContext(ctx, `SELECT generation, coalesce(length(payload), 0),
		CASE WHEN length(payload)<=? THEN payload END FROM checkpoints WHERE key=?`, limit, key).
		Scan(&generation, &size, &cp.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return cp, nil
	}
	if err != nil {
		return driver.Checkpoint{}, err
	}
	if size > limit {
		return driver.Checkpoint{}, driver.SourcePlaneCheckpointSizeError(size, limit, index)
	}
	cp.Generation, err = strconv.ParseUint(generation, 10, 64)
	return cp, err
}

type sourcePlaneSnapshot struct {
	*reader
	checkpoints map[string]driver.Checkpoint
}

func (r *sourcePlaneSnapshot) CheckpointKeys() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, driver.ErrTxClosed
	}
	keys := make([]string, 0, len(r.checkpoints))
	for key := range r.checkpoints {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (r *sourcePlaneSnapshot) SourcePlaneCheckpoint(key string) (driver.Checkpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return driver.Checkpoint{}, driver.ErrTxClosed
	}
	cp, ok := r.checkpoints[key]
	if !ok {
		return driver.Checkpoint{}, driver.ErrUnsupported
	}
	cp.Payload = bytes.Clone(cp.Payload)
	return cp, nil
}
func (s *Store) WorkflowCheckpointLifetime() driver.CheckpointLifetime {
	return driver.CheckpointLifetimeRestart
}

var _ driver.WorkflowCheckpointStore = (*Store)(nil)
var _ driver.CheckpointInventoryStore = (*Store)(nil)
