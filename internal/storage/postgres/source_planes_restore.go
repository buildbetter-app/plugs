package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sort"

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
		rows, err := r.sql.QueryContext(ctx, "SELECT key FROM plug_postgres.checkpoints ORDER BY key LIMIT $1", driver.SourcePlaneMaxListEntries+1)
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
	totalBytes := 0
	for _, key := range keys {
		cp, err := r.sourceCheckpoint(ctx, key)
		if err != nil {
			return nil, err
		}
		totalBytes += len(cp.Payload)
		if all && totalBytes > driver.SourcePlaneMaxPlanBytes {
			return nil, driver.ErrUnsupported
		}
		cp.Payload = bytes.Clone(cp.Payload)
		checkpoints[key] = cp
	}
	return checkpoints, nil
}
func (r *reader) sourceCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	id, err := checkpointKey(key)
	if err != nil {
		return driver.Checkpoint{}, err
	}
	var generation string
	var payload []byte
	err = r.sql.QueryRowContext(ctx, `SELECT generation::text,payload FROM plug_postgres.checkpoints WHERE key=$1`, id).Scan(&generation, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.Checkpoint{}, nil
	}
	if err != nil {
		return driver.Checkpoint{}, err
	}
	n, err := parseCounter(generation)
	return driver.Checkpoint{Generation: n, Payload: payload}, err
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
