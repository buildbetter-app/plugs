package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func checkpointKey(key string) ([]byte, error) {
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return nil, err
	}
	if len(key) > 2048 {
		return nil, fmt.Errorf("%w: postgres checkpoint key exceeds 2048 bytes", driver.ErrUnsupported)
	}
	return []byte(key), nil
}
func (s *Store) LoadCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	id, err := checkpointKey(key)
	if err != nil {
		return driver.Checkpoint{}, err
	}
	var generation string
	var payload []byte
	err = s.db.QueryRowContext(ctx, `SELECT generation::text,payload FROM plug_postgres.checkpoints WHERE key=$1`, id).Scan(&generation, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.Checkpoint{}, nil
	}
	if err != nil {
		return driver.Checkpoint{}, err
	}
	n, err := parseCounter(generation)
	return driver.Checkpoint{Generation: n, Payload: payload}, err
}
func (s *Store) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
	zero := driver.Checkpoint{}
	if err := s.check(ctx); err != nil {
		return zero, err
	}
	id, err := checkpointKey(key)
	if err != nil {
		return zero, err
	}
	if expected == math.MaxUint64 {
		return zero, fmt.Errorf("%w: postgres checkpoint generation exhausted", driver.ErrUnsupported)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	if _, err = s.checkLease(ctx, tx, lease, true); err != nil {
		return zero, err
	}
	data := append([]byte{}, payload...)
	var generation string
	if expected == 0 {
		err = tx.QueryRowContext(ctx, `INSERT INTO plug_postgres.checkpoints VALUES ($1,1,$2) ON CONFLICT DO NOTHING RETURNING generation::text`, id, data).Scan(&generation)
	} else {
		err = tx.QueryRowContext(ctx, `UPDATE plug_postgres.checkpoints SET generation=$3,payload=$2 WHERE key=$1 AND generation=$4 RETURNING generation::text`, id, data, decimal(expected+1), decimal(expected)).Scan(&generation)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return zero, driver.ErrCheckpointConflict
	}
	if err != nil {
		return zero, err
	}
	n, err := parseCounter(generation)
	if err != nil {
		return zero, err
	}
	if _, err = s.checkLease(ctx, tx, lease, true); err != nil {
		return zero, err
	}
	if err = tx.Commit(); err != nil {
		return zero, err
	}
	return driver.Checkpoint{Generation: n, Payload: data}, nil
}

func (s *Store) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	scope, err := scopeKey(principal, agent, method, key)
	if err != nil {
		return err
	}
	if _, err = revision.Uint64(); err != nil {
		return err
	}
	if warnings == nil {
		warnings = []model.Warning{}
	}
	post, err := encode(warnings)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = s.checkLease(ctx, tx, lease, true); err != nil {
		return err
	}
	var raw, storedPost []byte
	var finalized bool
	err = tx.QueryRowContext(ctx, `SELECT value,finalized,post FROM plug_postgres.receipts WHERE scope=$1 AND expires>clock_timestamp() FOR UPDATE`, scope).Scan(&raw, &finalized, &storedPost)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.ErrNotFound
	}
	if err != nil {
		return err
	}
	value, err := decodeReceipt(raw)
	if err != nil {
		return err
	}
	if value.Commit.Revision.String() != revision.String() {
		return driver.ErrConflict
	}
	if finalized {
		if !bytes.Equal(storedPost, post) {
			return driver.ErrConflict
		}
	} else {
		value.Commit.Warnings = append(value.Commit.Warnings, warnings...)
		if raw, err = encodeReceipt(value); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE plug_postgres.receipts SET value=$2,finalized=true,post=$3 WHERE scope=$1`, scope, raw, post); err != nil {
			return err
		}
	}
	if _, err = s.checkLease(ctx, tx, lease, true); err != nil {
		return err
	}
	return tx.Commit()
}

var _ driver.CheckpointStore = (*Store)(nil)
var _ driver.ResponseFinalizer = (*Store)(nil)
