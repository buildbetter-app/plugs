package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

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
	return s.saveCheckpoint(ctx, lease, key, expected, payload, nil, nil)
}

func (s *Store) SaveCheckpointAtHead(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, head driver.CoreSnapshotExpectation) (driver.Checkpoint, error) {
	return s.saveCheckpoint(ctx, lease, key, expected, payload, nil, &head)
}

var _ driver.HeadGuardedCheckpointStore = (*Store)(nil)

func (s *Store) SaveCheckpointIfRecord(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, ref driver.Ref, revision model.Revision, incarnation string) (driver.Checkpoint, error) {
	id, err := recordKey(ref)
	if err != nil {
		return driver.Checkpoint{}, err
	}
	if err := revision.Validate(); err != nil {
		return driver.Checkpoint{}, err
	}
	return s.saveCheckpoint(ctx, lease, key, expected, payload, func(tx *sql.Tx) error {
		meta, err := readMeta(ctx, tx, "")
		if err != nil {
			return err
		}
		if incarnation == "" || incarnation != meta.Incarnation {
			return driver.ErrIncarnationChanged{From: incarnation, To: meta.Incarnation}
		}
		var current string
		var deleted bool
		err = tx.QueryRowContext(ctx, `SELECT revision::text,deleted FROM plug_postgres.records WHERE key=$1 FOR SHARE`, []byte(id)).Scan(&current, &deleted)
		if errors.Is(err, sql.ErrNoRows) {
			return driver.ErrConflict
		}
		if err != nil {
			return err
		}
		if deleted || current != revision.String() {
			return driver.ErrConflict
		}
		return nil
	}, nil)
}

func (s *Store) saveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, guard func(*sql.Tx) error, head *driver.CoreSnapshotExpectation) (driver.Checkpoint, error) {
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
	if head != nil {
		// Match ordinary Head -> checkpoint lock ordering. FOR SHARE pins the
		// full graph through publication, including absent/predicate child reads.
		meta, err := readMeta(ctx, tx, " FOR SHARE")
		if err != nil {
			return zero, err
		}
		if err := head.Check(meta); err != nil {
			return zero, err
		}
	}
	if err = lockCheckpoint(ctx, tx, key); err != nil {
		return zero, err
	}
	if guard != nil {
		if err = guard(tx); err != nil {
			return zero, err
		}
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

var _ driver.RecordGuardedCheckpointStore = (*Store)(nil)

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
	err = tx.QueryRowContext(ctx, `SELECT value,post FROM plug_postgres.receipts WHERE scope=$1 AND expires>clock_timestamp() FOR UPDATE`, scope).Scan(&raw, &storedPost)
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
	if !value.ResponsePending {
		// An initially final receipt has no post outcome and accepts only an
		// empty replay. Finalized pending receipts retain their exact outcome.
		if storedPost == nil && len(warnings) != 0 || storedPost != nil && !bytes.Equal(storedPost, post) {
			return driver.ErrConflict
		}
	} else {
		value.Commit.Warnings = append(value.Commit.Warnings, warnings...)
		value.ResponsePending = false
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

func (s *Store) PutCoreSession(ctx context.Context, value driver.CoreSessionRecord, now time.Time, max int) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || max <= 0 {
		return fmt.Errorf("postgres: invalid core session bounds")
	}
	digest := bytes.Clone(value.TokenDigest)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Capacity is a provider bound, so serialize it across independent
	// processes rather than allowing two issuers to pass the same count.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(58193481276902)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM plug_postgres.core_sessions WHERE revoked=false AND expires_at<= $1`, now.UTC()); err != nil {
		return err
	}
	var active int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM plug_postgres.core_sessions WHERE revoked=false AND expires_at>$1`, now.UTC()).Scan(&active); err != nil {
		return err
	}
	if active >= max {
		return driver.ErrConflict
	}
	var id string
	err = tx.QueryRowContext(ctx, `INSERT INTO plug_postgres.core_sessions(id,token_digest,principal_kind,principal_id,expires_at,revoked) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING id`, value.ID, digest, value.Principal.Kind, value.Principal.ID, value.ExpiresAt.UTC(), value.Revoked).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.ErrConflict
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) LookupCoreSession(ctx context.Context, id string) (driver.CoreSessionRecord, error) {
	if err := s.check(ctx); err != nil {
		return driver.CoreSessionRecord{}, err
	}
	var value driver.CoreSessionRecord
	err := s.db.QueryRowContext(ctx, `SELECT token_digest,principal_kind,principal_id,expires_at,revoked FROM plug_postgres.core_sessions WHERE id=$1`, id).Scan(&value.TokenDigest, &value.Principal.Kind, &value.Principal.ID, &value.ExpiresAt, &value.Revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.CoreSessionRecord{}, driver.ErrNotFound
	}
	if err != nil {
		return driver.CoreSessionRecord{}, err
	}
	if len(value.TokenDigest) != 32 {
		return driver.CoreSessionRecord{}, fmt.Errorf("postgres: malformed core session record")
	}
	value.ID = id
	value.TokenDigest = bytes.Clone(value.TokenDigest)
	return value, nil
}

func (s *Store) RevokeCoreSession(ctx context.Context, id string) (bool, error) {
	if err := s.check(ctx); err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.core_sessions SET revoked=true WHERE id=$1 AND revoked=false`, id)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

var _ driver.CheckpointStore = (*Store)(nil)
var _ driver.ResponseFinalizer = (*Store)(nil)
var _ driver.CoreSessionStore = (*Store)(nil)
