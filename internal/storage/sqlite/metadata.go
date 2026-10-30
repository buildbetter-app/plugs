package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func canonicalPart(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}
func (t *transaction) replay(ctx context.Context, c *sql.Conn, h driver.ChangesetHeader) (*storedReplay, error) {
	if h.IdempotencyKey == "" {
		return nil, nil
	}
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return nil, err
	}
	if !canonicalPart(h.IdempotencyKey) || !canonicalPart(h.Method) || !canonicalPart(h.RequestHash) || strings.HasPrefix(principal, "anonymous:") && h.AgentLabel != "" && !canonicalPart(h.AgentLabel) {
		return nil, fmt.Errorf("sqlite: invalid idempotency scope or request digest")
	}
	value, err := readReplay(ctx, c, scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey))
	if err != nil {
		return nil, err
	}
	if value != nil && value.RequestHash != h.RequestHash {
		return nil, driver.ErrConflict
	}
	return value, nil
}
func responseBytes(h driver.ChangesetHeader, revision model.Revision) ([]byte, error) {
	response := h.Response
	response.Revision = revision
	if response.Results == nil {
		response.Results = []model.OpResult{}
	}
	if response.Warnings == nil {
		response.Warnings = []model.Warning{}
	}
	return json.Marshal(response)
}
func saveReplay(ctx context.Context, c *sql.Conn, h driver.ChangesetHeader, revision model.Revision, entry []byte) error {
	// Validate the response even without a replay key; invalid nested values must
	// abort the entire graph transaction rather than fabricate a response later.
	data, err := responseBytes(h, revision)
	if err != nil {
		return err
	}
	if h.IdempotencyKey == "" {
		return nil
	}
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `INSERT INTO replays(scope,created,rev,request_hash,response,changeset,finalized,post,pending) VALUES(?,?,?,?,?,?,0,NULL,?)
 ON CONFLICT(scope) DO UPDATE SET created=excluded.created,rev=excluded.rev,request_hash=excluded.request_hash,response=excluded.response,changeset=excluded.changeset,finalized=0,post=NULL,pending=excluded.pending`, scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey), time.Now().UnixNano(), revKey(mustUint(revision)), h.RequestHash, data, entry, h.ResponsePending)
	return err
}
func (t *transaction) CommitNoop(ctx context.Context, h driver.ChangesetHeader) (model.Commit, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateHeader(ctx, h); err != nil {
		return model.Commit{}, err
	}
	if t.restore != nil || h.ResponsePending || len(h.Changes) != 0 || len(t.records) != 0 || len(t.derived) != 0 || len(t.bumps) != 0 || len(t.blobs) != 0 {
		return model.Commit{}, fmt.Errorf("sqlite: no-op cannot publish staged mutations")
	}
	if h.Events != nil {
		return model.Commit{}, fmt.Errorf("sqlite: no-op commit cannot contain an event plan")
	}
	var result model.Commit
	err := t.store.immediate(ctx, t.lease, func(c *sql.Conn) error {
		previous, err := t.replay(ctx, c, h)
		if err != nil {
			return err
		}
		if previous != nil {
			if previous.ResponsePending {
				return driver.ErrConflict
			}
			result = previous.Commit
			return nil
		}
		m, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		data, err := responseBytes(h, m.Head)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(data, &result); err != nil {
			return err
		}
		return saveReplay(ctx, c, h, m.Head, nil)
	})
	if err == nil {
		t.finish()
		return result, nil
	}
	var uncertain commitError
	if errors.As(err, &uncertain) {
		t.finish()
		return model.Commit{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	var fenced driver.ErrFenced
	if errors.As(err, &fenced) {
		t.finish()
	}
	return model.Commit{}, err
}
func (s *Store) LoadCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return driver.Checkpoint{}, err
	}
	return loadCheckpoint(ctx, s.db, key)
}
func loadCheckpoint(ctx context.Context, q queryer, key string) (driver.Checkpoint, error) {
	var value driver.Checkpoint
	var generation string
	err := q.QueryRowContext(ctx, `SELECT generation,payload FROM checkpoints WHERE key=?`, key).Scan(&generation, &value.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.Checkpoint{}, nil
	}
	if err == nil {
		value.Generation, err = strconv.ParseUint(generation, 10, 64)
	}
	return value, err
}
func (s *Store) SaveCheckpoint(ctx context.Context, value driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
	l, err := s.leaseFor(ctx, value)
	if err != nil {
		return driver.Checkpoint{}, err
	}
	if err = driver.ValidateCheckpointKey(key); err != nil {
		return driver.Checkpoint{}, err
	}
	var result driver.Checkpoint
	payload = bytes.Clone(payload)
	err = s.immediate(ctx, l, func(c *sql.Conn) error {
		current, err := loadCheckpoint(ctx, c, key)
		if err != nil {
			return err
		}
		if current.Generation != expected {
			return driver.ErrCheckpointConflict
		}
		if expected == math.MaxUint64 {
			return fmt.Errorf("sqlite: checkpoint generation exhausted")
		}
		result = driver.Checkpoint{Generation: expected + 1, Payload: payload}
		_, err = c.ExecContext(ctx, `INSERT INTO checkpoints(key,generation,payload) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET generation=excluded.generation,payload=excluded.payload`, key, revKey(expected+1), payload)
		return err
	})
	if err != nil {
		return driver.Checkpoint{}, err
	}
	result.Payload = bytes.Clone(result.Payload)
	return result, nil
}
func (s *Store) FinalizeResponse(ctx context.Context, value driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	l, err := s.leaseFor(ctx, value)
	if err != nil {
		return err
	}
	if _, err = revision.Uint64(); err != nil {
		return err
	}
	if warnings == nil {
		warnings = []model.Warning{}
	}
	post, err := json.Marshal(warnings)
	if err != nil {
		return err
	}
	return s.immediate(ctx, l, func(c *sql.Conn) error {
		scope := scopeKey(principal, agent, method, key)
		previous, err := readReplay(ctx, c, scope)
		if err != nil {
			return err
		}
		if previous == nil {
			return driver.ErrNotFound
		}
		if previous.Commit.Revision.String() != revision.String() {
			return driver.ErrConflict
		}
		if !previous.ResponsePending {
			want := []byte("[]")
			if previous.finalized {
				want = previous.post
			}
			if !bytes.Equal(want, post) {
				return driver.ErrConflict
			}
			return nil
		}
		previous.Commit.Warnings = append(previous.Commit.Warnings, warnings...)
		data, err := json.Marshal(previous.Commit)
		if err != nil {
			return err
		}
		_, err = c.ExecContext(ctx, `UPDATE replays SET response=?,pending=0,finalized=1,post=? WHERE scope=?`, data, post, scope)
		return err
	})
}

var _ driver.NoopCommitter = (*transaction)(nil)
var _ driver.CheckpointStore = (*Store)(nil)
var _ driver.ResponseFinalizer = (*Store)(nil)
