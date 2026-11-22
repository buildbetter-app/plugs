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
	value, err := t.store.readReplay(ctx, c, scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey))
	if err != nil {
		return nil, err
	}
	if value != nil && value.RequestHash != h.RequestHash {
		if h.Kind == model.ChangesetRestore {
			return nil, driver.ErrRestoreBindingMismatch{Field: "requestHash"}
		}
		return nil, driver.ErrConflict
	}
	return value, nil
}

// receiptResponse keeps legacy flat Commit JSON readable and carries private
// invocation metadata outside the public Commit type.
type receiptResponse struct {
	model.Commit
	AttemptID string `json:"attemptID,omitempty"`
}

func (r receiptResponse) MarshalJSON() ([]byte, error) {
	if err := r.Commit.Validate(); err != nil {
		return nil, err
	}
	// Alias suppresses Commit.MarshalJSON, which would hide sibling fields.
	type commitFields model.Commit
	return json.Marshal(struct {
		commitFields
		AttemptID string `json:"attemptID,omitempty"`
	}{commitFields: commitFields(r.Commit), AttemptID: r.AttemptID})
}

func (r *receiptResponse) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var attemptID string
	if raw, ok := fields["attemptID"]; ok {
		if err := json.Unmarshal(raw, &attemptID); err != nil {
			return err
		}
		delete(fields, "attemptID")
	}
	public, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	var commit model.Commit
	if err := json.Unmarshal(public, &commit); err != nil {
		return err
	}
	*r = receiptResponse{Commit: commit, AttemptID: attemptID}
	return nil
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
	return json.Marshal(receiptResponse{Commit: response, AttemptID: h.AttemptID})
}
func (s *Store) saveReplay(ctx context.Context, c *sql.Conn, h driver.ChangesetHeader, revision model.Revision, entry []byte) error {
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
 ON CONFLICT(scope) DO UPDATE SET created=excluded.created,rev=excluded.rev,request_hash=excluded.request_hash,response=excluded.response,changeset=excluded.changeset,finalized=0,post=NULL,pending=excluded.pending`, scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey), s.now().UnixNano(), revKey(mustUint(revision)), h.RequestHash, data, entry, h.ResponsePending)
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
		// A replay never reapplies the namespace CAS. On a miss, guard and
		// publish the staged checkpoint in this same fenced SQL transaction.
		if err := t.checkCheckpointGuards(ctx, c); err != nil {
			return err
		}
		if err := t.publishCheckpoints(ctx, c); err != nil {
			return err
		}
		m, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		data, err := responseBytes(h, m.Head)
		if err != nil {
			return err
		}
		var response receiptResponse
		if err = json.Unmarshal(data, &response); err != nil {
			return err
		}
		result = response.Commit
		return t.store.saveReplay(ctx, c, h, m.Head, nil)
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
	return s.saveCheckpoint(ctx, value, key, expected, payload, nil)
}

func (s *Store) SaveCheckpointAtHead(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, head driver.CoreSnapshotExpectation) (driver.Checkpoint, error) {
	return s.saveCheckpoint(ctx, lease, key, expected, payload, func(c *sql.Conn) error {
		meta, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		// saveCheckpoint owns BEGIN IMMEDIATE and the active lease fence.
		// No ordinary commit can change this head until checkpoint publication.
		return head.Check(meta)
	})
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
	return s.saveCheckpoint(ctx, lease, key, expected, payload, func(c *sql.Conn) error {
		meta, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		if incarnation == "" || incarnation != meta.Incarnation {
			return driver.ErrIncarnationChanged{From: incarnation, To: meta.Incarnation}
		}
		var data []byte
		err = c.QueryRowContext(ctx, `SELECT data FROM records WHERE key=?`, id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return driver.ErrConflict
		}
		if err != nil {
			return err
		}
		var record driver.Record
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		if record.Revision.String() != revision.String() {
			return driver.ErrConflict
		}
		return nil
	})
}

func (s *Store) saveCheckpoint(ctx context.Context, value driver.ActiveLease, key string, expected uint64, payload []byte, guard func(*sql.Conn) error) (driver.Checkpoint, error) {
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
		if guard != nil {
			if err := guard(c); err != nil {
				return err
			}
		}
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
		var uncertain commitError
		if errors.As(err, &uncertain) {
			// Native COMMIT can publish before its execution reports cancellation.
			// The caller must resolve this through LoadCheckpoint, not treat the
			// underlying error as a rejection or the staged result as confirmed.
			return driver.Checkpoint{}, driver.ErrIndeterminate{}
		}
		return driver.Checkpoint{}, err
	}
	result.Payload = bytes.Clone(result.Payload)
	return result, nil
}

var _ driver.RecordGuardedCheckpointStore = (*Store)(nil)

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
	err = s.immediate(ctx, l, func(c *sql.Conn) error {
		scope := scopeKey(principal, agent, method, key)
		previous, err := s.readReplay(ctx, c, scope)
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
		data, err := json.Marshal(receiptResponse{Commit: previous.Commit, AttemptID: previous.AttemptID})
		if err != nil {
			return err
		}
		_, err = c.ExecContext(ctx, `UPDATE replays SET response=?,pending=0,finalized=1,post=? WHERE scope=?`, data, post, scope)
		return err
	})
	var uncertain commitError
	if errors.As(err, &uncertain) {
		// Resolve a possibly finalized receipt through a fresh reader. Never
		// expose native COMMIT's cancellation as a no-publication guarantee.
		return driver.ErrIndeterminate{IdempotencyKey: key}
	}
	return err
}

func (s *Store) PutCoreSession(ctx context.Context, value driver.CoreSessionRecord, now time.Time, max int) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || max <= 0 {
		return fmt.Errorf("sqlite: invalid core session bounds")
	}
	digest := bytes.Clone(value.TokenDigest)
	return s.immediate(ctx, nil, func(c *sql.Conn) error {
		nowNS := now.UnixNano()
		if _, err := c.ExecContext(ctx, `DELETE FROM core_sessions WHERE revoked=0 AND expires_at<=?`, nowNS); err != nil {
			return err
		}
		var active int
		if err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM core_sessions WHERE revoked=0 AND expires_at>?`, nowNS).Scan(&active); err != nil {
			return err
		}
		if active >= max {
			return driver.ErrConflict
		}
		var exists int
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM core_sessions WHERE id=?)`, value.ID).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			return driver.ErrConflict
		}
		_, err := c.ExecContext(ctx, `INSERT INTO core_sessions(id,token_digest,principal_kind,principal_id,expires_at,revoked) VALUES(?,?,?,?,?,?)`, value.ID, digest, value.Principal.Kind, value.Principal.ID, value.ExpiresAt.UnixNano(), value.Revoked)
		return err
	})
}

func (s *Store) LookupCoreSession(ctx context.Context, id string) (driver.CoreSessionRecord, error) {
	if err := s.check(ctx); err != nil {
		return driver.CoreSessionRecord{}, err
	}
	var value driver.CoreSessionRecord
	var digest []byte
	var expires int64
	var revoked int
	err := s.db.QueryRowContext(ctx, `SELECT token_digest,principal_kind,principal_id,expires_at,revoked FROM core_sessions WHERE id=?`, id).Scan(&digest, &value.Principal.Kind, &value.Principal.ID, &expires, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.CoreSessionRecord{}, driver.ErrNotFound
	}
	if err != nil {
		return driver.CoreSessionRecord{}, err
	}
	if len(digest) != 32 || (revoked != 0 && revoked != 1) {
		return driver.CoreSessionRecord{}, fmt.Errorf("sqlite: malformed core session record")
	}
	value.ID = id
	value.TokenDigest = bytes.Clone(digest)
	value.ExpiresAt = time.Unix(0, expires)
	value.Revoked = revoked != 0
	return value, nil
}

func (s *Store) RevokeCoreSession(ctx context.Context, id string) (bool, error) {
	if err := s.check(ctx); err != nil {
		return false, err
	}
	var changed int64
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		result, err := c.ExecContext(ctx, `UPDATE core_sessions SET revoked=1 WHERE id=? AND revoked=0`, id)
		if err != nil {
			return err
		}
		changed, err = result.RowsAffected()
		return err
	})
	return changed == 1, err
}

var _ driver.NoopCommitter = (*transaction)(nil)
var _ driver.CheckpointStore = (*Store)(nil)
var _ driver.ResponseFinalizer = (*Store)(nil)
var _ driver.CoreSessionStore = (*Store)(nil)
