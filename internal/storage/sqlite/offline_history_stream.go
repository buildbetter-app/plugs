package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

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

func NewOfflineHistoryStreamStage(tx driver.WriteTx, proof historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error) {
	t, ok := tx.(*transaction)
	if !ok {
		return nil, fmt.Errorf("%w: SQLite stream requires its own writer", driver.ErrUnsupported)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.stream != nil || s.private != nil {
		return driver.ErrRestoreBindingMismatch{Field: "stream.state"}
	}
	if b.Lifetime != driver.CheckpointLifetimeRestart {
		return driver.ErrRestoreBindingMismatch{Field: "stream.lifetime"}
	}
	if b.Current != t.meta {
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
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return zero, err
	}
	if s.private == nil || !t.hasHead || t.restore == nil {
		return zero, driver.ErrRestoreBindingMismatch{Field: "stream.state"}
	}
	if s.binding.Lifetime != driver.CheckpointLifetimeRestart {
		return zero, driver.ErrRestoreBindingMismatch{Field: "stream.lifetime"}
	}
	if *t.restore != s.binding.Meta {
		return zero, driver.ErrRestoreBindingMismatch{Field: "stream.meta"}
	}
	if t.meta != s.binding.Current {
		return zero, driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict}
	}
	if err := t.validatePredicates(ctx); err != nil {
		return zero, err
	}
	if t.sourcePlaneImport != nil {
		if err := t.sourcePlaneImport.Validate(h, t.checkpointWrites, t.records); err != nil {
			return zero, err
		}
	} else if len(t.checkpointWrites)+len(t.checkpointGuards) != 0 {
		return zero, driver.ErrUnsupported
	}
	bumps := make([]driver.LockKey, 0, len(t.bumps))
	for _, key := range t.bumps {
		bumps = append(bumps, key)
	}
	if err := s.private.CheckAuxiliary(historyverify.Auxiliary{Derived: t.derived, Allocators: t.allocators, Parents: t.parentCounters, Blobs: t.blobs, Bumps: t.bumps}); err != nil {
		return zero, err
	}
	if err := s.private.Finalize(ctx, h, t.records, t.acquired, bumps); err != nil {
		return zero, err
	}
	var final model.Changeset
	err := t.store.immediate(ctx, t.lease, func(c *sql.Conn) error {
		actual, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		if actual != s.binding.Current {
			return driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict}
		}
		if err = restoreEmpty(ctx, c); err != nil {
			return err
		}
		sources, err := t.prepareImportedSources(ctx, c, s.binding.Meta)
		if err != nil {
			return err
		}
		if err = t.checkCheckpointGuards(ctx, c); err != nil {
			return err
		}
		if err = t.publishAllocatorJournal(ctx, c, s.binding.Meta, true); err != nil {
			return err
		}
		if err = s.private.Rows(ctx, func(row historystream.Row) error {
			id, err := recordKey(row.Ref)
			if err != nil {
				return err
			}
			if source := sources.Records[id]; source != nil {
				source.Revision = row.Revision
			}
			revision := revKey(mustUint(row.Revision))
			if row.Deleted {
				_, err = c.ExecContext(ctx, `DELETE FROM records WHERE key=?`, id)
			} else {
				image, e := s.private.RecordJSON(ctx, row)
				if e != nil {
					return e
				}
				if err = s.private.WithImage(ctx, image, func(body []byte) error {
					_, e := c.ExecContext(ctx, `INSERT INTO records(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data`, id, body)
					return e
				}); err != nil {
					return err
				}
			}
			if err != nil {
				return err
			}
			if _, err = c.ExecContext(ctx, `INSERT OR IGNORE INTO objects(key,kind) VALUES(?,?)`, id, row.Kind); err != nil {
				return err
			}
			image, err := s.private.VersionJSON(ctx, row)
			if err != nil {
				return err
			}
			if err = s.private.WithImage(ctx, image, func(versionData []byte) error {
				if _, e := c.ExecContext(ctx, `INSERT INTO versions(key,rev,base,data) VALUES(?,?,1,?)`, id, revision, versionData); e != nil {
					return e
				}
				return populateHistoricalQueryFields(ctx, c, id, revision, versionData)
			}); err != nil {
				return err
			}
			return publishKey(ctx, c, driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref}, Revision: row.Revision, Present: !row.Deleted})
		}); err != nil {
			return err
		}
		if err = sources.PublishImport(ctx, c); err != nil {
			return err
		}
		if err = rebuildQueryOrder(ctx, c); err != nil {
			return err
		}
		for _, state := range t.derived {
			state.Revision = s.binding.Meta.Head
			if err = publishKey(ctx, c, state); err != nil {
				return err
			}
		}
		for _, key := range t.bumps {
			if err = publishKey(ctx, c, driver.LockState{Key: key, Revision: s.binding.Meta.Head, Present: true}); err != nil {
				return err
			}
		}
		for prefix, a := range t.allocators {
			raw, e := json.Marshal(a)
			if e != nil {
				return e
			}
			if _, e = c.ExecContext(ctx, `INSERT INTO allocator_states(prefix,data) VALUES(?,?)`, prefix, raw); e != nil {
				return e
			}
		}
		for hash, data := range t.blobs {
			if data == nil {
				data = []byte{}
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO blobs(hash,rev,data) VALUES(?,?,?)`, string(hash), revKey(mustUint(s.binding.Meta.Head)), data); err != nil {
				return err
			}
		}
		if err = s.private.Entries(ctx, func(revision model.Revision, image historystream.Image) error {
			key := revKey(mustUint(revision))
			if _, e := c.ExecContext(ctx, `INSERT INTO changesets(rev,data) VALUES(?,?)`, key, []byte{}); e != nil {
				return e
			}
			if e := s.private.Copy(ctx, image, func(part []byte) error {
				_, e := c.ExecContext(ctx, `UPDATE changesets SET data=CAST(data || ? AS BLOB) WHERE rev=?`, part, key)
				return e
			}); e != nil {
				return e
			}
			if revision == s.binding.Meta.Head {
				return json.NewDecoder(historystream.ContextReader(ctx, s.private.Reader(image))).Decode(&final)
			}
			return nil
		}); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE meta SET store_id=?,incarnation=?,head=?,history_floor=? WHERE singleton=1`, string(s.binding.Meta.StoreID), s.binding.Meta.Incarnation, revKey(mustUint(s.binding.Meta.Head)), revKey(mustUint(s.binding.Meta.HistoryFloor))); err != nil {
			return err
		}
		if err = t.publishCheckpoints(ctx, c); err != nil {
			return err
		}
		raw, err := json.Marshal(final)
		if err != nil {
			return err
		}
		if err = t.store.saveReplay(ctx, c, h, s.binding.Meta.Head, raw); err != nil {
			return err
		}
		return s.close()
	})
	if err == nil {
		t.finish()
		return final, nil
	}
	var uncertain commitError
	if errors.As(err, &uncertain) {
		t.finish()
		return zero, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	return zero, err
}

var _ driver.OfflineHistoryStreamStage = (*historyStreamStage)(nil)
