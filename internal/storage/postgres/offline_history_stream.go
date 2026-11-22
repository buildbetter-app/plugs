package postgres

import (
	"context"
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
		return nil, fmt.Errorf("%w: PostgreSQL stream requires its own writer", driver.ErrUnsupported)
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
		return zero, t.abortFenced(err)
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
	if err := t.restoreEmpty(ctx); err != nil {
		return zero, err
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
	fail := func(err error) (model.Changeset, error) { _ = t.finish(); return zero, err }
	if err := t.fence(ctx); err != nil {
		return zero, err
	}
	actual, err := readMeta(ctx, t.sql, "")
	if err != nil {
		return fail(err)
	}
	if actual != s.binding.Current {
		return fail(driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict})
	}
	sources, err := t.prepareImportedSources(ctx, s.binding.Meta)
	if err != nil {
		return fail(err)
	}
	if err = t.checkCheckpointGuards(ctx); err != nil {
		return fail(err)
	}
	if err = t.publishAllocatorJournal(ctx, t.sql, s.binding.Meta, true); err != nil {
		return fail(err)
	}
	if err = s.private.Rows(ctx, func(row historystream.Row) error {
		id, err := recordKey(row.Ref)
		if err != nil {
			return err
		}
		if source := sources.Records[id]; source != nil {
			source.Revision = row.Revision
		}
		ref, err := encode(row.Ref)
		if err != nil {
			return err
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.records VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(key) DO UPDATE SET revision=EXCLUDED.revision,body=EXCLUDED.body,deleted=EXCLUDED.deleted,version_count=EXCLUDED.version_count`, []byte(id), ref, row.Kind, row.Revision.String(), []byte{}, row.Deleted, decimal(row.Count)); err != nil {
			return err
		}
		if !row.Deleted {
			if err = s.private.WithImage(ctx, row.Image, func(body []byte) error {
				_, e := t.sql.ExecContext(ctx, `UPDATE plug_postgres.records SET body=$2::bytea WHERE key=$1`, []byte(id), body)
				return e
			}); err != nil {
				return err
			}
		}
		image, err := s.private.VersionJSON(ctx, row)
		if err != nil {
			return err
		}
		if err = s.private.WithImage(ctx, image, func(versionData []byte) error {
			if _, e := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.versions VALUES($1,$2,true,$3)`, []byte(id), row.Revision.String(), versionData); e != nil {
				return e
			}
			return populateHistoricalQueryFields(ctx, t.sql, id, row.Revision.String(), versionData)
		}); err != nil {
			return err
		}
		return t.writeState(ctx, id, driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref}, Revision: row.Revision, Present: !row.Deleted})
	}); err != nil {
		return fail(err)
	}
	if err = sources.PublishImport(ctx, t.sql); err != nil {
		return fail(err)
	}
	for id, state := range t.derived {
		state.Revision = s.binding.Meta.Head
		if err = t.writeState(ctx, id, state); err != nil {
			return fail(err)
		}
	}
	for id, key := range t.bumps {
		if err = t.writeState(ctx, id, driver.LockState{Key: key, Revision: s.binding.Meta.Head, Present: true}); err != nil {
			return fail(err)
		}
	}
	for prefix, a := range t.allocators {
		raw, e := encode(a)
		if e != nil {
			return fail(e)
		}
		if _, e = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.allocator_states(prefix,data) VALUES($1,$2)`, prefix, raw); e != nil {
			return fail(e)
		}
	}
	for hash, data := range t.blobs {
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.blobs VALUES($1,$2,$3)`, string(hash), s.binding.Meta.Head.String(), data); err != nil {
			return fail(err)
		}
	}
	var final model.Changeset
	if err = s.private.Entries(ctx, func(revision model.Revision, image historystream.Image) error {
		if _, e := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.changesets VALUES($1,$2)`, revision.String(), []byte{}); e != nil {
			return e
		}
		if e := s.private.Copy(ctx, image, func(part []byte) error {
			_, e := t.sql.ExecContext(ctx, `UPDATE plug_postgres.changesets SET value=value || $2::bytea WHERE revision=$1`, revision.String(), part)
			return e
		}); e != nil {
			return e
		}
		if revision == s.binding.Meta.Head {
			return json.NewDecoder(historystream.ContextReader(ctx, s.private.Reader(image))).Decode(&final)
		}
		return nil
	}); err != nil {
		return fail(err)
	}
	if _, err = t.sql.ExecContext(ctx, `UPDATE plug_postgres.meta SET store_id=$1,incarnation=$2,head=$3,history_floor=$4 WHERE singleton`, string(s.binding.Meta.StoreID), s.binding.Meta.Incarnation, s.binding.Meta.Head.String(), s.binding.Meta.HistoryFloor.String()); err != nil {
		return fail(err)
	}
	response, err := responseAt(h.Response, s.binding.Meta.Head)
	if err != nil {
		return fail(err)
	}
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return fail(err)
	}
	scope, err := scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey)
	if err != nil {
		return fail(err)
	}
	if err = t.saveReceipt(ctx, scope, h, response, final); err != nil {
		return fail(err)
	}
	if err = t.publishCheckpoints(ctx); err != nil {
		return fail(err)
	}
	if err = s.close(); err != nil {
		return fail(err)
	}
	if err = t.publish(ctx, h.IdempotencyKey); err != nil {
		return zero, err
	}
	return final, nil
}

var _ driver.OfflineHistoryStreamStage = (*historyStreamStage)(nil)
