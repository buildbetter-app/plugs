package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The caller holds Head. Commit repeats this over its BEGIN IMMEDIATE view,
// including receipts and checkpoints that do not advance the graph head.
func restoreEmpty(ctx context.Context, q queryer) error {
	m, err := readMeta(ctx, q)
	if err != nil {
		return err
	}
	if !m.Head.IsZero() || !m.HistoryFloor.IsZero() {
		return driver.ErrRestoreTargetNotEmpty
	}
	var occupied bool
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM records)
 OR EXISTS(SELECT 1 FROM versions) OR EXISTS(SELECT 1 FROM key_history)
 OR EXISTS(SELECT 1 FROM blobs) OR EXISTS(SELECT 1 FROM changesets)
 OR EXISTS(SELECT 1 FROM replays) OR EXISTS(SELECT 1 FROM checkpoints)
 OR EXISTS(SELECT 1 FROM allocator_states)`).Scan(&occupied)
	if err == nil && occupied {
		return driver.ErrRestoreTargetNotEmpty
	}
	return err
}

func (t *transaction) PrepareOfflineRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prepareRestore(ctx, source)
}

func (t *transaction) prepareRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if !t.hasHead || t.restore != nil || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs)+len(t.allocators)+len(t.parentCounters) != 0 {
		return driver.Meta{}, fmt.Errorf("offline restore requires Head before staging")
	}
	if err := restoreEmpty(ctx, t.tx); err != nil {
		return driver.Meta{}, err
	}
	m, err := driver.NewOfflineRestoreMeta(t.meta, source)
	if err != nil {
		return driver.Meta{}, err
	}
	t.restore = &m
	return m, nil
}

func (t *transaction) PrepareNativeRestore(ctx context.Context, profile driver.NativeRestoreProfile) (driver.Meta, error) {
	bound, err := profile.Detached()
	if err != nil {
		return driver.Meta{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.nativeAllocatorLocks(bound.AllocatorBinding); err != nil {
		return driver.Meta{}, err
	}
	meta, err := t.prepareRestore(ctx, bound.StoreID)
	if err == nil {
		t.nativeRestore = &bound
	}
	return meta, err
}

var _ driver.OfflineRestoreTx = (*transaction)(nil)
var _ driver.NativeRestoreTx = (*transaction)(nil)

func (t *transaction) PrepareRestoreImport(ctx context.Context, profile driver.RestoreImportProfile) (driver.Meta, error) {
	bound, err := profile.DetachedCoreOnly()
	if err != nil {
		return driver.Meta{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	meta, err := t.prepareRestore(ctx, bound.SourceStoreID)
	if err == nil {
		t.importRestore = &bound
	}
	return meta, err
}

var _ driver.RestoreImportTx = (*transaction)(nil)

// StageNativeRestoreBlobs registers a source for the atomic SQLite commit.
// The input ranges are borrowed. No blob payload is retained by the staged tx.
func (t *transaction) StageNativeRestoreBlobs(ctx context.Context, source driver.NativeRestoreBlobSource) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.nativeRestore == nil || t.nativeRestoreBlobs != nil || source == nil {
		return fmt.Errorf("sqlite: native restore blob source is not admissible")
	}
	t.nativeRestoreBlobs = source
	return nil
}

func publishNativeRestoreBlobs(ctx context.Context, c *sql.Conn, revision string, source driver.NativeRestoreBlobSource) error {
	meta, err := readMeta(ctx, c)
	if err != nil {
		return err
	}
	if meta.SchemaVersion == schemaVersion {
		if _, err := c.ExecContext(ctx, `ALTER TABLE blobs ADD COLUMN stream_size INTEGER CHECK(stream_size >= 0)`); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, blobChunkSchema); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, `UPDATE meta SET schema_version=? WHERE singleton=1`, streamSchemaVersion); err != nil {
			return err
		}
	}
	buffer := make([]byte, blobChunkBytes)
	return source(func(hash driver.Hash, size int64, r io.Reader) error {
		if size < 0 {
			return fmt.Errorf("invalid native restore blob size")
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO blobs(hash,rev,data,stream_size) VALUES(?,?,X'',?)`, string(hash), revision, size); err != nil {
			return err
		}
		digest := sha256.New()
		for remaining, ordinal := size, 0; remaining > 0; ordinal++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk := buffer[:min(int64(len(buffer)), remaining)]
			if _, err := io.ReadFull(r, chunk); err != nil {
				return err
			}
			_, _ = digest.Write(chunk)
			if _, err := c.ExecContext(ctx, `INSERT INTO blob_chunks(hash,ordinal,data) VALUES(?,?,?)`, string(hash), ordinal, chunk); err != nil {
				return err
			}
			remaining -= int64(len(chunk))
		}
		if n, err := r.Read(buffer[:1]); n != 0 || err != io.EOF || hex.EncodeToString(digest.Sum(nil)) != string(hash) {
			return fmt.Errorf("native restore blob does not match validated input")
		}
		return nil
	})
}

var _ driver.NativeRestoreBlobTx = (*transaction)(nil)
