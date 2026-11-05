package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// At most five bindings per row keep every statement below the older 999-variable
// SQLite limit. Every row retains its original JSON envelope and row triggers;
// all batches run on Commit's same fenced immediate connection.
const keyPublicationBatchSize = 128

type keyPublicationStatement interface {
	ExecContext(context.Context, ...any) (sql.Result, error)
	Close() error
}

type keyPublicationPreparer interface {
	prepareKeyPublication(context.Context, string) (keyPublicationStatement, error)
}

func prepareKeyPublication(ctx context.Context, c selectionPublicationExecutor, query string) (keyPublicationStatement, error) {
	if preparer, ok := c.(keyPublicationPreparer); ok {
		return preparer.prepareKeyPublication(ctx, query)
	}
	if conn, ok := c.(*sql.Conn); ok {
		return conn.PrepareContext(ctx, query)
	}
	return nil, errors.New("sqlite: key publication requires the held prepared connection")
}

type keyPublicationBatch struct {
	derived bool
	args    []any
	full    keyPublicationStatement
}

func closeKeyPublicationStatement(statement keyPublicationStatement, prior *error) {
	if err := statement.Close(); err != nil && *prior == nil {
		*prior = err
	}
}

func (b *keyPublicationBatch) close(prior *error) {
	if b.full != nil {
		closeKeyPublicationStatement(b.full, prior)
	}
}

func (b *keyPublicationBatch) add(ctx context.Context, c selectionPublicationExecutor, state driver.LockState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	b.args = append(b.args, keyID(state.Key), revKey(mustUint(state.Revision)), data)
	if b.derived {
		present, digest, err := derivedSourceMetadata(state)
		if err != nil {
			return err
		}
		b.args = append(b.args, present, digest)
	}
	if len(b.args) == b.width()*keyPublicationBatchSize {
		return b.flush(ctx, c)
	}
	return nil
}

func (b *keyPublicationBatch) width() int {
	if b.derived {
		return 5
	}
	return 3
}

func (b *keyPublicationBatch) flush(ctx context.Context, c selectionPublicationExecutor) (err error) {
	if len(b.args) == 0 {
		return nil
	}
	full := len(b.args) == b.width()*keyPublicationBatchSize
	statement := b.full
	if statement == nil || !full {
		columns, placeholders := "key,rev,data", "(?,?,?),"
		if b.derived {
			columns, placeholders = "key,rev,data,derived_source_present,derived_source_digest", "(?,?,?,?,?),"
		}
		query := `INSERT INTO key_history(` + columns + `) VALUES ` +
			strings.TrimSuffix(strings.Repeat(placeholders, len(b.args)/b.width()), ",")
		statement, err = prepareKeyPublication(ctx, c, query)
		if err != nil {
			return err
		}
		if full {
			b.full = statement
		} else {
			defer closeKeyPublicationStatement(statement, &err)
		}
	}
	if _, err = statement.ExecContext(ctx, b.args...); err != nil {
		return err
	}
	b.args = b.args[:0]
	return nil
}

// Reuse the full-batch statement only within this publication on Commit's held
// immediate connection. Full and tail handles close before the helper returns;
// a cleanup error cannot hide an earlier prepare, execution or cancellation error.
func publishDerivedKeys(ctx context.Context, c selectionPublicationExecutor, revision model.Revision, derived map[string]driver.LockState) (err error) {
	batch := keyPublicationBatch{derived: true}
	defer batch.close(&err)
	for _, state := range derived {
		state.Revision = revision
		if err := batch.add(ctx, c, state); err != nil {
			return err
		}
	}
	return batch.flush(ctx, c)
}

func publishBumpKeys(ctx context.Context, c selectionPublicationExecutor, revision model.Revision, bumps map[string]driver.LockKey) (err error) {
	var batch keyPublicationBatch
	defer batch.close(&err)
	for _, key := range bumps {
		state := driver.LockState{Key: key, Revision: revision, Present: true}
		if err := batch.add(ctx, c, state); err != nil {
			return err
		}
	}
	return batch.flush(ctx, c)
}
