//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
)

var _ storage.RawDBTxAccessor = (*EmbeddedDoltStore)(nil)

// WithRawDBTx lends a transaction on the store's database and branch. Like
// other embedded operations, it owns one OpenSQL handle for this call only.
func (s *EmbeddedDoltStore) WithRawDBTx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) (err error) {
	if s.closed.Load() {
		return errClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db, cleanup, err := OpenSQL(ctx, s.dataDir, s.database, s.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cleanup()) }()
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("embeddeddolt: begin raw read tx: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	return fn(tx)
}
