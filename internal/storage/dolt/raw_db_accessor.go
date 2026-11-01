package dolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
)

var _ storage.RawDBTxAccessor = (*DoltStore)(nil)

// WithRawDBTx lends a transaction while protecting the connection from Close.
// It does not retry fn: a raw census callback may have external side effects.
func (s *DoltStore) WithRawDBTx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) (err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return ErrStoreClosed
	}
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin raw read tx: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	return fn(tx)
}
