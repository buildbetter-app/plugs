package sourceindex

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// IsSelectionBackfillUnavailable identifies an optional selection refusal. It
// classifies selection operations, never graph decoder/publication errors. A joined
// cancellation, lease, SQL or cleanup failure must not be hidden by a refusal.
func IsSelectionBackfillUnavailable(err error) bool {
	return driver.IsSelectionBackfillUnavailable(err)
}

// SelectionBackfillRefused reports optional refusal AFTER the graph lifecycle
// completed. The engine must publish that graph transaction before returning
// this error. An arbitrary graph ErrUnsupported never has this completion tag.
type SelectionBackfillRefused = driver.SelectionBackfillRefused

func NewSelectionBackfillRefused(cause error) error {
	return driver.NewSelectionBackfillRefused(cause)
}

func IsSelectionBackfillRefused(err error) bool {
	return driver.IsSelectionBackfillRefused(err)
}

// Independent retains one stopped flag across the entire optional lifecycle.
// Each SQL step has its own savepoint: a refused activation cannot leave a
// prefix of family markers, or roll back the graph markers written before it.
// Clear may use separate short transactions, so it wraps each publication and
// lets a refusal unwind Clear before suppressing that optional result.
func (s BackfillSelection) Independent() BackfillSelection {
	if s.refusal != nil {
		return s
	}
	stopped := s.Clear == nil || s.Page == nil || s.Activate == nil
	var refusal error
	if stopped {
		refusal = driver.ErrSelectionUnavailable
	}
	finish := func(err error) error {
		if IsSelectionBackfillUnavailable(err) {
			stopped = true
			refusal = err
			return nil
		}
		return err
	}
	return BackfillSelection{
		refusal: &refusal,
		Clear: func(ctx context.Context, publish BackfillPublication) error {
			if stopped {
				return nil
			}
			err := s.Clear(ctx, func(ctx context.Context, fn func(SQL) error) error {
				return publish(ctx, func(q SQL) error { return selectionBackfillStep(ctx, q, fn) })
			})
			return finish(err)
		},
		Page: func(ctx context.Context, q SQL, records []driver.Record, metadata []driver.GraphSourceMetadata) error {
			if stopped {
				return nil
			}
			return finish(selectionBackfillStep(ctx, q, func(q SQL) error { return s.Page(ctx, q, records, metadata) }))
		},
		Activate: func(ctx context.Context, q SQL) error {
			if stopped {
				return nil
			}
			return finish(selectionBackfillStep(ctx, q, func(q SQL) error { return s.Activate(ctx, q) }))
		},
	}
}

// Refusal is readable after the lifecycle finishes. A caller composing the
// short-publication Plan methods returns it only after graph activation commits.
func (s BackfillSelection) Refusal() error {
	if s.refusal == nil {
		return nil
	}
	return *s.refusal
}

type selectionBackfillSQLFailure struct {
	operation string
	cause     error
}

func (e *selectionBackfillSQLFailure) Error() string {
	return fmt.Sprintf("selection savepoint %s: %v", e.operation, e.cause)
}

func (e *selectionBackfillSQLFailure) Unwrap() error { return e.cause }

func (e *selectionBackfillSQLFailure) SelectionBackfillFatal() {}

func selectionBackfillStep(ctx context.Context, q SQL, fn func(SQL) error) error {
	const savepoint = "plug_optional_selection_backfill"
	if _, err := q.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return &selectionBackfillSQLFailure{operation: "creation", cause: err}
	}
	err := fn(q)
	cleanup := context.WithoutCancel(ctx)
	if err != nil {
		if _, rollbackErr := q.ExecContext(cleanup, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			return errors.Join(err, &selectionBackfillSQLFailure{operation: "rollback", cause: rollbackErr})
		}
	}
	_, releaseErr := q.ExecContext(cleanup, "RELEASE SAVEPOINT "+savepoint)
	if releaseErr != nil {
		return errors.Join(err, &selectionBackfillSQLFailure{operation: "release", cause: releaseErr})
	}
	return err
}
