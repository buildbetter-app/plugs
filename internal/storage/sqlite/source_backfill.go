package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func (s *Store) buildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, preparationRefusal error) (err error) {
	if decode == nil {
		return fmt.Errorf("source index backfill requires a core decoder")
	}
	value, err := s.Write(ctx, lease)
	if err != nil {
		return err
	}
	defer value.Close()
	if _, err = value.Lock(ctx, driver.LockSet{{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate}}); err != nil {
		return err
	}
	if _, err = value.Head(ctx); err != nil {
		return err
	}
	t := value.(*transaction)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err = t.check(ctx); err != nil {
		return err
	}
	plan, err := sourceindex.SQLite.PrepareBackfill(ctx, t.tx, t.meta)
	if err != nil {
		return err
	}
	if !plan.ParentActive && !plan.LinksActive {
		if preparationRefusal != nil {
			return sourceindex.NewSelectionBackfillRefused(preparationRefusal)
		}
		return nil
	}
	// Retain graph/Head and the pinned source snapshot, not the publication
	// lock or BEGIN IMMEDIATE, while decoding. Heartbeats can publish between
	// bounded index pages. Markers remain absent/unbuilt until the final fence.
	publish := func(pageCtx context.Context, fn func(sourceindex.SQL) error) error {
		return s.immediate(pageCtx, t.lease, func(c *sql.Conn) error {
			meta, err := readMeta(pageCtx, c)
			if err != nil {
				return err
			}
			if meta != t.meta {
				return fmt.Errorf("%w: source backfill head changed", driver.ErrUnsupported)
			}
			return fn(c)
		})
	}
	defer func() {
		if err == nil || sourceindex.IsSelectionBackfillRefused(err) {
			return
		}
		// Roll back unpublished pages even on cancellation/lease loss. t.mu
		// retains graph/Head until cleanup finishes, including against the lease
		// watcher and a new holder. No public record or ready domain is changed.
		// If final COMMIT was indeterminate but actually activated coverage, keep
		// its complete indexes rather than deleting rows below a ready marker.
		cleanupCtx := context.WithoutCancel(ctx)
		cleanup := plan
		coverage, cleanupErr := sourceindex.SQLite.ReadCoverage(cleanupCtx, s.db, plan.Incarnation)
		if cleanupErr == nil {
			cleanup.ParentActive = plan.ParentActive && !coverage.Parent.Ready
			cleanup.LinksActive = plan.LinksActive && !coverage.Links.Ready
			cleanupErr = cleanup.ClearBackfill(cleanupCtx, func(cleanupCtx context.Context, fn func(sourceindex.SQL) error) error {
				return s.immediate(cleanupCtx, nil, func(c *sql.Conn) error { return fn(c) })
			})
		}
		err = errors.Join(err, cleanupErr)
	}()
	if err = plan.ClearBackfill(ctx, publish); err != nil {
		return err
	}
	if err = plan.BuildBackfill(ctx, t.tx, decode, publish); err != nil {
		return err
	}
	if err = publish(ctx, func(q sourceindex.SQL) error { return plan.ActivateBackfill(ctx, q) }); err != nil {
		return err
	}
	if preparationRefusal != nil {
		return sourceindex.NewSelectionBackfillRefused(preparationRefusal)
	}
	return nil
}

var _ driver.GraphSourceIndexBuilder = (*Store)(nil)
