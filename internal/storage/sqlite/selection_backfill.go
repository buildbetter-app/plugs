package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

// Graph activation and selection activation are independent. In particular,
// a graph-ready store still needs to enumerate its legacy selection sources.
// The selection pass never retains graph or Head across pages: ordinary saves
// remain free to publish, and any changed Head invalidates this attempt.
func (s *Store) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projections ...driver.GraphSourceProjection) error {
	// SQLite keeps graph publication and selection reconstruction in separate
	// short fenced transactions.  Remember declaration-time selection refusal
	// before starting graph work; graph activation is still authoritative and
	// the public caller receives a completion-tagged refusal only after it has
	// committed successfully.
	var preparationRefusal error
	for _, project := range projections {
		if err := project.Validate(); err != nil {
			if sourceindex.IsSelectionBackfillUnavailable(err) {
				preparationRefusal = errors.Join(preparationRefusal, err)
				break
			}
			return err
		}
	}
	if err := s.buildGraphSourceIndexes(ctx, lease, decode, preparationRefusal); err != nil {
		return err
	}
	if preparationRefusal != nil {
		// Graph publication succeeded; expose only the explicit completion
		// result so the public startup caller can suppress this optional
		// refusal without hiding a graph or SQL failure.
		return sourceindex.NewSelectionBackfillRefused(preparationRefusal)
	}
	for _, project := range projections {
		if err := s.buildSelectionSourceIndexes(ctx, lease, decode, project); err != nil {
			if sourceindex.IsSelectionBackfillUnavailable(err) {
				return sourceindex.NewSelectionBackfillRefused(err)
			}
			return err
		}
	}
	return nil
}

type sqliteSelectionBuild struct {
	store   *Store
	lease   *activeLease
	meta    driver.Meta
	run     string
	project driver.GraphSourceProjection
	pending []driver.SelectionProjectionFamily
	decode  driver.GraphSourceDecoder
}

func (s *Store) buildSelectionSourceIndexes(ctx context.Context, value driver.ActiveLease, decode driver.GraphSourceDecoder, project driver.GraphSourceProjection) (err error) {
	if decode == nil {
		return fmt.Errorf("%w: selection backfill needs a core decoder", driver.ErrSelectionUnavailable)
	}
	if err := project.Validate(); err != nil {
		return err
	}
	lease, err := s.leaseFor(ctx, value)
	if err != nil {
		return err
	}
	// This lock excludes another builder, not normal source publications. A
	// builder interrupted by a lease loss retains it through its cleanup.
	guard, err := s.fileLock(ctx, "selection-backfill", false, lease.Lost())
	if err != nil {
		return err
	}
	defer unlockFile(guard)
	meta, err := s.Meta(ctx)
	if err != nil {
		return err
	}
	build := sqliteSelectionBuild{store: s, lease: lease, meta: meta, run: randomID(), project: project, decode: decode}
	if err = build.clearInterrupted(ctx); err != nil {
		return err
	}
	for _, family := range project.Families {
		ready, readErr := selectionFamilyReady(ctx, s.db, meta.Incarnation, family, revKey(mustUint(meta.Head)))
		if readErr != nil {
			return readErr
		}
		if !ready {
			build.pending = append(build.pending, family)
		}
	}
	if len(build.pending) == 0 {
		return nil
	}
	// A legacy family may be incomplete, but an active source-built row with
	// no live bead owner is an orphan, not a missing page. Refuse it before
	// clearing any rows. Independently complete derived-source fact envelopes
	// are outside this census and remain untouched.
	for _, family := range build.pending {
		if err := verifySQLiteSelectionOwnedOrphans(ctx, s.db, meta.Incarnation, revKey(mustUint(meta.Head)), family); err != nil {
			return err
		}
	}
	build.project = selectionBackfillProject(project, build.pending)
	if err = build.begin(ctx); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// A final COMMIT can report an uncertain outcome. Cleanup checks the
			// durable run state, leaving a successfully certified interval alone.
			err = errors.Join(err, build.cleanup(context.WithoutCancel(ctx), build.run))
		}
	}()
	if err = build.clearPending(ctx); err != nil {
		return err
	}
	if err = build.pages(ctx); err != nil {
		return err
	}
	if err = build.verifyCurrentCensus(ctx); err != nil {
		return err
	}
	return build.activate(ctx)
}

// Clear only unready family images at this Head. An old interval is closed
// exactly at Head, preserving its earlier certified history; a same-Head
// unready row is discarded. Each fenced step is small enough for lease
// renewal and ordinary source publication to interleave. Any changed Head
// aborts this attempt before it can certify the old census.
func (b sqliteSelectionBuild) clearPending(ctx context.Context) error {
	for _, family := range b.pending {
		for {
			processed := 0
			err := b.store.immediate(ctx, b.lease, func(c *sql.Conn) error {
				if err := b.checkMeta(ctx, c); err != nil {
					return err
				}
				at := revKey(mustUint(b.meta.Head))
				type key struct{ row, start string }
				var batch []key
				var rows *sql.Rows
				var err error
				if family.Kind == "" {
					rows, err = c.QueryContext(ctx, `SELECT derived_key,start_revision FROM selection_bead_history
						WHERE incarnation=? AND branch=? AND start_revision<=? AND (end_revision IS NULL OR end_revision>?)
						ORDER BY derived_key,start_revision LIMIT 64`, b.meta.Incarnation, revKey(0), at, at)
				} else {
					rows, err = c.QueryContext(ctx, `SELECT f.row_key,f.start_revision FROM selection_fact_sets f
						JOIN objects o ON o.key=f.source_key AND o.kind='bead'
						WHERE f.incarnation=? AND f.branch=? AND f.row_table=? AND f.kind=? AND f.start_revision<=?
						AND f.source_key>='0:' AND f.source_key<'0;'
						AND (f.end_revision IS NULL OR f.end_revision>?)
						ORDER BY f.row_key,f.start_revision LIMIT 64`, b.meta.Incarnation, revKey(0), string(family.Table), string(family.Kind), at, at)
				}
				if err != nil {
					return err
				}
				for rows.Next() {
					var item key
					if err := rows.Scan(&item.row, &item.start); err != nil {
						_ = rows.Close()
						return err
					}
					batch = append(batch, item)
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					return err
				}
				if err := rows.Close(); err != nil {
					return err
				}
				for _, item := range batch {
					if err := clearSelectionFamilyRow(ctx, c, b.meta.Incarnation, at, family, item.row, item.start); err != nil {
						return err
					}
				}
				processed = len(batch)
				return nil
			})
			if err != nil {
				return err
			}
			if processed == 0 {
				break
			}
		}
	}
	return nil
}

func (b sqliteSelectionBuild) checkMeta(ctx context.Context, q queryer) error {
	actual, err := readMeta(ctx, q)
	if err != nil {
		return err
	}
	if actual != b.meta {
		return fmt.Errorf("%w: selection backfill source/head changed", driver.ErrSelectionUnavailable)
	}
	return nil
}

func (b sqliteSelectionBuild) begin(ctx context.Context) error {
	return b.store.immediate(ctx, b.lease, func(c *sql.Conn) error {
		if err := b.checkMeta(ctx, c); err != nil {
			return err
		}
		at := revKey(mustUint(b.meta.Head))
		if _, err := c.ExecContext(ctx, `INSERT INTO selection_backfill_runs(run_id,incarnation,at_revision,state) VALUES(?,?,?,'building')`, b.run, b.meta.Incarnation, at); err != nil {
			return err
		}
		for _, family := range b.pending {
			ready, checkErr := selectionFamilyReady(ctx, c, b.meta.Incarnation, family, at)
			if checkErr != nil {
				return checkErr
			}
			if ready {
				return fmt.Errorf("%w: selection family activated concurrently", driver.ErrSelectionUnavailable)
			}
			if _, err := c.ExecContext(ctx, `INSERT INTO selection_family_coverage
				(incarnation,branch,family_table,family_kind,format,built_from,certified_through,state)
				VALUES(?,?,?,?,?,?,?,'unbuilt')
				ON CONFLICT(incarnation,branch,family_table,family_kind,built_from)
				DO UPDATE SET certified_through=excluded.certified_through,state='unbuilt'`,
				b.meta.Incarnation, revKey(0), string(family.Table), string(family.Kind), family.Version, at, at); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b sqliteSelectionBuild) pages(ctx context.Context) error {
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Source records and every declared opaque range share one pinned WAL
		// snapshot. The transaction is released before the fenced write step.
		tx, err := b.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		if err = b.checkMeta(ctx, tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		records, err := sourceindex.SQLite.SelectionSourcePage(ctx, tx, after)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if len(records) == 0 {
			_ = tx.Rollback()
			return nil
		}
		// The source cursor can return 128 shallow beads, but a deep closure
		// may consume the entire bounded range budget alone. Retry a smaller
		// prefix in the same snapshot; the next cursor starts after only the
		// successfully staged prefix.
		limit := len(records)
		var page *driver.SelectionProjectionPage
		var outputs [][]driver.DerivedSelectionRow
		for {
			metadata := make([]driver.GraphSourceMetadata, limit)
			for i, record := range records[:limit] {
				metadata[i], err = b.decode(copyRecord(record))
				if err != nil {
					_ = tx.Rollback()
					return err
				}
				metadata[i].Selection = selectionBackfillRows(metadata[i].Selection, b.pending)
			}
			page, err = driver.NewSelectionProjectionPage(records[:limit], metadata, b.project)
			if err == nil {
				var inputs [][]driver.DerivedRow
				inputs, err = sourceindex.SQLite.SelectionRangePage(ctx, tx, b.meta.Head, page.Ranges, b.project.MaxPageRows)
				if err == nil {
					outputs, err = page.Decode(inputs, b.meta.Incarnation)
				}
			}
			if err == nil || limit == 1 || !errors.Is(err, driver.ErrSelectionUnavailable) {
				break
			}
			limit = (limit + 1) / 2
		}
		_ = tx.Rollback()
		if err != nil {
			return err
		}
		if err = b.publishPage(ctx, page.Records, outputs); err != nil {
			return err
		}
		last := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &records[limit-1].Ref})
		if last <= after {
			return fmt.Errorf("%w: unordered SQLite selection source page", driver.ErrSelectionUnavailable)
		}
		after = last
	}
}

func (b sqliteSelectionBuild) publishPage(ctx context.Context, records []driver.Record, outputs [][]driver.DerivedSelectionRow) error {
	if len(records) != len(outputs) {
		return fmt.Errorf("%w: incomplete SQLite selection page", driver.ErrSelectionUnavailable)
	}
	type event struct {
		record *driver.Record
		row    *driver.DerivedSelectionRow
	}
	events := make([]event, 0, len(records)*5)
	for i := range records {
		events = append(events, event{record: &records[i]})
		for j := range outputs[i] {
			events = append(events, event{row: &outputs[i][j]})
		}
	}
	// A deep closure may have thousands of rows. Its first source event and
	// subsequent row batches remain unavailable until the final marker, while
	// each individual writer step is capped and lease-fenced.
	const stepRows = 64
	for first := 0; first < len(events); first += stepRows {
		last := min(first+stepRows, len(events))
		if err := b.store.immediate(ctx, b.lease, func(c *sql.Conn) error {
			if err := b.checkMeta(ctx, c); err != nil {
				return err
			}
			at := revKey(mustUint(b.meta.Head))
			for _, item := range events[first:last] {
				if item.record != nil {
					record := item.record
					source := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
					if err := closeSelectionBackfillSource(ctx, c, b.meta.Incarnation, at, source, b.pending); err != nil {
						return err
					}
					inserted, sourceErr := ensureSelectionBackfillSource(ctx, c, b.meta.Incarnation, at, source, *record)
					if sourceErr != nil {
						return sourceErr
					}
					if inserted {
						if err := b.track(ctx, c, "source", source); err != nil {
							return err
						}
					}
					continue
				}
				row := item.row
				if err := publishSelectionBackfillRow(ctx, c, b.meta.Incarnation, at, *row); err != nil {
					return err
				}
				kind := "facts"
				if row.Row.Index.Bead != nil {
					kind = "bead"
				}
				if err := b.track(ctx, c, kind, keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}})); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b sqliteSelectionBuild) track(ctx context.Context, c *sql.Conn, table, key string) error {
	_, err := c.ExecContext(ctx, `INSERT INTO selection_backfill_rows(run_id,row_table,row_key) VALUES(?,?,?)`, b.run, table, key)
	return err
}

// The ordered source-page scan proves that each current bead was visited.
// This check detects an older stale counting row for a deleted source before
// the final marker; it runs outside the final publication fence. Head equality
// at activation binds the observation to the complete page scan.
func (b sqliteSelectionBuild) verifyCurrentCensus(ctx context.Context) error {
	tx, err := b.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := b.checkMeta(ctx, tx); err != nil {
		return err
	}
	at := revKey(mustUint(b.meta.Head))
	for _, family := range b.pending {
		if err := verifySQLiteSelectionFamilyCensus(ctx, tx, b.meta.Incarnation, at, family); err != nil {
			return err
		}
		if family.Kind == driver.SelectionAncestorFact {
			valid, err := verifySQLiteSelectionParentChains(ctx, tx, b.meta.Incarnation, at)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("%w: SQLite selection ancestor chain incomplete", driver.ErrSelectionUnavailable)
			}
		}
	}
	return nil
}

func (b sqliteSelectionBuild) activate(ctx context.Context) error {
	return b.store.immediate(ctx, b.lease, func(c *sql.Conn) error {
		if err := b.checkMeta(ctx, c); err != nil {
			return err
		}
		at := revKey(mustUint(b.meta.Head))
		for _, family := range b.pending {
			result, updateErr := c.ExecContext(ctx, `UPDATE selection_family_coverage SET state='ready',certified_through=?
				WHERE incarnation=? AND branch=? AND family_table=? AND family_kind=? AND built_from=? AND state='unbuilt'`,
				at, b.meta.Incarnation, revKey(0), string(family.Table), string(family.Kind), at)
			if updateErr != nil {
				return updateErr
			}
			changed, countErr := result.RowsAffected()
			if countErr != nil || changed != 1 {
				return fmt.Errorf("%w: SQLite selection family marker changed", driver.ErrSelectionUnavailable)
			}
		}
		_, err := c.ExecContext(ctx, `UPDATE selection_backfill_runs SET state='ready' WHERE run_id=? AND state='building'`, b.run)
		return err
	})
}

func (b sqliteSelectionBuild) clearInterrupted(ctx context.Context) error {
	for {
		var run string
		err := b.store.db.QueryRowContext(ctx, `SELECT run_id FROM selection_backfill_runs WHERE incarnation=? AND state='building' LIMIT 1`, b.meta.Incarnation).Scan(&run)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := b.cleanup(ctx, run); err != nil {
			return err
		}
	}
}

func (b sqliteSelectionBuild) cleanup(ctx context.Context, run string) error {
	var incarnation, at, state string
	err := b.store.db.QueryRowContext(ctx, `SELECT incarnation,at_revision,state FROM selection_backfill_runs WHERE run_id=?`, run).Scan(&incarnation, &at, &state)
	if errors.Is(err, sql.ErrNoRows) || state == "ready" || state == "aborted" {
		return nil
	}
	if err != nil {
		return err
	}
	for {
		rows, err := b.store.db.QueryContext(ctx, `SELECT row_table,row_key FROM selection_backfill_rows WHERE run_id=? ORDER BY row_table,row_key LIMIT 128`, run)
		if err != nil {
			return err
		}
		var batch [][2]string
		for rows.Next() {
			var item [2]string
			if err = rows.Scan(&item[0], &item[1]); err != nil {
				break
			}
			batch = append(batch, item)
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		// The ledger names only this attempt's row identities at its pinned
		// revision. A later ordinary H+1 publication is never deleted.
		if err = b.store.immediate(ctx, nil, func(c *sql.Conn) error {
			for _, item := range batch {
				if err := deleteSelectionBackfillRow(ctx, c, incarnation, at, item[0], item[1]); err != nil {
					return err
				}
				if _, err := c.ExecContext(ctx, `DELETE FROM selection_backfill_rows WHERE run_id=? AND row_table=? AND row_key=?`, run, item[0], item[1]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return b.store.immediate(ctx, nil, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx, `UPDATE selection_backfill_runs SET state='aborted' WHERE run_id=? AND state='building'`, run)
		return err
	})
}

var _ driver.GraphSourceIndexBuilder = (*Store)(nil)
