package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Derived source metadata shares the required key_history row. It is eager for
// every derived state, including unannotated Ready values and tombstones.
// Neither demand discovery nor decoding a value is part of selection reads.
func derivedSourceMetadata(state driver.LockState) (int, string, error) {
	if !state.Present {
		if state.Derived != nil {
			return 0, "", fmt.Errorf("%w: SQLite derived tombstone has a value", driver.ErrUnsupported)
		}
		return 0, "", nil
	}
	if state.Derived == nil {
		return 0, "", fmt.Errorf("%w: SQLite derived source value missing", driver.ErrUnsupported)
	}
	return 1, driver.SelectionDigest(state.Derived.Value), nil
}

func checkedDerivedSource(revision string, present int64, digest string, at uint64) (driver.LockState, string, error) {
	observed, err := parseRev(revision)
	if err != nil {
		return driver.LockState{}, "", err
	}
	if revision != revKey(mustUint(observed)) || mustUint(observed) > at || (present != 0 && present != 1) ||
		(present == 0 && digest != "") || (present == 1 && (len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "")) {
		return driver.LockState{}, "", fmt.Errorf("%w: SQLite malformed derived source metadata", driver.ErrUnsupported)
	}
	return driver.LockState{Revision: observed, Present: present == 1}, digest, nil
}

// A NULL pair is a legacy representation, not an absent source. The covering
// thin interval must be unique and bind the exact selected SQL mutation token.
func legacyDerivedSource(ctx context.Context, q queryer, incarnation, id string, at uint64) (state driver.LockState, digest string, found bool, err error) {
	rows, err := q.QueryContext(ctx, `SELECT source_revision,source_present,source_digest
		FROM selection_source_history WHERE incarnation=? AND branch=? AND source_key=?
		AND start_revision<=? AND (end_revision IS NULL OR end_revision>?)
		ORDER BY start_revision DESC LIMIT 2`, incarnation, revKey(0), id, revKey(at), revKey(at))
	if err != nil {
		return state, "", false, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	if !rows.Next() {
		return state, "", false, rows.Err()
	}
	var revision string
	var present int64
	if err = rows.Scan(&revision, &present, &digest); err != nil {
		return state, "", false, err
	}
	state, digest, err = checkedDerivedSource(revision, present, digest, at)
	if err != nil {
		return state, "", false, err
	}
	if rows.Next() {
		return state, "", false, fmt.Errorf("%w: SQLite overlapping derived source intervals", driver.ErrSelectionUnavailable)
	}
	return state, digest, true, rows.Err()
}

func projectedDerivedSource(ctx context.Context, q queryer, incarnation, id string, at uint64) (driver.LockState, string, error) {
	var revision string
	var present sql.NullInt64
	var digest sql.NullString
	err := q.QueryRowContext(ctx, `SELECT rev,derived_source_present,derived_source_digest
		FROM key_history WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`, id, revKey(at)).Scan(&revision, &present, &digest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return driver.LockState{}, "", err
	}
	if err == nil && (present.Valid || digest.Valid) {
		if !present.Valid || !digest.Valid {
			return driver.LockState{}, "", fmt.Errorf("%w: SQLite partial derived source metadata", driver.ErrUnsupported)
		}
		return checkedDerivedSource(revision, present.Int64, digest.String, at)
	}
	legacy, legacyDigest, found, legacyErr := legacyDerivedSource(ctx, q, incarnation, id, at)
	if legacyErr != nil {
		return driver.LockState{}, "", legacyErr
	}
	if errors.Is(err, sql.ErrNoRows) && !found {
		return driver.LockState{Revision: model.Natural64(0)}, "", nil
	}
	if err != nil || !found || revision != revKey(mustUint(legacy.Revision)) {
		return driver.LockState{}, "", fmt.Errorf("%w: SQLite retained derived source observation missing or changed", driver.ErrSelectionUnavailable)
	}
	return legacy, legacyDigest, nil
}

// Open calls this under its publication lock and BEGIN IMMEDIATE, before any
// version stamp. The minimal published v1 database has no key_history table.
// Existing rows remain NULL/NULL and retain their exact legacy thin witnesses.
func migrateDerivedSourceMetadata(ctx context.Context, c *sql.Conn, version int) (err error) {
	type column struct {
		kind             string
		notNull, primary int
		defaultValue     sql.NullString
	}
	columns := make(map[string]column)
	rows, err := c.QueryContext(ctx, `PRAGMA table_info(key_history)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var ordinal int
		var name string
		var col column
		if err = rows.Scan(&ordinal, &name, &col.kind, &col.notNull, &col.defaultValue, &col.primary); err != nil {
			_ = rows.Close()
			return err
		}
		columns[name] = col
	}
	err = rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	badShape := fmt.Errorf("%w: sqlite key_history schema does not match version %d", driver.ErrUnsupported, version)
	if len(columns) == 0 {
		if version == schemaVersion {
			return badShape
		}
		_, err = c.ExecContext(ctx, `CREATE TABLE key_history (
			key TEXT NOT NULL, rev TEXT NOT NULL, data BLOB NOT NULL,
			derived_source_present INTEGER, derived_source_digest TEXT, PRIMARY KEY(key,rev))`)
		return err
	}
	for name, expected := range map[string]column{
		"key":  {kind: "TEXT", notNull: 1, primary: 1},
		"rev":  {kind: "TEXT", notNull: 1, primary: 2},
		"data": {kind: "BLOB", notNull: 1},
	} {
		actual, ok := columns[name]
		if !ok || actual != expected {
			return badShape
		}
	}
	p, hasPresent := columns["derived_source_present"]
	d, hasDigest := columns["derived_source_digest"]
	if version == schemaVersion {
		if !hasPresent || !hasDigest || p != (column{kind: "INTEGER"}) || d != (column{kind: "TEXT"}) || len(columns) != 5 {
			return badShape
		}
		return nil
	}
	if hasPresent || hasDigest || len(columns) != 3 {
		return badShape
	}
	if _, err = c.ExecContext(ctx, `ALTER TABLE key_history ADD COLUMN derived_source_present INTEGER`); err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `ALTER TABLE key_history ADD COLUMN derived_source_digest TEXT`)
	return err
}
