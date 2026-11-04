package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are the four independent families constructed from complete current
// bead sources. Other fact tables keep their own exact envelope admission.
func sqliteSelectionFamilies() []driver.SelectionProjectionFamily {
	return []driver.SelectionProjectionFamily{
		{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAttribute, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedReferenceIndex, Kind: driver.SelectionLinkFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion},
	}
}

func sqliteSourceBuiltFamily(row driver.DerivedRowKey, kind driver.SelectionFactKind) (driver.SelectionProjectionFamily, bool) {
	for _, family := range sqliteSelectionFamilies()[1:] {
		if row.Table == family.Table && kind == family.Kind {
			return family, true
		}
	}
	return driver.SelectionProjectionFamily{}, false
}

type sqliteFamilyInterval struct {
	first, through, state string
}

// The latest interval at a token wins. A failed or unbuilt later interval
// cannot make an earlier ready interval claim that token, while that earlier
// interval remains available to genuine historical requests.
func selectionFamilyInterval(ctx context.Context, q queryer, incarnation string, family driver.SelectionProjectionFamily, at string) (sqliteFamilyInterval, bool, error) {
	if err := family.Validate(); err != nil {
		return sqliteFamilyInterval{}, false, err
	}
	var format int
	var interval sqliteFamilyInterval
	err := q.QueryRowContext(ctx, `SELECT format,built_from,certified_through,state
		FROM selection_family_coverage WHERE incarnation=? AND branch=? AND family_table=?
		AND family_kind=? AND built_from<=? ORDER BY built_from DESC LIMIT 1`,
		incarnation, revKey(0), string(family.Table), string(family.Kind), at).
		Scan(&format, &interval.first, &interval.through, &interval.state)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteFamilyInterval{}, false, nil
	}
	if err != nil {
		return sqliteFamilyInterval{}, false, err
	}
	if format != int(family.Version) || interval.first > interval.through ||
		(interval.state != "ready" && interval.state != "unbuilt") {
		return sqliteFamilyInterval{}, false, fmt.Errorf("%w: incompatible SQLite selection family coverage", driver.ErrSelectionUnavailable)
	}
	return interval, true, nil
}

func selectionFamilyReady(ctx context.Context, q queryer, incarnation string, family driver.SelectionProjectionFamily, at string) (bool, error) {
	interval, found, err := selectionFamilyInterval(ctx, q, incarnation, family, at)
	return found && err == nil && interval.state == "ready" && interval.through >= at, err
}

func (r *reader) requireSelectionFamily(ctx context.Context, family driver.SelectionProjectionFamily, at uint64) error {
	ready, err := selectionFamilyReady(ctx, r.tx, r.meta.Incarnation, family, revKey(at))
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("%w: SQLite selection family %s/%s unavailable at %d", driver.ErrSelectionUnavailable, family.Table, family.Kind, at)
	}
	return nil
}
