package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func selectionBackfillRows(rows []driver.DerivedSelectionRow, pending []driver.SelectionProjectionFamily) []driver.DerivedSelectionRow {
	var selected []driver.DerivedSelectionRow
	for _, row := range rows {
		for _, family := range pending {
			if family.Matches(row) {
				selected = append(selected, row)
				break
			}
		}
	}
	return selected
}

// Core may declare every family while only a subset needs reconstruction.
// Filter both metadata and decoder outputs so already-ready families keep
// their physical rows and historical floors untouched.
func selectionBackfillProject(original driver.GraphSourceProjection, pending []driver.SelectionProjectionFamily) driver.GraphSourceProjection {
	project := original
	project.Families = append([]driver.SelectionProjectionFamily(nil), pending...)
	project.Decode = func(record driver.Record, inputs []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
		rows, err := original.Decode(record, inputs)
		if err != nil {
			return nil, err
		}
		return selectionBackfillRows(rows, pending), nil
	}
	return project
}

func clearSelectionFamilyRow(ctx context.Context, c *sql.Conn, incarnation, at string, family driver.SelectionProjectionFamily, row, start string) error {
	branch := revKey(0)
	if family.Kind == "" {
		if start == at {
			_, err := c.ExecContext(ctx, `DELETE FROM selection_bead_history WHERE incarnation=? AND branch=? AND derived_key=? AND start_revision=?`, incarnation, branch, row, start)
			return err
		}
		_, err := c.ExecContext(ctx, `UPDATE selection_bead_history SET end_revision=? WHERE incarnation=? AND branch=? AND derived_key=? AND start_revision=?`, at, incarnation, branch, row, start)
		return err
	}
	if start == at {
		if _, err := c.ExecContext(ctx, `DELETE FROM selection_facts WHERE incarnation=? AND branch=? AND row_key=? AND start_revision=?`, incarnation, branch, row, start); err != nil {
			return err
		}
		_, err := c.ExecContext(ctx, `DELETE FROM selection_fact_sets WHERE incarnation=? AND branch=? AND row_key=? AND start_revision=?`, incarnation, branch, row, start)
		return err
	}
	_, err := c.ExecContext(ctx, `UPDATE selection_fact_sets SET end_revision=? WHERE incarnation=? AND branch=? AND row_key=? AND start_revision=?`, at, incarnation, branch, row, start)
	return err
}

// A page replaces the current physical rows for its source at the pinned
// revision. Earlier intervals are preserved; same-revision unready rows from
// an interrupted or partial legacy publication are replaced. The coverage
// marker stays unbuilt until all pages and the final Head check complete.
func closeSelectionBackfillSource(ctx context.Context, c *sql.Conn, incarnation, at, source string, pending []driver.SelectionProjectionFamily) error {
	branch := revKey(0)
	for _, family := range pending {
		if family.Kind == "" {
			if _, err := c.ExecContext(ctx, `DELETE FROM selection_bead_history WHERE incarnation=? AND branch=? AND record_key=? AND start_revision=? AND end_revision IS NULL`, incarnation, branch, source, at); err != nil {
				return err
			}
			if _, err := c.ExecContext(ctx, `UPDATE selection_bead_history SET end_revision=? WHERE incarnation=? AND branch=? AND record_key=? AND start_revision<? AND end_revision IS NULL`, at, incarnation, branch, source, at); err != nil {
				return err
			}
			continue
		}
		if _, err := c.ExecContext(ctx, `DELETE FROM selection_facts WHERE incarnation=? AND branch=? AND start_revision=? AND row_key IN (
			SELECT row_key FROM selection_fact_sets WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=? AND row_table=? AND kind=?)`,
			incarnation, branch, at, incarnation, branch, source, at, string(family.Table), string(family.Kind)); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, `DELETE FROM selection_fact_sets WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=? AND row_table=? AND kind=?`,
			incarnation, branch, source, at, string(family.Table), string(family.Kind)); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, `UPDATE selection_fact_sets SET end_revision=? WHERE incarnation=? AND branch=? AND source_key=? AND start_revision<? AND end_revision IS NULL AND row_table=? AND kind=?`,
			at, incarnation, branch, source, at, string(family.Table), string(family.Kind)); err != nil {
			return err
		}
	}
	return nil
}

func ensureSelectionBackfillSource(ctx context.Context, c *sql.Conn, incarnation, at, source string, record driver.Record) (bool, error) {
	var revision, digest string
	var present int
	err := c.QueryRowContext(ctx, `SELECT source_revision,source_present,source_digest FROM selection_source_history
		WHERE incarnation=? AND branch=? AND source_key=? AND start_revision<=?
		AND (end_revision IS NULL OR end_revision>?) ORDER BY start_revision DESC LIMIT 1`,
		incarnation, revKey(0), source, at, at).Scan(&revision, &present, &digest)
	if err == nil {
		if revision != revKey(mustUint(record.Revision)) || present != 1 || digest != driver.SelectionDigest(record.Body) {
			return false, fmt.Errorf("%w: SQLite selection source history disagrees with source", driver.ErrSelectionUnavailable)
		}
		return false, nil
	}
	if err != sql.ErrNoRows {
		return false, err
	}
	_, err = c.ExecContext(ctx, `INSERT INTO selection_source_history(
		incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
		VALUES(?,?,?,?,?,?,?)`, incarnation, revKey(0), source, at, revKey(mustUint(record.Revision)), 1, driver.SelectionDigest(record.Body))
	return err == nil, err
}

func publishSelectionBackfillRow(ctx context.Context, c *sql.Conn, incarnation, at string, row driver.DerivedSelectionRow) error {
	if err := row.Validate(); err != nil {
		return err
	}
	index := row.Row.Index
	id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}})
	branch := revKey(0)
	if index.Bead != nil {
		b := index.Bead
		var project, parent any
		var status any = b.Status
		var title any = b.Title
		var template any = b.Template
		var priority any = b.Priority
		for _, absent := range b.Absent {
			switch absent {
			case driver.SelectionStatus:
				status = nil
			case driver.SelectionTitle:
				title = nil
			case driver.SelectionTemplate:
				template = nil
			case driver.SelectionPriority:
				priority = nil
			}
		}
		if b.Project != nil {
			project = *b.Project
		}
		if b.Parent != nil {
			parent = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: b.Parent})
		}
		_, err := c.ExecContext(ctx, `INSERT INTO selection_bead_history(
			incarnation,branch,derived_key,record_key,start_revision,bead_id,bead_type,project,status,title,template,parent_key,priority,membership_leaf,revision_text,source_revision,source_digest)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, incarnation, branch, id, keyID(index.Source.Key), at, b.Ref.ID, b.Type, project, status, title, template, parent, priority, b.MembershipLeaf, index.Source.Revision.String(), revKey(mustUint(index.Source.Revision)), index.Source.Digest)
		return err
	}
	f := index.Facts
	if f == nil {
		return fmt.Errorf("%w: missing SQLite selection backfill facts", driver.ErrSelectionUnavailable)
	}
	present := 0
	if index.Source.Present {
		present = 1
	}
	if _, err := c.ExecContext(ctx, `INSERT INTO selection_fact_sets(
		incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, incarnation, branch, id, string(row.Table), string(f.Kind), at, keyID(index.Source.Key), revKey(mustUint(index.Source.Revision)), present, index.Source.Digest); err != nil {
		return err
	}
	for ordinal, fact := range f.Rows {
		var ref, target, link any
		if fact.Ref != nil {
			ref = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: fact.Ref})
		}
		if fact.Target != nil {
			target = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: fact.Target})
		}
		if fact.Link != nil {
			target = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &fact.Link.To})
			link = fact.Link.Kind
		}
		active := 0
		if fact.Active {
			active = 1
		}
		var scopeKind, projectName any
		if fact.Kind == driver.SelectionScopedGrantFact {
			scopeKind = string(fact.ScopeKind)
			if fact.ProjectName != nil {
				projectName = *fact.ProjectName
			}
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO selection_facts(
			incarnation,branch,row_key,start_revision,ordinal,kind,ref_key,target_key,link_kind,subject,scope,role,scope_kind,project_name,name,value,active,depth)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, incarnation, branch, id, at, ordinal, string(fact.Kind), ref, target, link, fact.Subject, fact.Scope, fact.Role, scopeKind, projectName, fact.Name, fact.Value, active, int64(fact.Depth)); err != nil {
			return err
		}
	}
	return nil
}

func deleteSelectionBackfillRow(ctx context.Context, c *sql.Conn, incarnation, at, table, key string) error {
	branch := revKey(0)
	switch table {
	case "bead":
		_, err := c.ExecContext(ctx, `DELETE FROM selection_bead_history WHERE incarnation=? AND branch=? AND derived_key=? AND start_revision=?`, incarnation, branch, key, at)
		return err
	case "facts":
		if _, err := c.ExecContext(ctx, `DELETE FROM selection_facts WHERE incarnation=? AND branch=? AND row_key=? AND start_revision=?`, incarnation, branch, key, at); err != nil {
			return err
		}
		_, err := c.ExecContext(ctx, `DELETE FROM selection_fact_sets WHERE incarnation=? AND branch=? AND row_key=? AND start_revision=?`, incarnation, branch, key, at)
		return err
	case "source":
		_, err := c.ExecContext(ctx, `DELETE FROM selection_source_history WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`, incarnation, branch, key, at)
		return err
	default:
		return fmt.Errorf("%w: invalid SQLite selection backfill ledger", driver.ErrSelectionUnavailable)
	}
}
