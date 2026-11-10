// Copyright 2026 BuildBetter. Restore-only CP5 switch-back plumbing.
package embedded

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
)

// PhysicalUndoPlan requires every user table. Original is the complete typed
// source binding, including unchanged reserved tables, never an inferred schema.
type PhysicalUndoPlan struct {
	BaseCommit, Database, WorkingRootHash string
	Original                              []PhysicalTable
	Tables                                []PhysicalTable
}
type PhysicalUndoReceipt struct {
	BaseCommit, CommitID, OriginalGraphSHA256              string
	PriorCommits, CurrentCommits, DeletedRows, WrittenRows int
}

type undoGraph struct {
	Head, Branch        string
	Log, Branches, Tags PhysicalTable
}

// RestorePhysicalUndo replaces all user-table contents and adds ONE native Dolt
// commit. DOLT_COMMIT itself atomically commits the active SQL transaction; it
// is not preceded by SQL COMMIT or followed by an extra snapshot commit.
func RestorePhysicalUndo(ctx context.Context, conn *sql.Conn, plan PhysicalUndoPlan) (receipt PhysicalUndoReceipt, resultErr error) {
	owned, err := ownRestoreTables(plan.Tables)
	if err != nil {
		return receipt, err
	}
	originals, err := ownUndoOriginal(plan.Original)
	if err != nil {
		return receipt, err
	}
	if conn == nil {
		return receipt, restoreError("connection", "", "", -1, errors.New("dedicated native connection required"))
	}
	if !physicalCommitID.MatchString(plan.BaseCommit) || !physicalCommitID.MatchString(plan.WorkingRootHash) || plan.Database == "" {
		return receipt, restoreError("source-binding", "", "", -1, errors.New("exact preserved commit required"))
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	resultErr = conn.Raw(func(raw any) (scopeErr error) {
		d, ok := raw.(*DoltConn)
		if !ok {
			return restoreError("unsupported-driver", "", "", -1, errors.New("embedded Dolt connection required"))
		}
		if d.gmsCtx.GetTransaction() != nil {
			return restoreError("occupied-transaction", "", "", -1, errors.New("dedicated idle connection required"))
		}
		tx, err := d.BeginTx(ctx, driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelSerializable)})
		if err != nil {
			return restoreError("begin", "", "", -1, err)
		}
		committed := false
		defer func() {
			if !committed {
				rollbackErr := tx.Rollback()
				if rollbackErr == nil && d.gmsCtx.GetTransaction() != nil {
					rollbackErr = errors.New("native transaction remains active after rollback")
				}
				if rollbackErr != nil {
					scopeErr = restoreError("rollback-indeterminate", "", "", -1, errors.Join(scopeErr, rollbackErr))
				}
			}
		}()
		// Authenticate the complete original working root before SQL deletion.
		observed, err := d.readPhysicalWorkingRoot(ctx, plan.BaseCommit)
		if err != nil {
			return err
		}
		if observed.Database != plan.Database || observed.WorkingRootHash != plan.WorkingRootHash || !observed.Complete {
			return restoreError("source-working-root", "", "*", -1, errors.New("preserved database/working root differs from typed capture"))
		}
		before, err := d.restoreWorkingImage(ctx)
		if err != nil {
			return err
		}
		graph, err := d.undoGraph(ctx)
		if err != nil {
			return err
		}
		if graph.Head != plan.BaseCommit {
			return restoreError("source-head", "", "", -1, errors.New("preserved HEAD differs from original typed source"))
		}
		source := map[string]PhysicalTable{}
		for _, table := range originals {
			if _, ok := source[table.Name]; ok {
				return restoreError("source-binding", table.Name, "", -1, errors.New("duplicate source table"))
			}
			source[table.Name] = table
		}
		selected := map[string]PhysicalTable{}
		for _, table := range owned {
			selected[table.Name] = table
		}
		for name, old := range before {
			if strings.HasPrefix(strings.ToLower(name), "dolt_") {
				continue
			}
			original, ok := source[name]
			if !ok {
				return restoreError("source-completeness", name, "*", -1, errors.New("original same-source typed user table absent; immutable capture cannot supply unversioned working-set tables"))
			}
			if row, column, equal := restoreTableDifference(original, old); !equal {
				return restoreError("source-physical-binding", name, column, row, errors.New("preserved copy differs from typed source"))
			}
			want, ok := selected[name]
			if !ok {
				return restoreError("user-table-completeness", name, "*", -1, errors.New("all user tables require explicit replacement rows, including empty tables"))
			}
			if !reflect.DeepEqual(want.Columns, old.Columns) {
				return restoreError("schema", name, "*", -1, errors.New("original exact SQL column types required"))
			}
		}
		for name := range selected {
			if _, ok := before[name]; !ok {
				return restoreError("unknown-user-table", name, "*", -1, errors.New("destination has no such physical user table"))
			}
		}
		for name, original := range source {
			old, ok := before[name]
			if !ok {
				return restoreError("source-census", name, "*", -1, errors.New("source table absent from preserved copy"))
			}
			if row, column, equal := restoreTableDifference(original, old); !equal {
				return restoreError("source-physical-binding", name, column, row, errors.New("preserved copy differs from original source cells"))
			}
		}
		if aliases, ok := before["dolt_nonlocal_tables"]; ok && len(aliases.Rows) != 0 {
			return restoreError("nonlocal-aliases", "dolt_nonlocal_tables", "*", -1, errors.New("undo cannot redirect any user-table SQL destination"))
		}
		order, err := d.undoOrder(ctx, selected)
		if err != nil {
			return err
		}
		type insertion struct {
			query string
			args  []driver.NamedValue
			table string
			row   int
		}
		var inserts []insertion
		// All expressions, cell kinds and JSON integers validate before ANY delete.
		for _, name := range order {
			table := selected[name]
			columns := make([]string, len(table.Columns))
			for i, col := range table.Columns {
				columns[i] = "`" + col.Name + "`"
			}
			for ri, row := range table.Rows {
				var args []driver.NamedValue
				expressions := make([]string, len(row))
				for ci, cell := range row {
					expr, err := restoreExpression(cell, table.Columns[ci].DatabaseTypeName, &args)
					if err != nil {
						return restoreError("cell", name, table.Columns[ci].Name, ri, err)
					}
					expressions[ci] = expr
				}
				inserts = append(inserts, insertion{"INSERT INTO `" + name + "` (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(expressions, ",") + ")", args, name, ri})
			}
		}
		for i := len(order) - 1; i >= 0; i-- {
			name := order[i]
			if err := ctx.Err(); err != nil {
				return restoreError("canceled", name, "*", -1, err)
			}
			if _, err := d.ExecContext(ctx, "DELETE FROM `"+name+"`", nil); err != nil {
				return restoreError("delete", name, "*", -1, errors.New("native SQL deletion refused"))
			}
			receipt.DeletedRows += len(before[name].Rows)
		}
		for _, insert := range inserts {
			if err := ctx.Err(); err != nil {
				return restoreError("canceled", insert.table, "*", insert.row, err)
			}
			if _, err := d.ExecContext(ctx, insert.query, insert.args); err != nil {
				if cancelErr := ctx.Err(); cancelErr != nil {
					return restoreError("canceled", insert.table, "*", insert.row, cancelErr)
				}
				return restoreError("insert", insert.table, "*", insert.row, errors.New("native SQL insertion refused"))
			}
			receipt.WrittenRows++
		}
		after, err := d.restoreWorkingImage(ctx)
		if err != nil {
			return err
		}
		if len(before) != len(after) {
			return restoreError("census", "", "*", -1, errors.New("physical table census changed"))
		}
		for name, old := range before {
			want := old
			if replacement, ok := selected[name]; ok {
				want = replacement
			}
			actual, ok := after[name]
			if !ok {
				return restoreError("physical-readback", name, "*", 0, errors.New("physical table missing"))
			}
			if row, column, equal := restoreTableDifference(want, actual); !equal {
				return restoreError("physical-readback", name, column, row, errors.New("physical SQL type, cell or multiplicity differs"))
			}
		}
		precommit, err := d.undoGraph(ctx)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(graph, precommit) {
			return restoreError("history-before-commit", "", "*", -1, errors.New("original graph or refs changed before switch-back commit"))
		}
		if err := ctx.Err(); err != nil {
			return restoreError("canceled", "", "*", receipt.WrittenRows, err)
		}
		// The pinned native procedure commits both its active SQL transaction and
		// exactly one new Dolt commit. No force, reset, migration or initialization.
		commit, err := d.undoQuery(ctx, "CALL DOLT_COMMIT('--allow-empty','-Am','CP5 undo switch-back; post-switch history not replayed')")
		if err != nil {
			return restoreError("commit-indeterminate", "", "*", -1, errors.New("native switch-back commit failed; inspect disposable copy before retry"))
		}
		committed = true
		if len(commit.Rows) != 1 || len(commit.Rows[0]) != 1 {
			return restoreError("commit-indeterminate", "", "*", -1, errors.New("native commit receipt unavailable"))
		}
		id, ok := commit.Rows[0][0].(string)
		if !ok || !physicalCommitID.MatchString(id) {
			return restoreError("commit-indeterminate", "", "*", -1, errors.New("native commit hash unavailable"))
		}
		next, err := d.undoGraph(ctx)
		if err != nil {
			return restoreError("post-commit-indeterminate", "", "*", -1, err)
		}
		parent, err := d.undoQuery(ctx, "SELECT DOLT_HASHOF('HEAD~1')")
		if err != nil {
			return restoreError("post-commit-indeterminate", "", "*", -1, err)
		}
		if len(parent.Rows) != 1 || len(parent.Rows[0]) != 1 || parent.Rows[0][0] != graph.Head || next.Head != id || !undoGraphExtends(graph, next, id) {
			return restoreError("post-commit-indeterminate", "", "*", -1, errors.New("expected exactly one child commit and unchanged original history/other refs"))
		}
		originalBytes, err := json.Marshal(graph)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(originalBytes)
		receipt.BaseCommit = graph.Head
		receipt.CommitID = id
		receipt.PriorCommits = len(graph.Log.Rows)
		receipt.CurrentCommits = len(next.Log.Rows)
		receipt.OriginalGraphSHA256 = hex.EncodeToString(sum[:])
		return nil
	})
	if resultErr != nil {
		receipt = PhysicalUndoReceipt{}
	}
	return receipt, resultErr
}

func (d *DoltConn) undoQuery(ctx context.Context, query string) (out PhysicalTable, resultErr error) {
	rows, err := d.QueryContext(ctx, query, nil)
	if err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	types, _ := rows.(driver.RowsColumnTypeDatabaseTypeName)
	for i, name := range rows.Columns() {
		typ := ""
		if types != nil {
			typ = types.ColumnTypeDatabaseTypeName(i)
		}
		out.Columns = append(out.Columns, PhysicalColumn{name, typ})
	}
	for {
		row := make([]driver.Value, len(out.Columns))
		err := rows.Next(row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, err
		}
		for i, v := range row {
			row[i], err = physicalCell(v)
			if err != nil {
				return out, err
			}
		}
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}
func (d *DoltConn) undoGraph(ctx context.Context) (out undoGraph, err error) {
	head, err := d.undoQuery(ctx, "SELECT DOLT_HASHOF('HEAD'), ACTIVE_BRANCH()")
	if err != nil {
		return out, err
	}
	if len(head.Rows) != 1 || len(head.Rows[0]) != 2 {
		return out, errors.New("native graph identity unavailable")
	}
	var ok bool
	out.Head, ok = head.Rows[0][0].(string)
	if !ok {
		return out, errors.New("native HEAD unavailable")
	}
	out.Branch, ok = head.Rows[0][1].(string)
	if !ok {
		return out, errors.New("native branch unavailable")
	}
	out.Log, err = d.undoQuery(ctx, "SELECT commit_hash,committer,email,date,message,commit_order,parents,signature,author,author_email,author_date FROM dolt_log ORDER BY commit_hash")
	if err != nil {
		return out, err
	}
	out.Branches, err = d.undoQuery(ctx, "SELECT name, hash FROM dolt_branches ORDER BY name")
	if err != nil {
		return out, err
	}
	out.Tags, err = d.undoQuery(ctx, "SELECT * FROM dolt_tags ORDER BY tag_name")
	return out, err
}
func undoGraphExtends(old, next undoGraph, id string) bool {
	if old.Branch != next.Branch || len(next.Log.Rows) != len(old.Log.Rows)+1 || !reflect.DeepEqual(old.Log.Columns, next.Log.Columns) || !reflect.DeepEqual(old.Tags, next.Tags) {
		return false
	}
	commits := map[string][]driver.Value{}
	for _, row := range next.Log.Rows {
		hash, ok := row[0].(string)
		if !ok {
			return false
		}
		commits[hash] = row
	}
	if _, ok := commits[id]; !ok {
		return false
	}
	for _, row := range old.Log.Rows {
		hash, ok := row[0].(string)
		if !ok || !reflect.DeepEqual(row, commits[hash]) {
			return false
		}
	}
	want := old.Branches
	want.Rows = make([][]driver.Value, len(old.Branches.Rows))
	for i, row := range old.Branches.Rows {
		want.Rows[i] = append([]driver.Value{}, row...)
		if len(row) == 2 && row[0] == old.Branch {
			want.Rows[i][1] = id
		}
	}
	return reflect.DeepEqual(want, next.Branches)
}
func (d *DoltConn) undoOrder(ctx context.Context, tables map[string]PhysicalTable) ([]string, error) {
	refs, err := d.undoQuery(ctx, "SELECT TABLE_NAME, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL")
	if err != nil {
		return nil, restoreError("foreign-key-schema", "", "*", -1, errors.New("native foreign-key census unavailable"))
	}
	parents := map[string]map[string]bool{}
	for name := range tables {
		parents[name] = map[string]bool{}
	}
	for _, row := range refs.Rows {
		if len(row) != 2 {
			return nil, restoreError("foreign-key-schema", "", "*", -1, errors.New("unsupported native foreign-key census"))
		}
		child, ok1 := row[0].(string)
		parent, ok2 := row[1].(string)
		if !ok1 || !ok2 {
			return nil, restoreError("foreign-key-schema", "", "*", -1, errors.New("unsupported native foreign-key names"))
		}
		if _, ok := tables[child]; !ok {
			continue
		}
		if _, ok := tables[parent]; !ok {
			return nil, restoreError("foreign-key-schema", child, "*", -1, errors.New("user table references an unowned table"))
		}
		parents[child][parent] = true
	}
	var order []string
	for len(parents) > 0 {
		var ready []string
		for name, dependencies := range parents {
			if len(dependencies) == 0 {
				ready = append(ready, name)
			}
		}
		if len(ready) == 0 {
			return nil, restoreError("foreign-key-cycle", "", "*", -1, errors.New("cyclic foreign keys cannot be replaced without changing native constraints"))
		}
		sort.Strings(ready)
		for _, name := range ready {
			order = append(order, name)
			delete(parents, name)
			for _, dependencies := range parents {
				delete(dependencies, name)
			}
		}
	}
	return order, nil
}

// Original capture cells are detached separately, including reserved tables.
// No caller-owned byte buffer or mutable row slice survives into the transaction.
func ownUndoOriginal(tables []PhysicalTable) ([]PhysicalTable, error) {
	out := make([]PhysicalTable, len(tables))
	seen := map[string]bool{}
	for ti, table := range tables {
		if !restoreSQLName.MatchString(table.Name) || seen[strings.ToLower(table.Name)] || len(table.Columns) == 0 {
			return nil, restoreError("source-binding", table.Name, "*", -1, errors.New("invalid or duplicate original table"))
		}
		seen[strings.ToLower(table.Name)] = true
		out[ti] = PhysicalTable{Name: table.Name, Columns: append([]PhysicalColumn{}, table.Columns...), Rows: make([][]driver.Value, len(table.Rows))}
		for ri, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return nil, restoreError("source-binding", table.Name, "*", ri, errors.New("invalid original row width"))
			}
			out[ti].Rows[ri] = make([]driver.Value, len(row))
			for ci, cell := range row {
				value, err := physicalCell(cell)
				if err != nil {
					return nil, restoreError("source-binding", table.Name, table.Columns[ci].Name, ri, errors.New("unsupported original detached cell"))
				}
				out[ti].Rows[ri][ci] = value
			}
		}
	}
	return out, nil
}
