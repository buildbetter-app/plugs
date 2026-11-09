// Copyright 2026 BuildBetter. Restore-only companion to physical_commit.go.
package embedded

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	gms "github.com/dolthub/go-mysql-server/sql"
)

// PhysicalRestoreError names a failing boundary without including cell values.
// A failed call never means a subset of the requested rows was accepted.
// Commit errors are explicitly indeterminate and must not be retried blindly.
type PhysicalRestoreError struct {
	Kind, Table, Column string
	Row                 int
	Cause               error
}

func (e *PhysicalRestoreError) Error() string {
	return fmt.Sprintf("physical restore %s table=%s column=%s row=%d: %v", e.Kind, e.Table, e.Column, e.Row, e.Cause)
}
func (e *PhysicalRestoreError) Unwrap() error { return e.Cause }
func restoreError(kind, table, column string, row int, err error) error {
	return &PhysicalRestoreError{kind, table, column, row, err}
}

var restoreSQLName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// RestorePhysicalSnapshot inserts into existing, exactly matching EMPTY tables.
// It performs no DDL, UPSERT, alias lookup, migrations, history replay or command
// mutation. The caller supplies a dedicated idle connection. Every input buffer
// is cloned before the transaction. Normal SQL INSERTs and their constraints are
// used, then the actual working root is checked before SQL COMMIT. Nonrequested
// tables must remain identical. No expectations are supplied to the read path.
func RestorePhysicalSnapshot(ctx context.Context, conn *sql.Conn, tables []PhysicalTable) error {
	owned, err := ownRestoreTables(tables)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return conn.Raw(func(raw any) (resultErr error) {
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
					resultErr = restoreError("rollback-indeterminate", "", "", -1, errors.Join(resultErr, rollbackErr))
				}
			}
		}()
		before, err := d.restoreWorkingImage(ctx)
		if err != nil {
			return err
		}
		selected := map[string]PhysicalTable{}
		for _, want := range owned {
			got, present := before[want.Name]
			if !present {
				return restoreError("missing-table", want.Name, "", -1, errors.New("no physical destination"))
			}
			if !reflect.DeepEqual(want.Columns, got.Columns) {
				return restoreError("schema", want.Name, "", -1, errors.New("exact physical columns and SQL types required"))
			}
			if len(got.Rows) != 0 {
				return restoreError("occupied-table", want.Name, "", -1, errors.New("restore destination must be empty"))
			}
			selected[want.Name] = want
		}
		// SQL resolves nonlocal declarations before physical table names. Never let
		// a restore be redirected; in particular do not restore this control table.
		if aliases, present := before["dolt_nonlocal_tables"]; present {
			patternIndex := -1
			for i, column := range aliases.Columns {
				if column.Name == "table_name" {
					patternIndex = i
				}
			}
			if len(aliases.Rows) != 0 && patternIndex < 0 {
				return restoreError("nonlocal-aliases", "dolt_nonlocal_tables", "", -1, errors.New("unsupported nonlocal declaration schema"))
			}
			for _, row := range aliases.Rows {
				pattern, ok := row[patternIndex].(string)
				if !ok || pattern == "" || len(pattern) > 128 {
					return restoreError("nonlocal-aliases", "dolt_nonlocal_tables", "", -1, errors.New("unsupported nonlocal declaration"))
				}
				expression := regexp.QuoteMeta(strings.ToLower(pattern))
				expression = strings.NewReplacer(`\?`, ".", `\*`, ".*", "%", ".*").Replace(expression)
				for name := range selected {
					matches, err := regexp.MatchString("^"+expression+"$", strings.ToLower(name))
					if err != nil || matches {
						return restoreError("nonlocal-aliases", name, "", -1, errors.New("SQL destination may be redirected"))
					}
				}
			}
		}
		// Validate every expression BEFORE the first mutation, including every JSON
		// number. The normal driver's ordinary JSON binding remains unchanged.
		type insertion struct {
			query string
			args  []driver.NamedValue
			table string
			row   int
		}
		var inserts []insertion
		for _, table := range owned {
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
						return restoreError("cell", table.Name, table.Columns[ci].Name, ri, err)
					}
					expressions[ci] = expr
				}
				inserts = append(inserts, insertion{"INSERT INTO `" + table.Name + "` (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(expressions, ",") + ")", args, table.Name, ri})
			}
		}
		for _, insert := range inserts {
			if err := ctx.Err(); err != nil {
				return restoreError("canceled", insert.table, "", insert.row, err)
			}
			if _, err := d.ExecContext(ctx, insert.query, insert.args); err != nil {
				if cancelErr := ctx.Err(); cancelErr != nil {
					return restoreError("canceled", insert.table, "", insert.row, cancelErr)
				}
				// Do not expose SQL's value-bearing diagnostic in a public restore error.
				return restoreError("insert", insert.table, "", insert.row, errors.New("native SQL insertion refused"))
			}
		}
		after, err := d.restoreWorkingImage(ctx)
		if err != nil {
			return err
		}
		if len(after) != len(before) {
			return restoreError("census", "", "", -1, errors.New("physical table census changed"))
		}
		for name, old := range before {
			want := old
			if restored, ok := selected[name]; ok {
				want = restored
			}
			got, ok := after[name]
			if !ok {
				return restoreError("physical-readback", name, "*", 0, errors.New("physical table missing after insertion"))
			}
			if row, column, equal := restoreTableDifference(want, got); !equal {
				return restoreError("physical-readback", name, column, row, errors.New("physical SQL type, cell or multiplicity differs"))
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return restoreError("commit-indeterminate", "", "", -1, errors.New("SQL commit failed; inspect destination before any retry"))
		}
		committed = true
		return nil
	})
}

func ownRestoreTables(tables []PhysicalTable) ([]PhysicalTable, error) {
	if len(tables) == 0 {
		return nil, restoreError("manifest", "", "", -1, errors.New("at least one table required"))
	}
	seen := map[string]bool{}
	out := make([]PhysicalTable, len(tables))
	for i, table := range tables {
		if !restoreSQLName.MatchString(table.Name) || strings.HasPrefix(strings.ToLower(table.Name), "dolt_") || seen[strings.ToLower(table.Name)] {
			return nil, restoreError("table", table.Name, "", -1, errors.New("invalid, reserved or duplicate table"))
		}
		seen[strings.ToLower(table.Name)] = true
		out[i] = PhysicalTable{Name: table.Name, Columns: append([]PhysicalColumn{}, table.Columns...), Rows: make([][]driver.Value, len(table.Rows))}
		cols := map[string]bool{}
		if len(table.Columns) == 0 {
			return nil, restoreError("schema", table.Name, "", -1, errors.New("columns required"))
		}
		for _, col := range table.Columns {
			if !restoreSQLName.MatchString(col.Name) || col.DatabaseTypeName == "" || cols[strings.ToLower(col.Name)] {
				return nil, restoreError("schema", table.Name, col.Name, -1, errors.New("invalid or duplicate column"))
			}
			cols[strings.ToLower(col.Name)] = true
		}
		for ri, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return nil, restoreError("row-width", table.Name, "", ri, errors.New("every physical column must be present"))
			}
			out[i].Rows[ri] = make([]driver.Value, len(row))
			for ci, value := range row {
				cell, err := physicalCell(value)
				if err != nil {
					return nil, restoreError("cell-type", table.Name, table.Columns[ci].Name, ri, errors.New("unsupported detached cell type"))
				}
				switch v := cell.(type) {
				case float64:
					if math.IsNaN(v) || math.IsInf(v, 0) {
						return nil, restoreError("cell-type", table.Name, table.Columns[ci].Name, ri, errors.New("nonfinite float"))
					}
				case float32:
					if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
						return nil, restoreError("cell-type", table.Name, table.Columns[ci].Name, ri, errors.New("nonfinite float"))
					}
				}
				out[i].Rows[ri][ci] = cell
			}
		}
	}
	return out, nil
}

func restoreBind(value driver.Value, args *[]driver.NamedValue) string {
	*args = append(*args, driver.NamedValue{Ordinal: len(*args) + 1, Value: value})
	return "?"
}
func restoreExpression(value driver.Value, sqlType string, args *[]driver.NamedValue) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	if strings.EqualFold(sqlType, "json") {
		text, ok := value.(string)
		if !ok {
			return "", errors.New("JSON physical cell must be detached JSON text")
		}
		parsed, err := restoreJSON(text)
		if err != nil {
			return "", err
		}
		if text, ok := parsed.(string); ok {
			return "JSON_QUOTE(" + restoreBind(text, args) + ")", nil
		}
		return restoreJSONExpression(parsed, args)
	}
	return restoreBind(value, args), nil
}

// JSON_OBJECT/ARRAY keep SQL integer arguments exact. Binding a JSON string
// through JsonType.Convert would decode it via float64 in the pinned engine.
// This route is private to restore and still uses the normal SQL evaluator.
func restoreJSONExpression(value any, args *[]driver.NamedValue) (string, error) {
	switch v := value.(type) {
	case nil:
		return "CAST('null' AS JSON)", nil
	case bool:
		if v {
			return "CAST('true' AS JSON)", nil
		}
		return "CAST('false' AS JSON)", nil
	case string:
		return restoreBind(v, args), nil
	case json.Number:
		literal := v.String()
		if n, err := strconv.ParseInt(literal, 10, 64); err == nil {
			return "CAST(" + restoreBind(n, args) + " AS SIGNED)", nil
		}
		if n, err := strconv.ParseUint(literal, 10, 64); err == nil {
			return "CAST(" + restoreBind(n, args) + " AS UNSIGNED)", nil
		}
		if !strings.ContainsAny(literal, ".eE") {
			return "", errors.New("JSON integer outside native signed/unsigned 64-bit range")
		}
		return "CAST(" + restoreBind(literal, args) + " AS JSON)", nil
	case []any:
		parts := make([]string, len(v))
		for i, child := range v {
			part, err := restoreJSONExpression(child, args)
			if err != nil {
				return "", err
			}
			parts[i] = part
		}
		return "JSON_ARRAY(" + strings.Join(parts, ",") + ")", nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(v)*2)
		for _, key := range keys {
			parts = append(parts, restoreBind(key, args))
			part, err := restoreJSONExpression(v[key], args)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		return "JSON_OBJECT(" + strings.Join(parts, ",") + ")", nil
	default:
		return "", errors.New("unsupported JSON node")
	}
}
func restoreJSON(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("invalid JSON physical cell")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON physical cell")
	}
	return value, nil
}

// Read the actual current working ROOT without SQL name resolution or aliases.
// This function does not receive any expected schema or cell values.
func (d *DoltConn) restoreWorkingImage(ctx context.Context) (map[string]PhysicalTable, error) {
	q, err := d.beginQuery(ctx, "restore physical working-root readback")
	if err != nil {
		return nil, err
	}
	defer d.endQuery(q)
	if q.Client().User != "root" {
		return nil, restoreError("permission", "", "", -1, errors.New("local root required"))
	}
	database, err := d.se.GetUnderlyingEngine().Analyzer.Catalog.Database(q, q.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	db, ok := database.(dsess.SqlDatabase)
	if !ok {
		return nil, errors.New("Dolt database required")
	}
	root, err := db.GetRoot(q)
	if err != nil {
		return nil, err
	}
	schemas, err := root.GetDatabaseSchemas(q)
	if err != nil {
		return nil, err
	}
	for _, schema := range schemas {
		if schema.Name != "" && schema.Name != "public" {
			return nil, errors.New("nondefault physical schema unsupported")
		}
	}
	names, err := root.GetTableNames(q, "", false)
	if err != nil {
		return nil, err
	}
	result := make(map[string]PhysicalTable, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stored, present, err := root.GetTable(q, doltdb.TableName{Name: name})
		if err != nil {
			return nil, err
		}
		if !present {
			return nil, errors.New("inconsistent physical census")
		}
		schema, err := stored.GetSchema(q)
		if err != nil {
			return nil, err
		}
		table, err := sqle.NewDoltTable(q, name, schema, stored, db, editor.Options{})
		if err != nil {
			return nil, err
		}
		locked, err := table.LockedToRoot(q, root)
		if err != nil {
			return nil, err
		}
		physical, err := readPhysicalTable(q, locked, name)
		if err != nil {
			return nil, err
		}
		result[name] = physical
	}
	return result, nil
}

// Return the zero-based requested row and physical column for a mismatch.
// Exact row matching is a multiset operation, retaining duplicate/keyless rows.
// A nearest remaining physical row only locates a useful error field; it never
// turns a mismatch into acceptance or supplies values to the read path.
func restoreTableDifference(want, got PhysicalTable) (int, string, bool) {
	if want.Name != got.Name || !reflect.DeepEqual(want.Columns, got.Columns) {
		return 0, "*", false
	}
	encode := func(table PhysicalTable) ([][]string, bool) {
		out := make([][]string, len(table.Rows))
		for ri, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return nil, false
			}
			out[ri] = make([]string, len(row))
			for ci, value := range row {
				if strings.EqualFold(table.Columns[ci].DatabaseTypeName, "json") && value != nil {
					text, ok := value.(string)
					if !ok {
						return nil, false
					}
					parsed, err := restoreJSON(text)
					if err != nil {
						return nil, false
					}
					canonical, err := json.Marshal(parsed)
					if err != nil {
						return nil, false
					}
					value = string(canonical)
				}
				var cell bytes.Buffer
				fmt.Fprintf(&cell, "%T:%#v", value, value)
				out[ri][ci] = cell.String()
			}
		}
		return out, true
	}
	left, leftOK := encode(want)
	right, rightOK := encode(got)
	if !leftOK || !rightOK {
		return 0, "*", false
	}
	used := make([]bool, len(right))
	for ri, row := range left {
		exact := -1
		for gi, actual := range right {
			if !used[gi] && reflect.DeepEqual(row, actual) {
				exact = gi
				break
			}
		}
		if exact >= 0 {
			used[exact] = true
			continue
		}
		nearest, best := -1, -1
		for gi, actual := range right {
			if used[gi] {
				continue
			}
			score := 0
			for ci, cell := range row {
				if cell == actual[ci] {
					score++
				}
			}
			if score > best {
				nearest, best = gi, score
			}
		}
		if nearest >= 0 {
			for ci, cell := range row {
				if cell != right[nearest][ci] {
					return ri, want.Columns[ci].Name, false
				}
			}
		}
		return ri, "*", false
	}
	if len(left) != len(right) {
		return len(left), "*", false
	}
	return -1, "", true
}
