package physicalrestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/physicalcapture"
)

// NativeRowSchema is original-root schema authority, separate from desired row
// records. Only ReadNativeRowSchema can mint it. It includes every stored current
// table, including ignored and reserved tables; no missing entry means keyless.
// It grants no v1 import/readback prerequisite and carries no writable values.
type NativeRowSchema struct {
	sourceSHA256 string
	columns      map[string][]dumpformat.NativeDeltaColumn
}

func nativeSchemaError(table, column, reason string) error {
	return &Error{Kind: "native-row-schema", Table: table, Column: column, Row: -1, Cause: errors.New(reason)}
}

// ReadNativeRowSchema reads the ORIGINAL dedicated idle source connection before
// any edit. The unchanged typed ports authenticate its complete source binding;
// SQL metadata is observed in one explicit read-only transaction at the exact
// database/HEAD/working root, and the typed binding is checked again afterwards.
// The destination's existing Undo independently authenticates that same root
// before mutation. Schema changes, missing catalog authority and failed rollback
// return no authority. The caller owns connection/store exclusion and lifetime.
func ReadNativeRowSchema(ctx context.Context, conn *sql.Conn, source SourceBinding) (authority NativeRowSchema, resultErr error) {
	defer func() {
		if resultErr != nil {
			authority = NativeRowSchema{}
			var typed *Error
			if !errors.As(resultErr, &typed) {
				resultErr = &Error{Kind: "native-row-schema", Column: "*", Row: -1, Cause: resultErr}
			}
		}
	}()
	if err := validateBinding(source); err != nil {
		return authority, err
	}
	if conn == nil || source.Working == nil {
		return authority, nativeSchemaError("", "*", "original connection and complete typed working binding required")
	}
	checkSource := func() error {
		immutable, err := physicalcapture.ReadCommit(ctx, conn, source.CommitID)
		if err != nil {
			return err
		}
		working, err := physicalcapture.ReadWorkingRoot(ctx, conn, source.CommitID)
		if err != nil {
			return err
		}
		actual, err := BindWorkingSource(immutable, working)
		if err != nil {
			return err
		}
		if actual.SHA256 != source.SHA256 {
			return nativeSchemaError("", "*", "original exact-root typed binding differs")
		}
		return nil
	}
	if err := checkSource(); err != nil {
		return authority, err
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION READ ONLY"); err != nil {
		return authority, err
	}
	active := true
	defer func() {
		if active {
			_, rollbackErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			resultErr = errors.Join(resultErr, rollbackErr)
		}
		if resultErr != nil {
			authority = NativeRowSchema{}
		}
	}()
	checkRoot := func() error {
		var database, head, root string
		if err := conn.QueryRowContext(ctx, "SELECT DATABASE(), DOLT_HASHOF('HEAD'), DOLT_HASHOF_DB('WORKING')").Scan(&database, &head, &root); err != nil {
			return err
		}
		if database != source.Working.Database || head != source.CommitID || root != source.Working.WorkingRootHash {
			return nativeSchemaError("", "*", "schema transaction differs from original database/HEAD/working root")
		}
		return nil
	}
	if err := checkRoot(); err != nil {
		return authority, err
	}
	authority = NativeRowSchema{sourceSHA256: source.SHA256, columns: make(map[string][]dumpformat.NativeDeltaColumn, len(source.Working.Image.Tables))}
	for _, table := range source.Working.Image.Tables {
		pks, err := readNativePrimaryKey(ctx, conn, table)
		if err != nil {
			return NativeRowSchema{}, err
		}
		columns := make([]dumpformat.NativeDeltaColumn, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = dumpformat.NativeDeltaColumn{Name: col.Name, SQLType: col.SQLType}
			if ordinal, present := pks[col.Name]; present {
				o := ordinal
				columns[i].PrimaryKeyOrdinal = &o
			}
		}
		authority.columns[table.Name] = columns
	}
	if err := checkRoot(); err != nil {
		return NativeRowSchema{}, err
	}
	_, err := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	active = false
	if err != nil {
		return NativeRowSchema{}, err
	}
	if err := checkSource(); err != nil {
		return NativeRowSchema{}, err
	}
	return authority, nil
}

func readNativePrimaryKey(ctx context.Context, conn *sql.Conn, table Table) (map[string]uint64, error) {
	rows, err := conn.QueryContext(ctx, "SHOW INDEX FROM `"+strings.ReplaceAll(table.Name, "`", "``")+"`")
	if err != nil {
		return nil, &Error{Kind: "native-row-schema", Table: table.Name, Column: "*", Row: -1, Cause: err}
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	index := map[string]int{}
	for i, col := range columns {
		index[strings.ToLower(col)] = i
	}
	for _, field := range []string{"key_name", "column_name", "seq_in_index"} {
		if _, present := index[field]; !present {
			return nil, nativeSchemaError(table.Name, "*", "complete native index metadata required")
		}
	}
	known := map[string]bool{}
	for _, col := range table.Columns {
		known[col.Name] = true
	}
	pks := map[string]uint64{}
	ordinals := map[uint64]bool{}
	for rows.Next() {
		values, targets := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		text := func(name string) string {
			value := values[index[name]]
			if b, ok := value.([]byte); ok {
				return string(b)
			}
			return fmt.Sprint(value)
		}
		if text("key_name") != "PRIMARY" {
			continue
		}
		name := text("column_name")
		ordinal, err := strconv.ParseUint(text("seq_in_index"), 10, 64)
		_, duplicate := pks[name]
		if err != nil || ordinal == 0 || !known[name] || duplicate || ordinals[ordinal-1] {
			return nil, nativeSchemaError(table.Name, name, "invalid/duplicate native PRIMARY descriptor")
		}
		pks[name], ordinals[ordinal-1] = ordinal-1, true
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for i := uint64(0); i < uint64(len(pks)); i++ {
		if !ordinals[i] {
			return nil, nativeSchemaError(table.Name, "*", "native PRIMARY order is incomplete")
		}
	}
	return pks, nil
}

func sameNativeColumns(left, right []dumpformat.NativeDeltaColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for i, a := range left {
		b := right[i]
		if a.Name != b.Name || a.SQLType != b.SQLType || (a.PrimaryKeyOrdinal == nil) != (b.PrimaryKeyOrdinal == nil) {
			return false
		}
		if a.PrimaryKeyOrdinal != nil && *a.PrimaryKeyOrdinal != *b.PrimaryKeyOrdinal {
			return false
		}
	}
	return true
}
