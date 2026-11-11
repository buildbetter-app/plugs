package physicalrestore

import (
	"context"
	"database/sql"
	"errors"

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
// any edit. The unchanged typed ports authenticate its complete source binding.
// The owning metadata port reads actual stored schemas in an authenticated
// read-only scope at the exact database/HEAD/working root. The original typed
// binding is checked again afterwards. SQL index listings are not authority.
// The destination's existing Undo independently authenticates that same root
// before mutation. Schema changes, missing stored authority and failed rollback
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
	stored, err := physicalcapture.ReadWorkingRootSchema(ctx, conn, source.CommitID)
	if err != nil {
		return authority, err
	}
	if !stored.ReadOnlyScope || !stored.Complete || stored.Database != source.Working.Database || stored.HeadCommitID != source.CommitID || stored.WorkingRootHash != source.Working.WorkingRootHash {
		return authority, nativeSchemaError("", "*", "stored schema differs from original complete database/HEAD/working root")
	}
	authority, err = bindNativeRowSchema(source, stored.Tables)
	if err != nil {
		return NativeRowSchema{}, err
	}
	if err := checkSource(); err != nil {
		return NativeRowSchema{}, err
	}
	return authority, nil
}

func bindNativeRowSchema(source SourceBinding, tables []physicalcapture.SchemaTable) (NativeRowSchema, error) {
	if source.Working == nil || len(tables) != len(source.Working.Image.Tables) {
		return NativeRowSchema{}, nativeSchemaError("", "*", "complete original stored-schema table census required")
	}
	original := make(map[string]Table, len(source.Working.Image.Tables))
	for _, table := range source.Working.Image.Tables {
		original[table.Name] = table
	}
	authority := NativeRowSchema{sourceSHA256: source.SHA256, columns: make(map[string][]dumpformat.NativeDeltaColumn, len(tables))}
	for _, table := range tables {
		old, present := original[table.Name]
		_, duplicate := authority.columns[table.Name]
		if !present || duplicate || len(table.Columns) != len(old.Columns) {
			return NativeRowSchema{}, nativeSchemaError(table.Name, "*", "stored-schema table/column census differs from original typed source")
		}
		columns := make([]dumpformat.NativeDeltaColumn, len(table.Columns))
		ordinals := map[uint64]bool{}
		for i, column := range table.Columns {
			if column.Name != old.Columns[i].Name || column.DatabaseTypeName != old.Columns[i].SQLType {
				return NativeRowSchema{}, nativeSchemaError(table.Name, column.Name, "stored-schema ordered SQL columns differ from original typed source")
			}
			columns[i] = dumpformat.NativeDeltaColumn{Name: column.Name, SQLType: column.DatabaseTypeName}
			if column.PrimaryKeyOrdinal != nil {
				ordinal := *column.PrimaryKeyOrdinal
				if ordinal >= uint64(len(table.Columns)) || ordinals[ordinal] {
					return NativeRowSchema{}, nativeSchemaError(table.Name, column.Name, "invalid/duplicate stored PRIMARY ordinal")
				}
				ordinals[ordinal] = true
				columns[i].PrimaryKeyOrdinal = &ordinal
			}
		}
		for i := uint64(0); i < uint64(len(ordinals)); i++ {
			if !ordinals[i] {
				return NativeRowSchema{}, nativeSchemaError(table.Name, "*", "stored PRIMARY order is incomplete")
			}
		}
		authority.columns[table.Name] = columns
	}
	return authority, nil
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
