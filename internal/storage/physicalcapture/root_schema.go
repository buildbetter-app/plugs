package physicalcapture

import (
	"context"
	"database/sql"

	embedded "github.com/dolthub/driver/v2"
)

// SchemaColumn preserves ordered SQL type and primary-key metadata from an
// actual stored schema. A nil ordinal is a non-key column; a non-nil ordinal
// is its zero-based position in that schema's primary key. Legacy row columns
// and their readback equality are intentionally a separate type.
type SchemaColumn struct {
	Name, DatabaseTypeName string
	PrimaryKeyOrdinal      *uint64
}

type SchemaTable struct {
	Name    string
	Columns []SchemaColumn
}

type WorkingSchemaImage struct {
	Database, HeadCommitID, WorkingRootHash string
	ReadOnlyScope, Complete                 bool
	Tables                                  []SchemaTable
}

type CommitSchemaImage struct {
	Database, CommitID, RootHash string
	ReadOnlyScope, Complete      bool
	Tables                       []SchemaTable
}

// ReadWorkingRootSchema reads every stored table, including reserved and ignored
// tables, in an authenticated read-only scope. It reads no rows. The caller must
// bind the returned database, HEAD and working-root hash to its original source
// before mutation; expectedHead alone does not authenticate a working image.
func ReadWorkingRootSchema(ctx context.Context, conn *sql.Conn, expectedHead string) (WorkingSchemaImage, error) {
	image, err := embedded.ReadPhysicalWorkingRootSchema(ctx, conn, expectedHead)
	if err != nil {
		return WorkingSchemaImage{}, err
	}
	return WorkingSchemaImage{Database: image.Database, HeadCommitID: image.CommitID, WorkingRootHash: image.RootHash, ReadOnlyScope: image.ReadOnlyScope, Complete: image.Complete, Tables: schemaTables(image.Tables)}, nil
}

// ReadCommitRootSchema resolves the exact selected commit and observes that
// root's complete stored schema. It never substitutes current schema metadata.
// Unavailable authority returns the driver's typed refusal and a zero image.
func ReadCommitRootSchema(ctx context.Context, conn *sql.Conn, id string) (CommitSchemaImage, error) {
	image, err := embedded.ReadPhysicalCommitRootSchema(ctx, conn, id)
	if err != nil {
		return CommitSchemaImage{}, err
	}
	return CommitSchemaImage{Database: image.Database, CommitID: image.CommitID, RootHash: image.RootHash, ReadOnlyScope: image.ReadOnlyScope, Complete: image.Complete, Tables: schemaTables(image.Tables)}, nil
}

func schemaTables(source []embedded.PhysicalSchemaTable) []SchemaTable {
	tables := make([]SchemaTable, 0, len(source))
	for _, table := range source {
		converted := SchemaTable{Name: table.Name, Columns: make([]SchemaColumn, 0, len(table.Columns))}
		for _, column := range table.Columns {
			detached := SchemaColumn{Name: column.Name, DatabaseTypeName: column.DatabaseTypeName}
			if column.PrimaryKeyOrdinal != nil {
				ordinal := *column.PrimaryKeyOrdinal
				detached.PrimaryKeyOrdinal = &ordinal
			}
			converted.Columns = append(converted.Columns, detached)
		}
		tables = append(tables, converted)
	}
	return tables
}
