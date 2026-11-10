package physicalcapture

import (
	"context"
	"database/sql"

	embedded "github.com/dolthub/driver/v2"
)

// SchemaImage is a detached, authenticated observation of the exact immutable
// root's two public migration cursors; it does not read ordinary user cells.
type SchemaImage struct {
	CommitID       string
	ReadOnlyScope  bool
	RootTableCount int
	Tables         []Table
}

func ReadCommitSchema(ctx context.Context, conn *sql.Conn, id string) (SchemaImage, error) {
	observed, err := embedded.ReadPhysicalCommitSchema(ctx, conn, id)
	if err != nil {
		return SchemaImage{}, err
	}
	image := SchemaImage{CommitID: observed.CommitID, ReadOnlyScope: observed.ReadOnlyScope, RootTableCount: observed.RootTableCount, Tables: make([]Table, 0, len(observed.Tables))}
	for _, source := range observed.Tables {
		table := Table{Name: source.Name, Rows: source.Rows, Columns: make([]Column, 0, len(source.Columns))}
		for _, column := range source.Columns {
			table.Columns = append(table.Columns, Column{Name: column.Name, DatabaseTypeName: column.DatabaseTypeName})
		}
		image.Tables = append(image.Tables, table)
	}
	return image, nil
}
