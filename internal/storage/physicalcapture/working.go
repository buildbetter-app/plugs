package physicalcapture

import (
	"context"
	"database/sql"

	embedded "github.com/dolthub/driver/v2"
)

const WorkingImageFormat = "beads-physical-working-root/1"

// WorkingImage retains all actual stored tables, including ignored current-only
// families, with the original Column SQL types and owned driver cells. nil is
// SQL NULL; a JSON column's string "null" is JSON null. No cell normalization,
// name-based type inference, lease filtering or optional exclusions occur here.
//
// Database, HeadCommitID and WorkingRootHash are observed in one read-only driver
// scope. The caller binds that source evidence to its store/project receipt; a
// content hash does not itself assert an external project identity. Complete
// means a full physical table census, not history completeness or a writer plan.
type WorkingImage struct {
	Format                                  string
	Database, HeadCommitID, WorkingRootHash string
	ReadOnlyScope, Complete                 bool
	Tables                                  []Table
}

// ReadWorkingRoot authenticates expectedHead against the actual source head.
// It never reads through SQL aliases, migrates, writes, or grafts current rows
// onto immutable images. The original Image/ReadCommit interface is unchanged.
func ReadWorkingRoot(ctx context.Context, conn *sql.Conn, expectedHead string) (WorkingImage, error) {
	physical, err := embedded.ReadPhysicalWorkingRoot(ctx, conn, expectedHead)
	if err != nil {
		return WorkingImage{}, err
	}
	image := WorkingImage{
		Format: WorkingImageFormat, Database: physical.Database,
		HeadCommitID: physical.HeadCommitID, WorkingRootHash: physical.WorkingRootHash,
		ReadOnlyScope: physical.ReadOnlyScope, Complete: physical.Complete,
		Tables: make([]Table, 0, len(physical.Tables)),
	}
	for _, source := range physical.Tables {
		table := Table{Name: source.Name, Rows: source.Rows, Columns: make([]Column, 0, len(source.Columns))}
		for _, column := range source.Columns {
			table.Columns = append(table.Columns, Column{Name: column.Name, DatabaseTypeName: column.DatabaseTypeName, PrimaryKeyOrdinal: column.PrimaryKeyOrdinal})
		}
		image.Tables = append(image.Tables, table)
	}
	return image, nil
}
