package plugdump

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

// PhysicalCommitSource is a private capture-spike adapter. Current rows, ignore
// rules, commit metadata and portable replay stay on the caller's original
// SQLSource scope. Only native immutable images use a separate dedicated driver
// connection. This does not change stock bd export or the archive contracts.
type PhysicalCommitSource struct {
	SQLSource
	PhysicalDB *sql.DB
}

var _ NativeSourceHistoryReader = PhysicalCommitSource{}

func (s PhysicalCommitSource) ReadNativeSourceImage(ctx context.Context, id string) (map[string]Table, error) {
	if s.PhysicalDB == nil {
		return nil, fmt.Errorf("physical capture database required")
	}
	conn, err := s.PhysicalDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	physical, err := physicalcapture.ReadCommit(ctx, conn, id)
	if err != nil {
		return nil, err
	}
	if physical.CommitID != id || !physical.ReadOnlyScope {
		return nil, fmt.Errorf("physical commit identity differs")
	}
	image := map[string]Table{}
	for _, source := range physical.Tables {
		table := Table{Columns: []string{}, Rows: []Row{}}
		for _, column := range source.Columns {
			table.Columns = append(table.Columns, column.Name)
		}
		for _, values := range source.Rows {
			if len(values) != len(source.Columns) {
				return nil, fmt.Errorf("physical row width differs")
			}
			row := Row{}
			for i, value := range values {
				converted, err := sqlValue(value, source.Columns[i].DatabaseTypeName)
				if err != nil {
					return nil, err
				}
				row[source.Columns[i].Name] = converted
			}
			table.Rows = append(table.Rows, row)
		}
		if _, duplicate := image[source.Name]; duplicate {
			return nil, fmt.Errorf("duplicate physical table name")
		}
		image[source.Name] = table
	}
	return image, nil
}
