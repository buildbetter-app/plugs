package plugdump

import (
	"context"
	"database/sql"
	"errors"
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

func (s PhysicalCommitSource) ReadNativeSourceImage(ctx context.Context, id string) (result map[string]Table, resultErr error) {
	if s.PhysicalDB == nil {
		return nil, fmt.Errorf("physical capture database required")
	}
	conn, err := s.PhysicalDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, conn.Close())
		if resultErr != nil {
			result = nil
		}
	}()
	cursorImage, err := physicalcapture.ReadCommitSchema(ctx, conn, id)
	if err != nil {
		return nil, &NativeCommitFailure{Cause: err}
	}
	if cursorImage.CommitID != id || !cursorImage.ReadOnlyScope {
		return nil, &NativeCommitFailure{Cause: fmt.Errorf("immutable cursor identity differs")}
	}
	cursorTables, err := physicalCaptureTables(cursorImage.Tables)
	if err != nil {
		return nil, &NativeCommitFailure{Cause: err}
	}
	cursor, err := nativeCommitCursors(cursorTables, cursorImage.RootTableCount)
	if err != nil {
		return nil, &NativeCommitFailure{SchemaVersion: cursor.MainVersion, Cause: err}
	}
	physical, err := physicalcapture.ReadCommit(ctx, conn, id)
	if err != nil {
		return nil, &NativeCommitFailure{SchemaVersion: cursor.MainVersion, Cause: err}
	}
	if physical.CommitID != id || !physical.ReadOnlyScope || len(physical.Tables) != cursorImage.RootTableCount {
		return nil, fmt.Errorf("physical commit identity differs")
	}
	image, err := physicalCaptureTables(physical.Tables)
	if err != nil {
		return nil, &NativeCommitFailure{SchemaVersion: cursor.MainVersion, Cause: err}
	}
	observed, err := nativeCommitCursors(image, len(image))
	if err != nil || !equalJSON(cursor, observed) {
		return nil, &NativeCommitFailure{SchemaVersion: cursor.MainVersion, Cause: errors.Join(fmt.Errorf("immutable cursor differs from full image"), err)}
	}
	return image, nil
}

func (PhysicalCommitSource) ProfileNativeSourceImage(image map[string]Table) (NativeCommitSchemaProfile, error) {
	return ProfileNativeCommit(image)
}

func physicalCaptureTables(tables []physicalcapture.Table) (map[string]Table, error) {
	image := map[string]Table{}
	for _, source := range tables {
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

// PhysicalCommitSchemaProfile authenticates the full detached image's cursor
// prefixes and public physical absences for the separate typed evidence file.
func PhysicalCommitSchemaProfile(image physicalcapture.Image) (NativeCommitSchemaProfile, error) {
	if !image.ReadOnlyScope || !validCommitID(image.CommitID) {
		return NativeCommitSchemaProfile{}, fmt.Errorf("unauthenticated physical image")
	}
	tables, err := physicalCaptureTables(image.Tables)
	if err != nil {
		return NativeCommitSchemaProfile{}, err
	}
	return ProfileNativeCommit(tables)
}
