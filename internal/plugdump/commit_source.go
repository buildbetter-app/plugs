package plugdump

import (
	"context"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

// OwnVersionCommitSource opts the standalone Beads reader into per-commit
// cursor authentication. The general PhysicalCommitSource stays schema-neutral
// for callers capturing arbitrary physical roots.
type OwnVersionCommitSource struct {
	PhysicalCommitSource
}

var _ NativeSourceHistoryReader = OwnVersionCommitSource{}
var _ NativeCommitSchemaReader = OwnVersionCommitSource{}

func (s OwnVersionCommitSource) ReadNativeSourceImage(ctx context.Context, id string) (result map[string]Table, resultErr error) {
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
	observed, err := ProfileNativeCommit(image)
	if err != nil {
		return nil, &NativeCommitFailure{SchemaVersion: cursor.MainVersion, Cause: err}
	}
	if !equalJSON(cursor.MainVersion, observed.MainVersion) || !equalJSON(cursor.IgnoredVersion, observed.IgnoredVersion) {
		return nil, &NativeCommitFailure{SchemaVersion: cursor.MainVersion, Cause: fmt.Errorf("immutable cursor differs from full image")}
	}
	return image, nil
}

func (OwnVersionCommitSource) ProfileNativeSourceImage(image map[string]Table) (NativeCommitSchemaProfile, error) {
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
