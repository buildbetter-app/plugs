// Package physicalcapture holds the private spike's Dolt driver boundary.
package physicalcapture

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"

	embedded "github.com/dolthub/driver/v2"
)

// Image contains detached standard driver values. No engine types or mutable
// engine handles cross this adapter; errors retain their driver identity.
type Image struct {
	CommitID      string
	ReadOnlyScope bool
	Tables        []Table
}

type Column struct {
	Name, DatabaseTypeName string
	PrimaryKeyOrdinal      *uint64
}
type Table struct {
	Name    string
	Columns []Column
	Rows    [][]driver.Value
}

// Open opens only the caller's explicitly selected disposable database. The
// caller closes the returned DB before releasing the connector with closeDriver.
func Open(dataDir, database string) (db *sql.DB, closeDriver func() error, err error) {
	params := url.Values{"database": {database}, "commitname": {"Capture"}, "commitemail": {"capture@example.test"}, "disable_singleton_cache": {"true"}}
	cfg, err := embedded.ParseDSN("file://" + dataDir + "?" + params.Encode())
	if err != nil {
		return nil, nil, err
	}
	connector, err := embedded.NewConnector(cfg)
	if err != nil {
		return nil, nil, err
	}
	return sql.OpenDB(connector), connector.Close, nil
}

// ReadCommit forwards the immutable operation without alias lookup or fallback.
// The driver authenticates the read-only transaction and owns all row values
// before returning. This adapter only reshapes detached column metadata.
func ReadCommit(ctx context.Context, conn *sql.Conn, id string) (Image, error) {
	physical, err := embedded.ReadPhysicalCommit(ctx, conn, id)
	if err != nil {
		return Image{}, err
	}
	image := Image{CommitID: physical.CommitID, ReadOnlyScope: physical.ReadOnlyScope, Tables: make([]Table, 0, len(physical.Tables))}
	for _, source := range physical.Tables {
		table := Table{Name: source.Name, Rows: source.Rows, Columns: make([]Column, 0, len(source.Columns))}
		for _, column := range source.Columns {
			table.Columns = append(table.Columns, Column{Name: column.Name, DatabaseTypeName: column.DatabaseTypeName, PrimaryKeyOrdinal: column.PrimaryKeyOrdinal})
		}
		image.Tables = append(image.Tables, table)
	}
	return image, nil
}
