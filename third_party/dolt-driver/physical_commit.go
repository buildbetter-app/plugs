// Copyright 2026 BuildBetter. Private capture spike; not an upstream API.
package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	gms "github.com/dolthub/go-mysql-server/sql"
)

// PhysicalColumn and PhysicalTable contain detached driver values only. Rows
// preserve order, multiplicity, NULLs, and independently owned binary cells.
type PhysicalColumn struct{ Name, DatabaseTypeName string }
type PhysicalTable struct {
	Name    string
	Columns []PhysicalColumn
	Rows    [][]driver.Value
}
type PhysicalImage struct {
	CommitID string
	Tables   []PhysicalTable
}

// PhysicalReadError distinguishes resolution, permission, schema, decoding and
// iteration failures from a successful census in which a table is absent.
// Cause contains only a standard or translated SQL-driver error, never an
// engine object. No SQL failure is converted into physical absence.
type PhysicalReadError struct {
	Kind, CommitID, Table string
	Cause                 error
}

func (e *PhysicalReadError) Error() string {
	return fmt.Sprintf("physical commit %s %s %s: %v", e.CommitID, e.Table, e.Kind, e.Cause)
}
func (e *PhysicalReadError) Unwrap() error { return e.Cause }

var physicalCommitID = regexp.MustCompile(`^[0-9a-v]{32}$`)

func physicalReadError(kind, id, name string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &PhysicalReadError{kind, id, name, err}
	}
	return &PhysicalReadError{kind, id, name, translateError(err)}
}

// ReadPhysicalCommit owns one serializable read-only transaction on the supplied
// dedicated connection. It resolves the hash exactly once and materializes all
// values before rolling back and leaving Raw. It performs no SQL table lookup,
// nonlocal resolution, schema override, checkout, commit, or working-set write.
func ReadPhysicalCommit(ctx context.Context, conn *sql.Conn, id string) (image PhysicalImage, resultErr error) {
	if !physicalCommitID.MatchString(id) {
		return image, &PhysicalReadError{Kind: "invalid-commit", CommitID: id, Cause: errors.New("expected exact immutable commit hash")}
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
	if err != nil {
		return image, physicalReadError("transaction", id, "", err)
	}
	defer func() {
		err := tx.Rollback()
		if resultErr == nil && err != nil && !errors.Is(err, sql.ErrTxDone) {
			image = PhysicalImage{}
			resultErr = physicalReadError("rollback", id, "", err)
		}
	}()
	resultErr = conn.Raw(func(raw any) error {
		d, ok := raw.(*DoltConn)
		if !ok {
			return &PhysicalReadError{Kind: "unsupported-driver", CommitID: id, Cause: errors.New("embedded Dolt connection required")}
		}
		var err error
		image, err = d.readPhysicalCommit(ctx, id)
		return err
	})
	if resultErr != nil {
		image = PhysicalImage{}
	}
	return
}

func (d *DoltConn) readPhysicalCommit(ctx context.Context, id string) (PhysicalImage, error) {
	q, err := d.beginQuery(ctx, "private immutable physical commit image")
	if err != nil {
		return PhysicalImage{}, physicalReadError("query", id, "", err)
	}
	defer d.endQuery(q)
	// The embedded connector establishes the local root principal. Refuse other
	// principals rather than bypassing SQL grants through this private interface.
	if q.Client().User != "root" {
		return PhysicalImage{}, &PhysicalReadError{Kind: "permission", CommitID: id, Cause: errors.New("physical capture requires local root principal")}
	}
	database := q.GetCurrentDatabase()
	db, err := d.se.GetUnderlyingEngine().Analyzer.Catalog.Database(q, database)
	if err != nil {
		return PhysicalImage{}, physicalReadError("database", id, "", err)
	}
	physicalDB, ok := db.(dsess.SqlDatabase)
	if !ok {
		return PhysicalImage{}, &PhysicalReadError{Kind: "database", CommitID: id, Cause: errors.New("Dolt database required")}
	}
	sess := dsess.DSessFromSess(q.Session)
	ddb, ok := sess.GetDoltDB(q, database)
	if !ok {
		return PhysicalImage{}, &PhysicalReadError{Kind: "database", CommitID: id, Cause: errors.New("Dolt database unavailable")}
	}
	spec, err := doltdb.NewCommitSpec(id)
	if err != nil {
		return PhysicalImage{}, physicalReadError("resolve", id, "", err)
	}
	optional, err := ddb.Resolve(q, spec, nil)
	if err != nil {
		return PhysicalImage{}, physicalReadError("resolve", id, "", err)
	}
	commit, ok := optional.ToCommit()
	if !ok {
		return PhysicalImage{}, physicalReadError("resolve", id, "", doltdb.ErrGhostCommitEncountered)
	}
	actual, err := commit.HashOf()
	if err != nil {
		return PhysicalImage{}, physicalReadError("identity", id, "", err)
	}
	if actual.String() != id {
		return PhysicalImage{}, &PhysicalReadError{Kind: "identity", CommitID: id, Cause: errors.New("resolved hash differs")}
	}
	root, err := commit.GetRootValue(q)
	if err != nil {
		return PhysicalImage{}, physicalReadError("root", id, "", err)
	}
	schemas, err := root.GetDatabaseSchemas(q)
	if err != nil {
		return PhysicalImage{}, physicalReadError("census", id, "", err)
	}
	for _, schema := range schemas {
		if schema.Name != "" && schema.Name != "public" {
			return PhysicalImage{}, &PhysicalReadError{Kind: "unsupported-schema", CommitID: id, Cause: errors.New("nondefault database schema requires qualified physical-image support")}
		}
	}
	names, err := root.GetTableNames(q, "", false)
	if err != nil {
		return PhysicalImage{}, physicalReadError("census", id, "", err)
	}
	sort.Strings(names)
	image := PhysicalImage{CommitID: id}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return PhysicalImage{}, physicalReadError("canceled", id, name, err)
		}
		stored, present, err := root.GetTable(q, doltdb.TableName{Name: name})
		if err != nil {
			return PhysicalImage{}, physicalReadError("table", id, name, err)
		}
		if !present {
			return PhysicalImage{}, &PhysicalReadError{Kind: "census-inconsistent", CommitID: id, Table: name, Cause: errors.New("enumerated physical table missing from immutable root")}
		}
		schema, err := stored.GetSchema(q)
		if err != nil {
			return PhysicalImage{}, physicalReadError("schema", id, name, err)
		}
		table, err := sqle.NewDoltTable(q, name, schema, stored, physicalDB, editor.Options{})
		if err != nil {
			return PhysicalImage{}, physicalReadError("schema", id, name, err)
		}
		locked, err := table.LockedToRoot(q, root)
		if err != nil {
			return PhysicalImage{}, physicalReadError("root-lock", id, name, err)
		}
		converted, err := readPhysicalTable(q, locked, name)
		if err != nil {
			return PhysicalImage{}, physicalReadError("rows", id, name, err)
		}
		image.Tables = append(image.Tables, converted)
	}
	return image, nil
}

func readPhysicalTable(ctx *gms.Context, table gms.Table, name string) (out PhysicalTable, resultErr error) {
	out.Name = name
	schema := table.Schema(ctx)
	for _, column := range schema {
		out.Columns = append(out.Columns, PhysicalColumn{column.Name, column.Type.String()})
	}
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return out, err
	}
	defer func() {
		if err := partitions.Close(ctx); resultErr == nil {
			resultErr = err
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return out, err
		}
		// Reuse the driver's existing value/enum/set/binary conversion, with the
		// physical schema rather than any current/history helper schema.
		converted := doltRows{sch: schema, rowIter: rows, gmsCtx: ctx}
		for {
			values := make([]driver.Value, len(schema))
			err := converted.Next(values)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = converted.Close()
				return out, err
			}
			for i, value := range values {
				if binary, ok := value.([]byte); ok {
					values[i] = append([]byte(nil), binary...)
				}
			}
			out.Rows = append(out.Rows, values)
		}
		if err := converted.Close(); err != nil {
			return out, err
		}
	}
}
