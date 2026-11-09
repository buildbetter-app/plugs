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
	"time"

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
	CommitID      string
	ReadOnlyScope bool
	Tables        []PhysicalTable
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

// ReadPhysicalCommit owns a read-only transaction inside one dedicated Raw
// connection scope. The pinned BeginTx ignores its ReadOnly argument, so this
// operation explicitly starts READ ONLY and authenticates the actual engine
// transaction. It refuses an occupied connection rather than committing an
// existing scope. Every value is detached before rollback and leaving Raw.
func ReadPhysicalCommit(ctx context.Context, conn *sql.Conn, id string) (image PhysicalImage, resultErr error) {
	if !physicalCommitID.MatchString(id) {
		return image, &PhysicalReadError{Kind: "invalid-commit", CommitID: id, Cause: errors.New("expected exact immutable commit hash")}
	}
	resultErr = conn.Raw(func(raw any) (scopeErr error) {
		d, ok := raw.(*DoltConn)
		if !ok {
			return &PhysicalReadError{Kind: "unsupported-driver", CommitID: id, Cause: errors.New("embedded Dolt connection required")}
		}
		if d.gmsCtx.GetTransaction() != nil {
			return &PhysicalReadError{Kind: "occupied-transaction", CommitID: id, Cause: errors.New("dedicated idle connection required")}
		}
		query, _, rows, err := d.queryWithBindings(ctx, "START TRANSACTION READ ONLY", nil)
		if err != nil {
			return physicalReadError("transaction", id, "", err)
		}
		// Preserve the driver's ordinary transaction implementation and rollback
		// context; no database/sql transaction exists outside this Raw lifetime.
		rollback := doltTx{ctx: d.gmsCtx.Context, conn: d}
		defer func() {
			rollbackErr := rollback.Rollback()
			if rollbackErr == nil && d.gmsCtx.GetTransaction() != nil {
				rollbackErr = errors.New("physical transaction remains active after rollback")
			}
			if rollbackErr != nil {
				image = PhysicalImage{}
				scopeErr = physicalReadError("rollback", id, "", rollbackErr)
			}
		}()
		if err := rows.Close(query); err != nil {
			return physicalReadError("transaction", id, "", err)
		}
		transaction := d.gmsCtx.GetTransaction()
		if transaction == nil || !transaction.IsReadOnly() {
			return &PhysicalReadError{Kind: "transaction", CommitID: id, Cause: errors.New("actual read-only transaction not established")}
		}
		image, scopeErr = d.readPhysicalCommit(ctx, id)
		if scopeErr == nil {
			image.ReadOnlyScope = true
		}
		return scopeErr
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
				detached, err := physicalCell(value)
				if err != nil {
					_ = converted.Close()
					return out, err
				}
				values[i] = detached
			}
			out.Rows = append(out.Rows, values)
		}
		if err := converted.Close(); err != nil {
			return out, err
		}
	}
}

// driver.Value is an interface, so enumerate concrete detached scalar types.
// Numeric widths and uint64 remain exact; unsupported engine/custom values
// fail decoding rather than escaping Raw or becoming physical absence.
func physicalCell(value driver.Value) (driver.Value, error) {
	switch cell := value.(type) {
	case nil, bool, string, time.Time,
		int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return cell, nil
	case []byte:
		return append([]byte{}, cell...), nil
	default:
		return nil, fmt.Errorf("unsupported physical cell type %T", value)
	}
}
