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
	"github.com/dolthub/dolt/go/store/datas"
	gms "github.com/dolthub/go-mysql-server/sql"
)

// PhysicalColumn and PhysicalTable contain detached driver values only. Rows
// preserve order, multiplicity, NULLs, and independently owned binary cells.
type PhysicalColumn struct {
	Name, DatabaseTypeName string
	PrimaryKeyOrdinal      *uint64
}
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
// Cause contains detached original type/code evidence and the translated
// SQL-driver error, never an engine object. No SQL failure alone proves absence.
type PhysicalReadError struct {
	Kind, CommitID, Table string
	Cause                 error
}

func (e *PhysicalReadError) Error() string {
	return fmt.Sprintf("physical commit %s %s %s: %v", e.CommitID, e.Table, e.Kind, e.Cause)
}
func (e *PhysicalReadError) Unwrap() error { return e.Cause }

// PhysicalStoreInconsistent refuses a read that failed after its stored chunk
// closure was authenticated as present. It can never establish a history
// boundary, even if a later engine error happens to wrap a missing sentinel.
// PanicType retains only the Go type; panic values may contain user data.
type PhysicalStoreInconsistent struct {
	Cause     error
	PanicType string
}

func (*PhysicalStoreInconsistent) Error() string   { return "probe-complete physical store read failed" }
func (e *PhysicalStoreInconsistent) Unwrap() error { return e.Cause }

var physicalCommitID = regexp.MustCompile(`^[0-9a-v]{32}$`)

func physicalReadError(kind, id, name string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &PhysicalReadError{kind, id, name, err}
	}
	var cause error = &physicalErrorEvidence{cause: translateError(err), types: physicalErrorTypes(err), codes: physicalHistorySQLCodes(err), innermost: physicalInnermostErrorType(err)}
	var inconsistent *PhysicalStoreInconsistent
	if errors.As(err, &inconsistent) {
		cause = &PhysicalStoreInconsistent{Cause: cause, PanicType: inconsistent.PanicType}
	} else if reason := missingPhysicalHistory(kind, err); reason != "" {
		cause = &PhysicalHistoryUnavailable{Reason: reason, Cause: cause, SQLCodes: physicalHistorySQLCodes(err)}
	}
	return &PhysicalReadError{kind, id, name, cause}
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
				scopeErr = errors.Join(scopeErr, physicalReadError("rollback", id, "", rollbackErr))
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

func (d *DoltConn) readPhysicalCommit(ctx context.Context, id string) (image PhysicalImage, resultErr error) {
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
	root, err := readPhysicalRoot(q, ddb, commit)
	if err != nil {
		return PhysicalImage{}, physicalReadError("root", id, "", err)
	}
	address, err := datas.GetCommitRootHash(commit.Value())
	if err != nil {
		return PhysicalImage{}, physicalReadError("root", id, "", err)
	}
	store := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	if err := probePhysicalRootChunks(q, store, address); err != nil {
		if missingPhysicalHistory("rows", err) != "" {
			if validationErr := checkPhysicalIncompleteRoot(q, ddb, root); validationErr != nil {
				if reason := missingPhysicalHistory("rows", validationErr); reason == "" {
					err = errors.Join(err, validationErr)
				} else if reason == "unreadable_schema" {
					err = validationErr
				}
			}
		}
		return PhysicalImage{}, physicalReadError("rows", id, "", err)
	}
	// Only this reader boundary recovers engine panics, and only after the
	// immutable root's entire reference closure passed the backing probe.
	defer func() {
		if value := recover(); value != nil {
			cause, _ := value.(error)
			resultErr = physicalReadError("inconsistent-store", id, "", &PhysicalStoreInconsistent{Cause: cause, PanicType: fmt.Sprintf("%T", value)})
		} else if resultErr != nil {
			var inconsistent *PhysicalStoreInconsistent
			if !errors.As(resultErr, &inconsistent) {
				// Keep the original evidence snapshot intact. Re-translating
				// its fallback MySQL error would invent an observed SQL code.
				var failure *PhysicalReadError
				var table string
				if errors.As(resultErr, &failure) {
					table = failure.Table
				}
				resultErr = &PhysicalReadError{Kind: "inconsistent-store", CommitID: id, Table: table, Cause: &PhysicalStoreInconsistent{Cause: resultErr}}
			}
		}
		if resultErr != nil {
			image = PhysicalImage{}
		}
	}()
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
	image = PhysicalImage{CommitID: id}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return PhysicalImage{}, physicalReadError("canceled", id, name, err)
		}
		stored, present, err := readPhysicalStoredTable(q, ddb, root, doltdb.TableName{Name: name})
		if err != nil {
			return PhysicalImage{}, physicalReadError("table", id, name, err)
		}
		if !present {
			return PhysicalImage{}, &PhysicalReadError{Kind: "census-inconsistent", CommitID: id, Table: name, Cause: errors.New("enumerated physical table missing from immutable root")}
		}
		if err := checkPhysicalSchemaChunk(q, ddb, stored); err != nil {
			return PhysicalImage{}, physicalReadError("schema", id, name, err)
		}
		schema, err := stored.GetSchema(q)
		if err != nil {
			return PhysicalImage{}, physicalReadError("schema", id, name, err)
		}
		if err := checkPhysicalTableRowChunks(q, ddb, stored); err != nil {
			return PhysicalImage{}, physicalReadError("rows", id, name, err)
		}
		table, err := sqle.NewDoltTable(q, name, schema, stored, physicalDB, editor.Options{})
		if err != nil {
			return PhysicalImage{}, physicalReadError("schema", id, name, err)
		}
		locked, err := table.LockedToRoot(q, root)
		if err != nil {
			return PhysicalImage{}, physicalReadError("root-lock", id, name, err)
		}
		converted, err := readPhysicalProbedTable(q, locked, name)
		if err != nil {
			return PhysicalImage{}, physicalReadError("rows", id, name, err)
		}
		if err := physicalPrimaryKeyOrdinals(&converted, schema.GetPkOrdinals()); err != nil {
			return PhysicalImage{}, physicalReadError("schema", id, name, err)
		}
		image.Tables = append(image.Tables, converted)
	}
	return image, nil
}

func readPhysicalTable(ctx *gms.Context, table gms.Table, name string) (out PhysicalTable, resultErr error) {
	return readPhysicalTableRows(ctx, table, name, false)
}

func readPhysicalProbedTable(ctx *gms.Context, table gms.Table, name string) (PhysicalTable, error) {
	return readPhysicalTableRows(ctx, table, name, true)
}

func readPhysicalTableRows(ctx *gms.Context, table gms.Table, name string, probed bool) (out PhysicalTable, resultErr error) {
	if probed {
		defer func() {
			if value := recover(); value != nil {
				cause, _ := value.(error)
				// Iterator cleanup defers have already run. Keep their errors
				// alongside the original panic type and stop the read.
				resultErr = &PhysicalStoreInconsistent{Cause: errors.Join(cause, resultErr), PanicType: fmt.Sprintf("%T", value)}
			} else if resultErr != nil {
				var inconsistent *PhysicalStoreInconsistent
				if !errors.As(resultErr, &inconsistent) {
					resultErr = &PhysicalStoreInconsistent{Cause: resultErr}
				}
			}
			if resultErr != nil {
				out = PhysicalTable{}
			}
		}()
	}
	out.Name = name
	schema := table.Schema(ctx)
	for _, column := range schema {
		out.Columns = append(out.Columns, PhysicalColumn{Name: column.Name, DatabaseTypeName: column.Type.String()})
	}
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return out, err
	}
	defer func() {
		resultErr = physicalReadCleanupError(resultErr, partitions.Close(ctx))
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
		values, err := readPhysicalRowsWithProbe(ctx, schema, rows, probed)
		if err != nil {
			return out, err
		}
		out.Rows = append(out.Rows, values...)
	}
}

// The immutable stored schema supplies key order, not display-column order or
// a family-name convention. This only detaches metadata; no schema is changed.
func physicalPrimaryKeyOrdinals(table *PhysicalTable, ordinals []int) error {
	seen := map[int]bool{}
	for order, column := range ordinals {
		if column < 0 || column >= len(table.Columns) || seen[column] {
			return errors.New("invalid stored primary key ordinals")
		}
		seen[column] = true
		value := uint64(order)
		table.Columns[column].PrimaryKeyOrdinal = &value
	}
	return nil
}

// Observe iterator failures before doltRows translates them for ordinary SQL
// compatibility. This adapter lives only in the immutable physical read path;
// the existing driver value/enum/set/geometry conversion stays unchanged.
type physicalRowIterator struct {
	gms.RowIter
	nextErr, closeErr error
}

func (rows *physicalRowIterator) Next(ctx *gms.Context) (gms.Row, error) {
	row, err := rows.RowIter.Next(ctx)
	if err != nil && err != io.EOF {
		rows.nextErr = err
	}
	return row, err
}

func (rows *physicalRowIterator) Close(ctx *gms.Context) error {
	rows.closeErr = rows.RowIter.Close(ctx)
	return rows.closeErr
}

func physicalReadCleanupError(readErr, closeErr error) error {
	if closeErr == nil {
		return readErr
	}
	closeErr = &physicalCleanupFailure{cause: closeErr}
	if readErr == nil {
		return closeErr
	}
	// A genuine read absence accompanied by failed cleanup must still refuse.
	return errors.Join(readErr, closeErr)
}

type physicalCleanupFailure struct{ cause error }

func (*physicalCleanupFailure) Error() string     { return "physical read cleanup failed" }
func (err *physicalCleanupFailure) Unwrap() error { return err.cause }

func readPhysicalRows(ctx *gms.Context, schema gms.Schema, iter gms.RowIter) (result [][]driver.Value, resultErr error) {
	return readPhysicalRowsWithProbe(ctx, schema, iter, false)
}

func readPhysicalRowsWithProbe(ctx *gms.Context, schema gms.Schema, iter gms.RowIter, probed bool) (result [][]driver.Value, resultErr error) {
	if probed {
		defer func() {
			if value := recover(); value != nil {
				cause, _ := value.(error)
				resultErr = &PhysicalStoreInconsistent{Cause: errors.Join(cause, resultErr), PanicType: fmt.Sprintf("%T", value)}
				result = nil
			}
		}()
	}
	observed := &physicalRowIterator{RowIter: iter}
	converted := doltRows{sch: schema, rowIter: observed, gmsCtx: ctx}
	defer func() {
		closeErr := converted.Close()
		if observed.closeErr != nil {
			closeErr = observed.closeErr
		}
		resultErr = physicalReadCleanupError(resultErr, closeErr)
		if resultErr != nil {
			result = nil
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		values := make([]driver.Value, len(schema))
		if err := converted.Next(values); err == io.EOF {
			return result, nil
		} else if err != nil {
			if observed.nextErr != nil {
				return nil, observed.nextErr
			}
			return nil, err
		}
		for i, value := range values {
			detached, err := physicalCell(value)
			if err != nil {
				return nil, err
			}
			values[i] = detached
		}
		result = append(result, values)
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
