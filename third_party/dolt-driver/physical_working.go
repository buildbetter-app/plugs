// Copyright 2026 BuildBetter. Private capture plumbing; unchanged engine semantics.
package embedded

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// PhysicalWorkingImage is a complete physical census of ONE working root. Head
// and root hashes are observed in the same authenticated read-only transaction.
// Database is observed, not a caller-supplied provenance label. This includes
// stored ignored tables and does not claim that their rows occur in history.
type PhysicalWorkingImage struct {
	Database, HeadCommitID, WorkingRootHash string
	ReadOnlyScope, Complete                 bool
	Tables                                  []PhysicalTable
}

// ReadPhysicalWorkingRoot requires a dedicated idle connection and an exact
// expected head. No expected table list, schema or row values enter the reader.
// Values and actual SQL types use the original immutable reader's cell boundary.
// Failure, including rollback failure, returns no partially qualified image.
func ReadPhysicalWorkingRoot(ctx context.Context, conn *sql.Conn, expectedHead string) (image PhysicalWorkingImage, resultErr error) {
	if !physicalCommitID.MatchString(expectedHead) {
		return image, physicalReadError("invalid-commit", "", "", errors.New("expected exact immutable head"))
	}
	resultErr = conn.Raw(func(raw any) (scopeErr error) {
		d, ok := raw.(*DoltConn)
		if !ok {
			return physicalReadError("unsupported-driver", "", "", errors.New("embedded Dolt connection required"))
		}
		if d.gmsCtx.GetTransaction() != nil {
			return physicalReadError("occupied-transaction", "", "", errors.New("dedicated idle connection required"))
		}
		query, _, rows, err := d.queryWithBindings(ctx, "START TRANSACTION READ ONLY", nil)
		if err != nil {
			return physicalReadError("transaction", "", "", err)
		}
		rollback := doltTx{ctx: d.gmsCtx.Context, conn: d}
		defer func() {
			rollbackErr := rollback.Rollback()
			if rollbackErr == nil && d.gmsCtx.GetTransaction() != nil {
				rollbackErr = errors.New("working-root capture transaction remains active")
			}
			if rollbackErr != nil {
				scopeErr = errors.Join(scopeErr, physicalReadError("rollback", "", "", rollbackErr))
			}
		}()
		if err := rows.Close(query); err != nil {
			return physicalReadError("transaction", "", "", err)
		}
		transaction := d.gmsCtx.GetTransaction()
		if transaction == nil || !transaction.IsReadOnly() {
			return physicalReadError("transaction", "", "", errors.New("actual read-only transaction not established"))
		}
		image, scopeErr = d.readPhysicalWorkingRoot(ctx, expectedHead)
		if scopeErr == nil {
			image.ReadOnlyScope = true
		}
		return scopeErr
	})
	if resultErr != nil {
		image = PhysicalWorkingImage{}
	}
	return
}

func (d *DoltConn) readPhysicalWorkingRoot(ctx context.Context, expectedHead string) (PhysicalWorkingImage, error) {
	q, err := d.beginQuery(ctx, "private typed physical working-root census")
	if err != nil {
		return PhysicalWorkingImage{}, physicalReadError("query", "", "", err)
	}
	defer d.endQuery(q)
	fail := func(kind, table string, err error) (PhysicalWorkingImage, error) {
		return PhysicalWorkingImage{}, physicalReadError(kind, "", table, err)
	}
	if q.Client().User != "root" {
		return fail("permission", "", errors.New("local root principal required"))
	}
	database := q.GetCurrentDatabase()
	db, err := d.se.GetUnderlyingEngine().Analyzer.Catalog.Database(q, database)
	if err != nil {
		return fail("database", "", err)
	}
	physicalDB, ok := db.(dsess.SqlDatabase)
	if !ok {
		return fail("database", "", errors.New("Dolt database required"))
	}
	head, err := dsess.DSessFromSess(q.Session).GetHeadCommit(q, database)
	if err != nil {
		return fail("identity", "", err)
	}
	if head == nil {
		return fail("identity", "", errors.New("working source head unavailable"))
	}
	headHash, err := head.HashOf()
	if err != nil {
		return fail("identity", "", err)
	}
	if headHash.String() != expectedHead {
		return fail("identity", "", errors.New("working source head differs from expected head"))
	}
	root, err := physicalDB.GetRoot(q)
	if err != nil {
		return fail("root", "", err)
	}
	rootHash, err := root.HashOf()
	if err != nil {
		return fail("identity", "", err)
	}
	schemas, err := root.GetDatabaseSchemas(q)
	if err != nil {
		return fail("census", "", err)
	}
	for _, schema := range schemas {
		if schema.Name != "" && schema.Name != "public" {
			return fail("unsupported-schema", "", errors.New("nondefault physical schema unsupported"))
		}
	}
	names, err := root.GetTableNames(q, "", false)
	if err != nil {
		return fail("census", "", err)
	}
	sort.Strings(names)
	image := PhysicalWorkingImage{Database: database, HeadCommitID: headHash.String(), WorkingRootHash: rootHash.String(), Tables: make([]PhysicalTable, 0, len(names))}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return fail("canceled", name, err)
		}
		stored, present, err := root.GetTable(q, doltdb.TableName{Name: name})
		if err != nil {
			return fail("table", name, err)
		}
		if !present {
			return fail("census-inconsistent", name, errors.New("enumerated working table missing"))
		}
		schema, err := stored.GetSchema(q)
		if err != nil {
			return fail("schema", name, err)
		}
		table, err := sqle.NewDoltTable(q, name, schema, stored, physicalDB, editor.Options{})
		if err != nil {
			return fail("schema", name, err)
		}
		locked, err := table.LockedToRoot(q, root)
		if err != nil {
			return fail("root-lock", name, err)
		}
		converted, err := readPhysicalTable(q, locked, name)
		if err != nil {
			return fail("rows", name, err)
		}
		if err := physicalPrimaryKeyOrdinals(&converted, schema.GetPkOrdinals()); err != nil {
			return fail("schema", name, err)
		}
		image.Tables = append(image.Tables, converted)
	}
	if err := ctx.Err(); err != nil {
		return fail("canceled", "", err)
	}
	image.Complete = true
	return image, nil
}
