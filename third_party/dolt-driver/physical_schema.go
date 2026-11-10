// Copyright 2026 BuildBetter. Private capture cursor observation; not an upstream API.
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

// PhysicalCommitSchema reads ONLY the two fixed migration cursor tables from
// an exact immutable root. RootTableCount distinguishes a truly empty genesis
// from a nonempty root with a missing cursor. No current alias or SQL AS OF is
// consulted, and no user-table cell is decoded by this operation.
type PhysicalCommitSchema struct {
	CommitID       string
	ReadOnlyScope  bool
	RootTableCount int
	Tables         []PhysicalTable
}

// ReadPhysicalCommitSchema has the same dedicated-idle/read-only/root identity
// boundary as ReadPhysicalCommit, but can establish a numeric schema cursor
// even if decoding a different historical user table subsequently fails.
func ReadPhysicalCommitSchema(ctx context.Context, conn *sql.Conn, id string) (image PhysicalCommitSchema, resultErr error) {
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
				image = PhysicalCommitSchema{}
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
		image, scopeErr = d.readPhysicalCommitSchema(ctx, id)
		if scopeErr == nil {
			image.ReadOnlyScope = true
		}
		return scopeErr
	})
	if resultErr != nil {
		image = PhysicalCommitSchema{}
	}
	return
}

func (d *DoltConn) readPhysicalCommitSchema(ctx context.Context, id string) (PhysicalCommitSchema, error) {
	q, err := d.beginQuery(ctx, "private immutable physical commit image")
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("query", id, "", err)
	}
	defer d.endQuery(q)
	// The embedded connector establishes the local root principal. Refuse other
	// principals rather than bypassing SQL grants through this private interface.
	if q.Client().User != "root" {
		return PhysicalCommitSchema{}, &PhysicalReadError{Kind: "permission", CommitID: id, Cause: errors.New("physical capture requires local root principal")}
	}
	database := q.GetCurrentDatabase()
	db, err := d.se.GetUnderlyingEngine().Analyzer.Catalog.Database(q, database)
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("database", id, "", err)
	}
	physicalDB, ok := db.(dsess.SqlDatabase)
	if !ok {
		return PhysicalCommitSchema{}, &PhysicalReadError{Kind: "database", CommitID: id, Cause: errors.New("Dolt database required")}
	}
	sess := dsess.DSessFromSess(q.Session)
	ddb, ok := sess.GetDoltDB(q, database)
	if !ok {
		return PhysicalCommitSchema{}, &PhysicalReadError{Kind: "database", CommitID: id, Cause: errors.New("Dolt database unavailable")}
	}
	spec, err := doltdb.NewCommitSpec(id)
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("resolve", id, "", err)
	}
	optional, err := ddb.Resolve(q, spec, nil)
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("resolve", id, "", err)
	}
	commit, ok := optional.ToCommit()
	if !ok {
		return PhysicalCommitSchema{}, physicalReadError("resolve", id, "", doltdb.ErrGhostCommitEncountered)
	}
	actual, err := commit.HashOf()
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("identity", id, "", err)
	}
	if actual.String() != id {
		return PhysicalCommitSchema{}, &PhysicalReadError{Kind: "identity", CommitID: id, Cause: errors.New("resolved hash differs")}
	}
	root, err := commit.GetRootValue(q)
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("root", id, "", err)
	}
	schemas, err := root.GetDatabaseSchemas(q)
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("census", id, "", err)
	}
	for _, schema := range schemas {
		if schema.Name != "" && schema.Name != "public" {
			return PhysicalCommitSchema{}, &PhysicalReadError{Kind: "unsupported-schema", CommitID: id, Cause: errors.New("nondefault database schema requires qualified physical-image support")}
		}
	}
	names, err := root.GetTableNames(q, "", false)
	if err != nil {
		return PhysicalCommitSchema{}, physicalReadError("census", id, "", err)
	}
	sort.Strings(names)
	image := PhysicalCommitSchema{CommitID: id, RootTableCount: len(names)}
	for _, name := range names {
		if name != "schema_migrations" && name != "ignored_schema_migrations" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return PhysicalCommitSchema{}, physicalReadError("canceled", id, name, err)
		}
		stored, present, err := root.GetTable(q, doltdb.TableName{Name: name})
		if err != nil {
			return PhysicalCommitSchema{}, physicalReadError("table", id, name, err)
		}
		if !present {
			return PhysicalCommitSchema{}, &PhysicalReadError{Kind: "census-inconsistent", CommitID: id, Table: name, Cause: errors.New("enumerated physical table missing from immutable root")}
		}
		schema, err := stored.GetSchema(q)
		if err != nil {
			return PhysicalCommitSchema{}, physicalReadError("schema", id, name, err)
		}
		table, err := sqle.NewDoltTable(q, name, schema, stored, physicalDB, editor.Options{})
		if err != nil {
			return PhysicalCommitSchema{}, physicalReadError("schema", id, name, err)
		}
		locked, err := table.LockedToRoot(q, root)
		if err != nil {
			return PhysicalCommitSchema{}, physicalReadError("root-lock", id, name, err)
		}
		converted, err := readPhysicalTable(q, locked, name)
		if err != nil {
			return PhysicalCommitSchema{}, physicalReadError("rows", id, name, err)
		}
		image.Tables = append(image.Tables, converted)
	}
	return image, nil
}
