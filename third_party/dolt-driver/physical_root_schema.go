package embedded

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// PhysicalSchemaColumn is detached stored-schema metadata. PrimaryKeyOrdinal
// is the zero-based position in the stored primary key, or nil for a non-key
// column. It deliberately does not extend PhysicalColumn's row/readback domain.
type PhysicalSchemaColumn struct {
	Name, DatabaseTypeName string
	PrimaryKeyOrdinal      *uint64
}

type PhysicalSchemaTable struct {
	Name    string
	Columns []PhysicalSchemaColumn
}

// PhysicalRootSchema describes every stored table in one authenticated root.
// CommitID is the selected immutable commit, or the actual HEAD for a working
// observation. RootHash is always the observed root, never an inferred alias.
type PhysicalRootSchema struct {
	Database, CommitID, RootHash string
	ReadOnlyScope, Complete      bool
	Tables                       []PhysicalSchemaTable
}

// ReadPhysicalWorkingRootSchema observes the actual working root at expectedHead.
// Callers bind RootHash to their original working-root receipt before writing.
func ReadPhysicalWorkingRootSchema(ctx context.Context, conn *sql.Conn, expectedHead string) (PhysicalRootSchema, error) {
	return readPhysicalRootSchema(ctx, conn, expectedHead, true)
}

// ReadPhysicalCommitRootSchema resolves an exact immutable commit and reads its
// complete stored schema, independently of the current working schema. Neither
// operation reads user rows or consults SQL metadata table/index adapters.
func ReadPhysicalCommitRootSchema(ctx context.Context, conn *sql.Conn, id string) (PhysicalRootSchema, error) {
	return readPhysicalRootSchema(ctx, conn, id, false)
}

func readPhysicalRootSchema(ctx context.Context, conn *sql.Conn, id string, working bool) (image PhysicalRootSchema, resultErr error) {
	if !physicalCommitID.MatchString(id) {
		return image, physicalReadError("invalid-commit", id, "", errors.New("exact immutable commit hash required"))
	}
	if conn == nil {
		return image, physicalReadError("unsupported-driver", id, "", errors.New("dedicated embedded connection required"))
	}
	resultErr = conn.Raw(func(raw any) (scopeErr error) {
		d, ok := raw.(*DoltConn)
		if !ok {
			return physicalReadError("unsupported-driver", id, "", errors.New("embedded Dolt connection required"))
		}
		if d.gmsCtx.GetTransaction() != nil {
			return physicalReadError("occupied-transaction", id, "", errors.New("dedicated idle connection required"))
		}
		query, _, rows, err := d.queryWithBindings(ctx, "START TRANSACTION READ ONLY", nil)
		if err != nil {
			return physicalReadError("transaction", id, "", err)
		}
		rollback := doltTx{ctx: d.gmsCtx.Context, conn: d}
		defer func() {
			err := rollback.Rollback()
			if err == nil && d.gmsCtx.GetTransaction() != nil {
				err = errors.New("schema observation transaction remains active")
			}
			if err != nil {
				scopeErr = errors.Join(scopeErr, physicalReadError("rollback", id, "", err))
			}
		}()
		if err := rows.Close(query); err != nil {
			return physicalReadError("transaction", id, "", err)
		}
		tx := d.gmsCtx.GetTransaction()
		if tx == nil || !tx.IsReadOnly() {
			return physicalReadError("transaction", id, "", errors.New("actual read-only transaction required"))
		}
		q, err := d.beginQuery(ctx, "private stored-root schema observation")
		if err != nil {
			return physicalReadError("query", id, "", err)
		}
		defer d.endQuery(q)
		if q.Client().User != "root" {
			return physicalReadError("permission", id, "", errors.New("local root principal required"))
		}
		database := q.GetCurrentDatabase()
		db, err := d.se.GetUnderlyingEngine().Analyzer.Catalog.Database(q, database)
		if err != nil {
			return physicalReadError("database", id, "", err)
		}
		physicalDB, ok := db.(dsess.SqlDatabase)
		if !ok {
			return physicalReadError("database", id, "", errors.New("Dolt database required"))
		}
		var root doltdb.RootValue
		if working {
			head, err := dsess.DSessFromSess(q.Session).GetHeadCommit(q, database)
			if err != nil {
				return physicalReadError("identity", id, "", err)
			}
			if head == nil {
				return physicalReadError("identity", id, "", errors.New("working source head unavailable"))
			}
			actual, err := head.HashOf()
			if err != nil {
				return physicalReadError("identity", id, "", err)
			}
			if actual.String() != id {
				return physicalReadError("identity", id, "", errors.New("working source head differs"))
			}
			root, err = physicalDB.GetRoot(q)
			if err != nil {
				return physicalReadError("root", id, "", err)
			}
		} else {
			ddb, ok := dsess.DSessFromSess(q.Session).GetDoltDB(q, database)
			if !ok {
				return physicalReadError("database", id, "", errors.New("Dolt database unavailable"))
			}
			spec, err := doltdb.NewCommitSpec(id)
			if err != nil {
				return physicalReadError("resolve", id, "", err)
			}
			optional, err := ddb.Resolve(q, spec, nil)
			if err != nil {
				return physicalReadError("resolve", id, "", err)
			}
			commit, ok := optional.ToCommit()
			if !ok {
				return physicalReadError("resolve", id, "", doltdb.ErrGhostCommitEncountered)
			}
			actual, err := commit.HashOf()
			if err != nil {
				return physicalReadError("identity", id, "", err)
			}
			if actual.String() != id {
				return physicalReadError("identity", id, "", errors.New("resolved commit hash differs"))
			}
			root, err = commit.GetRootValue(q)
			if err != nil {
				return physicalReadError("root", id, "", err)
			}
		}
		rootHash, err := root.HashOf()
		if err != nil {
			return physicalReadError("identity", id, "", err)
		}
		schemas, err := root.GetDatabaseSchemas(q)
		if err != nil {
			return physicalReadError("census", id, "", err)
		}
		for _, schema := range schemas {
			if schema.Name != "" && schema.Name != "public" {
				return physicalReadError("unsupported-schema", id, "", errors.New("nondefault physical schema unsupported"))
			}
		}
		names, err := root.GetTableNames(q, "", false)
		if err != nil {
			return physicalReadError("census", id, "", err)
		}
		sort.Strings(names)
		image = PhysicalRootSchema{Database: database, CommitID: id, RootHash: rootHash.String(), Tables: make([]PhysicalSchemaTable, 0, len(names))}
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return physicalReadError("canceled", id, name, err)
			}
			stored, present, err := root.GetTable(q, doltdb.TableName{Name: name})
			if err != nil {
				return physicalReadError("table", id, name, err)
			}
			if !present {
				return physicalReadError("census-inconsistent", id, name, errors.New("enumerated stored table missing"))
			}
			schema, err := stored.GetSchema(q)
			if err != nil {
				return physicalReadError("schema", id, name, err)
			}
			columns := schema.GetAllCols().GetColumns()
			table := PhysicalSchemaTable{Name: name, Columns: make([]PhysicalSchemaColumn, 0, len(columns))}
			for _, column := range columns {
				table.Columns = append(table.Columns, PhysicalSchemaColumn{Name: column.Name, DatabaseTypeName: column.TypeInfo.ToSqlType().String()})
			}
			seen := make(map[int]bool)
			for order, column := range schema.GetPkOrdinals() {
				if column < 0 || column >= len(table.Columns) || seen[column] {
					return physicalReadError("schema", id, name, errors.New("invalid stored primary key ordinals"))
				}
				seen[column] = true
				ordinal := uint64(order)
				table.Columns[column].PrimaryKeyOrdinal = &ordinal
			}
			image.Tables = append(image.Tables, table)
		}
		if err := ctx.Err(); err != nil {
			return physicalReadError("canceled", id, "", err)
		}
		image.ReadOnlyScope, image.Complete = true, true
		return nil
	})
	if resultErr != nil {
		image = PhysicalRootSchema{}
		var refusal *PhysicalReadError
		if !errors.As(resultErr, &refusal) {
			resultErr = &PhysicalReadError{Kind: "connection", CommitID: id, Cause: resultErr}
		}
	}
	return
}
