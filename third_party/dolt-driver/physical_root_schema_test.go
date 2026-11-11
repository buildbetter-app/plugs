package embedded

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func rootSchemaFixture(t *testing.T) (context.Context, *sql.Conn) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	params := url.Values{"commitname": {"Public fixture"}, "commitemail": {"fixture@example.test"}, "disable_singleton_cache": {"true"}, "multistatements": {"true"}}
	cfg, err := ParseDSN("file://" + dir + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close(); _ = connector.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rootSchemaSQL(t, ctx, conn, "CREATE DATABASE public_root_schema")
	rootSchemaSQL(t, ctx, conn, "USE public_root_schema")
	return ctx, conn
}

func rootSchemaSQL(t *testing.T, ctx context.Context, conn *sql.Conn, statement string) {
	t.Helper()
	rows, err := conn.QueryContext(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for {
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func rootSchemaCommit(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	rootSchemaSQL(t, ctx, conn, "CALL DOLT_ADD('-A')")
	rootSchemaSQL(t, ctx, conn, "CALL DOLT_COMMIT('--allow-empty','-m','public root schema','--author','Fixture <fixture@example.test>')")
	var id string
	if err := conn.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

type originalSchemaTable struct {
	names, types []string
	keys         map[string]int
}

// This oracle independently visits the original stored root before invoking
// either export. It never calls the metadata reader or its conversion logic,
// and represents PK authority by column name rather than exported pointers.
func originalStoredSchema(t *testing.T, ctx context.Context, conn *sql.Conn, commitID string) (string, map[string]originalSchemaTable) {
	t.Helper()
	rootSchemaSQL(t, ctx, conn, "START TRANSACTION READ ONLY")
	defer rootSchemaSQL(t, ctx, conn, "ROLLBACK")
	var rootHash string
	expected := make(map[string]originalSchemaTable)
	err := conn.Raw(func(raw any) error {
		d := raw.(*DoltConn)
		q, err := d.beginQuery(ctx, "independent original stored schema oracle")
		if err != nil {
			return err
		}
		defer d.endQuery(q)
		db, err := d.se.GetUnderlyingEngine().Analyzer.Catalog.Database(q, q.GetCurrentDatabase())
		if err != nil {
			return err
		}
		var root doltdb.RootValue
		if commitID == "" {
			root, err = db.(dsess.SqlDatabase).GetRoot(q)
		} else {
			ddb, ok := dsess.DSessFromSess(q.Session).GetDoltDB(q, q.GetCurrentDatabase())
			if !ok {
				return errors.New("oracle database missing")
			}
			spec, err := doltdb.NewCommitSpec(commitID)
			if err != nil {
				return err
			}
			optional, err := ddb.Resolve(q, spec, nil)
			if err != nil {
				return err
			}
			commit, ok := optional.ToCommit()
			if !ok {
				return errors.New("oracle commit missing")
			}
			root, err = commit.GetRootValue(q)
		}
		if err != nil {
			return err
		}
		hash, err := root.HashOf()
		if err != nil {
			return err
		}
		rootHash = hash.String()
		names, err := root.GetTableNames(q, "", false)
		if err != nil {
			return err
		}
		for _, name := range names {
			table, ok, err := root.GetTable(q, doltdb.TableName{Name: name})
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("oracle table missing")
			}
			definition, err := table.GetSchema(q)
			if err != nil {
				return err
			}
			want := originalSchemaTable{keys: make(map[string]int)}
			columns := definition.GetAllCols().GetColumns()
			for _, column := range columns {
				want.names = append(want.names, column.Name)
				want.types = append(want.types, column.TypeInfo.ToSqlType().String())
			}
			for position, ordinal := range definition.GetPkOrdinals() {
				want.keys[columns[ordinal].Name] = position
			}
			expected[name] = want
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return rootHash, expected
}

func assertOriginalSchema(t *testing.T, image PhysicalRootSchema, rootHash string, expected map[string]originalSchemaTable) {
	t.Helper()
	if !image.Complete || !image.ReadOnlyScope || image.Database != "public_root_schema" || image.RootHash != rootHash || len(image.Tables) != len(expected) {
		t.Fatalf("root/schema identity or complete census differs: %+v", image)
	}
	seen := make(map[string]bool)
	for _, table := range image.Tables {
		want, ok := expected[table.Name]
		if !ok || seen[table.Name] || len(table.Columns) != len(want.names) {
			t.Fatalf("extra/duplicate/incomplete schema table %s", table.Name)
		}
		seen[table.Name] = true
		keys := make(map[string]int)
		for i, column := range table.Columns {
			if column.Name != want.names[i] || column.DatabaseTypeName != want.types[i] {
				t.Fatalf("original ordered column/type differs: %s[%d]", table.Name, i)
			}
			if column.PrimaryKeyOrdinal != nil {
				keys[column.Name] = int(*column.PrimaryKeyOrdinal)
			}
		}
		if !reflect.DeepEqual(keys, want.keys) {
			t.Fatalf("original stored PRIMARY differs for %s: got %v want %v", table.Name, keys, want.keys)
		}
	}
	for _, name := range []string{"dolt_schemas", "dolt_ignore"} {
		if !seen[name] || len(expected[name].keys) == 0 {
			t.Fatalf("reserved stored PRIMARY was not independently observed: %s", name)
		}
	}
	t.Logf("original_root=%s complete_tables=%d reserved_stored_primary_authority=verified", rootHash, len(expected))
}

func TestPhysicalRootSchemaOriginalAuthority(t *testing.T) {
	ctx, conn := rootSchemaFixture(t)
	for _, statement := range []string{
		"CREATE TABLE port_ordered (a BIGINT,b BIGINT,payload VARBINARY(8),PRIMARY KEY(b,a))",
		"INSERT INTO port_ordered VALUES (1,2,X'00FF')",
		"CREATE TABLE port_empty (id INT PRIMARY KEY)",
		"CREATE TABLE port_keyless (cell TEXT)",
		"INSERT INTO dolt_ignore(pattern,ignored) VALUES ('port_ignored',true)",
		"CREATE TABLE port_ignored (id BIGINT PRIMARY KEY,note JSON)",
		"CREATE VIEW port_view AS SELECT a FROM port_ordered",
	} {
		rootSchemaSQL(t, ctx, conn, statement)
	}
	first := rootSchemaCommit(t, ctx, conn)
	rootSchemaSQL(t, ctx, conn, "ALTER TABLE port_ordered DROP PRIMARY KEY")
	rootSchemaSQL(t, ctx, conn, "ALTER TABLE port_ordered ADD PRIMARY KEY(a,b)")
	rootSchemaSQL(t, ctx, conn, "ALTER TABLE port_ordered ADD COLUMN later INT")
	second := rootSchemaCommit(t, ctx, conn)
	rootSchemaSQL(t, ctx, conn, "ALTER TABLE port_ordered DROP PRIMARY KEY")
	before, err := ReadPhysicalWorkingRoot(ctx, conn, second)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{first, second, ""} {
		name := id
		if name == "" {
			name = "working"
		}
		t.Run(name, func(t *testing.T) {
			root, expected := originalStoredSchema(t, ctx, conn, id)
			var image PhysicalRootSchema
			var err error
			if id == "" {
				image, err = ReadPhysicalWorkingRootSchema(ctx, conn, second)
				if _, ok := expected["port_ignored"]; !ok {
					t.Fatal("working authority omitted ignored table")
				}
			} else {
				image, err = ReadPhysicalCommitRootSchema(ctx, conn, id)
				if image.CommitID != id {
					t.Fatal("selected commit identity differs")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			assertOriginalSchema(t, image, root, expected)
			for i := range image.Tables {
				for j := range image.Tables[i].Columns {
					if ordinal := image.Tables[i].Columns[j].PrimaryKeyOrdinal; ordinal != nil {
						*ordinal = 999
					}
				}
			}
			if id == "" {
				image, err = ReadPhysicalWorkingRootSchema(ctx, conn, second)
			} else {
				image, err = ReadPhysicalCommitRootSchema(ctx, conn, id)
			}
			if err != nil {
				t.Fatal(err)
			}
			assertOriginalSchema(t, image, root, expected)
		})
	}
	for _, table := range []string{"dolt_schemas", "dolt_ignore"} {
		rows, err := conn.QueryContext(ctx, "SHOW INDEX FROM `"+table+"`")
		if err != nil {
			t.Fatal(err)
		}
		if rows.Next() {
			t.Fatal("reserved SQL adapter no longer demonstrates hidden PRIMARY")
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	after, err := ReadPhysicalWorkingRoot(ctx, conn, second)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("metadata observation altered legacy working image or values")
	}
}

func TestPhysicalRootSchemaRefusals(t *testing.T) {
	ctx, conn := rootSchemaFixture(t)
	rootSchemaSQL(t, ctx, conn, "CREATE TABLE cells(id INT PRIMARY KEY)")
	head := rootSchemaCommit(t, ctx, conn)
	for _, tc := range []struct {
		name string
		read func() (PhysicalRootSchema, error)
	}{
		{"alias", func() (PhysicalRootSchema, error) { return ReadPhysicalCommitRootSchema(ctx, conn, "HEAD") }},
		{"missing_connection", func() (PhysicalRootSchema, error) { return ReadPhysicalWorkingRootSchema(ctx, nil, head) }},
		{"missing_commit", func() (PhysicalRootSchema, error) {
			return ReadPhysicalCommitRootSchema(ctx, conn, strings.Repeat("v", 32))
		}},
		{"wrong_head", func() (PhysicalRootSchema, error) {
			return ReadPhysicalWorkingRootSchema(ctx, conn, strings.Repeat("v", 32))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image, err := tc.read()
			var refusal *PhysicalReadError
			if !errors.As(err, &refusal) || !reflect.DeepEqual(image, PhysicalRootSchema{}) {
				t.Fatalf("unavailable authority did not typed-refuse without a partial image: %v", err)
			}
		})
	}
	t.Run("occupied_transaction", func(t *testing.T) {
		rootSchemaSQL(t, ctx, conn, "START TRANSACTION READ ONLY")
		defer rootSchemaSQL(t, ctx, conn, "ROLLBACK")
		image, err := ReadPhysicalWorkingRootSchema(ctx, conn, head)
		var refusal *PhysicalReadError
		if !errors.As(err, &refusal) || refusal.Kind != "occupied-transaction" || !reflect.DeepEqual(image, PhysicalRootSchema{}) {
			t.Fatalf("occupied connection was accepted: %v", err)
		}
		if err := conn.Raw(func(raw any) error {
			if raw.(*DoltConn).gmsCtx.GetTransaction() == nil {
				return errors.New("caller transaction was consumed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("closed_connection", func(t *testing.T) {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name string
			read func(context.Context, *sql.Conn, string) (PhysicalRootSchema, error)
		}{
			{"working", ReadPhysicalWorkingRootSchema},
			{"commit", ReadPhysicalCommitRootSchema},
		} {
			t.Run(tc.name, func(t *testing.T) {
				image, err := tc.read(ctx, conn, head)
				var refusal *PhysicalReadError
				if !errors.As(err, &refusal) || refusal.Kind != "connection" || !errors.Is(err, sql.ErrConnDone) || !reflect.DeepEqual(image, PhysicalRootSchema{}) {
					t.Fatalf("closed connection did not preserve typed refusal, cause and zero authority: %v", err)
				}
			})
		}
	})
}
