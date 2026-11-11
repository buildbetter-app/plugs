package physicalcapture

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	nativeschema "github.com/buildbetter-app/plugs/internal/testutil/nativeschema"
	embedded "github.com/dolthub/driver/v2"
)

func TestRootSchemaAdapterDetachesAuthority(t *testing.T) {
	first, second := uint64(0), uint64(1)
	source := []embedded.PhysicalSchemaTable{{Name: "ordered", Columns: []embedded.PhysicalSchemaColumn{
		{Name: "a", DatabaseTypeName: "bigint", PrimaryKeyOrdinal: &first},
		{Name: "b", DatabaseTypeName: "bigint", PrimaryKeyOrdinal: &second},
		{Name: "payload", DatabaseTypeName: "text"},
	}}}
	got := schemaTables(source)
	first = 9
	source[0].Name = "changed"
	source[0].Columns[0].Name = "changed"
	if got[0].Name != "ordered" || got[0].Columns[0].Name != "a" || *got[0].Columns[0].PrimaryKeyOrdinal != 0 || got[0].Columns[2].PrimaryKeyOrdinal != nil {
		t.Fatal("descriptor aliases input slices or stored PK pointer")
	}
	*got[0].Columns[1].PrimaryKeyOrdinal = 8
	if second != 1 {
		t.Fatal("consumer can mutate original PK authority through returned pointer")
	}
}

func TestRootSchemaPublicNativeCensusAndLegacyReadback(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	params := url.Values{"commitname": {"Public fixture"}, "commitemail": {"fixture@example.test"}, "disable_singleton_cache": {"true"}, "multistatements": {"true"}}
	cfg, err := embedded.ParseDSN("file://" + dir + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	connector, err := embedded.NewConnector(cfg)
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
	exec := func(statement string) {
		t.Helper()
		if err := nativeschema.DrainCall(ctx, conn, statement); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE DATABASE public_schema_adapter")
	exec("USE public_schema_adapter")
	version, err := nativeschema.MigrateUp(ctx, conn)
	if err != nil || version != nativeschema.LatestVersion() {
		t.Fatalf("actual public native migrations incomplete: version=%d error=%v", version, err)
	}
	exec("CREATE TABLE public_keyorder(a BIGINT,b BIGINT,payload TEXT,PRIMARY KEY(b,a))")
	exec("CREATE TABLE public_keyless(payload TEXT)")
	commit := func() string {
		t.Helper()
		exec("CALL DOLT_ADD('-A')")
		exec("CALL DOLT_COMMIT('--allow-empty','-m','public schema adapter','--author','Fixture <fixture@example.test>')")
		id, err := nativeschema.CurrentCommit(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := commit()
	exec("ALTER TABLE public_keyorder DROP PRIMARY KEY")
	exec("ALTER TABLE public_keyorder ADD PRIMARY KEY(a,b)")
	exec("ALTER TABLE public_keyorder MODIFY COLUMN payload LONGTEXT")
	second := commit()
	exec("ALTER TABLE public_keyorder DROP PRIMARY KEY")
	before, err := ReadWorkingRoot(ctx, conn, second)
	if err != nil {
		t.Fatal(err)
	}
	working, err := ReadWorkingRootSchema(ctx, conn, second)
	if err != nil || !working.ReadOnlyScope || !working.Complete || working.Database != before.Database || working.HeadCommitID != before.HeadCommitID || working.WorkingRootHash != before.WorkingRootHash {
		t.Fatalf("working metadata is not bound to original typed image: %v", err)
	}
	assertSchemaLegacyColumns(t, working.Tables, before.Tables)
	reserved := make(map[string]bool)
	for _, table := range working.Tables {
		for _, column := range table.Columns {
			if column.PrimaryKeyOrdinal != nil {
				reserved[table.Name] = true
			}
		}
	}
	if !reserved["dolt_ignore"] || !reserved["dolt_schemas"] {
		t.Fatal("public native reserved stored PRIMARY authority is missing")
	}
	for _, id := range []string{first, second} {
		original, err := ReadCommit(ctx, conn, id)
		if err != nil {
			t.Fatal(err)
		}
		observed, err := ReadCommitRootSchema(ctx, conn, id)
		if err != nil || !observed.Complete || !observed.ReadOnlyScope || observed.CommitID != original.CommitID || observed.Database != before.Database || len(observed.RootHash) != 32 {
			t.Fatalf("selected original commit schema unavailable: %v", err)
		}
		assertSchemaLegacyColumns(t, observed.Tables, original.Tables)
		for _, table := range observed.Tables {
			if table.Name == "public_keyorder" {
				want := uint64(1)
				if id == second {
					want = 0
				}
				if table.Columns[0].PrimaryKeyOrdinal == nil || *table.Columns[0].PrimaryKeyOrdinal != want {
					t.Fatal("selected immutable PK order was replaced with current metadata")
				}
			}
		}
	}
	after, err := ReadWorkingRoot(ctx, conn, second)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("metadata-only export changed original typed image/readback")
	}
	zero, err := ReadWorkingRootSchema(ctx, conn, first)
	var refusal *embedded.PhysicalReadError
	if !errors.As(err, &refusal) || !reflect.DeepEqual(zero, WorkingSchemaImage{}) {
		t.Fatal("mismatched original HEAD did not return typed refusal and zero authority")
	}
	t.Logf("public_native_current_tables=%d original_head=%s original_working_root=%s legacy_readback=exact", len(working.Tables), second, before.WorkingRootHash)
}

func assertSchemaLegacyColumns(t *testing.T, schemas []SchemaTable, original []Table) {
	t.Helper()
	if len(schemas) != len(original) {
		t.Fatal("complete stored table census differs from original typed image")
	}
	for i, table := range original {
		observed := schemas[i]
		if observed.Name != table.Name || len(observed.Columns) != len(table.Columns) {
			t.Fatalf("ordered stored table/column census differs for %s", table.Name)
		}
		for j, column := range table.Columns {
			if observed.Columns[j].Name != column.Name || observed.Columns[j].DatabaseTypeName != column.DatabaseTypeName {
				t.Fatalf("original SQL descriptor differs for %s column%d", table.Name, j)
			}
		}
	}
}
