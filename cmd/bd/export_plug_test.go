package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

// This is a database/sql protocol fixture, not a database engine. All rows are
// synthetic; the real RunE, SQLSource reads, mapping, tar validation and atomic
// output path run together without opening a Beads store.
type plugSQLFixture struct {
	tables     map[string]plugdump.Table
	readOnly   bool
	rolledBack bool
}

func (f *plugSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *plugSQLFixture) Driver() driver.Driver                        { return plugFixtureDriver{} }

type plugFixtureDriver struct{}

func (plugFixtureDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("fixture requires its connector")
}
func (f *plugSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("fixture does not prepare writes")
}
func (f *plugSQLFixture) Close() error { return nil }
func (f *plugSQLFixture) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("fixture requires BeginTx read-only options")
}
func (f *plugSQLFixture) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	if !options.ReadOnly || options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) {
		return nil, fmt.Errorf("expected a read-only repeatable-read transaction")
	}
	f.readOnly = true
	return f, nil
}
func (f *plugSQLFixture) Commit() error   { return fmt.Errorf("read-only exporter must never commit") }
func (f *plugSQLFixture) Rollback() error { f.rolledBack = true; return nil }

func (f *plugSQLFixture) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.HasPrefix(query, "SELECT table_name FROM information_schema.tables ") {
		var names []string
		for name := range f.tables {
			names = append(names, name)
		}
		sort.Strings(names)
		var values [][]driver.Value
		for _, name := range names {
			values = append(values, []driver.Value{name})
		}
		return &plugSQLRows{columns: []string{"table_name"}, values: values}, nil
	}
	if !strings.HasPrefix(query, "SELECT * FROM `") || !strings.HasSuffix(query, "`") {
		return nil, fmt.Errorf("unexpected query in read-only fixture")
	}
	name := strings.TrimSuffix(strings.TrimPrefix(query, "SELECT * FROM `"), "`")
	table, ok := f.tables[name]
	if !ok {
		return nil, fmt.Errorf("unknown fixture table")
	}
	rows := &plugSQLRows{columns: table.Columns}
	for _, row := range table.Rows {
		var values []driver.Value
		for _, column := range table.Columns {
			value := row[column]
			switch value := value.(type) {
			case json.Number:
				number, err := value.Int64()
				if err != nil {
					return nil, err
				}
				values = append(values, number)
			case map[string]any:
				if encoded, ok := value["$bytes"].(string); ok {
					data, err := base64.StdEncoding.DecodeString(encoded)
					if err != nil {
						return nil, err
					}
					values = append(values, data)
				} else {
					data, err := json.Marshal(value)
					if err != nil {
						return nil, err
					}
					values = append(values, data)
				}
			case nil, string, bool:
				values = append(values, value)
			default:
				return nil, fmt.Errorf("unsupported fixture value")
			}
		}
		rows.values = append(rows.values, values)
	}
	return rows, nil
}

type plugSQLRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *plugSQLRows) Columns() []string { return r.columns }
func (r *plugSQLRows) Close() error      { return nil }
func (r *plugSQLRows) ColumnTypeDatabaseTypeName(index int) string {
	switch r.columns[index] {
	case "metadata":
		return "JSON"
	case "durable_state", "snapshot_json":
		return "LONGBLOB"
	default:
		return "TEXT"
	}
}
func (r *plugSQLRows) Next(dest []driver.Value) error {
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

type plugFixtureStore struct {
	storage.DoltStorage
	db *sql.DB
}

func (s plugFixtureStore) WithRawDBTx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx)
}

func TestPlugDumpCommandEndToEnd(t *testing.T) {
	isolateBeadsDirForTest(t)
	saveAndRestoreGlobals(t)
	oldFormat, oldInventory, oldOutput := exportFormat, exportPlugInventory, exportOutput
	oldReadonly, oldJSON, oldRootCtx := readonlyMode, jsonOutput, rootCtx
	oldOwners, oldVerbose := exportExcludeOwners, exportVerbose
	changed := map[string]bool{}
	for _, name := range []string{"format", "plug-inventory", "output"} {
		changed[name] = exportCmd.Flags().Lookup(name).Changed
	}
	cmd := &cobra.Command{Use: "export"}
	t.Cleanup(func() {
		exportFormat, exportPlugInventory, exportOutput = oldFormat, oldInventory, oldOutput
		readonlyMode, jsonOutput, rootCtx = oldReadonly, oldJSON, oldRootCtx
		exportExcludeOwners, exportVerbose = oldOwners, oldVerbose
		for _, name := range []string{"format", "plug-inventory", "output"} {
			exportCmd.Flags().Lookup(name).Changed = changed[name]
		}
	})
	sourcePath, err := bazeltest.RunfileEnv("BEADS_TEST_PLUG_SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &plugSQLFixture{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&fixture.tables); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() { _ = db.Close() })
	store = plugFixtureStore{db: db}
	rootCtx = context.Background()
	readonlyMode, jsonOutput = true, false
	exportAll, exportIncludeInfra, exportScrub, exportNoMemories, exportIncludeMemories, exportVerbose = false, false, false, false, false, false
	exportExcludeOwners = nil
	root := t.TempDir()
	inventoryPath, err := bazeltest.RunfileEnv("BEADS_TEST_PLUG_INVENTORY")
	if err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.ReplaceAll(data, []byte("$FIXTURE_ROOT"), []byte(filepath.ToSlash(root)))
	inventoryPath = filepath.Join(root, "inventory.json")
	if err := os.WriteFile(inventoryPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "dump.tar")
	if err := exportCmd.ParseFlags([]string{"--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", output}); err != nil {
		t.Fatal(err)
	}
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(first))
	if err != nil || manifest.Projects[0].LastID != 8 || !fixture.readOnly || !fixture.rolledBack {
		t.Fatalf("failed command archive/transaction contract: %v", err)
	}
	t.Logf("real export handler: %d archive bytes, sha256=%x, %d registered kinds, top-level counter=%d, child counter=%d; read-only transaction rolled back", len(first), sha256.Sum256(first), len(manifest.Kinds), manifest.Projects[0].LastID, manifest.Projects[0].ChildCounters["demo-7"])
	if artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); artifacts != "" {
		if err := os.WriteFile(filepath.Join(artifacts, "synthetic-plug-dump-1.tar"), first, 0o600); err != nil {
			t.Fatal(err)
		}
		manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(artifacts, "synthetic-manifest.json"), manifestJSON, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("dump file must be private")
	}
	for name, table := range fixture.tables {
		for i, j := 0, len(table.Rows)-1; i < j; i, j = i+1, j-1 {
			table.Rows[i], table.Rows[j] = table.Rows[j], table.Rows[i]
		}
		fixture.tables[name] = table
	}
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("equivalent command exports were not byte-identical")
	}
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	exportOutput = ""
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, stdout.Bytes()) {
		t.Fatal("stdout serialization differs from file serialization")
	}
	// A refusal leaves both an existing destination and stdout untouched.
	exportOutput = output
	if err := os.WriteFile(output, []byte("existing destination"), 0o600); err != nil {
		t.Fatal(err)
	}
	delete(fixture.tables, "leases")
	if err := exportCmd.RunE(cmd, nil); err == nil {
		t.Fatal("missing source family accepted")
	}
	unchanged, err := os.ReadFile(output)
	if err != nil || string(unchanged) != "existing destination" {
		t.Fatal("refusal replaced existing output")
	}
	exportOutput = ""
	stdout.Reset()
	if err := exportCmd.RunE(cmd, nil); err == nil || stdout.Len() != 0 {
		t.Fatal("refusal wrote partial stdout archive")
	}
	if exportCmd.Flags().Lookup("format").DefValue != "jsonl" {
		t.Fatal("legacy default changed")
	}
}
