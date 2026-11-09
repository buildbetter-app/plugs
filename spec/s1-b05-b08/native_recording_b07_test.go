package s1b05b08

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

//go:embed native_protocol_b07.json
var b07NativeProtocolBytes []byte

type b07NativeEdge struct{ Kind, From, To string }

type b07NativeProtocol struct {
	Schema, Card string
	Cases        []b07NativeEdge
	External     b07NativeEdge
}

type b07NativeCapture struct {
	Archive []byte
	Edges   []b07NativeEdge
	Ready   []string
	Rows    map[string][]json.RawMessage
}

func b07NativeOutput(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp(b07NativeEnv(t, "TEST_UNDECLARED_OUTPUTS_DIR"), "b07-native-")
	if err != nil {
		t.Fatal(err)
	}
	// Registered first, so the inventory includes server and consumer cleanup.
	t.Cleanup(func() {
		entries, err := b07NativeInventory(root)
		if err != nil {
			t.Error(err)
		}
		b07NativeJSON(t, filepath.Join(root, "output-inventory.json"), map[string]any{
			"entries": entries, "complete": err == nil, "testPassed": !t.Failed(),
			"exclusions": []string{"poll.log by basename before stat/read/hash", "output-inventory.json self"},
		})
	})
	b07NativeWrite(t, filepath.Join(root, "native-inputs.json"), b07NativeInputsBytes)
	b07NativeWrite(t, filepath.Join(root, "native-protocol.json"), b07NativeProtocolBytes)
	return root
}

func TestB07NativePublicVocabularyAndIntactSQLRoundtrip(t *testing.T) {
	root := b07NativeOutput(t)
	var protocol b07NativeProtocol
	b07NativeDecode(t, b07NativeProtocolBytes, &protocol)
	if protocol.Schema != "plug.s1.b07.native-protocol.v1" || protocol.Card != "B07" || len(protocol.Cases) != 20 {
		t.Fatal("B07 requires the complete 19-kind vocabulary and opaque custom control")
	}
	for _, twin := range []string{"run-a", "run-b"} {
		t.Run(twin, func(t *testing.T) {
			capture := b07NativeRecord(t, root, twin, protocol)
			for _, engine := range []string{"sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) { b07NativeImportAndExercise(t, engine, root, twin, capture) })
			}
		})
	}
}

func b07NativeRecord(t *testing.T, root, twin string, protocol b07NativeProtocol) b07NativeCapture {
	t.Helper()
	output := filepath.Join(root, twin)
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	defer b07NativeExportStates(t, output, "terminal")
	var inputs b07NativeInputs
	b07NativeDecode(t, b07NativeInputsBytes, &inputs)
	binary := b07NativePrepared(t, output, inputs)
	server := publicnative.StartDolt(t, output)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	env := server.ChildEnv([]string{
		"PATH=/usr/bin:/bin", "HOME=" + home, "USERPROFILE=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "USER=public-native-b07",
		"TZ=UTC", "LC_ALL=C", "CI=true", "NO_COLOR=1", "GIT_CONFIG_NOSYSTEM=1", "BEADS_ACTOR=b07-native",
	})
	preparation := map[string]string{}
	for _, name := range []string{"PLUG_PUBLIC_BEADS_BINARY", "PLUG_PUBLIC_BEADS_SHA256", "PLUG_PUBLIC_BEADS_BUILD_RUN", "PLUG_PUBLIC_BEADS_MANIFEST"} {
		preparation[name] = b07NativeEnv(t, name)
		env = append(env, name+"="+preparation[name])
	}
	b07NativeJSON(t, filepath.Join(output, "four-exports-childenv.json"), map[string]any{
		"preparation": preparation, "count": len(preparation), "childEnv": env,
		"routing": "publicnative.ChildEnv routing unchanged; exactly four receipt values appended unchanged",
	})
	c := &b07NativeCommand{t: t, binary: binary, workspace: workspace, output: output, env: env}
	var version struct{ Commit, Build string }
	b07NativeDecode(t, c.run("linked-version", "version", "--json"), &version)
	if version.Commit != inputs.Source || version.Build != "public-"+inputs.Source {
		t.Fatal("native linked version differs from the accepted producer")
	}
	database := "b07_" + strings.ReplaceAll(twin, "-", "_")
	c.run("init-owned-server", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b07", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	for _, edge := range protocol.Cases {
		for _, id := range []string{edge.From, edge.To} {
			c.run("create-"+id, "create", "--id", id, "--type", "task", "--title", id, "--json")
		}
	}
	c.run("create-external-source", "create", "--id", protocol.External.From, "--type", "task", "--title", "external source", "--json")
	query := b07NativeSQLCensusQuery(t, c)
	var edges []b07NativeEdge
	for i, edge := range protocol.Cases {
		if edge.Kind == "custom kind" {
			// This public command intentionally accepts only well-known kinds.
			// Its genuine refusal is useful coverage, but cannot stand in for
			// a producer-created stored opaque edge or its intact export.
			before := b07NativeSQLCensus(t, c, query, "custom-before")
			c.runExit("custom-public-refusal", 1, "dep", "add", edge.From, edge.To, "--type", edge.Kind, "--json")
			after := b07NativeSQLCensus(t, c, query, "custom-after")
			if !reflect.DeepEqual(before, after) {
				t.Fatal("native custom CLI refusal changed complete public SQL state")
			}
			continue
		}
		if !t.Run(edge.Kind, func(t *testing.T) {
			previous := c.t
			c.t = t
			defer func() { c.t = previous }()
			label := fmt.Sprintf("kind-%02d", i)
			c.run(label+"-add", "dep", "add", edge.From, edge.To, "--type", edge.Kind, "--json")
			before := b07NativeSQLCensus(t, c, query, label+"-before-duplicate")
			c.run(label+"-duplicate", "dep", "add", edge.From, edge.To, "--type", edge.Kind, "--json")
			after := b07NativeSQLCensus(t, c, query, label+"-after-duplicate")
			if !reflect.DeepEqual(before, after) {
				t.Fatal("same-type re-add changed native SQL rows")
			}
			retype := "blocks"
			if edge.Kind == retype {
				retype = "related"
			}
			c.runExit(label+"-retype", 1, "dep", "add", edge.From, edge.To, "--type", retype, "--json")
			if afterRefusal := b07NativeSQLCensus(t, c, query, label+"-after-retype"); !reflect.DeepEqual(after, afterRefusal) {
				t.Fatal("retype refusal changed native SQL rows")
			}
		}) {
			t.Fatal("native kind failed; intact corpus retained without replacing or filtering it")
		}
		edges = append(edges, edge)
	}
	c.run("external-add", "dep", "add", protocol.External.From, protocol.External.To, "--type", protocol.External.Kind, "--json")
	edges = append(edges, protocol.External)
	var readyRows []struct{ ID string }
	b07NativeDecode(t, c.run("ready-intact", "ready", "--limit", "1000", "--json"), &readyRows)
	ready := make([]string, 0, len(readyRows))
	for _, row := range readyRows {
		ready = append(ready, row.ID)
	}
	sort.Strings(ready)
	rows := b07NativeSQLCensus(t, c, query, "complete-pre-export")
	var opaqueRows []struct {
		IssueID, Type                                     string
		DependsOnID, DependsOnVolatile, DependsOnExternal *string
	}
	b07NativeDecode(t, c.run("stored-opaque-control", "sql", "SELECT issue_id AS IssueID, depends_on_issue_id AS DependsOnID, depends_on_wisp_id AS DependsOnVolatile, depends_on_external AS DependsOnExternal, type AS Type FROM dependencies WHERE type='custom kind'", "--json"), &opaqueRows)
	opaque := protocol.Cases[len(protocol.Cases)-1]
	opaqueStored := len(opaqueRows) == 1 && opaqueRows[0].IssueID == opaque.From && opaqueRows[0].DependsOnID != nil && *opaqueRows[0].DependsOnID == opaque.To && opaqueRows[0].DependsOnVolatile == nil && opaqueRows[0].DependsOnExternal == nil && opaqueRows[0].Type == opaque.Kind
	b07NativeJSON(t, filepath.Join(output, "source-coverage.json"), map[string]any{
		"edges": edges, "ready": ready, "sqlTables": len(rows), "opaqueStoredControl": opaqueStored,
		"scope": "19 genuine well-known kinds; custom CLI refusal is not stored-opaque-kind coverage",
	})
	c.run("quiesce-native-working-set", "dolt", "commit", "-m", "B07 "+twin+" intact public vocabulary")
	physical := server.RequireDatabaseDir(t, database)
	b07NativeJSON(t, filepath.Join(output, "physical-database.json"), map[string]any{
		"path": physical, "pid": server.PID, "port": server.Port, "dataRoot": server.DataRoot,
		"childEnv": env, "checkedUTC": time.Now().UTC(),
	})
	inventory := filepath.Join(output, "inventory.json")
	c.run("public-intact-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventory)
	var census struct {
		Version  string
		Quiesced bool
		Clean    bool `json:"working_set_clean"`
	}
	data, err := os.ReadFile(inventory)
	if err != nil {
		t.Fatal(err)
	}
	b07NativeDecode(t, data, &census)
	if census.Version != "beads-inventory/1" || !census.Quiesced || !census.Clean {
		t.Fatal("native inventory is not clean and quiesced")
	}
	before, err := b07NativeInventory(inventory + ".sources")
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(output, "readback.tar")
	c.run("public-intact-dump", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventory, "-o", archivePath)
	after, err := b07NativeInventory(inventory + ".sources")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("native dump changed original sidecar bytes: %v", err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil || len(archive) == 0 {
		t.Fatalf("native archive is empty or absent: %v", err)
	}
	b07NativeJSON(t, filepath.Join(output, "intact-sidecar-closure.json"), after)
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	// Accepted c8eb has no genuine public stored-opaque source control. Keep
	// this independent gap literal even if a future export path accepts the
	// other 19 kinds. No consumer replay may silently omit the opaque case.
	if !opaqueStored {
		t.Fatal("B07 native opaque stored control NOT_PROVED: genuine producer-owned public source is required; no generated import or SQL mutation substituted")
	}
	edges = append(edges, opaque)
	return b07NativeCapture{Archive: archive, Edges: edges, Ready: ready, Rows: rows}
}

// Read every table and every column. The single UNION query avoids a separate
// process per table at every guard. It performs no SQL writes, omits no source
// family, preserves NULL/empty distinctions, and sorts only observation rows.
func b07NativeSQLCensusQuery(t *testing.T, c *b07NativeCommand) string {
	t.Helper()
	var tables []map[string]string
	b07NativeDecode(t, c.run("public-sql-tables", "sql", "SHOW TABLES", "--json"), &tables)
	quote := func(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }
	var names []string
	for _, row := range tables {
		if len(row) != 1 {
			t.Fatal("SHOW TABLES did not return one table name per row")
		}
		for _, name := range row {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	// Keep current schema in every observation, including empty tables. A new
	// or changed table is a conservation failure even when it has no rows.
	selects := []string{"SELECT '@schema' AS table_name, CAST(JSON_ARRAY(TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,COLUMN_TYPE,COLUMN_KEY,EXTRA) AS CHAR) AS row_json FROM information_schema.columns WHERE table_schema=DATABASE()"}
	for _, name := range names {
		var columns []struct{ Field string }
		b07NativeDecode(t, c.run("public-sql-columns-"+name, "sql", "SHOW COLUMNS FROM "+quote(name), "--json"), &columns)
		if len(columns) == 0 || strings.ContainsAny(name, "'\x00") {
			t.Fatal("native table has no columns or an unsafe literal name")
		}
		var fields []string
		for _, column := range columns {
			fields = append(fields, quote(column.Field))
		}
		selects = append(selects, "SELECT '"+name+"' AS table_name, 'null' AS row_json", "SELECT '"+name+"' AS table_name, CAST(JSON_ARRAY("+strings.Join(fields, ",")+") AS CHAR) AS row_json FROM "+quote(name))
	}
	if len(names) == 0 {
		t.Fatal("owned native database has no public SQL tables")
	}
	b07NativeJSON(t, filepath.Join(c.output, "source-sql-tables.json"), names)
	return strings.Join(selects, " UNION ALL ")
}

func b07NativeSQLCensus(t *testing.T, c *b07NativeCommand, query, label string) map[string][]json.RawMessage {
	t.Helper()
	var rows []struct {
		Table string `json:"table_name"`
		Row   string `json:"row_json"`
	}
	b07NativeDecode(t, c.run(label, "sql", query, "--json"), &rows)
	result := map[string][]json.RawMessage{}
	for _, row := range rows {
		if !json.Valid([]byte(row.Row)) {
			t.Fatal("native SQL row is not a complete JSON value")
		}
		result[row.Table] = append(result[row.Table], json.RawMessage(row.Row))
	}
	for name := range result {
		sort.Slice(result[name], func(i, j int) bool { return bytes.Compare(result[name][i], result[name][j]) < 0 })
	}
	return result
}

// Keep operation contexts independent of CLI command deadlines and the native
// server lifetime. A SQL import never borrows a request's canceled context.
func b07NativeContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}
