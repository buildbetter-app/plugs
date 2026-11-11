//go:build cgo

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
	"github.com/steveyegge/beads/internal/types"
)

// The fixture is a disposable, migrated Beads store with one synthetic issue.
// The real embedded driver, SQL census, export handler and archive validator run
// together; no Dolt server or private ledger is used.
func TestEmbeddedPlugDumpCommandEndToEnd(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt export tests")
	}
	cmd := plugExportTestCommand(t)
	ctx := context.Background()
	beadsDir := filepath.Join(t.TempDir(), ".beads")
	s, err := embeddeddolt.Open(ctx, beadsDir, "demo", "main")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.SetConfig(ctx, "issue_prefix", "demo"); err != nil {
		t.Fatal(err)
	}
	issue := &types.Issue{ID: "demo-abc", Title: "synthetic embedded export", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
	if err := s.CreateIssue(ctx, issue, "synthetic-author"); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "synthetic offline capture"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = embeddeddolt.OpenReadOnly(ctx, beadsDir, "demo", "main")
	if err != nil {
		t.Fatal(err)
	}

	path, err := bazeltest.RunfileEnv("BEADS_TEST_PLUG_INVENTORY")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	data = bytes.ReplaceAll(data, []byte("$FIXTURE_ROOT"), []byte(filepath.ToSlash(root)))
	inventory, err := plugdump.ReadInventory(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	inventory.History = "unavailable"
	inventory.Projects = []plugdump.Project{{Prefix: "demo", IDMode: "hash", HashLength: 3, ChildCounters: map[string]uint64{}}}
	inventory.ConfigPolicy = map[string]string{}
	opts := &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}
	var sourceLog plugdump.SourceCommitLog
	var sourceEvents plugdump.Table
	if err := s.WithRawDBTx(ctx, opts, func(tx *sql.Tx) error {
		for _, table := range []string{"config", "metadata"} {
			rows, err := tx.QueryContext(ctx, "SELECT `key` FROM `"+table+"`")
			if err != nil {
				return err
			}
			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err != nil {
					_ = rows.Close()
					return err
				}
				// All values in this newly initialized synthetic store are public.
				inventory.ConfigPolicy[table+"/"+key] = "public"
			}
			readErr := rows.Err()
			_ = rows.Close()
			if readErr != nil {
				return readErr
			}
		}
		migrations, err := (plugdump.SQLSource{DB: tx}).ReadTable(ctx, "schema_migrations")
		if err != nil {
			return err
		}
		if len(migrations.Rows) != 0 {
			t.Logf("synthetic main cursor: %d rows; version type %T, content hash type %T", len(migrations.Rows), migrations.Rows[0]["version"], migrations.Rows[0]["content_hash"])
		}
		sourceLog, err = (plugdump.SQLSource{DB: tx}).ReadSourceCommits(ctx)
		if err != nil {
			return err
		}
		sourceEvents, err = (plugdump.SQLSource{DB: tx}).ReadTable(ctx, "events")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	exportPlugInventory = filepath.Join(root, "inventory.json")
	if err := os.WriteFile(exportPlugInventory, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, rootCtx, readonlyMode, jsonOutput = s, ctx, true, false
	exportFormat, exportOutput = plugdump.Format, filepath.Join(root, "dump.tar")
	exportAll, exportIncludeInfra, exportScrub, exportNoMemories, exportIncludeMemories, exportVerbose = false, false, false, false, false, false
	exportExcludeOwners = nil
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(exportOutput)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(first))
	if err != nil {
		t.Fatal(err)
	}
	beadCount := 0
	for _, kind := range manifest.Kinds {
		if kind.Name == "bead" {
			beadCount = kind.Count
		}
	}
	if beadCount != 1 || manifest.History == "unavailable" || !sourceLog.Clean {
		t.Fatalf("bead count = %d, history = %q", beadCount, manifest.History)
	}
	history := plugExportHistory(t, first)
	if len(history.Commits) != len(sourceLog.Commits) || len(history.Commits) == 0 {
		t.Fatal("command source commit census changed")
	}
	wantCommits := map[string][]byte{}
	for _, commit := range sourceLog.Commits {
		wantCommits[commit.ID], err = json.Marshal(commit)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, commit := range history.Commits {
		got, err := json.Marshal(commit)
		if err != nil || !bytes.Equal(got, wantCommits[commit.ID]) {
			t.Fatalf("command changed actual source commit %s: %v", commit.ID, err)
		}
	}
	wantEvents, err := json.Marshal(sourceEvents.Rows)
	if err != nil {
		t.Fatal(err)
	}
	gotEvents, err := json.Marshal(history.Events)
	if err != nil || !bytes.Equal(wantEvents, gotEvents) {
		t.Fatalf("command changed actual source events: %v", err)
	}
	if history.Range.Status == "verified" {
		if history.Range.Head != uint64(len(sourceLog.Commits)) || len(history.Range.Frames) == 0 || history.Range.Frames[len(history.Range.Frames)-1].CommitID != sourceLog.Head {
			t.Fatal("command claimed an unproven source head")
		}
	} else if history.Range.Status != "unverified" || history.Range.Reason == "" || len(history.Range.Frames) != 0 || manifest.History != "available-unverified" {
		t.Fatal("command invented a range from unavailable historical images")
	}
	t.Logf("actual embedded history commits=%d events=%d range=%s reason=%s", len(history.Commits), len(history.Events), history.Range.Status, history.Range.Reason)
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(exportOutput)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("embedded exports were not byte-identical")
	}
	if err := s.WithRawDBTx(ctx, opts, func(tx *sql.Tx) error {
		var dirty int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_status").Scan(&dirty); err != nil {
			return err
		}
		if dirty != 0 {
			t.Errorf("export dirtied %d source tables", dirty)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("embedded driver exported one synthetic issue in %d archive bytes without a server; repeated export identical and source clean", len(first))
}

// A standalone supported bd process must census its actual native init/import
// target. Handler fixtures cannot prove environment/path resolution or deliver
// the production executable that the reverse consumer needs.
func TestEmbeddedPlugNativeInventoryActualCLI(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("requires the explicit native public exporter lane")
	}
	bd, err := findPrebuiltBDBinary()
	if err != nil || bd == "" {
		t.Fatalf("canonical bd runfile required: %v", err)
	}
	binary, err := os.ReadFile(bd)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NATIVE_PUBLIC_BD_SHA256=%x", sha256.Sum256(binary))
	workspace, home := t.TempDir(), t.TempDir()
	workspace, err = filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{workspace, home} {
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	initGitRepoAt(t, workspace)
	env := bdEnv(home)
	for _, key := range []string{"XDG_CONFIG_HOME", "BEADS_DIR", "BD_DOLT_MODE", "BD_EVENTS_JOURNAL", "DOLT_ROOT_PATH"} {
		env = envWithout(env, key)
	}
	env = append(env, "XDG_CONFIG_HOME="+filepath.Join(home, ".config"), "BEADS_DIR="+filepath.Join(workspace, ".beads"), "BD_DOLT_MODE=embedded", "BD_EVENTS_JOURNAL=false", "DOLT_ROOT_PATH="+home)
	run := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command(bd, args...)
		cmd.Dir, cmd.Env = workspace, env
		out, err := cmd.CombinedOutput()
		t.Logf("NATIVE_PUBLIC_COMMAND args=%q exit_error=%v output=%s", args, err, out)
		if err != nil {
			t.Fatalf("actual bd %q: %v: %s", args, err, out)
		}
		return out
	}
	refuse := func(want string, args ...string) {
		t.Helper()
		cmd := exec.Command(bd, args...)
		cmd.Dir, cmd.Env = workspace, env
		out, err := cmd.CombinedOutput()
		t.Logf("NATIVE_PUBLIC_REFUSAL args=%q exit_error=%v output=%s", args, err, out)
		if err == nil || !bytes.Contains(out, []byte(want)) {
			t.Fatalf("actual bd refusal %q: %v: %s", args, err, out)
		}
	}
	run("version", "--json")
	run("init", "--prefix", "native", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	input := filepath.Join(workspace, "public.jsonl")
	if err := os.WriteFile(input, []byte(`{"id":"native-abc","title":"Actual public native import","status":"open","priority":2,"issue_type":"task","created_at":"2026-11-02T00:00:00Z","updated_at":"2026-11-02T00:00:00Z"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run("import", input, "--json")
	capture := t.TempDir()
	if err := os.Chmod(capture, 0700); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"public-declaration": {"--readonly", "export", "--format", "beads-inventory/1"},
		"readonly":           {"export", "--format", "beads-inventory/1", "--plug-capture-public"},
		"selection":          {"--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--all"},
		"wrong-format":       {"--readonly", "export", "--plug-capture-public"},
	} {
		path := filepath.Join(capture, name+".json")
		want := "requires --readonly and explicit --plug-capture-public"
		if name == "selection" {
			want = "requires only --output and public capture flags"
		} else if name == "wrong-format" {
			want = "requires --format beads-inventory/1"
		}
		refuse(want, append(args, "--output", path)...)
		for _, output := range []string{path, path + ".sources"} {
			if _, err := os.Lstat(output); !os.IsNotExist(err) {
				t.Fatalf("refused capture published %s: %v", output, err)
			}
		}
	}
	if err := os.Chmod(capture, 0755); err != nil {
		t.Fatal(err)
	}
	refuse("output parent must be an existing private directory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", filepath.Join(capture, "nonprivate.json"))
	if err := os.Chmod(capture, 0700); err != nil {
		t.Fatal(err)
	}
	inventoryPath := filepath.Join(capture, "inventory.json")
	run("--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryPath)
	raw, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := plugdump.ReadInventory(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if inventory.SourcePlaneAddressContract != plugdump.SourcePlaneAddressContract {
		t.Fatalf("native CLI omitted its explicit source-plane contract: %s", raw)
	}
	if len(inventory.External) != 26 || len(inventory.ConfigPolicy) == 0 || len(inventory.Projects) != 1 || inventory.Projects[0].Prefix != "native" {
		t.Fatalf("incomplete actual native inventory: %s", raw)
	}
	for path, mode := range map[string]os.FileMode{inventoryPath: 0600, inventoryPath + ".sources": 0700} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("native output permissions %s: %v %v", path, info, err)
		}
	}
	physicalCapture, err := filepath.EvalSymlinks(capture)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range inventory.External {
		if input.State != "json-config" || input.Path != filepath.Join(physicalCapture, "inventory.json.sources", input.Family+".json") {
			t.Fatalf("native source falsely omitted: %+v", input)
		}
		info, err := os.Stat(input.Path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("native sidecar permissions: %v %v", info, err)
		}
		data, err := os.ReadFile(input.Path)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("NATIVE_PUBLIC_EXTERNAL family=%s sha256=%x data=%s", input.Family, sha256.Sum256(data), data)
	}
	refuse("refuses existing output or sidecars", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryPath)
	if after, err := os.ReadFile(inventoryPath); err != nil || !bytes.Equal(raw, after) {
		t.Fatal("refused overwrite changed the inventory")
	}
	archivePath := filepath.Join(capture, "readback.tar")
	refuse("native-physical-root-capability-unavailable:stock-sql", "--readonly", "export", "--format", plugdump.Format, "--plug-inventory", inventoryPath, "--output", archivePath)
	if _, err := os.Lstat(archivePath); !os.IsNotExist(err) {
		t.Fatalf("stock SQL refusal published an archive: %v", err)
	}

	// Stock bd deliberately refuses immutable SQL images. Retain the positive
	// archive/readback assertions through the supported standalone physical
	// executable against the same quiesced, public init/import workspace.
	physicalBinary, err := bazeltest.RunfileEnv("BEADS_PHYSICAL_CAPTURE_BINARY")
	if err != nil {
		t.Fatal(err)
	}
	physicalELF, err := os.ReadFile(physicalBinary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NATIVE_PHYSICAL_CAPTURE_SHA256=%x", sha256.Sum256(physicalELF))
	beadsDir := filepath.Join(workspace, ".beads")
	cfg, err := configfile.Load(beadsDir)
	if err != nil || cfg == nil || cfg.DoltDatabase == "" || cfg.GetDoltMode() != configfile.DoltModeEmbedded {
		t.Fatalf("actual init did not declare its embedded database: %+v %v", cfg, err)
	}
	dataDir := filepath.Join(beadsDir, "embeddeddolt")
	for _, path := range []string{beadsDir, dataDir} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	capturePhysical := func() ([]byte, plugdump.Inventory) {
		t.Helper()
		output, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(output, 0o700); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(physicalBinary, "--disposable", "--data-dir", dataDir, "--database", cfg.DoltDatabase, "--beads-dir", beadsDir, "--output-dir", output)
		cmd.Dir, cmd.Env = workspace, env
		out, err := cmd.CombinedOutput()
		t.Logf("NATIVE_PHYSICAL_CAPTURE exit_error=%v output=%s", err, out)
		if err != nil {
			t.Fatalf("actual physical capture: %v: %s", err, out)
		}
		data, err := os.ReadFile(filepath.Join(output, "capture.tar"))
		if err != nil {
			t.Fatal(err)
		}
		inventoryData, err := os.ReadFile(filepath.Join(output, "inventory.json"))
		if err != nil {
			t.Fatal(err)
		}
		measured, err := plugdump.ReadInventory(bytes.NewReader(inventoryData))
		if err != nil || measured.SourcePlaneAddressContract != plugdump.SourcePlaneAddressContract {
			t.Fatalf("physical inventory marker: %+v %v", measured, err)
		}
		return data, measured
	}
	archive, physicalInventory := capturePhysical()
	if !bytes.Equal(mustNativeJSON(t, physicalInventory.ConfigPolicy), mustNativeJSON(t, inventory.ConfigPolicy)) {
		t.Fatal("physical capture changed the measured native config-policy census")
	}
	manifest, err := plugdump.Validate(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, kind := range manifest.Kinds {
		if kind.Name == "bead" && kind.Count == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("actual imported issue absent from native raw archive")
	}

	// Compare the delivered raw archive and exact policy set to fresh stored SQL
	// readbacks, independently of the producer's inventory construction.
	nativeStore, err := embeddeddolt.OpenReadOnly(context.Background(), beadsDir, cfg.DoltDatabase, "main")
	if err != nil {
		t.Fatal(err)
	}
	defer nativeStore.Close()
	records := map[string][]plugdump.Record{}
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(header.Name, "records/") {
			continue
		}
		decoder := json.NewDecoder(reader)
		decoder.UseNumber()
		for {
			var record plugdump.Record
			err := decoder.Decode(&record)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			records[record.Source.Table] = append(records[record.Source.Table], record)
		}
	}
	if err := nativeStore.WithRawDBTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		source := plugdump.SQLSource{DB: tx}
		policies := 0
		for _, name := range []string{"issues", "config", "metadata"} {
			table, err := source.ReadTable(context.Background(), name)
			if err != nil {
				return err
			}
			t.Logf("NATIVE_PUBLIC_STORED_%s=%s", name, mustNativeJSON(t, table))
			if len(table.Rows) != len(records[name]) {
				return fmt.Errorf("raw %s row census changed", name)
			}
			for _, row := range table.Rows {
				key := "id"
				if name != "issues" {
					key = "key"
					policies++
					if inventory.ConfigPolicy[name+"/"+fmt.Sprint(row[key])] == "" {
						return fmt.Errorf("native policy omitted stored %s key", name)
					}
				}
				found := false
				for _, record := range records[name] {
					if record.Source.Fields[key] != row[key] {
						continue
					}
					found = true
					if name == "issues" {
						delete(row, "row_lock")
						delete(row, "is_blocked")
					}
					if !bytes.Equal(mustNativeJSON(t, row), mustNativeJSON(t, record.Source.Fields)) {
						return fmt.Errorf("raw %s source values changed", name)
					}
				}
				if !found {
					return fmt.Errorf("raw %s source row missing", name)
				}
			}
		}
		if policies != len(inventory.ConfigPolicy) {
			return fmt.Errorf("native policy contains unmeasured keys")
		}
		log, err := source.ReadSourceCommits(context.Background())
		if err != nil {
			return err
		}
		if !log.Clean {
			return fmt.Errorf("native capture dirtied its source")
		}
		t.Logf("NATIVE_PUBLIC_SOURCE_COMMITS=%s", mustNativeJSON(t, log))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("NATIVE_PUBLIC_INVENTORY=%s", raw)
	t.Logf("NATIVE_PUBLIC_ARCHIVE bytes=%d sha256=%x history=%s", len(archive), sha256.Sum256(archive), manifest.History)
	if os.Getenv("PLUG_TEST_EXPORT_PUBLIC_HISTORY") == "1" {
		t.Logf("NATIVE_PUBLIC_ARCHIVE_BASE64=%s", base64.StdEncoding.EncodeToString(archive))
	}
	// A later imported legacy ID may be shorter than the native generator's
	// configured floor. Census the stored IDs without changing that generator.
	shortInput := filepath.Join(workspace, "public-short.jsonl")
	if err := os.WriteFile(shortInput, []byte(`{"id":"native-a","title":"Imported short source ID","status":"open","priority":2,"issue_type":"task","created_at":"2026-11-02T00:00:00Z","updated_at":"2026-11-02T00:00:00Z"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Release the read-only handle before invoking a writer process.
	if err := nativeStore.Close(); err != nil {
		t.Fatal(err)
	}
	run("import", shortInput, "--json")
	shortInventoryPath := filepath.Join(capture, "short-inventory.json")
	run("--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", shortInventoryPath)
	shortRaw, err := os.ReadFile(shortInventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	shortInventory, err := plugdump.ReadInventory(bytes.NewReader(shortRaw))
	if err != nil || len(shortInventory.Projects) != 1 || shortInventory.Projects[0].HashLength != 1 {
		t.Fatalf("actual imported short ID floor not measured: %s %v", shortRaw, err)
	}
	shortArchivePath := filepath.Join(capture, "short-readback.tar")
	refuse("native-physical-root-capability-unavailable:stock-sql", "--readonly", "export", "--format", plugdump.Format, "--plug-inventory", shortInventoryPath, "--output", shortArchivePath)
	if _, err := os.Lstat(shortArchivePath); !os.IsNotExist(err) {
		t.Fatalf("short-ID stock SQL refusal published an archive: %v", err)
	}
	shortArchive, measuredShort := capturePhysical()
	if len(measuredShort.Projects) != 1 || measuredShort.Projects[0].HashLength != 1 {
		t.Fatal("physical capture changed imported short-ID floor")
	}
	shortManifest, err := plugdump.Validate(bytes.NewReader(shortArchive))
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, kind := range shortManifest.Kinds {
		if kind.Name == "bead" && kind.Count == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("physical archive lost imported short-ID issue")
	}

}

func mustNativeJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
