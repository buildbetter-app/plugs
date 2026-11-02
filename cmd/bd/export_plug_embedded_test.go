//go:build cgo

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

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
	run("version", "--json")
	run("init", "--prefix", "native", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	input := filepath.Join(workspace, "public.jsonl")
	if err := os.WriteFile(input, []byte(`{"id":"native-abc","title":"Actual public native import","status":"open","priority":2,"issue_type":"task","created_at":"2026-11-02T00:00:00Z","updated_at":"2026-11-02T00:00:00Z"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run("import", input, "--json")
	capture := t.TempDir()
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
	if len(inventory.External) != 26 || len(inventory.ConfigPolicy) == 0 || len(inventory.Projects) != 1 || inventory.Projects[0].Prefix != "native" {
		t.Fatalf("incomplete actual native inventory: %s", raw)
	}
	for _, input := range inventory.External {
		if input.State != "json-config" {
			t.Fatalf("native source falsely omitted: %+v", input)
		}
		data, err := os.ReadFile(input.Path)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("NATIVE_PUBLIC_EXTERNAL family=%s sha256=%x data=%s", input.Family, sha256.Sum256(data), data)
	}
	archivePath := filepath.Join(capture, "readback.tar")
	run("--readonly", "export", "--format", plugdump.Format, "--plug-inventory", inventoryPath, "--output", archivePath)
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
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
	t.Logf("NATIVE_PUBLIC_INVENTORY=%s", raw)
	t.Logf("NATIVE_PUBLIC_ARCHIVE bytes=%d sha256=%x history=%s", len(archive), sha256.Sum256(archive), manifest.History)
	if os.Getenv("PLUG_TEST_EXPORT_PUBLIC_HISTORY") == "1" {
		t.Logf("NATIVE_PUBLIC_ARCHIVE_BASE64=%s", base64.StdEncoding.EncodeToString(archive))
	}
}
