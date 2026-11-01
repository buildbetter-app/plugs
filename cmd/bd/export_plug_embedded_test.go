//go:build cgo

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
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
		return nil
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
	issueCount := 0
	for _, kind := range manifest.Kinds {
		if kind.Name == "issue" {
			issueCount = kind.Count
		}
	}
	if issueCount != 1 || manifest.History != "unavailable" {
		t.Fatalf("issue count = %d, history = %q", issueCount, manifest.History)
	}
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
