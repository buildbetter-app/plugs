package main_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

func ownedDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// Read this fixture's fixed custom SQL schema directly, without relaxing the
// production Beads allowlist or skipping its current cells/multiplicity.
func publicArchivedRows(t *testing.T, ctx context.Context, conn *sql.Conn) [][]any {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT id,later_column,opaque FROM archived_rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result [][]any
	for rows.Next() {
		var id int64
		var later sql.NullString
		var opaque []byte
		if err := rows.Scan(&id, &later, &opaque); err != nil {
			t.Fatal(err)
		}
		result = append(result, []any{id, later, append([]byte(nil), opaque...)})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPublicNativeBackupRestoreAndPhysicalCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	binary := filepath.Join(bazeltest.OverrideRoot(), os.Getenv("NATIVE_BACKUP_RESTORE_BINARY"))
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if artifacts == "" {
		t.Fatal("Bazel artifacts required")
	}
	if err := os.WriteFile(filepath.Join(artifacts, "native-backup-restore"), binaryBytes, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", ownedDir(t))
	t.Setenv("DOLT_ROOT_PATH", ownedDir(t))
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	sourceDir, backupDir := ownedDir(t), ownedDir(t)
	sourceDB, closeSource, err := physicalcapture.Open(sourceDir, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceDB.Close(); _ = closeSource() })
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, query := range []string{
		"CREATE DATABASE `source`", "USE `source`",
		"CREATE TABLE archived_rows (id INT, old_column TEXT, opaque LONGBLOB)",
		"INSERT INTO archived_rows VALUES (1,'old',X'00FF'),(1,'old',X'00FF'),(2,NULL,NULL)",
		"CALL DOLT_ADD('-A')",
		"CALL DOLT_COMMIT('-m','public backup old schema','--author','Public fixture <source@example.test>')",
		"CREATE TABLE events (id INT, detail TEXT)",
		"INSERT INTO dolt_ignore (pattern,ignored) VALUES ('events',true)",
		"INSERT INTO events VALUES (1,'unversioned audit row'),(1,'unversioned audit row')",
		"ALTER TABLE archived_rows DROP COLUMN old_column, ADD COLUMN later_column TEXT",
		"UPDATE archived_rows SET later_column='later',opaque=X'11'",
		"CALL DOLT_ADD('-A')",
		"CALL DOLT_COMMIT('-m','public backup new schema','--author','Public fixture <source@example.test>')",
	} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			t.Fatalf("public setup %q: %v", query, err)
		}
	}
	port := plugdump.SQLSource{DB: conn}
	graph, err := port.ReadSourceCommits(ctx)
	if err != nil || len(graph.Commits) < 3 {
		t.Fatalf("source graph: %+v %v", graph, err)
	}
	want := map[string]physicalcapture.Image{}
	for _, commit := range graph.Commits {
		image, err := physicalcapture.ReadCommit(ctx, conn, commit.ID)
		if err != nil {
			t.Fatal(err)
		}
		want[commit.ID] = image
	}
	events, err := port.ReadTable(ctx, "events")
	if err != nil || len(events.Rows) != 2 {
		t.Fatalf("public current audit rows: %+v %v", events, err)
	}
	currentArchivedRows := publicArchivedRows(t, ctx, conn)
	url, err := versioncontrolops.DirToFileURL(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := versioncontrolops.BackupAdd(ctx, conn, "public_fixture", url); err != nil {
		t.Fatal(err)
	}
	if err := versioncontrolops.BackupSync(ctx, conn, "public_fixture"); err != nil {
		t.Fatal(err)
	}
	manifestBefore, err := os.ReadFile(filepath.Join(backupDir, "manifest"))
	if err != nil || len(manifestBefore) == 0 {
		t.Fatalf("actual backup manifest: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sourceDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeSource(); err != nil {
		t.Fatal(err)
	}

	invoke := func(scratch string) ([]byte, error) {
		command := exec.CommandContext(ctx, binary, "--disposable", "--backup-dir", backupDir, "--data-dir", scratch, "--database", "restored")
		return command.CombinedOutput()
	}
	t.Run("occupied_scratch_refuses_without_writes", func(t *testing.T) {
		scratch := ownedDir(t)
		path := filepath.Join(scratch, "sentinel")
		if err := os.WriteFile(path, []byte("preserved"), 0600); err != nil {
			t.Fatal(err)
		}
		if output, err := invoke(scratch); err == nil {
			t.Fatalf("occupied scratch accepted: %s", output)
		}
		data, err := os.ReadFile(path)
		entries, readErr := os.ReadDir(scratch)
		if err != nil || readErr != nil || string(data) != "preserved" || len(entries) != 1 {
			t.Fatal("refused scratch changed")
		}
	})

	scratch := ownedDir(t)
	output, err := invoke(scratch)
	t.Logf("PUBLIC_NATIVE_BACKUP_RESTORE binary_sha256=%x output=%s error=%v", sha256.Sum256(binaryBytes), output, err)
	if err != nil {
		t.Fatalf("actual normal-driver backup restore: %v: %s", err, output)
	}
	manifestAfter, err := os.ReadFile(filepath.Join(backupDir, "manifest"))
	if err != nil || !reflect.DeepEqual(manifestBefore, manifestAfter) {
		t.Fatal("backup manifest changed during restore")
	}
	readbackDB, closeReadback, err := physicalcapture.Open(scratch, "restored")
	if err != nil {
		t.Fatal(err)
	}
	defer closeReadback()
	defer readbackDB.Close()
	readbackConn, err := readbackDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer readbackConn.Close()
	readbackPort := plugdump.SQLSource{DB: readbackConn}
	actualGraph, err := readbackPort.ReadSourceCommits(ctx)
	if err != nil || !reflect.DeepEqual(graph.Commits, actualGraph.Commits) || graph.Head != actualGraph.Head {
		t.Fatalf("backup graph/parents/metadata changed: %v", err)
	}
	for _, commit := range graph.Commits {
		t.Run("immutable_image_"+commit.ID, func(t *testing.T) {
			actual, err := physicalcapture.ReadCommit(ctx, readbackConn, commit.ID)
			if err != nil || !reflect.DeepEqual(want[commit.ID], actual) {
				t.Fatalf("backup physical image/schema/cell/multiplicity changed: %v", err)
			}
		})
	}
	names, err := readbackPort.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	currentRows := map[string]int{}
	for _, name := range names {
		if name == "archived_rows" {
			actual := publicArchivedRows(t, ctx, readbackConn)
			if !reflect.DeepEqual(currentArchivedRows, actual) {
				t.Fatal("public current archived schema/cells/multiplicity changed")
			}
			currentRows[name] = len(actual)
			continue
		}
		table, err := readbackPort.ReadTable(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		currentRows[name] = len(table.Rows)
		if name == "events" && !reflect.DeepEqual(events, table) {
			t.Fatal("retained current audit rows changed")
		}
	}
	workingSet := "not-retained-in-backup"
	if currentRows["events"] == len(events.Rows) {
		workingSet = "retained-exact"
	}
	evidence, err := json.MarshalIndent(map[string]any{
		"binary_sha256":          fmt.Sprintf("%x", sha256.Sum256(binaryBytes)),
		"backup_manifest_sha256": fmt.Sprintf("%x", sha256.Sum256(manifestBefore)),
		"commits":                len(graph.Commits), "images_compared": len(want),
		"source_current_events_rows": len(events.Rows), "restored_current_rows": currentRows,
		"current_unversioned_events": workingSet,
		"scope":                      "fabricated public native backup, normal restore API, exact physical image comparison; no private or full working-set inference",
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifacts, "PUBLIC-BACKUP-CAPTURE.json"), evidence, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_BACKUP_CAPTURE %s", evidence)
}
