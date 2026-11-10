package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
)

func commitHistoryCommand(t *testing.T, ctx context.Context, binary, dataDir string) (*exec.Cmd, string) {
	t.Helper()
	workspace, home, output := diagnosticDir(t), diagnosticDir(t), diagnosticDir(t)
	beadsDir := filepath.Join(workspace, ".beads")
	if err := os.Mkdir(beadsDir, 0700); err != nil {
		t.Fatal(err)
	}
	environment := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "DOLT_ROOT_PATH" || strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "XDG_") {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(environment, "HOME="+home, "DOLT_ROOT_PATH="+home, "BEADS_DIR="+beadsDir, "XDG_CONFIG_HOME="+home, "BD_DISABLE_METRICS=1", "BD_DISABLE_EVENT_FLUSH=1")
	command := exec.CommandContext(ctx, binary, "--disposable", "--data-dir", dataDir, "--database", "public_commit_history", "--beads-dir", beadsDir, "--output-dir", output)
	command.Dir, command.Env = workspace, environment
	return command, output
}

func commitHistoryCurrent(t *testing.T, ctx context.Context, conn *sql.Conn) map[string]plugdump.NativeTable {
	t.Helper()
	port := plugdump.SQLSource{DB: conn}
	names, err := port.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]plugdump.NativeTable{}
	for _, name := range names {
		table, err := port.ReadTable(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		tables[name] = ownVersionCanonical(t, table)
	}
	return tables
}

func commitHistoryCursor(t *testing.T, image physicalcapture.Image) *uint64 {
	t.Helper()
	for _, table := range image.Tables {
		if table.Name != "schema_migrations" {
			continue
		}
		index := -1
		for i, column := range table.Columns {
			if column.Name == "version" {
				index = i
			}
		}
		if index < 0 {
			t.Fatal("actual cursor missing its numeric column")
		}
		version := uint64(0)
		for _, row := range table.Rows {
			value, err := strconv.ParseUint(fmt.Sprint(row[index]), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if value > version {
				version = value
			}
		}
		return &version
	}
	if len(image.Tables) == 1 && image.Tables[0].Name == "dolt_ignore" {
		columns := map[string]bool{}
		for _, column := range image.Tables[0].Columns {
			columns[column.Name] = true
		}
		if len(image.Tables[0].Columns) == 2 && columns["pattern"] && columns["ignored"] {
			return nil
		}
	}
	if len(image.Tables) != 0 {
		t.Fatal("noninitial public root has no cursor")
	}
	return nil
}

func commitHistoryTable(image physicalcapture.Image, name string) (physicalcapture.Table, bool) {
	for _, table := range image.Tables {
		if table.Name == name {
			return table, true
		}
	}
	return physicalcapture.Table{}, false
}

func TestPublicPerCommitOwnVersionAcrossPinnedMigrationHistory(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	dataDir := diagnosticDir(t)
	db, closeDriver, err := openPublicMigrationFixture(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(); _ = closeDriver() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE `public_commit_history`")
	ownVersionExec(t, ctx, conn, "USE `public_commit_history`")
	// Preserve the real initial Dolt ignore root as a cursor-absent image,
	// alongside the empty genesis. Neither has numeric Beads version0.
	ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore(pattern,ignored) VALUES ('__temp__%',true)")
	ownVersionCommit(t, ctx, conn)
	if _, err := schema.MigrateUpTo(ctx, conn, 39); err != nil {
		t.Fatal(err)
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	for _, id := range []string{"public-a1b", "public-c2d"} {
		ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,metadata,timeout_ns,hook_bead,role_bead,payload) VALUES (?,?,?,?,?,?,JSON_OBJECT('n',9007199254740993),9007199254740993,NULL,'','null')", id, "public migration history", "", "", "", "")
	}
	ownVersionExec(t, ctx, conn, "UPDATE issues SET metadata=CAST('null' AS JSON) WHERE id='public-c2d'")
	ownVersionExec(t, ctx, conn, "INSERT INTO compaction_snapshots(id,issue_id,compaction_level,snapshot_json) VALUES ('public-binary','public-a1b',1,?),('public-empty-binary','public-c2d',1,?)", []byte{0, 255, 128, 42}, []byte{})
	ownVersionExec(t, ctx, conn, "INSERT INTO dependencies(issue_id,depends_on_id,type,created_by) VALUES ('public-a1b','public-c2d','blocks','public')")
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-before-62','public-a1b','updated','public',NULL,'','pre-0062 retained event')")
	checkpoints := map[int]string{39: ownVersionCommit(t, ctx, conn)}
	for _, version := range []int{40, 41, 43, 54, 55, 61, 62, 63, 64, 66} {
		if _, err := schema.MigrateUpTo(ctx, conn, version); err != nil {
			t.Fatalf("pinned migration checkpoint %d: %v", version, err)
		}
		ownVersionExec(t, ctx, conn, "UPDATE issues SET title=? WHERE id='public-a1b'", fmt.Sprintf("public main %d", version))
		checkpoints[version] = ownVersionCommit(t, ctx, conn)
	}
	// The real ignored prefix is applied only in this disposable fixture's
	// current root. No ignored cursor/version is assigned to an old commit.
	for _, name := range ownVersionUnversioned {
		ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore(pattern,ignored) VALUES (?,true) ON DUPLICATE KEY UPDATE ignored=true", name)
	}
	ownVersionExec(t, ctx, conn, "CREATE TABLE IF NOT EXISTS ignored_schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64))")
	for i, name := range ownVersionIgnoredMigrations[:26] {
		query, err := schema.IgnoredMigrationSQL(name)
		if err != nil {
			t.Fatal(err)
		}
		ownVersionExec(t, ctx, conn, query)
		ownVersionExec(t, ctx, conn, "INSERT INTO ignored_schema_migrations(version,content_hash) VALUES (?,?)", i+1, fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
	}
	head := ownVersionCommit(t, ctx, conn)
	beforeWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	beforeCurrent := commitHistoryCurrent(t, ctx, conn)
	beforeLog, err := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	if err != nil || !beforeLog.Clean {
		t.Fatalf("public fixture must be clean: %v", err)
	}
	originals := map[string]physicalcapture.Image{}
	for _, commit := range beforeLog.Commits {
		image, err := physicalcapture.ReadCommit(ctx, conn, commit.ID)
		if err != nil {
			t.Fatal(err)
		}
		cursor, err := physicalcapture.ReadCommitSchema(ctx, conn, commit.ID)
		if err != nil || cursor.CommitID != commit.ID || !cursor.ReadOnlyScope || cursor.RootTableCount != len(image.Tables) {
			t.Fatal("cursor-only physical root boundary differs")
		}
		for _, table := range cursor.Tables {
			expected, present := commitHistoryTable(image, table.Name)
			if (table.Name != "schema_migrations" && table.Name != "ignored_schema_migrations") || !present || !reflect.DeepEqual(expected, table) {
				t.Fatal("cursor operation read a user table or changed cursor cells")
			}
		}
		originals[commit.ID] = image
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	occupied, occupiedErr := physicalcapture.ReadCommitSchema(ctx, conn, head)
	if occupiedErr == nil || occupied.ReadOnlyScope || occupied.CommitID != "" || len(occupied.Tables) != 0 {
		t.Fatal("cursor operation accepted/returned an occupied scope")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}
	command, output := commitHistoryCommand(t, ctx, binary, dataDir)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("multi-version full capture: %T stdout=%s stderr=%s", err, stdout.Bytes(), stderr.Bytes())
	}
	archive, err := os.ReadFile(filepath.Join(output, "capture.tar"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, native := ownVersionArchive(t, archive)
	evidenceData, err := os.ReadFile(filepath.Join(output, "physical-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence commitPhysicalEvidence
	decoder := json.NewDecoder(bytes.NewReader(evidenceData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		t.Fatal(err)
	}
	if manifest.MainVersion != 66 || manifest.IgnoredVersion != 26 || !native.Complete || len(native.Gaps) != 0 || len(native.Images) != len(originals) || !ownVersionEqual(t, native.Current, beforeCurrent) {
		t.Fatal("own-version current/history census differs")
	}
	if evidence.Format != "beads-physical-evidence/1" || evidence.SQLDefaults != "not-captured" || evidence.ArchiveSHA256 != fmt.Sprintf("%x", sha256.Sum256(archive)) || evidence.StoreID != native.StoreID || !reflect.DeepEqual(evidence.Projects, native.Projects) || len(evidence.Images) != len(native.Images) {
		t.Fatal("typed sidecar/archive binding differs")
	}
	sawVersions := map[uint64]int{}
	cursorAbsentInitialImages := 0
	for i, image := range native.Images {
		original, present := originals[image.CommitID]
		if !present || image.SchemaProfile == nil {
			t.Fatal("unbound immutable image/profile")
		}
		observedVersion := commitHistoryCursor(t, original)
		if !reflect.DeepEqual(image.SchemaProfile.MainVersion, observedVersion) || image.SchemaProfile.IgnoredVersion != nil || image.SchemaProfile.Scope != "immutable-commit-root" {
			t.Fatal("historical schema inferred from HEAD/ignored working root")
		}
		if observedVersion != nil {
			sawVersions[*observedVersion]++
		} else {
			cursorAbsentInitialImages++
		}
		typed, err := physicalcapture.EvidenceTables(original.Tables)
		if err != nil || evidence.Images[i].CommitID != image.CommitID || !evidence.Images[i].ReadOnlyScope || !reflect.DeepEqual(evidence.Images[i].Tables, typed) || !reflect.DeepEqual(evidence.Images[i].SchemaProfile, image.SchemaProfile) {
			t.Fatal("historical SQL types/tagged cells/schema binding differs")
		}
		// Independently match every claimed public absence against the full
		// original root. Extra old tables/columns are not a current projection.
		absent := map[string]bool{}
		for _, a := range image.SchemaProfile.Absences {
			key := a.Family + "/" + a.Column
			if absent[key] || a.Reason != plugdump.AbsentBySourceSchema {
				t.Fatal("duplicate/untyped historical absence")
			}
			absent[key] = true
			table, found := commitHistoryTable(original, a.Family)
			if a.Column == "" {
				if found {
					t.Fatal("present table marked absent")
				}
				continue
			}
			if !found {
				t.Fatal("column absence replaced table absence")
			}
			for _, column := range table.Columns {
				if column.Name == a.Column {
					t.Fatal("present column marked absent")
				}
			}
		}
		for family, columns := range plugdump.NativeCommitSchemaSurfaces() {
			table, found := commitHistoryTable(original, family)
			if !found {
				if !absent[family+"/"] {
					t.Fatal("physical table absence not typed")
				}
				continue
			}
			names := map[string]bool{}
			for _, column := range table.Columns {
				names[column.Name] = true
			}
			for _, column := range columns {
				if !names[column] && !absent[family+"/"+column] {
					t.Fatal("physical column absence not typed")
				}
			}
		}
		expectedNative := map[string]plugdump.NativeTable{}
		for _, table := range original.Tables {
			expectedNative[table.Name] = commitHistoryNativeTable(t, table)
		}
		if !ownVersionEqual(t, image.Tables, expectedNative) {
			t.Fatal("every historical native cell differs from original typed physical root")
		}
		if len(image.Tables) != len(original.Tables) {
			t.Fatal("old/dropped/reshaped table omitted")
		}
		for _, table := range original.Tables {
			kept, ok := image.Tables[table.Name]
			if !ok || len(kept.Columns) != len(table.Columns) || len(kept.Rows) != len(table.Rows) {
				t.Fatal("historical physical table/column/row census changed")
			}
			for _, column := range table.Columns {
				found := false
				for _, name := range kept.Columns {
					if name == column.Name {
						found = true
					}
				}
				if !found {
					t.Fatal("old physical column projected away")
				}
			}
		}
	}
	if cursorAbsentInitialImages < 2 {
		t.Fatal("empty and dolt_ignore initial roots were not captured with null cursors")
	}
	for _, version := range []int{39, 40, 41, 43, 54, 55, 61, 62, 63, 64, 66} {
		if sawVersions[uint64(version)] == 0 {
			t.Fatalf("actual migration checkpoint %d not captured", version)
		}
	}
	beforeSplit := originals[checkpoints[39]]
	afterSplit := originals[checkpoints[43]]
	oldDep, _ := commitHistoryTable(beforeSplit, "dependencies")
	newDep, _ := commitHistoryTable(afterSplit, "dependencies")
	hasColumn := func(table physicalcapture.Table, name string) bool {
		for _, column := range table.Columns {
			if column.Name == name {
				return true
			}
		}
		return false
	}
	if !hasColumn(oldDep, "depends_on_id") || hasColumn(newDep, "depends_on_id") || !hasColumn(newDep, "depends_on_issue_id") {
		t.Fatal("genuine dependency reshape not exercised")
	}
	oldIssue, _ := commitHistoryTable(originals[checkpoints[54]], "issues")
	newIssue, _ := commitHistoryTable(originals[checkpoints[55]], "issues")
	if !hasColumn(oldIssue, "lease_expires_at") || hasColumn(newIssue, "lease_expires_at") {
		t.Fatal("genuine dropped lease columns not exercised")
	}
	oldEvents, oldEventsPresent := commitHistoryTable(originals[checkpoints[61]], "events")
	_, newEventsPresent := commitHistoryTable(originals[checkpoints[62]], "events")
	if !oldEventsPresent || len(oldEvents.Rows) != 1 || newEventsPresent {
		t.Fatal("pre/post0062 retained event table transition not exercised")
	}
	typedWorking, err := physicalcapture.EvidenceTables(beforeWorking.Tables)
	if err != nil || !reflect.DeepEqual(evidence.Working.Tables, typedWorking) || evidence.Working.WorkingRootHash != beforeWorking.WorkingRootHash || evidence.Working.HeadCommitID != head {
		t.Fatal("working-root type/cell binding differs")
	}
	// Real close/reopen proves capture neither migrated nor added commits.
	db, closeDriver, err = physicalcapture.Open(dataDir, "public_commit_history")
	if err != nil {
		t.Fatal(err)
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	afterWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
	if err != nil || !reflect.DeepEqual(beforeWorking, afterWorking) {
		t.Fatal("reopened source root changed")
	}
	afterLog, err := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	if err != nil || !reflect.DeepEqual(beforeLog, afterLog) || !ownVersionEqual(t, beforeCurrent, commitHistoryCurrent(t, ctx, conn)) {
		t.Fatal("reopened source current/graph changed")
	}
	for id, original := range originals {
		after, err := physicalcapture.ReadCommit(ctx, conn, id)
		if err != nil || !reflect.DeepEqual(original, after) {
			t.Fatal("reopened immutable image changed")
		}
	}
	artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	for name, data := range map[string][]byte{"public-multiversion-v66.tar": archive, "public-multiversion-v66-physical-evidence.json": evidenceData} {
		if err := os.WriteFile(filepath.Join(artifacts, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	facts, err := json.MarshalIndent(map[string]any{"checkpoints": checkpoints, "schema_versions": sawVersions, "cursor_absent_initial_images": cursorAbsentInitialImages, "commits": len(native.Commits), "images": len(native.Images), "gaps": len(native.Gaps), "archive_sha256": fmt.Sprintf("%x", sha256.Sum256(archive)), "before_working": beforeWorking, "input_unchanged_after_reopen": true}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifacts, "public-multiversion-v66-facts.json"), facts, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_PER_COMMIT_EXACT main=66 ignored=26 versions=%v commits=%d images=%d gaps=0 current_and_every_physical_cell=true reopened_input_unchanged=true", sawVersions, len(native.Commits), len(native.Images))
	// Introduce an actually committed unknown prefix, then restore HEAD to66.
	// Refusal must name the historical73 and ordinal2, not current HEAD66.
	ownVersionExec(t, ctx, conn, "INSERT INTO schema_migrations(version,content_hash) VALUES (73,?)", strings.Repeat("0", 64))
	ownVersionCommit(t, ctx, conn)
	ownVersionExec(t, ctx, conn, "DELETE FROM schema_migrations WHERE version=73")
	refusedHead := ownVersionCommit(t, ctx, conn)
	refusalBefore, err := physicalcapture.ReadWorkingRoot(ctx, conn, refusedHead)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}
	refusedCommand, refusedOutput := commitHistoryCommand(t, ctx, binary, dataDir)
	stdout.Reset()
	stderr.Reset()
	refusedCommand.Stdout, refusedCommand.Stderr = &stdout, &stderr
	if err := refusedCommand.Run(); err == nil {
		t.Fatal("unknown historical schema accepted")
	}
	reports := decodeSafeReports(t, stderr.Bytes())
	failure := reports[len(reports)-1]
	if failure.Stage != stageHistory || failure.CommitSchemaVersion == nil || *failure.CommitSchemaVersion != 73 || failure.CommitOrdinalFromHead == nil || *failure.CommitOrdinalFromHead != 2 || failure.InnermostGoErrorType != "plugdump.ErrUnsupportedNativeCommitSchema" {
		t.Fatalf("historical typed refusal has wrong own schema/ordinal: %+v", failure)
	}
	files, err := os.ReadDir(refusedOutput)
	if err != nil || len(files) != 0 {
		t.Fatal("refused history published partial files")
	}
	db, closeDriver, err = physicalcapture.Open(dataDir, "public_commit_history")
	if err != nil {
		t.Fatal(err)
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	refusalAfter, err := physicalcapture.ReadWorkingRoot(ctx, conn, refusedHead)
	if err != nil || !reflect.DeepEqual(refusalBefore, refusalAfter) {
		t.Fatal("unknown-version refusal mutated source")
	}
	if err := os.WriteFile(filepath.Join(artifacts, "public-multiversion-unsupported-diagnostic.jsonl"), stderr.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

// Independent comparison starts from the original347 detached values, using
// the archive's documented timestamp/JSON/binary representation. It never
// calls the producer's sqlValue or physical-to-native adapter.
func commitHistoryNativeTable(t *testing.T, table physicalcapture.Table) plugdump.NativeTable {
	t.Helper()
	value := plugdump.Table{Columns: []string{}, Rows: []plugdump.Row{}}
	for _, column := range table.Columns {
		value.Columns = append(value.Columns, column.Name)
	}
	for _, cells := range table.Rows {
		row := plugdump.Row{}
		for i, cell := range cells {
			column := table.Columns[i]
			switch v := cell.(type) {
			case time.Time:
				cell = v.UTC().Format(time.RFC3339Nano)
			case []byte:
				kind := strings.ToUpper(column.DatabaseTypeName)
				if strings.Contains(kind, "BLOB") || strings.Contains(kind, "BINARY") {
					cell = map[string]string{"$bytes": base64.StdEncoding.EncodeToString(v)}
				} else {
					cell = string(v)
				}
			}
			if text, ok := cell.(string); ok && strings.EqualFold(column.DatabaseTypeName, "JSON") {
				decoder := json.NewDecoder(strings.NewReader(text))
				decoder.UseNumber()
				if err := decoder.Decode(&cell); err != nil {
					t.Fatal(err)
				}
			}
			row[column.Name] = cell
		}
		value.Rows = append(value.Rows, row)
	}
	return ownVersionCanonical(t, value)
}
