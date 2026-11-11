package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
)

// These names select the actual frozen public ignored migrations, not copied
// schema DDL. The prefix is applied only while constructing an empty fixture.
var ownVersionIgnoredMigrations = []string{
	"0001_create_local_state_tables.up.sql",
	"0002_create_wisp_child_counters.up.sql",
	"0003_split_wisp_dependencies_target.up.sql",
	"0004_add_wisp_aux_fks.up.sql",
	"0005_drop_wisp_dependencies_generated_column.up.sql",
	"0006_add_wisp_is_blocked.up.sql",
	"0007_recompute_wisp_is_blocked.up.sql",
	"0008_widen_wisp_event_value_columns.up.sql",
	"0009_aux_row_id_rekey_marker.up.sql",
	"0010_drop_wisp_id_defaults.up.sql",
	"0011_cleanup_orphaned_child_counters.up.sql",
	"0012_create_leases.up.sql",
	"0013_add_wisp_row_lock.up.sql",
	"0014_add_wisp_comments_keyset_index.up.sql",
	"0015_recompute_null_gate_wisp_is_blocked.up.sql",
	"0016_add_lease_granted_node.up.sql",
	"0017_add_wisps_defer_until_index.up.sql",
	"0018_aux_row_id_rekey2_marker.up.sql",
	"0019_create_events.up.sql",
	"0020_add_wisp_storage_class.up.sql",
	"0021_widen_wisp_large_content_columns.up.sql",
	"0022_create_events_journal.up.sql",
	"0023_repair_events_journal_shape.up.sql",
	"0024_widen_wisp_comments_text.up.sql",
	"0025_add_events_journal_actor.up.sql",
	"0026_dep_rekey_dedup_marker.up.sql",
	"0027_add_wisps_current_revision.up.sql",
	"0028_add_wisps_participation_generation.up.sql",
}

var ownVersionUnversioned = []string{
	"bd_events_journal", "bd_events_seq", "events", "ignored_schema_migrations",
	"leases", "local_metadata", "repo_mtimes", "wisp_child_counters",
	"wisp_comments", "wisp_dependencies", "wisp_events", "wisp_labels", "wisps",
}

func ownVersionExec(t *testing.T, ctx context.Context, conn *sql.Conn, query string, args ...any) {
	t.Helper()
	if _, err := conn.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("public fixture statement %s: %v", query, err)
	}
}

func ownVersionCommit(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	ownVersionExec(t, ctx, conn, "CALL DOLT_ADD('-A')")
	ownVersionExec(t, ctx, conn, "CALL DOLT_COMMIT('-m','public own-version fixture','--author','Public fixture <fixture@example.test>')")
	head, err := versioncontrolops.CurrentCommit(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func ownVersionArchive(t *testing.T, data []byte) (plugdump.Manifest, plugdump.NativeHistory) {
	t.Helper()
	manifest, err := plugdump.Validate(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == plugdump.NativeHistoryMember {
			var native plugdump.NativeHistory
			decoder := json.NewDecoder(reader)
			decoder.UseNumber()
			if err := decoder.Decode(&native); err != nil {
				t.Fatal(err)
			}
			return manifest, native
		}
	}
}

func ownVersionCanonical(t *testing.T, table plugdump.Table) plugdump.NativeTable {
	t.Helper()
	columns := append([]string{}, table.Columns...)
	sort.Strings(columns)
	rows := append([]plugdump.Row{}, table.Rows...)
	sort.Slice(rows, func(i, j int) bool {
		a, aErr := json.Marshal(rows[i])
		b, bErr := json.Marshal(rows[j])
		if aErr != nil || bErr != nil {
			t.Fatal("public SQL row cannot be encoded")
		}
		return bytes.Compare(a, b) < 0
	})
	return plugdump.NativeTable{Columns: columns, Rows: rows}
}

func ownVersionEqual(t *testing.T, a, b any) bool {
	t.Helper()
	x, xErr := json.Marshal(a)
	y, yErr := json.Marshal(b)
	if xErr != nil || yErr != nil {
		t.Fatal("public source cannot be encoded")
	}
	return bytes.Equal(x, y)
}

func TestPublicOwnVersionFullCaptureActualMigrationPrefixes(t *testing.T) {
	binary := diagnosticBinary(t)
	for _, version := range []int{66, 72} {
		t.Run(fmt.Sprintf("actual_main_%d", version), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			dataDir, home := diagnosticDir(t), diagnosticDir(t)
			db, closeDriver, err := openPublicMigrationFixture(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDriver()
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ownVersionExec(t, ctx, conn, "CREATE DATABASE `public_own_version`")
			ownVersionExec(t, ctx, conn, "USE `public_own_version`")
			if _, err := schema.MigrateUpTo(ctx, conn, version); err != nil {
				t.Fatal(err)
			}
			for _, name := range ownVersionUnversioned {
				ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore (pattern,ignored) VALUES (?,true) ON DUPLICATE KEY UPDATE ignored=true", name)
			}
			ignored := 26
			if version == 72 {
				ignored = 28
			}
			// Bootstrap matches migrationSource.bootstrapSQL exactly. The
			// subsequent DDL and hash receipts come from the embedded sources.
			ownVersionExec(t, ctx, conn, "CREATE TABLE IF NOT EXISTS ignored_schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64))")
			for i, name := range ownVersionIgnoredMigrations[:ignored] {
				query, err := schema.IgnoredMigrationSQL(name)
				if err != nil {
					t.Fatal(err)
				}
				ownVersionExec(t, ctx, conn, query)
				ownVersionExec(t, ctx, conn, "INSERT INTO ignored_schema_migrations(version,content_hash) VALUES (?,?)", i+1, fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
			}
			ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
			for _, table := range []string{"issues", "wisps"} {
				ownVersionExec(t, ctx, conn, "INSERT INTO `"+table+"` (id,title,description,design,acceptance_criteria,notes,created_by,metadata,timeout_ns,hook_bead,role_bead,payload,waiters,agent_state) VALUES (?,?,?,?,?,?,?,JSON_OBJECT('n',9007199254740993),9007199254740993,NULL,'public-role','{\"n\":9007199254740993}','[null,\"public\"]','working')", "public-a1b", "public native row", "", "", "", "", "public")
			}
			ownVersionCommit(t, ctx, conn)
			ownVersionExec(t, ctx, conn, "UPDATE issues SET title='public second committed title' WHERE id='public-a1b'")
			head := ownVersionCommit(t, ctx, conn)
			ownVersionExec(t, ctx, conn, "UPDATE wisps SET title='public current-only title' WHERE id='public-a1b'")
			ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,comment) VALUES ('public-event','public-a1b','updated','public','current-only audit')")
			beforeWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
			if err != nil {
				t.Fatal(err)
			}
			port := plugdump.SQLSource{DB: conn}
			beforeLog, err := port.ReadSourceCommits(ctx)
			if err != nil || !beforeLog.Clean {
				t.Fatalf("public source graph is not clean: %v", err)
			}
			beforeCurrent := map[string]plugdump.NativeTable{}
			names, err := port.Tables(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range names {
				table, err := port.ReadTable(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				beforeCurrent[name] = ownVersionCanonical(t, table)
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
			workspace, output := diagnosticDir(t), diagnosticDir(t)
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
			command := exec.CommandContext(ctx, binary, "--disposable", "--data-dir", dataDir, "--database", "public_own_version", "--beads-dir", beadsDir, "--output-dir", output)
			command.Dir, command.Env = workspace, environment
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			err = command.Run()
			t.Logf("PUBLIC_OWN_VERSION_FULL_CAPTURE main=%d ignored=%d stdout=%s stderr=%s error_type=%T", version, ignored, stdout.Bytes(), stderr.Bytes(), err)
			if err != nil {
				t.Fatal("actual own-version full capture failed")
			}
			data, err := os.ReadFile(filepath.Join(output, "capture.tar"))
			if err != nil {
				t.Fatal(err)
			}
			evidenceData, err := os.ReadFile(filepath.Join(output, "physical-evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			var evidence physicalcapture.Evidence
			if err := json.Unmarshal(evidenceData, &evidence); err != nil {
				t.Fatal(err)
			}
			expectedTables, err := physicalcapture.EvidenceTables(beforeWorking.Tables)
			if err != nil {
				t.Fatal(err)
			}
			if evidence.Format != "beads-physical-evidence/1" || evidence.ArchiveSHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || evidence.SQLDefaults != "not-captured" || evidence.Working.WorkingRootHash != beforeWorking.WorkingRootHash || evidence.Working.Database != beforeWorking.Database || evidence.Working.HeadCommitID != head || !evidence.Working.Complete || !evidence.Working.ReadOnlyScope || !reflect.DeepEqual(evidence.Working.Tables, expectedTables) {
				t.Fatal("typed evidence lost exact archive/root/SQL types/cells binding")
			}
			manifest, native := ownVersionArchive(t, data)
			if manifest.MainVersion != version || manifest.IgnoredVersion != ignored || !native.Complete || len(native.Gaps) != 0 || len(native.Images) != len(beforeLog.Commits) || !ownVersionEqual(t, native.Current, beforeCurrent) {
				t.Fatal("own-version profile, rows or native history was not conserved")
			}
			if version == 66 && (native.SchemaProfile == nil || len(native.SchemaProfile.Absences) != 7) || version == 72 && native.SchemaProfile != nil {
				t.Fatal("typed absences or unchanged72 wire differ")
			}
			readbackDB, closeReadback, err := physicalcapture.Open(dataDir, "public_own_version")
			if err != nil {
				t.Fatal(err)
			}
			defer closeReadback()
			defer readbackDB.Close()
			readback, err := readbackDB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer readback.Close()
			afterWorking, err := physicalcapture.ReadWorkingRoot(ctx, readback, head)
			if err != nil || !reflect.DeepEqual(beforeWorking, afterWorking) {
				t.Fatal("capture changed physical working-root schema, SQL types, cells or HEAD")
			}
			afterLog, err := (plugdump.SQLSource{DB: readback}).ReadSourceCommits(ctx)
			if err != nil || !ownVersionEqual(t, beforeLog, afterLog) {
				t.Fatal("capture changed actual commit IDs, parents or metadata")
			}
			physical := plugdump.PhysicalCommitSource{PhysicalDB: readbackDB}
			if evidence.StoreID != native.StoreID || !reflect.DeepEqual(evidence.Projects, native.Projects) || len(evidence.Images) != len(native.Images) {
				t.Fatal("typed evidence source tuple or history census differs")
			}
			for i, image := range native.Images {
				typed, err := physicalcapture.ReadCommit(ctx, readback, image.CommitID)
				if err != nil {
					t.Fatal(err)
				}
				typedTables, err := physicalcapture.EvidenceTables(typed.Tables)
				if err != nil || evidence.Images[i].CommitID != typed.CommitID || !evidence.Images[i].ReadOnlyScope || !reflect.DeepEqual(evidence.Images[i].Tables, typedTables) {
					t.Fatal("typed evidence changed immutable image schema/SQL types/cells")
				}
				observed, err := physical.ReadNativeSourceImage(ctx, image.CommitID)
				if err != nil || len(observed) != len(image.Tables) {
					t.Fatal("immutable physical image unavailable")
				}
				for name, table := range observed {
					if !ownVersionEqual(t, ownVersionCanonical(t, table), image.Tables[name]) {
						t.Fatal("historical physical schema or cells were rewritten")
					}
				}
			}
			artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if artifacts == "" {
				t.Fatal("actual public archive artifacts required")
			}
			if err := os.WriteFile(filepath.Join(artifacts, fmt.Sprintf("public-actual-v%d.tar", version)), data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(artifacts, fmt.Sprintf("public-actual-v%d-physical-evidence.json", version)), evidenceData, 0600); err != nil {
				t.Fatal(err)
			}
			facts, err := json.MarshalIndent(map[string]any{"main_version": version, "ignored_version": ignored, "native_schema_profile": native.SchemaProfile, "complete": native.Complete, "commits": len(native.Commits), "images": len(native.Images), "current": beforeWorking}, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(artifacts, fmt.Sprintf("public-actual-v%d-physical.json", version)), facts, 0600); err != nil {
				t.Fatal(err)
			}
			t.Logf("PUBLIC_OWN_VERSION_EXACT main=%d ignored=%d usertables=%d physicaltables=%d commits=%d images=%d gaps=0 unchanged=true", version, ignored, len(native.Current), len(beforeWorking.Tables), len(native.Commits), len(native.Images))
		})
	}
}
