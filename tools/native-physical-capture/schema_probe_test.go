package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	embedded "github.com/dolthub/driver/v2"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

func diagnosticDir(t *testing.T) string {
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

func diagnosticBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(bazeltest.OverrideRoot(), os.Getenv("BEADS_PHYSICAL_CAPTURE_BINARY"))
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if artifacts == "" {
		t.Fatal("canonical public artifacts required")
	}
	if err := os.WriteFile(filepath.Join(artifacts, "native-physical-capture"), data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("SAFE_CAPTURE_BINARY sha256=%x bytes=%d", sha256.Sum256(data), len(data))
	return binary
}

func decodeSafeReports(t *testing.T, data []byte) []safeCaptureReport {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var reports []safeCaptureReport
	for {
		var report safeCaptureReport
		err := decoder.Decode(&report)
		if err == io.EOF {
			return reports
		}
		if err != nil {
			t.Fatalf("non-taxonomy output: %s: %v", data, err)
		}
		if report.Format != "beads-capture-diagnostic/1" {
			t.Fatalf("unknown report format: %+v", report)
		}
		switch report.Stage {
		case stageOpen, stageSchema, stageCensus, stageHistory, stageCell:
		default:
			t.Fatalf("unregistered stage: %+v", report)
		}
		reports = append(reports, report)
	}
}

// The actual migration files contain multiple SQL statements. Enable the
// driver's existing multistatements setup mode only for constructing this public
// fixture. The reader and post-fixture physical port retain their original DSN.
func openPublicMigrationFixture(dataDir string) (*sql.DB, func() error, error) {
	params := url.Values{"commitname": {"Public fixture"}, "commitemail": {"fixture@example.test"}, "disable_singleton_cache": {"true"}, "multistatements": {"true"}}
	cfg, err := embedded.ParseDSN("file://" + dataDir + "?" + params.Encode())
	if err != nil {
		return nil, nil, err
	}
	connector, err := embedded.NewConnector(cfg)
	if err != nil {
		return nil, nil, err
	}
	return sql.OpenDB(connector), connector.Close, nil
}

func TestPublicCaptureSchemaProbeOwnVersionBeforeReader(t *testing.T) {
	binary := diagnosticBinary(t)
	for _, fixture := range []struct {
		name    string
		version int
	}{
		{"actual_old_schema_66", 66}, {"actual_unsupported_schema_65", 65}, {"actual_current_schema_72", 72}, {"missing_schema_cursor", 0},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			home := diagnosticDir(t)
			t.Setenv("HOME", home)
			t.Setenv("DOLT_ROOT_PATH", home)
			t.Setenv("DOLT_METRICS_DISABLED", "1")
			dataDir := diagnosticDir(t)
			db, closeDriver, err := openPublicMigrationFixture(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close(); _ = closeDriver() })
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			for _, query := range []string{"CREATE DATABASE public_schema_fixture", "USE public_schema_fixture"} {
				if _, err := conn.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			if fixture.version != 0 {
				// Apply the actual historical migration prefix. Do not forge a
				// cursor on a current schema or migrate the capture input.
				if _, err := schema.MigrateUpTo(ctx, conn, fixture.version); err != nil {
					t.Fatal(err)
				}
				for _, query := range []string{"CALL DOLT_ADD('-A')", "CALL DOLT_COMMIT('-m','public schema fixture','--author','Public fixture <fixture@example.test>')"} {
					if _, err := conn.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
			}
			head, err := versioncontrolops.CurrentCommit(ctx, conn)
			if err != nil {
				t.Fatal(err)
			}
			before, err := physicalcapture.ReadCommit(ctx, conn, head)
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
			workspace := diagnosticDir(t)
			beadsDir := filepath.Join(workspace, ".beads")
			if err := os.Mkdir(beadsDir, 0700); err != nil {
				t.Fatal(err)
			}
			output := diagnosticDir(t)
			environment := []string{}
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if key == "HOME" || key == "DOLT_ROOT_PATH" || key == "BEADS_DIR" || strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "XDG_") {
					continue
				}
				environment = append(environment, entry)
			}
			environment = append(environment, "HOME="+home, "DOLT_ROOT_PATH="+home, "BEADS_DIR="+beadsDir, "BD_DISABLE_METRICS=1", "BD_DISABLE_EVENT_FLUSH=1", "XDG_CONFIG_HOME="+home)
			invoke := func(schemaOnly bool) ([]byte, []byte, error) {
				args := []string{"--disposable", "--data-dir", dataDir, "--database", "public_schema_fixture", "--beads-dir", beadsDir, "--output-dir", output}
				if schemaOnly {
					args = append(args, "--schema-only")
				}
				command := exec.CommandContext(ctx, binary, args...)
				command.Dir, command.Env = workspace, environment
				var stdout, stderr bytes.Buffer
				command.Stdout, command.Stderr = &stdout, &stderr
				err := command.Run()
				return stdout.Bytes(), stderr.Bytes(), err
			}
			stdout, stderr, probeErr := invoke(true)
			t.Logf("PUBLIC_SCHEMA_PROBE fixture=%s stdout=%s stderr=%s exit_error_type=%T", fixture.name, stdout, stderr, probeErr)
			if fixture.version == 0 {
				if probeErr == nil || len(stdout) != 0 {
					t.Fatal("missing migration cursor fabricated a successful measurement")
				}
				reports := decodeSafeReports(t, stderr)
				if len(reports) != 1 || reports[0].Stage != stageSchema || reports[0].Schema.FoundVersion != nil || !reflect.DeepEqual(reports[0].SQLCodes, []uint16{1146}) {
					t.Fatalf("missing-table failure classification differs: %+v", reports)
				}
			} else {
				if probeErr != nil || len(stderr) != 0 {
					t.Fatalf("actual own-version numeric probe failed: %v: %s", probeErr, stderr)
				}
				reports := decodeSafeReports(t, stdout)
				if len(reports) != 1 || reports[0].Stage != stageSchema || reports[0].Schema.FoundVersion == nil || *reports[0].Schema.FoundVersion != uint64(fixture.version) || reports[0].Schema.ExpectedVersion != plugdump.CaptureSchemaVersion() || reports[0].Schema.RowCountClass != "series" || len(reports[0].GoErrorTypes) != 0 {
					t.Fatalf("actual numeric schema evidence differs: %+v", reports)
				}
			}
			if fixture.version == 65 {
				stdout, stderr, readerErr := invoke(false)
				t.Logf("PUBLIC_OWN_VERSION_REFUSAL stdout=%s stderr=%s exit_error_type=%T", stdout, stderr, readerErr)
				reports := decodeSafeReports(t, stderr)
				if readerErr == nil || len(stdout) != 0 || len(reports) != 2 || reports[1].Stage != stageSchema || *reports[1].Schema.FoundVersion != 65 || !reflect.DeepEqual(reports[1].GoErrorTypes, []string{"*main.captureFailure", "*errors.joinError", "*main.unsupportedCaptureSchema"}) {
					t.Fatal("unsupported own-version schema did not retain typed refusal")
				}
			}
			entries, err := os.ReadDir(output)
			if err != nil || len(entries) != 0 {
				t.Fatal("schema-only/refused reader published output")
			}
			readbackDB, closeReadback, err := physicalcapture.Open(dataDir, "public_schema_fixture")
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
			afterHead, err := versioncontrolops.CurrentCommit(ctx, readback)
			if err != nil || afterHead != head {
				t.Fatal("probe or unsupported read changed history")
			}
			after, err := physicalcapture.ReadCommit(ctx, readback, head)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("probe or unsupported read changed native schema/cells")
			}
		})
	}
}

func TestPublicCaptureFlagErrorNeverEchoesArgument(t *testing.T) {
	binary := diagnosticBinary(t)
	command := exec.Command(binary, "--timeout=private_path_and_value_abc")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err == nil || stdout.Len() != 0 || bytes.Contains(stderr.Bytes(), []byte("private_path_and_value_abc")) {
		t.Fatal("flag error accepted or echoed its value")
	}
	reports := decodeSafeReports(t, stderr.Bytes())
	if len(reports) != 1 || reports[0].Stage != stageOpen {
		t.Fatal("flag error is not an open-stage typed report")
	}
}
