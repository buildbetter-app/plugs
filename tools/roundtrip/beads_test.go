package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
)

func TestMissingPinnedBeadsCannotPassOrUsePATH(t *testing.T) {
	result := outcome{BeadsReimport: "WAIT", BeadsReadback: "WAIT"}
	runBeads(context.Background(), beadsOptions{}, t.TempDir(), nil, reverseconverter.Result{}, &result)
	if result.Status != "WAIT" || result.Reason != "pinned_beads_artifact_required" || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" {
		t.Fatalf("missing executable certified a roundtrip: %+v", result)
	}
}

func TestPublicCaptureCannotUseACallerInventory(t *testing.T) {
	output := filepath.Join(t.TempDir(), "not-created")
	got := run([]string{"--input", "not-opened", "--output-dir", output, "--beads-inventory", "not-opened", "--beads-capture-public"})
	if got.Status != "BLOCK" || got.Stage != "arguments" || got.Reason != "conflicting_beads_inventory_options" || got.BeadsReimport != "WAIT" || got.BeadsReadback != "WAIT" {
		t.Fatalf("ambiguous inventory accepted: %+v", got)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("ambiguous inventory opened a source or created a target")
	}
}

func TestPublicCaptureRequiresSeparateExplicitArtifact(t *testing.T) {
	for _, captureArgs := range [][]string{{"--beads-capture-public"}, {"--beads-capture-binary", "/not-opened"}} {
		output := filepath.Join(t.TempDir(), "not-created")
		got := run(append([]string{"--input", "not-opened", "--output-dir", output}, captureArgs...))
		if got.Status != "BLOCK" || got.Stage != "arguments" || got.Reason != "explicit_public_capture_artifact_required" || got.BeadsReimport != "WAIT" || got.BeadsReadback != "WAIT" {
			t.Fatalf("incomplete capture provenance accepted: %+v", got)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatal("incomplete capture created a target")
		}
	}
}

func TestPublicCaptureDigestMismatchCannotExecuteOrReplaceImporter(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "not-executable")
	if err := os.WriteFile(binary, []byte("untrusted capture bytes"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := pinPublicCapture(beadsOptions{CaptureBinary: binary, CaptureSHA256: hash([]byte("different")), CaptureBuildRun: "1"}, root); err == nil {
		t.Fatal("unverified capture bytes were accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "beads-capture-pinned")); !os.IsNotExist(err) {
		t.Fatal("unverified capture copy retained")
	}
	if _, err := os.Stat(filepath.Join(root, "beads-target")); !os.IsNotExist(err) {
		t.Fatal("unverified capture created a target")
	}
}

func TestBeadsDigestMismatchStopsBeforeAnyProcessOrTarget(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "untrusted-artifact")
	if err := os.WriteFile(binary, []byte("never executed"), 0o700); err != nil {
		t.Fatal(err)
	}
	result := outcome{BeadsReimport: "WAIT", BeadsReadback: "WAIT"}
	runBeads(context.Background(), beadsOptions{Binary: binary, SHA256: hash([]byte("different")), BuildRun: "1", Inventory: "not-opened"}, root, nil, reverseconverter.Result{}, &result)
	if result.Status != "BLOCK" || result.Reason != "beads_artifact_digest_mismatch" {
		t.Fatalf("artifact boundary: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, "beads-target")); !os.IsNotExist(err) {
		t.Fatal("unverified artifact created a target")
	}
}

func TestBeadsArtifactValidationDoesNotDependOnReadbackInventory(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "untrusted-artifact")
	if err := os.WriteFile(binary, []byte("never executed"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, run, digest, reason string
	}{
		{"invalid build provenance", "not-a-run", hash([]byte("never executed")), "invalid_beads_provenance"},
		{"invalid delivered bytes", "1", hash([]byte("different")), "beads_artifact_digest_mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := t.TempDir()
			result := outcome{BeadsReimport: "WAIT", BeadsReadback: "WAIT"}
			// No inventory is supplied. An unrelated missing readback census
			// must not conceal a concrete failure in the delivered executable.
			runBeads(context.Background(), beadsOptions{Binary: binary, SHA256: test.digest, BuildRun: test.run}, output, nil, reverseconverter.Result{}, &result)
			if result.Status != "BLOCK" || result.Reason != test.reason || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" {
				t.Fatalf("readback inventory masked invalid executable: %+v", result)
			}
			if _, err := os.Stat(filepath.Join(output, "beads-target")); !os.IsNotExist(err) {
				t.Fatal("invalid executable created a Beads target")
			}
		})
	}
}

func TestOriginalSourceArchiveIsNotPostReimportReadback(t *testing.T) {
	path := "../../internal/integration/offlinehistory/testdata/dolt-source-partial.tar"
	source, err := dumpformat.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the full source census so this control reaches field comparison
	// rather than the pinned default's intentional Ephemeral omissions.
	converted, err := reverseconverter.ConvertArchiveWithOptions(data, reverseconverter.Options{All: true})
	if err != nil {
		t.Fatal(err)
	}
	// The original real SQLSource fixture lacks the reverse stream's metadata
	// envelopes. Treating it as post-import evidence must fail independently.
	if reason := compareRawReadback(converted.JSONL, source); reason != "beads_raw_field_changed" {
		t.Fatalf("original source stood in for a reimported target: %s", reason)
	}
	if losses := nativeOpaqueLosses(source, source); len(losses) != 0 {
		t.Fatalf("same raw source changed: %v", losses)
	}
}

func TestPostgresDestinationOverridesRefuseBeforeConnection(t *testing.T) {
	for _, dsn := range []string{
		"postgres://example.invalid/plug_roundtrip_public",
		"postgres://127.0.0.1/production",
		"postgres://127.0.0.1/plug_roundtrip_public?host=example.invalid",
		"postgres://127.0.0.1/plug_roundtrip_public?service=external",
	} {
		if store, err := openDisposablePostgres(context.Background(), dsn); err == nil || store != nil {
			t.Fatal("unsafe target accepted")
		}
	}
}

func TestPublicPostgresPartialSnapshotReverseAndMissingBeadsBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-postgres-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	name := "plug-swarm-i6-publictool-" + hash([]byte(root))[:16]
	docker := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...).CombinedOutput()
	}
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("owned PostgreSQL: %v %s", err, out)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if out, err := exec.CommandContext(cleanup, "docker", "--host=unix:///var/run/docker.sock", "rm", "-f", "-v", name).CombinedOutput(); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	port, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := strings.TrimSpace(string(port))
	if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" {
		t.Fatal("PostgreSQL is not loopback")
	}
	base := "postgres://postgres@" + address + "/"
	db, err := sql.Open("pgx", base+"postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for db.PingContext(ctx) != nil {
		if ctx.Err() != nil {
			t.Fatal("owned PostgreSQL did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, mode := range []string{"partial", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			database := "plug_roundtrip_" + mode
			if _, err := db.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
				t.Fatal(err)
			}
			result := run([]string{"--input", "../../internal/integration/offlinehistory/testdata/dolt-source-partial.tar", "--output-dir", filepath.Join(root, mode), "--history-mode", mode, "--engine", "postgres", "--postgres-dsn", base + database + "?sslmode=disable", "--all"})
			if result.Status != "WAIT" || result.Reason != "pinned_beads_artifact_required" || result.Claims != 1 || result.Writes != 1 || result.HistoryFloor != "1" || result.RestoredIncarnation == "" || result.ReverseIncarnation != result.RestoredIncarnation || result.ReverseAt == "" || result.SourceHistoryComplete || len(result.SourceHistoryGaps) != 2 || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" {
				t.Fatalf("public PostgreSQL boundary: %+v", result)
			}
			encoded, _ := json.Marshal(result)
			t.Logf("PUBLIC_CONVERTER_OUTCOME %s", encoded)
		})
	}
	for _, exercise := range []bool{true, false} {
		name := "default-no-exercise"
		if exercise {
			name = "default-exercise"
		}
		t.Run(name, func(t *testing.T) {
			database := "plug_roundtrip_" + strings.ReplaceAll(name, "-", "_")
			if _, err := db.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
				t.Fatal(err)
			}
			assertDefaultEphemeralOutcome(t, filepath.Join(root, name), []string{"--engine", "postgres", "--postgres-dsn", base + database + "?sslmode=disable"}, exercise)
		})
	}
	for _, test := range publicProducerCases() {
		t.Run("producer-"+test.name, func(t *testing.T) {
			database := "plug_roundtrip_producer_" + strings.ReplaceAll(test.name, "-", "_")
			if _, err := db.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
				t.Fatal(err)
			}
			args := []string{"--input", "../../internal/integration/converterroundtrip/testdata/" + test.file, "--output-dir", filepath.Join(root, "producer-"+test.name), "--engine", "postgres", "--postgres-dsn", base + database + "?sslmode=disable"}
			if !test.complete {
				args = append(args, "--all")
			}
			if test.mode != "" {
				args = append(args, "--history-mode", test.mode)
			}
			assertPublicProducerOutcome(t, test, run(args))
		})
	}
}

func TestIndependentRawReadbackPinnedDomain(t *testing.T) {
	for _, raw := range []any{nil, "unexpected", "false", "00", "01", " 0", "1.0", json.Number("2"), json.Number("1e0")} {
		if samePinnedValue(false, raw) || samePinnedValue(true, raw) {
			t.Errorf("unsupported raw SQL pinned value certified: %#v", raw)
		}
	}
	for _, raw := range []any{true, json.Number("1"), "1"} {
		if !samePinnedValue(true, raw) || samePinnedValue(false, raw) {
			t.Errorf("actual SQL true encoding changed: %#v", raw)
		}
	}
	for _, raw := range []any{false, json.Number("0"), "0"} {
		if !samePinnedValue(false, raw) || samePinnedValue(true, raw) {
			t.Errorf("actual SQL false encoding changed: %#v", raw)
		}
	}
}
