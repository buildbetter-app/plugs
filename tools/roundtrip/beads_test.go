package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

func TestAutoInventoryUsesRealProducerCommandAndPrivateCopy(t *testing.T) {
	root := t.TempDir()
	want := []byte(`{"version":"beads-inventory/1","external":[]}`)
	var got []string
	path, err := captureAutoInventory(root, func(args ...string) ([]byte, error) {
		got = append([]string(nil), args...)
		if len(args) != 7 || args[0] != "--readonly" || args[1] != "export" || args[2] != "--format" || args[3] != "beads-inventory/1" || args[4] != "--plug-capture-public" || args[5] != "--output" {
			t.Fatalf("auto inventory command=%q", args)
		}
		if err := os.WriteFile(args[6], want, 0o600); err != nil {
			t.Fatal(err)
		}
		return nil, nil
	})
	if err != nil || path != filepath.Join(root, "beads-inventory.json") || len(got) != 7 {
		t.Fatalf("auto inventory capture path=%q command=%q err=%v", path, got, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, want) {
		t.Fatalf("private inventory copy=%q err=%v", data, err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("private inventory mode=%v err=%v", info, err)
	}
}

func TestAutoInventoryProducerFailureDoesNotPublishCopy(t *testing.T) {
	root := t.TempDir()
	path, err := captureAutoInventory(root, func(args ...string) ([]byte, error) {
		return nil, errors.New("producer refused")
	})
	if err == nil || path != "" {
		t.Fatalf("producer refusal was certified: path=%q err=%v", path, err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "beads-inventory.json")); !os.IsNotExist(statErr) {
		t.Fatalf("producer refusal published inventory: %v", statErr)
	}
}

func TestMissingPinnedBeadsCannotPassOrUsePATH(t *testing.T) {
	result := outcome{BeadsReimport: "WAIT", BeadsReadback: "WAIT"}
	runBeads(context.Background(), beadsOptions{}, t.TempDir(), nil, reverseconverter.Result{}, &result)
	if result.Status != "WAIT" || result.Reason != "pinned_beads_artifact_required" || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" {
		t.Fatalf("missing executable certified a roundtrip: %+v", result)
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
	converted, err := reverseconverter.ConvertArchive(data)
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
	if out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("owned PostgreSQL: %v %s", err, out)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if out, err := exec.CommandContext(cleanup, "docker", "--host=unix:///var/run/docker.sock", "rm", "-f", name).CombinedOutput(); err != nil {
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
			result := run([]string{"--input", "../../internal/integration/offlinehistory/testdata/dolt-source-partial.tar", "--output-dir", filepath.Join(root, mode), "--history-mode", mode, "--engine", "postgres", "--postgres-dsn", base + database + "?sslmode=disable"})
			if result.Status != "WAIT" || result.Reason != "pinned_beads_artifact_required" || result.Claims != 1 || result.Writes != 1 || result.HistoryFloor != "1" || result.RestoredIncarnation == "" || result.ReverseIncarnation != result.RestoredIncarnation || result.ReverseAt == "" || result.SourceHistoryComplete || len(result.SourceHistoryGaps) != 2 || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" {
				t.Fatalf("public PostgreSQL boundary: %+v", result)
			}
			encoded, _ := json.Marshal(result)
			t.Logf("PUBLIC_CONVERTER_OUTCOME %s", encoded)
		})
	}
	for _, test := range publicProducerCases() {
		t.Run("producer-"+test.name, func(t *testing.T) {
			database := "plug_roundtrip_producer_" + strings.ReplaceAll(test.name, "-", "_")
			if _, err := db.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
				t.Fatal(err)
			}
			args := []string{"--input", "../../internal/integration/converterroundtrip/testdata/" + test.file, "--output-dir", filepath.Join(root, "producer-"+test.name), "--engine", "postgres", "--postgres-dsn", base + database + "?sslmode=disable"}
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
