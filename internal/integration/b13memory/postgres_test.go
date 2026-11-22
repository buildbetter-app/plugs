package b13memory_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Same immutable image as the actual PostgreSQL driver/acceptance fixtures.
const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		switch name {
		case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
		default:
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

// No external DSN, substitute driver or missing-engine skip is admitted.
// The engine is started only by the PostgreSQL subtest, never by source checks.
func ownedPostgres(t *testing.T) string {
	t.Helper()
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	name := "plug-b13-public-" + hex.EncodeToString(random[:])
	out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_b13",
		"-p", "127.0.0.1::5432", postgresImage)
	if err != nil {
		t.Fatalf("owned PostgreSQL start failed: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup failed: %v %s", err, out)
		}
	})
	out, err = docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("owned PostgreSQL address failed: %v %s", err, out)
	}
	address := strings.TrimSpace(string(out))
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("owned PostgreSQL must bind only loopback: %q %v", address, err)
	}
	dsn := "postgres://postgres@" + address + "/plug_b13?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			t.Fatal("owned PostgreSQL did not become ready")
		case <-time.After(30 * time.Millisecond):
		}
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("B13 real engine=%s image=%s", version, postgresImage)
	return dsn
}
