package b07public

import (
	"context"
	"database/sql"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	_ "github.com/buildbetter-app/plugs/internal/storage/postgres"
)

// Postgres owns a fresh pinned public-fixture engine. Startup and cleanup are
// mandatory; neither external DSNs nor opt-in skips are accepted.
func Postgres(t *testing.T) string {
	t.Helper()
	docker := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, value := range os.Environ() {
			name, _, _ := strings.Cut(value, "=")
			switch name {
			case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
			default:
				command.Env = append(command.Env, value)
			}
		}
		return command.CombinedOutput()
	}
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	name := "plug-swarm-b07-positive-" + proof.Hash([]byte(t.TempDir()))[:16]
	if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=positive", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("owned PostgreSQL startup: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	out, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := strings.TrimSpace(string(out))
	if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" {
		t.Fatal("PostgreSQL fixture is not on loopback")
	}
	dsn := "postgres://postgres@" + address + "/positive?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		if ctx.Err() != nil {
			t.Fatal("owned PostgreSQL readiness failed")
		}
		time.Sleep(30 * time.Millisecond)
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	proof.Save(t, proof.Output(t), "postgres.json", map[string]any{"image": image, "version": version, "container": name, "address": address})
	return dsn
}
