package cardb20

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Owned real PostgreSQL fixture retained from the existing B20 journal tests.
func startPostgres(t *testing.T) string {
	t.Helper()
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	name := "plug-swarm-b20-journal-" + hex.EncodeToString(id[:])
	docker := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, entry := range os.Environ() {
			key, _, _ := strings.Cut(entry, "=")
			switch key {
			case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
			default:
				cmd.Env = append(cmd.Env, entry)
			}
		}
		return cmd.CombinedOutput()
	}
	out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_b20", "-p", "127.0.0.1::5432", image)
	if err != nil {
		t.Fatalf("start owned B20 PostgreSQL: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("rm", "-f", name); err != nil {
			t.Errorf("remove owned B20 PostgreSQL: %v %s", err, out)
		}
	})
	out, err = docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("owned B20 PostgreSQL port: %v %s", err, out)
	}
	address := strings.TrimSpace(string(out))
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("owned B20 PostgreSQL did not bind loopback")
	}
	dsn := "postgres://postgres@" + address + "/plug_b20?sslmode=disable"
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
			t.Fatal("owned B20 PostgreSQL did not become ready")
		case <-time.After(30 * time.Millisecond):
		}
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("B20 real engine: %s\nImage: %s\n", version, image)
	return dsn
}
