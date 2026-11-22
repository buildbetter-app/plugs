package s1relations_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The digest is the same disposable engine used by the existing SQL gates.
const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

func nonce() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
		default:
			cmd.Env = append(cmd.Env, entry)
		}
	}
	return cmd.CombinedOutput()
}

func TestMain(m *testing.M) { os.Exit(withPostgres(m)) }

// A missing engine fails this target. An external DSN or a skip cannot supply
// the PostgreSQL member of the required store matrix.
func withPostgres(m *testing.M) (code int) {
	name := "plug-swarm-s1-relations-" + nonce()
	out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test",
		"-p", "127.0.0.1::5432", postgresImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start owned PostgreSQL: %v %s\n", err, out)
		return 1
	}
	defer func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			fmt.Fprintf(os.Stderr, "remove owned PostgreSQL: %v %s\n", err, out)
			code = 1
		}
	}()
	out, err = docker("port", name, "5432/tcp")
	address := strings.TrimSpace(string(out))
	host, _, addressErr := net.SplitHostPort(address)
	if err != nil || addressErr != nil || host != "127.0.0.1" {
		fmt.Fprintln(os.Stderr, "owned PostgreSQL requires a loopback port", err, addressErr)
		return 1
	}
	postgresDSN = "postgres://postgres@" + address + "/plug_test?sslmode=disable"
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "owned PostgreSQL did not become ready")
			return 1
		case <-time.After(30 * time.Millisecond):
		}
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("Required PostgreSQL engine: %s\nImage: %s\n", version, postgresImage)
	return m.Run()
}

func freshDatabase(t *testing.T) string {
	t.Helper()
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	name := "s1_relations_" + nonce()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	dsn, err := url.Parse(postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + name
	return dsn.String()
}

func stores(t *testing.T, run func(*testing.T, string)) {
	t.Helper()
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) { run(t, kind) })
	}
}
