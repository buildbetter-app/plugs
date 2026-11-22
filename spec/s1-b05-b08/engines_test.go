package s1b05b08

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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

var testEngines = []string{"memory", "sqlite", "postgres"}
var postgresDSN string

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

func randomDatabaseID() string {
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(id[:])
}

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

// PostgreSQL is mandatory, disposable and real. Engine/setup failure fails the
// target; there is no opt-in skip, external DSN, or replacement store.
func TestMain(m *testing.M) {
	os.Exit(func() int {
		name := "plug-swarm-s1-b05-b08-" + randomDatabaseID()
		out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
			}
		}()
		out, err = docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		address := strings.TrimSpace(string(out))
		if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "test PostgreSQL must bind loopback")
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
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "test PostgreSQL did not become ready")
				return 1
			}
			runtime.Gosched()
		}
		var version string
		if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("Real engine: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}())
}

func postgresFixture(t *testing.T) (driver.Driver, string) {
	t.Helper()
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	database := "s1_b05_b08_" + randomDatabaseID()
	if _, err := db.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	dsn, err := url.Parse(postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + database
	return postgres.Driver{}, dsn.String()
}
