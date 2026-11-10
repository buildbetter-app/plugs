package g1exactstatus_test

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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

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
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
		default:
			cmd.Env = append(cmd.Env, entry)
		}
	}
	return cmd.CombinedOutput()
}

// Both engines are mandatory. No external database, skip or substitute store
// can turn a missing PostgreSQL engine into an acceptance pass.
func TestMain(m *testing.M) { os.Exit(runWithPostgres(m)) }

func runWithPostgres(m *testing.M) (code int) {
	name := "plug-swarm-g1-exact-status-" + nonce()
	out, err := docker("run", "-d", "--rm", "--name", name,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test",
		"-p", "127.0.0.1::5432", postgresImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start disposable PostgreSQL: %v %s\n", err, out)
		return 1
	}
	defer func() {
		if out, err := docker("rm", "-f", name); err != nil {
			fmt.Fprintf(os.Stderr, "remove owned PostgreSQL: %v %s\n", err, out)
			code = 1
		}
	}()
	out, err = docker("port", name, "5432/tcp")
	if err != nil {
		fmt.Fprintln(os.Stderr, "resolve disposable PostgreSQL port:", err)
		return 1
	}
	address := strings.TrimSpace(string(out))
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		fmt.Fprintln(os.Stderr, "disposable PostgreSQL must bind loopback")
		return 1
	}
	postgresDSN = "postgres://postgres@" + address + "/plug_test?sslmode=disable"
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open disposable PostgreSQL:", err)
		return 1
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "disposable PostgreSQL did not become ready")
			return 1
		case <-time.After(30 * time.Millisecond):
		}
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		fmt.Fprintln(os.Stderr, "read disposable PostgreSQL version:", err)
		return 1
	}
	fmt.Printf("Real engine: %s\nImage: %s\n", version, postgresImage)
	return m.Run()
}

type engine struct {
	name, sqlDriver, dsn string
	driver               driver.Driver
}

func engines(t *testing.T, run func(*testing.T, engine)) {
	t.Helper()
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			e := engine{name: name, sqlDriver: "sqlite", dsn: filepath.Join(t.TempDir(), "g1.db"), driver: sqlite.Driver{}}
			if name == "memory" {
				e.driver = memory.Driver{}
				e.dsn = ""
			}
			if name == "postgres" {
				admin, err := sql.Open("pgx", postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				database := "g1_exact_status_" + nonce()
				if _, err := admin.Exec(`CREATE DATABASE "` + database + `"`); err != nil {
					_ = admin.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if _, err := admin.ExecContext(ctx, `DROP DATABASE "`+database+`" WITH (FORCE)`); err != nil {
						t.Error("drop owned database:", err)
					}
					_ = admin.Close()
				})
				dsn, err := url.Parse(postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				dsn.Path = "/" + database
				e = engine{name: name, sqlDriver: "pgx", dsn: dsn.String(), driver: postgres.Driver{}}
			}
			run(t, e)
		})
	}
}
