package b02publicnotices_test

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
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

func uniqueID() string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
		default:
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

// An unavailable engine fails the target. There is no external DSN, skip, or
// substitute store. The container and every database belong only to this test.
func TestMain(m *testing.M) {
	os.Exit(func() (code int) {
		name := "plug-swarm-b02-public-notices-" + uniqueID()
		out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
				code = 1
			}
		}()
		out, err = docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		address := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" {
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
			time.Sleep(30 * time.Millisecond)
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

type engine struct {
	name, sqlDriver, dsn string
	storage              driver.Driver
}

func eachEngine(t *testing.T, run func(*testing.T, engine)) {
	t.Helper()
	for _, name := range []string{"sqlite_wal", "postgres"} {
		t.Run(name, func(t *testing.T) {
			e := engine{name: name, sqlDriver: "sqlite", dsn: filepath.Join(t.TempDir(), "b02.db"), storage: sqlite.Driver{}}
			if name == "postgres" {
				admin, err := sql.Open("pgx", postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				database := "b02_public_" + uniqueID()
				if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`); err != nil {
					_ = admin.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`)
					if err != nil {
						t.Error(err)
					}
					_ = admin.Close()
				})
				dsn, err := url.Parse(postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				dsn.Path = "/" + database
				e = engine{name: name, sqlDriver: "pgx", dsn: dsn.String(), storage: postgres.Driver{}}
			}
			run(t, e)
		})
	}
}

func (e engine) db(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(e.sqlDriver, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// These are the existing SQL graph/history/receipt payload families used by
// auditsql. Coordination-only PostgreSQL lock placeholders are separate.
func (e engine) payloadCounts(t *testing.T, ctx context.Context) map[string]int {
	t.Helper()
	tables := []string{"objects", "records", "versions", "key_history", "blobs", "changesets", "replays", "checkpoints"}
	if e.name == "postgres" {
		tables = []string{"plug_postgres.records", "plug_postgres.versions", "plug_postgres.states", "plug_postgres.blobs", "plug_postgres.changesets", "plug_postgres.receipts", "plug_postgres.checkpoints"}
	}
	db := e.db(t)
	defer db.Close()
	counts := map[string]int{}
	for _, table := range tables {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}
