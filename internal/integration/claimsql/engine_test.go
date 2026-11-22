package claimsql_test

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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

func randomID() string {
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

// The target owns one disposable real PostgreSQL engine. An unavailable
// engine is a test failure; there is no external DSN or substitute path.
func TestMain(m *testing.M) {
	os.Exit(func() int {
		name := "plug-swarm-v3-claim-sql-" + randomID()
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
			// The claim clock below is fully injected; readiness polling must not
			// use a wall-clock sleep as part of the expiry evidence.
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

type engine struct {
	name, sqlDriver, dsn string
	storage              driver.Driver
}

func eachEngine(t *testing.T, run func(*testing.T, engine)) {
	t.Helper()
	for _, name := range []string{"sqlite_wal", "postgres"} {
		t.Run(name, func(t *testing.T) {
			e := engine{name: name, sqlDriver: "sqlite", dsn: filepath.Join(t.TempDir(), "claim.db"), storage: sqlite.Driver{}}
			if name == "postgres" {
				admin, err := sql.Open("pgx", postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				database := "claim_sql_" + randomID()
				if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`); err != nil {
					_ = admin.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if _, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`); err != nil {
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
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Install a database-side failure at the same changeset publication boundary
// for both engines. It is removed before the retry and never replaces the
// production writer or its transaction.
func (e engine) recoveryFault(t *testing.T, install bool) {
	t.Helper()
	statement := `DROP TRIGGER IF EXISTS claimsql_recovery_fault`
	if install {
		statement = `CREATE TRIGGER claimsql_recovery_fault BEFORE INSERT ON changesets
BEGIN SELECT RAISE(ABORT,'claimsql recovery rollback'); END`
	}
	if e.name == "postgres" {
		statement = `DROP TRIGGER IF EXISTS claimsql_recovery_fault ON plug_postgres.changesets; DROP FUNCTION IF EXISTS plug_postgres.claimsql_recovery_fault()`
		if install {
			statement = `CREATE FUNCTION plug_postgres.claimsql_recovery_fault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'claimsql recovery rollback'; END$$;
CREATE TRIGGER claimsql_recovery_fault BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.claimsql_recovery_fault()`
		}
	}
	db := e.db(t)
	if _, err := db.ExecContext(context.Background(), statement); err != nil {
		t.Fatal(err)
	}
}
