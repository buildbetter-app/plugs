package b04claimidentity_test

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
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This is the same pinned real engine used by the existing claim SQL target.
// Failure to start it fails this target; no external DSN or substitute is used.
const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

func randomID() string {
	var value [16]byte
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

func TestMain(m *testing.M) {
	os.Exit(func() (code int) {
		name := "plug-swarm-b04-identity-" + randomID()
		out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", name); err != nil {
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
	name, dsn string
	storage   driver.Driver
}

func eachStore(t *testing.T, sqlOnly bool, run func(*testing.T, engine)) {
	t.Helper()
	names := []string{"memory", "sqlite_wal", "postgres"}
	if sqlOnly {
		names = names[1:]
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			e := engine{name: name, storage: memory.Driver{}}
			if name == "sqlite_wal" {
				e.storage, e.dsn = sqlite.Driver{}, filepath.Join(t.TempDir(), "b04.db")
			}
			if name == "postgres" {
				admin, err := sql.Open("pgx", postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				database := "b04_identity_" + randomID()
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
				e.storage, e.dsn = postgres.Driver{}, dsn.String()
			}
			run(t, e)
		})
	}
}

func (e engine) db(t *testing.T) *sql.DB {
	t.Helper()
	sqlDriver := "sqlite"
	if e.name == "postgres" {
		sqlDriver = "pgx"
	}
	db, err := sql.Open(sqlDriver, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// A real SQL trigger fails publication, after the production transaction has
// staged the claim. Removing it permits retry through the same real Writer.
func (e engine) publicationFault(t *testing.T, install bool) {
	t.Helper()
	statement := `DROP TRIGGER IF EXISTS b04_claim_publication_fault`
	if install {
		statement = `CREATE TRIGGER b04_claim_publication_fault BEFORE INSERT ON changesets
BEGIN SELECT RAISE(ABORT,'b04 claim publication rollback'); END`
	}
	if e.name == "postgres" {
		statement = `DROP TRIGGER IF EXISTS b04_claim_publication_fault ON plug_postgres.changesets; DROP FUNCTION IF EXISTS plug_postgres.b04_claim_publication_fault()`
		if install {
			statement = `CREATE FUNCTION plug_postgres.b04_claim_publication_fault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'b04 claim publication rollback'; END$$;
CREATE TRIGGER b04_claim_publication_fault BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.b04_claim_publication_fault()`
		}
	}
	db := e.db(t)
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), statement); err != nil {
		t.Fatal(err)
	}
}
