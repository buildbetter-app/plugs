package b03catalog_test

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

// An unavailable real engine fails the target. No external database, skip,
// cached result, or memory substitute supplies PostgreSQL evidence.
func TestMain(m *testing.M) {
	os.Exit(func() int {
		name := "plug-b03-catalog-" + randomID()
		out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "owned PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", name); err != nil {
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
			fmt.Fprintf(os.Stderr, "owned PostgreSQL must bind loopback: %q\n", address)
			return 1
		}
		postgresDSN = "postgres://postgres@" + address + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		for db.PingContext(ctx) != nil {
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "owned PostgreSQL did not become ready")
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

type engine struct {
	name, sqlDriver, dsn string
	storage              driver.Driver
}

func eachEngine(t *testing.T, names []string, run func(*testing.T, engine)) {
	t.Helper()
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			e := engine{name: name, storage: memory.Driver{}}
			switch name {
			case "memory":
			case "sqlite":
				e.storage, e.sqlDriver, e.dsn = sqlite.Driver{}, "sqlite", filepath.Join(t.TempDir(), "catalog.db")
			case "postgres":
				admin, err := sql.Open("pgx", postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				database := "b03_catalog_" + randomID()
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
				parsed, err := url.Parse(postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				parsed.Path = "/" + database
				e.storage, e.sqlDriver, e.dsn = postgres.Driver{}, "pgx", parsed.String()
			default:
				t.Fatalf("unknown engine %q", name)
			}
			run(t, e)
		})
	}
}

// The fault is in the actual SQL publication transaction, after the protected
// writer has constructed source and row post-images. Removal permits an exact
// same-key retry; no replacement writer supplies the rollback evidence.
func (e engine) publicationFault(t *testing.T, install bool) {
	t.Helper()
	db, err := sql.Open(e.sqlDriver, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statement := `DROP TRIGGER IF EXISTS b03_catalog_fault`
	if install {
		statement = `CREATE TRIGGER b03_catalog_fault BEFORE INSERT ON changesets
BEGIN SELECT RAISE(ABORT,'b03 catalog publication refused'); END`
	}
	if e.name == "postgres" {
		statement = `DROP TRIGGER IF EXISTS b03_catalog_fault ON plug_postgres.changesets; DROP FUNCTION IF EXISTS plug_postgres.b03_catalog_fault()`
		if install {
			statement = `CREATE FUNCTION plug_postgres.b03_catalog_fault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'b03 catalog publication refused'; END$$;
CREATE TRIGGER b03_catalog_fault BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.b03_catalog_fault()`
		}
	}
	if _, err := db.ExecContext(context.Background(), statement); err != nil {
		t.Fatal(err)
	}
}
