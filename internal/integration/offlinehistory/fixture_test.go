package offlinehistory_test

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

const pgImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

// TestMain owns one disposable local PostgreSQL engine. Every test gets a
// fresh database, so a failure cannot make a later case observe old history.
func TestMain(m *testing.M) {
	code := func() int {
		id, err := randomID()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-offline-history-" + id
		out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name,
			"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test",
			"-p", "127.0.0.1::5432", pgImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "PostgreSQL cleanup failed: %v %s\n", err, out)
			}
		}()
		out, err = docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		addr := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(addr)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "PostgreSQL test engine must bind loopback")
			return 1
		}
		postgresDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for {
			if err = db.PingContext(ctx); err == nil {
				break
			}
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "PostgreSQL did not become ready")
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		var version string
		if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("Real PostgreSQL: %s\nImage: %s\n", version, pgImage)
		return m.Run()
	}()
	os.Exit(code)
}

func randomID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_HOST=") && !strings.HasPrefix(value, "DOCKER_CONTEXT=") &&
			!strings.HasPrefix(value, "DOCKER_TLS=") && !strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") &&
			!strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
			command.Env = append(command.Env, value)
		}
	}
	return command.CombinedOutput()
}

func newPostgresDatabase(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	id, err := randomID()
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	name := "plug_offline_history_" + id
	if _, err = admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Errorf("drop PostgreSQL database %s: %v", name, err)
		}
		_ = admin.Close()
	})
	dsn, err := url.Parse(postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + name
	return dsn.String()
}

func openStore(t *testing.T, name string) driver.Store {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	switch name {
	case "memory":
		store = memory.New()
	case "sqlite":
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "store.db")})
	case "postgres":
		store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: newPostgresDatabase(t)})
	default:
		t.Fatal("unknown engine")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}
