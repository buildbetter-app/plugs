package nightlyrestore_test

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

func randomName() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

// A missing real engine fails this suite. There is no external DSN, skip, or
// memory substitution. Only this test's loopback container is ever removed.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) (code int) {
	name := "plug-swarm-n1-" + randomName()
	if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=nightly", "-p", "127.0.0.1::5432", postgresImage); err != nil {
		fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
		return 1
	}
	defer func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
			code = 1
		}
	}()
	out, err := docker("port", name, "5432/tcp")
	if err != nil {
		fmt.Fprintln(os.Stderr, "PostgreSQL port discovery failed", err)
		return 1
	}
	address := strings.TrimSpace(string(out))
	if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" {
		fmt.Fprintln(os.Stderr, "test PostgreSQL must bind loopback")
		return 1
	}
	postgresDSN = "postgres://postgres@" + address + "/nightly?sslmode=disable"
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		return 1
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "test PostgreSQL never became ready")
			return 1
		case <-time.After(50 * time.Millisecond):
		}
	}
	fmt.Println("N1 disposable real PostgreSQL image:", postgresImage)
	return m.Run()
}

func freshStore(t *testing.T, kind string) (driver.Store, func() driver.Store) {
	t.Helper()
	var open func() (driver.Store, error)
	switch kind {
	case "memory":
		open = func() (driver.Store, error) { return memory.New(), nil }
	case "sqlite":
		path := filepath.Join(t.TempDir(), "fresh.db")
		open = func() (driver.Store, error) {
			return (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
		}
	case "postgres":
		admin, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			t.Fatal(err)
		}
		name := "n1_" + randomName()
		if _, err := admin.Exec(`CREATE DATABASE "` + name + `"`); err != nil {
			admin.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := admin.Exec(`DROP DATABASE "` + name + `" WITH (FORCE)`); err != nil {
				t.Error("owned database cleanup", err)
			}
			admin.Close()
		})
		dsn, _ := url.Parse(postgresDSN)
		dsn.Path = "/" + name
		open = func() (driver.Store, error) {
			return postgres.Open(context.Background(), driver.Config{DSN: dsn.String()})
		}
	default:
		t.Fatalf("unknown store %q", kind)
	}
	opener := func() driver.Store {
		store, err := open()
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
	return opener(), opener
}
