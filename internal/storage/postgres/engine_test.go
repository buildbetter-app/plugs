package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const testImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var testDSN string

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "DOCKER_HOST=") && !strings.HasPrefix(v, "DOCKER_CONTEXT=") && !strings.HasPrefix(v, "DOCKER_TLS=") && !strings.HasPrefix(v, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(v, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	return cmd.CombinedOutput()
}

// Tests always own their disposable engine. There is no external-service DSN,
// memory substitute, or absent-engine skip path.
func TestMain(m *testing.M) {
	code := func() int {
		id, err := identity()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-v2-postgres-test-" + id
		out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", testImage)
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
		addr := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(addr)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "test engine must bind loopback")
			return 1
		}
		testDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", testDSN)
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
				fmt.Fprintln(os.Stderr, "test PostgreSQL never became ready")
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		var version string
		if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("Real engine: %s\nImage: %s\n", version, testImage)
		return m.Run()
	}()
	os.Exit(code)
}

func freshDSN(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity()
	if err != nil {
		t.Fatal(err)
	}
	name := "plug_swarm_v2_postgres_" + id
	if _, err = admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`)
		if err != nil {
			t.Error(err)
		}
		_ = admin.Close()
	})
	dsn, err := url.Parse(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + name
	return dsn.String()
}

func TestMetadataAndIndependentHandles(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	a, err := Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ma, err := a.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mb, err := b.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ma != mb || ma.StoreID == "" || ma.Incarnation == "" || !ma.Head.IsZero() {
		t.Fatalf("persistent metadata differs: %+v %+v", ma, mb)
	}
	old, err := a.Lease(ctx, "old", 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	<-old.Lost()
	next, err := b.Lease(ctx, "next", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release(ctx)
	if next.Epoch() <= old.Epoch() {
		t.Fatal("independent handle reused epoch")
	}
	if err = old.Renew(ctx); err == nil {
		t.Fatal("expired holder renewed")
	}
}

func TestCoreSessionPersistenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	value, err := Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	store := driver.CoreSessionStore(value)
	now := time.Unix(1_800_000_000, 123)
	record := driver.CoreSessionRecord{
		ID: "0123456789abcdef0123456789abcdef", TokenDigest: bytes.Repeat([]byte{0x42}, 32),
		Principal: model.PrincipalRef{Kind: "user", ID: "user:postgres"}, ExpiresAt: now.Add(time.Hour),
	}
	if err := store.PutCoreSession(ctx, record, now, 4); err != nil {
		t.Fatal(err)
	}
	active, err := store.LookupCoreSession(ctx, record.ID)
	if err != nil || active.Revoked || active.Principal.Kind != "user" || active.Principal.ID != record.Principal.ID {
		t.Fatalf("active core session=%+v err=%v", active, err)
	}
	if revoked, err := store.RevokeCoreSession(ctx, record.ID); err != nil || !revoked {
		t.Fatalf("revoke=%t err=%v", revoked, err)
	}
	if err := value.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	revoked, err := driver.CoreSessionStore(reopened).LookupCoreSession(ctx, record.ID)
	if err != nil || !revoked.Revoked || revoked.Principal.Kind != "user" {
		t.Fatalf("reopened revoked core session=%+v err=%v", revoked, err)
	}
	bad := record
	bad.ID = "fedcba9876543210fedcba9876543210"
	bad.Principal = model.PrincipalRef{Kind: "agent", ID: "agent:must-not-be-human"}
	if err := driver.CoreSessionStore(reopened).PutCoreSession(ctx, bad, now, 4); err == nil {
		t.Fatal("non-user core session was accepted")
	}
}
