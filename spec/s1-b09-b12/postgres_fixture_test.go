package s1b09b12

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

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

const b09b12PostgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var b09b12PostgresDSN string

// The B11 accepted replay target owns a real disposable PostgreSQL engine.
// Setup failure fails the target; it cannot become a SQLite or memory result.
func TestMain(m *testing.M) {
	os.Exit(func() (code int) {
		name := "plug-s1-b09-b12-" + b09b12RandomID()
		out, err := b09b12Docker("run", "-d", "--rm", "--name", name,
			"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test",
			"-p", "127.0.0.1::5432", b09b12PostgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			out, err := b09b12Docker("rm", "-f", name)
			removed := strings.TrimSpace(string(out))
			if err != nil || removed == "" {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
				code = 1
				return
			}
			fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup succeeded: %s\n", removed)
		}()
		out, err = b09b12Docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintf(os.Stderr, "PostgreSQL port lookup failed: %v %s\n", err, out)
			return 1
		}
		address := strings.TrimSpace(string(out))
		host, _, splitErr := net.SplitHostPort(address)
		if splitErr != nil || host != "127.0.0.1" {
			fmt.Fprintf(os.Stderr, "PostgreSQL must bind loopback: %q (%v)\n", address, splitErr)
			return 1
		}
		b09b12PostgresDSN = "postgres://postgres@" + address + "/plug_test?sslmode=disable"
		admin, err := sql.Open("pgx", b09b12PostgresDSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "PostgreSQL open failed: %v\n", err)
			return 1
		}
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		for admin.PingContext(ctx) != nil {
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "PostgreSQL did not become ready")
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		var version string
		if err := admin.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			fmt.Fprintf(os.Stderr, "PostgreSQL version query failed: %v\n", err)
			return 1
		}
		fmt.Printf("Real PostgreSQL: %s\nImage: %s\n", version, b09b12PostgresImage)
		code = m.Run()
		return code
	}())
}

func b09b12RandomID() string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func b09b12Docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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

func openPostgres(t *testing.T) opened {
	t.Helper()
	admin, err := sql.Open("pgx", b09b12PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	database := "s1_b09_b12_" + b09b12RandomID()
	if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	dsn, err := url.Parse(b09b12PostgresDSN)
	if err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	dsn.Path = "/" + database
	store, err := postgres.Open(context.Background(), driver.Config{DSN: dsn.String()})
	if err != nil {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`)
		_ = admin.Close()
		t.Fatal(err)
	}
	lease, err := store.Lease(context.Background(), "s1-b09-b12", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`)
		_ = admin.Close()
		t.Fatal(err)
	}
	return opened{store: store, lease: lease, admin: admin, database: database}
}
