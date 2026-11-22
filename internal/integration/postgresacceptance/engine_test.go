package postgresacceptance

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
)

// Match the admitted driver's engine, without accepting an external DSN or a
// substitute store. Missing Docker/engine access is a failure, never a skip.
const engineImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var engineDSN string

func uniqueID() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
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

func TestMain(m *testing.M) {
	os.Exit(runWithEngine(m))
}

func runWithEngine(m *testing.M) (code int) {
	id, err := uniqueID()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	name := "plug-swarm-v3-accept-postgres-" + id
	out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_acceptance",
		"-p", "127.0.0.1::5432", engineImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PostgreSQL acceptance engine failed to start: %v %s\n", err, out)
		return 1
	}
	defer func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			fmt.Fprintf(os.Stderr, "owned engine cleanup failed: %v %s\n", err, out)
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
		fmt.Fprintln(os.Stderr, "acceptance engine did not bind only to loopback")
		return 1
	}
	engineDSN = "postgres://postgres@" + address + "/plug_acceptance?sslmode=disable"
	db, err := sql.Open("pgx", engineDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "acceptance PostgreSQL never became ready")
			return 1
		case <-time.After(30 * time.Millisecond):
		}
	}
	var version string
	if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("Real acceptance engine: %s\nImage: %s\n", version, engineImage)
	return m.Run()
}

func freshDatabase(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", engineDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	id, err := uniqueID()
	if err != nil {
		t.Fatal(err)
	}
	name := "plug_v3_accept_" + id
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = admin.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
	})
	dsn, err := url.Parse(engineDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + name
	return dsn.String()
}
