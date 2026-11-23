package testclock

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// PostgresDSN starts only an owned loopback PostgreSQL on the test runner.
// It never accepts an external DSN and never skips an unavailable real engine.
func PostgresDSN(t testing.TB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	dockerWithContext := func(commandContext context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(commandContext, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, v := range os.Environ() {
			key, _, _ := strings.Cut(v, "=")
			switch key {
			case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
			default:
				cmd.Env = append(cmd.Env, v)
			}
		}
		return cmd.CombinedOutput()
	}
	docker := func(args ...string) ([]byte, error) { return dockerWithContext(ctx, args...) }
	name := fmt.Sprintf("plug-swarm-s1-clock-%d-%d", os.Getpid(), time.Now().UnixNano())
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("PostgreSQL start: %v %s", err, out)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 45*time.Second)
		defer done()
		if out, err := exec.CommandContext(cleanup, "docker", "--host=unix:///var/run/docker.sock", "rm", "-f", "-v", name).CombinedOutput(); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	out, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("PostgreSQL port: %v %s", err, out)
	}
	address := strings.TrimSpace(string(out))
	if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" {
		t.Fatalf("PostgreSQL must bind loopback: %q", address)
	}
	dsn := "postgres://postgres@" + address + "/plug_test?sslmode=disable"
	// Keep driver-level connection attempts bounded as well as the overall
	// fixture startup. A context deadline alone cannot bound every network
	// operation in older database/sql/pgx combinations.
	dsn += "&connect_timeout=2"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var lastPingErr error
	var lastReadinessErr error
	var lastReadinessOutput string
	for {
		probeCtx, probeCancel := context.WithTimeout(ctx, 2*time.Second)
		out, err := dockerWithContext(probeCtx, "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "plug_test")
		probeCancel()
		if err != nil {
			lastReadinessErr = err
			lastReadinessOutput = strings.TrimSpace(string(out))
			inspectCtx, inspectCancel := context.WithTimeout(ctx, 2*time.Second)
			state, stateErr := dockerWithContext(inspectCtx, "inspect", "--format", "{{.State.Status}}|{{.State.ExitCode}}", name)
			inspectCancel()
			stateText := strings.TrimSpace(string(state))
			if stateErr == nil && (strings.HasPrefix(stateText, "exited|") || strings.HasPrefix(stateText, "dead|")) {
				t.Fatalf("owned PostgreSQL exited before readiness: %s", stateText)
			}
		} else {
			lastReadinessErr = nil
			lastReadinessOutput = ""
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			lastPingErr = db.PingContext(pingCtx)
			pingCancel()
			if lastPingErr == nil {
				break
			}
		}
		if ctx.Err() != nil {
			if lastPingErr != nil {
				t.Fatalf("owned PostgreSQL did not become ready: ping: %v", lastPingErr)
			}
			t.Fatalf("owned PostgreSQL did not become ready: pg_isready: %v %s", lastReadinessErr, lastReadinessOutput)
		}
		time.Sleep(25 * time.Millisecond)
	}
	var version string
	if err := db.QueryRowContext(ctx, `SELECT version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("owned real engine=%s image=%s loopback_only=true", version, image)
	return dsn
}
