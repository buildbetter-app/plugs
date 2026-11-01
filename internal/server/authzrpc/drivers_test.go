package authzrpc_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type storeFactory func(*testing.T) driver.Store

// These tests use actual drivers and the authenticated HTTP handler. PostgreSQL
// is always an owned disposable engine on the runner's local Docker socket;
// no caller-supplied DSN or Docker context can select an external service.
func TestFixedPermissionsAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var open storeFactory
			switch engine {
			case "memory":
				open = func(*testing.T) driver.Store { return memory.New() }
			case "sqlite":
				open = func(t *testing.T) driver.Store {
					s, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "authz.db")})
					if err != nil {
						t.Fatal(err)
					}
					return s
				}
			case "postgres":
				open = postgresFactory(t)
			}
			t.Run("immutable_registry", func(t *testing.T) { immutableRegistry(t, open) })
		})
	}
}

func immutableRegistry(t *testing.T, open storeFactory) {
	for _, change := range []string{"rename", "remap", "delete", "authority"} {
		t.Run(change, func(t *testing.T) {
			f := newFixtureOn(t, open(t), nil)
			next := f.base
			next.Projects = append([]authzfixed.Project(nil), f.base.Projects...)
			switch change {
			case "rename":
				next.Projects[0].Name = "renamed"
			case "remap":
				next.Projects[0].Ref = model.RefByID("replacement", model.Natural64(0))
				// Keep the candidate internally valid: the old team edge would
				// otherwise fail validation before reaching the immutable registry.
				next.Grants = nil
			case "delete":
				next.Projects, next.Grants = nil, nil
			case "authority":
				next.Namespace.Authority = model.RefByID("replacement", model.Natural64(0))
			}
			before := *f.reader(t).Snapshot().At
			if err := f.facts.Replace(next); err == nil {
				t.Errorf("installed source accepted forbidden registry/authority %s", change)
			}
			if got := f.reader(t).Snapshot().At; got.String() != before.String() {
				t.Errorf("rejected registry change advanced head: %s -> %s", before, got)
			}
			got, err := f.facts.Load(context.Background(), f.reader(t))
			if err != nil || !reflect.DeepEqual(got, f.base) {
				t.Errorf("rejected registry change replaced the installed facts: %v", err)
			}
			// Refusal leaves the previously authorized runtime usable.
			success(t, f.call(t, worker("user:operator"), "bead.update", map[string]any{
				"id": model.RefByID(beadID, model.Natural64(0)), "fields": map[string]any{"title": "still authorized"},
			}, 100))
		})
	}
}

func localDocker(args ...string) ([]byte, error) {
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

func postgresFactory(t *testing.T) storeFactory {
	t.Helper()
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	name := fmt.Sprintf("plug-swarm-authz-i5-%d-%d", os.Getpid(), time.Now().UnixNano())
	if out, err := localDocker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("owned PostgreSQL start: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := localDocker("rm", "-f", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	out, err := localDocker("port", name, "5432/tcp")
	address := strings.TrimSpace(string(out))
	host, _, parseErr := net.SplitHostPort(address)
	if err != nil || parseErr != nil || host != "127.0.0.1" {
		t.Fatalf("PostgreSQL requires a single loopback binding: %q %v", address, err)
	}
	deadline := time.Now().Add(40 * time.Second)
	for {
		if _, err := localDocker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned PostgreSQL readiness timed out")
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Logf("real PostgreSQL image=%s loopback_only=true owned_cleanup=true", image)
	var sequence atomic.Uint64
	return func(t *testing.T) driver.Store {
		t.Helper()
		database := fmt.Sprintf("authz_%d", sequence.Add(1))
		if out, err := localDocker("exec", name, "createdb", "-h", "127.0.0.1", "-U", "postgres", database); err != nil {
			t.Fatalf("owned test database: %v %s", err, out)
		}
		s, err := postgres.Open(context.Background(), driver.Config{DSN: "postgres://postgres@" + address + "/" + database + "?sslmode=disable"})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}
