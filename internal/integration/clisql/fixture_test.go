package clisql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/integration/apikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_HOST=") && !strings.HasPrefix(value, "DOCKER_CONTEXT=") && !strings.HasPrefix(value, "DOCKER_TLS=") && !strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func TestMain(m *testing.M) {
	code := func() int {
		name := fmt.Sprintf("plug-swarm-v3-cli-sql-e2e-%d", time.Now().UnixNano())
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
			fmt.Fprintf(os.Stderr, "PostgreSQL port lookup failed: %v %s\n", err, out)
			return 1
		}
		addr := strings.TrimSpace(string(out))
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "test PostgreSQL must bind loopback")
			return 1
		}
		postgresDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "PostgreSQL SQL open failed: %v\n", err)
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
			fmt.Fprintf(os.Stderr, "PostgreSQL version query failed: %v\n", err)
			return 1
		}
		fmt.Printf("Real engine: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}()
	os.Exit(code)
}

type storeKind struct {
	name string
	open func(context.Context, string) (driver.Store, error)
}

func sqlKinds(t *testing.T) []storeKind {
	t.Helper()
	return []storeKind{
		{
			name: "sqlite",
			open: func(ctx context.Context, dsn string) (driver.Store, error) {
				return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn})
			},
		},
		{
			name: "postgres",
			open: func(ctx context.Context, dsn string) (driver.Store, error) {
				return (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
			},
		},
	}
}

// postgresDatabaseURL returns a fresh database URL while preserving the
// loopback address and sslmode of the TestMain connection.
func postgresDatabaseURL(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	name := "plug_swarm_v3_cli_sql_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err = admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		_ = admin.Close()
	})
	base := strings.TrimPrefix(postgresDSN, "postgres://postgres@")
	addr, _, ok := strings.Cut(base, "/plug_test?")
	if !ok {
		t.Fatal("test PostgreSQL DSN lost its database path")
	}
	return "postgres://postgres@" + addr + "/" + name + "?sslmode=disable"
}

type sqlServer struct {
	kind        string
	dsn         string
	open        func(context.Context, string) (driver.Store, error)
	provider    *authnapikey.Provider
	store       driver.Store
	lease       driver.ActiveLease
	server      *httpjson.Server
	done        chan error
	url         string
	renewCancel context.CancelFunc
	renewDone   chan struct{}
}

func openSQLServer(t *testing.T, kind storeKind, dsn string, provider *authnapikey.Provider) *sqlServer {
	t.Helper()
	f := &sqlServer{kind: kind.name, dsn: dsn, open: kind.open, provider: provider}
	ctx := context.Background()
	store, err := kind.open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	f.store = store
	lease, err := store.Lease(ctx, "cli-sql-e2e", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	f.lease = lease
	handler, err := apikey.New(ctx, apikey.Config{Store: store, Lease: lease, Keys: provider, DefaultProject: "sql-e2e"})
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	f.server = listener
	f.url = "http://" + listener.Addr().String()
	f.done = make(chan error, 1)
	go func() { f.done <- listener.Serve() }()
	renewCtx, renewCancel := context.WithCancel(ctx)
	f.renewCancel, f.renewDone = renewCancel, make(chan struct{})
	go func() {
		defer close(f.renewDone)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-f.lease.Lost():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(renewCtx, time.Second)
				err := f.lease.Renew(renewCtx)
				cancel()
				if err != nil {
					return
				}
			}
		}
	}()
	return f
}

func (f *sqlServer) close(t *testing.T) {
	t.Helper()
	if f.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := f.server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		cancel()
		if err := <-f.done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
		f.server = nil
	}
	if f.renewCancel != nil {
		f.renewCancel()
		<-f.renewDone
		f.renewCancel, f.renewDone = nil, nil
	}
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
		f.store = nil
	}
}

func (f *sqlServer) reader(t *testing.T) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func (f *sqlServer) reopen(t *testing.T) {
	t.Helper()
	if f.server != nil || f.store != nil {
		t.Fatal("reopen requires a closed server")
	}
	newServer := openSQLServer(t, storeKind{name: f.kind, open: f.open}, f.dsn, f.provider)
	f.store, f.lease, f.server, f.done, f.url = newServer.store, newServer.lease, newServer.server, newServer.done, newServer.url
	f.renewCancel, f.renewDone = newServer.renewCancel, newServer.renewDone
}

func sqliteDSN(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "plug.db")
}
