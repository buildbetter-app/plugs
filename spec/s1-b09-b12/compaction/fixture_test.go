package compaction_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

// Every SQL case uses an actual disposable engine. A missing engine fails the
// target; it cannot turn an unexecuted storage path into a card PASS.
func TestMain(m *testing.M) {
	code := func() int {
		name := fmt.Sprintf("plug-swarm-s1-b12-%d", os.Getpid())
		out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup: %v %s\n", err, out)
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
			fmt.Fprintln(os.Stderr, "test PostgreSQL must bind loopback")
			return 1
		}
		postgresDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
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
		fmt.Printf("Real PostgreSQL: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}()
	os.Exit(code)
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

type fixture struct {
	t       *testing.T
	engine  string
	dsn     string
	store   driver.Store
	lease   driver.ActiveLease
	domain  *service.Service
	server  *rpc.Server
	clock   time.Time
	ids     int
	options []service.Option
	trace   [][]byte
}

func newFixture(t *testing.T, engine string, options ...service.Option) *fixture {
	t.Helper()
	f := &fixture{t: t, engine: engine, clock: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), options: options}
	switch engine {
	case "sqlite":
		f.dsn = filepath.Join(t.TempDir(), "b12.db")
	case "postgres":
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(t.Name()))
		name := "b12_" + hex.EncodeToString(sum[:12])
		if _, err = db.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := db.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
				t.Error(err)
			}
			_ = db.Close()
		})
		u, err := url.Parse(postgresDSN)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
		f.dsn = u.String()
	}
	f.open()
	t.Cleanup(f.close)
	return f
}

func (f *fixture) open() {
	f.t.Helper()
	ctx := context.Background()
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: func() time.Time { return f.clock }, StoreID: "b12-memory", Incarnation: "b12-memory-incarnation"})
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatalf("unknown engine %s", f.engine)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(ctx, "b12-recorder", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	o := service.NewOntology(nil)
	w := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.clock }), pipeline.WithIDGenerator(func() (string, error) {
		f.ids++
		return fmt.Sprintf("b12-%04d", f.ids), nil
	}))
	opts := append([]service.Option{service.WithOntology(o), service.WithClock(func() time.Time { return f.clock })}, f.options...)
	f.domain = service.New(f.store, f.lease, w, opts...)
	r, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(r, f.domain); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(r, rpc.Config{DefaultProject: "b12", AuthMode: "anonymous", Incarnation: meta.Incarnation, CursorKey: []byte("b12-replay-fixed-cursor-signing-key")})
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) close() {
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) call(method, key string, params map[string]any, code string) map[string]any {
	f.t.Helper()
	p := map[string]any{}
	for k, v := range params {
		p[k] = v
	}
	sum := sha256.Sum256([]byte(key))
	p["_meta"] = map[string]any{"project": "b12", "agent": "b12-public-recorder", "idempotencyKey": hex.EncodeToString(sum[:])}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": method, "params": p})
	if err != nil {
		f.t.Fatal(err)
	}
	raw := f.server.Dispatch(context.Background(), request)
	f.trace = append(f.trace, request, raw)
	var response map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err = d.Decode(&response); err != nil {
		f.t.Fatalf("%s invalid response: %s: %v", method, raw, err)
	}
	if code != "" {
		failure, ok := response["error"].(map[string]any)
		if !ok {
			f.t.Fatalf("%s succeeded; wanted %s: %s", method, code, raw)
		}
		data, _ := failure["data"].(map[string]any)
		if data["code"] != code {
			f.t.Fatalf("%s code=%v; wanted %s: %s", method, data["code"], code, raw)
		}
		return response
	}
	if response["error"] != nil {
		f.t.Fatalf("%s refused: %s", method, raw)
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		f.t.Fatalf("%s missing result: %s", method, raw)
	}
	return result
}

func (f *fixture) create(key, text string) map[string]any {
	f.t.Helper()
	commit := f.call("bead.create", key, map[string]any{"type": "issue", "fields": map[string]any{"title": "Public B12 record", "description": text}}, "")
	results := commit["results"].([]any)
	return results[0].(map[string]any)["ref"].(map[string]any)
}

func (f *fixture) assertSameHead(before driver.Meta) {
	f.t.Helper()
	after, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		f.t.Fatalf("refusal published metadata: before=%+v after=%+v", before, after)
	}
}
