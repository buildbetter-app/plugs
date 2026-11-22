package historysql_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	project = "history-sql"
	pgImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
)

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
		name := "plug-swarm-v3-history-sql-" + id
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

type backend string

const (
	sqliteBackend   backend = "sqlite"
	postgresBackend backend = "postgres"
)

func (b backend) String() string { return string(b) }

type fixture struct {
	t         *testing.T
	backend   backend
	ctx       context.Context
	cancel    context.CancelFunc
	path      string
	dsn       string
	store     driver.Store
	lease     driver.ActiveLease
	writer    *pipeline.Writer
	domain    *service.Service
	rpc       *rpc.Server
	server    *httpjson.Server
	served    chan error
	client    *http.Client
	endpoint  string
	cursorKey []byte
	renewStop context.CancelFunc
	renewed   chan error
}

func newFixture(t *testing.T, b backend) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	f := &fixture{t: t, backend: b, ctx: ctx, cancel: cancel, cursorKey: make([]byte, 32)}
	if _, err := rand.Read(f.cursorKey); err != nil {
		t.Fatal(err)
	}
	if b == sqliteBackend {
		f.path = filepath.Join(t.TempDir(), "history.db")
	} else {
		f.dsn = newPostgresDatabase(t)
	}
	f.start()
	t.Cleanup(func() { f.stop() })
	return f
}

func (f *fixture) start() {
	f.t.Helper()
	var err error
	switch f.backend {
	case sqliteBackend:
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	case postgresBackend:
		f.store, err = postgres.Driver{}.Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatalf("unsupported backend %q", f.backend)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "history-sql", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	lease := f.lease
	renewCtx, stop := context.WithCancel(f.ctx)
	f.renewStop, f.renewed = stop, make(chan error, 1)
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				f.renewed <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if renewCtx.Err() != nil {
						err = nil
					}
					f.renewed <- err
					return
				}
			}
		}
	}()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology))
	f.domain = service.New(f.store, f.lease, f.writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, f.domain); err != nil {
		f.t.Fatal(err)
	}
	f.rpc, err = rpc.New(registry, rpc.Config{
		DefaultProject: project, Incarnation: meta.Incarnation,
		OntologyRevision: model.Natural64(1), CursorKey: f.cursorKey,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	handler, err := httpjson.New(f.rpc, authenticator, httpjson.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	f.server, err = handler.Listen(f.ctx, "127.0.0.1:0", nil)
	if err != nil {
		f.t.Fatal(err)
	}
	f.served = make(chan error, 1)
	go func() { f.served <- f.server.Serve() }()
	f.endpoint = "http://" + f.server.Addr().String() + httpjson.Path
	f.client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}
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
	name := "plug_history_" + id
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

func (f *fixture) stop() {
	f.t.Helper()
	if f.server != nil {
		f.client.CloseIdleConnections()
		if err := f.server.Close(); err != nil {
			f.t.Error(err)
		}
		select {
		case err := <-f.served:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				f.t.Error(err)
			}
		case <-time.After(5 * time.Second):
			f.t.Error("HTTP server did not stop")
		}
		f.server = nil
	}
	if f.renewStop != nil {
		f.renewStop()
		if err := <-f.renewed; err != nil {
			f.t.Error("lease renewal:", err)
		}
		f.renewStop = nil
	}
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

func (f *fixture) reopen() {
	f.t.Helper()
	before, epoch := f.meta(), f.lease.Epoch()
	f.stop()
	f.start()
	after := f.meta()
	if before != after || f.lease.Epoch() <= epoch {
		f.t.Fatalf("%s reopen changed durable identity/head or reused epoch: before=%+v after=%+v", f.backend, before, after)
	}
}

func (f *fixture) meta() driver.Meta {
	f.t.Helper()
	value, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return value
}

type reply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *wire.Error     `json:"error"`
}

func (f *fixture) invoke(method string, params map[string]any) reply {
	f.t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(`9007199254740993`), "method": method, "params": params})
	if err != nil {
		f.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.endpoint, bytes.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	response, err := f.client.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		f.t.Fatal(err)
	}
	var result reply
	if err := json.Unmarshal(raw, &result); err != nil {
		f.t.Fatalf("invalid HTTP/RPC response (%d): %s", response.StatusCode, raw)
	}
	if result.JSONRPC != "2.0" || string(result.ID) != "9007199254740993" || (result.Error == nil) == (len(result.Result) == 0) {
		f.t.Fatalf("invalid JSON-RPC envelope: %s", raw)
	}
	return result
}

func (f *fixture) success(method string, params map[string]any, target any) reply {
	f.t.Helper()
	result := f.invoke(method, params)
	if result.Error != nil {
		f.t.Fatalf("%s failed: %+v", method, result.Error)
	}
	if target != nil {
		decoder := json.NewDecoder(bytes.NewReader(result.Result))
		decoder.UseNumber()
		if err := decoder.Decode(target); err != nil {
			f.t.Fatalf("invalid %s result %s: %v", method, result.Result, err)
		}
	}
	return result
}

func (f *fixture) expectCode(method string, params map[string]any, code string) {
	f.t.Helper()
	result := f.invoke(method, params)
	if result.Error == nil || string(result.Error.Data.Code) != code || len(result.Result) != 0 {
		f.t.Fatalf("%s: want %s, got %+v", method, code, result)
	}
}

func idempotencyKey() string {
	value, err := randomID()
	if err != nil {
		panic(err)
	}
	return value + value
}

func writeParams(fields map[string]any, agent string) map[string]any {
	return map[string]any{
		"type": "issue", "fields": fields,
		"_meta": map[string]any{"idempotencyKey": idempotencyKey(), "agent": agent},
	}
}

type bead struct {
	ID       string         `json:"id"`
	Revision model.Revision `json:"revision"`
	Status   string         `json:"status"`
	Fields   map[string]any `json:"fields"`
}

type asOf struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation"`
}

type beadPage struct {
	Items      []bead `json:"items"`
	AsOf       asOf   `json:"asOf"`
	NextCursor string `json:"nextCursor"`
}

func (f *fixture) create(title, agent string) (model.Ref, bead) {
	var commit model.Commit
	params := writeParams(map[string]any{"title": title, "description": "original"}, agent)
	f.success("bead.create", params, &commit)
	if len(commit.Results) != 1 {
		f.t.Fatalf("create returned %+v", commit)
	}
	ref := commit.Results[0].Ref
	return ref, f.get(ref, nil)
}

func (f *fixture) get(ref model.Ref, at *asOf) bead {
	params := map[string]any{"id": ref}
	if at != nil {
		params["asOf"] = at
	}
	var result struct {
		Bead bead `json:"bead"`
	}
	f.success("bead.get", params, &result)
	return result.Bead
}

func (f *fixture) update(ref model.Ref, revision model.Revision, title, agent string) {
	params := map[string]any{
		"id": ref, "fields": map[string]any{"title": title},
		"expect": []model.Precondition{model.PreconditionByRevision(ref, revision)},
	}
	params["_meta"] = map[string]any{"idempotencyKey": idempotencyKey(), "agent": agent}
	var commit model.Commit
	f.success("bead.update", params, &commit)
}

func (f *fixture) changesets() []model.Changeset {
	f.t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.meta().Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Changesets(f.ctx, nil, 100)
	if err != nil {
		f.t.Fatal(err)
	}
	return rows
}
