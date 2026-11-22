package apikeysql_test

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/apikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresAdminDSN string

// TestMain owns the real PostgreSQL engine used by this package. Tests never
// fall back to memory or skip when the engine cannot be started.
func TestMain(m *testing.M) {
	code := func() int {
		id, err := randomHex(8)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-v3-apikey-sql-" + id
		if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name,
			"-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test",
			"-p", "127.0.0.1::5432", postgresImage); err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
			}
		}()
		out, err := docker("port", name, "5432/tcp")
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
		postgresAdminDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresAdminDSN)
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
				fmt.Fprintln(os.Stderr, "PostgreSQL test engine never became ready")
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		return m.Run()
	}()
	os.Exit(code)
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_HOST=") && !strings.HasPrefix(value, "DOCKER_CONTEXT=") &&
			!strings.HasPrefix(value, "DOCKER_TLS=") && !strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") &&
			!strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

type backend struct {
	name    string
	open    func(context.Context) (driver.Store, error)
	dispose func() error
}

func backends(t *testing.T) []backend {
	t.Helper()
	return []backend{sqliteBackend(t), postgresBackend(t)}
}

func sqliteBackend(t *testing.T) backend {
	t.Helper()
	path := filepath.Join(t.TempDir(), "apikey-sqlite.db")
	return backend{
		name: "sqlite",
		open: func(ctx context.Context) (driver.Store, error) {
			return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
		},
	}
}

func postgresBackend(t *testing.T) backend {
	t.Helper()
	admin, err := sql.Open("pgx", postgresAdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	id, err := randomHex(12)
	if err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	database := "plug_swarm_v3_apikey_sql_" + id
	if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	dsnURL, err := url.Parse(postgresAdminDSN)
	if err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	dsnURL.Path = "/" + database
	dsn := dsnURL.String()
	return backend{
		name: "postgres",
		open: func(ctx context.Context) (driver.Store, error) {
			return postgres.Driver{}.Open(ctx, driver.Config{DSN: dsn})
		},
		dispose: func() error {
			defer admin.Close()
			_, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`)
			return err
		},
	}
}

type fixture struct {
	t          *testing.T
	ctx        context.Context
	cancel     context.CancelFunc
	backend    backend
	keys       *authnapikey.Provider
	clock      atomic.Int64
	store      driver.Store
	lease      driver.ActiveLease
	server     *httpjson.Server
	served     chan error
	client     *http.Client
	endpoint   string
	stopRenew  context.CancelFunc
	renewed    chan error
	startCount atomic.Int32
}

func newFixture(t *testing.T, b backend) *fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f := &fixture{t: t, ctx: ctx, cancel: cancel, backend: b}
	f.clock.Store(time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC).UnixNano())
	f.keys = authnapikey.New(authnapikey.Config{Now: f.now})
	t.Cleanup(func() {
		f.stop(t)
		f.cancel()
		if b.dispose != nil {
			if err := b.dispose(); err != nil {
				t.Error(err)
			}
		}
	})
	f.start(t)
	return f
}

func (f *fixture) now() time.Time { return time.Unix(0, f.clock.Load()).UTC() }

func (f *fixture) start(t *testing.T) {
	t.Helper()
	store, err := f.backend.open(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(f.ctx, "apikey-sql-"+f.backend.name, driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	f.store, f.lease = store, lease
	meta, err := store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := apikey.New(f.ctx, apikey.Config{
		Store: store, Lease: lease, Keys: f.keys, DefaultProject: "apikey-sql",
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := handler.Listen(f.ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.server = server
	f.endpoint = "http://" + server.Addr().String() + httpjson.Path
	f.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	f.served = make(chan error, 1)
	go func() { f.served <- server.Serve() }()
	f.startRenewal(t)
	f.startCount.Add(1)
	if meta.Incarnation == "" {
		t.Fatal("SQL store did not initialize a durable incarnation")
	}
}

func (f *fixture) startRenewal(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	f.stopRenew, f.renewed = cancel, make(chan error, 1)
	lease := f.lease
	done := f.renewed
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					done <- err
					return
				}
			}
		}
	}()
}

func (f *fixture) stop(t *testing.T) {
	t.Helper()
	if f.server != nil {
		f.client.CloseIdleConnections()
		if err := f.server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-f.served:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("API-key SQL HTTP server did not stop")
		}
		f.server = nil
	}
	if f.stopRenew != nil {
		f.stopRenew()
		if err := <-f.renewed; err != nil {
			t.Error("SQL lease renewal:", err)
		}
		f.stopRenew = nil
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

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	before, oldEpoch := f.meta(t), f.lease.Epoch()
	f.stop(t)
	f.start(t)
	after := f.meta(t)
	if before.StoreID != after.StoreID || before.Incarnation != after.Incarnation || before.Head != after.Head || f.lease.Epoch() <= oldEpoch {
		t.Fatalf("SQL close/reopen changed durable identity/head or reused lease epoch: before=%+v after=%+v epochs=%d/%d", before, after, oldEpoch, f.lease.Epoch())
	}
	if f.startCount.Load() != 2 {
		t.Fatalf("expected exactly one listener/store/lease reopen, starts=%d", f.startCount.Load())
	}
}

func (f *fixture) issue(t *testing.T, principal model.PrincipalRef, expires time.Time) authnapikey.IssuedKey {
	t.Helper()
	issued, err := f.keys.Issue(principal, expires)
	if err != nil {
		t.Fatal(err)
	}
	return issued
}

type reply struct {
	status int
	raw    []byte
	result json.RawMessage
	meta   map[string]any
	value  map[string]any
}

func (f *fixture) call(t *testing.T, token, method string, params map[string]any, change func(*http.Request)) reply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.endpoint, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if change != nil {
		change(request)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal("API-key SQL HTTP round trip failed:", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
		Meta   map[string]any  `json:"_meta"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("SQL HTTP response was not JSON: %s", raw)
	}
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		t.Fatal(err)
	}
	return reply{status: response.StatusCode, raw: raw, result: value.Result, meta: value.Meta, value: object}
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func writeParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	key, err := randomHex(16)
	if err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{"idempotencyKey": key}
	return params
}

func fixedParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	params["_meta"] = map[string]any{"idempotencyKey": "0123456789abcdef0123456789abcdef"}
	return params
}

func success(t *testing.T, got reply) map[string]any {
	t.Helper()
	if got.status != http.StatusOK || len(got.result) == 0 {
		t.Fatalf("SQL API-key request failed: status=%d body=%s", got.status, got.raw)
	}
	if value, exists := got.value["error"]; exists && value != nil {
		t.Fatalf("SQL API-key request returned an error: status=%d body=%s", got.status, got.raw)
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(got.result))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func requireCode(t *testing.T, got reply, status int, want string) {
	t.Helper()
	if got.status != status || len(got.result) != 0 {
		t.Fatalf("SQL API-key refusal had wrong envelope: status=%d body=%s", got.status, got.raw)
	}
	errValue, ok := got.value["error"].(map[string]any)
	if !ok {
		t.Fatalf("SQL API-key refusal had no error: %s", got.raw)
	}
	data, ok := errValue["data"].(map[string]any)
	if !ok || data["code"] != want {
		t.Fatalf("SQL API-key refusal code=%v want=%s body=%s", data["code"], want, got.raw)
	}
}

func createdRef(t *testing.T, result map[string]any) model.Ref {
	t.Helper()
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("create result missing row: %+v", result)
	}
	row := rows[0].(map[string]any)
	refValue := row["ref"].(map[string]any)
	return model.RefByID(refValue["id"].(string), model.Natural64(0))
}

func credentialFree(t *testing.T, data []byte, keys ...authnapikey.IssuedKey) {
	t.Helper()
	for keyIndex, issued := range keys {
		parts := strings.SplitN(strings.TrimPrefix(issued.Token, "plg_k_"), "_", 2)
		if bytes.Contains(data, []byte(issued.Token)) || bytes.Contains(data, []byte(issued.ID)) || len(parts) == 2 && bytes.Contains(data, []byte(parts[1])) {
			t.Fatalf("credential material escaped into SQL/API data (credential check %d)", keyIndex+1)
		}
	}
}

func durableBytes(t *testing.T, f *fixture, refs []model.Ref) []byte {
	t.Helper()
	meta := f.meta(t)
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changes, err := reader.Changesets(f.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	versions := make([]driver.Version, 0)
	for _, ref := range refs {
		page, err := reader.Versions(f.ctx, ref, driver.PageReq{Limit: 100})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("SQL versions read failed: %+v %v", page, err)
		}
		versions = append(versions, page.Items...)
	}
	records, err := reader.Get(f.ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		Changes  []model.Changeset `json:"changes"`
		Versions []driver.Version  `json:"versions"`
		Records  []driver.Record   `json:"records"`
	}{changes, versions, records})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
