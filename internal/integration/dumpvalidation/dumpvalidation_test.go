package dumpvalidation_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

func TestMain(m *testing.M) {
	code := func() int {
		id, err := randomHex(8)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-v3-dump-validation-" + id
		if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage); err != nil {
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
		fmt.Printf("Real engine: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}()
	os.Exit(code)
}

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

func randomHex(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

type runtime struct {
	ctx     context.Context
	store   driver.Store
	lease   driver.ActiveLease
	server  *rpc.Server
	catalog *wire.Catalog
}

func newRuntime(t *testing.T, store driver.Store) *runtime {
	t.Helper()
	ctx := context.Background()
	lease, err := store.Lease(ctx, "dump-validation", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(ctx)
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time {
		return time.Date(2026, 10, 29, 16, 0, 0, 0, time.UTC)
	}))
	domain := service.New(store, lease, writer, service.WithOntology(ontology), service.WithClock(func() time.Time {
		return time.Date(2026, 10, 29, 16, 0, 0, 0, time.UTC)
	}))
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = lease.Release(ctx)
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		_ = lease.Release(ctx)
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{
		DefaultProject:   "dump-validation",
		Incarnation:      meta.Incarnation,
		OntologyRevision: model.Natural64(1),
		CursorKey:        bytesForCursorKey(),
	})
	if err != nil {
		_ = lease.Release(ctx)
		t.Fatal(err)
	}
	return &runtime{ctx: ctx, store: store, lease: lease, server: server, catalog: registry.Catalog()}
}

func bytesForCursorKey() []byte {
	return []byte("dump-validation-cursor-key-0123456789")
}

func (r *runtime) close(t *testing.T) {
	t.Helper()
	if r.lease != nil {
		if err := r.lease.Release(r.ctx); err != nil && !errors.Is(err, driver.ErrTxClosed) {
			t.Error(err)
		}
		r.lease = nil
	}
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			t.Error(err)
		}
		r.store = nil
	}
}

type receiptKey struct {
	Principal string
	Agent     string
	Method    string
	Key       string
}

type durableState struct {
	Meta     driver.Meta
	Records  []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
	Receipts map[string]*driver.IdempotencyRecord
}

func state(t *testing.T, r *runtime, keys []receiptKey) durableState {
	t.Helper()
	meta, err := r.store.Meta(r.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := r.store.Read(r.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(r.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 10000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("record query incomplete: %+v %v", page, err)
	}
	got := durableState{Meta: meta, Records: page.Items, Versions: map[string][]driver.Version{}, Receipts: map[string]*driver.IdempotencyRecord{}}
	for _, record := range got.Records {
		versions, err := reader.Versions(r.ctx, record.Ref, driver.PageReq{Limit: 10000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("version query incomplete for %s: %+v %v", record.Ref.ID, versions, err)
		}
		got.Versions[record.Ref.ID] = versions.Items
	}
	got.Log, err = reader.Changesets(r.ctx, nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		receipt, err := reader.IdempotencyByKey(r.ctx, key.Principal, key.Agent, key.Method, key.Key)
		if err != nil {
			t.Fatal(err)
		}
		got.Receipts[key.Key] = receipt
	}
	return got
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *wire.Error     `json:"error"`
}

func call(t *testing.T, r *runtime, method, params string) rpcResponse {
	t.Helper()
	request := []byte(`{"jsonrpc":"2.0","id":1,"method":` + strconvQuote(method) + `,"params":` + params + `}`)
	var response rpcResponse
	if raw := r.server.Dispatch(r.ctx, request); len(raw) == 0 {
		t.Fatalf("%s returned no response", method)
	} else if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("%s returned invalid JSON %s: %v", method, raw, err)
	}
	return response
}

func strconvQuote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func assertRestoreRefusals(t *testing.T, r *runtime) {
	t.Helper()
	cases := []struct {
		name, method, params string
	}{
		{name: "dump", method: "admin.dump", params: `{"history":"full","pageSize":1,"humanPresence":{"challenge":"invalid"}}`},
		{name: "begin-empty", method: "admin.restore.begin", params: `{}`},
		{name: "begin-unsupported-version", method: "admin.restore.begin", params: `{"manifest":{"format":"plug-dump/99","schema":99},"history":"full","humanPresence":{"challenge":"invalid"}}`},
		{name: "chunk-corrupt", method: "admin.restore.chunk", params: `{"restoreId":"restore-1","page":0,"data":"corrupt","sha256":"00","humanPresence":{"challenge":"invalid"}}`},
		{name: "finish-corrupt", method: "admin.restore.finish", params: `{"restoreId":"restore-1","manifestSha256":"00","humanPresence":{"challenge":"invalid"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := r.catalog.Descriptor(tc.method); !ok {
				t.Fatalf("%s is absent from the canonical catalog", tc.method)
			}
			response := call(t, r, tc.method, tc.params)
			if response.Error == nil || string(response.Error.Data.Code) != "method_not_found" || len(response.Result) != 0 {
				t.Fatalf("%s refusal=%+v", tc.method, response)
			}
			if err := r.catalog.ValidateError(response.Error); err != nil {
				t.Fatalf("catalog rejected actual refusal: %v", err)
			}
		})
	}
}

func create(t *testing.T, r *runtime, key string) receiptKey {
	t.Helper()
	params := `{"type":"issue","fields":{"title":"restore boundary"},"_meta":{"idempotencyKey":"` + key + `","agent":"dump-validation"}}`
	response := call(t, r, "bead.create", params)
	if response.Error != nil {
		t.Fatalf("ordinary create refused: %+v", response.Error)
	}
	var commit model.Commit
	if err := json.Unmarshal(response.Result, &commit); err != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" {
		t.Fatalf("ordinary create did not return a real commit: %s %v", response.Result, err)
	}
	return receiptKey{Principal: "anonymous:anonymous", Agent: "dump-validation", Method: "bead.create", Key: key}
}

func assertSameState(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable state changed across refused restore: before=%+v after=%+v", before, after)
	}
}

func TestSQLiteRestoreRefusesAtomicallyAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dump-validation.db")
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	r := newRuntime(t, store)
	keys := []receiptKey{}
	defer r.close(t)
	empty := state(t, r, keys)
	assertRestoreRefusals(t, r)
	assertSameState(t, empty, state(t, r, keys))
	key, err := randomHex(32)
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, create(t, r, key))
	created := state(t, r, keys)
	assertRestoreRefusals(t, r)
	assertSameState(t, created, state(t, r, keys))
	r.close(t)
	reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	r = newRuntime(t, reopened)
	defer r.close(t)
	assertSameState(t, created, state(t, r, keys))
	assertRestoreRefusals(t, r)
	assertSameState(t, created, state(t, r, keys))
	t.Logf("SQLite refusal boundary preserved empty and populated records, versions, changesets and receipt across close/reopen; store=%s incarnation=%s", created.Meta.StoreID, created.Meta.Incarnation)
}

func freshPostgresDSN(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	id, err := randomHex(8)
	if err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	name := "plug_swarm_v3_dump_" + id
	if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Error(err)
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

func TestPostgresRestoreRefusesAtomicallyAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dsn := freshPostgresDSN(t)
	store, err := (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	r := newRuntime(t, store)
	keys := []receiptKey{}
	defer r.close(t)
	empty := state(t, r, keys)
	assertRestoreRefusals(t, r)
	assertSameState(t, empty, state(t, r, keys))
	key, err := randomHex(32)
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, create(t, r, key))
	created := state(t, r, keys)
	assertRestoreRefusals(t, r)
	assertSameState(t, created, state(t, r, keys))
	r.close(t)
	reopened, err := (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	r = newRuntime(t, reopened)
	defer r.close(t)
	assertSameState(t, created, state(t, r, keys))
	assertRestoreRefusals(t, r)
	assertSameState(t, created, state(t, r, keys))
	t.Logf("PostgreSQL refusal boundary preserved empty and populated records, versions, changesets and receipt across close/reopen; store=%s incarnation=%s", created.Meta.StoreID, created.Meta.Incarnation)
}
