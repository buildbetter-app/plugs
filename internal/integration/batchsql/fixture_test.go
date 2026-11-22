package batchsql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresTestDSN string

// PostgreSQL acceptance owns one disposable engine for this package. There is
// deliberately no environment-driven skip: the SQL qualification is real or
// the target fails before reporting a false pass.
func TestMain(m *testing.M) {
	code := func() int {
		id, err := randomID()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-v3-batch-sql-" + id
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
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		addr := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(addr)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "PostgreSQL test engine must bind loopback")
			return 1
		}
		postgresTestDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresTestDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
		fmt.Printf("Real PostgreSQL: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}()
	os.Exit(code)
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_HOST=") && !strings.HasPrefix(value, "DOCKER_CONTEXT=") && !strings.HasPrefix(value, "DOCKER_TLS=") && !strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func freshPostgresDSN(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	id, err := randomID()
	if err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	database := "plug_batch_sql_" + id
	if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+database+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+database+`" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		_ = admin.Close()
	})
	parsed, err := url.Parse(postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	return parsed.String()
}

var batchPrincipal = model.PrincipalRef{Kind: "user", ID: "batch-sql-worker"}

type fixture struct {
	t        *testing.T
	ctx      context.Context
	engine   string
	path     string
	dsn      string
	clock    time.Time
	ids      atomic.Uint64
	keys     atomic.Uint64
	store    driver.Store
	lease    driver.ActiveLease
	ontology *service.Ontology
	writer   *pipeline.Writer
	closed   bool
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	f := &fixture{t: t, ctx: ctx, engine: engine, clock: time.Date(2026, 10, 29, 16, 0, 0, 0, time.UTC)}
	if engine == "sqlite" {
		f.path = t.TempDir() + "/batch.db"
	}
	if engine == "postgres" {
		f.dsn = freshPostgresDSN(t)
	}
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	return f
}

func (f *fixture) now() time.Time { return f.clock }

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{
			Clock: f.now, StoreID: driver.StoreID("batch-" + f.engine), Incarnation: "batch-" + f.engine + "-incarnation",
		})
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		t.Fatalf("unknown engine %q", f.engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "batch-sql-"+f.engine, driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatal(err)
	}
	f.ontology = service.NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology),
		pipeline.WithClock(f.now),
		pipeline.WithIDGenerator(f.nextID),
	)
	if f.writer == nil {
		t.Fatal("direct Writer was not constructed")
	}
	f.closed = false
	t.Logf("direct Writer boundary: engine=%s; tx.apply RPC is intentionally not exercised", f.engine)
}

func (f *fixture) close(t *testing.T) {
	t.Helper()
	if f.closed {
		return
	}
	f.closed = true
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			var fenced driver.ErrFenced
			if !errors.As(err, &fenced) {
				t.Error(err)
			}
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
		f.store = nil
	}
	f.writer = nil
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	if f.engine == "memory" {
		t.Fatalf("memory has no persistence reopen boundary")
	}
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store = nil
	f.closed = true
	f.open(t)
	after, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.StoreID != after.StoreID || before.Incarnation != after.Incarnation || before.Head.String() != after.Head.String() {
		t.Fatalf("%s reopen changed durable identity/head: before=%+v after=%+v", f.engine, before, after)
	}
	t.Logf("real %s close/reopen preserved store=%s incarnation=%s head=%s", f.engine, after.StoreID, after.Incarnation, after.Head)
}

func (f *fixture) nextID() (string, error) {
	n := f.ids.Add(1)
	return fmt.Sprintf("pl-batch-%s-%03d", f.engine, n), nil
}

func (f *fixture) meta(label string) model.WriteMeta {
	n := f.keys.Add(1)
	return model.WriteMeta{
		Method:         "tx.apply",
		Project:        "batch-local",
		IdempotencyKey: fmt.Sprintf("%032x", n),
		Traceparent:    "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
	}
}

func createOp(key, title, project string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Key: key, Type: "issue", Project: project, Fields: map[string]any{"title": title}}}
}

func patchOp(ref model.Ref, fields map[string]any) model.Op {
	if fields == nil {
		fields = map[string]any{}
	}
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: fields}}
}

func commentOp(ref model.Ref, body string) model.Op {
	return model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Kind: model.OpComment, Target: ref, Body: body}}
}

type durableState struct {
	Meta     driver.Meta
	Records  map[string]driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
}

func (f *fixture) state(t *testing.T) durableState {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("complete bead query failed: page=%+v err=%v", page, err)
	}
	s := durableState{Meta: meta, Records: map[string]driver.Record{}, Versions: map[string][]driver.Version{}}
	for _, record := range page.Items {
		s.Records[record.Ref.ID] = record
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("complete versions query for %s failed: page=%+v err=%v", record.Ref.ID, versions, err)
		}
		s.Versions[record.Ref.ID] = versions.Items
	}
	s.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *fixture) receipt(t *testing.T, meta model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(batchPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := r.IdempotencyByKey(f.ctx, principal, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func requireSameState(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable state changed: before=%+v after=%+v", before, after)
	}
}

func requireCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", want)
	}
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != want {
		t.Fatalf("expected %s, got %T %v", want, err, err)
	}
}

func decodeDocument(t *testing.T, record driver.Record) pipeline.Document {
	t.Helper()
	doc, err := pipeline.DecodeDocument(record.Body)
	if err != nil {
		t.Fatalf("decode %s: %v", record.Ref.ID, err)
	}
	return doc
}

func sortedRecordIDs(records map[string]driver.Record) []string {
	ids := make([]string, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
