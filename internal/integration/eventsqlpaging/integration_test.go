package eventsqlpaging_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var (
	postgresTestDSN string
	postgresDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	name := "plug-swarm-v3-event-sql-paging-" + strconv.Itoa(os.Getpid())
	if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage); err != nil {
		fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
		os.Exit(1)
	}
	cleanup := func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
		}
	}
	out, err := docker("port", name, "5432/tcp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "real PostgreSQL port lookup failed: %v %s\n", err, out)
		cleanup()
		os.Exit(1)
	}
	addr := strings.TrimSpace(string(out))
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		fmt.Fprintf(os.Stderr, "test PostgreSQL must bind loopback, got %q (%v)\n", addr, err)
		cleanup()
		os.Exit(1)
	}
	postgresTestDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
	db, err := sql.Open("pgx", postgresTestDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "real PostgreSQL SQL open failed: %v\n", err)
		cleanup()
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL never became ready: %v\n", err)
			_ = db.Close()
			cleanup()
			os.Exit(1)
		}
		time.Sleep(30 * time.Millisecond)
	}
	var version string
	if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		fmt.Fprintf(os.Stderr, "real PostgreSQL version query failed: %v\n", err)
		_ = db.Close()
		cleanup()
		os.Exit(1)
	}
	_ = db.Close()
	fmt.Printf("Real PostgreSQL: %s\nImage: %s\n", version, postgresImage)
	code := m.Run()
	cleanup()
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

type storeOpener func(context.Context, driver.Config) (driver.Store, error)

type engine struct {
	name string
	open storeOpener
	dsn  func(*testing.T) string
}

func TestSQLHistoryReaderAndWriterPaging(t *testing.T) {
	engines := []engine{
		{
			name: "sqlite",
			open: func(ctx context.Context, cfg driver.Config) (driver.Store, error) {
				return sqlite.Driver{}.Open(ctx, cfg)
			},
			dsn: func(t *testing.T) string { return filepath.Join(t.TempDir(), "events.db") },
		},
		{
			name: "postgres",
			open: func(ctx context.Context, cfg driver.Config) (driver.Store, error) {
				return postgres.Driver{}.Open(ctx, cfg)
			},
			dsn: freshPostgresDSN,
		},
	}
	for _, e := range engines {
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()
			runEngine(t, e)
		})
	}
}

func freshPostgresDSN(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	id := postgresDBSeq.Add(1)
	name := fmt.Sprintf("plug_event_sql_paging_%d_%d", os.Getpid(), id)
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
	parsed, err := url.Parse(postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

type runtime struct {
	ctx       context.Context
	store     driver.Store
	lease     driver.ActiveLease
	writer    *pipeline.Writer
	service   *service.Service
	inc       string
	principal model.PrincipalRef
}

func openRuntime(t *testing.T, e engine, dsn string) *runtime {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, err := e.open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "event-sql-paging", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(context.Background())
		_ = store.Close()
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	ids := []string{"event-a", "event-b"}
	var next atomic.Int64
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		i := next.Add(1) - 1
		if i >= int64(len(ids)) {
			return fmt.Sprintf("event-extra-%d", i), nil
		}
		return ids[i], nil
	}))
	principal := model.PrincipalRef{Kind: "user", ID: "event-writer"}
	return &runtime{ctx: ctx, store: store, lease: lease, writer: writer, service: service.New(store, lease, writer, service.WithOntology(ontology)), inc: meta.Incarnation, principal: principal}
}

func (r *runtime) close(t *testing.T) {
	t.Helper()
	if r.lease != nil {
		if err := r.lease.Release(context.Background()); err != nil {
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

func writeMeta(n int) model.WriteMeta {
	return model.WriteMeta{Project: "event-project", IdempotencyKey: fmt.Sprintf("%032x", n)}
}

type eventRecord struct {
	revision string
	index    string
	resource string
	typeName string
}

func eventPosition(e model.Event) coreevents.Position {
	position := coreevents.PositionOf(e)
	if position.Incarnation == "" {
		panic("event has no position")
	}
	return position
}

func readPage(t *testing.T, reader *coreevents.Reader, ctx context.Context, after coreevents.Position, principal model.PrincipalRef, filter coreevents.Filter, limit int) coreevents.Page {
	t.Helper()
	page, err := reader.Read(ctx, coreevents.ReadRequest{After: after, Principal: principal, Filter: filter, Limit: limit})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func runEngine(t *testing.T, e engine) {
	t.Helper()
	dsn := e.dsn(t)
	r := openRuntime(t, e, dsn)
	defer r.close(t)

	first, err := r.service.Create(r.ctx, r.principal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "first"}}, writeMeta(1))
	if err != nil {
		t.Fatalf("actual Writer create first: %v", err)
	}
	second, err := r.service.Create(r.ctx, r.principal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "second"}}, writeMeta(2))
	if err != nil {
		t.Fatalf("actual Writer create second: %v", err)
	}
	if first.Revision.String() != "1" || second.Revision.String() != "2" {
		t.Fatalf("ordinary create revisions=%s,%s", first.Revision, second.Revision)
	}
	if len(first.Results) != 1 || len(second.Results) != 1 {
		t.Fatalf("ordinary create results=%+v %+v", first, second)
	}
	patch := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: first.Results[0].Ref, Fields: map[string]any{"title": "first-patched"}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: second.Results[0].Ref, Fields: map[string]any{"title": "second-patched"}}},
	}}
	batched, err := r.writer.Apply(r.ctx, r.principal, patch, model.WriteMeta{Method: "tx.apply", Project: "event-project", IdempotencyKey: fmt.Sprintf("%032x", 3)})
	if err != nil {
		t.Fatalf("actual Writer patch group: %v", err)
	}
	comment, err := r.service.Comment(r.ctx, r.principal, service.CommentRequest{ID: first.Results[0].Ref, Body: "recorded"}, writeMeta(4))
	if err != nil {
		t.Fatalf("actual Writer comment: %v", err)
	}
	if batched.Revision.String() != "3" || len(batched.Results) != 2 || comment.Revision.String() != "4" {
		t.Fatalf("ordinary patch/comment revisions: patch=%+v comment=%+v", batched, comment)
	}

	reader := &coreevents.Reader{Log: serverevents.Log{Store: r.store}, Materializer: serverevents.HistoryMaterializer{Store: r.store}, Authorization: coreevents.AllowAll{}}
	firstPage := readPage(t, reader, r.ctx, coreevents.Position{}, r.principal, coreevents.Filter{}, 3)
	if len(firstPage.Events()) != 3 || len(firstPage.Groups) != 3 {
		t.Fatalf("first page should stop inside revision 3: %+v", firstPage)
	}
	if firstPage.Groups[2].Revision.String() != "3" || len(firstPage.Groups[2].Events) != 1 || firstPage.Position.Revision.String() != "3" || firstPage.Position.Index.String() != "0" {
		t.Fatalf("partial group cursor=%+v groups=%+v", firstPage.Position, firstPage.Groups)
	}

	// Close/reopen the actual SQL store before resuming the partial group. The
	// incarnation must stay stable while the lease epoch changes.
	oldIncarnation := r.inc
	oldEpoch := r.lease.Epoch()
	r.close(t)
	r = openRuntime(t, e, dsn)
	defer r.close(t)
	if r.inc != oldIncarnation || r.lease.Epoch() <= oldEpoch {
		t.Fatalf("reopen changed identity or reused lease epoch: incarnation %q/%q epoch %d/%d", oldIncarnation, r.inc, oldEpoch, r.lease.Epoch())
	}
	reader = &coreevents.Reader{Log: serverevents.Log{Store: r.store}, Materializer: serverevents.HistoryMaterializer{Store: r.store}, Authorization: coreevents.AllowAll{}}
	resume := readPage(t, reader, r.ctx, firstPage.Position, r.principal, coreevents.Filter{}, 1000)
	if len(resume.Events()) != 2 || resume.Events()[0].Durable.Index.String() != "1" || resume.Events()[0].Durable.Revision.String() != "3" || resume.Events()[1].Durable.Revision.String() != "4" {
		t.Fatalf("partial-group resume lost/duplicated events: %+v", resume)
	}
	all := append(append([]model.Event{}, firstPage.Events()...), resume.Events()...)
	want := []eventRecord{{"1", "0", "bead/event-a", "bead.created"}, {"2", "0", "bead/event-b", "bead.created"}, {"3", "0", "bead/event-a", "bead.updated"}, {"3", "1", "bead/event-b", "bead.updated"}, {"4", "0", "bead/event-a", "bead.commented"}}
	if len(all) != len(want) {
		t.Fatalf("all event count=%d want %d: %+v", len(all), len(want), all)
	}
	seen := map[string]bool{}
	for i, got := range all {
		if got.Durable == nil {
			t.Fatalf("event %d is not durable: %+v", i, got)
		}
		position := eventPosition(got)
		key := position.Revision.String() + ":" + position.Index.String()
		if seen[key] {
			t.Fatalf("duplicate event position %s", key)
		}
		seen[key] = true
		if got.Durable.Revision.String() != want[i].revision || got.Durable.Index.String() != want[i].index || got.Durable.Resource != want[i].resource || string(got.Durable.Type) != want[i].typeName {
			t.Fatalf("event %d=%+v want %+v", i, got.Durable, want[i])
		}
	}

	// A filter that matches no event still advances through every committed
	// group. Its end cursor remains valid across another SQL reopen.
	empty := readPage(t, reader, r.ctx, coreevents.Position{}, r.principal, coreevents.Filter{Types: []string{"bead.deleted"}}, 1)
	if len(empty.Events()) != 0 || empty.Position.Revision.String() != "4" || empty.Position.Index.String() != "0" {
		t.Fatalf("empty filtered scan did not preserve committed head: %+v", empty)
	}
	filteredPosition := empty.Position
	r.close(t)
	r = openRuntime(t, e, dsn)
	defer r.close(t)
	reader = &coreevents.Reader{Log: serverevents.Log{Store: r.store}, Materializer: serverevents.HistoryMaterializer{Store: r.store}, Authorization: coreevents.AllowAll{}}
	afterEmpty := readPage(t, reader, r.ctx, filteredPosition, r.principal, coreevents.Filter{Types: []string{"bead.deleted"}}, 1)
	if len(afterEmpty.Events()) != 0 || afterEmpty.Position != filteredPosition {
		t.Fatalf("filtered cursor changed after reopen: %+v", afterEmpty)
	}
	t.Logf("%s: actual Writer create/patch/comment; Log/HistoryMaterializer/Reader ordered 4 revisions and 5 events, resumed partial revision 3 after reopen, and preserved empty filtered cursor after reopen", e.name)

	checkUnknownDescription(t, e)
}

func checkUnknownDescription(t *testing.T, e engine) {
	t.Helper()
	dsn := e.dsn(t)
	r := openRuntime(t, e, dsn)
	defer r.close(t)
	ref := model.RefByID("unknown-description", model.Natural64(0))
	meta, err := r.store.Meta(r.ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(r.ctx, r.store, r.lease, driver.ChangesetHeader{Incarnation: meta.Incarnation, Principal: r.principal}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: []byte(`{"id":"unknown-description","type":"issue","project":"event-project","fields":{"title":"unknown"}}`), ChangedPaths: []string{"/status"}}})
	if err != nil {
		t.Fatalf("seed committed unknown description: %v", err)
	}
	reader := &coreevents.Reader{Log: serverevents.Log{Store: r.store}, Materializer: serverevents.HistoryMaterializer{Store: r.store}, Authorization: coreevents.AllowAll{}}
	page, err := reader.Read(r.ctx, coreevents.ReadRequest{After: coreevents.Position{}, Principal: r.principal, Filter: coreevents.Filter{}, Limit: 10})
	if !errors.Is(err, driver.ErrUnsupported) || len(page.Events()) != 0 {
		t.Fatalf("unknown committed description did not refuse honestly: page=%+v err=%v", page, err)
	}
	t.Logf("%s: unknown committed status description returns driver.ErrUnsupported with no successful event page", e.name)
}
