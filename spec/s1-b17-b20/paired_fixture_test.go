package s1b17b20

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// The original bounded tests and reference recordings remain unchanged. This
// fixture exercises real drivers, including SQL time on every PG connection;
// it never accepts an external database or skips an unavailable engine.
type pairedStore struct {
	*b17Harness
	engine string
	dsn    string
	clock  *testclock.Clock
}

func newPairedStore(t *testing.T, engine string) *pairedStore {
	t.Helper()
	clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456000, time.UTC))
	p := &pairedStore{b17Harness: &b17Harness{ctx: context.Background(), now: clock.Now()}, engine: engine, clock: clock}
	switch engine {
	case "memory":
	case "sqlite":
		p.dsn = filepath.Join(t.TempDir(), "paired.db")
	case "postgres":
		p.dsn = clock.BindPostgres(t, testclock.PostgresDSN(t))
		if got := testclock.PostgresNow(t, p.dsn); !got.Equal(clock.Now()) {
			t.Fatalf("actual SQL clock=%s application=%s", got, clock.Now())
		}
	default:
		t.Fatalf("unknown paired engine %q", engine)
	}
	p.open(t)
	t.Cleanup(func() { p.close(t) })
	return p
}

func (p *pairedStore) open(t *testing.T) {
	t.Helper()
	var err error
	switch p.engine {
	case "memory":
		p.store, err = memory.NewWithOptions(memory.Options{StoreID: "paired-store", Incarnation: "paired-incarnation", Clock: p.clock.Now})
	case "sqlite":
		p.store, err = (sqlite.Driver{Clock: p.clock.Now}).Open(p.ctx, driver.Config{DSN: p.dsn})
	case "postgres":
		p.store, err = postgres.Open(p.ctx, driver.Config{DSN: p.dsn})
	}
	if err != nil {
		t.Fatal(err)
	}
	p.lease, err = p.store.Lease(p.ctx, "paired-acceptance", driver.LeaseTTL)
	if err != nil {
		_ = p.store.Close()
		t.Fatal(err)
	}
}

func (p *pairedStore) advance(t *testing.T) {
	t.Helper()
	p.clock.Advance(t, time.Second)
	p.now = p.clock.Now()
	if p.engine == "postgres" && !testclock.PostgresNow(t, p.dsn).Equal(p.now) {
		t.Fatal("application and actual SQL time diverged")
	}
}

func (p *pairedStore) reopen(t *testing.T) {
	t.Helper()
	if p.engine == "memory" {
		return
	}
	p.close(t)
	p.open(t)
}

func (p *pairedStore) checkSQLReceiptTimes(t *testing.T, first time.Time) {
	t.Helper()
	if p.engine != "postgres" {
		return
	}
	db, err := sql.Open("pgx", p.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var earliest, latest time.Time
	if err := db.QueryRowContext(p.ctx, `SELECT min(expires),max(expires) FROM plug_postgres.receipts`).Scan(&earliest, &latest); err != nil {
		t.Fatal(err)
	}
	if !earliest.Equal(first.Add(driver.IdempotencyWindow)) || !latest.Equal(p.now.Add(driver.IdempotencyWindow)) {
		t.Fatalf("actual SQL-generated persisted receipt times=%s/%s, application=%s/%s", earliest, latest, first, p.now)
	}
	t.Log("actual PostgreSQL-generated persisted receipt expiry follows the explicit Go/SQL clock schedule")
}

func (p *pairedStore) receipt(t *testing.T, meta model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	state, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.store.Read(p.ctx, driver.View{Incarnation: state.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(b17Actor)
	if err != nil {
		t.Fatal(err)
	}
	// Authenticated principals never use the client agent label as identity.
	value, err := r.IdempotencyByKey(p.ctx, principal, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func (p *pairedStore) rpc(t *testing.T) *runtime {
	t.Helper()
	return p.rpcWithWriter(t, p.writer())
}

func (p *pairedStore) rpcWithWriter(t *testing.T, writer model.Writer) *runtime {
	t.Helper()
	codec, err := serverevents.NewCursorCodec([]byte(b20CursorKey))
	if err != nil {
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: &coreevents.Reader{
		Log: serverevents.Log{Store: p.store}, Materializer: serverevents.HistoryMaterializer{Store: p.store}, Authorization: coreevents.AllowAll{},
	}, Cursors: codec}
	ontology := service.NewOntology(nil)
	domain := service.New(p.store, p.lease, writer, service.WithOntology(ontology), service.WithClock(p.clock.Now), service.WithEventProvider(eventProvider{events: events}))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "cardb20", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte(b20CursorKey)})
	if err != nil {
		t.Fatal(err)
	}
	// pairedStore alone owns the store and lease lifetime.
	return &runtime{ctx: p.ctx, store: p.store, lease: p.lease, server: server, principal: b17Actor}
}
