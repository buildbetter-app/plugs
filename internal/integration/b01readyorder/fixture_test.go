package b01readyorder_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const project = "ready-order"

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

// fixtureKey gives stable fixture labels the required write-key encoding.
func fixtureKey(label string) string {
	sum := sha256.Sum256([]byte("b01readyorder/" + label))
	return fmt.Sprintf("%x", sum[:])
}

type fixture struct {
	ctx    context.Context
	clock  *testclock.Clock
	store  driver.Store
	lease  driver.ActiveLease
	svc    *service.Service
	server *rpc.Server
	onto   *service.Ontology
	serial int
	posts  atomic.Uint64
	sqlDSN string
}

type publicationProbe struct{ calls *atomic.Uint64 }

func (p publicationProbe) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return nil
}

func eachStore(t *testing.T, run func(*testing.T, *fixture)) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) { run(t, newFixture(t, engine)) })
	}
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	return newFixtureWithClocks(t, engine, testclock.New(t, time.Date(2026, 11, 3, 12, 0, 0, 123456000, time.UTC)), nil)
}

// A nil storage clock preserves the original fixture's clocks. An explicit
// storage clock is independent of the Writer/service clock, including the
// real SQL function reached by PostgreSQL's unchanged driver.
func newFixtureWithClocks(t *testing.T, engine string, application, storage *testclock.Clock) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, clock: application}
	driverClock := application
	if storage != nil {
		driverClock = storage
	}
	var err error
	switch engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: driverClock.Now})
	case "sqlite_wal":
		f.store, err = (sqlite.Driver{Clock: driverClock.Now}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "ready.db")})
	case "postgres":
		f.sqlDSN = testclock.PostgresDSN(t)
		if storage != nil {
			f.sqlDSN = storage.BindPostgres(t, f.sqlDSN)
		}
		f.store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: f.sqlDSN})
	default:
		t.Fatalf("unknown B01 storage engine %q", engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.lease != nil {
			if err := f.lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
		}
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	base := ontology.DefaultDocument()
	base.Statuses["custom_active"] = ontology.Status{Name: "Custom active", Category: ontology.Unstarted}
	workflow := base.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "custom_active")
	base.Workflows["issue"] = workflow
	issue := base.Types["issue"]
	issue.Name = "Other work"
	base.Types["other"] = issue
	for _, typ := range []string{"merge-request", "gate", "mole" + "cule", "r" + "ig", "agent", "role", "message", "epic"} {
		base.Types[typ] = issue
	}
	no := false
	base.Types["infrastructure"] = ontology.Type{Name: "Infrastructure", Workflow: "issue", ReadyEligible: &no}
	snapshot, problems := ontology.Compile(base, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("fixture ontology: %v", problems)
	}
	f.onto = service.NewOntology(snapshot.Effective)
	f.acquire(t)
	return f
}

func (f *fixture) acquire(t *testing.T) {
	t.Helper()
	var err error
	f.lease, err = f.store.Lease(f.ctx, "b01-ready-order", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	w := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.onto), pipeline.WithClock(f.clock.Now), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b01-publication", Subscriber: publicationProbe{&f.posts}}), pipeline.WithIDGenerator(func() (string, error) {
		f.serial++
		return fmt.Sprintf("pl-created-%04d", f.serial), nil
	}))
	f.svc = service.New(f.store, f.lease, w, service.WithOntology(f.onto), service.WithClock(f.clock.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.svc); err != nil {
		t.Fatal(err)
	}
	meta := f.meta(t)
	// Reacquiring a lease after a controlled clock advance must not rotate the
	// signing key and turn a live-order assertion into an HMAC refusal test.
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: []byte("b01readyorder-disposable-stable-key")})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) advance(t *testing.T, delta time.Duration) {
	t.Helper()
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	f.clock.Advance(t, delta)
	f.acquire(t)
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (f *fixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.meta(t).Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func document(id string, created time.Time, p int) map[string]any {
	return map[string]any{"id": id, "type": "issue", "project": project, "createdAt": created,
		"fields": map[string]any{"title": id, "priority": p, "status": "todo"}}
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f *fixture) seed(t *testing.T, docs ...map[string]any) {
	t.Helper()
	changes := make([]conformance.FixtureChange, len(docs))
	for i, doc := range docs {
		changes[i] = conformance.FixtureChange{Ref: model.Ref{ID: doc["id"].(string)}, Kind: "bead", Op: model.OpCreate, Body: jsonBytes(t, doc)}
	}
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock.Now()}, changes); err != nil {
		t.Fatal(err)
	}
}

// Literal projection fixtures exercise the actual driver's persisted derived
// rows. They are not evidence that a production rank-maintenance job ran.
func (f *fixture) ranks(t *testing.T, rows ...driver.DerivedRow) {
	t.Helper()
	tx, err := f.store.Write(f.ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	locks := make([]driver.LockRequest, len(rows))
	for i, row := range rows {
		locks[i] = driver.LockRequest{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: row.Key}}, Mode: driver.LockUpdate}
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i].Key)
		b, _ := driver.EncodeLockKey(locks[j].Key)
		return a < b
	})
	if _, err := tx.Lock(f.ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(f.ctx, driver.DerivedSortKey, nil, rows); err != nil {
		t.Fatal(err)
	}
	keys := make([]driver.LockKey, len(locks))
	for i := range locks {
		keys[i] = locks[i].Key
	}
	if _, err := tx.Commit(f.ctx, driver.ChangesetHeader{Incarnation: f.meta(t).Incarnation, Time: f.clock.Now(), Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "rank-fixture"}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
}

func rankRow(t *testing.T, id string, created, ranked time.Time, revision model.Revision, bucket, rank int) driver.DerivedRow {
	t.Helper()
	return driver.DerivedRow{Key: "ready/" + id, Value: jsonBytes(t, map[string]any{"bucket": bucket, "rank": rank, "createdAt": created, "rankedAt": ranked, "revision": revision})}
}

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct{ Code model.ErrorCode } `json:"data"`
	} `json:"error"`
}

func (f *fixture) call(t *testing.T, method string, params map[string]any) reply {
	t.Helper()
	if method == "bead.ready" {
		meta, _ := params["_meta"].(map[string]any)
		if meta == nil {
			meta = map[string]any{"project": project}
			params["_meta"] = meta
		}
		meta["idempotencyKey"] = fixtureKey("b01-read-probe")
	}
	raw, err := inprocess.New(f.server).RoundTrip(f.ctx, jsonBytes(t, map[string]any{"jsonrpc": "2.0", "id": "b01-ready", "method": method, "params": params}))
	if err != nil {
		t.Fatal(err)
	}
	var response reply
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("invalid public response %s: %v", raw, err)
	}
	return response
}

type publicPage struct {
	Items []struct {
		ID       string         `json:"id"`
		Revision model.Revision `json:"revision"`
	} `json:"items"`
	NextCursor *string      `json:"nextCursor"`
	AsOf       service.AsOf `json:"asOf"`
}

func decodePage(t *testing.T, response reply) publicPage {
	t.Helper()
	if response.Error != nil {
		t.Fatalf("public ready refusal: %+v", response.Error)
	}
	var page publicPage
	if err := json.Unmarshal(response.Result, &page); err != nil || page.Items == nil || page.AsOf.Revision.Validate() != nil || page.AsOf.Incarnation == "" {
		t.Fatalf("invalid ready page %s: %v", response.Result, err)
	}
	return page
}

// Expected order is supplied as a literal scenario oracle, never computed by
// the production comparator. No sorting or normalization hides semantic order.
func (f *fixture) pages(t *testing.T, filter map[string]any, want ...string) {
	t.Helper()
	for _, limit := range []int{1, 2, 1000} {
		params := map[string]any{"limit": limit, "_meta": map[string]any{"project": project}}
		if filter != nil {
			params["filter"] = filter
		}
		got := []string{}
		seen := map[string]bool{}
		for {
			page := decodePage(t, f.call(t, "bead.ready", params))
			if len(page.Items) > limit {
				t.Fatal("public page exceeded its limit")
			}
			for _, item := range page.Items {
				if seen[item.ID] || item.Revision.Validate() != nil {
					t.Fatalf("repeated ID or invalid row revision: %+v", item)
				}
				seen[item.ID] = true
				got = append(got, item.ID)
			}
			if len(got) > len(want) {
				t.Fatalf("ready exceeded literal membership: got %v want %v", got, want)
			}
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) != limit || *page.NextCursor == "" {
				t.Fatal("short or empty intermediate public page")
			}
			params["cursor"] = *page.NextCursor
		}
		if !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("limit %d: ready order/membership got %v want %v", limit, got, want)
		}
	}
}

type durableState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Versions     map[string][]driver.Version
	Derived      map[driver.DerivedTable][]driver.DerivedRow
	Facts        []driver.LockState
	Receipts     map[string]*driver.IdempotencyRecord
	Log          []model.Changeset
	Publications uint64
}

func (f *fixture) state(t *testing.T) durableState {
	t.Helper()
	s := durableState{Meta: f.meta(t), Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]*driver.IdempotencyRecord{}, Publications: f.posts.Load()}
	r := f.reader(t)
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("fixture state query: %+v, %v", page, err)
	}
	s.Records = page.Items
	facts := map[string]driver.LockKey{}
	addFact := func(key driver.LockKey) {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		facts[encoded] = key
	}
	for _, sentinel := range []driver.Sentinel{driver.SentinelGraphBlocking, driver.SentinelOntology, driver.SentinelPlugins} {
		addFact(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	for _, record := range s.Records {
		ref := record.Ref
		addFact(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		closure, err := driver.NewClosureSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		addFact(driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
		addFact(driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
		var doc struct{ Project, Type string }
		if err := json.Unmarshal(record.Body, &doc); err != nil {
			t.Fatal(err)
		}
		ready, err := driver.NewReadySentinel(doc.Project, doc.Type)
		if err != nil {
			t.Fatal(err)
		}
		addFact(driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("fixture version query: %+v, %v", versions, err)
		}
		s.Versions[record.Ref.ID] = versions.Items
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		s.Derived[table] = rows
		for _, row := range rows {
			addFact(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
		}
	}
	encoded := make([]string, 0, len(facts))
	for key := range facts {
		encoded = append(encoded, key)
	}
	sort.Strings(encoded)
	keys := make([]driver.LockKey, len(encoded))
	for i, key := range encoded {
		keys[i] = facts[key]
	}
	s.Facts, err = r.Observe(f.ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []struct{ method, agent, key string }{{"bead.ready", "", "b01-read-probe"}, {"bead.create", "author", "b01-create"}, {"bead.update", "author", "b01-update"}} {
		receipt, err := r.IdempotencyByKey(f.ctx, principal, scope.agent, scope.method, fixtureKey(scope.key))
		if err != nil {
			t.Fatal(err)
		}
		s.Receipts[scope.method] = receipt
	}
	var changesCursor *driver.Cursor
	if !s.Meta.HistoryFloor.IsZero() {
		// A captured snapshot starts at its authenticated history floor; asking
		// for the discarded prefix is a truthful truncation refusal.
		changesCursor = &driver.Cursor{Incarnation: s.Meta.Incarnation, Revision: s.Meta.HistoryFloor, Method: "changesets"}
	}
	s.Log, err = r.Changesets(f.ctx, changesCursor, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *fixture) unchanged(t *testing.T, run func()) {
	t.Helper()
	before := f.state(t)
	run()
	if after := f.state(t); !reflect.DeepEqual(before, after) {
		t.Fatalf("read/refusal changed durable state: before %+v after %+v", before, after)
	}
}
