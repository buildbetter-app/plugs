package receiptconcurrency_test

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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresBaseDSN string

var (
	primaryActor = model.PrincipalRef{Kind: "user", ID: "receipt-primary"}
	otherActor   = model.PrincipalRef{Kind: "user", ID: "receipt-other"}
)

type receiptScope struct {
	principal string
	method    string
	key       string
}

type durableState struct {
	meta     driver.Meta
	records  []driver.Record
	log      []model.Changeset
	versions map[string][]driver.Version
	receipts map[receiptScope]*driver.IdempotencyRecord
}

type fixture struct {
	t       *testing.T
	ctx     context.Context
	kind    string
	path    string
	dsn     string
	store   driver.Store
	lease   driver.ActiveLease
	ids     atomic.Uint64
	clock   time.Time
	closed  bool
	closeMu sync.Mutex
}

type runtime struct {
	writer  *pipeline.Writer
	service *service.Service
}

// normalizeGate coordinates only the existing ontology callback. It does not
// alter preparation or publication, and is released before either production
// Writer reaches its transaction boundary.
type normalizeGate struct {
	mu      sync.Mutex
	arrived int
	ready   chan struct{}
	release chan struct{}
}

type coordinatedOntology struct {
	base port.Ontology
	gate *normalizeGate
}

type noConditionalAudit struct{}

func (noConditionalAudit) DecideConditional(context.Context, port.Change, port.SnapshotReader) (port.ConditionalDecision, error) {
	return port.ConditionalDecision{}, errors.New("unexpected conditional decision for create")
}

func (o coordinatedOntology) Normalize(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	if err := o.gate.enter(ctx); err != nil {
		return nil, err
	}
	return o.base.Normalize(ctx, p, r)
}

func (o coordinatedOntology) Validate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) error {
	return o.base.Validate(ctx, p, r)
}

func (g *normalizeGate) enter(ctx context.Context) error {
	g.mu.Lock()
	g.arrived++
	n := g.arrived
	if n == 2 {
		close(g.ready)
	}
	g.mu.Unlock()
	if n > 2 {
		return nil
	}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fixture) nextID() (string, error) {
	return fmt.Sprintf("pl-receipt-%d", f.ids.Add(1)), nil
}

func (f *fixture) openStore() {
	f.t.Helper()
	var err error
	switch f.kind {
	case "memory":
		if f.store == nil {
			f.store, err = memory.NewWithOptions(memory.Options{
				StoreID: "receipt-memory", Incarnation: "receipt-memory",
				Clock: func() time.Time { return f.clock },
			})
		}
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatalf("unknown fixture kind %q", f.kind)
	}
	if err != nil {
		f.t.Fatalf("open %s: %v", f.kind, err)
	}
	f.lease, err = f.store.Lease(f.ctx, "receipt-concurrency-"+f.kind, 10*time.Second)
	if err != nil {
		f.t.Fatalf("lease %s: %v", f.kind, err)
	}
}

func (f *fixture) close() {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	if f.lease != nil {
		_ = f.lease.Release(context.Background())
		f.lease = nil
	}
	if f.store != nil {
		_ = f.store.Close()
		f.store = nil
	}
}

func (f *fixture) reopen() {
	f.t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Fatal(err)
		}
		f.lease = nil
	}
	if f.kind != "memory" {
		if err := f.store.Close(); err != nil {
			f.t.Fatal(err)
		}
		f.store = nil
	}
	f.openStore()
}

func (f *fixture) newRuntime(gate *normalizeGate) *runtime {
	f.t.Helper()
	base := service.NewOntology(nil)
	var ontology port.Ontology = base
	if gate != nil {
		ontology = coordinatedOntology{base: base, gate: gate}
	}
	writer := pipeline.New(
		f.store,
		f.lease,
		pipeline.WithOntology(ontology),
		pipeline.WithOntologyAudit(noConditionalAudit{}),
		pipeline.WithIDGenerator(f.nextID),
		pipeline.WithClock(func() time.Time { return f.clock }),
	)
	return &runtime{
		writer:  writer,
		service: service.New(f.store, f.lease, writer, service.WithOntology(base), service.WithClock(func() time.Time { return f.clock })),
	}
}

func newFixture(t *testing.T, kind string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	f := &fixture{t: t, ctx: ctx, kind: kind, clock: time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC)}
	switch kind {
	case "sqlite":
		f.path = t.TempDir() + "/receipt-concurrency.db"
	case "postgres":
		f.dsn = freshPostgresDSN(t)
	}
	f.openStore()
	t.Cleanup(f.close)
	return f
}

func createRequest() service.CreateRequest {
	return service.CreateRequest{Type: "issue", Fields: map[string]any{
		"title":       "identical payload",
		"description": "one durable outcome",
	}}
}

func createChangeSet() model.ChangeSet {
	request := createRequest()
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Type: request.Type, Fields: request.Fields,
	}}}}
}

func createMeta(method, key string) model.WriteMeta {
	return model.WriteMeta{Method: method, Project: "receipt-test", IdempotencyKey: key}
}

func principalKey(t *testing.T, principal model.PrincipalRef) string {
	t.Helper()
	key, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func scopeFor(t *testing.T, principal model.PrincipalRef, method, key string) receiptScope {
	t.Helper()
	return receiptScope{principal: principalKey(t, principal), method: method, key: key}
}

func (f *fixture) snapshot(t *testing.T, scopes ...receiptScope) durableState {
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
	if err != nil {
		t.Fatal(err)
	}
	log, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	versions := make(map[string][]driver.Version, len(page.Items))
	for _, record := range page.Items {
		value, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		versions[record.Ref.ID] = value.Items
	}
	receipts := make(map[receiptScope]*driver.IdempotencyRecord, len(scopes))
	for _, scope := range scopes {
		value, err := r.IdempotencyByKey(f.ctx, scope.principal, "", scope.method, scope.key)
		if err != nil {
			t.Fatal(err)
		}
		if value != nil {
			copy := *value
			receipts[scope] = &copy
		}
	}
	return durableState{meta: meta, records: page.Items, log: log, versions: versions, receipts: receipts}
}

func commitJSON(t *testing.T, commit model.Commit) []byte {
	t.Helper()
	data, err := json.Marshal(commit)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func errorCode(err error) model.ErrorCode {
	var value *model.Error
	if !errors.As(err, &value) {
		return ""
	}
	return value.Data.Code
}

func assertReceipt(t *testing.T, state durableState, scope receiptScope, actor model.PrincipalRef, want model.Commit) {
	t.Helper()
	receipt := state.receipts[scope]
	if receipt == nil {
		t.Fatalf("missing receipt for %+v", scope)
	}
	if receipt.ResponsePending || receipt.RequestHash == "" {
		t.Fatalf("receipt is not final and hashed: %+v", receipt)
	}
	if !reflect.DeepEqual(receipt.Changeset.Principal, actor) || receipt.Changeset.IdempotencyKey != scope.key {
		t.Fatalf("receipt actor/key scope=%+v", receipt.Changeset)
	}
	if !bytesEqual(commitJSON(t, receipt.Commit), commitJSON(t, want)) {
		t.Fatalf("stored receipt changed: stored=%s want=%s", commitJSON(t, receipt.Commit), commitJSON(t, want))
	}
	if receipt.Changeset.Revision.String() != want.Revision.String() || len(receipt.Changeset.Changes) != 1 || len(receipt.Changeset.Events) != 1 {
		t.Fatalf("receipt effect=%+v", receipt.Changeset)
	}
	event := receipt.Changeset.Events[0]
	if event.Type != model.EventBeadCreated || event.Provenance.Method != scope.method || event.Provenance.Cause.Root != scope.key {
		t.Fatalf("receipt event provenance=%+v", event)
	}
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func TestConcurrentDurableReceiptAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t, kind)
			key := "0123456789abcdef0123456789abcdef"
			primaryScope := scopeFor(t, primaryActor, "bead.create", key)
			gate := &normalizeGate{ready: make(chan struct{}), release: make(chan struct{})}
			firstRuntime, secondRuntime := f.newRuntime(gate), f.newRuntime(gate)
			start := make(chan struct{})
			type result struct {
				out port.WriteOutcome
				err error
			}
			results := make(chan result, 2)
			for _, instance := range []*runtime{firstRuntime, secondRuntime} {
				go func(instance *runtime) {
					<-start
					out, err := instance.service.CreateWithOutcome(f.ctx, primaryActor, createRequest(), createMeta("", key))
					results <- result{out: out, err: err}
				}(instance)
			}
			close(start)
			select {
			case <-gate.ready:
			case <-time.After(5 * time.Second):
				t.Fatal("concurrent writes did not reach the shared normalization boundary")
			}
			close(gate.release)
			outcomes := []result{<-results, <-results}
			var winner, replay port.WriteOutcome
			for _, outcome := range outcomes {
				if outcome.err != nil {
					t.Fatalf("concurrent create: %v", outcome.err)
				}
				if outcome.out.Replayed {
					replay = outcome.out
				} else {
					winner = outcome.out
				}
			}
			if winner.Commit.Revision.String() != "1" || replay.Commit.Revision.String() != "1" || winner.Commit.Results[0].Ref.ID == "" || !bytesEqual(commitJSON(t, winner.Commit), commitJSON(t, replay.Commit)) {
				t.Fatalf("concurrent outcome winner=%+v replay=%+v", winner, replay)
			}
			state := f.snapshot(t, primaryScope)
			if state.meta.Head.String() != "1" || len(state.records) != 1 || len(state.log) != 1 || len(state.log[0].Changes) != 1 || len(state.log[0].Events) != 1 || len(state.versions[winner.Commit.Results[0].Ref.ID]) != 1 {
				t.Fatalf("duplicate durable effect: head=%s records=%d log=%d changes=%d events=%d versions=%d", state.meta.Head, len(state.records), len(state.log), len(state.log[0].Changes), len(state.log[0].Events), len(state.versions[winner.Commit.Results[0].Ref.ID]))
			}
			assertReceipt(t, state, primaryScope, primaryActor, winner.Commit)
			if wrong, err := f.store.Read(f.ctx, driver.View{Incarnation: state.meta.Incarnation}); err != nil {
				t.Fatal(err)
			} else {
				defer wrong.Close()
				otherScope := scopeFor(t, otherActor, "bead.create", key)
				if value, err := wrong.IdempotencyByKey(f.ctx, otherScope.principal, "", otherScope.method, otherScope.key); err != nil || value != nil {
					t.Fatalf("actor scope leaked before its write: %+v %v", value, err)
				}
				if value, err := wrong.IdempotencyByKey(f.ctx, primaryScope.principal, "", "bead.update", key); err != nil || value != nil {
					t.Fatalf("method scope leaked: %+v %v", value, err)
				}
			}

			beforeReopen := state
			f.reopen()
			reopened := f.newRuntime(nil)
			replayed, err := reopened.service.CreateWithOutcome(f.ctx, primaryActor, createRequest(), createMeta("", key))
			if err != nil || !replayed.Replayed || !bytesEqual(commitJSON(t, replayed.Commit), commitJSON(t, winner.Commit)) {
				t.Fatalf("reopen replay=%+v err=%v", replayed, err)
			}
			if after := f.snapshot(t, primaryScope); !reflect.DeepEqual(beforeReopen, after) {
				t.Fatalf("replay after reopen changed durable state\nbefore=%+v\nafter=%+v", beforeReopen, after)
			}

			otherScope := scopeFor(t, otherActor, "bead.create", key)
			other, err := reopened.service.CreateWithOutcome(f.ctx, otherActor, createRequest(), createMeta("", key))
			if err != nil || other.Replayed || other.Commit.Revision.String() != "2" || other.Commit.Results[0].Ref.ID == winner.Commit.Results[0].Ref.ID {
				t.Fatalf("actor-scoped write=%+v err=%v", other, err)
			}
			methodScope := scopeFor(t, primaryActor, "tx.apply", key)
			methodOutcome, err := reopened.writer.ApplyWithOutcome(f.ctx, primaryActor, createChangeSet(), createMeta("tx.apply", key))
			if err != nil || methodOutcome.Replayed || methodOutcome.Commit.Revision.String() != "3" {
				t.Fatalf("method-scoped write=%+v err=%v", methodOutcome, err)
			}
			state = f.snapshot(t, primaryScope, otherScope, methodScope)
			if state.meta.Head.String() != "3" || len(state.records) != 3 || len(state.log) != 3 {
				t.Fatalf("scope writes have wrong durable cardinality: %+v", state)
			}
			assertReceipt(t, state, primaryScope, primaryActor, winner.Commit)
			assertReceipt(t, state, otherScope, otherActor, other.Commit)
			assertReceipt(t, state, methodScope, primaryActor, methodOutcome.Commit)
			for i, entry := range state.log {
				if entry.Revision.String() != fmt.Sprint(i+1) || len(entry.Changes) != 1 || len(entry.Events) != 1 {
					t.Fatalf("log effect %d=%+v", i, entry)
				}
			}

			beforeRefusal := state
			changed := createRequest()
			changed.Fields["title"] = "different payload"
			refused, err := reopened.service.CreateWithOutcome(f.ctx, primaryActor, changed, createMeta("", key))
			if errorCode(err) != "idempotency_key_reused" || !reflect.DeepEqual(refused, port.WriteOutcome{}) {
				t.Fatalf("changed payload refusal=%+v err=%v", refused, err)
			}
			if after := f.snapshot(t, primaryScope, otherScope, methodScope); !reflect.DeepEqual(beforeRefusal, after) {
				t.Fatalf("refused key reuse changed durable state\nbefore=%+v\nafter=%+v", beforeRefusal, after)
			}
			t.Logf("%s: coordinated same-key writes produced one revision/log/version/event, exact post-reopen replay, actor/method scope separation, and conserved changed-payload refusal", kind)
		})
	}
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

func randomID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func TestMain(m *testing.M) {
	os.Exit(runTestMain(m))
}

// runTestMain returns only after its defers run. TestMain must call os.Exit
// after this helper returns so the disposable PostgreSQL container is removed
// on normal completion and every setup failure after startup.
func runTestMain(m *testing.M) int {
	name := "plug-swarm-v3-receipt-concurrency-" + randomID()
	defer func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			fmt.Fprintf(os.Stderr, "remove PostgreSQL: %v: %s\n", err, out)
		}
	}()

	out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start PostgreSQL: %v: %s\n", err, out)
		return 1
	}
	out, err = docker("port", name, "5432/tcp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "find PostgreSQL port: %v: %s\n", err, out)
		return 1
	}
	address := strings.TrimSpace(string(out))
	host, _, splitErr := net.SplitHostPort(address)
	if splitErr != nil || host != "127.0.0.1" {
		fmt.Fprintf(os.Stderr, "PostgreSQL must bind loopback: %q\n", address)
		return 1
	}
	postgresBaseDSN = "postgres://postgres@" + address + "/plug_test?sslmode=disable"
	db, err := sql.Open("pgx", postgresBaseDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open PostgreSQL probe: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			fmt.Fprintf(os.Stderr, "PostgreSQL did not become ready: %v\n", err)
			_ = db.Close()
			return 1
		}
		time.Sleep(30 * time.Millisecond)
	}
	var version string
	if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		fmt.Fprintf(os.Stderr, "probe PostgreSQL version: %v\n", err)
		_ = db.Close()
		return 1
	}
	_ = db.Close()
	fmt.Fprintf(os.Stderr, "real PostgreSQL receipt-concurrency engine: %s\n", version)
	return m.Run()
}

func freshPostgresDSN(t *testing.T) string {
	t.Helper()
	admin, err := sql.Open("pgx", postgresBaseDSN)
	if err != nil {
		t.Fatal(err)
	}
	name := "plug_receipt_" + randomID()
	if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Errorf("drop PostgreSQL database %s: %v", name, err)
		}
		_ = admin.Close()
	})
	parsed, err := url.Parse(postgresBaseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}
