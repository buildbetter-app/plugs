package sqlreadersnapshots_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresTestDSN string
var postgresContainer string

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_HOST=") &&
			!strings.HasPrefix(value, "DOCKER_CONTEXT=") &&
			!strings.HasPrefix(value, "DOCKER_TLS=") &&
			!strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") &&
			!strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func randomSuffix() string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", bytes[:])
}

func TestMain(m *testing.M) {
	code := func() int {
		name := "plug-swarm-v3-sql-reader-snapshots-" + strconv.Itoa(os.Getpid()) + "-" + randomSuffix()
		out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name,
			"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
			"-e", "POSTGRES_DB=plug_snapshots_boot",
			"-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		postgresContainer = name
		defer func() {
			if out, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, out)
			}
		}()
		out, err = docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL port lookup failed: %v %s\n", err, out)
			return 1
		}
		addr := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(addr)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintf(os.Stderr, "real PostgreSQL must bind loopback, got %q\n", addr)
			return 1
		}
		postgresTestDSN = "postgres://postgres@" + addr + "/plug_snapshots_boot?sslmode=disable"
		readyContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for {
			store, openErr := postgres.Open(readyContext, driver.Config{DSN: postgresTestDSN})
			if openErr == nil {
				_ = store.Close()
				break
			}
			if readyContext.Err() != nil {
				cancel()
				fmt.Fprintf(os.Stderr, "real PostgreSQL never became ready: %v\n", openErr)
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		cancel()
		return m.Run()
	}()
	os.Exit(code)
}

type backend struct {
	name   string
	open   func(*testing.T, context.Context) (driver.Store, string)
	reopen func(*testing.T, context.Context, string) driver.Store
}

func TestSQLReadHandlesPinServiceAndDriverSnapshots(t *testing.T) {
	for _, backend := range []backend{
		{name: "sqlite", open: openSQLite, reopen: reopenSQLite},
		{name: "postgres", open: openPostgres, reopen: reopenPostgres},
	} {
		t.Run(backend.name, func(t *testing.T) {
			runSnapshotLifetime(t, backend)
		})
	}
}

func TestSQLLeaseEpochAndLifetimeRefusals(t *testing.T) {
	for _, backend := range []backend{
		{name: "sqlite", open: openSQLite, reopen: reopenSQLite},
		{name: "postgres", open: openPostgres, reopen: reopenPostgres},
	} {
		t.Run(backend.name, func(t *testing.T) {
			runLeaseLifetime(t, backend)
		})
	}
}

func openSQLite(t *testing.T, ctx context.Context) (driver.Store, string) {
	t.Helper()
	path := t.TempDir() + "/snapshots.db"
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	return store, path
}

func reopenSQLite(t *testing.T, ctx context.Context, path string) driver.Store {
	t.Helper()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openPostgres(t *testing.T, ctx context.Context) (driver.Store, string) {
	t.Helper()
	database := "plug_snapshots_" + randomSuffix()
	if out, err := docker("exec", postgresContainer, "createdb", "-U", "postgres", database); err != nil {
		t.Fatalf("create disposable PostgreSQL database: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("exec", postgresContainer, "dropdb", "-U", "postgres", "--if-exists", "--force", database); err != nil {
			t.Errorf("drop disposable PostgreSQL database: %v %s", err, out)
		}
	})
	dsn, err := url.Parse(postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Path = "/" + database
	store, err := postgres.Open(ctx, driver.Config{DSN: dsn.String()})
	if err != nil {
		t.Fatal(err)
	}
	return store, dsn.String()
}

func reopenPostgres(t *testing.T, ctx context.Context, dsn string) driver.Store {
	t.Helper()
	store, err := postgres.Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func runSnapshotLifetime(t *testing.T, backend backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, dsn := backend.open(t, ctx)
	lease, err := store.Lease(ctx, "snapshot-writer", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = lease.Release(context.Background())
		_ = store.Close()
	})
	principal := model.PrincipalRef{Kind: "user", ID: "snapshot-test"}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	svc := service.New(store, lease, writer, service.WithOntology(ontology))
	created, err := svc.Create(ctx, principal, service.CreateRequest{
		Type:   "issue",
		Fields: map[string]any{"title": "before"},
	}, writeMeta())
	if err != nil || len(created.Results) != 1 {
		t.Fatalf("service create: %+v %v", created, err)
	}
	ref := created.Results[0].Ref
	initial, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Head.String() != "1" {
		t.Fatalf("service create head=%s", initial.Head)
	}
	initialRevision := initial.Head
	pinned, err := store.Read(ctx, driver.View{Incarnation: initial.Incarnation, At: &initialRevision})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := pinned.Snapshot(); snapshot.At == nil || snapshot.At.String() != "1" || snapshot.Incarnation != initial.Incarnation {
		t.Fatalf("driver reader was not pinned at revision 1: %+v", snapshot)
	}
	pinnedRecord, err := pinned.Get(ctx, []driver.Ref{ref})
	if err != nil || len(pinnedRecord) != 1 {
		t.Fatalf("pinned initial read: %+v %v", pinnedRecord, err)
	}
	initialBody := append([]byte(nil), pinnedRecord[0].Body...)
	if !strings.Contains(string(initialBody), "before") {
		t.Fatalf("initial record body=%s", initialBody)
	}
	serviceInitial, err := svc.Get(ctx, principal, service.GetRequest{ID: ref})
	if err != nil || serviceInitial.Bead.Fields["title"] != "before" {
		t.Fatalf("service initial read: %+v %v", serviceInitial, err)
	}

	updated, err := svc.Update(ctx, principal, service.UpdateRequest{ID: ref, Fields: map[string]any{"title": "after"}}, writeMeta())
	if err != nil || updated.Revision.String() != "2" {
		t.Fatalf("service update: %+v %v", updated, err)
	}
	createdLater, err := svc.Create(ctx, principal, service.CreateRequest{
		Type:   "issue",
		Fields: map[string]any{"title": "later"},
	}, writeMeta())
	if err != nil || createdLater.Revision.String() != "3" {
		t.Fatalf("second service write: %+v %v", createdLater, err)
	}
	current, err := store.Meta(ctx)
	if err != nil || current.Head.String() != "3" {
		t.Fatalf("ordinary writes did not advance head: %+v %v", current, err)
	}
	stillPinned, err := pinned.Get(ctx, []driver.Ref{ref, createdLater.Results[0].Ref})
	if err != nil || len(stillPinned) != 1 || string(stillPinned[0].Body) != string(initialBody) {
		t.Fatalf("driver snapshot moved or admitted later record: %+v %v", stillPinned, err)
	}
	if snapshot := pinned.Snapshot(); snapshot.At == nil || snapshot.At.String() != "1" {
		t.Fatalf("driver reader snapshot advanced: %+v", snapshot)
	}
	serviceAsOf, err := svc.Get(ctx, principal, service.GetRequest{ID: ref, AsOf: &service.AsOf{Revision: initialRevision, Incarnation: initial.Incarnation}})
	if err != nil || serviceAsOf.Bead.Fields["title"] != "before" || serviceAsOf.Bead.Revision.String() != "1" {
		t.Fatalf("service as-of read moved: %+v %v", serviceAsOf, err)
	}
	history, err := svc.History(ctx, principal, service.HistoryRequest{
		Project: "snapshot-project",
		AsOf:    service.AsOf{Revision: initialRevision, Incarnation: initial.Incarnation},
		Filter:  &service.Filter{Resources: []string{ref.ID}},
	})
	if err != nil || len(history.Items) != 1 || history.Items[0].Fields["title"] != "before" || history.Items[0].Revision.String() != "1" {
		t.Fatalf("service historical page moved: %+v %v", history, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pinned.Get(ctx, []driver.Ref{ref}); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("closed reader error=%v, want ErrTxClosed", err)
	}

	// Leave both a real read handle and a staged write open. Store.Close must
	// close the handle and roll back the transaction before the store is reused.
	autoClosed, err := store.Read(ctx, driver.View{Incarnation: current.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	staged, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	stagedRef := model.RefByID("staged-only", model.Natural64(0))
	lock := driver.LockKey{Kind: driver.LockRecord, Ref: &stagedRef}
	if _, err := staged.Lock(ctx, []driver.LockRequest{
		{Key: lock, Mode: driver.LockUpdate},
		{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := staged.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.PutSourceRecord(ctx, staged, driver.Record{Ref: stagedRef, Kind: "bead", Body: []byte(`{"id":"staged-only","type":"issue","fields":{}}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := autoClosed.Get(ctx, []driver.Ref{ref}); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("store-close reader error=%v, want ErrTxClosed", err)
	}
	if err := staged.Rollback(ctx); err != nil {
		t.Fatalf("store-close transaction rollback=%v", err)
	}
	if err := staged.Rollback(ctx); err != nil {
		t.Fatalf("store-close transaction second rollback=%v", err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("store.Close left the active lease live")
	}

	reopened := backend.reopen(t, ctx, dsn)
	reopenedLease, err := reopened.Lease(ctx, "snapshot-reopened", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedLease.Release(context.Background())
	defer reopened.Close()
	metaAfterReopen, err := reopened.Meta(ctx)
	if err != nil || metaAfterReopen.Head.String() != "3" || metaAfterReopen.Incarnation != initial.Incarnation {
		t.Fatalf("reopen metadata changed: %+v %v (dsn %s)", metaAfterReopen, err, dsn)
	}
	fresh, err := reopened.Read(ctx, driver.View{Incarnation: metaAfterReopen.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	freshRows, err := fresh.Get(ctx, []driver.Ref{ref, createdLater.Results[0].Ref, stagedRef})
	if err != nil || len(freshRows) != 2 || !strings.Contains(string(freshRows[0].Body), "after") || !strings.Contains(string(freshRows[1].Body), "later") {
		t.Fatalf("reopened current rows=%+v err=%v", freshRows, err)
	}
	oldAfterReopen, err := reopened.Read(ctx, driver.View{Incarnation: metaAfterReopen.Incarnation, At: &initialRevision})
	if err != nil {
		t.Fatal(err)
	}
	defer oldAfterReopen.Close()
	oldRows, err := oldAfterReopen.Get(ctx, []driver.Ref{ref, createdLater.Results[0].Ref, stagedRef})
	if err != nil || len(oldRows) != 1 || string(oldRows[0].Body) != string(initialBody) {
		t.Fatalf("reopened historical rows=%+v err=%v", oldRows, err)
	}
	t.Logf("%s: service revisions 1->2->3, direct reader stayed at revision 1, store close rolled back staged data, and reopen preserved exact historical bytes", backend.name)
}

func runLeaseLifetime(t *testing.T, backend backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, _ := backend.open(t, ctx)
	defer store.Close()
	short, err := store.Lease(ctx, "short-lived", 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = short.Release(context.Background())
		_ = store.Close()
	})
	select {
	case <-short.Lost():
	case <-ctx.Done():
		t.Fatal("short lease did not end", ctx.Err())
	}
	if err := short.Renew(ctx); !isFenced(err) {
		t.Fatalf("expired lease renew=%v, want ErrFenced", err)
	}
	if _, err := store.Write(ctx, short); !isFenced(err) {
		t.Fatalf("expired lease write=%v, want ErrFenced", err)
	}
	next, err := store.Lease(ctx, "new-epoch", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release(context.Background())
	if next.Epoch() <= short.Epoch() {
		t.Fatalf("lease epoch did not advance: old=%d new=%d", short.Epoch(), next.Epoch())
	}
	if _, err := store.Write(ctx, short); !isFenced(err) {
		t.Fatalf("stale epoch write=%v, want ErrFenced", err)
	}
	tx, err := store.Write(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Get(ctx, nil); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("rolled-back transaction read=%v, want ErrTxClosed", err)
	}
	t.Logf("%s: expired and stale lease epochs refused through Store.Write/Renew; active epoch admitted one transaction; rollback was idempotent", backend.name)
}

func isFenced(err error) bool {
	var fenced driver.ErrFenced
	return errors.As(err, &fenced)
}

func writeMeta() model.WriteMeta {
	return model.WriteMeta{Project: "snapshot-project", IdempotencyKey: randomSuffix() + randomSuffix()}
}
