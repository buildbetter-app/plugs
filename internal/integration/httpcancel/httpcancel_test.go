package httpcancel_test

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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresTestDSN string

// The acceptance package owns its disposable PostgreSQL engine. A missing or
// unreachable engine is a test failure: the matrix must prove both SQL paths.
func TestMain(m *testing.M) {
	code := func() int {
		id, err := randomID()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-v3-http-cancel-acceptance-" + id
		if output, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage); err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, output)
			return 1
		}
		defer func() {
			if output, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup failed: %v %s\n", err, output)
			}
		}()
		output, err := docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL port lookup failed: %v %s\n", err, output)
			return 1
		}
		addr := strings.TrimSpace(string(output))
		host, _, err := netSplitHostPort(addr)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "acceptance PostgreSQL must bind loopback")
			return 1
		}
		postgresTestDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresTestDSN)
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

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]) + fmt.Sprintf("-%d", os.Getpid()), nil
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

func netSplitHostPort(value string) (string, string, error) {
	last := strings.LastIndexByte(value, ':')
	if last <= 0 || last == len(value)-1 {
		return "", "", fmt.Errorf("invalid host:port %q", value)
	}
	return value[:last], value[last+1:], nil
}

type engine struct {
	name string
	open func(context.Context, string) (driver.Store, error)
}

func engines() []engine {
	return []engine{
		{name: "sqlite", open: func(ctx context.Context, dsn string) (driver.Store, error) {
			return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn})
		}},
		{name: "postgres", open: func(ctx context.Context, dsn string) (driver.Store, error) {
			return postgres.Open(ctx, driver.Config{DSN: dsn})
		}},
	}
}

type sqlDatabase struct {
	engine engine
	dsn    string
}

func newDatabase(t *testing.T, e engine) sqlDatabase {
	t.Helper()
	if e.name == "sqlite" {
		return sqlDatabase{engine: e, dsn: filepath.Join(t.TempDir(), "plug.db")}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	id, err := randomID()
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	databaseName := "plug_http_cancel_" + strings.ReplaceAll(id, "-", "")
	if _, err = admin.ExecContext(ctx, `CREATE DATABASE "`+databaseName+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, cleanupErr := admin.ExecContext(cleanupCtx, `DROP DATABASE "`+databaseName+`" WITH (FORCE)`); cleanupErr != nil {
			t.Errorf("drop acceptance database: %v", cleanupErr)
		}
		_ = admin.Close()
	})
	parsed, err := url.Parse(postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + databaseName
	return sqlDatabase{engine: e, dsn: parsed.String()}
}

type preBlocker struct {
	started chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (p *preBlocker) Mutate(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		close(p.done)
		return nil, ctx.Err()
	}
}

type postProbe struct {
	started      chan struct{}
	release      chan struct{}
	finished     chan struct{}
	onceStarted  sync.Once
	onceFinished sync.Once
	calls        atomic.Int32
	key          atomic.Value
	revision     atomic.Value
}

func newPostProbe() *postProbe {
	return &postProbe{started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
}

func (p *postProbe) After(ctx context.Context, change port.CommittedChange, _ port.SnapshotReader) error {
	p.calls.Add(1)
	p.key.Store(change.RequestID)
	p.revision.Store(change.Revision.String())
	p.onceStarted.Do(func() { close(p.started) })
	defer p.onceFinished.Do(func() { close(p.finished) })
	select {
	case <-p.release:
		return errors.New("acceptance post subscriber failed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

type host struct {
	db        sqlDatabase
	store     driver.Store
	lease     driver.ActiveLease
	server    *httptest.Server
	transport *httpTransport
	post      *postProbe
}

func newHost(t *testing.T, db sqlDatabase, pre *preBlocker, post *postProbe) *host {
	t.Helper()
	ctx := context.Background()
	store, err := db.engine.open(ctx, db.dsn)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "http-cancel-host", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{pipeline.WithOntology(ontology)}
	if pre != nil {
		options = append(options, pipeline.WithMutators(pipeline.MutatorBinding{ID: "http-cancel-pre", Mutator: pre}))
	}
	if post != nil {
		options = append(options, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "http-cancel-post", Subscriber: post}))
	}
	writer := pipeline.New(store, lease, options...)
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	rpcServer, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "http-cancel", OntologyRevision: model.Natural64(1)})
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	handler, err := httpjson.New(rpcServer, authenticator, httpjson.Config{RequestTimeout: 5 * time.Second})
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	h := &host{db: db, store: store, lease: lease, server: server, post: post}
	h.transport = &httpTransport{client: server.Client(), endpoint: server.URL + httpjson.Path}
	t.Cleanup(func() {
		server.Close()
		if err := lease.Release(context.Background()); err != nil && !errors.Is(err, driver.ErrTxClosed) {
			t.Errorf("release %s lease: %v", db.engine.name, err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close %s store: %v", db.engine.name, err)
		}
	})
	return h
}

type httpTransport struct {
	client   *http.Client
	endpoint string
}

func (t *httpTransport) RoundTrip(ctx context.Context, data []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	return io.ReadAll(response.Body)
}

type callResult struct {
	result client.Result
	err    error
}

func callAsync(c *client.Client, ctx context.Context, method string, params map[string]any) <-chan callResult {
	done := make(chan callResult, 1)
	go func() {
		result, err := c.Call(ctx, method, params, "")
		done <- callResult{result: result, err: err}
	}()
	return done
}

type durableState struct {
	head       string
	recordBody string
	recordRev  string
	versions   int
	changesets []model.Changeset
	receipt    *driver.IdempotencyRecord
}

func readState(t *testing.T, store driver.Store, agent, method, key string, ref *model.Ref) durableState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := reader.IdempotencyByKey(ctx, principal, agent, method, key)
	if err != nil {
		t.Fatal(err)
	}
	changesets, err := reader.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	state := durableState{head: meta.Head.String(), changesets: changesets, receipt: receipt}
	if ref != nil {
		rows, err := reader.Get(ctx, []driver.Ref{*ref})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 {
			state.recordBody = string(rows[0].Body)
			state.recordRev = rows[0].Revision.String()
		}
		versions, err := reader.Versions(ctx, *ref, driver.PageReq{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		state.versions = len(versions.Items)
	}
	return state
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(message)
}

func commitFromResult(t *testing.T, result client.Result) model.Commit {
	t.Helper()
	var commit model.Commit
	if err := json.Unmarshal(result.JSON, &commit); err != nil {
		t.Fatal(err)
	}
	if len(commit.Results) != 1 {
		t.Fatalf("expected one committed result: %s", result.JSON)
	}
	return commit
}

func assertReceiptMatchesCommit(t *testing.T, receipt *driver.IdempotencyRecord, commit model.Commit) {
	t.Helper()
	if receipt == nil || receipt.ResponsePending {
		t.Fatalf("receipt is not final: %+v", receipt)
	}
	got, err := json.Marshal(receipt.Commit)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(commit)
	if err != nil {
		t.Fatal(err)
	}
	var gotValue, wantValue any
	decode := func(data []byte, into *any) error {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		return decoder.Decode(into)
	}
	if err = decode(got, &gotValue); err != nil {
		t.Fatal(err)
	}
	if err = decode(want, &wantValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("wire result differs from durable receipt: got=%s want=%s", got, want)
	}
}

func TestHTTPDeadlineBeforeCommitLeavesBothSQLStoresUnchanged(t *testing.T) {
	for _, e := range engines() {
		t.Run(e.name, func(t *testing.T) {
			db := newDatabase(t, e)
			pre := &preBlocker{started: make(chan struct{}), done: make(chan struct{})}
			h := newHost(t, db, pre, nil)
			c, err := client.New(h.transport, "http-cancel", "deadline-agent")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			params := map[string]any{"type": "issue", "fields": map[string]any{"title": "must not publish"}}
			started := callAsync(c, ctx, "bead.create", params)
			select {
			case <-pre.started:
			case <-time.After(time.Second):
				t.Fatal("pre subscriber did not begin before deadline")
			}
			// A real client deadline cancels the HTTP request while the pre phase
			// still owns no SQL transaction. The generated key is retained for a
			// later explicit retry even though this call has no response.
			<-ctx.Done()
			outcome := <-started
			if !errors.Is(outcome.err, context.DeadlineExceeded) || outcome.result.IdempotencyKey == "" {
				t.Fatalf("deadline call=%+v; expected client deadline cancellation and generated key", outcome)
			}
			select {
			case <-pre.done:
			case <-time.After(time.Second):
				t.Fatal("pre subscriber did not observe client deadline")
			}
			state := readState(t, h.store, "deadline-agent", "bead.create", outcome.result.IdempotencyKey, nil)
			if state.head != "0" || len(state.changesets) != 0 || state.receipt != nil {
				t.Fatalf("deadline before commit published durable state: %+v", state)
			}
			t.Logf("engine=%s deadline-before-commit key=%s head=%s changesets=%d receipt=%v", e.name, outcome.result.IdempotencyKey, state.head, len(state.changesets), state.receipt)
		})
	}
}

func TestHTTPDisconnectAfterCommitFinalizesAndReplaysGeneratedKey(t *testing.T) {
	for _, e := range engines() {
		t.Run(e.name, func(t *testing.T) {
			db := newDatabase(t, e)
			post := newPostProbe()
			h := newHost(t, db, nil, post)
			c, err := client.New(h.transport, "http-cancel", "disconnect-agent")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			params := map[string]any{"type": "issue", "fields": map[string]any{"title": "durable after disconnect"}}
			started := callAsync(c, ctx, "bead.create", params)
			select {
			case <-post.started:
			case <-time.After(2 * time.Second):
				t.Fatal("post subscriber did not begin after SQL commit")
			}
			key, _ := post.key.Load().(string)
			if key == "" {
				t.Fatal("post subscriber did not receive generated idempotency key")
			}
			pending := readState(t, h.store, "disconnect-agent", "bead.create", key, nil)
			if pending.head != "1" || len(pending.changesets) != 1 || pending.receipt == nil || !pending.receipt.ResponsePending {
				t.Fatalf("known committed request did not expose pending receipt: %+v", pending)
			}
			cancel()
			outcome := <-started
			if outcome.err == nil || outcome.result.IdempotencyKey != key {
				t.Fatalf("disconnect call=%+v; expected lost response with retained key", outcome)
			}
			close(post.release)
			waitFor(t, 2*time.Second, func() bool {
				return readState(t, h.store, "disconnect-agent", "bead.create", key, nil).receipt != nil && !readState(t, h.store, "disconnect-agent", "bead.create", key, nil).receipt.ResponsePending
			}, "post finalization did not become durable")
			final := readState(t, h.store, "disconnect-agent", "bead.create", key, nil)
			if final.head != "1" || len(final.changesets) != 1 || final.receipt == nil || final.receipt.ResponsePending || post.calls.Load() != 1 {
				t.Fatalf("disconnect final state=%+v callbackCalls=%d", final, post.calls.Load())
			}
			if len(final.receipt.Commit.Warnings) != 1 || final.receipt.Commit.Warnings[0].Code != "post_subscriber_failed" {
				t.Fatalf("post warning was not finalized: %+v", final.receipt.Commit.Warnings)
			}
			commit := final.receipt.Commit
			ref := commit.Results[0].Ref
			beforeReplay := readState(t, h.store, "disconnect-agent", "bead.create", key, &ref)
			replay, err := c.Call(context.Background(), "bead.create", params, key)
			if err != nil || !replay.Replayed || replay.IdempotencyKey != key {
				t.Fatalf("same-key replay=%+v err=%v", replay, err)
			}
			assertReceiptMatchesCommit(t, final.receipt, commitFromResult(t, replay))
			afterReplay := readState(t, h.store, "disconnect-agent", "bead.create", key, &ref)
			if !reflect.DeepEqual(beforeReplay, afterReplay) {
				t.Fatalf("replay changed SQL state/history/receipt: before=%+v after=%+v", beforeReplay, afterReplay)
			}
			if post.calls.Load() != 1 {
				t.Fatal("same-key replay reran post callback")
			}
			beforeReuse := afterReplay
			changedParams := map[string]any{"type": "issue", "fields": map[string]any{"title": "different request"}}
			_, reuseErr := c.Call(context.Background(), "bead.create", changedParams, key)
			var reused *wire.Error
			if !errors.As(reuseErr, &reused) || reused.Data.Code != "idempotency_key_reused" {
				t.Fatalf("same key with different parameters was not rejected: %v", reuseErr)
			}
			afterReuse := readState(t, h.store, "disconnect-agent", "bead.create", key, &ref)
			if !reflect.DeepEqual(beforeReuse, afterReuse) {
				t.Fatalf("idempotency mismatch changed SQL state: before=%+v after=%+v", beforeReuse, afterReuse)
			}
			t.Logf("engine=%s disconnect-after-commit key=%s revision=%s head=%s history=%d versions=%d callbackCalls=%d replayed=%t", e.name, key, commit.Revision, afterReplay.head, len(afterReplay.changesets), afterReplay.versions, post.calls.Load(), replay.Replayed)
		})
	}
}

func TestHTTPPendingReceiptFencesOldHostAndRecoversOnNewSQLLease(t *testing.T) {
	for _, e := range engines() {
		t.Run(e.name, func(t *testing.T) {
			db := newDatabase(t, e)
			post := newPostProbe()
			old := newHost(t, db, nil, post)
			c, err := client.New(old.transport, "http-cancel", "fence-agent")
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			params := map[string]any{"type": "issue", "fields": map[string]any{"title": "recover pending"}}
			started := callAsync(c, ctx, "bead.create", params)
			select {
			case <-post.started:
			case <-time.After(2 * time.Second):
				t.Fatal("post subscriber did not begin after SQL commit")
			}
			key, _ := post.key.Load().(string)
			revisionText, _ := post.revision.Load().(string)
			if key == "" || revisionText == "" {
				t.Fatalf("missing committed callback metadata key=%q revision=%q", key, revisionText)
			}
			pending := readState(t, old.store, "fence-agent", "bead.create", key, nil)
			if pending.receipt == nil || !pending.receipt.ResponsePending || pending.head != "1" || len(pending.changesets) != 1 {
				t.Fatalf("pending SQL receipt=%+v", pending)
			}
			if err := old.lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			outcome := <-started
			var remote *wire.Error
			if !errors.As(outcome.err, &remote) || remote.Data.Code != "busy" || outcome.result.IdempotencyKey != key {
				t.Fatalf("fenced host did not return durable busy outcome: result=%+v err=%v", outcome.result, outcome.err)
			}
			if remote.Data.Details["committedRevision"] == nil || remote.Data.Details["responsePending"] != true || remote.Data.Details["idempotencyKey"] != key {
				t.Fatalf("fenced busy details lost durable receipt identity: %+v", remote.Data.Details)
			}
			close(post.release)
			stale := readState(t, old.store, "fence-agent", "bead.create", key, nil)
			if stale.receipt == nil || !stale.receipt.ResponsePending || stale.head != "1" || len(stale.changesets) != 1 {
				t.Fatalf("stale host changed pending receipt: %+v", stale)
			}
			newHost := newHost(t, db, nil, nil)
			warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "post subscriber failed after commit", Details: map[string]any{"binding": "http-cancel-post", "revision": model.Natural(revisionText)}}}
			// Finalize through the new host's actual SQL lease. The old host was
			// fenced before it could clear the receipt, so no response is invented.
			revision := model.Natural(revisionText)
			if err := newHost.store.(driver.ResponseFinalizer).FinalizeResponse(context.Background(), newHost.lease, "anonymous:anonymous", "fence-agent", "bead.create", key, revision, warnings); err != nil {
				t.Fatal(err)
			}
			final := readState(t, newHost.store, "fence-agent", "bead.create", key, nil)
			if final.receipt == nil || final.receipt.ResponsePending || final.head != "1" || len(final.changesets) != 1 {
				t.Fatalf("new host did not finalize pending receipt: %+v", final)
			}
			ref := final.receipt.Commit.Results[0].Ref
			beforeReplay := readState(t, newHost.store, "fence-agent", "bead.create", key, &ref)
			replay, err := func() (client.Result, error) {
				newClient, createErr := client.New(newHost.transport, "http-cancel", "fence-agent")
				if createErr != nil {
					return client.Result{}, createErr
				}
				return newClient.Call(context.Background(), "bead.create", params, key)
			}()
			if err != nil || !replay.Replayed {
				t.Fatalf("replay after lease recovery=%+v err=%v", replay, err)
			}
			commit := commitFromResult(t, replay)
			assertReceiptMatchesCommit(t, final.receipt, commit)
			afterReplay := readState(t, newHost.store, "fence-agent", "bead.create", key, &commit.Results[0].Ref)
			if !reflect.DeepEqual(beforeReplay, afterReplay) {
				t.Fatalf("lease recovery replay changed SQL state: before=%+v after=%+v", beforeReplay, afterReplay)
			}
			if post.calls.Load() != 1 {
				t.Fatalf("lease recovery reran callback: %d", post.calls.Load())
			}
			t.Logf("engine=%s fenced-pending key=%s revision=%s oldPending=%t finalPending=%t history=%d callbackCalls=%d replayed=%t", e.name, key, revision, stale.receipt.ResponsePending, afterReplay.receipt.ResponsePending, len(afterReplay.changesets), post.calls.Load(), replay.Replayed)
		})
	}
}
