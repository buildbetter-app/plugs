package watchsqlacceptance_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	watchProject = "watch-sql"
	watchSession = "watch-sql-session"
)

var watchPrincipal = model.PrincipalRef{Kind: "user", ID: "watch-reader"}

// policyAuthorization is deliberately small: it is a current policy port
// used by the real event Reader. It never changes committed SQL data and
// fails closed when the current grant is withdrawn.
type policyAuthorization struct{ allowed atomic.Bool }

func newPolicyAuthorization() *policyAuthorization {
	a := &policyAuthorization{}
	a.allowed.Store(true)
	return a
}

func (a *policyAuthorization) Read(_ context.Context, _ model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	if a.allowed.Load() {
		return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
	}
	return coreevents.Visibility{}, nil
}

func (a *policyAuthorization) ReadResource(context.Context, model.PrincipalRef, model.AuditResource) (coreevents.ResourceVisibility, error) {
	return coreevents.ResourceVisibility{Readable: a.allowed.Load()}, nil
}

type bindingState struct{ policy atomic.Value }

func newBindingState() *bindingState {
	b := &bindingState{}
	b.policy.Store("policy-1")
	return b
}

func (b *bindingState) Policy() string         { return b.policy.Load().(string) }
func (b *bindingState) SetPolicy(value string) { b.policy.Store(value) }

func (b *bindingState) Current(ctx context.Context, request watch.Request) (watch.Binding, error) {
	if err := ctx.Err(); err != nil {
		return watch.Binding{}, err
	}
	return watch.Binding{
		Principal:     request.Principal,
		Filter:        request.Filter,
		Ontology:      model.Natural64(1),
		PolicyVersion: b.Policy(),
	}, nil
}

type runtime struct {
	ctx         context.Context
	cancel      context.CancelFunc
	path        string
	store       driver.Store
	lease       driver.ActiveLease
	writer      *pipeline.Writer
	domain      *service.Service
	principal   model.PrincipalRef
	incarnation string
}

func openRuntime(t *testing.T, path string) *runtime {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "watch-sql-acceptance", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		cancel()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(context.Background())
		_ = store.Close()
		cancel()
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		return fmt.Sprintf("watch-sql-%d", nextID()), nil
	}))
	return &runtime{
		ctx:         ctx,
		cancel:      cancel,
		path:        path,
		store:       store,
		lease:       lease,
		writer:      writer,
		domain:      service.New(store, lease, writer, service.WithOntology(ontology)),
		principal:   watchPrincipal,
		incarnation: meta.Incarnation,
	}
}

var globalID atomic.Uint64

func nextID() uint64 { return globalID.Add(1) }

func (r *runtime) close(t *testing.T) {
	t.Helper()
	if r == nil {
		return
	}
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
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
}

func (r *runtime) create(t *testing.T, title string) model.Commit {
	t.Helper()
	seq := nextID()
	commit, err := r.domain.Create(r.ctx, r.principal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": title}}, model.WriteMeta{
		Project:        watchProject,
		IdempotencyKey: fmt.Sprintf("%032x", seq),
	})
	if err != nil {
		t.Fatalf("actual service create: %v", err)
	}
	return commit
}

func (r *runtime) update(t *testing.T, ref model.Ref, title string) model.Commit {
	t.Helper()
	seq := nextID()
	commit, err := r.domain.Update(r.ctx, r.principal, service.UpdateRequest{ID: ref, Fields: map[string]any{"title": title}}, model.WriteMeta{
		Project:        watchProject,
		IdempotencyKey: fmt.Sprintf("%032x", seq),
	})
	if err != nil {
		t.Fatalf("actual service update: %v", err)
	}
	return commit
}

func (r *runtime) createBatch(t *testing.T, count int) model.Commit {
	t.Helper()
	ops := make([]model.Op, 0, count)
	for i := 0; i < count; i++ {
		ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
			Type:    "issue",
			Project: watchProject,
			Fields:  map[string]any{"title": fmt.Sprintf("batch-%d", i)},
		}})
	}
	seq := nextID()
	commit, err := r.writer.Apply(r.ctx, r.principal, model.ChangeSet{Ops: ops}, model.WriteMeta{
		Method:         "tx.apply",
		Project:        watchProject,
		IdempotencyKey: fmt.Sprintf("%032x", seq),
	})
	if err != nil {
		t.Fatalf("actual writer batch: %v", err)
	}
	return commit
}

func assertSQLite(t *testing.T, path string) {
	t.Helper()
	readOnly := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", readOnly.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, journal string
	if err := db.QueryRow("SELECT sqlite_version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("SQLite journal mode=%q: %v", journal, err)
	}
	header, err := os.ReadFile(path)
	if err != nil || len(header) < 16 || string(header[:16]) != "SQLite format 3\x00" {
		t.Fatalf("not an actual SQLite file: %v", err)
	}
	t.Logf("actual SQLite %s in WAL mode at %s", version, path)
}

func assertPopulatedWAL(t *testing.T, path string) {
	t.Helper()
	wal, err := os.Stat(path + "-wal")
	if err != nil || !wal.Mode().IsRegular() || wal.Size() == 0 {
		t.Fatalf("missing populated SQLite WAL: %v %v", wal, err)
	}
	t.Logf("actual populated SQLite WAL is %d bytes", wal.Size())
}

type harness struct {
	runtime   *runtime
	auth      *policyAuthorization
	bindings  *bindingState
	lifetime  context.Context
	stop      context.CancelFunc
	manager   *watch.Manager
	adapter   *watchhttp.Adapter
	rpc       *rpc.Server
	server    *httptest.Server
	codec     *serverevents.CursorCodec
	principal model.PrincipalRef
	sessionID string
	closed    bool
	clock     func() time.Time
	queueSize int
	maxLag    time.Duration
	coalesce  time.Duration
	pageLimit int
}

type harnessOptions struct {
	clock     func() time.Time
	queueSize int
	maxLag    time.Duration
	coalesce  time.Duration
	pageLimit int
}

func newHarness(t *testing.T, r *runtime, auth *policyAuthorization, bindings *bindingState, options harnessOptions) *harness {
	t.Helper()
	if options.queueSize == 0 {
		options.queueSize = watch.DefaultQueueSize
	}
	if options.maxLag == 0 {
		options.maxLag = watch.DefaultMaxLag
	}
	if options.coalesce == 0 {
		options.coalesce = time.Millisecond
	}
	if options.pageLimit == 0 {
		options.pageLimit = watch.DefaultPageLimit
	}
	if options.clock == nil {
		options.clock = time.Now
	}
	codec, err := serverevents.NewCursorCodec([]byte("watch-sql-stable-cursor-key-2026"))
	if err != nil {
		t.Fatal(err)
	}
	lifetime, stop := context.WithCancel(r.ctx)
	reader := &coreevents.Reader{
		Log:           serverevents.Log{Store: r.store},
		Materializer:  serverevents.HistoryMaterializer{Store: r.store},
		Authorization: auth,
	}
	events := &serverevents.Service{Reader: reader, Cursors: codec}
	manager, err := watch.New(lifetime, watch.Config{
		Events:    events,
		Bindings:  bindings,
		QueueSize: options.queueSize,
		MaxLag:    options.maxLag,
		Coalesce:  options.coalesce,
		PageLimit: options.pageLimit,
		Clock:     options.clock,
	})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	adapter, err := watchhttp.New(manager, watchhttp.Config{Ontology: model.Natural64(1), PolicyVersion: bindings.Policy()})
	if err != nil {
		_ = manager.Close()
		stop()
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = manager.Close()
		stop()
		t.Fatal(err)
	}
	if err := adapter.Register(registry); err != nil {
		_ = manager.Close()
		stop()
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: r.incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte("watch-sql-stable-cursor-key-2026")})
	if err != nil {
		_ = manager.Close()
		stop()
		t.Fatal(err)
	}
	h := &harness{runtime: r, auth: auth, bindings: bindings, lifetime: lifetime, stop: stop, manager: manager, adapter: adapter, rpc: server, codec: codec, principal: r.principal, sessionID: watchSession, clock: options.clock, queueSize: options.queueSize, maxLag: options.maxLag, coalesce: options.coalesce, pageLimit: options.pageLimit}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := watchhttp.WithPrincipal(req.Context(), h.principal)
		ctx = watchhttp.WithSession(ctx, h.sessionID, h.lifetime)
		h.adapter.ServeHTTP(w, req.WithContext(ctx))
	}))
	return h
}

func (h *harness) close(t *testing.T) {
	t.Helper()
	if h == nil || h.closed {
		return
	}
	h.closed = true
	if h.server != nil {
		h.server.Close()
	}
	if h.manager != nil {
		if err := h.manager.Close(); err != nil {
			t.Error(err)
		}
	}
	if h.stop != nil {
		h.stop()
	}
}

type subscriptionResult struct {
	SubscriptionID string `json:"subscriptionId"`
	Cursor         string `json:"cursor"`
}

func (h *harness) subscribe(t *testing.T, from string) subscriptionResult {
	t.Helper()
	ctx := watchhttp.WithSession(context.Background(), h.sessionID, h.lifetime)
	ctx = rpc.WithPrincipal(ctx, h.principal)
	params := `{"filter":{}}`
	if from != "" {
		params = fmt.Sprintf(`{"filter":{},"from":%q}`, from)
	}
	response := h.rpc.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"event.subscribe","params":`+params+`}`))
	var envelope struct {
		Result subscriptionResult `json:"result"`
		Error  json.RawMessage    `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("subscribe response=%s: %v", response, err)
	}
	if len(envelope.Error) != 0 || envelope.Result.SubscriptionID == "" || envelope.Result.Cursor == "" {
		t.Fatalf("subscribe refused: %s", response)
	}
	return envelope.Result
}

func (h *harness) openDirect(t *testing.T, from string) *watch.Subscription {
	t.Helper()
	subscription, err := h.manager.Open(h.lifetime, watch.Request{
		SessionID:     h.sessionID,
		Method:        watch.MethodEventSubscribe,
		Principal:     h.principal,
		From:          from,
		Ontology:      model.Natural64(1),
		PolicyVersion: h.bindings.Policy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return subscription
}

type stream struct {
	response *http.Response
	cancel   context.CancelFunc
	decoder  *json.Decoder
}

func (h *harness) startStream(t *testing.T, subscriptionID string) *stream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+watchhttp.DefaultPath, bytes.NewReader([]byte(fmt.Sprintf(`{"subscriptionId":%q}`, subscriptionID))))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	response, err := h.server.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("watch stream request: %v", err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/x-ndjson" {
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		cancel()
		t.Fatalf("watch stream response status=%d content-type=%q body=%s", response.StatusCode, response.Header.Get("Content-Type"), body)
	}
	return &stream{response: response, cancel: cancel, decoder: json.NewDecoder(bufio.NewReader(response.Body))}
}

func (s *stream) next(t *testing.T) eventNotify {
	t.Helper()
	var value eventNotify
	if err := s.decoder.Decode(&value); err != nil {
		t.Fatalf("decode NDJSON: %v", err)
	}
	return value
}

func (s *stream) close() {
	if s == nil {
		return
	}
	s.cancel()
	_ = s.response.Body.Close()
}

type eventNotify struct {
	SubscriptionID string        `json:"subscriptionId"`
	Events         []model.Event `json:"events"`
	Cursor         string        `json:"cursor"`
}

func nextFrame(t *testing.T, sub *watch.Subscription, ctx context.Context) watch.Frame {
	t.Helper()
	frame, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("watch frame: %v", err)
	}
	return frame
}

func waitWatchCount(t *testing.T, manager *watch.Manager, session string, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if manager.WatchCount(session) == want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("watch count=%d want %d", manager.WatchCount(session), want)
		}
	}
}

func decodePosition(t *testing.T, h *harness, token string, policy string) coreevents.Position {
	t.Helper()
	binding, err := serverevents.BindingFor("event.subscribe", coreevents.Filter{}, h.principal, model.Natural64(1), policy)
	if err != nil {
		t.Fatal(err)
	}
	position, err := h.codec.Decode(token, binding)
	if err != nil {
		t.Fatalf("decode watch cursor: %v", err)
	}
	return position
}

func requireDurable(t *testing.T, event model.Event, revision, index, resource string) model.DurableEvent {
	t.Helper()
	if event.Durable == nil || event.Control != nil {
		t.Fatalf("want durable event: %+v", event)
	}
	durable := *event.Durable
	if durable.Revision.String() != revision || durable.Index.String() != index || durable.Resource != resource {
		t.Fatalf("durable=%+v want revision/index/resource=%s/%s/%s", durable, revision, index, resource)
	}
	return durable
}

func TestSQLiteWatchSnapshotDeltaReplayAcrossStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watch.db")
	auth := newPolicyAuthorization()
	bindings := newBindingState()
	r := openRuntime(t, path)
	h := newHarness(t, r, auth, bindings, harnessOptions{})

	initialMeta, err := r.store.Meta(r.ctx)
	if err != nil {
		t.Fatal(err)
	}
	zero := model.Natural64(0)
	pinned, err := r.store.Read(r.ctx, driver.View{Incarnation: initialMeta.Incarnation, At: &zero})
	if err != nil {
		t.Fatal(err)
	}

	opened := h.subscribe(t, "")
	stream := h.startStream(t, opened.SubscriptionID)
	snapshot := stream.next(t)
	if snapshot.SubscriptionID != opened.SubscriptionID || snapshot.Cursor != opened.Cursor || len(snapshot.Events) != 0 {
		t.Fatalf("snapshot notify=%+v opened=%+v", snapshot, opened)
	}

	first := r.create(t, "first")
	if first.Revision.String() != "1" || len(first.Results) != 1 {
		t.Fatalf("first service commit=%+v", first)
	}
	assertSQLite(t, path)
	assertPopulatedWAL(t, path)
	firstDelta := stream.next(t)
	if len(firstDelta.Events) != 1 || firstDelta.Events[0].Control == nil || firstDelta.Events[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("first delta notify=%+v", firstDelta)
	}
	deltaDetails, ok := firstDelta.Events[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || len(deltaDetails.Events) != 1 || deltaDetails.FromCursor != snapshot.Cursor || deltaDetails.ToCursor != firstDelta.Cursor {
		t.Fatalf("first delta details=%+v", firstDelta.Events[0].Control.Details)
	}
	firstRef := first.Results[0].Ref
	requireDurable(t, deltaDetails.Events[0], "1", "0", "bead/"+firstRef.ID)
	if pos := decodePosition(t, h, firstDelta.Cursor, bindings.Policy()); pos.Revision.String() != "1" || pos.Index.String() != "0" {
		t.Fatalf("first cursor position=%+v", pos)
	}

	batch := r.createBatch(t, 2)
	if batch.Revision.String() != "2" || len(batch.Results) != 2 {
		t.Fatalf("batch commit=%+v", batch)
	}
	secondDelta := stream.next(t)
	if len(secondDelta.Events) != 1 || secondDelta.Events[0].Control == nil || secondDelta.Events[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("second delta notify=%+v", secondDelta)
	}
	deltaDetails, ok = secondDelta.Events[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || len(deltaDetails.Events) != 2 || deltaDetails.FromCursor != firstDelta.Cursor || deltaDetails.ToCursor != secondDelta.Cursor {
		t.Fatalf("second delta details=%+v", secondDelta.Events[0].Control.Details)
	}
	for i, result := range batch.Results {
		requireDurable(t, deltaDetails.Events[i], "2", fmt.Sprint(i), "bead/"+result.Ref.ID)
	}
	lastCursor := secondDelta.Cursor
	if pos := decodePosition(t, h, lastCursor, bindings.Policy()); pos.Revision.String() != "2" || pos.Index.String() != "1" {
		t.Fatalf("batch cursor position=%+v", pos)
	}
	pinnedRows, err := pinned.Get(r.ctx, []driver.Ref{firstRef})
	if err != nil || len(pinnedRows) != 0 {
		t.Fatalf("pinned WAL reader changed after writes: rows=%+v err=%v", pinnedRows, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	stream.close()
	waitWatchCount(t, h.manager, h.sessionID, 0)
	h.close(t)

	oldID, oldIncarnation, oldEpoch := initialMeta.StoreID, initialMeta.Incarnation, r.lease.Epoch()
	r.close(t)
	r = openRuntime(t, path)
	defer r.close(t)
	after, err := r.store.Meta(r.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.StoreID != oldID || after.Incarnation != oldIncarnation || after.Head.String() != "2" || r.lease.Epoch() <= oldEpoch {
		t.Fatalf("SQL reopen changed identity/head or reused lease epoch: before=%+v epoch=%d after=%+v epoch=%d", initialMeta, oldEpoch, after, r.lease.Epoch())
	}

	// This is dispatcher reconstruction over one durable SQLite incarnation.
	// No separate process is started, so this test deliberately makes no
	// process-failover claim.
	h = newHarness(t, r, auth, bindings, harnessOptions{})
	defer h.close(t)
	replayedWrite := r.create(t, "after-reopen")
	if replayedWrite.Revision.String() != "3" {
		t.Fatalf("post-reopen service commit=%+v", replayedWrite)
	}
	reopened := h.subscribe(t, lastCursor)
	replayedStream := h.startStream(t, reopened.SubscriptionID)
	replayed := replayedStream.next(t)
	if replayed.SubscriptionID != reopened.SubscriptionID || len(replayed.Events) != 1 {
		t.Fatalf("replay notify=%+v", replayed)
	}
	replayedEvent := requireDurable(t, replayed.Events[0], "3", "0", "bead/"+replayedWrite.Results[0].Ref.ID)
	if replayedEvent.Cursor != replayed.Cursor {
		t.Fatalf("replayed event cursor=%q notify cursor=%q", replayedEvent.Cursor, replayed.Cursor)
	}
	if pos := decodePosition(t, h, replayed.Cursor, bindings.Policy()); pos.Revision.String() != "3" || pos.Index.String() != "0" {
		t.Fatalf("replayed cursor position=%+v", pos)
	}
	replayedStream.close()
	waitWatchCount(t, h.manager, h.sessionID, 0)
	t.Logf("SQLite/WAL watch snapshot, service delta, two-event revision/index order, HTTP NDJSON replay, and dispatcher reconstruction survived store reopen at incarnation %s", after.Incarnation)
}

func TestSQLiteWatchCurrentAuthorizationAndPolicyReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.db")
	auth := newPolicyAuthorization()
	bindings := newBindingState()
	r := openRuntime(t, path)
	defer r.close(t)
	h := newHarness(t, r, auth, bindings, harnessOptions{})
	defer h.close(t)

	visible := r.create(t, "visible")
	sub := h.openDirect(t, "")
	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
	defer cancel()
	initial := nextFrame(t, sub, ctx)
	if initial.Kind != watch.FrameSnapshot || len(initial.Events) != 1 {
		t.Fatalf("initial frame=%+v", initial)
	}
	requireDurable(t, initial.Events[0], visible.Revision.String(), "0", "bead/"+visible.Results[0].Ref.ID)

	// The event is committed while the current read policy is denied. The
	// actual Reader emits only visibility.lost for the resource previously
	// delivered to this watch; no stale before/after image crosses the boundary.
	updated := r.update(t, visible.Results[0].Ref, "changed")
	if updated.Revision.String() != "2" {
		t.Fatalf("visible update=%+v", updated)
	}
	// The production manager queues the actual SQL event before this policy
	// transition; the delivery revalidation below is the authorization seam.
	time.Sleep(25 * time.Millisecond)
	auth.allowed.Store(false)
	lost := nextFrame(t, sub, ctx)
	if lost.Kind != watch.FrameDelta || lost.Reason != "visibility_changed" || len(lost.Events) != 1 || lost.Events[0].Control == nil || lost.Events[0].Control.Type != model.EventVisibilityLost || lost.Events[0].Durable != nil {
		t.Fatalf("authorization reset frame=%+v", lost)
	}
	if _, err := sub.Next(ctx); !errors.Is(err, watch.ErrResetRequired) {
		t.Fatalf("authorization reset resumed old generation: %v", err)
	}

	// A policy generation change is an independent reset boundary even when no
	// durable event is pending. The reset cursor is bound to the new policy.
	auth.allowed.Store(true)
	policySub := h.openDirect(t, "")
	policySnapshot := nextFrame(t, policySub, ctx)
	if policySnapshot.Kind != watch.FrameSnapshot {
		t.Fatalf("policy snapshot=%+v", policySnapshot)
	}
	bindings.SetPolicy("policy-2")
	reset := nextFrame(t, policySub, ctx)
	if reset.Kind != watch.FrameReset || reset.Reason != "policy_changed" || len(reset.Events) != 0 || !reset.Terminal() {
		t.Fatalf("policy reset=%+v", reset)
	}
	if _, err := policySub.Next(ctx); !errors.Is(err, watch.ErrResetRequired) {
		t.Fatalf("policy reset resumed old generation: %v", err)
	}
	if _, err := h.codec.Decode(reset.Cursor, mustBinding(t, h, "policy-2")); err != nil {
		t.Fatalf("policy reset cursor not bound to current policy: %v", err)
	}
	t.Logf("current authorization emitted visibility.lost and required reopen; policy generation change emitted terminal policy_changed reset")
}

func mustBinding(t *testing.T, h *harness, policy string) serverevents.CursorBinding {
	t.Helper()
	binding, err := serverevents.BindingFor("event.subscribe", coreevents.Filter{}, h.principal, model.Natural64(1), policy)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func TestSQLiteWatchLagAndOverflowAreTerminalResets(t *testing.T) {
	t.Run("lag", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lag.db")
		auth := newPolicyAuthorization()
		bindings := newBindingState()
		epoch := time.Unix(1_000, 0)
		var calls atomic.Int64
		clock := func() time.Time {
			if calls.Add(1) <= 3 {
				return epoch
			}
			return epoch.Add(time.Second)
		}
		r := openRuntime(t, path)
		defer r.close(t)
		h := newHarness(t, r, auth, bindings, harnessOptions{clock: clock, maxLag: 100 * time.Millisecond})
		defer h.close(t)
		sub := h.openDirect(t, "")
		ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
		defer cancel()
		_ = nextFrame(t, sub, ctx)
		r.create(t, "lagged")
		lag := nextFrame(t, sub, ctx)
		if lag.Kind != watch.FrameReset || lag.Reason != "lag" || len(lag.Events) != 0 {
			t.Fatalf("lag reset=%+v", lag)
		}
		if _, err := sub.Next(ctx); !errors.Is(err, watch.ErrResetRequired) {
			t.Fatalf("lag reset resumed: %v", err)
		}
		t.Log("bounded MaxLag delivery produced a terminal reset without a stale durable payload")
	})

	t.Run("overflow", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "overflow.db")
		auth := newPolicyAuthorization()
		bindings := newBindingState()
		r := openRuntime(t, path)
		defer r.close(t)
		h := newHarness(t, r, auth, bindings, harnessOptions{queueSize: 1, pageLimit: 1})
		defer h.close(t)
		sub := h.openDirect(t, "")
		ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
		defer cancel()
		_ = nextFrame(t, sub, ctx)
		// PageLimit=1 and two separate durable revisions force two producer
		// steps. The first fills the one-frame queue; the second overflows it.
		r.create(t, "overflow-1")
		r.create(t, "overflow-2")
		time.Sleep(50 * time.Millisecond)
		overflow := nextFrame(t, sub, ctx)
		if overflow.Kind != watch.FrameReset || overflow.Reason != "overflow" || len(overflow.Events) != 0 {
			t.Fatalf("overflow reset=%+v", overflow)
		}
		if _, err := sub.Next(ctx); !errors.Is(err, watch.ErrResetRequired) {
			t.Fatalf("overflow reset resumed: %v", err)
		}
		t.Log("bounded one-frame queue discarded undelivered payloads and emitted a terminal overflow reset")
	})
}
