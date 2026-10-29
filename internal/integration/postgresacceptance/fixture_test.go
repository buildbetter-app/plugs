package postgresacceptance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

const project = "postgres-acceptance"
const agent = "v3-postgres-client"

var actor = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
var clock = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func key(label string) string {
	sum := sha256.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

type ids struct {
	values []string
	used   atomic.Int64
}

func newIDs(values ...string) *ids { return &ids{values: values} }

func (s *ids) next() (string, error) {
	i := int(s.used.Add(1)) - 1
	if i >= len(s.values) {
		return "", fmt.Errorf("acceptance ID sequence exhausted at %d", i)
	}
	return s.values[i], nil
}

type frame struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

type trace struct {
	mu     sync.Mutex
	frames []frame
}

func newTrace(t *testing.T) *trace {
	t.Helper()
	r := &trace{}
	t.Cleanup(func() {
		dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
		if dir == "" {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		for _, f := range r.frames {
			if err := encoder.Encode(f); err != nil {
				t.Error(err)
				return
			}
		}
		path := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".jsonl")
		if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
			t.Error(err)
		}
	})
	return r
}

func openStore(t *testing.T, dsn string) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := postgres.Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// A host owns an actual pool, lease, service, ontology, Writer and HTTP server.
// Secondary handles are real postgres.Store values, never wrappers/adapters.
type host struct {
	store   *postgres.Store
	lease   driver.ActiveLease
	writer  *pipeline.Writer
	domain  *service.Service
	server  *httpjson.Server
	client  *http.Client
	url     string
	ctx     context.Context
	cancel  context.CancelFunc
	renewed chan error
	served  chan error
	once    sync.Once
	closed  error
	serial  atomic.Uint64
	trace   *trace
}

func startHost(t *testing.T, store *postgres.Store, sequence *ids, record *trace, extra ...pipeline.Option) *host {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &host{store: store, ctx: ctx, cancel: cancel, renewed: make(chan error, 1), served: make(chan error, 1), trace: record}
	leaseCtx, done := context.WithTimeout(ctx, 5*time.Second)
	var err error
	h.lease, err = store.Lease(leaseCtx, "v3-postgres-host", driver.LeaseTTL)
	done()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				h.renewed <- nil
				return
			case <-h.lease.Lost():
				h.renewed <- errors.New("acceptance host unexpectedly lost its active lease")
				return
			case <-ticker.C:
				renewCtx, done := context.WithTimeout(ctx, time.Second)
				err := h.lease.Renew(renewCtx)
				done()
				if err != nil {
					h.renewed <- err
					return
				}
			}
		}
	}()
	// Install cleanup before any subsequent setup can fail.
	t.Cleanup(func() {
		if err := h.close(); err != nil {
			t.Error(err)
		}
	})
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time { return clock }), pipeline.WithIDGenerator(sequence.next)}
	options = append(options, extra...)
	h.writer = pipeline.New(store, h.lease, options...)
	h.domain = service.New(store, h.lease, h.writer, service.WithOntology(ontology), service.WithClock(func() time.Time { return clock }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err = rpc.BindService(registry, h.domain); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic fixed signing material makes restart continuation observable.
	// It is local test data, not a production credential or configuration.
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: project, CursorKey: bytes.Repeat([]byte{0x31}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	h.server, err = handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.url = "http://" + h.server.Addr().String() + httpjson.Path
	h.client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}
	go func() { h.served <- h.server.Serve() }()
	return h
}

func (h *host) close() error {
	h.once.Do(func() {
		if h.client != nil {
			h.client.CloseIdleConnections()
		}
		if h.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			h.closed = errors.Join(h.closed, h.server.Shutdown(ctx))
			cancel()
			err := <-h.served
			if !errors.Is(err, http.ErrServerClosed) {
				h.closed = errors.Join(h.closed, err)
			}
		}
		h.cancel()
		h.closed = errors.Join(h.closed, <-h.renewed)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		h.closed = errors.Join(h.closed, h.lease.Release(ctx), h.store.Close())
	})
	return h.closed
}

type reply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *wire.Error     `json:"error"`
	Meta    *struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func (r reply) replayed() bool { return r.Meta != nil && r.Meta.Replayed }

func (h *host) exchange(method string, params map[string]any, requestKey, label string) (reply, error) {
	values := make(map[string]any, len(params)+1)
	for k, v := range params {
		values[k] = v
	}
	meta := map[string]any{"project": project, "agent": label}
	if requestKey != "" {
		meta["idempotencyKey"] = requestKey
	}
	values["_meta"] = meta
	id := h.serial.Add(1)
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": values})
	if err != nil {
		return reply{}, err
	}
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(request))
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := h.client.Do(req)
	if err != nil {
		return reply{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return reply{}, err
	}
	h.trace.mu.Lock()
	h.trace.frames = append(h.trace.frames, frame{request, append(json.RawMessage(nil), raw...)})
	h.trace.mu.Unlock()
	if response.StatusCode != http.StatusOK {
		return reply{}, fmt.Errorf("HTTP status %d: %s", response.StatusCode, raw)
	}
	var out reply
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	if out.JSONRPC != "2.0" || out.ID != id || (out.Result != nil) == (out.Error != nil) {
		return out, fmt.Errorf("invalid RPC envelope: %s", raw)
	}
	return out, nil
}

func (h *host) call(t *testing.T, method string, params map[string]any, requestKey string) reply {
	t.Helper()
	r, err := h.exchange(method, params, requestKey, agent)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func decode[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var out T
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

func committed(t *testing.T, r reply, revision uint64) model.Commit {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("expected commit at %d: %v", revision, r.Error)
	}
	out := decode[model.Commit](t, r.Result)
	if out.Revision.String() != fmt.Sprint(revision) || out.WouldCommit || r.replayed() {
		t.Fatalf("expected fresh commit at %d: %+v replay=%v", revision, out, r.replayed())
	}
	return out
}

func refused(t *testing.T, r reply, code string) {
	t.Helper()
	if r.Error == nil || r.Error.Data.Code != code || r.Result != nil || r.replayed() {
		t.Fatalf("expected %s refusal: %+v", code, r)
	}
}

func reader(t *testing.T, store driver.Store) driver.Reader {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func receipt(t *testing.T, store driver.Store, method, requestKey, label string) *driver.IdempotencyRecord {
	t.Helper()
	r := reader(t, store)
	defer r.Close()
	principal, err := driver.PrincipalKey(actor)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.IdempotencyByKey(context.Background(), principal, label, method, requestKey)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

type graphState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Changesets   []model.Changeset
	Versions     map[string][]driver.Version
	Observations []driver.LockState
}

// Compare whole stored images, every version/log entry and the relevant commit
// guard predicates. Receipt checks remain explicit because no-op receipts are
// durable metadata and correctly do not appear in graph history.
func graph(t *testing.T, store driver.Store, names ...string) graphState {
	t.Helper()
	ctx := context.Background()
	r := reader(t, store)
	defer r.Close()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("graph page: %+v %v", page, err)
	}
	sets, err := r.Changesets(ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	out := graphState{Meta: meta, Records: page.Items, Changesets: sets, Versions: map[string][]driver.Version{}}
	wanted := map[string]bool{}
	for _, name := range names {
		wanted[name] = true
	}
	for _, row := range page.Items {
		wanted[row.Ref.ID] = true
	}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock}
	ready, err := driver.NewReadySentinel(project, "issue")
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	for name := range wanted {
		id := ref(name)
		versions, err := r.Versions(ctx, id, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("versions %s: %+v %v", name, versions, err)
		}
		out.Versions[name] = versions.Items
		bead, err := driver.NewBeadSentinel(name)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &id}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	out.Observations, err = r.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func unchanged(t *testing.T, before, after graphState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal/no-op/replay changed graph history or predicates:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func ledger(t *testing.T, state graphState, head uint64, counts map[string]int) {
	t.Helper()
	if state.Meta.Head.String() != fmt.Sprint(head) || len(state.Changesets) != int(head) {
		t.Fatalf("head/log mismatch: head=%s log=%d want=%d", state.Meta.Head, len(state.Changesets), head)
	}
	for i, set := range state.Changesets {
		if set.Revision.String() != fmt.Sprint(i+1) || set.Incarnation != state.Meta.Incarnation || set.Kind != model.ChangesetWrite {
			t.Fatalf("non-gapless changeset %d: %+v", i, set)
		}
	}
	for name, want := range counts {
		if len(state.Versions[name]) != want {
			t.Fatalf("versions for %s=%d want=%d", name, len(state.Versions[name]), want)
		}
	}
}

type warningPost struct {
	calls       atomic.Int64
	badSnapshot atomic.Bool
}

func (p *warningPost) After(_ context.Context, change port.CommittedChange, r port.SnapshotReader) error {
	p.calls.Add(1)
	if r.Snapshot().At == nil || r.Snapshot().At.String() != change.Revision.String() {
		p.badSnapshot.Store(true)
		return errors.New("post snapshot does not match commit")
	}
	return errors.New("synthetic subscriber destination unavailable")
}
