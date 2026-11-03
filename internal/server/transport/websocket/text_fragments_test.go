package websocket_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type textFragmentStore struct {
	*memory.Store
	reads, writes atomic.Int64
}

func (s *textFragmentStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.reads.Add(1)
	return s.Store.Read(ctx, view)
}

func (s *textFragmentStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	return s.Store.Write(ctx, lease)
}

type textFragmentService struct {
	*service.Service
	creates atomic.Int64
}

func (s *textFragmentService) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	s.creates.Add(1)
	return s.Service.CreateWithOutcome(ctx, by, req, meta)
}

type textFragmentCall struct{ done chan struct{} }

type textFragmentFixture struct {
	http      *httptest.Server
	store     *textFragmentStore
	domain    *textFragmentService
	principal model.PrincipalRef
	token     string
	started   chan *textFragmentCall
}

func newTextFragmentFixture(t *testing.T) *textFragmentFixture {
	t.Helper()
	ctx := context.Background()
	f := &textFragmentFixture{
		store:     &textFragmentStore{Store: memory.New()},
		principal: model.PrincipalRef{Kind: "service", ID: "service:ws-text-fragments"},
		started:   make(chan *textFragmentCall, 1),
	}
	t.Cleanup(func() { _ = f.store.Close() })
	lease, err := f.store.Lease(ctx, "ws-text-fragments", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, lease, pipeline.WithOntology(ontology))
	f.domain = &textFragmentService{Service: service.New(f.store, lease, writer, service.WithOntology(ontology))}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	rpcServer, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "ws-text-fragments", AuthMode: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	keys := authnapikey.New(authnapikey.Config{})
	issued, err := keys.Issue(f.principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f.token = issued.Token
	authenticator, err := authn.New(authn.Config{Provider: keys})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := websocket.New(rpcServer, authenticator, websocket.Config{})
	if err != nil {
		t.Fatal(err)
	}
	f.http = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := &textFragmentCall{done: make(chan struct{})}
		f.started <- call
		defer close(call.done)
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		_ = handler.Close()
		f.http.Close()
	})
	return f
}

type textFragmentClient struct {
	*client
	done <-chan struct{}
}

func (f *textFragmentFixture) connect(t *testing.T) *textFragmentClient {
	t.Helper()
	parsed, err := url.Parse(f.http.URL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, f.http.URL+websocket.Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x44}, 16)))
	request.Header.Set("Origin", f.http.URL)
	request.Header.Set("Authorization", "Bearer "+f.token)
	if err := request.Write(conn); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("authenticated upgrade status=%d body=%s", response.StatusCode, body)
	}
	select {
	case call := <-f.started:
		return &textFragmentClient{client: &client{conn: conn, read: reader}, done: call.done}
	case <-time.After(2 * time.Second):
		t.Fatal("authenticated handler did not start")
		return nil
	}
}

func (c *textFragmentClient) waitDone(t *testing.T) {
	t.Helper()
	// A close frame precedes dispatch/writer joins. Wait for the actual
	// ServeHTTP return before checking that rejection had no late effects.
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket handler did not finish")
	}
}

func (c *textFragmentClient) writeFragment(t *testing.T, opcode byte, final bool, payload []byte) {
	t.Helper()
	header := []byte{opcode, 0x80}
	if final {
		header[0] |= 0x80
	}
	switch {
	case len(payload) <= 125:
		header[1] |= byte(len(payload))
	default:
		header[1] |= 126
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(payload)))
		header = append(header, length[:]...)
	}
	mask := [4]byte{5, 6, 7, 8}
	header = append(header, mask[:]...)
	for i, b := range payload {
		header = append(header, b^mask[i%len(mask)])
	}
	if _, err := c.conn.Write(header); err != nil {
		t.Fatal(err)
	}
}

func (c *textFragmentClient) ping(t *testing.T) {
	t.Helper()
	payload := []byte("between text fragments")
	c.writeFragment(t, 9, true, payload)
	opcode, pong, err := c.readFrame()
	if err != nil || opcode != 10 || !bytes.Equal(pong, payload) {
		t.Fatalf("interleaved pong opcode=%d payload=%q err=%v", opcode, pong, err)
	}
}

func textFragmentRequest(t *testing.T, id json.RawMessage, title, key string) []byte {
	t.Helper()
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": "bead.create",
		"params": map[string]any{"type": "issue", "fields": map[string]any{"title": title}, "_meta": map[string]any{"idempotencyKey": key}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

type textFragmentState struct {
	meta    driver.Meta
	records []driver.Record
	changes []model.Changeset
	receipt *driver.IdempotencyRecord
}

func (f *textFragmentFixture) state(t *testing.T, key string) textFragmentState {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect the real driver directly so these oracle reads do not count as
	// product work admitted by the transport.
	reader, err := f.store.Store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 100})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("fixture records err=%v cursor=%v", err, page.NextCursor)
	}
	changes, err := reader.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(f.principal)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := reader.IdempotencyByKey(ctx, principal, "", "bead.create", key)
	if err != nil {
		t.Fatal(err)
	}
	return textFragmentState{meta: meta, records: page.Items, changes: changes, receipt: receipt}
}

func (f *textFragmentFixture) assertCreate(t *testing.T, response, id []byte, title, key string) {
	t.Helper()
	if f.domain.creates.Load() != 1 || f.store.writes.Load() != 1 {
		t.Fatalf("real create calls=%d writes=%d", f.domain.creates.Load(), f.store.writes.Load())
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope) != 3 || string(envelope["jsonrpc"]) != `"2.0"` || !bytes.Equal(envelope["id"], id) || len(envelope["result"]) == 0 {
		t.Fatalf("raw create envelope changed: %s", response)
	}
	state := f.state(t, key)
	if state.meta.Head.String() != "1" || len(state.records) != 1 || len(state.changes) != 1 || len(state.changes[0].Changes) != 1 || state.changes[0].Changes[0].Op != model.OpCreate || !reflect.DeepEqual(state.changes[0].Principal, f.principal) {
		t.Fatalf("create did not persist exactly one authenticated operation: %+v", state)
	}
	if state.receipt == nil || state.receipt.ResponsePending || len(state.receipt.Commit.Results) != 1 || len(state.receipt.Commit.Warnings) != 0 {
		t.Fatalf("missing finalized create receipt: %+v", state.receipt)
	}
	want, err := json.Marshal(map[string]any{"revision": state.receipt.Commit.Revision, "results": state.receipt.Commit.Results, "warnings": state.receipt.Commit.Warnings})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(envelope["result"], want) {
		t.Fatalf("raw result changed from durable receipt: got=%s want=%s", envelope["result"], want)
	}
	var record struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(state.records[0].Body, &record); err != nil || record.Fields["title"] != title || state.records[0].Ref.ID != state.receipt.Commit.Results[0].Ref.ID {
		t.Fatalf("UTF-8 title/ref did not reach storage: body=%s err=%v", state.records[0].Body, err)
	}
}

func TestWebSocketFragmentedUTF8WithInterleavedPingExecutesOnce(t *testing.T) {
	f := newTextFragmentFixture(t)
	c := f.connect(t)
	id := json.RawMessage(`"fragment-\u0069d"`)
	const title, key = "café \U0001F331", "550e8400-e29b-41d4-a716-446655440000"
	request := textFragmentRequest(t, id, title, key)
	split := bytes.Index(request, []byte("é")) + 1
	if split < 1 || !utf8.Valid(request) || utf8.Valid(request[:split]) || utf8.Valid(request[split:]) {
		t.Fatal("fixture does not split actual UTF-8 inside a code point")
	}
	c.writeFragment(t, 1, false, request[:split])
	c.ping(t)
	if f.domain.creates.Load() != 0 || f.store.reads.Load() != 0 || f.store.writes.Load() != 0 {
		t.Fatal("incomplete text or ping reached the service/store")
	}
	c.writeFragment(t, 0, true, request[split:])
	opcode, response, err := c.readFrame()
	if err != nil || opcode != 1 {
		t.Fatalf("fragmented response opcode=%d body=%s err=%v", opcode, response, err)
	}
	if err := c.write(8, []byte{0x03, 0xe8}); err != nil {
		t.Fatal(err)
	}
	if got := c.readClose(t); got != 1000 {
		t.Fatalf("normal close=%d", got)
	}
	c.waitDone(t)
	f.assertCreate(t, response, id, title, key)
}

func TestWebSocketInvalidCompletedUTF8DoesNotDispatchAndRecovers(t *testing.T) {
	for _, fragmented := range []bool{false, true} {
		name := "complete"
		if fragmented {
			name = "fragmented"
		}
		t.Run(name, func(t *testing.T) {
			f := newTextFragmentFixture(t)
			id := json.RawMessage(`"recovery-\u0069d"`)
			const title, key = "recovered café \U0001F331", "550e8400-e29b-41d4-a716-446655440001"
			valid := textFragmentRequest(t, id, title, key)
			split := bytes.Index(valid, []byte("é")) + 1
			invalid := bytes.Clone(valid)
			invalid[split] = 'x' // A lead byte followed by ASCII, inside the JSON title.
			if utf8.Valid(invalid) || !json.Valid(invalid) || !utf8.Valid(valid) {
				t.Fatal("fixture must isolate invalid UTF-8 from JSON syntax")
			}
			before := f.state(t, key)
			c := f.connect(t)
			if fragmented {
				c.writeFragment(t, 1, false, invalid[:split])
				c.ping(t)
				c.writeFragment(t, 0, true, invalid[split:])
			} else {
				c.writeFragment(t, 1, true, invalid)
			}
			// Preserve the existing ErrProtocol -> 1002 mapping.
			if got := c.readClose(t); got != 1002 {
				t.Fatalf("invalid completed text close=%d", got)
			}
			c.waitDone(t)
			if f.domain.creates.Load() != 0 || f.store.reads.Load() != 0 || f.store.writes.Load() != 0 || !reflect.DeepEqual(before, f.state(t, key)) {
				t.Fatal("invalid completed text reached an operation or changed storage")
			}
			// Use the same listener and the same request key: refusal must not
			// mutate the store, consume the key, or poison later connections.
			recovery := f.connect(t)
			if err := recovery.write(1, valid); err != nil {
				t.Fatal(err)
			}
			opcode, response, err := recovery.readFrame()
			if err != nil || opcode != 1 {
				t.Fatalf("fresh connection response opcode=%d body=%s err=%v", opcode, response, err)
			}
			if err := recovery.write(8, []byte{0x03, 0xe8}); err != nil {
				t.Fatal(err)
			}
			if got := recovery.readClose(t); got != 1000 {
				t.Fatalf("recovery normal close=%d", got)
			}
			recovery.waitDone(t)
			f.assertCreate(t, response, id, title, key)
		})
	}
}
