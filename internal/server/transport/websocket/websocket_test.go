package websocket_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
)

type fixture struct {
	t       *testing.T
	server  *rpc.Server
	http    *httptest.Server
	handler *websocket.Handler
}

func newFixture(t *testing.T, config websocket.Config, register func(*rpc.Registry)) *fixture {
	t.Helper()
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if register != nil {
		register(registry)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "ws-fixture", OntologyRevision: "1"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := websocket.New(server, authenticator, config)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &fixture{t: t, server: server, handler: handler}
	fixture.http = httptest.NewServer(handler)
	t.Cleanup(fixture.http.Close)
	t.Cleanup(func() { _ = handler.Close() })
	return fixture
}

type client struct {
	conn net.Conn
	read *bufio.Reader
}

func (f *fixture) connect(t *testing.T, origin string) *client {
	t.Helper()
	parsed, err := url.Parse(f.http.URL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 16))
	request := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\nOrigin: %s\r\n\r\n", websocket.Path, parsed.Host, key, origin)
	if _, err := io.WriteString(conn, request); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		conn.Close()
		t.Fatalf("WebSocket handshake status=%d body=%s", response.StatusCode, body)
	}
	response.Body.Close()
	return &client{conn: conn, read: reader}
}

func (c *client) close() {
	_ = c.conn.Close()
}

func (c *client) write(opcode byte, payload []byte) error {
	const maskBit = byte(0x80)
	header := []byte{0x80 | opcode, maskBit}
	switch {
	case len(payload) <= 125:
		header[1] |= byte(len(payload))
	case len(payload) <= 65535:
		header[1] |= 126
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(len(payload)))
		header = append(header, ext[:]...)
	default:
		header[1] |= 127
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(len(payload)))
		header = append(header, ext[:]...)
	}
	header = append(header, 1, 2, 3, 4)
	masked := append([]byte(nil), payload...)
	for i := range masked {
		masked[i] ^= []byte{1, 2, 3, 4}[i%4]
	}
	header = append(header, masked...)
	_, err := c.conn.Write(header)
	return err
}

func (c *client) readFrame() (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(c.read, header[:]); err != nil {
		return 0, nil, err
	}
	length := uint64(header[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.read, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.read, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(c.read, payload); err != nil {
		return 0, nil, err
	}
	return header[0] & 0x0f, payload, nil
}

func (c *client) readClose(t *testing.T) uint16 {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	opcode, payload, err := c.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if opcode != 8 || len(payload) < 2 {
		t.Fatalf("close frame opcode=%d payload=%x", opcode, payload)
	}
	return binary.BigEndian.Uint16(payload[:2])
}

func TestWebSocketCarriesCanonicalRPCBytes(t *testing.T) {
	f := newFixture(t, websocket.Config{}, nil)
	c := f.connect(t, f.http.URL)
	defer c.close()

	request := []byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"server.hello\",\"params\":{}}")
	expected := f.server.Dispatch(rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), request)
	if err := c.write(1, request); err != nil {
		t.Fatal(err)
	}
	opcode, response, err := c.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if opcode != 1 || !bytes.Equal(response, expected) {
		t.Fatalf("websocket response opcode=%d body=%s expected=%s", opcode, response, expected)
	}

	notification := []byte("{\"jsonrpc\":\"2.0\",\"method\":\"bead.create\",\"params\":{}}")
	if err := c.write(1, notification); err != nil {
		t.Fatal(err)
	}
	opcode, response, err = c.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if opcode != 1 || !bytes.Contains(response, []byte("\"rpc.rejected\"")) || !bytes.Contains(response, []byte("\"notification_no_result\"")) {
		t.Fatalf("notification rejection=%s", response)
	}
}

func TestWebSocketAdmissionRejectsHostAndOrigin(t *testing.T) {
	f := newFixture(t, websocket.Config{}, nil)
	parsed, err := url.Parse(f.http.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		host   string
		origin string
	}{
		{name: "host", host: "example.invalid", origin: f.http.URL},
		{name: "origin", host: parsed.Host, origin: "http://example.invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, f.http.URL+websocket.Path, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Host = test.host
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Connection", "Upgrade")
			request.Header.Set("Sec-WebSocket-Version", "13")
			request.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x43}, 16)))
			request.Header.Set("Origin", test.origin)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusForbidden {
				t.Fatalf("status=%d", response.StatusCode)
			}
		})
	}
}

func TestWebSocketBoundsMalformedAndOversizeFrames(t *testing.T) {
	t.Run("unmasked", func(t *testing.T) {
		f := newFixture(t, websocket.Config{}, nil)
		c := f.connect(t, f.http.URL)
		defer c.close()
		_, _ = c.conn.Write([]byte{0x81, 0x01, 'x'})
		if got := c.readClose(t); got != 1002 {
			t.Fatalf("close=%d", got)
		}
	})
	t.Run("oversize", func(t *testing.T) {
		f := newFixture(t, websocket.Config{MaxFrameBytes: 8, MaxMessageBytes: 8}, nil)
		c := f.connect(t, f.http.URL)
		defer c.close()
		if err := c.write(1, bytes.Repeat([]byte{'x'}, 9)); err != nil {
			t.Fatal(err)
		}
		if got := c.readClose(t); got != 1009 {
			t.Fatalf("close=%d", got)
		}
	})
}

func TestWebSocketRejectsNonMinimalLengthsBeforeDispatch(t *testing.T) {
	const request = `{"jsonrpc":"2.0","id":9007199254740993,"method":"bead.get","params":{"id":{"id":"pl-framing"}}}`
	const result = `{"bead":{"id":"pl-framing","type":"task","revision":1},"attachments":[]}`
	// These fixed malformed headers carry an otherwise valid, bounded call.
	// RFC 6455 section 5.2 requires the shortest possible length encoding.
	for _, test := range []struct {
		name          string
		header        []byte
		body          string
		recoveryBytes int
	}{
		{"16-bit for short request", []byte{0x81, 0xfe, 0, byte(len(request)), 0, 0, 0, 0}, request, len(request)},
		{"64-bit for short request", []byte{0x81, 0xff, 0, 0, 0, 0, 0, 0, 0, byte(len(request)), 0, 0, 0, 0}, request, 65536},
		{"64-bit for 126 bytes", []byte{0x81, 0xff, 0, 0, 0, 0, 0, 0, 0, 126, 0, 0, 0, 0}, request + string(bytes.Repeat([]byte{' '}, 126-len(request))), 126},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			f := newFixture(t, websocket.Config{}, func(registry *rpc.Registry) {
				if err := registry.Register("bead.get", func(context.Context, rpc.Call) (any, error) {
					calls.Add(1)
					return json.RawMessage(result), nil
				}); err != nil {
					t.Fatal(err)
				}
			})
			malformed := f.connect(t, f.http.URL)
			defer malformed.close()
			if _, err := malformed.conn.Write(append(test.header, test.body...)); err != nil {
				t.Fatal(err)
			}
			if err := malformed.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			opcode, response, err := malformed.readFrame()
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 0 || opcode != 8 || len(response) < 2 || binary.BigEndian.Uint16(response[:2]) != 1002 {
				t.Fatalf("malformed length: calls=%d opcode=%d response=%q; want no calls and close 1002", calls.Load(), opcode, response)
			}

			// A protocol error terminates its connection. The same listener must
			// still accept the call with a legitimate length encoding, including
			// the smallest payloads that require 16-bit and 64-bit lengths.
			recovered := f.connect(t, f.http.URL)
			defer recovered.close()
			if err := recovered.conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			valid := append([]byte(request), bytes.Repeat([]byte{' '}, test.recoveryBytes-len(request))...)
			if err := recovered.write(1, valid); err != nil {
				t.Fatal(err)
			}
			opcode, response, err = recovered.readFrame()
			var envelope struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      json.RawMessage `json:"id"`
				Result  json.RawMessage `json:"result"`
			}
			if err != nil || opcode != 1 || json.Unmarshal(response, &envelope) != nil || envelope.JSONRPC != "2.0" || string(envelope.ID) != "9007199254740993" || string(envelope.Result) != result || calls.Load() != 1 {
				t.Fatalf("recovery: calls=%d opcode=%d response=%s err=%v", calls.Load(), opcode, response, err)
			}
		})
	}
}

func TestWebSocketDisconnectCancelsInFlightDispatch(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	var startedOnce, canceledOnce sync.Once
	f := newFixture(t, websocket.Config{RequestTimeout: 10 * time.Second}, func(registry *rpc.Registry) {
		if err := registry.Register("bead.get", func(ctx context.Context, _ rpc.Call) (any, error) {
			startedOnce.Do(func() { close(started) })
			<-ctx.Done()
			canceledOnce.Do(func() { close(canceled) })
			return nil, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
	})
	c := f.connect(t, f.http.URL)
	if err := c.write(1, []byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"bead.get\",\"params\":{\"id\":{\"id\":\"pl-ws\"}}}")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	c.close()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect did not cancel dispatch context")
	}
}

func TestWebSocketMalformedCloseFrameIsBounded(t *testing.T) {
	f := newFixture(t, websocket.Config{}, nil)
	c := f.connect(t, f.http.URL)
	defer c.close()
	// A close payload with one byte is invalid and must not be handed to RPC.
	payload := []byte{1}
	frame := []byte{0x88, 0x80 | byte(len(payload)), 1, 2, 3, 4, payload[0] ^ 1}
	if _, err := c.conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	if got := c.readClose(t); got != 1002 {
		t.Fatalf("close=%d", got)
	}
}

func TestWebSocketPeerCloseDoesNotLeakConnection(t *testing.T) {
	f := newFixture(t, websocket.Config{}, nil)
	c := f.connect(t, f.http.URL)
	if err := c.write(8, []byte{0x03, 0xe8}); err != nil {
		t.Fatal(err)
	}
	if got := c.readClose(t); got != 1000 {
		t.Fatalf("close=%d", got)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := c.readFrame()
	if err == nil || !errors.Is(err, io.EOF) {
		t.Fatalf("connection remained open: %v", err)
	}
}
