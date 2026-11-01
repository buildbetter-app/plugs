package websocket

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

func TestOfferRefusesFullWriteQueue(t *testing.T) {
	queue := make(chan outbound, 1)
	queue <- outbound{kind: TextMessage, data: []byte("already queued")}
	if offer(context.Background(), queue, outbound{kind: TextMessage, data: []byte("slow peer")}) {
		t.Fatal("full write queue accepted an additional response")
	}
}

func TestServeConnectionStopsOnSlowSocket(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "slow-peer"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(server, authenticator, Config{
		MaxPendingReads:  8,
		MaxPendingWrites: 1,
		ReadTimeout:      10 * time.Second,
		WriteTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	rw := bufio.NewReadWriter(bufio.NewReader(serverConn), bufio.NewWriter(serverConn))
	done := make(chan struct{})
	go func() {
		handler.serveConnection(newConn(serverConn, rw, handler.cfg), context.Background(), func(ctx context.Context) (authn.Identity, error) {
			return authenticator.Admit(ctx, "")
		})
		close(done)
	}()

	request := []byte(`{"jsonrpc":"2.0","id":1,"method":"server.hello","params":{}}`)
	for i := 0; i < 3; i++ {
		if err := writeMaskedTestFrame(clientConn, request); err != nil {
			// The third write may race with the bounded refusal closing the
			// socket. The handler must still complete promptly below.
			break
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slow-consumer refusal left the handler blocked on a socket write")
	}
}

// completedResponseConn holds a successful network write just before Flush
// returns. The peer has received its response, but the writer still owns the
// connection. A subsequent close deadline lets that completed write drain.
type completedResponseConn struct {
	net.Conn
	flushed chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *completedResponseConn) Write(data []byte) (int, error) {
	n, err := c.Conn.Write(data)
	if err == nil {
		c.once.Do(func() {
			close(c.flushed)
			<-c.resume
		})
	}
	return n, err
}

func (c *completedResponseConn) SetWriteDeadline(deadline time.Time) error {
	select {
	case <-c.flushed:
		select {
		case c.resume <- struct{}{}:
		default:
		}
	default:
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func TestCloseFlushesPolicyFrameAfterInFlightResponse(t *testing.T) {
	server, client := net.Pipe()
	paused := &completedResponseConn{Conn: server, flushed: make(chan struct{}), resume: make(chan struct{}, 1)}
	t.Cleanup(func() {
		select {
		case paused.resume <- struct{}{}:
		default:
		}
		_ = client.Close()
		_ = server.Close()
	})
	config, err := (Config{}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	connection := newConn(paused, bufio.NewReadWriter(bufio.NewReader(paused), bufio.NewWriter(paused)), config)
	response := []byte(`{"jsonrpc":"2.0","id":"accepted","result":{}}`)
	written := make(chan error, 1)
	go func() { written <- connection.WriteMessage(context.Background(), TextMessage, response) }()
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	packet := make([]byte, 2+len(response))
	if _, err := io.ReadFull(client, packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet, append([]byte{0x81, byte(len(response))}, response...)) {
		t.Fatal("accepted response frame changed")
	}
	<-paused.flushed
	closed := make(chan error, 1)
	go func() {
		closed <- connection.Close(context.Background(), closePolicy, "authentication no longer valid")
	}()
	var header [2]byte
	if _, err := io.ReadFull(client, header[:]); err != nil {
		t.Fatalf("policy close was lost behind the completed response: %v", err)
	}
	payload := make([]byte, int(header[1]&0x7f))
	if _, err := io.ReadFull(client, payload); err != nil {
		t.Fatal(err)
	}
	if header[0] != 0x88 || header[1]&0x80 != 0 || len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != closePolicy {
		t.Fatalf("policy close frame changed: header=%x payload=%x", header, payload)
	}
	if err := <-written; err != nil {
		t.Fatalf("accepted response failed: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

type observedWriteConn struct {
	net.Conn
	started chan struct{}
}

func (c *observedWriteConn) Write(data []byte) (int, error) {
	select {
	case c.started <- struct{}{}:
	default:
	}
	return c.Conn.Write(data)
}

func TestCloseBoundsStalledDataWriter(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	observed := &observedWriteConn{Conn: server, started: make(chan struct{}, 1)}
	config, err := (Config{WriteTimeout: 10 * time.Second}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	connection := newConn(observed, bufio.NewReadWriter(bufio.NewReader(observed), bufio.NewWriter(observed)), config)
	written := make(chan error, 1)
	go func() {
		written <- connection.WriteMessage(context.Background(), TextMessage, []byte("stalled response"))
	}()
	<-observed.started
	closed := make(chan error, 1)
	go func() {
		closed <- connection.Close(context.Background(), closePolicy, "authentication no longer valid")
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("policy close waited for the stalled data write's ten-second timeout")
	}
	if err := <-written; err == nil {
		t.Fatal("stalled response unexpectedly reached a peer that never reads")
	}
}

func writeMaskedTestFrame(conn net.Conn, payload []byte) error {
	frame := []byte{0x81, 0x80 | byte(len(payload)), 1, 2, 3, 4}
	masked := append([]byte(nil), payload...)
	for i := range masked {
		masked[i] ^= []byte{1, 2, 3, 4}[i%4]
	}
	frame = append(frame, masked...)
	_, err := conn.Write(frame)
	return err
}
