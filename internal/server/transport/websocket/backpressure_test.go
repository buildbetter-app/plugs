package websocket

import (
	"bufio"
	"context"
	"net"
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
		handler.serveConnection(newConn(serverConn, rw, handler.cfg), context.Background(), authn.Identity{})
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
