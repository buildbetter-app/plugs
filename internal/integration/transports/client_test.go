package transports_test

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
)

// MCP has its own protocol envelope. Its plug_call arguments carry the same
// canonical method and raw params; only the MCP tool envelope is unwrapped.
func mcpRequest(t *testing.T, request []byte) []byte {
	t.Helper()
	var canonical struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(request, &canonical); err != nil {
		t.Fatal(err)
	}
	return marshal(t, map[string]any{"jsonrpc": "2.0", "id": canonical.ID, "method": "tools/call", "params": map[string]any{
		"name": "plug_call", "arguments": map[string]any{"method": canonical.Method, "params": canonical.Params},
	}})
}

func marshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f *fixture) connect(t *testing.T, change func(*http.Request)) (net.Conn, *bufio.Reader, int) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(f.url, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		connection.Close()
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, f.url+websocket.Path, nil)
	if err != nil {
		connection.Close()
		t.Fatal(err)
	}
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef")))
	request.Header.Set("Origin", f.url)
	if f.token != "" {
		request.Header.Set("Authorization", "Bearer "+f.token)
	}
	if change != nil {
		change(request)
	}
	if err := request.Write(connection); err != nil {
		connection.Close()
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		connection.Close()
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		response.Body.Close()
	}
	return connection, reader, response.StatusCode
}

func writeFrame(t *testing.T, output io.Writer, payload []byte) {
	t.Helper()
	frame := []byte{0x81, 0x80}
	if len(payload) < 126 {
		frame[1] |= byte(len(payload))
	} else if len(payload) <= 65535 {
		frame[1] |= 126
		frame = append(frame, byte(len(payload)>>8), byte(len(payload)))
	} else {
		t.Fatal("test frame exceeds fixture limit")
	}
	mask := []byte{1, 2, 3, 4}
	frame = append(frame, mask...)
	for i, value := range payload {
		frame = append(frame, value^mask[i%len(mask)])
	}
	if _, err := output.Write(frame); err != nil {
		t.Fatal(err)
	}
}

func readFrame(t *testing.T, input io.Reader) []byte {
	t.Helper()
	opcode, data := readServerFrame(t, input)
	if opcode != 1 {
		t.Fatalf("expected text frame, got opcode %d", opcode)
	}
	return data
}

func readServerFrame(t *testing.T, input io.Reader) (byte, []byte) {
	t.Helper()
	var header [2]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		t.Fatal(err)
	}
	if header[0]&0xf0 != 0x80 || header[1]&0x80 != 0 {
		t.Fatalf("expected unmasked final frame, got %x", header)
	}
	length := uint64(header[1] & 0x7f)
	if length == 126 {
		var extended [2]byte
		if _, err := io.ReadFull(input, extended[:]); err != nil {
			t.Fatal(err)
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	} else if length == 127 {
		var extended [8]byte
		if _, err := io.ReadFull(input, extended[:]); err != nil {
			t.Fatal(err)
		}
		length = binary.BigEndian.Uint64(extended[:])
	}
	if length > 2<<20 {
		t.Fatal("server frame exceeds fixture limit")
	}
	data := make([]byte, int(length))
	if _, err := io.ReadFull(input, data); err != nil {
		t.Fatal(err)
	}
	return header[0] & 0x0f, data
}
