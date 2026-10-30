package stdio_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/server/transport/stdio"
)

func newServer(t *testing.T) *rpc.Server {
	t.Helper()
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "stdio-test", DefaultProject: "stdio"})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestRequestBytesAndErrorsMatchInProcess(t *testing.T) {
	server := newServer(t)
	transport, err := stdio.New(server, stdio.Config{})
	if err != nil {
		t.Fatal(err)
	}
	local := inprocess.New(server)
	requests := [][]byte{
		[]byte(`{"jsonrpc":"2.0","id":9007199254740993,"method":"server.hello"}`),
		[]byte(`{"jsonrpc":"2.0","id":"escaped\u002did","method":"server.hello","params":{}}`),
		[]byte(`{`),
		[]byte(`[]`),
	}
	var input bytes.Buffer
	for _, request := range requests {
		input.Write(request)
		input.WriteByte('\n')
	}
	var output bytes.Buffer
	if err := transport.Serve(context.Background(), &input, &output); err != nil {
		t.Fatal(err)
	}
	frames := bytes.Split(bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), []byte{'\n'})
	if len(frames) != len(requests) {
		t.Fatalf("stdio frames=%d want=%d output=%q", len(frames), len(requests), output.String())
	}
	for i, request := range requests {
		want, err := local.RoundTrip(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		got := frames[i]
		if isError(want) {
			if !sameError(want, got) {
				t.Fatalf("request %q changed error semantics\nin-process: %s\nstdio: %s", request, want, got)
			}
			continue
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("request %q changed response bytes\nin-process: %s\nstdio: %s", request, want, got)
		}
	}
}

func TestMalformedAndOversizeFramesAreTypedAndBounded(t *testing.T) {
	server := newServer(t)
	transport, err := stdio.New(server, stdio.Config{MaxFrameBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	valid := []byte(`{"jsonrpc":"2.0","id":1,"method":"server.hello"}`)
	var input bytes.Buffer
	input.Write(bytes.Repeat([]byte("x"), 129))
	input.WriteByte('\n')
	input.Write(valid)
	input.WriteByte('\n')
	input.WriteByte('{')
	input.WriteByte('\n')
	var output bytes.Buffer
	if err := transport.Serve(context.Background(), &input, &output); err != nil {
		t.Fatal(err)
	}
	frames := bytes.Split(bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), []byte{'\n'})
	if len(frames) != 3 {
		t.Fatalf("frames=%d output=%q", len(frames), output.String())
	}
	assertErrorCode(t, frames[0], "invalid_request")
	assertResult(t, frames[1], "server.hello")
	assertErrorCode(t, frames[2], "parse_error")
}

func TestCleanEOFAcceptsFinalFrameWithoutNewline(t *testing.T) {
	server := newServer(t)
	transport, err := stdio.New(server, stdio.Config{})
	if err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"jsonrpc":"2.0","id":1,"method":"server.hello"}`)
	var output bytes.Buffer
	if err := transport.Serve(context.Background(), bytes.NewReader(request), &output); err != nil {
		t.Fatal(err)
	}
	if got := bytes.TrimSuffix(output.Bytes(), []byte{'\n'}); !bytes.Equal(got, requestResponse(t, server, request)) {
		t.Fatalf("final frame response=%s", got)
	}
	output.Reset()
	if err := transport.Serve(context.Background(), bytes.NewReader(nil), &output); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("clean EOF emitted output: %q", output.String())
	}
}

func TestCancellationClosesBlockedInput(t *testing.T) {
	reader := newBlockingReader()
	transport, err := stdio.New(&echoDispatcher{}, stdio.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Serve(ctx, reader, io.Discard) }()
	cancel()
	select {
	case <-reader.closed:
	case <-time.After(time.Second):
		t.Fatal("stdio did not close blocked input on cancellation")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve error=%v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stdio Serve did not stop after cancellation")
	}
}

func TestCancellationDuringDispatchReturnsWithoutDetachedWork(t *testing.T) {
	dispatcher := &blockingDispatcher{started: make(chan struct{}), canceled: make(chan struct{})}
	transport, err := stdio.New(dispatcher, stdio.Config{RequestTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		input := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"server.hello"}` + "\n")
		done <- transport.Serve(ctx, input, io.Discard)
	}()
	select {
	case <-dispatcher.started:
	case <-time.After(time.Second):
		t.Fatal("dispatcher was not called")
	}
	cancel()
	select {
	case <-dispatcher.canceled:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not observe cancellation")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve error=%v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve retained a canceled dispatch")
	}
}

func TestBuiltExecutableFixture(t *testing.T) {
	fixture := os.Getenv("STDIO_FIXTURE")
	if fixture == "" {
		t.Skip("STDIO_FIXTURE is supplied by the Bazel executable fixture")
	}
	if !filepath.IsAbs(fixture) {
		fixture = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), fixture)
	}
	request := []byte(`{"jsonrpc":"2.0","id":1,"method":"server.hello"}`)
	command := exec.Command(fixture)
	command.Stdin = bytes.NewReader(append(request, '\n'))
	var output, stderr bytes.Buffer
	command.Stdout = &output
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("fixture failed: %v stderr=%s", err, stderr.String())
	}
	assertResult(t, bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), "server.hello")
	if stderr.Len() != 0 {
		t.Fatalf("fixture wrote stderr: %s", stderr.String())
	}
}

func requestResponse(t *testing.T, server *rpc.Server, request []byte) []byte {
	t.Helper()
	response := server.Dispatch(context.Background(), request)
	if len(response) == 0 {
		t.Fatal("request did not produce a response")
	}
	return response
}

func isError(data []byte) bool {
	var value struct {
		Error json.RawMessage `json:"error"`
	}
	return json.Unmarshal(data, &value) == nil && len(value.Error) != 0
}

func sameError(want, got []byte) bool {
	var left, right map[string]any
	if json.Unmarshal(want, &left) != nil || json.Unmarshal(got, &right) != nil {
		return false
	}
	delete(left["error"].(map[string]any)["data"].(map[string]any), "traceId")
	delete(right["error"].(map[string]any)["data"].(map[string]any), "traceId")
	return bytes.Equal(mustJSON(left), mustJSON(right))
}

func assertErrorCode(t *testing.T, data []byte, want string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("invalid error frame %s: %v", data, err)
	}
	if envelope.Error.Data.Code != want {
		t.Fatalf("error code=%q want=%q frame=%s", envelope.Error.Data.Code, want, data)
	}
}

func assertResult(t *testing.T, data []byte, method string) {
	t.Helper()
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  map[string]any  `json:"result"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.JSONRPC != "2.0" || envelope.Result == nil {
		t.Fatalf("invalid %s result frame: %s (%v)", method, data, err)
	}
	if envelope.Result["serverVersion"] == nil {
		t.Fatalf("%s result missing server metadata: %s", method, data)
	}
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

type echoDispatcher struct{}

func (*echoDispatcher) Dispatch(context.Context, []byte) []byte { return nil }

type blockingDispatcher struct {
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (d *blockingDispatcher) Dispatch(ctx context.Context, _ []byte) []byte {
	d.once.Do(func() { close(d.started) })
	<-ctx.Done()
	close(d.canceled)
	return []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"canceled","data":{"code":"busy","retryable":true}}}`)
}

type blockingReader struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingReader() *blockingReader { return &blockingReader{closed: make(chan struct{})} }

func (r *blockingReader) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *blockingReader) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}
