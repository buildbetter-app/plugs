package cli

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/client"
)

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitForEndpoint(t *testing.T, output *synchronizedBuffer) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fields := strings.Fields(output.String())
		for i, field := range fields {
			if field == "on" && i+1 < len(fields) && strings.HasPrefix(fields[i+1], "http://") {
				return fields[i+1]
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("server endpoint was not printed: %q", output.String())
	return ""
}

func TestServerCommandLocalLifecycleUsesHTTPAndShutsDownGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, errOut synchronizedBuffer
	done := make(chan int, 1)
	go func() { done <- execute(ctx, []string{"server", "--local"}, strings.NewReader(""), &out, &errOut, nil) }()
	endpoint := waitForEndpoint(t, &out)
	transport, err := client.NewHTTP(client.HTTPConfig{Server: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	remote, err := client.New(transport, "local", "server-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "local"},
	}, ""); err != nil {
		t.Fatal(err)
	}
	ui, err := http.Get(endpoint + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	ui.Body.Close()
	if ui.StatusCode != http.StatusOK {
		t.Fatalf("UI status=%d", ui.StatusCode)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("server signal exit=%d stderr=%s", code, errOut.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down after cancellation")
	}
	if !strings.Contains(errOut.String(), "memory storage is disposable") {
		t.Fatalf("local durability warning missing: %s", errOut.String())
	}
}

func TestServerCommandRejectsUnknownOptionsAndRedactsDSN(t *testing.T) {
	var out, errOut synchronizedBuffer
	if code := execute(context.Background(), []string{"server", "--unknown"}, strings.NewReader(""), &out, &errOut, nil); code != 2 || !strings.Contains(errOut.String(), "unsupported option") {
		t.Fatalf("unknown option: code=%d out=%s", code, errOut.String())
	}
	if got := redactServerError(errors.New("open /private/password: denied"), "/private/password"); strings.Contains(got, "/private/password") || !strings.Contains(got, "<redacted>") {
		t.Fatalf("DSN redaction=%q", got)
	}
}
