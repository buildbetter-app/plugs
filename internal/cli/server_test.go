package cli

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
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
	return waitForEndpointWithin(t, output, 3*time.Second)
}

func waitForEndpointWithin(t *testing.T, output *synchronizedBuffer, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
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

// Exercise the standalone command's actual LoadConfigFile -> Open -> Listen
// path. The bootstrap is an explicit fixture, not producer/admission evidence.
func TestServerCommandConfiguredOntologyUsesInstalledCatalogOverHTTP(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Mkdir(".plug", 0o700); err != nil {
		t.Fatal(err)
	}
	dsn := filepath.Join(dir, ".plug", "server.db")
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lease, err := store.Lease(context.Background(), "b03-cli-seed", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	doc := ontology.DefaultDocument()
	doc.Statuses["b03-ready"] = ontology.Status{Name: "Configured ready", Category: ontology.Unstarted}
	workflow := doc.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "b03-ready")
	workflow.Initial = "b03-ready"
	doc.Workflows["issue"] = workflow
	body, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(context.Background(), store, lease, driver.ChangesetHeader{
		Principal: model.PrincipalRef{Kind: "system", ID: "b03-cli-seed"},
	}, []conformance.FixtureChange{{Ref: model.Ref{ID: "ontology/b03-cli"}, Kind: "ontology", Op: model.OpCreate, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config := "driver: sqlite\ndsn: .plug/server.db\nproject: b03-cli\naddress: 127.0.0.1:0\nui: false\nontology:\n  source: ontology/b03-cli\n"
	if err := os.WriteFile(filepath.Join(".plug", "config.yaml"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var out, errOut synchronizedBuffer
	done := make(chan int, 1)
	go func() { done <- execute(ctx, []string{"server"}, strings.NewReader(""), &out, &errOut, nil) }()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("configured server exit=%d stderr=%s", code, errOut.String())
			}
		case <-time.After(5 * time.Second):
			t.Error("configured server did not shut down after cancellation")
		}
	})
	endpoint := waitForEndpoint(t, &out)
	transport, err := client.NewHTTP(client.HTTPConfig{Server: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	remote, err := client.New(transport, "b03-cli", "b03-cli-test")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := remote.Call(context.Background(), "ontology.get", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var installed struct {
		Revision model.Revision     `json:"revision"`
		Document *ontology.Document `json:"document"`
	}
	if err := json.Unmarshal(catalog.JSON, &installed); err != nil || installed.Document == nil || installed.Revision != "1" || installed.Document.Statuses["b03-ready"].Category != ontology.Unstarted {
		t.Fatalf("standalone configured catalog=%s err=%v", catalog.JSON, err)
	}
	created, err := remote.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "from configured standalone catalog"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if err := json.Unmarshal(created.JSON, &commit); err != nil || len(commit.Results) != 1 {
		t.Fatalf("configured create=%s err=%v", created.JSON, err)
	}
	createdDoc, err := pipeline.DecodeDocument(commit.Results[0].Record)
	if err != nil || createdDoc.Fields["status"] != "b03-ready" {
		t.Fatalf("standalone ordinary create lost installed initial status: %+v err=%v", createdDoc, err)
	}
}
