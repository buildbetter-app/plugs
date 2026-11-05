package hostcallers_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// The Python callers use real HTTP JSON-RPC, real SQLite and the actual writer,
// lifecycle guards, CAS, ID allocation, idempotency and pinned view readers.
// Only transport identity is a trusted fixture principal; no authentication or
// private restore/cutover qualification is claimed by this bounded target.
func TestSixHostCallersSQLiteEndToEnd(t *testing.T) {
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if root == "" {
		t.Fatal("PLUG_SOURCE_MODULE_ROOT required; run through remote-bazel")
	}
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "fixture.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lease, err := store.Lease(ctx, "i8-host-fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	raw, err := os.ReadFile(filepath.Join(root, "tools/hostcallers/host-ontology.json"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := ontology.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(document, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("prepared ontology: %+v", problems)
	}
	rules := service.NewOntology(snapshot.Effective)
	writer := pipeline.New(store, lease, pipeline.WithOntology(rules))
	domain := service.New(store, lease, writer, service.WithOntology(rules))
	views, err := viewops.New(store, lease, viewops.Config{})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.Bind(registry, views); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "wrong-ambient-default"})
	if err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/v1" {
			http.NotFound(w, r)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			t.Error(err)
			return
		}
		var envelope map[string]any
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(dispatcher.Dispatch(rpc.WithPrincipal(r.Context(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), raw))
	}))
	defer host.Close()
	cmd := exec.Command("python3", filepath.Join(root, "tools/hostcallers/fixture_test.py"), "-v")
	cmd.Env = append(os.Environ(), "I8_TEST_ENDPOINT="+host.URL, "I8_TEST_INCARNATION="+meta.Incarnation,
		"PYTHONDONTWRITEBYTECODE=1")
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatalf("host caller fixture: %v", err)
	}
}
