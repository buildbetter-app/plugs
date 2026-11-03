package pluginbuild_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// Retain the driver's actual post-response finalization capability while
// observing checkpoint IO. This decorator changes neither bytes nor leases.
type browserCheckpointStore struct {
	driver.Store
	driver.PersistentCheckpointStore
	driver.ResponseFinalizer
	mu     sync.Mutex
	leases []driver.ActiveLease
}

func (s *browserCheckpointStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, generation uint64, payload []byte) (driver.Checkpoint, error) {
	s.mu.Lock()
	s.leases = append(s.leases, lease)
	s.mu.Unlock()
	return s.PersistentCheckpointStore.SaveCheckpoint(ctx, lease, key, generation, payload)
}

// This deliberately joins the previously separate source-build, configured
// Writer and browser fixtures. No RPC response or plugin callback is mocked.
// The guest ABI only exposes scoped snapshot reads: checkpoint proof here is
// the Writer's real failed-post receipt, not a guest DurableState claim.
func TestConfiguredSourcePluginBrowserSafety(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root := moduleRoot(t)
	source := wasiCheckout(t, root)
	// Reuse the current public source fixture with its normalization output
	// mapped to the configured runtime's existing title field. Source and
	// manifest are pinned together; no production ontology is bypassed.
	for _, name := range []string{"plugin.go", "plug-plugin.json"} {
		path := filepath.Join(source.Root, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.ReplaceAll(data, []byte("fields.normalized"), []byte("fields.title"))
		if name == "plugin.go" {
			// Validation sees the normalized title in this field mapping.
			data = bytes.ReplaceAll(data, []byte(`title == "reject"`), []byte(`title == "REJECT"`))
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	assets := map[string][]byte{
		"index.html": []byte(`<!doctype html><title>Sandbox probe</title><script src="./frame.js" defer></script><script type="module" src="./module.js"></script><p id="ready">Loading</p>`),
		"frame.js":   []byte(`globalThis.safetyLoaded = true; document.querySelector("#ready").textContent = "Bundle script loaded";`),
		"module.js":  []byte(`globalThis.safetyModuleLoaded = true;`),
	}
	siblingAssets := map[string][]byte{}
	for name, data := range assets {
		siblingAssets[name] = append([]byte(nil), data...)
	}
	siblingAssets["frame.js"] = append(siblingAssets["frame.js"], []byte(`if (location.hash === "#navigation-probe") parent.postMessage({jsonrpc:"2.0",id:"navigation-fixture",method:"rpc.methods",params:{}}, "*");`)...)
	frameSources := map[string]map[string][]byte{"probe": assets, "sibling": siblingAssets}
	for id, files := range frameSources {
		folder := filepath.Join(source.Root, "ui", id)
		if err := os.MkdirAll(folder, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, data := range files {
			if err := os.WriteFile(filepath.Join(folder, name), data, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	git(t, source.Root, "add", "--all")
	git(t, source.Root, "commit", "-q", "--no-verify", "-m", "test: bind public WASI fixture to the configured title field")
	source.Commit = git(t, source.Root, "rev-parse", "HEAD")
	artifact, err := pluginbuild.Build(ctx, pluginbuild.Request{Source: source, GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	if err := artifact.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	manifest, err := artifact.Manifest()
	if err != nil || len(manifest.Plugins) != 1 {
		t.Fatalf("source manifest: %v", err)
	}
	wasm, err := os.ReadFile(artifact.Path())
	if err != nil {
		t.Fatal(err)
	}
	native, err := pluginhost.NewWASMNative(wasm, manifest.Plugins[0], "default")
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := pluginmanifest.Load(ctx, artifact.Source())
	if err != nil {
		t.Fatal(err)
	}
	natives, err := bundle.Natives(ctx, []sdk.Native{native})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("real source commit=%s sourceDigest=%s manifestDigest=%s artifactDigest=%s runtime=%s", artifact.Commit(), artifact.SourceDigest(), artifact.ManifestDigest(), artifact.ArtifactDigest(), artifact.Runtime())

	// Serve bytes read from the same verified source inventory as the guest.
	for id, files := range frameSources {
		for name := range files {
			data, err := os.ReadFile(filepath.Join(source.Root, "ui", id, name))
			if err != nil {
				t.Fatal(err)
			}
			files[name] = data
		}
	}
	digest, err := webui.FrameBundleDigest(assets)
	if err != nil {
		t.Fatal(err)
	}
	siblingDigest, err := webui.FrameBundleDigest(siblingAssets)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "configured-browser.sqlite")
	t.Setenv("PLUG_WASI_PROBE", "owned-loopback-public-canary")
	var observed *browserCheckpointStore
	runtime, err := composition.Open(ctx, composition.Config{
		Driver: "sqlite", DSN: path, Project: "web", UI: true,
		NativePlugins: natives,
		NativeBindings: []pluginhost.Binding{
			{ID: "browser-normalize", Alias: "wasi", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}, Timeout: 250 * time.Millisecond},
			{ID: "browser-check", Alias: "wasi", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.title"}},
			{ID: "browser-observe", Alias: "wasi", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}},
			// This effective binding intentionally has no field grant. Its real
			// guest post read must fail and enter the canonical durable receipt.
			{ID: "browser-denied-post", Alias: "wasi", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost},
		},
		UIFrames: []webui.FrameBundle{
			{ID: "probe", Digest: digest, Assets: assets, Grant: webui.BridgeGrant{Methods: []string{"rpc.methods", "bead.get"}}},
			{ID: "sibling", Digest: siblingDigest, Assets: siblingAssets},
		},
		StoreFactory: func(ctx context.Context, selected string, config driver.Config) (driver.Store, error) {
			if selected != "sqlite" {
				return nil, fmt.Errorf("unexpected driver %q", selected)
			}
			store, err := (sqlite.Driver{}).Open(ctx, config)
			if err != nil {
				return nil, err
			}
			observed = &browserCheckpointStore{Store: store, PersistentCheckpointStore: store.(driver.PersistentCheckpointStore), ResponseFinalizer: store.(driver.ResponseFinalizer)}
			return observed, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	if runtime.Store != observed || runtime.NativeHost == nil || !observed.Persistent() {
		t.Fatal("source plugin did not enter the persistent configured composition")
	}
	listener, err := runtime.Listen(ctx, composition.DefaultAddress, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- listener.Serve() }()
	backend := "http://" + listener.Addr().String()
	created := configuredBrowserRPC(t, backend, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "configured source"}}, "1", "")
	var commit model.Commit
	if err := json.Unmarshal(created, &commit); err != nil || len(commit.Results) != 1 || len(commit.Warnings) != 1 || commit.Warnings[0].Details["binding"] != "browser-denied-post" {
		t.Fatalf("source mutate/validate/real denied post: %s err=%v", created, err)
	}
	if commit.Results[0].SetBy["fields.title"] != "browser-normalize" {
		t.Fatalf("source callback provenance is missing from the configured commit: %s", created)
	}
	ref := commit.Results[0].Ref
	meta, err := observed.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := pipeline.PostDeadLetterKey(meta.Incarnation, commit.Revision, "browser-denied-post")
	checkpoint, err := observed.LoadCheckpoint(ctx, key)
	var dead pipeline.PostDeadLetter
	if err != nil || checkpoint.Generation != 1 || json.Unmarshal(checkpoint.Payload, &dead) != nil || dead.Binding != "browser-denied-post" || dead.Revision != commit.Revision || dead.Incarnation != meta.Incarnation || dead.FailedAt.IsZero() {
		t.Fatalf("actual source post did not persist its canonical receipt: %+v err=%v", checkpoint, err)
	}
	observed.mu.Lock()
	leases := append([]driver.ActiveLease(nil), observed.leases...)
	observed.mu.Unlock()
	if len(leases) != 1 || leases[0] != runtime.Lease {
		t.Fatal("source post receipt did not use the exact configured active lease")
	}
	for index, test := range []struct{ title, code string }{
		{"private", "subscriber_unavailable"},
		{"reject", "ontology_guard_failed"},
		{"timer-callback:bounded", "subscriber_unavailable"},
	} {
		started := time.Now()
		configuredBrowserRPC(t, backend, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": test.title}}, fmt.Sprint(index+2), test.code)
		if time.Since(started) > 2*time.Second {
			t.Fatalf("source callback refusal was not bounded: %s", test.title)
		}
		current, err := observed.Meta(ctx)
		if err != nil || current.Head != meta.Head {
			t.Fatalf("refused source callback published a graph change: %s meta=%+v err=%v", test.title, current, err)
		}
		t.Logf("source callback title=%s code=%s elapsed=%s head=%s", test.title, test.code, time.Since(started), current.Head)
	}
	// A real write after the timed-out guest proves its replacement is usable.
	configuredBrowserRPC(t, backend, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "recovered source"}}, "5", "")
	finalMeta, err := observed.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("source callbacks: owned-field and validator refusals; actual one-second guest wait bounded by 250ms binding; no refused publication; replacement Writer commit succeeds")

	probe := newConfiguredBrowserProbe(t, runtime, backend)
	defer probe.server.Close()
	trap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probe.egress.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer trap.Close()
	canaryPath := filepath.Join(t.TempDir(), "guest-denied-canary")
	if err := os.WriteFile(canaryPath, []byte("public test canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	guestProof, err := runtime.NativeHost.Invoke(ctx, "x.wasi.probe", json.RawMessage(fmt.Sprintf(`{"id":%q,"path":%q,"address":%q}`, ref.ID, canaryPath, trap.Listener.Addr().String())))
	if err != nil {
		t.Fatal(err)
	}
	var capabilities map[string]bool
	if err := json.Unmarshal(guestProof, &capabilities); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"read_denied", "write_denied", "create_denied", "dial_denied", "environment_empty"} {
		if !capabilities[name] {
			t.Fatalf("configured source guest capability escaped: %s %s", name, guestProof)
		}
	}
	if data, err := os.ReadFile(canaryPath); err != nil || string(data) != "public test canary" {
		t.Fatal("guest changed host canary")
	}
	if _, err := os.Stat(canaryPath + ".new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("guest created a host file: %v", err)
	}
	t.Log("configured source guest refused actual filesystem/socket operations against public disposable canaries")
	settings, _ := json.Marshal(map[string]any{"ref": ref, "title": "CONFIGURED SOURCE"})
	script := filepath.Join(root, "internal/server/webui/sandbox_check.mjs")
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, node, script, probe.server.URL, trap.URL, chrome, string(settings))
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("configured source/browser integration: %v; output above", err)
	}
	if probe.credentials.Load() != 0 || probe.outside.Load() != 0 || probe.egress.Load() != 0 || probe.canceled.Load() != 1 {
		t.Fatalf("concrete browser counters: credentials=%d outside=%d egress=%d canceled=%d", probe.credentials.Load(), probe.outside.Load(), probe.egress.Load(), probe.canceled.Load())
	}
	if _, err := runtime.NativeHost.Invoke(ctx, "x.wasi.echo", json.RawMessage(`{"id":"closed"}`)); !hasPluginHostCode(err, pluginhost.ErrInactive) {
		t.Fatalf("configured host retained access after teardown: %v", err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured listener did not terminate")
	}
	// Reopen only the disposable SQLite store: teardown must fence its old
	// active lease and preserve the actual graph/checkpoint bytes.
	reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkpoints := reopened.(driver.PersistentCheckpointStore)
	var fenced driver.ErrFenced
	if _, err := checkpoints.SaveCheckpoint(ctx, runtime.Lease, key, checkpoint.Generation, checkpoint.Payload); !errors.As(err, &fenced) {
		t.Fatalf("teardown left the old lease usable: %v", err)
	}
	retained, err := checkpoints.LoadCheckpoint(ctx, key)
	if err != nil || retained.Generation != checkpoint.Generation || !bytes.Equal(retained.Payload, checkpoint.Payload) {
		t.Fatal("teardown/stale lease changed the source callback checkpoint")
	}
	afterClose, err := reopened.Meta(ctx)
	if err != nil || afterClose.Head != finalMeta.Head {
		t.Fatalf("teardown browser request changed persistent graph state: %+v err=%v", afterClose, err)
	}
	t.Logf("configured source/browser teardown: host inactive, listener closed, old lease fenced, checkpoint bytes/head retained; RPC=%d credentialed=0 outsideAssets=0 egress=0 canceledRead=1", probe.calls.Load())
}

func configuredBrowserRPC(t *testing.T, base, method string, params map[string]any, key string, wantCode string) json.RawMessage {
	t.Helper()
	params["_meta"] = map[string]any{"project": "web", "idempotencyKey": strings.Repeat(key, 32)}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "configured-" + key, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+httpjson.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct{ Result, Error json.RawMessage }
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	var failure struct{ Data struct{ Code string } }
	if len(envelope.Error) != 0 && string(envelope.Error) != "null" {
		if err := json.Unmarshal(envelope.Error, &failure); err != nil {
			t.Fatal(err)
		}
	}
	if failure.Data.Code != wantCode || wantCode == "" && (len(envelope.Result) == 0 || string(envelope.Result) == "null") || wantCode != "" && len(envelope.Result) != 0 {
		t.Fatalf("%s code=%q want=%q error=%s result=%s", method, failure.Data.Code, wantCode, envelope.Error, envelope.Result)
	}
	return envelope.Result
}

type configuredBrowserProbe struct {
	server                                                       *httptest.Server
	calls, credentials, outside, egress, sibling, held, canceled atomic.Int64
	hold                                                         atomic.Bool
}

type configuredBrowserHoldKey struct{}

// The transparent loopback probe observes actual listener traffic. All UI and
// RPC bytes, Host/Origin and credentials pass unchanged to Runtime.Listen;
// only test controls/stats and the outside-asset trap are served here.
func newConfiguredBrowserProbe(t *testing.T, runtime *composition.Runtime, backend string) *configuredBrowserProbe {
	t.Helper()
	probe := &configuredBrowserProbe{}
	target, err := url.Parse(backend)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "owned runtime unavailable", http.StatusBadGateway)
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Context().Value(configuredBrowserHoldKey{}) == true {
			probe.held.Add(1)
			select {
			case <-response.Request.Context().Done():
				probe.canceled.Add(1)
				return response.Request.Context().Err()
			case <-time.After(5 * time.Second):
				return errors.New("held bridge read did not cancel")
			}
		}
		return nil
	}
	probe.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/safety-stats":
			_ = json.NewEncoder(w).Encode(map[string]int64{"rpcCalls": probe.calls.Load(), "siblingDocuments": probe.sibling.Load(), "heldReads": probe.held.Load(), "canceledReads": probe.canceled.Load()})
			return
		case "/fixture/hold":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			probe.hold.Store(true)
			return
		case "/fixture/close":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := runtime.Close(ctx); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		case "/outside.js":
			probe.outside.Add(1)
			w.Header().Set("Content-Type", "text/javascript")
			_, _ = w.Write([]byte(`globalThis.outsideBundleExecuted = true;`))
			return
		case "/ui/frames/sibling/index.html":
			probe.sibling.Add(1)
		case httpjson.Path:
			probe.calls.Add(1)
			if len(r.Header.Values("Cookie")) != 0 || len(r.Header.Values("Authorization")) != 0 {
				probe.credentials.Add(1)
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, "probe read", http.StatusBadRequest)
				return
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			var call struct{ Method string }
			if json.Unmarshal(body, &call) == nil && call.Method == "bead.get" && probe.hold.CompareAndSwap(true, false) {
				r = r.WithContext(context.WithValue(r.Context(), configuredBrowserHoldKey{}, true))
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	return probe
}
