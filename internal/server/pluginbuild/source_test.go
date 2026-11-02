package pluginbuild_test

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// The source checkout is deliberately committed into a disposable repository
// before the builder sees it. The root replacement is filled with the current
// Plug source tree so the real SDK packages have the same module identity as
// the external helper host.
//
//go:embed fixture/go.mod.template fixture/plugin.go fixture/plug-plugin.json
var sourceFiles embed.FS

//go:embed fixture-wasi/acceptance.go fixture-wasi/go.mod.template fixture-wasi/plugin.go fixture-wasi/plug-plugin.json
var wasiSourceFiles embed.FS

const helperMain = `package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
)

type snapshot struct{ revision model.Revision }

func (s *snapshot) Snapshot() driver.View { return driver.View{At: &s.revision, Incarnation: "source-test"} }
func (*snapshot) Get(context.Context, []model.Ref) ([]driver.Record, error) { return nil, nil }
func (s *snapshot) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	return driver.Page[driver.Record]{AsOf: s.revision}, nil
}
func (*snapshot) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) { return nil, nil }

type dispatcher struct{}
func (dispatcher) Effect(method string) (pluginhost.Effect, bool) { return pluginhost.Write, method == "tx.apply" }
func (dispatcher) Call(context.Context, pluginapi.PrincipalRef, string, json.RawMessage) (json.RawMessage, error) { return nil, fmt.Errorf("forbidden write reached dispatcher") }

func main() {
	ctx := context.Background()
	artifact, err := pluginbuild.Build(ctx, pluginbuild.Request{
		TrustedNative: true,
		Source: pluginmanifest.Checkout{Root: os.Getenv("SOURCE_ROOT"), Commit: os.Getenv("SOURCE_COMMIT")},
		ModuleRoot: os.Getenv("PLUG_ROOT"),
		GoBinary: os.Getenv("GO_BINARY"),
	})
	if err != nil { panic(err) }
	defer artifact.Close()
	host, err := artifact.Host(ctx, pluginhost.Config{Dispatcher: dispatcher{}, Bindings: []pluginhost.Binding{
		{ID: "source-normalize", Alias: "source", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}},
		{ID: "source-check", Alias: "source", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.title"}},
	}})
	if err != nil { panic(err) }
	other, err := artifact.Host(ctx, pluginhost.Config{})
	if err != nil { panic(err) }
	for index, current := range []*pluginhost.Host{host, other, host} {
		state, err := current.Invoke(ctx, "x.source.echo", json.RawMessage("{\"id\":\"source-1\",\"state\":true}"))
		want := []string{"1", "1", "2"}[index]
		if err != nil || string(state) != want { panic(fmt.Sprintf("instance state: %s want %s: %v", state, want, err)) }
	}
	echo, err := host.Invoke(ctx, "x.source.echo", json.RawMessage("{\"id\":\"source-1\",\"value\":9007199254740993}"))
	if err != nil { panic(err) }
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "source-build-test", driver.LeaseTTL)
	if err != nil { panic(err) }
	defer lease.Release(context.Background())
	doc := ontology.DefaultDocument()
	doc.Types["issue"].Fields["normalized"] = ontology.Field{Type: "string", Owner: "source-normalize"}
	compiled, problems := ontology.Compile(doc, nil, ontology.PluginCatalog{"source-normalize": true})
	if len(problems) != 0 { panic(fmt.Sprintf("ontology: %v", problems)) }
	hook := service.NewOntology(func(string) ontology.Effective { return compiled.Effective("") })
	writer := pipeline.New(store, lease, pipeline.WithOntology(hook), pipeline.WithMutators(host.Mutators()...), pipeline.WithValidators(host.Validators()...), pipeline.WithIDGenerator(func() (string, error) { return "source-build", nil }))
	commit, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "source-test"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "hello"}}}}}, model.WriteMeta{Method: "tx.apply", IdempotencyKey: "00000000000000000000000000000001"})
	if err != nil || len(commit.Results) != 1 { panic(fmt.Sprintf("pipeline commit: %v %+v", err, commit)) }
	metadata, err := store.Meta(ctx)
	if err != nil { panic(err) }
	reader, err := store.Read(ctx, driver.View{Incarnation: metadata.Incarnation})
	if err != nil { panic(err) }
	defer reader.Close()
	records, err := reader.Get(ctx, []model.Ref{commit.Results[0].Ref})
	if err != nil || len(records) != 1 { panic(fmt.Sprintf("pipeline read: %v", err)) }
	body, err := pipeline.DecodeDocument(records[0].Body)
	if err != nil { panic(err) }
	validators := host.Validators()
	if len(validators) != 1 { panic(fmt.Sprintf("validators=%d", len(validators))) }
	revision := model.Natural64(1)
	verdict, err := validators[0].Validator.Validate(ctx, port.ProposedChange{
		RequestID: "source-validation", Principal: model.PrincipalRef{Kind: "user", ID: "source-test"},
		BaseRevision: revision, Changes: []port.Change{{
			Resource: model.ResourceRef{Kind: "bead", Ref: model.RefByID("source-1", "")},
			Op: model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Fields: map[string]any{"title": "reject"}}},
			After: []byte("{\"type\":\"issue\",\"fields\":{\"title\":\"reject\"}}"),
		}}}, &snapshot{revision: revision})
	if err != nil { panic(err) }
	result := map[string]any{
		"echo": string(echo), "allow": verdict.Allow, "violations": len(verdict.Violations),
		"normalized": body.Fields["normalized"],
		"commit": artifact.Commit(), "sourceDigest": artifact.SourceDigest(),
		"manifestDigest": artifact.ManifestDigest(), "artifactDigest": artifact.ArtifactDigest(),
		"files": artifact.Files(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil { panic(err) }
}
`

func TestPinnedSourceBuildLoadsActualCompiledCallback(t *testing.T) {
	root := moduleRoot(t)
	source := checkout(t, root, false)
	result := runHelper(t, root, source)
	if result["echo"] != `{"id":"source-1","value":9007199254740993}` || result["allow"] != false || result["violations"] != float64(1) || result["normalized"] != "HELLO" {
		t.Fatalf("compiled callback/validator did not execute through host: %#v", result)
	}
	for _, key := range []string{"commit", "sourceDigest", "manifestDigest", "artifactDigest"} {
		if value, ok := result[key].(string); !ok || value == "" {
			t.Fatalf("missing artifact identity %s: %#v", key, result)
		}
	}
	files, ok := result["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("artifact inventory is not complete: %#v", result["files"])
	}
	t.Log("compiled source -> real SDK/host -> memory Writer: normalized=HELLO; validator refused; hot write/egress refused; instance state isolated")
}

func TestWASIPluginBuildRestoreAndAsyncRebuild(t *testing.T) {
	root := moduleRoot(t)
	source := wasiCheckout(t, root)
	ctx := context.Background()
	// Let Build infer the sandboxed runtime from the pinned manifest. This
	// protects the admission path from requiring a redundant caller hint.
	artifact, err := pluginbuild.Build(ctx, pluginbuild.Request{Source: source, GoBinary: goPath(t)})
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		requireBuildCode(t, err, pluginbuild.ErrUnsupported)
		if artifact != nil {
			t.Fatal("unsupported local WASI build produced an artifact")
		}
		t.Log("local mode-2 build refused on an unsupported host; runtime remains independently supported")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	if artifact.Runtime() != pluginapi.RuntimeWASI || len(artifact.Files()) != 1 {
		t.Fatalf("unexpected WASI artifact: runtime=%q files=%v", artifact.Runtime(), artifact.Files())
	}
	wasmBytes, err := os.ReadFile(artifact.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := pluginhost.ValidateWASMArtifact(wasmBytes); err != nil {
		t.Fatalf("static validation rejected the real artifact: %v", err)
	}
	for name, replacement := range map[string]string{
		"wasi_snapshot_preview1": "evil_snapshot_preview1",
		"plug_alloc":             "evil_alloc",
	} {
		mutated := bytes.Replace(wasmBytes, []byte(name), []byte(replacement), 1)
		if bytes.Equal(mutated, wasmBytes) {
			t.Fatalf("fixture did not contain %q", name)
		}
		if err := pluginhost.ValidateWASMArtifact(mutated); !hasPluginHostCode(err, pluginhost.ErrManifest) {
			t.Fatalf("mutated %s artifact was admitted: %v", name, err)
		}
	}
	t.Setenv("PLUG_WASI_PROBE", "host-only")
	host, err := artifact.Host(ctx, pluginhost.Config{Bindings: []pluginhost.Binding{
		{ID: "wasi-normalize", Alias: "wasi", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}},
		{ID: "wasi-check", Alias: "wasi", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.title"}},
		{ID: "wasi-observe", Alias: "wasi", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	echo, err := host.Invoke(ctx, "x.wasi.echo", json.RawMessage(`{"id":"wasi-1","value":9007199254740993}`))
	if err != nil || string(echo) != `{"id":"wasi-1","value":9007199254740993}` {
		t.Fatalf("WASI method did not round-trip JSON: %s (%v)", echo, err)
	}
	if _, err := host.Invoke(ctx, "x.other.echo", json.RawMessage(`{"id":"wasi-1"}`)); !hasPluginHostCode(err, pluginhost.ErrMethod) {
		t.Fatalf("undeclared method prefix was admitted: %v", err)
	}
	// Exercise real guest syscalls against a host path and a listening host
	// endpoint. No preopened files, ambient environment or sockets are granted.
	path := filepath.Join(t.TempDir(), "host-file")
	if err := os.WriteFile(path, []byte("host-only"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	params, _ := json.Marshal(map[string]string{"id": "wasi-1", "path": path, "address": listener.Addr().String()})
	probe, err := host.Invoke(ctx, "x.wasi.probe", params)
	if err != nil {
		t.Fatal(err)
	}
	var denied map[string]bool
	if err := json.Unmarshal(probe, &denied); err != nil {
		t.Fatal(err)
	}
	for _, capability := range []string{"read_denied", "write_denied", "create_denied", "dial_denied", "environment_empty"} {
		if !denied[capability] {
			t.Errorf("guest obtained ambient capability %s: %s", capability, probe)
		}
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "host-only" {
		t.Fatalf("guest changed the host path: %s (%v)", data, err)
	}
	if _, err := os.Stat(path + ".new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("guest created an undeclared host path: %v", err)
	}
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if conn, err := listener.Accept(); err == nil {
		_ = conn.Close()
		t.Fatal("guest reached the host network")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("network probe listener failed: %v", err)
	}
	// Drive the real memory Writer so all three WASI callback phases receive
	// their per-invocation scoped reader. The second write reads the admitted
	// first record at base revision 1; post reads the committed revision 2.
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "wasi-pipeline", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	doc := ontology.DefaultDocument()
	doc.Types["issue"].Fields["normalized"] = ontology.Field{Type: "string", Owner: "wasi-normalize"}
	doc.Types["issue"].Fields["private"] = ontology.Field{Type: "string"}
	compiled, problems := ontology.Compile(doc, nil, ontology.PluginCatalog{"wasi-normalize": true})
	if len(problems) != 0 {
		t.Fatalf("WASI ontology: %v", problems)
	}
	hook := service.NewOntology(func(string) ontology.Effective { return compiled.Effective("") })
	writer := pipeline.New(store, lease, pipeline.WithOntology(hook), pipeline.WithMutators(host.Mutators()...), pipeline.WithValidators(host.Validators()...), pipeline.WithPostSubscribers(host.Posts()...), pipeline.WithIDGenerator(func() (string, error) { return "wasi-pipeline", nil }))
	first, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "wasi-test"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "seed", "private": "host-only"}}}}}, model.WriteMeta{Method: "tx.apply", IdempotencyKey: "00000000000000000000000000000011"})
	if err != nil || len(first.Results) != 1 {
		t.Fatalf("WASI seed pipeline write failed: %+v (%v)", first, err)
	}
	second, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "wasi-test"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: first.Results[0].Ref, Fields: map[string]any{"title": "hello"}}}}}, model.WriteMeta{Method: "tx.apply", IdempotencyKey: "00000000000000000000000000000012"})
	if err != nil || second.Revision.String() != "2" || len(second.Warnings) != 0 {
		t.Fatalf("WASI callback read pipeline failed: %+v (%v)", second, err)
	}
	var committed struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(second.Results[0].Record, &committed); err != nil || committed.Fields["normalized"] != "HELLO" || committed.Fields["private"] != "host-only" {
		t.Fatalf("WASI scoped mutation was not committed: %s (%v)", second.Results[0].Record, err)
	}
	change := port.ProposedChange{RequestID: "wasi-test", Principal: model.PrincipalRef{Kind: "user", ID: "test"}, BaseRevision: model.Natural64(1), Changes: []port.Change{{
		Resource: model.ResourceRef{Kind: "bead", Ref: model.RefByID("wasi-1", "")},
		Op:       model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Fields: map[string]any{"title": "hello"}}},
		After:    []byte(`{"type":"issue","fields":{"title":"hello"}}`),
	}}}
	mutators := host.Mutators()
	patches, err := mutators[0].Mutator.Mutate(ctx, change, &wasiSnapshot{revision: model.Natural64(1)})
	if err != nil || len(patches) != 1 || string(patches[0].Paths["fields.normalized"]) != `"HELLO"` {
		t.Fatalf("WASI mutator did not return its declared patch: %#v (%v)", patches, err)
	}
	change.Changes[0].After = []byte(`{"type":"issue","fields":{"title":"private"}}`)
	if _, err := mutators[0].Mutator.Mutate(ctx, change, &wasiSnapshot{revision: model.Natural64(1)}); !hasPluginHostCode(err, pluginhost.ErrPatch) {
		t.Fatalf("undeclared WASI patch was admitted: %v", err)
	}
	change.Changes[0].After = []byte(`{"type":"issue","fields":{"title":"reject"}}`)
	verdict, err := host.Validators()[0].Validator.Validate(ctx, change, &wasiSnapshot{revision: model.Natural64(1)})
	if err != nil || verdict.Allow || len(verdict.Violations) != 1 {
		t.Fatalf("WASI validator did not reject the change: %+v (%v)", verdict, err)
	}
	lockStore := pluginbuild.NewArtifactStore()
	lock, err := lockStore.Put(ctx, artifact)
	if err != nil {
		t.Fatal(err)
	}
	// The store owns a detached lock. Mutating the caller's nested manifest
	// after Put must not alter the bytes admitted by the store.
	lock.Manifest.Plugins[0].Alias = "mutated"
	if restored, err := lockStore.Restore(ctx, lock); err == nil {
		_ = restored.Close()
		t.Fatal("mutated lock was accepted")
	}
	lock, err = artifact.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if restored, err := lockStore.Restore(ctx, lock); err != nil {
		t.Fatalf("mutating the returned lock changed stored admission: %v", err)
	} else {
		_ = restored.Close()
	}
	block := make(chan struct{})
	// Reproduce from the same pinned commit in a different clean checkout,
	// not the original path or a retained build directory. The manifest loader
	// must also re-admit the original complete source digest.
	rebuildSource := wasiCheckout(t, root)
	if rebuildSource.Root == source.Root || rebuildSource.Commit != source.Commit {
		t.Fatal("reproduction fixture is not an independent checkout of the exact pin")
	}
	rebuildSource.ExpectedDigest = artifact.SourceDigest()
	restored, checks, err := lockStore.RestoreAndCheck(ctx, lock, func(context.Context) (*pluginbuild.Artifact, error) {
		<-block
		return pluginbuild.Build(ctx, pluginbuild.Request{Runtime: pluginapi.RuntimeWASI, Source: rebuildSource, GoBinary: goPath(t)})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Verify(ctx); err != nil {
		t.Fatalf("digest restore was not immediately usable: %v", err)
	}
	restoredHost, err := restored.Host(ctx, pluginhost.Config{})
	if err != nil {
		t.Fatalf("restored WASI artifact could not be admitted: %v", err)
	}
	restoredEcho, err := restoredHost.Invoke(ctx, "x.wasi.echo", json.RawMessage(`{"id":"restored"}`))
	if err != nil || string(restoredEcho) != `{"id":"restored"}` {
		t.Fatalf("restored WASI host did not execute: %s (%v)", restoredEcho, err)
	}
	close(block)
	check := <-checks
	if check.Err != nil || !check.Match {
		t.Fatalf("rebuild check did not match lock: %+v", check)
	}
	t.Logf("pinned WASI reproduced across independent checkouts: commit=%s source=%s artifact=%s", lock.Commit, lock.SourceDigest, lock.ArtifactDigest)
	_ = restored.Close()
	// A rebuilt source revision with different bytes must remain a mismatch;
	// it cannot replace the digest-restored artifact.
	pluginPath := filepath.Join(source.Root, "plugin.go")
	plugin, err := os.ReadFile(pluginPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pluginPath, bytes.Replace(plugin, []byte("strings.ToUpper"), []byte("strings.ToLower"), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, source.Root, "add", "plugin.go")
	git(t, source.Root, "commit", "-q", "-m", "changed WASI fixture")
	changed := source
	changed.Commit = git(t, source.Root, "rev-parse", "HEAD")
	restored, checks, err = lockStore.RestoreAndCheck(ctx, lock, func(ctx context.Context) (*pluginbuild.Artifact, error) {
		return pluginbuild.Build(ctx, pluginbuild.Request{Source: changed, GoBinary: goPath(t)})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	check = <-checks
	if check.Err != nil || check.Match || check.Actual.ArtifactDigest == lock.ArtifactDigest {
		t.Fatalf("changed source was not reported as a rebuild mismatch: %+v", check)
	}
	if restored.ArtifactDigest() != lock.ArtifactDigest || restored.Verify(ctx) != nil {
		t.Fatal("rebuild mismatch changed the restored artifact")
	}
	t.Log("real GOOS=wasip1 build -> sandboxed wazero callbacks -> digest restore before async rebuild; mismatch, undeclared patch, path and egress refused")
}

func hasPluginHostCode(err error, want string) bool {
	var hostErr *pluginhost.Error
	return errors.As(err, &hostErr) && hostErr.Code == want
}

type wasiSnapshot struct{ revision model.Revision }

func (s *wasiSnapshot) Snapshot() driver.View {
	return driver.View{At: &s.revision, Incarnation: "wasi-test"}
}
func (*wasiSnapshot) Get(context.Context, []model.Ref) ([]driver.Record, error) { return nil, nil }
func (s *wasiSnapshot) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	return driver.Page[driver.Record]{AsOf: s.revision}, nil
}
func (*wasiSnapshot) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) {
	return nil, nil
}

func TestWASIArtifactOwnsPooledHostsAndRealGuestState(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("source-built guest acceptance requires the supported Linux/amd64 build host")
	}
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{Source: wasiCheckout(t, moduleRoot(t)), GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = artifact.Close() }()
	host := wasiGuestHost(t, artifact, 0)
	secondHost := wasiGuestHost(t, artifact, 0)
	callStateWithContext := func(ctx context.Context, mutator port.Mutator, principal, title string, reader port.SnapshotReader) (string, error) {
		after, _ := json.Marshal(map[string]any{"type": "issue", "fields": map[string]any{"title": title}})
		patches, err := mutator.Mutate(ctx, port.ProposedChange{
			RequestID:    principal + ":" + title,
			Principal:    model.PrincipalRef{Kind: "user", ID: principal},
			BaseRevision: model.Natural64(1),
			Changes: []port.Change{{
				Resource: model.ResourceRef{Kind: "bead", Ref: model.RefByID("pool-state", "")},
				Op:       model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: model.RefByID("pool-state", ""), Fields: map[string]any{"title": title}}},
				After:    after,
			}},
		}, reader)
		if err != nil {
			return "", err
		}
		if len(patches) != 1 {
			return "", fmt.Errorf("real WASI state callback returned %d patches", len(patches))
		}
		var value string
		if err := json.Unmarshal(patches[0].Paths["fields.normalized"], &value); err != nil {
			return "", err
		}
		return value, nil
	}
	callState := func(mutator port.Mutator, principal, title string, reader port.SnapshotReader) (string, error) {
		return callStateWithContext(context.Background(), mutator, principal, title, reader)
	}
	mutator := host.Mutators()[0].Mutator
	for _, tc := range []struct {
		principal, title, want string
	}{
		{"principal-a", "pool-state:a", "POOL-STATE:A#1"},
		{"principal-a", "pool-state:a", "POOL-STATE:A#2"},
		{"principal-b", "pool-state:b", "POOL-STATE:B#1"},
		{"principal-c", "pool-state:c", "POOL-STATE:C#1"},
		{"principal-d", "pool-state:d", "POOL-STATE:D#1"},
		// Four first claims fill the real pool; this fifth claim retires an
		// idle affinity and constructs a fresh guest instead of reusing state.
		{"principal-e", "pool-state:e", "POOL-STATE:E#1"},
	} {
		got, callErr := callState(mutator, tc.principal, tc.title, &wasiSnapshot{revision: model.Natural64(1)})
		if callErr != nil || got != tc.want {
			t.Fatalf("real WASI state principal=%s got=%q want=%q err=%v", tc.principal, got, tc.want, callErr)
		}
	}

	// Let two previously absent principals overlap at the real pool boundary.
	// The barrier is outside the guest; the calls below then execute through
	// distinct pooled WASI modules concurrently and expose private state.
	entered, release := make(chan struct{}, 2), make(chan struct{})
	type stateResult struct {
		value string
		err   error
	}
	results := make(chan stateResult, 2)
	for _, principal := range []string{"principal-f", "principal-g"} {
		principal := principal
		go func() {
			reader := &wasiBarrierSnapshot{
				wasiSnapshot: wasiSnapshot{revision: model.Natural64(1)},
				entered:      entered,
				release:      release,
			}
			value, err := callState(mutator, principal, "pool-state:"+strings.TrimPrefix(principal, "principal-"), reader)
			results <- stateResult{value: value, err: err}
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("real WASI overlapping claim did not reach its barrier")
		}
	}

	close(release)
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			if result.err != nil || !strings.HasSuffix(result.value, "#1") {
				t.Fatalf("overlapping real WASI claim leaked guest state: value=%q err=%v", result.value, result.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("overlapping real WASI claim did not finish")
		}
	}

	// A canceled real guest callback retires its generation while the old
	// module is still unwinding. A replacement may serve the same principal,
	// but the late old result must never publish into that replacement.
	lateEntered, lateRelease, lateDrained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	lateDone := make(chan error, 1)
	lateReader := &wasiLateBarrierSnapshot{
		wasiSnapshot: wasiSnapshot{revision: model.Natural64(1)},
		entered:      lateEntered,
		release:      lateRelease,
		drained:      lateDrained,
	}
	lateCtx, cancelLate := context.WithCancel(context.Background())
	go func() {
		_, err := callStateWithContext(lateCtx, mutator, "principal-late", "pool-state:late", lateReader)
		lateDone <- err
	}()
	select {
	case <-lateEntered:
	case <-time.After(2 * time.Second):
		cancelLate()
		t.Fatal("real WASI late callback did not reach its host-read barrier")
	}
	cancelLate()
	select {
	case err := <-lateDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled real WASI callback returned %v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled real WASI callback did not return")
	}
	if got, err := callState(mutator, "principal-late", "pool-state:replacement", &wasiSnapshot{revision: model.Natural64(1)}); err != nil || !strings.HasSuffix(got, "#1") {
		t.Fatalf("replacement guest was not fresh after canceled generation: value=%q err=%v", got, err)
	}
	close(lateRelease)
	select {
	case <-lateDrained:
	case <-time.After(2 * time.Second):
		t.Fatal("late real WASI callback did not drain after release")
	}
	if got, err := callState(mutator, "principal-late", "pool-state:after-late", &wasiSnapshot{revision: model.Natural64(1)}); err != nil || !strings.HasSuffix(got, "#2") {
		t.Fatalf("late generation changed replacement guest state: value=%q err=%v", got, err)
	}

	// A canceled callback may leave an uncooperative real guest reader blocked
	// until the test releases it. Retain four such readers, matching the live
	// pool ceiling, then prove a fifth admission is bounded without creating a
	// fifth reader worker. Release every barrier only after the bound oracle so
	// the test exercises the retired-generation resource accounting.
	const retainedReaderLimit = 4
	retained := make([]struct {
		release chan struct{}
		drained chan struct{}
	}, 0, retainedReaderLimit)
	for i := 0; i < retainedReaderLimit; i++ {
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		drained := make(chan struct{}, 1)
		reader := &wasiLateBarrierSnapshot{
			wasiSnapshot: wasiSnapshot{revision: model.Natural64(1)},
			entered:      entered,
			release:      release,
			drained:      drained,
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		principal := fmt.Sprintf("principal-retained-%d", i)
		go func() {
			_, callErr := callStateWithContext(ctx, mutator, principal, "pool-state:"+principal, reader)
			done <- callErr
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("retained real WASI callback did not reach its host-read barrier")
		}
		cancel()
		select {
		case callErr := <-done:
			if !errors.Is(callErr, context.Canceled) {
				t.Fatalf("retained real WASI callback returned %v, want cancellation", callErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("retained real WASI callback was not bounded after cancellation")
		}
		retained = append(retained, struct {
			release chan struct{}
			drained chan struct{}
		}{release: release, drained: drained})
	}
	overflowEntered := make(chan struct{}, 1)
	overflowRelease := make(chan struct{})
	overflowDrained := make(chan struct{}, 1)
	overflowReader := &wasiLateBarrierSnapshot{
		wasiSnapshot: wasiSnapshot{revision: model.Natural64(1)},
		entered:      overflowEntered,
		release:      overflowRelease,
		drained:      overflowDrained,
	}
	overflowCtx, cancelOverflow := context.WithTimeout(context.Background(), 2*time.Second)
	startedOverflow := time.Now()
	_, overflowErr := callStateWithContext(overflowCtx, mutator, "principal-overflow", "pool-state:overflow", overflowReader)
	cancelOverflow()
	if overflowErr == nil || time.Since(startedOverflow) > 2*time.Second {
		t.Fatalf("reader-worker admission was unbounded: err=%v elapsed=%s", overflowErr, time.Since(startedOverflow))
	}
	select {
	case <-overflowEntered:
		close(overflowRelease)
		<-overflowDrained
		t.Fatal("reader-worker admission exceeded the four-worker bound")
	default:
	}
	for _, blocked := range retained {
		close(blocked.release)
		select {
		case <-blocked.drained:
		case <-time.After(2 * time.Second):
			t.Fatal("retained real WASI reader did not drain after cleanup release")
		}
	}
	if got, err := callState(mutator, "principal-after-readers", "pool-state:after-readers", &wasiSnapshot{revision: model.Natural64(1)}); err != nil || !strings.HasSuffix(got, "#1") {
		t.Fatalf("bounded reader recovery did not refill a fresh guest: value=%q err=%v", got, err)
	}

	// A real guest refusal disposes its member. The next same-principal call
	// must observe a replacement module with a fresh private counter.
	if _, err := callState(mutator, "principal-refused", "acceptance:undeclared", &wasiSnapshot{revision: model.Natural64(1)}); err == nil {
		t.Fatal("real WASI undeclared callback unexpectedly succeeded")
	}
	if got, err := callState(mutator, "principal-refused", "pool-state:replacement", &wasiSnapshot{revision: model.Natural64(1)}); err != nil || !strings.HasSuffix(got, "#1") {
		t.Fatalf("real WASI refusal did not retire private guest state: value=%q err=%v", got, err)
	}

	// Artifact.Close owns every admitted host, including the second host, and
	// remains idempotent. The returned hosts must reject calls after ownership
	// closes even though callers never explicitly closed either host.
	if err := artifact.Close(); err != nil {
		t.Fatal(err)
	}
	for _, current := range []*pluginhost.Host{host, secondHost} {
		if _, err := current.Invoke(context.Background(), "x.wasi.echo", json.RawMessage(`{"id":"after-artifact-close"}`)); !hasPluginHostCode(err, pluginhost.ErrInactive) {
			t.Fatalf("artifact owner close left a usable host: %v", err)
		}
	}
	if err := artifact.Close(); err != nil {
		t.Fatalf("artifact owner close was not idempotent: %v", err)
	}
	requireBuildCode(t, func() error {
		_, err := artifact.Host(context.Background(), pluginhost.Config{})
		return err
	}(), pluginbuild.ErrClosed)
}

type wasiBarrierSnapshot struct {
	wasiSnapshot
	entered chan<- struct{}
	release <-chan struct{}
}

// wasiLateBarrierSnapshot deliberately ignores callback cancellation until
// the test releases the actual guest host import. This keeps the retired
// module in flight while its replacement serves the same principal.
type wasiLateBarrierSnapshot struct {
	wasiSnapshot
	entered chan<- struct{}
	release <-chan struct{}
	drained chan<- struct{}
}

func (s *wasiLateBarrierSnapshot) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	select {
	case s.drained <- struct{}{}:
	default:
	}
	return nil, nil
}

func TestWASIArtifactAdmissionCancellationAndOwnerClose(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("source-built guest acceptance requires the supported Linux/amd64 build host")
	}
	root := moduleRoot(t)
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{Source: slowWASICheckout(t, root), GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	path := artifact.Path()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	started := time.Now()
	_, err = artifact.Host(ctx, pluginhost.Config{})
	cancel()
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatalf("source WASI admission ignored caller cancellation: err=%v elapsed=%s", err, time.Since(started))
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("canceled admission removed the still-owned artifact: %v", statErr)
	}

	// Start a second real admission, observe its actual guest initializer entry,
	// then close its owner while the initializer is sleeping. Close must cancel
	// and await that admission before removing the output, rather than returning
	// with pluginhost.New in flight.
	hostDone := make(chan error, 1)
	initializerEntered := make(chan struct{}, 1)
	admissionCtx := pluginhost.WithWASMInitializerObserver(context.Background(), func() {
		select {
		case initializerEntered <- struct{}{}:
		default:
		}
	})
	go func() {
		_, hostErr := artifact.Host(admissionCtx, pluginhost.Config{})
		hostDone <- hostErr
	}()
	select {
	case <-initializerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("source WASI admission did not enter the real guest initializer")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- artifact.Close() }()
	select {
	case <-closeDone:
		select {
		case <-hostDone:
		default:
			t.Fatal("Artifact.Close returned while source WASI admission was still running")
		}
	case <-time.After(100 * time.Millisecond):
		// The owner is correctly waiting for the in-flight admission.
	}
	select {
	case <-hostDone:
	case <-time.After(3 * time.Second):
		t.Fatal("owner cancellation did not stop source WASI admission")
	}
	if closeErr := <-closeDone; closeErr != nil {
		t.Fatalf("owner close failed: %v", closeErr)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("owner close removed output with unexpected state: %v", statErr)
	}
	t.Log("real source-built WASI caller cancellation and owner-close admission ordering were bounded; output removal followed admission retirement")
}

func TestWASIQueueConstructionDeadlineIsRetryableBusy(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("source-built guest acceptance requires the supported Linux/amd64 build host")
	}
	root := moduleRoot(t)
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{Source: wasiCheckout(t, root), GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	wasm, err := os.ReadFile(artifact.Path())
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := artifact.Manifest()
	if err != nil || len(manifest.Plugins) != 1 {
		t.Fatalf("WASI manifest: %v", err)
	}
	native, err := pluginhost.NewWASMNative(wasm, manifest.Plugins[0], "default")
	if err != nil {
		t.Fatal(err)
	}
	original := native.NewWithContext
	var constructions atomic.Int32
	replacementStarted := make(chan struct{})
	native.NewWithContext = func(ctx context.Context, host pluginapi.Host) (sdk.Registration, error) {
		if constructions.Add(1) != 6 {
			return original(ctx, host)
		}
		registration, err := original(ctx, host)
		if err != nil {
			return sdk.Registration{}, err
		}
		close(replacementStarted)
		<-ctx.Done()
		// Return the fully initialized registration so construct can retire the
		// actual guest and exercise its bounded replacement/refill path.
		return registration, nil
	}
	host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, Bindings: []pluginhost.Binding{{
		ID: "wasi-normalize", Alias: "wasi", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate,
		Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	mutator := host.Mutators()[0].Mutator
	for _, principal := range []string{"queue-a", "queue-b", "queue-c", "queue-d"} {
		if _, err := mutator.Mutate(context.Background(), wasiStateChange(principal, "pool-state:"+principal), &wasiSnapshot{revision: model.Natural64(1)}); err != nil {
			t.Fatalf("initial source WASI principal %s failed: %v", principal, err)
		}
	}
	result := make(chan error, 1)
	go func() {
		_, mutateErr := mutator.Mutate(context.Background(), wasiStateChange("queue-e", "pool-state:queue-e"), &wasiSnapshot{revision: model.Natural64(1)})
		result <- mutateErr
	}()
	select {
	case <-replacementStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("queue replacement did not initialize a real WASI guest")
	}
	if got := <-result; !hasPluginHostCode(got, pluginhost.ErrPluginBusy) {
		t.Fatalf("queue-only construction expiry was not retryable busy: %v", got)
	}
	deadline := time.After(2 * time.Second)
	for {
		_, recoveredErr := mutator.Mutate(context.Background(), wasiStateChange("queue-recovered", "pool-state:queue-recovered"), &wasiSnapshot{revision: model.Natural64(1)})
		if recoveredErr == nil {
			break
		}
		if !hasPluginHostCode(recoveredErr, pluginhost.ErrPluginBusy) {
			t.Fatalf("real WASI replacement recovery failed: %v", recoveredErr)
		}
		select {
		case <-deadline:
			t.Fatal("real WASI replacement did not recover its bounded pool")
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Log("actual source-built WASI replacement construction deadline mapped to plugin_busy after guest initialization; caller context remained active")
}

func wasiStateChange(principal, title string) port.ProposedChange {
	after, _ := json.Marshal(map[string]any{"type": "issue", "fields": map[string]any{"title": title}})
	return port.ProposedChange{
		RequestID:    principal + ":" + title,
		Principal:    model.PrincipalRef{Kind: "user", ID: principal},
		BaseRevision: model.Natural64(1),
		Changes: []port.Change{{
			Resource: model.ResourceRef{Kind: "bead", Ref: model.RefByID("pool-state", "")},
			Op:       model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: model.RefByID("pool-state", ""), Fields: map[string]any{"title": title}}},
			After:    after,
		}},
	}
}

func (s *wasiBarrierSnapshot) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	select {
	case s.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func TestWASIGuestBridgeAcceptance(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("source-built guest acceptance requires the supported Linux/amd64 build host")
	}
	source := wasiCheckout(t, moduleRoot(t))
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{Source: source, GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	if artifact.Runtime() != pluginapi.RuntimeWASI {
		t.Fatalf("acceptance did not build WASI: %s", artifact.Runtime())
	}
	t.Logf("source-built guest commit=%s artifact=%s", artifact.Commit(), artifact.ArtifactDigest())
	for _, tc := range []struct {
		mode, reason string
		gets         int32
		trap         bool
	}{
		{mode: "undeclared", reason: pluginhost.ErrRead + ": query field fields.private is not declared"},
		{mode: "foreign-branch", reason: "memory supports branch zero", gets: 1},
		{mode: "foreign-cursor", reason: pluginhost.ErrRead + ": cursor is not bound to the callback snapshot"},
		{mode: "bounds", reason: "WASM host request is outside guest memory", gets: 1, trap: true},
		{mode: "oversized", reason: "WASM host request exceeds the message limit", gets: 1, trap: true},
		{mode: "expired", gets: 1, trap: true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			h := newWASIGuestStore(t)
			timeout := time.Duration(0)
			if tc.mode == "expired" {
				timeout = 100 * time.Millisecond
			}
			host := wasiGuestHost(t, artifact, timeout)
			// A working ordinary invocation precedes the callback. The bounds
			// fixture additionally completes a scoped guest read before trapping.
			if got, err := host.Invoke(context.Background(), "x.wasi.echo", json.RawMessage(`{"id":"acceptance"}`)); err != nil || string(got) != `{"id":"acceptance"}` {
				t.Fatalf("guest bootstrap: %s (%v)", got, err)
			}
			var gets, queries atomic.Int32
			readErrors := make(chan error, 2)
			callbackErrors := make(chan error, 2)
			binding := host.Mutators()[0]
			actual := binding.Mutator
			binding.Mutator = wasiGuestMutatorFunc(func(ctx context.Context, c port.ProposedChange, reader port.SnapshotReader) ([]port.Patch, error) {
				if view := reader.Snapshot(); view.At == nil || view.At.String() != c.BaseRevision.String() {
					return nil, fmt.Errorf("acceptance reader is not the actual callback snapshot")
				}
				tracked := wasiGuestReader{SnapshotReader: reader,
					get: func(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
						gets.Add(1)
						if tc.mode == "expired" {
							// The import has actually reached the real reader.
							// Let its callback expire, then ask that reader using
							// its expired context; do not manufacture an error.
							<-ctx.Done()
						}
						records, err := reader.Get(ctx, refs)
						readErrors <- err
						return records, err
					},
					query: func(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
						queries.Add(1)
						return reader.Query(ctx, q)
					},
				}
				patches, err := actual.Mutate(ctx, c, tracked)
				callbackErrors <- err
				return patches, err
			})
			writer := pipeline.New(h.store, h.lease, pipeline.WithOntology(h.hook), pipeline.WithMutators(binding), pipeline.WithValidators(host.Validators()...), pipeline.WithPostSubscribers(host.Posts()...))
			meta := wasiGuestMeta(2)
			before := h.state(t, meta)
			if before.Meta.Head.String() != "1" || len(before.Log) != 1 || len(before.Graph.Items) != 2 || before.Receipt != nil || before.ByKey != nil {
				t.Fatalf("invalid negative baseline: %+v", before)
			}
			commit, err := writer.Apply(context.Background(), h.by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: h.target, Fields: map[string]any{"title": "acceptance:" + tc.mode}}}}}, meta)
			var coreErr *model.Error
			if !errors.As(err, &coreErr) || coreErr.Data.Code != "subscriber_unavailable" || !commit.Revision.IsZero() || len(commit.Results) != 0 {
				t.Fatalf("guest refusal did not fail the ordinary Writer: %+v (%v)", commit, err)
			}
			var callbackErr error
			select {
			case callbackErr = <-callbackErrors:
			case <-time.After(2 * time.Second):
				t.Fatal("actual guest callback did not finish")
			}
			if tc.mode == "expired" {
				if !errors.Is(callbackErr, context.DeadlineExceeded) {
					t.Fatalf("expired guest callback did not preserve the deadline error: %v", callbackErr)
				}
			} else if callbackErr == nil || !strings.Contains(callbackErr.Error(), tc.reason) {
				t.Fatalf("guest did not issue the expected refused request: %v, want %q", callbackErr, tc.reason)
			}
			if tc.trap && tc.mode != "expired" && !hasPluginHostCode(callbackErr, pluginhost.ErrMethod) {
				t.Fatalf("invalid guest ABI did not return a typed host trap: %v", callbackErr)
			}
			if tc.gets != 0 {
				select {
				case readErr := <-readErrors:
					switch tc.mode {
					case "foreign-branch":
						if !errors.Is(readErr, driver.ErrUnsupported) {
							t.Fatalf("foreign branch did not return the real driver's typed refusal: %v", readErr)
						}
					case "expired":
						if !errors.Is(readErr, context.DeadlineExceeded) {
							t.Fatalf("expired actual snapshot read was not refused: %v", readErr)
						}
					default:
						if readErr != nil {
							t.Fatalf("bounds test lacked a usable scoped reader: %v", readErr)
						}
					}
				case <-time.After(2 * time.Second):
					t.Fatal("actual guest read did not finish")
				}
			}
			if gets.Load() != tc.gets || queries.Load() != 0 {
				t.Fatalf("unexpected underlying reads: get=%d query=%d want=%d/0", gets.Load(), queries.Load(), tc.gets)
			}
			echo, reuseErr := host.Invoke(context.Background(), "x.wasi.echo", json.RawMessage(`{"id":"after-refusal"}`))
			if reuseErr != nil || string(echo) != `{"id":"after-refusal"}` {
				t.Fatalf("method guest was affected by callback disposal: %s (%v)", echo, reuseErr)
			}
			// The failed callback member is disposed and removed from its pool;
			// the next callback for this principal must execute on a healthy
			// replacement member.
			replacement, replacementErr := host.Mutators()[0].Mutator.Mutate(context.Background(), port.ProposedChange{
				RequestID:    "replacement",
				Principal:    h.by,
				BaseRevision: model.Natural64(1),
				Changes: []port.Change{{
					Resource: model.ResourceRef{Kind: "bead", Ref: model.RefByID("pl-a", "")},
					Op:       model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: model.RefByID("pl-a", ""), Fields: map[string]any{"title": "replacement"}}},
					After:    []byte(`{"type":"issue","fields":{"title":"replacement"}}`),
				}},
			}, &wasiSnapshot{revision: model.Natural64(1)})
			if replacementErr != nil || len(replacement) != 1 {
				t.Fatalf("fresh callback member was not usable after disposal: patches=%v err=%v", replacement, replacementErr)
			}
			after := h.state(t, meta)
			if !bytes.Equal(wasiGuestJSON(t, before), wasiGuestJSON(t, after)) {
				t.Fatalf("refused guest changed graph, versions, log, head or receipt: before=%s after=%s", wasiGuestJSON(t, before), wasiGuestJSON(t, after))
			}
			t.Logf("actual guest %s: Writer code=%s callback=%v; head=1 graph=2 log=1 failed receipt absent; disposed=%v", tc.mode, coreErr.Data.Code, callbackErr, tc.trap)
		})
	}
	t.Run("stale-read-retry", func(t *testing.T) { testWASIGuestReadFactRetry(t, artifact) })
}

func testWASIGuestReadFactRetry(t *testing.T, artifact *pluginbuild.Artifact) {
	h := newWASIGuestStore(t)
	host := wasiGuestHost(t, artifact, 0)
	entered, release := make(chan struct{}), make(chan struct{})
	type readTrace struct{ base, record string }
	reads := make(chan readTrace, 4)
	callbacks := make(chan error, 4)
	var calls atomic.Int32
	binding := host.Mutators()[0]
	actual := binding.Mutator
	binding.Mutator = wasiGuestMutatorFunc(func(ctx context.Context, c port.ProposedChange, reader port.SnapshotReader) ([]port.Patch, error) {
		tracked := wasiGuestReader{SnapshotReader: reader, get: func(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
			if len(refs) != 1 || !wasiGuestSameRef(refs[0], h.watched) {
				return nil, fmt.Errorf("guest must read the separate watched record, got %v", refs)
			}
			records, err := reader.Get(ctx, refs)
			if err != nil || len(records) != 1 {
				return nil, fmt.Errorf("actual watched read: records=%d err=%v", len(records), err)
			}
			reads <- readTrace{base: reader.Snapshot().At.String(), record: records[0].Revision.String()}
			if calls.Add(1) == 1 {
				// The actual child reader has already recorded the watched
				// point fact. Pause its return while a separate Writer commits.
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return records, nil
		}}
		patches, err := actual.Mutate(ctx, c, tracked)
		callbacks <- err
		return patches, err
	})
	observed := &wasiGuestCommitObserver{Store: h.store, locks: make(chan wasiGuestLockTrace, 4)}
	writer, collector := pipeline.NewWithTelemetry(observed, h.lease, pipeline.WithOntology(h.hook), pipeline.WithMutators(binding), pipeline.WithValidators(host.Validators()...), pipeline.WithPostSubscribers(host.Posts()...))
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: h.target, Fields: map[string]any{"title": "acceptance:watch:" + h.watched.ID}}}}}
	meta := wasiGuestMeta(3)
	type outcome struct {
		value port.WriteOutcome
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := writer.ApplyWithOutcome(context.Background(), h.by, cs, meta)
		done <- outcome{value, err}
	}()
	select {
	case <-entered:
	case result := <-done:
		t.Fatalf("Writer finished without the actual watched guest read: %+v", result)
	case <-time.After(3 * time.Second):
		t.Fatal("guest never reached its watched read")
	}
	concurrent, err := h.writer.Apply(context.Background(), h.by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: h.watched, Fields: map[string]any{"title": "after"}}}}}, wasiGuestMeta(2))
	close(release)
	if err != nil || concurrent.Revision.String() != "2" {
		t.Fatalf("controlled concurrent Writer did not commit: %+v (%v)", concurrent, err)
	}
	var result outcome
	select {
	case result = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("guest Writer did not finish after the watched commit")
	}
	if result.err != nil || result.value.Replayed || result.value.Commit.Revision.String() != "3" || len(result.value.Commit.Results) != 1 || len(result.value.Commit.Warnings) != 0 {
		t.Fatalf("guest retry did not make one valid commit with successful post: %+v (%v)", result.value, result.err)
	}
	if calls.Load() != 2 || len(reads) != 2 || len(callbacks) != 2 || len(observed.locks) != 2 {
		t.Fatalf("retry counts: reads=%d traces=%d callbacks=%d locks=%d", calls.Load(), len(reads), len(callbacks), len(observed.locks))
	}
	for attempt, want := range []string{"1", "2"} {
		read, callbackErr, locked := <-reads, <-callbacks, <-observed.locks
		if read.base != want || read.record != want || callbackErr != nil {
			t.Fatalf("attempt %d did not execute the pinned guest read: %+v (%v)", attempt, read, callbackErr)
		}
		// This separate record is not the proposal's write target. Its shared
		// lock and 1->2 observation mismatch are supplied by the guest read.
		found := false
		for i, request := range locked.requests {
			if request.Key.Kind == driver.LockRecord && request.Key.Ref != nil && wasiGuestSameRef(*request.Key.Ref, h.watched) {
				found = request.Mode == driver.LockShare && locked.states[i].Present && locked.states[i].Revision.String() == "2"
			}
		}
		if !found {
			t.Fatalf("attempt %d omitted the real watched read fact from commit locks: %+v", attempt, locked)
		}
	}
	metrics := collector.Snapshot()
	if metrics.Conflicts[pipeline.ConflictStaleSnapshot] != 1 || metrics.LaneCount[pipeline.PhaseCommit][pipeline.ResultRetry] != 1 || metrics.Outcomes[pipeline.ResultCommit] != 1 {
		t.Fatalf("ordinary stale-read retry was not observed: %+v", metrics)
	}
	state := h.state(t, meta)
	if state.Meta.Head.String() != "3" || len(state.Graph.Items) != 2 || len(state.Log) != 3 || state.Receipt == nil || state.Receipt.ResponsePending || state.ByKey == nil {
		t.Fatalf("retry graph/log/receipt is incomplete: %+v", state)
	}
	if !bytes.Equal(wasiGuestJSON(t, state.Receipt.Commit), wasiGuestJSON(t, result.value.Commit)) {
		t.Fatal("durable receipt differs from the successful guest response")
	}
	for i, entry := range state.Log {
		if entry.Revision.String() != fmt.Sprint(i+1) || entry.IdempotencyKey != wasiGuestMeta(uint64(i+1)).IdempotencyKey {
			t.Fatalf("unexpected or extra changeset at %d: %+v", i, entry)
		}
	}
	if len(state.Log[1].Changes) != 1 || !wasiGuestSameRef(state.Log[1].Changes[0].Ref, h.watched) || len(state.Log[2].Changes) != 1 || !wasiGuestSameRef(state.Log[2].Changes[0].Ref, h.target) {
		t.Fatalf("stale attempt published changes or crossed targets: %+v", state.Log)
	}
	for _, record := range state.Graph.Items {
		body, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			t.Fatal(err)
		}
		if wasiGuestSameRef(record.Ref, h.target) && (body.Fields["normalized"] != "AFTER" || body.Fields["private"] != "host-only" || record.Revision.String() != "3") {
			t.Fatalf("guest published a stale or unmasked-derived target: %+v", body)
		}
		if wasiGuestSameRef(record.Ref, h.watched) && (body.Fields["title"] != "after" || body.Fields["private"] != "watched-secret" || record.Revision.String() != "2") {
			t.Fatalf("watched record changed outside its ordinary commit: %+v", body)
		}
	}
	if len(state.Versions[0]) != 2 || len(state.Versions[1]) != 2 {
		t.Fatalf("stale attempt left extra record versions: %+v", state.Versions)
	}
	replay, err := writer.ApplyWithOutcome(context.Background(), h.by, cs, meta)
	if err != nil || !replay.Replayed || !bytes.Equal(wasiGuestJSON(t, replay.Commit), wasiGuestJSON(t, result.value.Commit)) || calls.Load() != 2 {
		t.Fatalf("durable replay reran the guest or changed its receipt: %+v (%v)", replay, err)
	}
	if after := h.state(t, meta); !bytes.Equal(wasiGuestJSON(t, state), wasiGuestJSON(t, after)) {
		t.Fatal("receipt replay changed graph, versions, log or head")
	}
	t.Log("actual guest watched reads at bases 1/2; first commit lock observed watched revision2 against fact1; stale_snapshot=1 retry=1; normalized=AFTER; successful masked validate/post; head/log=3, two versions each, final receipt and replay unchanged")
}

// Native references can encode branch zero and their selected oneOf arm in
// different ways. Compare the same canonical identity used for commit locks.
func wasiGuestSameRef(a, b model.Ref) bool {
	x, xerr := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &a})
	y, yerr := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &b})
	return xerr == nil && yerr == nil && x == y
}

type wasiGuestLockTrace struct {
	requests []driver.LockRequest
	states   []driver.LockState
}

// Observe the real memory transactions without injecting a conflict/result.
// Embedding the concrete store preserves its response-finalization capability.
type wasiGuestCommitObserver struct {
	*memory.Store
	locks chan wasiGuestLockTrace
}

func (s *wasiGuestCommitObserver) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &wasiGuestObservedTx{WriteTx: tx, locks: s.locks}, nil
}

type wasiGuestObservedTx struct {
	driver.WriteTx
	locks chan wasiGuestLockTrace
}

func (tx *wasiGuestObservedTx) Lock(ctx context.Context, requests []driver.LockRequest) ([]driver.LockState, error) {
	states, err := tx.WriteTx.Lock(ctx, requests)
	if err == nil {
		tx.locks <- wasiGuestLockTrace{append([]driver.LockRequest(nil), requests...), append([]driver.LockState(nil), states...)}
	}
	return states, err
}

type wasiGuestMutatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error)

func (f wasiGuestMutatorFunc) Mutate(ctx context.Context, c port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return f(ctx, c, r)
}

// This interposer delegates to the actual Writer child reader. It supplies
// scheduling/observation only: neither records nor read facts are fabricated.
type wasiGuestReader struct {
	port.SnapshotReader
	get   func(context.Context, []model.Ref) ([]driver.Record, error)
	query func(context.Context, driver.Query) (driver.Page[driver.Record], error)
}

func (r wasiGuestReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	return r.get(ctx, refs)
}

func (r wasiGuestReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if r.query != nil {
		return r.query(ctx, q)
	}
	return r.SnapshotReader.Query(ctx, q)
}

type wasiGuestStore struct {
	store   *memory.Store
	lease   driver.ActiveLease
	hook    *service.Ontology
	writer  *pipeline.Writer
	by      model.PrincipalRef
	watched model.Ref
	target  model.Ref
}

func wasiGuestMeta(slot uint64) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%032x", slot)}
}

func newWASIGuestStore(t *testing.T) *wasiGuestStore {
	t.Helper()
	h := &wasiGuestStore{store: memory.New(), by: model.PrincipalRef{Kind: "user", ID: "wasi-guest-acceptance"}}
	t.Cleanup(func() { _ = h.store.Close() })
	var err error
	h.lease, err = h.store.Lease(context.Background(), "wasi-guest-acceptance", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.lease.Release(context.Background()) })
	doc := ontology.DefaultDocument()
	doc.Types["issue"].Fields["normalized"] = ontology.Field{Type: "string", Owner: "wasi-normalize"}
	doc.Types["issue"].Fields["private"] = ontology.Field{Type: "string"}
	compiled, problems := ontology.Compile(doc, nil, ontology.PluginCatalog{"wasi-normalize": true})
	if len(problems) != 0 {
		t.Fatalf("acceptance ontology: %v", problems)
	}
	h.hook = service.NewOntology(func(string) ontology.Effective { return compiled.Effective("") })
	var next int
	h.writer = pipeline.New(h.store, h.lease, pipeline.WithOntology(h.hook), pipeline.WithIDGenerator(func() (string, error) {
		next++
		return fmt.Sprintf("wasi-guest-%d", next), nil
	}))
	seed, err := h.writer.Apply(context.Background(), h.by, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "before", "private": "watched-secret"}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "seed", "private": "host-only"}}},
	}}, wasiGuestMeta(1))
	if err != nil || seed.Revision.String() != "1" || len(seed.Results) != 2 {
		t.Fatalf("acceptance seed: %+v (%v)", seed, err)
	}
	h.watched, h.target = seed.Results[0].Ref, seed.Results[1].Ref
	return h
}

func wasiGuestHost(t *testing.T, artifact *pluginbuild.Artifact, timeout time.Duration) *pluginhost.Host {
	t.Helper()
	host, err := artifact.Host(context.Background(), pluginhost.Config{Bindings: []pluginhost.Binding{
		{ID: "wasi-normalize", Alias: "wasi", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}, Timeout: timeout},
		{ID: "wasi-check", Alias: "wasi", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.title"}},
		{ID: "wasi-observe", Alias: "wasi", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

type wasiGuestDurableState struct {
	Meta     driver.Meta
	Graph    driver.Page[driver.Record]
	Versions [][]driver.Version
	Log      []model.Changeset
	Receipt  *driver.IdempotencyRecord
	ByKey    *model.Changeset
}

func (h *wasiGuestStore) state(t *testing.T, meta model.WriteMeta) wasiGuestDurableState {
	t.Helper()
	ctx := context.Background()
	var state wasiGuestDurableState
	var err error
	state.Meta, err = h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := h.store.Read(ctx, driver.View{Incarnation: state.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	state.Graph, err = reader.Query(ctx, driver.Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []model.Ref{h.watched, h.target} {
		versions, err := reader.Versions(ctx, ref, driver.PageReq{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		state.Versions = append(state.Versions, versions.Items)
	}
	state.Log, err = reader.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(h.by)
	if err != nil {
		t.Fatal(err)
	}
	state.Receipt, err = reader.IdempotencyByKey(ctx, principal, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	state.ByKey, err = reader.ChangesetByKey(ctx, principal, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func wasiGuestJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestBuildFailureAndCancellationAreRefusals(t *testing.T) {
	root := moduleRoot(t)
	goBinary := goPath(t)
	broken := checkout(t, root, true)
	_, err := pluginbuild.Build(context.Background(), pluginbuild.Request{TrustedNative: true, Source: broken, ModuleRoot: root, GoBinary: goBinary})
	var refusal *pluginbuild.Error
	if !errors.As(err, &refusal) || refusal.Code != pluginbuild.ErrBuild {
		t.Fatalf("broken source did not return build_failed: %v", err)
	}

	t.Log("actual invalid Go source returned build_failed")
	// An empty private cache makes compilation observable and prevents a cache
	// hit from finishing before cancellation. No wrapper or fake tool is used.
	cache := filepath.Join(t.TempDir(), "go-cache")
	t.Setenv("GOCACHE", cache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := checkout(t, root, false)
	done := make(chan error, 1)
	go func() {
		artifact, err := pluginbuild.Build(ctx, pluginbuild.Request{TrustedNative: true, Source: source, ModuleRoot: root, GoBinary: goBinary})
		if artifact != nil {
			_ = artifact.Close()
		}
		done <- err
	}()
	started := false
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !started {
		select {
		case err := <-done:
			t.Fatalf("real Go build finished before cancellation: %v", err)
		case <-deadline.C:
			t.Fatal("real Go build never opened its build cache")
		case <-ticker.C:
			_, err := os.Stat(filepath.Join(cache, "README"))
			started = err == nil
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled real build was not stopped: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("real Go build did not stop after cancellation")
	}
	t.Log("real Go build opened a fresh compiler cache; cancellation stopped the process group")
}

func TestSourceAndArtifactDigestDriftRefuseAdmission(t *testing.T) {
	root := moduleRoot(t)
	goBinary := goPath(t)
	source := checkout(t, root, false)
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{TrustedNative: true, Source: source, ModuleRoot: root, GoBinary: goBinary})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	second, err := pluginbuild.Build(context.Background(), pluginbuild.Request{TrustedNative: true, Source: source, ModuleRoot: root, GoBinary: goBinary})
	if err != nil {
		t.Fatal(err)
	}
	if second.ArtifactDigest() != artifact.ArtifactDigest() {
		_ = second.Close()
		t.Fatal("the same pinned source produced a different artifact digest")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = artifact.Host(context.Background(), pluginhost.Config{Natives: []sdk.Native{{}}})
	requireBuildCode(t, err, pluginbuild.ErrConfig)
	if err := os.WriteFile(filepath.Join(source.Root, "plugin.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := artifact.Verify(context.Background()); err == nil {
		t.Fatal("source revision drift was admitted")
	}
	// Restore the source so the output check is isolated from checkout drift.
	writeFixture(t, source.Root, root, false)
	if err := artifact.Verify(context.Background()); err != nil {
		t.Fatalf("restored source failed before isolated drift checks: %v", err)
	}
	git(t, source.Root, "commit", "-q", "--no-verify", "--allow-empty", "-m", "move source revision")
	if err := artifact.Verify(context.Background()); err == nil {
		t.Fatal("changed source HEAD was admitted")
	}
	git(t, source.Root, "reset", "--soft", source.Commit)
	if err := artifact.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact.Path(), []byte("not a plugin"), 0o755); err != nil {
		t.Fatal(err)
	}
	requireBuildCode(t, artifact.Verify(context.Background()), pluginbuild.ErrDigest)
	artifactPath := artifact.Path()
	if err := artifact.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(artifactPath); !os.IsNotExist(err) {
		t.Fatalf("Close retained output: %v", err)
	}
	requireBuildCode(t, artifact.Verify(context.Background()), pluginbuild.ErrClosed)
	t.Log("source bytes, source HEAD, artifact digest, native substitution and closed output all refused")
}

func TestExplicitTrustPackageContainmentAndDigestRefusals(t *testing.T) {
	_, err := pluginbuild.Build(context.Background(), pluginbuild.Request{})
	requireBuildCode(t, err, pluginbuild.ErrConfig)
	for _, name := range []string{"-x", "../outside", "/absolute", "./..."} {
		t.Run(name, func(t *testing.T) {
			_, err := pluginbuild.Build(context.Background(), pluginbuild.Request{TrustedNative: true, Package: name})
			requireBuildCode(t, err, pluginbuild.ErrConfig)
		})
	}
	root := moduleRoot(t)
	source := checkout(t, root, false)
	source.ExpectedDigest = strings.Repeat("0", 64)
	_, err = pluginbuild.Build(context.Background(), pluginbuild.Request{TrustedNative: true, Source: source})
	var bundleError *pluginmanifest.Error
	if !errors.As(err, &bundleError) || bundleError.Code != "digest_mismatch" {
		t.Fatalf("wrong source digest was not refused: %v", err)
	}
	source.ExpectedDigest = ""
	moduleFile := filepath.Join(source.Root, "go.mod")
	data, err := os.ReadFile(moduleFile)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, []byte("\nreplace fixture.example/external => "+t.TempDir()+"\n")...)
	if err := os.WriteFile(moduleFile, data, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, source.Root, "add", "go.mod")
	git(t, source.Root, "commit", "-q", "--no-verify", "-m", "external module replacement")
	source.Commit = git(t, source.Root, "rev-parse", "HEAD")
	_, err = pluginbuild.Build(context.Background(), pluginbuild.Request{TrustedNative: true, Source: source, ModuleRoot: root, GoBinary: goPath(t)})
	requireBuildCode(t, err, pluginbuild.ErrConfig)
	t.Log("explicit trust, local package arguments, source digest and external local dependency boundaries enforced")
}

func requireBuildCode(t *testing.T, err error, code string) {
	t.Helper()
	var refusal *pluginbuild.Error
	if !errors.As(err, &refusal) || refusal.Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

func runHelper(t *testing.T, root string, source pluginmanifest.Checkout) map[string]any {
	t.Helper()
	goBinary := goPath(t)
	helper := t.TempDir()
	if err := os.WriteFile(filepath.Join(helper, "main.go"), []byte(helperMain), 0o644); err != nil {
		t.Fatal(err)
	}
	moduleCache := strings.TrimSpace(string(mustCommand(t, goBinary, "env", "GOMODCACHE")))
	if home, err := os.UserHomeDir(); err == nil {
		candidate := filepath.Join(home, "go", "pkg", "mod")
		if _, statErr := os.Stat(filepath.Join(candidate, "github.com", "tetratelabs", "wazero@v1.9.0")); statErr == nil {
			moduleCache = candidate
		}
	}
	wazeroRoot := offlineModule(t, filepath.Join(moduleCache, "github.com", "tetratelabs", "wazero@v1.9.0"), "com_github_tetratelabs_wazero")
	sysRoot := offlineModule(t, filepath.Join(moduleCache, "golang.org", "x", "sys@v0.49.0"), "org_golang_x_sys")
	goMod := fmt.Sprintf("module github.com/buildbetter-app/plugs/runner\n\ngo 1.26.5\n\ntoolchain go1.26.10\n\nrequire (\n\tgithub.com/buildbetter-app/plugs v0.0.0\n\tgithub.com/tetratelabs/wazero v1.9.0\n\tgolang.org/x/sys v0.49.0\n)\n\nreplace github.com/buildbetter-app/plugs => %s\nreplace github.com/tetratelabs/wazero => %s\nreplace golang.org/x/sys => %s\n", root, wazeroRoot, sysRoot)
	if err := os.WriteFile(filepath.Join(helper, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatal(err)
	}
	rootSums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	var helperSums []byte
	for _, line := range strings.Split(string(rootSums), "\n") {
		if strings.HasPrefix(line, "github.com/tetratelabs/wazero ") || strings.HasPrefix(line, "golang.org/x/sys ") {
			helperSums = append(helperSums, []byte(line+"\n")...)
		}
	}
	if err := os.WriteFile(filepath.Join(helper, "go.sum"), helperSums, 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(goBinary, "run", "-p=2", "-trimpath", "-buildvcs=false", "-mod=readonly", ".")
	command.Dir = helper
	command.Env = append(os.Environ(),
		"GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOVCS=*:off", "GOWORK=off", "GOENV=off", "GOFLAGS=", "CGO_ENABLED=1",
		"SOURCE_ROOT="+source.Root, "SOURCE_COMMIT="+source.Commit, "PLUG_ROOT="+root, "GO_BINARY="+goBinary,
	)
	data, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("external helper failed: %v\n%s", err, data)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("helper returned non-JSON output: %v\n%s", err, data)
	}
	return result
}

func offlineModule(t *testing.T, cached, bazelRepository string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(cached, "go.mod")); err == nil {
		return cached
	}
	for current := mustWorkingDir(t); current != filepath.Dir(current); current = filepath.Dir(current) {
		candidate := filepath.Join(current, "external", "gazelle++go_deps+"+bazelRepository)
		if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err == nil {
			return candidate
		}
	}
	t.Fatalf("dependency source unavailable in offline cache or Bazel repository: %s", bazelRepository)
	return ""
}

func mustWorkingDir(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func mustCommand(t *testing.T, binary string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	data, err := command.Output()
	if err != nil {
		t.Fatalf("%s %v: %v", binary, args, err)
	}
	return data
}

func checkout(t *testing.T, root string, broken bool) pluginmanifest.Checkout {
	t.Helper()
	source := t.TempDir()
	writeFixture(t, source, root, broken)
	git(t, source, "init", "-q", "--template=", "--initial-branch=main")
	git(t, source, "add", "--all")
	git(t, source, "commit", "-q", "--no-verify", "-m", "source fixture")
	commit := git(t, source, "rev-parse", "HEAD")
	return pluginmanifest.Checkout{Root: source, Commit: commit}
}

func wasiCheckout(t *testing.T, root string) pluginmanifest.Checkout {
	t.Helper()
	source := t.TempDir()
	if err := fs.WalkDir(wasiSourceFiles, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := wasiSourceFiles.ReadFile(name)
		if err != nil {
			return err
		}
		if name == "fixture-wasi/go.mod.template" {
			name = "fixture-wasi/go.mod"
			data = []byte(strings.ReplaceAll(string(data), "__PLUG_ROOT__", root))
		}
		return os.WriteFile(filepath.Join(source, strings.TrimPrefix(name, "fixture-wasi/")), data, 0o644)
	}); err != nil {
		t.Fatal(err)
	}
	git(t, source, "init", "-q", "--template=", "--initial-branch=main")
	git(t, source, "add", "--all")
	git(t, source, "commit", "-q", "--no-verify", "-m", "WASI source fixture")
	return pluginmanifest.Checkout{Root: source, Commit: git(t, source, "rev-parse", "HEAD")}
}

func slowWASICheckout(t *testing.T, root string) pluginmanifest.Checkout {
	t.Helper()
	source := wasiCheckout(t, root)
	path := filepath.Join(source.Root, "plugin.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, []byte("\nfunc init() { time.Sleep(1 * time.Second) }\n")...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, source.Root, "add", "plugin.go")
	git(t, source.Root, "commit", "-q", "--no-verify", "-m", "test: gate WASI initializer")
	source.Commit = git(t, source.Root, "rev-parse", "HEAD")
	return source
}

func writeFixture(t *testing.T, source, root string, broken bool) {
	t.Helper()
	if err := fs.WalkDir(sourceFiles, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := sourceFiles.ReadFile(name)
		if err != nil {
			return err
		}
		if name == "fixture/go.mod.template" {
			name = "fixture/go.mod"
			data = []byte(strings.ReplaceAll(string(data), "__PLUG_ROOT__", root))
		}
		if broken && name == "fixture/plugin.go" {
			data = append(data, []byte("\nthis is invalid Go\n")...)
		}
		return os.WriteFile(filepath.Join(source, strings.TrimPrefix(name, "fixture/")), data, 0o644)
	}); err != nil {
		t.Fatal(err)
	}
}

func git(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-c", "core.hooksPath=" + os.DevNull, "-c", "commit.gpgsign=false", "-C", root}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_AUTHOR_NAME=PlugTest", "GIT_AUTHOR_EMAIL=plug@example.test", "GIT_COMMITTER_NAME=PlugTest", "GIT_COMMITTER_EMAIL=plug@example.test", "GIT_AUTHOR_DATE=2001-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2001-01-01T00:00:00Z")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", args[0], err, output)
	}
	return strings.TrimSpace(string(output))
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	candidates := []string{}
	if value := os.Getenv("PLUG_SOURCE_MODULE_ROOT"); value != "" {
		candidates = append(candidates, value)
	}
	if value := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); value != "" {
		candidates = append(candidates, value)
	}
	if cwd, err := os.Getwd(); err == nil {
		for _, marker := range []string{"/execroot/_main/", "/execroot/__main__/"} {
			if index := strings.Index(cwd, marker); index >= 0 {
				candidates = append(candidates, cwd[:index+len(marker)-1])
			}
		}
		if index := strings.Index(cwd, "/sandbox/processwrapper-sandbox/"); index >= 0 {
			// processwrapper gives the test a sparse execroot. The controller
			// source tree remains linked from the corresponding unsandboxed
			// execroot and is used only as a local module replacement.
			candidates = append(candidates, filepath.Join(cwd[:index], "execroot", "_main"))
		}
		for current := cwd; ; current = filepath.Dir(current) {
			candidates = append(candidates, current)
			parent := filepath.Dir(current)
			if parent == current {
				break
			}
		}
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		for current := filepath.Dir(file); ; current = filepath.Dir(current) {
			candidates = append(candidates, current)
			parent := filepath.Dir(current)
			if parent == current {
				break
			}
		}
	}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		candidate, err := filepath.Abs(candidate)
		if err != nil || seen[candidate] {
			continue
		}
		seen[candidate] = true
		if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(candidate, "pluginapi", "manifest.go")); err == nil {
				return candidate
			}
		}
	}
	t.Fatalf("could not locate Plug module root (cwd=%s)", mustGetwd())
	return ""
}

func mustGetwd() string {
	cwd, _ := os.Getwd()
	return cwd
}

func goPath(t *testing.T) string {
	t.Helper()
	pluginbuild.UseBazelSDK(t)
	if path, err := exec.LookPath("go"); err == nil {
		return path
	}
	for _, path := range []string{"/home/dev/.nix-profile/bin/go", "/usr/local/go/bin/go", "/usr/bin/go"} {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 {
			return path
		}
	}
	t.Fatal("Go toolchain is unavailable to the source-build behavior test")
	return ""
}
