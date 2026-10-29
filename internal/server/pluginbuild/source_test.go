package pluginbuild_test

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
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

//go:embed fixture-wasi/go.mod.template fixture-wasi/plugin.go fixture-wasi/plug-plugin.json
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
	host, err := artifact.Host(ctx, pluginhost.Config{Bindings: []pluginhost.Binding{
		{ID: "wasi-normalize", Alias: "wasi", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}},
		{ID: "wasi-check", Alias: "wasi", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.title"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	echo, err := host.Invoke(ctx, "x.wasi.echo", json.RawMessage(`{"id":"wasi-1","value":9007199254740993}`))
	if err != nil || string(echo) != `{"id":"wasi-1","value":9007199254740993}` {
		t.Fatalf("WASI method did not round-trip JSON: %s (%v)", echo, err)
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
	restored, checks, err := lockStore.RestoreAndCheck(ctx, lock, func(context.Context) (*pluginbuild.Artifact, error) {
		<-block
		return pluginbuild.Build(ctx, pluginbuild.Request{Runtime: pluginapi.RuntimeWASI, Source: source, GoBinary: goPath(t)})
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
	_ = restored.Close()
	t.Log("real GOOS=wasip1 build -> wazero host callbacks -> digest restore before asynchronous rebuild; undeclared patch refused")
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
	wazeroRoot := filepath.Join(moduleCache, "github.com", "tetratelabs", "wazero@v1.9.0")
	if _, err := os.Stat(wazeroRoot); err != nil {
		for current := mustWorkingDir(t); current != filepath.Dir(current); current = filepath.Dir(current) {
			candidate := filepath.Join(current, "external", "gazelle++go_deps+com_github_tetratelabs_wazero")
			if _, statErr := os.Stat(filepath.Join(candidate, "go.mod")); statErr == nil {
				wazeroRoot = candidate
				break
			}
		}
	}
	if _, err := os.Stat(wazeroRoot); err != nil {
		t.Fatalf("wazero source is not available in the offline module cache: %v", err)
	}
	goMod := fmt.Sprintf("module github.com/buildbetter-app/plugs/runner\n\ngo 1.26.5\n\ntoolchain go1.26.10\n\nrequire (\n\tgithub.com/buildbetter-app/plugs v0.0.0\n\tgithub.com/tetratelabs/wazero v1.9.0\n)\n\nreplace github.com/buildbetter-app/plugs => %s\nreplace github.com/tetratelabs/wazero => %s\n", root, wazeroRoot)
	if err := os.WriteFile(filepath.Join(helper, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatal(err)
	}
	rootSums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	var helperSums []byte
	for _, line := range strings.Split(string(rootSums), "\n") {
		if strings.HasPrefix(line, "github.com/tetratelabs/wazero ") {
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
