// Package pluginbuild builds a pinned native or wasip1 plugin from a local
// source checkout and admits only the resulting implementation to pluginhost.
//
// This is deliberately a small source-build slice. It does not fetch source,
// install a plugin, start a listener, or expose a write-capable control-plane
// adapter. ArtifactStore provides only the process-local lock/restore seam;
// the caller chooses the already checked-out commit and pluginmanifest owns
// source and public-manifest validation.
package pluginbuild

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"plugin"
	"runtime"
	"strings"
	"sync"

	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// Entrypoint is the one symbol a source plugin must export. The function is
// intentionally typed with the public SDK so the host never accepts a private
// or synthetic callback shape.
const Entrypoint = "PlugNatives"

const (
	defaultGoBinary = "go"
	maxBuildOutput  = 32 << 10
)

const (
	ErrBuild       = "build_failed"
	ErrArtifact    = "artifact_invalid"
	ErrDigest      = "artifact_digest_mismatch"
	ErrEntrypoint  = "entrypoint_invalid"
	ErrConfig      = "host_config_invalid"
	ErrClosed      = "artifact_closed"
	ErrUnsupported = "unsupported_platform"
	ErrContainment = "containment_unavailable"
)

// Request selects one already checked-out source revision. No URL, tag,
// branch, fetch token, or module proxy is accepted here. GoBinary is an
// optional executable override used by controlled callers and tests; it is
// executed directly, never through a shell.
type Request struct {
	// TrustedNative is an explicit opt-in to trusted, unisolated native code.
	// wasip1 builds require Linux build containment and a sandboxed runtime.
	TrustedNative bool
	// Runtime is empty for the source manifest's sole runtime. Explicit values
	// are "native" and "wasip1" and must match every declared plugin.
	Runtime    string
	Source     pluginmanifest.Checkout
	Package    string
	Export     string
	ModuleRoot string
	GoBinary   string
}

// File is the immutable inventory entry for a produced artifact.
type File struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Executable bool   `json:"executable"`
}

// Artifact is a disposable build result. Its source and output digests are
// retained so a later host admission can refuse source or output drift.
type Artifact struct {
	bundle         *pluginmanifest.Bundle
	manifest       pluginapi.Manifest
	outputDir      string
	path           string
	commit         string
	sourceDigest   string
	manifestDigest string
	digest         string
	runtime        string
	files          []File

	mu      sync.Mutex
	closed  bool
	hosts   map[*pluginhost.Host]struct{}
	loaded  bool
	loadedP *plugin.Plugin
	natives []sdk.Native
}

// Build verifies the complete local checkout, builds its root Go package with
// a fixed offline command, and inventories the disposable output. A native
// build is explicitly trusted; a wasip1 build requires Linux containment and
// is admitted only through the sandboxed two-export host. The output directory
// is removed on every failure.
func Build(ctx context.Context, request Request) (*Artifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Preserve a clear configuration refusal for an entirely empty request.
	// Runtime inference below must remain reachable when a pinned source
	// manifest declares wasip1; that runtime does not require TrustedNative.
	if !request.TrustedNative && request.Runtime == "" && request.Source.Root == "" {
		return nil, refusal(ErrConfig, "trusted native source must be explicitly selected")
	}
	packagePath := request.Package
	if packagePath == "" {
		packagePath = "."
	}
	if !safePackage(packagePath) {
		return nil, refusal(ErrConfig, "package must be a relative Go package directory")
	}
	bundle, err := pluginmanifest.Load(ctx, request.Source)
	if err != nil {
		return nil, err
	}
	manifest, err := bundle.Manifest()
	if err != nil {
		return nil, err
	}
	runtimeMode := request.Runtime
	for _, declared := range manifest.Plugins {
		if runtimeMode == "" {
			runtimeMode = declared.Runtime
		} else if declared.Runtime != runtimeMode {
			return nil, refusal(ErrConfig, "one source build cannot mix native and wasip1 plugins")
		}
	}
	if runtimeMode != pluginapi.RuntimeNative && runtimeMode != pluginapi.RuntimeWASI {
		return nil, refusal(ErrConfig, "unsupported plugin runtime %q", runtimeMode)
	}
	for _, declared := range manifest.Plugins {
		if declared.Runtime != runtimeMode {
			return nil, refusal(ErrConfig, "requested runtime %q differs from plugin %q", runtimeMode, declared.Alias)
		}
	}
	if runtimeMode == pluginapi.RuntimeNative {
		if !request.TrustedNative {
			return nil, refusal(ErrConfig, "trusted native source must be explicitly selected")
		}
		if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "freebsd" {
			return nil, refusal(ErrUnsupported, "native Go plugins are unsupported on %s", runtime.GOOS)
		}
	} else if request.Export != "" {
		return nil, refusal(ErrConfig, "wasip1 uses the fixed plug_alloc/plug_handle ABI")
	}
	if runtimeMode == pluginapi.RuntimeNative {
		export := request.Export
		if export == "" {
			export = Entrypoint
		}
		if export != Entrypoint {
			return nil, refusal(ErrConfig, "source entrypoint must be %s", Entrypoint)
		}
	}
	if request.ModuleRoot != "" {
		if err := validateModuleRoot(request.ModuleRoot); err != nil {
			return nil, err
		}
	}
	goBinary := request.GoBinary
	if goBinary == "" {
		goBinary = defaultGoBinary
	}
	outputDir, err := os.MkdirTemp("", "plug-build-")
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(outputDir) }
	stagingDir, err := os.MkdirTemp("", "plug-source-")
	if err != nil {
		cleanup()
		return nil, err
	}
	defer os.RemoveAll(stagingDir)
	if err := copySource(ctx, bundle, stagingDir); err != nil {
		cleanup()
		return nil, err
	}
	packageDir := filepath.Join(stagingDir, filepath.FromSlash(packagePath))
	if info, err := os.Stat(packageDir); err != nil || !info.IsDir() {
		cleanup()
		return nil, refusal(ErrConfig, "package must name a directory in the pinned source")
	}
	run := func(ctx context.Context, args ...string) ([]byte, error) {
		return runGo(ctx, goBinary, stagingDir, args...)
	}
	if runtimeMode == pluginapi.RuntimeWASI {
		run, err = containedGo(goBinary, stagingDir, outputDir, request.ModuleRoot)
		if err != nil {
			cleanup()
			return nil, err
		}
		defer os.RemoveAll(filepath.Join(outputDir, ".build"))
	}
	moduleFile, err := prepareModuleFile(ctx, run, stagingDir, request.ModuleRoot)
	if err != nil {
		cleanup()
		return nil, err
	}
	outputName := "plugin.so"
	if runtimeMode == pluginapi.RuntimeWASI {
		outputName = "plugin.wasm"
	}
	outputPath := filepath.Join(outputDir, outputName)
	args := []string{"build", "-p=2", "-trimpath", "-buildvcs=false", "-mod=readonly", "-modfile=" + filepath.Base(moduleFile), "-o", outputPath, "./" + path.Clean(packagePath)}
	if runtimeMode == pluginapi.RuntimeNative {
		args = []string{"build", "-p=2", "-trimpath", "-buildvcs=false", "-buildmode=plugin", "-mod=readonly", "-modfile=" + filepath.Base(moduleFile), "-o", outputPath, "./" + path.Clean(packagePath)}
	} else {
		args = append([]string{"build", "-p=2", "-trimpath", "-buildvcs=false", "-buildmode=c-shared", "-mod=readonly", "-modfile=" + filepath.Base(moduleFile), "-o", outputPath}, "./"+path.Clean(packagePath))
	}
	_, err = run(ctx, args...)
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return nil, err
	}
	if err := bundle.Verify(ctx); err != nil {
		cleanup()
		return nil, err
	}
	files, digest, err := inventory(outputPath)
	if err != nil {
		cleanup()
		return nil, err
	}
	if runtimeMode == pluginapi.RuntimeWASI {
		data, readErr := os.ReadFile(outputPath)
		if readErr != nil {
			cleanup()
			return nil, refusal(ErrArtifact, "WASM artifact cannot be read: %v", readErr)
		}
		if err := pluginhost.ValidateWASMArtifact(data); err != nil {
			cleanup()
			return nil, err
		}
	}
	return &Artifact{bundle: bundle, manifest: manifest, outputDir: outputDir, path: outputPath, commit: bundle.Commit(), sourceDigest: bundle.Digest(), manifestDigest: bundle.ManifestDigest(), digest: digest, runtime: runtimeMode, files: files}, nil
}

// BuildSource is a descriptive alias for callers that prefer the operation's
// source-building name.
func BuildSource(ctx context.Context, request Request) (*Artifact, error) {
	return Build(ctx, request)
}

// Commit is the full lowercase Git object id admitted by the manifest loader.
func (a *Artifact) Commit() string {
	if a == nil {
		return ""
	}
	return a.commit
}

// SourceDigest is the manifest loader's complete source/content digest.
func (a *Artifact) SourceDigest() string {
	if a == nil {
		return ""
	}
	return a.sourceDigest
}

// ManifestDigest is the canonical public-manifest digest.
func (a *Artifact) ManifestDigest() string {
	if a == nil {
		return ""
	}
	return a.manifestDigest
}

// ArtifactDigest is the SHA-256 of the produced plugin bytes. Digest is kept
// as a short alias for lock and inventory callers.
func (a *Artifact) ArtifactDigest() string {
	if a == nil {
		return ""
	}
	return a.digest
}

func (a *Artifact) Digest() string { return a.ArtifactDigest() }

// Runtime returns the admitted source runtime (native or wasip1).
func (a *Artifact) Runtime() string {
	if a == nil {
		return ""
	}
	return a.runtime
}

// Path returns the disposable output path. Callers must not retain it after
// Close; Host verifies it before every admission.
func (a *Artifact) Path() string {
	if a == nil {
		return ""
	}
	return a.path
}

// Files returns a detached artifact inventory.
func (a *Artifact) Files() []File {
	if a == nil {
		return nil
	}
	return append([]File(nil), a.files...)
}

// Source returns the immutable checkout identity used for this artifact.
func (a *Artifact) Source() pluginmanifest.Checkout {
	if a == nil {
		return pluginmanifest.Checkout{}
	}
	if a.bundle == nil {
		return pluginmanifest.Checkout{Commit: a.commit, ExpectedDigest: a.sourceDigest}
	}
	return pluginmanifest.Checkout{Root: a.bundle.Root(), Commit: a.bundle.Commit(), ManifestFile: a.bundle.ManifestPath(), ExpectedDigest: a.bundle.Digest()}
}

// Manifest returns a detached copy of the validated public manifest.
func (a *Artifact) Manifest() (pluginapi.Manifest, error) {
	if a == nil {
		return pluginapi.Manifest{}, refusal(ErrClosed, "artifact is required")
	}
	if a.bundle == nil {
		return pluginapi.CloneJSON(a.manifest)
	}
	return a.bundle.Manifest()
}

// Verify rechecks both the pinned source and produced output. It is safe to
// call before every use and deliberately refuses a changed checkout, removed
// output, mode change, or digest drift.
func (a *Artifact) Verify(ctx context.Context) error {
	if a == nil {
		return refusal(ErrClosed, "artifact is required")
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return refusal(ErrClosed, "artifact has been closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.bundle != nil {
		if err := a.bundle.Verify(ctx); err != nil {
			return err
		}
	}
	files, digest, err := inventory(a.path)
	if err != nil {
		return err
	}
	if digest != a.digest || len(files) != len(a.files) || len(files) == 0 || files[0] != a.files[0] {
		return refusal(ErrDigest, "produced artifact bytes differ from the recorded digest")
	}
	if a.runtime == pluginapi.RuntimeWASI {
		data, err := os.ReadFile(a.path)
		if err != nil {
			return refusal(ErrDigest, "WASM artifact cannot be read: %v", err)
		}
		if err := pluginhost.ValidateWASMArtifact(data); err != nil {
			return err
		}
	}
	return nil
}

// Host loads the exported implementation from the produced plugin and passes
// only those descriptors through the real manifest adapter and plugin host.
// Native implementations cannot be supplied by the caller, so a test fixture
// or compiled-in substitute cannot silently stand in for source execution.
// Bindings remain explicit; no subscriber, validator, ontology method,
// migration writer, listener, or network server is created here.
func (a *Artifact) Host(ctx context.Context, config pluginhost.Config) (*pluginhost.Host, error) {
	if len(config.Natives) != 0 {
		return nil, refusal(ErrConfig, "native implementations must come from the source artifact")
	}
	if err := a.Verify(ctx); err != nil {
		return nil, err
	}
	var err error
	var natives []sdk.Native
	if a.runtime == pluginapi.RuntimeWASI {
		data, err := os.ReadFile(a.path)
		if err != nil {
			return nil, refusal(ErrArtifact, "WASM artifact cannot be read: %v", err)
		}
		manifest, err := a.Manifest()
		if err != nil {
			return nil, err
		}
		for _, declared := range manifest.Plugins {
			native, err := pluginhost.NewWASMNative(data, declared, "default")
			if err != nil {
				return nil, err
			}
			natives = append(natives, native)
		}
	} else {
		var err error
		natives, err = a.loadNatives(ctx)
		if err != nil {
			return nil, err
		}
	}
	var descriptors []sdk.Native
	if a.bundle != nil {
		descriptors, err = a.bundle.Natives(ctx, natives)
		if err != nil {
			return nil, err
		}
	} else {
		if err := validateRestoredNatives(a.manifest, natives); err != nil {
			return nil, err
		}
		descriptors = natives
	}
	config.Natives = descriptors
	host, err := pluginhost.New(config)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		_ = host.Close()
		return nil, refusal(ErrClosed, "artifact has been closed")
	}
	if a.hosts == nil {
		a.hosts = map[*pluginhost.Host]struct{}{}
	}
	a.hosts[host] = struct{}{}
	a.mu.Unlock()
	return host, nil
}

// Close removes the disposable output directory. A loaded Go plugin has no
// unload operation; removing its file is still safe on supported Unix hosts,
// and subsequent admission is refused by the closed artifact state.
func (a *Artifact) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	dir := a.outputDir
	hosts := make([]*pluginhost.Host, 0, len(a.hosts))
	for host := range a.hosts {
		hosts = append(hosts, host)
	}
	a.hosts = nil
	a.mu.Unlock()
	var closeErr error
	for _, host := range hosts {
		if err := host.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if err := os.RemoveAll(dir); err != nil && closeErr == nil {
		closeErr = err
	}
	return closeErr
}

func (a *Artifact) loadNatives(ctx context.Context) ([]sdk.Native, error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, refusal(ErrClosed, "artifact has been closed")
	}
	if a.loaded {
		natives := append([]sdk.Native(nil), a.natives...)
		a.mu.Unlock()
		return natives, nil
	}
	a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loaded, err := plugin.Open(a.path)
	if err != nil {
		return nil, refusal(ErrEntrypoint, "open native plugin: %v", err)
	}
	symbol, err := loaded.Lookup(Entrypoint)
	if err != nil {
		return nil, refusal(ErrEntrypoint, "lookup %s: %v", Entrypoint, err)
	}
	factory, ok := symbol.(func() ([]sdk.Native, error))
	if !ok || factory == nil {
		return nil, refusal(ErrEntrypoint, "%s must have type func() ([]sdk.Native, error)", Entrypoint)
	}
	natives, err := factory()
	if err != nil {
		return nil, refusal(ErrEntrypoint, "%s failed: %v", Entrypoint, err)
	}
	if len(natives) == 0 {
		return nil, refusal(ErrEntrypoint, "%s returned no implementations", Entrypoint)
	}
	if err := pluginmanifest.ValidateNatives(natives); err != nil {
		return nil, err
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, refusal(ErrClosed, "artifact has been closed")
	}
	a.loaded, a.loadedP = true, loaded
	a.natives = append([]sdk.Native(nil), natives...)
	a.mu.Unlock()
	return natives, nil
}

func buildEnvironment(values []string, targetOS, targetArch string) []string {
	// Keep only toolchain/cache/process-location inputs. In particular, discard
	// GOFLAGS, GOENV, GOPROXY and VCS settings before applying the fixed local
	// build policy below.
	allowed := map[string]bool{}
	for _, key := range []string{"PATH", "HOME", "TMPDIR", "USER", "LOGNAME", "GOROOT", "GOPATH", "GOCACHE", "GOMODCACHE", "CGO_ENABLED", "CC", "CXX", "PKG_CONFIG", "PKG_CONFIG_PATH", "PKG_CONFIG_SYSROOT_DIR"} {
		allowed[key] = true
	}
	env := make([]string, 0, len(values)+12)
	for _, value := range values {
		key, _, ok := strings.Cut(value, "=")
		if ok && allowed[key] {
			env = append(env, value)
		}
	}
	// Explicitly pin the target instead of inheriting a cross-compilation
	// request. Native plugins match the host; mode-2 builds are wasip1/wasm.
	env = append(env,
		"GOTOOLCHAIN=local",
		"GOPROXY=off",
		"GOSUMDB=off",
		"GOVCS=*:off",
		"GOWORK=off",
		"GOENV=off",
		"GOFLAGS=",
		"GOOS="+targetOS,
		"GOARCH="+targetArch,
		"CGO_ENABLED="+func() string {
			if targetOS == "wasip1" {
				return "0"
			}
			return "1"
		}(),
	)
	return env
}

func safePackage(value string) bool {
	if value == "" || strings.HasPrefix(value, "-") || strings.ContainsAny(value, "\\\x00\n\r") || filepath.IsAbs(value) {
		return false
	}
	clean := path.Clean(filepath.ToSlash(value))
	if clean == "." {
		return value == "." || value == "./"
	}
	return !strings.HasPrefix(clean, "../") && clean != ".." && !strings.Contains(clean, "...") && (value == clean || value == "./"+clean)
}

func validateModuleRoot(value string) error {
	if !filepath.IsAbs(value) {
		return refusal(ErrConfig, "module root must be an absolute local path")
	}
	resolved, err := filepath.EvalSymlinks(value)
	if err != nil {
		return refusal(ErrConfig, "module root is unavailable")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return refusal(ErrConfig, "module root is not a directory")
	}
	if _, err := os.Stat(filepath.Join(resolved, "go.mod")); err != nil {
		return refusal(ErrConfig, "module root has no go.mod")
	}
	return nil
}

func copySource(ctx context.Context, bundle *pluginmanifest.Bundle, destination string) error {
	for _, file := range bundle.Files() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if file.Path == "" || filepath.IsAbs(file.Path) || strings.Contains(filepath.ToSlash(file.Path), "../") {
			return refusal(ErrArtifact, "source path escapes disposable checkout")
		}
		source := filepath.Join(bundle.Root(), filepath.FromSlash(file.Path))
		target := filepath.Join(destination, filepath.FromSlash(file.Path))
		if !within(destination, target) {
			return refusal(ErrArtifact, "source path escapes disposable checkout")
		}
		info, err := os.Lstat(source)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return refusal(ErrArtifact, "source %q is no longer regular", file.Path)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		input, err := os.Open(source)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = input.Close()
			return err
		}
		hash := sha256.New()
		size, copyErr := io.Copy(output, io.TeeReader(input, hash))
		closeOutErr := output.Close()
		closeInErr := input.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeOutErr != nil {
			return closeOutErr
		}
		if closeInErr != nil {
			return closeInErr
		}
		if size != file.Size || hex.EncodeToString(hash.Sum(nil)) != file.SHA256 {
			return refusal(ErrArtifact, "source %q changed during staging", file.Path)
		}
		if err := os.Chmod(target, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func within(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

type goRunner func(context.Context, ...string) ([]byte, error)

func prepareModuleFile(ctx context.Context, run goRunner, sourceRoot, moduleRoot string) (string, error) {
	goMod := filepath.Join(sourceRoot, "go.mod")
	data, err := os.ReadFile(goMod)
	if err != nil {
		return "", refusal(ErrBuild, "source checkout must contain go.mod")
	}
	moduleFile := filepath.Join(sourceRoot, ".plug-pluginbuild.mod")
	for _, name := range []string{moduleFile, strings.TrimSuffix(moduleFile, ".mod") + ".sum"} {
		if _, err := os.Lstat(name); !os.IsNotExist(err) {
			return "", refusal(ErrConfig, "source contains a reserved build module file")
		}
	}
	if err := os.WriteFile(moduleFile, data, 0o600); err != nil {
		return "", err
	}
	if sum, err := os.ReadFile(filepath.Join(sourceRoot, "go.sum")); err == nil {
		if err := os.WriteFile(filepath.Join(sourceRoot, ".plug-pluginbuild.sum"), sum, 0o600); err != nil {
			return "", err
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if moduleRoot != "" {
		moduleRoot, err = filepath.EvalSymlinks(moduleRoot)
		if err != nil {
			return "", err
		}
		if _, err := run(ctx, "mod", "edit", "-modfile="+moduleFile, "-replace=github.com/buildbetter-app/plugs="+moduleRoot); err != nil {
			return "", err
		}
	}
	// Parse with the installed Go tool rather than guessing at go.mod syntax.
	// Offline cached modules are allowed; local replacements must remain in
	// the frozen source or be the caller's explicit trusted SDK replacement.
	encoded, err := run(ctx, "mod", "edit", "-modfile="+moduleFile, "-json")
	if err != nil {
		return "", err
	}
	var module struct {
		Replace []struct {
			Old struct{ Path, Version string }
			New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(encoded, &module); err != nil {
		return "", refusal(ErrConfig, "module metadata is invalid or exceeds the build output limit")
	}
	for _, replacement := range module.Replace {
		if replacement.New.Version != "" {
			continue
		}
		target := replacement.New.Path
		if !filepath.IsAbs(target) {
			target = filepath.Join(sourceRoot, target)
		}
		target, err = filepath.EvalSymlinks(target)
		if err != nil {
			return "", refusal(ErrConfig, "local module replacement is unavailable")
		}
		if within(sourceRoot, target) || (moduleRoot != "" && replacement.Old.Path == "github.com/buildbetter-app/plugs" && target == moduleRoot) {
			continue
		}
		return "", refusal(ErrConfig, "local module replacement escapes the pinned source")
	}
	return moduleFile, nil
}

func runGo(ctx context.Context, binary, directory string, args ...string) ([]byte, error) {
	return runGoTarget(ctx, binary, directory, runtime.GOOS, runtime.GOARCH, args...)
}

func runGoTarget(ctx context.Context, binary, directory, targetOS, targetArch string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = directory
	command.Env = buildEnvironment(os.Environ(), targetOS, targetArch)
	configureBuildProcess(command)
	var output buildOutput
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, refusal(ErrBuild, "go %s failed: %s", args[0], boundedOutput(output.data.Bytes()))
	}
	return output.data.Bytes(), ctx.Err()
}

// Retain only a bounded diagnostic prefix, while draining the process pipes.
type buildOutput struct{ data bytes.Buffer }

func (b *buildOutput) Write(data []byte) (int, error) {
	if remaining := maxBuildOutput - b.data.Len(); remaining > 0 {
		_, _ = b.data.Write(data[:min(len(data), remaining)])
	}
	return len(data), nil
}

func inventory(path string) ([]File, string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, "", refusal(ErrArtifact, "artifact is unavailable: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, "", refusal(ErrArtifact, "artifact is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, "", refusal(ErrArtifact, "artifact cannot be opened: %v", err)
	}
	defer file.Close()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return nil, "", refusal(ErrArtifact, "artifact cannot be read: %v", err)
	}
	after, err := file.Stat()
	if err != nil {
		return nil, "", refusal(ErrArtifact, "artifact stat failed: %v", err)
	}
	if !after.Mode().IsRegular() || after.Size() != size || after.Mode() != info.Mode() {
		return nil, "", refusal(ErrArtifact, "artifact changed during inventory")
	}
	entry := File{Path: filepath.Base(path), Size: size, SHA256: hex.EncodeToString(digest.Sum(nil)), Executable: info.Mode()&0o111 != 0}
	return []File{entry}, entry.SHA256, nil
}

func boundedOutput(data []byte) string {
	if len(data) > maxBuildOutput {
		data = data[:maxBuildOutput]
	}
	return strings.TrimSpace(string(data))
}

type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return "plugin_build_" + e.Code + ": " + e.Detail }

func refusal(code, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

var _ io.Closer = (*Artifact)(nil)
