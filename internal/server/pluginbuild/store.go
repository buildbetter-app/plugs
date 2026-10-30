package pluginbuild

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// ArtifactLock is the immutable admission record persisted beside an artifact.
// The manifest is included so a restore can admit a digest before its source
// checkout is available; source identity remains recorded for the async check.
type ArtifactLock struct {
	Version        int                `json:"version"`
	Commit         string             `json:"commit"`
	SourceDigest   string             `json:"source_digest"`
	ManifestDigest string             `json:"manifest_digest"`
	ArtifactDigest string             `json:"artifact_digest"`
	Runtime        string             `json:"runtime"`
	Files          []File             `json:"files"`
	Manifest       pluginapi.Manifest `json:"manifest"`
}

// Lock returns a detached digest lock after rechecking both source and output.
func (a *Artifact) Lock(ctx context.Context) (ArtifactLock, error) {
	if err := a.Verify(ctx); err != nil {
		return ArtifactLock{}, err
	}
	manifest, err := a.Manifest()
	if err != nil {
		return ArtifactLock{}, err
	}
	lock := ArtifactLock{
		Version: 1, Commit: a.commit, SourceDigest: a.sourceDigest,
		ManifestDigest: a.manifestDigest, ArtifactDigest: a.digest,
		Runtime: a.runtime, Files: a.Files(), Manifest: manifest,
	}
	if err := lock.validate(); err != nil {
		return ArtifactLock{}, err
	}
	return lock, nil
}

func (l ArtifactLock) validate() error {
	if l.Version != 1 || (!validHexString(l.Commit, 40) && !validHexString(l.Commit, 64)) ||
		!validHexString(l.SourceDigest, 64) || !validHexString(l.ManifestDigest, 64) ||
		!validHexString(l.ArtifactDigest, 64) {
		return refusal(ErrArtifact, "artifact lock has invalid digest fields")
	}
	if l.Runtime != pluginapi.RuntimeNative && l.Runtime != pluginapi.RuntimeWASI {
		return refusal(ErrConfig, "artifact lock has unsupported runtime %q", l.Runtime)
	}
	if strings.ToLower(l.ArtifactDigest) != l.ArtifactDigest ||
		strings.ToLower(l.SourceDigest) != l.SourceDigest ||
		strings.ToLower(l.ManifestDigest) != l.ManifestDigest ||
		strings.ToLower(l.Commit) != l.Commit {
		return refusal(ErrArtifact, "artifact lock digests must be lowercase")
	}
	if err := l.Manifest.Validate(); err != nil {
		return refusal(ErrArtifact, "artifact lock manifest is invalid: %v", err)
	}
	for _, declared := range l.Manifest.Plugins {
		if declared.Runtime != l.Runtime {
			return refusal(ErrArtifact, "artifact lock runtime does not match plugin %q", declared.Alias)
		}
	}
	canonical, err := json.Marshal(l.Manifest)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	if hex.EncodeToString(digest[:]) != l.ManifestDigest {
		return refusal(ErrDigest, "artifact lock manifest digest does not match manifest")
	}
	expectedPath := "plugin.so"
	if l.Runtime == pluginapi.RuntimeWASI {
		expectedPath = "plugin.wasm"
	}
	if len(l.Files) != 1 || l.Files[0].Path != expectedPath ||
		path.Clean(l.Files[0].Path) != l.Files[0].Path || strings.ContainsAny(l.Files[0].Path, "/\\\x00") ||
		!validHexString(l.Files[0].SHA256, 64) || l.Files[0].SHA256 != l.ArtifactDigest || l.Files[0].Size < 1 {
		return refusal(ErrArtifact, "artifact lock has an incomplete output inventory")
	}
	return nil
}

func cloneArtifactLock(lock ArtifactLock) (ArtifactLock, error) {
	return pluginapi.CloneJSON(lock)
}

func validHexString(value string, lengths ...int) bool {
	validLength := false
	for _, length := range lengths {
		if len(value) == length {
			validLength = true
			break
		}
	}
	if !validLength {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

type storedArtifact struct {
	lock ArtifactLock
	data []byte
}

// ArtifactStore is an immutable content-addressed artifact repository. The
// zero-argument constructor retains the process-local store used by existing
// callers and host tests; OpenArtifactStore provides the durable file-backed
// implementation without changing the lock/restore contract.
type ArtifactStore struct {
	mu        sync.RWMutex
	artifacts map[string]storedArtifact
	root      string
}

func NewArtifactStore() *ArtifactStore {
	return &ArtifactStore{artifacts: map[string]storedArtifact{}}
}

// Put verifies an artifact and stores an independent copy indexed by its
// output digest. Existing bytes are immutable and may be put repeatedly only
// when the complete lock is identical.
func (s *ArtifactStore) Put(ctx context.Context, artifact *Artifact) (ArtifactLock, error) {
	if s == nil {
		return ArtifactLock{}, refusal(ErrConfig, "artifact store is required")
	}
	lock, err := artifact.Lock(ctx)
	if err != nil {
		return ArtifactLock{}, err
	}
	if s.root != "" {
		return s.putFile(ctx, lock, artifact.path)
	}
	data, err := os.ReadFile(artifact.path)
	if err != nil {
		return ArtifactLock{}, refusal(ErrArtifact, "artifact cannot be stored: %v", err)
	}
	if digestBytes(data) != lock.ArtifactDigest {
		return ArtifactLock{}, refusal(ErrDigest, "artifact changed while storing")
	}
	copyData := append([]byte(nil), data...)
	storedLock, err := cloneArtifactLock(lock)
	if err != nil {
		return ArtifactLock{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.artifacts[lock.ArtifactDigest]; ok {
		if !sameLock(existing.lock, lock) || string(existing.data) != string(copyData) {
			return ArtifactLock{}, refusal(ErrDigest, "artifact digest is already bound to different lock data")
		}
		return lock, nil
	}
	s.artifacts[lock.ArtifactDigest] = storedArtifact{lock: storedLock, data: copyData}
	return lock, nil
}

// Restore loads the already-built bytes by lock digest. It does not invoke Go
// or rebuild source; callers receive a usable artifact before any optional
// asynchronous reproducibility check starts.
func (s *ArtifactStore) Restore(ctx context.Context, lock ArtifactLock) (*Artifact, error) {
	if s == nil {
		return nil, refusal(ErrConfig, "artifact store is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	canonicalLock, err := cloneArtifactLock(lock)
	if err != nil {
		return nil, err
	}
	lock = canonicalLock
	if err := lock.validate(); err != nil {
		return nil, err
	}
	if s.root != "" {
		return s.restoreFile(ctx, lock)
	}
	s.mu.RLock()
	stored, ok := s.artifacts[lock.ArtifactDigest]
	s.mu.RUnlock()
	if !ok || !sameLock(stored.lock, lock) {
		return nil, refusal(ErrDigest, "artifact digest is not present with the requested lock")
	}
	if digestBytes(stored.data) != lock.ArtifactDigest {
		return nil, refusal(ErrDigest, "stored artifact bytes do not match the lock")
	}
	dir, err := os.MkdirTemp("", "plug-restore-")
	if err != nil {
		return nil, err
	}
	name := "plugin.so"
	if lock.Runtime == pluginapi.RuntimeWASI {
		name = "plugin.wasm"
	}
	path := filepath.Join(dir, name)
	mode := os.FileMode(0o644)
	if lock.Files[0].Executable {
		mode = 0o755
	}
	if err := os.WriteFile(path, stored.data, mode); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	files, digest, err := inventory(path)
	if err != nil || digest != lock.ArtifactDigest || len(files) != len(lock.Files) || files[0] != lock.Files[0] {
		_ = os.RemoveAll(dir)
		if err != nil {
			return nil, err
		}
		return nil, refusal(ErrDigest, "restored artifact inventory differs from the lock")
	}
	if lock.Runtime == pluginapi.RuntimeWASI {
		if err := pluginhost.ValidateWASMArtifact(stored.data); err != nil {
			_ = os.RemoveAll(dir)
			return nil, err
		}
	}
	manifest, err := pluginapi.CloneJSON(lock.Manifest)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return &Artifact{manifest: manifest, outputDir: dir, path: path, commit: lock.Commit, sourceDigest: lock.SourceDigest, manifestDigest: lock.ManifestDigest, digest: lock.ArtifactDigest, runtime: lock.Runtime, files: append([]File(nil), files...)}, nil
}

// RebuildCheck reports the result of an asynchronous reproducibility check.
// Match is false for either a build error or any lock field mismatch.
type RebuildCheck struct {
	Expected ArtifactLock
	Actual   ArtifactLock
	Match    bool
	Err      error
}

// RestoreAndCheck returns the digest-loaded artifact immediately and runs the
// pinned rebuild only as an asynchronous comparison. A mismatch never replaces
// the restored bytes; it is observable through the buffered result channel.
func (s *ArtifactStore) RestoreAndCheck(ctx context.Context, lock ArtifactLock, rebuild func(context.Context) (*Artifact, error)) (*Artifact, <-chan RebuildCheck, error) {
	if rebuild == nil {
		return nil, nil, refusal(ErrConfig, "rebuild check is required")
	}
	canonicalLock, err := cloneArtifactLock(lock)
	if err != nil {
		return nil, nil, err
	}
	lock = canonicalLock
	artifact, err := s.Restore(ctx, lock)
	if err != nil {
		return nil, nil, err
	}
	result := make(chan RebuildCheck, 1)
	go func() {
		defer close(result)
		rebuilt, err := rebuild(ctx)
		if err != nil {
			result <- RebuildCheck{Expected: lock, Err: err}
			return
		}
		if rebuilt == nil {
			result <- RebuildCheck{Expected: lock, Err: fmt.Errorf("rebuild returned no artifact")}
			return
		}
		defer rebuilt.Close()
		actual, err := rebuilt.Lock(ctx)
		if err != nil {
			result <- RebuildCheck{Expected: lock, Err: err}
			return
		}
		result <- RebuildCheck{Expected: lock, Actual: actual, Match: sameLock(lock, actual)}
	}()
	return artifact, result, nil
}

func sameLock(a, b ArtifactLock) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateRestoredNatives(manifest pluginapi.Manifest, natives []sdk.Native) error {
	if err := manifest.Validate(); err != nil {
		return refusal(ErrArtifact, "restored manifest is invalid: %v", err)
	}
	if len(manifest.Plugins) != len(natives) {
		return refusal(ErrEntrypoint, "restored native count differs from the lock")
	}
	remaining := map[string]pluginapi.PluginManifest{}
	for _, declared := range manifest.Plugins {
		remaining[declared.ID] = declared
	}
	for _, native := range natives {
		var matched bool
		for _, declared := range manifest.Plugins {
			left, _ := json.Marshal(declared)
			right, _ := json.Marshal(native.Manifest)
			if string(left) == string(right) && !matched {
				matched = true
				delete(remaining, declared.ID)
			}
		}
		if !matched {
			return refusal(ErrEntrypoint, "restored native manifest does not match the lock")
		}
	}
	if len(remaining) != 0 {
		return refusal(ErrEntrypoint, "restored native set is incomplete")
	}
	if err := pluginmanifest.ValidateNatives(natives); err != nil {
		return err
	}
	return nil
}
