package pluginbuild

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/pluginapi"
)

const (
	artifactObjectsDir = "objects"
	artifactLockName   = "lock.json"
	artifactBytesName  = "artifact"
	maxArtifactLock    = 4 << 20
)

var errArtifactObjectMissing = errors.New("artifact object is missing")

// OpenArtifactStore opens a durable content-addressed artifact repository.
// The root and every repository-owned child are private to the current
// operator. Publication is a complete directory rename, so a restart sees
// either the previous object or the complete new object, never a lock without
// its bytes (or vice versa).
func OpenArtifactStore(root string) (*ArtifactStore, error) {
	if root == "" {
		return nil, refusal(ErrConfig, "artifact store path is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, refusal(ErrConfig, "artifact store path is invalid")
	}
	if err := ensurePrivateDirectory(absolute); err != nil {
		return nil, err
	}
	objects := filepath.Join(absolute, artifactObjectsDir)
	if err := ensurePrivateDirectory(objects); err != nil {
		return nil, err
	}
	if err := ensurePrivatePath(absolute, objects); err != nil {
		return nil, err
	}
	if err := syncDirectory(absolute); err != nil {
		return nil, refusal(ErrConfig, "artifact store directory cannot be made durable")
	}
	store := &ArtifactStore{root: absolute}
	if err := store.scanPersistentObjects(); err != nil {
		return nil, err
	}
	return store, nil
}

// Root returns the operator-selected durable repository path. In-memory
// stores return an empty string.
func (s *ArtifactStore) Root() string {
	if s == nil {
		return ""
	}
	return s.root
}

func ensurePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return refusal(ErrConfig, "artifact store directory cannot be created")
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return refusal(ErrConfig, "artifact store directory is unavailable")
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return refusal(ErrConfig, "artifact store path must be a directory")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return refusal(ErrConfig, "artifact store directory permissions cannot be restricted")
	}
	return nil
}

// ensurePrivatePath checks both the repository root and its objects directory
// after creation. A symlink here would let a digest-derived child escape the
// operator-owned repository.
func ensurePrivatePath(root, objects string) error {
	for _, path := range []string{root, objects} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			return refusal(ErrConfig, "artifact store path is not a private directory")
		}
	}
	return nil
}

func (s *ArtifactStore) checkPersistentRoot() error {
	if s == nil || s.root == "" {
		return refusal(ErrConfig, "durable artifact store is required")
	}
	return ensurePrivatePath(s.root, filepath.Join(s.root, artifactObjectsDir))
}

func (s *ArtifactStore) objectPath(digest string) (string, error) {
	if err := s.checkPersistentRoot(); err != nil {
		return "", err
	}
	if !validHexString(digest, 64) || strings.ToLower(digest) != digest {
		return "", refusal(ErrDigest, "artifact digest is not a lowercase SHA-256")
	}
	objects := filepath.Join(s.root, artifactObjectsDir)
	object := filepath.Join(objects, digest)
	if filepath.Base(object) != digest || !within(s.root, object) {
		return "", refusal(ErrDigest, "artifact digest path escapes the store")
	}
	return object, nil
}

type persistentObject struct {
	lock ArtifactLock
	path string
}

// scanPersistentObjects validates every complete object before returning a
// reopened store. Staging directories are ignored: a concurrent publisher may
// still own one, and an interrupted publisher's private staging bytes are
// never visible through a digest load. Any final object that is incomplete,
// tampered or conflicting refuses the open.
func (s *ArtifactStore) scanPersistentObjects() error {
	if err := s.checkPersistentRoot(); err != nil {
		return err
	}
	objects := filepath.Join(s.root, artifactObjectsDir)
	entries, err := os.ReadDir(objects)
	if err != nil {
		return refusal(ErrArtifact, "artifact store entries cannot be read")
	}
	for _, entry := range entries {
		name := entry.Name()
		object := filepath.Join(objects, name)
		if strings.HasPrefix(name, ".staging-") {
			info, lstatErr := os.Lstat(object)
			if lstatErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !within(s.root, object) {
				return refusal(ErrArtifact, "artifact staging entry is invalid")
			}
			continue
		}
		if !validHexString(name, 64) || strings.ToLower(name) != name {
			return refusal(ErrArtifact, "artifact store contains an invalid digest path")
		}
		if _, err := inspectPersistentObject(object, name, nil); err != nil {
			return err
		}
	}
	return nil
}

func inspectPersistentObject(object, expectedDigest string, expected *ArtifactLock) (persistentObject, error) {
	if !validHexString(expectedDigest, 64) || strings.ToLower(expectedDigest) != expectedDigest {
		return persistentObject{}, refusal(ErrDigest, "artifact digest is not a lowercase SHA-256")
	}
	info, err := os.Lstat(object)
	if os.IsNotExist(err) {
		return persistentObject{}, errArtifactObjectMissing
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return persistentObject{}, refusal(ErrArtifact, "artifact publication directory is invalid")
	}
	entries, err := os.ReadDir(object)
	if err != nil {
		return persistentObject{}, refusal(ErrArtifact, "artifact publication is unreadable")
	}
	if len(entries) != 2 {
		return persistentObject{}, refusal(ErrArtifact, "artifact publication is incomplete")
	}
	lockPath := filepath.Join(object, artifactLockName)
	bytesPath := filepath.Join(object, artifactBytesName)
	for _, entry := range entries {
		if entry.Name() != artifactLockName && entry.Name() != artifactBytesName {
			return persistentObject{}, refusal(ErrArtifact, "artifact publication contains an unexpected entry")
		}
	}
	if err := checkPrivateRegularFile(lockPath); err != nil {
		return persistentObject{}, refusal(ErrArtifact, "artifact lock entry is invalid")
	}
	if err := checkPrivateRegularFile(bytesPath); err != nil {
		return persistentObject{}, refusal(ErrArtifact, "artifact bytes entry is invalid")
	}
	lockData, err := readBounded(lockPath, maxArtifactLock)
	if err != nil {
		return persistentObject{}, refusal(ErrArtifact, "artifact lock cannot be read")
	}
	var lock ArtifactLock
	if err := json.Unmarshal(lockData, &lock); err != nil {
		return persistentObject{}, refusal(ErrArtifact, "artifact lock is truncated or invalid")
	}
	canonical, err := json.Marshal(lock)
	if err != nil || string(canonical) != string(lockData) {
		return persistentObject{}, refusal(ErrArtifact, "artifact lock is not canonical")
	}
	if err := lock.validate(); err != nil {
		return persistentObject{}, err
	}
	if lock.ArtifactDigest != expectedDigest {
		return persistentObject{}, refusal(ErrDigest, "artifact path and lock digest conflict")
	}
	if expected != nil && !sameLock(lock, *expected) {
		return persistentObject{}, refusal(ErrDigest, "artifact digest is already bound to a different lock")
	}
	if err := verifyPersistentBytes(bytesPath, lock); err != nil {
		return persistentObject{}, err
	}
	return persistentObject{lock: lock, path: bytesPath}, nil
}

func checkPrivateRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return refusal(ErrArtifact, "artifact entry is not a private regular file")
	}
	return nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("bounded artifact record exceeded limit")
	}
	info, err := file.Stat()
	if err != nil || info.Size() != int64(len(data)) {
		return nil, fmt.Errorf("artifact record changed during read")
	}
	return data, nil
}

func verifyPersistentBytes(path string, lock ArtifactLock) error {
	info, err := os.Stat(path)
	if err != nil {
		return refusal(ErrArtifact, "artifact bytes are missing")
	}
	if info.Size() != lock.Files[0].Size {
		return refusal(ErrDigest, "artifact bytes are truncated or oversized")
	}
	digest, size, err := hashFile(path)
	if err != nil {
		return refusal(ErrArtifact, "artifact bytes cannot be read")
	}
	if size != lock.Files[0].Size || digest != lock.ArtifactDigest {
		return refusal(ErrDigest, "artifact bytes do not match the lock")
	}
	return nil
}

func hashFile(path string) (string, int64, error) {
	beforePath, err := os.Lstat(path)
	if err != nil || beforePath.Mode()&os.ModeSymlink != 0 || !beforePath.Mode().IsRegular() {
		return "", 0, fmt.Errorf("artifact path is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return "", 0, err
	}
	if !os.SameFile(beforePath, before) {
		return "", 0, fmt.Errorf("artifact path changed before read")
	}
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return "", 0, err
	}
	after, err := file.Stat()
	if err != nil || before.Size() != size || after.Size() != size || before.Mode() != after.Mode() {
		return "", 0, fmt.Errorf("artifact bytes changed during read")
	}
	return hex.EncodeToString(digest.Sum(nil)), size, nil
}

func (s *ArtifactStore) putFile(ctx context.Context, lock ArtifactLock, source string) (ArtifactLock, error) {
	object, err := s.objectPath(lock.ArtifactDigest)
	if err != nil {
		return ArtifactLock{}, err
	}
	if existing, inspectErr := inspectPersistentObject(object, lock.ArtifactDigest, &lock); inspectErr == nil {
		return cloneArtifactLock(existing.lock)
	} else if !errors.Is(inspectErr, errArtifactObjectMissing) {
		return ArtifactLock{}, inspectErr
	}
	if err := ctx.Err(); err != nil {
		return ArtifactLock{}, err
	}
	objects := filepath.Dir(object)
	stage, err := os.MkdirTemp(objects, ".staging-")
	if err != nil {
		return ArtifactLock{}, refusal(ErrArtifact, "artifact staging directory cannot be created")
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return ArtifactLock{}, refusal(ErrArtifact, "artifact staging permissions cannot be restricted")
	}
	bytesPath := filepath.Join(stage, artifactBytesName)
	if err := copySourceArtifact(ctx, source, bytesPath, lock); err != nil {
		return ArtifactLock{}, err
	}
	lockPath := filepath.Join(stage, artifactLockName)
	if err := writeLock(lockPath, lock); err != nil {
		return ArtifactLock{}, err
	}
	if err := syncDirectory(stage); err != nil {
		return ArtifactLock{}, refusal(ErrArtifact, "artifact publication cannot be made durable")
	}
	if err := os.Rename(stage, object); err != nil {
		// Another process may have won the idempotent race. Inspect its
		// complete object rather than replacing it or accepting a partial one.
		if existing, inspectErr := inspectPersistentObject(object, lock.ArtifactDigest, &lock); inspectErr == nil {
			return cloneArtifactLock(existing.lock)
		} else if !errors.Is(inspectErr, errArtifactObjectMissing) {
			return ArtifactLock{}, inspectErr
		}
		return ArtifactLock{}, refusal(ErrArtifact, "artifact publication failed")
	}
	if err := syncDirectory(objects); err != nil {
		return ArtifactLock{}, refusal(ErrArtifact, "artifact publication directory cannot be made durable")
	}
	existing, err := inspectPersistentObject(object, lock.ArtifactDigest, &lock)
	if err != nil {
		return ArtifactLock{}, err
	}
	return cloneArtifactLock(existing.lock)
}

func copySourceArtifact(ctx context.Context, source, destination string, lock ArtifactLock) error {
	info, err := os.Lstat(source)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return refusal(ErrArtifact, "artifact output is unavailable")
	}
	if info.Size() != lock.Files[0].Size {
		return refusal(ErrDigest, "artifact changed while storing")
	}
	in, err := os.Open(source)
	if err != nil {
		return refusal(ErrArtifact, "artifact output cannot be opened")
	}
	defer in.Close()
	opened, err := in.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return refusal(ErrArtifact, "artifact output changed while storing")
	}
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return refusal(ErrArtifact, "artifact staging file cannot be created")
	}
	defer out.Close()
	digest := sha256.New()
	reader := &contextReader{ctx: ctx, reader: io.TeeReader(in, digest)}
	size, copyErr := io.Copy(out, reader)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil || closeErr != nil {
		return refusal(ErrArtifact, "artifact staging file cannot be made durable")
	}
	after, err := os.Lstat(source)
	if err != nil || after.Mode() != info.Mode() || after.Size() != size {
		return refusal(ErrDigest, "artifact changed while storing")
	}
	if size != lock.Files[0].Size || hex.EncodeToString(digest.Sum(nil)) != lock.ArtifactDigest {
		return refusal(ErrDigest, "artifact bytes do not match the lock")
	}
	return nil
}

func writeLock(path string, lock ArtifactLock) error {
	data, err := json.Marshal(lock)
	if err != nil {
		return refusal(ErrArtifact, "artifact lock cannot be encoded")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return refusal(ErrArtifact, "artifact lock cannot be created")
	}
	written, writeErr := file.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return refusal(ErrArtifact, "artifact lock cannot be made durable")
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (s *ArtifactStore) restoreFile(ctx context.Context, lock ArtifactLock) (*Artifact, error) {
	object, err := s.objectPath(lock.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	stored, err := inspectPersistentObject(object, lock.ArtifactDigest, &lock)
	if err != nil {
		if errors.Is(err, errArtifactObjectMissing) {
			return nil, refusal(ErrDigest, "artifact is not present")
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "plug-restore-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	name := lock.Files[0].Path
	destination := filepath.Join(directory, name)
	mode := os.FileMode(0o600)
	if lock.Files[0].Executable {
		mode = 0o700
	}
	if err := copyPersistentArtifact(ctx, stored.path, destination, mode, lock); err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	if _, err := inspectPersistentObject(object, lock.ArtifactDigest, &lock); err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	files, digest, err := inventory(destination)
	if err != nil || digest != lock.ArtifactDigest || len(files) != 1 || files[0] != lock.Files[0] {
		_ = os.RemoveAll(directory)
		if err != nil {
			return nil, err
		}
		return nil, refusal(ErrDigest, "restored artifact inventory differs from the lock")
	}
	if lock.Runtime == pluginapi.RuntimeWASI {
		data, readErr := os.ReadFile(destination)
		if readErr != nil {
			_ = os.RemoveAll(directory)
			return nil, refusal(ErrArtifact, "restored WASM artifact cannot be read")
		}
		if err := pluginhost.ValidateWASMArtifact(data); err != nil {
			_ = os.RemoveAll(directory)
			return nil, err
		}
	}
	manifest, err := pluginapi.CloneJSON(lock.Manifest)
	if err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	return &Artifact{manifest: manifest, outputDir: directory, path: destination, commit: lock.Commit, sourceDigest: lock.SourceDigest, manifestDigest: lock.ManifestDigest, digest: lock.ArtifactDigest, runtime: lock.Runtime, files: append([]File(nil), files...)}, nil
}

func copyPersistentArtifact(ctx context.Context, source, destination string, mode os.FileMode, lock ArtifactLock) error {
	beforePath, err := os.Lstat(source)
	if err != nil || beforePath.Mode()&os.ModeSymlink != 0 || !beforePath.Mode().IsRegular() {
		return refusal(ErrArtifact, "artifact bytes are not regular")
	}
	in, err := os.Open(source)
	if err != nil {
		return refusal(ErrArtifact, "artifact bytes disappeared during restore")
	}
	defer in.Close()
	opened, err := in.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(beforePath, opened) {
		return refusal(ErrArtifact, "artifact bytes are not regular")
	}
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return refusal(ErrArtifact, "restored artifact path cannot be created")
	}
	defer out.Close()
	digest := sha256.New()
	reader := &contextReader{ctx: ctx, reader: io.TeeReader(in, digest)}
	size, copyErr := io.Copy(out, reader)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil || closeErr != nil {
		return refusal(ErrArtifact, "restored artifact cannot be made durable")
	}
	if size != lock.Files[0].Size || hex.EncodeToString(digest.Sum(nil)) != lock.ArtifactDigest {
		return refusal(ErrDigest, "restored artifact bytes do not match the lock")
	}
	return nil
}
