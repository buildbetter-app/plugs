// Package pluginmanifest validates pinned local source checkouts and adapts
// their public pluginapi manifests to trusted compiled-in native descriptors.
// It never fetches source, builds code, installs plugins, or applies grants.
package pluginmanifest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbetter-app/plugs/pluginapi"
)

// Checkout identifies an already checked-out repository. Commit must be a full
// lowercase Git object id, never a tag or branch. ExpectedDigest optionally
// binds a previously inspected source inventory. ManifestFile defaults to the
// sole root plug-plugin.json or plug-plugin.yaml (JSON representation only).
type Checkout struct {
	Root, Commit, ManifestFile, ExpectedDigest string
}

// File describes one source file, including its Git executable bit. Git
// administration metadata is the sole inventory exclusion.
type File struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Executable bool   `json:"executable"`
}

// Bundle holds an immutable snapshot. Accessors return detached public values.
// A builder must Verify again before using Root; this loader is not a build
// sandbox or an immutable filesystem snapshot.
type Bundle struct {
	source                 Checkout
	manifest               pluginapi.Manifest
	files                  []File
	digest, manifestDigest string
}

func (b *Bundle) Root() string           { return b.source.Root }
func (b *Bundle) Commit() string         { return b.source.Commit }
func (b *Bundle) ManifestPath() string   { return b.source.ManifestFile }
func (b *Bundle) Digest() string         { return b.digest }
func (b *Bundle) ManifestDigest() string { return b.manifestDigest }
func (b *Bundle) Files() []File          { return append([]File(nil), b.files...) }
func (b *Bundle) Manifest() (pluginapi.Manifest, error) {
	return pluginapi.CloneJSON(b.manifest)
}

// Load checks HEAD, every committed blob/mode, and the entire source inventory.
// Untracked/ignored source, missing files, symlinks, submodules, special files,
// unknown manifest fields, unsupported runtimes, and noncanonical pins refuse.
// Git runs local read-only builtin commands with ambient Git settings removed;
// no checkout hook, filter, source program, remote, or shell is invoked.
func Load(ctx context.Context, source Checkout) (*Bundle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validHex(source.Commit, 40) && !validHex(source.Commit, 64) {
		return nil, refuse("pin_invalid", "commit must be a full lowercase Git object id")
	}
	if source.ExpectedDigest != "" && !validHex(source.ExpectedDigest, 64) {
		return nil, refuse("pin_invalid", "source digest must be lowercase SHA-256")
	}
	if source.Root == "" {
		return nil, refuse("containment", "source root is required")
	}
	abs, err := filepath.Abs(source.Root)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	if resolved != abs {
		return nil, refuse("containment", "source root contains a symlink")
	}
	source.Root = abs
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err := checkHead(ctx, source); err != nil {
		return nil, err
	}
	source.ManifestFile, err = manifestPath(root, source.ManifestFile)
	if err != nil {
		return nil, err
	}
	files, objects, rawManifest, err := inventory(ctx, root, source.ManifestFile, len(source.Commit))
	if err != nil {
		return nil, err
	}
	if err := checkTree(ctx, source, files, objects); err != nil {
		return nil, err
	}
	if err := uniqueJSONKeys(rawManifest); err != nil {
		return nil, refuse("manifest_invalid", "%v", err)
	}
	manifest, err := pluginapi.DecodeManifest(rawManifest)
	if err != nil {
		return nil, refuse("manifest_invalid", "expected public JSON manifest: %v", err)
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(struct {
		Format string `json:"format"`
		Files  []File `json:"files"`
	}{"plug-source/1", files})
	if err != nil {
		return nil, err
	}
	digest := sha256Hex(encoded)
	if source.ExpectedDigest != "" && source.ExpectedDigest != digest {
		return nil, refuse("digest_mismatch", "source inventory differs from the pinned digest")
	}
	if err := checkHead(ctx, source); err != nil {
		return nil, err
	}
	return &Bundle{source: source, manifest: manifest, files: files, digest: digest, manifestDigest: sha256Hex(canonical)}, nil
}

// Verify refuses any drift from the loaded commit, inventory, manifest or
// containment rules. It does not alter the checkout or refresh a moved pin.
func (b *Bundle) Verify(ctx context.Context) error {
	if b == nil {
		return refuse("pin_invalid", "bundle is required")
	}
	source := b.source
	source.ExpectedDigest = b.digest
	_, err := Load(ctx, source)
	return err
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Error is a local plan-time refusal, not a public RPC error code.
type Error struct{ Code, Detail string }

func (e *Error) Error() string { return "plugin_bundle_" + e.Code + ": " + e.Detail }
func refuse(code, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}
