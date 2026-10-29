// Package pluginmanifest loads a checked-out plugin source bundle without
// building or executing it. The manifest remains the public pluginapi schema;
// this package only adds source containment, inventory, digest and native
// registration binding around that schema.
package pluginmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// ManifestFile is the preferred root manifest name. The compatibility names
// are accepted only when exactly one candidate exists, so a bundle can never
// silently choose between two declarations.
const ManifestFile = "plug-plugin.json"

var manifestCandidates = []string{ManifestFile, "plug-plugin.yaml", "plug-plugin.yml", "manifest.json"}

// InventoryEntry is one regular file in a source bundle. Paths are slash
// separated and relative to the bundle root. The digest is SHA-256 of the
// file bytes in lowercase hexadecimal.
type InventoryEntry struct {
	Path   string
	Size   int64
	SHA256 string
}

// SourceBundle is a validated, digest-bound local source checkout. The root
// is retained only to support Verify; no source is fetched or executed.
type SourceBundle struct {
	Root         string
	ManifestPath string
	Manifest     pluginapi.Manifest
	Files        []InventoryEntry
	Digest       string
}

// Options controls source-bundle loading. ManifestFile overrides discovery
// and ExpectedDigest binds the checkout to a caller-supplied digest.
type Options struct {
	ManifestFile   string
	ExpectedDigest string
}

// NativeRegistration is the narrow adapter from a loaded manifest entry to
// the compiled-in constructor supplied by the composition root. Plugin may be
// an id, alias, or either of the explicit ID/Alias fields. Instance defaults
// to "default".
type NativeRegistration struct {
	Plugin   string
	ID       string
	Alias    string
	Instance string
	New      func(pluginapi.Host) (sdk.Registration, error)
}

// Registration is a concise compatibility name for NativeRegistration.
type Registration = NativeRegistration

// Load reads and validates a local source bundle. It accepts the public JSON
// manifest schema; a .yaml file is accepted when it contains that same JSON
// representation (JSON is a valid YAML 1.2 subset), without introducing a
// second manifest schema or parser.
func Load(root string) (SourceBundle, error) {
	return LoadWithOptions(root, Options{})
}

// LoadSourceBundle is an explicit spelling of Load for callers that want to
// make the source-bundle boundary visible in a composition root.
func LoadSourceBundle(root string) (SourceBundle, error) { return Load(root) }

// LoadPinned loads a bundle and requires its canonical source digest to equal
// expectedDigest. The expected value must be a lowercase SHA-256 string.
func LoadPinned(root, expectedDigest string) (SourceBundle, error) {
	return LoadWithOptions(root, Options{ExpectedDigest: expectedDigest})
}

// LoadWithOptions loads a bundle with explicit manifest and digest policy.
func LoadWithOptions(root string, options Options) (SourceBundle, error) {
	if options.ExpectedDigest != "" {
		if err := validateDigest(options.ExpectedDigest); err != nil {
			return SourceBundle{}, err
		}
	}
	absRoot, err := canonicalRoot(root)
	if err != nil {
		return SourceBundle{}, err
	}
	return loadAt(absRoot, options)
}

func canonicalRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", bundleError("bundle_root_invalid", "", "root is empty")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", bundleError("bundle_root_invalid", root, "stat root: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", bundleError("bundle_containment", root, "root may not be a symlink")
	}
	if !info.IsDir() {
		return "", bundleError("bundle_root_invalid", root, "root is not a directory")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", bundleError("bundle_root_invalid", root, "resolve root: %v", err)
	}
	return filepath.Clean(absRoot), nil
}

func loadAt(root string, options Options) (SourceBundle, error) {
	fd, err := os.OpenRoot(root)
	if err != nil {
		return SourceBundle{}, bundleError("bundle_root_invalid", root, "open root: %v", err)
	}
	defer fd.Close()

	manifestPath, err := findManifest(fd, options.ManifestFile)
	if err != nil {
		return SourceBundle{}, err
	}
	entries, err := inventory(fd)
	if err != nil {
		return SourceBundle{}, err
	}
	manifestBytes, err := fd.ReadFile(manifestPath)
	if err != nil {
		return SourceBundle{}, bundleError("bundle_manifest_invalid", manifestPath, "read manifest: %v", err)
	}
	manifest, err := pluginapi.DecodeManifest(manifestBytes)
	if err != nil {
		return SourceBundle{}, bundleError("bundle_manifest_invalid", manifestPath, "%v", err)
	}
	if err := validateRequirements(manifest); err != nil {
		return SourceBundle{}, bundleError("bundle_manifest_invalid", manifestPath, "%v", err)
	}
	digest := inventoryDigest(entries)
	if options.ExpectedDigest != "" && digest != options.ExpectedDigest {
		return SourceBundle{}, bundleError("bundle_digest_mismatch", "", "got %s, want %s", digest, options.ExpectedDigest)
	}
	return SourceBundle{Root: root, ManifestPath: manifestPath, Manifest: manifest, Files: entries, Digest: digest}, nil
}

func validateRequirements(manifest pluginapi.Manifest) error {
	for _, plugin := range manifest.Plugins {
		seenMethods := map[string]bool{}
		for _, method := range plugin.Requires.Methods {
			if seenMethods[method] {
				return fmt.Errorf("plugin %q declares required method %q more than once", plugin.Alias, method)
			}
			seenMethods[method] = true
		}
		seenEgress := map[string]bool{}
		for _, endpoint := range plugin.Requires.Egress {
			if seenEgress[endpoint] {
				return fmt.Errorf("plugin %q declares egress endpoint %q more than once", plugin.Alias, endpoint)
			}
			seenEgress[endpoint] = true
		}
	}
	return nil
}

func findManifest(root *os.Root, requested string) (string, error) {
	if requested != "" {
		if err := validRelativePath(requested); err != nil {
			return "", bundleError("bundle_manifest_invalid", requested, "%v", err)
		}
		if strings.Contains(requested, "/") {
			return "", bundleError("bundle_manifest_invalid", requested, "manifest must be at the bundle root")
		}
		info, err := root.Lstat(requested)
		if err != nil {
			return "", bundleError("bundle_manifest_missing", requested, "manifest is not present: %v", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", bundleError("bundle_containment", requested, "manifest must be a regular file")
		}
		return requested, nil
	}
	var found []string
	for _, candidate := range manifestCandidates {
		info, err := root.Lstat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", bundleError("bundle_manifest_invalid", candidate, "stat manifest: %v", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", bundleError("bundle_containment", candidate, "manifest must be a regular file")
		}
		found = append(found, candidate)
	}
	if len(found) == 0 {
		return "", bundleError("bundle_manifest_missing", "", "no root manifest found (tried %s)", strings.Join(manifestCandidates, ", "))
	}
	if len(found) != 1 {
		return "", bundleError("bundle_manifest_ambiguous", "", "multiple root manifests: %s", strings.Join(found, ", "))
	}
	return found[0], nil
}

func inventory(root *os.Root) ([]InventoryEntry, error) {
	entries := make([]InventoryEntry, 0)
	err := fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return bundleError("bundle_containment", name, "walk source: %v", walkErr)
		}
		if name == "." {
			return nil
		}
		// Repository administration metadata is not source. A worktree .git
		// file and a normal .git directory are both ignored, but a symlink is
		// refused before this exception so it cannot retarget the bundle.
		if name == ".git" {
			if entry.Type()&os.ModeSymlink != 0 {
				return bundleError("bundle_containment", name, "symlink is not allowed")
			}
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return bundleError("bundle_containment", name, "symlink is not allowed")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return bundleError("bundle_containment", name, "stat source: %v", err)
		}
		if !info.Mode().IsRegular() {
			return bundleError("bundle_containment", name, "non-regular file is not allowed")
		}
		if err := validRelativePath(name); err != nil {
			return bundleError("bundle_containment", name, "%v", err)
		}
		file, err := root.Open(name)
		if err != nil {
			return bundleError("bundle_containment", name, "open source: %v", err)
		}
		data, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil {
			return bundleError("bundle_containment", name, "read source: %v", readErr)
		}
		if closeErr != nil {
			return bundleError("bundle_containment", name, "close source: %v", closeErr)
		}
		digest := sha256.Sum256(data)
		entries = append(entries, InventoryEntry{Path: path.Clean(filepath.ToSlash(name)), Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Path == entries[i].Path {
			return nil, bundleError("bundle_containment", entries[i].Path, "duplicate inventory path")
		}
	}
	return entries, nil
}

func validRelativePath(name string) error {
	if name == "" || name == "." || filepath.IsAbs(name) || strings.ContainsRune(name, '\x00') || strings.Contains(name, "\\") {
		return fmt.Errorf("path is not a relative slash-separated name")
	}
	slashName := filepath.ToSlash(name)
	if slashName != name || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") {
		return fmt.Errorf("path escapes the bundle root")
	}
	return nil
}

func inventoryDigest(entries []InventoryEntry) string {
	hash := sha256.New()
	for _, entry := range entries {
		// The path/hash record is deliberately simple and independent of
		// filesystem metadata, directory order, and host OS separators.
		fmt.Fprintf(hash, "%s\x00%s\n", entry.Path, entry.SHA256)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validateDigest(value string) error {
	if len(value) != sha256.Size*2 {
		return bundleError("bundle_digest_invalid", "", "digest must be a 64-character SHA-256 hex string")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(value) != value {
		return bundleError("bundle_digest_invalid", "", "digest must be lowercase SHA-256 hex")
	}
	return nil
}

// Verify re-scans the source root and refuses if any declared file, manifest,
// containment property, or canonical digest has changed since Load.
func (b SourceBundle) Verify() error {
	if b.Root == "" || b.ManifestPath == "" || b.Digest == "" {
		return bundleError("bundle_digest_invalid", "", "bundle is missing its source identity")
	}
	if err := validateDigest(b.Digest); err != nil {
		return err
	}
	fresh, err := loadAt(b.Root, Options{ManifestFile: b.ManifestPath, ExpectedDigest: b.Digest})
	if err != nil {
		return err
	}
	left, err := json.Marshal(b.Manifest)
	if err != nil {
		return err
	}
	right, err := json.Marshal(fresh.Manifest)
	if err != nil {
		return err
	}
	if string(left) != string(right) {
		return bundleError("bundle_digest_mismatch", b.ManifestPath, "manifest changed while digest remained stable")
	}
	return nil
}

// CanonicalDigest returns the source digest recorded by Load.
func (b SourceBundle) CanonicalDigest() string { return b.Digest }

// NativeDescriptors binds every manifest entry to exactly one supplied native
// constructor and returns descriptors accepted directly by pluginhost.New.
func (b SourceBundle) NativeDescriptors(registrations []NativeRegistration) ([]sdk.Native, error) {
	if err := b.Verify(); err != nil {
		return nil, err
	}
	if len(registrations) != len(b.Manifest.Plugins) {
		return nil, bundleError("bundle_registration_missing", "", "got %d registrations for %d manifest plugins", len(registrations), len(b.Manifest.Plugins))
	}
	used := make([]bool, len(registrations))
	natives := make([]sdk.Native, 0, len(registrations))
	for _, manifest := range b.Manifest.Plugins {
		match := -1
		for index, registration := range registrations {
			if used[index] {
				continue
			}
			matches, err := registrationMatches(registration, manifest)
			if err != nil {
				return nil, err
			}
			if matches {
				if match >= 0 {
					return nil, bundleError("bundle_registration_ambiguous", manifest.Alias, "multiple registrations match plugin")
				}
				match = index
			}
		}
		if match < 0 {
			return nil, bundleError("bundle_registration_missing", manifest.Alias, "no native registration supplied")
		}
		registration := registrations[match]
		used[match] = true
		instance := registration.Instance
		if instance == "" {
			instance = "default"
		}
		if !pluginapi.ValidName(instance) || registration.New == nil {
			return nil, bundleError("bundle_registration_invalid", manifest.Alias, "instance and constructor are required")
		}
		copy, err := pluginapi.CloneJSON(manifest)
		if err != nil {
			return nil, err
		}
		natives = append(natives, sdk.Native{Manifest: copy, Instance: instance, New: registration.New})
	}
	for index, registration := range registrations {
		if !used[index] {
			return nil, bundleError("bundle_registration_extra", registrationKey(registration), "registration does not name a manifest plugin")
		}
	}
	if err := validateNativeDescriptors(natives); err != nil {
		return nil, err
	}
	return natives, nil
}

// Natives is a short adapter spelling used by composition roots.
func (b SourceBundle) Natives(registrations []NativeRegistration) ([]sdk.Native, error) {
	return b.NativeDescriptors(registrations)
}

// ValidateNativeDescriptors checks a composed set before handing it to the
// host. It mirrors the host's identity boundary so two independently loaded
// bundles cannot silently share an alias, id, or alias/instance slot.
func ValidateNativeDescriptors(natives []sdk.Native) error {
	return validateNativeDescriptors(natives)
}

// AdaptNative is the package-level form of SourceBundle.NativeDescriptors.
func AdaptNative(bundle SourceBundle, registrations ...NativeRegistration) ([]sdk.Native, error) {
	return bundle.NativeDescriptors(registrations)
}

// LoadNatives loads a source bundle and adapts its registrations in one step.
func LoadNatives(root string, registrations []NativeRegistration) (SourceBundle, []sdk.Native, error) {
	bundle, err := Load(root)
	if err != nil {
		return SourceBundle{}, nil, err
	}
	natives, err := bundle.NativeDescriptors(registrations)
	if err != nil {
		return SourceBundle{}, nil, err
	}
	return bundle, natives, nil
}

func registrationMatches(registration NativeRegistration, manifest pluginapi.PluginManifest) (bool, error) {
	keys := []string{}
	if registration.Plugin != "" {
		keys = append(keys, registration.Plugin)
	}
	if registration.ID != "" {
		keys = append(keys, registration.ID)
	}
	if registration.Alias != "" {
		keys = append(keys, registration.Alias)
	}
	if len(keys) == 0 {
		return false, bundleError("bundle_registration_invalid", manifest.Alias, "registration must name a plugin")
	}
	for _, key := range keys[1:] {
		if key != keys[0] {
			return false, bundleError("bundle_registration_invalid", manifest.Alias, "Plugin, ID and Alias disagree")
		}
	}
	return keys[0] == manifest.ID || keys[0] == manifest.Alias, nil
}

func registrationKey(registration NativeRegistration) string {
	if registration.Plugin != "" {
		return registration.Plugin
	}
	if registration.ID != "" {
		return registration.ID
	}
	return registration.Alias
}

func validateNativeDescriptors(natives []sdk.Native) error {
	ids, aliases, instances := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, native := range natives {
		if err := native.Manifest.Validate(); err != nil {
			return bundleError("bundle_manifest_invalid", native.Manifest.Alias, "%v", err)
		}
		if !pluginapi.ValidName(native.Instance) || native.New == nil {
			return bundleError("bundle_registration_invalid", native.Manifest.Alias, "instance and constructor are required")
		}
		if ids[native.Manifest.ID] || aliases[native.Manifest.Alias] {
			return bundleError("bundle_registration_collision", native.Manifest.Alias, "duplicate plugin id or alias")
		}
		key := native.Manifest.Alias + "\x00" + native.Instance
		if instances[key] {
			return bundleError("bundle_registration_collision", native.Manifest.Alias, "duplicate alias/instance")
		}
		ids[native.Manifest.ID], aliases[native.Manifest.Alias], instances[key] = true, true, true
	}
	return nil
}

// Error identifies a refused bundle without exposing filesystem or source
// contents in the diagnostic. Code is stable for callers that need to map a
// refusal to a plan-time result.
type Error struct {
	Code   string
	Path   string
	Detail string
}

func (e *Error) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Detail)
	}
	return fmt.Sprintf("%s (%s): %s", e.Code, e.Path, e.Detail)
}

func bundleError(code, path, format string, args ...any) error {
	return &Error{Code: code, Path: path, Detail: fmt.Sprintf(format, args...)}
}
