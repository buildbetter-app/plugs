package pluginmanifest

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxManifestBytes = 1 << 20

func sourcePath(name string) bool {
	return name != "" && name != "." && name != ".." && path.Clean(name) == name &&
		!strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") &&
		!strings.Contains(name, "\\") && utf8.ValidString(name) &&
		strings.IndexFunc(name, unicode.IsControl) < 0
}

func manifestPath(root *os.Root, requested string) (string, error) {
	candidates := []string{"plug-plugin.json", "plug-plugin.yaml"}
	if requested != "" {
		if !sourcePath(requested) || strings.Contains(requested, "/") || requested == ".git" {
			return "", refuse("containment", "manifest must be a regular file at the repository root")
		}
		candidates = []string{requested}
	}
	var found []string
	for _, name := range candidates {
		info, err := root.Lstat(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", refuse("containment", "manifest %q is not a regular file", name)
		}
		found = append(found, name)
	}
	if len(found) != 1 {
		return "", refuse("manifest_invalid", "expected exactly one root manifest, found %d", len(found))
	}
	return found[0], nil
}

func inventory(ctx context.Context, root *os.Root, manifest string, objectIDLength int) ([]File, map[string]string, []byte, error) {
	files := []File{}
	objects := map[string]string{}
	var raw []byte
	err := fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if name == "." {
			return nil
		}
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !sourcePath(name) || info.Mode()&os.ModeSymlink != 0 {
			return refuse("containment", "source %q is a symlink or invalid path", name)
		}
		if name == ".git" {
			if info.IsDir() {
				return fs.SkipDir
			}
			if info.Mode().IsRegular() { // Linked checkout administration file.
				return nil
			}
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return refuse("containment", "source %q is not a regular file", name)
		}
		if name == manifest && info.Size() > maxManifestBytes {
			return refuse("manifest_invalid", "manifest exceeds 1 MiB")
		}
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		defer file.Close()
		opened, err := file.Stat()
		if err != nil {
			return err
		}
		if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
			return refuse("containment", "source %q changed during open", name)
		}
		content := sha256.New()
		var object hash.Hash = sha1.New() // Git SHA-1 object name, not an integrity digest.
		if objectIDLength == 64 {
			object = sha256.New()
		}
		fmt.Fprintf(object, "blob %d\x00", info.Size())
		writers := []io.Writer{content, object}
		var manifestBytes bytes.Buffer
		if name == manifest {
			writers = append(writers, &manifestBytes)
		}
		reader := io.Reader(file)
		if name == manifest {
			reader = io.LimitReader(file, maxManifestBytes+1)
		}
		size, err := io.Copy(io.MultiWriter(writers...), reader)
		if err != nil {
			return err
		}
		after, err := file.Stat()
		if err != nil {
			return err
		}
		if size != info.Size() || after.Size() != info.Size() || after.Mode() != info.Mode() || !after.ModTime().Equal(info.ModTime()) {
			return refuse("source_changed", "source %q changed during read", name)
		}
		files = append(files, File{Path: name, Size: size, SHA256: hex.EncodeToString(content.Sum(nil)), Executable: info.Mode()&0o111 != 0})
		objects[name] = hex.EncodeToString(object.Sum(nil))
		if name == manifest {
			raw = manifestBytes.Bytes()
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, objects, raw, nil
}
