package physicalrestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type StoreFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type CopyReceipt struct {
	Files  []StoreFile `json:"files"`
	SHA256 string      `json:"sha256"`
}

// InventoryPreservedStore hashes regular files without opening a native engine.
// The caller must have closed/quiesced its explicitly selected preserved copy.
// Symlinks, sockets and devices refuse; no private/live path is discovered.
func InventoryPreservedStore(ctx context.Context, directory string) (receipt CopyReceipt, resultErr error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return receipt, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, root.Close())
		if resultErr != nil {
			receipt = CopyReceipt{}
		}
	}()
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("preserved store contains nonregular files")
		}
		file, err := root.Open(name)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			return errors.Join(err, file.Close())
		}
		hash := sha256.New()
		size, readErr := io.Copy(hash, file)
		err = errors.Join(readErr, file.Close())
		if err != nil {
			return err
		}
		if size != info.Size() {
			return errors.New("preserved store changed while hashing")
		}
		receipt.Files = append(receipt.Files, StoreFile{name, size, hex.EncodeToString(hash.Sum(nil))})
		return nil
	})
	if err != nil {
		return CopyReceipt{}, err
	}
	sort.Slice(receipt.Files, func(i, j int) bool { return receipt.Files[i].Path < receipt.Files[j].Path })
	data, err := json.Marshal(receipt.Files)
	if err != nil {
		return CopyReceipt{}, err
	}
	sum := sha256.Sum256(data)
	receipt.SHA256 = hex.EncodeToString(sum[:])
	return receipt, nil
}

// CopyPreservedStore creates only the explicitly absent disposable destination.
// Every byte is copied before a target engine opens; source hashes are checked
// again afterward. On copy failure only this operation's new directory is removed.
func CopyPreservedStore(ctx context.Context, source, destination string) (receipt CopyReceipt, resultErr error) {
	if source == destination || strings.HasPrefix(destination, source+string(os.PathSeparator)) || strings.HasPrefix(source, destination+string(os.PathSeparator)) {
		return receipt, errors.New("preserved source and disposable destination must not overlap")
	}
	for _, path := range []string{source, filepath.Dir(destination)} {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || resolved != path {
			return receipt, errors.New("canonical existing paths without symlinks required")
		}
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return receipt, errors.New("owner-only source/destination parent required")
		}
	}
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return receipt, errors.New("canonical disposable destination required")
	}
	before, err := InventoryPreservedStore(ctx, source)
	if err != nil {
		return receipt, err
	}
	if len(before.Files) == 0 {
		return receipt, errors.New("preserved store copy cannot be empty")
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return receipt, err
	}
	copied := false
	defer func() {
		if !copied || resultErr != nil {
			resultErr = errors.Join(resultErr, os.RemoveAll(destination))
			receipt = CopyReceipt{}
		}
	}()
	src, err := os.OpenRoot(source)
	if err != nil {
		return receipt, err
	}
	defer func() { resultErr = errors.Join(resultErr, src.Close()) }()
	dst, err := os.OpenRoot(destination)
	if err != nil {
		return receipt, err
	}
	defer func() { resultErr = errors.Join(resultErr, dst.Close()) }()
	err = fs.WalkDir(src.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if entry.IsDir() {
			return dst.Mkdir(name, 0700)
		}
		if !entry.Type().IsRegular() {
			return errors.New("preserved store contains nonregular files")
		}
		in, err := src.Open(name)
		if err != nil {
			return err
		}
		out, err := dst.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return errors.Join(err, in.Close())
		}
		_, copyErr := io.Copy(out, in)
		syncErr := out.Sync()
		return errors.Join(copyErr, syncErr, in.Close(), out.Close())
	})
	if err != nil {
		return receipt, err
	}
	after, err := InventoryPreservedStore(ctx, source)
	if err != nil {
		return receipt, err
	}
	actual, err := InventoryPreservedStore(ctx, destination)
	if err != nil {
		return receipt, err
	}
	if before.SHA256 != after.SHA256 || before.SHA256 != actual.SHA256 {
		return receipt, fmt.Errorf("preserved store/copy byte census changed")
	}
	copied = true
	return before, nil
}
