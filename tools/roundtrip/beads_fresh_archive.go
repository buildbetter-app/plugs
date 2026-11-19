package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const freshNativeInputsArchiveSHA = "d948a03670d1f0a1dcad68ba9a682dc8b7b3e51407e5756b6d0f77d347ab2745"
const freshNativeInputsProvenanceSHA = "d589277d0564f9476b752ec21a115ba489f624e537434daf505d9229e9ed5323"
const freshNativeInputsArchiveFiles = 284

// The archive is a transport for the original committed inputs. Its member
// names and bytes, including the original provenance, are unchanged. The
// existing capture-set reader and independent oracle still consume the
// authenticated directory; extraction creates only a disposable test copy.
func unpackFreshNativeInputs(data []byte, root string, maxBytes int64, wantFiles int) error {
	if int64(len(data)) > maxBytes || maxBytes < 1 || wantFiles < 1 {
		return fmt.Errorf("native input archive limit exceeded")
	}
	compressed := bytes.NewReader(data)
	gz, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}
	defer gz.Close()
	gz.Multistream(false)
	reader := tar.NewReader(gz)
	seen := make(map[string]bool)
	var total int64
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := header.Name
		if !filepath.IsLocal(name) || filepath.ToSlash(filepath.Clean(name)) != name || header.Typeflag != tar.TypeReg || header.Linkname != "" || header.Mode != 0o644 || header.Size < 0 || header.Size > maxBytes-total || seen[name] || len(seen) >= wantFiles {
			return fmt.Errorf("native input archive member refused: %q", name)
		}
		seen[name] = true
		total += header.Size
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, copyErr := io.CopyN(file, reader, header.Size)
		modeErr := file.Chmod(0o644)
		closeErr := file.Close()
		if copyErr != nil || modeErr != nil || closeErr != nil {
			return fmt.Errorf("native input archive write failed: %v %v %v", copyErr, modeErr, closeErr)
		}
	}
	// Reach the gzip checksum and reject another stream, nonzero tar trailer,
	// or an unbounded trailer. tar.Writer pads its final record with zeros.
	tail, err := io.ReadAll(io.LimitReader(gz, 10241))
	if err != nil || len(tail) > 10240 || bytes.Count(tail, []byte{0}) != len(tail) || compressed.Len() != 0 || len(seen) != wantFiles {
		return fmt.Errorf("native input archive trailer/census refused: %v", err)
	}
	return nil
}
