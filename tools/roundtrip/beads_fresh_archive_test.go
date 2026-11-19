package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

func freshInputTestArchive(t *testing.T, headers ...tar.Header) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	tw := tar.NewWriter(gz)
	for _, header := range headers {
		if err := tw.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if header.Size != 0 {
			if _, err := tw.Write(bytes.Repeat([]byte{'x'}, int(header.Size))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestFreshNativeInputArchivePreservesRegularBytes(t *testing.T) {
	root := t.TempDir()
	data := freshInputTestArchive(t, tar.Header{Name: "oracle/rows.json", Mode: 0o644, Typeflag: tar.TypeReg, Size: 3})
	if err := unpackFreshNativeInputs(data, root, 4096, 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "oracle", "rows.json")
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "xxx" {
		t.Fatalf("member bytes changed: %q %v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Fatal("member mode changed", err)
	}
}

func TestFreshNativeInputArchiveRefusesUnsafeOrIncompletePackets(t *testing.T) {
	regular := tar.Header{Name: "rows.json", Mode: 0o644, Typeflag: tar.TypeReg, Size: 3}
	for _, test := range []struct {
		name    string
		headers []tar.Header
		count   int
	}{
		{"parent-path", []tar.Header{{Name: "../rows.json", Mode: 0o644, Typeflag: tar.TypeReg}}, 1},
		{"absolute-path", []tar.Header{{Name: "/rows.json", Mode: 0o644, Typeflag: tar.TypeReg}}, 1},
		{"unclean-path", []tar.Header{{Name: "oracle/../rows.json", Mode: 0o644, Typeflag: tar.TypeReg}}, 1},
		{"symlink", []tar.Header{{Name: "rows.json", Mode: 0o644, Typeflag: tar.TypeSymlink, Linkname: "elsewhere"}}, 1},
		{"wrong-mode", []tar.Header{{Name: "rows.json", Mode: 0o600, Typeflag: tar.TypeReg}}, 1},
		{"duplicate", []tar.Header{regular, regular}, 2},
		{"missing-member", []tar.Header{regular}, 2},
		{"extra-member", []tar.Header{regular, {Name: "extra.json", Mode: 0o644, Typeflag: tar.TypeReg}}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := freshInputTestArchive(t, test.headers...)
			if err := unpackFreshNativeInputs(data, t.TempDir(), 4096, test.count); err == nil {
				t.Fatal("unbound archive admitted")
			}
		})
	}
	data := freshInputTestArchive(t, regular)
	for _, changed := range [][]byte{data[:len(data)-1], append(append([]byte(nil), data...), data...)} {
		if err := unpackFreshNativeInputs(changed, t.TempDir(), 4096, 1); err == nil {
			t.Fatal("incomplete or concatenated archive admitted")
		}
	}
	if err := unpackFreshNativeInputs(data, t.TempDir(), 2, 1); err == nil {
		t.Fatal("archive above the byte cap admitted")
	}
	expanding := freshInputTestArchive(t, tar.Header{Name: "large.json", Mode: 0o644, Typeflag: tar.TypeReg, Size: 8192})
	if err := unpackFreshNativeInputs(expanding, t.TempDir(), 4096, 1); err == nil {
		t.Fatal("expanded member above the byte cap admitted")
	}
}
