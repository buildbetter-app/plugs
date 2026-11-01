package nightlyrestore_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

const fixturePath = "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar"
const fixtureSHA = "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c"

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func publicFixture(t *testing.T) []byte {
	t.Helper()
	paths := []string{filepath.Join("..", "converterroundtrip", "testdata", "synthetic-plug-dump-1.tar")}
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		paths = append(paths, filepath.Join(root, os.Getenv("TEST_WORKSPACE"), fixturePath))
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			if digest(data) != fixtureSHA {
				t.Fatal("public exporter fixture digest changed; review the census before repinning")
			}
			return data
		}
	}
	t.Fatal("declared public exporter fixture is absent")
	return nil
}

func readArchive(t *testing.T, data []byte) *dumpformat.Archive {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return archive
}

// This detached synthetic snapshot is deliberately separate from the immutable
// exporter fixture. It uses the already admitted import profile: hash source
// IDs, explicit snapshot history, and a supported blocking edge. Source
// counters stay nonzero; the empty project exercises zero-row allocator state.
// Source versions remain in the original archive and its strict-refusal test;
// removing them here is not evidence of history replay.
func snapshotFixture(t *testing.T, original []byte) []byte {
	t.Helper()
	a := readArchive(t, original)
	m := a.Manifest()
	m.History = "unavailable"
	m.Projects[0].IDMode, m.Projects[0].HashLength = "hash", 1
	m.Projects = append(m.Projects, dumpformat.Project{Prefix: "empty", IDMode: "hash", HashLength: 1, ChildCounters: map[string]uint64{}})
	members := map[string][]byte{}
	for name := range m.Files {
		members[name], _ = a.Member(name)
	}
	for i := range m.Kinds {
		kind := &m.Kinds[i]
		var out bytes.Buffer
		count := 0
		for _, record := range a.Records(kind.Name) {
			if record.Source.Table == "issue_versions" {
				continue
			}
			if kind.Name == "edge" {
				record.Body["type"], record.Source.Fields["type"] = "blocks", "blocks"
			}
			out.Write(jsonBytes(t, record))
			out.WriteByte('\n')
			count++
		}
		members[kind.File], kind.Count = out.Bytes(), count
	}
	for i := range m.Sources {
		if m.Sources[i].Family == "issue_versions" {
			m.Sources[i].Rows = 0
		}
	}
	for name, data := range members {
		m.Files[name] = dumpformat.File{SHA256: digest(data), Size: int64(len(data))}
	}
	members["manifest.json"] = append(jsonBytes(t, m), '\n')
	names := make([]string, 0, len(members)-1)
	for name := range members {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	names = append([]string{"manifest.json"}, names...)
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, name := range names {
		data := members[name]
		if err := w.WriteHeader(&tar.Header{Name: name, Size: int64(len(data)), Mode: 0600, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	readArchive(t, out.Bytes())
	return out.Bytes()
}
