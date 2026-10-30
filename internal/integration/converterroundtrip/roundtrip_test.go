package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const fixtureDigest = "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c"

func fixtureBytes(t *testing.T) []byte {
	t.Helper()
	// Only the Bazel runfile (or this package's checked-in data) is used.
	path := filepath.Join("testdata", "synthetic-plug-dump-1.tar")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if digestBytes(data) != fixtureDigest {
		t.Fatal("actual exporter fixture digest changed")
	}
	return data
}

func readFixture(t *testing.T) *dumpformat.Archive {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(fixtureBytes(t)))
	if err != nil {
		t.Fatal(err)
	}
	return archive
}
func digestBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func TestActualExporterFixtureRetainsEveryFieldAndDisclosure(t *testing.T) {
	a := readFixture(t)
	m := a.Manifest()
	if m.Format != dumpformat.Format || m.Scope != dumpformat.Scope || m.History != "available-unverified" || m.MainVersion != 72 || m.IgnoredVersion != 28 {
		t.Fatalf("disclosures=%+v", m)
	}
	want := map[string]int{"bead": 3, "edge": 1, "comment": 1, "memory": 1, "config": 2, "beads-archive": 105}
	for _, kind := range m.Kinds {
		rows := a.Records(kind.Name)
		if len(rows) != want[kind.Name] || kind.Count != want[kind.Name] {
			t.Fatalf("census %s=%d", kind.Name, len(rows))
		}
		data, ok := a.Member(kind.File)
		if !ok {
			t.Fatal("lost member")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		for _, got := range rows {
			var expected dumpformat.Record
			if err := decoder.Decode(&expected); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("%s lost source fields", got.ID)
			}
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			t.Fatal("lost trailing record")
		}
	}
	categories := []string{"unstarted", "started", "completed"}
	for i, r := range a.Records("bead") {
		if r.Body["status_category"] != categories[i] {
			t.Fatal("source status mapping changed")
		}
		if _, ok := r.Body["lease"]; ok {
			t.Fatal("fabricated lease")
		}
	}
	edge := a.Records("edge")[0]
	if edge.Body["from"] != "demo-7.4" || edge.Body["to"] != "demo-7" || edge.Body["type"] != "parent-child" || edge.Body["thread_id"] != "thread-fixture" {
		t.Fatal("relation mapping changed")
	}
	comment := a.Records("comment")[0]
	if comment.Body["author"] != "fixture-commenter" || comment.Body["created_at"] != "2026-01-02T00:00:00Z" || comment.Body["text"] != "Full comment text" {
		t.Fatal("source comment changed")
	}
	if m.Projects[0].LastID != 8 || m.Projects[0].ChildCounters["demo-7"] != 4 {
		t.Fatal("allocator lost")
	}
	if len(m.Sources) != 61 || m.Verification["a3"] != "not-run" || m.Verification["authz.verify"] != "not-run" || m.Verification["plugin_lock"] != "not-produced" {
		t.Fatal("source census or limits lost")
	}
	config, ok := a.Member("config/sources.json")
	if !ok || len(config) == 0 {
		t.Fatal("lost external configuration")
	}
	// No source revision is ever promoted into a Plug revision or changeset.
	if len(a.Records("beads-archive")) != 105 {
		t.Fatal("lost unverified archives")
	}
	t.Log("producer 10953a8: 3 beads, 1 edge, 1 comment, 1 memory, 2 config, 105 source archives; all raw fields equal; restore unqualified")
}

func TestArchiveAccessorsAreDetached(t *testing.T) {
	a := readFixture(t)
	original := a.Manifest()
	m := a.Manifest()
	m.Projects[0].ChildCounters["demo-7"] = 999
	m.Files["anything"] = dumpformat.File{}
	m.Verification["a3"] = "changed"
	m.Kinds[0].Name = "changed"
	m.Sources[0].Action = "changed"
	m.Warnings[0] = "changed"
	if !reflect.DeepEqual(original, a.Manifest()) {
		t.Fatal("manifest mutation escaped")
	}
	rows := a.Records("bead")
	rows[0].Body["metadata"].(map[string]any)["custom"] = "changed"
	rows[0].Body["labels"].([]any)[0] = "changed"
	rows[0].Source.Fields["title"] = "changed"
	if a.Records("bead")[0].Body["metadata"].(map[string]any)["custom"] != "kept" || a.Records("bead")[0].Source.Fields["title"] == "changed" {
		t.Fatal("record mutation escaped")
	}
	data, _ := a.Member("manifest.json")
	data[0] = 'x'
	again, _ := a.Member("manifest.json")
	if again[0] != '{' {
		t.Fatal("member mutation escaped")
	}
}

func patchMemberSize(data []byte, member string, size int64) []byte {
	mutated := append([]byte(nil), data...)
	for offset := 0; offset+512 <= len(mutated); offset += 512 {
		header := mutated[offset : offset+512]
		name := strings.TrimRight(string(header[:100]), "\x00")
		if name != member {
			continue
		}
		for i := 124; i < 136; i++ {
			header[i] = 0
		}
		copy(header[124:136], fmt.Sprintf("%011o\x00", size))
		for i := 148; i < 156; i++ {
			header[i] = ' '
		}
		var sum int
		for _, value := range header {
			sum += int(value)
		}
		copy(header[148:156], fmt.Sprintf("%06o\x00 ", sum))
		break
	}
	return mutated
}

func repackTar(t *testing.T, data []byte, mutate func(string, []byte, *tar.Header) (string, []byte)) []byte {
	t.Helper()
	input := bytes.NewReader(data)
	reader := tar.NewReader(input)
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		copyHeader := *header
		originalSize := copyHeader.Size
		name, body := mutate(header.Name, body, &copyHeader)
		copyHeader.Name = name
		if copyHeader.Typeflag == tar.TypeReg && copyHeader.Size == originalSize {
			copyHeader.Size = int64(len(body))
		} else {
			if copyHeader.Typeflag != tar.TypeReg {
				copyHeader.Size = 0
			}
		}
		if err := writer.WriteHeader(&copyHeader); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(body); err != nil && copyHeader.Typeflag == tar.TypeReg && copyHeader.Size == int64(len(body)) {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func openTarget(t *testing.T, kind string) (driver.Store, driver.ActiveLease, func()) {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	if kind == "memory" {
		store = memory.New()
	} else {
		path := filepath.Join(t.TempDir(), "restore.db")
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
	}
	lease, err := store.Lease(ctx, "converter-"+kind, driver.LeaseTTL)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return store, lease, func() {
		_ = lease.Release(context.Background())
		_ = store.Close()
	}
}

// rewriteData recomputes only the ordinary wire checksums so malformed JSON
// fixtures reach semantic validation instead of stopping at transport integrity.
func rewriteData(t *testing.T, change func(map[string][]byte)) []byte {
	t.Helper()
	members := map[string][]byte{}
	reader := tar.NewReader(bytes.NewReader(fixtureBytes(t)))
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		members[h.Name] = data
	}
	change(members)
	var manifest map[string]any
	decode(t, members["manifest.json"], &manifest)
	files := manifest["files"].(map[string]any)
	for path, data := range members {
		if path == "manifest.json" {
			continue
		}
		if _, ok := files[path]; ok {
			files[path] = map[string]any{"sha256": digestBytes(data), "size": len(data)}
		}
	}
	members["manifest.json"] = encode(t, manifest)
	var out bytes.Buffer
	writer := tar.NewWriter(&out)
	names := []string{}
	for name := range members {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	names = append([]string{"manifest.json"}, names...)
	for _, name := range names {
		data := members[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func decode(t *testing.T, data []byte, target any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		t.Fatal(err)
	}
}
func encode(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}
func changeManifest(t *testing.T, members map[string][]byte, f func(map[string]any)) {
	t.Helper()
	var m map[string]any
	decode(t, members["manifest.json"], &m)
	f(m)
	members["manifest.json"] = encode(t, m)
}
func changeRecord(t *testing.T, members map[string][]byte, kind string, f func(map[string]any)) {
	t.Helper()
	path := "records/" + kind + ".ndjson"
	lines := bytes.Split(bytes.TrimSuffix(members[path], []byte{'\n'}), []byte{'\n'})
	var row map[string]any
	decode(t, lines[0], &row)
	f(row)
	lines[0] = bytes.TrimSuffix(encode(t, row), []byte{'\n'})
	members[path] = append(bytes.Join(lines, []byte{'\n'}), '\n')
}
func source(r map[string]any) map[string]any {
	return r["source"].(map[string]any)["fields"].(map[string]any)
}
func body(r map[string]any) map[string]any { return r["body"].(map[string]any) }

func TestStrictArchiveSemanticRefusals(t *testing.T) {
	tests := []struct {
		name   string
		change func(map[string][]byte)
	}{
		{"unknown scope", func(m map[string][]byte) { changeManifest(t, m, func(x map[string]any) { x["scope"] = "full" }) }},
		{"unknown schema", func(m map[string][]byte) { changeManifest(t, m, func(x map[string]any) { x["schema"] = 2 }) }},
		{"unknown migration cursor", func(m map[string][]byte) { changeManifest(t, m, func(x map[string]any) { x["ignored_version"] = 29 }) }},
		{"missing required scalar", func(m map[string][]byte) { changeManifest(t, m, func(x map[string]any) { delete(x, "main_version") }) }},
		{"null typed scalar", func(m map[string][]byte) { changeManifest(t, m, func(x map[string]any) { x["schema"] = nil }) }},
		{"case alias", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["Schema"] = x["schema"]; delete(x, "schema") })
		}},
		{"unknown field", func(m map[string][]byte) { changeManifest(t, m, func(x map[string]any) { x["extra"] = true }) }},
		{"false verification", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["verification"].(map[string]any)["a3"] = "passed" })
		}},
		{"unknown registry", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["kind_registry"].([]any)[0].(map[string]any)["name"] = "unknown" })
		}},
		{"registry version", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["kind_registry"].([]any)[0].(map[string]any)["version"] = 2 })
		}},
		{"record count", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["kind_registry"].([]any)[0].(map[string]any)["count"] = 9 })
		}},
		{"unlisted record member", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { delete(x["files"].(map[string]any), "records/comment.ndjson") })
		}},
		{"unknown source column", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) { source(r)["extra"] = nil })
		}},
		{"missing source column", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) { delete(source(r), "close_reason") })
		}},
		{"body source mismatch", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) { body(r)["title"] = "changed" })
		}},
		{"unknown source status", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) { source(r)["status"] = "unknown" })
		}},
		{"fabricated live lease", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) { body(r)["lease"] = map[string]any{"holder": "source"} })
		}},
		{"source identity mismatch", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) { source(r)["id"] = "wrong" })
		}},
		{"duplicate record", func(m map[string][]byte) {
			lines := bytes.Split(m["records/bead.ndjson"], []byte{'\n'})
			lines[1] = lines[0]
			m["records/bead.ndjson"] = bytes.Join(lines, []byte{'\n'})
		}},
		{"duplicate JSON key", func(m map[string][]byte) {
			m["records/bead.ndjson"] = bytes.Replace(m["records/bead.ndjson"], []byte(`"kind":"bead"`), []byte(`"kind":"bead","kind":"bead"`), 1)
		}},
		{"invalid UTF8", func(m map[string][]byte) {
			m["records/bead.ndjson"] = bytes.Replace(m["records/bead.ndjson"], []byte("Synthetic"), []byte{255}, 1)
		}},
		{"missing NDJSON newline", func(m map[string][]byte) {
			m["records/bead.ndjson"] = bytes.TrimSuffix(m["records/bead.ndjson"], []byte{'\n'})
		}},
		{"dangling relation", func(m map[string][]byte) {
			changeRecord(t, m, "edge", func(r map[string]any) { source(r)["depends_on_issue_id"] = "demo-99"; body(r)["to"] = "demo-99" })
		}},
		{"multiple typed targets", func(m map[string][]byte) {
			changeRecord(t, m, "edge", func(r map[string]any) { source(r)["depends_on_external"] = "external" })
		}},
		{"comment mapping mismatch", func(m map[string][]byte) {
			changeRecord(t, m, "comment", func(r map[string]any) { body(r)["author"] = "invented" })
		}},
		{"config value mismatch", func(m map[string][]byte) {
			changeRecord(t, m, "config", func(r map[string]any) { body(r)["value"] = "raw" })
		}},
		{"raw credential or digest", func(m map[string][]byte) {
			changeRecord(t, m, "config", func(r map[string]any) {
				body(r)["classification"] = "public"
				body(r)["value"] = "raw-or-digest"
				source(r)["value"] = "raw-or-digest"
			})
		}},
		{"invalid secret reference", func(m map[string][]byte) {
			changeRecord(t, m, "config", func(r map[string]any) {
				ref := map[string]any{"$secret": "config/github.token", "extra": "raw"}
				source(r)["value"] = ref
				body(r)["value"] = ref
			})
		}},
		{"nested credential", func(m map[string][]byte) {
			changeRecord(t, m, "bead", func(r map[string]any) {
				v := map[string]any{"api-key": "raw"}
				source(r)["metadata"] = v
				body(r)["metadata"] = v
			})
		}},
		{"archive verification claim", func(m map[string][]byte) {
			changeRecord(t, m, "beads-archive", func(r map[string]any) { body(r)["history_verified"] = true })
		}},
		{"archive source tamper", func(m map[string][]byte) {
			changeRecord(t, m, "beads-archive", func(r map[string]any) {
				for k := range source(r) {
					source(r)[k] = nil
					break
				}
			})
		}},
		{"allocator below source ID", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["projects"].([]any)[0].(map[string]any)["last_id"] = 7 })
		}},
		{"child allocator below source ID", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) {
				x["projects"].([]any)[0].(map[string]any)["child_counters"].(map[string]any)["demo-7"] = 3
			})
		}},
		{"wrong source action", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) {
				for _, v := range x["sources"].([]any) {
					d := v.(map[string]any)
					if d["family"] == "issues" {
						d["action"] = "archive"
					}
				}
			})
		}},
		{"nonempty refused source", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) {
				for _, v := range x["sources"].([]any) {
					d := v.(map[string]any)
					if d["family"] == "leases" {
						d["rows"] = 1
					}
				}
			})
		}},
		{"missing source family", func(m map[string][]byte) {
			changeManifest(t, m, func(x map[string]any) { x["sources"] = x["sources"].([]any)[1:] })
		}},
		{"unknown external family", func(m map[string][]byte) {
			var x []map[string]any
			decode(t, m["config/sources.json"], &x)
			x[0]["input"].(map[string]any)["family"] = "unknown"
			m["config/sources.json"] = encode(t, x)
		}},
		{"absent external with values", func(m map[string][]byte) {
			var x []map[string]any
			decode(t, m["config/sources.json"], &x)
			x[0]["values"] = map[string]any{}
			m["config/sources.json"] = encode(t, x)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := rewriteData(t, test.change)
			if _, err := dumpformat.Read(bytes.NewReader(data)); err == nil {
				t.Fatal("invalid archive accepted")
			}
		})
	}
}

func TestArchiveTransportRefusals(t *testing.T) {
	original := fixtureBytes(t)
	for _, test := range []struct {
		name   string
		change func(string, []byte, *tar.Header) (string, []byte)
	}{
		{"checksum", func(n string, b []byte, h *tar.Header) (string, []byte) {
			if n == "records/comment.ndjson" {
				b = bytes.Replace(b, []byte("Full comment text"), []byte("changed comment!!"), 1)
			}
			return n, b
		}},
		{"duplicate member", func(n string, b []byte, h *tar.Header) (string, []byte) {
			if n == "config/sources.json" {
				n = "manifest.json"
			}
			return n, b
		}},
		{"unknown member", func(n string, b []byte, h *tar.Header) (string, []byte) {
			if n == "config/sources.json" {
				n = "extra.bin"
			}
			return n, b
		}},
		{"parent path", func(n string, b []byte, h *tar.Header) (string, []byte) {
			if n == "config/sources.json" {
				n = "../config/sources.json"
			}
			return n, b
		}},
		{"absolute path", func(n string, b []byte, h *tar.Header) (string, []byte) {
			if n == "config/sources.json" {
				n = "/config/sources.json"
			}
			return n, b
		}},
		{"nonregular member", func(n string, b []byte, h *tar.Header) (string, []byte) {
			if n == "config/sources.json" {
				h.Typeflag = tar.TypeSymlink
				h.Linkname = "elsewhere"
			}
			return n, b
		}},
		{"noncanonical mode", func(n string, b []byte, h *tar.Header) (string, []byte) { h.Mode = 0644; return n, b }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := dumpformat.Read(bytes.NewReader(repackTar(t, original, test.change))); err == nil {
				t.Fatal("invalid transport accepted")
			}
		})
	}
	t.Run("oversized member", func(t *testing.T) {
		if _, err := dumpformat.Read(bytes.NewReader(patchMemberSize(original, "config/sources.json", dumpformat.MaxArchiveBytes+1))); err == nil {
			t.Fatal("oversized member accepted")
		}
	})
	t.Run("truncated tar", func(t *testing.T) {
		if _, err := dumpformat.Read(bytes.NewReader(original[:len(original)-512])); err == nil {
			t.Fatal("truncated tar accepted")
		}
	})
	t.Run("trailing data after end", func(t *testing.T) {
		data := append(append([]byte(nil), original...), make([]byte, 1536)...)
		data[len(original)] = 'x'
		if _, err := dumpformat.Read(bytes.NewReader(data)); err == nil {
			t.Fatal("unaccounted trailing data accepted")
		}
	})
	t.Run("read failure", func(t *testing.T) {
		if _, err := dumpformat.Read(failedReader{}); err == nil {
			t.Fatal("reader error accepted")
		}
	})
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("fixture read failed") }

type storeState struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   map[string][]driver.Version
	Changesets []model.Changeset
}

func snapshot(t *testing.T, store driver.Store) storeState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := r.Changesets(ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	result := storeState{Meta: meta, Records: page.Items, Versions: map[string][]driver.Version{}, Changesets: changes}
	for _, row := range page.Items {
		v, err := r.Versions(ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		result.Versions[row.Ref.ID] = v.Items
	}
	return result
}

func TestActualFixtureRefusesWithoutChangingMemoryOrSQLite(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			before := snapshot(t, store)
			if !before.Meta.Head.IsZero() || len(before.Records) != 0 || len(before.Changesets) != 0 {
				t.Fatal("target not empty")
			}
			report, err := Restore(context.Background(), bytes.NewReader(fixtureBytes(t)), store)
			if !errors.Is(err, ErrRestoreUnavailable) {
				t.Fatalf("restore error=%v", err)
			}
			if report.Archive == nil || len(report.Archive.Records("beads-archive")) != 105 || !reflect.DeepEqual(report.Target, before.Meta) {
				t.Fatal("refusal lost source evidence or target metadata")
			}
			if !reflect.DeepEqual(before, snapshot(t, store)) {
				t.Fatal("empty-target refusal changed records/history/metadata")
			}
			// Existing ordinary service/Writer still functions after refusal. This is
			// a current write, with current attribution, not a source restore.
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			svc := service.New(store, lease, writer, service.WithOntology(ontology))
			principal := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			meta := model.WriteMeta{Project: "demo", Agent: "converter-test", AgentLabel: "converter-test", IdempotencyKey: strings.Repeat("ab", 16)}
			committed, err := svc.Create(context.Background(), principal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "Existing target record"}}, meta)
			if err != nil {
				t.Fatal(err)
			}
			ref := committed.Results[0].Ref
			if _, err := svc.Comment(context.Background(), principal, service.CommentRequest{ID: ref, Body: "Existing target comment"}, meta); err != nil {
				t.Fatal(err)
			}
			populated := snapshot(t, store)
			if len(populated.Records) != 1 || len(populated.Changesets) != 2 || len(populated.Versions[ref.ID]) != 2 {
				t.Fatalf("ordinary write boundary=%+v", populated)
			}
			report, err = Restore(context.Background(), bytes.NewReader(fixtureBytes(t)), store)
			if !errors.Is(err, ErrTargetNotEmpty) || report.Archive == nil {
				t.Fatalf("populated target=%v", err)
			}
			if !reflect.DeepEqual(populated, snapshot(t, store)) {
				t.Fatal("populated-target refusal changed records/history/metadata")
			}
			t.Log("real " + kind + ": empty and populated restore refusals preserve records, versions, changesets, head, history floor and incarnation")
		})
	}
}

func TestOrdinaryWriterIsNotASourceIdentityRestore(t *testing.T) {
	a := readFixture(t)
	sourceBead := a.Records("bead")[0]
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			meta := model.WriteMeta{Method: "bead.create", Project: "demo", Agent: "ordinary-writer", AgentLabel: "ordinary-writer", IdempotencyKey: strings.Repeat("cd", 16)}
			before := snapshot(t, store)
			// The normal ontology refuses an already-started creation; a source lease
			// cannot be invented to work around this boundary.
			_, err := writer.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "demo", Fields: map[string]any{"title": "Source started", "status": "in_progress"}}}}}, meta)
			if err == nil {
				t.Fatal("normal ontology accepted noninitial status")
			}
			if !reflect.DeepEqual(before, snapshot(t, store)) {
				t.Fatal("ordinary create refusal changed target")
			}
			commit, err := writer.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Key: sourceBead.ID, Type: "issue", Project: "demo", Fields: map[string]any{"title": sourceBead.Body["title"]}}}}}, meta)
			if err != nil {
				t.Fatal(err)
			}
			if len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" || commit.Results[0].Ref.ID == sourceBead.ID {
				t.Fatal("CreateOp.Key was treated as source identity")
			}
			state := snapshot(t, store)
			doc, err := pipeline.DecodeDocument(state.Records[0].Body)
			if err != nil {
				t.Fatal(err)
			}
			if doc.Fields["status"] != "todo" || doc.Lease != nil || len(state.Changesets) != 1 || !reflect.DeepEqual(state.Changesets[0].Principal, by) || state.Changesets[0].Kind != model.ChangesetWrite {
				t.Fatal("ordinary write did not follow current Writer semantics")
			}
			t.Log("CreateOp.Key is an alias; normal create mints ID/revision/current attribution and requires initial status; this is not a converter roundtrip")
		})
	}
}

func TestInvalidArchiveAndCancellationConserveTarget(t *testing.T) {
	store, _, cleanup := openTarget(t, "memory")
	defer cleanup()
	before := snapshot(t, store)
	if _, err := Restore(context.Background(), strings.NewReader("not a tar"), store); err == nil {
		t.Fatal("invalid archive accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Restore(ctx, bytes.NewReader(fixtureBytes(t)), store); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if !reflect.DeepEqual(before, snapshot(t, store)) {
		t.Fatal("invalid/cancelled input changed store")
	}
}
