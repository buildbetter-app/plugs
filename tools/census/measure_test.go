package census

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var dumpFixture = flag.String("dump-fixture", "", "Bazel runfile for the public synthetic exporter fixture")

func publicFixture(t *testing.T) []byte {
	t.Helper()
	file := *dumpFixture
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		file = filepath.Join(root, filepath.FromSlash(file))
	}
	data, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	return data
}

func metric(s Snapshot, name string) Metric {
	for _, m := range s.Metrics {
		if m.Dimension == name {
			return m
		}
	}
	return Metric{}
}

func TestMeasuredSourceIncludesPairedSchemaAndAllFamilies(t *testing.T) {
	capture, e := MeasureArchive(bytes.NewReader(publicFixture(t)))
	if e != nil {
		t.Fatal(e)
	}
	s := capture.Summary()
	if !validSnapshot(s) || s.Schema.Main != 72 || s.Schema.Ignored != 28 || metric(s, "counts").Count != 115 || metric(s, "dependencies").Count != 1 || metric(s, "archives").Count != 105 || metric(s, "versions").Count != 2 || metric(s, "versions").Block != HistoryUnavailable {
		t.Fatal("source families or paired schema were not measured")
	}
	if metric(s, "prefixes").Count != 1 || metric(s, "counters").Count != 1 {
		t.Fatal("project state disappeared")
	}
	raw, _ := json.Marshal(s)
	for _, value := range []string{"demo-7", "demo", "Synthetic", "kv.memory", "source_table"} {
		if bytes.Contains(raw, []byte(value)) {
			t.Fatalf("raw source value escaped the measurement: %q", value)
		}
	}
	again, e := MeasureArchive(bytes.NewReader(publicFixture(t)))
	if e != nil || !reflect.DeepEqual(s, again.Summary()) {
		t.Fatal("same archive produced a different census")
	}
	s.Metrics[0].Count = 0
	if capture.Summary().Metrics[0].Count == 0 {
		t.Fatal("summary mutated the capture")
	}
	writeSanitized(t, "synthetic-source-census.json", capture.Summary())
}

func TestMeasuredSourceNamesWorkflowDispositions(t *testing.T) {
	capture, err := MeasureArchive(bytes.NewReader(publicFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	s := capture.Summary()
	if len(s.WorkflowDispositions) != 6 {
		t.Fatalf("workflow dispositions=%d, want six declared workflow-plane families", len(s.WorkflowDispositions))
	}
	want := []string{"\x77isp_child_counters", "\x77isp_comments", "\x77isp_dependencies", "\x77isp_events", "\x77isp_labels", "\x77isps"}
	for i, disposition := range s.WorkflowDispositions {
		if disposition.Family != want[i] || disposition.Rows != 0 || disposition.Action != workflowDispositionAction || disposition.Reason != workflowDispositionReason {
			t.Fatalf("workflow disposition[%d]=%+v, want family=%q, empty, refuse-if-nonempty", i, disposition, want[i])
		}
	}
}

func TestMeasuredArchiveRefusesUntrustedWorkflowReason(t *testing.T) {
	const marker = "private-workflow-reason-needle"
	data := rewritePublic(t, func(m map[string][]byte) {
		var manifest map[string]any
		mustDecode(t, m["manifest.json"], &manifest)
		for _, raw := range manifest["sources"].([]any) {
			source := raw.(map[string]any)
			if source["family"] == "wisps" {
				source["reason"] = marker
			}
		}
		m["manifest.json"] = mustEncode(t, manifest)
	})
	if _, err := MeasureArchive(bytes.NewReader(data)); err == nil || strings.Contains(err.Error(), marker) {
		t.Fatalf("untrusted workflow reason was admitted or leaked: %v", err)
	}
}

func TestMeasuredArchiveRefusesDriftAndSanitizesErrors(t *testing.T) {
	for _, change := range []func(map[string][]byte){
		func(m map[string][]byte) {
			var v map[string]any
			mustDecode(t, m["manifest.json"], &v)
			v["ignored_version"] = 26
			m["manifest.json"] = mustEncode(t, v)
		},
		func(m map[string][]byte) {
			m["records/bead.ndjson"] = bytes.Replace(m["records/bead.ndjson"], []byte(`"title"`), []byte(`"private-field-needle"`), 1)
		},
		func(m map[string][]byte) {
			m["records/beads-archive.ndjson"] = bytes.Replace(m["records/beads-archive.ndjson"], []byte(`"content_hash":"`), []byte(`"content_hash":"private-needle-`), 1)
		},
	} {
		data := rewritePublic(t, change)
		if _, e := MeasureArchive(bytes.NewReader(data)); e == nil || strings.Contains(e.Error(), "needle") {
			t.Fatal("invalid source archive was admitted or leaked")
		}
	}
	if _, e := MeasureArchive(failingInput{}); e == nil || strings.Contains(e.Error(), "needle") {
		t.Fatal("input error was not sanitized")
	}
}

type failingInput struct{}

func (failingInput) Read([]byte) (int, error) { return 0, errors.New("private-path-and-row-needle") }

// Construct a separate supported head fixture from PUBLIC synthetic bytes.
// This does not change or weaken the original fixture: its unsupported
// history/allocator profile remains refused by the owning restore target.
func boundedHead(t *testing.T) []byte {
	t.Helper()
	return rewritePublic(t, func(m map[string][]byte) {
		replace := strings.NewReplacer("demo-7.4", "pl-"+strings.Repeat("b", 32), "demo-7", "pl-"+strings.Repeat("a", 32), "demo-8", "pl-"+strings.Repeat("c", 32), `"demo"`, `"pl"`)
		for name, data := range m {
			m[name] = []byte(replace.Replace(string(data)))
		}
		var manifest map[string]any
		mustDecode(t, m["manifest.json"], &manifest)
		manifest["history"] = "unavailable"
		manifest["projects"] = []any{map[string]any{"prefix": "pl", "id_mode": "hash", "hash_length": 32, "last_id": 0, "child_counters": map[string]any{}}}
		kinds := []any{}
		counts := map[string]int{}
		for _, raw := range manifest["kind_registry"].([]any) {
			kind := raw.(map[string]any)
			name, file := kind["name"].(string), kind["file"].(string)
			if name == "comment" {
				delete(m, file)
				delete(manifest["files"].(map[string]any), file)
				continue
			}
			var out bytes.Buffer
			n := 0
			for _, line := range bytes.Split(bytes.TrimSpace(m[file]), []byte{'\n'}) {
				var row map[string]any
				mustDecode(t, line, &row)
				source := row["source"].(map[string]any)["fields"].(map[string]any)
				table := row["source"].(map[string]any)["table"].(string)
				if table == "events" || table == "issue_versions" || table == "child_counters" || table == "issue_counter" {
					continue
				}
				body := row["body"].(map[string]any)
				if name == "edge" {
					source["type"] = "blocks"
					body["type"] = "blocks"
				}
				counts[table]++
				if name == "bead" {
					counts["labels"] += len(body["labels"].([]any))
				}
				out.Write(mustEncode(t, row))
				n++
			}
			m[file] = out.Bytes()
			kind["count"] = n
			kinds = append(kinds, kind)
		}
		manifest["kind_registry"] = kinds
		counts["issues.row_lock"], counts["issues.is_blocked"] = counts["issues"], counts["issues"]
		for _, raw := range manifest["sources"].([]any) {
			row := raw.(map[string]any)
			family := row["family"].(string)
			if _, ok := counts[family]; ok || family == "comments" || family == "events" || family == "issue_versions" || family == "child_counters" || family == "issue_counter" {
				row["rows"] = counts[family]
			}
		}
		m["manifest.json"] = mustEncode(t, manifest)
	})
}

func rewritePublic(t *testing.T, change func(map[string][]byte)) []byte {
	t.Helper()
	members := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(publicFixture(t)))
	for {
		h, e := r.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(r)
		if e != nil {
			t.Fatal(e)
		}
		members[h.Name] = data
	}
	change(members)
	var manifest map[string]any
	mustDecode(t, members["manifest.json"], &manifest)
	files := manifest["files"].(map[string]any)
	names := []string{}
	for name, data := range members {
		if name == "manifest.json" {
			continue
		}
		if _, ok := files[name]; ok {
			files[name] = map[string]any{"size": len(data), "sha256": bytesDigest(data)}
		}
		names = append(names, name)
	}
	members["manifest.json"] = mustEncode(t, manifest)
	sort.Strings(names)
	names = append([]string{"manifest.json"}, names...)
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, name := range names {
		data := members[name]
		if e := w.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); e != nil {
			t.Fatal(e)
		}
		if _, e := w.Write(data); e != nil {
			t.Fatal(e)
		}
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}

func mustDecode(t *testing.T, data []byte, target any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if e := d.Decode(target); e != nil {
		t.Fatal(e)
	}
}
func mustEncode(t *testing.T, value any) []byte {
	t.Helper()
	data, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return append(data, '\n')
}

func TestPairedCensusReadsActualMemoryAndReopenedSQLite(t *testing.T) {
	data := boundedHead(t)
	capture, e := MeasureArchive(bytes.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	const derived = "89f6eb61310b4f9193e9925e2a2305101b126f763e870986bc7ca51f6043d28a"
	check := converterroundtrip.Verification{ArchiveSHA256: bytesDigest(data), DerivedSHA256: derived}
	var first []Metric
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			var store driver.Store = memory.New()
			file := filepath.Join(t.TempDir(), "disposable.db")
			if engine == "sqlite" {
				_ = store.Close()
				store, e = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: file})
				if e != nil {
					t.Fatal(e)
				}
			}
			defer func() { _ = store.Close() }()
			lease, e := store.Lease(ctx, "census-synthetic", 10*time.Second)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = converterroundtrip.RestoreWithLease(ctx, bytes.NewReader(data), store, lease, check); e != nil {
				t.Fatal(e)
			}
			if e = lease.Release(ctx); e != nil {
				t.Fatal(e)
			}
			if engine == "sqlite" {
				if e = store.Close(); e != nil {
					t.Fatal(e)
				}
				store, e = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: file})
				if e != nil {
					t.Fatal(e)
				}
			}
			before, _ := store.Meta(ctx)
			observed := &observedCensusStore{Store: store}
			s, e := capture.MeasureStore(ctx, observed, 7)
			if e != nil {
				t.Fatal(e)
			}
			after, _ := store.Meta(ctx)
			if !reflect.DeepEqual(before, after) || observed.reads != 1 || observed.closes != 1 || observed.writes != 0 || observed.leases != 0 {
				t.Fatal("measurement changed state or leaked its reader")
			}
			if s.NativeHistory == nil || s.NativeHistory.Versions != 109 || s.NativeHistory.Changesets != 0 || s.NativeHistory.Head != "1" || s.NativeHistory.Floor != "1" {
				t.Fatal("native history inventory was overstated or omitted")
			}
			if metric(s, "counters").Block != AllocatorUnproven || metric(s, "versions").Block != HistoryUnavailable {
				t.Fatal("unsupported continuity was certified")
			}
			for _, m := range s.Metrics {
				want := metric(capture.Summary(), m.Dimension)
				if m.Count != want.Count || m.SHA256 != want.SHA256 {
					t.Fatalf("lost source %s", m.Dimension)
				}
			}
			if first == nil {
				first = s.Metrics
			} else if !reflect.DeepEqual(first, s.Metrics) {
				t.Fatal("memory/SQLite source census differs")
			}
			writeSanitized(t, "synthetic-"+engine+"-census.json", s)
			// An actual ordinary Writer update leaves source provenance intact.
			// A provenance-only census would miss this changed live description.
			lease, e = store.Lease(ctx, "census-edit", 10*time.Second)
			if e != nil {
				t.Fatal(e)
			}
			defer lease.Release(ctx)
			writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
			id := "pl-" + strings.Repeat("a", 32)
			_, e = writer.Apply(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(id, model.Natural64(0)), Fields: map[string]any{"description": "changed live description"}}}}}, model.WriteMeta{Method: "tx.apply", Project: "pl", Agent: "census-test", AgentLabel: "census-test", IdempotencyKey: strings.Repeat("cd", 16)})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = capture.MeasureStore(ctx, observed, 7); e == nil {
				t.Fatal("native content loss was hidden by retained provenance")
			}
			if observed.reads != observed.closes {
				t.Fatal("refusal leaked a reader")
			}
			// Restore the original content through a second ordinary write.
			// The now-equal head must still inventory all retained versions and
			// changesets, including multiple pages for the edited record.
			var original any
			for _, row := range capture.archive.Records("bead") {
				if row.ID == id {
					original = row.Body["description"]
				}
			}
			_, e = writer.Apply(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(id, model.Natural64(0)), Fields: map[string]any{"description": original}}}}}, model.WriteMeta{Method: "tx.apply", Project: "pl", Agent: "census-test", AgentLabel: "census-test", IdempotencyKey: strings.Repeat("ef", 16)})
			if e != nil {
				t.Fatal(e)
			}
			returned, e := capture.MeasureStore(ctx, observed, 1)
			if e != nil || returned.NativeHistory == nil || returned.NativeHistory.Versions != 111 || returned.NativeHistory.Changesets != 2 {
				t.Fatalf("retained multi-page history census: %v", e)
			}
			if observed.reads != observed.closes {
				t.Fatal("history measurement leaked a reader")
			}
		})
	}
}

type observedCensusStore struct {
	driver.Store
	reads, closes, writes, leases int
	failBlob                      bool
}

func (s *observedCensusStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, e := s.Store.Read(ctx, v)
	if e != nil {
		return nil, e
	}
	s.reads++
	return &observedCensusReader{Reader: r, owner: s}, nil
}
func (s *observedCensusStore) Write(context.Context, driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return nil, errors.New("measurement attempted write")
}
func (s *observedCensusStore) Lease(context.Context, string, time.Duration) (driver.ActiveLease, error) {
	s.leases++
	return nil, errors.New("measurement attempted lease")
}

type observedCensusReader struct {
	driver.Reader
	owner *observedCensusStore
}

func (r *observedCensusReader) Close() error { r.owner.closes++; return r.Reader.Close() }
func (r *observedCensusReader) Blob(ctx context.Context, h driver.Hash) ([]byte, error) {
	if r.owner.failBlob {
		return nil, errors.New("private-blob-value-needle")
	}
	return r.Reader.Blob(ctx, h)
}

func TestCensusBlobRefusalIsReadOnlyAndClosesReader(t *testing.T) {
	data := boundedHead(t)
	capture, e := MeasureArchive(bytes.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	store := memory.New()
	defer store.Close()
	if _, e = converterroundtrip.Restore(context.Background(), bytes.NewReader(data), store, converterroundtrip.Verification{ArchiveSHA256: bytesDigest(data), DerivedSHA256: "89f6eb61310b4f9193e9925e2a2305101b126f763e870986bc7ca51f6043d28a"}); e != nil {
		t.Fatal(e)
	}
	wrapped := &observedCensusStore{Store: store, failBlob: true}
	if _, e = capture.MeasureStore(context.Background(), wrapped, 3); e == nil || strings.Contains(e.Error(), "needle") {
		t.Fatal("missing retained blob did not safely refuse")
	}
	if wrapped.reads != 1 || wrapped.closes != 1 || wrapped.writes != 0 || wrapped.leases != 0 {
		t.Fatal("blob refusal changed state or leaked handles")
	}
}
