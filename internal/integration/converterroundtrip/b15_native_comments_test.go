package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These expectations come only from the independently recorded pre-export SQL
// tables. Neither the receiver, its archive projection nor target readback is an
// expectation source. The original native ID stays distinct from a public page
// ordinal and from the content-addressed retained active-family record ID.
type b15NativeComment struct {
	Table   string         `json:"table"`
	Project string         `json:"project"`
	ID      string         `json:"id"`
	Fields  dumpformat.Row `json:"fields"`
}

func b15JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// The consumer accepts only the final r11 producer packet and its separately
// derived current-envelope oracle. Both immutable inputs bind the invocation
// to the original source; the original request and oracle remain unchanged.
type b15NativeSourceBinding struct {
	StoreID              string
	CaptureSHA256        string
	LedgerSHA256         string
	StatesSHA256         string
	RequestSHA256        string
	ConsumerLedgerSHA256 string
}

var b15NativeSourceBindings = map[string]b15NativeSourceBinding{
	"commentcody": {
		StoreID:              "native/e08de6af03275f11375cc6639b6fb1476da3e8ad7a8bf98513dc223792d18d9e",
		CaptureSHA256:        "982e40d7e2fc175b8892cc9b00d00b633d9a6d7df4ce807a239b4da9e4373ed4",
		LedgerSHA256:         "6c3354509db532cf7dd62fa7e34d667b5acb8ead30dceb7d92829acb9f993b1a",
		StatesSHA256:         "a932df103024d943f1caa241092589cf93a2fc7fb391d767a8bfb81b03d0b781",
		RequestSHA256:        "e43074c719e8242c577e3ee914841357e530b85cab68868b2a702968f87a8202",
		ConsumerLedgerSHA256: "c32e8ce695cb82e6c7bc0e23dc29f1f520a34265006cb139085402465a31f4d5",
	},
	"commentsignals": {
		StoreID:              "native/c3f9fd5817d3380c213c557020d5945b676c2059528e1345327dc1fc60f60b13",
		CaptureSHA256:        "c29754b3857d6d842e70ae67004930630e21fad70214c903f9cad1ea5fa8e058",
		LedgerSHA256:         "499d7bd57f22efc118825ec6f901be689e2f53ae44812e2a9caa9d3dec67c9a3",
		StatesSHA256:         "e6b878c9e417cb4b1c7a204c9eddec13e270ce4bb9abce0bc04ea46c7a5312c4",
		RequestSHA256:        "2152a90f804b895295221922e844fdb20ae486a9fc9d439588bd93f2c9adbb9e",
		ConsumerLedgerSHA256: "8acd24c3e774aed1a902bd9b8392bd01e909e1bd754ea23ba0bdf35f65b688b1",
	},
}

const (
	b15NativePacketEnv      = "PLUG_B15_NATIVE_PACKET"
	b15NativeProjectsEnv    = "PLUG_B15_NATIVE_PROJECTS"
	b15NativeDriversEnv     = "PLUG_B15_NATIVE_DRIVERS"
	b15NativeRootEnv        = "PLUG_B15_NATIVE_ROOT"
	b15NativeOriginalReqEnv = "PLUG_B15_NATIVE_ORIGINAL_REQUEST_SHA256"
	b15NativePacketRunfile  = "internal/integration/converterroundtrip/testdata/b15-native-r11-d74f91f9.packet.tar.gz"
	b15NativePacketSHA256   = "12ebe1d5a091f0878cfd7825ac84d33c07a233b4571d090cb79b1a3b47d04aa9"
	b15NativeOracleRunfile  = "internal/integration/converterroundtrip/testdata/b15-native-r11-current-oracle.tar.gz"
	b15NativeOracleSHA256   = "4d1eb105e3697c7eda1d3518c857b8fe2944a6ac7fd65bdb54b247739f6bfb9b"
)

func b15NativeRunfile(t *testing.T, rel string) string {
	t.Helper()
	candidates := []string{}
	if root, workspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"); root != "" && workspace != "" {
		candidates = append(candidates, filepath.Join(root, workspace, rel))
	}
	candidates = append(candidates, filepath.Join("testdata", filepath.Base(rel)))
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	t.Fatalf("B15 native packet runfile is unavailable: %s", rel)
	return ""
}

// The producer packet is immutable source input, transported as a runfile so
// remote Bazel sees the exact request, native capture and independent oracle.
// The original request and oracle stay unchanged. A separate consumer oracle
// corrects only current history-envelope labels using the original source
// manifest's classifications; all rows, typed hashes and state roots survive.
func b15MaterializeNativePacket(t *testing.T) string {
	t.Helper()
	if os.Getenv(b15NativePacketEnv) != "1" {
		t.Fatal("B15 packet materialization requires packet mode")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	b15UnpackNativeInput(t, root, b15NativePacketRunfile, b15NativePacketSHA256)
	b15UnpackNativeInput(t, root, b15NativeOracleRunfile, b15NativeOracleSHA256)
	return root
}

func b15UnpackNativeInput(t *testing.T, root, runfile, digest string) {
	t.Helper()
	path := b15NativeRunfile(t, runfile)
	if publicProofHashFile(t, path) != digest {
		t.Fatalf("B15 committed input digest differs: %s", runfile)
	}
	input, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	compressed, err := gzip.NewReader(input)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader := tar.NewReader(compressed)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Clean(filepath.FromSlash(header.Name))
		if name == "." || filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(filepath.Separator)) || name == ".." || header.Size < 0 || header.Size > 64<<20 {
			t.Fatalf("invalid B15 packet member %q", header.Name)
		}
		destination := filepath.Join(root, name)
		if destination != root && !strings.HasPrefix(destination, root+string(filepath.Separator)) {
			t.Fatalf("B15 packet member escapes staging root: %q", header.Name)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(destination, 0700); err != nil {
				t.Fatal(err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, copyErr := io.CopyN(file, reader, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				t.Fatal(copyErr)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
		default:
			t.Fatalf("unsupported B15 packet member type %q", header.Name)
		}
	}
}

func b15PrepareNativeRequest(t *testing.T, root, project string) (string, b15NativeSourceBinding) {
	t.Helper()
	binding, ok := b15NativeSourceBindings[project]
	if !ok {
		t.Fatalf("B15 packet project is not admitted: %s", project)
	}
	originalPath := filepath.Join(root, project, "request.full.json")
	original, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(original) != binding.RequestSHA256 {
		t.Fatalf("B15 original request digest differs for %s", project)
	}
	var req publicProofRequest
	decoder := json.NewDecoder(bytes.NewReader(original))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		t.Fatal(err)
	}
	if req.Source.Project != project {
		t.Fatalf("B15 original request project differs: %s", req.Source.Project)
	}
	if publicProofHashFile(t, filepath.Join(root, project, "oracle", "tables.ndjson")) != binding.LedgerSHA256 || publicProofHashFile(t, filepath.Join(root, project, "oracle", "states.ndjson")) != binding.StatesSHA256 {
		t.Fatal("B15 preserved original oracle binding differs")
	}
	req.Capture = filepath.Join(root, project, "capture", "capture-set")
	req.Oracle = filepath.Join(root, project, "consumer-oracle")
	req.LedgerSHA256 = binding.ConsumerLedgerSHA256
	if publicProofHashFile(t, filepath.Join(req.Oracle, "tables.ndjson")) != req.LedgerSHA256 || publicProofHashFile(t, filepath.Join(req.Oracle, "states.ndjson")) != binding.StatesSHA256 {
		t.Fatal("B15 current consumer oracle binding differs")
	}
	resolved, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resolvedPath := filepath.Join(root, project, "request.resolved.json")
	if err := os.WriteFile(resolvedPath, append(resolved, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(b15NativeRootEnv, root)
	t.Setenv(b15NativeOriginalReqEnv, binding.RequestSHA256)
	return resolvedPath, binding
}

func b15NativeList(t *testing.T, env, fallback string) []string {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		raw = fallback
	}
	var values []string
	seen := map[string]bool{}
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			t.Fatalf("invalid repeated B15 %s value", env)
		}
		seen[value] = true
		values = append(values, value)
	}
	return values
}

func b15BindNativeExecutable(t *testing.T) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	actual := publicProofHashFile(t, self)
	if configured := os.Getenv("PLUG_PUBLIC_PROOF_ELF_SHA256"); configured != "" && configured != actual {
		t.Fatalf("B15 compiled ELF binding differs: configured=%s actual=%s", configured, actual)
	}
	t.Setenv("PLUG_PUBLIC_PROOF_ELF_SHA256", actual)
}

func b15NativeRequest(t *testing.T) publicProofRequest {
	t.Helper()
	if os.Getenv(b15NativePacketEnv) != "1" {
		t.Fatal("B15 requires the committed native packet")
	}
	path := os.Getenv("PLUG_PUBLIC_PROOF_REQUEST")
	if path == "" || publicProofHashFile(t, path) != os.Getenv("PLUG_PUBLIC_PROOF_REQUEST_SHA256") {
		t.Fatal("B15 request binding differs")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var req publicProofRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		t.Fatal(err)
	}
	want, ok := b15NativeSourceBindings[req.Source.Project]
	if !ok || os.Getenv(b15NativeOriginalReqEnv) != want.RequestSHA256 || req.Format != "public-native-capture-proof/1" || req.ProducerHead != "d74f91f9f0fdcd9c95c7f30bd42a2d257fc219b3" || req.Source.StoreID != want.StoreID || req.CaptureSHA256 != want.CaptureSHA256 || req.LedgerSHA256 != want.ConsumerLedgerSHA256 || req.StatesSHA256 != want.StatesSHA256 || req.RestoreTime != "2040-01-02T03:05:00Z" || req.RestoreMode != "full" || len(req.RestoreFirstSequence) != 0 || req.Commits != 85 || req.JournalTable != dumpformat.SourceEventJournalTable {
		t.Fatal("B15 request is not the final r11 source packet")
	}
	root := os.Getenv(b15NativeRootEnv)
	if root == "" || !filepath.IsAbs(root) {
		t.Fatal("B15 resolved packet root is missing")
	}
	for _, path := range []string{req.Capture, req.Oracle} {
		clean := filepath.Clean(path)
		if !filepath.IsAbs(clean) || !strings.HasPrefix(clean, root+string(filepath.Separator)) {
			t.Fatalf("B15 request points outside committed native packet: %s", path)
		}
	}
	return req
}

func b15Multiset(t *testing.T, values any) []string {
	t.Helper()
	v := reflect.ValueOf(values)
	out := make([]string, v.Len())
	for i := range out {
		out[i] = string(b15JSON(t, v.Index(i).Interface()))
	}
	sort.Strings(out)
	return out
}

// These literal source inputs are independent of both the native SQL recorder
// and the Plug receiver. The live IDs were independently calculated from the
// frozen native rule and are not derived from any observed response.
func b15FixtureExpectedNativeComments(t *testing.T, project string) []b15NativeComment {
	t.Helper()
	liveIDs := map[string][2][2]string{
		"commentcody": {
			{"5c1a1eda-f52c-5862-beb5-e5e85a7ae208", "4034cdec-c306-5363-a94f-cc528fbff023"},
			{"6e138e5a-af3b-5926-b57b-cf8d4db8b766", "deb50b36-1322-58fe-aa55-9dbaf807f8d6"},
		},
		"commentsignals": {
			{"f314a5f3-09b4-52f2-9dbd-564aa3ddbb08", "75380e39-5d77-5539-88e0-5aaed96e93ac"},
			{"c5bd979b-be0e-56ea-bf7e-f1fdf4802464", "f93e90b2-d5f0-561c-abf2-0cf0b72e6454"},
		},
	}
	ids, ok := liveIDs[project]
	if !ok {
		t.Fatal("B15 input is not one of the two declared disposable native sources")
	}
	future := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
	equal := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	var out []b15NativeComment
	add := func(table, anchor, id, author, body string, at time.Time) {
		out = append(out, b15NativeComment{Table: table, Project: project, ID: id, Fields: dumpformat.Row{"id": id, "issue_id": anchor, "author": author, "text": body, "created_at": at.Format(time.RFC3339Nano)}})
	}
	for plane, suffix := range []string{"a1b", "c2d", "e3f", "4de"} {
		table, anchor := "comments", project+"-"+suffix
		if plane%2 == 1 {
			table = dumpformat.EphemeralCommentTable
		}
		count := 503
		if plane >= 2 {
			count = 3
			id := fmt.Sprintf("7%d000000-0000-4000-8000-%012d", plane, 0)
			add(table, anchor, id, "Imported Past Source", "preserved past source seed", equal.Add(-time.Minute))
		}
		for ordinal := 1; ordinal <= count; ordinal++ {
			at := future
			body := fmt.Sprintf("%s public import comment %03d", anchor, ordinal)
			if plane < 2 {
				switch ordinal {
				case 1:
					body = ""
				case 2:
					body = " \t\n "
				case 3:
					body = "\n  λ Unicode 雪\n"
				}
			} else {
				at = equal.Add(time.Duration(ordinal-1) * time.Second)
			}
			id := fmt.Sprintf("7%d000000-0000-4000-8000-%012d", plane, ordinal)
			add(table, anchor, id, fmt.Sprintf("Imported Source %d", plane), body, at)
		}
		if plane < 2 {
			for i, body := range []string{"\n  native live λ 雪\n", "second native live comment"} {
				add(table, anchor, ids[plane][i], "Native Public Author", body, future.Add(time.Duration(i+1)*time.Second))
			}
		}
	}
	return out
}

func b15ReadNativeComments(t *testing.T, req publicProofRequest) []b15NativeComment {
	t.Helper()
	var expected []b15NativeComment
	tables := map[string]bool{}
	for _, entry := range publicProofLedger(t, req) {
		if entry.Kind != "current" || entry.Family != "comments" && entry.Family != dumpformat.EphemeralCommentTable {
			continue
		}
		if tables[entry.Family] || entry.History != b15NativeCommentHistory(entry.Family) || entry.SourceRows != 509 || entry.ImportedRows != entry.SourceRows {
			t.Fatal("B15 needs each complete, nonempty original comment family exactly once")
		}
		tables[entry.Family] = true
		raw, err := os.ReadFile(filepath.Join(req.Oracle, entry.File))
		if err != nil || len(raw) != entry.Bytes || hashBytes(raw) != entry.SHA256 {
			t.Fatal("B15 independent original table bytes differ", err)
		}
		var table struct {
			Source  dumpformat.SourceIdentity `json:"source_identity"`
			Table   string                    `json:"table"`
			History string                    `json:"history"`
			Columns []string                  `json:"columns"`
			Rows    []dumpformat.Row          `json:"rows"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&table); err != nil {
			t.Fatal(err)
		}
		if table.Source != req.Source || table.Table != entry.Family || table.History != entry.History || len(table.Rows) != entry.SourceRows || !reflect.DeepEqual(table.Columns, []string{"author", "created_at", "id", "issue_id", "text"}) {
			t.Fatal("B15 independent table identity/schema/census differs")
		}
		seen := map[string]bool{}
		for _, fields := range table.Rows {
			for _, field := range table.Columns {
				value, ok := fields[field].(string)
				// Native import admits historical empty/whitespace bodies. Live
				// comment validation must never rewrite or drop those source rows.
				if !ok || field != "text" && strings.TrimSpace(value) == "" {
					t.Fatal("B15 native comment field is absent or invalid exact text", field)
				}
			}
			id := fields["id"].(string)
			if len(fields) != len(table.Columns) || seen[id] {
				t.Fatal("B15 native comment schema or original ID repeats")
			}
			seen[id] = true
			if _, err := time.Parse(time.RFC3339Nano, fields["created_at"].(string)); err != nil {
				t.Fatal(err)
			}
			expected = append(expected, b15NativeComment{Table: table.Table, Project: req.Source.Project, ID: id, Fields: fields})
		}
	}
	if len(tables) != 2 {
		t.Fatal("B15 requires both actual native comment planes")
	}
	if !reflect.DeepEqual(b15Multiset(t, expected), b15Multiset(t, b15FixtureExpectedNativeComments(t, req.Source.Project))) {
		t.Fatal("B15 independent pre-export SQL differs from the exact declared native input tuples")
	}
	return expected
}

func b15AssertSourceCommentRecords(t *testing.T, archive *dumpformat.Archive, req publicProofRequest, expected []b15NativeComment) {
	t.Helper()
	manifest := archive.Manifest()
	if manifest.StoreID != req.Source.StoreID || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != req.Source.Project || manifest.SourceIdentityContract != "" {
		t.Fatal("B15 requires the original single-source, non-namespaced native capture")
	}
	var want, got []dumpformat.Record
	for _, tuple := range expected {
		if tuple.Table == "comments" {
			want = append(want, dumpformat.Record{Kind: "comment", ID: tuple.ID, Project: tuple.Project, Body: tuple.Fields, Source: dumpformat.Provenance{Table: tuple.Table, Fields: tuple.Fields}})
		} else {
			id := tuple.Table + "/" + hashBytes(append(b15JSON(t, tuple.Fields), '\n'))
			body := dumpformat.Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}
			// Opaque native archive records deliberately have no portable
			// project field; the source identity and exact source fields carry
			// the project binding independently.
			want = append(want, dumpformat.Record{Kind: "beads-archive", ID: id, Body: body, Source: dumpformat.Provenance{Table: tuple.Table, Fields: tuple.Fields}})
		}
	}
	for _, kind := range []string{"comment", "beads-archive"} {
		for _, record := range archive.Records(kind) {
			if record.Source.Table != "comments" && record.Source.Table != dumpformat.EphemeralCommentTable {
				continue
			}
			// An unmerged native capture binds its exact store/project in the
			// manifest. The comparison retains the complete provenance object,
			// including the required absent per-record namespace marker.
			got = append(got, record)
		}
	}
	if !reflect.DeepEqual(b15Multiset(t, got), b15Multiset(t, want)) {
		t.Fatal("B15 complete logical comment ID/project/body/source tuple multiset differs from native SQL")
	}
}

func b15NativeCommentHistory(table string) string {
	switch table {
	case "comments":
		return dumpformat.FamilyVersioned
	case dumpformat.EphemeralCommentTable:
		return dumpformat.FamilyUnversioned
	default:
		return ""
	}
}

func b15AssertStoredNativeComments(t *testing.T, ctx context.Context, store driver.Store, req publicProofRequest, expected []b15NativeComment) {
	t.Helper()
	meta, err := store.Meta(ctx)
	publicProofError(t, "B15-current-meta", err)
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	publicProofError(t, "B15-current-reader", err)
	defer reader.Close()
	var got []b15NativeComment
	metas := map[string]int{}
	var cursor *driver.Cursor
	for {
		page, err := reader.Query(ctx, driver.Query{AsOf: &meta.Head, Limit: 256, Cursor: cursor})
		publicProofError(t, "B15-current-records", err)
		for _, record := range page.Items {
			// Retained commit rows have separate history identities. Only the
			// current namespace is the imported/reopened source tuple multiset.
			if !strings.HasPrefix(record.Ref.ID, "source/native-current/") {
				continue
			}
			tableMeta, row, err := dumpformat.DecodeNativeRowRecord(record)
			publicProofError(t, "B15-native-row-decode", err)
			if tableMeta != nil {
				if tableMeta.Table == "comments" || tableMeta.Table == dumpformat.EphemeralCommentTable {
					if tableMeta.Source != req.Source || tableMeta.History != b15NativeCommentHistory(tableMeta.Table) || tableMeta.IdentityKind != dumpformat.NativeRowD3Physical || tableMeta.RowCount != 509 {
						t.Fatal("B15 current comment metadata changed source, history class or physical census")
					}
					metas[tableMeta.Table]++
				}
				continue
			}
			if row.Table != "comments" && row.Table != dumpformat.EphemeralCommentTable {
				continue
			}
			if row.Source != req.Source || row.History != b15NativeCommentHistory(row.Table) || row.IdentityKind != dumpformat.NativeRowD3Physical {
				t.Fatal("B15 current physical comment source, history class or identity kind changed")
			}
			id, ok := row.Fields["id"].(string)
			if !ok || id == "" {
				t.Fatal("B15 current physical comment lost its native ID")
			}
			got = append(got, b15NativeComment{Table: row.Table, Project: row.Source.Project, ID: id, Fields: row.Fields})
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || cursor != nil && reflect.DeepEqual(cursor, page.NextCursor) {
			t.Fatal("B15 complete current native row walk stalled")
		}
		cursor = page.NextCursor
	}
	if metas["comments"] != 1 || metas[dumpformat.EphemeralCommentTable] != 1 || !reflect.DeepEqual(b15Multiset(t, got), b15Multiset(t, expected)) {
		t.Fatal("B15 imported/reopened physical comment tuple multiset differs from independent pre-export SQL")
	}
}

// This adapter installs the actual receiver-produced source provider on the
// actual callback store. Import, leases, draining and reopen stay with the
// shared owner; neither the request helper nor this binding owns a lifecycle.
type b15NativeClient struct {
	t          *testing.T
	ctx        context.Context
	store      driver.Store
	project    string
	actor      model.PrincipalRef
	server     *rpc.Server
	checkpoint driver.WorkflowCheckpointStore
	namespace  string
	serial     int
	frames     []json.RawMessage
}

var b15NativeWriteClock = time.Date(2026, 11, 11, 13, 29, 0, 0, time.UTC)

func b15BindNativeClient(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report, actor model.PrincipalRef) *b15NativeClient {
	t.Helper()
	if report.SourcePlanes == nil || len(report.SourcePlanes.Providers) != 1 || report.Receipt == nil {
		t.Fatal("B15 actual admitted source provider or original import receipt is missing")
	}
	meta, err := store.Meta(ctx)
	publicProofError(t, "B15-bind-current-meta", err)
	checkpoints, err := driver.RequireWorkflowCheckpointStore(store, lease)
	publicProofError(t, "B15-bind-checkpoint-capability", err)
	state, err := pluginhost.NewSourceRuntimeState(ctx, checkpoints, lease, report.SourcePlanes.Providers[0])
	publicProofError(t, "B15-bind-source-state", err)
	marker, ok := state.(interface{ Persistent() bool })
	wantLifetime := driver.CheckpointLifetimeRestart
	if os.Getenv("PLUG_PUBLIC_PROOF_DRIVER") == "memory" {
		wantLifetime = driver.CheckpointLifetimeProcess
	}
	if !ok || marker.Persistent() != (wantLifetime == driver.CheckpointLifetimeRestart) || checkpoints.WorkflowCheckpointLifetime() != wantLifetime {
		t.Fatal("B15 actual source checkpoint lifetime/persistence differs from the selected driver")
	}
	provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
	publicProofError(t, "B15-bind-source-provider", err)
	provider = provider.WithClock(func() time.Time { return b15NativeWriteClock })
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithClock(func() time.Time { return b15NativeWriteClock }), pipeline.WithOntology(ontology), pipeline.WithAuthorizer(&sourcePublicPolicy{}))
	domain := service.New(store, lease, writer, service.WithClock(func() time.Time { return b15NativeWriteClock }), service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	publicProofError(t, "B15-bind-registry", err)
	publicProofError(t, "B15-bind-public-service", rpc.BindService(registry, domain))
	publicProofError(t, "B15-install-actual-source-provider", domain.InstallSourceWorkflow(ctx, provider, profile, *report.SourcePlanes, *report.Receipt))
	plan, err := profile.SourcePlanes.Plan()
	publicProofError(t, "B15-bound-source-plan", err)
	if len(plan.Entities) == 0 {
		t.Fatal("B15 actual source plan has no public entities")
	}
	project := plan.Entities[0].Source.Project
	server, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, AuthMode: "authenticated", CursorKey: bytes.Repeat([]byte{0x15}, 32)})
	publicProofError(t, "B15-bind-public-server", err)
	return &b15NativeClient{t: t, ctx: ctx, store: store, project: project, actor: actor, server: server, checkpoint: checkpoints, namespace: report.SourcePlanes.Providers[0].Provider.Namespace}
}

func (c *b15NativeClient) call(method string, params map[string]any, write bool, wantCode string) json.RawMessage {
	c.t.Helper()
	before := b15JSON(c.t, params)
	copyParams := make(map[string]any, len(params)+1)
	for key, value := range params {
		copyParams[key] = value
	}
	c.serial++
	meta := map[string]any{"project": c.project, "agent": "B15-native-comments"}
	if write {
		meta["idempotencyKey"] = hashBytes([]byte(fmt.Sprintf("B15-native-comment/%s/%d/%s", os.Getenv("PLUG_PUBLIC_PROOF_DRIVER"), c.serial, before)))
	}
	copyParams["_meta"] = meta
	request := b15JSON(c.t, map[string]any{"jsonrpc": "2.0", "id": c.serial, "method": method, "params": copyParams})
	raw := c.server.Dispatch(rpc.WithPrincipal(c.ctx, c.actor), request)
	c.frames = append(c.frames, b15JSON(c.t, map[string]json.RawMessage{"request": request, "response": raw}))
	if !bytes.Equal(before, b15JSON(c.t, params)) {
		c.t.Fatal("B15 actual public source request map was mutated")
	}
	var reply sourcePublicReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		c.t.Fatal(err)
	}
	if wantCode == "" {
		if reply.Error != nil {
			c.t.Fatalf("B15 %s refused: %s", method, raw)
		}
	} else if reply.Error == nil || reply.Error.Data.Code != wantCode || len(reply.Result) != 0 {
		c.t.Fatalf("B15 %s wants %s: %s", method, wantCode, raw)
	}
	return reply.Result
}

type b15NativeThread struct {
	Table, SourceID, TargetID string
	Active                    bool
	Actor                     model.PrincipalRef
	Expected                  []pipeline.Comment
}

func b15NativeThreads(t *testing.T, req publicProofRequest, profile driver.RestoreImportProfile, expected []b15NativeComment) []*b15NativeThread {
	t.Helper()
	plan, err := profile.SourcePlanes.Plan()
	publicProofError(t, "B15-thread-plan", err)
	when, err := time.Parse(time.RFC3339Nano, req.RestoreTime)
	publicProofError(t, "B15-independent-import-time", err)
	groups := map[driver.SourceEntityKey][]b15NativeComment{}
	for _, tuple := range expected {
		plane := dumpformat.DurableEntityTable
		if tuple.Table == dumpformat.EphemeralCommentTable {
			plane = dumpformat.EphemeralEntityTable
		}
		key := driver.SourceEntityKey{Project: tuple.Project, Plane: plane, ID: tuple.Fields["issue_id"].(string)}
		groups[key] = append(groups[key], tuple)
	}
	var out []*b15NativeThread
	large := map[bool]bool{}
	for source, tuples := range groups {
		active := source.Plane == dumpformat.EphemeralEntityTable
		var entity *driver.SourcePlaneEntityV1
		for i := range plan.Entities {
			if plan.Entities[i].Source == source {
				if entity != nil {
					t.Fatal("B15 repeated explicit source comment target")
				}
				entity = &plan.Entities[i]
			}
		}
		wantKind := "bead"
		if active {
			wantKind = "workflow:item"
		}
		if entity == nil || entity.TargetKind != wantKind || entity.TargetID == "" {
			t.Fatal("B15 requires an explicitly installed target on each actual source comment plane", source)
		}
		thread := &b15NativeThread{Table: tuples[0].Table, SourceID: source.ID, TargetID: entity.TargetID, Active: active}
		principals := 0
		for _, principal := range plan.Principals {
			if principal.Key == entity.PrincipalKey {
				publicProofError(t, "B15-explicit-source-owner", json.Unmarshal(principal.PrincipalJSON, &thread.Actor))
				principals++
			}
		}
		if principals != 1 {
			t.Fatal("B15 source target has no unique declared caller")
		}
		// Durable import appends the canonical logical comment records in native
		// ID order. The active origin adapter orders by source time then native
		// ID. The public clock is IMPORT time in both cases; sourceTime remains
		// independent provenance. Public IDs are stored ordinals, never native IDs.
		sort.Slice(tuples, func(i, j int) bool {
			if active {
				a, _ := time.Parse(time.RFC3339Nano, tuples[i].Fields["created_at"].(string))
				b, _ := time.Parse(time.RFC3339Nano, tuples[j].Fields["created_at"].(string))
				if !a.Equal(b) {
					return a.Before(b)
				}
			}
			return tuples[i].ID < tuples[j].ID
		})
		for _, tuple := range tuples {
			at, err := time.Parse(time.RFC3339Nano, tuple.Fields["created_at"].(string))
			publicProofError(t, "B15-independent-native-time", err)
			at = at.UTC()
			thread.Expected = append(thread.Expected, pipeline.Comment{Body: tuple.Fields["text"].(string), Principal: profile.Admission.Actor, Time: when.UTC(), Origin: "import", SourceAuthor: strings.TrimSpace(tuple.Fields["author"].(string)), SourceTime: &at})
		}
		large[active] = large[active] || len(tuples) > 500
		out = append(out, thread)
	}
	if !large[false] || !large[true] {
		t.Fatal("B15 actual native input must cross 100/500 boundaries on both planes")
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].SourceID < out[j].SourceID
	})
	return out
}

type b15NativePageItem struct {
	ID      uint64           `json:"id"`
	Comment pipeline.Comment `json:"comment"`
}

func (c *b15NativeClient) assertThread(thread *b15NativeThread) {
	c.t.Helper()
	before := b15NativeState(c.t, c.ctx, c.store, []*b15NativeThread{thread})
	checkpoint, err := c.checkpoint.LoadCheckpoint(c.ctx, c.namespace)
	publicProofError(c.t, "B15-page-checkpoint-before", err)
	var full struct {
		Comments []pipeline.Comment `json:"comments"`
	}
	publicProofError(c.t, "B15-public-full-thread", json.Unmarshal(c.call("bead.get", map[string]any{"id": model.RefByID(thread.SourceID, model.Natural64(0))}, false, ""), &full))
	if !reflect.DeepEqual(b15Multiset(c.t, full.Comments), b15Multiset(c.t, thread.Expected)) {
		c.t.Fatal("B15 public imported/reopened principal/body/time/origin/source-author/source-time multiset differs", thread.Table, thread.SourceID)
	}
	want := make([]b15NativePageItem, len(thread.Expected))
	for i, comment := range thread.Expected {
		want[i] = b15NativePageItem{ID: uint64(i + 1), Comment: comment}
	}
	sort.Slice(want, func(i, j int) bool {
		if !want[i].Comment.Time.Equal(want[j].Comment.Time) {
			return want[i].Comment.Time.Before(want[j].Comment.Time)
		}
		return want[i].ID < want[j].ID
	})
	for _, limit := range []int{0, 500} {
		var walked []b15NativePageItem
		cursor := ""
		for {
			options := map[string]any{}
			if limit != 0 {
				options["limit"] = limit
			}
			if cursor != "" {
				options["cursor"] = cursor
			}
			var reply struct {
				Page struct {
					Comments []b15NativePageItem `json:"comments"`
					Next     string              `json:"nextCursor"`
				} `json:"commentPage"`
			}
			publicProofError(c.t, "B15-public-page", json.Unmarshal(c.call("bead.get", map[string]any{"id": model.RefByID(thread.SourceID, model.Natural64(0)), "commentPage": options}, false, ""), &reply))
			page := reply.Page
			size := limit
			if size == 0 {
				size = 100
			}
			end := len(walked) + size
			if end > len(want) {
				end = len(want)
			}
			if !reflect.DeepEqual(page.Comments, want[len(walked):end]) || (page.Next != "") != (end < len(want)) {
				c.t.Fatal("B15 actual imported page changed exact ordinal/time tuple order, size or continuation", thread.Table, thread.SourceID, limit)
			}
			walked = append(walked, page.Comments...)
			if page.Next == "" {
				break
			}
			if page.Next == cursor || len(page.Comments) == 0 {
				c.t.Fatal("B15 actual imported public keyset walk stalled")
			}
			cursor = page.Next
		}
		if !reflect.DeepEqual(walked, want) {
			c.t.Fatal("B15 actual imported keyset walk lost or duplicated a tuple")
		}
	}
	c.call("bead.get", map[string]any{"id": model.RefByID(thread.SourceID, model.Natural64(0)), "commentPage": map[string]any{"limit": 501}}, false, "invalid_params")
	afterCheckpoint, err := c.checkpoint.LoadCheckpoint(c.ctx, c.namespace)
	publicProofError(c.t, "B15-page-checkpoint-after", err)
	if !reflect.DeepEqual(before, b15NativeState(c.t, c.ctx, c.store, []*b15NativeThread{thread})) || !reflect.DeepEqual(checkpoint, afterCheckpoint) {
		c.t.Fatal("B15 actual full/page/refused-limit reads mutated current head, anchor, history or source checkpoint")
	}
}

type b15NativeConservation struct {
	Meta    driver.Meta
	History []model.Changeset
	Anchors map[string]pipeline.Document
}

func b15NativeState(t *testing.T, ctx context.Context, store driver.Store, threads []*b15NativeThread) b15NativeConservation {
	t.Helper()
	meta, err := store.Meta(ctx)
	publicProofError(t, "B15-conservation-meta", err)
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	publicProofError(t, "B15-conservation-reader", err)
	defer reader.Close()
	history, err := reader.Changesets(ctx, nil, 10000)
	publicProofError(t, "B15-conservation-history", err)
	if len(history) == 10000 {
		t.Fatal("B15 native fixture history exceeded the complete conservation bound")
	}
	out := b15NativeConservation{Meta: meta, History: history, Anchors: map[string]pipeline.Document{}}
	for _, thread := range threads {
		if thread.Active {
			continue
		}
		rows, err := reader.Get(ctx, []model.Ref{model.RefByID(thread.TargetID, model.Natural64(0))})
		publicProofError(t, "B15-conservation-anchor", err)
		if len(rows) != 1 || rows[0].Kind != "bead" {
			t.Fatal("B15 imported durable comment anchor is missing")
		}
		var doc pipeline.Document
		publicProofError(t, "B15-conservation-anchor-document", json.Unmarshal(rows[0].Body, &doc))
		out.Anchors[thread.TargetID] = doc
	}
	return out
}

func (c *b15NativeClient) appendComment(thread *b15NativeThread, body string, all []*b15NativeThread) {
	c.t.Helper()
	before := b15NativeState(c.t, c.ctx, c.store, all)
	var newest time.Time
	for _, comment := range thread.Expected {
		at := comment.Time
		if thread.Active && comment.Origin == "import" {
			at = *comment.SourceTime
		}
		if at.After(newest) {
			newest = at
		}
	}
	at := b15NativeWriteClock
	if !newest.Before(at) {
		at = newest.Add(time.Second)
	}
	c.call("bead.comment", map[string]any{"id": model.RefByID(thread.SourceID, model.Natural64(0)), "body": body}, true, "")
	thread.Expected = append(thread.Expected, pipeline.Comment{Body: body, Principal: c.actor, Time: at})
	after := b15NativeState(c.t, c.ctx, c.store, all)
	if thread.Active {
		if !reflect.DeepEqual(before, after) {
			c.t.Fatal("B15 active public comment changed durable head, history or any durable issue anchor")
		}
	} else {
		if len(after.History) != len(before.History)+1 || !reflect.DeepEqual(after.History[:len(before.History)], before.History) {
			c.t.Fatal("B15 durable public comment did not append exactly one history entry")
		}
		entry := after.History[len(before.History)]
		if len(entry.Changes) != 1 || entry.Changes[0].Op != model.OpComment || entry.Changes[0].Ref.ID != thread.TargetID || !reflect.DeepEqual(entry.Changes[0].ChangedPaths, []string{"/comments"}) || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadCommented {
			c.t.Fatal("B15 durable public comment changed its history/event identity or multiplicity")
		}
		for id, previous := range before.Anchors {
			current := after.Anchors[id]
			if id == thread.TargetID {
				previous.Comments, current.Comments = nil, nil
			}
			if !reflect.DeepEqual(previous, current) {
				c.t.Fatal("B15 durable public comment mutated a non-comment issue member or another issue")
			}
		}
	}
	c.assertThread(thread)
}

func (c *b15NativeClient) assertRefusals(thread *b15NativeThread, all []*b15NativeThread) {
	c.t.Helper()
	before := b15NativeState(c.t, c.ctx, c.store, all)
	checkpoint, err := c.checkpoint.LoadCheckpoint(c.ctx, c.namespace)
	publicProofError(c.t, "B15-refusal-checkpoint-before", err)
	prefix := thread.SourceID[:len(thread.SourceID)-1]
	for _, other := range all {
		if other.SourceID == prefix {
			c.t.Fatal("B15 fixture prefix refusal would address an existing full identity")
		}
	}
	for _, test := range []struct{ ID, Body, Code string }{
		{thread.SourceID, "", "invalid_params"},
		{thread.SourceID, " \t\n", "invalid_params"},
		{"", "body", "invalid_params"},
		{" \t", "body", "not_found"},
		{c.project + "-b15-missing-comment-anchor", "body", "not_found"},
		{prefix, "body", "not_found"},
	} {
		c.call("bead.comment", map[string]any{"id": map[string]any{"id": test.ID}, "body": test.Body}, true, test.Code)
	}
	for _, test := range []struct{ ID, Code string }{{"", "invalid_params"}, {" \t", "not_found"}, {c.project + "-b15-missing-comment-anchor", "not_found"}, {prefix, "not_found"}} {
		c.call("bead.get", map[string]any{"id": map[string]any{"id": test.ID}, "commentPage": map[string]any{}}, false, test.Code)
	}
	afterCheckpoint, err := c.checkpoint.LoadCheckpoint(c.ctx, c.namespace)
	publicProofError(c.t, "B15-refusal-checkpoint-after", err)
	if !reflect.DeepEqual(before, b15NativeState(c.t, c.ctx, c.store, all)) || !reflect.DeepEqual(checkpoint, afterCheckpoint) {
		c.t.Fatal("B15 refused public source comment/read changed any durable or source state")
	}
}

// Each pair of callbacks belongs to one actual capture and one driver run.
// The closure retains detached expectations/readback only, never live handles.
func b15NativeContinuations(req publicProofRequest) (publicProofContinuation, publicProofContinuation) {
	var threads []*b15NativeThread
	var committed b15NativeConservation
	writer := func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
		expected := b15ReadNativeComments(t, req)
		publicProofJSON(t, filepath.Join(os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT"), "B15-independent-native-comments.json"), expected)
		b15AssertSourceCommentRecords(t, report.Archive, req, expected)
		b15AssertStoredNativeComments(t, ctx, store, req, expected)
		threads = b15NativeThreads(t, req, profile, expected)
		var clients []*b15NativeClient
		defer func() {
			var frames []json.RawMessage
			for _, client := range clients {
				frames = append(frames, client.frames...)
			}
			publicProofJSON(t, filepath.Join(os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT"), "B15-writer-public-frames.json"), frames)
		}()
		for _, thread := range threads {
			client := b15BindNativeClient(t, ctx, store, lease, profile, report, thread.Actor)
			clients = append(clients, client)
			client.assertThread(thread)
			client.assertRefusals(thread, threads)
			for _, body := range []string{" \t**literal**\n東京 e\u0301  ", "same operation clock", "already future newest"} {
				client.appendComment(thread, body, threads)
			}
		}
		committed = b15NativeState(t, ctx, store, threads)
		b15AssertStoredNativeComments(t, ctx, store, req, expected)
		publicProofJSON(t, filepath.Join(os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT"), "B15-committed-state.json"), committed)
		t.Logf("B15 writer assertions reached: project=%s driver=%s native_tuples=%d threads=%d live_appends=12", req.Source.Project, os.Getenv("PLUG_PUBLIC_PROOF_DRIVER"), len(expected), len(threads))
		return nil
	}
	assertion := func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
		if len(threads) == 0 || !reflect.DeepEqual(committed, b15NativeState(t, ctx, store, threads)) {
			t.Fatal("B15 actual shared reopen changed exact CURRENT committed identity, anchor or history")
		}
		expected := b15ReadNativeComments(t, req)
		b15AssertSourceCommentRecords(t, report.Archive, req, expected)
		b15AssertStoredNativeComments(t, ctx, store, req, expected)
		var clients []*b15NativeClient
		defer func() {
			var frames []json.RawMessage
			for _, client := range clients {
				frames = append(frames, client.frames...)
			}
			publicProofJSON(t, filepath.Join(os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT"), "B15-reopened-public-frames.json"), frames)
		}()
		for _, thread := range threads {
			client := b15BindNativeClient(t, ctx, store, lease, profile, report, thread.Actor)
			clients = append(clients, client)
			client.assertThread(thread)
		}
		if !reflect.DeepEqual(committed, b15NativeState(t, ctx, store, threads)) {
			t.Fatal("B15 read-only post-reopen assertion changed current target state")
		}
		t.Logf("B15 post-writer reopen assertions reached: project=%s driver=%s native_tuples=%d threads=%d", req.Source.Project, os.Getenv("PLUG_PUBLIC_PROOF_DRIVER"), len(expected), len(threads))
		return nil
	}
	return writer, assertion
}

// Explicit acceptance-owned host intent for the genuine single-project source.
// Only active comment origins become live auxiliary state here. Every other
// family keeps its complete existing disposition; this is not graph/label/
// counter/event qualification and does not change the receiver lifecycle.
func b15NativeHostPlan(t *testing.T, ctx context.Context, archive *dumpformat.Archive, plan driver.SourcePlanePlanV1) (driver.SourcePlanePlanV1, error) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		return driver.SourcePlanePlanV1{}, err
	}
	manifest := archive.Manifest()
	if len(manifest.Projects) != 1 || len(plan.Principals) != 1 || len(plan.Providers) != 1 || len(plan.Runs) != 0 {
		t.Fatal("B15 actual native host plan requires one explicit source project, caller and provider")
	}
	project := manifest.Projects[0].Prefix
	// Imported native authors never authorize a caller. This is the explicit
	// disposable host principal, separate from both source authors and importer.
	plan.Principals[0].Source = "B15 explicitly authorized public fixture caller"
	plan.Principals[0].PrincipalJSON = b15JSON(t, model.PrincipalRef{Kind: "user", ID: "B15-native-comment-writer"})
	raw, err := archive.SourcePlaneRecords()
	publicProofError(t, "B15-host-original-source-records", err)
	byRecord := map[string]dumpformat.Record{}
	active := map[string]driver.SourceEntityKey{}
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	// Retain the actual admitted source status names. Their categories remain
	// in the authenticated source catalog and the receiver's installed ontology.
	// Host intent does not invent a smaller open/closed status vocabulary.
	var statuses []driver.SourcePlaneStringPairV1
	statusNames := map[string]bool{}
	for _, status := range manifest.Projects[0].Statuses {
		if status.Name == "" || statusNames[status.Name] {
			t.Fatal("B15 actual source catalog has an empty or repeated status")
		}
		statusNames[status.Name] = true
		statuses = append(statuses, driver.SourcePlaneStringPairV1{From: status.Name, To: status.Name})
	}
	if len(statuses) == 0 {
		t.Fatal("B15 actual source catalog has no status dispositions")
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].From < statuses[j].From })
	run := driver.SourcePlaneRunV1{Project: project, Key: "B15-native-comments", TargetID: "workflow:run/b15-native-" + project, Template: "explicit B15 source host intent", Schema: workflowplugin.SourceStateSchema, Mode: "ephemeral", State: "active", PrincipalKey: plan.Principals[0].Key}
	flag := func(value any) bool {
		switch value := value.(type) {
		case bool:
			return value
		case json.Number:
			if value == "0" || value == "1" {
				return value == "1"
			}
		}
		t.Fatal("B15 actual source classification flag is not an exact native boolean")
		return false
	}
	for _, record := range raw {
		var row dumpformat.Record
		decode(t, record.Raw, &row)
		byRecord[record.Table+"/"+record.ID] = row
		if record.Table != dumpformat.EphemeralEntityTable {
			continue
		}
		id, ok := row.Source.Fields["id"].(string)
		if !ok || !strings.HasPrefix(id, project+"-") {
			t.Fatal("B15 actual active source identity is outside its declared project")
		}
		key := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: id}
		if _, exists := active[id]; exists {
			t.Fatal("B15 actual active source repeats an identity")
		}
		active[id] = key
		fields := row.Source.Fields
		typ, typeOK := fields["issue_type"].(string)
		storageValue, storagePresent := fields["storage_class"]
		var storage string
		if storageValue != nil {
			var storageOK bool
			storage, storageOK = storageValue.(string)
			if !storageOK || storage == "" {
				t.Fatal("B15 actual active source storage class is not exact nonempty text or SQL NULL")
			}
		}
		// The frozen V1 contract uses the empty disposition string only for
		// a present SQL NULL. Preserve that distinction from missing/empty text.
		if !typeOK || typ == "" || !storagePresent {
			t.Fatal("B15 actual active source classification is missing")
		}
		status, statusOK := fields["status"].(string)
		if !statusOK || !statusNames[status] {
			t.Fatal("B15 actual active source status has no admitted catalog entry")
		}
		disposition := driver.SourcePlaneDispositionV1{SourceType: typ, StorageClass: storage, Ephemeral: flag(fields["ephemeral"]), NoHistory: flag(fields["no_history"]), SinkKind: "workflow:item", Schema: workflowplugin.SourceStateSchema, Lifetime: string(plan.Target.Lifetime), Statuses: append([]driver.SourcePlaneStringPairV1(nil), statuses...), RefusalPolicy: "exact-only"}
		disposition.Key = "B15-active-" + hashBytes(b15JSON(t, []any{typ, storage, disposition.Ephemeral, disposition.NoHistory}))
		census := jointFields(t, record)
		for _, field := range census {
			rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
			switch field.Name {
			case "id", "title", "status", "metadata", "created_at":
				rule.Target, rule.Disposition = field.Name, "exact-map"
			case "issue_type":
				rule.Target, rule.Disposition = "type", "exact-map"
			}
			disposition.Fields = append(disposition.Fields, rule)
		}
		if prior, exists := dispositions[disposition.Key]; exists && !reflect.DeepEqual(prior, disposition) {
			t.Fatal("B15 repeated classification has a different field disposition")
		}
		dispositions[disposition.Key] = disposition
		target := "workflow:item/" + hashBytes(b15JSON(t, key))[:32]
		plan.Entities = append(plan.Entities, driver.SourcePlaneEntityV1{Source: key, TargetKind: "workflow:item", TargetID: target, DispositionKey: disposition.Key, PrincipalKey: plan.Principals[0].Key, RunKey: run.Key, Route: driver.SourcePlaneRouteV1{Project: project, SourceID: id, ActiveTargetID: target, Preference: "active-first"}, Origin: jointCommitment(record.Raw, "object"), Fields: census})
		run.Members = append(run.Members, key)
	}
	if len(active) == 0 {
		t.Fatal("B15 actual native source has no active entities")
	}
	var origins []driver.SourcePlaneOriginV1
	for _, origin := range plan.Origins {
		if origin.Table == dumpformat.EphemeralEntityTable {
			row, ok := byRecord[origin.Table+"/"+origin.ID]
			if !ok || active[stringValue(row.Source.Fields["id"])].ID == "" {
				t.Fatal("B15 active origin lacks its complete explicit entity")
			}
			continue // Its original bytes/field census are now on the entity.
		}
		if origin.Table == dumpformat.EphemeralCommentTable {
			row, ok := byRecord[origin.Table+"/"+origin.ID]
			owner, found := active[stringValue(row.Source.Fields["issue_id"])]
			if !ok || !found {
				t.Fatal("B15 actual active comment lacks its exact source-plane owner")
			}
			origin.Project, origin.Owner, origin.Action = project, &owner, "exact-map"
		}
		origins = append(origins, origin)
	}
	plan.Origins = origins
	sort.Slice(plan.Origins, func(i, j int) bool {
		a, b := plan.Origins[i], plan.Origins[j]
		left, right := []string{a.Project, a.Table, a.ID, a.Path, a.Layer}, []string{b.Project, b.Table, b.ID, b.Path, b.Layer}
		for field := range left {
			if left[field] != right[field] {
				return left[field] < right[field]
			}
		}
		return false
	})
	for _, disposition := range dispositions {
		plan.Dispositions = append(plan.Dispositions, disposition)
	}
	sort.Slice(plan.Dispositions, func(i, j int) bool { return plan.Dispositions[i].Key < plan.Dispositions[j].Key })
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
	})
	sort.Slice(run.Members, func(i, j int) bool { return driver.CompareSourceEntityKeys(run.Members[i], run.Members[j]) < 0 })
	plan.Runs = append(plan.Runs, run)
	return plan, nil
}

// B15 consumes one authenticated project from the final native r11 producer
// packet per lifecycle invocation. The shared receiver owns import, lease,
// reopen and cleanup; this test owns only the source disposition and its
// read-only/live comment assertions. The committed packet mode runs the same
// lifecycle separately for both source projects and all three target drivers.
func TestB15NativeCommentImportedReopen(t *testing.T) {
	publicProofExternalRoot(t, "PLUG_PUBLIC_PROOF_RUN")
	packetRoot := b15MaterializeNativePacket(t)
	// Bind the actual compiled ELF before entering the shared callback. A
	// caller-supplied digest must agree with it.
	b15BindNativeExecutable(t)
	if os.Getenv(b15NativeProjectsEnv) == "" || os.Getenv(b15NativeDriversEnv) == "" {
		t.Fatal("B15 committed packet mode requires explicit project and driver lists")
	}
	outputRoot := os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT")
	if outputRoot == "" {
		artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
		if !filepath.IsAbs(artifacts) {
			t.Fatal("B15 needs an absolute output root or Bazel artifact directory")
		}
		outputRoot = filepath.Join(artifacts, "B15-native")
	}
	if !filepath.IsAbs(outputRoot) {
		t.Fatal("B15 committed packet mode requires an absolute output root")
	}
	for _, project := range b15NativeList(t, b15NativeProjectsEnv, "") {
		if _, ok := b15NativeSourceBindings[project]; !ok {
			t.Fatalf("B15 packet project is not admitted: %s", project)
		}
		for _, engine := range b15NativeList(t, b15NativeDriversEnv, "") {
			if engine != "memory" && engine != "sqlite" && engine != "postgres" {
				t.Fatalf("B15 target driver is not admitted: %s", engine)
			}
			t.Run(project+"/"+engine, func(t *testing.T) {
				request, binding := b15PrepareNativeRequest(t, packetRoot, project)
				resolvedHash := publicProofHashFile(t, request)
				output := filepath.Join(outputRoot, project, engine)
				if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
					t.Fatal(err)
				}
				for key, value := range map[string]string{
					"PLUG_PUBLIC_PROOF_DRIVER":         engine,
					"PLUG_PUBLIC_PROOF_REQUEST":        request,
					"PLUG_PUBLIC_PROOF_REQUEST_SHA256": resolvedHash,
					"PLUG_PUBLIC_PROOF_OUTPUT":         output,
				} {
					t.Setenv(key, value)
				}
				req := b15NativeRequest(t)
				bindings := map[string]any{
					"project":                 project,
					"driver":                  engine,
					"packet_sha256":           b15NativePacketSHA256,
					"consumer_oracle_sha256":  b15NativeOracleSHA256,
					"original_request_sha256": binding.RequestSHA256,
					"resolved_request":        request,
					"resolved_request_sha256": resolvedHash,
					"capture":                 req.Capture,
					"capture_sha256":          req.CaptureSHA256,
					"oracle":                  req.Oracle,
					"ledger_sha256":           req.LedgerSHA256,
					"states_sha256":           req.StatesSHA256,
					"elf_sha256":              os.Getenv("PLUG_PUBLIC_PROOF_ELF_SHA256"),
					"output":                  output,
				}
				t.Logf("B15 native invocation: %s", b15JSON(t, bindings))
				t.Cleanup(func() {
					if publicProofHashFile(t, request) != resolvedHash || publicProofHashFile(t, filepath.Join(packetRoot, project, "request.full.json")) != binding.RequestSHA256 || publicProofHashFile(t, filepath.Join(req.Oracle, "tables.ndjson")) != binding.ConsumerLedgerSHA256 || publicProofHashFile(t, filepath.Join(req.Oracle, "states.ndjson")) != binding.StatesSHA256 {
						t.Fatal("B15 lifecycle mutated its request or independent oracle")
					}
				})
				writer, afterReopen := b15NativeContinuations(req)
				runPublicNativeExternalCaptureWithHostPlan(t, b15NativeHostPlan, writer, afterReopen)
				publicProofJSON(t, filepath.Join(output, "B15-input-bindings.json"), bindings)
			})
		}
	}
}
