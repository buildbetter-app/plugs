package s1b25b28_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func sourceFile(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func exporterArchive(t *testing.T) []byte {
	t.Helper()
	data := sourceFile(t, "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	if got := digest(data); got != "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c" {
		t.Fatalf("admitted exporter artifact changed: %s", got)
	}
	return data
}

type target struct {
	store driver.Store
	path  string
}

func openTarget(t *testing.T) *target {
	t.Helper()
	f := &target{path: filepath.Join(t.TempDir(), "target.sqlite")}
	f.reopen(t)
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *target) reopen(t *testing.T) {
	t.Helper()
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	f.store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: f.path})
	if err != nil {
		t.Fatal(err)
	}
}

// Read every persisted table, including replay receipts, derived rows, source
// metadata, versions, blobs and lease state. Values keep their SQL types; row
// order is not a storage contract, so only that order is canonicalized.
func (f *target) durableImage(t *testing.T) []byte {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+f.path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	type table struct {
		Name, Schema string
		Columns      []string
		Rows         []json.RawMessage
	}
	tables := []table{}
	for rows.Next() {
		var item table
		if err := rows.Scan(&item.Name, &item.Schema); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		t.Fatal("SQLite image contains no tables")
	}
	for i := range tables {
		item := &tables[i]
		rows, err := tx.Query(`SELECT * FROM "` + strings.ReplaceAll(item.Name, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		item.Columns, err = rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		item.Rows = []json.RawMessage{}
		for rows.Next() {
			values := make([]any, len(item.Columns))
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			item.Rows = append(item.Rows, data)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Slice(item.Rows, func(a, b int) bool { return bytes.Compare(item.Rows[a], item.Rows[b]) < 0 })
	}
	data, err := json.Marshal(tables)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f *target) assertUnchanged(t *testing.T, before []byte) {
	t.Helper()
	retain(t, "before.json", before)
	after := f.durableImage(t)
	retain(t, "after.json", after)
	if !bytes.Equal(before, after) {
		t.Fatalf("refusal changed durable SQLite image: before=%s after=%s", digest(before), digest(after))
	}
	f.reopen(t)
	after = f.durableImage(t)
	retain(t, "reopened.json", after)
	if !bytes.Equal(before, after) {
		t.Fatalf("reopen changed refused target: before=%s after=%s", digest(before), digest(after))
	}
	t.Logf("all SQLite tables unchanged after refusal and reopen: %s", digest(before))
}

func retain(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" {
		t.Fatal("Bazel test output directory is required")
	}
	path := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+"-"+name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRecordedJSONLIsRefusedAtPublicConverter(t *testing.T) {
	for _, run := range []string{"run1", "run2"} {
		t.Run(run, func(t *testing.T) {
			data := sourceFile(t, "spec/s1-b25-b28/testdata/"+run+"/source-export.jsonl")
			// Valid current-state interchange is recorded by the real reference
			// exporter. P5c/P44 adopts an archive boundary instead of JSONL append.
			if len(decodeRows(t, data)) != 2 {
				t.Fatal("reference did not export both source rows")
			}
			f := openTarget(t)
			before := f.durableImage(t)
			report, err := converterroundtrip.Restore(context.Background(), bytes.NewReader(data), f.store)
			if err == nil || err.Error() != "invalid tar length or end blocks" || report.Archive != nil {
				t.Fatalf("public converter accepted nonarchive interchange: %+v %v", report, err)
			}
			f.assertUnchanged(t, before)
		})
	}
}

func TestExporterHistoryProfileRefusesBeforeWrite(t *testing.T) {
	f := openTarget(t)
	before := f.durableImage(t)
	report, err := converterroundtrip.Restore(context.Background(), bytes.NewReader(exporterArchive(t)), f.store)
	if !errors.Is(err, converterroundtrip.ErrRestoreUnavailable) || !strings.HasSuffix(err.Error(), ": unverified source history cannot be restored") {
		t.Fatalf("unsupported profile error: %v", err)
	}
	if report.Archive == nil || report.Archive.Native() || report.Archive.Manifest().History != "available-unverified" || report.Archive.Manifest().Reference != referenceCommit || report.Archive.Manifest().Scope != dumpformat.Scope {
		t.Fatalf("refusal lost actual exporter provenance: %+v", report)
	}
	if report.Restored.StoreID != "" || report.IdempotencyKey != "" || report.Recovered {
		t.Fatal("unsupported exporter profile was represented as a commit")
	}
	f.assertUnchanged(t, before)
}

// Mutate only the selected producer member and its transport checksum. This
// constructs negative inputs, never a replacement success archive or golden.
func mutateArchive(t *testing.T, member string, change func(map[string]json.RawMessage)) []byte {
	t.Helper()
	input := tar.NewReader(bytes.NewReader(exporterArchive(t)))
	members := map[string][]byte{}
	order := []string{}
	for {
		header, err := input.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(input)
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name] = data
		order = append(order, header.Name)
	}
	var value map[string]json.RawMessage
	var err error
	if member == "config/sources.json" {
		var entries []map[string]json.RawMessage
		if err := json.Unmarshal(members[member], &entries); err != nil || len(entries) == 0 {
			t.Fatalf("configuration input: %v", err)
		}
		change(entries[0])
		members[member], err = json.Marshal(entries)
	} else {
		if err := json.Unmarshal(members[member], &value); err != nil {
			t.Fatal(err)
		}
		change(value)
		members[member], err = json.Marshal(value)
	}
	if err != nil {
		t.Fatal(err)
	}
	if member != "manifest.json" {
		var manifest dumpformat.Manifest
		if err := json.Unmarshal(members["manifest.json"], &manifest); err != nil {
			t.Fatal(err)
		}
		manifest.Files[member] = dumpformat.File{SHA256: digest(members[member]), Size: int64(len(members[member]))}
		members["manifest.json"], err = json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, name := range order {
		data := members[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

// mutateBeadStorageClass constructs only a negative archive input. The
// source record is otherwise unchanged and its transport checksum is updated,
// so dumpformat must reach the typed storage-class guard rather than failing
// on a stale manifest. No Plug producer is substituted for this source row.
func mutateBeadStorageClass(t *testing.T, class string) []byte {
	t.Helper()
	input := tar.NewReader(bytes.NewReader(exporterArchive(t)))
	members := map[string][]byte{}
	order := []string{}
	for {
		header, err := input.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(input)
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name] = data
		order = append(order, header.Name)
	}
	lines := bytes.Split(bytes.TrimSpace(members["records/bead.ndjson"]), []byte{'\n'})
	if len(lines) == 0 {
		t.Fatal("reference archive contains no bead rows")
	}
	var row map[string]json.RawMessage
	if err := json.Unmarshal(lines[0], &row); err != nil {
		t.Fatal(err)
	}
	var source map[string]json.RawMessage
	if err := json.Unmarshal(row["source"], &source); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(source["fields"], &fields); err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(class)
	if err != nil {
		t.Fatal(err)
	}
	fields["storage_class"] = value
	source["fields"], err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	row["source"], err = json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	lines[0], err = json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	members["records/bead.ndjson"] = append(bytes.Join(lines, []byte{'\n'}), '\n')
	var manifest dumpformat.Manifest
	if err := json.Unmarshal(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Files["records/bead.ndjson"] = dumpformat.File{SHA256: digest(members["records/bead.ndjson"]), Size: int64(len(members["records/bead.ndjson"]))}
	members["manifest.json"], err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, name := range order {
		data := members[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestB26EphemeralArchiveIsExplicitlyRefused(t *testing.T) {
	// B26 has no admitted public Plug ephemeral-plane producer. Keep that
	// boundary typed and visible: a source archive declaring an ephemeral row
	// is refused by the real dump-format validator before restore admission.
	archive, err := dumpformat.Read(bytes.NewReader(mutateBeadStorageClass(t, "ephemeral")))
	if archive != nil || err == nil || err.Error() != "unsupported storage class" {
		t.Fatalf("ephemeral source was not explicitly refused: archive=%v err=%v", archive, err)
	}
	t.Log("B26 remains unqualified: no admitted public ephemeral-plane producer; storage_class=ephemeral is refused")
}

func TestStrictManifestAndSourceIdentityRefusals(t *testing.T) {
	cases := []struct {
		name, want string
		member     string
		change     func(map[string]json.RawMessage)
	}{
		{"unknown_member", "invalid manifest: unknown typed JSON member", "manifest.json", func(m map[string]json.RawMessage) { m["unadmitted"] = json.RawMessage(`true`) }},
		{"source_pin", "unsupported manifest schema, source or scope", "manifest.json", func(m map[string]json.RawMessage) {
			m["beads_reference"] = json.RawMessage(`"0000000000000000000000000000000000000000"`)
		}},
		{"configuration_checksum", "checksum/size mismatch or missing member", "manifest.json", func(m map[string]json.RawMessage) {
			var files map[string]dumpformat.File
			if err := json.Unmarshal(m["files"], &files); err != nil {
				t.Fatal(err)
			}
			file := files["config/sources.json"]
			file.SHA256 = strings.Repeat("0", 64)
			files["config/sources.json"] = file
			data, err := json.Marshal(files)
			if err != nil {
				t.Fatal(err)
			}
			m["files"] = data
		}},
		{"configuration_unknown_member", "invalid configuration provenance: unknown typed JSON member", "config/sources.json", func(m map[string]json.RawMessage) { m["unadmitted"] = json.RawMessage(`true`) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := mutateArchive(t, tc.member, tc.change)
			f := openTarget(t)
			before := f.durableImage(t)
			report, err := converterroundtrip.Restore(context.Background(), bytes.NewReader(data), f.store)
			if err == nil || !strings.Contains(err.Error(), tc.want) || errors.Is(err, converterroundtrip.ErrRestoreUnavailable) || report.Archive != nil {
				t.Fatalf("malformed archive reached profile/write admission: %+v %v", report, err)
			}
			f.assertUnchanged(t, before)
		})
	}
}

var principal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

func seedCurrentWrite(t *testing.T, f *target) (model.Ref, *driver.IdempotencyRecord) {
	t.Helper()
	ctx := context.Background()
	lease, err := f.store.Lease(ctx, "cardb27", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	rules := service.NewOntology(nil)
	writer := pipeline.New(f.store, lease, pipeline.WithOntology(rules), pipeline.WithIDGenerator(func() (string, error) {
		return "pl-0123456789abcdef0123456789abcdef", nil
	}))
	svc := service.New(f.store, lease, writer, service.WithOntology(rules))
	commit, err := svc.Create(ctx, principal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "Retained current write", "description": "Exact durable receipt"}}, model.WriteMeta{Project: "local", Agent: "cardb27", AgentLabel: "cardb27", IdempotencyKey: strings.Repeat("a1", 16)})
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("current service write: %+v %v", commit, err)
	}
	return commit.Results[0].Ref, currentReceipt(t, f)
}

func currentReceipt(t *testing.T, f *target) *driver.IdempotencyRecord {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	key, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := reader.IdempotencyByKey(ctx, key, "cardb27", "bead.create", strings.Repeat("a1", 16))
	if err != nil || receipt == nil || receipt.ResponsePending || receipt.Commit.Revision.String() != "1" || receipt.Changeset.Kind != model.ChangesetWrite || receipt.Changeset.Revision.String() != "1" || receipt.Changeset.IdempotencyKey != strings.Repeat("a1", 16) || len(receipt.Commit.Results) != 1 || len(receipt.RequestHash) != 64 {
		t.Fatalf("actual final current-write receipt: %+v %v", receipt, err)
	}
	return receipt
}

func TestNonemptyTargetRetainsCurrentWriteAndReceipt(t *testing.T) {
	f := openTarget(t)
	ref, receipt := seedCurrentWrite(t, f)
	before := f.durableImage(t)
	report, err := converterroundtrip.Restore(context.Background(), bytes.NewReader(exporterArchive(t)), f.store)
	if !errors.Is(err, converterroundtrip.ErrTargetNotEmpty) || report.Archive == nil || report.Target.Head.String() != "1" || report.Restored.StoreID != "" {
		t.Fatalf("public converter nonempty refusal: %+v %v", report, err)
	}
	f.assertUnchanged(t, before)
	got, err := json.Marshal(currentReceipt(t, f))
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("existing receipt changed after refused conversion/reopen")
	}
	retain(t, "receipt-before.json", want)
	retain(t, "receipt-reopened.json", got)
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || !bytes.Contains(rows[0].Body, []byte("Retained current write")) {
		t.Fatalf("current record readback: %+v %v", rows, err)
	}
	t.Logf("ordinary service/Writer receipt survived public converter refusal: %s", digest(got))
}
