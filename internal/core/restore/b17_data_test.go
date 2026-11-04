package restore_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This is an authored data-fidelity fixture derived in memory from the admitted
// archive. It never changes reference/archive bytes on disk or uses setMeta.
func b17NullArchive(t *testing.T) []byte {
	t.Helper()
	p := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	original, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	files := map[string][]byte{}
	tr := tar.NewReader(bytes.NewReader(original))
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		files[h.Name], e = io.ReadAll(tr)
		if e != nil {
			t.Fatal(e)
		}
	}
	var rows []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(files["records/bead.ndjson"]), []byte{'\n'}) {
		var r map[string]any
		d := json.NewDecoder(bytes.NewReader(line))
		d.UseNumber()
		if e := d.Decode(&r); e != nil {
			t.Fatal(e)
		}
		rows = append(rows, r)
	}
	r := rows[0]
	for _, data := range []map[string]any{r["body"].(map[string]any), r["source"].(map[string]any)["fields"].(map[string]any)} {
		metadata := data["metadata"].(map[string]any)
		metadata["b17.null"] = nil
		metadata["b17.sibling"] = map[string]any{"kept": true}
	}
	var body bytes.Buffer
	for _, r := range rows {
		b, e := json.Marshal(r)
		if e != nil {
			t.Fatal(e)
		}
		body.Write(b)
		body.WriteByte('\n')
	}
	files["records/bead.ndjson"] = body.Bytes()
	var m dumpformat.Manifest
	if e := json.Unmarshal(files["manifest.json"], &m); e != nil {
		t.Fatal(e)
	}
	m.Files["records/bead.ndjson"] = dumpformat.File{SHA256: sha(body.Bytes()), Size: int64(body.Len())}
	b, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	files["manifest.json"] = append(b, '\n')
	names := []string{}
	for name := range files {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var out bytes.Buffer
	tw := tar.NewWriter(&out)
	for _, name := range append([]string{"manifest.json"}, names...) {
		data := files[name]
		if e := tw.WriteHeader(&tar.Header{Name: name, Mode: 0600, Typeflag: tar.TypeReg, Size: int64(len(data)), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); e != nil {
			t.Fatal(e)
		}
		if _, e := tw.Write(data); e != nil {
			t.Fatal(e)
		}
	}
	if e := tw.Close(); e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}

// b17SupportedOfflineArchive is a separately named, authored fixture for the
// bounded offline profile described by the admitted #104 source. It starts
// from the unchanged tracked tar, matching the independent fixture builder;
// the separate oversized fixture above retains its typed-manifest rewrite.
func b17SupportedOfflineArchive(t *testing.T) []byte {
	t.Helper()
	p := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	original, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	members := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(original))
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		members[h.Name], err = io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
	}
	const idA = "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const idB = "pl-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const idC = "pl-cccccccccccccccccccccccccccccccc"
	replace := strings.NewReplacer("demo-7.4", idB, "demo-7", idA, "demo-8", idC, `"demo"`, `"pl"`)
	for name, data := range members {
		members[name] = []byte(replace.Replace(string(data)))
	}
	var manifest map[string]any
	if err := json.Unmarshal(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest["history"] = "unavailable"
	manifest["projects"] = []any{map[string]any{
		"prefix": "pl", "id_mode": "hash", "hash_length": 32,
		"last_id": 0, "child_counters": map[string]any{},
	}}
	counts := map[string]int{}
	kinds := []any{}
	for _, rawKind := range manifest["kind_registry"].([]any) {
		kind := rawKind.(map[string]any)
		name, path := kind["name"].(string), kind["file"].(string)
		if name == "comment" {
			delete(members, path)
			delete(manifest["files"].(map[string]any), path)
			continue
		}
		var rows bytes.Buffer
		rowCount := 0
		for _, line := range bytes.Split(bytes.TrimSpace(members[path]), []byte{'\n'}) {
			var row map[string]any
			if err := json.Unmarshal(line, &row); err != nil {
				t.Fatal(err)
			}
			source := row["source"].(map[string]any)["fields"].(map[string]any)
			body := row["body"].(map[string]any)
			table := row["source"].(map[string]any)["table"].(string)
			switch table {
			case "events", "issue_versions", "child_counters", "issue_counter":
				continue
			}
			if name == "edge" {
				source["type"], body["type"] = "blocks", "blocks"
			}
			if name == "bead" && row["id"] == idA {
				for _, metadata := range []map[string]any{
					body["metadata"].(map[string]any), source["metadata"].(map[string]any),
				} {
					metadata["b17.null"] = nil
					metadata["b17.sibling"] = map[string]any{"kept": true}
				}
			}
			counts[table]++
			if name == "bead" {
				counts["labels"] += len(body["labels"].([]any))
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			rows.Write(encoded)
			rows.WriteByte('\n')
			rowCount++
		}
		members[path] = rows.Bytes()
		kind["count"] = rowCount
		kinds = append(kinds, kind)
	}
	manifest["kind_registry"] = kinds
	counts["issues.row_lock"], counts["issues.is_blocked"] = counts["issues"], counts["issues"]
	for _, raw := range manifest["sources"].([]any) {
		entry := raw.(map[string]any)
		family := entry["family"].(string)
		if _, present := counts[family]; present {
			entry["rows"] = counts[family]
			continue
		}
		switch family {
		case "comments", "events", "issue_versions", "child_counters", "issue_counter":
			entry["rows"] = 0
		}
	}
	files := manifest["files"].(map[string]any)
	for name, data := range members {
		if name != "manifest.json" {
			files[name] = map[string]any{"sha256": sha(data), "size": len(data)}
		}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members["manifest.json"] = append(encoded, '\n')
	names := []string{}
	for name := range members {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var output bytes.Buffer
	w := tar.NewWriter(&output)
	for _, name := range append([]string{"manifest.json"}, names...) {
		data := members[name]
		if err := w.WriteHeader(&tar.Header{
			Name: name, Mode: 0600, Typeflag: tar.TypeReg,
			Size: int64(len(data)), Format: tar.FormatUSTAR,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func requireB17Null(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var metadata map[string]json.RawMessage
	if e := json.Unmarshal(raw, &metadata); e != nil {
		t.Fatal(e)
	}
	v, present := metadata["b17.null"]
	if !present || string(v) != "null" || string(metadata["b17.sibling"]) != `{"kept":true}` {
		t.Fatalf("archive data lost literal null/key/sibling: %s", raw)
	}
}

func TestB17ArchiveStoredNullReverseConversionAndRestore(t *testing.T) {
	data := b17NullArchive(t)
	archive, e := dumpformat.Read(bytes.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("authored B17 archive sha256=%s independent recorded golden untouched", sha(data))
	row := archive.Records("bead")[0]
	md, _ := json.Marshal(row.Body["metadata"])
	requireB17Null(t, md)
	t.Run("AdmittedReverseConverter", func(t *testing.T) {
		dump, e := reverseconverter.ReadBytes(data)
		if e != nil {
			t.Fatal(e)
		}
		result, e := reverseconverter.Convert(dump)
		if e != nil {
			t.Fatalf("reverse converter archive data: %v", e)
		}
		for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
			var issue struct {
				ID       string          `json:"id"`
				Metadata json.RawMessage `json:"metadata"`
			}
			if e := json.Unmarshal(line, &issue); e != nil {
				t.Fatal(e)
			}
			if issue.ID == row.ID {
				requireB17Null(t, issue.Metadata)
				t.Logf("B17 reverse JSONL data=%s; no external importer/readback claimed", issue.Metadata)
				return
			}
		}
		t.Fatal("reverse converter omitted issue")
	})
	for _, name := range []string{"memory", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var store driver.Store
			var err error
			path := filepath.Join(t.TempDir(), "b17-restore.db")
			if name == "memory" {
				store = memory.New()
			} else {
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "b17-data", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			mgr := restore.NewManager(store, lease, trustedPresenceVerifier())
			manifest, hash := manifestForRestore(t, archive)
			status, err := mgr.Begin(ctx, manifest, "head", presence)
			if err != nil {
				t.Fatal(err)
			}
			uploadManagerArchive(t, ctx, mgr, status.RestoreID, data, presence)
			_, err = mgr.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, presence)
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "restore_violation" {
				t.Fatalf("oversized converter archive must refuse with typed unsupported-profile error: %T %v", err, err)
			}
			if typed.Data.Hint != "converter archive restore is unavailable; source disclosures and allocator state must be preserved by the converter boundary" {
				t.Fatalf("unsupported-profile refusal changed: %+v", typed.Data)
			}
			t.Logf("B17 original archive unsupported-profile refusal: %+v", typed.Data)
			meta, err := store.Meta(ctx)
			if err != nil || !meta.Head.IsZero() || !meta.HistoryFloor.IsZero() {
				t.Fatalf("unsupported archive refusal mutated target: meta=%+v err=%v", meta, err)
			}
			r := readStore(t, ctx, store, meta.Incarnation)
			defer r.Close()
			page, err := r.Query(ctx, driver.Query{Limit: 1})
			if err != nil || len(page.Items) != 0 {
				t.Fatalf("unsupported archive refusal wrote records: page=%+v err=%v", page, err)
			}
			principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := r.IdempotencyByKey(ctx, principal, "anonymous", "admin.restore.finish", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			if err != nil || receipt != nil {
				t.Fatalf("unsupported archive refusal wrote receipt: %+v err=%v", receipt, err)
			}
		})
	}
}

// TestB17SupportedOfflineStoredNullRestoreAndReverse activates the supported
// fixture drafted at archived B17 checkpoint 1a77ee231 after #104 admission.
// It keeps the independently calculated Go USTAR archive and derived-image
// digests as fixed expectations rather than accepting production-computed values.
func TestB17SupportedOfflineStoredNullRestoreAndReverse(t *testing.T) {
	const id = "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const archiveSHA = "2f06663c3d3778f689e971398028f88cd8d6c9e85d987108abbdb92363d20b18"
	// The archive is unchanged. Its current default ontology image additionally admits blocked:boolean.
	// Historical pre-B26 derived digest: f170bf742699fb0bbc577c57b19e7907872e2ea99a34a94fda5e0659197f20ed
	const derivedSHA = "6f1b59b29636cf665d447b1a4d49c1b5670af05f9d0170cf2c548ff1e51d62ab"
	data := b17SupportedOfflineArchive(t)
	if got := sha(data); got != archiveSHA {
		t.Fatalf("authored supported archive digest=%s size=%d, want %s", got, len(data), archiveSHA)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil || archive.Manifest().History != "unavailable" {
		t.Fatalf("supported archive validation: %v", err)
	}
	rows := archive.Records("bead")
	if len(rows) == 0 || rows[0].ID != id {
		t.Fatalf("supported archive omitted first bead: %+v", rows)
	}
	for _, metadata := range []any{rows[0].Body["metadata"], rows[0].Source.Fields["metadata"]} {
		raw, err := json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		requireB17Null(t, raw)
	}
	verification := converterroundtrip.Verification{
		ArchiveSHA256: archiveSHA, DerivedSHA256: derivedSHA,
	}
	dump, err := reverseconverter.ReadBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	converted, err := reverseconverter.Convert(dump)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, line := range bytes.Split(bytes.TrimSpace(converted.JSONL), []byte{'\n'}) {
		var issue struct {
			ID       string          `json:"id"`
			Metadata json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(line, &issue); err != nil {
			t.Fatal(err)
		}
		if issue.ID == id {
			requireB17Null(t, issue.Metadata)
			found = true
		}
	}
	if !found {
		t.Fatal("reverse converter omitted supported B17 bead")
	}
	for _, name := range []string{"memory", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "supported-b17.db")
			var store driver.Store
			var err error
			if name == "memory" {
				store = memory.New()
			} else {
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if store != nil {
					_ = store.Close()
				}
			}()
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := store.Lease(ctx, "b17-supported", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if lease != nil {
					_ = lease.Release(ctx)
				}
			}()
			result, err := converterroundtrip.RestoreWithLease(
				ctx, bytes.NewReader(data), store, lease, verification,
			)
			if err != nil {
				t.Fatalf("supported offline restore: %v", err)
			}
			if result.Restored.Head.String() != "1" ||
				result.Restored.HistoryFloor.String() != "1" ||
				result.Restored.Incarnation == before.Incarnation ||
				result.Restored.StoreID != driver.StoreID(archive.Manifest().StoreID) ||
				result.IdempotencyKey == "" || result.RequestHash == "" {
				t.Fatalf("restore meta=%+v", result.Restored)
			}
			recovered, err := converterroundtrip.Recover(ctx, store, verification)
			if err != nil || recovered.Restored != result.Restored ||
				recovered.IdempotencyKey != result.IdempotencyKey ||
				recovered.RequestHash != result.RequestHash {
				t.Fatalf("archive marker and receipt mismatch: recovery=%+v err=%v", recovered, err)
			}
			assertB17RestoredNull(t, ctx, store, id)
			if name == "sqlite" {
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				lease = nil
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store = nil
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				recovered, err = converterroundtrip.Recover(ctx, store, verification)
				if err != nil || recovered.Restored != result.Restored ||
					recovered.IdempotencyKey != result.IdempotencyKey ||
					recovered.RequestHash != result.RequestHash {
					t.Fatalf("reopened archive marker and receipt mismatch: recovery=%+v err=%v", recovered, err)
				}
				assertB17RestoredNull(t, ctx, store, id)
			}
		})
	}
}

func assertB17RestoredNull(t *testing.T, ctx context.Context, store driver.Store, id string) {
	t.Helper()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := readStore(t, ctx, store, meta.Incarnation)
	defer r.Close()
	records, err := r.Get(ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
	if err != nil || len(records) != 1 {
		t.Fatalf("restored bead %s: count=%d err=%v", id, len(records), err)
	}
	var document struct {
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(records[0].Body, &document); err != nil {
		t.Fatal(err)
	}
	requireB17Null(t, document.Metadata)
}

func readStore(t *testing.T, ctx context.Context, store driver.Store, incarnation string) driver.Reader {
	t.Helper()
	r, err := store.Read(ctx, driver.View{Incarnation: incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
