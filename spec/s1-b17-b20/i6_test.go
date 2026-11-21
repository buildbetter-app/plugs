package s1b17b20

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const i6FixtureSHA256 = "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c"

func i6FixtureBytes(t *testing.T) []byte {
	t.Helper()
	relative := filepath.Join("internal", "integration", "converterroundtrip", "testdata", "synthetic-plug-dump-1.tar")
	path := relative
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), relative)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := i6SHA256(data); got != i6FixtureSHA256 {
		t.Fatalf("public synthetic fixture digest=%s want %s", got, i6FixtureSHA256)
	}
	return data
}

// i6ImportArchive is a deterministic, test-only admission transform. The
// public fixture deliberately records its source sequential allocator and a
// parent-child edge. The import contract admits hash-shaped source IDs and the
// bounded restore seam represents that edge as a blocking relation. The raw
// fixture remains untouched and is the reverse-converter input.
func i6ImportArchive(t *testing.T, data []byte) []byte {
	t.Helper()
	members := i6TarMembers(t, data)
	var manifest map[string]any
	i6Decode(t, members["manifest.json"], &manifest)
	projects := manifest["projects"].([]any)
	project := projects[0].(map[string]any)
	project["id_mode"] = "hash"
	project["hash_length"] = 1
	for _, name := range []string{"records/edge.ndjson"} {
		lines := bytes.Split(bytes.TrimSuffix(members[name], []byte{'\n'}), []byte{'\n'})
		for i, line := range lines {
			var record map[string]any
			i6Decode(t, line, &record)
			body := record["body"].(map[string]any)
			source := record["source"].(map[string]any)["fields"].(map[string]any)
			body["type"] = "blocks"
			source["type"] = "blocks"
			lines[i] = bytes.TrimSuffix(i6JSON(t, record), []byte{'\n'})
		}
		members[name] = append(bytes.Join(lines, []byte{'\n'}), '\n')
	}
	files := manifest["files"].(map[string]any)
	for name, member := range members {
		if name == "manifest.json" {
			continue
		}
		files[name] = map[string]any{"sha256": i6SHA256(member), "size": len(member)}
	}
	members["manifest.json"] = i6JSON(t, manifest)
	return i6TarBytes(t, members)
}

func i6TarMembers(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	members := map[string][]byte{}
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name] = body
	}
	return members
}

func i6TarBytes(t *testing.T, members map[string][]byte) []byte {
	t.Helper()
	names := make([]string, 0, len(members))
	for name := range members {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	names = append([]string{"manifest.json"}, names...)
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, name := range names {
		body := members[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(body)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func i6Decode(t *testing.T, data []byte, target any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
	}
}

func i6JSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func i6SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func i6OpenStore(t *testing.T, engine string) (driver.Store, driver.ActiveLease, func()) {
	t.Helper()
	ctx := context.Background()
	var (
		store driver.Store
		err   error
	)
	switch engine {
	case "memory":
		store, err = memory.NewWithOptions(memory.Options{
			StoreID:     driver.StoreID("i6-" + engine + "-store"),
			Incarnation: "i6-" + engine + "-incarnation",
		})
	case "sqlite":
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "i6.db")})
	default:
		t.Fatalf("unknown I6 engine %q", engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "s1-i6-holder", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return store, lease, func() {
		_ = lease.Release(context.Background())
		_ = store.Close()
	}
}

type i6Census struct {
	Meta  driver.Meta
	Rows  int
	Kinds map[string]int
	Refs  []string
}

func i6StoreCensus(t *testing.T, store driver.Store) i6Census {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("store census page=%+v err=%v", page, err)
	}
	census := i6Census{Meta: meta, Rows: len(page.Items), Kinds: map[string]int{}, Refs: make([]string, 0, len(page.Items))}
	for _, row := range page.Items {
		census.Kinds[row.Kind]++
		census.Refs = append(census.Refs, row.Kind+"/"+row.Ref.ID)
	}
	sort.Strings(census.Refs)
	return census
}

func i6NativeAllocatorBinding(t *testing.T, archive *dumpformat.Archive, captured driver.Meta) {
	t.Helper()
	member, ok := archive.Member("allocator/snapshot.json")
	if !ok {
		t.Fatal("native archive omitted the complete allocator snapshot")
	}
	// Independently spell the admitted fixture's complete live inventory. Its
	// source sequential counters remain provenance; import seeds the adaptive
	// hash policy and direct-child counters. Do not use the production codec or digest.
	want, err := json.Marshal(struct {
		Version       string            `json:"version"`
		StoreID       string            `json:"store_id"`
		Incarnation   string            `json:"incarnation"`
		At            json.Number       `json:"at"`
		CoverageFloor json.Number       `json:"coverage_floor"`
		Projects      []json.RawMessage `json:"projects"`
		Parents       []json.RawMessage `json:"parents"`
	}{
		Version: "plug-native-allocator/1", StoreID: string(captured.StoreID), Incarnation: captured.Incarnation,
		At: json.Number(captured.Head.String()), CoverageFloor: json.Number("1"),
		Projects: []json.RawMessage{json.RawMessage(`{"prefix":"demo","id_mode":"hash","hash_length":8,"min_hash_length":3,"max_hash_length":8,"last_id":8,"child_counters":{"demo-7":4},"max_collision_prob":0.25}`)},
		Parents:  []json.RawMessage{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(member, want) {
		t.Fatalf("native allocator snapshot differs from the captured source inventory:\n got %s\nwant %s", member, want)
	}
	manifest := archive.Manifest()
	if manifest.StoreID != string(captured.StoreID) || manifest.NativeStoreID != string(captured.StoreID) || manifest.NativeIncarnation != captured.Incarnation || manifest.NativeHead != captured.Head.String() || manifest.NativeAllocatorProfile != "plug-native-allocator/1" || manifest.NativeAllocatorDigest != i6SHA256(member) {
		t.Fatalf("native allocator profile/digest/source binding=%+v", manifest)
	}
}

func i6NativeDigest(t *testing.T, archive *dumpformat.Archive) string {
	t.Helper()
	h := sha256.New()
	required := map[string]struct{}{
		"allocator/snapshot.json": {},
		"config/sources.json":     {},
		"records/native.ndjson":   {},
		"versions.ndjson":         {},
		"changesets.ndjson":       {},
	}
	files := archive.Manifest().Files
	// dumpformat.Read has already rejected any tar member absent from this
	// manifest map. The census below keeps that authenticated inventory while
	// allowing optional content-addressed evidence blobs for new backups.
	names := make([]string, 0, len(files))
	for name, file := range files {
		if _, ok := required[name]; !ok {
			if !strings.HasPrefix(name, "blobs/") {
				t.Fatalf("native member census has unrecognized member %q", name)
			}
			digest := strings.TrimPrefix(name, "blobs/")
			if len(digest) != sha256.Size*2 || digest != strings.ToLower(digest) {
				t.Fatalf("native evidence blob path is not a lowercase sha256 %q", name)
			}
			decoded, err := hex.DecodeString(digest)
			if err != nil || len(decoded) != sha256.Size || file.SHA256 != digest {
				t.Fatalf("native evidence blob manifest binding=%q file=%+v", name, file)
			}
		}
		names = append(names, name)
	}
	for name := range required {
		if _, ok := files[name]; !ok {
			t.Fatalf("native archive omitted required member %q, census=%v", name, files)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		member, ok := archive.Member(name)
		if !ok {
			t.Fatalf("native archive omitted %s", name)
		}
		file, ok := files[name]
		if !ok || file.Size != int64(len(member)) || file.SHA256 != i6SHA256(member) {
			t.Fatalf("native member %s checksum/size binding=%+v", name, file)
		}
		_, _ = io.WriteString(h, name)
		_, _ = h.Write(member)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func i6NativeRows(t *testing.T, archive *dumpformat.Archive) []map[string]any {
	t.Helper()
	data, ok := archive.NativeMember("records/native.ndjson")
	if !ok {
		t.Fatal("native record member is missing")
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'})
	rows := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var row map[string]any
		i6Decode(t, line, &row)
		rows = append(rows, row)
	}
	return rows
}

func i6Profile(t *testing.T, data []byte) driver.RestoreImportProfile {
	t.Helper()
	inspection, err := converterroundtrip.InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("inspect admitted synthetic archive: %v", err)
	}
	return driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64),
		},
		SourceStoreID:  inspection.SourceStoreID,
		Method:         "offline.restore.import",
		IdempotencyKey: "s1-i6-restore-import-0000000001",
		ArchiveDigest:  inspection.ArchiveSHA256,
		DerivedDigest:  inspection.DerivedSHA256,
		HistoryMode:    driver.RestoreHistoryModeSnapshot,
	}
}

func TestI6PublicSyntheticRestoreDumpReverseCensus(t *testing.T) {
	ctx := context.Background()
	raw := i6FixtureBytes(t)
	source, err := dumpformat.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := map[string]int{"bead": 3, "beads-archive": 105, "comment": 1, "config": 2, "edge": 1, "memory": 1}
	if source.Manifest().History != "available-unverified" || source.Manifest().Projects[0].Prefix != "demo" || source.Manifest().Projects[0].LastID != 8 || source.Manifest().Projects[0].ChildCounters["demo-7"] != 4 {
		t.Fatalf("source allocator/history census=%+v", source.Manifest())
	}
	for kind, want := range wantKinds {
		if got := len(source.Records(kind)); got != want {
			t.Fatalf("source kind %s=%d want %d", kind, got, want)
		}
	}
	admitted := i6ImportArchive(t, raw)
	if _, err := dumpformat.Read(bytes.NewReader(admitted)); err != nil {
		t.Fatalf("admitted derivative failed canonical validation: %v", err)
	}
	profile := i6Profile(t, admitted)
	if profile.SourceStoreID != "synthetic-store" || profile.ArchiveDigest != i6SHA256(admitted) {
		t.Fatalf("profile is not bound to admitted bytes: %+v", profile)
	}

	// Reverse conversion validates the unchanged public archive. It is kept
	// separate from the native Plug dump: native records are Plug-owned state,
	// while this boundary converts the source-shaped plug-dump/1 interchange.
	reverseDump, err := reverseconverter.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("reverse read: %v", err)
	}
	reversed, err := reverseconverter.Convert(reverseDump)
	if err != nil {
		t.Fatalf("reverse convert: %v", err)
	}
	if reversed.Report.ArchiveRows != 105 || len(reversed.Projection.Issues) != 3 || len(reversed.Projection.Edges) != 1 || len(reversed.Projection.Comments) != 1 || len(reversed.Projection.Memories) != 3 {
		t.Fatalf("reverse census projection=%+v report=%+v", reversed.Projection, reversed.Report)
	}
	if got := i6SHA256(reversed.JSONL); got != reversed.Digest || reversed.Report.OutputDigest != got {
		t.Fatalf("reverse digest got=%s result=%s report=%s", got, reversed.Digest, reversed.Report.OutputDigest)
	}
	if got := []string{reversed.Projection.Issues[0].ID, reversed.Projection.Issues[1].ID, reversed.Projection.Issues[2].ID}; !strings.EqualFold(strings.Join(got, ","), "demo-7,demo-7.4,demo-8") {
		t.Fatalf("reverse issue IDs=%v", got)
	}
	if edge := reversed.Projection.Edges[0]; edge.From != "demo-7.4" || edge.To != "demo-7" || edge.Type != "parent-child" {
		t.Fatalf("reverse dependency=%+v", edge)
	}
	var header struct {
		Schema string `json:"_schema"`
	}
	i6Decode(t, bytes.Split(reversed.JSONL, []byte{'\n'})[0], &header)
	if header.Schema != reverseconverter.BeadsJSONL {
		t.Fatalf("reverse header=%+v", header)
	}

	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, cleanup := i6OpenStore(t, engine)
			defer cleanup()
			before := i6StoreCensus(t, store)
			if before.Rows != 0 || !before.Meta.Head.IsZero() {
				t.Fatalf("target was not empty: %+v", before)
			}
			report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(admitted), store, lease, profile)
			if err != nil {
				t.Fatalf("snapshot restore on %s: %v", engine, err)
			}
			afterRestore := i6StoreCensus(t, store)
			if report.Restored.Head.String() != "1" || afterRestore.Meta.Head.String() != "1" || afterRestore.Meta.StoreID != profile.SourceStoreID || afterRestore.Rows <= source.Manifest().Kinds[0].Count {
				t.Fatalf("restore census=%+v report=%+v", afterRestore, report.Restored)
			}
			if afterRestore.Kinds["bead"] != 3 || afterRestore.Kinds["edge"] != 1 || afterRestore.Kinds["comment"] != 1 || afterRestore.Kinds["memory"] != 1 || afterRestore.Kinds["config"] != 2 || afterRestore.Kinds["beads-archive"] != 105 {
				t.Fatalf("restored source census=%v", afterRestore.Kinds)
			}
			for _, id := range []string{"demo-7", "demo-7.4", "demo-8"} {
				if !containsString(i6RefsForKind(afterRestore.Refs, "bead"), "bead/"+id) {
					t.Fatalf("restored bead %s missing: %v", id, afterRestore.Refs)
				}
			}

			native, manifest, err := restore.Dump(ctx, store, "head")
			if err != nil {
				t.Fatalf("native admin dump: %v", err)
			}
			nativeArchive, err := dumpformat.Read(bytes.NewReader(native))
			if err != nil || !nativeArchive.Native() {
				t.Fatalf("native dump validation=%v", err)
			}
			i6NativeAllocatorBinding(t, nativeArchive, afterRestore.Meta)
			nativeDigest := i6NativeDigest(t, nativeArchive)
			tarDigest := i6SHA256(native)
			if manifest.NativeHead != "1" || manifest.RecordCount != afterRestore.Rows || manifest.NativeDigest == "" || manifest.NativeDigest != nativeDigest || nativeArchive.SHA256() != tarDigest {
				t.Fatalf("native dump manifest=%+v rows=%d nativeDigest=%s tarDigest=%s archiveDigest=%s", manifest, afterRestore.Rows, nativeDigest, tarDigest, nativeArchive.SHA256())
			}
			kindCounts := map[string]int{}
			for _, row := range i6NativeRows(t, nativeArchive) {
				kind, ok := row["kind"].(string)
				if !ok {
					t.Fatalf("native row kind=%v", row["kind"])
				}
				kindCounts[kind]++
			}
			if kindCounts["bead"] != 3 || kindCounts["edge"] != 1 || kindCounts["comment"] != 1 || kindCounts["config"] != 2 || kindCounts["memory"] != 1 || kindCounts["beads-archive"] != 105 {
				t.Fatalf("native kind census=%v", kindCounts)
			}

			ontology := service.NewOntology(nil)
			fixedNow := time.Date(2026, 11, 1, 23, 30, 0, 0, time.UTC)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time { return fixedNow }))
			svc := service.New(store, lease, writer, service.WithOntology(ontology))
			actor := profile.Admission.Actor
			claimed, err := svc.ClaimNextWithOutcome(ctx, actor, service.ClaimNextRequest{Project: "demo"}, model.WriteMeta{Method: "bead.claimNext", Project: "demo", Agent: "s1-i6", AgentLabel: "s1-i6", IdempotencyKey: "s1-i6-claim-0000000000000001"})
			if err != nil || claimed == nil || len(claimed.Commit.Results) != 1 {
				t.Fatalf("claimNext=%+v err=%v", claimed, err)
			}
			claimedID := claimed.Commit.Results[0].Ref.ID
			if claimedID != "demo-7" {
				t.Fatalf("claimNext selected %q", claimedID)
			}
			changeSet := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(claimedID, model.Natural64(0)), Fields: map[string]any{"description": "updated after public restore"}}}}}
			applied, err := writer.Apply(ctx, actor, changeSet, model.WriteMeta{Method: "tx.apply", Project: "demo", Agent: "s1-i6", AgentLabel: "s1-i6", IdempotencyKey: "s1-i6-apply-0000000000000001"})
			if err != nil || len(applied.Results) != 1 || applied.Revision.String() != "3" {
				t.Fatalf("tx.apply=%+v err=%v", applied, err)
			}
			if versions, err := i6Versions(t, store, claimedID); err != nil || len(versions) < 3 || versions[len(versions)-1].Revision.String() != "3" {
				t.Fatalf("version readback=%v err=%v", versions, err)
			}
			finalNative, finalManifest, err := restore.Dump(ctx, store, "head")
			if err != nil {
				t.Fatalf("post-workload native dump: %v", err)
			}
			if finalManifest.NativeHead != "3" || i6SHA256(finalNative) == i6SHA256(native) {
				t.Fatalf("post-workload dump did not move head: head=%s", finalManifest.NativeHead)
			}
			t.Logf("I6 %s: source=%s admitted=%s restore rows=%d native=%s/%s claim=%s apply=%s reverse=%s", engine, i6FixtureSHA256, profile.ArchiveDigest, afterRestore.Rows, manifest.NativeHead, manifest.NativeDigest, claimedID, applied.Revision, reversed.Digest)
		})
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func i6RefsForKind(values []string, kind string) []string {
	result := make([]string, 0)
	for _, value := range values {
		if strings.HasPrefix(value, kind+"/") {
			result = append(result, value)
		}
	}
	return result
}

func i6Versions(t *testing.T, store driver.Store, id string) ([]driver.Version, error) {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		return nil, err
	}
	reader, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	page, err := reader.Versions(context.Background(), model.RefByID(id, model.Natural64(0)), driver.PageReq{Limit: 100})
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}
