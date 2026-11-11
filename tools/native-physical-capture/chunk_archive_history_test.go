package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
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
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// Engine types are confined to these public fixture builders. No engine object
// crosses the production read-only capture port. All graph, root, schema and
// cell expectations are observations made BEFORE the disposable copy is changed.
type cp11PublicStore struct {
	DataDir     string
	Log         plugdump.SourceCommitLog
	PhysicalLog physicalcapture.History
	Working     physicalcapture.WorkingImage
	Current     map[string]plugdump.NativeTable
	Images      map[string]physicalcapture.Image
	Profiles    map[string]plugdump.NativeCommitSchemaProfile
	Checkpoints map[int]string
	Roots       map[string]hash.Hash
	StoreRoot   hash.Hash
	Version     string
	Chunks      map[hash.Hash]chunks.Chunk
}

func cp11Require(t *testing.T, err error, operation string) {
	t.Helper()
	if err != nil {
		t.Fatalf("public CP11 %s failed: error_type=%T", operation, err)
	}
}

func cp11Artifact(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" || filepath.Base(name) != name {
		t.Fatal("public CP11 canonical artifact directory/name required")
	}
	cp11Require(t, os.WriteFile(filepath.Join(dir, name), data, 0600), "artifact write")
}

func cp11JSONArtifact(t *testing.T, name string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	cp11Require(t, err, "public evidence encode")
	cp11Artifact(t, name, append(data, '\n'))
}

// cp11Tree rejects symlinks and special files; its relative byte map is also
// the exact preimage used to construct each independently disposable copy.
func cp11Tree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	result := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("public fixture contains a nonregular entry")
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[filepath.ToSlash(rel)], err = os.ReadFile(path)
		return err
	})
	cp11Require(t, err, "public tree read")
	return result
}

func cp11WriteTree(t *testing.T, root string, files map[string][]byte, omitNoms bool) {
	t.Helper()
	for name, data := range files {
		if omitNoms && strings.HasPrefix(name, "public_commit_history/.dolt/noms/") {
			continue
		}
		path := filepath.Join(root, filepath.FromSlash(name))
		cp11Require(t, os.MkdirAll(filepath.Dir(path), 0700), "public copy directory")
		cp11Require(t, os.WriteFile(path, data, 0600), "public copy file")
	}
}

func cp11Tar(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		data := files[name]
		cp11Require(t, w.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg}), "public tar header")
		_, err := w.Write(data)
		cp11Require(t, err, "public tar body")
	}
	cp11Require(t, w.Close(), "public tar close")
	return out.Bytes()
}

func cp11LoadEngine(t *testing.T, ctx context.Context, dataDir string) *doltdb.DoltDB {
	t.Helper()
	fs, err := filesys.LocalFilesysWithWorkingDir(filepath.Join(dataDir, "public_commit_history"))
	cp11Require(t, err, "engine fixture filesystem")
	ddb, err := doltdb.LoadDoltDBWithParams(ctx, types.Format_DOLT, doltdb.LocalDirDoltDB, fs, map[string]interface{}{"disable_singleton_cache": struct{}{}})
	cp11Require(t, err, "engine fixture open")
	return ddb
}

func cp11NewPublicStore(t *testing.T, ctx context.Context) cp11PublicStore {
	t.Helper()
	f := cp11PublicStore{DataDir: diagnosticDir(t), Images: map[string]physicalcapture.Image{}, Profiles: map[string]plugdump.NativeCommitSchemaProfile{}, Checkpoints: map[int]string{}, Roots: map[string]hash.Hash{}, Chunks: map[hash.Hash]chunks.Chunk{}}
	db, closeDriver, err := openPublicMigrationFixture(f.DataDir)
	cp11Require(t, err, "fixture driver open")
	defer func() { _ = db.Close(); _ = closeDriver() }()
	conn, err := db.Conn(ctx)
	cp11Require(t, err, "fixture connection")
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE `public_commit_history`")
	ownVersionExec(t, ctx, conn, "USE `public_commit_history`")
	_, err = schema.MigrateUpTo(ctx, conn, 39)
	cp11Require(t, err, "actual migration prefix 39")
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	for _, id := range []string{"public-a1b", "public-c2d"} {
		ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,metadata,timeout_ns,hook_bead,role_bead,payload) VALUES (?,'public chunk history','','','','',JSON_OBJECT('n',9007199254740993),9007199254740993,NULL,'','null')", id)
	}
	ownVersionExec(t, ctx, conn, "UPDATE issues SET metadata=CAST('null' AS JSON) WHERE id='public-c2d'")
	ownVersionExec(t, ctx, conn, "INSERT INTO compaction_snapshots(id,issue_id,compaction_level,snapshot_json) VALUES ('public-cp11-binary','public-a1b',1,?),('public-cp11-empty-binary','public-c2d',1,?)", []byte{0, 255, 128, 42}, []byte{})
	ownVersionExec(t, ctx, conn, "INSERT INTO dependencies(issue_id,depends_on_id,type,created_by) VALUES ('public-a1b','public-c2d','blocks','public')")
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-cp11-before-62','public-a1b','updated','public',NULL,'','actual historical event')")
	f.Checkpoints[39] = commitHistoryCommit(t, ctx, conn)
	for _, version := range []int{40, 41, 61, 62, 66} {
		_, err = schema.MigrateUpTo(ctx, conn, version)
		cp11Require(t, err, "actual historical migration checkpoint")
		ownVersionExec(t, ctx, conn, "UPDATE issues SET title=? WHERE id='public-a1b'", fmt.Sprintf("public chunk checkpoint %d", version))
		f.Checkpoints[version] = commitHistoryCommit(t, ctx, conn)
	}
	boundaryIgnoredPrefix(t, ctx, conn)
	commitHistoryCommit(t, ctx, conn)
	// Migration 62 restores the historical event into ignored working events.
	// Remove only that working copy; immutable checkpoints retain the original.
	ownVersionExec(t, ctx, conn, "DELETE FROM events WHERE id=?", "public-cp11-before-62")
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-cp11-current-1','public-a1b','updated','public',NULL,'','first current-only attachment'),('public-cp11-current-2','public-a1b','updated','public','','after','second current-only attachment')")
	f.Log, err = (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	cp11Require(t, err, "complete actual graph before removal")
	if !f.Log.Clean || len(f.Log.Commits) < 8 || len(commitHistoryStatus(t, ctx, conn)) != 0 {
		t.Fatal("public CP11 fixture is not a clean multi-commit source")
	}
	f.Working, err = physicalcapture.ReadWorkingRoot(ctx, conn, f.Log.Head)
	cp11Require(t, err, "working root before removal")
	f.PhysicalLog, err = physicalcapture.ReadHistory(ctx, conn, f.Log.Head)
	cp11Require(t, err, "complete original physical graph before removal")
	if !f.PhysicalLog.ReadOnlyScope || len(f.PhysicalLog.Commits) != len(f.Log.Commits) || len(f.PhysicalLog.Boundaries) != 0 {
		t.Fatal("original full physical graph differs from the SQL census")
	}
	f.Current = commitHistoryCurrent(t, ctx, conn)
	if len(f.Current["events"].Rows) != 2 {
		t.Fatal("public CP11 current events attachment differs")
	}
	for _, commit := range f.Log.Commits {
		image, err := physicalcapture.ReadCommit(ctx, conn, commit.ID)
		cp11Require(t, err, "every historical physical image before removal")
		profile, err := plugdump.PhysicalCommitSchemaProfile(image)
		cp11Require(t, err, "every historical own-schema profile before removal")
		f.Images[commit.ID], f.Profiles[commit.ID] = image, profile
	}
	cp11AssertHistoricalEvent(t, f, "public-cp11-before-62")
	cp11Require(t, conn.Close(), "fixture connection close")
	cp11Require(t, db.Close(), "fixture database close")
	cp11Require(t, closeDriver(), "fixture driver close")

	ddb := cp11LoadEngine(t, ctx, f.DataDir)
	defer func() { cp11Require(t, ddb.Close(), "engine preimage close") }()
	cs := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	f.StoreRoot, err = cs.Root(ctx)
	cp11Require(t, err, "actual Noms store root")
	f.Version = cs.Version()
	iter, ok := cs.(interface {
		IterateAllChunks(context.Context, func(chunks.Chunk)) error
	})
	if !ok {
		t.Fatal("pinned public chunk enumeration unavailable")
	}
	err = iter.IterateAllChunks(ctx, func(c chunks.Chunk) {
		if c.IsGhost() || c.IsEmpty() {
			t.Fatal("full public preimage unexpectedly contains a ghost/empty chunk")
		}
		if before, present := f.Chunks[c.Hash()]; present && !bytes.Equal(before.Data(), c.Data()) {
			t.Fatal("one public chunk address has inconsistent bytes")
		}
		f.Chunks[c.Hash()] = c
	})
	cp11Require(t, err, "all physical chunks before removal")
	for _, commit := range f.Log.Commits {
		optional, err := ddb.ReadCommit(ctx, hash.Parse(commit.ID))
		cp11Require(t, err, "actual engine commit before removal")
		stored, ok := optional.ToCommit()
		if !ok {
			t.Fatal("full public source contains a ghost commit")
		}
		root, err := stored.GetRootValue(ctx)
		cp11Require(t, err, "actual historical root before removal")
		f.Roots[commit.ID], err = root.HashOf()
		cp11Require(t, err, "actual historical root address")
		if _, present := f.Chunks[f.Roots[commit.ID]]; !present {
			t.Fatal("actual historical root absent from enumerated public preimage")
		}
	}
	return f
}

func cp11AssertHistoricalEvent(t *testing.T, f cp11PublicStore, eventID string) {
	t.Helper()
	for _, version := range []int{39, 40, 41, 61} {
		found := false
		for _, table := range f.Images[f.Checkpoints[version]].Tables {
			if table.Name != "events" {
				continue
			}
			rows := commitHistoryNativeTable(t, table).Rows
			if len(rows) != 1 || rows[0]["id"] != eventID {
				t.Fatal("public CP11 immutable historical event differs")
			}
			found = true
		}
		if !found {
			t.Fatal("public CP11 immutable historical events table missing")
		}
	}
}

// Only existing actual chunk payloads enter the pinned engine writer. Repacking
// never creates a commit, changes an edge, grafts rows or renames a loose file
// to .darc. Loss is exactly one independently observed historical root address.
func cp11Repack(t *testing.T, ctx context.Context, f cp11PublicStore, preimage map[string][]byte, archive bool, omit hash.Hash) (string, string) {
	t.Helper()
	dir := diagnosticDir(t)
	cp11WriteTree(t, dir, preimage, true)
	nomsDir := filepath.Join(dir, "public_commit_history", ".dolt", "noms")
	cp11Require(t, os.MkdirAll(nomsDir, 0700), "repacked store directory")
	var writer nbs.GenericTableWriter
	var err error
	if archive {
		writer, err = nbs.NewArchiveStreamWriter(diagnosticDir(t))
	} else {
		writer, err = nbs.NewCmpChunkTableWriter(diagnosticDir(t))
	}
	cp11Require(t, err, "pinned table/archive writer")
	defer func() { cp11Require(t, writer.Remove(), "pinned writer cleanup") }()
	addresses := make([]hash.Hash, 0, len(f.Chunks))
	for address := range f.Chunks {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].String() < addresses[j].String() })
	removed := 0
	for _, address := range addresses {
		if address == omit {
			removed++
			continue
		}
		_, err = writer.AddChunk(nbs.ChunkToCompressedChunk(f.Chunks[address]))
		cp11Require(t, err, "actual chunk repack")
	}
	if omit.IsEmpty() && removed != 0 || !omit.IsEmpty() && removed != 1 {
		t.Fatal("public CP11 repack did not remove exactly the requested actual object")
	}
	_, filename, err := writer.Finish()
	cp11Require(t, err, "pinned writer finish")
	if writer.ChunkCount() != len(f.Chunks)-removed || strings.HasSuffix(filename, ".darc") != archive {
		t.Fatal("pinned repack chunk count/format differs")
	}
	// The two pinned writers have different FlushToFile conventions. Stream the
	// finished engine bytes without modifying them, using the engine's own name.
	reader, err := writer.Reader()
	cp11Require(t, err, "pinned finished file reader")
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	cp11Require(t, readErr, "pinned finished file bytes")
	cp11Require(t, closeErr, "pinned finished reader close")
	file := filepath.Join(nomsDir, filename)
	cp11Require(t, os.WriteFile(file, data, 0600), "actual table/archive write")
	store, err := nbs.NewLocalStore(ctx, f.Version, nomsDir, 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	cp11Require(t, err, "repacked NBS open")
	cp11Require(t, store.AddTableFilesToManifest(ctx, map[string]int{strings.TrimSuffix(filename, nbs.ArchiveFileSuffix): writer.ChunkCount()}, nil), "pinned manifest registration")
	ok, err := store.Commit(ctx, f.StoreRoot, hash.Hash{})
	cp11Require(t, err, "same actual current Noms root publication")
	if !ok {
		t.Fatal("public repack did not publish the original Noms root")
	}
	root, err := store.Root(ctx)
	cp11Require(t, err, "repacked Noms root readback")
	observed := map[hash.Hash][]byte{}
	cp11Require(t, store.IterateAllChunks(ctx, func(c chunks.Chunk) { observed[c.Hash()] = append([]byte{}, c.Data()...) }), "all repacked chunk bytes")
	if root != f.StoreRoot || len(observed) != len(f.Chunks)-removed {
		t.Fatal("repack changed current root or physical chunk cardinality")
	}
	for address, original := range f.Chunks {
		data, present := observed[address]
		if address == omit {
			if present {
				t.Fatal("removed historical chunk remained readable")
			}
		} else if !present || !bytes.Equal(data, original.Data()) {
			t.Fatal("repack changed a retained physical chunk")
		}
	}
	cp11Require(t, store.Close(), "repacked NBS close")
	if archive {
		inspector, err := nbs.NewArchiveInspectorFromFileWithMmap(ctx, file, nbs.NewUnlimitedMemQuotaProvider(), false)
		cp11Require(t, err, "genuine Dolt archive inspection")
		if inspector.FileSignature() != "DOLTARC" || int(inspector.ChunkCount()) != len(observed) {
			t.Fatal("repacked file is not the genuine measured Dolt archive")
		}
		cp11Require(t, inspector.Close(), "archive inspector close")
	}
	return dir, file
}

type cp11ProbeObservation struct {
	Format                       string   `json:"format"`
	ReachableCommitCount         uint64   `json:"reachable_commit_count"`
	ReachableCountLowerBound     bool     `json:"reachable_count_is_lower_bound"`
	ReadableRootCount            uint64   `json:"readable_root_count"`
	ChunkCompleteCommitCount     uint64   `json:"chunk_complete_commit_count"`
	FirstUnreadableOrdinal       *uint64  `json:"first_unreadable_ordinal"`
	FirstUnreadableSchemaVersion *uint64  `json:"first_unreadable_schema_version"`
	GhostMarkers                 uint64   `json:"shallow_ghost_marker_count"`
	ArchiveChunks                *uint64  `json:"archive_chunk_count"`
	LooseChunks                  *uint64  `json:"loose_chunk_count"`
	ArchiveFiles                 *uint64  `json:"archive_file_count"`
	LooseFiles                   *uint64  `json:"loose_file_count"`
	ReasonSubcode                string   `json:"reason_subcode"`
	GoErrorTypes                 []string `json:"go_error_types"`
	SQLDoltCodes                 []uint16 `json:"sql_dolt_codes"`
}

func cp11StorageCounts(t *testing.T, ctx context.Context, dataDir string) [4]uint64 {
	t.Helper()
	ddb := cp11LoadEngine(t, ctx, dataDir)
	defer func() { cp11Require(t, ddb.Close(), "storage census close") }()
	cs := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	files, ok := cs.(chunks.TableFileStore)
	if !ok {
		t.Fatal("pinned public store does not expose actual storage sources")
	}
	sources, err := files.Sources(ctx)
	cp11Require(t, err, "actual active table sources")
	var counts [4]uint64
	for _, source := range sources.TableFiles {
		if source.LocationSuffix() == nbs.ArchiveFileSuffix {
			counts[0] += uint64(source.NumChunks())
			counts[2]++
		} else {
			counts[1] += uint64(source.NumChunks())
			counts[3]++
		}
	}
	return counts
}

func cp11RunProbe(t *testing.T, ctx context.Context, binary, dataDir, name string, reachable, readable uint64, first *uint64, storage [4]uint64) cp11ProbeObservation {
	t.Helper()
	complete, reason := reachable, ""
	if first != nil {
		complete--
		reason = "missing_root_chunk"
	}
	return cp11RunProbeBoundary(t, ctx, binary, dataDir, name, reachable, readable, complete, first, reason, nil, storage)
}

// Root-value readability and complete root closures are independent counts.
// A genuine missing row leaf must produce a boundary even when all roots decode.
func cp11RunProbeBoundary(t *testing.T, ctx context.Context, binary, dataDir, name string, reachable, readable, complete uint64, first *uint64, reason string, schemaVersion *uint64, storage [4]uint64) cp11ProbeObservation {
	t.Helper()
	command, output := commitHistoryCommand(t, ctx, binary, dataDir)
	command.Args = append(command.Args, "--history-probe-only")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	cp11Artifact(t, name+"-probe-stdout.json", stdout.Bytes())
	cp11Artifact(t, name+"-probe-stderr.jsonl", stderr.Bytes())
	if err != nil || stderr.Len() != 0 {
		t.Fatalf("public CP11 counts-only probe failed: error_type=%T", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	var observed cp11ProbeObservation
	cp11Require(t, decoder.Decode(&observed), "strict counts-only probe decode")
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		t.Fatal("public CP11 probe emitted more than one JSON object")
	}
	var fields map[string]json.RawMessage
	cp11Require(t, json.Unmarshal(stdout.Bytes(), &fields), "explicit counts-only probe fields")
	for _, key := range []string{"format", "reachable_commit_count", "reachable_count_is_lower_bound", "readable_root_count", "chunk_complete_commit_count", "first_unreadable_ordinal", "first_unreadable_schema_version", "shallow_ghost_marker_count", "archive_chunk_count", "loose_chunk_count", "archive_file_count", "loose_file_count", "reason_subcode", "go_error_types", "sql_dolt_codes"} {
		if _, present := fields[key]; !present {
			t.Fatal("public CP11 probe omitted an explicit required count/null/code field")
		}
	}
	for _, key := range []string{"go_error_types", "sql_dolt_codes"} {
		if !bytes.HasPrefix(bytes.TrimSpace(fields[key]), []byte("[")) {
			t.Fatal("public CP11 probe error types/numeric codes must be explicit arrays")
		}
	}
	if observed.Format != "beads-history-completeness/1" || observed.ReachableCountLowerBound || observed.ReachableCommitCount != reachable || observed.ReadableRootCount != readable || observed.GhostMarkers != 0 || !reflect.DeepEqual(observed.FirstUnreadableOrdinal, first) {
		t.Fatalf("public CP11 measured graph/root census differs: reachable=%d readable=%d ghosts=%d lower_bound=%t", observed.ReachableCommitCount, observed.ReadableRootCount, observed.GhostMarkers, observed.ReachableCountLowerBound)
	}
	if observed.ChunkCompleteCommitCount != complete || !reflect.DeepEqual(observed.FirstUnreadableSchemaVersion, schemaVersion) {
		t.Fatal("public CP11 measured chunk-closure completeness/schema differs from the actual omission")
	}
	if schemaVersion == nil && !bytes.Equal(fields["first_unreadable_schema_version"], []byte("null")) {
		t.Fatal("public CP11 probe invented an unreadable commit schema version")
	}
	for i, count := range []*uint64{observed.ArchiveChunks, observed.LooseChunks, observed.ArchiveFiles, observed.LooseFiles} {
		if count == nil || *count != storage[i] {
			t.Fatal("public CP11 probe storage count differs from active pinned engine sources")
		}
	}
	if first == nil {
		if complete != reachable || reason != "" || schemaVersion != nil || !bytes.Equal(fields["reason_subcode"], []byte("null")) || !bytes.Equal(fields["first_unreadable_ordinal"], []byte("null")) || observed.ReasonSubcode != "" || len(observed.GoErrorTypes) != 0 || len(observed.SQLDoltCodes) != 0 {
			t.Fatal("complete public probe invented unavailable-history diagnostics")
		}
	} else if complete >= reachable || observed.ReasonSubcode != reason || len(observed.GoErrorTypes) == 0 || len(observed.SQLDoltCodes) != 0 {
		t.Fatal("missing public chunk probe lost actual reason/error types or invented SQL numbers")
	}
	entries, err := os.ReadDir(output)
	if err != nil || len(entries) != 0 {
		t.Fatal("counts-only public probe published capture output")
	}
	return observed
}

func cp11RunCapture(t *testing.T, ctx context.Context, binary, dataDir, name string, success bool) ([]byte, []byte, []byte) {
	t.Helper()
	command, output := commitHistoryCommand(t, ctx, binary, dataDir)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	cp11Artifact(t, name+"-stdout.json", stdout.Bytes())
	cp11Artifact(t, name+"-stderr.jsonl", stderr.Bytes())
	if (err == nil) != success {
		t.Fatalf("public CP11 actual reader result differs: expected_success=%t error_type=%T", success, err)
	}
	if !success {
		entries, readErr := os.ReadDir(output)
		if readErr != nil || len(entries) != 0 || stdout.Len() != 0 {
			t.Fatal("refused public history published output")
		}
		// Every refusal remains structured safe diagnostics, including corrupt
		// present storage. It cannot silently become a successful boundary.
		if len(decodeSafeReports(t, stderr.Bytes())) == 0 {
			t.Fatal("refused public history omitted diagnostics")
		}
		return nil, nil, stderr.Bytes()
	}
	archive, err := os.ReadFile(filepath.Join(output, "capture.tar"))
	cp11Require(t, err, "actual public capture archive")
	evidence, err := os.ReadFile(filepath.Join(output, "physical-evidence.json"))
	cp11Require(t, err, "actual public typed physical evidence")
	cp11Artifact(t, name+".tar", archive)
	cp11Artifact(t, name+"-physical-evidence.json", evidence)
	return archive, evidence, stderr.Bytes()
}

func cp11NewestFirst(t *testing.T, log plugdump.SourceCommitLog) []string {
	t.Helper()
	commits := map[string]plugdump.SourceCommit{}
	for _, commit := range log.Commits {
		commits[commit.ID] = commit
	}
	result := []string{}
	seen := map[string]bool{}
	for next := log.Head; next != ""; {
		commit, present := commits[next]
		if !present || seen[next] || len(commit.Parents) > 1 {
			t.Fatal("actual public fixture is not one complete linear graph")
		}
		seen[next] = true
		result = append(result, next)
		next = ""
		if len(commit.Parents) == 1 {
			next = commit.Parents[0]
		}
	}
	if len(result) != len(log.Commits) {
		t.Fatal("public fixture contains unreachable or duplicate commits")
	}
	return result
}

func cp11AssertPreserved(t *testing.T, f cp11PublicStore, archive, evidenceData []byte, expected map[string]bool) plugdump.NativeHistory {
	t.Helper()
	manifest, native := ownVersionArchive(t, archive)
	if manifest.MainVersion != 66 || manifest.IgnoredVersion != 26 || native.Head != f.Log.Head || len(native.Gaps) != 0 || len(native.Commits) != len(expected) || len(native.Images) != len(expected) || !ownVersionEqual(t, native.Current, f.Current) {
		t.Fatal("public CP11 archive lost current root/cells, schema versions or exact retained census")
	}
	var evidence commitPhysicalEvidence
	decoder := json.NewDecoder(bytes.NewReader(evidenceData))
	decoder.DisallowUnknownFields()
	cp11Require(t, decoder.Decode(&evidence), "strict original typed evidence decode")
	working, err := physicalcapture.EvidenceTables(f.Working.Tables)
	cp11Require(t, err, "original working typed cells")
	if evidence.ArchiveSHA256 != fmt.Sprintf("%x", sha256.Sum256(archive)) || !reflect.DeepEqual(evidence.HistoryCoverage, native.HistoryCoverage) || len(evidence.Images) != len(expected) || evidence.Working.WorkingRootHash != f.Working.WorkingRootHash || !reflect.DeepEqual(working, evidence.Working.Tables) {
		t.Fatal("public CP11 typed evidence differs from original root/types/cells/coverage")
	}
	byID := map[string]plugdump.SourceCommit{}
	for _, commit := range f.Log.Commits {
		byID[commit.ID] = commit
	}
	seen := map[string]bool{}
	for i, image := range native.Images {
		id := image.CommitID
		if !expected[id] || seen[id] || native.Commits[i].ID != id || image.Sequence != uint64(i+1) || !ownVersionEqual(t, native.Commits[i], byID[id]) || !ownVersionEqual(t, image.SchemaProfile, f.Profiles[id]) {
			t.Fatal("public CP11 retained commit identity, adjacency, metadata or own schema changed")
		}
		seen[id] = true
		original := f.Images[id]
		want := map[string]plugdump.NativeTable{}
		for _, table := range original.Tables {
			want[table.Name] = commitHistoryNativeTable(t, table)
		}
		typed, err := physicalcapture.EvidenceTables(original.Tables)
		cp11Require(t, err, "original immutable typed cells")
		if !ownVersionEqual(t, want, image.Tables) || evidence.Images[i].CommitID != id || !evidence.Images[i].ReadOnlyScope || !reflect.DeepEqual(typed, evidence.Images[i].Tables) || !ownVersionEqual(t, evidence.Images[i].SchemaProfile, f.Profiles[id]) {
			t.Fatal("public CP11 retained image changed a physical table/schema/type/cell")
		}
	}
	for _, version := range []int{40, 41, 61, 62, 66} {
		if !seen[f.Checkpoints[version]] {
			t.Fatal("public CP11 suffix omitted a populated migration checkpoint")
		}
	}
	return native
}

func cp11AssertReopened(t *testing.T, ctx context.Context, f cp11PublicStore, dataDir, missing string) error {
	t.Helper()
	db, closeDriver, err := physicalcapture.Open(dataDir, "public_commit_history")
	cp11Require(t, err, "post-reader disposable reopen")
	defer func() {
		cp11Require(t, db.Close(), "post-reader database close")
		cp11Require(t, closeDriver(), "post-reader driver close")
	}()
	conn, err := db.Conn(ctx)
	cp11Require(t, err, "post-reader connection")
	defer conn.Close()
	working, err := physicalcapture.ReadWorkingRoot(ctx, conn, f.Log.Head)
	cp11Require(t, err, "post-reader actual working root")
	if !reflect.DeepEqual(working, f.Working) || !ownVersionEqual(t, commitHistoryCurrent(t, ctx, conn), f.Current) {
		t.Fatal("public probe/reader changed latest root or physical rows/events")
	}
	log, err := physicalcapture.ReadHistory(ctx, conn, f.Log.Head)
	cp11Require(t, err, "post-reader authenticated graph")
	if !reflect.DeepEqual(log, f.PhysicalLog) {
		t.Fatal("root-chunk removal changed the readable commit graph")
	}
	var missingErr error
	for _, id := range cp11NewestFirst(t, f.Log) {
		original := f.Images[id]
		image, err := physicalcapture.ReadCommit(ctx, conn, id)
		if id == missing {
			if err == nil || image.CommitID != "" || image.ReadOnlyScope || len(image.Tables) != 0 || physicalcapture.UnavailableReason(err) != "missing_root_chunk" {
				t.Fatal("actual removed root did not yield a detached typed absence")
			}
			missingErr = err
			// Older roots remain covered by full preimage/one-object byte
			// conservation. Do not iterate their rows past this boundary.
			break
		} else if err != nil || !reflect.DeepEqual(image, original) {
			t.Fatal("an unremoved historical root changed or became unreadable")
		}
	}
	return missingErr
}

func cp11SafeBoundaryDiagnostic(t *testing.T, data []byte, ordinal, verified uint64, reason string, schema *uint64, originalCause error) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	found := 0
	for {
		var fields map[string]json.RawMessage
		err := decoder.Decode(&fields)
		if err == io.EOF {
			break
		}
		cp11Require(t, err, "structured safe history diagnostics")
		if _, present := fields["reason_subcode"]; !present {
			continue
		}
		var actualReason string
		cp11Require(t, json.Unmarshal(fields["reason_subcode"], &actualReason), "explicit diagnostic reason_subcode")
		if actualReason != reason {
			continue
		}
		found++
		for _, key := range []string{"commit_ordinal_from_head", "commit_schema_version", "verified_commit_count", "sql_dolt_codes", "go_error_types"} {
			if _, present := fields[key]; !present {
				t.Fatal("unavailable public history omitted a required diagnostic field")
			}
		}
		for _, key := range []string{"sql_dolt_codes", "go_error_types"} {
			if !bytes.HasPrefix(bytes.TrimSpace(fields[key]), []byte("[")) {
				t.Fatal("public boundary diagnostic error types/numeric codes must be explicit arrays")
			}
		}
		var actualOrdinal, actualVerified *uint64
		var actualSchema *uint64
		var codes []uint16
		var goTypes []string
		cp11Require(t, json.Unmarshal(fields["commit_ordinal_from_head"], &actualOrdinal), "diagnostic ordinal")
		cp11Require(t, json.Unmarshal(fields["commit_schema_version"], &actualSchema), "diagnostic schema")
		cp11Require(t, json.Unmarshal(fields["verified_commit_count"], &actualVerified), "diagnostic verified count")
		cp11Require(t, json.Unmarshal(fields["sql_dolt_codes"], &codes), "diagnostic SQL/Dolt codes")
		cp11Require(t, json.Unmarshal(fields["go_error_types"], &goTypes), "diagnostic Go error types")
		if actualOrdinal == nil || *actualOrdinal != ordinal || actualVerified == nil || *actualVerified != verified || !reflect.DeepEqual(actualSchema, schema) || len(codes) != 0 || len(goTypes) == 0 {
			t.Fatal("actual unavailable public boundary coordinates/schema/codes/type differ")
		}
		for _, originalType := range safeReport(originalCause).GoErrorTypes {
			present := false
			for _, actualType := range goTypes {
				present = present || actualType == originalType
			}
			if !present {
				t.Fatal("actual unavailable history diagnostic dropped an observed driver/cause Go error type")
			}
		}
	}
	if found != 1 {
		t.Fatal("actual public boundary did not emit exactly one complete diagnostic")
	}
}

// Observe the real driver's initial read independently of the subprocess. A
// missing HEAD root or present corrupt archive can prevent opening / schema
// SQL before immutable history has supplied any ordinal or schema coordinate.
// If neither operation refuses, this oracle deliberately remains RED pending
// a concrete later-stage binding; an unrelated subprocess error cannot pass.
func cp11ObserveEarlyRefusal(t *testing.T, ctx context.Context, dataDir string) (captureStage, error) {
	t.Helper()
	db, closeDriver, err := physicalcapture.Open(dataDir, "public_commit_history")
	if err != nil {
		return stageOpen, err
	}
	defer func() {
		cp11Require(t, db.Close(), "independent refusal database close")
		cp11Require(t, closeDriver(), "independent refusal driver close")
	}()
	conn, err := db.Conn(ctx)
	if err != nil {
		return stageOpen, err
	}
	defer func() { cp11Require(t, conn.Close(), "independent refusal connection close") }()
	if _, err := probeSchema(ctx, conn); err != nil {
		return stageSchema, err
	}
	t.Fatal("public CP11 expected early refusal was not observed; later-stage diagnostic binding remains unproved")
	return "", nil
}

func cp11SafeEarlyRefusalDiagnostic(t *testing.T, name string, data []byte, stage captureStage, originalCause error) {
	t.Helper()
	if originalCause == nil || stage != stageOpen && stage != stageSchema || physicalcapture.UnavailableReason(originalCause) != "" {
		t.Fatal("public early refusal requires an actual unlocalized initial driver failure")
	}
	original := safeReport(originalCause)
	original.Stage = stage
	cp11JSONArtifact(t, name+"-independent-refusal-observation.json", original)
	if len(original.GoErrorTypes) == 0 || original.InnermostGoErrorType == "" {
		t.Fatal("independent public refusal did not retain actual error types")
	}
	// Keep strict report decoding in addition to raw-field checks: unknown
	// diagnostic keys or an arbitrary stderr message cannot satisfy the oracle.
	reports := decodeSafeReports(t, data)
	if len(reports) != 1 {
		t.Fatal("public early refusal did not emit exactly one classified failure diagnostic")
	}
	report := reports[0]
	decoder := json.NewDecoder(bytes.NewReader(data))
	var fields map[string]json.RawMessage
	cp11Require(t, decoder.Decode(&fields), "explicit early refusal diagnostic fields")
	var actualReason string
	cp11Require(t, json.Unmarshal(fields["reason_subcode"], &actualReason), "early refusal reason code")
	if actualReason != "other" || report.Stage != stage || report.HistoryBoundaryReason != "" {
		t.Fatal("initial public store failure was misclassified as an admitted history boundary")
	}
	for _, key := range []string{"commit_ordinal_from_head", "commit_schema_version", "verified_commit_count"} {
		if !bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
			t.Fatal("unlocalized public early refusal fabricated or omitted a nullable coordinate/count")
		}
	}
	for _, key := range []string{"go_error_types", "sql_dolt_codes"} {
		if !bytes.HasPrefix(bytes.TrimSpace(fields[key]), []byte("[")) {
			t.Fatal("public early refusal error types/numeric codes must be explicit arrays")
		}
	}
	if report.InnermostGoErrorType != original.InnermostGoErrorType || !reflect.DeepEqual(report.SQLCodes, original.SQLCodes) {
		t.Fatal("public early refusal changed the observed driver leaf type or original numeric codes")
	}
	for _, observedType := range original.GoErrorTypes {
		found := false
		for _, actualType := range report.GoErrorTypes {
			found = found || observedType == actualType
		}
		if !found {
			t.Fatal("public early refusal dropped an independently observed driver/cause Go error type")
		}
	}
}

func cp11NoSourceValuesInSafeOutput(t *testing.T, f cp11PublicStore, name string, suffixes ...string) {
	t.Helper()
	if len(suffixes) == 0 {
		suffixes = []string{"-stdout.json", "-stderr.jsonl", "-probe-stdout.json", "-probe-stderr.jsonl"}
	}
	for _, suffix := range suffixes {
		data, err := os.ReadFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), name+suffix))
		cp11Require(t, err, "original safe output readback")
		for _, id := range append(cp11NewestFirst(t, f.Log), f.StoreRoot.String(), "public-a1b", "public-c2d", "public chunk checkpoint", "current-only attachment") {
			if bytes.Contains(data, []byte(id)) {
				t.Fatal("public safe output leaked a source identifier/row value")
			}
		}
		for _, root := range f.Roots {
			if bytes.Contains(data, []byte(root.String())) {
				t.Fatal("public safe output leaked a physical source root")
			}
		}
	}
}

// The negative archive is derived from this ACTUAL missing-root reader output.
// All member checksums are recomputed, so a refusal must survive a truthful
// container digest and reject the false semantic completeness claim itself.
func cp11FalseCompleteArchive(t *testing.T, archive []byte) []byte {
	t.Helper()
	members := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		cp11Require(t, err, "actual boundary archive member header")
		members[h.Name], err = io.ReadAll(r)
		cp11Require(t, err, "actual boundary archive member bytes")
	}
	var native plugdump.NativeHistory
	decoder := json.NewDecoder(bytes.NewReader(members[plugdump.NativeHistoryMember]))
	decoder.UseNumber()
	cp11Require(t, decoder.Decode(&native), "actual boundary native decode")
	if native.Complete || native.HistoryCoverage == nil {
		t.Fatal("false-complete control requires an actual verified boundary archive")
	}
	native.Complete = true
	var err error
	members[plugdump.NativeHistoryMember], err = json.Marshal(native)
	cp11Require(t, err, "false-complete native encode")
	var manifest plugdump.Manifest
	cp11Require(t, json.Unmarshal(members["manifest.json"], &manifest), "actual boundary manifest decode")
	data := members[plugdump.NativeHistoryMember]
	manifest.Files[plugdump.NativeHistoryMember] = plugdump.File{SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Size: int64(len(data))}
	members["manifest.json"], err = json.Marshal(manifest)
	cp11Require(t, err, "false-complete manifest encode")
	return cp11Tar(t, members)
}

func TestPublicCP11ActualChunkRemovalAndDoltArchiveRepack(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	home := diagnosticDir(t)
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	f := cp11NewPublicStore(t, ctx)
	preimage := cp11Tree(t, f.DataDir)
	preimageTar := cp11Tar(t, preimage)
	cp11Artifact(t, "public-cp11-original-store-preimage.tar", preimageTar)
	newest := cp11NewestFirst(t, f.Log)
	targetCommit := f.Checkpoints[39]
	target := f.Roots[targetCommit]
	boundaryIndex := -1
	all := map[string]bool{}
	for i, id := range newest {
		all[id] = true
		if id == targetCommit {
			boundaryIndex = i
		}
	}
	if boundaryIndex < 5 || target.IsEmpty() || target == f.Roots[f.Log.Head] || target == f.StoreRoot {
		t.Fatal("public historical root is not independently removable before the populated suffix")
	}
	// A unique historical root is required; one omitted object must not cause
	// several immutable roots to disappear or remove HEAD's working state.
	for id, root := range f.Roots {
		if id != targetCommit && root == target {
			t.Fatal("public removal target is shared by another historical commit")
		}
	}
	verified := map[string]bool{}
	for _, id := range newest[:boundaryIndex] {
		verified[id] = true
	}
	ordinal := uint64(boundaryIndex + 1)
	chunkFacts := map[string]map[string]any{}
	for address, c := range f.Chunks {
		chunkFacts[address.String()] = map[string]any{"bytes": len(c.Data()), "sha256": fmt.Sprintf("%x", sha256.Sum256(c.Data()))}
	}
	rootFacts := map[string]string{}
	for id, root := range f.Roots {
		rootFacts[id] = root.String()
	}
	// IDs, hashes and original public values are confined to this reference
	// artifact. The actual CLI probe/diagnostic streams are counts/codes only.
	cp11JSONArtifact(t, "public-cp11-preimage-and-removal-facts.json", map[string]any{
		"fixture": "actual-public-migrations-pinned-nbs-single-root-removal", "dolt_go_pin": "v0.40.5-0.20260806213044-796d07497741",
		"preimage_sha256": fmt.Sprintf("%x", sha256.Sum256(preimageTar)), "noms_root": f.StoreRoot.String(), "noms_version": f.Version,
		"full_actual_graph": f.Log, "original_physical_graph": f.PhysicalLog, "commit_root_addresses": rootFacts, "commit_schema_profiles": f.Profiles, "original_physical_images": f.Images,
		"original_working_root": f.Working, "migration_checkpoints": f.Checkpoints, "physical_chunk_manifest": chunkFacts,
		"removed_commit": targetCommit, "removed_root_chunk": target.String(), "boundary_child": newest[boundaryIndex-1], "boundary_ordinal_from_head": ordinal,
		"removed_root_original_bytes": f.Chunks[target].Data(), "expected_verified_commits": len(verified), "all_roots_demonstrably_readable_before_removal": true,
	})

	var baselineNative plugdump.NativeHistory
	t.Run("unrepacked_complete_positive", func(t *testing.T) {
		storage := cp11StorageCounts(t, ctx, f.DataDir)
		cp11RunProbe(t, ctx, binary, f.DataDir, "public-cp11-baseline", uint64(len(all)), uint64(len(all)), nil, storage)
		archive, evidence, _ := cp11RunCapture(t, ctx, binary, f.DataDir, "public-cp11-baseline", true)
		baselineNative = cp11AssertPreserved(t, f, archive, evidence, all)
		if !baselineNative.Complete || baselineNative.HistoryCoverage != nil {
			t.Fatal("unrepacked public baseline is not complete")
		}
		cp11AssertReopened(t, ctx, f, f.DataDir, "")
		cp11NoSourceValuesInSafeOutput(t, f, "public-cp11-baseline")
		t.Logf("PUBLIC_CP11_BASELINE commits=%d readable_roots=%d events=2 complete=true all_schema_types_cells=true", len(all), len(all))
	})

	t.Run("actual_dolt_archive_complete_positive", func(t *testing.T) {
		dir, _ := cp11Repack(t, ctx, f, preimage, true, hash.Hash{})
		cp11Artifact(t, "public-cp11-darc-complete-store.tar", cp11Tar(t, cp11Tree(t, dir)))
		storage := cp11StorageCounts(t, ctx, dir)
		if storage[0] != uint64(len(f.Chunks)) || storage[1] != 0 || storage[2] != 1 {
			t.Fatal("actual archive was not the sole source of all public chunks")
		}
		cp11RunProbe(t, ctx, binary, dir, "public-cp11-darc-complete", uint64(len(all)), uint64(len(all)), nil, storage)
		archive, evidence, _ := cp11RunCapture(t, ctx, binary, dir, "public-cp11-darc-complete", true)
		native := cp11AssertPreserved(t, f, archive, evidence, all)
		if !native.Complete || native.HistoryCoverage != nil || !ownVersionEqual(t, native.Commits, baselineNative.Commits) || !ownVersionEqual(t, native.Images, baselineNative.Images) || !ownVersionEqual(t, native.Current, baselineNative.Current) || !ownVersionEqual(t, native.Ignore, baselineNative.Ignore) {
			t.Fatal("genuine archive repack differs from complete unrepacked public baseline")
		}
		cp11AssertReopened(t, ctx, f, dir, "")
		cp11NoSourceValuesInSafeOutput(t, f, "public-cp11-darc-complete")
		t.Logf("PUBLIC_CP11_DARC_COMPLETE commits=%d readable_roots=%d archive_chunks=%d loose_chunks=0 archive_files=1 events=2 complete=true preserved=true", len(all), len(all), storage[0])
	})

	for _, archiveForm := range []bool{false, true} {
		name := "public-cp11-loose-missing-root"
		if archiveForm {
			name = "public-cp11-darc-missing-root"
		}
		t.Run(strings.TrimPrefix(name, "public-cp11-"), func(t *testing.T) {
			dir, _ := cp11Repack(t, ctx, f, preimage, archiveForm, target)
			cp11Artifact(t, name+"-store.tar", cp11Tar(t, cp11Tree(t, dir)))
			missingErr := cp11AssertReopened(t, ctx, f, dir, targetCommit)
			if len(physicalcapture.UnavailableSQLCodes(missingErr)) != 0 {
				t.Fatal("public missing root unexpectedly supplied a SQL/Dolt number")
			}
			storage := cp11StorageCounts(t, ctx, dir)
			cp11RunProbe(t, ctx, binary, dir, name, uint64(len(all)), uint64(len(all)-1), &ordinal, storage)
			archive, evidence, stderr := cp11RunCapture(t, ctx, binary, dir, name, true)
			native := cp11AssertPreserved(t, f, archive, evidence, verified)
			coverage := native.HistoryCoverage
			if native.Complete || coverage == nil || coverage.Format != plugdump.NativeHistoryCoverageFormat || coverage.Status != "verified-to-boundary" || coverage.VerifiedCommitCount != uint64(len(verified)) || len(coverage.Boundaries) != 1 {
				t.Fatal("actual missing historical root did not publish the exact verified suffix")
			}
			b := coverage.Boundaries[0]
			if b.CommitID != targetCommit || b.Reason != "missing_root_chunk" || b.CommitOrdinalFromHead != ordinal || b.CommitSchemaVersion != nil || len(b.SQLDoltCodes) != 0 || !reflect.DeepEqual(b.ChildCommitIDs, []string{newest[boundaryIndex-1]}) {
				t.Fatal("actual single-root boundary changed adjacency/subcode/ordinal or fabricated unreadable schema")
			}
			sourceBytes := boundarySourceMember(t, archive)
			var source plugdump.SourceHistory
			decoder := json.NewDecoder(bytes.NewReader(sourceBytes))
			decoder.UseNumber()
			cp11Require(t, decoder.Decode(&source), "actual events attachment decode")
			if coverage.EventsAttachment.Member != plugdump.SourceHistoryMember || coverage.EventsAttachment.SHA256 != fmt.Sprintf("%x", sha256.Sum256(sourceBytes)) || coverage.EventsAttachment.Rows != 2 || source.Range.Complete || source.Range.Reason != "native-history-boundary" || !ownVersionEqual(t, source.Commits, native.Commits) || !ownVersionEqual(t, source.Events, f.Current["events"].Rows) {
				t.Fatal("actual missing-root suffix lost its exact current events attachment")
			}
			cp11SafeBoundaryDiagnostic(t, stderr, ordinal, uint64(len(verified)), "missing_root_chunk", nil, missingErr)
			cp11NoSourceValuesInSafeOutput(t, f, name)
			cp11AssertReopened(t, ctx, f, dir, targetCommit)
			falseComplete := cp11FalseCompleteArchive(t, archive)
			cp11Artifact(t, name+"-false-complete-refused.tar", falseComplete)
			if _, err := plugdump.Validate(bytes.NewReader(falseComplete)); err == nil {
				t.Fatal("actual missing-root archive accepted a falsely complete claim with recomputed checksums")
			}
			t.Logf("PUBLIC_CP11_GENUINE_ROOT_REMOVAL archive_form=%t removed_chunks=1 reachable_commits=%d readable_roots=%d verified_commits=%d first_unreadable_ordinal=%d reason_subcode=missing_root_chunk schema_known=false events=2 preserved=true false_complete_refused=true", archiveForm, len(all), len(all)-1, len(verified), ordinal)
		})
	}

	t.Run("missing_head_root_refuses_no_verified_suffix", func(t *testing.T) {
		dir, _ := cp11Repack(t, ctx, f, preimage, false, f.Roots[f.Log.Head])
		cp11Artifact(t, "public-cp11-missing-head-refused-store.tar", cp11Tar(t, cp11Tree(t, dir)))
		stage, cause := cp11ObserveEarlyRefusal(t, ctx, dir)
		_, _, stderr := cp11RunCapture(t, ctx, binary, dir, "public-cp11-missing-head-refused", false)
		cp11SafeEarlyRefusalDiagnostic(t, "public-cp11-missing-head-refused", stderr, stage, cause)
		cp11NoSourceValuesInSafeOutput(t, f, "public-cp11-missing-head-refused", "-stdout.json", "-stderr.jsonl")
		t.Logf("PUBLIC_CP11_MISSING_HEAD_REFUSED stage=%s reason_subcode=other ordinal_known=false schema_known=false verified_count_known=false actual_error_types_codes=true no_publication=true", stage)
	})
	t.Run("corrupt_present_archive_signature_refuses", func(t *testing.T) {
		dir, file := cp11Repack(t, ctx, f, preimage, true, hash.Hash{})
		data, err := os.ReadFile(file)
		cp11Require(t, err, "corruption control archive read")
		if len(data) < 7 || string(data[len(data)-7:]) != "DOLTARC" {
			t.Fatal("corruption control did not start with a genuine Dolt archive")
		}
		data[len(data)-1] = 'X'
		cp11Require(t, os.WriteFile(file, data, 0600), "public archive signature corruption")
		cp11Artifact(t, "public-cp11-corrupt-archive-store.tar", cp11Tar(t, cp11Tree(t, dir)))
		inspector, inspectionErr := nbs.NewArchiveInspectorFromFileWithMmap(ctx, file, nbs.NewUnlimitedMemQuotaProvider(), false)
		if inspector != nil {
			cp11Require(t, inspector.Close(), "unexpected corrupt inspector close")
		}
		if !errors.Is(inspectionErr, nbs.ErrInvalidFileSignature) {
			t.Fatal("present corrupt archive did not produce the pinned invalid-signature sentinel")
		}
		stage, cause := cp11ObserveEarlyRefusal(t, ctx, dir)
		_, _, stderr := cp11RunCapture(t, ctx, binary, dir, "public-cp11-corrupt-archive-refused", false)
		cp11SafeEarlyRefusalDiagnostic(t, "public-cp11-corrupt-archive-refused", stderr, stage, cause)
		cp11NoSourceValuesInSafeOutput(t, f, "public-cp11-corrupt-archive-refused", "-stdout.json", "-stderr.jsonl")
		t.Logf("PUBLIC_CP11_CORRUPT_ARCHIVE_REFUSED stage=%s reason_subcode=other ordinal_known=false schema_known=false verified_count_known=false actual_error_types_codes=true present_corrupt_archive=true no_publication=true", stage)
	})
	// Original donor still has every root, including the genuinely removed one.
	cp11AssertReopened(t, ctx, f, f.DataDir, "")
}
