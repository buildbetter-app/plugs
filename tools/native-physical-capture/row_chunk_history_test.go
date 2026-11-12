package main

import (
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
	"testing"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	embedded "github.com/dolthub/driver/v2"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
)

const cp11RowPopulation = 1024

// This separate donor leaves the six original CP11 controls unchanged. Every
// populated row changes at v40, so a v39 primary leaf can be removed without
// deleting data needed by any retained image or by the current working root.
func cp11NewRowStore(t *testing.T, ctx context.Context) cp11PublicStore {
	t.Helper()
	f := cp11PublicStore{DataDir: diagnosticDir(t), Images: map[string]physicalcapture.Image{}, Profiles: map[string]plugdump.NativeCommitSchemaProfile{}, Checkpoints: map[int]string{}, Roots: map[string]hash.Hash{}, Chunks: map[hash.Hash]chunks.Chunk{}}
	db, closeDriver, err := openPublicMigrationFixture(f.DataDir)
	cp11Require(t, err, "row donor driver open")
	defer func() { _ = db.Close(); _ = closeDriver() }()
	conn, err := db.Conn(ctx)
	cp11Require(t, err, "row donor connection")
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE `public_commit_history`")
	ownVersionExec(t, ctx, conn, "USE `public_commit_history`")
	_, err = schema.MigrateUpTo(ctx, conn, 39)
	cp11Require(t, err, "row donor migration prefix 39")
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	for _, id := range []string{"public-a1b", "public-c2d"} {
		ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,metadata,timeout_ns,hook_bead,role_bead,payload) VALUES (?,'public row donor','','','','',JSON_OBJECT('n',9007199254740993),9007199254740993,NULL,'','null')", id)
	}
	for i := 0; i < cp11RowPopulation; i++ {
		// Inline sized, distinct public values give the real primary map many
		// leaves without depending on a guessed storage-file size or name.
		title := fmt.Sprintf("public row donor v39 %04d %x", i, sha256.Sum256([]byte(fmt.Sprintf("public row %d", i))))
		ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,metadata,timeout_ns,hook_bead,role_bead,payload) VALUES (?,?,'','','','','null',9007199254740993,NULL,'','null')", fmt.Sprintf("public-row-%04d", i), title)
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO compaction_snapshots(id,issue_id,compaction_level,snapshot_json) VALUES ('public-row-binary','public-a1b',1,?)", []byte{0, 255, 128, 42})
	ownVersionExec(t, ctx, conn, "INSERT INTO dependencies(issue_id,depends_on_id,type,created_by) VALUES ('public-a1b','public-c2d','blocks','public')")
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-row-before-62','public-a1b','updated','public',NULL,'','actual row donor historical event')")
	f.Checkpoints[39] = commitHistoryCommit(t, ctx, conn)
	for _, version := range []int{40, 41, 61, 62, 66} {
		if version == 40 {
			// Migration 40 commits internally before recording its cursor.
			// Stage every rewritten row first so even a scoped repair commit
			// cannot retain a target v39 primary leaf in a newer root.
			ownVersionExec(t, ctx, conn, "UPDATE issues SET title=CONCAT('public row donor rewritten v40 ',id)")
			ownVersionExec(t, ctx, conn, "CALL DOLT_ADD('issues')")
		}
		_, err = schema.MigrateUpTo(ctx, conn, version)
		cp11Require(t, err, "row donor historical migration")
		if version != 40 {
			ownVersionExec(t, ctx, conn, "UPDATE issues SET title=? WHERE id='public-a1b'", fmt.Sprintf("public row donor checkpoint %d", version))
		}
		f.Checkpoints[version] = commitHistoryCommit(t, ctx, conn)
	}
	boundaryIgnoredPrefix(t, ctx, conn)
	commitHistoryCommit(t, ctx, conn)
	// Migration 62 restores the historical event into ignored working events.
	// Remove only that working copy; immutable checkpoints retain the original.
	ownVersionExec(t, ctx, conn, "DELETE FROM events WHERE id=?", "public-row-before-62")
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-row-current-1','public-a1b','updated','public',NULL,'','first row donor current-only attachment'),('public-row-current-2','public-a1b','updated','public','','after','second row donor current-only attachment')")
	f.Log, err = (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	cp11Require(t, err, "row donor complete SQL graph")
	if !f.Log.Clean || len(f.Log.Commits) < 8 || len(commitHistoryStatus(t, ctx, conn)) != 0 {
		t.Fatal("public row donor is not a clean actual multi-commit store")
	}
	f.Working, err = physicalcapture.ReadWorkingRoot(ctx, conn, f.Log.Head)
	cp11Require(t, err, "row donor working root before loss")
	f.PhysicalLog, err = physicalcapture.ReadHistory(ctx, conn, f.Log.Head)
	cp11Require(t, err, "row donor physical graph before loss")
	if !f.PhysicalLog.ReadOnlyScope || len(f.PhysicalLog.Commits) != len(f.Log.Commits) || len(f.PhysicalLog.Boundaries) != 0 {
		t.Fatal("public row donor original physical graph differs from SQL census")
	}
	f.Current = commitHistoryCurrent(t, ctx, conn)
	if len(f.Current["events"].Rows) != 2 || len(f.Current["issues"].Rows) != cp11RowPopulation+2 {
		t.Fatal("public row donor current row/events census differs")
	}
	for _, commit := range f.Log.Commits {
		image, err := physicalcapture.ReadCommit(ctx, conn, commit.ID)
		cp11Require(t, err, "every original row donor immutable image")
		profile, err := plugdump.PhysicalCommitSchemaProfile(image)
		cp11Require(t, err, "every original row donor own-schema profile")
		f.Images[commit.ID], f.Profiles[commit.ID] = image, profile
	}
	cp11AssertHistoricalEvent(t, f, "public-row-before-62")
	cp11Require(t, conn.Close(), "row donor connection close")
	cp11Require(t, db.Close(), "row donor database close")
	cp11Require(t, closeDriver(), "row donor driver close")

	ddb := cp11LoadEngine(t, ctx, f.DataDir)
	defer func() { cp11Require(t, ddb.Close(), "row donor preimage engine close") }()
	cs := datas.ChunkStoreFromDatabase(doltdb.ExposeDatabaseFromDoltDB(ddb))
	f.StoreRoot, err = cs.Root(ctx)
	cp11Require(t, err, "row donor actual Noms root")
	f.Version = cs.Version()
	iter, ok := cs.(interface {
		IterateAllChunks(context.Context, func(chunks.Chunk)) error
	})
	if !ok {
		t.Fatal("public row donor physical chunk enumeration unavailable")
	}
	addressFacts := []map[string]any{}
	var reconstructedAddresses uint64
	cp11Require(t, iter.IterateAllChunks(ctx, func(c chunks.Chunk) {
		if c.IsGhost() || c.IsEmpty() {
			t.Fatal("complete row donor has a ghost/empty chunk")
		}
		// The pinned journal iterator reports only 16 address bytes for cached
		// entries. Derive the full address from the actual enumerated payload;
		// every resulting address is checked against backing storage below.
		canonical := chunks.NewChunk(bytes.Clone(c.Data()))
		address := canonical.Hash()
		var journalPrefix hash.Hash
		copy(journalPrefix[:16], address[:16])
		if c.Hash() != address && c.Hash() != journalPrefix {
			t.Fatal("row donor enumerated address disagrees with actual chunk payload")
		}
		if c.Hash() != address {
			reconstructedAddresses++
		}
		if before, present := f.Chunks[address]; present && !bytes.Equal(before.Data(), canonical.Data()) {
			t.Fatal("row donor content address has inconsistent original bytes")
		}
		f.Chunks[address] = canonical
		addressFacts = append(addressFacts, map[string]any{"reported_address": c.Hash().String(), "content_address": address.String(), "bytes": len(canonical.Data()), "sha256": fmt.Sprintf("%x", sha256.Sum256(canonical.Data()))})
	}), "row donor original chunk census")
	// IterateAllChunks holds the journal read lock, so backing reads must follow
	// enumeration rather than reentering the store from its callback.
	for address, expected := range f.Chunks {
		present, err := cs.Has(ctx, address)
		cp11Require(t, err, "row donor canonical chunk backing presence")
		actual, err := cs.Get(ctx, address)
		cp11Require(t, err, "row donor canonical chunk backing bytes")
		if !present || actual.IsEmpty() || actual.IsGhost() || actual.Hash() != address || hash.Of(actual.Data()) != address || !bytes.Equal(actual.Data(), expected.Data()) {
			t.Fatal("row donor canonical chunk lacks an authenticated backing preimage")
		}
	}
	sort.Slice(addressFacts, func(i, j int) bool {
		if addressFacts[i]["content_address"] == addressFacts[j]["content_address"] {
			return addressFacts[i]["reported_address"].(string) < addressFacts[j]["reported_address"].(string)
		}
		return addressFacts[i]["content_address"].(string) < addressFacts[j]["content_address"].(string)
	})
	cp11JSONArtifact(t, "public-cp11-row-original-chunk-address-census.json", map[string]any{"entries": addressFacts, "enumerated_entry_count": len(addressFacts), "unique_content_chunk_count": len(f.Chunks), "reconstructed_address_count": reconstructedAddresses, "backing_has_get_hash_and_bytes_verified": true})
	for _, commit := range f.Log.Commits {
		root := cp11RowRoot(t, ctx, ddb, commit.ID)
		f.Roots[commit.ID], err = root.HashOf()
		cp11Require(t, err, "row donor original root address")
		if _, present := f.Chunks[f.Roots[commit.ID]]; !present {
			t.Fatal("row donor original root is absent from physical chunk census")
		}
	}
	return f
}

func cp11RowRoot(t *testing.T, ctx context.Context, ddb *doltdb.DoltDB, id string) doltdb.RootValue {
	t.Helper()
	optional, err := ddb.ReadCommit(ctx, hash.Parse(id))
	cp11Require(t, err, "row fixture immutable commit")
	commit, present := optional.ToCommit()
	if !present {
		t.Fatal("row fixture unexpectedly has a ghost commit")
	}
	root, err := commit.GetRootValue(ctx)
	cp11Require(t, err, "row fixture immutable root")
	return root
}

type cp11RowLeaf struct {
	Address      string `json:"address"`
	FirstKey     string `json:"first_key"`
	LastKey      string `json:"last_key"`
	StartOrdinal uint64 `json:"start_ordinal"`
	Rows         uint64 `json:"rows"`
}

// Walk the original immutable root bytes, including table/index/blob refs,
// independently of the engine node cache and of the product's new preflight.
// Commit roots, rather than commit objects, intentionally exclude parent edges.
func cp11RowRootReferences(t *testing.T, f cp11PublicStore) map[string][]string {
	t.Helper()
	roots := map[string]hash.Hash{"working": hash.Parse(f.Working.WorkingRootHash)}
	for id, root := range f.Roots {
		roots[id] = root
	}
	result := map[string][]string{}
	for id, root := range roots {
		seen := map[hash.Hash]bool{root: true}
		queue := []hash.Hash{root}
		for i := 0; i < len(queue); i++ {
			chunk, present := f.Chunks[queue[i]]
			if !present || chunk.IsEmpty() || chunk.IsGhost() || hash.Of(chunk.Data()) != queue[i] {
				t.Fatal("public row donor root closure has a missing or unauthenticated original chunk")
			}
			cp11Require(t, types.WalkAddrsFromNomsValue(chunk, types.Format_DOLT, func(address hash.Hash) error {
				if address.IsEmpty() {
					t.Fatal("public row donor original root closure contains an empty reference")
				}
				if !seen[address] {
					seen[address] = true
					queue = append(queue, address)
				}
				return nil
			}), "independent original immutable root reference walk")
		}
		for address := range seen {
			result[id] = append(result[id], address.String())
		}
		sort.Strings(result[id])
	}
	return result
}

// The leaf census is derived from decoded, actually reachable tree nodes.
// Its keys/hashes are public reference evidence, never safe diagnostic output.
func cp11SelectHistoricalRowLeaf(t *testing.T, ctx context.Context, f cp11PublicStore) (hash.Hash, []cp11RowLeaf, map[string][]string, map[string][]string) {
	t.Helper()
	ddb := cp11LoadEngine(t, ctx, f.DataDir)
	defer func() { cp11Require(t, ddb.Close(), "row leaf census engine close") }()
	references := map[string][]string{}
	leaves := []cp11RowLeaf{}
	for _, id := range cp11NewestFirst(t, f.Log) {
		root := cp11RowRoot(t, ctx, ddb, id)
		table, present, err := root.GetTable(ctx, doltdb.TableName{Name: "issues"})
		cp11Require(t, err, "row leaf actual table")
		if !present {
			references[id] = []string{}
			continue
		}
		index, err := table.GetRowData(ctx)
		cp11Require(t, err, "row leaf actual primary index")
		rows, err := durable.ProllyMapFromIndex(index)
		cp11Require(t, err, "row leaf pinned primary map")
		cp11Require(t, rows.WalkNodes(ctx, func(_ context.Context, node *tree.Node) error {
			address := node.HashOf()
			references[id] = append(references[id], address.String())
			if id != f.Checkpoints[39] || !node.IsLeaf() {
				return nil
			}
			if node.Count() == 0 {
				t.Fatal("populated historical row tree contains an empty leaf")
			}
			first, firstOK := rows.KeyDesc().GetString(0, val.Tuple(node.GetKey(0)))
			last, lastOK := rows.KeyDesc().GetString(0, val.Tuple(node.GetKey(node.Count()-1)))
			if !firstOK || !lastOK {
				t.Fatal("historical row leaf primary key is not independently readable")
			}
			leaves = append(leaves, cp11RowLeaf{Address: address.String(), FirstKey: first, LastKey: last, Rows: uint64(node.Count())})
			return nil
		}), "row leaf actual node census")
		sort.Strings(references[id])
	}
	if len(leaves) < 3 {
		t.Fatal("actual historical primary map does not have multiple removable leaves")
	}
	// Fixture primary keys are ASCII and sorted under the actual id collation;
	// validate that decoded leaf ranges and total census agree independently.
	sort.Slice(leaves, func(i, j int) bool { return leaves[i].FirstKey < leaves[j].FirstKey })
	var total uint64
	for i := range leaves {
		if i > 0 && leaves[i-1].LastKey >= leaves[i].FirstKey {
			t.Fatal("actual historical row leaf ranges overlap")
		}
		leaves[i].StartOrdinal = total
		total += leaves[i].Rows
	}
	if total != cp11RowPopulation+2 {
		t.Fatal("historical row tree leaf census differs from original populated image")
	}
	rootReferences := cp11RowRootReferences(t, f)
	for _, leaf := range leaves[1 : len(leaves)-1] {
		address := hash.Parse(leaf.Address)
		unique := true
		targetReferenced := false
		for id, nodes := range rootReferences {
			if id == f.Checkpoints[39] {
				for _, node := range nodes {
					targetReferenced = targetReferenced || node == leaf.Address
				}
				continue
			}
			for _, node := range nodes {
				unique = unique && node != leaf.Address
			}
		}
		if !unique {
			continue
		}
		if !targetReferenced {
			t.Fatal("selected historical row leaf is not reachable from its actual commit root bytes")
		}
		original, present := f.Chunks[address]
		if !present || original.IsEmpty() || original.IsGhost() || address == f.StoreRoot {
			t.Fatal("historical row leaf lacks an authenticated physical preimage")
		}
		decoded, _, err := tree.NodeFromChunk(&original)
		cp11Require(t, err, "independent original row leaf chunk decode")
		if !decoded.IsLeaf() || uint64(decoded.Count()) != leaf.Rows || decoded.HashOf() != address {
			t.Fatal("selected physical chunk is not the independently observed primary-map leaf")
		}
		for _, root := range f.Roots {
			if address == root {
				t.Fatal("row leaf selection is a commit root rather than row data")
			}
		}
		return address, leaves, references, rootReferences
	}
	t.Fatal("no nonfirst historical primary leaf is unique to the omitted commit")
	return hash.Hash{}, nil, nil, nil
}

// No error/panic value is serialized. Stage and Next counters record the actual
// operation that failed, including pinned eager PartitionRows failures. A panic
// is never converted into a typed missing chunk or successful history boundary.
type cp11RawRowObservation struct {
	Stage              string   `json:"stage"`
	Partitions         uint64   `json:"partitions"`
	PartitionRows      uint64   `json:"partition_rows_calls"`
	EnteredNext        uint64   `json:"entered_next_calls"`
	FailureEnteredNext bool     `json:"failure_entered_next"`
	ReturnedRows       uint64   `json:"returned_rows"`
	Complete           bool     `json:"complete"`
	GoErrorTypes       []string `json:"go_error_types"`
	SQLDoltCodes       []uint16 `json:"sql_dolt_codes"`
	PanicType          *string  `json:"panic_type"`
	CleanupErrorTypes  []string `json:"cleanup_error_types"`
	CleanupPanicTypes  []string `json:"cleanup_panic_types"`
	SchemaVersion      *uint64  `json:"observed_schema_version"`
}

func cp11RawRowCall(operation func() error) (err error, panicType *string) {
	defer func() {
		if caught := recover(); caught != nil {
			name := fmt.Sprintf("%T", caught)
			panicType = &name
		}
	}()
	err = operation()
	return
}

func cp11ObserveActualRows(ctx context.Context, dataDir, commitID, tableName string) (observed cp11RawRowObservation, cause error) {
	observed.GoErrorTypes, observed.SQLDoltCodes = []string{}, []uint16{}
	observed.CleanupErrorTypes, observed.CleanupPanicTypes = []string{}, []string{}
	step := func(stage string, operation func() error) bool {
		observed.Stage = stage
		cause, observed.PanicType = cp11RawRowCall(operation)
		observed.FailureEnteredNext = stage == "row-next" && (cause != nil || observed.PanicType != nil)
		if cause != nil {
			report := safeReport(cause)
			observed.GoErrorTypes = append(observed.GoErrorTypes, report.GoErrorTypes...)
			observed.SQLDoltCodes = append(observed.SQLDoltCodes, report.SQLCodes...)
		}
		return cause == nil && observed.PanicType == nil
	}
	cleanup := func(operation func() error) {
		err, panicType := cp11RawRowCall(operation)
		if err != nil {
			observed.CleanupErrorTypes = append(observed.CleanupErrorTypes, safeReport(err).GoErrorTypes...)
		}
		if panicType != nil {
			observed.CleanupPanicTypes = append(observed.CleanupPanicTypes, *panicType)
		}
	}
	var ddb *doltdb.DoltDB
	if !step("engine-open", func() error {
		fs, err := filesys.LocalFilesysWithWorkingDir(filepath.Join(dataDir, "public_commit_history"))
		if err != nil {
			return err
		}
		ddb, err = doltdb.LoadDoltDBWithParams(ctx, types.Format_DOLT, doltdb.LocalDirDoltDB, fs, map[string]interface{}{"disable_singleton_cache": struct{}{}})
		return err
	}) {
		return
	}
	defer cleanup(ddb.Close)
	// NewNodeStore uses a shared engine cache. The supported purge prevents the
	// original donor's reads from supplying the omitted chunk to this copy.
	ddb.NodeStore().PurgeCaches()
	q := gms.NewContext(ctx)
	var commit *doltdb.Commit
	if !step("commit-read", func() error {
		optional, err := ddb.ReadCommit(q, hash.Parse(commitID))
		if err != nil {
			return err
		}
		var present bool
		commit, present = optional.ToCommit()
		if !present {
			return doltdb.ErrGhostCommitEncountered
		}
		return nil
	}) {
		return
	}
	var root doltdb.RootValue
	if !step("root-read", func() error { var err error; root, err = commit.GetRootValue(q); return err }) {
		return
	}
	var stored *doltdb.Table
	if !step("table-read", func() error {
		var present bool
		var err error
		stored, present, err = root.GetTable(q, doltdb.TableName{Name: tableName})
		if err == nil && !present {
			return doltdb.ErrTableNotFound
		}
		return err
	}) {
		return
	}
	var database sqle.Database
	if !step("database-open", func() error {
		var err error
		database, err = sqle.NewDatabase(ctx, "public_commit_history", env.DbData[context.Context]{Ddb: ddb}, editor.Options{})
		return err
	}) {
		return
	}
	defer cleanup(func() error { database.Close(); return nil })
	var table *sqle.DoltTable
	if !step("table-schema", func() error {
		sch, err := stored.GetSchema(q)
		if err != nil {
			return err
		}
		table, err = sqle.NewDoltTable(q, tableName, sch, stored, database, editor.Options{})
		return err
	}) {
		return
	}
	var locked gms.IndexAddressableTable
	if !step("root-lock", func() error { var err error; locked, err = table.LockedToRoot(q, root); return err }) {
		return
	}
	versionColumn := -1
	if tableName == "schema_migrations" {
		for i, column := range locked.Schema(q) {
			if column.Name == "version" {
				versionColumn = i
			}
		}
	}
	var partitions gms.PartitionIter
	if !step("partitions", func() error { var err error; partitions, err = locked.Partitions(q); return err }) {
		return
	}
	defer cleanup(func() error { return partitions.Close(q) })
	for {
		var partition gms.Partition
		var err error
		if !step("partitions-next", func() error {
			partition, err = partitions.Next(q)
			if err == io.EOF {
				return nil
			}
			return err
		}) {
			return
		}
		if err == io.EOF {
			observed.Stage, observed.Complete = "complete", true
			return
		}
		observed.Partitions++
		var rows gms.RowIter
		observed.PartitionRows++
		if !step("partition-rows", func() error { var err error; rows, err = locked.PartitionRows(q, partition); return err }) {
			if rows != nil {
				cleanup(func() error { return rows.Close(q) })
			}
			return
		}
		for {
			observed.EnteredNext++
			var row gms.Row
			var err error
			if !step("row-next", func() error {
				row, err = rows.Next(q)
				if err == io.EOF {
					return nil
				}
				return err
			}) {
				cleanup(func() error { return rows.Close(q) })
				return
			}
			if err == io.EOF {
				break
			}
			observed.ReturnedRows++
			if versionColumn >= 0 {
				var version uint64
				switch value := row[versionColumn].(type) {
				case int64:
					if value > 0 {
						version = uint64(value)
					}
				case uint64:
					version = value
				case int32:
					if value > 0 {
						version = uint64(value)
					}
				case uint32:
					version = uint64(value)
				}
				if version > 0 && (observed.SchemaVersion == nil || version > *observed.SchemaVersion) {
					observed.SchemaVersion = &version
				}
			}
		}
		if !step("rows-close", func() error { return rows.Close(q) }) {
			return
		}
	}
}

func cp11RowPurgeCaches(t *testing.T, ctx context.Context, dataDir string) {
	t.Helper()
	ddb := cp11LoadEngine(t, ctx, dataDir)
	ddb.NodeStore().PurgeCaches()
	cp11Require(t, ddb.Close(), "independent row fixture cache purge close")
}

func cp11RowRequireNoCleanupFailure(t *testing.T, observed cp11RawRowObservation) {
	t.Helper()
	if len(observed.CleanupErrorTypes) != 0 || len(observed.CleanupPanicTypes) != 0 {
		t.Fatal("actual public row observation had a separate cleanup failure")
	}
}

// Observe the protected product operation, not an unguarded engine iterator.
// This observer fails the positive control if any panic crosses the reader
// boundary; it does not classify a recovered fixture panic as missing data.
type cp11GuardedRowObservation struct {
	Stage           string                      `json:"stage"`
	ReadCommitCalls uint64                      `json:"read_commit_calls"`
	DetachedImage   bool                        `json:"detached_image"`
	ReasonSubcode   string                      `json:"reason_subcode"`
	PanicType       *string                     `json:"panic_type"`
	GoErrorTypes    []string                    `json:"go_error_types"`
	SQLDoltCodes    []uint16                    `json:"sql_dolt_codes"`
	Probe           cp11ProbeObservation        `json:"presence_probe"`
	SchemaCursor    physicalcapture.SchemaImage `json:"schema_cursor"`
}

// Only the target's rows may be unavailable. Its root, schema cursor, actual
// graph and every other complete immutable image must remain readable. This
// direct product observation binds full CLI diagnostics to the original driver
// failure without entering the pinned engine's unguarded eager-fetch path.
func cp11AssertRowReopened(t *testing.T, ctx context.Context, f cp11PublicStore, dataDir, missing string) (error, cp11GuardedRowObservation) {
	t.Helper()
	cp11RowPurgeCaches(t, ctx, dataDir)
	db, closeDriver, err := physicalcapture.Open(dataDir, "public_commit_history")
	cp11Require(t, err, "row loss direct physical open")
	defer func() {
		cp11Require(t, db.Close(), "row loss direct physical database close")
		cp11Require(t, closeDriver(), "row loss direct physical driver close")
	}()
	conn, err := db.Conn(ctx)
	cp11Require(t, err, "row loss direct physical connection")
	defer func() { cp11Require(t, conn.Close(), "row loss direct physical connection close") }()
	working, err := physicalcapture.ReadWorkingRoot(ctx, conn, f.Log.Head)
	cp11Require(t, err, "row loss unchanged working root")
	if !reflect.DeepEqual(working, f.Working) || !ownVersionEqual(t, commitHistoryCurrent(t, ctx, conn), f.Current) {
		t.Fatal("historical row loss changed current roots/cells/events")
	}
	graph, err := physicalcapture.ReadHistory(ctx, conn, f.Log.Head)
	cp11Require(t, err, "row loss root-independent graph")
	if !reflect.DeepEqual(graph, f.PhysicalLog) {
		t.Fatal("historical row loss changed actual commit identity, metadata or adjacency")
	}
	var observedCause error
	observed := cp11GuardedRowObservation{Stage: "immutable-reader-boundary", GoErrorTypes: []string{}, SQLDoltCodes: []uint16{}}
	for _, id := range cp11NewestFirst(t, f.Log) {
		cursor, err := physicalcapture.ReadCommitSchema(ctx, conn, id)
		cp11Require(t, err, "row loss retained immutable schema cursor")
		if cursor.CommitID != id || !cursor.ReadOnlyScope {
			t.Fatal("row loss schema cursor is not bound to its actual immutable root")
		}
		var image physicalcapture.Image
		err, panicType := cp11RawRowCall(func() error {
			var readErr error
			image, readErr = physicalcapture.ReadCommit(ctx, conn, id)
			return readErr
		})
		if panicType != nil {
			t.Fatal("guarded immutable reader let a panic escape its boundary")
		}
		if id == missing {
			if err == nil || image.CommitID != "" || image.ReadOnlyScope || len(image.Tables) != 0 || physicalcapture.UnavailableReason(err) != "missing_table_chunk" {
				t.Fatal("actual missing row chunk did not yield a detached typed table absence; positive boundary remains UNPROVED")
			}
			observedCause = err
			original := f.Images[id]
			var originalCursors []physicalcapture.Table
			for _, table := range original.Tables {
				if table.Name == "schema_migrations" || table.Name == "ignored_schema_migrations" {
					originalCursors = append(originalCursors, table)
				}
			}
			if cursor.RootTableCount != len(original.Tables) || !reflect.DeepEqual(cursor.Tables, originalCursors) {
				t.Fatal("historical row loss changed its independently readable own-schema cursor")
			}
			observed.SchemaCursor = cursor
			observed.ReadCommitCalls++
			observed.DetachedImage = reflect.DeepEqual(image, physicalcapture.Image{})
			observed.ReasonSubcode, observed.PanicType = physicalcapture.UnavailableReason(err), panicType
			report := safeReport(err)
			observed.GoErrorTypes = append(observed.GoErrorTypes, report.GoErrorTypes...)
			observed.SQLDoltCodes = append(observed.SQLDoltCodes, report.SQLCodes...)
			// Full preimage and actual one-object conservation retain every
			// older image's bytes. Never enter its iterator after the boundary.
			break
		} else if err != nil || !reflect.DeepEqual(image, f.Images[id]) {
			t.Fatal("a retained immutable row image changed or became unreadable")
		}
	}
	return observedCause, observed
}

func cp11RowSafeOutputs(t *testing.T, f cp11PublicStore, name string) {
	t.Helper()
	cp11NoSourceValuesInSafeOutput(t, f, name)
	for _, suffix := range []string{"-stdout.json", "-stderr.jsonl", "-probe-stdout.json", "-probe-stderr.jsonl"} {
		data, err := os.ReadFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), name+suffix))
		cp11Require(t, err, "row fixture original safe output readback")
		for _, value := range []string{"public-row-", "public row donor", "row donor current-only attachment"} {
			if bytes.Contains(data, []byte(value)) {
				t.Fatal("safe public row diagnostic/probe output leaked a source row value")
			}
		}
		for address := range f.Chunks {
			if bytes.Contains(data, []byte(address.String())) {
				t.Fatal("safe public row diagnostic/probe output leaked a source chunk address")
			}
		}
	}
}

func cp11AssertRowBoundary(t *testing.T, f cp11PublicStore, archive, evidence, diagnostics []byte, verified map[string]bool, targetCommit, child string, ordinal uint64, cause error) {
	t.Helper()
	native := cp11AssertPreserved(t, f, archive, evidence, verified)
	coverage := native.HistoryCoverage
	if native.Complete || coverage == nil || coverage.Format != plugdump.NativeHistoryCoverageFormat || coverage.Status != "verified-to-boundary" || coverage.VerifiedCommitCount != uint64(len(verified)) || len(coverage.Boundaries) != 1 {
		t.Fatal("actual row loss did not publish exactly the authenticated verified suffix")
	}
	boundary := coverage.Boundaries[0]
	if boundary.CommitID != targetCommit || boundary.Reason != "missing_table_chunk" || boundary.CommitOrdinalFromHead != ordinal || boundary.CommitSchemaVersion == nil || *boundary.CommitSchemaVersion != 39 || len(boundary.SQLDoltCodes) != 0 || !reflect.DeepEqual(boundary.ChildCommitIDs, []string{child}) {
		t.Fatal("row loss boundary fabricated or lost actual adjacency/schema/ordinal/subcode/original SQL codes")
	}
	sourceBytes := boundarySourceMember(t, archive)
	var source plugdump.SourceHistory
	decoder := json.NewDecoder(bytes.NewReader(sourceBytes))
	decoder.UseNumber()
	cp11Require(t, decoder.Decode(&source), "row loss actual source/events attachment")
	if coverage.EventsAttachment.Member != plugdump.SourceHistoryMember || coverage.EventsAttachment.SHA256 != fmt.Sprintf("%x", sha256.Sum256(sourceBytes)) || coverage.EventsAttachment.Rows != 2 || source.Range.Complete || source.Range.Reason != "native-history-boundary" || !ownVersionEqual(t, source.Commits, native.Commits) || !ownVersionEqual(t, source.Events, f.Current["events"].Rows) {
		t.Fatal("row loss suffix lost its exact authenticated current events attachment")
	}
	version := uint64(39)
	cp11SafeBoundaryDiagnostic(t, diagnostics, ordinal, uint64(len(verified)), "missing_table_chunk", &version, cause)
}

func TestPublicCP11HistoricalPrimaryRowLeafLossRetainsVerifiedSuffix(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	home := diagnosticDir(t)
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	f := cp11NewRowStore(t, ctx)
	target, leaves, references, rootReferences := cp11SelectHistoricalRowLeaf(t, ctx, f)
	targetCommit := f.Checkpoints[39]
	if f.Profiles[targetCommit].MainVersion == nil || *f.Profiles[targetCommit].MainVersion != 39 {
		t.Fatal("actual row donor target does not have independently readable schema 39")
	}
	newest := cp11NewestFirst(t, f.Log)
	all, verified := map[string]bool{}, map[string]bool{}
	boundaryIndex := -1
	for i, id := range newest {
		all[id] = true
		if id == targetCommit {
			boundaryIndex = i
		}
	}
	if boundaryIndex < 5 {
		t.Fatal("historical row loss has no populated migration/event suffix")
	}
	for _, id := range newest[:boundaryIndex] {
		verified[id] = true
	}
	ordinal := uint64(boundaryIndex + 1)
	beforeRows, beforeCause := cp11ObserveActualRows(ctx, f.DataDir, targetCommit, "issues")
	cp11JSONArtifact(t, "public-cp11-row-original-iterator-observation.json", beforeRows)
	cp11RowRequireNoCleanupFailure(t, beforeRows)
	if beforeCause != nil || beforeRows.PanicType != nil || !beforeRows.Complete || beforeRows.ReturnedRows != cp11RowPopulation+2 || beforeRows.EnteredNext == 0 {
		t.Fatal("actual original historical row iteration was not complete before removal")
	}
	beforeSchema, beforeSchemaCause := cp11ObserveActualRows(ctx, f.DataDir, targetCommit, "schema_migrations")
	cp11JSONArtifact(t, "public-cp11-row-original-schema-observation.json", beforeSchema)
	cp11RowRequireNoCleanupFailure(t, beforeSchema)
	if beforeSchemaCause != nil || beforeSchema.PanicType != nil || !beforeSchema.Complete || beforeSchema.SchemaVersion == nil || *beforeSchema.SchemaVersion != 39 {
		t.Fatal("actual row donor schema cursor was not independently readable before removal")
	}
	preimage := cp11Tree(t, f.DataDir)
	preimageTar := cp11Tar(t, preimage)
	cp11Artifact(t, "public-cp11-row-original-store-preimage.tar", preimageTar)
	chunkFacts := map[string]map[string]any{}
	for address, chunk := range f.Chunks {
		chunkFacts[address.String()] = map[string]any{"bytes": len(chunk.Data()), "sha256": fmt.Sprintf("%x", sha256.Sum256(chunk.Data()))}
	}
	rootFacts := map[string]string{}
	for id, root := range f.Roots {
		rootFacts[id] = root.String()
	}
	cp11JSONArtifact(t, "public-cp11-row-preimage-and-removal-facts.json", map[string]any{
		"fixture": "actual-public-historical-primary-row-leaf-loss", "dolt_go_pin": "v0.40.5-0.20260806213044-796d07497741", "gms_pin": "v0.20.1-0.20260805191915-e5eafe0da809",
		"preimage_sha256": fmt.Sprintf("%x", sha256.Sum256(preimageTar)), "noms_root": f.StoreRoot.String(), "noms_version": f.Version,
		"full_actual_graph": f.Log, "original_physical_graph": f.PhysicalLog, "commit_root_addresses": rootFacts, "commit_schema_profiles": f.Profiles, "original_physical_images": f.Images,
		"original_working_root": f.Working, "migration_checkpoints": f.Checkpoints, "physical_chunk_manifest": chunkFacts,
		"target_primary_map_leaf_census": leaves, "primary_map_nodes_by_commit": references, "original_root_reference_closures": rootReferences,
		"removed_commit": targetCommit, "removed_row_chunk": target.String(), "removed_chunk_original_bytes": f.Chunks[target].Data(),
		"boundary_child": newest[boundaryIndex-1], "boundary_ordinal_from_head": ordinal, "expected_verified_commits": len(verified),
		"all_original_images_demonstrably_readable": true, "every_populated_row_rewritten_at_schema_version": 40,
	})

	t.Run("populated_unrepacked_complete_positive", func(t *testing.T) {
		name := "public-cp11-row-baseline"
		storage := cp11StorageCounts(t, ctx, f.DataDir)
		cp11RunProbe(t, ctx, binary, f.DataDir, name, uint64(len(all)), uint64(len(all)), nil, storage)
		archive, evidence, _ := cp11RunCapture(t, ctx, binary, f.DataDir, name, true)
		native := cp11AssertPreserved(t, f, archive, evidence, all)
		if !native.Complete || native.HistoryCoverage != nil {
			t.Fatal("populated row donor complete baseline is not complete")
		}
		cp11AssertRowReopened(t, ctx, f, f.DataDir, "")
		cp11RowSafeOutputs(t, f, name)
		t.Logf("PUBLIC_CP11_ROW_BASELINE populated_rows=%d reachable_commits=%d readable_roots=%d events=2 complete=true", cp11RowPopulation+2, len(all), len(all))
	})
	for _, archiveForm := range []bool{false, true} {
		name := "public-cp11-row-loose-missing"
		if archiveForm {
			name = "public-cp11-row-darc-missing"
		}
		t.Run(name, func(t *testing.T) {
			dir, _ := cp11Repack(t, ctx, f, preimage, archiveForm, target)
			cp11Artifact(t, name+"-store.tar", cp11Tar(t, cp11Tree(t, dir)))
			storage := cp11StorageCounts(t, ctx, dir)
			if archiveForm && (storage[0] != uint64(len(f.Chunks)-1) || storage[1] != 0 || storage[2] != 1) || !archiveForm && (storage[0] != 0 || storage[1] != uint64(len(f.Chunks)-1) || storage[2] != 0 || storage[3] != 1) {
				t.Fatal("row loss active storage counts do not match the actual one-object omission")
			}
			// All root values remain readable. Complete closure checks must
			// nevertheless find this actual absent primary leaf before rows.
			version := uint64(39)
			probe := cp11RunProbeBoundary(t, ctx, binary, dir, name, uint64(len(all)), uint64(len(all)), uint64(len(all)-1), &ordinal, "missing_table_chunk", &version, storage)
			cp11JSONArtifact(t, name+"-raw-iteration-proof-status.json", map[string]any{
				"raw_row_next_missing_table_chunk_proved":       false,
				"raw_row_next_proof_state":                      "UNPROVED",
				"raw_issues_iteration_attempted_after_omission": false,
				"proof_route": "probe-before-reader",
				"generic_panic_or_refusal_is_positive_iteration_proof": false,
			})
			cause, guarded := cp11AssertRowReopened(t, ctx, f, dir, targetCommit)
			guarded.Probe = probe
			// The fixed cursor reader uses direct prolly iteration. Preserve
			// schema39 evidence without invoking raw PartitionRows at this
			// accepted missing-chunk boundary.
			cp11JSONArtifact(t, name+"-independent-schema-observation.json", guarded.SchemaCursor)
			cp11JSONArtifact(t, name+"-guarded-reader-observation.json", guarded)
			if guarded.ReadCommitCalls != 1 || !guarded.DetachedImage || guarded.PanicType != nil || guarded.ReasonSubcode != "missing_table_chunk" || len(guarded.GoErrorTypes) == 0 || len(guarded.SQLDoltCodes) != 0 {
				t.Fatal("actual row loss did not yield a detached typed missing boundary without an escaped panic")
			}
			cp11JSONArtifact(t, name+"-independent-reader-cause.json", safeReport(cause))
			// A safe generic refusal cannot satisfy this positive boundary test.
			// Only the independently authenticated missing-table classification
			// with the exact suffix, schema, original cause and events may pass.
			archive, evidence, stderr := cp11RunCapture(t, ctx, binary, dir, name, true)
			cp11AssertRowBoundary(t, f, archive, evidence, stderr, verified, targetCommit, newest[boundaryIndex-1], ordinal, cause)
			cp11RowSafeOutputs(t, f, name)
			cp11AssertRowReopened(t, ctx, f, dir, targetCommit)
			falseComplete := cp11FalseCompleteArchive(t, archive)
			cp11Artifact(t, name+"-false-complete-refused.tar", falseComplete)
			if _, err := plugdump.Validate(bytes.NewReader(falseComplete)); err == nil {
				t.Fatal("actual missing-row archive accepted false semantic completeness with valid checksums")
			}
			t.Logf("PUBLIC_CP11_GENUINE_ROW_LEAF_REMOVAL archive_form=%t removed_chunks=1 reachable_commits=%d readable_roots=%d verified_commits=%d boundary_ordinal_from_head=%d reason_subcode=missing_table_chunk commit_schema_version=39 events=2 preserved=true false_complete_refused=true guarded_read_panic=false raw_next_missing_proved=false", archiveForm, len(all), len(all), len(verified), ordinal)
		})
	}
	cp11AssertRowReopened(t, ctx, f, f.DataDir, "")
}

// This separate refusal donor contains no missing object. Its malformed ENUM
// is written through the pinned tuple/map/table/root/commit serializers, so a
// complete presence walk must not be mistaken for readable SQL-domain values.
// The original valid current root is restored by a real child commit; opening
// the store and reading current inventory cannot cause the intended refusal.
func cp11InstallPresentEnumRefusal(t *testing.T, ctx context.Context, f cp11PublicStore) (badCommit, head string) {
	t.Helper()
	var validEnumCommit, restoredCommit string
	func() {
		db, closeDriver, err := physicalcapture.Open(f.DataDir, "public_commit_history")
		cp11Require(t, err, "present ENUM donor open")
		defer func() {
			cp11Require(t, db.Close(), "present ENUM donor database close")
			cp11Require(t, closeDriver(), "present ENUM donor driver close")
		}()
		conn, err := db.Conn(ctx)
		cp11Require(t, err, "present ENUM donor connection")
		defer func() { cp11Require(t, conn.Close(), "present ENUM donor connection close") }()
		ownVersionExec(t, ctx, conn, "CREATE TABLE cp11_present_enum_refusal (id INT PRIMARY KEY, domain_value ENUM('public-valid') NOT NULL)")
		ownVersionExec(t, ctx, conn, "INSERT INTO cp11_present_enum_refusal VALUES (1,'public-valid')")
		validEnumCommit = commitHistoryCommit(t, ctx, conn)
		valid, err := physicalcapture.ReadCommit(ctx, conn, validEnumCommit)
		cp11Require(t, err, "valid actual ENUM historical image")
		if valid.CommitID != validEnumCommit || !valid.ReadOnlyScope {
			t.Fatal("valid ENUM control did not produce an authenticated immutable image")
		}
		ownVersionExec(t, ctx, conn, "DROP TABLE cp11_present_enum_refusal")
		// Stage this real deletion directly. The shared migration helper
		// force-stages prior table names, including this now-absent table.
		ownVersionExec(t, ctx, conn, "CALL DOLT_ADD('-A')")
		ownVersionExec(t, ctx, conn, "CALL DOLT_COMMIT('-m','restore public current root after valid ENUM template','--author','Public fixture <fixture@example.test>')")
		cp11Require(t, conn.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&restoredCommit), "actual restored-root commit address")
	}()

	ddb := cp11LoadEngine(t, ctx, f.DataDir)
	defer func() { cp11Require(t, ddb.Close(), "present ENUM serializer engine close") }()
	validRoot := cp11RowRoot(t, ctx, ddb, validEnumCommit)
	currentRoot := cp11RowRoot(t, ctx, ddb, restoredCommit)
	name := doltdb.TableName{Name: "cp11_present_enum_refusal"}
	stored, present, err := validRoot.GetTable(ctx, name)
	cp11Require(t, err, "actual ENUM table before mutation")
	if !present {
		t.Fatal("actual ENUM historical table is absent")
	}
	definition, err := stored.GetSchema(ctx)
	cp11Require(t, err, "actual ENUM schema before mutation")
	column, present := definition.GetNonPKCols().GetByName("domain_value")
	if !present {
		t.Fatal("actual ENUM definition is absent")
	}
	domain, ok := column.TypeInfo.ToSqlType().(gms.EnumType)
	if !ok {
		t.Fatal("actual historical column is not an ENUM")
	}
	if value, valid := domain.At(1); !valid || value != "public-valid" {
		t.Fatal("actual ENUM definition lost its single valid member")
	}
	if _, valid := domain.At(2); valid {
		t.Fatal("ENUM refusal value is valid in the actual SQL definition")
	}
	index, err := stored.GetRowData(ctx)
	cp11Require(t, err, "actual ENUM row data before mutation")
	rows, err := durable.ProllyMapFromIndex(index)
	cp11Require(t, err, "actual ENUM primary map before mutation")
	if len(rows.ValDesc().Types) != 1 || rows.ValDesc().Types[0].Enc != val.EnumEnc {
		t.Fatal("actual ENUM value descriptor is not the expected single enum")
	}
	iter, err := rows.IterAll(ctx)
	cp11Require(t, err, "actual ENUM primary row iterator")
	key, original, err := iter.Next(ctx)
	cp11Require(t, err, "actual ENUM primary row before mutation")
	key, original = append(val.Tuple{}, key...), append(val.Tuple{}, original...)
	if value, valid := rows.ValDesc().GetEnum(0, original); !valid || value != 1 {
		t.Fatal("original persisted ENUM tuple does not contain the valid member")
	}
	if _, _, err := iter.Next(ctx); err != io.EOF {
		t.Fatal("actual ENUM refusal donor does not have exactly one row")
	}
	builder := val.NewTupleBuilder(rows.ValDesc(), rows.NodeStore())
	builder.PutEnum(0, 2)
	invalid, err := builder.Build(ctx, rows.NodeStore().Pool())
	cp11Require(t, err, "pinned out-of-domain ENUM tuple serialization")
	mutated := rows.Mutate()
	cp11Require(t, mutated.Put(ctx, key, invalid), "pinned ENUM primary-map mutation")
	badRows, err := mutated.Map(ctx)
	cp11Require(t, err, "pinned ENUM primary-map serialization")
	badTable, err := stored.UpdateRows(ctx, durable.IndexFromProllyMap(badRows))
	cp11Require(t, err, "pinned ENUM table serialization")
	badRoot, err := validRoot.PutTable(ctx, name, badTable)
	cp11Require(t, err, "pinned ENUM immutable-root serialization")
	_, badRootHash, err := ddb.WriteRootValue(ctx, badRoot)
	cp11Require(t, err, "pinned ENUM immutable-root write")
	_, currentRootHash, err := ddb.WriteRootValue(ctx, currentRoot)
	cp11Require(t, err, "original valid current-root write")
	branches, err := ddb.GetBranches(ctx)
	cp11Require(t, err, "actual ENUM donor branch census")
	if len(branches) != 1 {
		t.Fatal("actual ENUM donor does not have one unambiguous branch")
	}
	parent, err := ddb.ResolveCommitRef(ctx, branches[0])
	cp11Require(t, err, "actual ENUM donor branch head")
	parentHash, err := parent.HashOf()
	cp11Require(t, err, "actual ENUM donor branch-head address")
	if parentHash.String() != restoredCommit {
		t.Fatal("ENUM donor branch differs from the observed restored commit")
	}
	// The valid SQL template proves the real descriptor/conversion, but is
	// only construction material. Append this separate control to the
	// original donor head so its reachable ancestry has exactly two new
	// commits: the malformed root and its valid restored child.
	cp11Require(t, ddb.SetHead(ctx, branches[0], hash.Parse(f.Log.Head)), "restore original donor ancestry before ENUM control")
	meta, err := datas.NewCommitMeta("Public CP11", "public@example.test", "persist present out-of-domain ENUM control")
	cp11Require(t, err, "public ENUM refusal commit metadata")
	bad, err := ddb.Commit(ctx, badRootHash, branches[0], meta)
	cp11Require(t, err, "real present out-of-domain ENUM commit")
	badHash, err := bad.HashOf()
	cp11Require(t, err, "actual ENUM refusal commit address")
	meta, err = datas.NewCommitMeta("Public CP11", "public@example.test", "restore original valid current root after ENUM control")
	cp11Require(t, err, "public restored-root commit metadata")
	current, err := ddb.Commit(ctx, currentRootHash, branches[0], meta)
	cp11Require(t, err, "real valid child of ENUM refusal commit")
	headHash, err := current.HashOf()
	cp11Require(t, err, "actual restored child commit address")

	// Decode the persisted bytes through the pinned map API rather than
	// trusting the builder inputs or inventing a chunk-address preimage.
	persistedRoot, err := bad.GetRootValue(ctx)
	cp11Require(t, err, "persisted malformed ENUM root readback")
	persisted, present, err := persistedRoot.GetTable(ctx, name)
	cp11Require(t, err, "persisted malformed ENUM table readback")
	if !present {
		t.Fatal("persisted malformed ENUM table is absent")
	}
	persistedIndex, err := persisted.GetRowData(ctx)
	cp11Require(t, err, "persisted malformed ENUM primary index")
	persistedRows, err := durable.ProllyMapFromIndex(persistedIndex)
	cp11Require(t, err, "persisted malformed ENUM primary map")
	persistedIter, err := persistedRows.IterAll(ctx)
	cp11Require(t, err, "persisted malformed ENUM tuple iterator")
	persistedKey, persistedValue, err := persistedIter.Next(ctx)
	cp11Require(t, err, "persisted malformed ENUM tuple readback")
	if !bytes.Equal(persistedKey, key) || !bytes.Equal(persistedValue, invalid) || rows.HashOf() == persistedRows.HashOf() {
		t.Fatal("persisted ENUM control differs from its real serialized tuple mutation")
	}
	if value, valid := persistedRows.ValDesc().GetEnum(0, persistedValue); !valid || value != 2 {
		t.Fatal("persisted ENUM control did not retain the out-of-domain primitive")
	}
	cp11JSONArtifact(t, "public-cp11-present-enum-refusal-facts.json", map[string]any{
		"fixture": "actual-persisted-present-out-of-domain-enum", "valid_template_commit": validEnumCommit,
		"original_reachable_head": f.Log.Head, "template_commits_reachable": false,
		"valid_current_commit_before_mutation": restoredCommit, "malformed_commit": badHash.String(), "restored_child_commit": headHash.String(),
		"original_primary_root": rows.HashOf().String(), "malformed_primary_root": persistedRows.HashOf().String(),
		"actual_enum_members": []string{"public-valid"}, "original_tuple": original, "persisted_malformed_tuple": persistedValue,
		"persisted_enum_index": 2, "missing_chunk_injected": false, "runtime_fault_hook": false,
	})
	return badHash.String(), headHash.String()
}

func TestPublicCP11CompleteChunkProbeReadFailureRefusesWithoutBoundary(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	home := diagnosticDir(t)
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	f := cp11NewPublicStore(t, ctx)
	badCommit, head := cp11InstallPresentEnumRefusal(t, ctx, f)
	cp11RowPurgeCaches(t, ctx, f.DataDir)
	cp11Artifact(t, "public-cp11-present-enum-refusal-store.tar", cp11Tar(t, cp11Tree(t, f.DataDir)))
	db, closeDriver, err := physicalcapture.Open(f.DataDir, "public_commit_history")
	cp11Require(t, err, "complete-probe refusal reader open")
	defer func() {
		cp11Require(t, db.Close(), "complete-probe refusal database close")
		cp11Require(t, closeDriver(), "complete-probe refusal driver close")
	}()
	conn, err := db.Conn(ctx)
	cp11Require(t, err, "complete-probe refusal connection")
	defer func() { cp11Require(t, conn.Close(), "complete-probe refusal connection close") }()
	log, err := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	cp11Require(t, err, "complete-probe refusal actual graph")
	newest := cp11NewestFirst(t, log)
	if !log.Clean || log.Head != head || len(newest) != len(f.Log.Commits)+2 || len(newest) < 3 || newest[1] != badCommit || newest[2] != f.Log.Head || len(commitHistoryStatus(t, ctx, conn)) != 0 || !ownVersionEqual(t, commitHistoryCurrent(t, ctx, conn), f.Current) {
		t.Fatal("present ENUM refusal donor changed current inventory or failed to persist the real parent/child graph")
	}
	name := "public-cp11-probe-complete-read-fails-refused"
	storage := cp11StorageCounts(t, ctx, f.DataDir)
	probe := cp11RunProbe(t, ctx, binary, f.DataDir, name, uint64(len(newest)), uint64(len(newest)), nil, storage)
	var image physicalcapture.Image
	cause, panicType := cp11RawRowCall(func() error {
		var readErr error
		image, readErr = physicalcapture.ReadCommit(ctx, conn, badCommit)
		return readErr
	})
	var inconsistent *embedded.PhysicalStoreInconsistent
	if panicType != nil || cause == nil || !errors.As(cause, &inconsistent) || inconsistent.Cause == nil || inconsistent.PanicType != "" || !reflect.DeepEqual(image, physicalcapture.Image{}) || physicalcapture.UnavailableReason(cause) != "" {
		t.Fatal("presence-complete malformed ENUM did not yield a detached typed inconsistent-store refusal without a panic or missing-history boundary")
	}
	direct := safeReport(cause)
	if direct.ReasonSubcode == nil || *direct.ReasonSubcode != "inconsistent_store" || len(direct.GoErrorTypes) == 0 {
		t.Fatal("actual complete-probe read failure lost its safe typed refusal diagnostic")
	}
	cp11JSONArtifact(t, name+"-guarded-reader-observation.json", map[string]any{
		"presence_probe": probe, "panic_type": panicType, "detached_image": reflect.DeepEqual(image, physicalcapture.Image{}),
		"typed_inconsistent_store": errors.As(cause, &inconsistent), "reader_recovered_panic_type": inconsistent.PanicType,
		"missing_history_reason": physicalcapture.UnavailableReason(cause), "failure": direct,
	})
	_, _, diagnostics := cp11RunCapture(t, ctx, binary, f.DataDir, name, false)
	typedRefusal := false
	for _, report := range decodeSafeReports(t, diagnostics) {
		if report.HistoryBoundaryReason != "" || report.VerifiedCommitCount != nil {
			t.Fatal("presence-complete read failure was admitted as a history boundary")
		}
		for _, typ := range report.GoErrorTypes {
			if typ == "*embedded.PhysicalStoreInconsistent" {
				typedRefusal = true
				if report.ReasonSubcode == nil || *report.ReasonSubcode != "inconsistent_store" || report.CommitOrdinalFromHead == nil || *report.CommitOrdinalFromHead != 2 || report.CommitSchemaVersion == nil || *report.CommitSchemaVersion != 66 || !reflect.DeepEqual(append([]uint16{}, report.SQLCodes...), append([]uint16{}, direct.SQLCodes...)) {
					t.Fatal("typed inconsistent-store refusal lost actual ordinal/schema/codes")
				}
			}
		}
	}
	if !typedRefusal {
		t.Fatal("actual complete-probe read-failure capture omitted the typed inconsistent-store refusal")
	}
	good, err := physicalcapture.ReadCommit(ctx, conn, head)
	cp11Require(t, err, "original valid current image after typed refusal")
	if good.CommitID != head || !good.ReadOnlyScope || !ownVersionEqual(t, commitHistoryCurrent(t, ctx, conn), f.Current) || len(commitHistoryStatus(t, ctx, conn)) != 0 {
		t.Fatal("refused capture changed current data or left source state dirty")
	}
	cp11NoSourceValuesInSafeOutput(t, f, name)
	t.Logf("PUBLIC_CP11_COMPLETE_PROBE_READ_FAILURE_REFUSED reachable_commits=%d readable_roots=%d chunk_complete_commits=%d first_unreadable=null typed_inconsistent_store=true reason_subcode=inconsistent_store commit_ordinal_from_head=2 commit_schema_version=66 no_panic=true no_history_boundary=true no_publication=true current_preserved=true", probe.ReachableCommitCount, probe.ReadableRootCount, probe.ChunkCompleteCommitCount)
}
