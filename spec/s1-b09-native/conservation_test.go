package b09native

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func b09RecordKey(t *testing.T, ref model.Ref) string {
	t.Helper()
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// JSON comparison retains every field and value, including explicit nulls.
// It ignores only object member ordering, never paths, provenance or controls.
func b09Equal(t *testing.T, label string, want, got any) {
	t.Helper()
	encode := func(value any) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var tree any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&tree); err != nil {
			t.Fatal(err)
		}
		raw, err = json.Marshal(tree)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	a, b := encode(want), encode(got)
	if !bytes.Equal(a, b) {
		t.Fatalf("%s differs\nwant=%s\ngot=%s", label, a, b)
	}
}

func assertB09Source(t *testing.T, archive *dumpformat.Archive, profile driver.RestoreImportProfile, derivedDigest string, started, finished time.Time, stage b09ReplayStage, indexesBuilt bool) {
	t.Helper()
	wantRecords := map[string]driver.Record{}
	add := func(id, kind string, body any) {
		ref := model.RefByID(id, model.Natural64(0))
		key := b09RecordKey(t, ref)
		if _, exists := wantRecords[key]; exists {
			t.Fatalf("duplicate expected restored identity %s", id)
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		wantRecords[key] = driver.Record{Ref: ref, Kind: kind, Revision: model.Natural64(1), Body: raw}
	}
	for _, source := range archive.Records("bead") {
		// This named native corpus has open tasks, no labels, links or comments.
		if source.Source.Table != "issues" || source.Source.Fields["status"] != "open" {
			t.Fatalf("unexpected native corpus row: %+v", source)
		}
		fields := map[string]any{"status": "todo"}
		for _, name := range []string{"title", "description", "priority"} {
			fields[name] = source.Body[name]
		}
		metadata := map[string]json.RawMessage{}
		addMetadata := func(name string, value any) {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			metadata[name] = raw
		}
		for _, name := range dumpformat.OpaqueIssueFields() {
			if value, ok := source.Source.Fields[name]; ok {
				addMetadata("beads."+name, value)
			}
		}
		for name, value := range source.Body {
			switch name {
			case "title", "description", "priority", "metadata", "labels":
				continue
			}
			addMetadata("beads.source."+name, value)
		}
		addMetadata("beads.source.status", "open")
		addMetadata("beads.source_fields", source.Source.Fields)
		created, err := time.Parse(time.RFC3339Nano, fmt.Sprint(source.Source.Fields["created_at"]))
		if err != nil {
			t.Fatal(err)
		}
		want := pipeline.Document{ID: source.ID, Type: "issue", Project: "b09", Fields: fields, Metadata: metadata, CreatedAt: &created}
		add(source.ID, "bead", want)
	}
	for _, kind := range []string{"edge", "comment", "memory", "config", "beads-archive"} {
		for _, source := range archive.Records(kind) {
			want := map[string]any{}
			for k, v := range source.Body {
				want[k] = v
			}
			want["source_id"], want["source_table"], want["source"] = source.ID, source.Source.Table, source.Source.Fields
			if kind == "beads-archive" {
				want["read_only"], want["redactable"] = true, true
			}
			id := "source/" + kind + "/" + source.ID
			if profile.SourcePlanes != nil {
				identity, err := json.Marshal([]string{source.Kind, source.Project, source.Source.Table, source.ID})
				if err != nil {
					t.Fatal(err)
				}
				id = "source/" + kind + "/" + hashBytes(identity)
				want["source_project"] = source.Project
			}
			add(id, kind, want)
		}
	}
	manifest := archive.Manifest()
	members := map[string]dumpformat.File{}
	for name, member := range manifest.Files {
		members[name] = member
	}
	rawManifest, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("validated archive lost manifest")
	}
	members["manifest.json"] = dumpformat.File{SHA256: hashBytes(rawManifest), Size: int64(len(rawManifest))}
	imageProfile := "builtin-head/1"
	if migration, _ := archive.ImportMigration(); migration != nil {
		imageProfile = "beads-compat-union-head/1"
	}
	add("restore/archive", "restore-archive", map[string]any{
		"archive_sha256": archive.SHA256(), "derived_sha256": derivedDigest,
		"profile": imageProfile, "source_store": manifest.StoreID, "members": members,
	})
	projects := []any{}
	for _, project := range manifest.Projects {
		allocator := driver.NewHashAllocator(project.Prefix, 3, 8, 0.25)
		projects = append(projects, map[string]any{"sourceAllocator": project, "targetAllocator": allocator})
		add("instance_config/restore-import/project/"+project.Prefix, "instance_config", map[string]any{
			"project": project.Prefix, "importProfile": profile,
			"metadata": map[string]any{"plug.restore.sourceAllocator": project}, "targetAllocator": allocator,
		})
	}
	requestHash, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	add("instance_config/restore-import/binding", "instance_config", map[string]any{"profile": profile, "request_hash": requestHash, "projects": projects})
	if profile.SourcePlanes != nil {
		add(driver.SourcePlaneCommitRecordID, "instance_config", b09ExpectedSourceCommit(t, profile, stage))
	}
	wantKeys, recordKeys, versionKeys := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for key := range wantRecords {
		wantKeys[key] = true
	}
	for key := range stage.Records {
		recordKeys[key] = true
	}
	for key := range stage.Versions {
		versionKeys[key] = true
	}
	b09Equal(t, "complete initial restored record identity set", wantKeys, recordKeys)
	b09Equal(t, "complete initial restored version identity set", wantKeys, versionKeys)
	if stage.Meta.Head.String() != "1" || stage.Meta.HistoryFloor.String() != "1" || stage.Meta.StoreID != profile.SourceStoreID {
		t.Fatalf("snapshot metadata differs from admitted source: %+v", stage.Meta)
	}
	receipt := stage.Receipts[profile.Method]
	if receipt == nil || receipt.RequestHash != requestHash || receipt.Changeset.Revision.String() != "1" || !reflect.DeepEqual(receipt.Changeset.Principal, profile.Admission.Actor) {
		t.Fatal("initial restore receipt does not bind the admitted request")
	}
	// Snapshot restore chooses wall time internally. Bind every version to the
	// committed restore receipt and bound that instant by the actual call.
	stamp := receipt.Changeset.Time
	if stamp.Before(started) || stamp.After(finished) {
		t.Fatalf("restore time %s falls outside call [%s,%s]", stamp, started, finished)
	}
	for key, want := range wantRecords {
		got := stage.Records[key]
		b09Equal(t, "complete initial restored body "+key, json.RawMessage(want.Body), json.RawMessage(got.Body))
		want.Body, got.Body = nil, nil
		b09Equal(t, "complete initial restored record envelope "+key, want, got)
		versions := stage.Versions[key]
		if len(versions) != 1 {
			t.Fatalf("snapshot restore must have one base for %s: %+v", key, versions)
		}
		version := versions[0]
		b09Equal(t, "complete initial snapshot version body "+key, json.RawMessage(wantRecords[key].Body), json.RawMessage(version.Body))
		version.Body = nil
		b09Equal(t, "complete initial snapshot version envelope "+key, driver.Version{Ref: want.Ref, Revision: want.Revision, Base: true, CreatedAt: stamp, Principal: profile.Admission.Actor}, version)
	}
	if stage.SQL != nil {
		assertB09InitialSQL(t, stage, wantRecords, indexesBuilt)
	}
}

func assertB09ResultPair(t *testing.T, label, id string, commit model.Commit, want driver.Record) {
	t.Helper()
	if len(commit.Results) != 2 || commit.Revision.IsZero() {
		t.Fatalf("%s must publish patch and metadata results: %+v", label, commit)
	}
	for i, result := range commit.Results {
		if result.Ref.ID != id || !result.Ref.Branch.IsZero() || result.Revision.String() != commit.Revision.String() || len(result.Record) == 0 {
			t.Fatalf("%s result %d changed identity/revision: %+v", label, i, result)
		}
		b09Equal(t, fmt.Sprintf("%s result %d complete expected state", label, i), json.RawMessage(want.Body), json.RawMessage(result.Record))
	}
	if !bytes.Equal(commit.Results[0].Record, commit.Results[1].Record) {
		t.Fatalf("%s per-operation results disagree on committed state", label)
	}
}

func assertB09Reopen(t *testing.T, before, after b09ReplayStage) {
	t.Helper()
	b09Equal(t, "reopen metadata", before.Meta, after.Meta)
	b09Equal(t, "reopen all records", before.Records, after.Records)
	b09Equal(t, "reopen all permanent versions", before.Versions, after.Versions)
	b09Equal(t, "reopen full changesets", before.Changesets, after.Changesets)
	b09Equal(t, "reopen allocator inventory and history binding", before.Allocator, after.Allocator)
	b09Equal(t, "reopen all known receipts", before.Receipts, after.Receipts)
	b09Equal(t, "reopen complete source checkpoints", before.Checkpoints, after.Checkpoints)
	if after.LeaseEpoch != before.LeaseEpoch+1 {
		t.Fatalf("reopen lease epoch %d -> %d", before.LeaseEpoch, after.LeaseEpoch)
	}
	if before.SQL != nil {
		assertB09SQLReopen(t, before.SQL, after.SQL)
	}
}

func (r *b09ReplayTarget) assertMutation(before, after b09ReplayStage, id, operation string) driver.Record {
	r.t.Helper()
	t := r.t
	key := b09RecordKey(t, model.RefByID(id, model.Natural64(0)))
	prior, exists := before.Records[key]
	creating := strings.HasPrefix(operation, "create-")
	if exists == creating {
		t.Fatalf("%s preimage existence=%v", operation, exists)
	}
	head, err := before.Meta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	wantMeta := before.Meta
	wantMeta.Head = model.Natural64(head + 1)
	b09Equal(t, operation+" exact metadata delta", wantMeta, after.Meta)
	b09Equal(t, operation+" preserves complete source checkpoints", before.Checkpoints, after.Checkpoints)
	wantAllocator := before.Allocator
	wantAllocator.At = wantMeta.Head
	b09Equal(t, operation+" exact allocator delta", wantAllocator, after.Allocator)
	var wantDoc pipeline.Document
	method := map[string]string{"create-first": "bead.create", "create-after-reopen": "bead.create", "close": "bead.close", "compact": "admin.compact", "revert": "history.revert"}[operation]
	if creating {
		label := strings.TrimPrefix(operation, "create-")
		stamp := r.now
		wantDoc = pipeline.Document{ID: id, Type: "issue", Project: "b09", Fields: map[string]any{"title": "Plug native continuation " + label, "description": "continuation after accepted native restore", "status": "todo", "priority": 2, "template": false}, CreatedAt: &stamp}
	} else {
		wantDoc, err = pipeline.DecodeDocument(prior.Body)
		if err != nil {
			t.Fatal(err)
		}
		switch operation {
		case "close":
			stamp := r.now
			wantDoc.Fields["status"] = "done"
			wantDoc.ClosedAt = &stamp
			reason := "B09 native paired close"
			wantDoc.CloseReason = &reason
			wantDoc.Comments = append(wantDoc.Comments, pipeline.Comment{Body: "B09 native paired close", Principal: b09Principal, Time: r.now})
		case "compact":
			wantDoc.Fields["description"] = "native compacted summary"
			raw, err := json.Marshal(map[string]any{"format": "plug-compaction/1", "tier": 1, "sourceRevision": before.Meta.Head, "time": r.now, "originalSize": len("B09 accepted native recorder")})
			if err != nil {
				t.Fatal(err)
			}
			wantDoc.Metadata["plug.compaction"] = raw
		case "revert":
			wantDoc.Fields["description"] = "B09 accepted native recorder"
			delete(wantDoc.Metadata, "plug.compaction")
		default:
			t.Fatalf("unknown exact B09 delta %s", operation)
		}
	}
	wantBody, err := json.Marshal(wantDoc)
	if err != nil {
		t.Fatal(err)
	}
	wantRecords := map[string]driver.Record{}
	for k, row := range before.Records {
		wantRecords[k] = row
	}
	wantRecords[key] = driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: wantMeta.Head, Body: wantBody}
	if len(wantRecords) != len(after.Records) {
		t.Fatalf("%s changed unrelated record cardinality", operation)
	}
	for k, want := range wantRecords {
		got, ok := after.Records[k]
		if !ok {
			t.Fatalf("%s lost record %s", operation, k)
		}
		if k == key {
			b09Equal(t, operation+" target envelope", want.Ref, got.Ref)
			if got.Kind != want.Kind || got.Revision.String() != want.Revision.String() {
				t.Fatalf("%s target envelope changed", operation)
			}
			b09Equal(t, operation+" complete target body", json.RawMessage(want.Body), json.RawMessage(got.Body))
		} else {
			b09Equal(t, operation+" unaffected record "+k, want, got)
		}
	}
	if len(after.Versions) != len(wantRecords) {
		t.Fatalf("%s version identity set changed", operation)
	}
	for k, got := range after.Versions {
		old := before.Versions[k]
		if k != key {
			b09Equal(t, operation+" unaffected permanent history "+k, old, got)
			continue
		}
		if len(got) != len(old)+1 {
			t.Fatalf("%s must append exactly one version", operation)
		}
		if len(old) != 0 {
			b09Equal(t, operation+" retained version prefix", old, got[:len(old)])
		}
		latest := got[len(got)-1]
		wantVersion := driver.Version{Ref: r.expectedVersionRef(id, creating), Revision: wantMeta.Head, Base: creating, CreatedAt: r.now, Principal: b09Principal}
		if creating {
			wantVersion.Body = wantBody
		} else {
			wantVersion.Patch, err = json.Marshal([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(wantBody)}})
			if err != nil {
				t.Fatal(err)
			}
		}
		assertB09Version(t, operation+" complete appended version", wantVersion, latest)
		r.assertHistoryState(operation+"-current", after.Meta, wantRecords[key], wantVersion)
		if !creating {
			r.assertHistoryState(operation+"-preimage", before.Meta, prior, old[len(old)-1])
		}
	}
	if len(after.Changesets) != len(before.Changesets)+1 {
		t.Fatalf("%s must append exactly one changeset", operation)
	}
	if len(before.Changesets) != 0 {
		b09Equal(t, operation+" retained changeset prefix", before.Changesets, after.Changesets[:len(before.Changesets)])
	}
	last := after.Changesets[len(after.Changesets)-1]
	wantKind := model.ChangesetWrite
	if operation == "revert" {
		wantKind = model.ChangesetRevert
	}
	if last.Revision.String() != wantMeta.Head.String() || last.Incarnation != before.Meta.Incarnation || last.Kind != wantKind || !reflect.DeepEqual(last.Principal, b09Principal) || !last.Time.Equal(r.now) || last.IdempotencyKey != b09Meta(r.engine, operation).IdempotencyKey || len(last.Changes) != 1 || last.Changes[0].Ref.ID != id || last.Changes[0].FromRevision.String() != prior.Revision.String() {
		t.Fatalf("%s unexpected complete changeset: %+v", operation, last)
	}
	if len(after.Receipts) != len(before.Receipts)+1 {
		t.Fatalf("%s must append exactly one receipt", operation)
	}
	for k, old := range before.Receipts {
		b09Equal(t, operation+" unaffected receipt "+k, old, after.Receipts[k])
	}
	receiptName := method
	if creating {
		receiptName += "/" + strings.TrimPrefix(operation, "create-")
	}
	receipt := after.Receipts[receiptName]
	if receipt == nil || receipt.ResponsePending || receipt.RequestHash == "" || receipt.Commit.Revision.String() != wantMeta.Head.String() {
		t.Fatalf("%s receipt missing/pending: %+v", operation, receipt)
	}
	b09Equal(t, operation+" retained receipt changeset", last, receipt.Changeset)
	if before.SQL != nil {
		r.assertSQLMutation(before, after, id, operation, wantRecords[key])
	}
	return wantRecords[key]
}

func b09SQLCells(t *testing.T, row string) []string {
	t.Helper()
	var cells []string
	if err := json.Unmarshal([]byte(row), &cells); err != nil {
		t.Fatal(err)
	}
	return cells
}
func b09SQLValue(value any) string {
	switch v := value.(type) {
	case []byte:
		return "bytes:" + hex.EncodeToString(v)
	case time.Time:
		return "time:" + v.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}
func b09SQLText(t *testing.T, cell string) string {
	t.Helper()
	kind, value, _ := strings.Cut(cell, ":")
	if kind == "bytes" {
		raw, err := hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	return value
}
func b09SQLColumn(t *testing.T, table b09SQLTable, row, column string) string {
	t.Helper()
	for i, name := range table.Columns {
		if name == column {
			return b09SQLCells(t, row)[i]
		}
	}
	t.Fatalf("SQL column %s missing", column)
	return ""
}
func b09SQLNumber(t *testing.T, value string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(b09SQLText(t, value), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func b09SQLHasColumn(table b09SQLTable, name string) bool {
	for _, column := range table.Columns {
		if column == name {
			return true
		}
	}
	return false
}

// SQLite's objects table carries dense query ranks, so publishing one record
// legitimately rewrites those derived cells on every live row. The recorder
// still binds the complete live identity set, kind, rank values, added-row
// cardinality, and every non-rank cell; only the documented derived columns
// are allowed to differ from the preimage.
func assertB09SQLiteObjectMutation(t *testing.T, operation string, after b09ReplayStage, old, next b09SQLTable, added []string, keys map[string]bool, recordKey string, creating bool) {
	t.Helper()
	if !b09SQLHasColumn(next, "query_order") || !b09SQLHasColumn(next, "query_kind_order") {
		t.Fatalf("%s SQLite objects schema lacks derived query-rank columns", operation)
	}
	if len(added) != boolB09(creating) {
		t.Fatalf("%s SQLite objects exact add cardinality: got=%d want=%d", operation, len(added), boolB09(creating))
	}
	rowsByKey := func(table b09SQLTable) map[string]string {
		rows := map[string]string{}
		for _, row := range table.Rows {
			key := b09SQLText(t, b09SQLColumn(t, table, row, "key"))
			if _, exists := rows[key]; exists {
				t.Fatalf("%s duplicate SQLite objects key %q", operation, key)
			}
			rows[key] = row
		}
		return rows
	}
	oldByKey, newByKey := rowsByKey(old), rowsByKey(next)
	for _, row := range added {
		key := b09SQLText(t, b09SQLColumn(t, next, row, "key"))
		if !keys[key] || creating && key != recordKey {
			t.Fatalf("%s unexpected added SQLite objects key %q", operation, key)
		}
	}

	allKeys := make([]string, 0, len(after.Records))
	byKind := map[string][]string{}
	for key, record := range after.Records {
		allKeys = append(allKeys, key)
		byKind[record.Kind] = append(byKind[record.Kind], key)
	}
	sort.Strings(allKeys)
	queryOrder := make(map[string]uint64, len(allKeys))
	for i, key := range allKeys {
		queryOrder[key] = uint64(i + 1)
	}
	queryKindOrder := make(map[string]uint64, len(allKeys))
	for _, keys := range byKind {
		sort.Strings(keys)
		for i, key := range keys {
			queryKindOrder[key] = uint64(i + 1)
		}
	}
	for key, record := range after.Records {
		row, ok := newByKey[key]
		if !ok {
			t.Fatalf("%s SQLite objects omitted live record key %q", operation, key)
		}
		if got := b09SQLText(t, b09SQLColumn(t, next, row, "kind")); got != record.Kind {
			t.Fatalf("%s SQLite objects kind for %q: got=%q want=%q", operation, key, got, record.Kind)
		}
	}
	for key, row := range newByKey {
		wantOrder, live := queryOrder[key]
		wantKindOrder := queryKindOrder[key]
		if !live {
			wantOrder, wantKindOrder = 0, 0
		}
		if got := b09SQLNumber(t, b09SQLColumn(t, next, row, "query_order")); got != wantOrder {
			t.Fatalf("%s SQLite objects query_order for %q: got=%d want=%d", operation, key, got, wantOrder)
		}
		if got := b09SQLNumber(t, b09SQLColumn(t, next, row, "query_kind_order")); got != wantKindOrder {
			t.Fatalf("%s SQLite objects query_kind_order for %q: got=%d want=%d", operation, key, got, wantKindOrder)
		}
	}
	for key, oldRow := range oldByKey {
		newRow, ok := newByKey[key]
		if !ok {
			t.Fatalf("%s removed SQLite objects key %q", operation, key)
		}
		oldCells, newCells := b09SQLCells(t, oldRow), b09SQLCells(t, newRow)
		for i, column := range next.Columns {
			if column == "query_order" || column == "query_kind_order" {
				continue
			}
			if oldCells[i] != newCells[i] {
				t.Fatalf("%s changed non-rank SQLite objects cell %s for %q", operation, column, key)
			}
		}
	}
}

// These images are constructed from the archive/request or the expected
// logical delta. No field in an affected SQL image supplies its own oracle.
func assertB09SQLSourceRow(t *testing.T, table b09SQLTable, row string, meta driver.Meta, want driver.Record, history bool) {
	t.Helper()
	doc, err := pipeline.DecodeDocument(want.Body)
	if err != nil {
		t.Fatal(err)
	}
	keyColumn := "source_key"
	for _, c := range table.Columns {
		if c == "child_key" {
			keyColumn = c
		}
	}
	text := map[string]string{"incarnation": meta.Incarnation, keyColumn: b09RecordKey(t, want.Ref), "ref": want.Ref.ID, "kind": want.Kind}
	numbers := map[string]uint64{"branch": mustB09Revision(t, want.Ref.Branch), "revision": mustB09Revision(t, want.Revision)}
	nulls := map[string]bool{}
	if keyColumn == "child_key" {
		if doc.Parent == nil {
			nulls["parent_key"] = true
		} else {
			text["parent_key"] = b09RecordKey(t, *doc.Parent)
		}
	}
	if history {
		numbers["start_revision"] = mustB09Revision(t, want.Revision)
		nulls["end_revision"] = true
	}
	if len(table.Columns) != len(text)+len(numbers)+len(nulls)+1 {
		t.Fatal("source image schema differs from complete expected columns")
	}
	for name, value := range text {
		if got := b09SQLText(t, b09SQLColumn(t, table, row, name)); got != value {
			t.Fatalf("complete SQL source image %s want=%q got=%q", name, value, got)
		}
	}
	for name, value := range numbers {
		if got := b09SQLNumber(t, b09SQLColumn(t, table, row, name)); got != value {
			t.Fatalf("complete SQL source image %s want=%d got=%d", name, value, got)
		}
	}
	for name := range nulls {
		if b09SQLColumn(t, table, row, name) != b09SQLValue(nil) {
			t.Fatalf("complete SQL source image %s must be SQL NULL", name)
		}
	}
	b09Equal(t, "complete SQL source image body", json.RawMessage(want.Body), json.RawMessage(b09SQLText(t, b09SQLColumn(t, table, row, "body"))))
}

func assertB09SQLRecord(t *testing.T, engine string, table b09SQLTable, row string, want driver.Record, versionCount int) {
	t.Helper()
	column := func(name string) string { return b09SQLText(t, b09SQLColumn(t, table, row, name)) }
	if column("key") != b09RecordKey(t, want.Ref) {
		t.Fatal("physical record identity differs from expected record")
	}
	if engine == "sqlite" {
		if len(table.Columns) != 2 {
			t.Fatal("SQLite record schema changed")
		}
		var got driver.Record
		if err := json.Unmarshal([]byte(column("data")), &got); err != nil {
			t.Fatal(err)
		}
		b09Equal(t, "complete physical SQLite record body", json.RawMessage(want.Body), json.RawMessage(got.Body))
		want.Body, got.Body = nil, nil
		b09Equal(t, "complete physical SQLite record envelope", want, got)
		return
	}
	if len(table.Columns) != 7 || column("kind") != want.Kind || column("deleted") != "false" || b09SQLNumber(t, b09SQLColumn(t, table, row, "revision")) != mustB09Revision(t, want.Revision) || b09SQLNumber(t, b09SQLColumn(t, table, row, "version_count")) != uint64(versionCount) {
		t.Fatal("complete PostgreSQL record envelope differs")
	}
	b09Equal(t, "complete physical PostgreSQL record ref", want.Ref, json.RawMessage(column("ref")))
	b09Equal(t, "complete physical PostgreSQL record body", json.RawMessage(want.Body), json.RawMessage(column("body")))
}

// Main's historical-query index stores a compact projection beside every
// physical version. Keep this projection in the recorder oracle: it is part
// of the durable SQL image, not a derived value that can be ignored.
func b09SQLQueryFields(t *testing.T, engine string, want driver.Version) string {
	t.Helper()
	encode := func(value string) string {
		if engine == "postgres" {
			return base64.StdEncoding.EncodeToString([]byte(value))
		}
		return value
	}
	fields := map[string]any{
		"deleted": want.Deleted,
		"id":      encode(want.Ref.ID),
		"key":     encode(want.Ref.Key),
	}
	if !want.Deleted {
		body := want.Body
		if !want.Base {
			var patches []struct {
				Op, Path string
				Value    json.RawMessage
			}
			if err := json.Unmarshal(want.Patch, &patches); err != nil || len(patches) != 1 || patches[0].Op != "replace" || patches[0].Path != "" {
				t.Fatalf("physical version query_fields: invalid expected forward patch: %v", err)
			}
			body = patches[0].Value
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(body, &object); err != nil {
			fields["body_error"] = true
		} else {
			for _, name := range []string{"type", "project", "status"} {
				raw, ok := object[name]
				if !ok {
					continue
				}
				var value string
				if err := json.Unmarshal(raw, &value); err == nil {
					fields[name] = encode(value)
				}
			}
		}
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func assertB09SQLVersion(t *testing.T, engine string, table b09SQLTable, row string, want driver.Version) {
	t.Helper()
	revColumn, baseColumn, dataColumn := "rev", "base", "data"
	base := strconv.Itoa(boolB09(want.Base))
	if engine == "postgres" {
		revColumn, baseColumn, dataColumn = "revision", "is_base", "value"
		base = strconv.FormatBool(want.Base)
	}
	text := func(name string) string { return b09SQLText(t, b09SQLColumn(t, table, row, name)) }
	if len(table.Columns) != 5 || text("key") != b09RecordKey(t, want.Ref) || text(baseColumn) != base || b09SQLNumber(t, b09SQLColumn(t, table, row, revColumn)) != mustB09Revision(t, want.Revision) || text("query_fields") != b09SQLQueryFields(t, engine, want) {
		t.Fatal("complete physical version columns differ")
	}
	var got driver.Version
	if err := json.Unmarshal([]byte(text(dataColumn)), &got); err != nil {
		t.Fatal(err)
	}
	assertB09Version(t, "physical version", want, got)
}

func assertB09Version(t *testing.T, label string, want, got driver.Version) {
	t.Helper()
	for _, bodies := range []struct {
		name      string
		want, got []byte
	}{{"body", want.Body, got.Body}, {"patch", want.Patch, got.Patch}} {
		if bodies.want == nil {
			b09Equal(t, label+" absent "+bodies.name, bodies.want, bodies.got)
		} else {
			b09Equal(t, label+" complete "+bodies.name, json.RawMessage(bodies.want), json.RawMessage(bodies.got))
		}
	}
	want.Body, want.Patch, got.Body, got.Patch = nil, nil, nil, nil
	b09Equal(t, label+" complete envelope", want, got)
}

// Create prepares model.Ref{ID: id}, with the branch omitted. Memory and
// SQLite retain that acquired lock reference in each raw version; PostgreSQL
// constructs versions from its staged record, whose branch is explicitly zero.
// The other operations in this sequence supply an explicit zero branch.
// Keep these independently expected wire images exact, including presence.
func (r *b09ReplayTarget) expectedVersionRef(id string, creating bool) model.Ref {
	if creating && r.engine != "postgres" {
		return model.Ref{ID: id}
	}
	return model.RefByID(id, model.Natural64(0))
}

func assertB09InitialSQL(t *testing.T, stage b09ReplayStage, want map[string]driver.Record, indexesBuilt bool) {
	t.Helper()
	for _, name := range []string{"records", "versions", "hierarchy_source_current", "hierarchy_source_history", "graph_link_source_current", "graph_link_source_history"} {
		table := stage.SQL.Tables[name]
		remaining := map[string]driver.Record{}
		for key, record := range want {
			sourceImage := strings.HasSuffix(name, "_source_current") || strings.HasSuffix(name, "_source_history")
			if name == "records" || name == "versions" || sourceImage && record.Kind == "bead" {
				remaining[key] = record
			}
		}
		for _, row := range table.Rows {
			keyColumn := "key"
			if strings.HasPrefix(name, "hierarchy_") {
				keyColumn = "child_key"
			}
			if strings.HasPrefix(name, "graph_") {
				keyColumn = "source_key"
			}
			key := b09SQLText(t, b09SQLColumn(t, table, row, keyColumn))
			record, ok := remaining[key]
			if !ok {
				t.Fatalf("unexplained/duplicate initial physical %s identity %s", name, key)
			}
			delete(remaining, key)
			switch name {
			case "records":
				assertB09SQLRecord(t, stage.SQL.Engine, table, row, record, 1)
			case "versions":
				assertB09SQLVersion(t, stage.SQL.Engine, table, row, driver.Version{Ref: record.Ref, Revision: record.Revision, Base: true, Body: record.Body, Principal: b09Principal, CreatedAt: stage.Receipts["offline.restore.import"].Changeset.Time})
			default:
				assertB09SQLSourceRow(t, table, row, stage.Meta, record, strings.HasSuffix(name, "_history"))
			}
		}
		if len(remaining) != 0 {
			t.Fatalf("initial physical %s omitted expected identities: %v", name, remaining)
		}
	}
	// The admitted source corpus has no graph links. Typed source rows and their
	// coverage are published atomically with import; explicit startup preserves
	// that exact image rather than creating a second, different source state.
	for _, name := range []string{"graph_link_current", "graph_link_history", "hierarchy_index_coverage", "graph_link_index_coverage"} {
		table := stage.SQL.Tables[name]
		if name == "graph_link_current" || name == "graph_link_history" {
			if len(table.Rows) != 0 {
				t.Fatalf("unexpected initial %s rows before/after explicit startup", name)
			}
			continue
		}
		if len(table.Rows) != 1 || len(table.Columns) != 6 {
			t.Fatalf("startup %s must have one complete coverage marker", name)
		}
		row := table.Rows[0]
		for column, want := range map[string]string{"incarnation": stage.Meta.Incarnation, "state": "ready"} {
			if got := b09SQLText(t, b09SQLColumn(t, table, row, column)); got != want {
				t.Fatalf("startup %s %s want=%q got=%q", name, column, want, got)
			}
		}
		for column, want := range map[string]uint64{"branch": 0, "format": 1, "built_from": mustB09Revision(t, stage.Meta.Head), "certified_through": mustB09Revision(t, stage.Meta.Head)} {
			if got := b09SQLNumber(t, b09SQLColumn(t, table, row, column)); got != want {
				t.Fatalf("startup %s %s want=%d got=%d", name, column, want, got)
			}
		}
	}
}

func assertB09IndexActivation(t *testing.T, before, after b09ReplayStage) {
	t.Helper()
	prior, current := before, after
	prior.Name, current.Name = "", ""
	prior.SQL, current.SQL = nil, nil
	b09Equal(t, "startup preserves every logical/allocator/history/receipt/lease field", prior, current)
	if before.SQL == nil {
		if after.SQL != nil {
			t.Fatal("startup added an unexpected SQL engine")
		}
		return
	}
	if after.SQL == nil || before.SQL.Engine != after.SQL.Engine || len(before.SQL.Tables) != len(after.SQL.Tables) {
		t.Fatal("startup changed the physical engine/table set")
	}
	for name, old := range before.SQL.Tables {
		next, ok := after.SQL.Tables[name]
		if !ok {
			t.Fatalf("startup lost physical table %s", name)
		}
		b09Equal(t, "startup preserves physical schema "+name, old.Columns, next.Columns)
		switch name {
		case "hierarchy_source_current", "hierarchy_source_history", "graph_link_source_current", "graph_link_source_history", "graph_link_current", "graph_link_history", "hierarchy_index_coverage", "graph_link_index_coverage":
			// Main publishes typed source rows and ready coverage atomically
			// during import; startup must retain that exact physical image.
			b09Equal(t, "startup preserves complete source SQL "+name, old, next)
		default:
			b09Equal(t, "startup preserves every unrelated SQL row "+name, old, next)
		}
	}
}

func assertB09SQLReopen(t *testing.T, before, after *b09SQLCensus) {
	t.Helper()
	if after == nil || before.Engine != after.Engine || len(before.Tables) != len(after.Tables) {
		t.Fatal("reopen physical SQL table census changed")
	}
	lease := "active_lease"
	owner := "token"
	if before.Engine == "postgres" {
		lease, owner = "lease", "owner"
	}
	for name, old := range before.Tables {
		next, ok := after.Tables[name]
		if !ok {
			t.Fatalf("reopen lost SQL table %s", name)
		}
		if name != lease {
			b09Equal(t, "reopen complete physical SQL "+name, old, next)
			continue
		}
		b09Equal(t, "reopen lease columns", old.Columns, next.Columns)
		if len(old.Rows) != 1 || len(next.Rows) != 1 {
			t.Fatal("reopen lease cardinality")
		}
		a, b := b09SQLCells(t, old.Rows[0]), b09SQLCells(t, next.Rows[0])
		for i, column := range old.Columns {
			switch column {
			case "epoch":
				if b09SQLNumber(t, b[i]) != b09SQLNumber(t, a[i])+1 {
					t.Fatal("SQL reopen lease epoch did not advance once")
				}
			case owner:
				if b09SQLText(t, b[i]) == "" || a[i] == b[i] {
					t.Fatal("SQL reopen ownership token did not change")
				}
			case "expires":
				if b09SQLText(t, b[i]) == "" {
					t.Fatal("SQL reopen expiry missing")
				}
			default:
				if a[i] != b[i] {
					t.Fatalf("SQL reopen lease column changed: %s", column)
				}
			}
		}
	}
}

// The primary-key census is explicit for every table in the two driver schemas.
// Unknown tables are a source/schema change, not permission to omit their rows.
var b09SQLPrimary = map[string][]string{
	"meta": {"singleton"}, "active_lease": {"singleton"}, "lease": {"singleton"}, "locks": {"key"}, "objects": {"key"}, "records": {"key"}, "versions": {"key", "rev"}, "key_history": {"key", "rev"}, "states": {"key", "revision"}, "blobs": {"hash"}, "changesets": {"rev"}, "replays": {"scope"}, "receipts": {"scope"}, "checkpoints": {"key"}, "core_sessions": {"id"}, "allocator_states": {"prefix"},
	"hierarchy_index_coverage": {"incarnation", "branch"}, "hierarchy_source_current": {"incarnation", "branch", "child_key"}, "hierarchy_source_history": {"incarnation", "branch", "child_key", "start_revision"}, "graph_link_index_coverage": {"incarnation", "branch"}, "graph_link_source_current": {"incarnation", "branch", "source_key"}, "graph_link_current": {"incarnation", "branch", "source_key", "link_kind", "target_key"}, "graph_link_source_history": {"incarnation", "branch", "source_key", "start_revision"}, "graph_link_history": {"incarnation", "branch", "source_key", "start_revision", "link_kind", "target_key"},
}

func b09SQLRows(t *testing.T, engine, name string, table b09SQLTable) map[string]string {
	t.Helper()
	columns, ok := b09SQLPrimary[name]
	if !ok {
		t.Fatalf("unreviewed physical table %s", name)
	}
	if engine == "postgres" && (name == "versions" || name == "changesets") {
		columns = append([]string{}, columns...)
		for i, c := range columns {
			if c == "rev" {
				columns[i] = "revision"
			}
		}
	}
	result := map[string]string{}
	for _, row := range table.Rows {
		key := []string{}
		for _, column := range columns {
			key = append(key, b09SQLColumn(t, table, row, column))
		}
		raw, _ := json.Marshal(key)
		if _, ok := result[string(raw)]; ok {
			t.Fatal("duplicate physical SQL primary key")
		}
		result[string(raw)] = row
	}
	return result
}

func (r *b09ReplayTarget) expectedSQLStates(want driver.Record, creating bool) map[string]driver.LockState {
	t := r.t
	t.Helper()
	result := map[string]driver.LockState{}
	add := func(state driver.LockState) {
		key, err := driver.EncodeLockKey(state.Key)
		if err != nil {
			t.Fatal(err)
		}
		state.Revision = want.Revision
		result[key] = state
	}
	// PostgreSQL keeps record state in records; SQLite also journals its key.
	if r.engine == "sqlite" {
		ref := r.expectedVersionRef(want.Ref.ID, creating)
		add(driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Present: true})
	}
	for _, sentinel := range []driver.Sentinel{driver.Sentinel("bead/" + want.Ref.ID), driver.SentinelGraphBlocking} {
		add(driver.LockState{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}, Present: true})
	}
	// The imported graph certificate is retained as a durable read fact. Both
	// parentless creates and the field-only close/compact/revert preserve the
	// certified graph while appending their ordinary state deltas.
	add(driver.LockState{Key: driver.LockKey{Kind: driver.LockReadFact, Fact: pipeline.GraphValidityReadFact}, Present: true})
	if creating {
		// Random allocation stages even an unchanged allocator inventory;
		// both SQL drivers publish its typed fact in the same transaction.
		key, err := driver.AllocatorLock("b09")
		if err != nil {
			t.Fatal(err)
		}
		add(driver.LockState{Key: key, Present: true})
		add(driver.LockState{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: driver.Sentinel("closure/" + want.Ref.ID)}, Present: true})
		row, err := pipeline.AncestorRow(want.Ref.ID, want.Ref.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		add(driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}, Derived: &row, Present: true})
	}
	doc, err := pipeline.DecodeDocument(want.Body)
	if err != nil {
		t.Fatal(err)
	}
	if doc.CreatedAt == nil {
		t.Fatal("named B09 corpus lacks expected creation time")
	}
	// This corpus explicitly supplies priority 2. The canonical rank rule
	// changes to the old bucket strictly after 48 hours.
	bucket, rank := 0, 2
	if doc.CreatedAt.Before(r.now.Add(-48 * time.Hour)) {
		bucket, rank = 1, 999
	}
	value, err := json.Marshal(pipeline.ReadySortKey{Bucket: bucket, Rank: rank, CreatedAt: *doc.CreatedAt, RankedAt: r.now, Revision: want.Revision})
	if err != nil {
		t.Fatal(err)
	}
	row := driver.DerivedRow{Key: "ready/" + want.Ref.ID, Value: value}
	add(driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: row.Key}}, Derived: &row, Present: true})
	return result
}

func (r *b09ReplayTarget) assertSQLMutation(before, after b09ReplayStage, id, operation string, want driver.Record) {
	t := r.t
	t.Helper()
	if len(before.SQL.Tables) != len(after.SQL.Tables) {
		t.Fatal("mutation changed physical SQL table set")
	}
	ref := model.RefByID(id, model.Natural64(0))
	recordKey := b09RecordKey(t, ref)
	ancestor, err := pipeline.AncestorRow(id, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{recordKey: true, "2:bead/" + id: true, "2:graph/blocking": true, "1:" + string(pipeline.GraphValidityReadFact): true, "2:closure/" + id: true, "3:ancestor_closure:" + ancestor.Key: true, "3:sort_key:ready/" + id: true}
	creating := strings.HasPrefix(operation, "create-")
	wantAllocator := before.Allocator
	wantAllocator.At = want.Revision
	journal, err := driver.AllocatorJournalDelta(before.Allocator, wantAllocator)
	if err != nil {
		t.Fatal(err)
	}
	wantStates := r.expectedSQLStates(want, creating)
	for key := range wantStates {
		keys[key] = true
	}
	for name, old := range before.SQL.Tables {
		next, ok := after.SQL.Tables[name]
		if !ok {
			t.Fatalf("lost physical table %s", name)
		}
		b09Equal(t, "physical SQL schema "+name, old.Columns, next.Columns)
		a, b := b09SQLRows(t, r.engine, name, old), b09SQLRows(t, r.engine, name, next)
		added, updated, removed := []string{}, []string{}, []string{}
		for key, row := range a {
			if v, ok := b[key]; !ok {
				removed = append(removed, row)
			} else if v != row {
				updated = append(updated, v)
			}
		}
		for key, row := range b {
			if _, ok := a[key]; !ok {
				added = append(added, row)
			}
		}
		if len(added)+len(updated)+len(removed) == 0 {
			switch name {
			case "meta", "records", "versions", "key_history", "states", "changesets", "replays", "receipts", "allocator_states", "hierarchy_source_current", "graph_link_source_current", "hierarchy_source_history", "graph_link_source_history", "hierarchy_index_coverage", "graph_link_index_coverage":
			default:
				continue
			}
		}
		if len(removed) != 0 {
			t.Fatalf("%s removed physical rows from %s: %v", operation, name, removed)
		}
		columnText := func(row, column string) string { return b09SQLText(t, b09SQLColumn(t, next, row, column)) }
		revision := func(row, column string) {
			if b09SQLNumber(t, b09SQLColumn(t, next, row, column)) != mustB09Revision(t, after.Meta.Head) {
				t.Fatalf("%s unrelated physical revision in %s", operation, name)
			}
		}
		switch name {
		case "meta":
			if len(added) != 0 || len(updated) != 1 {
				t.Fatal("SQL meta exact delta")
			}
			oldCells, newCells := b09SQLCells(t, old.Rows[0]), b09SQLCells(t, next.Rows[0])
			for i, c := range old.Columns {
				if c == "head" {
					revision(next.Rows[0], c)
				} else if oldCells[i] != newCells[i] {
					t.Fatalf("SQL meta changed %s", c)
				}
			}
		case "records":
			if len(added)+len(updated) != 1 || len(added) != boolB09(creating) {
				t.Fatal("SQL record cardinality delta")
			}
			assertB09SQLRecord(t, r.engine, next, append(added, updated...)[0], want, len(before.Versions[recordKey])+1)
		case "objects":
			if r.engine == "sqlite" && b09SQLHasColumn(next, "query_order") && b09SQLHasColumn(next, "query_kind_order") {
				assertB09SQLiteObjectMutation(t, operation, after, old, next, added, keys, recordKey, creating)
				break
			}
			if len(updated) != 0 {
				t.Fatalf("%s mutated existing %s rows", operation, name)
			}
			for _, row := range added {
				if !keys[columnText(row, "key")] {
					t.Fatalf("%s unexpected physical %s key %q", operation, name, columnText(row, "key"))
				}
			}
		case "locks":
			if len(updated) != 0 {
				t.Fatalf("%s mutated existing %s rows", operation, name)
			}
			for _, row := range added {
				if !keys[columnText(row, "key")] {
					t.Fatalf("%s unexpected physical %s key %q", operation, name, columnText(row, "key"))
				}
			}
		case "versions":
			if len(updated) != 0 || len(added) != 1 {
				t.Fatal("SQL version exact append")
			}
			version := driver.Version{Ref: r.expectedVersionRef(id, creating), Revision: want.Revision, Base: creating, CreatedAt: r.now, Principal: b09Principal}
			if creating {
				version.Body = want.Body
			} else {
				version.Patch, err = json.Marshal([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(want.Body)}})
				if err != nil {
					t.Fatal(err)
				}
			}
			assertB09SQLVersion(t, r.engine, next, added[0], version)
		case "key_history", "states":
			if len(updated) != 0 || len(added) != len(wantStates) || len(next.Columns) != 3 {
				t.Fatalf("%s exact immutable state append differs: added=%d want=%d", operation, len(added), len(wantStates))
			}
			remaining := map[string]driver.LockState{}
			for key, state := range wantStates {
				remaining[key] = state
			}
			for _, row := range added {
				key := columnText(row, "key")
				state, ok := remaining[key]
				if !ok {
					t.Fatalf("%s unexplained/duplicate physical state key %q", operation, key)
				}
				delete(remaining, key)
				column, payload := "rev", "data"
				if name == "states" {
					column, payload = "revision", "value"
				}
				revision(row, column)
				b09Equal(t, operation+" complete physical state payload "+key, state, json.RawMessage(columnText(row, payload)))
			}
			if len(remaining) != 0 {
				t.Fatalf("%s omitted expected physical states: %v", operation, remaining)
			}
		case "changesets", "replays", "receipts":
			if len(updated) != 0 || len(added) != 1 {
				t.Fatalf("%s must append exactly one physical %s", operation, name)
			}
			column := "rev"
			if r.engine == "postgres" {
				column = "revision"
			}
			revision(added[0], column)
		case "allocator_states":
			if len(updated) != 0 || len(added) != len(journal) {
				t.Fatal("SQL allocator journal exact append")
			}
			for _, row := range added {
				value, ok := journal[columnText(row, "prefix")]
				if !ok || columnText(row, "data") != string(value) {
					t.Fatal("SQL allocator journal bytes differ from unchanged inventory at next head")
				}
			}
		case "hierarchy_index_coverage", "graph_link_index_coverage":
			if len(added) != 0 || len(updated) != len(old.Rows) {
				t.Fatal("SQL coverage exact delta")
			}
			for key, oldRow := range a {
				newRow := b[key]
				oldCells, newCells := b09SQLCells(t, oldRow), b09SQLCells(t, newRow)
				for i, c := range old.Columns {
					if c == "certified_through" {
						revision(newRow, c)
					} else if oldCells[i] != newCells[i] {
						t.Fatalf("SQL coverage changed %s", c)
					}
				}
			}
		case "hierarchy_source_current", "graph_link_source_current", "hierarchy_source_history", "graph_link_source_history":
			column := "child_key"
			if strings.HasPrefix(name, "graph_") {
				column = "source_key"
			}
			history := strings.HasSuffix(name, "_history")
			if len(added) != boolB09(creating || history) || len(updated) != boolB09(!creating) {
				t.Fatalf("%s SQL %s exact delta add=%d update=%d", operation, name, len(added), len(updated))
			}
			newImages := added
			if !history {
				newImages = append(newImages, updated...)
			}
			for _, row := range newImages {
				assertB09SQLSourceRow(t, next, row, before.Meta, want, history)
			}
			if history {
				for key, oldRow := range a {
					newRow, ok := b[key]
					if !ok || newRow == oldRow {
						continue
					}
					if columnText(oldRow, column) != recordKey || b09SQLColumn(t, old, oldRow, "end_revision") != b09SQLValue(nil) {
						t.Fatal("SQL changed an unrelated/already closed source interval")
					}
					assertB09SQLSourceRow(t, old, oldRow, before.Meta, before.Records[recordKey], true)
					oldCells, newCells := b09SQLCells(t, oldRow), b09SQLCells(t, newRow)
					for i, c := range old.Columns {
						if c == "end_revision" {
							revision(newRow, c)
						} else if oldCells[i] != newCells[i] {
							t.Fatalf("SQL prior source interval changed %s", c)
						}
					}
				}
			}
		default:
			t.Fatalf("%s unexpectedly mutated complete physical table %s", operation, name)
		}
	}
}
func mustB09Revision(t *testing.T, revision model.Revision) uint64 {
	t.Helper()
	n, err := revision.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func boolB09(b bool) int {
	if b {
		return 1
	}
	return 0
}
