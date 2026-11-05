package b09native

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
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

func assertB09Source(t *testing.T, archive *dumpformat.Archive, stage b09ReplayStage) {
	t.Helper()
	for _, source := range archive.Records("bead") {
		key := b09RecordKey(t, model.RefByID(source.ID, model.Natural64(0)))
		row, ok := stage.Records[key]
		if !ok || row.Kind != "bead" || row.Revision.String() != "1" {
			t.Fatalf("source bead missing at snapshot restore: %s", source.ID)
		}
		// This named native corpus has open tasks, no labels, links or comments.
		if source.Source.Table != "issues" || source.Source.Fields["status"] != "open" {
			t.Fatalf("unexpected native corpus row: %+v", source)
		}
		fields := map[string]any{"status": "open"}
		for _, name := range []string{"title", "description", "priority"} {
			fields[name] = source.Body[name]
		}
		metadata := map[string]json.RawMessage{}
		add := func(name string, value any) {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			metadata[name] = raw
		}
		for _, name := range dumpformat.OpaqueIssueFields() {
			if value, ok := source.Source.Fields[name]; ok {
				add("beads."+name, value)
			}
		}
		for name, value := range source.Body {
			switch name {
			case "title", "description", "priority", "metadata", "labels":
				continue
			}
			add("beads.source."+name, value)
		}
		add("beads.source.status", "open")
		add("beads.source_fields", source.Source.Fields)
		created, err := time.Parse(time.RFC3339Nano, fmt.Sprint(source.Source.Fields["created_at"]))
		if err != nil {
			t.Fatal(err)
		}
		want := pipeline.Document{ID: source.ID, Type: "issue", Project: "b09", Fields: fields, Metadata: metadata, CreatedAt: &created}
		b09Equal(t, "complete restored native bead and source provenance "+source.ID, want, json.RawMessage(row.Body))
		versions := stage.Versions[key]
		if len(versions) != 1 || !versions[0].Base || versions[0].Deleted || versions[0].Revision.String() != "1" {
			t.Fatalf("snapshot restore fabricated/missed versions for %s: %+v", source.ID, versions)
		}
		b09Equal(t, "snapshot version body "+source.ID, json.RawMessage(row.Body), json.RawMessage(versions[0].Body))
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
			key := b09RecordKey(t, model.RefByID("source/"+kind+"/"+source.ID, model.Natural64(0)))
			row, ok := stage.Records[key]
			if !ok || row.Kind != kind {
				t.Fatalf("complete source row absent %s/%s", kind, source.ID)
			}
			b09Equal(t, "complete restored source "+key, want, json.RawMessage(row.Body))
		}
	}
}

func assertB09ResultPair(t *testing.T, label, id string, commit model.Commit) {
	t.Helper()
	if len(commit.Results) != 2 || commit.Revision.IsZero() {
		t.Fatalf("%s must publish patch and metadata results: %+v", label, commit)
	}
	for i, result := range commit.Results {
		if result.Ref.ID != id || !result.Ref.Branch.IsZero() || result.Revision.String() != commit.Revision.String() || len(result.Record) == 0 {
			t.Fatalf("%s result %d changed identity/revision: %+v", label, i, result)
		}
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
	if after.LeaseEpoch != before.LeaseEpoch+1 {
		t.Fatalf("reopen lease epoch %d -> %d", before.LeaseEpoch, after.LeaseEpoch)
	}
	if before.SQL != nil {
		assertB09SQLReopen(t, before.SQL, after.SQL)
	}
}

func (r *b09ReplayTarget) assertMutation(before, after b09ReplayStage, id, operation string) {
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
	wantAllocator := before.Allocator
	wantAllocator.At = wantMeta.Head
	b09Equal(t, operation+" exact allocator delta", wantAllocator, after.Allocator)
	var wantDoc pipeline.Document
	method := map[string]string{"create-first": "bead.create", "create-after-reopen": "bead.create", "close": "bead.close", "compact": "admin.compact", "revert": "history.revert"}[operation]
	if creating {
		label := strings.TrimPrefix(operation, "create-")
		stamp := r.now
		wantDoc = pipeline.Document{ID: id, Type: "issue", Project: "b09", Fields: map[string]any{"title": "Plug native continuation " + label, "description": "continuation after accepted native restore", "status": "todo", "priority": 2}, CreatedAt: &stamp}
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
		if latest.Revision.String() != wantMeta.Head.String() || latest.Deleted || latest.Base != creating || !latest.CreatedAt.Equal(r.now) || !reflect.DeepEqual(latest.Principal, b09Principal) {
			t.Fatalf("%s invalid appended version: %+v", operation, latest)
		}
		b09Equal(t, operation+" appended version body", json.RawMessage(wantBody), json.RawMessage(latest.Body))
	}
	if len(after.Changesets) != len(before.Changesets)+1 {
		t.Fatalf("%s must append exactly one changeset", operation)
	}
	if len(before.Changesets) != 0 {
		b09Equal(t, operation+" retained changeset prefix", before.Changesets, after.Changesets[:len(before.Changesets)])
	}
	last := after.Changesets[len(after.Changesets)-1]
	if last.Revision.String() != wantMeta.Head.String() || last.Incarnation != before.Meta.Incarnation || last.Kind != model.ChangesetWrite || !reflect.DeepEqual(last.Principal, b09Principal) || !last.Time.Equal(r.now) || last.IdempotencyKey != b09Meta(r.engine, operation).IdempotencyKey || len(last.Changes) != 1 || last.Changes[0].Ref.ID != id || last.Changes[0].FromRevision.String() != prior.Revision.String() {
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
	journal, err := driver.AllocatorJournalDelta(before.Allocator, after.Allocator)
	if err != nil {
		t.Fatal(err)
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
			case "meta", "records", "versions", "changesets", "replays", "receipts", "allocator_states", "hierarchy_source_current", "graph_link_source_current", "hierarchy_source_history", "graph_link_source_history":
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
			for _, row := range append(added, updated...) {
				if columnText(row, "key") != recordKey {
					t.Fatalf("%s mutated unrelated physical record", operation)
				}
			}
			if r.engine == "postgres" {
				row := append(added, updated...)[0]
				revision(row, "revision")
				b09Equal(t, operation+" physical complete target body", json.RawMessage(want.Body), json.RawMessage(columnText(row, "body")))
			}
		case "objects", "locks":
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
			row := added[0]
			if columnText(row, "key") != recordKey {
				t.Fatal("SQL unrelated appended version")
			}
			column := "rev"
			if r.engine == "postgres" {
				column = "revision"
			}
			revision(row, column)
		case "key_history", "states":
			if len(updated) != 0 {
				t.Fatal("SQL immutable key history changed")
			}
			for _, row := range added {
				if !keys[columnText(row, "key")] {
					t.Fatalf("%s unexpected physical state key %q", operation, columnText(row, "key"))
				}
				column := "rev"
				if name == "states" {
					column = "revision"
				}
				revision(row, column)
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
			for _, row := range append(added, updated...) {
				if columnText(row, column) != recordKey {
					t.Fatal("SQL unrelated source image changed")
				}
			}
			for _, row := range added {
				revision(row, "revision")
				if history {
					revision(row, "start_revision")
				}
			}
			if history {
				for key, oldRow := range a {
					newRow, ok := b[key]
					if !ok || newRow == oldRow {
						continue
					}
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
