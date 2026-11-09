package dolt

import (
	"archive/tar"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// The collision is two actual persisted rows with independent labels. Keeping
// both images in an archive cannot establish which image the native read uses.
func TestGetIssueActiveImageWinsStoredCollision(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	const id = "test-active-collision"
	durable := &types.Issue{ID: id, Title: "durable title", Description: "durable description", Notes: "durable notes", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
	if err := store.CreateIssue(ctx, durable, "tester"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO labels (issue_id, label) VALUES (?, ?)`, id, "durable-label"); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO wisps
		(id, title, description, notes, status, priority, issue_type, created_at, updated_at, ephemeral, no_history, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, "active title", "active description", "active notes", "blocked", 1, "bug", at, at, true, true, `{"plane":"active","n":9007199254740993}`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO wisp_labels (issue_id, label) VALUES (?, ?)`, id, "active-label"); err != nil {
		t.Fatal(err)
	}
	type image struct {
		ID, Title, Description, Notes, Status, Type, Metadata string
		Priority, Ephemeral, NoHistory                        int
	}
	readImage := func(table string) image {
		t.Helper()
		var row image
		//nolint:gosec // table is one of the two literal issue planes below.
		err := store.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT id, title, description, notes, status, priority, issue_type, ephemeral, no_history, metadata FROM %s WHERE id = ?`, table), id).
			Scan(&row.ID, &row.Title, &row.Description, &row.Notes, &row.Status, &row.Priority, &row.Type, &row.Ephemeral, &row.NoHistory, &row.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	beforeDurable, beforeActive := readImage("issues"), readImage("wisps")
	got, err := store.GetIssue(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != beforeActive.ID || got.Title != beforeActive.Title || got.Description != beforeActive.Description || got.Notes != beforeActive.Notes || string(got.Status) != beforeActive.Status || got.Priority != beforeActive.Priority || string(got.IssueType) != beforeActive.Type || !got.Ephemeral || !got.NoHistory || string(got.Metadata) != beforeActive.Metadata || len(got.Labels) != 1 || got.Labels[0] != "active-label" {
		t.Fatalf("GetIssue returned a different source image: got=%+v active=%+v", got, beforeActive)
	}
	if after := readImage("issues"); after != beforeDurable {
		t.Fatalf("durable image changed: before=%+v after=%+v", beforeDurable, after)
	}
	if after := readImage("wisps"); after != beforeActive {
		t.Fatalf("active image changed: before=%+v after=%+v", beforeActive, after)
	}
	var durableLabel string
	if err := store.db.QueryRowContext(ctx, `SELECT label FROM labels WHERE issue_id = ?`, id).Scan(&durableLabel); err != nil || durableLabel != "durable-label" {
		t.Fatalf("durable labels changed: label=%q err=%v", durableLabel, err)
	}
	durableOnly := &types.Issue{ID: "test-durable-only", Title: "durable-only title", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
	if err := store.CreateIssue(ctx, durableOnly, "tester"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetIssue(ctx, durableOnly.ID); err != nil || got.Title != durableOnly.Title || got.Ephemeral {
		t.Fatalf("durable-only fallback: got=%+v err=%v", got, err)
	}
	if _, err := store.GetIssue(ctx, "test-absent-image"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing image: %v", err)
	}
	// The test owns a separate database. A broken active label source must
	// remain an error even though an intact durable row with this ID exists.
	if _, err := store.db.ExecContext(ctx, `DROP TABLE wisp_labels`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetIssue(ctx, id); err == nil || errors.Is(err, storage.ErrNotFound) || !strings.Contains(err.Error(), "wisp_labels") {
		t.Fatalf("active label failure hidden by durable image: %v", err)
	}
}

// This control starts with the real migrated schema and actual stored rows,
// not fixtureSource or prebuilt archive bytes. It exercises the native SQL
// census, filesystem observation, mapper, encoder and self-validator together.
// It remains a disposable test control, not an official executable delivery or
// a consumer restore/history qualification.
func TestNativeProducerBundleFromStoredDoltRows(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := store.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"test-abc", "test-def", "test-jkl", "test-mno", "test-pqr"} {
		issue := &types.Issue{ID: id, Title: "durable " + id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
		if err := store.CreateIssue(ctx, issue, "producer-control"); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"test-abc", "test-ghi", "test-stu"} {
		exec(`INSERT INTO wisps (id, title, status, priority, issue_type, created_at, updated_at, ephemeral, no_history)
			VALUES (?, ?, 'open', 2, 'task', ?, ?, 1, 1)`, id, "active "+id, at, at)
	}
	exec(`INSERT INTO wisp_labels (issue_id, label) VALUES ('test-abc', 'active-label')`)
	exec(`INSERT INTO wisp_comments (id, issue_id, author, text, created_at) VALUES ('stored-active-comment', 'test-abc', 'producer-control', 'stored active comment', ?)`, at)
	exec(`INSERT INTO wisp_events (id, issue_id, event_type, actor, old_value, new_value, comment, created_at)
		VALUES ('stored-active-event', 'test-abc', 'created', 'producer-control', NULL, 'open', 'stored active event', ?)`, at)
	exec(`INSERT INTO wisp_child_counters (parent_id, last_child) VALUES ('test-abc', 2)`)
	exec(`INSERT INTO custom_statuses (name, category) VALUES ('review', 'active')`)
	exec(`INSERT INTO custom_types (name) VALUES ('gate')`)
	if err := store.SetConfig(ctx, "status.custom", "review:active"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetConfig(ctx, "types.custom", "gate"); err != nil {
		t.Fatal(err)
	}
	// Opaque creation is deliberately direct SQL: the existing public custom
	// kind creation refusal stays in force. All later reads use SQLSource.
	edges := []struct {
		id, kind, arm, target string
		metadata              any
	}{
		{"opaque-null", "custom-stored", "depends_on_issue_id", "test-jkl", nil},
		{"opaque-object", "custom-stored", "depends_on_issue_id", "test-mno", `{}`},
		{"opaque-number", "custom-stored", "depends_on_issue_id", "test-pqr", `{"n":9007199254740993,"gate":"inert"}`},
		{"known-tracks", "tracks", "depends_on_issue_id", "test-def", `{"report":"expanded"}`},
		{"durable-active", "blocks", "depends_on_wisp_id", "test-ghi", `{}`},
		{"durable-external", "blocks", "depends_on_external", "external:tracker:stored", `{}`},
	}
	fixedKinds := []string{
		"blocks", "parent-child", "conditional-blocks", "waits-for", "related", "discovered-from",
		"replies-to", "relates-to", "duplicates", "supersedes", "authored-by", "assigned-to",
		"approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from",
	}
	for i, kind := range fixedKinds {
		// The source has a unique (issue_id, target) constraint independent of
		// type, so every fixed kind gets a distinct real target row.
		target := fmt.Sprintf("test-k%02d", i)
		issue := &types.Issue{ID: target, Title: "fixed target " + kind, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
		if err := store.CreateIssue(ctx, issue, "producer-control"); err != nil {
			t.Fatal(err)
		}
		metadata := []any{nil, `{}`, `{"gate":"inert","n":9007199254740993}`}[i%3]
		if kind == "waits-for" {
			metadata = `{"gate":"","also_blocks":false,"n":9007199254740993}`
		}
		edges = append(edges, struct {
			id, kind, arm, target string
			metadata              any
		}{"fixed-" + kind, kind, "depends_on_issue_id", target, metadata})
	}
	insertEdges := func(opaque bool) {
		for _, edge := range edges {
			if strings.HasPrefix(edge.id, "opaque-") != opaque {
				continue
			}
			//nolint:gosec // arm is one of the three literal typed columns above.
			exec(fmt.Sprintf(`INSERT INTO dependencies (id, issue_id, %s, type, created_at, created_by, metadata, thread_id)
				VALUES (?, 'test-abc', ?, ?, ?, 'producer-control', ?, 'stored-thread')`, edge.arm), edge.id, edge.target, edge.kind, at, edge.metadata)
		}
	}
	insertEdges(false)
	exec(`INSERT INTO wisp_dependencies (id, issue_id, depends_on_wisp_id, type, created_at, created_by, metadata, thread_id)
		VALUES ('active-waits', 'test-abc', 'test-ghi', 'waits-for', ?, 'producer-control', ?, 'active-thread')`, at, `{"gate":"any-children","also_blocks":true}`)
	exec(`CALL DOLT_ADD('-A')`)
	exec(`CALL DOLT_COMMIT('-m', 'disposable producer capability control')`)

	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	// This explicitly test-authored comment-only file is an observation
	// regression control; it is not relabeled as a genuine initializer file.
	configBytes := []byte("# disposable public producer observation\n\n")
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	retained := []string{"issues", "wisps", "dependencies", "wisp_dependencies", "wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters", "custom_statuses", "custom_types"}
	var before map[string]plugdump.Table
	build := func(t *testing.T) ([]byte, error) {
		t.Helper()
		sidecarDir := t.TempDir()
		paths := map[string][]string{}
		for _, family := range []string{"user-legacy-config", "user-xdg-config", "user-native-config", "project-config", "beads-dir-config", "project-local-config", "metadata-json", "legacy-config-json", "beads-hooks", "legacy-hooks", "git-hooks", "external-hook-managers", "routes-jsonl", "formulas", "formula-search-paths", "issues-jsonl", "interactions-jsonl"} {
			paths[family] = []string{filepath.Join(root, "absent-"+family)}
		}
		paths["project-config"], paths["beads-dir-config"] = []string{configPath}, []string{configPath}
		var data []byte
		err := store.WithRawDBTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
			source := plugdump.SQLSource{DB: tx}
			inventory, sidecars, err := source.NativeInventory(ctx, plugdump.NativeCapture{Public: true, StoreDir: store.cfg.Path, BeadsDir: store.cfg.Path, Machine: "disposable-control", SidecarDir: sidecarDir, Paths: paths})
			if err != nil {
				return err
			}
			for name, raw := range sidecars {
				if err := os.WriteFile(filepath.Join(sidecarDir, name), raw, 0o600); err != nil {
					return err
				}
			}
			before = map[string]plugdump.Table{}
			for _, family := range retained {
				table, err := source.ReadTable(ctx, family)
				if err != nil {
					return err
				}
				before[family] = table
			}
			data, err = plugdump.Build(ctx, source, inventory)
			return err
		})
		return data, err
	}
	fixed, err := build(t)
	if err != nil {
		t.Fatal(err)
	}
	fixedManifest, err := plugdump.Validate(bytes.NewReader(fixed))
	if err != nil {
		t.Fatal(err)
	}
	edgeVersion := func(manifest plugdump.Manifest) int {
		for _, kind := range manifest.Kinds {
			if kind.Name == "edge" {
				return kind.Version
			}
		}
		return 0
	}
	if got := edgeVersion(fixedManifest); got != plugdump.EdgeVersionTypedDependencies {
		t.Fatalf("fixed-only stored archive selected edge version %d", got)
	}
	insertEdges(true)
	exec(`CALL DOLT_ADD('-A')`)
	exec(`CALL DOLT_COMMIT('-m', 'disposable stored opaque dependency control')`)
	data, err := build(t)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if got := edgeVersion(manifest); got != plugdump.EdgeVersionLocalOpaque {
		t.Fatalf("stored opaque row selected edge version %d", got)
	}
	rowKey := func(value any) string {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	actual := map[string]map[string]int{}
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(header.Name, "records/") || !strings.HasSuffix(header.Name, ".ndjson") {
			continue
		}
		decoder := json.NewDecoder(reader)
		decoder.UseNumber()
		for {
			var record plugdump.Record
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			family := record.Source.Table
			if actual[family] == nil {
				actual[family] = map[string]int{}
			}
			actual[family][rowKey(record.Source.Fields)]++
			if record.Kind == "edge" {
				fields := record.Source.Fields
				if record.Body["from"] != fields["issue_id"] || record.Body["type"] != fields["type"] || record.Body["thread_id"] != fields["thread_id"] {
					t.Fatalf("stored edge identity changed: %+v", record)
				}
				arm, ok := record.Body["target_kind"].(string)
				if !ok || record.Body["to"] != fields[arm] {
					t.Fatalf("stored edge endpoint changed: %+v", record)
				}
				if rowKey(record.Body["metadata"]) != rowKey(fields["metadata"]) {
					t.Fatalf("stored edge metadata changed: %+v", record)
				}
			}
		}
	}
	for _, family := range retained {
		want := map[string]int{}
		for _, row := range before[family].Rows {
			fields := row
			if family == "issues" {
				// The durable issue source contract omits only transient row_lock
				// and derived is_blocked. Active rows retain every physical field.
				fields = make(plugdump.Row, len(row))
				for name, value := range row {
					if name != "row_lock" && name != "is_blocked" {
						fields[name] = value
					}
				}
			}
			want[rowKey(fields)]++
		}
		if !reflect.DeepEqual(actual[family], want) {
			t.Fatalf("stored %s rows/fields changed: got=%v want=%v", family, actual[family], want)
		}
	}
	for _, test := range []struct{ name, update, restore, want string }{
		{"unsupported known external arm", `UPDATE dependencies SET type='duplicates', depends_on_issue_id=NULL, depends_on_external='external:tracker:unsupported' WHERE id='opaque-null'`, `UPDATE dependencies SET type='custom-stored', depends_on_issue_id='test-jkl', depends_on_external=NULL WHERE id='opaque-null'`, "unsupported dependency type"},
		{"opaque active target", `UPDATE dependencies SET depends_on_issue_id=NULL, depends_on_wisp_id='test-stu' WHERE id='opaque-null'`, `UPDATE dependencies SET depends_on_issue_id='test-jkl', depends_on_wisp_id=NULL WHERE id='opaque-null'`, "unsupported dependency type"},
		{"catalog category conflict", "UPDATE config SET value='review:done' WHERE `key`='status.custom'", "UPDATE config SET value='review:active' WHERE `key`='status.custom'", "conflicting custom status"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec(test.update)
			t.Cleanup(func() { exec(test.restore) })
			// Commit actual test mutations, retaining honest source history.
			exec(`CALL DOLT_ADD('-A')`)
			exec(`CALL DOLT_COMMIT('-m', 'disposable stored refusal control')`)
			data, err := build(t)
			if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("stored invalid source did not refuse atomically: bytes=%d err=%v", len(data), err)
			}
		})
	}
}

// Genuine migrated SQL rows and observed sidecars exercise the explicit
// source-plane contract. This is a disposable producer control; it does not
// establish consumer workflow mapping, public replay or official delivery.
func TestNativeSourcePlaneContractFromStoredDoltRows(t *testing.T) {
	store, cleanup := setupConcurrentTestStore(t)
	defer cleanup()
	ctx, cancel := testContext(t)
	defer cancel()
	exec := func(t *testing.T, query string, args ...any) {
		t.Helper()
		if _, err := store.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(t *testing.T) {
		t.Helper()
		exec(t, `CALL DOLT_ADD('-A')`)
		exec(t, `CALL DOLT_COMMIT('-m', 'disposable physical source-plane control')`)
	}
	for _, id := range []string{"test-abc", "test-def"} {
		if err := store.CreateIssue(ctx, &types.Issue{ID: id, Title: "durable " + id, Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}, "plane-control"); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"test-abc", "test-ghi"} {
		exec(t, `INSERT INTO wisps (id,title,status,priority,issue_type,created_at,updated_at,ephemeral,no_history)
			VALUES (?,?,'review',2,'gate',?,?,1,1)`, id, "active "+id, at, at)
	}
	exec(t, `INSERT INTO custom_statuses (name,category) VALUES ('review','active')`)
	exec(t, `INSERT INTO custom_types (name) VALUES ('gate')`)
	if err := store.SetConfig(ctx, "status.custom", "review:active"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetConfig(ctx, "types.custom", "gate"); err != nil {
		t.Fatal(err)
	}
	exec(t, `INSERT INTO dependencies (id,issue_id,depends_on_issue_id,type,created_at,created_by,metadata,thread_id)
		VALUES ('shared-edge','test-abc','test-def','waits-for',?,'plane-control',?,'durable-thread')`, at, `{"gate":"","n":9007199254740993}`)
	exec(t, `INSERT INTO wisp_dependencies (id,issue_id,depends_on_wisp_id,type,created_at,created_by,metadata,thread_id)
		VALUES ('shared-edge','test-abc','test-ghi','waits-for',?,'plane-control',?,'active-thread')`, at, `{"gate":"all-children","also_blocks":true,"n":9007199254740993}`)
	exec(t, `INSERT INTO wisp_dependencies (id,issue_id,depends_on_external,type,created_at,created_by,metadata,thread_id)
		VALUES ('external-edge','test-abc','external:tracker:raw','tracks',?,'plane-control',?,'external-thread')`, at, `{"raw":true}`)
	exec(t, `INSERT INTO dependencies (id,issue_id,depends_on_wisp_id,type,created_at,created_by,metadata,thread_id)
		VALUES ('active-target','test-abc','test-ghi','blocks',?,'plane-control',NULL,'target-thread')`, at)
	exec(t, `INSERT INTO wisp_labels (issue_id,label) VALUES ('test-abc','active-label')`)
	exec(t, `INSERT INTO wisp_comments (id,issue_id,author,text,created_at) VALUES ('plane-comment','test-abc','plane-control','raw comment',?)`, at)
	exec(t, `INSERT INTO wisp_events (id,issue_id,event_type,actor,old_value,new_value,comment,created_at)
		VALUES ('plane-event','test-abc','created','plane-control',NULL,'review','raw event',?)`, at)
	exec(t, `INSERT INTO wisp_child_counters (parent_id,last_child) VALUES ('test-abc',7)`)
	commit(t)
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	configBytes := []byte("# raw process config observation\nstatus:\n  custom: [review:active]\ntypes:\n  custom: [gate]\n")
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	retained := []string{"issues", "wisps", "dependencies", "wisp_dependencies", "wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters", "custom_statuses", "custom_types"}
	var before map[string]plugdump.Table
	build := func(t *testing.T, contract string) ([]byte, error) {
		t.Helper()
		sidecarDir := t.TempDir()
		paths := map[string][]string{}
		for _, family := range []string{"user-legacy-config", "user-xdg-config", "user-native-config", "project-config", "beads-dir-config", "project-local-config", "metadata-json", "legacy-config-json", "beads-hooks", "legacy-hooks", "git-hooks", "external-hook-managers", "routes-jsonl", "formulas", "formula-search-paths", "issues-jsonl", "interactions-jsonl"} {
			paths[family] = []string{filepath.Join(root, "absent-"+family)}
		}
		paths["project-config"], paths["beads-dir-config"] = []string{configPath}, []string{configPath}
		var data []byte
		err := store.WithRawDBTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
			source := plugdump.SQLSource{DB: tx}
			inventory, sidecars, err := source.NativeInventory(ctx, plugdump.NativeCapture{Public: true, StoreDir: store.cfg.Path, BeadsDir: store.cfg.Path, Machine: "stored-plane-control", SidecarDir: sidecarDir, Paths: paths, SourcePlaneAddressContract: contract})
			if err != nil {
				return err
			}
			if inventory.SourcePlaneAddressContract != contract {
				t.Fatal("native inventory lost explicit selector")
			}
			encoded, err := plugdump.MarshalNativeInventory(inventory)
			if err != nil {
				return err
			}
			inventory, err = plugdump.ReadInventory(bytes.NewReader(encoded))
			if err != nil {
				return err
			}
			for name, raw := range sidecars {
				if err := os.WriteFile(filepath.Join(sidecarDir, name), raw, 0o600); err != nil {
					return err
				}
			}
			before = map[string]plugdump.Table{}
			for _, family := range retained {
				table, err := source.ReadTable(ctx, family)
				if err != nil {
					return err
				}
				before[family] = table
			}
			data, err = plugdump.Build(ctx, source, inventory)
			return err
		})
		return data, err
	}
	data, err := build(t, plugdump.SourcePlaneAddressContract)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(data))
	if err != nil || manifest.SourcePlaneAddressContract != plugdump.SourcePlaneAddressContract {
		t.Fatalf("marker/readback: %+v %v", manifest, err)
	}
	if manifest.History == "verified" {
		t.Fatal("active raw capture claimed portable history completion")
	}
	key := func(value any) string {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	actual := map[string]map[string]int{}
	physicalEdges := map[string]map[string]bool{}
	rawConfigFound := false
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "config/sources.json" {
			raw, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			rawConfigFound = bytes.Contains(raw, []byte("review:active"))
			continue
		}
		if !strings.HasPrefix(header.Name, "records/") {
			continue
		}
		decoder := json.NewDecoder(reader)
		decoder.UseNumber()
		for {
			var record plugdump.Record
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			family := record.Source.Table
			if actual[family] == nil {
				actual[family] = map[string]int{}
			}
			actual[family][key(record.Source.Fields)]++
			if record.Kind == "edge" {
				if physicalEdges[family] == nil {
					physicalEdges[family] = map[string]bool{}
				}
				if physicalEdges[family][record.ID] {
					t.Fatal("duplicate physical edge tuple")
				}
				physicalEdges[family][record.ID] = true
				fields := record.Source.Fields
				if len(fields) != 10 || record.ID != fields["id"] || record.Body["from"] != fields["issue_id"] || record.Body["type"] != fields["type"] || record.Body["thread_id"] != fields["thread_id"] || key(record.Body["metadata"]) != key(fields["metadata"]) {
					t.Fatalf("stored edge raw identity/metadata changed: %+v", record)
				}
				arm, ok := record.Body["target_kind"].(string)
				if !ok || record.Body["to"] != fields[arm] {
					t.Fatalf("stored target arm changed: %+v", record)
				}
			}
		}
	}
	if !rawConfigFound || !physicalEdges["dependencies"]["shared-edge"] || !physicalEdges["wisp_dependencies"]["shared-edge"] {
		t.Fatal("raw catalog or cross-plane equal edge IDs lost")
	}
	for _, family := range retained {
		want := map[string]int{}
		for _, row := range before[family].Rows {
			fields := row
			if family == "issues" {
				fields = plugdump.Row{}
				for name, value := range row {
					if name != "row_lock" && name != "is_blocked" {
						fields[name] = value
					}
				}
			}
			want[key(fields)]++
		}
		if !reflect.DeepEqual(want, actual[family]) {
			t.Fatalf("stored %s raw multiset/count changed: got=%v want=%v", family, actual[family], want)
		}
	}
	legacy, err := build(t, "")
	if err == nil || len(legacy) != 0 || !strings.Contains(err.Error(), "unsupported wisp status") {
		t.Fatalf("legacy active-status refusal changed: bytes=%d err=%v", len(legacy), err)
	}
	for _, test := range []struct{ name, change, restore, want string }{
		{"undeclared active status", `UPDATE wisps SET status='undeclared' WHERE id='test-abc'`, `UPDATE wisps SET status='review' WHERE id='test-abc'`, "wisp status absent from project category catalog"},
		{"opaque active source", `UPDATE wisp_dependencies SET type='custom-kind',depends_on_wisp_id=NULL,depends_on_issue_id='test-def' WHERE id='shared-edge'`, `UPDATE wisp_dependencies SET type='waits-for',depends_on_wisp_id='test-ghi',depends_on_issue_id=NULL WHERE id='shared-edge'`, "durable issue source"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec(t, test.change)
			t.Cleanup(func() { exec(t, test.restore) })
			// Both mutations affect only dolt-ignored active tables. Their live
			// rows are read directly; there is no versioned change to commit.
			data, err := build(t, plugdump.SourcePlaneAddressContract)
			if err == nil || len(data) != 0 || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("stored refusal not atomic: bytes=%d err=%v", len(data), err)
			}
			t.Logf("stored source refusal: %v", err)
		})
	}
}
