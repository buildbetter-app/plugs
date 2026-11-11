package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/testutil"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
	"github.com/steveyegge/beads/internal/testutil/recipehooks"
	"github.com/steveyegge/beads/internal/types"
)

var publicRecipeHookFlags = recipehooks.RegisterFlags(flag.CommandLine)

// This public fixture has a deliberately complete paired SQL census, with
// history tracking opted in before its first source action. It is not a
// native-schema or pre-existing-ledger completeness claim. Every business
// record, event identity, event time and later Dolt commit comes from the
// supported Beads storage operations; none is filled in by the exporter.
func TestPlugDumpCompleteRootFromPublicDoltActions(t *testing.T) {
	runPlugDumpCompleteRootRecipe(t, publicRecipeHookFlags.Options())
}

func runPlugDumpCompleteRootRecipe(t *testing.T, options recipehooks.Options) []byte {
	t.Helper()
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit public source-history Dolt lane")
	}
	hooks, err := recipehooks.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	cmd := plugExportTestCommand(t)
	t.Setenv("BEADS_TEST_SERVER", "1")
	server := testutil.StartIsolatedDoltContainerHandle(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%s)/beads_test?parseTime=true", server.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path, err := bazeltest.RunfileEnv("BEADS_TEST_PLUG_SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var template map[string]plugdump.Table
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&template); err != nil {
		t.Fatal(err)
	}
	seedPublicHistoryCensus(t, ctx, db, template)
	// Dolt honors explicit operator tracking overrides. This new disposable
	// source retains events from inception; an old ignored event table cannot
	// obtain historical coverage by making this choice after the fact.
	for _, query := range []string{
		"INSERT INTO dolt_ignore (pattern, ignored) VALUES ('events', 0), ('ignored_schema_migrations', 0)",
		"CALL DOLT_ADD('-A')",
		"CALL DOLT_COMMIT('-m', 'public complete census before source actions')",
	} {
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(server.Port)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &dolt.Config{Path: t.TempDir(), Database: "beads_test", ServerHost: "127.0.0.1", ServerPort: port, ServerUser: "root", DisableAutoStart: true, CommitterName: "Public source", CommitterEmail: "source@example.test"}
	s, err := dolt.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	issue := &types.Issue{Title: "Public source created through Beads", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, Payload: `{"n":9007199254740993,"null":null}`, Timeout: time.Duration(9007199254740993)}
	if err := s.CreateIssue(ctx, issue, "public-source-author"); err != nil {
		t.Fatal(err)
	}
	if issue.ID == "" {
		t.Fatal("source driver did not assign the issue identity")
	}
	if err := s.UpdateIssue(ctx, issue.ID, map[string]interface{}{"title": "Public source updated through Beads"}, "public-source-author"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddLabel(ctx, issue.ID, "public-source-label", "public-source-author"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddComment(ctx, issue.ID, "public-source-author", "Public source comment through Beads"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.ReadOnly = true
	s, err = dolt.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	path, err = bazeltest.RunfileEnv("BEADS_TEST_PLUG_INVENTORY")
	if err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	data = bytes.ReplaceAll(data, []byte("$FIXTURE_ROOT"), []byte(filepath.ToSlash(root)))
	inventory, err := plugdump.ReadInventory(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	inventory.History = "unavailable"
	inventory.Projects = []plugdump.Project{{Prefix: "demo", IDMode: "hash", HashLength: 3, ChildCounters: map[string]uint64{}}}
	inventory.ConfigPolicy = map[string]string{"config/issue_prefix": "public"}
	data, err = json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	exportPlugInventory = filepath.Join(root, "inventory.json")
	if err := os.WriteFile(exportPlugInventory, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, rootCtx, readonlyMode, jsonOutput = s, ctx, true, false
	exportFormat, exportOutput = plugdump.Format, filepath.Join(root, "complete-root.tar")
	exportAll, exportIncludeInfra, exportScrub, exportNoMemories, exportIncludeMemories, exportVerbose = false, false, false, false, false, false
	exportExcludeOwners = nil
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(exportOutput)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(first))
	if err != nil {
		t.Fatal(err)
	}
	history := plugExportHistory(t, first)
	logPublicHistoryReadbacks(t, ctx, s, history)
	if manifest.History != "verified" || history.Range.Status != "verified" || !history.Range.Complete || history.Range.First != 1 || history.Range.Head != uint64(len(history.Commits)) || len(history.Range.Frames) != len(history.Commits) || len(history.Range.Gaps)+len(history.Range.Baseline)+len(history.Range.BaselineEvents) != 0 {
		t.Fatalf("actual source root/event coverage incomplete: status=%s reason=%s complete=%t first=%d head=%d commits=%d gaps=%+v", history.Range.Status, history.Range.Reason, history.Range.Complete, history.Range.First, history.Range.Head, len(history.Commits), history.Range.Gaps)
	}
	if len(history.Commits[0].Parents) != 0 || len(history.Range.Frames[0].Records)+len(history.Range.Frames[0].Events) != 0 || len(history.Events) != 4 {
		t.Fatal("source root was not empty or actual Beads action events are missing")
	}
	actions := map[string]int{
		"bd: create " + issue.ID:    1,
		"bd: update " + issue.ID:    2,
		"bd: label add " + issue.ID: 3,
		"bd: comment " + issue.ID:   4,
	}
	for _, frame := range history.Range.Frames {
		message, _ := history.Commits[frame.Sequence-1].Metadata["message"].(string)
		if count, ok := actions[message]; ok {
			if len(frame.Events) != count {
				t.Fatalf("actual %s commit retained %d events, want %d", message, len(frame.Events), count)
			}
			delete(actions, message)
		}
	}
	if len(actions) != 0 {
		t.Fatalf("driver source commits missing: %v", actions)
	}
	eventTypes := map[string]bool{}
	for _, event := range history.Events {
		if event["issue_id"] != issue.ID || event["actor"] != "public-source-author" {
			t.Fatal("source action identity/attribution changed")
		}
		eventType, _ := event["event_type"].(string)
		eventTypes[eventType] = true
	}
	for _, eventType := range []types.EventType{types.EventCreated, types.EventUpdated, types.EventLabelAdded, types.EventCommented} {
		if !eventTypes[string(eventType)] {
			t.Fatalf("actual source event %s missing", eventType)
		}
	}
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(exportOutput)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("same actual source did not produce byte-identical archives")
	}
	reader := tar.NewReader(bytes.NewReader(first))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("PUBLIC_SOURCE_MEMBER name=%s bytes=%d sha256=%x", header.Name, len(body), sha256.Sum256(body))
	}
	t.Logf("PUBLIC_SOURCE_COMPLETE issue_id=%s commits=%d events=%d archive_bytes=%d archive_sha256=%x", issue.ID, len(history.Commits), len(history.Events), len(first), sha256.Sum256(first))
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	defer binary.Close()
	binaryHash := sha256.New()
	if _, err := io.Copy(binaryHash, binary); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_SOURCE_EXPORTER_EXECUTABLE_SHA256=%x", binaryHash.Sum(nil))
	if os.Getenv("PLUG_TEST_EXPORT_PUBLIC_HISTORY") == "1" {
		t.Logf("PUBLIC_COMPLETE_SOURCE_ARCHIVE_BASE64=%s", base64.StdEncoding.EncodeToString(first))
	}
	if options.RetainStore != "" {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hooks.Handoff(ctx, server); err != nil {
		t.Fatal(err)
	}
	return first
}

func seedPublicHistoryCensus(t *testing.T, ctx context.Context, db *sql.DB, template map[string]plugdump.Table) {
	t.Helper()
	names := make([]string, 0, len(template))
	for name := range template {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		table := template[name]
		definitions := make([]string, len(table.Columns))
		for i, column := range table.Columns {
			typeName := "TEXT"
			for _, row := range table.Rows {
				switch row[column].(type) {
				case json.Number:
					typeName = "BIGINT"
				case map[string]any, []any:
					typeName = "JSON"
				}
			}
			if name == "issues" || name == "events" {
				switch column {
				case "created_at", "updated_at", "closed_at", "started_at", "due_at", "defer_until", "compacted_at", "last_activity":
					typeName = "DATETIME(6)"
				case "timeout_ns":
					typeName = "BIGINT"
				case "payload":
					typeName = "JSON"
				}
			}
			definitions[i] = "`" + column + "` " + typeName
		}
		if _, err := db.ExecContext(ctx, "CREATE TABLE `"+name+"` ("+strings.Join(definitions, ",")+")"); err != nil {
			t.Fatal(err)
		}
		// Only the paired migration cursor is seeded. Business/event/identity
		// fixture rows are deliberately not copied into this new source.
		if name == "schema_migrations" || name == "ignored_schema_migrations" {
			for _, row := range table.Rows {
				if _, err := db.ExecContext(ctx, "INSERT INTO `"+name+"` (version, content_hash) VALUES (?, ?)", fmt.Sprint(row["version"]), row["content_hash"]); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO config (`key`, value) VALUES ('issue_prefix', 'demo')"); err != nil {
		t.Fatal(err)
	}
}

func logPublicHistoryReadbacks(t *testing.T, ctx context.Context, s storage.RawDBTxAccessor, history plugdump.SourceHistory) {
	t.Helper()
	if err := s.WithRawDBTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		source := plugdump.SQLSource{DB: tx}
		log, err := source.ReadSourceCommits(ctx)
		if err != nil {
			return err
		}
		if !log.Clean {
			return fmt.Errorf("export changed the actual source working set")
		}
		wantCommits := append([]plugdump.SourceCommit{}, log.Commits...)
		gotCommits := append([]plugdump.SourceCommit{}, history.Commits...)
		sort.Slice(wantCommits, func(i, j int) bool { return wantCommits[i].ID < wantCommits[j].ID })
		sort.Slice(gotCommits, func(i, j int) bool { return gotCommits[i].ID < gotCommits[j].ID })
		want, _ := json.Marshal(wantCommits)
		got, _ := json.Marshal(gotCommits)
		if !bytes.Equal(want, got) || log.Head != history.Commits[len(history.Commits)-1].ID {
			return fmt.Errorf("archive commit metadata/parents/head differ from actual SQL census")
		}
		ancestors, err := tx.QueryContext(ctx, "SELECT commit_hash, parent_hash FROM dolt_commit_ancestors ORDER BY commit_hash, parent_hash")
		if err != nil {
			return err
		}
		for ancestors.Next() {
			var id string
			var parent sql.NullString
			if err := ancestors.Scan(&id, &parent); err != nil {
				_ = ancestors.Close()
				return err
			}
			var parentValue any
			if parent.Valid {
				parentValue = parent.String
			}
			body, _ := json.Marshal(plugdump.Row{"commit_hash": id, "parent_hash": parentValue})
			t.Logf("PUBLIC_SOURCE_RAW_ANCESTRY=%s", body)
		}
		readErr := ancestors.Err()
		_ = ancestors.Close()
		if readErr != nil {
			return readErr
		}
		for _, name := range []string{"issues", "events"} {
			table, err := source.ReadTable(ctx, name)
			if err != nil {
				return err
			}
			if name == "events" {
				sort.Slice(table.Rows, func(i, j int) bool {
					return plugdump.SourceEventID(table.Rows[i]) < plugdump.SourceEventID(table.Rows[j])
				})
				want, _ := json.Marshal(table.Rows)
				got, _ := json.Marshal(history.Events)
				if !bytes.Equal(want, got) {
					return fmt.Errorf("archive event identities/order/values differ from stored SQL")
				}
			}
			body, _ := json.Marshal(table)
			t.Logf("PUBLIC_SOURCE_CURRENT_%s=%s", name, body)
		}
		for _, commit := range history.Commits {
			image, err := source.ReadSourceImage(ctx, commit.ID)
			if err != nil {
				return err
			}
			body, err := json.Marshal(struct {
				Commit plugdump.SourceCommit     `json:"commit"`
				Tables map[string]plugdump.Table `json:"tables"`
			}{commit, image})
			if err != nil {
				return err
			}
			t.Logf("PUBLIC_SOURCE_STORED_IMAGE=%s", body)
			for _, frame := range history.Range.Frames {
				if frame.CommitID == commit.ID {
					checkPublicStoredHistoryFrame(t, frame, image)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func checkPublicStoredHistoryFrame(t *testing.T, frame plugdump.SourceHistoryFrame, image map[string]plugdump.Table) {
	t.Helper()
	events := append([]plugdump.Row{}, image["events"].Rows...)
	sort.Slice(events, func(i, j int) bool { return plugdump.SourceEventID(events[i]) < plugdump.SourceEventID(events[j]) })
	want, _ := json.Marshal(events)
	got, _ := json.Marshal(frame.Events)
	if !bytes.Equal(want, got) {
		t.Fatalf("frame %d changed stored event identity/order/values/time", frame.Sequence)
	}
	counts := map[string]int{}
	for _, record := range frame.Records {
		if record.Kind == "bead" {
			labels := []string{}
			for _, row := range image["labels"].Rows {
				if row["issue_id"] == record.ID {
					labels = append(labels, row["label"].(string))
				}
			}
			sort.Strings(labels)
			want, _ := json.Marshal(labels)
			got, _ := json.Marshal(record.Body["labels"])
			if !bytes.Equal(want, got) {
				t.Fatalf("frame %d changed stored labels", frame.Sequence)
			}
		}
		found := false
		for _, row := range image[record.Source.Table].Rows {
			key := "id"
			if record.Source.Table == "config" || record.Source.Table == "metadata" {
				key = "key"
			}
			if row[key] != record.Source.Fields[key] {
				continue
			}
			fields := plugdump.Row{}
			for name, value := range row {
				if record.Source.Table != "issues" || name != "row_lock" && name != "is_blocked" {
					fields[name] = value
				}
			}
			want, _ := json.Marshal(fields)
			got, _ := json.Marshal(record.Source.Fields)
			if !bytes.Equal(want, got) {
				t.Fatalf("frame %d changed %s stored fields/JSON/BIGINT/time", frame.Sequence, record.Source.Table)
			}
			found = true
			break
		}
		if !found {
			t.Fatalf("frame %d invented source %s/%s", frame.Sequence, record.Source.Table, record.ID)
		}
		counts[record.Source.Table]++
	}
	for _, table := range []string{"issues", "comments", "config", "metadata", "dependencies"} {
		if counts[table] != len(image[table].Rows) {
			t.Fatalf("frame %d omitted or duplicated %s rows", frame.Sequence, table)
		}
	}
}

// This is a database/sql protocol fixture, not a database engine. All rows are
// synthetic; the real RunE, SQLSource reads, mapping, tar validation and atomic
// output path run together without opening a Beads store.
type plugSQLFixture struct {
	tables     map[string]plugdump.Table
	readOnly   bool
	rolledBack bool
	censuses   int
}

func (f *plugSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *plugSQLFixture) Driver() driver.Driver                        { return plugFixtureDriver{} }

type plugFixtureDriver struct{}

func (plugFixtureDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("fixture requires its connector")
}
func (f *plugSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("fixture does not prepare writes")
}
func (f *plugSQLFixture) Close() error { return nil }
func (f *plugSQLFixture) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("fixture requires BeginTx read-only options")
}
func (f *plugSQLFixture) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	if !options.ReadOnly || options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, fmt.Errorf("expected a read-only serializable transaction")
	}
	f.readOnly = true
	f.rolledBack = false
	return f, nil
}
func (f *plugSQLFixture) Commit() error   { return fmt.Errorf("read-only exporter must never commit") }
func (f *plugSQLFixture) Rollback() error { f.rolledBack = true; return nil }

func (f *plugSQLFixture) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !f.readOnly || f.rolledBack {
		return nil, fmt.Errorf("fixture read outside its active read-only transaction")
	}
	if rows, ok := f.historyQuery(query); ok {
		return rows, nil
	}
	query = strings.TrimSuffix(query, " AS OF '"+plugFixtureCommitID+"'")
	if strings.HasPrefix(query, "SELECT table_name FROM information_schema.tables ") {
		var names []string
		for name := range f.tables {
			names = append(names, name)
		}
		sort.Strings(names)
		var values [][]driver.Value
		for _, name := range names {
			values = append(values, []driver.Value{name})
		}
		return &plugSQLRows{columns: []string{"table_name"}, values: values}, nil
	}
	if !strings.HasPrefix(query, "SELECT * FROM `") || !strings.HasSuffix(query, "`") {
		return nil, fmt.Errorf("unexpected query in read-only fixture")
	}
	name := strings.TrimSuffix(strings.TrimPrefix(query, "SELECT * FROM `"), "`")
	table, ok := f.tables[name]
	if !ok {
		return nil, fmt.Errorf("unknown fixture table")
	}
	rows := &plugSQLRows{columns: table.Columns}
	for _, row := range table.Rows {
		var values []driver.Value
		for _, column := range table.Columns {
			value := row[column]
			switch value := value.(type) {
			case json.Number:
				number, err := value.Int64()
				if err != nil {
					return nil, err
				}
				values = append(values, number)
			case map[string]any:
				if encoded, ok := value["$bytes"].(string); ok {
					data, err := base64.StdEncoding.DecodeString(encoded)
					if err != nil {
						return nil, err
					}
					values = append(values, data)
				} else {
					data, err := json.Marshal(value)
					if err != nil {
						return nil, err
					}
					values = append(values, data)
				}
			case nil, string, bool:
				values = append(values, value)
			default:
				return nil, fmt.Errorf("unsupported fixture value")
			}
		}
		rows.values = append(rows.values, values)
	}
	return rows, nil
}

const plugFixtureCommitID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// This protocol fixture explicitly declares one synthetic complete root image.
// The separate source-history lane reads actual pinned Dolt ancestry/images;
// these fixed SQL replies only exercise command dispatch and scoped reads.
func (f *plugSQLFixture) historyQuery(query string) (driver.Rows, bool) {
	switch query {
	case "SELECT DOLT_HASHOF('HEAD')":
		f.censuses++
		return &plugSQLRows{columns: []string{"hash"}, values: [][]driver.Value{{plugFixtureCommitID}}}, true
	case "SELECT table_name, staged, status FROM dolt_status":
		return &plugSQLRows{columns: []string{"table_name", "staged", "status"}}, true
	case "SELECT * FROM dolt_log LIMIT 10001":
		return &plugSQLRows{columns: []string{"commit_hash", "message"}, values: [][]driver.Value{{plugFixtureCommitID, "public synthetic protocol root"}}}, true
	case "SELECT commit_hash, parent_hash FROM dolt_commit_ancestors LIMIT 20001":
		return &plugSQLRows{columns: []string{"commit_hash", "parent_hash"}, values: [][]driver.Value{{plugFixtureCommitID, nil}}}, true
	case "SHOW FULL TABLES AS OF '" + plugFixtureCommitID + "'":
		names := make([]string, 0, len(f.tables))
		for name := range f.tables {
			names = append(names, name)
		}
		sort.Strings(names)
		rows := &plugSQLRows{columns: []string{"table_name", "table_type"}}
		for _, name := range names {
			rows.values = append(rows.values, []driver.Value{name, "BASE TABLE"})
		}
		return rows, true
	default:
		return nil, false
	}
}

func plugExportHistory(t *testing.T, data []byte) plugdump.SourceHistory {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			t.Fatal("source history member missing from command archive")
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name != plugdump.SourceHistoryMember {
			continue
		}
		var history plugdump.SourceHistory
		decoder := json.NewDecoder(reader)
		decoder.UseNumber()
		if err = decoder.Decode(&history); err != nil {
			t.Fatal(err)
		}
		return history
	}
}

type plugSQLRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *plugSQLRows) Columns() []string { return r.columns }
func (r *plugSQLRows) Close() error      { return nil }
func (r *plugSQLRows) ColumnTypeDatabaseTypeName(index int) string {
	switch r.columns[index] {
	case "metadata":
		return "JSON"
	case "durable_state", "snapshot_json":
		return "LONGBLOB"
	default:
		return "TEXT"
	}
}
func (r *plugSQLRows) Next(dest []driver.Value) error {
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

type plugFixtureStore struct {
	storage.DoltStorage
	db               *sql.DB
	afterCallbackErr error
}

func (s plugFixtureStore) WithRawDBTx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return errors.Join(fn(tx), s.afterCallbackErr)
}

func plugExportTestCommand(t *testing.T) *cobra.Command {
	t.Helper()
	isolateBeadsDirForTest(t)
	saveAndRestoreGlobals(t)
	oldFormat, oldInventory, oldOutput := exportFormat, exportPlugInventory, exportOutput
	oldReadonly, oldJSON, oldRootCtx := readonlyMode, jsonOutput, rootCtx
	oldOwners, oldVerbose := exportExcludeOwners, exportVerbose
	changed := map[string]bool{}
	for _, name := range []string{"format", "plug-inventory", "output"} {
		changed[name] = exportCmd.Flags().Lookup(name).Changed
	}
	cmd := &cobra.Command{Use: "export"}
	t.Cleanup(func() {
		exportFormat, exportPlugInventory, exportOutput = oldFormat, oldInventory, oldOutput
		readonlyMode, jsonOutput, rootCtx = oldReadonly, oldJSON, oldRootCtx
		exportExcludeOwners, exportVerbose = oldOwners, oldVerbose
		for _, name := range []string{"format", "plug-inventory", "output"} {
			exportCmd.Flags().Lookup(name).Changed = changed[name]
		}
	})
	return cmd
}

func TestPlugDumpCommandEndToEnd(t *testing.T) {
	cmd := plugExportTestCommand(t)
	sourcePath, err := bazeltest.RunfileEnv("BEADS_TEST_PLUG_SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &plugSQLFixture{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&fixture.tables); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() { _ = db.Close() })
	store = plugFixtureStore{db: db}
	rootCtx = context.Background()
	readonlyMode, jsonOutput = true, false
	exportAll, exportIncludeInfra, exportScrub, exportNoMemories, exportIncludeMemories, exportVerbose = false, false, false, false, false, false
	exportExcludeOwners = nil
	root := t.TempDir()
	inventoryPath, err := bazeltest.RunfileEnv("BEADS_TEST_PLUG_INVENTORY")
	if err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.ReplaceAll(data, []byte("$FIXTURE_ROOT"), []byte(filepath.ToSlash(root)))
	inventoryPath = filepath.Join(root, "inventory.json")
	if err := os.WriteFile(inventoryPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "dump.tar")
	if err := exportCmd.ParseFlags([]string{"--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", output}); err != nil {
		t.Fatal(err)
	}
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(first))
	if err != nil || manifest.Projects[0].LastID != 8 || !fixture.readOnly || !fixture.rolledBack {
		t.Fatalf("failed command archive/transaction contract: %v", err)
	}
	history := plugExportHistory(t, first)
	if fixture.censuses != 2 || manifest.History != "verified" || !history.Range.Complete || history.Range.First != 1 || history.Range.Head != 1 || len(history.Range.Frames) != 1 || history.Range.Frames[0].CommitID != plugFixtureCommitID {
		t.Fatalf("command did not preserve its explicit synthetic source proof: censuses=%d history=%s range=%+v", fixture.censuses, manifest.History, history.Range)
	}
	t.Logf("real export handler: %d archive bytes, sha256=%x, %d registered kinds, top-level counter=%d, child counter=%d; read-only transaction rolled back", len(first), sha256.Sum256(first), len(manifest.Kinds), manifest.Projects[0].LastID, manifest.Projects[0].ChildCounters["demo-7"])
	if artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); artifacts != "" {
		if err := os.WriteFile(filepath.Join(artifacts, "synthetic-plug-dump-1.tar"), first, 0o600); err != nil {
			t.Fatal(err)
		}
		manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(artifacts, "synthetic-manifest.json"), manifestJSON, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("dump file must be private")
	}
	for name, table := range fixture.tables {
		for i, j := 0, len(table.Rows)-1; i < j; i, j = i+1, j-1 {
			table.Rows[i], table.Rows[j] = table.Rows[j], table.Rows[i]
		}
		fixture.tables[name] = table
	}
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("equivalent command exports were not byte-identical")
	}
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	exportOutput = ""
	if err := exportCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, stdout.Bytes()) {
		t.Fatal("stdout serialization differs from file serialization")
	}
	// A failed storage scope must finish before publishing any archive bytes.
	store = plugFixtureStore{db: db, afterCallbackErr: errors.New("scope cleanup failed")}
	stdout.Reset()
	if err := exportCmd.RunE(cmd, nil); err == nil || stdout.Len() != 0 {
		t.Fatal("failed storage scope published an archive to stdout")
	}
	exportOutput = output
	if err := os.WriteFile(output, []byte("existing destination"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := exportCmd.RunE(cmd, nil); err == nil {
		t.Fatal("storage scope failure accepted")
	}
	if data, err := os.ReadFile(output); err != nil || string(data) != "existing destination" {
		t.Fatal("failed storage scope replaced existing output")
	}
	store = plugFixtureStore{db: db}
	// A refusal leaves both an existing destination and stdout untouched.
	exportOutput = output
	if err := os.WriteFile(output, []byte("existing destination"), 0o600); err != nil {
		t.Fatal(err)
	}
	delete(fixture.tables, "leases")
	if err := exportCmd.RunE(cmd, nil); err == nil {
		t.Fatal("missing source family accepted")
	}
	unchanged, err := os.ReadFile(output)
	if err != nil || string(unchanged) != "existing destination" {
		t.Fatal("refusal replaced existing output")
	}
	exportOutput = ""
	stdout.Reset()
	if err := exportCmd.RunE(cmd, nil); err == nil || stdout.Len() != 0 {
		t.Fatal("refusal wrote partial stdout archive")
	}
	if exportCmd.Flags().Lookup("format").DefValue != "jsonl" {
		t.Fatal("legacy default changed")
	}
}
