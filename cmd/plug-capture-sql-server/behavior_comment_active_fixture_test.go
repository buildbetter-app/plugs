package main_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/steveyegge/beads/internal/captureclock"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
	"github.com/steveyegge/beads/issueops"
)

// This is a new public, disposable SOURCE fixture. The independent inputs and
// expected comment tuples are written before native execution. No Plug output
// supplies an expected value. Native import, CLI append, CLI full read and the
// native public storage page reader are explicitly different interfaces.
// Consumer import/reopen, official admission and FullS1 are outside this test.
func TestPublicNativeCommentActiveFixture(t *testing.T) {
	for _, prefix := range []string{"commentcody", "commentsignals"} {
		t.Run(prefix, func(t *testing.T) {
			f := newNativeFixtureForProject(t, prefix)
			out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if out == "" {
				t.Fatal("Bazel undeclared outputs required")
			}
			out = filepath.Join(out, prefix)
			if err := os.Mkdir(out, 0700); err != nil {
				t.Fatal(err)
			}
			goTime := time.Date(2026, 11, 11, 12, 0, 0, 123456789, time.UTC)
			sqlTime := time.Date(2026, 11, 11, 12, 0, 10, 654321000, time.UTC)
			future := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
			if !time.Now().Before(future) {
				t.Fatal("future monotonic fixture requires the real native wall clock before 2040")
			}
			f.step("comment-active-public-import", goTime, sqlTime)
			f.sqlClock(sqlTime)
			durable, active := prefix+"-a1b", prefix+"-c2d"
			equalDurable, equalActive := prefix+"-e3f", prefix+"-4de"
			expected := []commentFixtureTuple{}
			input := &bytes.Buffer{}
			for plane, anchor := range []string{durable, active, equalDurable, equalActive} {
				family := "comments"
				if plane%2 == 1 {
					family = "wisp_comments"
				}
				comments := []map[string]any{}
				count := 503
				if plane >= 2 {
					count = 3
					// A preserved past seed proves newest<now before the three
					// omitted-time comments reach now, equality, and future.
					id := fmt.Sprintf("7%d000000-0000-4000-8000-%012d", plane, 0)
					stamp := goTime.Truncate(time.Second).Add(-time.Minute)
					text := "preserved past source seed"
					author := "Imported Past Source"
					comments = append(comments, map[string]any{"id": id, "issue_id": anchor, "author": author, "text": text, "created_at": stamp.Format(time.RFC3339Nano)})
					expected = append(expected, commentFixtureTuple{Project: prefix, Table: family, ID: id, IssueID: anchor, Author: author, Text: text, CreatedAt: stamp})
				}
				for i := 0; i < count; i++ {
					ordinal := count - i
					stamp := future
					if plane >= 2 {
						ordinal = i + 1
						stamp = goTime.Truncate(time.Second).Add(time.Duration(i) * time.Second)
					}
					id := fmt.Sprintf("7%d000000-0000-4000-8000-%012d", plane, ordinal)
					text := fmt.Sprintf("%s public import comment %03d", anchor, ordinal)
					if plane < 2 {
						switch ordinal {
						case 1:
							text = ""
						case 2:
							text = " \t\n "
						case 3:
							text = "\n  λ Unicode 雪\n"
						}
					}
					author := "Imported Source " + strconv.Itoa(plane)
					comment := map[string]any{"id": id, "issue_id": anchor, "author": author, "text": text}
					if plane < 2 {
						comment["created_at"] = stamp.Format(time.RFC3339Nano)
					}
					comments = append(comments, comment)
					expected = append(expected, commentFixtureTuple{Project: prefix, Table: family, ID: id, IssueID: anchor, Author: author, Text: text, CreatedAt: stamp})
				}
				issue := map[string]any{
					"id": anchor, "title": "public comment source " + anchor,
					"status": "open", "priority": 2, "issue_type": "task",
					"ephemeral":  plane%2 == 1,
					"created_at": goTime.Format(time.RFC3339Nano), "updated_at": goTime.Format(time.RFC3339Nano),
					"metadata":      map[string]any{"fixture": prefix, "empty": "", "json_null": nil, "integer": 17},
					"source_system": "public-native-comment-fixture", "sender": "public-source-" + prefix,
					"comments": comments,
				}
				if plane%2 == 1 {
					issue["storage_class"], issue["wisp_type"] = "ephemeral", "patrol"
				}
				if err := json.NewEncoder(input).Encode(issue); err != nil {
					t.Fatal(err)
				}
			}
			for _, thread := range []struct{ table, anchor string }{{"comments", durable}, {"wisp_comments", active}} {
				for i, body := range []string{"\n  native live λ 雪\n", "second native live comment"} {
					stamp := future.Add(time.Duration(i+1) * time.Second)
					author := "Native Public Author"
					id := independentCommentFixtureID(thread.table, thread.anchor, author, body, stamp)
					expected = append(expected, commentFixtureTuple{Project: prefix, Table: thread.table, ID: id, IssueID: thread.anchor, Author: author, Text: body, CreatedAt: stamp})
				}
			}
			sortCommentFixtureTuples(expected)
			commentFixtureWrite(t, filepath.Join(out, "expected-comments.json"), map[string]any{
				"format": "public-native-comment-tuples/1", "project_prefix": prefix,
				"source": "independently authored public input and frozen UUIDv5 rule; before native execution",
				"fields": []string{"project", "table", "id", "issue_id", "author", "text", "created_at"},
				"order":  "created_at ASC, id ASC within exact project/table/issue_id",
				"tuples": expected, "go_time": goTime, "sql_time": sqlTime,
				"future": future, "suggested_restore_time": "2040-01-02T03:05:00Z",
				"scope": "source fixture only; consumer supplies its own current write clock",
			})
			inputPath := filepath.Join(f.work, "public-comment-input.jsonl")
			if err := os.WriteFile(inputPath, input.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, "public-comment-input.jsonl"), input.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			commentFixtureCommand(t, f, out, "import", false, "import", inputPath, "--json")
			// All six active families must be produced by genuine native commands.
			child := f.create("public active comment child", "--ephemeral", "--parent", active)
			if child != active+".1" {
				t.Fatal("native public child allocator changed source identity")
			}
			commentFixtureCommand(t, f, out, "label-parent", false, "label", "add", active, "comment-active", "--json")
			commentFixtureCommand(t, f, out, "label-child", false, "label", "add", child, "comment-child", "--json")
			commentFixtureCommand(t, f, out, "related", false, "dep", "add", child, durable, "--type", "related", "--json")
			// Real live commands use time.Now, NOT the captured Go input. Future
			// imported comments make the max+1 result independently predictable.
			for _, thread := range []struct{ table, anchor string }{{"comments", durable}, {"wisp_comments", active}} {
				for i, body := range []string{"\n  native live λ 雪\n", "second native live comment"} {
					name := fmt.Sprintf("append-%s-%d", thread.table, i+1)
					before := commentFixtureObserve(t, f)
					data := commentFixtureCommand(t, f, out, name, false, "comments", "add", thread.anchor, body, "--author", "Native Public Author", "--json")
					var actual commentFixtureComment
					if err := json.Unmarshal(data, &actual); err != nil {
						t.Fatal(err)
					}
					want := commentFixtureComment{ID: independentCommentFixtureID(thread.table, thread.anchor, "Native Public Author", body, future.Add(time.Duration(i+1)*time.Second)), IssueID: thread.anchor, Author: "Native Public Author", Text: body, CreatedAt: future.Add(time.Duration(i+1) * time.Second)}
					if !reflect.DeepEqual(actual, want) || !time.Now().Before(future) {
						t.Fatalf("native live max+1 tuple differs: got=%+v want=%+v", actual, want)
					}
					after := commentFixtureObserve(t, f)
					assertCommentFixtureAppend(t, before, after, thread.table, thread.anchor)
					commentFixtureWrite(t, filepath.Join(out, name+"-conservation.json"), map[string]any{"before": before, "after": after, "expected": want})
				}
			}
			for _, refusal := range []struct {
				name    string
				args    []string
				message string
			}{
				{"empty-body", []string{"comments", "add", durable, "", "--json"}, "comment text cannot be empty"},
				{"whitespace-body", []string{"comments", "add", active, " \t\n ", "--json"}, "comment text cannot be empty"},
				{"empty-id", []string{"comments", "add", "", "refused", "--json"}, "resolving : refusing an empty string as an issue ID"},
				{"unknown-id", []string{"comments", "add", prefix + "-deadbeef", "refused", "--json"}, fmt.Sprintf("resolving %s-deadbeef: no issue found matching %q", prefix, prefix+"-deadbeef")},
				{"abbreviated-id", []string{"comments", "add", prefix + "-a1", "refused", "--json"}, "id abbreviations are not accepted on comment writes; use the full id from `bd show " + prefix + "-a1`"},
			} {
				before := commentFixtureObserve(t, f)
				data := commentFixtureCommand(t, f, out, refusal.name, true, refusal.args...)
				var envelope struct {
					Error         string `json:"error"`
					SchemaVersion string `json:"schema_version"`
				}
				if err := json.Unmarshal(data, &envelope); err != nil || envelope.Error != refusal.message || envelope.SchemaVersion == "" {
					t.Fatalf("native CLI refusal envelope differs: %s: %s (%v)", refusal.name, data, err)
				}
				after := commentFixtureObserve(t, f)
				if !equalPhysicalCaptureJSON(before, after) {
					t.Fatalf("native refusal %s changed source state", refusal.name)
				}
				commentFixtureWrite(t, filepath.Join(out, refusal.name+"-conservation.json"), map[string]any{"before": before, "after": after, "expected_cli_error": refusal.message, "contract": "cmd/bd/comments.go exact write resolution; cmd/bd/flags.go requireTextFromSources; internal/utils/id_parser.go"})
			}
			for _, anchor := range []string{durable, active, equalDurable, equalActive} {
				data := commentFixtureCommand(t, f, out, "list-"+anchor, false, "comments", anchor, "--json")
				var actual []commentFixtureComment
				if err := json.Unmarshal(data, &actual); err != nil {
					t.Fatal(err)
				}
				if want := commentFixtureThread(expected, anchor); !reflect.DeepEqual(actual, want) {
					t.Fatalf("native CLI exact thread differs for %s: got=%d want=%d", anchor, len(actual), len(want))
				}
			}
			commentFixturePages(t, f, out, expected, durable, active, child, prefix+"-notfound")
			observed := commentFixtureObserve(t, f)
			commentFixtureActive(t, observed, prefix, durable, active, equalDurable, equalActive, child, out)
			for _, family := range []string{"wisps", "wisp_dependencies", "wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters"} {
				if len(observed.Tables[family].Rows) == 0 {
					t.Fatalf("actual active source family %s is empty", family)
				}
			}
			for _, family := range []string{"comments", "wisp_comments"} {
				want := []plugdump.Row{}
				for _, row := range expected {
					if row.Table == family {
						want = append(want, plugdump.Row{"id": row.ID, "issue_id": row.IssueID, "author": row.Author, "text": row.Text, "created_at": row.CreatedAt.Format(time.RFC3339Nano)})
					}
				}
				if !equalPhysicalCaptureJSON(canonicalPhysicalCaptureTable(plugdump.Table{Columns: observed.Tables[family].Columns, Rows: want}), observed.Tables[family]) {
					t.Fatalf("native %s SQL differs from independent complete tuple multiset", family)
				}
			}
			commentFixtureWrite(t, filepath.Join(out, "pre-export-sql.json"), observed)
			commentFixtureCapture(t, f, out, prefix, expected, observed)
			t.Logf("PUBLIC_NATIVE_COMMENT_ACTIVE_FIXTURE project=%s comments=%d wisp_comments=%d source_only=true", prefix, len(observed.Tables["comments"].Rows), len(observed.Tables["wisp_comments"].Rows))
		})
	}
}

type commentFixtureComment struct {
	ID        string    `json:"id"`
	IssueID   string    `json:"issue_id"`
	Author    string    `json:"author"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

type commentFixtureTuple struct {
	Project   string    `json:"project"`
	Table     string    `json:"table"`
	ID        string    `json:"id"`
	IssueID   string    `json:"issue_id"`
	Author    string    `json:"author"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

func sortCommentFixtureTuples(rows []commentFixtureTuple) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		if a.IssueID != b.IssueID {
			return a.IssueID < b.IssueID
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
}

func commentFixtureThread(rows []commentFixtureTuple, anchor string) []commentFixtureComment {
	result := []commentFixtureComment{}
	for _, row := range rows {
		if row.IssueID == anchor {
			result = append(result, commentFixtureComment{row.ID, row.IssueID, row.Author, row.Text, row.CreatedAt})
		}
	}
	return result
}

func commentFixtureActive(t *testing.T, observed commentFixtureObservation, prefix, durable, active, equalDurable, equalActive, child, out string) {
	t.Helper()
	wantMembership := map[string][]string{"issues": {durable, equalDurable}, "wisps": {active, equalActive, child}}
	for family, want := range wantMembership {
		got := []string{}
		for _, row := range observed.Tables[family].Rows {
			id, ok := row["id"].(string)
			if !ok {
				t.Fatal("native anchor ID is not text")
			}
			got = append(got, id)
			if id != child {
				for column, value := range map[string]any{"title": "public comment source " + id, "status": "open", "priority": 2, "issue_type": "task", "source_system": "public-native-comment-fixture", "sender": "public-source-" + prefix, "metadata": map[string]any{"fixture": prefix, "empty": "", "json_null": nil, "integer": 17}} {
					if !equalPhysicalCaptureJSON(row[column], value) {
						t.Fatalf("independent native %s anchor field differs: %s %s", family, id, column)
					}
				}
				if family == "wisps" && (row["storage_class"] != "ephemeral" || row["wisp_type"] != "patrol" || fmt.Sprint(row["ephemeral"]) != "1") {
					t.Fatal("native active classification differs from explicit public import")
				}
			}
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("native exact %s membership differs: got=%v want=%v", family, got, want)
		}
	}
	wantLabels := []plugdump.Row{{"issue_id": active, "label": "comment-active"}, {"issue_id": child, "label": "comment-child"}}
	if !equalPhysicalCaptureJSON(observed.Tables["wisp_labels"], canonicalPhysicalCaptureTable(plugdump.Table{Columns: []string{"issue_id", "label"}, Rows: wantLabels})) {
		t.Fatal("native exact active label multiset differs")
	}
	wantCounters := []plugdump.Row{{"parent_id": active, "last_child": 1}}
	if !equalPhysicalCaptureJSON(observed.Tables["wisp_child_counters"], canonicalPhysicalCaptureTable(plugdump.Table{Columns: []string{"parent_id", "last_child"}, Rows: wantCounters})) {
		t.Fatal("native active parent/last_child tuple differs")
	}
	deps := observed.Tables["wisp_dependencies"]
	if len(deps.Rows) != 2 || len(deps.Columns) != 10 {
		t.Fatal("native active dependency multiset/ten-column schema differs")
	}
	seen := map[string]bool{}
	for _, row := range deps.Rows {
		kind, ok := row["type"].(string)
		if !ok || seen[kind] || row["issue_id"] != child || row["depends_on_external"] != nil {
			t.Fatal("native active dependency identity/isolation differs")
		}
		seen[kind] = true
		switch kind {
		case "parent-child":
			if row["depends_on_wisp_id"] != active || row["depends_on_issue_id"] != nil {
				t.Fatal("native active parent target plane differs")
			}
		case "related":
			if row["depends_on_issue_id"] != durable || row["depends_on_wisp_id"] != nil {
				t.Fatal("native mixed-plane related target differs")
			}
		default:
			t.Fatal("unexpected native active relation")
		}
	}
	commentFixtureWrite(t, filepath.Join(out, "expected-active-membership.json"), map[string]any{"source": "independent public input and native creation contract; full typed original rows are in pre-export-sql and oracle raw states", "membership": wantMembership, "labels": wantLabels, "child_counters": wantCounters, "dependency_source": child, "parent_target": active, "related_target": durable, "dependency_columns": deps.Columns, "expected_dependency_rows": 2, "events": "actual original rows only; no event count inferred from comments or history"})
}

// Independent implementation of the frozen native ID encoding. Deliberately
// does not import rowid or call a native insert/mapper as the expected oracle.
func independentCommentFixtureID(table, anchor, author, body string, stamp time.Time) string {
	var content strings.Builder
	for _, field := range []string{anchor, author, body, stamp.UTC().Format("2006-01-02 15:04:05")} {
		fmt.Fprintf(&content, "v%d:%s", len(field), field)
	}
	digest := sha256.Sum256([]byte(content.String()))
	namespace, _ := hex.DecodeString("92da14b520a852528eb23839e97b2156")
	key := table + "\x1f0\x1f" + hex.EncodeToString(digest[:])
	hash := sha1.New() // UUIDv5 requires SHA1; this is not a security signature.
	_, _ = hash.Write(namespace)
	_, _ = hash.Write([]byte(key))
	id := hash.Sum(nil)[:16]
	id[6], id[8] = (id[6]&0x0f)|0x50, (id[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
}

func commentFixtureWrite(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func commentFixtureCommand(t *testing.T, f *nativeFixture, out, name string, refuse bool, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.bd, args...)
	cmd.Dir, cmd.Env = f.work, f.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now().UTC()
	err := cmd.Run()
	code := 0
	if err != nil {
		code = -1
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		}
	}
	commentFixtureWrite(t, filepath.Join(out, "command-"+name+".json"), map[string]any{"executable": f.bd, "argv": args, "cwd": f.work, "started": start, "finished": time.Now().UTC(), "exit_code": code, "stdout": stdout.String(), "stderr": stderr.String()})
	t.Logf("PUBLIC_COMMENT_NATIVE_COMMAND name=%s argv=%q exit=%d stdout=%s stderr=%s", name, args, code, stdout.Bytes(), stderr.Bytes())
	if (err != nil) != refuse || ctx.Err() != nil || code == -1 || (refuse && code != 1) {
		t.Fatalf("native command %s expected_refusal=%t error=%v", name, refuse, err)
	}
	return stdout.Bytes()
}

type commentFixtureObservation struct {
	Log    plugdump.SourceCommitLog        `json:"log"`
	Tables map[string]plugdump.NativeTable `json:"tables"`
	RawSQL map[string]commentFixtureSQL    `json:"raw_sql"`
}

func commentFixtureObserve(t *testing.T, f *nativeFixture) commentFixtureObservation {
	t.Helper()
	ctx := context.Background()
	source := plugdump.SQLSource{DB: f.db}
	log, err := source.ReadSourceCommits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	names, err := source.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := commentFixtureObservation{Log: log, Tables: map[string]plugdump.NativeTable{}, RawSQL: map[string]commentFixtureSQL{}}
	for _, name := range names {
		table, err := source.ReadTable(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		result.Tables[name] = canonicalPhysicalCaptureTable(table)
		result.RawSQL[name] = commentFixtureReadSQL(t, f.db, name)
	}
	return result
}

type commentFixtureCell struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type commentFixtureSQL struct {
	Query   string                   `json:"query"`
	Columns []physicalcapture.Column `json:"columns"`
	Rows    [][]commentFixtureCell   `json:"rows"`
}

func commentFixtureReadSQL(t *testing.T, db *sql.DB, name string) commentFixtureSQL {
	t.Helper()
	// Names come from the actual closed native table census, not user input.
	query := "SELECT * FROM `" + name + "`"
	rows, err := db.QueryContext(context.Background(), query) //nolint:gosec // native census table names
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	result := commentFixtureSQL{Query: query, Columns: []physicalcapture.Column{}, Rows: [][]commentFixtureCell{}}
	for _, column := range columns {
		result.Columns = append(result.Columns, physicalcapture.Column{Name: column.Name(), DatabaseTypeName: column.DatabaseTypeName()})
	}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		cells := make([]commentFixtureCell, len(values))
		for i, value := range values {
			cells[i] = commentFixtureTypedCell(t, value)
		}
		result.Rows = append(result.Rows, cells)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(result.Rows, func(i, j int) bool {
		a, _ := json.Marshal(result.Rows[i])
		b, _ := json.Marshal(result.Rows[j])
		return bytes.Compare(a, b) < 0
	})
	return result
}

func commentFixtureTypedCell(t *testing.T, value any) commentFixtureCell {
	t.Helper()
	if value == nil {
		return commentFixtureCell{Kind: "sql-null"}
	}
	switch v := value.(type) {
	case string:
		return commentFixtureCell{Kind: "string", Value: v}
	case []byte:
		return commentFixtureCell{Kind: "bytes", Value: base64.StdEncoding.EncodeToString(v)}
	case time.Time:
		return commentFixtureCell{Kind: "time", Value: v.Format(time.RFC3339Nano)}
	case float32:
		return commentFixtureCell{Kind: "float32-bits", Value: strconv.FormatUint(uint64(math.Float32bits(v)), 16)}
	case float64:
		return commentFixtureCell{Kind: "float64-bits", Value: strconv.FormatUint(math.Float64bits(v), 16)}
	case bool:
		return commentFixtureCell{Kind: "bool", Value: strconv.FormatBool(v)}
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return commentFixtureCell{Kind: v.Kind().String(), Value: strconv.FormatInt(v.Int(), 10)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return commentFixtureCell{Kind: v.Kind().String(), Value: strconv.FormatUint(v.Uint(), 10)}
	default:
		t.Fatalf("unsupported independently observed native cell type %T", value)
		return commentFixtureCell{}
	}
}

type commentFixtureSource struct {
	StoreID string `json:"store_id"`
	Project string `json:"project"`
}

type commentFixtureState struct {
	Kind               string `json:"kind"`
	Sequence           uint64 `json:"sequence"`
	CommitID           string `json:"commit_id"`
	TableCount         int    `json:"table_count"`
	PhysicalRootSHA256 string `json:"physical_root_sha256"`
}

type commentFixtureOracleTable struct {
	Kind           string            `json:"kind"`
	Sequence       uint64            `json:"sequence"`
	CommitID       string            `json:"commit_id"`
	Family         string            `json:"family"`
	History        string            `json:"history"`
	File           string            `json:"file"`
	SHA256         string            `json:"sha256"`
	Bytes          int               `json:"bytes"`
	SourceRows     int               `json:"source_rows"`
	ImportedRows   int               `json:"imported_rows"`
	SchemaID       string            `json:"schema_id"`
	PhysicalSHA256 string            `json:"physical_sha256"`
	ConfigPolicy   map[string]string `json:"config_policy"`
}

// This encoder is the fixture's independent interpretation of the frozen
// source wire. It reads detached physical values before running the producer;
// it never calls the producer's digest, normalization or receiver lowering.
func commentFixtureU(out *bytes.Buffer, n uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], n)
	out.Write(encoded[:])
}

func commentFixtureB(out *bytes.Buffer, value []byte) {
	commentFixtureU(out, uint64(len(value)))
	out.Write(value)
}

func commentFixtureS(out *bytes.Buffer, value string) {
	commentFixtureB(out, []byte(value))
}

func commentFixtureDigest(domain string, data []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(domain + "\x00"))
	_, _ = h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func commentFixtureJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func commentFixtureValue(t *testing.T, value driver.Value, sqlType string) any {
	t.Helper()
	if stamp, ok := value.(time.Time); ok {
		return stamp.UTC().Format(time.RFC3339Nano)
	}
	if data, ok := value.([]byte); ok {
		upper := strings.ToUpper(sqlType)
		if strings.Contains(upper, "BLOB") || strings.Contains(upper, "BINARY") {
			return map[string]string{"$bytes": base64.StdEncoding.EncodeToString(data)}
		}
		value = string(data)
	}
	if text, ok := value.(string); ok {
		if !utf8.ValidString(text) {
			t.Fatal("fixture source text is not valid UTF8")
		}
		if strings.EqualFold(sqlType, "JSON") {
			decoder := json.NewDecoder(strings.NewReader(text))
			decoder.UseNumber()
			var result, trailing any
			if err := decoder.Decode(&result); err != nil {
				t.Fatal(err)
			}
			if err := decoder.Decode(&trailing); err != io.EOF {
				t.Fatal("trailing data in original physical JSON cell")
			}
			return result
		}
		return text
	}
	return value
}

func commentFixtureNormalizedTable(t *testing.T, table physicalcapture.Table) plugdump.NativeTable {
	t.Helper()
	result := plugdump.NativeTable{Columns: []string{}, Rows: []plugdump.Row{}}
	for _, column := range table.Columns {
		result.Columns = append(result.Columns, column.Name)
	}
	sort.Strings(result.Columns)
	for _, values := range table.Rows {
		if len(values) != len(table.Columns) {
			t.Fatal("physical source row width differs")
		}
		row := plugdump.Row{}
		for i, value := range values {
			row[table.Columns[i].Name] = commentFixtureValue(t, value, table.Columns[i].DatabaseTypeName)
		}
		result.Rows = append(result.Rows, row)
	}
	sort.Slice(result.Rows, func(i, j int) bool {
		return bytes.Compare(commentFixtureJSON(t, result.Rows[i]), commentFixtureJSON(t, result.Rows[j])) < 0
	})
	return result
}

type commentFixturePhysicalRow struct {
	Key        string               `json:"key"`
	Cells      []commentFixtureCell `json:"cells"`
	Normalized string               `json:"normalized_json"`
	key        []byte
	payload    []byte
}

type commentFixturePhysicalTable struct {
	Name    string                      `json:"name"`
	Columns []physicalcapture.Column    `json:"columns"`
	Rows    []commentFixturePhysicalRow `json:"rows"`
}

func commentFixturePhysical(t *testing.T, table physicalcapture.Table) (commentFixturePhysicalTable, string, string) {
	t.Helper()
	var schema bytes.Buffer
	commentFixtureS(&schema, table.Name)
	commentFixtureU(&schema, uint64(len(table.Columns)))
	keys := map[uint64]int{}
	for i, column := range table.Columns {
		commentFixtureS(&schema, column.Name)
		commentFixtureS(&schema, column.DatabaseTypeName)
		if column.PrimaryKeyOrdinal == nil {
			schema.WriteByte(0)
		} else {
			schema.WriteByte(1)
			commentFixtureU(&schema, *column.PrimaryKeyOrdinal)
			if _, exists := keys[*column.PrimaryKeyOrdinal]; exists {
				t.Fatal("physical source duplicate PK ordinal")
			}
			keys[*column.PrimaryKeyOrdinal] = i
		}
	}
	schemaID := commentFixtureDigest("beads-native-table-schema/2", schema.Bytes())
	result := commentFixturePhysicalTable{Name: table.Name, Columns: table.Columns, Rows: []commentFixturePhysicalRow{}}
	occurrences := map[string]uint64{}
	for _, values := range table.Rows {
		if len(values) != len(table.Columns) {
			t.Fatal("physical source row width differs")
		}
		row := commentFixturePhysicalRow{Cells: []commentFixtureCell{}}
		normalized := map[string]any{}
		for i, value := range values {
			row.Cells = append(row.Cells, commentFixtureTypedCell(t, value))
			normalized[table.Columns[i].Name] = commentFixtureValue(t, value, table.Columns[i].DatabaseTypeName)
		}
		jsonRow := append(commentFixtureJSON(t, normalized), '\n')
		row.Normalized = base64.StdEncoding.EncodeToString(jsonRow)
		var payload, key bytes.Buffer
		commentFixtureU(&payload, uint64(len(row.Cells)))
		for _, cell := range row.Cells {
			commentFixtureS(&payload, cell.Kind)
			commentFixtureS(&payload, cell.Value)
		}
		commentFixtureB(&payload, jsonRow)
		row.payload = payload.Bytes()
		if len(keys) > 0 {
			key.WriteString("pk\x00")
			commentFixtureU(&key, uint64(len(keys)))
			for ordinal := uint64(0); ordinal < uint64(len(keys)); ordinal++ {
				index, ok := keys[ordinal]
				if !ok || row.Cells[index].Kind == "sql-null" {
					t.Fatal("physical source invalid PK metadata/value")
				}
				commentFixtureS(&key, row.Cells[index].Kind)
				commentFixtureS(&key, row.Cells[index].Value)
			}
		} else {
			key.WriteString("bag\x00")
			digest := sha256.Sum256(row.payload)
			key.Write(digest[:])
			group := hex.EncodeToString(digest[:])
			commentFixtureU(&key, occurrences[group])
			occurrences[group]++
		}
		row.key = key.Bytes()
		row.Key = base64.StdEncoding.EncodeToString(row.key)
		result.Rows = append(result.Rows, row)
	}
	sort.Slice(result.Rows, func(i, j int) bool { return bytes.Compare(result.Rows[i].key, result.Rows[j].key) < 0 })
	var encoded bytes.Buffer
	encoded.WriteString("beads-native-table/2\x00")
	schemaBytes, err := hex.DecodeString(schemaID)
	if err != nil {
		t.Fatal(err)
	}
	encoded.Write(schemaBytes)
	commentFixtureU(&encoded, uint64(len(result.Rows)))
	for i, row := range result.Rows {
		if i > 0 && bytes.Compare(result.Rows[i-1].key, row.key) >= 0 {
			t.Fatal("physical source duplicate row key")
		}
		commentFixtureB(&encoded, row.key)
		commentFixtureB(&encoded, row.payload)
	}
	return result, schemaID, fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
}

func commentFixtureHistory(kind, family string) string {
	if kind == "current" {
		switch family {
		case "bd_events_journal", "bd_events_seq", "events", "ignored_schema_migrations", "leases", "local_metadata", "repo_mtimes", "wisp_child_counters", "wisp_comments", "wisp_dependencies", "wisp_events", "wisp_labels", "wisps":
			return "unversioned-in-beads-by-design"
		}
	}
	return "retained-native-commit"
}

func commentFixtureOracleState(t *testing.T, out string, identity commentFixtureSource, policy map[string]string, kind string, sequence uint64, commit string, tables []physicalcapture.Table, tableLedger, stateLedger *bytes.Buffer) string {
	t.Helper()
	ordered := append([]physicalcapture.Table{}, tables...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	var root bytes.Buffer
	raw := []commentFixturePhysicalTable{}
	for i, table := range ordered {
		if i > 0 && ordered[i-1].Name == table.Name {
			t.Fatal("duplicate physical source table")
		}
		typed, schemaID, physicalSHA := commentFixturePhysical(t, table)
		raw = append(raw, typed)
		normalized := commentFixtureNormalizedTable(t, table)
		history := commentFixtureHistory(kind, table.Name)
		envelope := struct {
			Source  commentFixtureSource `json:"source_identity"`
			Table   string               `json:"table"`
			History string               `json:"history"`
			Columns []string             `json:"columns"`
			Rows    []plugdump.Row       `json:"rows"`
		}{identity, table.Name, history, normalized.Columns, normalized.Rows}
		data := commentFixtureJSON(t, envelope)
		sha := fmt.Sprintf("%x", sha256.Sum256(data))
		file := sha + ".table.json"
		if err := os.WriteFile(filepath.Join(out, file), data, 0600); err != nil {
			t.Fatal(err)
		}
		entry := commentFixtureOracleTable{Kind: kind, Sequence: sequence, CommitID: commit, Family: table.Name, History: history, File: file, SHA256: sha, Bytes: len(data), SourceRows: len(table.Rows), ImportedRows: len(table.Rows), SchemaID: schemaID, PhysicalSHA256: physicalSHA, ConfigPolicy: policy}
		if err := json.NewEncoder(tableLedger).Encode(entry); err != nil {
			t.Fatal(err)
		}
		commentFixtureS(&root, table.Name)
		digest, err := hex.DecodeString(physicalSHA)
		if err != nil {
			t.Fatal(err)
		}
		root.Write(digest)
		commentFixtureU(&root, uint64(len(table.Rows)))
	}
	rootSHA := commentFixtureDigest("beads-native-commit/2", root.Bytes())
	state := commentFixtureState{Kind: kind, Sequence: sequence, CommitID: commit, TableCount: len(ordered), PhysicalRootSHA256: rootSHA}
	if err := json.NewEncoder(stateLedger).Encode(state); err != nil {
		t.Fatal(err)
	}
	commentFixtureWrite(t, filepath.Join(out, fmt.Sprintf("raw-%s-%03d-%s.json", kind, sequence, commit)), map[string]any{"state": state, "source_identity": identity, "tables": raw})
	return rootSHA
}

func commentFixtureVerifyParts(t *testing.T, capture string, set plugdump.NativeCaptureSet, archive []byte, headRoot, currentRoot, out string) {
	t.Helper()
	if set.HeadArchive.Size != int64(len(archive)) || set.HeadArchive.SHA256 != fmt.Sprintf("%x", sha256.Sum256(archive)) {
		t.Fatal("native head archive size/digest binding differs")
	}
	partsDir := filepath.Join(capture, "capture-set", "parts")
	entries, err := os.ReadDir(partsDir)
	if err != nil || uint64(len(entries)) != set.PartCount {
		t.Fatalf("native part census differs: %v", err)
	}
	streamHash := sha256.New()
	streamBytes := uint64(0)
	previous := ""
	payloads := []io.Reader{}
	proof := []map[string]any{}
	for i, entry := range entries {
		name := fmt.Sprintf("%020d.part", i+1)
		if entry.Name() != name || entry.IsDir() {
			t.Fatal("native part path/sequence differs")
		}
		data, err := os.ReadFile(filepath.Join(partsDir, name))
		if err != nil {
			t.Fatal(err)
		}
		end := bytes.IndexByte(data, '\n')
		if end < 0 || end >= 4096 {
			t.Fatal("native part header absent/oversize")
		}
		var header plugdump.NativeDeltaPartHeader
		if err := json.Unmarshal(data[:end], &header); err != nil {
			t.Fatal(err)
		}
		payload := data[end+1:]
		if header.Format != "beads-native-part/2" || header.Sequence != uint64(i+1) || header.PayloadBytes != uint64(len(payload)) || header.PayloadSHA256 != fmt.Sprintf("%x", sha256.Sum256(payload)) || (i == 0 && header.PreviousPartSHA256 != nil) || (i > 0 && (header.PreviousPartSHA256 == nil || *header.PreviousPartSHA256 != previous)) {
			t.Fatal("native part header/hash-chain binding differs")
		}
		previous = fmt.Sprintf("%x", sha256.Sum256(data))
		_, _ = streamHash.Write(payload)
		streamBytes += uint64(len(payload))
		payloads = append(payloads, bytes.NewReader(payload))
		proof = append(proof, map[string]any{"path": name, "sha256": previous, "header": header, "eof": true})
	}
	if previous != set.FinalPartSHA256 || streamBytes != set.StreamBytes || hex.EncodeToString(streamHash.Sum(nil)) != set.StreamSHA256 {
		t.Fatal("native stream final part/bytes/digest differs")
	}
	decoder := json.NewDecoder(io.MultiReader(payloads...))
	commits, ended := uint64(0), false
	for {
		var record struct {
			Type              string                     `json:"type"`
			Totals            plugdump.NativeDeltaTotals `json:"totals"`
			HeadRootSHA256    string                     `json:"head_root_sha256"`
			CurrentRootSHA256 string                     `json:"current_root_sha256"`
		}
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil || ended {
			t.Fatalf("native stream invalid/trailing records: %v", err)
		}
		if record.Type == "commit" {
			commits++
		}
		if record.Type == "end" {
			ended = true
			if record.Totals != set.Totals || record.HeadRootSHA256 != headRoot || record.CurrentRootSHA256 != currentRoot {
				t.Fatal("native stream end totals/independent roots differ")
			}
		}
	}
	if !ended || commits != set.Totals.Commits {
		t.Fatal("native stream EOF/commit census missing")
	}
	commentFixtureWrite(t, filepath.Join(out, "capture-accounting.json"), map[string]any{"parts": proof, "stream_bytes": streamBytes, "commits": commits, "eof": true, "totals": set.Totals})
}

func commentFixtureCapture(t *testing.T, f *nativeFixture, out, prefix string, expected []commentFixtureTuple, observed commentFixtureObservation) {
	t.Helper()
	dataDir := ""
	for _, entry := range f.env {
		if value, ok := strings.CutPrefix(entry, "BEADS_DOLT_DATA_DIR="); ok {
			dataDir = value
		}
	}
	if dataDir == "" {
		t.Fatal("existing companion source directory missing")
	}
	copyDir, workspace, capture := privateDir(t), privateDir(t), privateDir(t)
	before := fingerprintPublicCapture(t, filepath.Join(dataDir, "capture"), filepath.Join(copyDir, "capture"))
	if !reflect.DeepEqual(before, fingerprintPublicCapture(t, filepath.Join(dataDir, "capture"), "")) || !reflect.DeepEqual(before, fingerprintPublicCapture(t, filepath.Join(copyDir, "capture"), "")) {
		t.Fatal("quiescent native source/copy/source fingerprint differs")
	}
	beadsDir := filepath.Join(workspace, ".beads")
	metadataBefore := fingerprintPublicCapture(t, filepath.Join(f.work, ".beads"), beadsDir)
	if !reflect.DeepEqual(metadataBefore, fingerprintPublicCapture(t, filepath.Join(f.work, ".beads"), "")) || !reflect.DeepEqual(metadataBefore, fingerprintPublicCapture(t, beadsDir, "")) {
		t.Fatal("quiescent native workspace copy differs")
	}
	identity := commentFixtureSource{StoreID: "native/" + fmt.Sprintf("%x", sha256.Sum256([]byte(filepath.Join(copyDir, "capture")))), Project: prefix}
	policy := map[string]string{}
	for _, family := range []string{"config", "metadata"} {
		for _, row := range observed.Tables[family].Rows {
			key, ok := row["key"].(string)
			if !ok || key == "" {
				t.Fatal("public fixture config policy missing source key")
			}
			policy[family+"/"+key] = "public"
		}
	}
	oracle := filepath.Join(out, "oracle")
	if err := os.Mkdir(oracle, 0700); err != nil {
		t.Fatal(err)
	}
	db, closeDriver, err := physicalcapture.Open(copyDir, "capture")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = conn.Close()
			_ = db.Close()
			_ = closeDriver()
		}
	}()
	working, err := physicalcapture.ReadWorkingRoot(ctx, conn, observed.Log.Head)
	if err != nil || !working.Complete || !working.ReadOnlyScope {
		t.Fatalf("independent pre-export physical current read: %v", err)
	}
	for _, table := range working.Tables {
		actual := commentFixtureNormalizedTable(t, table)
		if !equalPhysicalCaptureJSON(actual, observed.Tables[table.Name]) {
			t.Fatalf("independent physical current differs from original SQL: %s", table.Name)
		}
	}
	if len(working.Tables) != len(observed.Tables) {
		t.Fatal("physical and SQL current family census differ")
	}
	history, err := physicalcapture.ReadHistory(ctx, conn, observed.Log.Head)
	if err != nil || !history.ReadOnlyScope || len(history.Boundaries) != 0 || len(history.Commits) != len(observed.Log.Commits) {
		t.Fatalf("independent original native graph: %v", err)
	}
	commentFixtureWrite(t, filepath.Join(oracle, "raw-history.json"), history)
	order := commentFixtureCommitOrder(t, observed.Log)
	var tableLedger, stateLedger bytes.Buffer
	var headTables []physicalcapture.Table
	for i, commit := range order {
		image, err := physicalcapture.ReadCommit(ctx, conn, commit.ID)
		if err != nil || image.CommitID != commit.ID || !image.ReadOnlyScope {
			t.Fatalf("independent original immutable image %d: %v", i+1, err)
		}
		if commit.ID == observed.Log.Head {
			headTables = image.Tables
		}
		commentFixtureOracleState(t, oracle, identity, policy, "commit", uint64(i+1), commit.ID, image.Tables, &tableLedger, &stateLedger)
	}
	if headTables == nil {
		t.Fatal("independent native head image missing")
	}
	headRoot := commentFixtureOracleState(t, oracle, identity, policy, "head-checkpoint", 0, observed.Log.Head, headTables, &tableLedger, &stateLedger)
	currentRoot := commentFixtureOracleState(t, oracle, identity, policy, "current", 0, observed.Log.Head, working.Tables, &tableLedger, &stateLedger)
	commentFixtureWrite(t, filepath.Join(oracle, "working-identity.json"), map[string]any{"format": working.Format, "database": working.Database, "head_commit_id": working.HeadCommitID, "working_root_hash": working.WorkingRootHash, "read_only_scope": working.ReadOnlyScope, "complete": working.Complete, "source_identity": identity})
	if err := os.WriteFile(filepath.Join(oracle, "tables.ndjson"), tableLedger.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oracle, "states.ndjson"), stateLedger.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	// The complete independently observed physical oracle is closed and durable
	// BEFORE the unchanged producer is run on the same quiescent public copy.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}
	closed = true
	binary := filepath.Join(bazeltest.OverrideRoot(), os.Getenv("BEADS_PHYSICAL_CAPTURE_BINARY"))
	tool := *f
	tool.work, tool.env = workspace, nil
	for _, entry := range f.env {
		key, _, _ := strings.Cut(entry, "=")
		if key != captureclock.FileEnv && key != captureclock.PublicEnv && key != "BEADS_DIR" {
			tool.env = append(tool.env, entry)
		}
	}
	tool.env = append(tool.env, "BEADS_DIR="+beadsDir)
	tool.run(binary, "--disposable", "--data-dir", copyDir, "--database", "capture", "--beads-dir", beadsDir, "--output-dir", capture, "--native-delta")
	setBytes, err := os.ReadFile(filepath.Join(capture, "capture-set", "capture-set.json"))
	if err != nil {
		t.Fatal(err)
	}
	var set plugdump.NativeCaptureSet
	if err := json.Unmarshal(setBytes, &set); err != nil || set.Format != plugdump.NativeCaptureSetFormat || set.StoreID != identity.StoreID || !reflect.DeepEqual(set.Projects, []string{prefix}) || set.Head != observed.Log.Head || set.Totals.Commits != uint64(len(order)) {
		t.Fatalf("actual native capture-set identity/census: %+v %v", set, err)
	}
	evidenceBytes, err := os.ReadFile(filepath.Join(capture, "physical-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence plugdump.NativeDeltaEvidence
	if err := json.Unmarshal(evidenceBytes, &evidence); err != nil || evidence.HeadRootSHA256 != headRoot || evidence.CurrentRootSHA256 != currentRoot || evidence.Working.WorkingRootHash != working.WorkingRootHash {
		t.Fatalf("producer physical roots differ from independent pre-export source oracle: %v", err)
	}
	archive, err := os.ReadFile(filepath.Join(capture, "capture-set", "head.tar"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.ValidateNativeDeltaHead(bytes.NewReader(archive))
	if err != nil || manifest.StoreID != identity.StoreID || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != prefix {
		t.Fatalf("actual native head archive validation/policy: %v", err)
	}
	inventoryBytes, err := os.ReadFile(filepath.Join(capture, "inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := plugdump.ReadInventory(bytes.NewReader(inventoryBytes))
	if err != nil || !reflect.DeepEqual(inventory.ConfigPolicy, policy) {
		t.Fatalf("actual native inventory public policy differs: %v", err)
	}
	commentFixtureVerifyParts(t, capture, set, archive, headRoot, currentRoot, out)
	if !equalPhysicalCaptureJSON(observed, commentFixtureObserve(t, f)) {
		t.Fatal("public native source changed during oracle/capture")
	}
	files := fingerprintPublicCapture(t, capture, filepath.Join(out, "capture"))
	commentFixtureWrite(t, filepath.Join(out, "capture-files-sha256.json"), files)
	commentFixtureWrite(t, filepath.Join(out, "SOURCE.json"), map[string]any{
		"source_identity": identity, "commits": len(order), "capture_sha256": fmt.Sprintf("%x", sha256.Sum256(setBytes)),
		"ledger_sha256": fmt.Sprintf("%x", sha256.Sum256(tableLedger.Bytes())), "states_sha256": fmt.Sprintf("%x", sha256.Sum256(stateLedger.Bytes())),
		"journal_table": "bd_events_journal", "restore_time": "2040-01-02T03:05:00Z", "capture": "capture/capture-set", "oracle": "oracle",
		"comment_tuples": len(expected), "current_census": manifest.Sources, "capture_clock_applies_to": "public import missing-time path and original server SQL; live append uses real wall time",
		"private": "HARD_DISABLED", "official": "UNSPENT", "consumer_import_reopen": "NOT_RUN",
	})
	commentFixtureWrite(t, filepath.Join(out, "source-fingerprints.json"), map[string]any{"database": before, "beads": metadataBefore})
}

func commentFixtureCommitOrder(t *testing.T, log plugdump.SourceCommitLog) []plugdump.SourceCommit {
	t.Helper()
	byID := map[string]plugdump.SourceCommit{}
	for _, commit := range log.Commits {
		byID[commit.ID] = commit
	}
	result := []plugdump.SourceCommit{}
	seen := map[string]bool{}
	var visit func(string)
	visit = func(id string) {
		if seen[id] {
			t.Fatal("native fixture graph repeats an ancestor")
		}
		seen[id] = true
		commit, ok := byID[id]
		if !ok || len(commit.Parents) > 1 {
			t.Fatal("bounded fixture requires the actual complete linear source graph")
		}
		for _, parent := range commit.Parents {
			visit(parent)
		}
		result = append(result, commit)
	}
	visit(log.Head)
	if len(result) != len(log.Commits) {
		t.Fatal("unreachable native fixture commits")
	}
	return result
}

func assertCommentFixtureAppend(t *testing.T, before, after commentFixtureObservation, family, anchor string) {
	t.Helper()
	for _, table := range []string{"issues", "wisps"} {
		if !equalPhysicalCaptureJSON(before.Tables[table], after.Tables[table]) {
			t.Fatalf("native comment changed complete %s anchor rows", table)
		}
	}
	other := "comments"
	if family == "comments" {
		other = "wisp_comments"
	}
	if len(after.Tables[family].Rows) != len(before.Tables[family].Rows)+1 || !equalPhysicalCaptureJSON(before.Tables[other], after.Tables[other]) {
		t.Fatal("native append changed incorrect source comment membership")
	}
	wantCommits := len(before.Log.Commits)
	if family == "comments" {
		wantCommits++
	}
	if len(after.Log.Commits) != wantCommits {
		t.Fatalf("native %s history count got=%d want=%d", family, len(after.Log.Commits), wantCommits)
	}
	old := map[string]plugdump.SourceCommit{}
	for _, commit := range before.Log.Commits {
		old[commit.ID] = commit
	}
	newCommits := 0
	for _, commit := range after.Log.Commits {
		if previous, exists := old[commit.ID]; exists {
			if !equalPhysicalCaptureJSON(previous, commit) {
				t.Fatal("native comment rewrote original commit metadata/parents")
			}
			delete(old, commit.ID)
		} else {
			newCommits++
			if family != "comments" || commit.Metadata["message"] != "bd: comment "+anchor {
				t.Fatalf("unexpected native comment history entry: %+v", commit)
			}
		}
	}
	if len(old) != 0 || newCommits != wantCommits-len(before.Log.Commits) || (family == "wisp_comments" && before.Log.Head != after.Log.Head) {
		t.Fatal("native durable/wisp commit conservation failed")
	}
}

func commentFixturePages(t *testing.T, f *nativeFixture, out string, expected []commentFixtureTuple, anchors ...string) {
	t.Helper()
	port, dataDir := 0, ""
	for _, entry := range f.env {
		if value, ok := strings.CutPrefix(entry, "BEADS_DOLT_SERVER_PORT="); ok {
			var err error
			port, err = strconv.Atoi(value)
			if err != nil {
				t.Fatal(err)
			}
		}
		if value, ok := strings.CutPrefix(entry, "BEADS_DOLT_DATA_DIR="); ok {
			dataDir = value
		}
	}
	ctx := context.Background()
	store, err := dolt.New(ctx, &dolt.Config{Path: dataDir, BeadsDir: filepath.Join(f.work, ".beads"), Database: "capture", ServerMode: true, ServerHost: "127.0.0.1", ServerPort: port, ServerUser: "root", DisableAutoStart: true, MaxOpenConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	before := commentFixtureObserve(t, f)
	role, err := store.Commenter()
	if err != nil {
		t.Fatal(err)
	}
	for _, refusal := range []struct {
		name    string
		request issueops.AddCommentRequest
		want    error
	}{
		{"empty-author", issueops.AddCommentRequest{IssueID: anchors[0], Text: "refused"}, storage.ErrValidation},
		{"empty-id", issueops.AddCommentRequest{Author: "Native Role Author", Text: "refused"}, storage.ErrValidation},
		{"empty-body", issueops.AddCommentRequest{Author: "Native Role Author", IssueID: anchors[0]}, storage.ErrValidation},
		{"whitespace-body", issueops.AddCommentRequest{Author: "Native Role Author", IssueID: anchors[1], Text: " \t\n "}, storage.ErrValidation},
		{"missing-id", issueops.AddCommentRequest{Author: "Native Role Author", IssueID: anchors[3], Text: "refused"}, storage.ErrNotFound},
		{"exact-prefix", issueops.AddCommentRequest{Author: "Native Role Author", IssueID: strings.TrimSuffix(anchors[0], "b"), Text: "refused"}, storage.ErrNotFound},
	} {
		beforeRole := commentFixtureObserve(t, f)
		requestBefore := refusal.request
		result, actualError := role.AddComment(ctx, refusal.request)
		afterRole := commentFixtureObserve(t, f)
		if !errors.Is(actualError, refusal.want) || result.Comment != nil || !reflect.DeepEqual(refusal.request, requestBefore) || !equalPhysicalCaptureJSON(beforeRole, afterRole) {
			t.Fatalf("native Commenter typed refusal/conservation differs: %s: %v", refusal.name, actualError)
		}
		commentFixtureWrite(t, filepath.Join(out, "role-refusal-"+refusal.name+".json"), map[string]any{"interface": "DoltStore.Commenter().AddComment", "contract": "issueops/commenter.go: Commenter.AddComment; internal/storage/issueops/commenter.go validation and exact plane lookup", "request": refusal.request, "expected_sentinel": refusal.want.Error(), "actual_error": actualError.Error(), "errors_is": true, "before": beforeRole, "after": afterRole})
	}
	for _, anchor := range anchors {
		want := commentFixtureThread(expected, anchor)
		for _, limit := range []int{0, 100, 500, 501} {
			cursor := storage.CommentPageCursor{}
			walk := []commentFixtureComment{}
			pageCount := 0
			for {
				page, err := store.GetIssueCommentsPage(ctx, anchor, cursor, limit)
				if err != nil {
					t.Fatal(err)
				}
				cap := limit
				if cap <= 0 {
					cap = 100
				}
				cap = min(cap, 500)
				if len(page) != min(cap, len(want)-len(walk)) {
					t.Fatalf("native page clamp/default differs: anchor=%s limit=%d rows=%d", anchor, limit, len(page))
				}
				values := []commentFixtureComment{}
				for _, comment := range page {
					values = append(values, commentFixtureComment{comment.ID, comment.IssueID, comment.Author, comment.Text, comment.CreatedAt})
				}
				commentFixtureWrite(t, filepath.Join(out, fmt.Sprintf("page-%s-%d-%d.json", anchor, limit, pageCount)), map[string]any{"interface": "DoltStore.GetIssueCommentsPage; native storage API, not CLI/HTTP", "anchor": anchor, "requested_limit": limit, "cursor": cursor, "rows": values})
				walk = append(walk, values...)
				pageCount++
				if len(page) == 0 {
					break
				}
				last := page[len(page)-1]
				cursor = storage.CommentPageCursor{CreatedAt: last.CreatedAt, ID: last.ID}
				if pageCount > 10 {
					t.Fatal("native page walk did not terminate")
				}
			}
			if !reflect.DeepEqual(walk, want) {
				t.Fatalf("native page walk differs from independent tuple order: %s limit=%d", anchor, limit)
			}
		}
	}
	if !equalPhysicalCaptureJSON(before, commentFixtureObserve(t, f)) {
		t.Fatal("native page reads changed source state")
	}
}
