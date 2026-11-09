package plugdump

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/testutil"
)

func TestSQLSourceHistoryReadsPinnedDoltCommitImages(t *testing.T) {
	if os.Getenv("BEADS_TEST_SKIP") == "dolt" {
		t.Skip("requires the explicit pinned Dolt source-history lane")
	}
	server := testutil.StartIsolatedDoltContainerHandle(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%s)/beads_test?parseTime=true", server.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A real, unsupported older business schema deliberately makes this
	// source incomplete. Empty genesis alone is no longer a history gap.
	for _, query := range []string{
		"CREATE TABLE issues (id TEXT, title TEXT)",
		"INSERT INTO issues VALUES ('demo-legacy', 'unmappable earlier source state')",
		"CALL DOLT_ADD('-A')",
		"CALL DOLT_COMMIT('-m', 'public unsupported earlier schema', '--author', 'Synthetic source <source@example.test>')",
		"DROP TABLE issues",
	} {
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	source, inventory := fixture(t)
	inventory.Projects[0].IDMode, inventory.Projects[0].HashLength = "hash", 1
	source["dependencies"].Rows[0]["type"] = "blocks"
	source["events"].Rows[0]["id"] = json.Number("1")
	for _, row := range source["issues"].Rows {
		row["agent_state"] = "waiting\x00opaque"
		row["await_id"] = "approval/byte-for-byte"
		row["await_type"] = "human"
		row["ephemeral"] = json.Number("1")
		row["event_kind"] = nil
		row["hook_bead"] = "demo-8"
		row["is_template"] = json.Number("1")
		row["mol_type"] = "formula"
		row["role_bead"] = "demo-7"
		row["role_type"] = "agent"
		row["target"] = "demo-8"
		row["timeout_ns"] = json.Number("9007199254740993")
		row["payload"] = map[string]any{"n": json.Number("9007199254740993"), "null": nil}
		row["waiters"] = []any{"demo-8", nil}
		row["wisp_type"] = "run"
		row["work_type"] = "workflow"
	}
	// This is an explicitly synthetic source database. Its complete closed
	// column census is real SQL state; no old production schema is inferred.
	for _, name := range sortedNames(source) {
		table := source[name]
		definitions := make([]string, len(table.Columns))
		for i, column := range table.Columns {
			typeName := "TEXT"
			for _, row := range table.Rows {
				switch value := row[column].(type) {
				case json.Number:
					typeName = "BIGINT"
				case map[string]any:
					typeName = "JSON"
					if _, ok := value["$bytes"]; ok {
						typeName = "LONGBLOB"
					}
				case []any:
					typeName = "JSON"
				}
			}
			definitions[i] = "`" + column + "` " + typeName
		}
		if _, err = db.ExecContext(ctx, "CREATE TABLE `"+name+"` ("+strings.Join(definitions, ",")+")"); err != nil {
			t.Fatal(err)
		}
		for _, row := range table.Rows {
			values := make([]any, len(table.Columns))
			columns := make([]string, len(table.Columns))
			marks := make([]string, len(table.Columns))
			for i, column := range table.Columns {
				columns[i], marks[i] = "`"+column+"`", "?"
				value := row[column]
				switch v := value.(type) {
				case json.Number:
					value = string(v)
				case map[string]any:
					if raw, ok := v["$bytes"].(string); ok {
						value, err = base64.StdEncoding.DecodeString(raw)
					} else {
						value, err = json.Marshal(v)
					}
				case []any:
					value, err = json.Marshal(v)
				}
				if err != nil {
					t.Fatal(err)
				}
				values[i] = value
			}
			if _, err = db.ExecContext(ctx, "INSERT INTO `"+name+"` ("+strings.Join(columns, ",")+") VALUES ("+strings.Join(marks, ",")+")", values...); err != nil {
				t.Fatal(err)
			}
		}
	}
	commit := func(message string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, "CALL DOLT_ADD('-A')"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('-m', ?, '--author', 'Synthetic source <source@example.test>')", message); err != nil {
			t.Fatal(err)
		}
	}
	// Derived views occur in real migrated stores. Historical census must use
	// each object's actual kind, matching the current base-table census.
	if _, err := db.ExecContext(ctx, "CREATE VIEW public_ready_fixture AS SELECT id FROM issues"); err != nil {
		t.Fatal(err)
	}
	commit("public synthetic schema and baseline")
	if _, err = db.ExecContext(ctx, "UPDATE issues SET title = ? WHERE id = ?", "Dolt historical update", "demo-7"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO events (id, issue_id, event_type, actor, old_value, new_value, comment, created_at) VALUES (2, 'demo-7', 'updated', 'unverified source author', NULL, 'Dolt historical update', NULL, '2026-01-02T00:00:00Z')"); err != nil {
		t.Fatal(err)
	}
	commit("public synthetic update")
	// Compare to direct SQL readback, rather than assuming a native JSON
	// column retains the numeric representation that the insert requested.
	var storedPayloadJSON string
	var storedTimeout int64
	if err := db.QueryRowContext(ctx, "SELECT payload, timeout_ns FROM issues WHERE id = ?", "demo-7").Scan(&storedPayloadJSON, &storedTimeout); err != nil {
		t.Fatal(err)
	}
	var storedPayload map[string]any
	if err := decodeJSON([]byte(storedPayloadJSON), &storedPayload); err != nil {
		t.Fatal(err)
	}
	if storedTimeout != 9007199254740993 {
		t.Fatalf("source BIGINT changed before export: %d", storedTimeout)
	}
	t.Logf("public SQL readback before export: payload=%s timeout_ns=%d (requested payload.n=9007199254740993)", storedPayloadJSON, storedTimeout)
	wantOpaque := Row{}
	for _, row := range source["issues"].Rows {
		if row["id"] == "demo-7" {
			for _, field := range opaqueIssueFields {
				wantOpaque[field] = row[field]
			}
		}
	}
	wantOpaque["payload"] = storedPayload
	checkOpaque := func(image string, records []Record) {
		t.Helper()
		for _, record := range records {
			if record.Kind != "bead" || record.ID != "demo-7" {
				continue
			}
			for _, field := range opaqueIssueFields {
				got, present := record.Source.Fields[field]
				if !present || !equalJSON(got, wantOpaque[field]) {
					t.Fatalf("%s opaque %s differs from source SQL: got %v want %v", image, field, got, wantOpaque[field])
				}
			}
			return
		}
		t.Fatalf("%s missing public source issue", image)
	}
	build := func() []byte {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		data, err := Build(ctx, SQLSource{DB: tx}, inventory)
		if err != nil {
			// This database is fixed public test data. Keep the actual system
			// row shape in the receipt when its ancestry cannot be validated.
			if ancestry, readErr := (SQLSource{DB: tx}).historyTable(ctx, "SELECT commit_hash, parent_hash FROM dolt_commit_ancestors"); readErr == nil {
				for _, row := range ancestry.Rows {
					t.Logf("public fixture ancestry commit=%v parent=%v parent_type=%T", row["commit_hash"], row["parent_hash"], row["parent_hash"])
				}
			}
			t.Fatal(err)
		}
		return data
	}
	data := build()
	m, h := sourceCapture(t, data)
	if m.History != "available-unverified" || h.Range.Status != "verified" || h.Range.Complete || len(h.Range.Baseline) == 0 || len(h.Range.Frames) == 0 {
		t.Fatalf("real Dolt suffix was not proven: status=%s reason=%s first=%d head=%d frames=%d", h.Range.Status, h.Range.Reason, h.Range.First, h.Range.Head, len(h.Range.Frames))
	}
	if len(h.Events) != 2 || !bytes.Contains(data, []byte("9007199254740993")) || !bytes.Contains(data, []byte("waiting\\u0000opaque")) {
		t.Fatal("source audit or opaque bytes were lost")
	}
	checkOpaque("baseline", h.Range.Baseline)
	for _, frame := range h.Range.Frames {
		checkOpaque(fmt.Sprintf("sequence %d", frame.Sequence), frame.Records)
	}
	members := archiveMembers(t, data)
	for _, kind := range m.Kinds {
		if kind.Name != "bead" {
			continue
		}
		var records []Record
		for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
			var record Record
			if err := decodeJSON(line, &record); err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
		checkOpaque("head", records)
	}
	t.Logf("actual Dolt capture commits=%d first=%d head=%d events=%d sha256=%s", len(h.Commits), h.Range.First, h.Range.Head, len(h.Events), digest(data))
	if os.Getenv("PLUG_TEST_EXPORT_PUBLIC_HISTORY") == "1" {
		// Only this fixed public synthetic database can reach this branch.
		// The standard hosted receipt retains test.log, so a consumer can
		// qualify these exact exporter-produced bytes without another build.
		t.Logf("PUBLIC_SOURCE_ARCHIVE_BASE64=%s", base64.StdEncoding.EncodeToString(data))
	}
	if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
		if err := os.WriteFile(filepath.Join(output, "dolt-source-partial.tar"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.ExecContext(ctx, "UPDATE issues SET title = 'uncommitted head' WHERE id = 'demo-7'"); err != nil {
		t.Fatal(err)
	}
	m, h = sourceCapture(t, build())
	if m.History != "available-unverified" || h.Range.Status != "unverified" || h.Range.Reason != "working-set-not-clean" || len(h.Range.Frames) != 0 {
		t.Fatal("dirty SQL working set became verified history")
	}
}

func TestSQLSourceHistoricalCensusDistinguishesViewsFromBaseTables(t *testing.T) {
	for _, test := range []struct {
		name   string
		rows   [][2]any
		refuse bool
	}{
		{"view", [][2]any{{"ready_issues", "VIEW"}, {"issues", "BASE TABLE"}}, false},
		{"unknown base", [][2]any{{"ready_issues", "BASE TABLE"}}, true},
		{"unknown kind", [][2]any{{"issues", "TEMPORARY"}}, true},
		{"missing kind", [][2]any{{"issues", nil}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			id := strings.Repeat("a", 32)
			rows := sqlmock.NewRows([]string{"Tables_in_fixture", "Table_type"})
			for _, row := range test.rows {
				rows.AddRow(row[0], row[1])
			}
			mock.ExpectQuery(regexp.QuoteMeta("SHOW FULL TABLES AS OF '" + id + "'")).WillReturnRows(rows)
			if !test.refuse {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `issues` AS OF '" + id + "'")).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("demo-a7b"))
			}
			image, err := (SQLSource{DB: db}).ReadSourceImage(context.Background(), id)
			if test.refuse != (err != nil) {
				t.Fatalf("historical object kind refusal=%t, err=%v", test.refuse, err)
			}
			if !test.refuse && (len(image) != 1 || len(image["issues"].Rows) != 1) {
				t.Fatal("base image omitted or view read as business rows")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
