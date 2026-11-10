package physicalrestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

func undoHead(t *testing.T, conn *sql.Conn) string {
	t.Helper()
	var head string
	if err := conn.QueryRowContext(context.Background(), "SELECT DOLT_HASHOF('HEAD')").Scan(&head); err != nil {
		t.Fatal(err)
	}
	return head
}
func undoCapture(t *testing.T, conn *sql.Conn) SourceBinding {
	t.Helper()
	image, err := physicalcapture.ReadCommit(context.Background(), conn, undoHead(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	working, err := physicalcapture.ReadWorkingRoot(context.Background(), conn, image.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindWorkingSource(image, working)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}
func undoFixture(t *testing.T) (*sql.Conn, string, func() error, UndoRequest) {
	t.Helper()
	_, conn, directory, closeStore := restoreDB(t, false)
	for _, ddl := range []string{
		"CREATE TABLE undo_cells (id BIGINT PRIMARY KEY, metadata JSON, nullable_text TEXT, opaque BLOB)",
		"CREATE TABLE undo_children (id INT PRIMARY KEY, parent_id BIGINT, CONSTRAINT undo_fk FOREIGN KEY (parent_id) REFERENCES undo_cells(id))",
		"CREATE TABLE undo_empty (id INT PRIMARY KEY)",
		"CREATE TABLE undo_keyless (value TEXT)",
		"CREATE TABLE undo_unversioned (id BIGINT PRIMARY KEY, sql_null JSON, json_null JSON, payload BLOB)",
	} {
		execSQL(t, conn, ddl)
	}
	execSQL(t, conn, "INSERT INTO undo_cells VALUES (1,JSON_OBJECT('native',9007199254740993),NULL,?)", []byte{0, 255, 7})
	execSQL(t, conn, "INSERT INTO undo_children VALUES (1,1)")
	execSQL(t, conn, "INSERT INTO undo_empty VALUES (1)")
	execSQL(t, conn, "INSERT INTO undo_keyless VALUES ('old')")
	execSQL(t, conn, "INSERT INTO undo_unversioned VALUES (1,NULL,CAST('null' AS JSON),?)", []byte{0, 255, 8})
	execSQL(t, conn, "INSERT INTO dolt_ignore VALUES ('undo_unversioned',true)")
	execSQL(t, conn, "CALL DOLT_ADD('.')")
	execSQL(t, conn, "CALL DOLT_COMMIT('-Am','preserved native first commit')")
	execSQL(t, conn, "INSERT INTO undo_cells VALUES (2,JSON_OBJECT('positive',9007199254740994,'negative',-9007199254740994),'',?)", []byte{})
	execSQL(t, conn, "CALL DOLT_COMMIT('-Am','preserved native second commit')")
	execSQL(t, conn, "CALL DOLT_BRANCH('preserved-other')")
	execSQL(t, conn, "CALL DOLT_TAG('preserved-tag')")
	binding := undoCapture(t, conn)
	current := cloneManifest(t, binding.Working.Image)
	users := make([]Table, 0, len(current.Tables))
	for _, table := range current.Tables {
		if !strings.HasPrefix(table.Name, "dolt_") {
			users = append(users, table)
		}
	}
	current.Tables = users
	for ti := range current.Tables {
		table := &current.Tables[ti]
		switch table.Name {
		case "undo_cells":
			table.Rows[0][0].Data = "11"
			table.Rows[1][0].Data = "12"
		case "undo_children":
			for ci, col := range table.Columns {
				if col.Name == "parent_id" {
					table.Rows[0][ci].Data = "11"
				}
			}
		case "undo_unversioned":
			table.Rows[0][0].Data = "21"
		case "undo_empty":
			table.Rows = [][]Cell{}
		case "undo_keyless":
			table.Rows = [][]Cell{{{Type: "string", Data: "current"}}, {{Type: "string", Data: "current"}}, {{Type: "null"}}}
		}
	}
	// This public fixture retains an externally supplied existing Report verbatim.
	// It does not claim a measured Plug changeset/namespace census.
	report := json.RawMessage(`{"lost_post_switch_history":["fixture head state; Plug-created history not replayed"],"core_kind_losses":[],"namespace_losses":[],"warnings":[]}`)
	request := UndoRequest{Format: UndoFormat, HistoryMode: "switch-back", Source: binding, Current: current, ReverseReport: report}
	return conn, directory, closeStore, request
}
func undoTable(t *testing.T, manifest *Manifest, name string) *Table {
	t.Helper()
	for i := range manifest.Tables {
		if manifest.Tables[i].Name == name {
			return &manifest.Tables[i]
		}
	}
	t.Fatalf("missing fixture table %s", name)
	return nil
}
func undoClone(t *testing.T, request UndoRequest) UndoRequest {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var out UndoRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func undoCheckUnchanged(t *testing.T, conn *sql.Conn, request UndoRequest) {
	t.Helper()
	if head := undoHead(t, conn); head != request.Source.CommitID {
		t.Fatalf("refusal changed HEAD: %s", head)
	}
	actual := undoCapture(t, conn)
	if err := compareManifests(request.Source.Image, actual.Image); err != nil {
		t.Fatal(err)
	}
	if actual.Working.WorkingRootHash != request.Source.Working.WorkingRootHash {
		t.Fatal("refusal changed original working-root hash")
	}
	if err := compareManifests(request.Source.Working.Image, actual.Working.Image); err != nil {
		t.Fatal(err)
	}
	// SQL queries independently inspect working rows, since immutable HEAD alone
	// cannot prove transaction rollback of the pre-existing populated working set.
	for _, name := range []string{"undo_cells", "undo_children", "undo_empty", "undo_keyless", "undo_unversioned"} {
		got, err := readActualSQLTable(conn, name)
		if err != nil {
			t.Fatal(err)
		}
		table := undoTable(t, &request.Source.Working.Image, name)
		if len(got.Rows) != len(table.Rows) {
			t.Fatalf("refusal changed %s working rows", name)
		}
	}
	var id int64
	if err := conn.QueryRowContext(context.Background(), "SELECT id FROM undo_cells ORDER BY id LIMIT 1").Scan(&id); err != nil || id != 1 {
		t.Fatalf("original nonempty row not restored: %d %v", id, err)
	}
}
func undoOpen(t *testing.T, directory string) (*sql.Conn, func() error) {
	t.Helper()
	db, driverClose, err := physicalcapture.Open(directory, "restored")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	closeOnce := func() error {
		if closed {
			return nil
		}
		closed = true
		return errors.Join(conn.Close(), db.Close(), driverClose())
	}
	t.Cleanup(func() {
		if err := closeOnce(); err != nil {
			t.Error(err)
		}
	})
	return conn, closeOnce
}

func undoWork(t *testing.T) string {
	t.Helper()
	work, err := os.MkdirTemp("/tmp", "public-physical-undo-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(work); err != nil {
			t.Error(err)
		}
		_, err := os.Lstat(work)
		t.Logf("UNDO_COPY_CLEANUP target_absent=%t", os.IsNotExist(err))
		if !os.IsNotExist(err) {
			t.Error("public undo copy not removed")
		}
	})
	return work
}

func TestPhysicalUndoPreservedCopyOneCommit(t *testing.T) {
	_, source, release, request := undoFixture(t)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	work := undoWork(t)
	if err := os.Chmod(work, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(work, "copy")
	copied, err := CopyPreservedStore(context.Background(), source, target)
	if err != nil {
		t.Fatal(err)
	}
	conn, closeCopy := undoOpen(t, target)
	receipt, err := Undo(context.Background(), conn, request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.CurrentCommits != receipt.OriginalCommits+1 || receipt.DeletedRows != 6 || receipt.WrittenRows != 7 || !receipt.PhysicalReadbackVerified {
		t.Fatalf("wrong native receipt: %+v", receipt)
	}
	if !reflect.DeepEqual(receipt.ReverseReport, request.ReverseReport) {
		t.Fatal("existing loss report changed")
	}
	if rowCount(t, conn, "undo_empty") != 0 || rowCount(t, conn, "undo_keyless") != 3 {
		t.Fatal("deletion/keyless replacement failed")
	}
	// New foreign-key destination is real SQL, requiring child-first deletion and
	// parent-first insertion; disabling native constraints is never necessary.
	var parent int64
	if err := conn.QueryRowContext(context.Background(), "SELECT parent_id FROM undo_children").Scan(&parent); err != nil || parent != 11 {
		t.Fatalf("actual FK row: %d %v", parent, err)
	}
	if err := closeCopy(); err != nil {
		t.Fatal(err)
	}
	reopened, closeAgain := undoOpen(t, target)
	actual, err := VerifyUndo(context.Background(), reopened, request, receipt)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Reopened = true
	old, err := physicalcapture.ReadCommit(context.Background(), reopened, request.Source.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	original, err := FromImage(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareManifests(request.Source.Image, original); err != nil {
		t.Fatal(err)
	}
	if err := closeAgain(); err != nil {
		t.Fatal(err)
	}
	unchanged, err := InventoryPreservedStore(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if copied.SHA256 != unchanged.SHA256 {
		t.Fatal("preserved original store bytes changed")
	}
	saveEvidence(t, "undo-one-commit-request.json", request)
	saveEvidence(t, "undo-one-commit-actual-root.json", actual)
	saveEvidence(t, "undo-one-commit-original-root.json", original)
	saveEvidence(t, "undo-one-commit-receipt.json", receipt)
	saveEvidence(t, "undo-preserved-copy-files.json", copied)
	t.Logf("UNDO_NATIVE_ONE_COMMIT original=%s switch_back=%s graph=%d->%d deleted=%d inserted=%d old_roots_exact=true source_bytes_exact=true reopened=true", receipt.OriginalCommit, receipt.SwitchBackCommit, receipt.OriginalCommits, receipt.CurrentCommits, receipt.DeletedRows, receipt.WrittenRows)
}

func TestPhysicalUndoAtomicRefusals(t *testing.T) {
	conn, _, _, original := undoFixture(t)
	cases := []struct {
		name, kind string
		mutate     func(*UndoRequest)
	}{
		{"missing_working_port", "source-completeness", func(r *UndoRequest) { r.Source.Working = nil; r.Source.SHA256, _ = bindingDigest(r.Source) }},
		{"missing_original_unversioned_table", "source-completeness", func(r *UndoRequest) {
			for i, table := range r.Source.Working.Image.Tables {
				if table.Name == "undo_unversioned" {
					r.Source.Working.Image.Tables = append(r.Source.Working.Image.Tables[:i], r.Source.Working.Image.Tables[i+1:]...)
					break
				}
			}
			r.Source.SHA256, _ = bindingDigest(r.Source)
		}},
		{"report_bytes_disagree", "reverse-report", func(r *UndoRequest) { r.ReverseReportBytes = []byte("{}") }},
		{"strict_history_replay", "history-replay-refused", func(r *UndoRequest) { r.HistoryMode = "full" }},
		{"tampered_source", "source-binding", func(r *UndoRequest) { r.Source.Image.Tables[0].Columns[0].SQLType = "text" }},
		{"wrong_source_head", "source-binding", func(r *UndoRequest) {
			r.Source.CommitID = strings.Repeat("0", 32)
			r.Source.SHA256, _ = bindingDigest(r.Source)
		}},
		{"missing_user_table", "user-table-completeness", func(r *UndoRequest) {
			for i, table := range r.Current.Tables {
				if table.Name == "undo_empty" {
					r.Current.Tables = append(r.Current.Tables[:i], r.Current.Tables[i+1:]...)
					break
				}
			}
		}},
		{"unknown_user_table", "unknown-user-table", func(r *UndoRequest) {
			table := *undoTable(t, &r.Current, "undo_empty")
			table.Name = "unknown_user_table"
			r.Current.Tables = append(r.Current.Tables, table)
		}},
		{"wrong_sql_type", "schema", func(r *UndoRequest) { undoTable(t, &r.Current, "undo_cells").Columns[0].SQLType = "text" }},
		{"native_second_insert_duplicate", "insert", func(r *UndoRequest) {
			table := undoTable(t, &r.Current, "undo_cells")
			table.Rows[1][0] = table.Rows[0][0]
		}},
		{"native_coercion_every_cell_readback", "physical-readback", func(r *UndoRequest) {
			undoTable(t, &r.Current, "undo_cells").Rows[1][0] = Cell{Type: "string", Data: "12"}
		}},
		{"unrepresentable_new_metadata", "physical-readback", func(r *UndoRequest) {
			table := undoTable(t, &r.Current, "undo_cells")
			for ci, col := range table.Columns {
				if col.Name == "metadata" {
					table.Rows[0][ci] = Cell{Type: "string", Data: `{"new":9007199254740993}`}
				}
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := undoClone(t, original)
			test.mutate(&request)
			_, err := Undo(context.Background(), conn, request)
			var failure *Error
			if !errors.As(err, &failure) || failure.Kind != test.kind {
				t.Fatalf("want %s: %v", test.kind, err)
			}
			if test.name == "unrepresentable_new_metadata" && (failure.Table != "undo_cells" || failure.Column != "metadata" || failure.Row != 0) {
				t.Fatalf("metadata field+row missing: %v", err)
			}
			if test.name == "native_coercion_every_cell_readback" && (failure.Column != "id" || failure.Row != 1) {
				t.Fatalf("coercion field+row missing: %v", err)
			}
			undoCheckUnchanged(t, conn, original)
			t.Logf("UNDO_REFUSAL kind=%s field=%s row=%d preexisting_rows_preserved=true head_preserved=true", failure.Kind, failure.Column, failure.Row)
		})
	}
}

func TestPhysicalUndoCancellationAfterNativeInserts(t *testing.T) {
	conn, _, _, request := undoFixture(t)
	table := undoTable(t, &request.Current, "undo_keyless")
	table.Rows = make([][]Cell, 50000)
	for i := range table.Rows {
		table.Rows[i] = []Cell{{Type: "string", Data: fmt.Sprint(i)}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Undo(ctx, conn, request)
	var failure *Error
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &failure) || failure.Kind != "canceled" || failure.Row < 1 {
		t.Fatalf("must cancel after actual native insertions: %v", err)
	}
	undoCheckUnchanged(t, conn, request)
	t.Logf("UNDO_CANCEL actual_native_insertions_before_cancel=%d prior_populated_rows_preserved=true original_graph_preserved=true", failure.Row)
}

func TestPhysicalUndoPublicBinaryCopyReopenLossReceipt(t *testing.T) {
	_, source, release, request := undoFixture(t)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	request.ReverseReportBytes = append([]byte(" \n"), request.ReverseReport...)
	request.ReverseReportBytes = append(request.ReverseReportBytes, '\n')
	binary, err := bazeltest.RunfileEnv("BEADS_PHYSICAL_RESTORE_BINARY")
	if err != nil {
		t.Fatal(err)
	}
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "undo-native-physical-restore"), binaryBytes, 0700); err != nil {
		t.Fatal(err)
	}
	work := undoWork(t)
	if err := os.Chmod(work, 0700); err != nil {
		t.Fatal(err)
	}
	input, target, output, loss, receiptFile := filepath.Join(work, "request.json"), filepath.Join(work, "copy"), filepath.Join(work, "actual.json"), filepath.Join(work, "loss.json"), filepath.Join(work, "receipt.json")
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--undo-preserved-copy", "--disposable-target", "--preserved-data-dir", source, "--data-dir", target, "--database", "restored", "--undo-request", input, "--readback-output", output, "--loss-report-output", loss, "--undo-receipt-output", receiptFile)
	result, err := cmd.CombinedOutput()
	t.Logf("UNDO_BINARY_ACTUAL sha256=%x argv=%q output=%s", sha256.Sum256(binaryBytes), cmd.Args, result)
	if err != nil {
		t.Fatalf("actual undo command: %v", err)
	}
	actualBytes, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := Read(strings.NewReader(string(actualBytes)))
	if err != nil {
		t.Fatal(err)
	}
	lossBytes, err := os.ReadFile(loss)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(lossBytes, request.ReverseReportBytes) {
		t.Fatal("original converter report bytes changed")
	}
	receiptBytes, err := os.ReadFile(receiptFile)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Undo UndoReceipt `json:"undo"`
		Copy CopyReceipt `json:"preserved_copy"`
	}
	if err := json.Unmarshal(receiptBytes, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Undo.ReverseReportSHA256 != fmt.Sprintf("%x", sha256.Sum256(lossBytes)) {
		t.Fatal("loss report hash does not bind original bytes")
	}
	if !envelope.Undo.Reopened || !envelope.Undo.PhysicalReadbackVerified || envelope.Undo.CurrentCommits != envelope.Undo.OriginalCommits+1 {
		t.Fatal("actual receipt missing reopen/graph/loss binding")
	}
	conn, closeCopy := undoOpen(t, target)
	verified, err := VerifyUndo(context.Background(), conn, request, envelope.Undo)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareManifests(actual, verified); err != nil {
		t.Fatal(err)
	}
	if err := closeCopy(); err != nil {
		t.Fatal(err)
	}
	unchanged, err := InventoryPreservedStore(context.Background(), source)
	if err != nil || unchanged.SHA256 != envelope.Copy.SHA256 {
		t.Fatalf("source byte binding: %v", err)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "undo-binary-original-reverse-report.json"), lossBytes, 0600); err != nil {
		t.Fatal(err)
	}
	saveEvidence(t, "undo-binary-request.json", request)
	saveEvidence(t, "undo-binary-actual-root.json", actual)
	saveEvidence(t, "undo-binary-loss-receipt.json", envelope)
	saveEvidence(t, "undo-binary-command.json", map[string]any{"argv": cmd.Args, "output": string(result), "binary_sha256": fmt.Sprintf("%x", sha256.Sum256(binaryBytes))})
}

func TestPhysicalUndoCopyAndEnvelopeRefusals(t *testing.T) {
	_, source, release, request := undoFixture(t)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	t.Run("owned_source_bytes_and_overlap", func(t *testing.T) {
		if _, err := CopyPreservedStore(context.Background(), source, filepath.Join(source, "overlap")); err == nil {
			t.Fatal("overlapping copy accepted")
		}
	})
	t.Run("canceled_copy_no_target", func(t *testing.T) {
		parent := t.TempDir()
		if err := os.Chmod(parent, 0700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(parent, "copy")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := CopyPreservedStore(ctx, source, target); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled copy: %v", err)
		}
		if _, err := os.Lstat(target); !os.IsNotExist(err) {
			t.Fatal("canceled copy left target")
		}
	})
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{string(data) + " {}", strings.Replace(string(data), `"format":`, `"unknown":0,"format":`, 1), strings.Replace(string(data), `"format":`, `"format":"duplicate","format":`, 1)} {
		if _, err := ReadUndo(strings.NewReader(invalid)); err == nil {
			t.Fatal("malformed undo accepted")
		}
	}
	t.Run("source_report_binding", func(t *testing.T) {
		decoded, err := ReadUndo(strings.NewReader(string(data)))
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Source.SHA256 != request.Source.SHA256 || !reflect.DeepEqual(decoded.ReverseReport, request.ReverseReport) {
			t.Fatal("typed source/report bytes changed")
		}
	})
}

func TestPhysicalUndoActualNativeSchemaWorkingFamilies(t *testing.T) {
	_, conn, _, _ := restoreDB(t, true)
	execSQL(t, conn, `INSERT INTO issues (id,title,agent_state,hook_bead,role_bead,role_type,timeout_ns,metadata,payload,waiters) VALUES ('undo-1','preserved issue','waiting','undo-2','undo-1','agent',9007199254740993,JSON_OBJECT('native',9007199254740993),'{"opaque":null}','["undo-2",null]')`)
	execSQL(t, conn, `INSERT INTO wisps (id,title,agent_state,timeout_ns,payload) VALUES ('undo-wisp','preserved ignored wisp','working',9007199254740993,'{"null":null}')`)
	execSQL(t, conn, "CALL DOLT_ADD('.')")
	execSQL(t, conn, "CALL DOLT_COMMIT('-Am','public preserved native schema before undo')")
	source := undoCapture(t, conn)
	for _, table := range source.Image.Tables {
		if table.Name == "wisps" {
			t.Fatal("fixture wisps must be actually unversioned in immutable HEAD")
		}
	}
	current := cloneManifest(t, source.Working.Image)
	users := make([]Table, 0, len(current.Tables))
	for _, table := range current.Tables {
		if !strings.HasPrefix(table.Name, "dolt_") {
			users = append(users, table)
		}
	}
	current.Tables = users
	for _, name := range []string{"issues", "wisps"} {
		table := undoTable(t, &current, name)
		for ci, col := range table.Columns {
			if col.Name == "title" {
				table.Rows[0][ci] = Cell{Type: "string", Data: "current Plug head " + name}
			}
		}
	}
	request := UndoRequest{Format: UndoFormat, HistoryMode: "switch-back", Source: source, Current: current, ReverseReport: json.RawMessage(`{"lost_post_switch_history":["public native fixture has no supplied Plug history census"],"core_kind_losses":[],"namespace_losses":[]}`)}
	receipt, err := Undo(context.Background(), conn, request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := VerifyUndo(context.Background(), conn, request, receipt)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"issues", "wisps"} {
		var title string
		if err := conn.QueryRowContext(context.Background(), "SELECT title FROM `"+name+"`").Scan(&title); err != nil || title != "current Plug head "+name {
			t.Fatalf("actual native %s replacement %q %v", name, title, err)
		}
	}
	immutable, err := physicalcapture.ReadCommit(context.Background(), conn, receipt.SwitchBackCommit)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range immutable.Tables {
		if table.Name == "wisps" {
			t.Fatal("undo fabricated ignored wisp history")
		}
	}
	saveEvidence(t, "undo-native-schema-request.json", request)
	saveEvidence(t, "undo-native-schema-actual-working-root.json", actual)
	saveEvidence(t, "undo-native-schema-actual-immutable-root.json", immutable)
	saveEvidence(t, "undo-native-schema-receipt.json", receipt)
	t.Logf("UNDO_NATIVE_SCHEMA complete_working_tables=%d actual_immutable_tables=%d ignored_wisp_replaced=true ignored_history_grafted=false new_commits=%d", len(actual.Tables), len(immutable.Tables), receipt.CurrentCommits-receipt.OriginalCommits)
}

func TestPhysicalUndoPublicationFailureIsPostCommitIndeterminate(t *testing.T) {
	_, source, release, request := undoFixture(t)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	binary, err := bazeltest.RunfileEnv("BEADS_PHYSICAL_RESTORE_BINARY")
	if err != nil {
		t.Fatal(err)
	}
	work := undoWork(t)
	parent := filepath.Join(work, "publication")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	target, input := filepath.Join(work, "copy"), filepath.Join(work, "request.json")
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "--undo-preserved-copy", "--disposable-target", "--preserved-data-dir", source, "--data-dir", target, "--database", "restored", "--undo-request", input, "--readback-output", filepath.Join(parent, "actual.json"), "--loss-report-output", filepath.Join(parent, "loss.json"), "--undo-receipt-output", filepath.Join(parent, "receipt.json"))
	// Destination mkdir occurs only after all path/input preflights. Remove this
	// test-owned output parent then: a real filesystem failure after the native
	// operation must never be described as precommit atomic refusal.
	removed := make(chan error, 1)
	go func() {
		timer := time.NewTicker(2 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				removed <- ctx.Err()
				return
			case <-timer.C:
				if _, err := os.Stat(target); err == nil {
					removed <- os.RemoveAll(parent)
					return
				}
			}
		}
	}()
	result, runErr := cmd.CombinedOutput()
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	if runErr == nil || !strings.Contains(string(result), "post-commit-indeterminate") || !strings.Contains(string(result), "receipt-publication-after-commit") {
		t.Fatalf("must retain actual postcommit publication error: %v %s", runErr, result)
	}
	conn, closeCopy := undoOpen(t, target)
	head := undoHead(t, conn)
	var parentHash string
	if err := conn.QueryRowContext(context.Background(), "SELECT DOLT_HASHOF('HEAD~1')").Scan(&parentHash); err != nil || parentHash != request.Source.CommitID || head == parentHash {
		t.Fatalf("actual accepted switchback must remain explicit: %s %s %v", head, parentHash, err)
	}
	working, err := physicalcapture.ReadWorkingRoot(context.Background(), conn, head)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := FromWorkingImage(working)
	if err != nil {
		t.Fatal(err)
	}
	expected := cloneManifest(t, request.Source.Working.Image)
	for i, table := range expected.Tables {
		for _, replacement := range request.Current.Tables {
			if replacement.Name == table.Name {
				expected.Tables[i] = replacement
			}
		}
	}
	if err := compareManifests(expected, actual); err != nil {
		t.Fatal(err)
	}
	if err := closeCopy(); err != nil {
		t.Fatal(err)
	}
	saveEvidence(t, "undo-postcommit-publication-failure.json", map[string]any{"argv": cmd.Args, "stderr": string(result), "original_commit": request.Source.CommitID, "actual_head": head, "actual_parent": parentHash, "status": "post-commit-indeterminate", "atomic_refusal": false})
	saveEvidence(t, "undo-postcommit-publication-actual-root.json", actual)
	t.Log("UNDO_POSTCOMMIT_PUBLICATION actual_commit_present=true real_reopen=true status=post-commit-indeterminate publication_cause_retained=true")
}

func TestPhysicalUndoCLIRefusesOutputsInsidePreservedSource(t *testing.T) {
	_, source, release, request := undoFixture(t)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	original, err := InventoryPreservedStore(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := bazeltest.RunfileEnv("BEADS_PHYSICAL_RESTORE_BINARY")
	if err != nil {
		t.Fatal(err)
	}
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	var commands []map[string]any
	for _, name := range []string{"readback", "loss", "receipt"} {
		t.Run(name, func(t *testing.T) {
			work := undoWork(t)
			target, input := filepath.Join(work, "copy"), filepath.Join(work, "request.json")
			data, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(input, data, 0600); err != nil {
				t.Fatal(err)
			}
			outputs := map[string]string{"readback": filepath.Join(work, "actual.json"), "loss": filepath.Join(work, "loss.json"), "receipt": filepath.Join(work, "receipt.json")}
			forbidden := filepath.Join(source, "forbidden-"+name+".json")
			outputs[name] = forbidden
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "--undo-preserved-copy", "--disposable-target", "--preserved-data-dir", source, "--data-dir", target, "--database", "restored", "--undo-request", input, "--readback-output", outputs["readback"], "--loss-report-output", outputs["loss"], "--undo-receipt-output", outputs["receipt"])
			result, runErr := cmd.CombinedOutput()
			if runErr == nil || !strings.Contains(string(result), "preserved-source-overlap") {
				t.Fatalf("expected explicit pre-copy preserved-source refusal: %v %s", runErr, result)
			}
			for _, path := range []string{target, outputs["readback"], outputs["loss"], outputs["receipt"]} {
				if _, err := os.Lstat(path); !os.IsNotExist(err) {
					t.Fatalf("refusal created target/output: %v", err)
				}
			}
			unchanged, err := InventoryPreservedStore(context.Background(), source)
			if err != nil || unchanged.SHA256 != original.SHA256 {
				t.Fatalf("refusal changed original preserved bytes: %v", err)
			}
			commands = append(commands, map[string]any{"output": name, "argv": cmd.Args, "stderr": string(result), "binary_sha256": fmt.Sprintf("%x", sha256.Sum256(binaryBytes)), "original_sha256": original.SHA256, "after_sha256": unchanged.SHA256, "target_absent": true, "outputs_absent": true, "status": "preserved-source-overlap"})
			t.Logf("UNDO_OUTPUT_OVERLAP output=%s explicit_refusal=true copy_absent=true original_bytes_exact=true all_outputs_absent=true", name)
		})
	}
	saveEvidence(t, "undo-preserved-output-overlap-commands.json", commands)
	saveEvidence(t, "undo-preserved-output-overlap-original-files.json", original)
}
