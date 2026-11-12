package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
)

func TestPublicNativeDeltaCapturesOver64MiBEquivalentHistory(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	dataDir, home := diagnosticDir(t), diagnosticDir(t)
	db, closeDriver, err := openPublicMigrationFixture(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDriver()
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE public_native_delta")
	ownVersionExec(t, ctx, conn, "USE public_native_delta")
	if _, err := schema.MigrateUpTo(ctx, conn, 66); err != nil {
		t.Fatal(err)
	}
	for _, name := range ownVersionUnversioned {
		ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore (pattern,ignored) VALUES (?,true) ON DUPLICATE KEY UPDATE ignored=true", name)
	}
	ownVersionExec(t, ctx, conn, "CREATE TABLE IF NOT EXISTS ignored_schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64))")
	for i, name := range ownVersionIgnoredMigrations[:26] {
		query, err := schema.IgnoredMigrationSQL(name)
		if err != nil {
			t.Fatal(err)
		}
		ownVersionExec(t, ctx, conn, query)
		ownVersionExec(t, ctx, conn, "INSERT INTO ignored_schema_migrations(version,content_hash) VALUES (?,?)", i+1, fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	large := strings.Repeat("public unchanged large description ", (3<<20)/35+1)
	singleLeaf := strings.Repeat("public single-leaf description ", 100)
	// The pinned Dolt tuple target is 2048 bytes and blob leaves are 4000
	// bytes. This TEXT exceeds the inline target but needs only one leaf.
	if len(singleLeaf) <= 2048 || len(singleLeaf) > 4000 {
		t.Fatal("single-leaf out-of-line fixture has the wrong size")
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,created_by) VALUES ('public-a1b','large ordinary row',?,'','','','public')", large)
	ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,created_by) VALUES ('public-b2c','single-leaf ordinary row',?,'','','','public')", singleLeaf)
	descriptions := map[string]string{"public-a1b": large, "public-b2c": singleLeaf}
	ownVersionExec(t, ctx, conn, "INSERT INTO wisps (id,title,created_by) VALUES ('public-w1','current-only row','public')")
	// A real public view gives dolt_schemas nonempty physical content, even
	// though the portable information_schema BASE TABLE census excludes it.
	ownVersionExec(t, ctx, conn, "CREATE VIEW public_delta_issue_titles AS SELECT id,title FROM issues")
	var head string
	for i := 0; i < 25; i++ {
		ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('public.delta_counter',?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)", fmt.Sprint(i))
		head = ownVersionCommit(t, ctx, conn)
	}
	ownVersionExec(t, ctx, conn, "UPDATE wisps SET title='separate current-only value' WHERE id='public-w1'")
	before, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}
	workspace, output := diagnosticDir(t), diagnosticDir(t)
	beadsDir := filepath.Join(workspace, ".beads")
	if err := os.Mkdir(beadsDir, 0700); err != nil {
		t.Fatal(err)
	}
	environment := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "DOLT_ROOT_PATH" || strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "XDG_") {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(environment, "HOME="+home, "DOLT_ROOT_PATH="+home, "BEADS_DIR="+beadsDir, "XDG_CONFIG_HOME="+home, "BD_DISABLE_METRICS=1", "BD_DISABLE_EVENT_FLUSH=1")
	command := exec.CommandContext(ctx, binary, "--disposable", "--native-delta", "--data-dir", dataDir, "--database", "public_native_delta", "--beads-dir", beadsDir, "--output-dir", output)
	command.Dir, command.Env = workspace, environment
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		// This fixture contains only public, valid SQL values. Preserve its
		// actual nested reader cause for diagnosis without changing the closed
		// product diagnostic or inspecting any malformed-data control.
		deltaFixtureReadCause(t, ctx, dataDir, head)
		deltaFixtureCaptureCause(t, command.Args[1:], workspace, environment)
		t.Fatalf("ordinary native delta capture failed: %v stdout=%s stderr=%s", err, stdout.Bytes(), stderr.Bytes())
	}
	setDirectory := filepath.Join(output, "capture-set")
	entries, err := os.ReadDir(setDirectory)
	if err != nil || len(entries) != 3 || entries[0].Name() != "capture-set.json" || entries[1].Name() != "head.tar" || entries[2].Name() != "parts" {
		t.Fatal("capture-set directory has extra or missing members")
	}
	setData, err := os.ReadFile(filepath.Join(setDirectory, "capture-set.json"))
	if err != nil {
		t.Fatal(err)
	}
	var set plugdump.NativeCaptureSet
	if err := json.Unmarshal(setData, &set); err != nil {
		t.Fatal(err)
	}
	if set.Head != head || set.Totals.Commits < 25 || set.Totals.FullImageBytes <= 64<<20 || set.StreamBytes >= set.Totals.FullImageBytes || set.PartCount == 0 {
		t.Fatalf("old capacity failure not exercised: %+v", set.Totals)
	}
	headData, err := os.ReadFile(filepath.Join(setDirectory, "head.tar"))
	if err != nil || set.HeadArchive.Size != int64(len(headData)) || set.HeadArchive.SHA256 != deltaFixtureDigest(headData) {
		t.Fatal("head attachment differs")
	}
	if _, err := plugdump.ValidateNativeDeltaHead(bytes.NewReader(headData)); err != nil {
		t.Fatal(err)
	}
	if _, err := plugdump.Validate(bytes.NewReader(headData)); err == nil {
		t.Fatal("legacy archive validator silently accepted v2 head")
	}
	stream := deltaFixtureStream(t, setDirectory, set)
	verified := deltaFixtureReplay(t, stream, set, descriptions, before)
	if verified != set.Totals.Commits {
		t.Fatal("not every commit was reconstructed")
	}
	evidenceData, err := os.ReadFile(filepath.Join(output, "physical-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence plugdump.NativeDeltaEvidence
	if err := json.Unmarshal(evidenceData, &evidence); err != nil || evidence.CaptureSetSHA256 != deltaFixtureDigest(setData) || evidence.Totals != set.Totals || evidence.Working.WorkingRootHash != before.WorkingRootHash {
		t.Fatal("evidence lost exact source/set binding")
	}
	reopened, closeReopened, err := openPublicMigrationFixture(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReopened()
	defer reopened.Close()
	check, err := reopened.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	ownVersionExec(t, ctx, check, "USE public_native_delta")
	after, err := physicalcapture.ReadWorkingRoot(ctx, check, head)
	if err != nil || before.WorkingRootHash != after.WorkingRootHash || !reflect.DeepEqual(before.Tables, after.Tables) {
		t.Fatal("capture changed source working root or typed rows")
	}
	// Preserve the genuine producer output for the separately pinned receiver
	// import proof. The three-entry set and sibling evidence keep their exact
	// layout; copying does not turn this capture-only test into import proof.
	retained := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "public-native-delta-over64mib")
	root, err := os.OpenRoot(output)
	if err != nil {
		t.Fatal(err)
	}
	copyErr := os.CopyFS(retained, root.FS())
	if err := errors.Join(copyErr, root.Close()); err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{}
	for id, description := range descriptions {
		expected[id] = map[string]any{"bytes": len(description), "sha256": deltaFixtureDigest([]byte(description))}
	}
	facts, err := json.Marshal(map[string]any{"format": "public-native-delta-fixture/1", "configured_commits": 25, "verified_commits": verified, "capture_set_sha256": deltaFixtureDigest(setData), "expected_descriptions": expected, "scope": "genuine producer capture and replay; receiver import remains separate"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(retained, "fixture.json"), facts, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("PUBLIC_NATIVE_DELTA_RETAINED capture_set=public-native-delta-over64mib/capture-set evidence=public-native-delta-over64mib/physical-evidence.json")
	t.Logf("PUBLIC_NATIVE_DELTA_CAPTURE commits=%d old_full_image_bytes=%d stream_bytes=%d parts=%d all_commit_table_hashes=verified scope=ordinary_fixture_capture_not_receiver_import", verified, set.Totals.FullImageBytes, set.StreamBytes, set.PartCount)
}

func deltaFixtureReadCause(t *testing.T, ctx context.Context, dataDir, head string) {
	t.Helper()
	db, closeDriver, err := openPublicMigrationFixture(dataDir)
	if err != nil {
		t.Logf("PUBLIC_NATIVE_DELTA_READER_OBSERVATION open=%v", err)
		return
	}
	defer closeDriver()
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Logf("PUBLIC_NATIVE_DELTA_READER_OBSERVATION connection=%v", err)
		return
	}
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "USE public_native_delta")
	_, err = physicalcapture.ReadCommit(ctx, conn, head)
	var visit func(error, int)
	visit = func(cause error, depth int) {
		if cause == nil || depth > 32 {
			return
		}
		t.Logf("PUBLIC_NATIVE_DELTA_READER_OBSERVATION depth=%d type=%T cause=%v", depth, cause, cause)
		switch wrapped := cause.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child, depth+1)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
	if err == nil {
		t.Log("PUBLIC_NATIVE_DELTA_READER_OBSERVATION immutable_read=success")
	}
}

// Reproduce only this known-public fixture through the same capture entry
// point so its actual wrapped cause can be observed inside the test process.
// Product diagnostics remain closed, and the original ELF result stays RED.
func deltaFixtureCaptureCause(t *testing.T, args []string, workspace string, environment []string) {
	t.Helper()
	t.Chdir(workspace)
	keys := map[string]bool{}
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		keys[key] = true
	}
	// command.Env replaces the environment; an overlay would retain the
	// wrapper's intentionally excluded BD_/BEADS_ and other source settings.
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if !keys[key] {
			t.Cleanup(func() {
				if err := os.Setenv(key, value); err != nil {
					t.Errorf("restore public observer environment: %v", err)
				}
			})
			if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			t.Setenv(key, value)
		}
	}
	args = append([]string{}, args...)
	for i, arg := range args {
		if arg == "--output-dir" && i+1 < len(args) {
			args[i+1] = diagnosticDir(t)
		}
	}
	previousArgs := os.Args
	os.Args = append([]string{"public-native-delta-cause-observer"}, args...)
	defer func() { os.Args = previousArgs }()
	var visit func(error, int)
	visit = func(cause error, depth int) {
		if cause == nil || depth > 32 {
			return
		}
		t.Logf("PUBLIC_NATIVE_DELTA_CAPTURE_OBSERVATION depth=%d type=%T cause=%v", depth, cause, cause)
		switch wrapped := cause.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child, depth+1)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(run(), 0)
}

func deltaFixtureDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func TestNativeDeltaRootPublishesClosedBytesExclusively(t *testing.T) {
	parent := t.TempDir()
	directory := filepath.Join(parent, "capture-set")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "capture-set.json")
	data := []byte("{\"format\":\"ordinary atomic publication fixture\"}\n")
	if err := writeNativeDeltaRoot(path, data); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, data) {
		t.Fatal("published root is incomplete")
	}
	if err := writeNativeDeltaRoot(path, []byte("replacement")); !errors.Is(err, os.ErrExist) {
		t.Fatal("existing root was not refused")
	}
	actual, err = os.ReadFile(path)
	entries, listErr := os.ReadDir(parent)
	if err != nil || !bytes.Equal(actual, data) || listErr != nil || len(entries) != 1 || entries[0].Name() != "capture-set" {
		t.Fatal("existing root changed or owned staging file leaked")
	}
}

func deltaFixtureStream(t *testing.T, directory string, set plugdump.NativeCaptureSet) []byte {
	t.Helper()
	var stream bytes.Buffer
	var previous *string
	for i := uint64(1); i <= set.PartCount; i++ {
		data, err := os.ReadFile(filepath.Join(directory, "parts", fmt.Sprintf("%020d.part", i)))
		if err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(bytes.NewReader(data))
		line, err := reader.ReadBytes('\n')
		if err != nil || len(line) > plugdump.NativeDeltaPartHeaderBytes {
			t.Fatal("invalid part header")
		}
		var header plugdump.NativeDeltaPartHeader
		if err := json.Unmarshal(line, &header); err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(reader)
		if err != nil || header.PayloadBytes != uint64(len(payload)) || header.PayloadBytes == 0 || header.PayloadBytes > plugdump.NativeDeltaPartBytes || i < set.PartCount && header.PayloadBytes != plugdump.NativeDeltaPartBytes || header.PayloadSHA256 != deltaFixtureDigest(payload) || header.Sequence != i || !reflect.DeepEqual(header.PreviousPartSHA256, previous) {
			t.Fatal("part commitment differs")
		}
		hash := deltaFixtureDigest(data)
		previous = &hash
		stream.Write(payload)
	}
	if previous == nil || *previous != set.FinalPartSHA256 || set.StreamBytes != uint64(stream.Len()) || set.StreamSHA256 != deltaFixtureDigest(stream.Bytes()) {
		t.Fatal("capture-set stream commitment differs")
	}
	return stream.Bytes()
}

type deltaFixtureState map[string]map[string]plugdump.NativeDeltaRow

// This fixture replay checks every emitted commit/table checksum and exact
// full-image byte metric. The separately owned receiver/import proof is required.
func deltaFixtureReplay(t *testing.T, stream []byte, set plugdump.NativeCaptureSet, descriptions map[string]string, working physicalcapture.WorkingImage) uint64 {
	t.Helper()
	schemas := map[string]plugdump.NativeDeltaSchema{}
	commits := map[string]deltaFixtureState{}
	decoder := json.NewDecoder(bytes.NewReader(stream))
	var state deltaFixtureState
	var currentSchema plugdump.NativeDeltaSchema
	var commit plugdump.NativeDeltaCommit
	var table plugdump.NativeDeltaTable
	var rowOps uint64
	var immutable bool
	var digests map[string]plugdump.NativeDeltaTableDigest
	var totals plugdump.NativeDeltaTotals
	var descriptionStates uint64
	var checkedCurrent bool
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		var tag struct{ Type string }
		deltaFixtureDecode(t, raw, &tag)
		switch tag.Type {
		case "header":
			var header plugdump.NativeDeltaHeader
			deltaFixtureDecode(t, raw, &header)
			if header.Head != set.Head || header.CommitCount != set.Totals.Commits || header.Ignore.Rows == nil || header.ConfigPolicy == nil {
				t.Fatal("stream authority differs")
			}
		case "schema":
			var schema plugdump.NativeDeltaSchema
			deltaFixtureDecode(t, raw, &schema)
			if _, exists := schemas[schema.SchemaID]; exists || schema.SchemaID != plugdump.NativeDeltaSchemaID(schema) {
				t.Fatal("schema reused or misbound")
			}
			schemas[schema.SchemaID] = schema
			totals.Schemas++
		case "commit", "head-checkpoint", "current":
			state, digests = deltaFixtureState{}, map[string]plugdump.NativeDeltaTableDigest{}
			immutable = tag.Type == "commit"
			if immutable {
				deltaFixtureDecode(t, raw, &commit)
				currentSchema = schemas[commit.SchemaID]
				var parent deltaFixtureState
				if commit.FirstParent != nil {
					parent = commits[*commit.FirstParent]
				}
				if (commit.Mode == "delta") != (parent != nil) || commit.Sequence != totals.Commits+1 {
					t.Fatal("noncanonical commit mode/order")
				}
				for name, rows := range parent {
					state[name] = map[string]plugdump.NativeDeltaRow{}
					for key, row := range rows {
						state[name][key] = row
					}
				}
			} else {
				var checkpoint plugdump.NativeDeltaCheckpoint
				deltaFixtureDecode(t, raw, &checkpoint)
				currentSchema = schemas[checkpoint.SchemaID]
			}
		case "drop_table":
			var drop plugdump.NativeDeltaDropTable
			deltaFixtureDecode(t, raw, &drop)
			if _, exists := state[drop.Name]; !exists {
				t.Fatal("drop without retained table")
			}
			delete(state, drop.Name)
		case "table":
			deltaFixtureDecode(t, raw, &table)
			rowOps = 0
			if table.Mode == "replace" {
				state[table.Name] = map[string]plugdump.NativeDeltaRow{}
			} else if state[table.Name] == nil {
				t.Fatal("delta table without base")
			}
			totals.TableTransactions++
		case "row":
			var row plugdump.NativeDeltaRow
			deltaFixtureDecode(t, raw, &row)
			_, exists := state[table.Name][row.Key]
			if row.Op == "insert" && exists || row.Op != "insert" && !exists {
				t.Fatal("invalid row operation precondition")
			}
			if row.Op == "delete" {
				delete(state[table.Name], row.Key)
			} else {
				state[table.Name][row.Key] = row
			}
			rowOps++
			totals.RowOperations++
		case "end_table":
			var end plugdump.NativeDeltaEndTable
			deltaFixtureDecode(t, raw, &end)
			var definition plugdump.NativeDeltaTableSchema
			for _, candidate := range currentSchema.Tables {
				if candidate.Name == table.Name {
					definition = candidate
				}
			}
			rows := deltaFixtureSortedRows(t, state[table.Name])
			if table.Name == "issues" && len(rows) != 0 {
				deltaFixtureDescriptions(t, definition, rows, descriptions)
				descriptionStates++
			}
			hash, err := plugdump.NativeDeltaTableSHA256(definition, rows)
			if err != nil || end.Rows != uint64(len(rows)) || table.ExpectedRows != end.Rows || end.SHA256 != hash || table.ExpectedSHA256 != hash || end.RowOperations != rowOps || table.TableSchemaID != plugdump.NativeDeltaTableSchemaID(definition) {
				t.Fatal("reconstructed table count/hash differs")
			}
			digests[table.Name] = plugdump.NativeDeltaTableDigest{Rows: end.Rows, SHA256: hash}
			totals.ReconstructedRows += end.Rows
			totals.ReconstructedCells += end.Rows * uint64(len(definition.Columns))
		case "end_commit", "end_head-checkpoint", "end_current":
			var end plugdump.NativeDeltaEndCommit
			deltaFixtureDecode(t, raw, &end)
			root, err := plugdump.NativeDeltaRootSHA256(digests)
			if err != nil || end.RootSHA256 != root || end.TableCount != uint64(len(state)) || len(digests) != len(state) {
				t.Fatal("reconstructed commit count/hash differs")
			}
			if tag.Type == "end_current" {
				deltaFixtureWorkingRoot(t, currentSchema, state, working)
				checkedCurrent = true
			}
			if immutable {
				if commit.ExpectedRootSHA256 != root || end.Sequence != commit.Sequence || end.CommitID != commit.CommitID {
					t.Fatal("commit identity/root differs")
				}
				tables := map[string]plugdump.NativeTable{}
				for _, definition := range currentSchema.Tables {
					columns, rows := []string{}, []plugdump.Row{}
					for _, column := range definition.Columns {
						columns = append(columns, column.Name)
					}
					for _, row := range state[definition.Name] {
						data, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
						if err != nil {
							t.Fatal(err)
						}
						var normal plugdump.Row
						decode := json.NewDecoder(bytes.NewReader(data))
						decode.UseNumber()
						if err := decode.Decode(&normal); err != nil {
							t.Fatal(err)
						}
						rows = append(rows, normal)
					}
					tables[definition.Name] = ownVersionCanonical(t, plugdump.Table{Columns: columns, Rows: rows})
				}
				frame, err := json.Marshal(plugdump.NativeHistoryImage{SchemaProfile: &currentSchema.Profile, Sequence: commit.Sequence, CommitID: commit.CommitID, Tables: tables})
				if err != nil {
					t.Fatal(err)
				}
				totals.FullImageBytes += uint64(len(frame) + 1)
				commits[commit.CommitID] = state
				totals.Commits++
			}
		case "end":
			var end plugdump.NativeDeltaEnd
			deltaFixtureDecode(t, raw, &end)
			if end.Totals != totals || set.Totals != totals {
				t.Fatalf("cumulative reconstruction counters differ: got=%+v want=%+v", totals, set.Totals)
			}
		default:
			t.Fatal("unknown ordinary fixture record")
		}
	}
	if descriptionStates < 27 { // 25 immutable commits plus independent HEAD/current.
		t.Fatal("large and single-leaf values were not checked in every expected state")
	}
	if !checkedCurrent {
		t.Fatal("complete physical current state was not checked")
	}
	return totals.Commits
}

// Compare the emitted current state with the independently detached source
// census, including the actual Dolt backing tables and stored schema fragments.
func deltaFixtureWorkingRoot(t *testing.T, schema plugdump.NativeDeltaSchema, state deltaFixtureState, working physicalcapture.WorkingImage) {
	t.Helper()
	evidence, err := physicalcapture.EvidenceTables(working.Tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Tables) != len(working.Tables) || len(state) != len(working.Tables) {
		t.Fatal("physical current census was omitted or expanded")
	}
	definitions := map[string]plugdump.NativeDeltaTableSchema{}
	for _, table := range schema.Tables {
		definitions[table.Name] = table
	}
	var schemaFragments bool
	for i, table := range working.Tables {
		definition, present := definitions[table.Name]
		columns := make([]plugdump.NativeDeltaColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			columns = append(columns, plugdump.NativeDeltaColumn{Name: column.Name, SQLType: column.DatabaseTypeName, PrimaryKeyOrdinal: column.PrimaryKeyOrdinal})
		}
		if !present || !reflect.DeepEqual(definition.Columns, columns) || len(state[table.Name]) != len(table.Rows) {
			t.Fatalf("physical current schema or row count differs for %s", table.Name)
		}
		expected, actual := []string{}, []string{}
		for _, row := range evidence[i].Rows {
			cells := make([]plugdump.NativeDeltaCell, len(row))
			for j, cell := range row {
				cells[j] = plugdump.NativeDeltaCell{Kind: cell.Kind, Value: cell.Value}
			}
			data, err := json.Marshal(cells)
			if err != nil {
				t.Fatal(err)
			}
			expected = append(expected, string(data))
		}
		for _, row := range state[table.Name] {
			data, err := json.Marshal(row.Cells)
			if err != nil {
				t.Fatal(err)
			}
			actual = append(actual, string(data))
		}
		sort.Strings(expected)
		sort.Strings(actual)
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("physical current typed values differ for %s", table.Name)
		}
		if table.Name == "dolt_schemas" && len(table.Rows) > 0 {
			schemaFragments = true
		}
	}
	if !schemaFragments {
		t.Fatal("public view did not exercise stored schema fragments")
	}
}

func deltaFixtureDescriptions(t *testing.T, schema plugdump.NativeDeltaTableSchema, rows []plugdump.NativeDeltaRow, expected map[string]string) {
	t.Helper()
	idColumn, descriptionColumn := -1, -1
	for i, column := range schema.Columns {
		switch column.Name {
		case "id":
			idColumn = i
		case "description":
			descriptionColumn = i
		}
	}
	if idColumn < 0 || descriptionColumn < 0 {
		t.Fatal("fixture issue columns absent")
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if len(row.Cells) != len(schema.Columns) {
			t.Fatal("fixture typed row width differs")
		}
		id := row.Cells[idColumn]
		want, present := expected[id.Value]
		if !present {
			continue
		}
		if id.Kind != "string" || row.Cells[descriptionColumn] != (plugdump.NativeDeltaCell{Kind: "string", Value: want}) {
			t.Fatal("captured typed large or single-leaf value differs from SQL input")
		}
		data, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
		if err != nil {
			t.Fatal(err)
		}
		var normalized plugdump.Row
		if err := json.Unmarshal(data, &normalized); err != nil || normalized["id"] != id.Value || normalized["description"] != want {
			t.Fatal("captured normalized large or single-leaf value differs from SQL input")
		}
		seen[id.Value] = true
	}
	if len(seen) != len(expected) {
		t.Fatal("captured state omitted a fixture large or single-leaf value")
	}
}

func deltaFixtureDecode(t *testing.T, data []byte, value any) {
	t.Helper()
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func deltaFixtureSortedRows(t *testing.T, rows map[string]plugdump.NativeDeltaRow) []plugdump.NativeDeltaRow {
	t.Helper()
	result := make([]plugdump.NativeDeltaRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool {
		a, aErr := base64.StdEncoding.DecodeString(result[i].Key)
		b, bErr := base64.StdEncoding.DecodeString(result[j].Key)
		if aErr != nil || bErr != nil {
			t.Fatal("invalid row key")
		}
		return bytes.Compare(a, b) < 0
	})
	return result
}

// A separate public landing factory preserves the larger qualification fixture.
func TestPublicNativeDeltaSmallLandingFixture(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	dataDir, home := diagnosticDir(t), diagnosticDir(t)
	db, closeDriver, err := openPublicMigrationFixture(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDriver()
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE public_native_delta_landing")
	ownVersionExec(t, ctx, conn, "USE public_native_delta_landing")
	if _, err := schema.MigrateUpTo(ctx, conn, 66); err != nil {
		t.Fatal(err)
	}
	for _, name := range ownVersionUnversioned {
		ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore (pattern,ignored) VALUES (?,true) ON DUPLICATE KEY UPDATE ignored=true", name)
	}
	ownVersionExec(t, ctx, conn, "CREATE TABLE IF NOT EXISTS ignored_schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64))")
	for i, name := range ownVersionIgnoredMigrations[:26] {
		query, err := schema.IgnoredMigrationSQL(name)
		if err != nil {
			t.Fatal(err)
		}
		ownVersionExec(t, ctx, conn, query)
		ownVersionExec(t, ctx, conn, "INSERT INTO ignored_schema_migrations(version,content_hash) VALUES (?,?)", i+1, fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	large := strings.Repeat("public unchanged large description ", (1<<20)/35+1)[:1<<20]
	singleLeaf := strings.Repeat("public single-leaf description ", 100)
	// The pinned Dolt tuple target is 2048 bytes and blob leaves are 4000
	// bytes. This TEXT exceeds the inline target but needs only one leaf.
	if len(singleLeaf) != 3100 || len(large) != 1<<20 {
		t.Fatal("single-leaf out-of-line fixture has the wrong size")
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,created_by) VALUES ('public-a1b','large ordinary row',?,'','','','public')", large)
	ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,created_by) VALUES ('public-b2c','single-leaf ordinary row',?,'','','','public')", singleLeaf)
	descriptions := map[string]string{"public-a1b": large, "public-b2c": singleLeaf}
	ownVersionExec(t, ctx, conn, "INSERT INTO wisps (id,title,created_by) VALUES ('public-w1','current-only row','public')")
	// A real public view gives dolt_schemas nonempty physical content, even
	// though the portable information_schema BASE TABLE census excludes it.
	ownVersionExec(t, ctx, conn, "CREATE VIEW public_delta_issue_titles AS SELECT id,title FROM issues")
	ownVersionExec(t, ctx, conn, "INSERT INTO federation_peers (name,remote_url,password_encrypted) VALUES ('public-example-peer','https://example.invalid/public-synthetic',NULL)")
	ownVersionExec(t, ctx, conn, "INSERT INTO routes (prefix,path) VALUES ('public-other','/public/example')")
	ownVersionExec(t, ctx, conn, "INSERT INTO interactions (id,kind,created_at,actor,issue_id,tool_name,exit_code,extra) VALUES ('public-interaction','tool_call',NOW(),'public-fixture','public-a1b','public-example',0,'{\"synthetic\":true}')")
	var head string
	for i := 0; i < 25; i++ {
		ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('public.delta_counter',?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)", fmt.Sprint(i))
		head = ownVersionCommit(t, ctx, conn)
	}
	ownVersionExec(t, ctx, conn, "UPDATE wisps SET title='separate current-only value' WHERE id='public-w1'")
	before, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	// The migrated schema has ready_issues and blocked_issues before this view.
	requiredRows := map[string]int{"federation_peers": 1, "routes": 1, "interactions": 1, "dolt_schemas": 3}
	currentRows := map[string]int{}
	for _, table := range before.Tables {
		if _, required := requiredRows[table.Name]; required {
			currentRows[table.Name] = len(table.Rows)
		}
	}
	if !reflect.DeepEqual(requiredRows, currentRows) {
		t.Fatalf("small fixture opaque current row counts differ: %v", currentRows)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}
	workspace, output := diagnosticDir(t), diagnosticDir(t)
	beadsDir := filepath.Join(workspace, ".beads")
	if err := os.Mkdir(beadsDir, 0700); err != nil {
		t.Fatal(err)
	}
	environment := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "DOLT_ROOT_PATH" || strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "XDG_") {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(environment, "HOME="+home, "DOLT_ROOT_PATH="+home, "BEADS_DIR="+beadsDir, "XDG_CONFIG_HOME="+home, "BD_DISABLE_METRICS=1", "BD_DISABLE_EVENT_FLUSH=1")
	command := exec.CommandContext(ctx, binary, "--disposable", "--native-delta", "--data-dir", dataDir, "--database", "public_native_delta_landing", "--beads-dir", beadsDir, "--output-dir", output)
	command.Dir, command.Env = workspace, environment
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		// This fixture contains only public, valid SQL values. Preserve its
		// actual nested reader cause for diagnosis without changing the closed
		// product diagnostic or inspecting any malformed-data control.
		deltaFixtureReadCause(t, ctx, dataDir, head)
		deltaFixtureCaptureCause(t, command.Args[1:], workspace, environment)
		t.Fatalf("ordinary native delta capture failed: %v stdout=%s stderr=%s", err, stdout.Bytes(), stderr.Bytes())
	}
	setDirectory := filepath.Join(output, "capture-set")
	entries, err := os.ReadDir(setDirectory)
	if err != nil || len(entries) != 3 || entries[0].Name() != "capture-set.json" || entries[1].Name() != "head.tar" || entries[2].Name() != "parts" {
		t.Fatal("capture-set directory has extra or missing members")
	}
	setData, err := os.ReadFile(filepath.Join(setDirectory, "capture-set.json"))
	if err != nil {
		t.Fatal(err)
	}
	var set plugdump.NativeCaptureSet
	if err := json.Unmarshal(setData, &set); err != nil {
		t.Fatal(err)
	}
	if set.Head != head || set.Totals.Commits < 25 || set.StreamBytes >= set.Totals.FullImageBytes || set.PartCount != 1 {
		t.Fatalf("small single-part fixture contract differs: %+v", set)
	}
	headData, err := os.ReadFile(filepath.Join(setDirectory, "head.tar"))
	if err != nil || set.HeadArchive.Size != int64(len(headData)) || set.HeadArchive.SHA256 != deltaFixtureDigest(headData) {
		t.Fatal("head attachment differs")
	}
	if _, err := plugdump.ValidateNativeDeltaHead(bytes.NewReader(headData)); err != nil {
		t.Fatal(err)
	}
	if _, err := plugdump.Validate(bytes.NewReader(headData)); err == nil {
		t.Fatal("legacy archive validator silently accepted v2 head")
	}
	stream := deltaFixtureStream(t, setDirectory, set)
	verified := deltaFixtureReplay(t, stream, set, descriptions, before)
	opaqueStates := smallDeltaFixtureOpaqueHistory(t, stream, requiredRows)
	if verified != set.Totals.Commits {
		t.Fatal("not every commit was reconstructed")
	}
	evidenceData, err := os.ReadFile(filepath.Join(output, "physical-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence plugdump.NativeDeltaEvidence
	if err := json.Unmarshal(evidenceData, &evidence); err != nil || evidence.CaptureSetSHA256 != deltaFixtureDigest(setData) || evidence.Totals != set.Totals || evidence.Working.WorkingRootHash != before.WorkingRootHash {
		t.Fatal("evidence lost exact source/set binding")
	}
	reopened, closeReopened, err := openPublicMigrationFixture(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReopened()
	defer reopened.Close()
	check, err := reopened.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	ownVersionExec(t, ctx, check, "USE public_native_delta_landing")
	after, err := physicalcapture.ReadWorkingRoot(ctx, check, head)
	if err != nil || before.WorkingRootHash != after.WorkingRootHash || !reflect.DeepEqual(before.Tables, after.Tables) {
		t.Fatal("capture changed source working root or typed rows")
	}
	// Preserve the genuine producer output for the separately pinned receiver
	// import proof. The three-entry set and sibling evidence keep their exact
	// layout; copying does not turn this capture-only test into import proof.
	retained := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "public-native-delta-landing-small")
	root, err := os.OpenRoot(output)
	if err != nil {
		t.Fatal(err)
	}
	copyErr := os.CopyFS(retained, root.FS())
	if err := errors.Join(copyErr, root.Close()); err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{}
	for id, description := range descriptions {
		expected[id] = map[string]any{"bytes": len(description), "sha256": deltaFixtureDigest([]byte(description))}
	}
	facts, err := json.Marshal(map[string]any{"format": "public-native-delta-small-landing-fixture/1", "configured_commits": 25, "verified_commits": verified, "part_count": set.PartCount, "capture_set_sha256": deltaFixtureDigest(setData), "expected_descriptions": expected, "required_current_rows": requiredRows, "opaque_history_states": opaqueStates, "credential": "SQL NULL", "scope": "genuine single-part public producer capture and replay; receiver import remains separate"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(retained, "fixture.json"), facts, 0600); err != nil {
		t.Fatal(err)
	}
	var retainedBytes int64
	if err := filepath.WalkDir(retained, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			retainedBytes += info.Size()
		} else if !info.IsDir() {
			return errors.New("unexpected small fixture member type")
		}
		return nil
	}); err != nil || retainedBytes > 20_000_000 {
		t.Fatalf("small fixture retained bytes exceed 20 MB: bytes=%d error=%v", retainedBytes, err)
	}
	t.Logf("PUBLIC_NATIVE_DELTA_SMALL_LANDING retained_bytes=%d opaque_history_states=%v", retainedBytes, opaqueStates)
	t.Log("PUBLIC_NATIVE_DELTA_RETAINED capture_set=public-native-delta-landing-small/capture-set evidence=public-native-delta-landing-small/physical-evidence.json")
	t.Logf("PUBLIC_NATIVE_DELTA_CAPTURE commits=%d old_full_image_bytes=%d stream_bytes=%d parts=%d all_commit_table_hashes=verified scope=ordinary_fixture_capture_not_receiver_import", verified, set.Totals.FullImageBytes, set.StreamBytes, set.PartCount)
}

// Check the nonempty opaque families through the same 25 committed states
// and independent HEAD/current boundaries that retain the two blob values.
func smallDeltaFixtureOpaqueHistory(t *testing.T, stream []byte, required map[string]int) map[string]int {
	t.Helper()
	states := map[string]int{}
	decoder := json.NewDecoder(bytes.NewReader(stream))
	var table string
	var rows map[string]uint64
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		var tag struct{ Type string }
		deltaFixtureDecode(t, raw, &tag)
		switch tag.Type {
		case "commit", "head-checkpoint", "current":
			rows = map[string]uint64{}
		case "table":
			var boundary plugdump.NativeDeltaTable
			deltaFixtureDecode(t, raw, &boundary)
			table = boundary.Name
		case "end_table":
			var end plugdump.NativeDeltaEndTable
			deltaFixtureDecode(t, raw, &end)
			rows[table] = end.Rows
		case "end_commit", "end_head-checkpoint", "end_current":
			// Earlier migration commits predate both fixture issues and the
			// added view; check the complete 25-commit fixture state instead.
			if rows["issues"] == 2 {
				for name, count := range required {
					if rows[name] != uint64(count) {
						t.Fatalf("opaque historical row count differs for %s", name)
					}
					states[name]++
				}
			}
		}
	}
	for name := range required {
		if states[name] < 27 {
			t.Fatalf("opaque family %s omitted from committed or HEAD/current states", name)
		}
	}
	return states
}

// A physical 72/28 store distinguishes valid cross-plane ancestry from a
// genuinely missing parent. The CLI must publish only the former.
func TestPublicNativeDeltaCrossPlaneCounterParent(t *testing.T) {
	binary := diagnosticBinary(t)
	for _, present := range []bool{true, false} {
		t.Run(fmt.Sprintf("parent_present_%t", present), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			dataDir, home := diagnosticDir(t), diagnosticDir(t)
			db, closeDriver, err := openPublicMigrationFixture(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDriver()
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ownVersionExec(t, ctx, conn, "CREATE DATABASE public_counter_parent")
			ownVersionExec(t, ctx, conn, "USE public_counter_parent")
			if _, err := schema.MigrateUpTo(ctx, conn, 72); err != nil {
				t.Fatal(err)
			}
			for _, name := range ownVersionUnversioned {
				ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore (pattern,ignored) VALUES (?,true) ON DUPLICATE KEY UPDATE ignored=true", name)
			}
			ownVersionExec(t, ctx, conn, "CREATE TABLE IF NOT EXISTS ignored_schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64))")
			for i, name := range ownVersionIgnoredMigrations {
				query, err := schema.IgnoredMigrationSQL(name)
				if err != nil {
					t.Fatal(err)
				}
				ownVersionExec(t, ctx, conn, query)
				ownVersionExec(t, ctx, conn, "INSERT INTO ignored_schema_migrations(version,content_hash) VALUES (?,?)", i+1, fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
			}
			ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
			ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,created_by) VALUES ('public-w1.3','durable child of active parent','','','','','public')")
			if present {
				ownVersionExec(t, ctx, conn, "INSERT INTO wisps (id,title,description,design,acceptance_criteria,notes,created_by) VALUES ('public-w1','active-only parent','','','','','public')")
				ownVersionExec(t, ctx, conn, "INSERT INTO wisp_child_counters (parent_id,last_child) VALUES ('public-w1',17)")
			}
			head := ownVersionCommit(t, ctx, conn)
			before, err := physicalcapture.ReadWorkingRoot(ctx, conn, head)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := closeDriver(); err != nil {
				t.Fatal(err)
			}
			workspace, output := diagnosticDir(t), diagnosticDir(t)
			beadsDir := filepath.Join(workspace, ".beads")
			if err := os.Mkdir(beadsDir, 0700); err != nil {
				t.Fatal(err)
			}
			environment := []string{}
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if key == "HOME" || key == "DOLT_ROOT_PATH" || strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "XDG_") {
					continue
				}
				environment = append(environment, entry)
			}
			environment = append(environment, "HOME="+home, "DOLT_ROOT_PATH="+home, "BEADS_DIR="+beadsDir, "XDG_CONFIG_HOME="+home, "BD_DISABLE_METRICS=1", "BD_DISABLE_EVENT_FLUSH=1")
			command := exec.CommandContext(ctx, binary, "--disposable", "--native-delta", "--data-dir", dataDir, "--database", "public_counter_parent", "--beads-dir", beadsDir, "--output-dir", output)
			command.Dir, command.Env = workspace, environment
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			err = command.Run()
			if present {
				if err != nil {
					t.Fatalf("valid active parent refused: %v stderr=%s", err, stderr.Bytes())
				}
				raw, err := os.ReadFile(filepath.Join(output, "capture-set/head.tar"))
				if err != nil {
					t.Fatal(err)
				}
				manifest, err := plugdump.ValidateNativeDeltaHead(bytes.NewReader(raw))
				if err != nil || len(manifest.Projects) != 1 || manifest.Projects[0].ChildCounters["public-w1"] != 17 {
					t.Fatal("active counter changed", err)
				}
				var set plugdump.NativeCaptureSet
				root, err := os.ReadFile(filepath.Join(output, "capture-set/capture-set.json"))
				if err != nil || json.Unmarshal(root, &set) != nil || set.Head != head {
					t.Fatal("native set absent or different", err)
				}
				if len(deltaFixtureStream(t, filepath.Join(output, "capture-set"), set)) == 0 {
					t.Fatal("authenticated native delta stream missing")
				}
				var childExact, counterExact bool
				reader := tar.NewReader(bytes.NewReader(raw))
				for {
					header, err := reader.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
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
						if record.Source.Table == "issues" {
							childExact = record.ID == "public-w1.3" && record.Source.Fields["id"] == "public-w1.3"
						}
						if record.Source.Table == "wisp_child_counters" {
							counterExact = record.Source.Fields["parent_id"] == "public-w1" && fmt.Sprint(record.Source.Fields["last_child"]) == "17"
						}
					}
				}
				if !childExact || !counterExact {
					t.Fatal("physical child's ID or parent's raw counter changed")
				}
			} else {
				if err == nil || stdout.Len() != 0 {
					t.Fatal("missing parent published success")
				}
				scanner := bufio.NewScanner(bytes.NewReader(stderr.Bytes()))
				var refusal safeCaptureReport
				for scanner.Scan() {
					if err := json.Unmarshal(scanner.Bytes(), &refusal); err != nil {
						t.Fatal(err)
					}
				}
				if scanner.Err() != nil || refusal.ReasonSubcode == nil || *refusal.ReasonSubcode != "child_counter_parent_unresolved" || refusal.HistoryBoundaryReason != "" || refusal.CommitSchemaVersion == nil || *refusal.CommitSchemaVersion != 72 || refusal.CommitOrdinalFromHead == nil || *refusal.CommitOrdinalFromHead != 1 {
					t.Fatalf("missing parent lost typed coordinates: %s", stderr.Bytes())
				}
				entries, err := os.ReadDir(output)
				if err != nil || len(entries) != 0 {
					t.Fatal("refusal left partial output", err)
				}
			}
			reopened, closeReopened, err := physicalcapture.Open(dataDir, "public_counter_parent")
			if err != nil {
				t.Fatal(err)
			}
			defer closeReopened()
			defer reopened.Close()
			readback, err := reopened.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer readback.Close()
			after, err := physicalcapture.ReadWorkingRoot(ctx, readback, head)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("capture changed complete physical source on reopen", err)
			}
			facts, err := json.Marshal(map[string]any{"parent_present": present, "child": "public-w1.3", "parent_counter": 17, "before": before, "after": after, "stdout": stdout.String(), "stderr": stderr.String()})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), fmt.Sprintf("public-cross-plane-counter-%t.json", present)), facts, 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
}
