package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type freshSQLRows struct {
	Query     string      `json:"query"`
	Arguments []any       `json:"arguments"`
	Columns   []string    `json:"columns"`
	Types     []string    `json:"database_types"`
	Rows      [][]*string `json:"rows"`
}

func freshContinuationEnvironment(t *testing.T) []string {
	data := publicNativeProtocolInput(t, "tools/roundtrip/testdata/fresh-native/continuation-fields.json", freshFieldProfileSHA)
	var profile map[string]json.RawMessage
	var environment []string
	if strictTypedJSON(data, &profile) != nil || strictTypedJSON(profile["native_continuation_environment"], &environment) != nil || len(environment) != 5 {
		t.Fatal("native continuation environment profile differs")
	}
	return environment
}

func freshNativeJournalTable(t *testing.T) string {
	data := publicNativeProtocolInput(t, "tools/roundtrip/testdata/fresh-native/continuation-fields.json", freshFieldProfileSHA)
	var profile map[string]any
	if strictTypedJSON(data, &profile) != nil {
		t.Fatal("immutable native journal profile invalid")
	}
	name, ok := profile["native_journal_table"].(string)
	if !ok || name == "" {
		t.Fatal("native journal table missing")
	}
	for _, r := range name {
		if r != '_' && (r < 'a' || r > 'z') {
			t.Fatal("native journal table invalid")
		}
	}
	return name
}

// Keep SQL spelling and NULL separate in the evidence. Numeric interpretation
// is confined to the specifically named allocator facts below.
func freshSQL(t *testing.T, db *sql.DB, query string, args ...any) freshSQLRows {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	result := freshSQLRows{Query: query, Arguments: args, Columns: columns, Rows: [][]*string{}}
	for _, column := range types {
		result.Types = append(result.Types, column.DatabaseTypeName())
	}
	for rows.Next() {
		cells := make([]sql.RawBytes, len(columns))
		dest := make([]any, len(columns))
		for i := range cells {
			dest[i] = &cells[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		values := make([]*string, len(cells))
		for i, cell := range cells {
			if cell != nil {
				value := string(cell)
				values[i] = &value
			}
		}
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func freshNativeTables(t *testing.T) []string {
	data := publicNativeProtocolInput(t, "tools/roundtrip/testdata/fresh-native/continuation-fields.json", freshFieldProfileSHA)
	var profile struct {
		Format      string   `json:"format"`
		Commit      string   `json:"source_commit"`
		Zero        []string `json:"zero_create_fields"`
		Tables      []string `json:"native_issue_tables"`
		Journal     string   `json:"native_journal_table"`
		Environment []string `json:"native_continuation_environment"`
	}
	if strictTypedJSON(data, &profile) != nil || profile.Format != "public-id-style-a-field-profile/1" || profile.Commit != freshNativeSource.Commit || len(profile.Tables) != 2 {
		t.Fatal("native issue census profile differs")
	}
	for _, name := range profile.Tables {
		for _, r := range name {
			if r < 'a' || r > 'z' {
				t.Fatal("invalid immutable SQL table name")
			}
		}
	}
	return profile.Tables
}

func freshReadNativeFacts(t *testing.T, server *freshNativeServer, evidence, label string) freshNativeFacts {
	t.Helper()
	queries := []freshSQLRows{}
	config := freshSQL(t, server.db, "SELECT `key`, value FROM config ORDER BY `key`")
	queries = append(queries, config)
	settings := map[string]string{}
	for _, row := range config.Rows {
		if len(row) != 2 || row[0] == nil || row[1] == nil {
			t.Fatal("native config row incomplete")
		}
		if _, exists := settings[*row[0]]; exists {
			t.Fatal("native config key repeats")
		}
		settings[*row[0]] = *row[1]
	}
	prefix := settings["issue_prefix"]
	if prefix != "demo" || settings["issue_id_mode"] != "" && settings["issue_id_mode"] != "hash" {
		t.Fatal("fresh public native project/hash policy differs")
	}
	min, max, probability := uint32(3), uint32(8), 0.25
	for key, destination := range map[string]*uint32{"min_hash_length": &min, "max_hash_length": &max} {
		if raw := settings[key]; raw != "" {
			n, err := strconv.ParseUint(raw, 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			*destination = uint32(n)
		}
	}
	if raw := settings["max_collision_prob"]; raw != "" {
		n, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatal(err)
		}
		probability = n
	}
	facts := freshNativeFacts{Allocator: driver.OfflineAllocator{Prefix: prefix, Mode: "hash", HashLength: max, MinHashLength: min, MaxHashLength: max, MaxCollisionProb: probability}, IDs: []string{}}
	counter := freshSQL(t, server.db, "SELECT prefix, last_id FROM issue_counter ORDER BY prefix")
	queries = append(queries, counter)
	for _, row := range counter.Rows {
		if len(row) != 2 || row[0] == nil || row[1] == nil || *row[0] != prefix {
			t.Fatal("unexpected native project counter")
		}
		n, err := strconv.ParseUint(*row[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		facts.Allocator.LastID = n
	}
	children := freshSQL(t, server.db, "SELECT parent_id, last_child FROM child_counters ORDER BY parent_id")
	queries = append(queries, children)
	for _, row := range children.Rows {
		if len(row) != 2 || row[0] == nil || row[1] == nil {
			t.Fatal("native child counter incomplete")
		}
		n, err := strconv.ParseUint(*row[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if facts.Allocator.Children == nil {
			facts.Allocator.Children = map[string]uint64{}
		}
		facts.Allocator.Children[*row[0]] = n
	}
	for index, table := range freshNativeTables(t) {
		ids := freshSQL(t, server.db, "SELECT id FROM `"+table+"` ORDER BY id")
		queries = append(queries, ids)
		for _, row := range ids.Rows {
			if len(row) != 1 || row[0] == nil {
				t.Fatal("native occupancy ID missing")
			}
			id := *row[0]
			facts.IDs = append(facts.IDs, id)
			if index == 0 && strings.HasPrefix(id, prefix+"-") && !strings.Contains(strings.TrimPrefix(id, prefix+"-"), ".") {
				facts.RootCount++
			}
		}
	}
	sort.Strings(facts.IDs)
	if facts.Allocator.Validate() != nil || len(facts.IDs) == 0 || facts.RootCount == 0 {
		t.Fatal("fresh native allocator facts invalid")
	}
	freshJSON(t, filepath.Join(evidence, label+"-sql.json"), queries)
	freshJSON(t, filepath.Join(evidence, label+"-facts.json"), facts)
	return facts
}

func freshNativeContinuation(t *testing.T, item freshCase, evidence, workspace, mutableRoot, nativeCLI, captureSHA, producerSHA string, firstSequence json.RawMessage, result freshResult) freshNativeRequest {
	t.Helper()
	server := freshStartNativeServer(t, mutableRoot, evidence, workspace)
	before := freshReadNativeFacts(t, server, evidence, "initial")
	parent := ""
	for _, id := range before.IDs {
		if strings.HasPrefix(id, "demo-") && !strings.Contains(id, ".") {
			parent = id
			break
		}
	}
	if parent == "" {
		t.Fatal("restored native parent absent")
	}
	request := freshNativeRequest{Format: "public-id-style-a-same-run/1", RunID: os.Getenv("GITHUB_RUN_ID"), Case: item.Engine + "/" + item.Name, Engine: item.Engine, Mode: item.Mode, CaptureSHA256: captureSHA, RecipeExportSHA256: result.RecipeExportSHA256, NativeCLISHA256: freshFileSHA(t, nativeCLI), ProducerSHA256: producerSHA, Items: []freshNativeItem{}, Comments: []freshNativeComment{}, Operations: []freshNativeOperation{}}
	request.FirstSequence = firstSequence
	actor := model.PrincipalRef{Kind: "user", ID: "fresh-native"}
	env := append(freshChildEnv(workspace), "BEADS_DOLT_SERVER_MODE=1", "BEADS_DOLT_SERVER_HOST=127.0.0.1", "BEADS_DOLT_SERVER_PORT="+strconv.Itoa(server.port), "BEADS_DOLT_SERVER_DATABASE=beads_test", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1")
	env = append(env, freshContinuationEnvironment(t)...)
	rootID := ""
	for index, kind := range []string{"root", "child", "comment", "comment", "child"} {
		freshGuard(t)
		op := freshNativeOperation{Kind: kind, Actor: actor, NativeActor: "user:fresh-native", Project: "demo", Before: before.Allocator}
		args := []string{"--actor", op.NativeActor}
		if kind == "comment" {
			op.Target = rootID
			op.Text = fmt.Sprintf("Public %s continuation comment %d", item.Engine, index)
			args = append(args, "comments", "add", op.Target, op.Text, "--author", op.NativeActor, "--json")
			// Avoid native's intentional forward skew for a same-second burst;
			// each original comment clock must fit its actual observation window.
			if len(request.Comments) > 0 {
				next := request.Comments[len(request.Comments)-1].Time.Add(time.Second)
				if delay := time.Until(next); delay > 0 {
					time.Sleep(delay)
				}
			}
		} else {
			op.Fields = map[string]any{"title": fmt.Sprintf("Public %s continuation %s %d", item.Engine, kind, index), "description": "Fresh same-run public native continuation", "priority": json.Number("2"), "owner": "public@example.invalid"}
			args = append(args, "create", "--title", op.Fields["title"].(string), "--description", op.Fields["description"].(string), "--priority", "2", "--type", "task", "--status", "open")
			if kind == "child" {
				op.Parent = parent
				args = append(args, "--parent", parent)
			}
			args = append(args, "--json")
		}
		op.NativeRequest = args
		op.Window.Started = time.Now().UTC()
		raw, err := freshCommand(t, evidence, fmt.Sprintf("native-operation-%02d", index), workspace, nativeCLI, args, env, 3*time.Minute)
		op.Window.Ended = time.Now().UTC()
		if err != nil {
			t.Fatalf("native original operation %d: %v", index, err)
		}
		op.NativeResponse = json.RawMessage(raw)
		var response map[string]any
		if err := strictTypedJSON(raw, &response); err != nil {
			t.Fatal(err)
		}
		clock, ok := response["created_at"].(string)
		if !ok {
			t.Fatal("native original timestamp absent")
		}
		op.Clock, err = time.Parse(time.RFC3339Nano, clock)
		if err != nil {
			t.Fatal(err)
		}
		after := freshReadNativeFacts(t, server, evidence, fmt.Sprintf("after-%02d", index))
		op.After = after.Allocator
		if kind == "comment" {
			request.Comments = append(request.Comments, freshNativeComment{Item: rootID, Body: op.Text, Author: op.NativeActor, Time: op.Clock})
		} else {
			op.ExpectedID, ok = response["id"].(string)
			if !ok || op.ExpectedID == "" {
				t.Fatal("native create returned no ID")
			}
			op.DerivedID, err = freshDerivedID(before, op)
			if err != nil || op.DerivedID != op.ExpectedID {
				t.Fatalf("independent native ID derivation differs: %s/%s %v", op.DerivedID, op.ExpectedID, err)
			}
			// Retain every journal row for this new identity. The consumer
			// validates the complete create/parent/label sequence, including
			// any unexpected intervening item mutation.
			journal := freshSQL(t, server.db, "SELECT * FROM `"+freshNativeJournalTable(t)+"` WHERE issue_id = ? ORDER BY seq", op.ExpectedID)
			op.NativeCreateJournal = freshJSONBytes(t, journal)
			freshJSON(t, filepath.Join(evidence, fmt.Sprintf("native-create-%02d-journal.json", index)), journal)
			if kind == "root" {
				rootID = op.ExpectedID
			}
			request.Items = append(request.Items, freshNativeItem{Document: freshExpectedItem(op), Native: op.NativeResponse})
			// Retain actual native SQL spelling separately from the full-precision
			// original CLI timestamp used in the content hash/operation WithClock.
			freshJSON(t, filepath.Join(evidence, fmt.Sprintf("native-item-%02d-sql.json", index)), freshSQL(t, server.db, "SELECT * FROM issues WHERE id = ?", op.ExpectedID))
		}
		request.Operations = append(request.Operations, op)
		before = after
	}
	freshJSON(t, filepath.Join(evidence, "native-comments-final-sql.json"), freshSQL(t, server.db, "SELECT * FROM comments WHERE issue_id = ? ORDER BY created_at, id", rootID))
	if err := server.stop(t); err != nil {
		t.Fatal(err)
	}
	freshJSON(t, filepath.Join(evidence, "native-continuation-request.json"), request)
	return request
}
