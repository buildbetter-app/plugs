package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b06NativeWorkspace struct {
	dir                   string
	env                   []string
	ephemeralTable        string
	ephemeralDependencies string
	ephemeralEvents       string
	targetColumns         []string
}
type b06NativeRecorder struct {
	*nativeRecorder
	serial int
}
type b06NativeCycle struct {
	Members []struct {
		ID    string          `json:"id"`
		Issue json.RawMessage `json:"issue"`
	} `json:"members"`
	Partial bool `json:"partial"`
}

func (r *b06NativeRecorder) command(w b06NativeWorkspace, label, binary string, args []string, fail bool) []byte {
	r.t.Helper()
	r.serial++
	label = fmt.Sprintf("%03d-%s", r.serial, label)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = w.dir
	cmd.Env = append([]string(nil), w.env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -2
		var e *exec.ExitError
		if errors.As(err, &e) {
			exit = e.ExitCode()
		}
	}
	for suffix, data := range map[string][]byte{"stdout": stdout.Bytes(), "stderr": stderr.Bytes()} {
		if err := os.WriteFile(filepath.Join(r.root, label+"."+suffix), data, 0600); err != nil {
			r.t.Fatal(err)
		}
	}
	r.writeJSON(filepath.Join(r.root, label+".request.json"), map[string]any{"argv": cmd.Args, "directory": w.dir, "environment": w.env, "exit": exit, "error": fmt.Sprint(err), "timeout": ctx.Err() != nil, "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if ctx.Err() != nil || exit < 0 || (fail && exit == 0) || (!fail && exit != 0) {
		r.t.Fatalf("%s exit=%d: %v\nstdout=%s\nstderr=%s", label, exit, err, stdout.Bytes(), stderr.Bytes())
	}
	if fail {
		return append(append([]byte(nil), stdout.Bytes()...), stderr.Bytes()...)
	}
	return stdout.Bytes()
}

func (r *b06NativeRecorder) workspace(binary string, server *publicnative.Server, database string) b06NativeWorkspace {
	r.t.Helper()
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0700); err != nil {
		r.t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp/plug-swarm/work", "b06-native-")
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			r.t.Error(err)
		}
	})
	for _, name := range []string{"home", "project", "tmp"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			r.t.Fatal(err)
		}
	}
	base := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + filepath.Join(root, "home"), "USER=b06-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B06 Public", "GIT_AUTHOR_EMAIL=b06@example.invalid", "GIT_COMMITTER_NAME=B06 Public", "GIT_COMMITTER_EMAIL=b06@example.invalid", "TMPDIR=" + filepath.Join(root, "tmp"), "BEADS_ACTOR=b06-recorder", "BEADS_NO_DAEMON=1"}
	w := b06NativeWorkspace{dir: filepath.Join(root, "project"), env: server.ChildEnv(base)}
	r.command(w, "git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="}, false)
	version := r.command(w, "version", binary, []string{"version", "--json"}, false)
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		r.t.Fatalf("linked native source/build mismatch %s", version)
	}
	r.command(w, "init", binary, []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b06", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, false)
	server.RequireDatabaseDir(r.t, database)
	// Bind native graph roles from actual schema metadata. The canonical names
	// remain intact in requests and raw results, including both target planes.
	foreignQuery := "SELECT table_name AS family, column_name AS field, referenced_table_name AS source_family FROM information_schema.key_column_usage WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL AND table_name IN (SELECT table_name FROM information_schema.columns WHERE table_schema = DATABASE() AND column_name = 'depends_on_issue_id') ORDER BY table_name, column_name"
	foreignRows := r.rows(w, binary, database+"-source-foreign-keys", foreignQuery)
	foreign := map[string]map[string]string{}
	text := func(row map[string]any, name string) string {
		for key, value := range row {
			if strings.EqualFold(key, name) {
				if value, ok := value.(string); ok && value != "" {
					return value
				}
			}
		}
		r.t.Fatalf("missing native catalog field %s: %+v", name, row)
		return ""
	}
	for _, row := range foreignRows {
		family, field, source := text(row, "family"), text(row, "field"), text(row, "source_family")
		if foreign[family] == nil {
			foreign[family] = map[string]string{}
		}
		if foreign[family][field] != "" {
			r.t.Fatalf("ambiguous native foreign key: %+v", row)
		}
		foreign[family][field] = source
	}
	columnQuery := "SELECT table_name AS family, column_name AS field FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name NOT LIKE 'dolt_%' AND (column_name LIKE 'depends_on_%' OR column_name = 'event_type') ORDER BY table_name, ordinal_position"
	columnRows := r.rows(w, binary, database+"-source-columns", columnQuery)
	targets, events := map[string][]string{}, map[string]bool{}
	for _, row := range columnRows {
		family, field := text(row, "family"), text(row, "field")
		if field == "event_type" {
			events[family] = true
		} else if foreign[family]["issue_id"] != "" {
			targets[family] = append(targets[family], field)
		}
	}
	if len(targets) != 2 || foreign["dependencies"]["issue_id"] != "issues" || len(targets["dependencies"]) != 3 || len(events) != 2 || !events["events"] {
		r.t.Fatal("native graph/event catalog does not identify the two canonical planes")
	}
	w.targetColumns = targets["dependencies"]
	for family, fields := range targets {
		if family == "dependencies" {
			continue
		}
		if !reflect.DeepEqual(fields, w.targetColumns) {
			r.t.Fatal("native dependency target arms differ between planes")
		}
		w.ephemeralDependencies, w.ephemeralTable = family, foreign[family]["issue_id"]
	}
	for family := range events {
		if family != "events" {
			w.ephemeralEvents = family
		}
	}
	otherTargets := 0
	for _, field := range w.targetColumns {
		if field != "depends_on_issue_id" && field != "depends_on_external" {
			otherTargets++
			if foreign[w.ephemeralDependencies][field] != w.ephemeralTable {
				r.t.Fatal("native ephemeral target lacks its actual source foreign key")
			}
		}
	}
	if otherTargets != 1 || w.ephemeralTable == "issues" {
		r.t.Fatal("native target planes are not distinct")
	}
	families := map[string]bool{}
	for _, family := range []string{"issues", w.ephemeralTable, "dependencies", w.ephemeralDependencies, "events", w.ephemeralEvents} {
		if families[family] {
			r.t.Fatal("native six graph/audit families are not distinct")
		}
		families[family] = true
	}
	for _, name := range append([]string{w.ephemeralTable, w.ephemeralDependencies, w.ephemeralEvents}, w.targetColumns...) {
		if name == "" {
			r.t.Fatal("native graph family is absent")
		}
		for _, c := range name {
			if c != '_' && (c < 'a' || c > 'z') {
				r.t.Fatal("unsafe native catalog identifier")
			}
		}
	}
	r.writeJSON(filepath.Join(r.root, database+"-source-table-binding.json"), map[string]any{"foreign_query": foreignQuery, "column_query": columnQuery, "raw_foreign_keys": foreignRows, "raw_columns": columnRows, "source_table": w.ephemeralTable, "dependency_table": w.ephemeralDependencies, "event_table": w.ephemeralEvents, "target_columns": w.targetColumns})
	return w
}

func (r *b06NativeRecorder) create(w b06NativeWorkspace, binary, id string, ephemeral bool) {
	args := []string{"create", "--id", id, "--title", id, "--json"}
	if ephemeral {
		args = append(args, "--ephemeral")
	}
	r.command(w, "create-"+id, binary, args, false)
}

func (r *b06NativeRecorder) edge(w b06NativeWorkspace, binary string, e b06Edge, fail bool) []byte {
	return r.command(w, "edge-"+e.From+"-"+e.To+"-"+e.Kind, binary, []string{"dep", "add", e.From, e.To, "--type", e.Kind, "--json"}, fail)
}

func (r *b06NativeRecorder) rows(w b06NativeWorkspace, binary, label, query string) []map[string]any {
	data := r.command(w, label, binary, []string{"sql", query, "--json"}, false)
	var rows []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&rows); err != nil {
		r.t.Fatalf("SQL rows %s: %v %s", label, err, data)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		r.t.Fatalf("SQL rows %s have trailing data: %v", label, err)
	}
	return rows
}

func (r *b06NativeRecorder) image(w b06NativeWorkspace, binary, label string) map[string][]map[string]any {
	image := map[string][]map[string]any{}
	for _, table := range []string{"issues", w.ephemeralTable, "dependencies", w.ephemeralDependencies, "events", w.ephemeralEvents} {
		image[table] = r.rows(w, binary, label+"-"+table, "SELECT * FROM "+table+" ORDER BY id")
	}
	r.writeJSON(filepath.Join(r.root, label+"-image.json"), image)
	return image
}

func (r *b06NativeRecorder) cycles(w b06NativeWorkspace, binary, label string, tracks bool) []b06NativeCycle {
	before := r.image(w, binary, label+"-before")
	args := []string{"dep", "cycles", "--json", "--limit", "1"}
	if tracks {
		args = append(args, "--include-tracks")
	}
	raw := r.command(w, label, binary, args, false)
	var cycles []b06NativeCycle
	if err := json.Unmarshal(raw, &cycles); err != nil || cycles == nil {
		r.t.Fatalf("native report %s %v", raw, err)
	}
	if !reflect.DeepEqual(before, r.image(w, binary, label+"-after")) {
		r.t.Fatal("native read changed source graph/audit")
	}
	return cycles
}

func b06NativePaths(cycles []b06NativeCycle) [][]string {
	paths := make([][]string, 0, len(cycles))
	for _, c := range cycles {
		path := make([]string, 0, len(c.Members))
		for _, m := range c.Members {
			path = append(path, m.ID)
		}
		paths = append(paths, path)
	}
	return paths
}

func (r *b06NativeRecorder) requirePaths(cycles []b06NativeCycle, want [][]string) {
	r.t.Helper()
	if !reflect.DeepEqual(b06NativePaths(cycles), want) {
		r.t.Fatalf("native raw path/order/count=%v want=%v", b06NativePaths(cycles), want)
	}
	for _, c := range cycles {
		if c.Partial {
			r.t.Fatal("unexpected native partial")
		}
		for _, m := range c.Members {
			if len(m.Issue) == 0 || bytes.Equal(m.Issue, []byte("null")) {
				r.t.Fatal("native member not hydrated")
			}
		}
	}
}

func (r *b06NativeRecorder) refuse(w b06NativeWorkspace, binary string, e b06Edge, fragment string) {
	before := r.image(w, binary, "refusal-before-"+strconv.Itoa(r.serial))
	raw := r.edge(w, binary, e, true)
	if !strings.Contains(strings.ToLower(string(raw)), fragment) {
		r.t.Fatalf("wrong native refusal %s", raw)
	}
	if !reflect.DeepEqual(before, r.image(w, binary, "refusal-after-"+strconv.Itoa(r.serial))) {
		r.t.Fatal("native refusal changed graph/audit")
	}
}

// Hierarchy refusals are structured canonical errors, not diagnostics that
// promise a generic keyword. Pin both endpoint directions and the exact reason
// from the unchanged native issueops.DependencyHierarchyConflictError.
func (r *b06NativeRecorder) refuseHierarchy(w b06NativeWorkspace, binary string, edge b06Edge, expected string) {
	r.t.Helper()
	label := "hierarchy-refusal-" + strconv.Itoa(r.serial)
	before := r.image(w, binary, label+"-before")
	history := r.rows(w, binary, label+"-history-before", "SELECT commit_hash FROM dolt_log ORDER BY commit_hash")
	raw := r.edge(w, binary, edge, true)
	var got struct {
		Error         string `json:"error"`
		SchemaVersion int    `json:"schema_version"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&got); err != nil {
		r.t.Fatalf("invalid structured hierarchy refusal: %v %s", err, raw)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		r.t.Fatalf("trailing hierarchy refusal output: %v %s", err, raw)
	}
	if got.SchemaVersion != 1 || got.Error != expected {
		r.t.Fatalf("wrong structured hierarchy refusal: %+v want=%q", got, expected)
	}
	after := r.image(w, binary, label+"-after")
	afterHistory := r.rows(w, binary, label+"-history-after", "SELECT commit_hash FROM dolt_log ORDER BY commit_hash")
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(history, afterHistory) {
		r.t.Fatal("hierarchy refusal changed graph/audit/history")
	}
	r.writeJSON(filepath.Join(r.root, label+"-conservation.json"), map[string]any{"edge": edge, "expected_error": expected, "actual_response": json.RawMessage(raw), "before": before, "after": after, "before_history": history, "after_history": afterHistory, "graph_audit_history": "exactly unchanged"})
}

// Deliberate disposable SQL diagnostics are needed because ordinary writes
// correctly refuse scheduling cycles and conflicting endpoint kinds. Raw DDL,
// rows, SQL requests and unmodified canonical detector output are retained.
func (r *b06NativeRecorder) parallel(w b06NativeWorkspace, binary string) {
	for _, id := range []string{"b06-parallel-b", "b06-parallel-a"} {
		r.create(w, binary, id, false)
	}
	schema := r.rows(w, binary, "parallel-original-schema", "SHOW CREATE TABLE dependencies")
	r.writeJSON(filepath.Join(r.root, "parallel-diagnostic-origin.json"), map[string]any{"setup": "SQL corruption of disposable native graph only; no ordinary admission claim", "original_schema": schema, "raw_edges": b06ParallelEdges()})
	// Only the per-endpoint unique index is relaxed in this disposable database.
	r.command(w, "parallel-drop-endpoint-index", binary, []string{"sql", "ALTER TABLE dependencies DROP INDEX uk_dep_issue_target", "--json"}, false)
	for i, e := range b06ParallelEdges() {
		q := fmt.Sprintf("INSERT INTO dependencies (id,issue_id,depends_on_issue_id,type,created_by) VALUES ('b06-diagnostic-%d','%s','%s','%s','b06-public-diagnostic')", i, e.From, e.To, e.Kind)
		r.command(w, "parallel-insert-"+strconv.Itoa(i), binary, []string{"sql", q, "--json"}, false)
	}
	rows := r.rows(w, binary, "parallel-multiplicity", "SELECT issue_id,depends_on_issue_id,type FROM dependencies WHERE issue_id LIKE 'b06-parallel-%' ORDER BY issue_id,type,id")
	if len(rows) != 4 {
		r.t.Fatalf("native actual parallel multiplicity=%d want4", len(rows))
	}
	counts := map[string]int{}
	for _, row := range rows {
		counts[fmt.Sprint(row["issue_id"])+"/"+fmt.Sprint(row["depends_on_issue_id"])+"/"+fmt.Sprint(row["type"])]++
	}
	if counts["b06-parallel-a/b06-parallel-b/blocks"] != 2 || counts["b06-parallel-a/b06-parallel-b/tracks"] != 1 || counts["b06-parallel-b/b06-parallel-a/blocks"] != 1 {
		r.t.Fatalf("actual duplicate graph differs %+v", rows)
	}
}

func (r *b06NativeRecorder) core(binary string, server *publicnative.Server) [][]string {
	w := r.workspace(binary, server, "b06_core")
	expectedEdges := []b06Edge{}
	for _, c := range b06CycleCases() {
		for _, id := range c.IDs {
			r.create(w, binary, id, false)
		}
		for _, e := range c.Edges {
			r.edge(w, binary, e, false)
		}
		expectedEdges = append(expectedEdges, c.Edges...)
		r.requireEdges(w, binary, c.Name, expectedEdges)
		r.refuse(w, binary, b06Edge{c.IDs[0], c.IDs[0], "tracks"}, "itself")
		if c.Name == "pure-tracks-loop" {
			r.requirePaths(r.cycles(w, binary, "pure-default", false), [][]string{})
			r.requirePaths(r.cycles(w, binary, "pure-tracks", true), [][]string{})
		}
	}
	edge := b06CycleCases()[0].Edges[0]
	beforeDuplicate := r.image(w, binary, "before-ordinary-duplicate")
	beforeHistory := r.rows(w, binary, "before-duplicate-history", "SELECT commit_hash FROM dolt_log ORDER BY commit_hash")
	r.edge(w, binary, edge, false)
	afterDuplicate := r.image(w, binary, "after-ordinary-duplicate")
	afterHistory := r.rows(w, binary, "after-duplicate-history", "SELECT commit_hash FROM dolt_log ORDER BY commit_hash")
	if !reflect.DeepEqual(beforeDuplicate, afterDuplicate) || !reflect.DeepEqual(beforeHistory, afterHistory) {
		r.t.Fatal("ordinary duplicate changed native graph/audit/history")
	}
	r.writeJSON(filepath.Join(r.root, "ordinary-duplicate-conservation.json"), map[string]any{"before": beforeDuplicate, "after": afterDuplicate, "before_commits": beforeHistory, "after_commits": afterHistory, "graph_audit_history": "exactly unchanged"})
	r.refuse(w, binary, b06Edge{edge.From, edge.To, "blocks"}, "type")
	for _, id := range []string{"b06-schedule-a", "b06-schedule-b"} {
		r.create(w, binary, id, false)
	}
	r.edge(w, binary, b06Edge{"b06-schedule-a", "b06-schedule-b", "blocks"}, false)
	expectedEdges = append(expectedEdges, b06Edge{"b06-schedule-a", "b06-schedule-b", "blocks"})
	r.refuse(w, binary, b06Edge{"b06-schedule-b", "b06-schedule-a", "blocks"}, "cycle")
	r.requireEdges(w, binary, "ordinary", expectedEdges)
	r.requirePaths(r.cycles(w, binary, "ordinary-default", false), [][]string{})
	r.requirePaths(r.cycles(w, binary, "ordinary-tracks", true), b06ExpectedPaths(false))
	r.parallel(w, binary)
	expectedEdges = append(expectedEdges, b06ParallelEdges()...)
	r.requireEdges(w, binary, "parallel", expectedEdges)
	r.requirePaths(r.cycles(w, binary, "parallel-default", false), [][]string{{"b06-parallel-a", "b06-parallel-b"}})
	final := r.cycles(w, binary, "parallel-tracks", true)
	r.requirePaths(final, b06ExpectedPaths(true))
	again := r.cycles(w, binary, "parallel-tracks-repeat", true)
	if !reflect.DeepEqual(final, again) {
		r.t.Fatal("unchanged native report order/images nondeterministic")
	}
	r.t.Log("B06_NATIVE durable pure_tracks=excluded mixed=reported self=refused scheduling=refused duplicate_edges=4 parallel_cycles=1 report_order=raw_exact")
	return b06NativePaths(final)
}

func (r *b06NativeRecorder) requireEdges(w b06NativeWorkspace, binary, label string, expected []b06Edge) {
	r.t.Helper()
	image := r.image(w, binary, label+"-edge-census")
	actual := []b06Edge{}
	for _, table := range []string{"dependencies", w.ephemeralDependencies} {
		for _, row := range image[table] {
			var target string
			for _, key := range w.targetColumns {
				if row[key] != nil {
					if target != "" {
						r.t.Fatal("native raw edge has multiple target arms")
					}
					target = fmt.Sprint(row[key])
				}
			}
			if target == "" {
				r.t.Fatal("native raw edge has no typed target")
			}
			actual = append(actual, b06Edge{fmt.Sprint(row["issue_id"]), target, fmt.Sprint(row["type"])})
		}
	}
	r.writeJSON(filepath.Join(r.root, label+"-edge-census.json"), map[string]any{"raw_edges": actual, "expected_edges": expected, "actual_edge_count": len(actual), "raw_source_image": image})
	b06RequireEdges(r.t, actual, expected)
}

func (r *b06NativeRecorder) missing(binary string, server *publicnative.Server) {
	w := r.workspace(binary, server, "b06_missing")
	for _, id := range []string{"b06-missing-c", "b06-missing-a", "b06-missing-b"} {
		r.create(w, binary, id, id == "b06-missing-b")
	}
	for _, e := range []b06Edge{{"b06-missing-a", "b06-missing-b", "blocks"}, {"b06-missing-b", "b06-missing-c", "tracks"}, {"b06-missing-c", "b06-missing-a", "tracks"}} {
		r.edge(w, binary, e, false)
	}
	r.refuse(w, binary, b06Edge{"b06-missing-b", "b06-missing-b", "tracks"}, "itself")
	r.refuse(w, binary, b06Edge{"b06-missing-b", "b06-missing-a", "blocks"}, "cycle")
	r.requirePaths(r.cycles(w, binary, "mixed-plane-default", false), [][]string{})
	want := [][]string{{"b06-missing-a", "b06-missing-b", "b06-missing-c"}}
	r.requirePaths(r.cycles(w, binary, "mixed-plane-tracks", true), want)
	before := r.image(w, binary, "missing-complete")
	// The existing FK cascades would honestly remove topology on deletion.
	// Drop only dependency-family FKs in this explicit damaged-database control,
	// so absent hydration and preserved edges are independently observable.
	constraint := regexp.MustCompile("CONSTRAINT `([^`]+)` FOREIGN KEY")
	for _, table := range []string{"dependencies", w.ephemeralDependencies} {
		rows := r.rows(w, binary, "missing-schema-"+table, "SHOW CREATE TABLE "+table)
		var ddl string
		for _, row := range rows {
			for key, value := range row {
				if strings.EqualFold(key, "Create Table") {
					ddl = fmt.Sprint(value)
				}
			}
		}
		matches := constraint.FindAllStringSubmatch(ddl, -1)
		if len(matches) == 0 {
			r.t.Fatal("missing actual dependency foreign-key schema")
		}
		for _, m := range matches {
			r.command(w, "diagnostic-drop-"+m[1], binary, []string{"sql", "ALTER TABLE " + table + " DROP FOREIGN KEY `" + m[1] + "`", "--json"}, false)
		}
	}
	for _, victim := range []struct{ table, id string }{{"issues", "b06-missing-a"}, {w.ephemeralTable, "b06-missing-b"}} {
		r.command(w, "diagnostic-delete-"+victim.id, binary, []string{"sql", "DELETE FROM " + victim.table + " WHERE id='" + victim.id + "'", "--json"}, false)
		after := r.image(w, binary, "missing-"+victim.id)
		for _, table := range []string{"dependencies", w.ephemeralDependencies} {
			if !reflect.DeepEqual(before[table], after[table]) {
				r.t.Fatal("diagnostic deletion did not preserve source edges")
			}
		}
		cycles := r.cycles(w, binary, "partial-"+victim.id, true)
		if !reflect.DeepEqual(b06NativePaths(cycles), want) || len(cycles) != 1 || !cycles[0].Partial {
			r.t.Fatalf("missing raw member shortened report %+v", cycles)
		}
		missing := 0
		for _, member := range cycles[0].Members {
			if len(member.Issue) == 0 {
				missing++
				if member.ID != "b06-missing-a" && member.ID != "b06-missing-b" {
					r.t.Fatal("wrong member absent")
				}
			}
		}
		wantMissing := 1
		if victim.table == w.ephemeralTable {
			wantMissing = 2
		}
		if missing != wantMissing {
			r.t.Fatalf("missing hydration=%d want=%d", missing, wantMissing)
		}
		for _, row := range after[victim.table] {
			if row["id"] == victim.id {
				r.t.Fatal("victim is not physically absent")
			}
		}
		text := r.command(w, "partial-text-"+victim.id, binary, []string{"dep", "cycles", "--include-tracks", "--limit", "0"}, false)
		if !bytes.Contains(text, []byte("no record in this database")) || !bytes.Contains(text, []byte(victim.id)) {
			r.t.Fatal("native text concealed missing member")
		}
	}
	r.writeJSON(filepath.Join(r.root, "missing-counterpart-boundary.json"), map[string]any{"native": "actual durable and ephemeral rows physically removed; both dependency families preserved and reported with original IDs/positions", "plug": "physical-missing topology OPEN: durable cycle input only includes extant bead documents; denied-member placeholders are separately tested", "positive_mixed_plane": "OPEN: combined-Ready owner must supply same-image complete graph snapshot", "setup": "public disposable SQL damaged-database diagnostic; not ordinary deletion admission"})
	r.t.Log("B06_NATIVE mixed_plane=observed durable_missing=1 ephemeral_missing=1 full_members=3 partial=true plug_physical_missing=OPEN plug_mixed_plane=OPEN")
}

// A separate native workspace keeps these additional boundaries independent
// from the historical raw durable comparison vectors.
func (r *b06NativeRecorder) boundaries(binary string, server *publicnative.Server) {
	w := r.workspace(binary, server, "b06_boundaries")
	for _, id := range []string{"b06-parent", "b06-child", "b06-grandchild", "b06-wait-a", "b06-wait-b"} {
		r.create(w, binary, id, false)
	}
	for _, e := range []b06Edge{{"b06-child", "b06-parent", "parent-child"}, {"b06-grandchild", "b06-child", "parent-child"}} {
		r.edge(w, binary, e, false)
	}
	r.refuse(w, binary, b06Edge{"b06-parent", "b06-grandchild", "parent-child"}, "cycle")
	r.refuseHierarchy(w, binary, b06Edge{"b06-grandchild", "b06-parent", "blocks"}, "b06-grandchild cannot be blocked by its ancestor b06-parent: b06-parent cannot close until its descendants finish, so the gate would never clear")
	r.refuseHierarchy(w, binary, b06Edge{"b06-parent", "b06-grandchild", "blocks"}, "b06-parent cannot be blocked by its descendant b06-grandchild: waiting on your own subtree is a close gate, not a blocks edge — use a waits-for gate over the children (bd gate)")
	// Diagnostic SQL creates a waits-for/blocks loop without asserting that an
	// ordinary scheduler accepts it. The unmodified detector must exclude it.
	for i, e := range []b06Edge{{"b06-wait-a", "b06-wait-b", "waits-for"}, {"b06-wait-b", "b06-wait-a", "blocks"}} {
		query := fmt.Sprintf("INSERT INTO dependencies (id,issue_id,depends_on_issue_id,type,created_by) VALUES ('b06-wait-diagnostic-%d','%s','%s','%s','b06-public-diagnostic')", i, e.From, e.To, e.Kind)
		r.command(w, "waits-diagnostic-"+strconv.Itoa(i), binary, []string{"sql", query, "--json"}, false)
	}
	r.requirePaths(r.cycles(w, binary, "waits-default", false), [][]string{})
	r.requirePaths(r.cycles(w, binary, "waits-tracks", true), [][]string{})
	r.parallel(w, binary)
	for _, id := range []string{"b06-count-a", "b06-count-z"} {
		r.create(w, binary, id, false)
	}
	for i, e := range []b06Edge{{"b06-count-a", "b06-count-z", "blocks"}, {"b06-count-z", "b06-count-a", "blocks"}} {
		query := fmt.Sprintf("INSERT INTO dependencies (id,issue_id,depends_on_issue_id,type,created_by) VALUES ('b06-count-diagnostic-%d','%s','%s','%s','b06-public-diagnostic')", i, e.From, e.To, e.Kind)
		r.command(w, "count-diagnostic-"+strconv.Itoa(i), binary, []string{"sql", query, "--json"}, false)
	}
	r.requirePaths(r.cycles(w, binary, "count-json-limit-one", false), [][]string{{"b06-count-a", "b06-count-z"}, {"b06-parallel-a", "b06-parallel-b"}})
	before := r.image(w, binary, "count-text-before")
	text := r.command(w, "count-text-limit-one", binary, []string{"dep", "cycles", "--limit", "1"}, false)
	if !bytes.Contains(text, []byte("Found 2 dependency cycles:")) || !bytes.Contains(text, []byte("... and 1 more not shown. The count above is the whole report; use --limit 0 to print all of it.")) || !bytes.Contains(text, []byte("b06-count-a")) || bytes.Contains(text, []byte("b06-parallel-a")) {
		r.t.Fatalf("native count/truncation text: %s", text)
	}
	if !reflect.DeepEqual(before, r.image(w, binary, "count-text-after")) {
		r.t.Fatal("native limited text read changed state")
	}
	r.t.Log("B06_NATIVE_BOUNDARIES hierarchy_cycle=refused ancestor_blocker=refused waits_for=excluded count=2 limit=1 raw_order=exact")
}
