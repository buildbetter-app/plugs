package cardb20

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type nativeWorkspace struct {
	dir      string
	env      []string
	families map[string]string
}
type nativeImage struct {
	Rows      map[string][]map[string]any `json:"rows"`
	RawSHA256 map[string]string           `json:"raw_sha256"`
}
type nativeTwin struct {
	Name       string                 `json:"name"`
	Projection []stableEvent          `json:"journal_projection"`
	Audit      []map[string]any       `json:"audit_projection"`
	Images     map[string]nativeImage `json:"current_images"`
	Delivery   []map[string]any       `json:"follow_delivery"`
}

func TestB20FreshNativeCurrentWorkingSetTwins(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute declared output directory required")
	}
	r := &nativeRecorder{t: t, root: root}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	for _, key := range []string{"PLUG_B20_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing exact request/run binding %s", key)
		}
	}
	if !filepath.IsAbs(checkout) {
		t.Fatal("actual checked-out candidate root required")
	}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual != os.Getenv("PLUG_B20_REQUEST_SHA") {
		t.Fatal("candidate request/checkout mismatch")
	}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing canonical build protocol changed")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if err := json.Unmarshal(input, &protocol); err != nil || protocol.ExporterLabel == "" {
		t.Fatal("missing canonical production executable label")
	}
	r.target = protocol.ExporterLabel
	manifest, err := os.ReadFile(filepath.Join(checkout, "tools/lexicon/source_manifest.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "candidate-source-manifest.txt"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{"candidate": actual, "target": os.Getenv("TEST_TARGET"), "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "source_manifest_sha256": hashBytes(manifest), "native_source": pinnedSource, "qualification": "PUBLIC_SOURCE_BUILT_FIXTURE_ONLY", "official_bundle_admission": "UNRUN"})
	binary := r.buildCanonicalSource(pinnedSource)
	var twins []nativeTwin
	for _, name := range []string{"a", "b"} {
		if !t.Run("independent_"+name, func(t *testing.T) {
			dir := filepath.Join(root, "native-"+name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			twin := (&nativeRecorder{t: t, root: dir, target: r.target}).recordTwin(binary, name)
			twins = append(twins, twin)
		}) {
			t.Fatal("fresh native twin incomplete; no parity credit")
		}
	}
	if len(twins) != 2 || !reflect.DeepEqual(twins[0].Projection, twins[1].Projection) || !reflect.DeepEqual(twins[0].Audit, twins[1].Audit) {
		t.Fatal("independent non-temporal reference projections diverged")
	}
	twinsPath := filepath.Join(root, "native-twins.json")
	r.writeJSON(twinsPath, twins)
	if !t.Run("saved_rows_conserve_raw", func(t *testing.T) {
		conservation, err := verifyNativeSQLRecordingFile(twinsPath, map[string]string{"a": filepath.Join(root, "native-a"), "b": filepath.Join(root, "native-b")})
		if err != nil {
			t.Fatalf("saved aggregate SQL row conservation: %v", err)
		}
		r.writeJSON(filepath.Join(root, "native-twins-row-conservation.json"), conservation)
	}) {
		t.Fatal("saved native derivatives incomplete; no row conservation credit")
	}
	for _, engine := range engines {
		t.Run("paired_"+engine, func(t *testing.T) {
			plug := replayCurrent(t, engine)
			native := append([]stableEvent(nil), twins[0].Projection...)
			// A4/P6 adopts total-order Plug changesets and ontology statuses.
			// Only this explicit semantic projection is compared: native sequence
			// is never restored as a Plug revision or manufactured history.
			for i := range native {
				native[i].Actor = "cardb20-reader"
				switch native[i].Status {
				case "open":
					native[i].Status = "todo"
				case "closed":
					native[i].Status = "done"
				default:
					t.Fatalf("unadopted native status %q", native[i].Status)
				}
			}
			if !reflect.DeepEqual(plug, native) {
				t.Fatalf("actual native/Plug current projection mismatch: native=%+v plug=%+v", native, plug)
			}
			retain(t, "comparison-"+engine+".json", map[string]any{"native_raw_projection": twins[0].Projection, "native_adopted_projection": native, "actual_plug_projection": plug, "verdict": "PASS_BOUNDED_CURRENT_WORKING_SET", "adopted_deviations": []string{"A4/P6 always-on changesets versus opt-in source journal", "ontology open/closed to todo/done", "fixture principals remain separately attributed"}})
		})
	}
}

func (r *nativeRecorder) command(w nativeWorkspace, label, binary string, args []string, wantFailure bool) []byte {
	r.t.Helper()
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
	r.writeJSON(filepath.Join(r.root, label+".request.json"), map[string]any{"argv": cmd.Args, "directory": w.dir, "environment": w.env, "exit": exit, "error": fmt.Sprint(err), "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if ctx.Err() != nil || exit < 0 || (!wantFailure && exit != 0) || (wantFailure && exit == 0) {
		r.t.Fatalf("%s exit=%d: %v\n%s", label, exit, err, stderr.Bytes())
	}
	return stdout.Bytes()
}

func (r *nativeRecorder) recordTwin(binary, name string) nativeTwin {
	t := r.t
	workRoot := "/tmp/plug-swarm/work"
	if err := os.MkdirAll(workRoot, 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp(workRoot, "b20-native-"+name+"-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	home := filepath.Join(workspace, "home")
	dir := filepath.Join(workspace, "project")
	for _, p := range []string{home, dir, filepath.Join(workspace, "tmp")} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, r.root)
	base := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "USER=b20-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B20 Public", "GIT_AUTHOR_EMAIL=b20@example.invalid", "GIT_COMMITTER_NAME=B20 Public", "GIT_COMMITTER_EMAIL=b20@example.invalid", "TMPDIR=" + filepath.Join(workspace, "tmp"), "BEADS_ACTOR=b20-recorder", "BEADS_NO_DAEMON=1"}
	w := nativeWorkspace{dir: dir, env: server.ChildEnv(base)}
	// Use supported workspace config without an environment override, so the
	// retained config readback names the actual activation for each process.
	r.command(w, "git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="}, false)
	version := r.command(w, "version", binary, []string{"version", "--json"}, false)
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatalf("executed native version does not match exact source build: %s", version)
	}
	r.command(w, "init", binary, []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b20_current", "--prefix", "b20", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, false)
	server.RequireDatabaseDir(t, "b20_current")
	for _, kv := range [][2]string{{"events-journal", "true"}, {"events-journal-auto-prune", "false"}} {
		r.command(w, "config-set-"+kv[0], binary, []string{"config", "set", kv[0], kv[1], "--json"}, false)
		data := r.command(w, "config-get-"+kv[0], binary, []string{"config", "get", kv[0], "--json"}, false)
		var got struct{ Key, Value string }
		if json.Unmarshal(data, &got) != nil || got.Key != kv[0] || got.Value != kv[1] {
			t.Fatalf("effective journal config differs: %s", data)
		}
	}
	w.families = r.discoverFamilies(w, binary)
	twin := nativeTwin{Name: name, Images: map[string]nativeImage{}}
	twin.Images["empty"] = r.image(w, binary, "empty")
	if len(twin.Images["empty"].Rows["events"]) != 0 || len(twin.Images["empty"].Rows["journal"]) != 0 {
		t.Fatal("fresh public workspace was not empty")
	}
	follow := r.startFollow(w, binary)
	r.command(w, "create", binary, []string{"create", "--id", "b20-1", "--title", "journal anchor", "--json"}, false)
	twin.Delivery = append(twin.Delivery, follow.next(t))
	twin.Images["create"] = r.image(w, binary, "create")
	r.command(w, "update", binary, []string{"update", "b20-1", "--title", "journal edited", "--json"}, false)
	twin.Delivery = append(twin.Delivery, follow.next(t))
	twin.Images["update"] = r.image(w, binary, "update")
	r.command(w, "same-value-update", binary, []string{"update", "b20-1", "--title", "journal edited", "--json"}, false)
	twin.Images["no-op"] = r.image(w, binary, "no-op")
	if !reflect.DeepEqual(twin.Images["update"].Rows, twin.Images["no-op"].Rows) {
		t.Fatal("same-value native update changed current event families")
	}
	batch := filepath.Join(workspace, "abort.txt")
	batchBytes := []byte("update b20-1 title=rollback\nupdate b20-missing title=missing\n")
	if err := os.WriteFile(batch, batchBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.root, "abort.txt"), batchBytes, 0600); err != nil {
		t.Fatal(err)
	}
	r.command(w, "abort-batch", binary, []string{"batch", "--file", batch, "--json"}, true)
	failure, err := os.ReadFile(filepath.Join(r.root, "abort-batch.stderr"))
	if err != nil {
		t.Fatal(err)
	}
	var batchError struct {
		Code  string
		Error string
	}
	// This native command writes its typed error first on stderr, followed by
	// human diagnostics. Retain the complete original stream separately.
	if err := json.NewDecoder(bytes.NewReader(failure)).Decode(&batchError); err != nil || batchError.Code != "batch_error" || !strings.Contains(batchError.Error, "line 2 (update b20-missing title=missing)") {
		t.Fatalf("batch did not reach second operation: %s (%v)", failure, err)
	}
	twin.Images["abort"] = r.image(w, binary, "abort")
	if !reflect.DeepEqual(twin.Images["no-op"].Rows, twin.Images["abort"].Rows) {
		t.Fatal("aborted native batch changed current event families or sequence")
	}
	r.command(w, "close-after-abort", binary, []string{"close", "b20-1", "--reason", "complete", "--json"}, false)
	twin.Delivery = append(twin.Delivery, follow.next(t))
	follow.stop(t)
	completeFollow, err := os.ReadFile(filepath.Join(r.root, "follow.stdout"))
	if err != nil {
		t.Fatal(err)
	}
	// Include buffered extras after the three synchronization reads so an
	// unexpected duplicate cannot hide in the follower's bounded queue.
	twin.Delivery = decodeLines(t, completeFollow)
	twin.Images["final"] = r.image(w, binary, "final")
	journalRaw := r.command(w, "export-current-journal", binary, []string{"events", "export", "--json"}, false)
	journal := decodeLines(t, journalRaw)
	unbounded := decodeLines(t, r.command(w, "literal-limit-zero-unbounded", binary, []string{"events", "tail", "--since", "0", "--limit", "0", "--json"}, false))
	if !reflect.DeepEqual(unbounded, journal) {
		t.Fatal("native literal limit0 did not return the complete ordered journal")
	}
	if !reflect.DeepEqual(journal, twin.Delivery) {
		t.Fatal("original follow delivery differs from committed journal export order or rows")
	}
	if len(journal) != 3 {
		t.Fatalf("post-abort native journal has%d rows, want3", len(journal))
	}
	for i, row := range journal {
		if fmt.Sprint(row["seq"]) != fmt.Sprint(i+1) {
			t.Fatalf("native sequence burned after rollback: %+v", journal)
		}
		issue, ok := row["issue"].(map[string]any)
		if !ok {
			t.Fatal("journal issue image missing")
		}
		twin.Projection = append(twin.Projection, stableEvent{fmt.Sprint(row["seq"]), fmt.Sprint(row["op"]), fmt.Sprint(row["issue_id"]), fmt.Sprint(row["actor"]), fmt.Sprint(issue["title"]), fmt.Sprint(issue["status"])})
	}
	for _, row := range twin.Images["final"].Rows["events"] {
		selected := map[string]any{}
		for _, key := range []string{"issue_id", "event_type", "actor", "comment"} {
			selected[key] = row[key]
		}
		for _, key := range []string{"old_value", "new_value"} {
			selected[key] = projectAuditValue(t, row[key])
		}
		twin.Audit = append(twin.Audit, selected)
	}
	if len(twin.Audit) == 0 {
		t.Fatal("actual current audit rows absent")
	}
	seq := twin.Images["final"].Rows["sequence"]
	if len(seq) != 1 || fmt.Sprint(seq[0]["id"]) != "0" || fmt.Sprint(seq[0]["next_seq"]) != "3" {
		t.Fatalf("actual native sequence singleton=%+v", seq)
	}
	compareSQLJournal(t, twin.Images["final"].Rows["journal"], journal)
	since := decodeLines(t, r.command(w, "since-exclusive-unbounded", binary, []string{"events", "tail", "--since", "1", "--limit", "0", "--json"}, false))
	if !reflect.DeepEqual(since, journal[1:]) {
		t.Fatal("native unbounded since1 differs")
	}
	page := decodeLines(t, r.command(w, "since-exclusive-page", binary, []string{"events", "tail", "--since", "1", "--limit", "1", "--json"}, false))
	if len(page) != 1 || !reflect.DeepEqual(page[0], journal[1]) {
		t.Fatal("native since-exclusive bounded page differs")
	}
	if raw := r.command(w, "caught-up", binary, []string{"events", "tail", "--since", "3", "--limit", "1", "--json"}, false); len(bytes.TrimSpace(raw)) != 0 {
		t.Fatalf("caught-up native page=%s", raw)
	}
	// Native prefix deletion is a folded design difference (commands.md:228).
	// Plug retains audit forever and exposes no client prune.
	for _, key := range []string{"events-journal-retain-days", "events-journal-retain-rows"} {
		r.command(w, "prune-config-"+key, binary, []string{"config", "set", key, "0", "--json"}, false)
	}
	r.command(w, "prune-before-three", binary, []string{"events", "prune", "--before", "3", "--json"}, false)
	twin.Images["pruned"] = r.image(w, binary, "pruned")
	pruned := twin.Images["pruned"]
	if !reflect.DeepEqual(pruned.Rows["events"], twin.Images["final"].Rows["events"]) || !reflect.DeepEqual(pruned.Rows["issues"], twin.Images["final"].Rows["issues"]) || !reflect.DeepEqual(pruned.Rows["sequence"], seq) {
		t.Fatal("native journal prune changed audit, issue, or sequence-head state")
	}
	if rows := pruned.Rows["journal"]; len(rows) != 1 || !reflect.DeepEqual(rows[0], twin.Images["final"].Rows["journal"][2]) {
		t.Fatal("native prune did not preserve precisely the since-exclusive suffix")
	}
	for _, vector := range []struct {
		label string
		args  []string
	}{
		{"pruned-since-zero", []string{"events", "tail", "--since", "0", "--limit", "0", "--json"}},
		{"pruned-export", []string{"events", "export", "--json"}},
	} {
		raw := r.command(w, vector.label, binary, vector.args, true)
		var truncated struct {
			Code               string
			Since, Floor, Head int64
		}
		if err := json.Unmarshal(raw, &truncated); err != nil || truncated.Code != "events_journal_truncated" || truncated.Since != 0 || truncated.Floor != 3 || truncated.Head != 3 {
			t.Fatalf("native typed truncation=%s err=%v", raw, err)
		}
	}
	suffix := decodeLines(t, r.command(w, "pruned-known-gap-suffix", binary, []string{"events", "tail", "--since", "2", "--limit", "0", "--json"}, false))
	if !reflect.DeepEqual(suffix, journal[2:]) {
		t.Fatal("native accepted-gap resume changed surviving row")
	}
	r.writeJSON(filepath.Join(r.root, "limit-zero-deviation.json"), map[string]any{
		"native_limit_zero": "OBSERVED_UNBOUNDED", "native_unbounded_rows": len(unbounded),
		"native_pruned_rows": len(pruned.Rows["journal"]), "native_head": seq,
		"adopted_wire_deviation":    "P6/P20 spec/commands.md:227-232; bounded pages and typed nonpositive-limit refusals",
		"real_compat_client_export": "PAIRED_SEPARATELY_THROUGH_REAL_CLI", "plug_prune": "FOLDED_OPERATOR_RETENTION_DESIGN_DIFFERENCE_NO_CLIENT_PRUNE", "operator_retention_floor": "UNIMPLEMENTED_NONBLOCKING_OPERATOR_FEATURE",
	})
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	recordingPath := filepath.Join(r.root, "recording.json")
	r.writeJSON(recordingPath, twin)
	conservation, err := verifyNativeSQLRecordingFile(recordingPath, map[string]string{name: r.root})
	if err != nil {
		t.Fatalf("saved individual SQL row conservation: %v", err)
	}
	r.writeJSON(filepath.Join(r.root, "recording-row-conservation.json"), conservation)
	return twin
}

func projectAuditValue(t *testing.T, value any) any {
	t.Helper()
	text, ok := value.(string)
	if !ok || !strings.HasPrefix(strings.TrimSpace(text), "{") {
		return value
	}
	var source map[string]any
	if err := json.Unmarshal([]byte(text), &source); err != nil {
		t.Fatal(err)
	}
	selected := map[string]any{}
	for _, key := range []string{"id", "title", "status", "close_reason"} {
		if value, present := source[key]; present {
			selected[key] = value
		}
	}
	return selected
}

func compareSQLJournal(t *testing.T, rows, cli []map[string]any) {
	t.Helper()
	if len(rows) != len(cli) {
		t.Fatal("SQL journal and CLI journal counts differ")
	}
	for i, row := range rows {
		for _, key := range []string{"seq", "op", "issue_id", "actor"} {
			sqlValue, sqlPresent := row[key]
			cliValue, cliPresent := cli[i][key]
			if !sqlPresent || !cliPresent || !nativeSQLJournalCellEqual(sqlValue, cliValue) {
				t.Fatalf("SQL/CLI journal row%d field%s differs", i, key)
			}
		}
		for _, pair := range [][2]string{{"issue_json", "issue"}, {"dep_json", "dep"}, {"comment_json", "comment"}} {
			var payload any
			if raw := row[pair[0]]; raw != nil {
				text, ok := raw.(string)
				if !ok {
					t.Fatalf("SQL journal JSON is not text: %T", raw)
				}
				if err := json.Unmarshal([]byte(text), &payload); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(payload, cli[i][pair[1]]) {
				t.Fatalf("SQL/CLI journal row%d complete%s payload differs", i, pair[0])
			}
		}
	}
}

// Discover the actual current families by their unique source schema shapes.
// The complete physical-name/column result and binding are retained verbatim.
func (r *nativeRecorder) discoverFamilies(w nativeWorkspace, binary string) map[string]string {
	data := r.command(w, "current-family-catalog", binary, []string{"--readonly", "sql", "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'b20_current' ORDER BY table_name, ordinal_position", "--json"}, false)
	var rows []map[string]string
	if err := json.Unmarshal(data, &rows); err != nil {
		r.t.Fatal(err)
	}
	columns := map[string]map[string]bool{}
	for _, row := range rows {
		table, column := row["table_name"], row["column_name"]
		// Dolt's metadata preserves driver-selected column-label casing.
		if table == "" {
			table = row["TABLE_NAME"]
		}
		if column == "" {
			column = row["COLUMN_NAME"]
		}
		if table == "" || column == "" {
			r.t.Fatalf("catalog omitted physical identity: %+v", row)
		}
		if columns[table] == nil {
			columns[table] = map[string]bool{}
		}
		columns[table][column] = true
	}
	bindings := map[string]string{"events": "events", "issues": "issues"}
	for logical, required := range map[string][]string{"journal": {"seq", "op", "issue_id", "actor", "issue_json"}, "sequence": {"id", "next_seq"}} {
		for table, cols := range columns {
			matches := true
			for _, name := range required {
				matches = matches && cols[name]
			}
			if matches {
				if bindings[logical] != "" {
					r.t.Fatalf("ambiguous %s family", logical)
				}
				bindings[logical] = table
			}
		}
		if bindings[logical] == "" {
			r.t.Fatalf("missing actual %s family", logical)
		}
	}
	for _, name := range bindings {
		for _, c := range name {
			if c != '_' && (c < 'a' || c > 'z') {
				r.t.Fatalf("unsafe catalog identifier %q", name)
			}
		}
	}
	r.writeJSON(filepath.Join(r.root, "current-family-bindings.json"), bindings)
	return bindings
}

func (r *nativeRecorder) image(w nativeWorkspace, binary, label string) nativeImage {
	result := nativeImage{Rows: map[string][]map[string]any{}, RawSHA256: map[string]string{}}
	for _, q := range []struct{ name, order string }{{"events", "created_at,id"}, {"journal", "seq"}, {"sequence", "id"}, {"issues", "id"}} {
		table := w.families[q.name]
		data := r.command(w, label+"-"+q.name, binary, []string{"--readonly", "sql", "SELECT * FROM " + table + " ORDER BY " + q.order, "--json"}, false)
		rows, err := decodeNativeSQLRows(data)
		if err != nil {
			r.t.Fatalf("actual current SQL%s: %v: %s", q.name, err, data)
		}
		if err := verifyNativeSQLRows(data, rows); err != nil {
			r.t.Fatalf("actual current SQL%s row conservation: %v", q.name, err)
		}
		result.Rows[q.name] = rows
		result.RawSHA256[q.name] = hashBytes(data)
	}
	return result
}
func decodeLines(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var result []map[string]any
	s := bufio.NewScanner(bytes.NewReader(data))
	s.Buffer(make([]byte, 4096), 8<<20)
	for s.Scan() {
		var row map[string]any
		if err := json.Unmarshal(s.Bytes(), &row); err != nil {
			t.Fatalf("journal line: %v: %s", err, s.Bytes())
		}
		result = append(result, row)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

type follower struct {
	rows           chan map[string]any
	done           chan error
	stdin          *os.File
	once           sync.Once
	r              *nativeRecorder
	cmd            *exec.Cmd
	stdout, stderr *os.File
}

func (r *nativeRecorder) startFollow(w nativeWorkspace, binary string) *follower {
	t := r.t
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(filepath.Join(r.root, "follow.stdout"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	errFile, err := os.OpenFile(filepath.Join(r.root, "follow.stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "events", "tail", "--since", "0", "--follow", "--exit-on-stdin-eof", "--json")
	cmd.Dir = w.dir
	cmd.Env = w.env
	cmd.Stdin = read
	cmd.Stderr = errFile
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	f := &follower{rows: make(chan map[string]any, 16), done: make(chan error, 1), stdin: write, r: r, cmd: cmd, stdout: out, stderr: errFile}
	go func() {
		s := bufio.NewScanner(pipe)
		s.Buffer(make([]byte, 4096), 8<<20)
		var scanErr error
		for s.Scan() {
			line := append(append([]byte(nil), s.Bytes()...), '\n')
			if _, err := out.Write(line); err != nil {
				scanErr = err
				break
			}
			var row map[string]any
			if err := json.Unmarshal(s.Bytes(), &row); err != nil {
				scanErr = err
				break
			}
			select {
			case f.rows <- row:
			default:
				scanErr = errors.New("native follow exceeded bounded queue")
			}
			if scanErr != nil {
				break
			}
		}
		if scanErr == nil {
			scanErr = s.Err()
		}
		f.done <- errors.Join(scanErr, cmd.Wait())
	}()
	t.Cleanup(func() { f.stop(t) })
	r.writeJSON(filepath.Join(r.root, "follow-start.json"), map[string]any{"argv": cmd.Args, "directory": w.dir, "environment": w.env, "pid": cmd.Process.Pid})
	return f
}
func (f *follower) next(t *testing.T) map[string]any {
	t.Helper()
	select {
	case row := <-f.rows:
		return row
	case err := <-f.done:
		t.Fatalf("native follow exited before delivery: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("native follow delivery timed out")
	}
	return nil
}
func (f *follower) stop(t *testing.T) {
	t.Helper()
	f.once.Do(func() {
		closeErr := f.stdin.Close()
		var waitErr error
		reaped := false
		select {
		case waitErr = <-f.done:
			reaped = true
		case <-time.After(10 * time.Second):
			_ = f.cmd.Process.Kill()
			select {
			case waitErr = <-f.done:
				reaped = true
			case <-time.After(5 * time.Second):
			}
			t.Error("native follow exceeded graceful exit budget")
		}
		streamErr := errors.Join(f.stdout.Close(), f.stderr.Close())
		f.r.writeJSON(filepath.Join(f.r.root, "follow-stop.json"), map[string]any{"pid": f.cmd.Process.Pid, "reaped": reaped, "wait_error": fmt.Sprint(waitErr), "close_error": fmt.Sprint(errors.Join(closeErr, streamErr))})
		if waitErr != nil || closeErr != nil || streamErr != nil {
			t.Errorf("native follow shutdown: %v", errors.Join(waitErr, closeErr, streamErr))
		}
	})
}
