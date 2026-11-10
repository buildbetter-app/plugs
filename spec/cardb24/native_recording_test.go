// The package name permits direct reuse of B20's unchanged private test build
// helper. This directory owns only B24 command fixtures and comparisons.
package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// Command intervals are observations, never a clock injection. In particular,
// SQL expression precision and stored timestamp precision are recorded apart.
type b24CommandObservation struct {
	Argv      []string  `json:"argv"`
	Started   time.Time `json:"started_utc"`
	Ended     time.Time `json:"ended_utc"`
	Exit      int       `json:"exit"`
	StdoutSHA string    `json:"stdout_sha256"`
	StderrSHA string    `json:"stderr_sha256"`
}

type b24Native struct {
	t      *testing.T
	r      *nativeRecorder
	binary string
	dir    string
	env    []string
}

type b24NativeOutcome struct {
	Selected           []string  `json:"selected_stale_ids"`
	NonpositiveRefused int       `json:"nonpositive_ages_refused"`
	Reason             string    `json:"exact_appended_reason"`
	Notes              string    `json:"native_notes_after_append"`
	BlankRefused       bool      `json:"blank_reason_refused"`
	FutureExcluded     bool      `json:"future_deferred_excluded"`
	Woke               bool      `json:"normal_ready_woke_target"`
	PastWarned         bool      `json:"past_target_warning"`
	PastWritten        bool      `json:"past_target_deliberate_write"`
	Target             time.Time `json:"future_target"`
	ExactEquality      string    `json:"exact_native_equality"`
	VersionRead        string    `json:"version_load_measurement"`
}

func (n *b24Native) command(label string, wantFailure bool, args ...string) []byte {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir, cmd.Env = n.dir, append([]string(nil), n.env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now().UTC()
	err := cmd.Run()
	ended := time.Now().UTC()
	exit := 0
	if err != nil {
		exit = -2
		var failure *exec.ExitError
		if errors.As(err, &failure) {
			exit = failure.ExitCode()
		}
	}
	for suffix, data := range map[string][]byte{"stdout": stdout.Bytes(), "stderr": stderr.Bytes()} {
		if err := os.WriteFile(filepath.Join(n.r.root, label+"."+suffix), data, 0600); err != nil {
			n.t.Fatal(err)
		}
	}
	n.r.writeJSON(filepath.Join(n.r.root, label+".request.json"), map[string]any{
		"command":   b24CommandObservation{cmd.Args, started, ended, exit, hashBytes(stdout.Bytes()), hashBytes(stderr.Bytes())},
		"directory": n.dir, "environment": n.env, "error": fmt.Sprint(err),
	})
	if ctx.Err() != nil || exit < 0 || (wantFailure && exit == 0) || (!wantFailure && exit != 0) {
		n.t.Fatalf("native %s exit=%d error=%v stderr=%s", label, exit, err, stderr.Bytes())
	}
	return append([]byte(nil), stdout.Bytes()...)
}

func b24Rows(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var rows []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&rows); err != nil {
		t.Fatalf("native JSON rows: %v: %s", err, raw)
	}
	return rows
}

func b24Value(row map[string]any, name string) any {
	for key, value := range row {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return nil
}

func (n *b24Native) sql(label, query string) []map[string]any {
	return b24Rows(n.t, n.command(label, false, "--readonly", "sql", query, "--json"))
}

func (n *b24Native) clock(label string) time.Time {
	n.t.Helper()
	rows := n.sql(label, "SELECT DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%dT%H:%i:%s.%fZ') AS sql_now, DATE_FORMAT(UTC_TIMESTAMP(),'%Y-%m-%dT%H:%i:%s.%fZ') AS sql_default")
	if len(rows) != 1 {
		n.t.Fatal("native SQL clock observation absent")
	}
	at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(b24Value(rows[0], "sql_now")))
	if err != nil {
		n.t.Fatal(err)
	}
	n.t.Logf("B24_NATIVE_SQL_CLOCK phase=%s sql=%s default=%v exact_go_sql_equality=NOT_MEASURED", label, at.Format(time.RFC3339Nano), b24Value(rows[0], "sql_default"))
	return at
}

func (n *b24Native) image(label string) map[string][]string {
	n.t.Helper()
	catalog := n.sql(label+"-catalog", "SELECT table_name AS name FROM information_schema.tables WHERE table_schema='b24_current' AND table_type='BASE TABLE' ORDER BY table_name")
	if len(catalog) == 0 {
		n.t.Fatal("native physical table census is empty")
	}
	image := map[string][]string{}
	for _, row := range catalog {
		table, ok := b24Value(row, "name").(string)
		if !ok || table == "" {
			n.t.Fatal("physical table identity absent")
		}
		for _, char := range table {
			if char != '_' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
				n.t.Fatalf("unsafe native physical table %q", table)
			}
		}
		image[table] = []string{}
		for _, value := range n.sql(label+"-table-"+table, "SELECT * FROM `"+table+"`") {
			encoded, err := json.Marshal(value)
			if err != nil {
				n.t.Fatal(err)
			}
			image[table] = append(image[table], string(encoded))
		}
		sort.Strings(image[table])
	}
	// The actual Dolt history is observed separately from application versions.
	image["@dolt_log"] = []string{}
	for _, row := range n.sql(label+"-dolt-log", "SELECT * FROM dolt_log ORDER BY commit_hash") {
		encoded, err := json.Marshal(row)
		if err != nil {
			n.t.Fatal(err)
		}
		image["@dolt_log"] = append(image["@dolt_log"], string(encoded))
	}
	n.r.writeJSON(filepath.Join(n.r.root, label+"-physical-image.json"), image)
	return image
}

func (n *b24Native) conserved(label string, operation func()) {
	n.t.Helper()
	before := n.image(label + "-before")
	operation()
	after := n.image(label + "-after")
	if !reflect.DeepEqual(before, after) {
		n.t.Fatalf("native %s mutated complete physical state", label)
	}
	n.t.Logf("B24_NATIVE_CONSERVATION phase=%s physical_tables=%d", label, len(before)-1)
}

func b24NativeIDs(t *testing.T, raw []byte) []string {
	t.Helper()
	ids := []string{}
	for _, row := range b24Rows(t, raw) {
		id, ok := row["id"].(string)
		if !ok || id == "" {
			t.Fatal("native public result omitted entity ID")
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func b24HasID(ids []string, id string) bool {
	for _, value := range ids {
		if value == id {
			return true
		}
	}
	return false
}

func b24RecordNative(t *testing.T, binary, root string) b24NativeOutcome {
	t.Helper()
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp("/tmp/plug-swarm/work", "b24-public-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	for _, dir := range []string{"home", "project", "tmp"} {
		if err := os.Mkdir(filepath.Join(workspace, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	r := &nativeRecorder{t: t, root: root}
	server := publicnative.StartDolt(t, root)
	env := []string{
		"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + filepath.Join(workspace, "home"),
		"TMPDIR=" + filepath.Join(workspace, "tmp"), "USER=b24-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true",
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B24 Public", "GIT_AUTHOR_EMAIL=b24@example.invalid",
		"GIT_COMMITTER_NAME=B24 Public", "GIT_COMMITTER_EMAIL=b24@example.invalid", "BEADS_ACTOR=b24-public", "BEADS_NO_DAEMON=1",
	}
	n := &b24Native{t: t, r: r, binary: "/usr/bin/git", dir: filepath.Join(workspace, "project"), env: server.ChildEnv(env)}
	n.command("workspace-git-init", false, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	version := n.command("native-version", false, "version", "--json")
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("actual executed canonical source identity differs")
	}
	n.command("native-init", false, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b24_current", "--prefix", "b24", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b24_current")
	n.command("history-off-set", false, "config", "set", "versioned-history.enabled", "false", "--json")
	var effective struct{ Key, Value string }
	if json.Unmarshal(n.command("history-off-get", false, "config", "get", "versioned-history.enabled", "--json"), &effective) != nil || effective.Key != "versioned-history.enabled" || effective.Value != "false" {
		t.Fatal("effective native history-off config was not read back")
	}
	for _, id := range []string{"b24-old", "b24-younger", "b24-closed"} {
		n.command("create-"+id, false, "create", "--id", id, "--title", id, "--type", "task", "--json")
	}
	n.command("close-status-control", false, "close", "b24-closed", "--reason", "completed", "--json")
	// Public SQL is deliberate fixture setup, not a substituted clock. Actual
	// persisted times and the unchanged canonical predicates are retained.
	n.command("seed-stale-times", false, "sql", "UPDATE issues SET updated_at='2001-01-01 00:00:00' WHERE id IN ('b24-old','b24-closed')", "--json")
	n.command("seed-younger-time", false, "sql", "UPDATE issues SET updated_at='2100-01-01 00:00:00' WHERE id='b24-younger'", "--json")
	n.sql("actual-stale-row-times", "SELECT id,status,updated_at FROM issues ORDER BY id")
	n.sql("actual-time-column-metadata", "SELECT table_name,column_name,data_type,datetime_precision FROM information_schema.columns WHERE table_schema='b24_current' AND column_name IN ('created_at','updated_at','defer_until') ORDER BY table_name,ordinal_position")
	outcome := b24NativeOutcome{ExactEquality: "NOT_MEASURED", VersionRead: "NOT_MEASURED"}
	n.clock("stale-sql-before")
	n.conserved("stale-and-age-refusals", func() {
		outcome.Selected = b24NativeIDs(t, n.command("actual-stale-selection", false, "stale", "--days", "1", "--limit", "100", "--json"))
		if !reflect.DeepEqual(outcome.Selected, []string{"b24-old"}) {
			t.Fatalf("native stale status/age selection=%v", outcome.Selected)
		}
		for _, age := range []string{"0", "-1"} {
			label := "actual-stale-invalid-age-" + age
			n.command(label, true, "stale", "--days", age, "--json")
			stderr, err := os.ReadFile(filepath.Join(root, label+".stderr"))
			stdout, readErr := os.ReadFile(filepath.Join(root, label+".stdout"))
			if err != nil || readErr != nil || !bytes.Contains(append(stdout, stderr...), []byte("--days must be at least 1")) {
				t.Fatal("native failure did not reach nonpositive-age validation")
			}
			outcome.NonpositiveRefused++
		}
	})
	n.clock("stale-sql-after")
	n.command("create-defer-target", false, "create", "--id", "b24-deferred", "--title", "defer preserves title", "--type", "task", "--json")
	n.command("existing-notes", false, "update", "b24-deferred", "--notes", "existing notes", "--json")
	n.conserved("blank-reason", func() {
		n.command("actual-blank-reason", true, "defer", "b24-deferred", "--reason", " ", "--json")
		stderr, err := os.ReadFile(filepath.Join(root, "actual-blank-reason.stderr"))
		stdout, readErr := os.ReadFile(filepath.Join(root, "actual-blank-reason.stdout"))
		if err != nil || readErr != nil || !bytes.Contains(append(stdout, stderr...), []byte("reason cannot be empty")) {
			t.Fatal("native failure did not reach explicit blank-reason validation")
		}
		outcome.BlankRefused = true
	})
	outcome.Target = time.Now().UTC().Add(60 * time.Second).Truncate(time.Second)
	future := n.command("actual-future-defer", false, "defer", "b24-deferred", "--reason", "  waiting for API  ", "--until", outcome.Target.Format(time.RFC3339), "--json")
	rows := b24Rows(t, future)
	if len(rows) != 1 || rows[0]["status"] != "deferred" || rows[0]["notes"] != "existing notes\nwaiting for API" {
		t.Fatalf("native exact reason append/status=%s", future)
	}
	storedTarget, err := time.Parse(time.RFC3339Nano, fmt.Sprint(rows[0]["defer_until"]))
	if err != nil || !storedTarget.Equal(outcome.Target) {
		t.Fatalf("actual persisted future target=%v error=%v", rows[0]["defer_until"], err)
	}
	outcome.Reason, outcome.Notes = "waiting for API", rows[0]["notes"].(string)
	n.conserved("future-ready", func() {
		if !n.clock("future-ready-sql-before").Before(outcome.Target) {
			t.Fatal("future-before observation missed its measurement window")
		}
		ids := b24NativeIDs(t, n.command("actual-future-ready", false, "ready", "--limit", "100", "--json"))
		if b24HasID(ids, "b24-deferred") || !n.clock("future-ready-sql-after").Before(outcome.Target) {
			t.Fatal("native future defer was included or command crossed its before window")
		}
		outcome.FutureExcluded = true
	})
	// Wait for an actual public time crossing. This changes no clock, binary,
	// stored target or SQL predicate and makes no latency claim.
	if remaining := time.Until(outcome.Target.Add(2 * time.Second)); remaining > 0 {
		timer := time.NewTimer(remaining)
		<-timer.C
	}
	if n.clock("wake-sql-before").Before(outcome.Target) {
		t.Fatal("native SQL clock did not cross the recorded target")
	}
	beforeWake := n.image("wake-before")
	ready := n.command("actual-crossed-ready", false, "ready", "--limit", "100", "--json")
	if !b24HasID(b24NativeIDs(t, ready), "b24-deferred") {
		t.Fatal("normal native ready did not wake the expired dated defer")
	}
	afterWake := n.image("wake-after")
	versions, present := beforeWake["issue_versions"]
	if !present || len(versions) != 0 || !reflect.DeepEqual(versions, afterWake["issue_versions"]) {
		t.Fatal("native history-off wake minted application version rows or version census is absent")
	}
	woken := b24Rows(t, n.command("actual-woken-show", false, "show", "b24-deferred", "--json"))
	if len(woken) != 1 || woken[0]["status"] != "open" || woken[0]["defer_until"] != nil || woken[0]["notes"] != outcome.Notes {
		t.Fatalf("normal wake lost status, target clearing or exact notes: %+v", woken)
	}
	n.clock("wake-sql-after")
	outcome.Woke = true
	n.conserved("second-ready-noop", func() {
		if !b24HasID(b24NativeIDs(t, n.command("actual-second-ready", false, "ready", "--limit", "100", "--json")), "b24-deferred") {
			t.Fatal("second native ready lost woken target")
		}
	})
	n.command("create-past-target", false, "create", "--id", "b24-past", "--title", "past remains deliberate write", "--type", "task", "--json")
	n.command("actual-past-defer-warning", false, "defer", "b24-past", "--until", "2001-01-01", "--reason", "past target")
	warning, err := os.ReadFile(filepath.Join(root, "actual-past-defer-warning.stderr"))
	if err != nil || !bytes.Contains(warning, []byte("is in the past")) {
		t.Fatal("canonical non-JSON past-target warning absent")
	}
	outcome.PastWarned = true
	past := b24Rows(t, n.command("actual-past-written", false, "show", "b24-past", "--json"))
	if len(past) != 1 || past[0]["status"] != "deferred" || past[0]["defer_until"] == nil {
		t.Fatal("past-target warning was not followed by the deliberate deferred write")
	}
	outcome.PastWritten = true
	if !b24HasID(b24NativeIDs(t, n.command("actual-past-ready", false, "ready", "--limit", "100", "--json")), "b24-past") {
		t.Fatal("past-target native defer failed to wake through public ready")
	}
	n.r.writeJSON(filepath.Join(root, "outcome.json"), outcome)
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	return outcome
}

func TestB24CanonicalNativePublicStaleDeferWake(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("actual RunsOn checkout and output directories required")
	}
	r := &nativeRecorder{t: t, root: root}
	for _, key := range []string{"PLUG_B24_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing native request/control binding %s", key)
		}
	}
	head := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if head != os.Getenv("PLUG_B24_REQUEST_SHA") {
		t.Fatal("actual candidate differs from requested B24 head")
	}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("unchanged canonical build protocol binding differs")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(input, &protocol) != nil || protocol.ExporterLabel == "" {
		t.Fatal("canonical production executable label absent")
	}
	r.target = protocol.ExporterLabel
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{
		"candidate": head, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"),
		"workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"),
		"native_source": pinnedSource, "shared_recipe_sha256": "0003df7485b16dc2ad3ace5533194f47b748ab15319e13698365ec562798c87b",
		"qualification": "PUBLIC_CANONICAL_OPERATIONAL_RECORDING", "official_admission": "NOT_RUN",
		"native_exact_equality": "NOT_MEASURED", "native_version_load": "NOT_MEASURED",
	})
	binary := r.buildCanonicalSource(pinnedSource)
	nativeRoot := filepath.Join(root, "native-original")
	if err := os.Mkdir(nativeRoot, 0700); err != nil {
		t.Fatal(err)
	}
	var native b24NativeOutcome
	if !t.Run("canonical_native", func(t *testing.T) { native = b24RecordNative(t, binary, nativeRoot) }) {
		t.Fatal("actual canonical native control incomplete; no comparison credit")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_"+engine, func(t *testing.T) {
			plug := b24RunPublicCounterpart(t, engine, native.Target)
			if !reflect.DeepEqual(native.Selected, plug.Selected) || native.NonpositiveRefused != plug.NonpositiveRefused || native.Reason != plug.Reason || !native.BlankRefused || !plug.BlankRefused || !native.FutureExcluded || !plug.FutureExcluded || !native.Woke || !plug.ExplicitReopenReady || !native.PastWarned || !native.PastWritten || plug.TimedRequestsRefused != 2 {
				t.Fatalf("native/Plug supported public outcomes differ: native=%+v plug=%+v", native, plug)
			}
			b24Retain(t, "comparison-"+engine+".json", map[string]any{
				"native": native, "plug": plug, "disposition": "PASS_BOUNDED_PUBLIC_COMPARISON",
				"adopted_differences":   []string{"native notes append versus Plug ordered comment", "native timed wake versus Plug explicit reopen", "native past target writes/warns versus unsupported Plug until refusal", "native history-off no version writes versus always-on Plug ChangeSets"},
				"native_exact_equality": "NOT_MEASURED", "native_version_load": "NOT_MEASURED", "native_twin_pair": "NOT_RUN",
			})
		})
	}
}
