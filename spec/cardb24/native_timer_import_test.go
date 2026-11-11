package cardb20

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// Existing B09 capture-only source. It observes the canonical database; every
// behavioral command below still uses the unchanged canonical 9d6fff binary.
var b24TimerCaptureSource = nativeSource{
	"beads-446dbb", "446dbbdaf978385099aeb8625e08016889fffccf",
	"64f0d22c8622c2a20da2bfe8c3ad49458fd12b06", "a367be65df60baa1ee8444f1e437904122f25f87",
	"32034b6d242f252144f6353f0d9d2f8b7d50739faf7973d62de93cafe25f3959",
	"a2456b25849b281d86ac57a461e21e0cc244cd80602c20269d88e7b9e28bc246", 5158,
}

func b24RecordTimerSource(t *testing.T, r *nativeRecorder, binary, exporter string) []byte {
	t.Helper()
	r = &nativeRecorder{t: t, root: r.root, target: r.target}
	workspace := t.TempDir()
	for _, name := range []string{"home", "project", "tmp"} {
		if err := os.Mkdir(filepath.Join(workspace, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, r.root)
	env := []string{
		"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + filepath.Join(workspace, "home"),
		"TMPDIR=" + filepath.Join(workspace, "tmp"), "USER=b24-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true",
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B24 Public", "GIT_AUTHOR_EMAIL=b24@example.invalid",
		"GIT_COMMITTER_NAME=B24 Public", "GIT_COMMITTER_EMAIL=b24@example.invalid", "BEADS_ACTOR=b24-public", "BEADS_NO_DAEMON=1",
	}
	n := &b24Native{t: t, r: r, binary: "/usr/bin/git", dir: filepath.Join(workspace, "project"), env: server.ChildEnv(env)}
	n.command("timer-workspace-git-init", false, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	var identity struct{ Commit, Build string }
	if json.Unmarshal(n.command("timer-native-version", false, "version", "--json"), &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("canonical timer executable identity mismatch")
	}
	n.command("timer-native-init", false, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b24_current", "--prefix", "b24", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b24_current")
	n.command("timer-history-off-set", false, "config", "set", "versioned-history.enabled", "false", "--json")
	var effective struct{ Key, Value string }
	if json.Unmarshal(n.command("timer-history-off-get", false, "config", "get", "versioned-history.enabled", "--json"), &effective) != nil || effective.Key != "versioned-history.enabled" || effective.Value != "false" {
		t.Fatal("effective history-off timer config missing")
	}
	n.clock("timer-create-sql-before")
	created := n.command("timer-create-due", false, "create", "--id", "b24-timer", "--title", "Scheduled task", "--type", "task", "--priority", "2", "--due", "2099-01-15", "--json")
	var bead map[string]any
	decoder := json.NewDecoder(bytes.NewReader(created))
	decoder.UseNumber()
	if decoder.Decode(&bead) != nil || bead["id"] != "b24-timer" {
		t.Fatalf("canonical create did not return the requested timer bead: %s", created)
	}
	due := time.Date(2099, 1, 15, 0, 0, 0, 0, time.UTC)
	deferUntil := time.Date(2099, 6, 15, 0, 0, 0, 0, time.UTC)
	b24TimerValue(t, "create.due_at", bead["due_at"], &due)
	n.command("timer-update-defer", false, "update", "b24-timer", "--defer", "2099-06-15", "--json")
	n.command("timer-unrelated-title-update", false, "update", "b24-timer", "--title", "Scheduled task (updated)", "--json")
	rows := n.sql("timer-physical-dates", "SELECT id,title,status,due_at,defer_until,updated_at FROM issues WHERE id='b24-timer'")
	if len(rows) != 1 || rows[0]["status"] != "deferred" || rows[0]["title"] != "Scheduled task (updated)" {
		t.Fatalf("canonical independent timer oracle mismatch: %+v", rows)
	}
	b24TimerValue(t, "SQL.due_at", b24Value(rows[0], "due_at"), &due)
	b24TimerValue(t, "SQL.defer_until", b24Value(rows[0], "defer_until"), &deferUntil)
	n.sql("timer-column-precision", "SELECT table_name,column_name,data_type,datetime_precision FROM information_schema.columns WHERE table_schema='b24_current' AND column_name IN ('due_at','defer_until','updated_at') ORDER BY table_name,ordinal_position")
	n.clock("timer-update-sql-after")
	n.conserved("timer-canonical-jsonl-export", func() {
		n.command("timer-canonical-export", false, "--readonly", "export", "--format", "jsonl", "--output", filepath.Join(r.root, "canonical-timers.jsonl"))
	})
	// Use only the existing capture recipe, on the same quiescent public store.
	// This is neither a new producer nor official/trusted bundle admission.
	inventoryPath := filepath.Join(r.root, "timer-inventory.json")
	archivePath := filepath.Join(r.root, "timer-native-dump.tar")
	n.conserved("timer-readonly-capture", func() {
		capture := *n
		capture.binary = exporter
		capture.command("timer-exporter-version", false, "version", "--json")
		capture.command("timer-inventory", false, "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryPath)
		capture.command("timer-dump", false, "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "--output", archivePath)
	})
	for _, path := range []string{inventoryPath, archivePath} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			t.Fatalf("public capture is not a private regular original: %s %v", path, err)
		}
	}
	data, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	want := b24TimerOracle{ID: "b24-timer", Project: "b24", Title: "Scheduled task (updated)", Status: "backlog", Due: &due, Defer: &deferUntil}
	b24TimerSource(t, data, want)
	// Further canonical lifecycle controls operate after the immutable capture,
	// preserving an independent, non-NULL import fixture from the actual source.
	n.conserved("timer-blank-reason", func() {
		n.command("timer-blank-defer", true, "defer", "b24-timer", "--reason", " ", "--json")
		stderr, err := os.ReadFile(filepath.Join(r.root, "timer-blank-defer.stderr"))
		stdout, outErr := os.ReadFile(filepath.Join(r.root, "timer-blank-defer.stdout"))
		if err != nil || outErr != nil || !bytes.Contains(append(stdout, stderr...), []byte("reason cannot be empty")) {
			t.Fatal("native blank reason did not reach validation")
		}
	})
	n.command("timer-seed-notes", false, "update", "b24-timer", "--notes", "existing timer notes", "--json")
	appended := b24Rows(t, n.command("timer-reason-append", false, "defer", "b24-timer", "--reason", "  waiting for timer input  ", "--until", "2099-06-15", "--json"))
	if len(appended) != 1 || appended[0]["notes"] != "existing timer notes\nwaiting for timer input" {
		t.Fatalf("exact canonical timer reason append: %+v", appended)
	}
	b24TimerValue(t, "reason.due_at", appended[0]["due_at"], &due)
	n.conserved("timer-future-ready", func() {
		if !n.clock("timer-future-sql-before").Before(deferUntil) {
			t.Fatal("native future timer not in future")
		}
		if b24HasID(b24NativeIDs(t, n.command("timer-future-ready-public", false, "ready", "--limit", "100", "--json")), "b24-timer") {
			t.Fatal("future native timer became ready")
		}
	})
	n.command("timer-past-target", false, "update", "b24-timer", "--defer", "2001-01-01")
	stderr, err := os.ReadFile(filepath.Join(r.root, "timer-past-target.stderr"))
	if err != nil || !bytes.Contains(stderr, []byte("in the past")) {
		t.Fatal("canonical past timer did not warn")
	}
	rows = n.sql("timer-deliberate-past-write", "SELECT id,status,due_at,defer_until FROM issues WHERE id='b24-timer'")
	past := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	if len(rows) != 1 || rows[0]["status"] != "deferred" {
		t.Fatal("past timer write lost deferred state before wake")
	}
	b24TimerValue(t, "past.defer_until", b24Value(rows[0], "defer_until"), &past)
	versions := n.sql("timer-version-rows-before-wake", "SELECT * FROM issue_versions")
	n.clock("timer-expired-wake-sql-before")
	if !b24HasID(b24NativeIDs(t, n.command("timer-expired-wake-public", false, "ready", "--limit", "100", "--json")), "b24-timer") {
		t.Fatal("actual expired native timer did not wake")
	}
	n.clock("timer-expired-wake-sql-after")
	rows = n.sql("timer-wake-readback", "SELECT id,status,due_at,defer_until,updated_at FROM issues WHERE id='b24-timer'")
	if len(rows) != 1 || rows[0]["status"] != "open" || b24Value(rows[0], "defer_until") != nil {
		t.Fatalf("native wake readback: %+v", rows)
	}
	b24TimerValue(t, "wake.due_at", b24Value(rows[0], "due_at"), &due)
	if len(versions) != 0 || !reflect.DeepEqual(versions, n.sql("timer-version-rows-after-wake", "SELECT * FROM issue_versions")) {
		t.Fatal("history-off native wake minted or changed version rows")
	}
	r.writeJSON(filepath.Join(r.root, "timer-native-limits.json"), map[string]any{
		"native_source": pinnedSource, "capture_source": b24TimerCaptureSource, "archive_sha256": hashBytes(data), "oracle": want,
		"history_off_version_write": "NO_MINT_ACTUAL_ROWS", "history_off_version_read": "NOT_MEASURED",
		"version_read_missing_interface": "canonical public CLI has no per-operation issue_versions query/read trace",
		"exact_native_stale_go_cutoff":   "NOT_MEASURED", "exact_native_wake_sql_equality": "NOT_MEASURED",
		"clock_missing_interface": "stale.go:25,43 private time.Now cutoff; wake_defers.go:83,119 in-statement UTC_TIMESTAMP evaluation",
		"observed_clocks":         "actual SQL expressions, physical column precision, command wall intervals, actual persisted Go updated_at",
		"official_admission":      "NOT_RUN",
	})
	t.Logf("B24_NATIVE_TIMER due=%s defer=%s archive=%s no_mint=PASS version_read=NOT_MEASURED exact_equality=NOT_MEASURED", due, deferUntil, hashBytes(data))
	return data
}

func TestB24CanonicalNativeDueDeferImportDurable(t *testing.T) {
	root, checkout := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("RunsOn checkout/output binding required")
	}
	r := &nativeRecorder{t: t, root: root, target: "//cmd/bd:bd"}
	for _, key := range []string{"PLUG_B24_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing request/control %s", key)
		}
	}
	head := strings.TrimSpace(string(r.buildCommand(checkout, "timer-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if head != os.Getenv("PLUG_B24_REQUEST_SHA") {
		t.Fatal("requested native timer candidate differs from checkout")
	}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("unchanged canonical input protocol differs")
	}
	r.writeJSON(filepath.Join(root, "timer-request-run-binding.json"), map[string]any{"candidate": head, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "native_source": pinnedSource, "capture_source": b24TimerCaptureSource, "shared_recipe_sha256": "0003df7485b16dc2ad3ace5533194f47b748ab15319e13698365ec562798c87b"})
	binary := r.buildCanonicalSource(pinnedSource)
	exporter := r.buildCanonicalSource(b24TimerCaptureSource)
	var data []byte
	if !t.Run("canonical_native_due_defer_capture", func(t *testing.T) { data = b24RecordTimerSource(t, r, binary, exporter) }) {
		t.Fatal("canonical native timer original incomplete")
	}
	due, until := time.Date(2099, 1, 15, 0, 0, 0, 0, time.UTC), time.Date(2099, 6, 15, 0, 0, 0, 0, time.UTC)
	want := b24TimerOracle{ID: "b24-timer", Project: "b24", Title: "Scheduled task (updated)", Status: "backlog", Due: &due, Defer: &until}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_import_"+engine, func(t *testing.T) { b24RunTimerImport(t, engine, data, want) })
	}
}
