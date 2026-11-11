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

// This records the real canonical native interface, independently of the Plug
// fixed-guard oracle. The existing B20 owner supplies its unchanged build recipe.
type b08Native struct {
	t                           *testing.T
	binary, dir, root, database string
	env                         []string
	seq                         int
	lastStderr                  string
}

func (n *b08Native) command(label string, refuse bool, args ...string) []byte {
	n.t.Helper()
	n.seq++
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir, cmd.Env = n.dir, append([]string(nil), n.env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now().UTC()
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -1
		var failure *exec.ExitError
		if errors.As(err, &failure) {
			exit = failure.ExitCode()
		}
	}
	b08Save(n.t, n.root, fmt.Sprintf("%04d-%s-command.json", n.seq, label), map[string]any{"argv": cmd.Args, "dir": n.dir, "environment": n.env, "exit": exit, "started": start, "ended": time.Now().UTC(), "stdout": string(stdout.Bytes()), "stderr": string(stderr.Bytes()), "error": fmt.Sprint(err)})
	if ctx.Err() != nil || exit < 0 || refuse && exit == 0 || !refuse && exit != 0 {
		n.t.Fatalf("native %s exit=%d error=%v stdout=%s stderr=%s", label, exit, err, stdout.Bytes(), stderr.Bytes())
	}
	n.lastStderr = stderr.String()
	return append([]byte(nil), stdout.Bytes()...)
}

func (n *b08Native) rows(label, query string) []map[string]any {
	n.t.Helper()
	// Parse stdout only; command.json retains both original channels.
	raw := n.command(label, false, "--readonly", "sql", query, "--json")
	var rows []map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&rows); err != nil {
		n.t.Fatalf("native SQL %s: %v: %s", label, err, raw)
	}
	return rows
}

func b08NativeValue(row map[string]any, name string) any {
	for key, value := range row {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return nil
}

func b08NativeSubjectImage(t *testing.T, image map[string][]string, id string) (string, map[string]any) {
	t.Helper()
	var found map[string]any
	selected := ""
	for _, table := range []string{"issues", "wisps"} {
		for _, raw := range image[table] {
			var row map[string]any
			d := json.NewDecoder(strings.NewReader(raw))
			d.UseNumber()
			if err := d.Decode(&row); err != nil {
				t.Fatal(err)
			}
			if b08NativeValue(row, "id") == "b08-"+id {
				if found != nil {
					t.Fatal("native identity exists in both planes")
				}
				selected, found = table, row
			}
		}
	}
	if found == nil {
		t.Fatal("native subject missing from full physical census", id)
	}
	return selected, found
}

func b08NativeCloseEvents(t *testing.T, image map[string][]string, table, id string) int {
	t.Helper()
	events := "events"
	if table == "wisps" {
		events = "wisp_events"
	}
	rows, exists := image[events]
	if !exists {
		t.Fatal("native event table absent", events)
	}
	count := 0
	for _, raw := range rows {
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			t.Fatal(err)
		}
		if b08NativeValue(row, "issue_id") == "b08-"+id && b08NativeValue(row, "event_type") == "closed" {
			count++
		}
	}
	return count
}

func (n *b08Native) image(label string) map[string][]string {
	n.t.Helper()
	catalog := n.rows(label+"-catalog", "SELECT table_name AS name FROM information_schema.tables WHERE table_schema='"+n.database+"' AND table_type='BASE TABLE' ORDER BY table_name")
	if len(catalog) == 0 {
		n.t.Fatal("native SQL base-table census is empty")
	}
	image := map[string][]string{}
	for _, row := range catalog {
		table, ok := b08NativeValue(row, "name").(string)
		if !ok || table == "" {
			n.t.Fatal("missing actual SQL table name")
		}
		for _, c := range table {
			if c != '_' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
				n.t.Fatal("unsafe native SQL table name")
			}
		}
		image[table] = []string{}
		for _, value := range n.rows(label+"-table-"+table, "SELECT * FROM `"+table+"`") {
			data, err := json.Marshal(value)
			if err != nil {
				n.t.Fatal(err)
			}
			image[table] = append(image[table], string(data))
		}
		sort.Strings(image[table])
	}
	image["@dolt_log"] = []string{}
	for _, row := range n.rows(label+"-history", "SELECT * FROM dolt_log ORDER BY commit_hash") {
		data, err := json.Marshal(row)
		if err != nil {
			n.t.Fatal(err)
		}
		image["@dolt_log"] = append(image["@dolt_log"], string(data))
	}
	b08Save(n.t, n.root, label+"-physical-image.json", image)
	return image
}

func (n *b08Native) create(id string, ephemeral bool) {
	n.t.Helper()
	args := []string{"create", "--id", "b08-" + id, "--title", id, "--type", "task", "--force", "--json"}
	if ephemeral {
		args = append(args, "--ephemeral")
	}
	n.command("create-"+id, false, args...)
}

func (n *b08Native) setup(label, sql string) {
	n.command(label, false, "sql", sql, "--json")
}

func (n *b08Native) close(label, id string, force bool, refusal string) {
	n.t.Helper()
	before := n.image(label + "-before")
	args := []string{"close", "b08-" + id, "--reason", "B08 " + label, "--json"}
	if force {
		args = append(args, "--force")
	}
	raw := n.command(label, refusal != "", args...)
	refusalText := string(raw) + "\n" + n.lastStderr
	after := n.image(label + "-after")
	if refusal != "" {
		if !strings.Contains(strings.ToLower(refusalText), refusal) || !reflect.DeepEqual(before, after) {
			n.t.Fatalf("native refusal %s missing exact guard/no-write conservation: %s", label, refusalText)
		}
	} else {
		plane, first := b08NativeSubjectImage(n.t, before, id)
		afterPlane, last := b08NativeSubjectImage(n.t, after, id)
		if plane != afterPlane || b08NativeValue(last, "status") != "closed" {
			n.t.Fatal("native close lost subject identity or closed status")
		}
		delta := b08NativeCloseEvents(n.t, after, plane, id) - b08NativeCloseEvents(n.t, before, plane, id)
		if b08NativeValue(first, "status") == "closed" {
			if !reflect.DeepEqual(first, last) || delta != 0 {
				n.t.Fatal("native reclose rewrote the first subject or emitted a duplicate close event")
			}
		} else if delta != 1 || b08NativeValue(last, "close_reason") != "B08 "+label || b08NativeValue(last, "closed_at") == nil {
			n.t.Fatal("native close did not publish exactly one close event and exact stored values", delta)
		}
	}
	changed := []string{}
	for name, rows := range before {
		if !reflect.DeepEqual(rows, after[name]) {
			changed = append(changed, name)
		}
	}
	sort.Strings(changed)
	b08Save(n.t, n.root, label+"-native-outcome.json", map[string]any{"expected_native_refusal": refusal, "force": force, "changed_tables": changed, "original_response": string(raw), "native_pin": pinnedSource.Commit, "plug_force": "undeclared parameter: typed no-write refusal; native force is not transferred"})
}

func (n *b08Native) subject(label, table, id string) map[string]any {
	n.t.Helper()
	rows := n.rows(label, "SELECT id,status,assignee,pinned,is_template,closed_at,close_reason FROM "+table+" WHERE id='b08-"+id+"'")
	if len(rows) != 1 {
		n.t.Fatal("native subject absent or duplicated", table, id)
	}
	return rows[0]
}

func (n *b08Native) ready(label string, want []string) {
	n.t.Helper()
	raw := n.command(label, false, "ready", "--include-ephemeral", "--label", "b08-settlement", "--sort", "priority", "--limit", "0", "--json")
	var rows []struct{ ID string }
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&rows); err != nil {
		n.t.Fatal(err)
	}
	got := []string{}
	for _, row := range rows {
		got = append(got, row.ID)
	}
	expected := []string{}
	for _, id := range want {
		expected = append(expected, "b08-"+id)
	}
	sort.Strings(got)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		n.t.Fatalf("native %s ready=%v want=%v", label, got, expected)
	}
	b08Save(n.t, n.root, label+"-ready-expectation.json", map[string]any{"actual_ids": got, "independent_expected_ids": expected})
}

func TestB08NativeOriginalGuardsAndLifecycle(t *testing.T) {
	root := b08Root(t, "b08-canonical-native")
	r := &nativeRecorder{t: t, root: root, target: "//cmd/bd:bd"}
	binary := r.buildCanonicalSource(pinnedSource)
	server := publicnative.StartDolt(t, root)
	work := t.TempDir()
	for _, name := range []string{"home", "project", "tmp"} {
		if err := os.Mkdir(filepath.Join(work, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	env := server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + filepath.Join(work, "home"), "TMPDIR=" + filepath.Join(work, "tmp"), "USER=b08-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B08 Public", "GIT_AUTHOR_EMAIL=b08@example.invalid", "GIT_COMMITTER_NAME=B08 Public", "GIT_COMMITTER_EMAIL=b08@example.invalid", "BEADS_ACTOR=b08-owner", "BEADS_NO_DAEMON=1"})
	n := &b08Native{t: t, binary: "/usr/bin/git", dir: filepath.Join(work, "project"), root: root, database: "b08_current", env: env}
	n.command("workspace-init", false, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	var version struct{ Commit, Build string }
	if json.NewDecoder(bytes.NewReader(n.command("native-version", false, "version", "--json"))).Decode(&version) != nil || version.Commit != pinnedSource.Commit || version.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("native executable linked identity differs from the declared canonical source")
	}
	n.command("native-init", false, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", n.database, "--prefix", "b08", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, n.database)
	n.command("native-history-off", false, "config", "set", "versioned-history.enabled", "false", "--json")
	n.command("native-history-readback", false, "config", "get", "versioned-history.enabled", "--json")

	n.create("template", false)
	n.setup("template-multiple-guards", "UPDATE issues SET is_template=1,pinned=1,assignee='foreign-owner' WHERE id='b08-template'")
	n.close("template-first", "template", false, "template")
	n.close("force-never-template", "template", true, "template")
	for _, spelling := range []string{"column", "status"} {
		id := "pin-" + spelling
		n.create(id, false)
		field := "pinned=1"
		if spelling == "status" {
			field = "status='pinned'"
		}
		n.setup(id+"-setup", "UPDATE issues SET "+field+" WHERE id='b08-"+id+"'")
		n.close(id+"-refusal", id, false, "pinned")
		n.close(id+"-force", id, true, "")
		first := n.subject(id+"-first-close", "issues", id)
		n.close(id+"-plain-idempotent-reclose", id, false, "")
		if second := n.subject(id+"-second-close", "issues", id); !reflect.DeepEqual(first, second) || b08NativeValue(first, "closed_at") == nil || b08NativeValue(first, "status") != "closed" {
			t.Fatal("native plain re-close lost first close or re-applied pin guards")
		}
	}
	n.create("foreign-holder", false)
	n.setup("foreign-holder-setup", "UPDATE issues SET assignee='foreign-owner' WHERE id='b08-foreign-holder'")
	n.close("foreign-holder-refusal", "foreign-holder", false, "foreign-owner")
	n.close("foreign-holder-force", "foreign-holder", true, "")
	n.command("foreign-holder-reopen", false, "reopen", "b08-foreign-holder", "--json")
	if row := n.subject("holder-retained", "issues", "foreign-holder"); b08NativeValue(row, "assignee") != "foreign-owner" || b08NativeValue(row, "status") != "open" || b08NativeValue(row, "closed_at") != nil {
		t.Fatal("native close/reopen lost the original assignee")
	}

	n.create("parent", false)
	n.create("durable-child", false)
	n.create("active-child", true)
	for _, id := range []string{"durable-child", "active-child"} {
		n.command("parent-"+id, false, "update", "b08-"+id, "--parent", "b08-parent", "--json")
	}
	n.setup("pin-child", "UPDATE issues SET status='pinned' WHERE id='b08-durable-child'")
	n.close("both-planes-open-children", "parent", false, "open child")
	n.close("force-open-children", "parent", true, "")
	n.close("preclosed-still-open-children", "parent", false, "open child")
	n.close("close-pinned-child", "durable-child", true, "")
	n.close("close-active-child", "active-child", false, "")
	n.close("preclosed-no-children", "parent", false, "")

	n.create("stale-blocked", false)
	n.setup("stale-blocked-setup", "UPDATE issues SET is_blocked=1 WHERE id='b08-stale-blocked'")
	n.close("stale-blocked-no-direct-blocker", "stale-blocked", false, "")
	// The timer uses an actual far-future stored creation time, with an
	// ordinary short timeout. No external service or clock substitution runs.
	n.create("timer-gate", false)
	n.setup("timer-gate-setup", "UPDATE issues SET issue_type='gate',await_type='timer',timeout_ns=3600000000000,created_at='2099-01-01 00:00:00' WHERE id='b08-timer-gate'")
	n.close("unsatisfied-native-gate", "timer-gate", false, "gate condition not satisfied")
	n.close("force-native-gate", "timer-gate", true, "")

	for _, id := range []string{"settle-blocker", "settle-direct", "settle-transitive"} {
		n.create(id, id == "settle-direct")
		n.command("label-"+id, false, "label", "add", "b08-"+id, "b08-settlement", "--json")
	}
	n.command("native-direct-edge", false, "dep", "add", "b08-settle-direct", "b08-settle-blocker", "--type", "blocks", "--json")
	n.command("native-transitive-edge", false, "dep", "add", "b08-settle-transitive", "b08-settle-direct", "--type", "blocks", "--json")
	n.ready("native-before-settlement", []string{"settle-blocker"})
	n.close("native-live-blocker", "settle-direct", false, "blocked")
	n.close("native-blocker-close", "settle-blocker", false, "")
	n.ready("native-direct-ready", []string{"settle-direct"})
	n.command("native-blocker-reopen", false, "reopen", "b08-settle-blocker", "--json")
	n.ready("native-reopen-reblocks", []string{"settle-blocker"})
	n.close("native-blocker-reclose", "settle-blocker", false, "")
	n.close("native-direct-close", "settle-direct", false, "")
	n.ready("native-transitive-ready", []string{"settle-transitive"})
	n.command("native-final-blocker-reopen", false, "reopen", "b08-settle-blocker", "--json")
	n.ready("native-closed-direct-still-satisfies", []string{"settle-blocker", "settle-transitive"})
	if row := n.subject("native-wisp-retained-closed", "wisps", "settle-direct"); b08NativeValue(row, "status") != "closed" {
		t.Fatal("canonical native closed wisp was not retained")
	}
	b08Save(t, root, "qualification-boundary.json", map[string]any{"native_source": pinnedSource, "native_clock": "ordinary Go and SQL clocks; no equality required by these guard cases", "native_closed_wisp": "retained physical row", "plug_completed_ephemeral": "removed provider item; reopen remains contract-refused", "force_pin_holder": "native literals only; Plug P23b fixed guards and strict undeclared-force refusal", "original_channels": "each command preserves stdout and stderr independently", "official": "UNSPENT", "private": "HARD_DISABLED"})
}
