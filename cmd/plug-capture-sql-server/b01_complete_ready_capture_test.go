package main_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/captureclock"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

// This leaf uses the existing public fixture and capture executable. Its
// expectations come from the independently frozen B01 oracle, never from Plug
// output. The original native controls and their assertions are unchanged.
type b01ReadyCapture struct {
	f      *nativeFixture
	out    string
	seq    int
	roles  map[string]string
	points []b01ReadyPoint
	files  map[string]string
}

type b01ReadyPoint struct {
	Name         string              `json:"name"`
	GoTime       time.Time           `json:"go_time"`
	SQLTime      time.Time           `json:"sql_time"`
	ClockSHA256  string              `json:"clock_sha256"`
	Default      []string            `json:"default_roles"`
	Hybrid       []string            `json:"hybrid_roles"`
	Observations map[string][]string `json:"actual_ids"`
}

type b01ReadyState struct {
	Log    plugdump.SourceCommitLog        `json:"commit_log"`
	Tables map[string]plugdump.NativeTable `json:"tables"`
	Ignore plugdump.NativeTable            `json:"dolt_ignore"`
}

func (b *b01ReadyCapture) save(name string, data []byte) {
	b.f.t.Helper()
	if filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "../") {
		b.f.t.Fatal("noncanonical owned output name")
	}
	for _, part := range strings.Split(name, "/") {
		if part == "poll.log" || part == ".artifact" {
			b.f.t.Fatal("excluded output member")
		}
	}
	path := filepath.Join(b.out, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		b.f.t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		b.f.t.Fatal(err)
	}
	b.files[name] = fmt.Sprintf("%x", sha256.Sum256(data))
}

func (b *b01ReadyCapture) json(name string, value any) {
	b.f.t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		b.f.t.Fatal(err)
	}
	b.save(name, append(raw, '\n'))
}

func (b *b01ReadyCapture) run(name, binary string, args ...string) []byte {
	b.f.t.Helper()
	b.seq++
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = b.f.work, b.f.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now().UTC()
	err := cmd.Run()
	finished := time.Now().UTC()
	exit := 0
	if err != nil {
		exit = -1
		if e, ok := err.(*exec.ExitError); ok {
			exit = e.ExitCode()
		}
	}
	stem := fmt.Sprintf("commands/%03d-%s", b.seq, name)
	b.save(stem+".stdout", stdout.Bytes())
	b.save(stem+".stderr", stderr.Bytes())
	b.json(stem+".json", map[string]any{"argv": cmd.Args, "cwd": cmd.Dir, "started_at": started, "finished_at": finished, "exit_code": exit, "error": fmt.Sprint(err), "stdout_sha256": fmt.Sprintf("%x", sha256.Sum256(stdout.Bytes())), "stderr_sha256": fmt.Sprintf("%x", sha256.Sum256(stderr.Bytes()))})
	b.f.t.Logf("B01_NATIVE_COMMAND name=%s exit=%d argv=%q", name, exit, cmd.Args)
	if err != nil {
		b.f.t.Fatalf("native command %s: %v; stderr=%s", name, err, stderr.Bytes())
	}
	return stdout.Bytes()
}

func (b *b01ReadyCapture) create(role, title string, args ...string) string {
	b.f.t.Helper()
	argv := append([]string{"create", title, "--json"}, args...)
	var row struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(b.run("create-"+role, b.f.bd, argv...), &row); err != nil || row.ID == "" {
		b.f.t.Fatalf("public create identity %s: %+v %v", role, row, err)
	}
	b.roles[role] = row.ID
	return row.ID
}

func (b *b01ReadyCapture) state(name string) b01ReadyState {
	b.f.t.Helper()
	source, ctx := plugdump.SQLSource{DB: b.f.db}, context.Background()
	log, err := source.ReadSourceCommits(ctx)
	if err != nil {
		b.f.t.Fatal("actual source commit census", err)
	}
	names, err := source.Tables(ctx)
	if err != nil {
		b.f.t.Fatal(err)
	}
	state := b01ReadyState{Log: log, Tables: map[string]plugdump.NativeTable{}}
	for _, name := range names {
		table, err := source.ReadTable(ctx, name)
		if err != nil {
			b.f.t.Fatal("actual complete native table", name, err)
		}
		state.Tables[name] = canonicalPhysicalCaptureTable(table)
	}
	ignore, err := source.ReadSourceIgnore(ctx)
	if err != nil {
		b.f.t.Fatal(err)
	}
	state.Ignore = canonicalPhysicalCaptureTable(ignore)
	b.json(name+".json", state)
	return state
}

func (b *b01ReadyCapture) ready(name string, want []string, limit int, extra ...string) []string {
	b.f.t.Helper()
	argv := append([]string{"ready", "--json", "--limit", strconv.Itoa(limit)}, extra...)
	actual, err := ids(b.run(name, b.f.bd, argv...), false)
	if err != nil {
		b.f.t.Fatal(err)
	}
	expected := make([]string, len(want))
	for i, role := range want {
		expected[i] = b.roles[role]
		if expected[i] == "" {
			b.f.t.Fatal("oracle role has no create receipt", role)
		}
	}
	if limit > 0 && len(expected) > limit {
		expected = expected[:limit]
	}
	if !reflect.DeepEqual(actual, expected) {
		b.f.t.Fatalf("native %s got=%q want=%q", name, actual, expected)
	}
	return actual
}

func (b *b01ReadyCapture) point(name string, goTime, sqlTime time.Time, hybrid []string) {
	b.f.t.Helper()
	b.f.step(name, goTime, sqlTime)
	b.f.sqlClock(sqlTime)
	clockBytes, err := os.ReadFile(b.f.clock)
	if err != nil {
		b.f.t.Fatal(err)
	}
	b.save("clocks/"+name+"/clock.json", clockBytes)
	var sqlAt, sqlDefault string
	if err := b.f.db.QueryRow(`SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ'), DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%s.%fZ')`).Scan(&sqlAt, &sqlDefault); err != nil {
		b.f.t.Fatal(err)
	}
	b.json("clocks/"+name+"/actual-sql.json", map[string]string{"utc_timestamp_6": sqlAt, "utc_timestamp": sqlDefault})
	before := b.state("states/" + name + "-before")
	// This unchanged public inventory port reads the actual Go hook and SQL
	// clock, authenticating the same clock-file bytes used by ready commands.
	inventory := filepath.Join(b.out, "clocks", name, "inventory.json")
	b.run(name+"-clock-inventory", b.f.bd, "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventory)
	sidecar, err := os.ReadFile(inventory + ".sources/environment-and-flags.json")
	if err != nil {
		b.f.t.Fatal(err)
	}
	var observed struct {
		Clock struct {
			Input      captureclock.Snapshot `json:"input"`
			GoObserved time.Time             `json:"go_observed"`
			SQLResult  plugdump.Table        `json:"sql_result"`
			Columns    plugdump.Table        `json:"column_precision"`
		} `json:"capture_clock"`
	}
	if err := json.Unmarshal(sidecar, &observed); err != nil || observed.Clock.Input.Step != name || !observed.Clock.Input.GoTime.Equal(goTime) || !observed.Clock.Input.SQLTime.Equal(sqlTime) || !observed.Clock.GoObserved.Equal(goTime) || len(observed.Clock.SQLResult.Rows) != 1 || len(observed.Clock.Columns.Rows) != 6 {
		b.f.t.Fatalf("actual independent clock sidecar differs: %v %s", err, sidecar)
	}
	b.save("clocks/"+name+"/actual-clock-sidecar.json", sidecar)
	point := b01ReadyPoint{Name: name, GoTime: goTime, SQLTime: sqlTime, ClockSHA256: fmt.Sprintf("%x", sha256.Sum256(clockBytes)), Default: []string{"aging", "wisp", "control"}, Hybrid: hybrid, Observations: map[string][]string{}}
	for _, limit := range []int{0, 1, 2, 1000} {
		for _, sortBy := range []string{"default", "hybrid"} {
			args := []string{"--include-ephemeral", "--label", "active-clock"}
			want := point.Default
			if sortBy == "hybrid" {
				args = append(args, "--sort", "hybrid")
				want = hybrid
			}
			key := sortBy + "-" + strconv.Itoa(limit)
			point.Observations[key] = b.ready(name+"-"+key, want, limit, args...)
		}
		b.ready(name+"-durable-default-"+strconv.Itoa(limit), []string{"aging", "control"}, limit, "--label", "active-clock")
		durableHybrid := []string{"aging", "control"}
		if hybrid[0] == "wisp" {
			durableHybrid = []string{"control", "aging"}
		}
		b.ready(name+"-durable-hybrid-"+strconv.Itoa(limit), durableHybrid, limit, "--label", "active-clock", "--sort", "hybrid")
	}
	if after := b.state("states/" + name + "-after"); !equalPhysicalCaptureJSON(before, after) {
		b.f.t.Fatal("native ready/clock inventory changed source rows, schema, ignore or commit graph")
	}
	b.points = append(b.points, point)
	b.json("observations.json", map[string]any{"format": "b01-native-ready-observations/1", "roles": b.roles, "points": b.points})
}

func b01CopyFingerprint(t *testing.T, root, destination string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if entry.Name() == "poll.log" || entry.Name() == ".artifact" {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if destination != "" {
				return os.MkdirAll(filepath.Join(destination, rel), 0700)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular public source member")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[rel] = fmt.Sprintf("%x", sha256.Sum256(data))
		if destination != "" {
			return os.WriteFile(filepath.Join(destination, rel), data, 0600)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// The unchanged physical capture tool owns graph/image/delta construction.
// The only new adapter choice is its already supported --native-delta mode.
func (b *b01ReadyCapture) physicalSet(want b01ReadyState) {
	b.f.t.Helper()
	t := b.f.t
	dataDir := ""
	for _, entry := range b.f.env {
		if value, ok := strings.CutPrefix(entry, "BEADS_DOLT_DATA_DIR="); ok {
			dataDir = value
		}
	}
	if dataDir == "" {
		t.Fatal("actual native data directory absent")
	}
	copyDir, workspace := privateDir(t), privateDir(t)
	before := b01CopyFingerprint(t, filepath.Join(dataDir, "capture"), filepath.Join(copyDir, "capture"))
	if !reflect.DeepEqual(before, b01CopyFingerprint(t, filepath.Join(dataDir, "capture"), "")) || !reflect.DeepEqual(before, b01CopyFingerprint(t, filepath.Join(copyDir, "capture"), "")) {
		t.Fatal("quiescent public physical source copy changed")
	}
	beadsDir := filepath.Join(workspace, ".beads")
	metadata := b01CopyFingerprint(t, filepath.Join(b.f.work, ".beads"), beadsDir)
	if !reflect.DeepEqual(metadata, b01CopyFingerprint(t, filepath.Join(b.f.work, ".beads"), "")) || !reflect.DeepEqual(metadata, b01CopyFingerprint(t, beadsDir, "")) {
		t.Fatal("native workspace metadata copy changed")
	}
	b.json("source-byte-census.json", map[string]any{"database": before, "metadata": metadata})
	binary := filepath.Join(bazeltest.OverrideRoot(), os.Getenv("BEADS_PHYSICAL_CAPTURE_BINARY"))
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	b.json("capture-executable.json", map[string]any{"sha256": fmt.Sprintf("%x", sha256.Sum256(binaryBytes)), "bytes": len(binaryBytes), "source_base_commit": "b1c03e196f277b6821610af983499d49ecc366c5", "target": "//tools/native-physical-capture"})
	local := *b.f
	local.work, local.env = workspace, nil
	for _, entry := range b.f.env {
		key, _, _ := strings.Cut(entry, "=")
		if key != captureclock.FileEnv && key != captureclock.PublicEnv && key != "BEADS_DIR" && key != "BEADS_DOLT_DATA_DIR" {
			local.env = append(local.env, entry)
		}
	}
	local.env = append(local.env, "BEADS_DIR="+beadsDir, "BEADS_DOLT_DATA_DIR="+copyDir)
	output := filepath.Join(b.out, "physical")
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	tool := *b
	tool.f = &local
	tool.run("complete-physical-native-delta", binary, "--disposable", "--native-delta", "--data-dir", copyDir, "--database", "capture", "--beads-dir", beadsDir, "--output-dir", output)
	b.seq = tool.seq
	if !equalPhysicalCaptureJSON(want, b.state("states/final-after-capture")) {
		t.Fatal("physical capture changed original native state or graph")
	}
	rootBytes, err := os.ReadFile(filepath.Join(output, "capture-set/capture-set.json"))
	if err != nil {
		t.Fatal(err)
	}
	var root plugdump.NativeCaptureSet
	if err := json.Unmarshal(rootBytes, &root); err != nil || root.Format != plugdump.NativeCaptureSetFormat || root.Contract != plugdump.NativeDeltaFormat || root.Head != want.Log.Head || root.PartCount == 0 || root.Totals.Commits != uint64(len(want.Log.Commits)) {
		t.Fatalf("complete native capture root differs: %+v %v", root, err)
	}
	head, err := os.ReadFile(filepath.Join(output, "capture-set/head.tar"))
	if err != nil || root.HeadArchive.SHA256 != fmt.Sprintf("%x", sha256.Sum256(head)) || root.HeadArchive.Size != int64(len(head)) {
		t.Fatal("native capture head commitment differs", err)
	}
	manifest, err := plugdump.ValidateNativeDeltaHead(bytes.NewReader(head))
	if err != nil || manifest.SourcePlaneAddressContract != plugdump.SourcePlaneAddressContract || manifest.SourceFamilyContract != plugdump.SourceFamilyContract {
		t.Fatal("native head source/plane contract", err)
	}
	rows := map[string][]plugdump.Row{}
	reader := tar.NewReader(bytes.NewReader(head))
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(h.Name, "records/") {
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
			rows[record.Source.Table] = append(rows[record.Source.Table], record.Source.Fields)
		}
	}
	canonicalRows := func(rows []plugdump.Row) []string {
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			raw, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, string(raw))
		}
		sort.Strings(out)
		return out
	}
	for _, family := range []string{"issues", "dependencies", "wisps", "wisp_dependencies", "wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters"} {
		if len(want.Tables[family].Rows) == 0 || !reflect.DeepEqual(canonicalRows(rows[family]), canonicalRows(want.Tables[family].Rows)) {
			t.Fatalf("complete native capture live family differs: %s SQL=%d head=%d", family, len(want.Tables[family].Rows), len(rows[family]))
		}
	}
	b.json("capture-binding.json", map[string]any{"format": "b01-complete-ready-capture-binding/1", "capture_set_sha256": fmt.Sprintf("%x", sha256.Sum256(rootBytes)), "head_sha256": root.HeadArchive.SHA256, "head_commit": root.Head, "part_count": root.PartCount, "totals": root.Totals, "SQL_current_and_all_active_families_equal_head": true, "full_stream_authentication": "consumer must use existing dumpformat.ReadNativeCaptureSet", "controlled_embedded_SQL": false, "clock_evidence": "live persistent companion observations bound before immutable capture"})
}

func (b *b01ReadyCapture) pack() {
	b.f.t.Helper()
	files := b01CopyFingerprint(b.f.t, b.out, "")
	b.json("INNER-HASHES.json", files)
	path := filepath.Join(filepath.Dir(b.out), "b01-complete-ready-capture.tar.gz")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		b.f.t.Fatal(err)
	}
	compressed := gzip.NewWriter(file)
	writer := tar.NewWriter(compressed)
	names := make([]string, 0, len(files)+1)
	for name := range files {
		names = append(names, name)
	}
	names = append(names, "INNER-HASHES.json")
	sort.Strings(names)
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(b.out, name))
		if err != nil {
			b.f.t.Fatal(err)
		}
		if err := writer.WriteHeader(&tar.Header{Name: "b01-ready/" + filepath.ToSlash(name), Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			b.f.t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			b.f.t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		b.f.t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		b.f.t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.f.t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) > 20971520 {
		b.f.t.Fatal("complete committed public input exceeds its 20MiB contract", err)
	}
	b.f.t.Logf("B01_COMPLETE_READY_CAPTURE path=%s sha256=%x bytes=%d members=%d", path, sha256.Sum256(raw), len(raw), len(names))
}

func TestB01CompleteIndependentReadyCapture(t *testing.T) {
	f := newNativeFixtureForProject(t, "clockready")
	artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if artifacts == "" {
		t.Fatal("original Bazel output directory required")
	}
	b := &b01ReadyCapture{f: f, out: filepath.Join(artifacts, "b01-ready"), roles: map[string]string{}, files: map[string]string{}}
	if err := os.Mkdir(b.out, 0700); err != nil {
		t.Fatal(err)
	}
	boundary := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	type input struct {
		Role     string    `json:"role"`
		ID       string    `json:"id"`
		Title    string    `json:"title"`
		Created  time.Time `json:"created_at"`
		Priority int       `json:"priority"`
	}
	inputs := []input{
		{"old", "clockready-old", "old priority four", boundary.Add(-72 * time.Hour), 4},
		{"aging", "clockready-aging", "aging priority zero", boundary.Add(-48 * time.Hour), 0},
		{"tie-z", "clockready-tie-z", "recent tie z first insertion", boundary.Add(-time.Hour), 2},
		{"tie-a", "clockready-tie-a", "recent tie a second insertion", boundary.Add(-time.Hour), 2},
		{"a-later", "clockready-a-later", "later created sorts before ID", boundary.Add(-time.Hour + time.Second), 2},
	}
	b.json("operation-inputs.json", inputs)
	for _, in := range inputs {
		f.step("create-"+in.Role, in.Created, boundary.Add(10*time.Second))
		if b.create(in.Role, in.Title, "--id", in.ID, "--priority", strconv.Itoa(in.Priority), "--labels", "ready-clock") != in.ID {
			t.Fatal("public create changed explicit input identity")
		}
	}
	created := b.state("states/created")
	for _, in := range inputs {
		found := false
		for _, row := range created.Tables["issues"].Rows {
			if row["id"] != in.ID {
				continue
			}
			found = true
			for key, want := range map[string]any{"title": in.Title, "status": "open", "issue_type": "task", "priority": in.Priority, "created_at": in.Created.Format(time.RFC3339Nano), "updated_at": in.Created.Format(time.RFC3339Nano), "due_at": nil, "defer_until": nil} {
				if fmt.Sprint(row[key]) != fmt.Sprint(want) {
					t.Fatalf("actual public input readback %s.%s=%v want=%v", in.Role, key, row[key], want)
				}
			}
		}
		if !found {
			t.Fatal("declared public input missing", in.Role)
		}
	}
	priority := []string{"aging", "tie-a", "tie-z", "a-later", "old"}
	hybridAfter := []string{"tie-a", "tie-z", "a-later", "old", "aging"}
	oldest := []string{"old", "aging", "tie-a", "tie-z", "a-later"}
	for _, point := range []struct {
		name  string
		delta time.Duration
	}{
		{"48h-before", -time.Microsecond}, {"48h-at", 0}, {"48h-after", time.Microsecond},
		{"60s-before", time.Minute - time.Nanosecond}, {"60s-at", time.Minute}, {"60s-after", time.Minute + time.Nanosecond},
	} {
		f.step("go-only-"+point.name, boundary.Add(point.delta), boundary.Add(10*time.Second))
		f.sqlClock(boundary.Add(10 * time.Second))
		hybrid := priority
		if point.delta > 0 {
			hybrid = hybridAfter
		}
		for _, policy := range []struct {
			name string
			want []string
		}{{"default", priority}, {"priority", priority}, {"hybrid", hybrid}, {"oldest", oldest}} {
			for _, limit := range []int{0, 1, 2, 1000} {
				args := []string{"--label", "ready-clock"}
				if policy.name != "default" {
					args = append(args, "--sort", policy.name)
				}
				b.ready(point.name+"-"+policy.name+"-"+strconv.Itoa(limit), policy.want, limit, args...)
			}
		}
	}
	for _, point := range []struct {
		name  string
		delta time.Duration
	}{{"before", -time.Microsecond}, {"at", 0}, {"after", time.Microsecond}} {
		f.step("sql-only-"+point.name, boundary, boundary.Add(point.delta))
		f.sqlClock(boundary.Add(point.delta))
		b.ready("sql-only-"+point.name, priority, 1000, "--label", "ready-clock", "--sort", "hybrid")
	}
	if !equalPhysicalCaptureJSON(created, b.state("states/five-role-read-only-final")) {
		t.Fatal("five-role default/hybrid/oldest or SQL-only reads changed source")
	}
	f.step("active-create", boundary.Add(-time.Hour), boundary.Add(10*time.Second))
	wisp := b.create("wisp", "active recent priority two", "--ephemeral", "--priority", "2", "--labels", "active-clock")
	blocked := b.create("blocked", "active blocked priority zero", "--ephemeral", "--priority", "0", "--labels", "active-clock")
	control := b.create("control", "ordinary priority four companion", "--id", "clockready-control", "--priority", "4", "--labels", "active-clock")
	blocker := b.create("blocker", "started ordinary blocker", "--id", "clockready-blocker")
	b.run("start-blocker", f.bd, "update", blocker, "--status", "in_progress", "--json")
	b.run("active-blocked-by-durable", f.bd, "dep", "add", blocked, blocker, "--type", "blocks", "--json")
	b.run("include-aging-in-active-clock", f.bd, "label", "add", b.roles["aging"], "active-clock", "--json")
	b.create("ordinary_child", "ordinary child of active parent", "--parent", wisp, "--no-inherit-labels", "--labels", "active-child")
	b.create("active_child", "active child of ordinary parent", "--ephemeral", "--parent", control, "--no-inherit-labels", "--labels", "active-child")
	b.run("retain-genuine-active-comment", f.bd, "comments", "add", wisp, "retained B01 active comment", "--author", "capture", "--json")
	b.json("active-operation-inputs.json", b.roles)
	active := b.state("states/active-placement")
	for _, role := range []string{"wisp", "blocked", "active_child"} {
		found := false
		for _, row := range active.Tables["wisps"].Rows {
			if row["id"] == b.roles[role] {
				found = true
			}
		}
		if !found {
			t.Fatal("public active input missing from genuine wisps family", role)
		}
	}
	for _, role := range []string{"control", "blocker", "ordinary_child"} {
		found := false
		for _, row := range active.Tables["issues"].Rows {
			if row["id"] == b.roles[role] {
				found = true
			}
		}
		if !found {
			t.Fatal("public durable input missing from issues family", role)
		}
	}
	for _, edge := range []struct{ family, from, to, arm, kind string }{
		{"wisp_dependencies", "blocked", "blocker", "depends_on_issue_id", "blocks"},
		{"dependencies", "ordinary_child", "wisp", "depends_on_wisp_id", "parent-child"},
		{"wisp_dependencies", "active_child", "control", "depends_on_issue_id", "parent-child"},
	} {
		found := false
		for _, row := range active.Tables[edge.family].Rows {
			if row["issue_id"] == b.roles[edge.from] && row[edge.arm] == b.roles[edge.to] && row["type"] == edge.kind {
				for _, arm := range []string{"depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
					if arm != edge.arm && row[arm] != nil {
						t.Fatal("conflicting native physical target arm", row)
					}
				}
				found = true
			}
		}
		if !found {
			t.Fatal("actual native typed cross-plane edge absent", edge)
		}
	}
	for _, point := range []struct {
		name            string
		goTime, sqlTime time.Time
		hybrid          []string
	}{
		{"mixed-before", boundary.Add(-time.Microsecond), boundary.Add(7 * time.Hour), []string{"aging", "wisp", "control"}},
		{"mixed-at", boundary, boundary.Add(7 * time.Hour), []string{"aging", "wisp", "control"}},
		{"mixed-after", boundary.Add(time.Microsecond), boundary.Add(7 * time.Hour), []string{"wisp", "control", "aging"}},
		{"mixed-sql-only", boundary.Add(time.Microsecond), boundary.Add(7*time.Hour + time.Second), []string{"wisp", "control", "aging"}},
		{"mixed-60s-before", boundary.Add(time.Minute - time.Nanosecond), boundary.Add(7*time.Hour + time.Second), []string{"wisp", "control", "aging"}},
		{"mixed-60s-at", boundary.Add(time.Minute), boundary.Add(7*time.Hour + time.Second), []string{"wisp", "control", "aging"}},
		{"mixed-60s-after", boundary.Add(time.Minute + time.Nanosecond), boundary.Add(7*time.Hour + time.Second), []string{"wisp", "control", "aging"}},
	} {
		b.point(point.name, point.goTime, point.sqlTime, point.hybrid)
	}
	for _, limit := range []int{0, 1, 2, 1000} {
		b.ready("ordinary-child-of-active-"+strconv.Itoa(limit), []string{"ordinary_child"}, limit, "--parent", wisp, "--label", "active-child")
		b.ready("active-child-of-ordinary-"+strconv.Itoa(limit), []string{"active_child"}, limit, "--include-ephemeral", "--parent", control, "--label", "active-child")
	}
	b.ready("unknown-parent", []string{}, 0, "--include-ephemeral", "--parent", "clockready-missing")
	b.run("close-durable-blocker", f.bd, "close", blocker, "--json")
	for _, limit := range []int{0, 1, 2, 1000} {
		b.ready("active-unblocked-"+strconv.Itoa(limit), []string{"aging", "blocked", "wisp", "control"}, limit, "--include-ephemeral", "--label", "active-clock")
	}
	b.run("reopen-durable-blocker", f.bd, "reopen", blocker, "--json")
	b.ready("active-reblocked", []string{"aging", "wisp", "control"}, 0, "--include-ephemeral", "--label", "active-clock")
	b.json("sort-contract.json", map[string]any{"public_default": "priority", "lower_level_empty_policy": "hybrid", "old_bucket_rank": 999, "native_stored_lag_seconds": 0, "plug_p74_max_lag_seconds": 60, "native_created_precision_ns": 1000000000, "native_go_input_tick_ns": 1, "native_SQL_tick_ns": 1000})
	b.physicalSet(b.state("states/final-before-capture"))
	b.pack()
}
