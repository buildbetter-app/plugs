package main_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/captureclock"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/testutil/bazeltest"
)

type nativeFixture struct {
	t                          *testing.T
	work, clock, bd, companion string
	env                        []string
	db                         *sql.DB
}

func privateDir(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func (f *nativeFixture) run(binary string, args ...string) []byte {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = f.work, f.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	f.t.Logf("NATIVE_CAPTURE_COMMAND executable=%s argv=%q error=%v stdout=%s stderr=%s", filepath.Base(binary), args, err, stdout.Bytes(), stderr.Bytes())
	if err != nil {
		f.t.Fatalf("actual native command: %v", err)
	}
	return stdout.Bytes()
}

func (f *nativeFixture) start(binary string, args ...string) {
	f.t.Helper()
	logPath := filepath.Join(privateDir(f.t), "process.log")
	log, err := os.Create(logPath)
	if err != nil {
		f.t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = f.work, f.env, log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		f.t.Fatal(err)
	}
	f.t.Logf("NATIVE_CAPTURE_PROCESS executable=%s argv=%q pid=%d", filepath.Base(binary), args, cmd.Process.Pid)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	f.t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		if err := log.Close(); err != nil {
			f.t.Error(err)
		}
		data, err := os.ReadFile(logPath)
		if err != nil {
			f.t.Error(err)
		} else {
			f.t.Logf("NATIVE_CAPTURE_PROCESS_LOG executable=%s sha256=%x data=%s", filepath.Base(binary), sha256.Sum256(data), data)
		}
	})
}

func (f *nativeFixture) step(name string, goTime, sqlTime time.Time) {
	f.t.Helper()
	data, err := json.Marshal(map[string]string{"version": captureclock.Version, "step": name, "go_time": goTime.UTC().Format(time.RFC3339Nano), "sql_time": sqlTime.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		f.t.Fatal(err)
	}
	staged := f.clock + ".next"
	if err := os.WriteFile(staged, data, 0600); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Rename(staged, f.clock); err != nil {
		f.t.Fatal(err)
	}
	f.t.Logf("NATIVE_CAPTURE_CLOCK_INPUT sha256=%x bytes=%s", sha256.Sum256(data), data)
}

func (f *nativeFixture) sqlClock(want time.Time) {
	f.t.Helper()
	var actual, defaultUTC string
	err := f.db.QueryRow(`SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ'), DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%s.%fZ')`).Scan(&actual, &defaultUTC)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Logf("NATIVE_CAPTURE_SQL_CLOCK microseconds=%s default=%s", actual, defaultUTC)
	if actual != want.Format("2006-01-02T15:04:05.000000Z") || defaultUTC != want.Format("2006-01-02T15:04:05.000000Z") {
		f.t.Fatalf("real SQL query clock differs: %s / %s want %s", actual, defaultUTC, want)
	}
}

func newNativeFixture(t *testing.T) *nativeFixture {
	t.Helper()
	return newNativeFixtureForProject(t, "native")
}

func newNativeFixtureForProject(t *testing.T, prefix string) *nativeFixture {
	t.Helper()
	root := bazeltest.OverrideRoot()
	if root == "" {
		t.Fatal("canonical Bazel runfiles required; no source-only substitute")
	}
	f := &nativeFixture{t: t, work: privateDir(t)}
	f.bd = filepath.Join(root, os.Getenv("BEADS_CAPTURE_BD_BINARY"))
	f.companion = filepath.Join(root, os.Getenv("BEADS_CAPTURE_SQL_BINARY"))
	for _, binary := range []string{f.bd, f.companion} {
		data, err := os.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("NATIVE_CAPTURE_BINARY path=%s bytes=%d sha256=%x", binary, len(data), sha256.Sum256(data))
	}
	home := privateDir(t)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "TZ" || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "DOLT_") || strings.HasPrefix(key, "XDG_") || strings.HasPrefix(key, "GIT_") {
			continue
		}
		f.env = append(f.env, entry)
	}
	f.clock = filepath.Join(privateDir(t), "clock.json")
	f.env = append(f.env, "HOME="+home, "TZ=UTC", "XDG_CONFIG_HOME="+filepath.Join(home, ".config"), "DOLT_ROOT_PATH="+home,
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "BD_DISABLE_METRICS=1", "BD_DISABLE_EVENT_FLUSH=1", "BD_EVENTS_JOURNAL=false",
		captureclock.FileEnv+"="+f.clock, captureclock.PublicEnv+"=1", "BEADS_DOLT_AUTO_START=0")
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 123456000, time.UTC)
	f.step("initial-independent-inputs", t0, t0.Add(10*time.Second))
	f.run("git", "init", "-b", "main")
	f.run("git", "config", "user.name", "Native capture")
	f.run("git", "config", "user.email", "capture@example.invalid")
	f.run(f.bd, "version", "--json")
	f.run(f.companion, "--capture-version")
	f.run(f.companion, "--help")
	port := freePort(t)
	// The pinned server initializes root@localhost for a fresh data directory.
	// Its parser rejects the removed --user/--password startup options.
	// Keep the server's pre-init config outside the fresh workspace: a nonempty
	// .beads/dolt without a version witness correctly triggers the legacy guard.
	// This existing public override binds the client locator to the same real
	// server data root for the entire CLI/HTTP/SQL lifetime.
	dataDir := privateDir(t)
	f.env = append(f.env, "BEADS_DOLT_DATA_DIR="+dataDir)
	f.start(f.companion, "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--data-dir", dataDir)
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/?parseTime=true&loc=UTC&timeout=1s&readTimeout=10s&writeTimeout=10s", port))
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err = db.Ping(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture SQL server did not become ready: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	f.sqlClock(t0.Add(10 * time.Second))
	f.env = append(f.env, "BEADS_DOLT_SERVER_MODE=1", "BEADS_DOLT_SERVER_HOST=127.0.0.1", "BEADS_DOLT_SERVER_PORT="+strconv.Itoa(port))
	// Leave BEADS_DIR unset: explicit workspace selection plus an external
	// data root skips init's local metadata block. The isolated CWD owns the
	// fresh workspace, so ordinary init must write its discovery metadata.
	f.run(f.bd, "init", "--server", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(port), "--database", "capture", "--prefix", prefix, "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	metadata, err := os.ReadFile(filepath.Join(f.work, ".beads", "metadata.json"))
	if err != nil {
		t.Fatalf("public init workspace metadata: %v", err)
	}
	t.Logf("NATIVE_CAPTURE_WORKSPACE_METADATA sha256=%x data=%s", sha256.Sum256(metadata), metadata)
	// Observe the actual server-created database; never create a directory to
	// stand in for the native inventory's storage observation.
	for _, path := range []string{filepath.Join(dataDir, "capture"), filepath.Join(dataDir, "capture", ".dolt")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() {
			t.Fatalf("server-created native database directory %q: %v", path, err)
		}
		t.Logf("NATIVE_CAPTURE_DATABASE_DIRECTORY path=%s mode=%04o", path, info.Mode().Perm())
	}
	if _, err := db.Exec("USE `capture`"); err != nil {
		t.Fatal(err)
	}
	// One retained SQL connection is used across all published time steps.
	db.SetMaxOpenConns(1)
	return f
}

func (f *nativeFixture) create(title string, args ...string) string {
	f.t.Helper()
	argv := append([]string{"create", title, "--json"}, args...)
	var issue struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(f.run(f.bd, argv...), &issue); err != nil || issue.ID == "" {
		f.t.Fatalf("actual create identity: %+v %v", issue, err)
	}
	return issue.ID
}

func ids(data []byte, page bool) ([]string, error) {
	var issues []struct {
		ID string `json:"id"`
	}
	if page {
		var result struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}
		issues = result.Items
	} else if err := json.Unmarshal(data, &issues); err != nil {
		return nil, err
	}
	result := []string{}
	for _, issue := range issues {
		result = append(result, issue.ID)
	}
	return result, nil
}

func TestPublicControlledClockAndActiveFamilyCapture(t *testing.T) {
	f := newNativeFixture(t)
	// Native DATETIME columns have second precision. These native points are
	// separate from Plug's nanosecond fixture at T=.123456Z; no rounding claim.
	nativeBoundary := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	f.step("aging-create", nativeBoundary.Add(-48*time.Hour), nativeBoundary.Add(-48*time.Hour))
	aging := f.create("aging priority zero", "--priority", "0", "--labels", "clock-boundary")
	f.step("recent-create", nativeBoundary.Add(-time.Hour), nativeBoundary.Add(-time.Hour))
	recent := f.create("recent priority four", "--priority", "4", "--labels", "clock-boundary")
	active := f.create("genuine active parent", "--ephemeral", "--priority", "2")
	child := f.create("genuine active child", "--ephemeral", "--parent", active)
	f.run(f.bd, "label", "add", active, "native-active", "--json")
	f.run(f.bd, "comments", "add", active, "retained active comment", "--author", "capture", "--json")
	f.run(f.bd, "dep", "add", child, aging, "--type", "related", "--json")
	// Keep a genuine HTTP process alive while the same SQL server and clock
	// input are stepped. Health checks are liveness only; ready below is real.
	httpPort := freePort(t)
	f.start(f.bd, "serve", "--addr", fmt.Sprintf("127.0.0.1:%d", httpPort))
	client := &http.Client{Timeout: 10 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := client.Get(baseURL + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("native HTTP server did not become live: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, point := range []struct {
		name  string
		delta time.Duration
		want  []string
	}{
		{"before", -time.Microsecond, []string{aging, recent}},
		{"at", 0, []string{aging, recent}},
		{"after", time.Microsecond, []string{recent, aging}},
		// Keep a separately named coarse point as well as the actual pinned
		// UTC_TIMESTAMP() microsecond crossing above; do not infer precision.
		{"one-second-after", time.Second, []string{recent, aging}},
	} {
		t.Run("hybrid-48h-"+point.name, func(t *testing.T) {
			local := *f
			local.t = t
			f := &local
			instant := nativeBoundary.Add(point.delta)
			f.step("hybrid-48h-"+point.name, instant, instant)
			f.sqlClock(instant)
			if point.name == "at" {
				f.run(f.bd, "update", aging, "--notes", "updated at native cutoff; no Plug stored-rank lag", "--json")
			}
			for _, limit := range []int{1, 2, 1000} {
				want := point.want[:min(limit, len(point.want))]
				actual, err := ids(f.run(f.bd, "ready", "--sort", "hybrid", "--label", "clock-boundary", "--limit", strconv.Itoa(limit), "--json"), false)
				if err != nil || !reflect.DeepEqual(actual, want) {
					t.Fatalf("CLI native boundary want=%q got=%q error=%v", want, actual, err)
				}
				response, err := client.Get(fmt.Sprintf("%s/v0/beads/ready?sort=hybrid&label=clock-boundary&limit=%d", baseURL, limit))
				if err != nil {
					t.Fatal(err)
				}
				data, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				t.Logf("NATIVE_CAPTURE_HTTP status=%d raw=%s", response.StatusCode, data)
				actual, err = ids(data, true)
				if response.StatusCode != http.StatusOK || readErr != nil || err != nil || !reflect.DeepEqual(actual, want) {
					t.Fatalf("persistent HTTP native boundary want=%q got=%q error=%v/%v", want, actual, readErr, err)
				}
			}
		})
	}
	// Preserve native defer semantics: a past date warns and an indefinite
	// defer stays deferred. The exact wake boundary uses a representable second.
	f.step("defer-before", nativeBoundary, nativeBoundary)
	f.run(f.bd, "defer", active, "--until", nativeBoundary.Add(time.Second).Format(time.RFC3339Nano), "--reason", "capture timed", "--json")
	f.run(f.bd, "defer", child, "--reason", "waiting explicitly", "--json")
	for _, point := range []struct {
		name    string
		delta   time.Duration
		visible bool
	}{
		{"before", time.Second - time.Microsecond, false},
		{"at", time.Second, true},
		{"after", time.Second + time.Microsecond, true},
		{"one-second-after", 2 * time.Second, true},
	} {
		instant := nativeBoundary.Add(point.delta)
		f.step("defer-"+point.name, instant, instant)
		f.sqlClock(instant)
		data := f.run(f.bd, "ready", "--include-ephemeral", "--label", "native-active", "--json")
		actual, err := ids(data, false)
		want := []string{}
		if point.visible {
			want = append(want, active)
		}
		if err != nil || !reflect.DeepEqual(actual, want) {
			t.Fatalf("native defer %s visible=%v actual=%q error=%v", point.name, point.visible, actual, err)
		}
		var status string
		if err := f.db.QueryRow("SELECT status FROM wisps WHERE id=?", child).Scan(&status); err != nil || status != "deferred" {
			t.Fatalf("indefinite defer changed: %s %v", status, err)
		}
	}
	// Actual public native export, self-Validate, and independent raw multiset
	// readback cover every live active family. No empty-family/marker substitute.
	capture := privateDir(t)
	inventoryPath := filepath.Join(capture, "inventory.json")
	archivePath := filepath.Join(capture, "readback.tar")
	f.run(f.bd, "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryPath)
	f.run(f.bd, "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "--output", archivePath)
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := plugdump.Validate(bytes.NewReader(archive))
	if err != nil || manifest.SourcePlaneAddressContract != plugdump.SourcePlaneAddressContract {
		t.Fatalf("actual self-Validate: %+v %v", manifest, err)
	}
	records := map[string][]plugdump.Row{}
	reader := tar.NewReader(bytes.NewReader(archive))
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
			records[record.Source.Table] = append(records[record.Source.Table], record.Source.Fields)
		}
	}
	for _, family := range []string{"wisps", "wisp_dependencies", "wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters"} {
		table, err := (plugdump.SQLSource{DB: f.db}).ReadTable(context.Background(), family)
		if err != nil {
			t.Fatal(err)
		}
		if len(table.Rows) == 0 || len(records[family]) != len(table.Rows) {
			t.Fatalf("live %s census missing: SQL=%d archive=%d", family, len(table.Rows), len(records[family]))
		}
		encodeRows := func(rows []plugdump.Row) []string {
			values := []string{}
			for _, row := range rows {
				data, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, string(data))
			}
			sort.Strings(values)
			return values
		}
		if !reflect.DeepEqual(encodeRows(table.Rows), encodeRows(records[family])) {
			t.Fatalf("live %s raw multiset changed", family)
		}
		t.Logf("NATIVE_CAPTURE_ACTIVE_RAW family=%s rows=%s", family, encodeRows(table.Rows))
	}
	sidecar, err := os.ReadFile(inventoryPath + ".sources/environment-and-flags.json")
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Clock struct {
			Input      captureclock.Snapshot `json:"input"`
			GoObserved time.Time             `json:"go_observed"`
			SQLResult  plugdump.Table        `json:"sql_result"`
			Columns    plugdump.Table        `json:"column_precision"`
		} `json:"capture_clock"`
	}
	if err := json.Unmarshal(sidecar, &evidence); err != nil || evidence.Clock.Input.Step != "defer-one-second-after" || !evidence.Clock.GoObserved.Equal(evidence.Clock.Input.GoTime) || len(evidence.Clock.SQLResult.Rows) != 1 || len(evidence.Clock.Columns.Rows) != 6 {
		t.Fatalf("actual clock sidecar: %s %v", sidecar, err)
	}
	t.Logf("NATIVE_CAPTURE_CLOCK_SIDECAR sha256=%x bytes=%s", sha256.Sum256(sidecar), sidecar)
	t.Logf("NATIVE_CAPTURE_ARCHIVE sha256=%x bytes=%d history=%s", sha256.Sum256(archive), len(archive), manifest.History)
}

// Both fixtures are fabricated from anonymous type/ID shapes. These archives
// are bounded producer proof for the consumer's two-project merge/restore
// tests, not real-source captures or rehearsal streak credit.
func TestPublicNativeCustomTypesAndNamespacedHashes(t *testing.T) {
	for _, test := range []struct {
		prefix  string
		tracked bool
	}{
		{"reprocody", false}, {"reprosignals", false},
		{"reprocody", true}, {"reprosignals", true},
	} {
		name := test.prefix
		if test.tracked {
			name += "-tracked"
		}
		t.Run(name, func(t *testing.T) {
			prefix := test.prefix
			f := newNativeFixtureForProject(t, prefix)
			types := []string{"zzzzzzzz"}
			roots := []string{"a1b", "c2d"}
			if prefix == "reprosignals" {
				types = []string{"aaaaa", "zzzzzzzz", "yyyyyyyy", "xxxxxxxxx"}
				roots = []string{"abc-4de", "abc-45d", "abc-456", "abc-d4e", "abc-de4", "abc-def"}
			}
			f.run(f.bd, "config", "set", "types.custom", strings.Join(types, ","))
			statuses := []string{"todo", "done", "parked"}
			categories := map[string]string{"todo": "active", "done": "done", "parked": "frozen"}
			declarations := "todo:active,done:done,parked:frozen"
			if prefix == "reprosignals" {
				// Equal spellings across projects must retain their own category.
				statuses = []string{"todo", "review", "done", "parked"}
				categories["todo"], categories["review"] = "wip", "active"
				declarations = "todo:wip,review:active,done:done,parked:frozen"
			}
			f.run(f.bd, "config", "set", "status.custom", declarations)
			for i, root := range roots {
				id := prefix + "-" + root
				if got := f.create("synthetic shape control", "--id", id, "--type", types[i%len(types)], "--status", statuses[i%len(statuses)]); got != id {
					t.Fatalf("public create relabelled a synthetic ID: got %q want %q", got, id)
				}
			}
			if test.tracked {
				f.recordPublicHistoryImage("before-child")
			}
			parent := prefix + "-" + roots[0]
			// The public CLI deliberately makes --id and --parent exclusive.
			// Let its real child allocator preserve the complete namespaced parent.
			child := parent + ".1"
			if got := f.create("synthetic child control", "--parent", parent, "--status", "parked"); got != child {
				t.Fatal("public child create relabelled the synthetic source")
			}
			if test.tracked {
				f.recordPublicHistoryImage("after-child")
			}
			source, err := (plugdump.SQLSource{DB: f.db}).ReadTable(context.Background(), "issues")
			if err != nil || len(source.Rows) != len(roots)+1 {
				t.Fatalf("synthetic SQL issue census: %d %v", len(source.Rows), err)
			}
			wanted := map[string]string{}
			wantedStatuses := map[string]string{}
			for _, row := range source.Rows {
				wanted[row["id"].(string)] = row["issue_type"].(string)
				wantedStatuses[row["id"].(string)] = row["status"].(string)
			}
			capture := privateDir(t)
			inventoryPath, archivePath := filepath.Join(capture, "inventory.json"), filepath.Join(capture, "readback.tar")
			f.run(f.bd, "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryPath)
			inventoryBytes, err := os.ReadFile(inventoryPath)
			if err != nil {
				t.Fatal(err)
			}
			inventory, err := plugdump.ReadInventory(bytes.NewReader(inventoryBytes))
			if err != nil || len(inventory.Projects) != 1 || inventory.Projects[0].Prefix != prefix || len(inventory.Ontology) != len(types) {
				t.Fatalf("actual native project/type inventory: %+v %v", inventory, err)
			}
			if prefix == "reprosignals" {
				if !reflect.DeepEqual(inventory.Projects[0].HashNamespaces, []string{"abc"}) || inventory.Projects[0].HashLength != 3 {
					t.Fatalf("actual namespace/hash-length inventory: %+v", inventory.Projects)
				}
			} else if len(inventory.Projects[0].HashNamespaces) != 0 || bytes.Contains(inventoryBytes, []byte(`"hash_namespaces"`)) {
				t.Fatal("bare-hash project acquired namespace metadata")
			}
			f.run(f.bd, "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "--output", archivePath)
			archive, err := os.ReadFile(archivePath)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := plugdump.Validate(bytes.NewReader(archive))
			if err != nil || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != prefix || manifest.Projects[0].ChildCounters[parent] != 1 || len(manifest.Ontology) != len(types) {
				t.Fatalf("actual archive identity/ontology validation: %+v %v", manifest, err)
			}
			if manifest.StatusCategoryContract != plugdump.StatusCategoryContract || inventory.StatusCategoryContract != plugdump.StatusCategoryContract || len(manifest.Projects[0].Statuses) != 7+len(categories) {
				t.Fatal("native project status/category catalog missing")
			}
			for _, status := range manifest.Projects[0].Statuses {
				if category, custom := categories[status.Name]; custom && category != status.Category {
					t.Fatal("native custom category changed")
				}
			}
			observed := map[string]string{}
			var history plugdump.SourceHistory
			historySeen := false
			reader := tar.NewReader(bytes.NewReader(archive))
			for {
				header, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if header.Name == plugdump.SourceHistoryMember {
					decoder := json.NewDecoder(reader)
					decoder.UseNumber()
					if err := decoder.Decode(&history); err != nil {
						t.Fatal(err)
					}
					historySeen = true
					continue
				}
				if header.Name != "records/bead.ndjson" {
					continue
				}
				decoder := json.NewDecoder(reader)
				for {
					var record plugdump.Record
					if err := decoder.Decode(&record); err == io.EOF {
						break
					} else if err != nil {
						t.Fatal(err)
					}
					id, ok := record.Source.Fields["id"].(string)
					issueType, typeOK := record.Source.Fields["issue_type"].(string)
					if !ok || !typeOK || record.ID != id || record.Project != prefix || record.Body["source_type"] != issueType {
						t.Fatal("archive relabelled a source ID, project or type")
					}
					if _, duplicate := observed[id]; duplicate {
						t.Fatal("duplicate archived source ID")
					}
					if record.Source.Fields["status"] != wantedStatuses[id] || record.Body["status_category"] != categories[wantedStatuses[id]] {
						t.Fatal("native status name/category changed")
					}
					observed[id] = issueType
				}
			}
			if !reflect.DeepEqual(observed, wanted) {
				t.Fatalf("actual SQL/archive identity/type conservation: got %v want %v", observed, wanted)
			}

			output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if output == "" {
				t.Fatal("Bazel artifact directory required for independent consumer fixture")
			}
			if err := os.WriteFile(filepath.Join(output, name+"-synthetic.tar"), archive, 0600); err != nil {
				t.Fatal(err)
			}
			if !test.tracked {
				// Native defaults deliberately exclude clone-local and event
				// tables from immutable commits. Preserve this actual refusal;
				// no historical table or event is synthesized from head state.
				if !historySeen || history.Range.Status != "unverified" || history.Range.Complete || len(history.Range.Frames) != 0 {
					t.Fatal("default ignored history acquired invented replay proof")
				}
				f.assertNativeDefaultHistoryBoundary(prefix, inventory, history)
			} else if !historySeen || history.Range.Status != "verified" || history.Range.Complete || len(history.Range.Gaps) == 0 || len(history.Range.Frames) == 0 || history.Range.Head != uint64(len(history.Commits)) {
				// This diagnostic reads only this fabricated public fixture. Keep
				// exact native errors and the failed archive for causal follow-up.
				port := plugdump.SQLSource{DB: f.db}
				log, logErr := port.ReadSourceCommits(context.Background())
				t.Logf("B1A_PUBLIC_HISTORY_DIAGNOSTIC commit_census_error=%v", logErr)
				if logErr == nil {
					image, imageErr := port.ReadSourceImage(context.Background(), log.Head)
					t.Logf("B1A_PUBLIC_HISTORY_DIAGNOSTIC head_image_tables=%d error=%v", len(image), imageErr)
					if imageErr == nil {
						_, buildErr := plugdump.Build(context.Background(), publicImageSource(image), inventory)
						t.Logf("B1A_PUBLIC_HISTORY_DIAGNOSTIC head_mapping_error=%v", buildErr)
					}
				}
				t.Fatalf("native child history head not proven: status=%s reason=%s frames=%d", history.Range.Status, history.Range.Reason, len(history.Range.Frames))
			}
			if test.tracked {
				seenBefore, seenAfter := false, false
				images := [][]plugdump.Record{history.Range.Baseline}
				for _, frame := range history.Range.Frames {
					images = append(images, frame.Records)
				}
				for _, image := range images {
					rows, hasChild := 0, false
					for _, record := range image {
						if record.Kind != "bead" {
							continue
						}
						rows++
						id := record.Source.Fields["id"]
						if record.ID != id || record.Project != prefix || record.Source.Fields["status"] != wantedStatuses[record.ID] || record.Body["status_category"] != categories[wantedStatuses[record.ID]] {
							t.Fatal("tracked history relabelled native identity/status/category")
						}
						hasChild = hasChild || record.ID == child
					}
					seenBefore = seenBefore || !hasChild && rows == len(roots)
					seenAfter = seenAfter || hasChild && rows == len(roots)+1
				}
				if !seenBefore || !seenAfter {
					t.Fatal("tracked suffix did not preserve before/after real child allocation")
				}
			}
			t.Logf("B1A_SYNTHETIC_EXPORT fixture=%s project=%s source_rows=%d archive_rows=%d ontology=%d namespaces=%q child_counter=%d sha256=%x bytes=%d", name, prefix, len(wanted), len(observed), len(manifest.Ontology), manifest.Projects[0].HashNamespaces, manifest.Projects[0].ChildCounters[parent], sha256.Sum256(archive), len(archive))
			t.Logf("B1A_SYNTHETIC_HISTORY fixture=%s project=%s status=%s complete=%t first=%d head=%d frames=%d gaps=%d", name, prefix, history.Range.Status, history.Range.Complete, history.Range.First, history.Range.Head, len(history.Range.Frames), len(history.Range.Gaps))
		})
	}
}

// Read both censuses from the quiesced public fixture. This records the native
// source-data boundary without force-staging, inferring audit chronology or
// borrowing an absent historical family from the current working set.
func (f *nativeFixture) assertNativeDefaultHistoryBoundary(prefix string, inventory plugdump.Inventory, history plugdump.SourceHistory) {
	f.t.Helper()
	ctx := context.Background()
	port := plugdump.SQLSource{DB: f.db}
	log, err := port.ReadSourceCommits(ctx)
	if err != nil || !log.Clean || len(history.Commits) == 0 || log.Head != history.Commits[len(history.Commits)-1].ID {
		f.t.Fatalf("native default source head unavailable or changed: %+v %v", log, err)
	}
	image, err := port.ReadSourceImage(ctx, log.Head)
	if err != nil || image == nil {
		f.t.Fatalf("actual native immutable head census: %v", err)
	}
	names, err := port.Tables(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	workingRows, immutableRows := map[string]int{}, map[string]int{}
	missing := []string{}
	for name, table := range image {
		immutableRows[name] = len(table.Rows)
	}
	for _, name := range names {
		table, err := port.ReadTable(ctx, name)
		if err != nil {
			f.t.Fatalf("actual native working family %s: %v", name, err)
		}
		workingRows[name] = len(table.Rows)
		if _, present := image[name]; !present {
			missing = append(missing, name)
		}
		if name == "issues" {
			// Prove the actual durable issue head independently of the absent
			// audit/clone-local planes; row counts alone would not establish it.
			byID := map[string]plugdump.Row{}
			for _, row := range image[name].Rows {
				id, ok := row["id"].(string)
				if !ok || byID[id] != nil {
					f.t.Fatal("invalid immutable native issue identity")
				}
				byID[id] = row
			}
			if len(byID) != len(table.Rows) {
				f.t.Fatal("native committed issue head count differs")
			}
			for _, row := range table.Rows {
				id, _ := row["id"].(string)
				if !reflect.DeepEqual(row, byID[id]) {
					f.t.Fatal("native committed issue head fields differ")
				}
			}
		}
	}
	sort.Strings(missing)
	wantReason := "head-image-missing-families:" + strings.Join(missing, ",")
	_, hasHistoricalEvents := image["events"]
	if hasHistoricalEvents || workingRows["events"] == 0 || workingRows["events"] != len(history.Events) || history.Range.Reason != wantReason {
		f.t.Fatalf("native audit absence or named gap not preserved: historical=%t working=%d archived=%d reason=%s want=%s", hasHistoricalEvents, workingRows["events"], len(history.Events), history.Range.Reason, wantReason)
	}
	_, buildErr := plugdump.Build(ctx, publicImageSource(image), inventory)
	if buildErr == nil || !strings.HasPrefix(buildErr.Error(), "unaccounted table family ") {
		f.t.Fatalf("incomplete immutable head acquired a complete portable image: %v", buildErr)
	}
	finalLog, err := port.ReadSourceCommits(ctx)
	if err != nil || !reflect.DeepEqual(log, finalLog) {
		f.t.Fatalf("native head moved during boundary census: %v", err)
	}
	evidence, err := json.MarshalIndent(map[string]any{
		"fixture": prefix, "head": log.Head, "commits": len(history.Commits),
		"working_rows": workingRows, "immutable_rows": immutableRows, "absent_families": missing,
		"working_audit_rows": workingRows["events"], "archived_audit_rows": len(history.Events),
		"history_range": history.Range, "head_build_error": buildErr.Error(),
		"durable_issue_head_exact": true,
		"scope":                    "Actual native-default SQL census in fabricated public fixture only; audit/clone-local history absent, no reconstruction or real-store credit",
	}, "", "  ")
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), prefix+"-native-default-history-boundary.json")
	if err := os.WriteFile(path, evidence, 0600); err != nil {
		f.t.Fatal(err)
	}
	f.t.Logf("B1A_NATIVE_DEFAULT_HISTORY_BOUNDARY sha256=%x evidence=%s", sha256.Sum256(evidence), evidence)
}

// recordPublicHistoryImage is confined to the fabricated two-project fixture.
// Explicit exact-name ignore overrides and force-staging record the actual
// complete SQL census, including clone-local/event rows native defaults omit.
// This is an opt-in synthetic store configuration, not the native default. It
// does not recover earlier edits or alter Beads'
// command semantics; the resulting range must retain its explicit prefix gaps.
func (f *nativeFixture) recordPublicHistoryImage(stage string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	names, err := (plugdump.SQLSource{DB: f.db}).Tables(ctx)
	if err != nil || len(names) == 0 {
		f.t.Fatalf("synthetic history census: %v", err)
	}
	sort.Strings(names)
	// Exact-name false overrides are supported by native Dolt matching and
	// the native migration opener. They keep this public fixture's captured
	// ignored cursor/events tracked during the real CLI child allocation.
	for _, name := range names {
		if _, err := f.db.ExecContext(ctx, "REPLACE INTO dolt_ignore VALUES (?, false)", name); err != nil {
			f.t.Fatal(err)
		}
	}
	args := []any{"-f", "dolt_ignore"}
	marks := []string{"?", "?"}
	for _, name := range names {
		args = append(args, name)
		marks = append(marks, "?")
	}
	drain := func(query string, args ...any) {
		f.t.Helper()
		rows, err := f.db.QueryContext(ctx, query, args...)
		if err != nil {
			f.t.Fatalf("synthetic history %s: %v", stage, err)
		}
		defer rows.Close()
		for {
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				f.t.Fatal(err)
			}
			if !rows.NextResultSet() {
				break
			}
		}
		if err := rows.Err(); err != nil {
			f.t.Fatal(err)
		}
	}
	drain("CALL DOLT_ADD("+strings.Join(marks, ",")+")", args...)
	drain("CALL DOLT_COMMIT('-m', ?, '--author', 'Synthetic capture <capture@example.invalid>')", "synthetic explicit complete image: "+stage)
	f.t.Logf("B1A_PUBLIC_TRACKED_HISTORY stage=%s tables=%q; explicitly captured public fixture only, earlier default history remains unavailable", stage, names)
}

func TestCaptureExecutablesRefuseInvalidOptIn(t *testing.T) {
	root := bazeltest.OverrideRoot()
	if root == "" {
		t.Fatal("canonical Bazel runfiles required")
	}
	work := privateDir(t)
	clock := filepath.Join(privateDir(t), "clock.json")
	if err := os.WriteFile(clock, []byte(`null`), 0600); err != nil {
		t.Fatal(err)
	}
	env := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "BD_") {
			continue
		}
		env = append(env, entry)
	}
	for _, tc := range []struct {
		name, binary, reason string
		args, additions      []string
	}{
		{"bd-partial", "BEADS_CAPTURE_BD_BINARY", "requires BEADS_CAPTURE_CLOCK_PUBLIC=1", []string{"version", "--json"}, []string{captureclock.FileEnv + "=" + clock}},
		{"bd-invalid-input", "BEADS_CAPTURE_BD_BINARY", "must be an object", []string{"version", "--json"}, []string{captureclock.FileEnv + "=" + clock, captureclock.PublicEnv + "=1"}},
		{"sql-absent", "BEADS_CAPTURE_SQL_BINARY", "requires an explicit public clock file", nil, nil},
		{"sql-invalid-input", "BEADS_CAPTURE_SQL_BINARY", "must be an object", nil, []string{captureclock.FileEnv + "=" + clock, captureclock.PublicEnv + "=1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(filepath.Join(root, os.Getenv(tc.binary)), tc.args...)
			cmd.Dir, cmd.Env = work, append(append([]string{}, env...), tc.additions...)
			data, err := cmd.CombinedOutput()
			t.Logf("NATIVE_CAPTURE_REFUSAL case=%s argv=%q error=%v raw=%s", tc.name, tc.args, err, data)
			if err == nil || !bytes.Contains(data, []byte(tc.reason)) {
				t.Fatalf("specific startup refusal %q: %v %s", tc.reason, err, data)
			}
		})
	}
	entries, err := os.ReadDir(work)
	if err != nil || len(entries) != 0 {
		t.Fatalf("refused startup mutated disposable working directory: %v %v", entries, err)
	}
}

// publicImageSource is used only for the fixed synthetic native fixture's
// failure diagnostic; it deliberately has no SourceHistoryReader capability.
type publicImageSource map[string]plugdump.Table

func (s publicImageSource) Tables(context.Context) ([]string, error) {
	names := make([]string, 0, len(s))
	for name := range s {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
func (s publicImageSource) ReadTable(_ context.Context, name string) (plugdump.Table, error) {
	return s[name], nil
}
