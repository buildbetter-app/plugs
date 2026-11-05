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
	var actual, seconds string
	err := f.db.QueryRow(`SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ'), DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%s.%fZ')`).Scan(&actual, &seconds)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Logf("NATIVE_CAPTURE_SQL_CLOCK microseconds=%s default=%s", actual, seconds)
	if actual != want.Format("2006-01-02T15:04:05.000000Z") || seconds != want.Truncate(time.Second).Format("2006-01-02T15:04:05.000000Z") {
		f.t.Fatalf("real SQL query clock differs: %s / %s want %s", actual, seconds, want)
	}
}

func newNativeFixture(t *testing.T) *nativeFixture {
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
		captureclock.FileEnv+"="+f.clock, captureclock.PublicEnv+"=1", "BEADS_DOLT_AUTO_START=0", "BEADS_DIR="+filepath.Join(f.work, ".beads"))
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 123456000, time.UTC)
	f.step("initial-independent-inputs", t0, t0.Add(10*time.Second))
	f.run("git", "init", "-b", "main")
	f.run("git", "config", "user.name", "Native capture")
	f.run("git", "config", "user.email", "capture@example.invalid")
	f.run(f.bd, "version", "--json")
	f.run(f.companion, "--capture-version")
	f.run(f.companion, "--help")
	port := freePort(t)
	f.start(f.companion, "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--data-dir", privateDir(t), "--user", "root")
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
	f.run(f.bd, "init", "--server", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(port), "--database", "capture", "--prefix", "native", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
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
	if err := json.Unmarshal(sidecar, &evidence); err != nil || evidence.Clock.Input.Step != "defer-after" || !evidence.Clock.GoObserved.Equal(evidence.Clock.Input.GoTime) || len(evidence.Clock.SQLResult.Rows) != 1 || len(evidence.Clock.Columns.Rows) != 6 {
		t.Fatalf("actual clock sidecar: %s %v", sidecar, err)
	}
	t.Logf("NATIVE_CAPTURE_CLOCK_SIDECAR sha256=%x bytes=%s", sha256.Sum256(sidecar), sidecar)
	t.Logf("NATIVE_CAPTURE_ARCHIVE sha256=%x bytes=%d history=%s", sha256.Sum256(archive), len(archive), manifest.History)
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
