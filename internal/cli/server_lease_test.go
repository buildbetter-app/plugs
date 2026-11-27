package cli

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	nativesqlite "modernc.org/sqlite"
)

func TestServerCommandSQLiteStalledCommitRestartsAndReplays(t *testing.T) {
	if mode := os.Getenv("PLUG_TEST_STALLED_COMMIT"); mode != "" {
		if mode == "stall" {
			nativesqlite.RegisterConnectionHook(func(conn nativesqlite.ExecQuerierContext, _ string) error {
				hooks := conn.(nativesqlite.HookRegisterer)
				var publishingReceipt bool
				hooks.RegisterPreUpdateHook(func(row nativesqlite.SQLitePreUpdateData) {
					publishingReceipt = publishingReceipt || row.TableName == "replays"
				})
				hooks.RegisterCommitHook(func() int32 {
					if publishingReceipt {
						fmt.Fprintln(os.Stderr, "entered native COMMIT with a staged replay receipt")
						select {} // Native COMMIT cannot return or acknowledge cancellation.
					}
					return 0
				})
				return nil
			})
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		os.Exit(Execute(ctx, []string{"server"}, os.Stdin, os.Stdout, os.Stderr))
	}
	writeSQLiteLeaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	start := func(mode string, startup time.Duration) (string, *exec.Cmd, *synchronizedBuffer, <-chan error) {
		var out, stderr synchronizedBuffer
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestServerCommandSQLiteStalledCommitRestartsAndReplays$")
		cmd.Env = append(os.Environ(), "PLUG_TEST_STALLED_COMMIT="+mode)
		cmd.Stdout, cmd.Stderr = &out, &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { defer close(done); done <- cmd.Wait() }()
		t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
		return waitForEndpointWithin(t, &out, startup), cmd, &stderr, done
	}
	connect := func(endpoint string) *client.Client {
		transport, err := client.NewHTTP(client.HTTPConfig{Server: endpoint})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(transport.Close)
		remote, err := client.New(transport, "ops", "commit-watch-test")
		if err != nil {
			t.Fatal(err)
		}
		return remote
	}
	const key = "e53f53fd768849b298b6d743b8b6f211"
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": "stalled publication"}}
	endpoint, _, stderr, done := start("stall", 5*time.Second)
	remote := connect(endpoint)
	started := time.Now()
	callDone := make(chan error, 1)
	go func() { _, err := remote.Call(ctx, "bead.create", params, key); callDone <- err }()
	var exit *exec.ExitError
	select {
	case err := <-done:
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("stalled process exit = %v, stderr = %q", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("stalled process exceeded COMMIT cap, stderr = %q", stderr.String())
	}
	if elapsed := time.Since(started); elapsed < 7500*time.Millisecond || elapsed > 9500*time.Millisecond {
		t.Fatalf("process exit after %s, want near the 8s cap", elapsed)
	} else {
		t.Logf("process exited 1 after %s: %s", elapsed, stderr.String())
	}
	if log := stderr.String(); !strings.Contains(log, "entered native COMMIT with a staged replay receipt") ||
		!strings.Contains(log, "sqlite COMMIT stalled: commit cap exceeded; outcome uncertain (method=bead.create epoch=1") ||
		strings.Contains(log, "reacquiring") {
		t.Fatalf("process did not exit directly from the COMMIT watchdog: %s", log)
	}
	if err := <-callDone; err == nil {
		t.Fatal("crashed COMMIT returned success")
	}

	endpoint, replacement, replacementErr, replacementDone := start("restart", driver.LeaseTTL+8*time.Second)
	remote = connect(endpoint)
	if _, err := remote.Call(ctx, "admin.health", nil, ""); err != nil {
		t.Fatalf("health on replacement: %v", err)
	}
	checkStore := func(wantHead uint64, wantReceipts int) {
		t.Helper()
		db, err := sql.Open("sqlite", "file:.plug/server.db?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var epoch, head uint64
		var receipts int
		err = db.QueryRowContext(ctx, "SELECT epoch, head, (SELECT count(*) FROM replays) FROM active_lease, meta").Scan(&epoch, &head, &receipts)
		if err != nil || epoch != 2 || head != wantHead || receipts != wantReceipts {
			t.Fatalf("durable epoch/head/receipts = %d/%d/%d, want 2/%d/%d; error=%v", epoch, head, receipts, wantHead, wantReceipts, err)
		}
	}
	checkStore(0, 0)
	first, err := remote.Call(ctx, "bead.create", params, key)
	if err != nil || first.Replayed {
		t.Fatalf("retry after rollback should publish: replayed=%t error=%v", first.Replayed, err)
	}
	replay, err := remote.Call(ctx, "bead.create", params, key)
	if err != nil || !replay.Replayed || !bytes.Equal(first.JSON, replay.JSON) {
		t.Fatalf("key did not replay the exact result: replayed=%t error=%v", replay.Replayed, err)
	}
	checkStore(1, 1)
	if err := replacement.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-replacementDone; err != nil {
		t.Fatalf("replacement failed normal shutdown: %v, stderr=%q", err, replacementErr.String())
	}
	t.Log("fresh process served under epoch 2, published the absent keyed write once, and replayed the exact receipt")
}

func TestServerCommandSQLiteStartupAfterKilledPredecessor(t *testing.T) {
	if os.Getenv("PLUG_TEST_LEASE_PREDECESSOR") == "1" {
		os.Exit(Execute(context.Background(), []string{"server"}, os.Stdin, os.Stdout, os.Stderr))
	}
	writeSQLiteLeaseConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var out, stderr synchronizedBuffer
	predecessor := exec.CommandContext(ctx, executable, "-test.run=^TestServerCommandSQLiteStartupAfterKilledPredecessor$")
	predecessor.Env = append(os.Environ(), "PLUG_TEST_LEASE_PREDECESSOR=1")
	predecessor.Stdout, predecessor.Stderr = &out, &stderr
	if err := predecessor.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if predecessor.ProcessState == nil {
			_ = predecessor.Process.Kill()
			_ = predecessor.Wait()
		}
	})
	waitForEndpoint(t, &out)
	if err := predecessor.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL predecessor: %v, stderr = %q", err, stderr.String())
	}
	var exitErr *exec.ExitError
	if err := predecessor.Wait(); !errors.As(err, &exitErr) || exitErr.Exited() {
		t.Fatalf("expected signal termination, got %v", err)
	}
	db, err := sql.Open("sqlite", "file:.plug/server.db?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var priorEpoch uint64
	var expires int64
	if err := db.QueryRowContext(ctx, "SELECT epoch, expires FROM active_lease WHERE singleton=1 AND token<>''").Scan(&priorEpoch, &expires); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if remaining := time.Until(time.Unix(0, expires)); remaining <= 3*time.Second {
		t.Fatalf("predecessor heartbeat only has %s left, need to exercise the old 3s limit", remaining)
	}
	endpoint, _, _ := startSQLiteLeaseServer(t, ctx, driver.LeaseTTL+8*time.Second)
	var epoch uint64
	if err := db.QueryRowContext(ctx, "SELECT epoch FROM active_lease WHERE singleton=1").Scan(&epoch); err != nil || epoch != priorEpoch+1 {
		t.Fatalf("replacement lease = %d, want %d, error = %v", epoch, priorEpoch+1, err)
	}
	t.Setenv("PLUG_SERVER", endpoint)
	t.Setenv("PLUG_TOKEN", "")
	var healthErr strings.Builder
	if code := Execute(ctx, []string{"health"}, strings.NewReader(""), io.Discard, &healthErr); code != 0 {
		t.Fatalf("replacement health exit = %d, stderr = %q", code, healthErr.String())
	}
	t.Logf("restarted after SIGKILL in %s, lease epoch %d -> %d", time.Since(started), priorEpoch, epoch)
}

func writeSQLiteLeaseConfig(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.Mkdir(".plug", 0o700); err != nil {
		t.Fatal(err)
	}
	config := "driver: sqlite\ndsn: .plug/server.db\nproject: ops\naddress: 127.0.0.1:0\nui: false\nrollback_window: true\n"
	if err := os.WriteFile(filepath.Join(".plug", "config.yaml"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestServerCommandSQLiteLeaseContention(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transient bool
	}{
		{name: "transient_writer_contention_keeps_same_lease", transient: true},
		{name: "expired_lease_reacquires_on_same_endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writeSQLiteLeaseConfig(t)

			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			endpoint, stderr, done := startSQLiteLeaseServer(t, ctx, 3*time.Second)
			transport, err := client.NewHTTP(client.HTTPConfig{Server: endpoint})
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			remote, err := client.New(transport, "ops", "lease-test")
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("PLUG_SERVER", endpoint)
			t.Setenv("PLUG_TOKEN", "")
			if _, err := remote.Call(ctx, "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "persistent across the lease change"},
			}, ""); err != nil {
				t.Fatal(err)
			}
			var readyCursor, mcpSession string
			if !tc.transient {
				if _, err := remote.Call(ctx, "bead.create", map[string]any{
					"type": "issue", "fields": map[string]any{"title": "second ready page"},
				}, ""); err != nil {
					t.Fatal(err)
				}
				page, err := remote.Call(ctx, "bead.ready", map[string]any{"limit": 1}, "")
				if err != nil {
					t.Fatal(err)
				}
				var result struct{ NextCursor string }
				if err := json.Unmarshal(page.JSON, &result); err != nil || result.NextCursor == "" {
					t.Fatalf("first Ready page = %s, error = %v", page.JSON, err)
				}
				readyCursor = result.NextCursor
				mcpSession = leaseMCPRequest(t, ctx, endpoint, "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"lease-test","version":"1"}}}`)
				if mcpSession == "" {
					t.Fatal("MCP initialization did not issue a session")
				}
			}

			db, err := sql.Open("sqlite", "file:.plug/server.db?mode=rw&_pragma=busy_timeout(0)")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			writer, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
				t.Fatal(err)
			}
			defer writer.ExecContext(context.Background(), "ROLLBACK")

			var epoch uint64
			var expires int64
			if err := writer.QueryRowContext(ctx, "SELECT epoch, expires FROM active_lease WHERE singleton=1").Scan(&epoch, &expires); err != nil {
				t.Fatal(err)
			}
			deadline := time.Unix(0, expires)
			var heldConn net.Conn
			var draining *bufio.Reader
			if !tc.transient {
				heldConn, draining = holdLeaseRPCBody(t, endpoint, deadline.Add(15*time.Second))
			}

			// An independent native writer holds SQLite's write lock across
			// renewal attempts. The HTTP load exercises WAL reads concurrently.
			loadCtx, stopLoad := context.WithCancel(ctx)
			var successful atomic.Int64
			var workers sync.WaitGroup
			for range 4 {
				workers.Go(func() {
					ticker := time.NewTicker(100 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-loadCtx.Done():
							return
						case <-ticker.C:
							if _, err := remote.Call(loadCtx, "admin.health", nil, ""); err == nil {
								successful.Add(1)
							}
						}
					}
				})
			}
			defer func() {
				stopLoad()
				workers.Wait()
			}()
			releaseAt := deadline.Add(-3 * time.Second)
			if !tc.transient {
				releaseAt = deadline.Add(-500 * time.Millisecond)
			}
			select {
			case <-time.After(time.Until(releaseAt)):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if successful.Load() < 4 {
				t.Fatalf("healthy WAL reads while the native writer held the lock = %d", successful.Load())
			}
			t.Logf("successful concurrent HTTP health calls before releasing the native writer = %d", successful.Load())
			if tc.transient {
				if _, err := writer.ExecContext(ctx, "ROLLBACK"); err != nil {
					t.Fatal(err)
				}
				select {
				case <-time.After(time.Until(deadline.Add(250 * time.Millisecond))):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if _, err := remote.Call(ctx, "admin.health", nil, ""); err != nil {
					logSQLiteLeaseFailure(t, ctx, remote, err)
					t.Fatalf("health after the original lease deadline: %v", err)
				}
				if _, err := remote.Call(ctx, "bead.create", map[string]any{
					"type": "issue", "fields": map[string]any{"title": "same lease after contention"},
				}, ""); err != nil {
					t.Fatalf("write after the original lease deadline: %v", err)
				}
				var currentEpoch uint64
				if err := writer.QueryRowContext(ctx, "SELECT epoch FROM active_lease WHERE singleton=1").Scan(&currentEpoch); err != nil || currentEpoch != epoch {
					t.Fatalf("lease epoch after recovery = %d, want %d, error = %v", currentEpoch, epoch, err)
				}
				return
			}

			for until := time.Now().Add(5 * time.Second); !strings.Contains(stderr.String(), "lease was lost, reacquiring"); {
				if time.Now().After(until) {
					if _, err := remote.Call(ctx, "admin.health", nil, ""); err != nil {
						logSQLiteLeaseFailure(t, ctx, remote, err)
					}
					t.Fatalf("server did not attempt reacquisition, stderr = %q", stderr.String())
				}
				select {
				case code := <-done:
					t.Fatalf("server exited after lease loss (%d), stderr = %q", code, stderr.String())
				case <-time.After(20 * time.Millisecond):
				}
			}
			// The external writer is still blocking acquisition. The same
			// listener must answer with a retryable JSON-RPC error.
			_, err = remote.Call(ctx, "admin.health", nil, "")
			var busy *wire.Error
			if !errors.As(err, &busy) || busy.Data.Code != "busy" || !busy.Data.Retryable {
				t.Fatalf("health during reacquisition = %v, want retryable busy", err)
			}
			if _, err := writer.ExecContext(ctx, "ROLLBACK"); err != nil {
				t.Fatal(err)
			}
			for {
				if _, err := remote.Call(ctx, "admin.health", nil, ""); err == nil {
					break
				}
				select {
				case code := <-done:
					t.Fatalf("server exited during lease recovery (%d), stderr = %q", code, stderr.String())
				case <-ctx.Done():
					t.Fatalf("same endpoint failed to recover, stderr = %q", stderr.String())
				case <-time.After(20 * time.Millisecond):
				}
			}
			// A body admitted under the old epoch can finish reading, but
			// cannot dispatch even after the replacement is accepting traffic.
			if _, err := io.WriteString(heldConn, strings.Repeat(" ", 98)+"{}"); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(draining, &http.Request{Method: http.MethodPost})
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusRequestTimeout {
				t.Fatalf("retired request status = %s, want canceled body", response.Status)
			}
			var out, errOut strings.Builder
			if code := Execute(ctx, []string{"health"}, strings.NewReader(""), &out, &errOut); code != 0 {
				t.Fatalf("health after reacquiring = %d, stderr = %q", code, errOut.String())
			}
			var currentEpoch, head string
			err = db.QueryRowContext(ctx, "SELECT active_lease.epoch, meta.head FROM active_lease JOIN meta USING (singleton)").Scan(&currentEpoch, &head)
			nextEpoch, parseErr := strconv.ParseUint(currentEpoch, 10, 64)
			if err != nil || parseErr != nil || nextEpoch != epoch+1 || head != "00000000000000000002" {
				t.Fatalf("after recovery epoch = %q, want %d, head = %q, errors = %v / %v", currentEpoch, epoch+1, head, err, parseErr)
			}
			// Remove the replacement's identity before its first renewal.
			// Fencing is detected on that renewal, and must exit instead of
			// repeatedly rebuilding the runtime.
			if _, err := writer.ExecContext(ctx, "UPDATE active_lease SET token='', expires=0 WHERE singleton=1"); err != nil {
				t.Fatal(err)
			}
			if _, err := remote.Call(ctx, "bead.ready", map[string]any{"limit": 1, "cursor": readyCursor}, ""); err != nil {
				t.Fatalf("Ready continuation after reacquiring: %v", err)
			}
			leaseMCPRequest(t, ctx, endpoint, mcpSession, `{"jsonrpc":"2.0","id":2,"method":"ping","params":{}}`)
			select {
			case code := <-done:
				if code != 1 || !strings.Contains(stderr.String(), "lost before its first successful renewal") {
					t.Fatalf("exit after early replacement loss = %d, stderr = %q", code, stderr.String())
				}
			case <-ctx.Done():
				t.Fatalf("server did not exit after early replacement loss, stderr = %q", stderr.String())
			}
		})
	}
}

func holdLeaseRPCBody(t *testing.T, endpoint string, deadline time.Time) (net.Conn, *bufio.Reader) {
	t.Helper()
	address := strings.TrimPrefix(endpoint, "http://")
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "POST /rpc/v1 HTTP/1.1\r\nHost: %s\r\nPlug-RPC: 1\r\nContent-Type: application/json\r\nContent-Length: 100\r\nExpect: 100-continue\r\n\r\n", address); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusContinue {
		t.Fatalf("incomplete request was not admitted: %s", response.Status)
	}
	return conn, reader
}

func leaseMCPRequest(t *testing.T, ctx context.Context, endpoint, session, payload string) string {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/mcp", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	if session != "" {
		request.Header.Set("MCP-Session-Id", session)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("MCP request returned %s", response.Status)
	}
	var reply struct{ Error *wire.Error }
	if err := json.NewDecoder(response.Body).Decode(&reply); err != nil || reply.Error != nil {
		t.Fatalf("MCP reply error = %v, decode error = %v", reply.Error, err)
	}
	return response.Header.Get("MCP-Session-Id")
}

func TestLeaseRecoveryBudget(t *testing.T) {
	start := time.Unix(1000, 0)
	t.Run("fourth_rebind_within_ten_minutes", func(t *testing.T) {
		var budget leaseRecoveryBudget
		for _, elapsed := range []time.Duration{0, time.Minute, 2 * time.Minute} {
			at := start.Add(elapsed)
			if err := budget.check(at, true); err != nil {
				t.Fatal(err)
			}
			budget.record(at)
		}
		if err := budget.check(start.Add(9*time.Minute), true); err == nil || !strings.Contains(err.Error(), "recovery limit") {
			t.Fatalf("fourth rebind error = %v", err)
		}
		if err := budget.check(start.Add(10*time.Minute), true); err != nil {
			t.Fatalf("oldest rebind should leave the window: %v", err)
		}
		budget.record(start.Add(10 * time.Minute))
		if err := budget.check(start.Add(10*time.Minute+time.Second), true); err == nil {
			t.Fatal("other rebinds should remain in the sliding window")
		}
	})
	t.Run("replacement_lost_before_first_renewal", func(t *testing.T) {
		var budget leaseRecoveryBudget
		if err := budget.check(start, false); err != nil {
			t.Fatalf("first recovery should be allowed: %v", err)
		}
		budget.record(start)
		if err := budget.check(start.Add(time.Second), false); err == nil || !strings.Contains(err.Error(), "first successful renewal") {
			t.Fatalf("early replacement loss error = %v", err)
		}
	})
}

func startSQLiteLeaseServer(t *testing.T, parent context.Context, startupTimeout time.Duration) (string, *synchronizedBuffer, <-chan int) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	var out, stderr synchronizedBuffer
	done := make(chan int, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		done <- Execute(ctx, []string{"server"}, strings.NewReader(""), &out, &stderr)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Errorf("disposable server did not stop, stderr = %q", stderr.String())
		}
	})
	return waitForEndpointWithin(t, &out, startupTimeout), &stderr, done
}

func logSQLiteLeaseFailure(t *testing.T, ctx context.Context, remote *client.Client, err error) {
	t.Helper()
	var typed *wire.Error
	if errors.As(err, &typed) {
		t.Logf("admin.health wire error = %q, hint = %q", typed.Data.Code, typed.Data.Hint)
	}
	if _, err := remote.Call(ctx, "rpc.methods", nil, ""); err == nil {
		t.Log("rpc.methods succeeded on the same listener after admin.health failed")
	}
	var stderr strings.Builder
	code := Execute(ctx, []string{"health"}, strings.NewReader(""), io.Discard, &stderr)
	t.Logf("plug health exit = %d, stderr = %q", code, stderr.String())
}
