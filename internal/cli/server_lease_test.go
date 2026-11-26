package cli

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
)

func TestServerCommandSQLiteLeaseContention(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transient bool
	}{
		{name: "transient_writer_contention_keeps_same_lease", transient: true},
		{name: "expired_lease_stops_listener_and_allows_restart"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if err := os.Mkdir(".plug", 0o700); err != nil {
				t.Fatal(err)
			}
			config := "driver: sqlite\ndsn: .plug/server.db\nproject: ops\naddress: 127.0.0.1:0\nui: false\nrollback_window: true\n"
			if err := os.WriteFile(filepath.Join(".plug", "config.yaml"), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			endpoint, stderr, done := startSQLiteLeaseServer(t, ctx)
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

			var epoch string
			var expires int64
			if err := writer.QueryRowContext(ctx, "SELECT epoch, expires FROM active_lease WHERE singleton=1").Scan(&epoch, &expires); err != nil {
				t.Fatal(err)
			}
			deadline := time.Unix(0, expires)

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
				var currentEpoch string
				if err := writer.QueryRowContext(ctx, "SELECT epoch FROM active_lease WHERE singleton=1").Scan(&currentEpoch); err != nil || currentEpoch != epoch {
					t.Fatalf("lease epoch after recovery = %q, want %q, error = %v", currentEpoch, epoch, err)
				}
				return
			}

			select {
			case code := <-done:
				if code != 1 || !strings.Contains(stderr.String(), "active storage lease was lost") {
					t.Fatalf("server exit after lease loss = %d, stderr = %q", code, stderr.String())
				}
			case <-time.After(time.Until(deadline.Add(4 * time.Second))):
				if _, err := remote.Call(ctx, "admin.health", nil, ""); err != nil {
					logSQLiteLeaseFailure(t, ctx, remote, err)
				}
				t.Fatalf("server kept serving after lease loss, stderr = %q", stderr.String())
			}
			if _, err := writer.ExecContext(ctx, "ROLLBACK"); err != nil {
				t.Fatal(err)
			}
			if _, err := remote.Call(ctx, "admin.health", nil, ""); err == nil {
				t.Fatal("old HTTP listener still serves health after exit")
			}
			restarted, _, _ := startSQLiteLeaseServer(t, ctx)
			t.Setenv("PLUG_SERVER", restarted)
			var out, errOut strings.Builder
			if code := Execute(ctx, []string{"health"}, strings.NewReader(""), &out, &errOut); code != 0 {
				t.Fatalf("health after restart = %d, stderr = %q", code, errOut.String())
			}
		})
	}
}

func startSQLiteLeaseServer(t *testing.T, parent context.Context) (string, *synchronizedBuffer, <-chan int) {
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
	return waitForEndpoint(t, &out), &stderr, done
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
