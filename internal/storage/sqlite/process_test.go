package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func waitMarker(ctx context.Context, path string) error {
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func childCommand(t *testing.T, ctx context.Context, path, mode string) *exec.Cmd {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestProcessCoordination$", "-test.v")
	cmd.Env = append(os.Environ(), "PLUG_SQLITE_TEST_CHILD="+mode, "PLUG_SQLITE_TEST_DB="+path)
	return cmd
}
func TestProcessCoordination(t *testing.T) {
	if mode := os.Getenv("PLUG_SQLITE_TEST_CHILD"); mode != "" {
		runProcessChild(t, mode, os.Getenv("PLUG_SQLITE_TEST_DB"))
		return
	}
	t.Run("expiry_takeover_fences_parent", func(t *testing.T) {
		ctx := testContext(t)
		path := filepath.Join(t.TempDir(), "process.db")
		s := openStore(t, ctx, path)
		l := getLease(t, ctx, s, 150*time.Millisecond)
		tx, h := beginRecord(t, ctx, s, l, "pl-process")
		if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-process"), Kind: "test", Body: []byte(`{"owner":"stale-parent"}`)}); err != nil {
			t.Fatal(err)
		}
		cmd := childCommand(t, ctx, path, "takeover")
		result := make(chan struct {
			out []byte
			err error
		}, 1)
		go func() {
			out, err := cmd.CombinedOutput()
			result <- struct {
				out []byte
				err error
			}{out, err}
		}()
		t.Cleanup(func() { _ = os.WriteFile(path+".finish", []byte("done"), 0600) })
		if err := waitMarker(ctx, path+".ready"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(ctx, h); err == nil {
			t.Fatal("stale process published")
		} else {
			requireFenced(t, err)
		}
		requireFenced(t, l.Renew(ctx))
		// An old release cannot clear the other process's newly acquired epoch.
		if err := l.Release(ctx); err != nil {
			t.Fatal(err)
		}
		short, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		denied, err := s.Lease(short, "must-wait", driver.LeaseTTL)
		cancel()
		if denied != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("old release removed child lease: %v %v", denied, err)
		}
		if err := os.WriteFile(path+".finish", []byte("done"), 0600); err != nil {
			t.Fatal(err)
		}
		child := <-result
		if child.err != nil {
			t.Fatalf("child=%v\n%s", child.err, child.out)
		}
		if !strings.Contains(string(child.out), "child committed under epoch 2") {
			t.Fatalf("child evidence missing:\n%s", child.out)
		}
		meta := getMeta(t, ctx, s)
		r := getReader(t, ctx, s, meta, nil)
		rows, err := r.Get(ctx, []driver.Ref{testRef("pl-process")})
		if err != nil || len(rows) != 1 || string(rows[0].Body) != `{"owner":"child"}` || meta.Head.String() != "1" {
			t.Fatalf("process takeover rows/head=%v %+v %v", rows, meta, err)
		}
		t.Logf("separate process acquired epoch 2 after parent expiry; parent commit/renew fenced; stale release preserved child lease; durable head=%s", meta.Head)
	})
	t.Run("crash_releases_file_locks_and_never_publishes", func(t *testing.T) {
		ctx := testContext(t)
		path := filepath.Join(t.TempDir(), "crash.db")
		out, err := childCommand(t, ctx, path, "crash").CombinedOutput()
		if err != nil || !strings.Contains(string(out), "exiting with staged uncommitted data") {
			t.Fatalf("crash child=%v\n%s", err, out)
		}
		s := openStore(t, ctx, path)
		if !getMeta(t, ctx, s).Head.IsZero() {
			t.Fatal("process exit committed staged work")
		}
		l := getLease(t, ctx, s, driver.LeaseTTL)
		if l.Epoch() != 2 {
			t.Fatalf("crash takeover epoch=%d", l.Epoch())
		}
		// This uses the identical row/head files the dead process held.
		entry := seedRecord(t, ctx, s, l, "pl-process", `{"owner":"parent-after-crash"}`)
		if entry.Revision.String() != "1" {
			t.Fatal("crashed transaction consumed revision")
		}
		t.Log("process exit with row/head locks and staged data consumed no revision; next process acquired epoch 2 and committed revision 1")
	})
}
func runProcessChild(t *testing.T, mode, path string) {
	t.Helper()
	ctx := testContext(t)
	s := openStore(t, ctx, path)
	switch mode {
	case "publication-takeover":
		if err := os.WriteFile(path+".opened", []byte("opened"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := waitMarker(ctx, path+".acquire"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".attempting", []byte("attempting"), 0600); err != nil {
			t.Fatal(err)
		}
		l := getLease(t, ctx, s, driver.LeaseTTL)
		cp, err := s.SaveCheckpoint(ctx, l, "process-publication", 1, []byte("child"))
		if err != nil || l.Epoch() != 2 || cp.Generation != 2 {
			t.Fatalf("child epoch=%d checkpoint=%+v err=%v", l.Epoch(), cp, err)
		}
		if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := waitMarker(ctx, path+".finish"); err != nil {
			t.Fatal(err)
		}
		t.Logf("child acquired epoch %d and checkpoint generation %d after renewed parent really expired", l.Epoch(), cp.Generation)
	case "takeover", "takeover-check":
		l := getLease(t, ctx, s, driver.LeaseTTL)
		entry := seedRecord(t, ctx, s, l, "pl-process", `{"owner":"child"}`)
		if l.Epoch() != 2 || entry.Revision.String() != "1" {
			t.Fatalf("child epoch/revision=%d/%s", l.Epoch(), entry.Revision)
		}
		if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := waitMarker(ctx, path+".finish"); err != nil {
			t.Fatal(err)
		}
		if mode == "takeover-check" {
			if err := l.Renew(ctx); err != nil {
				t.Fatalf("child lost its current epoch: %v", err)
			}
			t.Logf("child renewed current epoch %d after parent cancellation checks", l.Epoch())
		}
		t.Logf("child committed under epoch %d", l.Epoch())
	case "crash":
		l := getLease(t, ctx, s, 100*time.Millisecond)
		tx, _ := beginRecord(t, ctx, s, l, "pl-process")
		if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-process"), Kind: "test", Body: []byte(`{"owner":"crashed"}`)}); err != nil {
			t.Fatal(err)
		}
		fmt.Println("exiting with staged uncommitted data")
		os.Exit(0) // Deliberately bypass defers; OS locks and persistent TTL recover.
	default:
		t.Fatalf("unknown child mode %q", mode)
	}
}

func TestLeaseCommitContextErrorsPreserveChildEpoch(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "deadline.db")
	s := openStore(t, ctx, path)
	old := getLease(t, ctx, s, 150*time.Millisecond)
	cmd := childCommand(t, ctx, path, "takeover-check")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile(path+".finish", []byte("done"), 0600); err != nil {
			t.Error(err)
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("child=%v\n%s", err, &output)
		}
		if !strings.Contains(output.String(), "child renewed current epoch 2 after parent cancellation checks") {
			t.Errorf("child renewal evidence missing:\n%s", &output)
		}
		t.Log(output.String())
	})
	if err := waitMarker(ctx, path+".ready"); err != nil {
		t.Fatal(err)
	}
	current, err := readLease(ctx, s.db)
	if err != nil || current.epoch != 2 || current.token == "" || time.Now().UnixNano() >= current.expires {
		t.Fatalf("child lease not ready: epoch=%d err=%v", current.epoch, err)
	}
	var engine, journal string
	if err := s.db.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&engine); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal mode=%q err=%v", journal, err)
	}
	if err := old.Release(ctx); err != nil {
		t.Fatal(err)
	}
	assertCurrent := func(t *testing.T) {
		t.Helper()
		row, err := readLease(ctx, s.db)
		if err != nil || row != current {
			t.Fatalf("child lease changed: epoch=%d want=%d err=%v", row.epoch, current.epoch, err)
		}
		if head := getMeta(t, ctx, s).Head; head.String() != "1" {
			t.Fatalf("denied acquisition changed head: %s", head)
		}
	}
	assertCurrent(t)
	for _, deadline := range []bool{true, false} {
		name, want := "cancel_at_commit", context.Canceled
		if deadline {
			name, want = "deadline_at_commit", context.DeadlineExceeded
		}
		t.Run(name, func(t *testing.T) {
			var opCtx context.Context
			var cancel context.CancelFunc
			if deadline {
				opCtx, cancel = context.WithTimeout(ctx, time.Second)
			} else {
				opCtx, cancel = context.WithCancel(ctx)
			}
			defer cancel()
			reachedCommit := false
			// Lease's occupied-row poll uses this same real SQL transaction and
			// COMMIT. End the actual context after the read, before the final
			// check, to exercise refusal without relying on the poll race.
			err := s.immediate(opCtx, nil, func(c *sql.Conn) error {
				row, err := readLease(opCtx, c)
				if err != nil {
					return err
				}
				if row != current {
					return fmt.Errorf("occupied child lease changed: epoch=%d", row.epoch)
				}
				if deadline {
					<-opCtx.Done()
				} else {
					cancel()
				}
				reachedCommit = true
				return nil
			})
			var uncertain commitError
			if !reachedCommit || errors.As(err, &uncertain) || !errors.Is(err, want) {
				t.Fatalf("expected pre-COMMIT refusal: reached=%v err=%T %v", reachedCommit, err, err)
			}
			t.Logf("context ended before native COMMIT: %v", err)
			assertCurrent(t)
		})
	}
	t.Logf("actual SQLite %s/%s file; separate child retains epoch 2, exact lease row and head 1 after stale release and canceled COMMITs", engine, journal)
}
