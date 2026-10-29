package postphase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

type retainedCapabilities struct {
	host   pluginapi.Host
	reader pluginapi.SnapshotReader
	ref    pluginapi.Ref
}

func (c retainedCapabilities) revoked() error {
	ctx := context.Background() // A fresh caller context must not revive I/O.
	if _, err := c.reader.Get(ctx, []pluginapi.Ref{c.ref}); !revocationError(err) {
		return fmt.Errorf("retained reader did not reject expired invocation: %v", err)
	}
	if err := c.host.Call(ctx, "tx.apply", map[string]any{}, nil); !revocationError(err) {
		return fmt.Errorf("retained Host.Call did not reject expired invocation: %v", err)
	}
	if _, err := c.host.HTTP().Get("https://example.com"); !revocationError(err) {
		return fmt.Errorf("retained HTTP did not reject expired invocation: %v", err)
	}
	return nil
}

func revocationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || hostErrorCode(err) == pluginhost.ErrInactive
}

func TestNativePostReadsCommittedRevisionOutsideLaneAndIsolatesMutations(t *testing.T) {
	f := newFixture(t)
	plain := f.writer(nil, f.store, f.lease)
	seed := create("base", "first")
	seed.Ops[0].Create.Fields["description"] = "private-description"
	base := mustApply(t, f, plain, seed, writeMeta(1))
	ref := base.Results[0].Ref
	started := make(chan string, 2)
	advanced := make(chan struct{})
	mutated := make(chan struct{})
	observed := make(chan error, 2)
	retained := make(chan retainedCapabilities, 1)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(advanced) }) })
	var calls atomic.Int32
	observe := func(label string) postCallback {
		return func(ctx context.Context, host pluginapi.Host, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) (err error) {
			calls.Add(1)
			started <- label
			defer func() { observed <- err }()
			select {
			case <-advanced:
			case <-ctx.Done():
				return ctx.Err()
			}
			if label == "beta" {
				select {
				case <-mutated:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if change.Revision.String() != "2" || len(change.Changes) != 1 {
				return fmt.Errorf("%s committed payload=%+v", label, change)
			}
			item := &change.Changes[0]
			if err := assertTitle(item.Before, "base"); err != nil {
				return err
			}
			if err := assertTitle(item.After, "committed"); err != nil {
				return err
			}
			if item.SetBy["fields.title"] != "request" || item.Resource.Ref.ID != ref.ID {
				return fmt.Errorf("%s mutated peer provenance/address=%+v", label, item)
			}
			for _, raw := range [][]byte{item.Before, item.After, item.Op.Payload} {
				if strings.Contains(string(raw), "private-description") || strings.Contains(string(raw), "description") {
					return fmt.Errorf("%s undeclared field leaked: %s", label, raw)
				}
			}
			if _, err := reader.Query(ctx, pluginapi.Query{Filter: map[string]any{"fields.description": "private-description"}}); hostErrorCode(err) != pluginhost.ErrRead {
				return fmt.Errorf("undeclared query field refusal=%v", err)
			}
			page, err := reader.Query(ctx, pluginapi.Query{Limit: 10})
			if err != nil || page.AsOf.String() != "2" || len(page.Items) != 1 {
				return fmt.Errorf("%s query moved with head: %+v err=%v", label, page, err)
			}
			if err := assertTitle(page.Items[0].Body, "committed"); err != nil {
				return err
			}
			rows, err := reader.Get(ctx, []pluginapi.Ref{item.Resource.Ref})
			if err != nil || len(rows) != 1 || rows[0].Revision.String() != "2" {
				return fmt.Errorf("%s point read moved with head: %+v err=%v", label, rows, err)
			}
			if err := assertTitle(rows[0].Body, "committed"); err != nil {
				return err
			}
			if strings.Contains(string(rows[0].Body), "description") || strings.Contains(string(page.Items[0].Body), "description") {
				return errors.New("reader leaked undeclared fields")
			}
			if label == "alpha" {
				if err := host.Call(ctx, "tx.apply", map[string]any{}, nil); hostErrorCode(err) != pluginhost.ErrReentrant {
					return fmt.Errorf("graph write refusal=%v", err)
				}
				if _, err := host.HTTP().Get("https://example.com"); hostErrorCode(err) != pluginhost.ErrEgress {
					return fmt.Errorf("undeclared egress refusal=%v", err)
				}
				retained <- retainedCapabilities{host, reader, item.Resource.Ref}
				// All these objects belong to the callback. Corrupting them must
				// affect neither another binding nor the engine or durable receipt.
				item.Before[0], item.After[0], item.Op.Payload[0] = '!', '!', '!'
				item.SetBy["fields.title"] = "corrupted"
				page.Items[0].Body[0], rows[0].Body[0] = '!', '!'
				rows, err = reader.Get(ctx, []pluginapi.Ref{item.Resource.Ref})
				if err != nil || len(rows) != 1 {
					return fmt.Errorf("repeat read=%+v err=%v", rows, err)
				}
				if err := assertTitle(rows[0].Body, "committed"); err != nil {
					return err
				}
				if _, err := reader.Query(ctx, pluginapi.Query{Limit: 1}); hostErrorCode(err) != pluginhost.ErrBudget {
					return fmt.Errorf("second query exceeded grant without budget refusal: %v", err)
				}
				close(mutated)
			}
			return nil
		}
	}
	host := f.host(t, nativePost("alpha", observe("alpha")), nativePost("beta", observe("beta")))
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(2)
	result := applyAsync(f.ctx, writer, f.principal, patch(ref, "committed"), meta)
	labels := map[string]bool{}
	for range 2 {
		labels[await(t, f.ctx, started)] = true
	}
	if len(labels) != 2 || !receipt(t, f.store, f.principal, meta).ResponsePending {
		t.Fatal("post bindings did not run concurrently before finalization")
	}
	// This must commit while both callbacks are blocked, proving they retain
	// no lane locks. They read only after the current image has changed.
	later := mustApply(t, f, plain, patch(ref, "later"), writeMeta(3))
	if later.Revision.String() != "3" {
		t.Fatalf("concurrent write revision=%s", later.Revision)
	}
	release.Do(func() { close(advanced) })
	got := await(t, f.ctx, result)
	if got.err != nil || len(got.commit.Warnings) != 0 || calls.Load() != 2 {
		t.Fatalf("commit=%+v err=%v calls=%d", got.commit, got.err, calls.Load())
	}
	for range 2 {
		if err := await(t, f.ctx, observed); err != nil {
			t.Fatal(err)
		}
	}
	if err := await(t, f.ctx, retained).revoked(); err != nil {
		t.Fatal(err)
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending {
		t.Fatal("receipt remained pending")
	}
	assertJSONEqual(t, got.commit, entry.Commit)
	if err := assertTitle(entry.Commit.Results[0].Record, "committed"); err != nil {
		t.Fatal(err)
	}
	if entry.Changeset.Events[0].Provenance.SetBy["fields.title"] != "request" {
		t.Fatalf("post mutated canonical event provenance=%+v", entry.Changeset.Events)
	}
	rows, err := openReader(t, f).Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || head(t, f.store).String() != "3" {
		t.Fatalf("current graph rows=%+v err=%v", rows, err)
	}
	if err := assertTitle(rows[0].Body, "later"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rows[0].Body), "private-description") {
		t.Fatal("post mutation changed a field omitted from its read grant")
	}
	versions, err := openReader(t, f).Versions(f.ctx, ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 3 {
		t.Fatalf("post modified history versions=%+v err=%v", versions, err)
	}
	t.Log("native proof: two parallel bindings; head=3; callback/read/receipt revision=2; payload isolation and retained I/O revocation")
}

func TestNativePostDeadlineBoundsUncooperativeBindingsAndRevokesIO(t *testing.T) {
	f := newFixture(t)
	type running struct {
		remaining time.Duration
		caps      retainedCapabilities
	}
	started := make(chan running, 2)
	exited := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	callback := func(ctx context.Context, host pluginapi.Host, c pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
		deadline, ok := ctx.Deadline()
		remaining := time.Duration(0)
		if ok {
			remaining = time.Until(deadline)
		}
		started <- running{remaining, retainedCapabilities{host, r, c.Changes[0].Resource.Ref}}
		<-ctx.Done()
		// Deliberately ignore cancellation until the test releases us. The
		// Writer must finish and capabilities must expire while we still run.
		<-release
		exited <- struct{}{}
		return nil
	}
	host := f.hostWithTimeout(t, time.Second, nativePost("slowalpha", callback), nativePost("slowbeta", callback))
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(10)
	begin := time.Now()
	result := applyAsync(f.ctx, writer, f.principal, create("budget", "budget"), meta)
	active := []running{await(t, f.ctx, started), await(t, f.ctx, started)}
	for _, call := range active {
		if call.remaining <= 0 || call.remaining > time.Second {
			t.Fatalf("callback entered without a positive <=1s deadline: %s", call.remaining)
		}
	}
	got := await(t, f.ctx, result)
	elapsed := time.Since(begin)
	if got.err != nil || len(got.commit.Warnings) != 2 || elapsed > 1500*time.Millisecond {
		t.Fatalf("uncooperative callbacks held response: commit=%+v err=%v elapsed=%s", got.commit, got.err, elapsed)
	}
	for _, warning := range got.commit.Warnings {
		if warning.Code != "post_subscriber_failed" {
			t.Fatalf("deadline became wrong warning=%+v", warning)
		}
	}
	for _, call := range active {
		if err := call.caps.revoked(); err != nil {
			t.Fatal(err)
		}
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending || head(t, f.store).String() != "1" {
		t.Fatalf("deadline damaged committed state=%+v", entry)
	}
	assertJSONEqual(t, entry.Commit, got.commit)
	once.Do(func() { close(release) })
	for range 2 {
		await(t, f.ctx, exited)
	}
	t.Logf("deadline proof: remaining_at_entry=%s,%s aggregate_elapsed=%s two warnings; running callbacks retained no usable I/O", active[0].remaining, active[1].remaining, elapsed)
}

func TestCallerCancellationAfterCommitStillFinalizesNativePostReceipt(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	var calls atomic.Int32
	host := f.host(t, nativePost("cancel", func(postCtx context.Context, _ pluginapi.Host, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
		calls.Add(1)
		if change.Revision.String() != "1" {
			return fmt.Errorf("post revision=%s", change.Revision)
		}
		cancel()
		if err := postCtx.Err(); err != nil {
			return fmt.Errorf("caller cancellation reached post: %w", err)
		}
		rows, err := reader.Get(postCtx, []pluginapi.Ref{change.Changes[0].Resource.Ref})
		if err != nil || len(rows) != 1 {
			return fmt.Errorf("post read after caller cancelled=%+v err=%v", rows, err)
		}
		return assertTitle(rows[0].Body, "cancel")
	}))
	meta := writeMeta(11)
	writer := f.writer(host, f.store, f.lease)
	commit, err := writer.Apply(ctx, f.principal, create("cancel", "cancel"), meta)
	if err != nil || ctx.Err() != context.Canceled || len(commit.Warnings) != 0 || calls.Load() != 1 {
		t.Fatalf("caller cancellation commit=%+v err=%v ctx=%v calls=%d", commit, err, ctx.Err(), calls.Load())
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending {
		t.Fatal("caller cancellation left pending receipt")
	}
	assertJSONEqual(t, entry.Commit, commit)
	t.Log("caller cancellation proof: committed native read and response finalization survive caller cancellation")
}
