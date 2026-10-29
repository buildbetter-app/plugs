package postsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

type writeResult struct {
	out port.WriteOutcome
	err error
}

func title(t *testing.T, raw []byte, want string) {
	t.Helper()
	var body struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Fields["title"] != want {
		t.Fatalf("title must be %q: body=%s err=%v", want, raw, err)
	}
}

func TestNativePostReadsCommittedSQLSnapshotAndReplaysAfterReopen(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		journal := filepath.Join(t.TempDir(), "native-effects.ndjson")
		entered := make(chan pluginapi.CommittedChange, 1)
		resume := make(chan struct{})
		read := make(chan []pluginapi.Record, 1)
		p := post{"snapshot", func(ctx context.Context, change pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
			entered <- change
			select {
			case <-resume:
			case <-ctx.Done():
				return ctx.Err()
			}
			refs := []pluginapi.Ref{change.Changes[0].Resource.Ref, change.Changes[1].Resource.Ref}
			rows, err := r.Get(ctx, refs)
			if err != nil {
				return err
			}
			read <- rows
			return appendEffect(journal, change)
		}}
		host := nativeHost(t, p)
		cs, m := create("committed-first", "committed-second"), meta(101)
		result := make(chan writeResult, 1)
		go func() {
			out, err := f.writer(host, nil).ApplyWithOutcome(f.ctx, writerPrincipal, cs, m)
			result <- writeResult{out, err}
		}()
		change := await(t, f.ctx, entered)
		if change.Revision.String() != "2" || len(change.Changes) != 2 || change.RequestID != m.IdempotencyKey {
			t.Fatalf("native committed change=%+v", change)
		}
		same(t, writerPrincipal, change.Principal)
		pending := f.receipt(t, m)
		if !pending.ResponsePending || pending.Commit.Revision.String() != "2" {
			t.Fatalf("callback ran without a real pending committed receipt: %+v", pending)
		}
		ref := pending.Commit.Results[0].Ref
		advanced := apply(t, f, f.writer(nil, nil), patch(ref, "newer-head"), meta(102))
		if advanced.Commit.Revision.String() != "3" {
			t.Fatal("independent Writer did not advance SQL head during post callback")
		}
		close(resume)
		completed := await(t, f.ctx, result)
		if completed.err != nil || completed.out.Replayed || len(completed.out.Commit.Warnings) != 0 {
			t.Fatalf("native snapshot result=%+v", completed)
		}
		rows := await(t, f.ctx, read)
		if len(rows) != 2 {
			t.Fatalf("native snapshot returned %d rows", len(rows))
		}
		for i, want := range []string{"committed-first", "committed-second"} {
			if rows[i].Revision.String() != "2" {
				t.Fatalf("native read escaped committed revision: %+v", rows[i])
			}
			title(t, rows[i].Body, want)
			title(t, change.Changes[i].After, want)
		}
		entry := f.receipt(t, m)
		if entry.ResponsePending || len(entry.Changeset.Events) != 2 {
			t.Fatalf("native completion did not finalize the actual SQL response: %+v", entry)
		}
		same(t, pending.Changeset, entry.Changeset)
		same(t, entry.Commit, completed.out.Commit)
		refs := []model.Ref{entry.Commit.Results[0].Ref, entry.Commit.Results[1].Ref}
		before := f.graph(t, refs...)
		title(t, before.Records[0].Body, "newer-head")
		if len(before.Log) != 3 || len(before.Versions[0]) != 2 || len(before.Versions[1]) != 1 {
			t.Fatalf("unexpected SQL history: %+v", before)
		}
		effectBefore := effects(t, journal)
		if bytes.Count(effectBefore, []byte{'\n'}) != 1 {
			t.Fatal("native callback did not persist exactly one disposable effect")
		}
		f.reopen(t)
		same(t, before, f.graph(t, refs...))
		same(t, entry, f.receipt(t, m))
		replayed := apply(t, f, f.writer(nativeHost(t, p), nil), cs, m)
		if !replayed.Replayed {
			t.Fatal("stored SQL response did not report replay")
		}
		same(t, entry.Commit, replayed.Commit)
		same(t, effectBefore, effects(t, journal))
		same(t, before, f.graph(t, refs...))
		t.Log("native SQL proof: post reads revision 2 after head 3; finalized bytes, versions, log and external effect persist across handle reopen; replay runs no post")
	})
}

func TestNativeWarningAndSuppressedPostsRemainDurableOnSQL(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		good, bad := filepath.Join(t.TempDir(), "success.ndjson"), filepath.Join(t.TempDir(), "failure.ndjson")
		posts := []post{
			{"good", func(_ context.Context, c pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
				return appendEffect(good, c)
			}},
			{"bad", func(_ context.Context, c pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
				if err := appendEffect(bad, c); err != nil {
					return err
				}
				return errors.New("disposable destination failed after observing the committed write")
			}},
		}
		w := f.writer(nativeHost(t, posts...), nil)
		cs, m := create("warning-source"), meta(201)
		out := apply(t, f, w, cs, m)
		if out.Replayed || len(out.Commit.Warnings) != 1 || out.Commit.Warnings[0].Code != "post_subscriber_failed" {
			t.Fatalf("expected a committed warning: %+v", out)
		}
		same(t, map[string]any{"binding": "post-bad", "revision": out.Commit.Revision}, out.Commit.Warnings[0].Details)
		entry := f.receipt(t, m)
		if entry.ResponsePending {
			t.Fatal("native warning was not finalized in SQL")
		}
		same(t, out.Commit, entry.Commit)
		ref := out.Commit.Results[0].Ref
		before := f.graph(t, ref)
		goodBefore, badBefore := effects(t, good), effects(t, bad)
		for _, data := range [][]byte{goodBefore, badBefore} {
			if bytes.Count(data, []byte{'\n'}) != 1 {
				t.Fatal("peer callback effect missing or repeated")
			}
		}
		if _, err := w.Apply(f.ctx, writerPrincipal, create("changed-request"), m); code(err) != "idempotency_key_reused" {
			t.Fatalf("changed request reused stored response: %v", err)
		}
		noopMeta := meta(202)
		noop := apply(t, f, w, patch(ref, "warning-source"), noopMeta)
		if noop.Replayed || len(noop.Commit.Warnings) != 0 || noop.Commit.Revision.String() != "2" || f.receipt(t, noopMeta).ResponsePending {
			t.Fatalf("no-op incorrectly ran post or lacked a final receipt: %+v", noop)
		}
		dryMeta := meta(203)
		dryMeta.DryRun = true
		dry := apply(t, f, w, patch(ref, "dry-run-only"), dryMeta)
		if !dry.Commit.WouldCommit || dry.Replayed || len(dry.Commit.Warnings) != 0 {
			t.Fatalf("dry-run post suppression failed: %+v", dry)
		}
		if _, err := w.Apply(f.ctx, writerPrincipal, model.ChangeSet{}, meta(204)); code(err) != "invalid_params" {
			t.Fatalf("invalid write was not refused: %v", err)
		}
		r := f.reader(t)
		principal, _ := driver.PrincipalKey(writerPrincipal)
		for _, absent := range []model.WriteMeta{dryMeta, meta(204)} {
			got, err := r.IdempotencyByKey(f.ctx, principal, "", absent.Method, absent.IdempotencyKey)
			if err != nil || got != nil {
				t.Fatalf("non-committed operation published a receipt: %+v err=%v", got, err)
			}
		}
		_ = r.Close()
		same(t, before, f.graph(t, ref))
		f.reopen(t)
		replay := apply(t, f, f.writer(nativeHost(t, posts...), nil), cs, m)
		if !replay.Replayed {
			t.Fatal("reopened native warning did not replay")
		}
		same(t, entry.Commit, replay.Commit)
		same(t, entry, f.receipt(t, m))
		same(t, goodBefore, effects(t, good))
		same(t, badBefore, effects(t, bad))
		same(t, before, f.graph(t, ref))
		t.Log("native warning proof: failed binding and successful peer both observe SQL commit; warning survives reopen; changed-key request refuses; no-op/dry-run/rejection add no post effects or graph history")
	})
}
