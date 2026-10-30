package postsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

func pendingError(t *testing.T, err error, m model.WriteMeta, revision model.Revision) {
	t.Helper()
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != "busy" || !value.Data.Retryable {
		t.Fatalf("pending committed write must return retryable busy: %v", err)
	}
	same(t, map[string]any{"committedRevision": revision, "idempotencyKey": m.IdempotencyKey, "responsePending": true}, value.Data.Details)
}

func failedNative(path string) post {
	return post{"receipt", func(_ context.Context, c pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
		if err := appendEffect(path, c); err != nil {
			return err
		}
		return errors.New("actual disposable native destination failure")
	}}
}

func TestPendingSQLResponseRecoveryUsesActualNativeOutcome(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		journal := filepath.Join(t.TempDir(), "pending-native.ndjson")
		p := failedNative(journal)
		host := nativeHost(t, p)
		removeFault := rejectSQLUpdate(t, f, "response")
		capture := &finalizerCapture{Store: f.store, actual: f.store.(driver.ResponseFinalizer)}
		cs, m := create("pending-native"), meta(301)
		_, err := f.writer(host, capture).ApplyWithOutcome(f.ctx, writerPrincipal, cs, m)
		pending := f.receipt(t, m)
		pendingError(t, err, m, pending.Commit.Revision)
		if !pending.ResponsePending || pending.Commit.Revision.String() != "2" || len(pending.Commit.Warnings) != 0 || len(capture.calls) != 4 {
			t.Fatalf("actual SQL response rejection: pending=%+v finalizer_calls=%d", pending, len(capture.calls))
		}
		actual := capture.calls[0]
		for i, call := range capture.calls {
			same(t, actual, call)
			if capture.errors[i] == nil || !strings.Contains(capture.errors[i].Error(), "post_sql_rejected") {
				t.Fatalf("attempt %d did not reach the rejecting SQL trigger: %v", i, capture.errors[i])
			}
		}
		var warnings []model.Warning
		if err := json.Unmarshal(actual.Warnings, &warnings); err != nil || len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
			t.Fatalf("missing actual captured native warning: %s err=%v", actual.Warnings, err)
		}
		same(t, map[string]any{"binding": "post-receipt", "revision": pending.Commit.Revision}, warnings[0].Details)
		ref := pending.Commit.Results[0].Ref
		before := f.graph(t, ref)
		effectBefore := effects(t, journal)
		if bytes.Count(effectBefore, []byte{'\n'}) != 1 || pending.RequestHash == "" {
			t.Fatal("pending publication lacks actual effect or request identity")
		}
		// The duplicate is cancelled only after it reads the real pending SQL
		// receipt. The observer changes no storage result or callback outcome.
		observer := &pendingObserver{Store: f.store, ResponseFinalizer: f.store.(driver.ResponseFinalizer), seen: make(chan struct{})}
		ctx, cancel := context.WithCancel(f.ctx)
		defer cancel()
		duplicate := make(chan writeResult, 1)
		go func() {
			out, err := f.writer(host, observer).ApplyWithOutcome(ctx, writerPrincipal, cs, m)
			duplicate <- writeResult{out, err}
		}()
		await(t, f.ctx, observer.seen)
		cancel()
		pendingError(t, await(t, f.ctx, duplicate).err, m, pending.Commit.Revision)
		if _, err := f.writer(host, nil).Apply(f.ctx, writerPrincipal, create("different-pending-request"), m); code(err) != "idempotency_key_reused" {
			t.Fatalf("pending key accepted changed parameters: %v", err)
		}
		same(t, pending, f.receipt(t, m))
		same(t, before, f.graph(t, ref))
		same(t, effectBefore, effects(t, journal))
		removeFault()
		oldLease := f.lease
		f.reopen(t)
		same(t, pending, f.receipt(t, m))
		same(t, before, f.graph(t, ref))
		var fenced driver.ErrFenced
		if err := actual.finish(f.ctx, f.store, oldLease); !errors.As(err, &fenced) {
			t.Fatalf("old SQL epoch finalized a reopened response: %v", err)
		}
		same(t, pending, f.receipt(t, m))
		retained := f.reader(t)
		// Explicit recovery reuses the Writer's captured outcome. This is the
		// real SQL capability, not an invented warning or a claimed worker.
		if err := actual.finish(f.ctx, f.store, f.lease); err != nil {
			t.Fatal(err)
		}
		entry := f.receipt(t, m)
		if entry.ResponsePending {
			t.Fatal("active holder failed to finalize known SQL response")
		}
		same(t, warnings, entry.Commit.Warnings)
		same(t, pending.Changeset, entry.Changeset)
		same(t, pending.Commit.Results, entry.Commit.Results)
		same(t, pending.RequestHash, entry.RequestHash)
		old, err := retained.IdempotencyByKey(f.ctx, actual.Principal, actual.Agent, actual.Method, actual.Key)
		if err != nil || old == nil || !old.ResponsePending {
			t.Fatalf("finalization mutated a retained SQL reader: old=%+v err=%v", old, err)
		}
		_ = retained.Close()
		f.reopen(t)
		same(t, entry, f.receipt(t, m))
		replay := apply(t, f, f.writer(nativeHost(t, p), nil), cs, m)
		if !replay.Replayed {
			t.Fatal("known-outcome recovery did not produce a stored replay")
		}
		same(t, entry.Commit, replay.Commit)
		same(t, before, f.graph(t, ref))
		same(t, effectBefore, effects(t, journal))
		t.Log("pending SQL proof: four real trigger rejections, cancelled pending duplicate, persistent pending receipt, old epoch fenced, captured warning finalized by new holder, final bytes survive second reopen; no repeated native effect")
	})
}

func TestSQLFinalizerLostAcknowledgementReloadsPersistedWarning(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		journal := filepath.Join(t.TempDir(), "lost-ack-native.ndjson")
		p := failedNative(journal)
		capture := &finalizerCapture{Store: f.store, actual: f.store.(driver.ResponseFinalizer), loseAck: true}
		cs, m := create("lost-finalizer-ack"), meta(302)
		out := apply(t, f, f.writer(nativeHost(t, p), capture), cs, m)
		entry := f.receipt(t, m)
		if out.Replayed || entry.ResponsePending || len(out.Commit.Warnings) != 1 || len(capture.calls) != 1 || capture.errors[0] != nil {
			t.Fatalf("real SQL CAS/lost-ack reload failed: out=%+v entry=%+v calls=%d", out, entry, len(capture.calls))
		}
		same(t, entry.Commit, out.Commit)
		same(t, encoded(t, out.Commit.Warnings), []byte(capture.calls[0].Warnings))
		before := f.graph(t, out.Commit.Results[0].Ref)
		effectBefore := effects(t, journal)
		if bytes.Count(effectBefore, []byte{'\n'}) != 1 {
			t.Fatal("lost finalizer acknowledgement repeated native callback")
		}
		f.reopen(t)
		replay := apply(t, f, f.writer(nativeHost(t, p), nil), cs, m)
		if !replay.Replayed {
			t.Fatal("reopened finalizer result was not replayed")
		}
		same(t, entry.Commit, replay.Commit)
		same(t, entry, f.receipt(t, m))
		same(t, before, f.graph(t, out.Commit.Results[0].Ref))
		same(t, effectBefore, effects(t, journal))
		t.Log("lost SQL finalizer ack proof: real CAS succeeded once; Writer reloaded persisted warning before returning success; reopen replay adds no graph write or external effect")
	})
}
