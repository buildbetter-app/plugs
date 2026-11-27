package sqlite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Both cases return cancellation from the COMMIT execution itself, not merely
// cancel after a successful ExecContext has already returned to the store. The
// published case performs the real native COMMIT first, then deterministically
// reproduces modernc's deferred context check. The other case cancels just before
// native execution: the same public error must not be converted blindly to success.
func cancelMetadataCommit(t *testing.T, ctx context.Context, s *Store, path string, published bool) (context.Context, *publicationCommitConnector) {
	t.Helper()
	connector := hookPublicationCommit(t, s, path)
	call, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	connector.checkContextAfter = true
	if published {
		connector.after = cancel
	} else {
		connector.before = cancel
	}
	return call, connector
}

func requireMetadataIndeterminate(t *testing.T, err error, key string) {
	t.Helper()
	var unknown driver.ErrIndeterminate
	if !errors.As(err, &unknown) || unknown.IdempotencyKey != key {
		t.Fatalf("uncertain metadata COMMIT must return public ErrIndeterminate with key %q, got %T %v", key, err, err)
	}
	var fenced driver.ErrFenced
	var private commitError
	if errors.As(err, &fenced) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &private) {
		t.Fatalf("uncertain publication exposed a determinate rejection or private error: %T %v", err, err)
	}
	code, details, ok := driver.ErrorMapping(err)
	if !ok || code != "stale_snapshot" || details["outcome"] != "unknown" || details["idempotencyKey"] != key {
		t.Fatalf("uncertain metadata error mapping=%s %+v %v", code, details, ok)
	}
}

func requireUncertainLeaseLost(t *testing.T, ctx context.Context, lease driver.ActiveLease) {
	t.Helper()
	select {
	case <-lease.Lost():
	default:
		t.Fatal("uncertain native COMMIT did not fail the holder closed")
	}
	requireFenced(t, lease.Renew(ctx))
}

func assertCanceledCommitLease(t *testing.T, ctx context.Context, lease driver.ActiveLease, published bool) {
	t.Helper()
	if published {
		requireUncertainLeaseLost(t, ctx, lease)
	} else if err := lease.Renew(ctx); err != nil {
		t.Fatalf("confirmed rollback lost the active lease: %v", err)
	}
}

func TestSQLiteCheckpointCanceledCommitOutcomeUnknown(t *testing.T) {
	for _, method := range []string{"SaveCheckpoint", "SaveCheckpointIfRecord"} {
		for _, published := range []bool{false, true} {
			outcome := "unpublished"
			if published {
				outcome = "published"
			}
			t.Run(method+"/"+outcome, func(t *testing.T) {
				ctx := testContext(t)
				path := filepath.Join(t.TempDir(), "checkpoint-canceled-commit.db")
				s := openStore(t, ctx, path)
				observer := openStore(t, ctx, path)
				lease := getLease(t, ctx, s, driver.LeaseTTL)
				ref := testRef("pl-checkpoint-canceled-commit")
				entry := seedRecord(t, ctx, s, lease, ref.ID, `{"value":"guard"}`)
				meta := getMeta(t, ctx, observer)
				const key = "checkpoint-canceled-commit"
				initial, err := s.SaveCheckpoint(ctx, lease, key, 0, []byte("before"))
				if err != nil || initial.Generation != 1 {
					t.Fatalf("initial checkpoint=%+v err=%v", initial, err)
				}
				save := func(ctx context.Context, store *Store, value driver.ActiveLease, payload []byte) (driver.Checkpoint, error) {
					if method == "SaveCheckpointIfRecord" {
						return driver.RecordGuardedCheckpointStore(store).SaveCheckpointIfRecord(ctx, value, key, initial.Generation, payload, ref, entry.Revision, meta.Incarnation)
					}
					return driver.CheckpointStore(store).SaveCheckpoint(ctx, value, key, initial.Generation, payload)
				}
				call, connector := cancelMetadataCommit(t, ctx, s, path, published)
				payload := []byte("possibly published")
				saved, saveErr := save(call, s, lease, payload)
				want := initial
				wantCommits := int32(0)
				if published {
					want = driver.Checkpoint{Generation: initial.Generation + 1, Payload: payload}
					wantCommits = 1
				}
				durable, readErr := observer.LoadCheckpoint(ctx, key)
				if !errors.Is(call.Err(), context.Canceled) || connector.steps.Load() != wantCommits || readErr != nil || durable.Generation != want.Generation || !bytes.Equal(durable.Payload, want.Payload) {
					t.Fatalf("real COMMIT boundary: call=%v nativeCommits=%d durable=%+v err=%v want=%+v", call.Err(), connector.steps.Load(), durable, readErr, want)
				}
				requireMetadataIndeterminate(t, saveErr, "")
				if saved.Generation != 0 || saved.Payload != nil {
					t.Fatalf("uncertain COMMIT fabricated a successful checkpoint: %+v", saved)
				}
				assertCanceledCommitLease(t, ctx, lease, published)
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				next := getLease(t, ctx, observer, driver.LeaseTTL)
				// Recovery retains the original CAS generation; loading a newer
				// generation must never silently authorize another publication.
				recovered, retryErr := save(ctx, observer, next, payload)
				if published {
					if !errors.Is(retryErr, driver.ErrCheckpointConflict) {
						t.Fatalf("same-generation retry republished a committed checkpoint: %+v err=%v", recovered, retryErr)
					}
				} else if retryErr != nil || recovered.Generation != initial.Generation+1 || !bytes.Equal(recovered.Payload, payload) {
					t.Fatalf("unpublished checkpoint recovery=%+v err=%v", recovered, retryErr)
				}
				final, err := observer.LoadCheckpoint(ctx, key)
				if err != nil || final.Generation != initial.Generation+1 || !bytes.Equal(final.Payload, payload) || !reflect.DeepEqual(getMeta(t, ctx, observer), meta) {
					t.Fatalf("recovery duplicated checkpoint publication or changed graph metadata: %+v err=%v", final, err)
				}
				t.Logf("%s/%s: native COMMIT count=%d, durable generation=%d, public outcome=unknown without cancellation/fencing; original-generation retry remains safe", method, outcome, wantCommits, durable.Generation)
			})
		}
	}
}

func TestSQLiteFinalizerCanceledCommitOutcomeUnknown(t *testing.T) {
	for _, published := range []bool{false, true} {
		outcome := "unpublished"
		if published {
			outcome = "published"
		}
		t.Run(outcome, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "finalizer-canceled-commit.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			entry, h := commitReceipt(t, ctx, s, lease, "pl-finalizer-canceled-commit", true)
			meta := getMeta(t, ctx, observer)
			readReceipt := func() *driver.IdempotencyRecord {
				r := getReader(t, ctx, observer, meta, nil)
				defer r.Close()
				return lookupReceipt(t, ctx, r, h)
			}
			before := readReceipt()
			if !before.ResponsePending {
				t.Fatal("setup receipt was already finalized")
			}
			principal, err := driver.PrincipalKey(h.Principal)
			if err != nil {
				t.Fatal(err)
			}
			post := []model.Warning{{Code: "post_subscriber_failed", Message: "known durable post outcome"}}
			call, connector := cancelMetadataCommit(t, ctx, s, path, published)
			finalErr := driver.ResponseFinalizer(s).FinalizeResponse(call, lease, principal, h.AgentLabel, h.Method, h.IdempotencyKey, entry.Revision, post)
			durable := readReceipt()
			want := *before
			wantCommits := int32(0)
			if published {
				want.ResponsePending = false
				want.Commit.Warnings = append(append([]model.Warning{}, before.Commit.Warnings...), post...)
				wantCommits = 1
			}
			if !errors.Is(call.Err(), context.Canceled) || connector.steps.Load() != wantCommits || !reflect.DeepEqual(*durable, want) {
				t.Fatalf("real COMMIT boundary: call=%v nativeCommits=%d durable=%+v want=%+v", call.Err(), connector.steps.Load(), durable, want)
			}
			requireMetadataIndeterminate(t, finalErr, h.IdempotencyKey)
			assertCanceledCommitLease(t, ctx, lease, published)
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			next := getLease(t, ctx, observer, driver.LeaseTTL)
			for range 2 {
				if err := driver.ResponseFinalizer(observer).FinalizeResponse(ctx, next, principal, h.AgentLabel, h.Method, h.IdempotencyKey, entry.Revision, post); err != nil {
					t.Fatal(err)
				}
			}
			after := readReceipt()
			want.ResponsePending = false
			want.Commit.Warnings = append(append([]model.Warning{}, before.Commit.Warnings...), post...)
			if !reflect.DeepEqual(*after, want) || !reflect.DeepEqual(getMeta(t, ctx, observer), meta) {
				t.Fatalf("same-outcome recovery duplicated warnings or changed receipt/graph metadata: %+v want=%+v", after, want)
			}
			t.Logf("%s: native COMMIT count=%d, independent pending=%v, public outcome=unknown without cancellation/fencing; next holder's same-outcome retries preserve the receipt", outcome, wantCommits, durable.ResponsePending)
		})
	}
}
