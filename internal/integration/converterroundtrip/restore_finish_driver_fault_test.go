package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	coreRestore "github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// restoreFinishFaultStore injects at the storage transaction boundary. The
// real driver writer and its optional restore capabilities are
// still used for every operation before Commit.
type restoreFinishFaultStore struct {
	driver.Store
	beforeCommit func(context.Context) error
}

func (s *restoreFinishFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	importTx, ok := tx.(driver.RestoreImportTx)
	if !ok {
		_ = tx.Close()
		return nil, driver.ErrUnsupported
	}
	allocators, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		_ = tx.Close()
		return nil, driver.ErrUnsupported
	}
	return &restoreFinishFaultTx{
		WriteTx:               tx,
		RestoreImportTx:       importTx,
		AllocatorContinuation: allocators,
		beforeCommit:          s.beforeCommit,
	}, nil
}

type restoreFinishFaultTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	beforeCommit func(context.Context) error
}

func (t *restoreFinishFaultTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	if t.beforeCommit != nil {
		if err := t.beforeCommit(ctx); err != nil {
			return model.Changeset{}, err
		}
	}
	return t.WriteTx.Commit(ctx, header)
}

func TestRestoreFinishDriverFaultsClassifyAcrossSQLiteAndPostgres(t *testing.T) {
	ctx := context.Background()
	data := admittedFixtureBytes(t)
	profile, archive := admittedImportProfile(t, data)
	var manifest map[string]any
	manifestBytes, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("fixture has no manifest")
	}
	decode(t, manifestBytes, &manifest)
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := hashBytes(canonical)
	presence := coreRestore.Presence{"mode": "presence_check", "challenge": "driver-fault", "proof": "driver-fault"}
	verifier := coreRestore.PresenceVerifierFunc(func(context.Context, string, string, coreRestore.Presence) error { return nil })

	cases := []struct {
		name      string
		fault     error
		code      model.ErrorCode
		retryable bool
		hint      string
	}{
		{
			name:      "fenced-commit-is-busy",
			fault:     driver.ErrFenced{ExpectedEpoch: 7, ObservedEpoch: 8},
			code:      "busy",
			retryable: true,
			hint:      "active storage lease was lost",
		},
		{
			name:      "receipt-field-mismatch-is-violation",
			fault:     driver.ErrRestoreBindingMismatch{Field: "requestHash"},
			code:      "restore_violation",
			retryable: false,
			hint:      "driver transaction conflict: restore import binding field requestHash does not match the durable receipt",
		},
	}
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, tc := range cases {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				store, lease, _ := openIdentityTarget(t, engine)
				faulted := &restoreFinishFaultStore{Store: store, beforeCommit: func(context.Context) error { return tc.fault }}
				manager := coreRestore.NewManager(faulted, lease, verifier)
				manager.SetImportHook(func(ctx context.Context, _ string, _ model.PrincipalRef, archiveBytes []byte, _ *dumpformat.Archive, _ driver.RestoreHistoryMode, _, _ string, meta model.WriteMeta, _ coreRestore.Presence) (coreRestore.RestoreStatus, error) {
					if meta.IdempotencyKey != profile.IdempotencyKey {
						return coreRestore.RestoreStatus{}, errors.New("fault test lost the finish receipt key")
					}
					report, err := RestoreImportWithLease(ctx, bytes.NewReader(archiveBytes), faulted, lease, profile)
					return coreRestore.RestoreStatus{Incarnation: report.Restored.Incarnation}, err
				})
				principal := profile.Admission.Actor
				status, err := manager.BeginAs(ctx, principal, manifest, "head", presence)
				if err != nil {
					t.Fatal(err)
				}
				for page, offset := uint64(0), 0; offset < len(data); page++ {
					end := offset + 512
					if end > len(data) {
						end = len(data)
					}
					if _, err := manager.ChunkAs(ctx, principal, status.RestoreID, page, data[offset:end], hashBytes(data[offset:end]), presence); err != nil {
						t.Fatal(err)
					}
					offset = end
				}
				_, err = manager.Finish(ctx, status.RestoreID, manifestHash, principal, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: profile.IdempotencyKey}, presence)
				var typed *model.Error
				if err == nil || !errors.As(err, &typed) || typed.Data.Code != tc.code || typed.Data.Retryable != tc.retryable || typed.Data.Hint != tc.hint {
					t.Fatalf("finish error=%v want code=%s retryable=%t hint=%q", err, tc.code, tc.retryable, tc.hint)
				}
			})
		}
	}
}

func TestAdminRestoreDeadlineAndCancellationAreRetryableAcrossEngines(t *testing.T) {
	ctx := context.Background()
	data := admittedFixtureBytes(t)
	profile, archive := admittedImportProfile(t, data)
	defer archive.ReleaseNativeDecoded()
	profile.Admission.Mode = "anonymous-local"
	profile.Admission.Actor = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	profile.Admission.Authority = "trusted-human-presence"
	plan, err := buildPlanWithProfile(archive, &profile, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	profile.DerivedDigest = plan.derivedDigest
	manifest := archive.Manifest()
	presence := coreRestore.Presence{"mode": "presence_check", "challenge": "interrupt", "proof": "interrupt"}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, _ := openIdentityTarget(t, engine)
			before := reverseState(t, store)
			target := &restoreFinishFaultStore{Store: store}
			var presenceFault func(context.Context) error
			manager := coreRestore.NewManager(target, lease, coreRestore.PresenceVerifierFunc(func(ctx context.Context, _ string, _ string, _ coreRestore.Presence) error {
				if presenceFault != nil {
					return presenceFault(ctx)
				}
				return nil
			}))
			manager.SetImportHook(func(ctx context.Context, _ string, principal model.PrincipalRef, _ []byte, parsed *dumpformat.Archive, _ driver.RestoreHistoryMode, _, _ string, meta model.WriteMeta, _ coreRestore.Presence) (coreRestore.RestoreStatus, error) {
				if !reflect.DeepEqual(principal, profile.Admission.Actor) || meta.IdempotencyKey != profile.IdempotencyKey {
					return coreRestore.RestoreStatus{}, errors.New("restore lost its admitted principal or key")
				}
				report, err := RestoreValidatedImportWithLease(ctx, parsed, target, lease, profile)
				return coreRestore.RestoreStatus{Incarnation: report.Restored.Incarnation}, err
			})
			registry, err := rpc.NewRegistry()
			if err != nil {
				t.Fatal(err)
			}
			if err := rpc.BindAdmin(registry, manager); err != nil {
				t.Fatal(err)
			}
			server, err := rpc.New(registry, rpc.Config{Incarnation: before.Meta.Incarnation, DefaultProject: "fixture"})
			if err != nil {
				t.Fatal(err)
			}
			expectUnknown := false
			call := func(ctx context.Context, method, key string, params map[string]any, want string) coreRestore.RestoreStatus {
				t.Helper()
				params["_meta"] = map[string]any{"idempotencyKey": key}
				request := encode(t, map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
				var reply struct {
					Result *coreRestore.RestoreStatus
					Error  *struct {
						Data model.ErrorData
					}
				}
				raw := server.Dispatch(ctx, request)
				decode(t, raw, &reply)
				if want != "" {
					if reply.Result != nil || reply.Error == nil || string(reply.Error.Data.Code) != want || reply.Error.Data.Retryable != (want == "busy") {
						t.Fatalf("%s = %s, want %s", method, raw, want)
					}
					if expectUnknown && (reply.Error.Data.Details["outcome"] != "unknown" || reply.Error.Data.Details["idempotencyKey"] != profile.IdempotencyKey) {
						t.Fatalf("interrupted commit lost its keyed unknown outcome: %s", raw)
					}
					return coreRestore.RestoreStatus{}
				}
				if reply.Error != nil || reply.Result == nil {
					t.Fatalf("%s = %s, want result", method, raw)
				}
				return *reply.Result
			}
			receipt := func() *driver.IdempotencyRecord {
				t.Helper()
				meta, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				key, err := driver.PrincipalKey(profile.Admission.Actor)
				if err != nil {
					t.Fatal(err)
				}
				row, err := reader.IdempotencyByKey(ctx, key, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
				if err != nil {
					t.Fatal(err)
				}
				return row
			}
			begin := map[string]any{"manifest": manifest, "history": "head", "humanPresence": presence}
			presenceFault = func(context.Context) error { return errors.New("refused proof") }
			call(ctx, "admin.restore.begin", profile.IdempotencyKey, begin, "human_presence_required")
			presenceFault = func(context.Context) error { return fmt.Errorf("presence: %w", context.DeadlineExceeded) }
			call(ctx, "admin.restore.begin", profile.IdempotencyKey, begin, "busy")
			presenceFault = nil
			session := call(ctx, "admin.restore.begin", profile.IdempotencyKey, begin, "")
			chunk := map[string]any{"restoreId": session.RestoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": hashBytes(data), "humanPresence": presence}
			cancelledChunk, cancelChunk := context.WithCancel(ctx)
			presenceFault = func(context.Context) error {
				cancelChunk()
				return nil
			}
			call(cancelledChunk, "admin.restore.chunk", strings.Repeat("c", 32), chunk, "busy")
			presenceFault = nil
			if got := call(ctx, "admin.restore.chunk", strings.Repeat("c", 32), chunk, ""); got.ReceivedPages != 1 {
				t.Fatalf("retry did not stage exactly one page: %+v", got)
			}
			finish := map[string]any{"restoreId": session.RestoreID, "manifestSha256": hashBytes(bytes.TrimSuffix(encode(t, manifest), []byte{'\n'})), "humanPresence": presence}
			finishKey := strings.Repeat("d", 32)
			commitAttempts := 0
			target.beforeCommit = func(ctx context.Context) error {
				commitAttempts++
				// Start the timer only after the real driver staged the import.
				slow, stop := context.WithTimeout(ctx, time.Millisecond)
				defer stop()
				<-slow.Done()
				return fmt.Errorf("staging reached publication: %w", slow.Err())
			}
			call(ctx, "admin.restore.finish", finishKey, finish, "busy")
			if commitAttempts != 1 || !reflect.DeepEqual(before, reverseState(t, store)) || receipt() != nil {
				t.Fatal("deadline did not reach publication or changed durable state")
			}
			cancelledFinish, cancelFinish := context.WithCancel(ctx)
			target.beforeCommit = func(ctx context.Context) error {
				commitAttempts++
				cancelFinish()
				// A failed SQL observation may carry both a transaction error
				// and a binding check's wrapper instead of a context cause.
				return errors.Join(driver.ErrRestoreBindingMismatch{Field: "receipt"}, sql.ErrTxDone)
			}
			call(cancelledFinish, "admin.restore.finish", finishKey, finish, "busy")
			if commitAttempts != 2 || !reflect.DeepEqual(before, reverseState(t, store)) || receipt() != nil {
				t.Fatal("cancellation did not reach publication or changed durable state")
			}
			// Exercise an unsettled interrupted COMMIT on a separate session.
			// Retrying that ID must report the same keyed unknown outcome and
			// never make a second commit attempt.
			unresolved := call(ctx, "admin.restore.begin", profile.IdempotencyKey, begin, "")
			chunk["restoreId"] = unresolved.RestoreID
			call(ctx, "admin.restore.chunk", strings.Repeat("c", 32), chunk, "")
			target.beforeCommit = func(context.Context) error {
				commitAttempts++
				return driver.ErrIndeterminate{IdempotencyKey: profile.IdempotencyKey, RestoreInterrupted: true}
			}
			expectUnknown = true
			finish["restoreId"] = unresolved.RestoreID
			call(ctx, "admin.restore.finish", finishKey, finish, "busy")
			call(ctx, "admin.restore.finish", finishKey, finish, "busy")
			if commitAttempts != 3 || !reflect.DeepEqual(before, reverseState(t, store)) || receipt() != nil {
				t.Fatal("an unknown interrupted COMMIT admitted another write")
			}
			expectUnknown = false
			finish["restoreId"] = session.RestoreID
			target.beforeCommit = nil
			completed := call(ctx, "admin.restore.finish", finishKey, finish, "")
			durable := receipt()
			if completed.State != "complete" || completed.Incarnation == before.Meta.Incarnation || durable == nil || durable.ResponsePending || durable.Changeset.Incarnation != completed.Incarnation {
				t.Fatalf("same-session retry did not publish once: status=%+v receipt=%v", completed, durable)
			}
		})
	}
}
