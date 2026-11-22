package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	coreRestore "github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// restoreFinishFaultStore injects at the storage transaction boundary. The
// real SQLite or PostgreSQL writer and its optional restore capabilities are
// still used for every operation before Commit.
type restoreFinishFaultStore struct {
	driver.Store
	fault error
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
		fault:                 s.fault,
	}, nil
}

type restoreFinishFaultTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	fault error
}

func (t *restoreFinishFaultTx) Commit(context.Context, driver.ChangesetHeader) (model.Changeset, error) {
	return model.Changeset{}, t.fault
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
				faulted := &restoreFinishFaultStore{Store: store, fault: tc.fault}
				manager := coreRestore.NewManager(faulted, lease, verifier)
				manager.SetImportHook(func(ctx context.Context, _ string, _ model.PrincipalRef, archiveBytes []byte, _ string, _ driver.RestoreHistoryMode, _, _ string, meta model.WriteMeta, _ coreRestore.Presence) (coreRestore.RestoreStatus, error) {
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
