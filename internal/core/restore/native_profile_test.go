package restore_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func nativeProfileArchive(t *testing.T) ([]byte, *dumpformat.Archive) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "native", Agent: "source", Storage: cli.StorageConfig{Driver: "provided"}, PresenceVerifier: trustedPresenceVerifier(),
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return store, store.Close, nil
		},
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "native identity fixture"}}, ""); err != nil {
		t.Fatal(err)
	}
	data, _, err := restore.Dump(ctx, store, "head")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return data, archive
}

func TestNativeRestorePublishesArchiveIdentityReceiptAndFloor(t *testing.T) {
	data, archive := nativeProfileArchive(t)
	manifest, manifestHash := manifestForRestore(t, archive)
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "native.db")
			var store driver.Store = memory.New()
			if kind == "sqlite" {
				store.Close()
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if string(before.StoreID) == archive.Manifest().NativeStoreID {
				t.Fatal("test requires distinct source and target identities")
			}
			lease, err := store.Lease(ctx, "native-profile", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			manager := restore.NewManager(store, lease, trustedPresenceVerifier())
			status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
			if err != nil {
				t.Fatal(err)
			}
			uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
			principal := model.PrincipalRef{Kind: "user", ID: "native-owner", Sponsor: &model.PrincipalRef{Kind: "service", ID: "native-host"}, Caveats: map[string]any{"purpose": "restore"}}
			meta := model.WriteMeta{Method: "admin.restore.finish", AgentLabel: "native-agent", IdempotencyKey: "native-final-key"}
			status, err = manager.Finish(ctx, status.RestoreID, manifestHash, principal, meta, restore.Presence(presence))
			if err != nil || status.State != "complete" {
				t.Fatalf("native finish=%+v %v", status, err)
			}
			after, err := store.Meta(ctx)
			if err != nil || string(after.StoreID) != archive.Manifest().NativeStoreID || after.Incarnation == before.Incarnation || after.Incarnation == archive.Manifest().NativeIncarnation || after.Incarnation != status.Incarnation || after.Head.String() != "1" || after.HistoryFloor.String() != "1" {
				t.Fatalf("native metadata=%+v completion=%+v err=%v", after, status, err)
			}
			check := func(store driver.Store) {
				t.Helper()
				got, err := store.Meta(ctx)
				if err != nil || !reflect.DeepEqual(got, after) {
					t.Fatalf("durable native identity=%+v err=%v", got, err)
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: got.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				page, err := reader.Query(ctx, driver.Query{Limit: 10})
				if err != nil || len(page.Items) != 1 || page.Items[0].Revision.String() != "1" {
					t.Fatalf("native records=%+v %v", page, err)
				}
				versions, err := reader.Versions(ctx, page.Items[0].Ref, driver.PageReq{Limit: 10})
				if err != nil || len(versions.Items) != 1 || versions.Items[0].Revision.String() != "1" {
					t.Fatalf("native versions=%+v %v", versions, err)
				}
				key, _ := driver.PrincipalKey(principal)
				receipt, err := reader.IdempotencyByKey(ctx, key, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
				if err != nil || receipt == nil || receipt.ResponsePending || receipt.RequestHash != sha(data) || !reflect.DeepEqual(receipt.Changeset.Principal, principal) || receipt.Changeset.Incarnation != after.Incarnation || receipt.Changeset.Revision.String() != "1" || len(receipt.Commit.Results) != 1 {
					t.Fatalf("native durable receipt=%+v err=%v", receipt, err)
				}
				var truncated driver.ErrTruncated
				if _, err := reader.Changesets(ctx, nil, 10); !errors.As(err, &truncated) {
					t.Fatalf("native discarded history accepted: %v", err)
				}
			}
			check(store)
			if dump, _, err := restore.Dump(ctx, store, "head"); err != nil || len(dump) == 0 {
				t.Fatalf("post-restore head dump=%d err=%v", len(dump), err)
			}
			var truncated driver.ErrTruncated
			if _, _, err := restore.Dump(ctx, store, "full"); !errors.As(err, &truncated) {
				t.Fatalf("post-restore full history accepted: %v", err)
			}
			_, err = manager.Begin(ctx, manifest, "head", restore.Presence(presence))
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "restore_requires_empty" {
				t.Fatalf("initialized native restore=%v", err)
			}
			check(store)
			if kind == "sqlite" {
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				check(reopened)
			}
		})
	}
}

// Change only the bound manifest identity of a real native archive. Its member
// checksums remain valid, so the exact native identity check is exercised.
func rewriteNativeIdentity(t *testing.T, input []byte, change func(*dumpformat.Manifest)) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var out bytes.Buffer
	reader, writer := tar.NewReader(bytes.NewReader(input)), tar.NewWriter(&out)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "manifest.json" {
			var manifest dumpformat.Manifest
			if err := json.Unmarshal(data, &manifest); err != nil {
				t.Fatal(err)
			}
			change(&manifest)
			data, err = json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
		}
		header.Size = int64(len(data))
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), archive
}

func TestNativeRestoreRefusesInconsistentBoundIdentityBeforeWrite(t *testing.T) {
	input, _ := nativeProfileArchive(t)
	for _, scenario := range []string{"mismatched", "whitespace", "nul"} {
		data, archive := rewriteNativeIdentity(t, input, func(m *dumpformat.Manifest) {
			switch scenario {
			case "mismatched":
				m.StoreID = "different"
			case "whitespace":
				m.NativeStoreID = " source "
				m.StoreID = m.NativeStoreID
			case "nul":
				m.NativeStoreID = "source\x00id"
				m.StoreID = m.NativeStoreID
			}
		})
		for _, kind := range []string{"memory", "sqlite"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				var store driver.Store = memory.New()
				if kind == "sqlite" {
					store.Close()
					var err error
					store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "invalid.db")})
					if err != nil {
						t.Fatal(err)
					}
				}
				defer store.Close()
				before, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := store.Lease(ctx, "native-invalid", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(ctx)
				manager := restore.NewManager(store, lease, trustedPresenceVerifier())
				manifest, hash := manifestForRestore(t, archive)
				status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
				if err != nil {
					t.Fatal(err)
				}
				uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
				_, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "native"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "invalid-native"}, restore.Presence(presence))
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Code != "restore_violation" || !strings.Contains(typed.Data.Hint, "StoreID") {
					t.Fatalf("identity refusal=%v", err)
				}
				after, err := store.Meta(ctx)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("invalid identity changed metadata: %+v %v", after, err)
				}
				assertEmptyRestoreTarget(t, ctx, store)
				reader, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				if receipt, err := reader.IdempotencyByKey(ctx, "user:native", "", "admin.restore.finish", "invalid-native"); err != nil || receipt != nil {
					t.Fatalf("invalid identity published receipt: %+v %v", receipt, err)
				}
			})
		}
	}
}

type nativeRestoreFaultStore struct {
	driver.Store
	fault   string
	commits int
}

func (s *nativeRestoreFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	if s.fault == "unsupported" {
		return struct{ driver.WriteTx }{tx}, nil
	}
	return &nativeRestoreFaultTx{WriteTx: tx, owner: s}, nil
}

type nativeRestoreFaultTx struct {
	driver.WriteTx
	owner *nativeRestoreFaultStore
}

func (t *nativeRestoreFaultTx) RestoreNativeAllocators(ctx context.Context, snapshot driver.AllocatorSnapshot) error {
	return t.WriteTx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, snapshot)
}

func (t *nativeRestoreFaultTx) PrepareNativeRestore(ctx context.Context, p driver.NativeRestoreProfile) (driver.Meta, error) {
	return t.WriteTx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, p)
}
func (t *nativeRestoreFaultTx) Put(ctx context.Context, r driver.Record) error {
	if err := t.WriteTx.Put(ctx, r); err != nil {
		return err
	}
	if t.owner.fault == "stage" {
		return fmt.Errorf("injected native staging failure")
	}
	return nil
}
func (t *nativeRestoreFaultTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.owner.commits++
	if t.owner.fault == "interrupted-uncommitted" {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey, RestoreInterrupted: true}
	}
	if t.owner.fault == "uncertain-uncommitted" {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	entry, err := t.WriteTx.Commit(ctx, h)
	if err == nil && t.owner.fault == "uncertain-committed" {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	return entry, err
}

func TestRestoreInterruptedOutcomeSurvivesLostLease(t *testing.T) {
	data, archive := nativeProfileArchive(t)
	manifest, hash := manifestForRestore(t, archive)
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "interrupted-restore", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	target := &nativeRestoreFaultStore{Store: store, fault: "interrupted-uncommitted"}
	bound := trustedPresenceVerifier()
	reject := false
	manager := restore.NewManager(target, lease, restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, proof restore.Presence) error {
		if reject {
			return errors.New("refused proof")
		}
		return bound.Verify(ctx, method, digest, proof)
	}))
	proof := restore.Presence(presence)
	principal := model.PrincipalRef{Kind: "user", ID: "interrupted-restore-owner"}
	meta := model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "interrupted-restore-key"}
	session, err := manager.BeginAs(ctx, principal, manifest, "head", proof)
	if err != nil {
		t.Fatal(err)
	}
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := min(offset+1024, len(data))
		if _, err := manager.ChunkAs(ctx, principal, session.RestoreID, page, data[offset:end], sha(data[offset:end]), proof); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	assertPending := func() {
		t.Helper()
		_, err := manager.Finish(ctx, session.RestoreID, hash, principal, meta, proof)
		var uncertain driver.ErrIndeterminate
		if !errors.As(err, &uncertain) || !uncertain.RestoreInterrupted || uncertain.IdempotencyKey != meta.IdempotencyKey {
			t.Fatalf("finish lost the interrupted publication outcome: %v", err)
		}
	}
	assertPending()
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	reject = true
	_, err = manager.Finish(ctx, session.RestoreID, hash, principal, meta, proof)
	var refused *model.Error
	if !errors.As(err, &refused) || refused.Data.Code != "human_presence_required" {
		t.Fatalf("unverified caller could inspect the unknown publication: %v", err)
	}
	reject = false
	assertPending()
	if target.commits != 1 {
		t.Fatalf("read-only repeat entered commit %d times", target.commits)
	}
	assertEmptyRestoreTarget(t, ctx, store)
}

func TestNativeRestoreFailureCleanupAndReadOnlyUncertainLookup(t *testing.T) {
	data, archive := nativeProfileArchive(t)
	manifest, hash := manifestForRestore(t, archive)
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		for _, fault := range []string{"unsupported", "stage", "uncertain-uncommitted", "uncertain-committed"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				var store driver.Store = memory.New()
				if kind == "sqlite" {
					store.Close()
					var err error
					store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "fault.db")})
					if err != nil {
						t.Fatal(err)
					}
				}
				defer store.Close()
				before, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := store.Lease(ctx, "native-fault", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(ctx)
				target := &nativeRestoreFaultStore{Store: store, fault: fault}
				manager := restore.NewManager(target, lease, trustedPresenceVerifier())
				status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
				if err != nil {
					t.Fatal(err)
				}
				uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
				meta := model.WriteMeta{Method: "admin.restore.finish", AgentLabel: "native-fault-agent", IdempotencyKey: "native-fault-key"}
				status, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "native-fault-owner"}, meta, restore.Presence(presence))
				if err == nil || status.State == "complete" {
					t.Fatalf("failed native restore reported completion: %+v %v", status, err)
				}
				if strings.HasPrefix(fault, "uncertain") {
					var uncertain driver.ErrIndeterminate
					if !errors.As(err, &uncertain) || uncertain.IdempotencyKey != meta.IdempotencyKey || target.commits != 1 {
						t.Fatalf("native uncertainty retried/lost key: calls=%d %v", target.commits, err)
					}
				}
				after, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := reader.IdempotencyByKey(ctx, "user:native-fault-owner", meta.AgentLabel, meta.Method, meta.IdempotencyKey)
				reader.Close()
				if err != nil {
					t.Fatal(err)
				}
				if fault == "uncertain-committed" {
					if receipt == nil || receipt.RequestHash != sha(data) || receipt.ResponsePending || after.Head.String() != "1" || after.Incarnation == before.Incarnation {
						t.Fatalf("native read-only recovery=%+v meta=%+v", receipt, after)
					}
				} else {
					if !reflect.DeepEqual(before, after) || receipt != nil {
						t.Fatalf("native failed attempt published: meta=%+v receipt=%+v", after, receipt)
					}
					assertEmptyRestoreTarget(t, ctx, store)
					// A new eligible transaction can acquire the same complete
					// locks and finish after the failed transaction's Close.
					fresh := restore.NewManager(store, lease, trustedPresenceVerifier())
					s, err := fresh.Begin(ctx, manifest, "head", restore.Presence(presence))
					if err != nil {
						t.Fatal(err)
					}
					uploadManagerArchive(t, ctx, fresh, s.RestoreID, data, restore.Presence(presence))
					if _, err := fresh.Finish(ctx, s.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "native-retry"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "native-retry-key"}, restore.Presence(presence)); err != nil {
						t.Fatalf("native failure leaked locks/transaction: %v", err)
					}
				}
			})
		}
	}
}

type nativeCompletionStore struct {
	driver.Store
	mode         string
	committed    model.Changeset
	commits      int
	observations int
	cancel       context.CancelFunc
}

func (s *nativeCompletionStore) Meta(ctx context.Context) (driver.Meta, error) {
	meta, err := s.Store.Meta(ctx)
	if s.committed.Incarnation == "" {
		return meta, err
	}
	s.observations++
	if ctx.Err() != nil {
		return driver.Meta{}, fmt.Errorf("completion observation retained cancelled context")
	}
	if s.mode == "unavailable" {
		return driver.Meta{}, fmt.Errorf("injected post-commit metadata failure")
	}
	if s.mode == "later-incarnation" {
		meta.Incarnation = "later-observed-incarnation"
	}
	return meta, err
}

func (s *nativeCompletionStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &nativeCompletionTx{WriteTx: tx, owner: s}, nil
}

type nativeCompletionTx struct {
	driver.WriteTx
	owner *nativeCompletionStore
}

func (t *nativeCompletionTx) RestoreNativeAllocators(ctx context.Context, snapshot driver.AllocatorSnapshot) error {
	return t.WriteTx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, snapshot)
}

func (t *nativeCompletionTx) PrepareNativeRestore(ctx context.Context, p driver.NativeRestoreProfile) (driver.Meta, error) {
	return t.WriteTx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, p)
}

func (t *nativeCompletionTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.owner.commits++
	entry, err := t.WriteTx.Commit(ctx, h)
	if err == nil {
		t.owner.committed = entry
		if t.owner.mode == "cancelled" {
			t.owner.cancel()
		}
	}
	return entry, err
}

func TestNativeRestoreCompletionRetainsAcknowledgedIncarnation(t *testing.T) {
	data, archive := nativeProfileArchive(t)
	manifest, hash := manifestForRestore(t, archive)
	for _, kind := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"unavailable", "later-incarnation", "cancelled"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var store driver.Store = memory.New()
				if kind == "sqlite" {
					store.Close()
					var err error
					store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "completion.db")})
					if err != nil {
						t.Fatal(err)
					}
				}
				defer store.Close()
				lease, err := store.Lease(ctx, "native-completion", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(context.Background())
				target := &nativeCompletionStore{Store: store, mode: mode, cancel: cancel}
				manager := restore.NewManager(target, lease, trustedPresenceVerifier())
				status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
				if err != nil {
					t.Fatal(err)
				}
				uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
				by := model.PrincipalRef{Kind: "user", ID: "completion-owner"}
				meta := model.WriteMeta{Method: "admin.restore.finish", AgentLabel: "completion-agent", IdempotencyKey: "completion-key"}
				status, err = manager.Finish(ctx, status.RestoreID, hash, by, meta, restore.Presence(presence))
				if err != nil || status.State != "complete" || status.Incarnation == "" || status.Incarnation != target.committed.Incarnation || target.observations != 1 || target.commits != 1 {
					t.Fatalf("acknowledged completion=%+v commit=%+v observations=%d commits=%d err=%v", status, target.committed, target.observations, target.commits, err)
				}
				if len(status.Warnings) != 0 {
					t.Fatal("completion invented a new wire warning", status)
				}
				repeated, err := manager.Finish(context.Background(), status.RestoreID, hash, by, meta, restore.Presence(presence))
				if err != nil || !reflect.DeepEqual(repeated, status) || target.commits != 1 || target.observations != 1 {
					t.Fatalf("repeated completion lost immutable status or retried: %+v %v", repeated, err)
				}
				actual, err := store.Meta(context.Background())
				if err != nil || actual.Incarnation != status.Incarnation || actual.Head.String() != "1" || actual.HistoryFloor.String() != "1" {
					t.Fatalf("acknowledgment differed from durable publication: %+v %v", actual, err)
				}
			})
		}
	}
}
