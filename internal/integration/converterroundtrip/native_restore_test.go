package converterroundtrip

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeProfile() driver.NativeRestoreProfile {
	binding, err := nativeAllocatorFixture().Binding()
	if err != nil {
		panic(err)
	}
	return driver.NativeRestoreProfile{
		StoreID: "native-archive-source", Method: "admin.restore.finish", AllocatorBinding: binding,
		Principal:  model.PrincipalRef{Kind: "user", ID: "native-admin", Sponsor: &model.PrincipalRef{Kind: "service", ID: "trusted-host"}, Caveats: map[string]any{"scope": map[string]any{"project": "native"}}},
		AgentLabel: "native-agent", IdempotencyKey: "native-restore-key", RequestHash: strings.Repeat("ab", 32),
	}
}

// Explicit empty input is a labelled driver fixture, never a public producer.
func nativeAllocatorFixture() driver.AllocatorSnapshot {
	return driver.AllocatorSnapshot{Version: driver.NativeAllocatorVersion, StoreID: "native-archive-source", Incarnation: "native-source-incarnation", At: model.Natural64(0), CoverageFloor: model.Natural64(0), Inventory: driver.AllocatorInventory{Projects: []driver.OfflineAllocator{}, Parents: []driver.ParentCounter{}}}
}

func nativeAllocatorFence() driver.LockKey {
	key, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		panic(err)
	}
	return key
}

func nativeAllocatorLocks() driver.LockSet {
	return driver.LockSet{{Key: nativeAllocatorFence(), Mode: driver.LockUpdate}}
}

func nativeHeader(p driver.NativeRestoreProfile, meta driver.Meta) driver.ChangesetHeader {
	return driver.ChangesetHeader{
		Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: p.Principal,
		Method: p.Method, AgentLabel: p.AgentLabel, IdempotencyKey: p.IdempotencyKey, RequestHash: p.RequestHash,
		LockKeys: []driver.LockKey{nativeAllocatorFence()},
		Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}},
	}
}

func TestNativePreparedRestoreBindsDetachedReceiptAcrossDrivers(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			before := snapshot(t, store)
			tx, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			profile := nativeProfile()
			capability := tx.(driver.NativeRestoreTx)
			if _, err := capability.PrepareNativeRestore(ctx, profile); err == nil {
				t.Fatal("native restore prepared without Head")
			}
			if _, err := tx.Lock(ctx, nativeAllocatorLocks()); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			meta, err := capability.PrepareNativeRestore(ctx, profile)
			if err != nil {
				t.Fatal(err)
			}
			if meta.StoreID != profile.StoreID || meta.Incarnation == before.Meta.Incarnation || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" {
				t.Fatalf("native prepared metadata=%+v", meta)
			}
			if !reflect.DeepEqual(before, snapshot(t, store)) {
				t.Fatal("preparation published metadata")
			}
			if _, err := capability.PrepareNativeRestore(ctx, profile); err == nil {
				t.Fatal("repeated native preparation accepted")
			}
			if _, err := tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(ctx, "converter"); err == nil {
				t.Fatal("prepared native profile switched to converter")
			}
			_, err = tx.Commit(ctx, nativeHeader(profile, meta))
			var missingStage *driver.NativeAllocatorSnapshotError
			if !errors.As(err, &missingStage) || missingStage.Reason != driver.NativeAllocatorBindingRequired {
				t.Fatalf("native restore missing-stage refusal: %v", err)
			}
			if err := tx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, nativeAllocatorFixture()); err != nil {
				t.Fatal(err)
			}

			// Mutate both nested caller-owned containers after binding. The
			// original attribution still commits; the mutated attribution fails.
			profile.Principal.Sponsor.ID = "changed-host"
			profile.Principal.Caveats["scope"].(map[string]any)["project"] = "changed"
			if _, err := tx.Commit(ctx, nativeHeader(profile, meta)); err == nil {
				t.Fatal("caller mutated the bound native principal")
			}
			header := nativeHeader(nativeProfile(), meta)
			for _, mutate := range []func(*driver.ChangesetHeader){
				func(h *driver.ChangesetHeader) { h.Method = "offline.restore" },
				func(h *driver.ChangesetHeader) { h.Principal = restorePrincipal },
				func(h *driver.ChangesetHeader) { h.AgentLabel = "other" },
				func(h *driver.ChangesetHeader) { h.IdempotencyKey = "other" },
				func(h *driver.ChangesetHeader) { h.RequestHash = strings.Repeat("cd", 32) },
				func(h *driver.ChangesetHeader) { h.ResponsePending = true },
				func(h *driver.ChangesetHeader) { h.Kind = model.ChangesetWrite },
				func(h *driver.ChangesetHeader) { h.Incarnation = before.Meta.Incarnation },
			} {
				bad := header
				mutate(&bad)
				if _, err := tx.Commit(ctx, bad); err == nil {
					t.Fatal("mismatched native header accepted")
				}
				if !reflect.DeepEqual(before, snapshot(t, store)) {
					t.Fatal("refused native header changed target")
				}
			}
			if _, err := tx.(driver.NoopCommitter).CommitNoop(ctx, header); err == nil {
				t.Fatal("native metadata escaped through no-op")
			}
			entry, err := tx.Commit(ctx, header)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.Meta(ctx)
			if err != nil || !reflect.DeepEqual(got, meta) {
				t.Fatalf("native metadata=%+v want=%+v err=%v", got, meta, err)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: got.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			principal, _ := driver.PrincipalKey(header.Principal)
			receipt, err := reader.IdempotencyByKey(ctx, principal, header.AgentLabel, header.Method, header.IdempotencyKey)
			if err != nil || receipt == nil || receipt.RequestHash != header.RequestHash || receipt.ResponsePending || !reflect.DeepEqual(receipt.Changeset, entry) || !reflect.DeepEqual(receipt.Changeset.Principal, header.Principal) {
				t.Fatalf("native final receipt=%+v err=%v", receipt, err)
			}
			if receipt.Commit.Revision.String() != "1" || entry.Revision.String() != "1" {
				t.Fatal("native receipt did not publish at revision one")
			}
			if wrong, err := reader.IdempotencyByKey(ctx, "system:plug-converter", "", "offline.restore", header.IdempotencyKey); err != nil || wrong != nil {
				t.Fatalf("native receipt appeared in converter scope: %+v %v", wrong, err)
			}
			var truncated driver.ErrTruncated
			if _, err := reader.Changesets(ctx, nil, 1); !errors.As(err, &truncated) {
				t.Fatalf("native below-floor history=%v", err)
			}
			reader.Close()
			var changed driver.ErrIncarnationChanged
			if old, err := store.Read(ctx, driver.View{Incarnation: before.Meta.Incarnation}); !errors.As(err, &changed) {
				if old != nil {
					old.Close()
				}
				t.Fatalf("old native lifetime accepted: %v", err)
			}
			fresh, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			if _, err := fresh.Lock(ctx, nativeAllocatorLocks()); err != nil {
				t.Fatal(err)
			}
			if _, err := fresh.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := fresh.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, nativeProfile()); !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
				t.Fatalf("initialized native augmentation=%v", err)
			}
			if _, err := fresh.(driver.OfflineRestoreTx).PrepareOfflineRestore(ctx, "allocator-source"); !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
				t.Fatalf("initialized allocator augmentation=%v", err)
			}
		})
	}
}

func TestNativePreparationRefusalsRollbackAndReleaseAcrossDrivers(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		for _, scenario := range []string{"invalid-profile", "staged-record", "cancel", "lease-loss", "rollback"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				store, lease, cleanup := openTarget(t, kind)
				defer cleanup()
				before := snapshot(t, store)
				tx, err := store.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Close()
				ref := model.RefByID("native-staged", model.Natural64(0))
				key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
				locks := driver.LockSet{{Key: key, Mode: driver.LockUpdate}, {Key: nativeAllocatorFence(), Mode: driver.LockUpdate}}
				if _, err := tx.Lock(ctx, locks); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Head(ctx); err != nil {
					t.Fatal(err)
				}
				capability := tx.(driver.NativeRestoreTx)
				profile := nativeProfile()
				if scenario == "invalid-profile" {
					for _, mutate := range []func(*driver.NativeRestoreProfile){
						func(p *driver.NativeRestoreProfile) { p.StoreID = " source " },
						func(p *driver.NativeRestoreProfile) { p.Method = "offline.restore" },
						func(p *driver.NativeRestoreProfile) { p.Principal.Kind = "unknown" },
						func(p *driver.NativeRestoreProfile) { p.IdempotencyKey = "" },
						func(p *driver.NativeRestoreProfile) { p.RequestHash = strings.Repeat("zz", 32) },
					} {
						bad := nativeProfile()
						mutate(&bad)
						if _, err := capability.PrepareNativeRestore(ctx, bad); err == nil {
							t.Fatal("invalid native profile accepted")
						}
					}
				}
				if scenario == "staged-record" {
					if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "config", Body: []byte(`{"value":1}`)}); err != nil {
						t.Fatal(err)
					}
					if _, err := capability.PrepareNativeRestore(ctx, profile); err == nil {
						t.Fatal("native preparation after staging accepted")
					}
				} else {
					meta, err := capability.PrepareNativeRestore(ctx, profile)
					if err != nil {
						t.Fatal(err)
					}
					if err := tx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, nativeAllocatorFixture()); err != nil {
						t.Fatal(err)
					}
					header := nativeHeader(profile, meta)
					header.LockKeys = []driver.LockKey{key, nativeAllocatorFence()}
					switch scenario {
					case "cancel":
						canceled, cancel := context.WithCancel(ctx)
						cancel()
						if _, err := tx.Commit(canceled, header); !errors.Is(err, context.Canceled) {
							t.Fatalf("canceled native commit=%v", err)
						}
					case "lease-loss":
						if err := lease.Release(ctx); err != nil {
							t.Fatal(err)
						}
						if _, err := tx.Commit(ctx, header); err == nil {
							t.Fatal("lost-lease native commit accepted")
						}
					}
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, snapshot(t, store)) {
					t.Fatal("refused/rolled-back native restore changed durable target")
				}
				if scenario == "lease-loss" {
					lease, err = store.Lease(ctx, "native-retry", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
					defer lease.Release(ctx)
				}
				retry, err := store.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer retry.Close()
				if _, err := retry.Lock(ctx, locks); err != nil {
					t.Fatal(err)
				}
				if _, err := retry.Head(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := retry.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, profile); err != nil {
					t.Fatalf("native cleanup prevented fresh transaction: %v", err)
				}
			})
		}
	}
}

func TestNativeRestoreFencesReceiptsCheckpointsAndWritersAcrossDrivers(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		for _, scenario := range []string{"receipt-only", "checkpoint-only", "checkpoint-at-publication", "writer-before-head"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				store, lease, cleanup := openTarget(t, kind)
				defer cleanup()
				before := snapshot(t, store)
				checkpoint := func() {
					if _, err := store.(driver.CheckpointStore).SaveCheckpoint(ctx, lease, "native-fence", 0, []byte("preserved")); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "receipt-only" {
					tx, err := store.Write(ctx, lease)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Lock(ctx, nil); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Head(ctx); err != nil {
						t.Fatal(err)
					}
					_, err = tx.(driver.NoopCommitter).CommitNoop(ctx, driver.ChangesetHeader{Incarnation: before.Meta.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, Method: "other.noop", IdempotencyKey: "native-other", RequestHash: "other"})
					tx.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "checkpoint-only" {
					checkpoint()
				}
				tx, err := store.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Close()
				if _, err := tx.Lock(ctx, nativeAllocatorLocks()); err != nil {
					t.Fatal(err)
				}
				if scenario == "writer-before-head" {
					putOther(t, store, lease)
				}
				if _, err := tx.Head(ctx); err != nil {
					t.Fatal(err)
				}
				meta, err := tx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, nativeProfile())
				if scenario == "checkpoint-at-publication" {
					if err != nil {
						t.Fatal(err)
					}
					if err := tx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, nativeAllocatorFixture()); err != nil {
						t.Fatal(err)
					}
					checkpoint()
					_, err = tx.Commit(ctx, nativeHeader(nativeProfile(), meta))
				}
				if !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
					t.Fatalf("native %s fence=%v", scenario, err)
				}
				tx.Close()
				after := snapshot(t, store)
				if scenario == "writer-before-head" {
					if len(after.Records) != 1 || after.Records[0].Ref.ID != "other" || after.Meta.Incarnation != before.Meta.Incarnation || !after.Meta.HistoryFloor.IsZero() {
						t.Fatalf("native restore changed competing writer=%+v", after)
					}
				} else if !reflect.DeepEqual(before, after) {
					t.Fatal("native refusal published metadata/records/history")
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: after.Meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				if receipt, err := reader.IdempotencyByKey(ctx, "user:native-admin", "native-agent", "admin.restore.finish", "native-restore-key"); err != nil || receipt != nil {
					t.Fatalf("native refusal published receipt=%+v %v", receipt, err)
				}
				if scenario == "receipt-only" {
					p, _ := driver.PrincipalKey(restorePrincipal)
					if receipt, err := reader.IdempotencyByKey(ctx, p, "", "other.noop", "native-other"); err != nil || receipt == nil || receipt.RequestHash != "other" {
						t.Fatalf("native refusal changed competing receipt=%+v %v", receipt, err)
					}
				}
				if strings.HasPrefix(scenario, "checkpoint") {
					got, err := store.(driver.CheckpointStore).LoadCheckpoint(ctx, "native-fence")
					if err != nil || got.Generation != 1 || string(got.Payload) != "preserved" {
						t.Fatalf("native refusal changed checkpoint=%+v %v", got, err)
					}
				}
			})
		}
	}
}

func TestConcurrentSameLeaseNativeRestoresHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			txs := make([]driver.WriteTx, 2)
			for i := range txs {
				var err error
				txs[i], err = store.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer txs[i].Close()
			}
			start := make(chan struct{})
			results := make(chan error, 2)
			var wg sync.WaitGroup
			for _, tx := range txs {
				wg.Add(1)
				go func(tx driver.WriteTx) {
					defer wg.Done()
					defer tx.Close()
					<-start
					if _, err := tx.Lock(ctx, nativeAllocatorLocks()); err != nil {
						results <- err
						return
					}
					if _, err := tx.Head(ctx); err != nil {
						results <- err
						return
					}
					meta, err := tx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, nativeProfile())
					if err != nil {
						results <- err
						return
					}
					if err := tx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, nativeAllocatorFixture()); err != nil {
						results <- err
						return
					}
					_, err = tx.Commit(ctx, nativeHeader(nativeProfile(), meta))
					results <- err
				}(tx)
			}
			close(start)
			wg.Wait()
			close(results)
			wins := 0
			for err := range results {
				if err == nil {
					wins++
				}
			}
			if wins != 1 {
				t.Fatalf("native same-lease restore winners=%d", wins)
			}
			meta, err := store.Meta(ctx)
			if err != nil || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" || meta.StoreID != "native-archive-source" {
				t.Fatalf("native race metadata=%+v %v", meta, err)
			}
		})
	}
}
