package batchsql_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type allocatorInventoryHiddenStore struct{ driver.Store }

func (s allocatorInventoryHiddenStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	// An unknown adapter must not turn missing optional inventory into an
	// assertion that the underlying store has no continuation state.
	return struct{ driver.Reader }{r}, nil
}

func TestNativeDumpPreservesRetainedAllocatorStateAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, kind := range []string{"deleted-child", "sequential", "hash", "random", "missing-inventory"} {
				t.Run(kind, func(t *testing.T) {
					f := newFixture(t, engine)
					var store driver.Store = f.store
					switch kind {
					case "deleted-child":
						parent := hierarchyApply(t, f, hierarchyCreate("root", nil, "")).Results[0].Ref
						child := hierarchyApply(t, f, hierarchyCreate("child", &parent, "")).Results[0].Ref
						hierarchyApply(t, f, childDelete(child))
						if len(childAllocatorDurableState(t, f).Records) != 1 {
							t.Fatal("fixture still has a live child")
						}
					case "sequential", "hash":
						profile := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential}
						if kind == "hash" {
							profile = driver.NewHashAllocator("demo", 3, 8, 0.25)
						}
						seedChildAllocatorFixture(t, f, profile, false)
					case "random":
						seedChildAllocatorFixture(t, f, driver.NewRandomAllocator("demo"), false)
					case "missing-inventory":
						store = allocatorInventoryHiddenStore{f.store}
					}
					before := childAllocatorDurableState(t, f)
					for _, history := range []string{"head", "full"} {
						t.Run(history, func(t *testing.T) {
							data, manifest, err := restore.Dump(f.ctx, store, history)
							if kind == "missing-inventory" {
								if !errors.Is(err, driver.ErrUnsupported) || len(data) != 0 || manifest.Format != "" {
									t.Fatalf("missing capability admitted: bytes=%d manifest=%+v err=%v", len(data), manifest, err)
								}
							} else if history == "full" && !before.Meta.HistoryFloor.IsZero() {
								var truncated driver.ErrTruncated
								if !errors.As(err, &truncated) || truncated.Floor.String() != before.Meta.HistoryFloor.String() || len(data) != 0 || manifest.Format != "" {
									t.Fatalf("native snapshot invented discarded full history: bytes=%d manifest=%+v err=%v", len(data), manifest, err)
								}
							} else {
								if err != nil || len(data) == 0 {
									t.Fatalf("complete native snapshot refused: %s %s %v", kind, history, err)
								}
								tr := tar.NewReader(bytes.NewReader(data))
								var payload []byte
								for {
									h, e := tr.Next()
									if e == io.EOF {
										break
									}
									if e != nil {
										t.Fatal(e)
									}
									if h.Name == "allocator/snapshot.json" {
										payload, e = io.ReadAll(tr)
										if e != nil {
											t.Fatal(e)
										}
									}
								}
								got, e := driver.DecodeAllocatorSnapshot(payload)
								if e != nil {
									t.Fatalf("native allocator payload: %v", e)
								}
								want := observedAllocatorSnapshot(t, f, nil)
								requireAllocatorSnapshotBytes(t, want, got)
								if got.At.String() != manifest.NativeHead || got.Incarnation != manifest.NativeIncarnation || string(got.StoreID) != manifest.NativeStoreID {
									t.Fatal("allocator source is not bound to native manifest")
								}
							}
							requireSameState(t, before, childAllocatorDurableState(t, f))
						})
					}
				})
			}
		})
	}
}

func TestNativeDumpWithoutAllocatorStateRemainsSupportedAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			for _, root := range []bool{false, true} {
				if root {
					hierarchyApply(t, f, hierarchyCreate("ordinary-root", nil, ""))
				}
				before := childAllocatorDurableState(t, f)
				data, manifest, err := restore.Dump(f.ctx, f.store, "head")
				if err != nil || len(data) == 0 || manifest.RecordCount != len(before.Records) {
					t.Fatalf("no-allocator native snapshot: bytes=%d manifest=%+v err=%v", len(data), manifest, err)
				}
				requireSameState(t, before, childAllocatorDurableState(t, f))
			}
		})
	}
}
