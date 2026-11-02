package batchsql_test

import (
	"context"
	"errors"
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

func TestNativeDumpRefusesRetainedAllocatorStateAcrossStores(t *testing.T) {
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
							if !errors.Is(err, driver.ErrUnsupported) || len(data) != 0 || manifest.Format != "" {
								t.Fatalf("native snapshot silently omitted %s continuation state: bytes=%d format=%q err=%v", kind, len(data), manifest.Format, err)
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
