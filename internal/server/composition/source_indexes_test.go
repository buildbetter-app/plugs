package composition

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestStartupUpgradesLegacySourceIndexesBeforeServing(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			config := Config{Driver: kind, Project: "local", Address: "127.0.0.1:0"}
			if kind == "sqlite" {
				config.DSN = filepath.Join(t.TempDir(), "legacy.db")
			}
			store, err := openStore(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "old-publisher", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			ref := model.RefByID("pl-legacy", model.Natural64(0))
			// An actual legacy image has no typed declarations. Do not initialize
			// indexes or call the builder in the fixture; Runtime owns that upgrade.
			if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
				{Ref: model.RefByID("ontology/global", model.Natural64(0)), Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"version":1}`)},
				{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: []byte(` {"id":"pl-legacy","type":"issue","project":"local","fields":{"status":"todo"}} `)},
			}); err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0)
			_ = r.Close()
			if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("fixture did not retain unknown coverage: %v", err)
			}
			if kind == "sqlite" {
				// Runtime must upgrade the physically reopened database, not a
				// process-local representation retained by the fixture.
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil }
			}
			f := startRuntime(t, config)
			after, err := f.r.Store.Meta(ctx)
			if err != nil || before != after {
				t.Fatalf("startup maintenance changed metadata: before=%+v after=%+v err=%v", before, after, err)
			}
			r, err = f.r.Store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			_, _, parentErr := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0)
			_, _, linksErr := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0)
			_ = r.Close()
			if parentErr != nil || linksErr != nil {
				t.Fatalf("runtime admitted requests before coverage: parent=%v links=%v", parentErr, linksErr)
			}
			result, err := f.client.Call(ctx, "bead.close", map[string]any{"id": ref}, "")
			if err != nil {
				t.Fatal(err)
			}
			var commit model.Commit
			if err := json.Unmarshal(result.JSON, &commit); err != nil || commit.Revision.String() != "2" || len(commit.Results) != 1 || commit.Results[0].Ref != ref {
				t.Fatalf("upgraded lifecycle did not commit once: %s err=%v", result.JSON, err)
			}
		})
	}
}
