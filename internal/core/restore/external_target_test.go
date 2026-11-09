package restore_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func TestPlugDumpRestorePreservesExternalIdentityAndLocalIndexIsolation(t *testing.T) {
	ctx := context.Background()
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "external-restore", Agent: "source", Storage: cli.StorageConfig{Driver: "memory"}, PresenceVerifier: trustedPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	created, err := source.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "dependent"}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "local collision"}}},
	}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if err := json.Unmarshal(created.JSON, &commit); err != nil || len(commit.Results) != 2 {
		t.Fatalf("create: %s %v", created.JSON, err)
	}
	from, collision := commit.Results[0].Ref, commit.Results[1].Ref
	opaque := model.RefByID("external:tracker/1", model.Natural64(0))
	for _, target := range []model.Ref{collision, opaque} {
		if _, err := source.Client.Call(ctx, "bead.link", map[string]any{"from": from, "to": target, "kind": "blocks", "targetKind": "external"}, ""); err != nil {
			t.Fatal(err)
		}
	}
	data, archive := captureDump(t, ctx, source.Client)
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "external.db")
			var store driver.Store
			if engine == "memory" {
				store = memory.New()
			} else {
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
			}
			target, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "external-restore", Agent: "target", Storage: cli.StorageConfig{Driver: "provided"}, PresenceVerifier: trustedPresenceVerifier(), Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
				return store, store.Close, nil
			}})
			if err != nil {
				store.Close()
				t.Fatal(err)
			}
			restoreArchive(t, ctx, target.Client, data, archive)
			check := func() {
				t.Helper()
				meta, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(ctx, []model.Ref{from})
				if err != nil || len(rows) != 1 {
					t.Fatalf("restored source: %v", err)
				}
				doc, err := pipeline.DecodeDocument(rows[0].Body)
				if err != nil || len(doc.Links) != 2 {
					t.Fatalf("restored links: %+v %v", doc.Links, err)
				}
				want := map[string]bool{collision.ID: true, opaque.ID: true}
				for _, link := range doc.Links {
					if link.TargetKind != model.LinkTargetExternal || !want[link.To.ID] {
						t.Fatalf("typed identity changed: %+v", link)
					}
					delete(want, link.To.ID)
				}
				if len(want) != 0 {
					t.Fatal("external identity omitted")
				}
				incoming, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, []model.Ref{collision}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10)
				if err != nil || more || len(incoming) != 0 {
					t.Fatalf("external restore entered local index: %v more=%v err=%v", incoming, more, err)
				}
			}
			check()
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
			if engine == "sqlite" {
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				check()
			}
		})
	}
}
