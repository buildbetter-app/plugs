package restore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Historical pre-B26 derived image: f170bf742699fb0bbc577c57b19e7907872e2ea99a34a94fda5e0659197f20ed
// This uses the admitted supported fixture and its fixed independent digests.
// An ordinary field patch must preserve archive metadata without setMeta.
func TestB17SupportedArchiveNullThroughWriterAndServiceQueries(t *testing.T) {
	const id = "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const description = "B17 ordinary field update preserves archive metadata"
	verification := converterroundtrip.Verification{
		ArchiveSHA256: "2f06663c3d3778f689e971398028f88cd8d6c9e85d987108abbdb92363d20b18",
		DerivedSHA256: "6f1b59b29636cf665d447b1a4d49c1b5670af05f9d0170cf2c548ff1e51d62ab",
	}
	data := b17SupportedOfflineArchive(t)
	if sha(data) != verification.ArchiveSHA256 {
		t.Fatal("supported archive differs from its independent digest")
	}
	for _, name := range []string{"memory", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "b17-connected-query.db")
			var store driver.Store
			var err error
			if name == "memory" {
				store = memory.New()
			} else {
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if store != nil {
					_ = store.Close()
				}
			}()
			lease, err := store.Lease(ctx, "b17-connected-query", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if lease != nil {
					_ = lease.Release(ctx)
				}
			}()
			restored, err := converterroundtrip.RestoreWithLease(ctx, bytes.NewReader(data), store, lease, verification)
			if err != nil {
				t.Fatalf("supported public archive restore: %v", err)
			}
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			svc := service.New(store, lease, writer, service.WithOntology(ontology))
			principal := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			ref := model.RefByID(id, model.Natural64(0))
			assertQuery := func(revision, wantDescription string) {
				t.Helper()
				got, err := svc.Get(ctx, principal, service.GetRequest{ID: ref})
				if err != nil {
					t.Fatalf("service query: %v", err)
				}
				if got.Bead.ID != id || got.Bead.Project != "pl" || got.Bead.Revision.String() != revision {
					t.Fatal("service query changed restored identity or revision")
				}
				if wantDescription != "" && got.Bead.Fields["description"] != wantDescription {
					t.Fatal("service query lost the ordinary Writer field update")
				}
				raw, err := json.Marshal(got.Bead.Metadata)
				if err != nil {
					t.Fatal(err)
				}
				requireB17Null(t, raw)
				assertB17RestoredNull(t, ctx, store, id)
			}
			assertQuery("1", "")
			committed, err := writer.Apply(ctx, principal, model.ChangeSet{Ops: []model.Op{{
				Kind: model.OpPatch,
				Patch: &model.PatchOp{Target: ref, Fields: map[string]any{
					"description": description,
				}},
			}}}, model.WriteMeta{
				Method: "tx.apply", Project: "pl", Agent: "b17-acceptance", AgentLabel: "b17-acceptance",
				IdempotencyKey: strings.Repeat("e7", 16),
			})
			if err != nil {
				t.Fatalf("ordinary Writer patch: %v", err)
			}
			if committed.Revision.String() != "2" || len(committed.Results) != 1 || committed.Results[0].Ref.ID != id {
				t.Fatal("ordinary Writer patch did not commit the expected bead at revision two")
			}
			assertQuery("2", description)
			if name == "sqlite" {
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				lease = nil
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store = nil
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				lease, err = store.Lease(ctx, "b17-connected-reopen", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				writer = pipeline.New(store, lease, pipeline.WithOntology(ontology))
				svc = service.New(store, lease, writer, service.WithOntology(ontology))
				assertQuery("2", description)
			}
			meta, err := store.Meta(ctx)
			if err != nil || meta.Incarnation != restored.Restored.Incarnation || meta.StoreID != restored.Restored.StoreID ||
				meta.Head.String() != "2" || meta.HistoryFloor.String() != "1" {
				t.Fatal("Writer/query/reopen changed the restore identity, head or history floor")
			}
			recovered, err := converterroundtrip.Recover(ctx, store, verification)
			if err != nil || recovered.Restored.Incarnation != meta.Incarnation ||
				recovered.IdempotencyKey != restored.IdempotencyKey || recovered.RequestHash != restored.RequestHash {
				t.Fatal("archive marker or restore receipt did not survive the ordinary write and reopen")
			}
			t.Log("supported archive null key, literal null and sibling preserved through service query, real Writer patch and driver lifetime")
		})
	}
}
