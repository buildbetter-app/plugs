package compaction_test

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB12ActualCompositionSuppliedSummaryAndPresence(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, mode := range []string{"absent-config", "summary-without-verifier", "supplied-provider-and-verifier"} {
				t.Run(mode, func(t *testing.T) {
					f := newFixture(t, engine)
					// Seed via real ordinary service/Writer with a controlled historical
					// header clock. Production composition then uses its actual clock.
					f.clock = time.Now().UTC().Add(-31 * 24 * time.Hour)
					original := strings.Repeat("Composition original source. ", 200)
					ref := f.create("composition-create", original)
					closed := f.call("bead.close", "composition-close", map[string]any{"id": ref}, "")
					if err := f.lease.Release(context.Background()); err != nil {
						t.Fatal(err)
					}
					f.lease = nil
					authority := &presenceAuthority{}
					var calls atomic.Int32
					cfg := composition.Config{Driver: engine, DSN: f.dsn, Project: "b12", AuthMode: "anonymous", CursorKey: []byte("b12-replay-fixed-cursor-signing-key"), StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return f.store, nil }}
					if mode != "absent-config" {
						cfg.Compaction = &service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(context.Context, service.CompactionInput) (string, error) {
							calls.Add(1)
							return "supplied composition summary", nil
						})}
						if mode == "supplied-provider-and-verifier" {
							cfg.Compaction.VerifyPresence = authority.verify
						}
					}
					rt, err := composition.Open(context.Background(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := rt.Close(context.Background()); err != nil {
							t.Error(err)
						}
					})
					f.server, f.domain = rt.RPC, rt.Service
					before := f.meta()
					if mode == "absent-config" {
						f.call("admin.compact", "composition-missing", map[string]any{}, "compaction_unavailable")
						f.assertSameHead(before)
						f.assertNoReceipt("admin.compact", "composition-missing")
						params := map[string]any{"ref": ref, "baseRevision": closed["revision"], "expect": []any{}, "humanPresence": map[string]any{"mode": "presence_check", "challenge": "unissued", "proof": "unissued"}}
						f.call("history.revert", "composition-missing-verifier", params, "human_presence_required")
						f.assertSameHead(before)
						f.assertNoReceipt("history.revert", "composition-missing-verifier")
						return
					}
					compact := f.call("admin.compact", "composition-compact", map[string]any{}, "")
					if calls.Load() != 1 {
						t.Fatal("composition did not forward supplied provider")
					}
					params := map[string]any{"ref": ref, "baseRevision": compact["revision"], "expect": []any{}, "humanPresence": map[string]any{"mode": "presence_check", "challenge": "unissued", "proof": "unissued"}}
					before = f.meta()
					f.call("history.revert", "composition-forged", params, "human_presence_required")
					f.assertSameHead(before)
					f.assertNoReceipt("history.revert", "composition-forged")
					if mode == "summary-without-verifier" {
						return
					}
					params["humanPresence"] = authority.issue(f, params, "composition-revert")
					reverted := f.call("history.revert", "composition-revert", params, "")
					assertKind(t, f, reverted["revision"], model.ChangesetRevert)
					assertHistoricalText(t, f, ref, closed["revision"], original)
					before = f.meta()
					if engine != "memory" {
						if err = rt.Close(context.Background()); err != nil {
							t.Fatal(err)
						}
						cfg.StoreFactory = nil
						rt, err = composition.Open(context.Background(), cfg)
						if err != nil {
							t.Fatal(err)
						}
						f.store, f.server, f.domain = rt.Store, rt.RPC, rt.Service
					}
					replay := f.call("history.revert", "composition-revert", params, "")
					if !reflect.DeepEqual(reverted, replay) {
						t.Fatal("composition reopen replay changed response")
					}
					assertReplayed(t, f)
					f.assertSameHead(before)
					if calls.Load() != 1 {
						t.Fatal("reversal replay regenerated a summary")
					}
				})
			}
		})
	}
}
