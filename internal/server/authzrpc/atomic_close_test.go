package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// The synthetic trusted policy deliberately grants the enclosing transaction.
// It isolates the extra close request from the fixed policy's tx.apply refusal.
type atomicCloseRules struct {
	denyClose  atomic.Bool
	txCalls    atomic.Int32
	closeCalls atomic.Int32
}

func (r *atomicCloseRules) Eval(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(requests))
	for i, request := range requests {
		out[i].Effect = authz.Allow
		switch request.Method {
		case "tx.apply":
			r.txCalls.Add(1)
		case "bead.close":
			r.closeCalls.Add(1)
			if r.denyClose.Load() {
				out[i] = authz.Decision{Effect: authz.Deny, Reasons: []string{"test close permission denied"}}
			}
		}
	}
	return out, nil
}
func (*atomicCloseRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

func TestAtomicCloseRequiresItsOwnPermissionAndRechecksReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			var store driver.Store
			if engine == "sqlite" {
				var err error
				store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "close-policy.db")})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				store = memory.New()
			}
			f := newHFixtureStore(t, false, store)
			policy := &atomicCloseRules{}
			policy.denyClose.Store(true)
			f.app.Checker.chain.Rules = []authz.RuleSet{policy}
			principal, err := driver.PrincipalKey(hAgent())
			if err != nil {
				t.Fatal(err)
			}
			const receiptKey = "b2100000000000000000000000000001"
			read := func() (model.Revision, driver.Record, *driver.IdempotencyRecord) {
				t.Helper()
				meta, err := store.Meta(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				r, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(context.Background(), []model.Ref{hRef("child")})
				if err != nil || len(rows) != 1 {
					t.Fatalf("target rows=%+v err=%v", rows, err)
				}
				receipt, err := r.IdempotencyByKey(context.Background(), principal, "", "tx.apply", receiptKey)
				if err != nil {
					t.Fatal(err)
				}
				return *r.Snapshot().At, rows[0], receipt
			}
			before, original, _ := read()
			request := func(revision model.Revision) map[string]any {
				return map[string]any{
					"changeSet": map[string]any{"ops": []any{map[string]any{
						"kind": "close", "target": hRef("child"), "reason": "permitted close",
						"expect": []model.Precondition{model.PreconditionByRevision(hRef("child"), revision)},
					}}},
					"_meta": map[string]any{"project": "child-project", "idempotencyKey": receiptKey},
				}
			}
			stale := request(model.Natural64(0))
			denied := f.callRPC(t, hAgent(), "tx.apply", stale)
			if denied.Error == nil || denied.Error.Data.Code != "forbidden" || len(denied.Error.Data.Details) != 0 || len(denied.Result) != 0 {
				t.Fatalf("transaction permit bypassed close or exposed CAS: %+v", denied)
			}
			if policy.txCalls.Load() == 0 || policy.closeCalls.Load() == 0 {
				t.Fatal("test did not exercise both independent permission requests")
			}
			after, row, receipt := read()
			if after.String() != before.String() || !reflect.DeepEqual(row, original) || receipt != nil {
				t.Fatal("denied close changed target/head/receipt")
			}
			policy.denyClose.Store(false)
			refused := f.callRPC(t, hAgent(), "tx.apply", stale)
			if refused.Error == nil || refused.Error.Data.Code != "precondition_failed" || refused.Error.Data.Details["reason"] != "revision" {
				t.Fatalf("allowed close did not reach its private guard: %+v", refused)
			}
			after, row, receipt = read()
			if after.String() != before.String() || !reflect.DeepEqual(row, original) || receipt != nil {
				t.Fatal("stale close changed target/head/receipt")
			}
			valid := request(original.Revision)
			first := f.callRPC(t, hAgent(), "tx.apply", valid)
			if first.Error != nil {
				t.Fatalf("both permitted close failed: %+v", first.Error)
			}
			var commit model.Commit
			if err := json.Unmarshal(first.Result, &commit); err != nil {
				t.Fatal(err)
			}
			committed, row, stored := read()
			if committed.String() == before.String() || stored == nil || committed.String() != commit.Revision.String() || row.Revision.String() != committed.String() {
				t.Fatal("permitted close was not one durable commit")
			}
			var body struct{ Fields map[string]any }
			if err := json.Unmarshal(row.Body, &body); err != nil || body.Fields["status"] != "done" {
				t.Fatal("permitted close missed close slot", err)
			}
			policy.denyClose.Store(true)
			denied = f.callRPC(t, hAgent(), "tx.apply", valid)
			if denied.Error == nil || denied.Error.Data.Code != "forbidden" || len(denied.Error.Data.Details) != 0 || len(denied.Result) != 0 || denied.Meta.Replayed {
				t.Fatal("stored close replay bypassed current close permit")
			}
			after, replayedRow, replayedReceipt := read()
			if after.String() != committed.String() || !reflect.DeepEqual(row, replayedRow) || !reflect.DeepEqual(stored, replayedReceipt) {
				t.Fatal("refused replay changed committed state or receipt")
			}
			policy.denyClose.Store(false)
			replay := f.callRPC(t, hAgent(), "tx.apply", valid)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("allowed close replay did not return stored response")
			}
			t.Log("B21 authenticated HTTP: tx.apply permit alone cannot close; both permits reach CAS/commit; revoking only close refuses stored replay")
		})
	}
}
