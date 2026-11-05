package s1b21b24

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func casHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (f *fixture) casReceipt(t *testing.T, method, label string) *driver.IdempotencyRecord {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	value, err := r.IdempotencyByKey(f.ctx, principal, actor, method, key(label))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func requireCASReason(t *testing.T, got reply, reason, current string) {
	t.Helper()
	if got.Error == nil || got.Error.Data.Code != "precondition_failed" || got.Error.Data.Retryable || got.Meta.Replayed || len(got.Result) != 0 {
		t.Fatalf("CAS must be a nonretryable error without result/replay: %+v", got)
	}
	if got.Error.Data.Details["reason"] != reason {
		t.Fatalf("CAS reason=%v want=%s", got.Error.Data.Details, reason)
	}
	token, present := got.Error.Data.Details["currentRevision"]
	if current == "" {
		if present {
			t.Fatalf("hash/absent CAS fabricated a revision token: %v", token)
		}
	} else if number, ok := token.(json.Number); !ok || number.String() != current {
		t.Fatalf("CAS currentRevision=%v want=%s", token, current)
	}
}

func TestS1B22TypedCASRefusalsLeaveNoDurableDelta(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			target, _ := f.create(t, "typed-cas-target", "target")
			guard, _ := f.create(t, "typed-cas-guard", "guard")
			commit(t, f.call("tx.apply", "typed-cas-seed-metadata", map[string]any{"changeSet": map[string]any{"ops": []any{
				map[string]any{"kind": "setMeta", "target": guard, "key": "plugin.compare", "value": map[string]any{"n": 1}},
			}}}))
			memory := commit(t, f.call("memory.save", "typed-cas-memory", map[string]any{"key": "runbook", "content": "stored", "metadata": map[string]any{"source": "test"}})).Results[0].Ref
			missing := model.RefByID("s1-absent-expectation", model.Natural64(0))
			for _, tc := range []struct {
				name, method, reason, current string
				expect                        model.Precondition
			}{
				{"field", "tx.apply", "hash", "", model.PreconditionByHash(guard, "fields.title", casHash(`"stale"`))},
				{"metadata", "tx.apply", "hash", "", model.PreconditionByHash(guard, "metadata.plugin.compare", casHash(`{"n":2}`))},
				{"metadata_absent", "tx.apply", "hash", "", model.PreconditionByHash(guard, "metadata.missing", casHash(`null`))},
				{"record_absent_hash", "tx.apply", "hash", "", model.PreconditionByHash(missing, "fields.title", casHash(`"guard"`))},
				{"record_absent_revision", "tx.apply", "revision", "", model.PreconditionByRevision(missing, model.Natural64(0))},
				{"revision", "tx.apply", "revision", "3", model.PreconditionByRevision(guard, model.Natural64(0))},
				{"memory_content", "memory.save", "hash", "", model.PreconditionByHash(memory, "content", casHash(`"stale"`))},
				{"memory_metadata", "memory.save", "hash", "", model.PreconditionByHash(memory, "metadata.source", casHash(`"other"`))},
				{"memory_record_absent_hash", "memory.save", "hash", "", model.PreconditionByHash(missing, "content", casHash(`"stored"`))},
				{"memory_record_absent_revision", "memory.save", "revision", "", model.PreconditionByRevision(missing, model.Natural64(0))},
				{"memory_revision", "memory.save", "revision", "4", model.PreconditionByRevision(memory, model.Natural64(0))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					label := "typed-cas-" + tc.name
					params := map[string]any{"changeSet": map[string]any{
						"ops": []any{
							map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"title": "must-rollback"}},
							map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"description": "must-rollback-too"}},
						},
						"expect": []model.Precondition{tc.expect},
					}}
					if tc.method == "memory.save" {
						params = map[string]any{"key": "runbook", "content": "must-rollback", "expect": []model.Precondition{tc.expect}}
					}
					before := f.state(t)
					got := f.call(tc.method, label, params)
					sameState(t, before, f.state(t))
					if f.casReceipt(t, tc.method, label) != nil {
						t.Fatal("refused CAS published a receipt")
					}
					requireCASReason(t, got, tc.reason, tc.current)
				})
			}
			t.Logf("B22 typed refusal: engine=%s field/metadata/memory/absent guards preserve records, versions, history and receipts", engine)
		})
	}
}

func TestS1B22CorrectedHashCanReuseRefusedKeyAndReplayAfterReopen(t *testing.T) {
	for _, engine := range transactionEngines() {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			id, _ := f.create(t, "corrected-hash-create", "selected")
			commit(t, f.call("bead.update", "corrected-hash-disjoint", map[string]any{"id": id, "fields": map[string]any{"description": "disjoint"}}))
			params := map[string]any{"id": id, "fields": map[string]any{"description": "accepted"}, "expect": []model.Precondition{model.PreconditionByHash(id, "fields.title", casHash(`"stale"`))}}
			before := f.state(t)
			refused := f.call("bead.update", "corrected-hash", params)
			sameState(t, before, f.state(t))
			if f.casReceipt(t, "bead.update", "corrected-hash") != nil {
				t.Fatal("refused hash reserved the idempotency key")
			}
			requireCASReason(t, refused, "hash", "")
			params["expect"] = []model.Precondition{model.PreconditionByHash(id, "fields.title", casHash(`"selected"`))}
			request := f.request("bead.update", "corrected-hash", params)
			first := f.dispatch(request)
			accepted := commit(t, first)
			if first.Meta.Replayed || accepted.Revision.String() != "3" || len(accepted.Results) != 1 {
				t.Fatalf("corrected expectation did not commit once: %+v", first)
			}
			after := f.state(t)
			if len(after.Changesets) != 3 || len(after.Versions[id.ID]) != 3 {
				t.Fatalf("corrected hash did not publish one row version/changeset: %+v", after)
			}
			f.reopen(t)
			replayed := f.dispatch(request)
			if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(first.Result, replayed.Result) {
				t.Fatalf("corrected hash receipt changed after reopen: %+v", replayed)
			}
			sameState(t, after, f.state(t))
			t.Logf("B22 hash CAS: engine=%s refusal leaves key reusable, disjoint edit preserves selected hash, stored commit replays after reopen", engine)
		})
	}
}
