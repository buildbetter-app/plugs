package ontologyrefusals_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

func TestProtectedStatusPatches(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		ref := f.create(t, map[string]any{"title": "status protection"})
		for _, tc := range []struct {
			name  string
			value any
		}{
			{"same_initial", "todo"},
			{"claim_slot", "in_progress"},
			{"close_slot", "done"},
			{"fail_slot", "failed"},
			{"cancel_slot", "canceled"},
			{"defer_slot", "backlog"},
			{"unknown_status", "not-a-status"},
			{"remove_status", nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// A valid sibling field must not survive a protected status patch.
				fields := map[string]any{"title": "must not land", "status": tc.value}
				f.refuse(t, "bead.update", "ontology_use_slot_verb", f.update(ref, fields))
				f.refuse(t, "tx.apply", "ontology_use_slot_verb", f.apply(patch(ref, fields)))
			})
		}
		f.commit(t, "bead.claim", f.claim(ref))
		for _, status := range []string{"in_progress", "todo"} {
			t.Run("claimed_to_"+status, func(t *testing.T) {
				f.refuse(t, "bead.update", "ontology_use_slot_verb", f.update(ref, map[string]any{"status": status}))
			})
		}
		f.commit(t, "bead.close", f.settle(ref, "finished"))
		for _, status := range []string{"done", "todo"} {
			t.Run("closed_to_"+status, func(t *testing.T) {
				f.refuse(t, "tx.apply", "ontology_use_slot_verb", f.apply(patch(ref, map[string]any{"status": status})))
			})
		}
	})
}

func TestMalformedTypedFields(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		ref := f.create(t, map[string]any{"title": "valid", "description": "retained"})
		for _, tc := range []struct {
			name, field string
			value       any
		}{
			{"title_number", "title", json.Number("7")},
			{"title_boolean", "title", true},
			{"title_object", "title", map[string]any{"text": "bad"}},
			{"description_array", "description", []any{"bad"}},
			{"description_boolean", "description", false},
			{"priority_string", "priority", "1"},
			{"priority_boolean", "priority", true},
			{"priority_fraction", "priority", json.Number("1.5")},
			{"priority_negative", "priority", -1},
			{"priority_above_p4", "priority", 5},
			{"template_string", "template", "false"},
			{"template_number", "template", 0},
			{"template_array", "template", []any{false}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fields := map[string]any{tc.field: tc.value}
				f.refuse(t, "bead.create", "invalid_params", func(meta model.WriteMeta) (model.Commit, error) {
					return f.svc.Create(f.ctx, owner, service.CreateRequest{Type: "issue", Fields: fields}, meta)
				})
				f.refuse(t, "bead.update", "invalid_params", f.update(ref, fields))
				f.refuse(t, "tx.apply", "invalid_params", f.apply(patch(ref, fields)))
			})
		}
		// Null is an ordinary field removal, not malformed typed data.
		f.commit(t, "bead.update", f.update(ref, map[string]any{"description": nil}))
		if _, exists := f.get(t, ref).Bead.Fields["description"]; exists {
			t.Fatal("supported null removal did not remove optional description")
		}
	})
}

func TestIdentityAndCoreFieldsRemainProtected(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		ref := f.create(t, map[string]any{"title": "immutable envelope"})
		for _, field := range []string{"id", "type", "project", "createdAt", "lease", "comments", "metadata", "unrecognized"} {
			t.Run("unqualified_"+field, func(t *testing.T) {
				// These are unknown typed field keys, NOT top-level mutation APIs.
				// Refusal plus exact state comparison proves they cannot shadow the
				// stored identity or core-owned envelope through ordinary updates.
				fields := map[string]any{field: "replacement", "title": "must not land"}
				f.refuse(t, "bead.update", "ontology_unknown_field", f.update(ref, fields))
				f.refuse(t, "tx.apply", "ontology_unknown_field", f.apply(patch(ref, fields)))
			})
		}
		for _, field := range []string{"fields.title", "fields.status", "id.value", "/id", "title/part", "title\x00part"} {
			t.Run("qualified_"+field, func(t *testing.T) {
				f.refuse(t, "bead.update", "invalid_params", f.update(ref, map[string]any{field: "replacement"}))
			})
		}
		for _, key := range []string{"_plug.id", "core.status"} {
			t.Run("reserved_metadata_"+key, func(t *testing.T) {
				f.refuse(t, "tx.apply", "protected_record", f.apply(model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: key, Value: json.RawMessage(`"replacement"`)}}))
			})
		}
		t.Run("unsupported_project_move", func(t *testing.T) {
			f.refuse(t, "tx.apply", "invalid_params", f.apply(model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref, Project: "other"}}))
		})
		t.Run("malformed_named_parent", func(t *testing.T) {
			parent := model.RefByKey("unresolved-parent")
			f.refuse(t, "bead.create", "invalid_params", func(meta model.WriteMeta) (model.Commit, error) {
				return f.svc.Create(f.ctx, owner, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "child"}, Parent: &parent}, meta)
			})
		})
		f.get(t, ref) // also checks ID, type and project after every attempted change
	})
}

func TestNamedSlotEntryAndExitRefusals(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		f.create(t, map[string]any{"title": "receipt and history control"})
		for _, status := range []string{"in_progress", "done", "failed", "canceled", "backlog"} {
			t.Run("create_in_"+status, func(t *testing.T) {
				f.refuse(t, "bead.create", "ontology_use_slot_verb", func(meta model.WriteMeta) (model.Commit, error) {
					return f.svc.Create(f.ctx, owner, service.CreateRequest{Type: "issue", Fields: map[string]any{"status": status}}, meta)
				})
			})
		}
		for _, tc := range []struct{ method, status string }{
			{"bead.close", "done"}, {"bead.fail", "failed"},
			{"bead.cancel", "canceled"}, {"bead.defer", "backlog"},
		} {
			t.Run("claim_from_"+tc.status, func(t *testing.T) {
				ref := f.create(t, map[string]any{"title": tc.status})
				// Trusted Writer lowering is tested explicitly; fail/cancel/defer
				// are not being claimed as public service or RPC methods.
				f.commit(t, tc.method, f.apply(patch(ref, map[string]any{})))
				if f.get(t, ref).Bead.Status != tc.status {
					t.Fatal("named Writer slot did not select its canonical status")
				}
				f.refuse(t, "bead.claim", "invalid_params", f.claim(ref))
			})
		}
		t.Run("reopen_already_initial", func(t *testing.T) {
			ref := f.create(t, map[string]any{"title": "already initial"})
			f.refuse(t, "bead.reopen", "invalid_params", f.apply(patch(ref, map[string]any{})))
		})
		t.Run("template_guards", func(t *testing.T) {
			ref := f.create(t, map[string]any{"title": "template", "template": true})
			f.refuse(t, "bead.claim", "ontology_guard_failed", f.claim(ref))
			f.refuse(t, "bead.close", "ontology_guard_failed", f.settle(ref, "must not append"))
		})
	})
}

func TestNamedSlotContextCannotAdmitAnUnrelatedBatch(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		a := f.create(t, map[string]any{"title": "first"})
		b := f.create(t, map[string]any{"title": "second"})
		comment := func(ref model.Ref) model.Op {
			return model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: "must not append"}}
		}
		for _, tc := range []struct {
			name string
			ops  []model.Op
		}{
			{"create_first", []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "must not exist"}}}}},
			{"comment_first", []model.Op{comment(a)}},
			{"two_patches", []model.Op{patch(a, map[string]any{}), patch(b, map[string]any{"title": "must not land"})}},
			{"foreign_reason", []model.Op{patch(a, map[string]any{}), comment(b)}},
			{"third_operation", []model.Op{patch(a, map[string]any{}), comment(a), comment(a)}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f.refuse(t, "bead.close", "invalid_params", f.apply(tc.ops...))
			})
		}
	})
}

func TestOpenBlockerGuardsUseRealStoredGraph(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		blocked := f.create(t, map[string]any{"title": "dependent"})
		blocker := f.create(t, map[string]any{"title": "blocker"})
		f.commit(t, "tx.apply", f.apply(model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: blocked, To: blocker, LinkKind: "blocks"}}))
		f.refuse(t, "bead.claim", "ontology_guard_failed", f.claim(blocked))
		f.refuse(t, "bead.close", "ontology_guard_failed", f.settle(blocked, "must not append"))
		f.commit(t, "bead.close", f.settle(blocker, "unblocks dependent"))
		f.commit(t, "bead.claim", f.claim(blocked))
		f.commit(t, "bead.close", f.settle(blocked, "now allowed"))
		if value := f.get(t, blocked); value.Bead.Status != "done" || value.Lease != nil {
			t.Fatalf("resolved blocker did not permit normal lifecycle: %+v", value)
		}
	})
}

func TestRefusedMixedBatchPublishesNoPrefix(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		a := f.create(t, map[string]any{"title": "retained first"})
		b := f.create(t, map[string]any{"title": "retained second"})
		for _, tc := range []struct {
			name, code string
			fields     map[string]any
		}{
			{"protected_status", "ontology_use_slot_verb", map[string]any{"status": "done"}},
			{"malformed_field", "invalid_params", map[string]any{"title": true}},
			{"identity_shadow", "ontology_unknown_field", map[string]any{"id": "replacement"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f.refuse(t, "tx.apply", model.ErrorCode(tc.code), f.apply(
					patch(a, map[string]any{"title": "valid prefix must roll back"}),
					model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: "new", Type: "issue", Fields: map[string]any{"title": "new prefix must roll back"}}},
					model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: a, Body: "comment prefix must roll back"}},
					patch(b, tc.fields),
				))
			})
		}
		// All prefix operations are supported when the final operation is valid.
		c := f.commit(t, "tx.apply", f.apply(
			patch(a, map[string]any{"title": "valid batch first"}),
			model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: "new", Type: "issue", Fields: map[string]any{"title": "valid batch create"}}},
			model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: a, Body: "valid batch comment"}},
			patch(b, map[string]any{"title": "valid batch second"}),
		))
		if len(c.Results) != 4 {
			t.Fatalf("valid batch did not execute every prefix operation: %+v", c)
		}
		for _, result := range c.Results {
			if result.Revision != c.Revision {
				t.Fatalf("valid atomic batch did not share one revision: %+v", c)
			}
		}
	})
}

func TestSupportedLifecycleHistoryAndNoops(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		ref := f.create(t, map[string]any{"title": "ordinary lifecycle", "description": "original"})
		value := f.get(t, ref)
		if value.Bead.Status != "todo" || value.Bead.Fields["priority"] != json.Number("2") || value.Bead.Fields["template"] != false {
			t.Fatalf("canonical create defaults: %+v", value)
		}
		created := value.Bead.Revision
		f.commit(t, "bead.update", f.update(ref, map[string]any{"title": "updated", "priority": 0}))
		claimed := f.commit(t, "bead.claim", f.claim(ref))
		value = f.get(t, ref)
		if value.Bead.Status != "in_progress" || value.Lease == nil || !reflect.DeepEqual(value.Lease.Principal, owner) || value.Lease.TTL.Milliseconds() != 300000 || !value.Lease.ExpiresAt.Equal(f.now().Add(value.Lease.TTL)) {
			t.Fatalf("claim did not persist canonical status and lease: %+v", value)
		}
		f.noop(t, "bead.claim", f.claim(ref), claimed.Revision)
		f.commit(t, "bead.comment", func(meta model.WriteMeta) (model.Commit, error) {
			return f.svc.Comment(f.ctx, owner, service.CommentRequest{ID: ref, Body: "progress"}, meta)
		})
		closed := f.commit(t, "bead.close", f.settle(ref, "finished"))
		if len(closed.Results) != 2 {
			t.Fatalf("close reason was not included in atomic response: %+v", closed)
		}
		f.noop(t, "bead.close", f.settle(ref, ""), closed.Revision)
		value = f.get(t, ref)
		if value.Bead.Status != "done" || value.Lease != nil || value.Bead.Fields["title"] != "updated" {
			t.Fatalf("close did not preserve fields and clear lease: %+v", value)
		}
		state := f.snapshot(t)
		versions := state.Versions[ref.ID]
		if len(versions) != 5 || len(state.Log) != 5 || len(state.Ready.Items) != 0 {
			t.Fatalf("lifecycle versions=%d log=%d ready=%d", len(versions), len(state.Log), len(state.Ready.Items))
		}
		for i, changeset := range state.Log {
			if changeset.Revision != model.Natural64(uint64(i+1)) || !reflect.DeepEqual(changeset.Principal, owner) || changeset.IdempotencyKey == "" {
				t.Fatalf("incorrect lifecycle revision or attribution: %+v", changeset)
			}
		}
		doc, err := pipeline.DecodeDocument(state.Records[0].Body)
		if err != nil || len(doc.Comments) != 2 || doc.Comments[0].Body != "progress" || doc.Comments[1].Body != "finished" {
			t.Fatalf("comment/close history missing: %+v %v", doc.Comments, err)
		}
		old, err := f.svc.Get(f.ctx, owner, service.GetRequest{ID: ref, AsOf: &service.AsOf{Revision: created, Incarnation: state.Meta.Incarnation}})
		if err != nil || old.Bead.Status != "todo" || old.Bead.Fields["title"] != "ordinary lifecycle" || old.Lease != nil {
			t.Fatalf("historical create image: %+v %v", old, err)
		}
		// Reopen is the existing trusted Writer slot seam, not a public RPC claim.
		f.commit(t, "bead.reopen", f.apply(patch(ref, map[string]any{})))
		if value := f.get(t, ref); value.Bead.Status != "todo" || value.Lease != nil {
			t.Fatalf("Writer reopen did not return to initial: %+v", value)
		}
		if f.backend == "sqlite" {
			before := f.snapshot(t)
			f.close(t)
			f.open(t)
			requireEqual(t, "SQLite close/reopen durable state", before, f.snapshot(t))
			f.refuse(t, "bead.update", "ontology_use_slot_verb", f.update(ref, map[string]any{"status": "todo"}))
			t.Log("real file-backed SQLite reopen preserved records, history, receipts and status protection")
		}
	})
}

func (f *fixture) noop(t *testing.T, method string, call writeCall, recordRevision model.Revision) {
	t.Helper()
	meta := f.request(method)
	before := f.snapshot(t)
	c, err := call(meta)
	if err != nil || c.Revision != before.Meta.Head || len(c.Results) != 1 || c.Results[0].Revision != recordRevision || len(c.Warnings) != 0 {
		t.Fatalf("supported %s no-op: %+v %v", method, c, err)
	}
	after := f.snapshot(t)
	scope := meta.Method + "/" + meta.IdempotencyKey
	if after.ByKey[scope] != nil {
		t.Fatal("no-op created a changeset")
	}
	// Only this final receipt may be added by an admitted no-op.
	after.Receipts[scope] = nil
	requireEqual(t, "supported no-op graph/history and prior receipts", before, after)
	f.requireReceipt(t, meta, c)
}
