package batchsql_test

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// TestDirectWriterAtomicBatchesAcrossMemoryAndSQL is intentionally a direct
// Writer qualification. The current catalog does not bind tx.apply to RPC;
// this test therefore makes no transport claim while proving the protected
// core write boundary against memory, SQLite and PostgreSQL.
func TestDirectWriterAtomicBatchesAcrossMemoryAndSQL(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)

			// Seed a bead in another project. IDs are global, so a later write from
			// batch-local may reference it while the two records keep their projects.
			seedMeta := f.meta("seed-foreign")
			seed, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{createOp("foreign", "foreign seed", "batch-foreign")}}, seedMeta)
			if err != nil || seed.Revision.String() != "1" || len(seed.Results) != 1 {
				t.Fatalf("foreign seed commit=%+v err=%v", seed, err)
			}
			foreign := seed.Results[0].Ref

			// This is the positive atomic batch: create, patch and comment all use
			// the backward alias; the final comment references a different project.
			positiveMeta := f.meta("positive")
			positive := model.ChangeSet{Ops: []model.Op{
				createOp("local", "created", "batch-local"),
				patchOp(model.RefByKey("local"), map[string]any{"description": "patched in batch"}),
				commentOp(model.RefByKey("local"), "local comment"),
				commentOp(foreign, "cross-project comment"),
			}}
			outcome, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, positive, positiveMeta)
			if err != nil || outcome.Replayed || outcome.Commit.Revision.String() != "2" || len(outcome.Commit.Results) != 4 {
				t.Fatalf("direct Writer positive batch=%+v err=%v", outcome, err)
			}
			commit := outcome.Commit
			local := commit.Results[0].Ref
			if !local.IsID() || commit.Results[1].Ref.ID != local.ID || commit.Results[2].Ref.ID != local.ID || commit.Results[3].Ref.ID != foreign.ID {
				t.Fatalf("backward or cross-project references resolved incorrectly: %+v", commit.Results)
			}
			for i, result := range commit.Results {
				if result.Revision.String() != "2" {
					t.Fatalf("result %d has revision %s, want one atomic revision", i, result.Revision)
				}
			}

			state := f.state(t)
			if state.Meta.Head.String() != "2" || len(state.Records) != 2 || len(state.Log) != 2 {
				t.Fatalf("positive durable shape: meta=%+v records=%d log=%d", state.Meta, len(state.Records), len(state.Log))
			}
			localRecord, ok := state.Records[local.ID]
			if !ok {
				t.Fatalf("local record missing from durable state: ids=%v", sortedRecordIDs(state.Records))
			}
			localDoc := decodeDocument(t, localRecord)
			if localDoc.Project != "batch-local" || localDoc.Fields["title"] != "created" || localDoc.Fields["description"] != "patched in batch" || len(localDoc.Comments) != 1 || localDoc.Comments[0].Body != "local comment" {
				t.Fatalf("local final image=%+v", localDoc)
			}
			foreignRecord := state.Records[foreign.ID]
			foreignDoc := decodeDocument(t, foreignRecord)
			if foreignDoc.Project != "batch-foreign" || len(foreignDoc.Comments) != 1 || foreignDoc.Comments[0].Body != "cross-project comment" {
				t.Fatalf("foreign final image=%+v", foreignDoc)
			}
			if len(state.Versions[local.ID]) != 1 || len(state.Versions[foreign.ID]) != 2 || len(state.Log[1].Changes) != 2 {
				t.Fatalf("versions/changeset shape: local=%d foreign=%d changes=%d", len(state.Versions[local.ID]), len(state.Versions[foreign.ID]), len(state.Log[1].Changes))
			}
			receipt := f.receipt(t, positiveMeta)
			if receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, commit) || receipt.Changeset.Kind == "" || receipt.Changeset.Revision.String() != "2" {
				t.Fatalf("positive receipt=%+v", receipt)
			}

			// Every refusal starts with already valid work, then fails late. The
			// complete public state, including the cross-project record, must remain
			// byte-for-byte equivalent and no failed request gets a receipt.
			refusals := []struct {
				name string
				cs   model.ChangeSet
				code model.ErrorCode
			}{
				{
					name: "late-invalid-target",
					cs: model.ChangeSet{Ops: []model.Op{
						patchOp(local, map[string]any{"title": "must roll back"}),
						commentOp(foreign, "must roll back"),
						patchOp(model.RefByID("missing-late", model.Natural64(0)), map[string]any{"title": "missing"}),
					}},
					code: "not_found",
				},
				{
					name: "bad-precondition",
					cs: model.ChangeSet{
						Ops:    []model.Op{patchOp(foreign, map[string]any{"description": "must roll back"}), commentOp(local, "must roll back")},
						Expect: []model.Precondition{model.PreconditionByRevision(foreign, model.Natural64(999))},
					},
					code: "precondition_failed",
				},
				{
					name: "forward-alias",
					cs: model.ChangeSet{Ops: []model.Op{
						patchOp(local, map[string]any{"title": "must roll back"}),
						commentOp(model.RefByKey("declared-later"), "must refuse"),
						createOp("declared-later", "too late", "batch-local"),
					}},
					code: "invalid_params",
				},
				{
					name: "cross-project-reference-late-failure",
					cs: model.ChangeSet{Ops: []model.Op{
						commentOp(foreign, "cross-project must roll back"),
						patchOp(local, map[string]any{"description": "must roll back"}),
						commentOp(model.RefByID("missing-cross-project", model.Natural64(0)), "must refuse"),
					}},
					code: "not_found",
				},
			}
			for _, refusal := range refusals {
				t.Run(refusal.name, func(t *testing.T) {
					before := f.state(t)
					meta := f.meta(refusal.name)
					_, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, refusal.cs, meta)
					requireCode(t, err, refusal.code)
					requireSameState(t, before, f.state(t))
					if got := f.receipt(t, meta); got != nil {
						t.Fatalf("refused batch published receipt=%+v", got)
					}
				})
			}

			beforeMismatch := f.state(t)
			mismatch := model.ChangeSet{Ops: []model.Op{
				createOp("local", "different request", "batch-local"),
				patchOp(model.RefByKey("local"), map[string]any{"description": "different request"}),
			}}
			_, err = f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, mismatch, positiveMeta)
			requireCode(t, err, "idempotency_key_reused")
			requireSameState(t, beforeMismatch, f.state(t))
			if got := f.receipt(t, positiveMeta); got == nil || !reflect.DeepEqual(got.Commit, commit) {
				t.Fatalf("replay mismatch changed receipt=%+v", got)
			}

			if engine != "memory" {
				beforeReopen := f.state(t)
				f.reopen(t)
				requireSameState(t, beforeReopen, f.state(t))
				reopened := f.receipt(t, positiveMeta)
				if reopened == nil || !reflect.DeepEqual(reopened.Commit, commit) || reopened.Changeset.Kind == "" {
					t.Fatalf("receipt did not survive %s reopen=%+v", engine, reopened)
				}
				replayed, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, positive, positiveMeta)
				if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, commit) {
					t.Fatalf("durable replay after %s reopen=%+v err=%v", engine, replayed, err)
				}
				requireSameState(t, beforeReopen, f.state(t))
				t.Logf("direct Writer %s: atomic create/patch/comment, rollback refusals, receipt mismatch and SQL reopen/replay passed", engine)
			} else {
				t.Logf("direct Writer memory: atomic create/patch/comment, rollback refusals and receipt mismatch passed")
			}
		})
	}
}
