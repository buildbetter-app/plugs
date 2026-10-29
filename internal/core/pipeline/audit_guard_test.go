package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func assertAuditUnpublished(t *testing.T, h *harness, blocker model.Ref, at model.Revision, m model.WriteMeta) {
	t.Helper()
	r := h.read(t)
	if r.Snapshot().At.String() != at.String() {
		t.Fatalf("refused write moved head from %s to %s", at, r.Snapshot().At)
	}
	rows, err := r.Get(context.Background(), []model.Ref{blocker})
	if err != nil || len(rows) != 1 {
		t.Fatalf("blocker=%+v %v", rows, err)
	}
	doc, err := DecodeDocument(rows[0].Body)
	if err != nil || doc.Fields["status"] != "todo" {
		t.Fatalf("refused write changed blocker: %+v %v", doc, err)
	}
	principal, _ := driver.PrincipalKey(by)
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
	if err != nil || receipt != nil {
		t.Fatalf("refused receipt=%+v %v", receipt, err)
	}
}

func TestAuditBadDecisionAndConfigurationRefuseAtomically(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*port.ConditionalDecision)
	}{
		{"status", func(d *port.ConditionalDecision) { d.After.Status = "invented" }},
		{"project", func(d *port.ConditionalDecision) { d.Ontology.Project.ID = "other-project" }},
		{"projectless", func(d *port.ConditionalDecision) { d.Ontology.Project = nil }},
		{"source-revision", func(d *port.ConditionalDecision) { d.Ontology.Revision = model.Natural64(0) }},
		{"source-miss", func(d *port.ConditionalDecision) { d.Ontology.Ref.ID = "ontology/missing" }},
		{"source-kind", func(d *port.ConditionalDecision) { d.Ontology.Ref.ID = "pl-blocker" }},
		{"digest", func(d *port.ConditionalDecision) { d.Ontology.Digest = "unknown" }},
		{"decision", func(d *port.ConditionalDecision) { d.After.Satisfied = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, blocker, _ := auditFixture(t, 1, "blocker-project")
			w := auditWriter(h, WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
				d, err := fixtureAudit(ctx, c, r)
				tc.edit(&d)
				return d, err
			})))
			m := slotMeta(10, "bead.close")
			for _, dry := range []bool{true, false} {
				m.DryRun = dry
				_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, m)
				code(t, err, "invalid_params")
				assertAuditUnpublished(t, h, blocker, model.Natural64(1), m)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		option Option
	}{
		{"explicit-nil", WithOntologyAudit(nil)},
		{"bad-context", WithEventContext(eventContextFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.EventContext, error) {
			return port.EventContext{}, nil
		}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, blocker, _ := auditFixture(t, 1, "blocker-project")
			m := slotMeta(10, "bead.close")
			_, err := auditWriter(h, tc.option).Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, m)
			code(t, err, "invalid_params")
			assertAuditUnpublished(t, h, blocker, model.Natural64(1), m)
		})
	}
}

func TestAuditCompletePlanCapIncludesOrdinaryAndNeverTruncates(t *testing.T) {
	for _, count := range []int{999, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			h, blocker, _ := auditFixture(t, count, "blocker-project")
			at := *h.read(t).Snapshot().At
			w, m := auditWriter(h), slotMeta(10, "bead.close")
			cs := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}
			if count == 1000 {
				for _, dry := range []bool{true, false} {
					m.DryRun = dry
					_, err := w.Apply(context.Background(), by, cs, m)
					code(t, err, "scope_too_large")
					assertAuditUnpublished(t, h, blocker, at, m)
				}
				return
			}
			result := apply(t, w, cs, m)
			entry := auditEntry(t, h, result.Revision)
			if len(entry.Events) != model.MaxChangesetEvents || entry.Events[999].Resource.Ref.ID != "pl-dependent-0998" || len(entry.Changes) != 1 {
				t.Fatalf("capped plan was truncated: count=%d changes=%d", len(entry.Events), len(entry.Changes))
			}
		})
	}
}

func TestAuditFinalEdgeMembershipAndMixedSlotBatchRefusal(t *testing.T) {
	h, blocker, dependents := auditFixture(t, 2, "blocker-project")
	w := auditWriter(h)
	remove := link(dependents[0], blocker, "conditional-blocks")
	remove.Kind = model.OpUnlink
	// A graph batch is interpreted by its final image: the temporary removal
	// and restoration is a no-op, while the second edge is actually removed.
	other := link(dependents[1], blocker, "conditional-blocks")
	other.Kind = model.OpUnlink
	batch := apply(t, w, model.ChangeSet{Ops: []model.Op{remove, link(dependents[0], blocker, "conditional-blocks"), other}}, meta(10))
	entry := auditEntry(t, h, batch.Revision)
	if len(entry.Changes) != 1 || entry.Changes[0].Ref.ID != dependents[1].ID || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadUnlinked {
		t.Fatalf("final graph image plan=%+v", entry)
	}
	m := slotMeta(11, "bead.close")
	_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{}), remove}}, m)
	code(t, err, "invalid_params")
	assertAuditUnpublished(t, h, blocker, batch.Revision, m)
	closed := apply(t, w, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, slotMeta(12, "bead.close"))
	entry = auditEntry(t, h, closed.Revision)
	if len(entry.Events) != 2 || entry.Events[1].Resource.Ref.ID != dependents[0].ID || entry.Events[1].Details.Dependent.Revision.String() != "1" {
		t.Fatalf("notice did not use final retained edge=%+v", entry.Events)
	}
}

func TestAuditOntologyAndEdgeInterferenceRetryUnderSameGuard(t *testing.T) {
	for _, action := range []string{"ontology", "remove-edge", "insert-edge", "continual-ontology"} {
		t.Run(action, func(t *testing.T) {
			h, blocker, dependents := auditFixture(t, 1, "blocker-project")
			ordinary := h.writer()
			late := apply(t, ordinary, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
			var calls atomic.Int32
			w := auditWriter(h, WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
				d, err := fixtureAudit(ctx, c, r)
				if err != nil {
					return d, err
				}
				n := calls.Add(1)
				if n == 1 || action == "continual-ontology" {
					switch action {
					case "ontology", "continual-ontology":
						body, _ := json.Marshal(fixtureCatalog())
						_, err = conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: auditOntologyRef, Kind: "ontology", Op: model.OpPatch, Body: body}})
					case "remove-edge":
						op := link(dependents[0], blocker, "conditional-blocks")
						op.Kind = model.OpUnlink
						_, err = ordinary.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{op}}, meta(100))
					case "insert-edge":
						_, err = ordinary.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{link(late, blocker, "conditional-blocks")}}, meta(100))
					}
				}
				return d, err
			})))
			m := slotMeta(10, "bead.close")
			result, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, m)
			if action == "continual-ontology" {
				code(t, err, "write_conflict")
				if calls.Load() != 4 {
					t.Fatalf("unbounded retry count=%d", calls.Load())
				}
				assertAuditUnpublished(t, h, blocker, model.Natural64(6), m)
				return
			}
			if err != nil || result.Revision.String() != "4" {
				t.Fatalf("retry result=%+v err=%v", result, err)
			}
			entry := auditEntry(t, h, result.Revision)
			switch action {
			case "ontology":
				if calls.Load() != 2 || entry.Events[1].Details.Ontology.Revision.String() != "3" {
					t.Fatalf("stale ontology decision published: %+v calls=%d", entry.Events, calls.Load())
				}
			case "remove-edge":
				if len(entry.Events) != 1 {
					t.Fatalf("removed edge noticed: %+v", entry.Events)
				}
			case "insert-edge":
				if calls.Load() != 2 || len(entry.Events) != 3 {
					t.Fatalf("inserted edge omitted: %+v calls=%d", entry.Events, calls.Load())
				}
			}
		})
	}
}

type auditCommitFailureStore struct{ driver.Store }
type auditCommitFailureTx struct{ driver.WriteTx }

func (s auditCommitFailureStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	return auditCommitFailureTx{tx}, err
}

func (tx auditCommitFailureTx) Commit(context.Context, driver.ChangesetHeader) (model.Changeset, error) {
	return model.Changeset{}, errors.New("injected commit refusal after staging")
}

func TestAuditCancellationFenceAndCommitFailurePublishNothing(t *testing.T) {
	for _, mode := range []string{"cancel", "fence", "commit"} {
		t.Run(mode, func(t *testing.T) {
			h, blocker, _ := auditFixture(t, 1, "blocker-project")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := auditWriter(h, WithOntologyAudit(auditFunc(func(call context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
				d, err := fixtureAudit(call, c, r)
				if mode == "cancel" {
					cancel()
				}
				if mode == "fence" {
					err = h.lease.Release(context.Background())
				}
				return d, err
			})))
			if mode == "commit" {
				w.store = auditCommitFailureStore{h.store}
			}
			m := slotMeta(10, "bead.close")
			if _, err := w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, m); err == nil {
				t.Fatal("fault accepted")
			}
			assertAuditUnpublished(t, h, blocker, model.Natural64(1), m)
		})
	}
}

func TestAuditCallbacksCannotWriteOrExceedReadBudget(t *testing.T) {
	for _, mode := range []string{"reentrant", "budget", "panic", "malformed-status"} {
		t.Run(mode, func(t *testing.T) {
			h, blocker, _ := auditFixture(t, 1, "blocker-project")
			w := auditWriter(h, WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
				switch mode {
				case "reentrant":
					_, err := h.writer().Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("")}}, meta(90))
					return port.ConditionalDecision{}, err
				case "budget":
					for i := 0; i < 33; i++ {
						if _, err := r.Get(ctx, []model.Ref{auditOntologyRef}); err != nil {
							return port.ConditionalDecision{}, err
						}
					}
				case "panic":
					panic("fixture callback panic")
				}
				return fixtureAudit(ctx, c, r)
			})))
			cs, m := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, slotMeta(10, "bead.close")
			if mode == "malformed-status" {
				m.Method = "tx.apply"
				cs.Ops[0].Patch.Fields["status"] = map[string]any{"invalid": true}
			}
			_, err := w.Apply(context.Background(), by, cs, m)
			if err == nil {
				t.Fatal("callback fault accepted")
			}
			if mode == "reentrant" {
				code(t, err, "plugin_reentrant_write")
			}
			assertAuditUnpublished(t, h, blocker, model.Natural64(1), m)
		})
	}
}

func TestAuditInstalledAuthorizerRequiresTrustedContext(t *testing.T) {
	h, blocker, _ := auditFixture(t, 1, "blocker-project")
	allow := WithAuthorizer(authorizerFunc(func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error {
		return nil
	}))
	cs, m := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, slotMeta(10, "bead.close")
	_, err := auditWriter(h, allow).Apply(context.Background(), by, cs, m)
	code(t, err, "invalid_params")
	assertAuditUnpublished(t, h, blocker, model.Natural64(1), m)
	source := WithEventContext(eventContextFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.EventContext, error) {
		return port.EventContext{Cause: model.EventCause{Root: "trusted", Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{"policy/project"}}, nil
	}))
	result := apply(t, auditWriter(h, allow, source), cs, m)
	if auditEntry(t, h, result.Revision).Chain[0] != "policy/project" {
		t.Fatal("trusted authorization chain omitted")
	}
}
