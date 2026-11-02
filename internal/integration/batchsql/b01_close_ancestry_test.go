package batchsql_test

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Canonical parents control scheduling, while a child's close guard continues
// to consider its own blockers. Every observation crosses the public RPC host.
func TestB01PublicAncestorSchedulingPreservesChildCloseAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, condition := range []string{"deferred-parent", "blocked-parent"} {
				t.Run(condition, func(t *testing.T) {
					f := newFixture(t, engine)
					h := newB01Public(t, f)
					parent := h.create(t, b01Epic, "scheduling ancestor", nil)
					child := h.create(t, b01Child, "closable child", &parent)
					blocker := h.create(t, b01New, "live direct blocker", nil)
					viewer := h.viewer(t, b01Projects...)
					link := func(kind model.OpKind, from model.Ref, project string) {
						t.Helper()
						op := model.Op{Kind: kind, Link: &model.LinkOp{Kind: kind, From: from, To: blocker, LinkKind: "blocks"}}
						params := map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{op}}, "_meta": map[string]any{"project": project, "idempotencyKey": f.meta("blocker").IdempotencyKey}}
						b01Committed(t, h.call(t, h.slots, "", "tx.apply", params))
					}
					if condition == "deferred-parent" {
						h.slot(t, "bead.defer", b01Epic, parent)
					} else {
						link(model.OpLink, parent, b01Epic)
					}
					parentBefore := f.state(t)
					h.ready(t, h.viewer(t, b01Child))
					h.unchanged(t, func() b01Reply {
						return h.call(t, h.slots, "", "bead.claim", map[string]any{"id": child, "_meta": map[string]any{"project": b01Child, "idempotencyKey": f.meta("claim").IdempotencyKey}})
					}, "ontology_guard_failed")

					// The child's own live explicit dependency still blocks close.
					link(model.OpLink, child, b01Child)
					h.unchanged(t, func() b01Reply {
						return h.call(t, h.slots, "", "bead.close", map[string]any{"id": child, "_meta": map[string]any{"project": b01Child, "idempotencyKey": f.meta("close-refusal").IdempotencyKey}})
					}, "ontology_guard_failed")
					link(model.OpUnlink, child, b01Child)
					h.slot(t, "bead.close", b01Child, child)
					if bead := h.get(t, viewer, child, nil); bead["status"] != "done" || bead["parent"] != parent.ID {
						t.Fatalf("child close lost terminal status or parent: %v", bead)
					}
					closed := f.state(t)
					if !reflect.DeepEqual(parentBefore.Records[parent.ID], closed.Records[parent.ID]) || !reflect.DeepEqual(parentBefore.Versions[parent.ID], closed.Versions[parent.ID]) {
						t.Fatal("child close changed its deferred or blocked parent")
					}

					// A fresh idempotent close still uses the settlement predicate.
					h.slot(t, "bead.close", b01Child, child)
					settled := f.state(t)
					if !reflect.DeepEqual(closed.Records, settled.Records) || !reflect.DeepEqual(closed.Versions, settled.Versions) {
						t.Fatal("close settlement changed bead records or versions")
					}
					verifyHierarchy(t, f)
				})
			}
		})
	}
}
