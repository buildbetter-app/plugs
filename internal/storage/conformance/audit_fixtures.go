package conformance

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type auditFixture struct {
	ctx     context.Context
	store   driver.Store
	lease   driver.ActiveLease
	meta    driver.Meta
	header  driver.ChangesetHeader
	changes []FixtureChange
}

// Source records are real driver history. Only the blocker changes in the
// audited write; every dependent and ontology pin names an unchanged version.
func newAuditFixture(t *testing.T, factory Factory, notices int) auditFixture {
	t.Helper()
	ctx, s, lease, meta := setup(t, factory)
	initial := []FixtureChange{
		{Ref: ref("pl-blocker"), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"status":"working"}`)},
		{Ref: ref("pl-ontology"), Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"task":{"working":"started","finished":"completed"}}`)},
		{Ref: ref("pl-project"), Kind: "project", Op: model.OpCreate, Body: []byte(`{"name":"project"}`)},
	}
	for i := 0; i < notices; i++ {
		initial = append(initial, FixtureChange{Ref: ref(fmt.Sprintf("pl-dependent-%04d", i)), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"status":"waiting","dependencies":[{"kind":"conditional-blocks","blocker":"pl-blocker"}]}`)})
	}
	versions := map[string]model.Revision{}
	var head model.Revision
	for start := 0; start < len(initial); start += 500 {
		batch := initial[start:min(start+500, len(initial))]
		entry, err := Seed(ctx, s, lease, driver.ChangesetHeader{}, batch)
		if err != nil {
			t.Fatal(err)
		}
		head = entry.Revision
		for _, change := range batch {
			versions[change.Ref.ID] = head
		}
	}
	n, err := head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	provenance := model.EventProvenance{
		Method: "bead.close", Slot: "close", SetBy: map[string]string{"status": "request"},
		Cause: model.EventCause{Root: "audit-request", Depth: model.Natural64(0), Chain: []string{"source"}},
		Chain: []string{"authz.team/default"},
	}
	events := []model.EventDescriptor{{Index: model.Natural64(0), ChangeIndex: model.Natural64(0), Type: model.EventBeadClosed, Resource: model.AuditResource{Kind: "bead", Ref: ref("pl-blocker")}, Provenance: provenance}}
	for i := 0; i < notices; i++ {
		dependent := ref(fmt.Sprintf("pl-dependent-%04d", i))
		project := ref("pl-project")
		details := &model.ConditionUnmetDetails{
			Dependent:     model.EventImage{Ref: dependent, Revision: versions[dependent.ID]},
			BlockerBefore: model.EventImage{Ref: ref("pl-blocker"), Revision: versions["pl-blocker"]},
			BlockerAfter:  model.EventImage{Ref: ref("pl-blocker"), Revision: model.Natural64(n + 1)},
			Edge:          model.ConditionalEdge{Dependent: dependent, Blocker: ref("pl-blocker"), Kind: "conditional-blocks", Revision: versions[dependent.ID]},
			Ontology:      model.OntologyPin{Ref: ref("pl-ontology"), Revision: versions["pl-ontology"], Digest: strings.Repeat("ab", 32), Project: &project},
			Before:        model.ConditionDecision{Status: "working", Category: "started"},
			After:         model.ConditionDecision{Status: "finished", Category: "completed", Outcome: "success", Terminal: true},
		}
		events = append(events, model.EventDescriptor{Index: model.Natural64(uint64(i + 1)), ChangeIndex: model.Natural64(0), Type: model.EventDependencyConditionUnmet, Resource: model.AuditResource{Kind: "bead", Ref: dependent}, Provenance: provenance, Details: details})
	}
	h := driver.ChangesetHeader{
		Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "conformance"},
		Chain: append([]string(nil), provenance.Chain...), Method: "bead.close", IdempotencyKey: "audit-key", RequestHash: "audit-digest", Events: events,
		Response: model.Commit{Results: []model.OpResult{{Ref: ref("pl-blocker"), Revision: model.Natural64(n + 1)}, {Ref: ref("pl-dependent-0000"), Revision: versions["pl-dependent-0000"]}}, Warnings: []model.Warning{}},
	}
	return auditFixture{ctx: ctx, store: s, lease: lease, meta: meta, header: h, changes: []FixtureChange{{Ref: ref("pl-blocker"), Kind: "bead", Op: model.OpPatch, Body: []byte(`{"status":"finished"}`), ChangedPaths: []string{"/status"}}}}
}
