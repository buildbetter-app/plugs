package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type auditFunc func(context.Context, port.Change, port.SnapshotReader) (port.ConditionalDecision, error)

func (f auditFunc) DecideConditional(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
	return f(ctx, c, r)
}

type eventContextFunc func(context.Context, port.ProposedChange, port.SnapshotReader) (port.EventContext, error)

func (f eventContextFunc) EventContext(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.EventContext, error) {
	return f(ctx, p, r)
}

// These decisions are test fixtures, not the service ontology adapter. The
// Writer, tracked reader, header and memory publication are real. Product
// ontology compilation is exercised separately with the service owner's code.
type auditCatalog map[string]map[string]model.ConditionDecision

var auditOntologyRef = model.Ref{ID: "ontology/audit-fixture"}

func fixtureCondition(status, category, outcome string) model.ConditionDecision {
	return model.ConditionDecision{Status: status, Category: category, Outcome: outcome, Terminal: category == "completed" || category == "canceled", Satisfied: category == "completed" && outcome == "failure"}
}

func fixtureCatalog() auditCatalog {
	catalog := auditCatalog{}
	for _, project := range []string{"blocker-project", "failure-project", ""} {
		outcome := "success"
		if project == "failure-project" {
			outcome = "failure"
		}
		catalog[project] = map[string]model.ConditionDecision{
			"todo": fixtureCondition("todo", "unstarted", ""), "in_progress": fixtureCondition("in_progress", "started", ""),
			"done": fixtureCondition("done", "completed", outcome), "failed": fixtureCondition("failed", "completed", "failure"),
			"canceled": fixtureCondition("canceled", "canceled", ""), "backlog": fixtureCondition("backlog", "backlog", ""),
		}
	}
	return catalog
}

func fixtureAudit(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
	rows, err := r.Get(ctx, []model.Ref{auditOntologyRef})
	if err != nil || len(rows) != 1 {
		return port.ConditionalDecision{}, fmt.Errorf("fixture ontology unavailable: %v", err)
	}
	var catalog auditCatalog
	if err := json.Unmarshal(rows[0].Body, &catalog); err != nil {
		return port.ConditionalDecision{}, err
	}
	before, err := DecodeDocument(c.Before)
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	after, err := DecodeDocument(c.After)
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	pin := model.OntologyPin{Ref: auditOntologyRef, Revision: rows[0].Revision, Digest: strings.Repeat("ab", 32)}
	if after.Project != "" {
		pin.Project = &model.Ref{ID: after.Project}
	}
	return port.ConditionalDecision{Ontology: pin, Before: catalog[after.Project][before.Fields["status"].(string)], After: catalog[after.Project][after.Fields["status"].(string)]}, nil
}

type auditSlotOntology struct{ testOntology }

func (o auditSlotOntology) Normalize(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	patches, err := o.testOntology.Normalize(ctx, p, r)
	status := map[string]string{"bead.fail": "failed", "bead.cancel": "canceled", "bead.reopen": "todo", "bead.defer": "backlog"}[p.Meta.Method]
	if status != "" {
		for _, c := range p.Changes {
			patches = append(patches, port.Patch{Resource: c.Resource, Paths: map[string]json.RawMessage{"fields.status": json.RawMessage(fmt.Sprintf("%q", status))}})
		}
	}
	return patches, err
}

func auditFixture(t *testing.T, count int, project string) (*harness, model.Ref, []model.Ref) {
	t.Helper()
	h := setup(t)
	blocker := model.Ref{ID: "pl-blocker"}
	body, _ := json.Marshal(fixtureCatalog())
	changes := []conformance.FixtureChange{{Ref: auditOntologyRef, Kind: "ontology", Op: model.OpCreate, Body: body}}
	doc := Document{ID: blocker.ID, Type: "issue", Project: project, Fields: map[string]any{"title": "blocker", "status": "todo"}}
	body, _ = json.Marshal(doc)
	changes = append(changes, conformance.FixtureChange{Ref: blocker, Kind: "bead", Op: model.OpCreate, Body: body})
	dependents := make([]model.Ref, count)
	// Deliberately seed reverse order. Logical notice order is by dependent ID.
	for i := count - 1; i >= 0; i-- {
		dependent := model.Ref{ID: fmt.Sprintf("pl-dependent-%04d", i)}
		dependents[i] = dependent
		doc := Document{ID: dependent.ID, Type: "issue", Project: "other-project", Fields: map[string]any{"title": "dependent", "status": "todo"}, Links: []Link{{To: blocker, Kind: "conditional-blocks"}}}
		body, _ = json.Marshal(doc)
		changes = append(changes, conformance.FixtureChange{Ref: dependent, Kind: "bead", Op: model.OpCreate, Body: body})
	}
	for start := 0; start < len(changes); start += 500 {
		if _, err := conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, changes[start:min(start+500, len(changes))]); err != nil {
			t.Fatal(err)
		}
	}
	return h, blocker, dependents
}

func auditWriter(h *harness, options ...Option) *Writer {
	return h.writer(append([]Option{WithOntology(auditSlotOntology{}), WithOntologyAudit(auditFunc(fixtureAudit))}, options...)...)
}

func auditEntry(t *testing.T, h *harness, revision model.Revision) model.Changeset {
	t.Helper()
	log, err := h.read(t).Changesets(context.Background(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range log {
		if entry.Revision.String() == revision.String() {
			if err := entry.ValidateEvents(); err != nil {
				t.Fatal(err)
			}
			return entry
		}
	}
	t.Fatalf("missing revision %s", revision)
	return model.Changeset{}
}

func slotMeta(n int, method string) model.WriteMeta {
	m := meta(n)
	m.Method = method
	return m
}

func TestAuditWriterTerminalOutcomeAndOwnProject(t *testing.T) {
	for _, tc := range []struct {
		method, project string
		ordinary        model.DurableEventType
		notices         int
	}{
		{"bead.close", "blocker-project", model.EventBeadClosed, 2},
		{"bead.cancel", "blocker-project", model.EventBeadCanceled, 2},
		{"bead.fail", "blocker-project", model.EventBeadFailed, 0},
		{"bead.claim", "blocker-project", model.EventBeadClaimed, 0},
		{"bead.defer", "blocker-project", model.EventBeadDeferred, 0},
		{"bead.close", "failure-project", model.EventBeadClosed, 0},
		{"bead.close", "", model.EventBeadClosed, 2},
	} {
		t.Run(tc.method+"/"+tc.project, func(t *testing.T) {
			h, blocker, dependents := auditFixture(t, 2, tc.project)
			before := h.read(t)
			old, _ := before.Get(context.Background(), append([]model.Ref{blocker, auditOntologyRef}, dependents...))
			versions := map[string]model.Revision{}
			for _, row := range old {
				versions[row.Ref.ID] = row.Revision
			}
			cs := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{}), {Kind: model.OpComment, Comment: &model.CommentOp{Target: blocker, Body: "terminal reason"}}}}
			result := apply(t, auditWriter(h), cs, slotMeta(10, tc.method))
			entry := auditEntry(t, h, result.Revision)
			if len(entry.Changes) != 1 || entry.Changes[0].Ref.ID != blocker.ID || entry.Changes[0].Op != model.OpPatch || len(entry.Events) != 1+tc.notices || entry.Events[0].Type != tc.ordinary {
				t.Fatalf("atomic plan=%+v", entry)
			}
			for i, event := range entry.Events {
				if event.Index.String() != fmt.Sprint(i) || event.ChangeIndex.String() != "0" || event.Provenance.Method != tc.method || event.Provenance.Slot != strings.TrimPrefix(tc.method, "bead.") || event.Provenance.SetBy["fields.status"] != "request" || event.Provenance.SetBy["comments"] != "request" {
					t.Fatalf("event provenance/order=%+v", event)
				}
				if i == 0 {
					continue
				}
				d := event.Details
				if event.Type != model.EventDependencyConditionUnmet || event.Resource.Ref.ID != dependents[i-1].ID || d.Dependent.Revision.String() != versions[event.Resource.Ref.ID].String() || d.Edge.Revision.String() != d.Dependent.Revision.String() || d.BlockerBefore.Revision.String() != versions[blocker.ID].String() || d.BlockerAfter.Revision.String() != result.Revision.String() || d.Ontology.Revision.String() != versions[auditOntologyRef.ID].String() || d.After.Satisfied || !d.After.Terminal {
					t.Fatalf("immutable notice=%+v", event)
				}
				if tc.project == "" && d.Ontology.Project != nil || tc.project != "" && (d.Ontology.Project == nil || d.Ontology.Project.ID != tc.project) {
					t.Fatalf("wrong project pin=%+v", d.Ontology)
				}
			}
			for _, dependent := range dependents {
				versions, err := h.read(t).Versions(context.Background(), dependent, driver.PageReq{})
				if err != nil || len(versions.Items) != 1 {
					t.Fatalf("dependent gained fake version: %+v %v", versions, err)
				}
			}
			if len(receipt(t, h, slotMeta(10, tc.method)).Commit.Results) != 2 {
				t.Fatal("event plan changed the operation receipt")
			}
		})
	}
}

func TestAuditNoopReplayDryRunAndReceiptBytes(t *testing.T) {
	h, blocker, _ := auditFixture(t, 1, "blocker-project")
	var calls atomic.Int32
	w := auditWriter(h, WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
		calls.Add(1)
		return fixtureAudit(ctx, c, r)
	})))
	cs := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}
	m := slotMeta(10, "bead.close")
	dry := m
	dry.DryRun = true
	dryResult := apply(t, w, cs, dry)
	if !dryResult.WouldCommit || h.read(t).Snapshot().At.String() != "1" {
		t.Fatalf("dry run=%+v", dryResult)
	}
	principal, _ := driver.PrincipalKey(by)
	if receipt, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey); err != nil || receipt != nil {
		t.Fatalf("dry-run receipt=%+v %v", receipt, err)
	}
	first, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
	if err != nil || first.Replayed {
		t.Fatalf("first=%+v %v", first, err)
	}
	again, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
	if err != nil || !again.Replayed {
		t.Fatalf("replay=%+v %v", again, err)
	}
	want, _ := json.Marshal(first.Commit)
	got, _ := json.Marshal(again.Commit)
	stored, _ := json.Marshal(receipt(t, h, m).Commit)
	if string(want) != string(got) || string(want) != string(stored) || calls.Load() != 2 {
		t.Fatalf("receipt/replay bytes differ or callback reran: calls=%d", calls.Load())
	}
	noop := apply(t, w, cs, slotMeta(11, "bead.close"))
	if noop.Revision.String() != first.Commit.Revision.String() || calls.Load() != 2 {
		t.Fatalf("fresh no-op emitted: %+v calls=%d", noop, calls.Load())
	}
	entry := auditEntry(t, h, first.Commit.Revision)
	if len(entry.Events) != 2 || h.read(t).Snapshot().At.String() != "2" {
		t.Fatalf("unexpected events/revision: %+v", entry)
	}
}

func TestAuditTrustedContextAndDecisionOwnership(t *testing.T) {
	h, blocker, _ := auditFixture(t, 2, "blocker-project")
	var returned port.ConditionalDecision
	contextValue := port.EventContext{Cause: model.EventCause{Root: "trusted-root", Depth: model.Natural64(1), Chain: []string{"trusted-trigger"}}, Chain: []string{"authz/project"}}
	w := auditWriter(h, WithEventContext(eventContextFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) (port.EventContext, error) {
		p.Meta.Method = "forged"
		p.Changes[0].SetBy["fields.status"] = "forged"
		return contextValue, nil
	})), WithOntologyAudit(auditFunc(func(ctx context.Context, c port.Change, r port.SnapshotReader) (port.ConditionalDecision, error) {
		value, err := fixtureAudit(ctx, c, r)
		c.SetBy["fields.status"] = "forged"
		c.After[0] = '!'
		returned = value
		return value, err
	})))
	result := apply(t, w, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, slotMeta(10, "bead.close"))
	contextValue.Chain[0], contextValue.Cause.Chain[0] = "changed", "changed"
	returned.Ontology.Project.ID = "changed"
	entry := auditEntry(t, h, result.Revision)
	if !reflect.DeepEqual(entry.Chain, []string{"authz/project"}) {
		t.Fatalf("header chain=%v", entry.Chain)
	}
	for _, event := range entry.Events {
		if event.Provenance.Method != "bead.close" || event.Provenance.SetBy["fields.status"] != "request" || event.Provenance.Cause.Root != "trusted-root" || event.Provenance.Cause.Chain[0] != "trusted-trigger" || event.Provenance.Chain[0] != "authz/project" {
			t.Fatalf("provenance alias=%+v", event)
		}
		if event.Details != nil && event.Details.Ontology.Project.ID != "blocker-project" {
			t.Fatalf("decision alias=%+v", event)
		}
	}
	entry.Events[1].Provenance.Chain[0] = "corrupted"
	entry.Events[1].Details.Ontology.Project.ID = "corrupted"
	fresh := auditEntry(t, h, result.Revision)
	if fresh.Events[1].Provenance.Chain[0] != "authz/project" || fresh.Events[2].Details.Ontology.Project.ID != "blocker-project" {
		t.Fatal("read-side descriptor alias escaped")
	}
}

func TestAuditPostFinalizationRetainsPlanAndReplayBytes(t *testing.T) {
	h, blocker, _ := auditFixture(t, 1, "blocker-project")
	var posts atomic.Int32
	w := auditWriter(h, WithPostSubscribers(PostBinding{ID: "audit-post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
		posts.Add(1)
		return errors.New("post warning fixture")
	})}))
	cs, m := model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{})}}, slotMeta(10, "bead.close")
	first := apply(t, w, cs, m)
	again := apply(t, w, cs, m)
	entry := auditEntry(t, h, first.Revision)
	stored := receipt(t, h, m)
	want, _ := json.Marshal(first)
	got, _ := json.Marshal(again)
	durable, _ := json.Marshal(stored.Commit)
	if posts.Load() != 1 || len(first.Warnings) != 1 || stored.ResponsePending || len(entry.Events) != 2 || string(want) != string(got) || string(want) != string(durable) || h.read(t).Snapshot().At.String() != "2" {
		t.Fatalf("audit post/replay changed receipt or plan: first=%+v replay=%+v stored=%+v posts=%d", first, again, stored, posts.Load())
	}
}
