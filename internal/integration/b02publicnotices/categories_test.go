package b02publicnotices_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

type categoryCase struct {
	name, method, status           string
	category                       ontology.Category
	outcome                        ontology.Outcome
	event                          model.DurableEventType
	ordinary, conditional, notices bool
}

var categories = []categoryCase{
	{name: "completed_success", method: "bead.close", status: "b02-done", category: ontology.Completed, outcome: ontology.Success, event: model.EventBeadClosed, ordinary: true, notices: true},
	{name: "completed_failure", method: "bead.fail", status: "b02-failed", category: ontology.Completed, outcome: ontology.Failure, event: model.EventBeadFailed, ordinary: true, conditional: true},
	{name: "canceled", method: "bead.cancel", status: "b02-canceled", category: ontology.Canceled, event: model.EventBeadCanceled, ordinary: true, notices: true},
	{name: "frozen_backlog", method: "bead.defer", status: "frozen", category: ontology.Backlog, event: model.EventBeadDeferred},
	{name: "pinned_backlog", method: "bead.defer", status: "pinned", category: ontology.Backlog, event: model.EventBeadDeferred},
}

// Same custom keys deliberately disagree across the installed project
// catalogs. Slot targets remain valid in each workflow. Readiness and notices
// must judge a cross-project blocker using its own persisted project.
func projectCatalog(dependent bool, deferred string) ontology.Document {
	statuses := map[string]ontology.Status{
		"b02-open":     {Name: "Available", Category: ontology.Unstarted},
		"b02-working":  {Name: "In flight", Category: ontology.Started},
		"b02-done":     {Name: "Successful", Category: ontology.Completed, Outcome: ontology.Success},
		"b02-failed":   {Name: "Unsuccessful", Category: ontology.Completed, Outcome: ontology.Failure},
		"b02-canceled": {Name: "Stopped", Category: ontology.Canceled},
		"frozen":       {Name: "Frozen import mapping", Category: ontology.Backlog},
		"pinned":       {Name: "Pinned import mapping", Category: ontology.Backlog},
	}
	w := ontology.DefaultDocument().Workflows["issue"]
	w.Initial = "b02-open"
	w.Slots[ontology.Claim] = "b02-working"
	w.Slots[ontology.Close] = "b02-done"
	w.Slots[ontology.Fail] = "b02-failed"
	w.Slots[ontology.Cancel] = "b02-canceled"
	w.Slots[ontology.Defer] = deferred
	w.Slots[ontology.Reopen] = "b02-open"
	if dependent {
		statuses["b02-done"] = ontology.Status{Name: "Same key is backlog here", Category: ontology.Backlog}
		statuses["b02-failed"] = ontology.Status{Name: "Same key is success here", Category: ontology.Completed, Outcome: ontology.Success}
		statuses["b02-canceled"] = ontology.Status{Name: "Same key is backlog here", Category: ontology.Backlog}
		statuses["b02-dependent-canceled"] = ontology.Status{Name: "Dependent project stopped", Category: ontology.Canceled}
		statuses["frozen"] = ontology.Status{Name: "Same key is failure here", Category: ontology.Completed, Outcome: ontology.Failure}
		statuses["pinned"] = ontology.Status{Name: "Same key is success here", Category: ontology.Completed, Outcome: ontology.Success}
		w.Slots[ontology.Close], w.Slots[ontology.Fail], w.Slots[ontology.Cancel], w.Slots[ontology.Defer] = "b02-failed", "frozen", "b02-dependent-canceled", "b02-done"
	}
	for key := range statuses {
		w.Statuses = append(w.Statuses, key)
	}
	sort.Strings(w.Statuses)
	// Carry the canonical guards to the renamed slot targets. No guard is
	// weakened to arrange a passing transition.
	for i := range w.Transitions {
		rule := &w.Transitions[i]
		if rule.To == "in_progress" {
			rule.From = []string{"b02-open"}
			rule.To = w.Slots[ontology.Claim]
		} else {
			for slot, original := range ontology.DefaultDocument().Workflows["issue"].Slots {
				if rule.To == original {
					rule.To = w.Slots[slot]
					break
				}
			}
		}
	}
	return ontology.Document{Statuses: statuses, Workflows: map[string]ontology.Workflow{"issue": w}}
}

type graph struct {
	blocker     model.Ref
	ordinary    model.Ref
	conditional []model.Ref // requested z,a; notices must persist a,z
}

func (g graph) dependents() []model.Ref {
	return append([]model.Ref{g.ordinary}, g.conditional...)
}

func (g graph) all() []model.Ref {
	return append([]model.Ref{sourceRef, g.blocker}, g.dependents()...)
}

func (f *fixture) graph(t *testing.T) graph {
	t.Helper()
	refs := []model.Ref{}
	for _, project := range []string{blockerProject, dependentProject, dependentProject, dependentProject} {
		commit := f.write(t, "bead.create", project, map[string]any{"type": "issue", "fields": map[string]any{"title": "B02 public category proof"}})
		ref := commit.Results[0].Ref
		refs = append(refs, ref)
		var got struct {
			Bead struct{ Status, Project string }
		}
		f.result(t, "bead.get", map[string]any{"id": ref}, &got)
		if got.Bead.Status != "b02-open" || got.Bead.Project != project {
			t.Fatalf("ordinary RPC did not use the publicly installed workflow: %+v", got)
		}
	}
	g := graph{refs[0], refs[1], refs[2:]}
	for i, dependent := range g.dependents() {
		kind := "conditional-blocks"
		if i == 0 {
			kind = "blocks"
		}
		f.write(t, "bead.link", dependentProject, map[string]any{"from": dependent, "to": g.blocker, "kind": kind})
	}
	return g
}

func (f *fixture) ready(t *testing.T, project string, want ...string) {
	t.Helper()
	var page struct {
		Items      []struct{ ID string }
		NextCursor string
	}
	f.result(t, "bead.ready", map[string]any{"limit": 100, "_meta": map[string]any{"project": project}}, &page)
	if page.NextCursor != "" {
		t.Fatal("small exact ready set unexpectedly paginated")
	}
	got := []string{}
	for _, bead := range page.Items {
		got = append(got, bead.ID)
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) || len(got) > 0 && !reflect.DeepEqual(got, want) {
		t.Fatalf("project %s ready IDs=%v want=%v", project, got, want)
	}
}

func (f *fixture) readyAfter(t *testing.T, c categoryCase, g graph) {
	t.Helper()
	want := []string{}
	if c.ordinary {
		want = append(want, g.ordinary.ID)
	}
	if c.conditional {
		for _, ref := range g.conditional {
			want = append(want, ref.ID)
		}
	}
	f.ready(t, dependentProject, want...)
	f.ready(t, blockerProject) // terminal and backlog subjects are not ready
}

var eventTypes = []string{"bead.closed", "bead.failed", "bead.canceled", "bead.deferred", "bead.reopened", "dependency.conditionUnmet"}

type eventPage struct {
	Events []model.Event `json:"events"`
	Cursor string        `json:"cursor"`
}

func (f *fixture) events(t *testing.T, after string, limit int) eventPage {
	t.Helper()
	params := map[string]any{"filter": map[string]any{"events": eventTypes}, "limit": limit}
	if after != "" {
		params["after"] = after
	}
	var page eventPage
	f.result(t, "event.list", params, &page)
	if page.Cursor == "" {
		t.Fatal("public event reader omitted persisted position cursor")
	}
	return page
}

// Filtered ontology-change boundaries still advance the public cursor. Drain
// the setup stream using only those returned opaque cursors before exercising
// lifecycle; an empty visible page alone does not establish the baseline.
func (f *fixture) setupBaseline(t *testing.T) eventPage {
	t.Helper()
	const maxPages = 16
	cursor := ""
	for page := 1; page <= maxPages; page++ {
		baseline := f.events(t, cursor, 100)
		if len(baseline.Events) != 0 {
			t.Fatalf("setup page %d synthesized lifecycle or condition events: %+v", page, baseline.Events)
		}
		if baseline.Cursor == cursor {
			t.Logf("B02 setup baseline: %d public pages, no lifecycle/notice events, returned cursor stopped advancing", page)
			return baseline
		}
		cursor = baseline.Cursor
	}
	t.Fatalf("setup event baseline cursor did not settle within %d public pages", maxPages)
	return eventPage{}
}

func assertPlan(t *testing.T, f *fixture, c categoryCase, g graph, before map[string]retained, set model.Changeset, request map[string]any) {
	t.Helper()
	wantCount := 1
	if c.notices {
		wantCount += len(g.conditional)
	}
	if set.Incarnation != f.meta(t).Incarnation || set.Kind != model.ChangesetWrite || set.Time.IsZero() || len(set.Changes) != 1 || len(set.Events) != wantCount || !reflect.DeepEqual(set.Principal, caller) {
		t.Fatalf("incomplete real lifecycle publication: %+v", set)
	}
	if !sameRef(set.Changes[0].Ref, g.blocker) || set.Changes[0].FromRevision != before[g.blocker.ID].Record.Revision || set.Events[0].Type != c.event || !sameRef(set.Events[0].Resource.Ref, g.blocker) || set.Events[0].Details != nil {
		t.Fatalf("lifecycle mutation/ordinary event does not match public request: %+v", set)
	}
	key := request["_meta"].(map[string]any)["idempotencyKey"].(string)
	sorted := append([]model.Ref(nil), g.conditional...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for i, descriptor := range set.Events {
		if descriptor.Index != model.Natural64(uint64(i)) || !descriptor.ChangeIndex.IsZero() || descriptor.Provenance.Method != c.method || descriptor.Provenance.Slot != c.method[len("bead."):] || descriptor.Provenance.Cause.Root != key || !descriptor.Provenance.Cause.Depth.IsZero() {
			t.Fatalf("persisted descriptor lost order/source provenance: %+v", descriptor)
		}
		if i == 0 {
			continue
		}
		dependent := sorted[i-1]
		if descriptor.Type != model.EventDependencyConditionUnmet || !sameRef(descriptor.Resource.Ref, dependent) || descriptor.Details == nil {
			t.Fatalf("notice order must be a,z after the ordinary event: %+v", descriptor)
		}
		d := descriptor.Details
		if !sameRef(d.Dependent.Ref, dependent) || d.Dependent.Revision != before[dependent.ID].Record.Revision || !sameRef(d.Edge.Dependent, dependent) || !sameRef(d.Edge.Blocker, g.blocker) || d.Edge.Kind != "conditional-blocks" || d.Edge.Revision != d.Dependent.Revision || !sameRef(d.BlockerBefore.Ref, g.blocker) || d.BlockerBefore.Revision != before[g.blocker.ID].Record.Revision || !sameRef(d.BlockerAfter.Ref, g.blocker) || d.BlockerAfter.Revision != set.Revision {
			t.Fatalf("notice is not pinned to its real SQL graph sources: %+v", d)
		}
		if !sameRef(d.Ontology.Ref, sourceRef) || d.Ontology.Revision != f.source.Revision || d.Ontology.Digest != f.digest || d.Ontology.Project == nil || d.Ontology.Project.ID != blockerProject {
			t.Fatalf("notice did not retain the installed blocker-project catalog: %+v", d.Ontology)
		}
		equalJSON(t, d.Before, model.ConditionDecision{Status: "b02-open", Category: "unstarted"})
		equalJSON(t, d.After, model.ConditionDecision{Status: c.status, Category: string(c.category), Outcome: string(c.outcome), Terminal: true})
	}
}

func assertEvents(t *testing.T, events []model.Event, set model.Changeset, c categoryCase) {
	t.Helper()
	if len(events) != len(set.Events) {
		t.Fatalf("public events=%d persisted plan=%d", len(events), len(set.Events))
	}
	for i, item := range events {
		e := item.Durable
		d := set.Events[i]
		if item.Category != model.EventDurable || e == nil || e.Revision != set.Revision || e.Incarnation != set.Incarnation || e.Index != d.Index || e.Type != d.Type || e.Resource != "bead/"+d.Resource.Ref.ID || e.Derived || e.Cursor == "" {
			t.Fatalf("public event lost its durable descriptor binding: %+v", item)
		}
		equalJSON(t, e.Details, d.Details)
		equalJSON(t, e.Cause, d.Provenance.Cause)
		equalJSON(t, e.Principal, set.Principal)
		equalJSON(t, e.SetBy, d.Provenance.SetBy)
		equalJSON(t, e.Chain, d.Provenance.Chain)
		if i == 0 {
			equalJSON(t, e.Changed, set.Changes[0].ChangedPaths)
			if e.Before["fields"].(map[string]any)["status"] != "b02-open" || e.After["fields"].(map[string]any)["status"] != c.status {
				t.Fatalf("public lifecycle history images do not match the actual slot: %+v", e)
			}
		} else if e.Before != nil || e.After != nil || e.Changed == nil || len(e.Changed) != 0 {
			t.Fatalf("condition notice invented a dependent mutation: %+v", e)
		}
	}
}

func TestB02PublicCategoriesPersistNoticesAndReopen(t *testing.T) {
	for _, c := range categories {
		t.Run(c.name, func(t *testing.T) {
			eachEngine(t, func(t *testing.T, e engine) {
				runCategory(t, e, c)
			})
		})
	}
}

func runCategory(t *testing.T, e engine, c categoryCase) {
	t.Helper()
	f := newFixture(t, e)
	deferred := "frozen"
	if c.category == ontology.Backlog {
		deferred = c.status
	}
	f.install(t, blockerProject, projectCatalog(false, deferred))
	f.install(t, dependentProject, projectCatalog(true, deferred))
	g := f.graph(t)
	f.ready(t, blockerProject, g.blocker.ID)
	f.ready(t, dependentProject)
	before := f.retained(t, g.all())
	baseline := f.setupBaseline(t)

	// The same public transition with a stale row token must leave no record,
	// version, changeset, event, head movement, or idempotency receipt.
	stale := f.writeParams(blockerProject, map[string]any{
		"id": g.blocker, "expect": []model.Precondition{model.PreconditionByRevision(g.blocker, model.Natural64(1))},
	})
	assertRefusalUnchanged(t, f, g, c.method, stale, "precondition_failed")

	request := f.writeParams(blockerProject, map[string]any{
		"id":     g.blocker,
		"expect": []model.Precondition{model.PreconditionByRevision(g.blocker, before[g.blocker.ID].Record.Revision)},
	})
	first := f.call(t, c.method, request)
	var commit model.Commit
	decodeResult(t, first, &commit)
	if first.Meta.Replayed || len(commit.Results) != 1 || !sameRef(commit.Results[0].Ref, g.blocker) {
		t.Fatalf("first public lifecycle call did not commit its blocker once: %s", first.Result)
	}
	set := f.changeset(t, commit.Revision)
	assertPlan(t, f, c, g, before, set, request)
	f.readyAfter(t, c, g)
	var get struct{ Bead struct{ Status string } }
	f.result(t, "bead.get", map[string]any{"id": g.blocker}, &get)
	if get.Bead.Status != c.status {
		t.Fatalf("public slot status=%q want=%q", get.Bead.Status, c.status)
	}
	stableRefs := append([]model.Ref{sourceRef}, g.dependents()...)
	stable := map[string]retained{}
	for _, ref := range stableRefs {
		stable[ref.ID] = before[ref.ID]
	}
	equalJSON(t, f.retained(t, stableRefs), stable)
	all := f.events(t, baseline.Cursor, 100)
	assertEvents(t, all.Events, set, c)
	entry := f.receipt(t, c.method, request)
	if entry == nil || entry.ResponsePending {
		t.Fatalf("public lifecycle receipt is absent or incomplete: %+v", entry)
	}
	head := f.meta(t)
	state := f.retained(t, g.all())
	log := f.log(t)
	payloads := f.engine.payloadCounts(t, f.ctx)

	// Persisted pagination starts with the lifecycle event. Shut down every
	// service/store handle before consuming the first conditional notice.
	firstPage := f.events(t, baseline.Cursor, 1)
	if len(firstPage.Events) != 1 {
		t.Fatalf("one-event public page=%+v", firstPage)
	}
	equalJSON(t, firstPage.Events[0], all.Events[0])
	f.reopen(t)
	equalJSON(t, f.meta(t), head)
	equalJSON(t, f.retained(t, g.all()), state)
	equalJSON(t, f.log(t), log)
	equalJSON(t, f.engine.payloadCounts(t, f.ctx), payloads)
	equalJSON(t, f.changeset(t, commit.Revision), set)
	equalJSON(t, f.receipt(t, c.method, request), entry)
	f.readyAfter(t, c, g)

	resumed := append([]model.Event(nil), firstPage.Events...)
	cursor := firstPage.Cursor
	for len(resumed) < len(all.Events) {
		page := f.events(t, cursor, 1)
		if len(page.Events) != 1 || page.Cursor == cursor {
			t.Fatalf("persisted page failed to advance after reopen: %+v", page)
		}
		resumed = append(resumed, page.Events...)
		cursor = page.Cursor
	}
	equalJSON(t, resumed, all.Events)
	if tail := f.events(t, cursor, 1); len(tail.Events) != 0 {
		t.Fatalf("public cursor duplicated or invented a notice: %+v", tail.Events)
	}
	replay := f.call(t, c.method, request)
	var replayCommit model.Commit
	decodeResult(t, replay, &replayCommit)
	if !replay.Meta.Replayed {
		t.Fatal("same public request did not replay after SQL reopen")
	}
	equalJSON(t, replayCommit, commit)
	if string(replay.Result) != string(first.Result) {
		t.Fatalf("public replay changed original response bytes: first=%s replay=%s", first.Result, replay.Result)
	}
	equalJSON(t, f.meta(t), head)
	equalJSON(t, f.retained(t, g.all()), state)
	equalJSON(t, f.log(t), log)
	equalJSON(t, f.engine.payloadCounts(t, f.ctx), payloads)
	equalJSON(t, f.receipt(t, c.method, request), entry)

	// A second physical store-handle reopen also retains the completed page
	// cursor and original receipt; there is no process-local notice cache.
	f.reopen(t)
	equalJSON(t, f.events(t, baseline.Cursor, 100), all)
	equalJSON(t, f.receipt(t, c.method, request), entry)
	equalJSON(t, f.log(t), log)
	equalJSON(t, f.engine.payloadCounts(t, f.ctx), payloads)
	f.readyAfter(t, c, g)

	// Reopen the actual blocker through its public slot. Every dependent is
	// blocked again without changing any dependent's retained bytes/history.
	reopened := f.write(t, "bead.reopen", blockerProject, map[string]any{"id": g.blocker})
	f.ready(t, blockerProject, g.blocker.ID)
	f.ready(t, dependentProject)
	equalJSON(t, f.retained(t, stableRefs), stable)
	reopenSet := f.changeset(t, reopened.Revision)
	if len(reopenSet.Events) != 1 || reopenSet.Events[0].Type != model.EventBeadReopened || reopenSet.Events[0].Details != nil {
		t.Fatalf("nonterminal reopen emitted a terminal condition notice: %+v", reopenSet)
	}
	t.Logf("B02 %s/%s: public ontology.plan/apply -> HTTP bead.create/link/%s -> Writer/tracked ontology -> persisted plan r%s (%d events) -> HTTP event.list; two SQL handle reopens, exact receipt replay, unchanged dependent histories, public blocker reopen/reblocking", e.name, c.name, c.method, commit.Revision, len(set.Events))
}

func assertRefusalUnchanged(t *testing.T, f *fixture, g graph, method string, request map[string]any, code model.ErrorCode) {
	t.Helper()
	meta := f.meta(t)
	rows := f.retained(t, g.all())
	log := f.log(t)
	payloads := f.engine.payloadCounts(t, f.ctx)
	page := f.events(t, "", 100)
	result := f.call(t, method, request)
	if result.Error == nil || result.Error.Data.Code != string(code) || len(result.Result) != 0 || result.Meta.Replayed {
		t.Fatalf("expected pure %s refusal, got %+v result=%s", code, result.Error, result.Result)
	}
	equalJSON(t, f.meta(t), meta)
	equalJSON(t, f.retained(t, g.all()), rows)
	equalJSON(t, f.log(t), log)
	equalJSON(t, f.engine.payloadCounts(t, f.ctx), payloads)
	equalJSON(t, f.events(t, "", 100), page)
	if receipt := f.receipt(t, method, request); receipt != nil {
		t.Fatalf("refused public transition retained a receipt: %+v", receipt)
	}
}
