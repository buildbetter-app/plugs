package b03catalog_test

import (
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
)

func withRetirableDefinitions() ontology.Document {
	doc := catalogDocument()
	doc.Workflows["retired-workflow"] = doc.Workflows["delivery"]
	doc.Types["retired-type"] = ontology.Type{Name: "Retirable unused type", Workflow: "delivery"}
	doc.GateKinds = map[string]ontology.GateKind{"retired-gate": {Resolver: "builtin.manual"}}
	return doc
}

func archiveDefinitions(doc *ontology.Document) {
	status := doc.Statuses[retired]
	status.Archived = true
	doc.Statuses[retired] = status
	workflow := doc.Workflows["retired-workflow"]
	workflow.Archived = true
	doc.Workflows["retired-workflow"] = workflow
	typ := doc.Types["retired-type"]
	typ.Archived = true
	doc.Types["retired-type"] = typ
	gate := doc.GateKinds["retired-gate"]
	gate.Archived = true
	doc.GateKinds["retired-gate"] = gate
}

func copyDocument(t *testing.T, doc ontology.Document) ontology.Document {
	t.Helper()
	copy, err := ontology.Decode(marshal(t, doc))
	if err != nil {
		t.Fatal(err)
	}
	return copy
}

type definitionCase struct {
	path   string
	drop   func(*ontology.Document)
	revive func(*ontology.Document)
}

var definitionCases = []definitionCase{
	{"statuses." + retired,
		func(d *ontology.Document) { delete(d.Statuses, retired) },
		func(d *ontology.Document) {
			d.Statuses[retired] = ontology.Status{Name: "Replacement must be refused", Category: ontology.Unstarted}
		}},
	{"workflows.retired-workflow",
		func(d *ontology.Document) { delete(d.Workflows, "retired-workflow") },
		func(d *ontology.Document) { d.Workflows["retired-workflow"] = catalogDocument().Workflows["delivery"] }},
	{"types.retired-type",
		func(d *ontology.Document) { delete(d.Types, "retired-type") },
		func(d *ontology.Document) {
			d.Types["retired-type"] = ontology.Type{Name: "Replacement must be refused", Workflow: "delivery"}
		}},
	{"gate_kinds.retired-gate",
		func(d *ontology.Document) { delete(d.GateKinds, "retired-gate") },
		func(d *ontology.Document) {
			d.GateKinds["retired-gate"] = ontology.GateKind{Resolver: "builtin.manual"}
		}},
}

func (f *fixture) refuseContinuity(t *testing.T, project string, doc ontology.Document, path, reason string) {
	t.Helper()
	out := f.refuse(t, "ontology.plan", map[string]any{"project": project, "document": doc}, "invalid_params")
	if out.Error == nil || !strings.Contains(out.Error.Data.Hint, path) || !strings.Contains(out.Error.Data.Hint, reason) {
		t.Fatalf("plan did not reach the installed-key continuity boundary: %+v", out.Error)
	}
}

func TestInstalledBaseKeysArchiveWithoutDeletionRenameOrResurrection(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		doc := withRetirableDefinitions()
		f.install(t, "", doc)
		// An otherwise valid active key cannot be deleted or renamed either:
		// old elements must remain represented as archived definitions.
		removed := copyDocument(t, doc)
		delete(removed.Statuses, retired)
		f.refuseContinuity(t, "", removed, "base.statuses."+retired, "never removed")
		olderPlan := f.plan(t, "", doc)
		archiveDefinitions(&doc)
		f.install(t, "", doc)
		var installed ontologyops.Result
		f.result(t, "ontology.get", map[string]any{"project": "base-project"}, &installed)
		status := installed.Document.Statuses[retired]
		if !status.Archived || status.Category != ontology.Completed || status.Outcome != ontology.Failure {
			t.Fatalf("archival changed the retained status meaning: %+v", status)
		}
		f.refuse(t, "ontology.apply", f.applyParams(olderPlan), "ontology_plan_stale")
		for _, tc := range definitionCases {
			t.Run(tc.path, func(t *testing.T) {
				removed := copyDocument(t, doc)
				tc.drop(&removed)
				f.refuseContinuity(t, "", removed, "base."+tc.path, "never removed")
				reused := copyDocument(t, doc)
				tc.revive(&reused)
				f.refuseContinuity(t, "", reused, "base."+tc.path, "cannot be reused")
			})
		}
		renamed := copyDocument(t, doc)
		delete(renamed.Statuses, retired)
		renamed.Statuses["renamed-identity"] = ontology.Status{Name: "Same display name, different key", Category: ontology.Unstarted}
		f.refuseContinuity(t, "", renamed, "base.statuses."+retired, "never removed")

		// A display-name edit retains the archived key and its category/outcome;
		// the continuity guard must not freeze legitimate free-text names.
		status.Name = "Archived display name may be edited"
		doc.Statuses[retired] = status
		f.install(t, "", doc)
		if e.name != "memory" {
			before := f.snapshot(t)
			f.reopen(t)
			equal(t, "archived identities and receipts survive durable reopen", before, f.snapshot(t))
			reused := copyDocument(t, doc)
			definitionCases[0].revive(&reused)
			f.refuseContinuity(t, "", reused, "base.statuses."+retired, "cannot be reused")
		}
	})
}

func TestProjectArchivedKeysCannotBeReactivatedByOverlayRemovalOrReplacement(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.install(t, "", withRetirableDefinitions())
		overlay := ontology.Document{
			Statuses:  map[string]ontology.Status{retired: {Archived: true}},
			Workflows: map[string]ontology.Workflow{"retired-workflow": {Archived: true}},
			Types:     map[string]ontology.Type{"retired-type": {Archived: true}},
			GateKinds: map[string]ontology.GateKind{"retired-gate": {Archived: true}},
		}
		f.install(t, "west", overlay)
		var west, base ontologyops.Result
		f.result(t, "ontology.get", map[string]any{"project": "west"}, &west)
		f.result(t, "ontology.get", map[string]any{"project": "base-project"}, &base)
		want := base.Document.Statuses[retired]
		want.Archived = true
		equal(t, "minimal project tombstone retains the base status definition whole", want, west.Document.Statuses[retired])
		if base.Document.Statuses[retired].Archived {
			t.Fatal("project archival leaked into the server base")
		}
		for _, tc := range definitionCases {
			t.Run(tc.path, func(t *testing.T) {
				removed := copyDocument(t, overlay)
				tc.drop(&removed)
				f.refuseContinuity(t, "west", removed, "projects.west."+tc.path, "cannot be reused")
				replaced := copyDocument(t, overlay)
				tc.revive(&replaced)
				f.refuseContinuity(t, "west", replaced, "projects.west."+tc.path, "cannot be reused")
			})
		}
		f.refuseContinuity(t, "west", ontology.Document{}, "projects.west.statuses."+retired, "cannot be reused")
		unknownArchive := copyDocument(t, overlay)
		unknownArchive.Statuses["not-in-base"] = ontology.Status{Archived: true}
		f.refuse(t, "ontology.plan", map[string]any{"project": "west", "document": unknownArchive}, "invalid_params")

		// An unrelated project's free definition is still admitted. Its removal
		// must not discard the catalog identity even when no bead uses it.
		east := ontology.Document{Statuses: map[string]ontology.Status{"east-only": {Name: "East local key", Category: ontology.Backlog}}}
		f.install(t, "east", east)
		f.refuseContinuity(t, "east", ontology.Document{}, "projects.east.statuses.east-only", "never removed")
		if e.name != "memory" {
			before := f.snapshot(t)
			f.reopen(t)
			equal(t, "project tombstones persist across close/open", before, f.snapshot(t))
			f.refuseContinuity(t, "west", ontology.Document{}, "projects.west.statuses."+retired, "cannot be reused")
		}
	})
}
