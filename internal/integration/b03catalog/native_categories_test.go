package b03catalog_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

const (
	nativeActive = "awaiting_review"
	nativeWIP    = "qa_testing"
	nativeDone   = "archived"
	nativeFrozen = "on_hold"
	nativeAgain  = "ready_again"
	nativeType   = "delivery-native"
)

// These are public catalog inputs. They exercise the retained native wire
// categories, without claiming a native archive, importer or producer run.
func nativeCategoryDocument() ontology.Document {
	doc := ontology.DefaultDocument()
	for key, status := range map[string]ontology.Status{
		nativeActive: {Name: "Awaiting review", Category: ontology.Active},
		nativeWIP:    {Name: "QA testing", Category: ontology.WIP},
		nativeDone:   {Name: "Archived delivery", Category: ontology.Done, Outcome: ontology.Success},
		nativeFrozen: {Name: "On hold", Category: ontology.Frozen},
		nativeAgain:  {Name: "Ready again", Category: ontology.Active},
	} {
		doc.Statuses[key] = status
	}
	doc.Workflows[nativeType] = ontology.Workflow{
		Statuses: []string{nativeActive, nativeWIP, nativeDone, nativeFrozen, nativeAgain},
		Initial:  nativeActive,
		Slots: map[ontology.Slot]string{
			ontology.Claim: nativeWIP, ontology.Close: nativeDone,
			ontology.Reopen: nativeActive, ontology.Defer: nativeFrozen,
		},
		Transitions: []ontology.Rule{
			{From: []string{nativeActive, nativeAgain}, To: nativeWIP, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
			{To: nativeDone, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
			{To: nativeActive, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: nativeAgain, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: nativeFrozen, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		},
	}
	doc.Types[nativeType] = ontology.Type{
		Name: "Native delivery", Workflow: nativeType,
		Fields: map[string]ontology.Field{"title": {Type: "string"}},
	}
	return doc
}

type nativeCategoryImage struct {
	Phase    string            `json:"phase"`
	Status   string            `json:"status"`
	Category ontology.Category `json:"category"`
	Type     string            `json:"type"`
	Ready    bool              `json:"ready"`
	Started  bool              `json:"started"`
	Closed   bool              `json:"closed"`
}

type nativeCategoryTrace struct {
	Kind      string `json:"kind"`
	Test      string `json:"test"`
	Engine    string `json:"engine"`
	Complete  bool   `json:"complete"`
	Exchanges []struct {
		Request  string `json:"request"`
		Response string `json:"response"`
	} `json:"exchanges"`
	Checkpoints []struct {
		Image    nativeCategoryImage `json:"image"`
		Ref      model.Ref           `json:"ref"`
		Document pipeline.Document   `json:"document"`
	} `json:"checkpoints"`
}

func traceNativeCategories(t *testing.T, f *fixture) *nativeCategoryTrace {
	t.Helper()
	trace := &nativeCategoryTrace{Kind: "PUBLIC_CATALOG_FIXTURE_NOT_NATIVE_ARCHIVE", Test: t.Name(), Engine: f.engine.name}
	f.traceRPC = func(request, response []byte) {
		trace.Exchanges = append(trace.Exchanges, struct {
			Request  string `json:"request"`
			Response string `json:"response"`
		}{string(request), string(response)})
	}
	t.Cleanup(func() { writeNativeCategoryEvidence(t, trace) })
	return trace
}

func writeNativeCategoryEvidence(t *testing.T, value any) {
	t.Helper()
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + ".public-catalog.json"
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func nativeCategoryCatalog(t *testing.T, f *fixture, project string, expected ontology.Document) {
	t.Helper()
	var got ontologyops.Result
	f.result(t, "ontology.get", map[string]any{"project": project}, &got)
	if got.Document == nil || got.Drifted {
		t.Fatalf("installed native-category document is absent or drifted: %+v", got)
	}
	for _, key := range []string{nativeActive, nativeWIP, nativeDone, nativeFrozen, nativeAgain} {
		status, exists := got.Document.Statuses[key]
		if !exists || status != expected.Statuses[key] {
			t.Fatalf("native status key/category/name changed at %s: got=%+v want=%+v", key, status, expected.Statuses[key])
		}
	}
	equal(t, "custom native type preserved exactly", expected.Types[nativeType], got.Document.Types[nativeType])
	equal(t, "custom native workflow preserved exactly", expected.Workflows[nativeType], got.Document.Workflows[nativeType])
}

func observeNativeCategory(t *testing.T, f *fixture, trace *nativeCategoryTrace, ref model.Ref, phase, project, status string, category ontology.Category, ready, started, closed bool) pipeline.Document {
	t.Helper()
	f.status(t, ref, status)
	f.ready(t, project, ref, ready)
	doc := lifecycleDocument(t, f, ref)
	if doc.Type != nativeType || doc.Fields["status"] != status || doc.CreatedAt == nil || doc.CreatedAt.IsZero() ||
		(doc.StartedAt != nil) != started || (doc.ClosedAt != nil) != closed {
		t.Fatalf("%s lost exact type or lifecycle timestamp presence: %+v", phase, doc)
	}
	var got ontologyops.Result
	f.result(t, "ontology.get", map[string]any{"project": project}, &got)
	if got.Document == nil || got.Document.Statuses[status].Category != category {
		t.Fatalf("%s did not retain exact native category %s: %+v", phase, category, got)
	}
	image := nativeCategoryImage{Phase: phase, Status: doc.Fields["status"].(string), Category: got.Document.Statuses[status].Category, Type: doc.Type, Ready: ready, Started: started, Closed: closed}
	trace.Checkpoints = append(trace.Checkpoints, struct {
		Image    nativeCategoryImage `json:"image"`
		Ref      model.Ref           `json:"ref"`
		Document pipeline.Document   `json:"document"`
	}{image, ref, doc})
	return doc
}

func TestPublicNativeCategoryCatalogLifecycleAndSQLReopen(t *testing.T) {
	correspondence := map[string][]nativeCategoryImage{}
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		trace := traceNativeCategories(t, f)
		doc := nativeCategoryDocument()
		f.install(t, "", doc)
		nativeCategoryCatalog(t, f, "base-project", doc)
		ref, createRequest, createResponse := f.create(t, "base-project", nativeType)
		created := observeNativeCategory(t, f, trace, ref, "create", "base-project", nativeActive, ontology.Active, true, false, false)
		slot(f, t, "base-project", ref, "bead.claim", nativeWIP)
		claimed := observeNativeCategory(t, f, trace, ref, "claim", "base-project", nativeWIP, ontology.WIP, false, true, false)
		if !claimed.CreatedAt.Equal(*created.CreatedAt) {
			t.Fatal("native wip rewrote the creation instant")
		}
		slot(f, t, "base-project", ref, "bead.close", nativeDone)
		closed := observeNativeCategory(t, f, trace, ref, "close", "base-project", nativeDone, ontology.Done, false, true, true)
		f.result(t, "bead.close", f.params("bead.close", "base-project", map[string]any{"id": ref, "reason": "native done settlement"}), nil)
		repeated := observeNativeCategory(t, f, trace, ref, "repeat-close", "base-project", nativeDone, ontology.Done, false, true, true)
		if !repeated.ClosedAt.Equal(*closed.ClosedAt) || !repeated.StartedAt.Equal(*claimed.StartedAt) {
			t.Fatal("native done did not retain the first close and started instants")
		}
		f.refuse(t, "bead.claim", f.params("bead.claim", "base-project", map[string]any{"id": ref}), "invalid_params")
		slot(f, t, "base-project", ref, "bead.reopen", nativeActive)
		reopened := observeNativeCategory(t, f, trace, ref, "reopen", "base-project", nativeActive, ontology.Active, true, true, false)
		if !reopened.CreatedAt.Equal(*created.CreatedAt) || !reopened.StartedAt.Equal(*claimed.StartedAt) {
			t.Fatal("native reopen rewrote creation or started instant")
		}
		slot(f, t, "base-project", ref, "bead.defer", nativeFrozen)
		observeNativeCategory(t, f, trace, ref, "defer", "base-project", nativeFrozen, ontology.Frozen, false, true, false)
		f.refuse(t, "bead.claim", f.params("bead.claim", "base-project", map[string]any{"id": ref}), "invalid_params")
		f.refuse(t, "bead.update", f.params("bead.update", "base-project", map[string]any{"id": ref, "fields": map[string]any{"status": "unknown-native-status"}}), "ontology_use_slot_verb")
		slot(f, t, "base-project", ref, "bead.reopen", nativeActive)
		observeNativeCategory(t, f, trace, ref, "ready-again", "base-project", nativeActive, ontology.Active, true, true, false)
		// The same native-category state, histories, receipts and views must
		// survive actual SQL host reconstruction, without reseeding a catalog.
		if e.name != "memory" {
			before := f.snapshot(t)
			f.reopen(t)
			equal(t, "native category SQL reopen retains complete durable image", before, f.snapshot(t))
			nativeCategoryCatalog(t, f, "base-project", doc)
			f.status(t, ref, nativeActive)
			f.ready(t, "base-project", ref, true)
			replay := f.result(t, "bead.create", createRequest, nil)
			if !replay.Meta.Replayed || string(replay.Result) != string(createResponse.Result) {
				t.Fatal("native custom type create receipt changed across SQL reopen")
			}
			equal(t, "native receipt replay after SQL reopen is read only", before, f.snapshot(t))
		}
		for _, point := range trace.Checkpoints {
			if !point.Document.CreatedAt.Equal(*created.CreatedAt) || (point.Document.StartedAt != nil && !point.Document.StartedAt.Equal(*claimed.StartedAt)) {
				t.Fatalf("%s changed an immutable lifecycle instant", point.Image.Phase)
			}
			correspondence[e.name] = append(correspondence[e.name], point.Image)
		}
		trace.Complete = !t.Failed()
	})
	if len(correspondence) != len(allEngines) {
		t.Fatalf("missing actual public category replay engine: %+v", correspondence)
	}
	for _, engine := range []string{"sqlite", "postgres"} {
		equal(t, "native category named public correspondence "+engine, correspondence["memory"], correspondence[engine])
	}
	writeNativeCategoryEvidence(t, correspondence)
}

func TestPublicNativeCategoryOverlaysUseProjectCategoryAndKeepExactKeys(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		trace := traceNativeCategories(t, f)
		base := nativeCategoryDocument()
		f.install(t, "", base)
		expected := map[string]ontology.Document{"base-project": base}
		for _, tc := range []struct {
			project  string
			category ontology.Category
		}{{"west", ontology.Frozen}, {"east", ontology.WIP}} {
			overlay := nativeCategoryDocument()
			status := overlay.Statuses[nativeActive]
			status.Category = tc.category
			overlay.Statuses[nativeActive] = status
			workflow := overlay.Workflows[nativeType]
			workflow.Slots[ontology.Reopen] = nativeAgain
			overlay.Workflows[nativeType] = workflow
			f.install(t, tc.project, overlay)
			expected[tc.project] = overlay
		}
		refs := map[string]model.Ref{}
		for _, project := range projects {
			doc := expected[project]
			nativeCategoryCatalog(t, f, project, doc)
			ref, _, _ := f.create(t, project, nativeType)
			refs[project] = ref
			category := doc.Statuses[nativeActive].Category
			f.status(t, ref, nativeActive)
			f.ready(t, project, ref, project == "base-project")
			if project != "base-project" {
				// The same exact status key is active in base and non-ready in
				// each overlay. Its spelling cannot supply the claim decision.
				f.refuse(t, "bead.claim", f.params("bead.claim", project, map[string]any{"id": ref}), "invalid_params")
				slot(f, t, project, ref, "bead.reopen", nativeAgain)
				f.ready(t, project, ref, true)
			}
			t.Logf("exact-key project=%s status=%s category=%s initial_ready=%t", project, nativeActive, category, project == "base-project")
			slot(f, t, project, ref, "bead.claim", nativeWIP)
			f.ready(t, project, ref, false)
		}
		if e.name != "memory" {
			before := f.snapshot(t)
			f.reopen(t)
			equal(t, "native overlay SQL reopen retains complete durable image", before, f.snapshot(t))
		}
		for _, project := range projects {
			nativeCategoryCatalog(t, f, project, expected[project])
			f.status(t, refs[project], nativeWIP)
			f.ready(t, project, refs[project], false)
		}
		trace.Complete = !t.Failed()
	})
}

func TestPublicMalformedNativeCategoryCatalogRefusesWithoutMutation(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		trace := traceNativeCategories(t, f)
		f.install(t, "", nativeCategoryDocument())
		ref, _, _ := f.create(t, "base-project", nativeType)
		for _, tc := range []string{"unknown-category", "case-sensitive-category", "done-failure", "frozen-outcome", "claim-to-active", "missing-slot-status"} {
			t.Run(tc, func(t *testing.T) {
				doc := nativeCategoryDocument()
				status := doc.Statuses[nativeDone]
				switch tc {
				case "unknown-category":
					status.Category = "unknown"
				case "case-sensitive-category":
					status.Category = "DONE"
				case "done-failure":
					status.Outcome = ontology.Failure
				case "frozen-outcome":
					status.Category = ontology.Frozen
				case "claim-to-active":
					doc.Workflows[nativeType].Slots[ontology.Claim] = nativeActive
				case "missing-slot-status":
					doc.Workflows[nativeType].Slots[ontology.Close] = "unconfigured-native-done"
				}
				doc.Statuses[nativeDone] = status
				f.refuse(t, "ontology.plan", map[string]any{"project": "", "document": doc}, "invalid_params")
			})
		}
		// A valid write prefix must not escape the native catalog's public
		// status-slot boundary when the same batch tries to patch status.
		f.refuse(t, "tx.apply", f.params("tx.apply", "base-project", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": "must remain atomic"}}},
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"status": nativeDone}}},
		}}}), "ontology_use_slot_verb")
		nativeCategoryCatalog(t, f, "base-project", nativeCategoryDocument())
		f.status(t, ref, nativeActive)
		f.ready(t, "base-project", ref, true)
		trace.Complete = !t.Failed()
	})
}
