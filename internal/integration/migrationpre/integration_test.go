package migrationpre_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/migrationpre"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

var migrationCaller = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type fixtureOptions struct {
	overlay bool
	reject  string
	block   bool
	mutate  bool
}

type fixture struct {
	store     *memory.Store
	lease     driver.ActiveLease
	core      *ontologyops.Service
	pre       *migrationpre.NativePre
	probe     *nativeProbe
	base      ontology.Document
	source    serviceSource
	sourceRef model.Ref
	bindingID string
}

// serviceSource keeps the fixture's source envelope in the test package. It
// mirrors the internal service type through the small aliases below to keep
// the composition assertions readable.
type serviceSource = struct {
	Base     ontology.Document
	Overlays map[string]ontology.Document
	Plugins  ontology.PluginCatalog
}

// The real source envelope is imported under this local helper only where it
// is seeded; the alias above is converted explicitly by sourceEnvelope.
func sourceEnvelope(s serviceSource) sourceBody {
	return sourceBody{Base: s.Base, Overlays: s.Overlays, Plugins: s.Plugins}
}

type sourceBody struct {
	Base     ontology.Document            `json:"base"`
	Overlays map[string]ontology.Document `json:"overlays,omitempty"`
	Plugins  ontology.PluginCatalog       `json:"plugins,omitempty"`
}

// nativeProbe is a real plugin callback, rather than a pre-filled planner
// verdict. It reads the pinned snapshot, records the effective project and can
// reject one concrete candidate image.
type nativeProbe struct {
	calls       atomic.Int64
	entered     chan struct{}
	exited      chan struct{}
	enteredOnce sync.Once
	projectsMu  sync.Mutex
	projects    []string
	rejectTitle string
	block       bool
	mutate      bool
}

func (p *nativeProbe) native() sdk.Native {
	return sdk.Native{
		Manifest: pluginapi.PluginManifest{
			ID:        "example.migration-pre",
			Alias:     "migrationpre",
			Version:   "1.0.0",
			PluginAPI: pluginapi.APIVersion,
			Runtime:   "native",
			Provides: []pluginapi.Provision{{
				Kind: "subscriber", Name: "validate", Phases: []pluginapi.Phase{pluginapi.PhaseValidate},
				Reads: []string{"project", "fields.*"}, MaxPointReads: 8, MaxQueries: 1,
			}},
		},
		Instance: "default",
		New: func(pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{Validators: map[string]pluginapi.Validator{
				"validate": sdk.ValidateFunc(p.validate),
			}}, nil
		},
	}
}

func (p *nativeProbe) validate(ctx context.Context, change pluginapi.ProposedChange, reader pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
	p.calls.Add(1)
	if p.block {
		p.enteredOnce.Do(func() { close(p.entered) })
		defer close(p.exited)
		<-ctx.Done()
		return pluginapi.Verdict{}, ctx.Err()
	}
	if len(change.Changes) != 1 {
		return pluginapi.Verdict{}, fmt.Errorf("migration callback expected one row change")
	}
	var after struct {
		Project string         `json:"project"`
		Fields  map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(change.Changes[0].After, &after); err != nil {
		return pluginapi.Verdict{}, err
	}
	p.projectsMu.Lock()
	p.projects = append(p.projects, after.Project)
	p.projectsMu.Unlock()
	refs, err := reader.Get(ctx, []pluginapi.Ref{change.Changes[0].Resource.Ref})
	if err != nil {
		return pluginapi.Verdict{}, fmt.Errorf("pinned callback read: %w", err)
	}
	if len(refs) != 1 || refs[0].Revision == "" {
		return pluginapi.Verdict{}, fmt.Errorf("pinned callback read returned no actual row")
	}
	if p.mutate {
		// Both values are callback-owned copies. The retained planner report must
		// still contain the unmodified candidate image after this returns.
		change.Changes[0].After = json.RawMessage(`{"tampered":true}`)
		refs[0].Body = json.RawMessage(`{"tampered":true}`)
	}
	if p.rejectTitle != "" && after.Fields["title"] == p.rejectTitle {
		return pluginapi.Verdict{Allow: false, Violations: []pluginapi.Violation{{
			Guard: "ontology_guard_failed", Message: "native migration validator rejected this row",
			Details: map[string]any{"field": "fields.title"},
		}}}, nil
	}
	return pluginapi.Verdict{Allow: true}, nil
}

func newFixture(t *testing.T, options fixtureOptions) *fixture {
	t.Helper()
	store, err := memory.NewWithOptions(memory.Options{StoreID: "migration-pre", Incarnation: "migration-pre"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(context.Background(), "migration-pre", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	base := ontology.DefaultDocument()
	source := serviceSource{Base: base}
	if options.overlay {
		overlay := overlayDocument(base, "")
		source.Overlays = map[string]ontology.Document{"overlay-project": overlay}
	}
	sourceRef := model.Ref{ID: "ontology/migration-pre"}
	body, err := json.Marshal(sourceEnvelope(source))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(context.Background(), store, lease, driver.ChangesetHeader{Principal: migrationCaller, Time: time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	probe := &nativeProbe{entered: make(chan struct{}), exited: make(chan struct{}), rejectTitle: options.reject, block: options.block, mutate: options.mutate}
	hostBinding := pluginhost.Binding{ID: "migration-validator", Alias: "migrationpre", Instance: "default", Subscriber: "validate", Phase: pluginapi.PhaseValidate, Reads: []string{"project", "fields.*"}}
	host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{probe.native()}, Bindings: []pluginhost.Binding{hostBinding}})
	if err != nil {
		t.Fatal(err)
	}
	digest := bindingDigest(hostBinding)
	pre, err := migrationpre.NewNativePre(store, host, []ontologymigration.Binding{{ID: hostBinding.ID, Type: "issue", Digest: digest}})
	if err != nil {
		t.Fatal(err)
	}
	core, err := ontologyops.New(store, lease, ontologyops.Config{Source: staticSource(sourceRef), ValidatingPre: pre})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{store: store, lease: lease, core: core, pre: pre, probe: probe, base: base, source: source, sourceRef: sourceRef, bindingID: hostBinding.ID}
	t.Cleanup(func() {
		_ = pre.Close()
		_ = lease.Release(context.Background())
		_ = store.Close()
	})
	return f
}

func staticSource(ref model.Ref) func(string) (model.Ref, error) {
	return func(string) (model.Ref, error) { return ref, nil }
}

func bindingDigest(binding pluginhost.Binding) string {
	raw, _ := json.Marshal(binding)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (f *fixture) seed(t *testing.T, changes ...conformance.FixtureChange) {
	t.Helper()
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: migrationCaller, Time: time.Date(2026, 10, 29, 15, 1, 0, 0, time.UTC)}, changes); err != nil {
		t.Fatal(err)
	}
}

func bead(t *testing.T, id, project, title string) conformance.FixtureChange {
	t.Helper()
	body, err := json.Marshal(pipeline.Document{ID: id, Project: project, Type: "issue", Fields: map[string]any{"status": "todo", "title": title}})
	if err != nil {
		t.Fatal(err)
	}
	return conformance.FixtureChange{Ref: model.Ref{ID: id}, Kind: "bead", Op: model.OpPatch, Body: body}
}

func beadWithFields(t *testing.T, id, project string, fields map[string]any) conformance.FixtureChange {
	t.Helper()
	copy := map[string]any{}
	for key, value := range fields {
		copy[key] = value
	}
	body, err := json.Marshal(pipeline.Document{ID: id, Project: project, Type: "issue", Fields: copy})
	if err != nil {
		t.Fatal(err)
	}
	return conformance.FixtureChange{Ref: model.Ref{ID: id}, Kind: "bead", Op: model.OpPatch, Body: body}
}

func (f *fixture) plan(t *testing.T, project string, candidate ontology.Document) ontologyops.PlanResult {
	t.Helper()
	raw, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.core.Plan(context.Background(), migrationCaller, ontologyops.PlanRequest{Project: project, Document: raw})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func candidateField(t *testing.T, base ontology.Document, name string, field ontology.Field) ontology.Document {
	t.Helper()
	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	copy, err := ontology.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	typ := copy.Types["issue"]
	if typ.Fields == nil {
		typ.Fields = map[string]ontology.Field{}
	}
	typ.Fields[name] = field
	copy.Types["issue"] = typ
	return copy
}

func overlayDocument(base ontology.Document, marker string) ontology.Document {
	typ := base.Types["issue"]
	typ.Fields = cloneFields(typ.Fields)
	typ.Fields["overlay_marker"] = ontology.Field{Type: "string"}
	if marker != "" {
		typ.Fields["migration_marker"] = ontology.Field{Type: "string", Default: marker}
	}
	return ontology.Document{Types: map[string]ontology.Type{"issue": typ}}
}

func cloneFields(fields map[string]ontology.Field) map[string]ontology.Field {
	copy := map[string]ontology.Field{}
	for name, field := range fields {
		copy[name] = field
	}
	return copy
}

func refusal(report ontologymigration.Report, code string) bool {
	for _, item := range report.Refusals {
		if string(item.Code) == code {
			return true
		}
	}
	return false
}

func requirePlanStale(t *testing.T, err error) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "ontology_plan_stale" {
		t.Fatalf("error=%v, want ontology_plan_stale", err)
	}
}

func (p *nativeProbe) seenProjects() []string {
	p.projectsMu.Lock()
	defer p.projectsMu.Unlock()
	return append([]string(nil), p.projects...)
}

func TestConnectedServicePlanRunsActualNativePreAgainstOverlayRows(t *testing.T) {
	f := newFixture(t, fixtureOptions{overlay: true})
	f.seed(t, bead(t, "base-row", "base-project", "base"), beadWithFields(t, "overlay-row", "overlay-project", map[string]any{"status": "todo", "title": "overlay", "overlay_marker": "current"}))
	candidate := overlayDocument(f.base, "overlay")
	result := f.plan(t, "overlay-project", candidate)
	if !result.Changes.Ready || result.Changes.Migration.ObservedRowCount != 2 || result.Changes.Migration.Affected != 2 || f.probe.calls.Load() != 2 {
		t.Fatalf("connected overlay plan=%+v calls=%d", result, f.probe.calls.Load())
	}
	rows := result.Changes.Migration.Rows
	if len(rows) != 2 || rows[1].After.State.Fields["migration_marker"] != "overlay" {
		// Rows are sorted by project and ID; locate the affected overlay row if
		// another fixture ordering changes.
		found := false
		for _, row := range rows {
			if row.After.State.Project == "overlay-project" && row.After.State.Fields["migration_marker"] == "overlay" {
				found = true
			}
		}
		if !found {
			t.Fatalf("overlay candidate image was not planned: %+v", rows)
		}
	}
	projects := f.probe.seenProjects()
	if len(projects) != 2 || projects[0] != "base-project" || projects[1] != "overlay-project" {
		t.Fatalf("native callback scope=%v, want complete row enumeration", projects)
	}
	if result.Changes.Migration.PreInventory.Bindings[0].Project != "" || result.Changes.Migration.PreInventory.Bindings[0].Type != "issue" {
		t.Fatalf("native global binding was narrowed or lost: %+v", result.Changes.Migration.PreInventory)
	}
}

func TestNativeRejectionRequiresExactDispositionAndChangesPlanDigest(t *testing.T) {
	f := newFixture(t, fixtureOptions{reject: "reject"})
	f.seed(t, bead(t, "reject-row", "p", "reject"))
	candidate := candidateField(t, f.base, "migration_marker", ontology.Field{Type: "string", Default: "planned"})
	result := f.plan(t, "", candidate)
	if result.Changes.Ready || len(result.Changes.Migration.Rows) != 1 || len(result.Changes.Migration.Rows[0].Findings) != 1 || result.Changes.Migration.Rows[0].PreRuns[0].Binding != f.bindingID {
		t.Fatalf("native rejection evidence missing: %+v", result.Changes.Migration)
	}
	finding := result.Changes.Migration.Rows[0].Findings[0]
	if finding.Subscriber != f.bindingID || finding.Violation.Code != "ontology_guard_failed" || finding.Disposition != "" {
		t.Fatalf("unexpected native finding: %+v", finding)
	}
	current, problems := ontology.Compile(f.source.Base, f.source.Overlays, f.source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	candidateSnapshot, problems := ontology.Compile(candidate, f.source.Overlays, f.source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	source := reportRows{inventory: result.Changes.Migration.Inventory, rows: []ontologymigration.Row{result.Changes.Migration.Rows[0].Before}}
	directive := ontologymigration.Directive{ID: "retain-native-rejection", Kind: ontologymigration.Grandfather, Target: ontologymigration.Target{Project: "p", Type: "issue", ID: "reject-row"}, Violation: finding.ID, Reason: "explicit migration exception"}
	plan, err := ontologymigration.Build(context.Background(), ontologymigration.Request{
		Current:    ontologymigration.Version{Revision: result.Changes.Migration.Current.Revision, Snapshot: current},
		Candidate:  ontologymigration.Version{Revision: result.Changes.Migration.Candidate.Revision, Snapshot: candidateSnapshot},
		Directives: []ontologymigration.Directive{directive},
	}, source, f.pre)
	if err != nil || !plan.Ready() || plan.Report().Grandfathered != 1 || plan.Report().Rows[0].Findings[0].Disposition != directive.ID || plan.Digest() == result.PlanDigest {
		t.Fatalf("exact native disposition plan=%+v err=%v", plan.Report(), err)
	}
	wrong := directive
	wrong.ID = "wrong-finding"
	wrong.Violation = strings.Repeat("0", 64)
	wrongPlan, err := ontologymigration.Build(context.Background(), ontologymigration.Request{
		Current:    ontologymigration.Version{Revision: result.Changes.Migration.Current.Revision, Snapshot: current},
		Candidate:  ontologymigration.Version{Revision: result.Changes.Migration.Candidate.Revision, Snapshot: candidateSnapshot},
		Directives: []ontologymigration.Directive{wrong},
	}, source, f.pre)
	if err != nil || wrongPlan.Ready() || !refusal(wrongPlan.Report(), "invalid_params") {
		t.Fatalf("wrong disposition was accepted: %+v err=%v", wrongPlan.Report(), err)
	}
}

func TestConnectedPlanDigestSourceRowAndBindingInvalidation(t *testing.T) {
	cases := []string{"wrong digest", "source revision", "row image", "binding manifest"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, fixtureOptions{mutate: true})
			f.seed(t, bead(t, "guard-row", "p", "stable"))
			candidate := candidateField(t, f.base, "migration_marker", ontology.Field{Type: "string", Default: "planned"})
			result := f.plan(t, "", candidate)
			if !result.Changes.Ready || result.PlanDigest == "" {
				t.Fatalf("ready plan=%+v", result)
			}
			if name == "wrong digest" {
				_, err := f.core.Planned(context.Background(), migrationCaller, strings.Repeat("0", 64))
				requirePlanStale(t, err)
				return
			}
			if _, err := f.core.Planned(context.Background(), migrationCaller, result.PlanDigest); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "source revision":
				body, _ := json.Marshal(sourceEnvelope(f.source))
				f.seed(t, conformance.FixtureChange{Ref: f.sourceRef, Kind: "ontology", Op: model.OpPatch, Body: body})
			case "row image":
				f.seed(t, bead(t, "guard-row", "p", "changed"))
			case "binding manifest":
				if err := f.pre.SetBindingDigest(f.bindingID, strings.Repeat("b", 64)); err != nil {
					t.Fatal(err)
				}
			}
			_, err := f.core.Planned(context.Background(), migrationCaller, result.PlanDigest)
			requirePlanStale(t, err)
		})
	}
}

func TestNativeCallbackCopiesStayDetachedFromServicePlan(t *testing.T) {
	f := newFixture(t, fixtureOptions{mutate: true})
	f.seed(t, bead(t, "copy-row", "p", "stable"))
	candidate := candidateField(t, f.base, "migration_marker", ontology.Field{Type: "string", Default: "planned"})
	result := f.plan(t, "", candidate)
	if !result.Changes.Ready || f.probe.calls.Load() != 1 {
		t.Fatalf("detached plan did not run native callback: %+v", result)
	}
	result.Changes.Migration.Rows[0].After.State.Fields["migration_marker"] = "caller-edit"
	result.Changes.Document.Base.Types["issue"].Fields["migration_marker"] = ontology.Field{Type: "string", Default: "caller-edit"}
	retained, err := f.core.Planned(context.Background(), migrationCaller, result.PlanDigest)
	if err != nil {
		t.Fatal(err)
	}
	if retained.Migration.Rows[0].After.State.Fields["migration_marker"] != "planned" || retained.Document.Base.Types["issue"].Fields["migration_marker"].Default != "planned" {
		t.Fatalf("callback/caller edits escaped detached retention: %+v", retained)
	}
}

func TestExactFiveThousandAffectedRowsRunsNativePreAndFiveThousandOneRefusesBeforeCallback(t *testing.T) {
	for _, count := range []int{5000, 5001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newFixture(t, fixtureOptions{})
			changes := make([]conformance.FixtureChange, 0, count)
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("row-%05d", i)
				changes = append(changes, bead(t, id, "p", id))
			}
			f.seed(t, changes...)
			candidate := candidateField(t, f.base, "migration_marker", ontology.Field{Type: "string", Default: "planned"})
			result := f.plan(t, "", candidate)
			if result.Changes.Migration.Affected != count || result.Changes.Migration.ObservedRowCount != count {
				t.Fatalf("row boundary report affected=%d observed=%d", result.Changes.Migration.Affected, result.Changes.Migration.ObservedRowCount)
			}
			if count == 5000 {
				if !result.Changes.Ready || f.probe.calls.Load() != int64(count) || refusal(result.Changes.Migration, "ontology_migration_too_large") {
					t.Fatalf("5000 rows did not execute native pre: ready=%t calls=%d report=%+v", result.Changes.Ready, f.probe.calls.Load(), result.Changes.Migration)
				}
			} else if result.Changes.Ready || f.probe.calls.Load() != 0 || !refusal(result.Changes.Migration, "ontology_migration_too_large") {
				t.Fatalf("5001 rows did not refuse before callbacks: ready=%t calls=%d report=%+v", result.Changes.Ready, f.probe.calls.Load(), result.Changes.Migration)
			}
		})
	}
}

func TestNativePreCancellationReturnsNoPlan(t *testing.T) {
	f := newFixture(t, fixtureOptions{block: true})
	f.seed(t, bead(t, "cancel-row", "p", "cancel"))
	candidate := candidateField(t, f.base, "migration_marker", ontology.Field{Type: "string", Default: "planned"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct {
		result ontologyops.PlanResult
		err    error
	}, 1)
	go func() {
		data, err := json.Marshal(candidate)
		if err != nil {
			done <- struct {
				result ontologyops.PlanResult
				err    error
			}{err: err}
			return
		}
		result, err := f.core.Plan(ctx, migrationCaller, ontologyops.PlanRequest{Document: data})
		done <- struct {
			result ontologyops.PlanResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-f.probe.entered:
		cancel()
	case <-time.After(3 * time.Second):
		t.Fatal("native callback did not start")
	}
	select {
	case outcome := <-done:
		var typed *model.Error
		if outcome.err == nil || !errors.As(outcome.err, &typed) || typed.Data.Code != "busy" || outcome.result.PlanDigest != "" {
			t.Fatalf("cancellation outcome=%+v", outcome)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled migration plan did not return")
	}
	select {
	case <-f.probe.exited:
	case <-time.After(3 * time.Second):
		t.Fatal("native callback did not observe cancellation")
	}
}

func TestPluginOwnedFieldDirectiveRefusesInConnectedPlannerBoundary(t *testing.T) {
	current := ontology.DefaultDocument()
	current.Types["issue"].Fields["owned"] = ontology.Field{Type: "string", Owner: "plugin/owner"}
	candidate := ontology.DefaultDocument()
	candidate.Types["issue"].Fields["owned"] = ontology.Field{Type: "string"}
	currentSnapshot, problems := ontology.Compile(current, nil, ontology.PluginCatalog{"plugin/owner": true})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	candidateSnapshot, problems := ontology.Compile(candidate, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	row := ontologymigration.Row{Revision: "2", State: ontology.BeadState{ID: "owned-row", Project: "p", Type: "issue", Status: "todo", Fields: map[string]any{"title": "kept", "owned": "old"}}}
	rows := reportRows{inventory: ontologymigration.Inventory{DataRevision: "2", Ontology: ontologymigration.Reference{Revision: "1", Digest: currentSnapshot.Digest()}, Count: "1"}, rows: []ontologymigration.Row{row}}
	rows.inventory.RowsDigest, _ = ontologymigration.RowsDigest(rows.rows)
	plan, err := ontologymigration.Build(context.Background(), ontologymigration.Request{
		Current:    ontologymigration.Version{Revision: "1", Snapshot: currentSnapshot},
		Candidate:  ontologymigration.Version{Revision: "2", Snapshot: candidateSnapshot},
		Directives: []ontologymigration.Directive{{ID: "owned-set", Kind: ontologymigration.SetField, Target: ontologymigration.Target{Project: "p", Type: "issue"}, Field: "owned", Value: "new"}},
	}, rows, ontologyops.NoValidatingPre())
	if err != nil || plan.Ready() || !refusal(plan.Report(), "field_owned") || plan.Report().Rows[0].After.State.Fields["owned"] != "old" {
		t.Fatalf("plugin-owned directive was accepted or altered: %+v err=%v", plan.Report(), err)
	}
}

type reportRows struct {
	inventory ontologymigration.Inventory
	rows      []ontologymigration.Row
}

func (s reportRows) Inventory(context.Context) (ontologymigration.Inventory, error) {
	return s.inventory, nil
}
func (s reportRows) Scan(ctx context.Context, _ ontologymigration.Inventory, visit func(ontologymigration.Row) error) error {
	for _, row := range s.rows {
		if err := visit(row); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (s reportRows) Observe(_ context.Context, _ ontologymigration.Inventory, change ontologymigration.Change) (ontologymigration.Observation, error) {
	digest, err := ontologymigration.RowDigest(change.After)
	falseValue := false
	trueValue := true
	return ontologymigration.Observation{Candidate: change.Candidate, RowDigest: digest, AuthzInstalled: &falseValue, Facts: ontology.GuardFacts{AuthzInstalled: false, NoOpenBlockers: &trueValue, NoOpenChildren: &trueValue}}, err
}
