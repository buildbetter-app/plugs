package batchsql_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const b02InstalledRoot = "b02-installed-root"
const b02InstalledDependents = "b02-installed-dependents"

var b02InstalledSource = model.RefByID("ontology/b02-installed-categories", model.Natural64(0))

// These are installed Plug expectations. The public reference recorder's
// b02active/b02done/b02frozen literals remain separate observations; this test
// neither records a reference process nor claims imported-source qualification.
// Native done permits success only. Failure keeps the adopted completed
// category, rather than inventing a done/failure pairing.
func b02InstalledDocument() ontology.Document {
	return ontology.Document{
		Statuses: map[string]ontology.Status{
			"b02active":   {Name: "Active", Category: ontology.Active},
			"b02wip":      {Name: "Working", Category: ontology.WIP},
			"b02done":     {Name: "Done", Category: ontology.Done, Outcome: ontology.Success},
			"b02frozen":   {Name: "Frozen", Category: ontology.Frozen},
			"b02failed":   {Name: "Failed", Category: ontology.Completed, Outcome: ontology.Failure},
			"b02canceled": {Name: "Canceled", Category: ontology.Canceled},
		},
		Workflows: map[string]ontology.Workflow{"issue": {
			Statuses: []string{"b02active", "b02wip", "b02done", "b02frozen", "b02failed", "b02canceled"}, Initial: "b02active",
			Slots:       map[ontology.Slot]string{ontology.Claim: "b02wip", ontology.Close: "b02done", ontology.Reopen: "b02active", ontology.Defer: "b02frozen", ontology.Fail: "b02failed", ontology.Cancel: "b02canceled"},
			Transitions: []ontology.Rule{{To: "b02active"}, {To: "b02wip"}, {To: "b02done", Guards: []ontology.Guard{{Kind: ontology.NoOpenBlockers}}}, {To: "b02frozen"}, {To: "b02failed"}, {To: "b02canceled"}},
		}},
	}
}

type b02InstalledReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
}

type b02InstalledHTTP struct {
	f      *fixture
	api    *httptest.Server
	frames *os.File
	source driver.Record
	digest string
}

func newB02InstalledHTTP(t *testing.T, f *fixture, frames *os.File) *b02InstalledHTTP {
	t.Helper()
	meta := f.state(t).Meta
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, readErr := r.Get(f.ctx, []model.Ref{b02InstalledSource})
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(rows) != 1 {
		t.Fatalf("retained installed source: rows=%d read=%v close=%v", len(rows), readErr, closeErr)
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	f.ontology = service.NewOntology(compiled.Effective)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(b02InstalledSource))))
	core, err := ontologyops.New(f.store, f.lease, ontologyops.Config{Source: service.StaticOntologySource(b02InstalledSource), ValidatingPre: ontologyops.NoValidatingPre(), ProtectedApplier: f.writer, OntologyRefresher: f.ontology})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.now))); err != nil {
		t.Fatal(err)
	}
	if err := ontologyrpc.Bind(registry, core); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: b02InstalledDependents, AuthMode: "anonymous", OntologyRevision: rows[0].Revision})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	h := &b02InstalledHTTP{f: f, api: httptest.NewServer(handler), frames: frames, source: rows[0], digest: compiled.Digest()}
	t.Cleanup(h.api.Close)
	return h
}

func (h *b02InstalledHTTP) call(t *testing.T, method, project string, write bool, params map[string]any) json.RawMessage {
	t.Helper()
	meta := map[string]any{"project": project, "agent": "b02-installed"}
	if write {
		meta["idempotencyKey"] = h.f.meta("b02-installed").IdempotencyKey
	}
	params["_meta"] = meta
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "b02-installed", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.f.ctx, http.MethodPost, h.api.URL+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := h.api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(h.frames).Encode(map[string]json.RawMessage{"request": body, "response": raw}); err != nil {
		t.Fatal(err)
	}
	var reply b02InstalledReply
	if response.StatusCode != http.StatusOK || json.Unmarshal(raw, &reply) != nil || reply.Error != nil || reply.Result == nil {
		t.Fatalf("actual %s response: HTTP %d %s", method, response.StatusCode, raw)
	}
	return reply.Result
}

func (h *b02InstalledHTTP) write(t *testing.T, method, project string, params map[string]any) model.Commit {
	t.Helper()
	var commit model.Commit
	if err := json.Unmarshal(h.call(t, method, project, true, params), &commit); err != nil || len(commit.Results) == 0 {
		t.Fatalf("public %s commit: %+v %v", method, commit, err)
	}
	state := h.f.state(t)
	if commit.Revision != state.Meta.Head {
		t.Fatal("public commit differs from actual retained head")
	}
	for _, result := range commit.Results {
		if record, ok := state.Records[result.Ref.ID]; ok && (record.Revision != commit.Revision || !bytes.Equal(record.Body, result.Record)) {
			t.Fatal("public committed record differs from actual reader body/revision")
		}
	}
	return commit
}

func (h *b02InstalledHTTP) catalogs(t *testing.T) {
	t.Helper()
	for _, project := range []string{b02InstalledRoot, b02InstalledDependents} {
		var got ontologyops.Result
		if err := json.Unmarshal(h.call(t, "ontology.get", project, false, map[string]any{"project": project}), &got); err != nil || got.Document == nil || got.Drifted || len(got.Problems) != 0 {
			t.Fatalf("actual installed catalog: %+v %v", got, err)
		}
		if project == b02InstalledRoot {
			for key, want := range b02InstalledDocument().Statuses {
				if !reflect.DeepEqual(got.Document.Statuses[key], want) {
					t.Fatalf("installed exact key %s: got=%+v want=%+v", key, got.Document.Statuses[key], want)
				}
			}
		} else if got.Document.Statuses["b02done"].Category != ontology.Frozen {
			t.Fatal("contradictory dependent-project category was not retained")
		}
	}
}

func (h *b02InstalledHTTP) observe(t *testing.T, phase, status string, root, ordinary, conditional model.Ref, wantOrdinary, wantConditional bool) {
	t.Helper()
	if err := h.f.lease.Renew(h.f.ctx); err != nil {
		t.Fatal(err)
	}
	before := h.f.state(t)
	h.f.clock = h.f.clock.Add(time.Second)
	for attempt := 0; attempt < 2; attempt++ {
		h.catalogs(t)
		var bead struct {
			Bead struct {
				ID, Status string
				Fields     map[string]any
			}
		}
		if err := json.Unmarshal(h.call(t, "bead.get", b02InstalledRoot, false, map[string]any{"id": root}), &bead); err != nil || bead.Bead.ID != root.ID || bead.Bead.Status != status || bead.Bead.Fields["status"] != status {
			t.Fatalf("public exact status %s: %+v %v", status, bead, err)
		}
		var page struct {
			Items      []struct{ ID string }
			NextCursor string
		}
		if err := json.Unmarshal(h.call(t, "bead.ready", b02InstalledDependents, false, map[string]any{"limit": 1000}), &page); err != nil || page.NextCursor != "" {
			t.Fatalf("complete ready: %+v %v", page, err)
		}
		got, want := []string{}, []string{}
		for _, item := range page.Items {
			got = append(got, item.ID)
		}
		if wantOrdinary {
			want = append(want, ordinary.ID)
		}
		if wantConditional {
			want = append(want, conditional.ID)
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s ready=%v want=%v", phase, got, want)
		}
		// This authored category matrix is independent of ready membership.
		blockedExpected := map[string][]string{}
		switch status {
		case "b02active", "b02wip", "b02frozen":
			blockedExpected[ordinary.ID] = []string{root.ID}
			blockedExpected[conditional.ID] = []string{root.ID}
		case "b02done", "b02canceled":
			blockedExpected[conditional.ID] = []string{root.ID}
		case "b02failed":
		default:
			t.Fatal("unauthored installed blocked category", status)
		}
		var blocked struct {
			Items []struct {
				Bead struct {
					ID      string
					Blocked bool
				}
				BlockedBy                      []model.Ref
				BlockedByCount, HiddenBlockers int
			}
			NextCursor string
		}
		if err := json.Unmarshal(h.call(t, "bead.blocked", b02InstalledDependents, false, map[string]any{"limit": 1000}), &blocked); err != nil || blocked.NextCursor != "" {
			t.Fatal("complete installed blocked page", err)
		}
		blockedGot := map[string][]string{}
		for _, item := range blocked.Items {
			if !item.Bead.Blocked || item.HiddenBlockers != 0 || item.BlockedByCount != len(item.BlockedBy) {
				t.Fatal("incomplete installed blocked reasons")
			}
			for _, ref := range item.BlockedBy {
				blockedGot[item.Bead.ID] = append(blockedGot[item.Bead.ID], ref.ID)
			}
		}
		if !reflect.DeepEqual(blockedGot, blockedExpected) {
			t.Fatalf("%s installed blocked reasons=%v want=%v", phase, blockedGot, blockedExpected)
		}
	}
	requireSameState(t, before, h.f.state(t))
	if err := json.NewEncoder(h.frames).Encode(map[string]any{"phase": phase, "engine": h.f.engine, "state": before, "source": h.source, "digest": h.digest, "status": status, "ordinaryReady": wantOrdinary, "conditionalReady": wantConditional}); err != nil {
		t.Fatal(err)
	}
}

func TestB02InstalledNativeCategoriesBlockingAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if output == "" {
				t.Fatal("declared original evidence directory required")
			}
			frames, err := os.Create(filepath.Join(output, strings.ReplaceAll(t.Name(), "/", "_")+".jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := frames.Close(); err != nil {
					t.Error(err)
				}
			})
			base := ontology.DefaultDocument()
			base.Statuses["b02done"] = ontology.Status{Name: "Frozen in the dependent project", Category: ontology.Frozen}
			body, err := service.MarshalOntologySource(service.OntologySource{Base: base})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: b02InstalledSource, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
				t.Fatal(err)
			}
			h := newB02InstalledHTTP(t, f, frames)
			// The actual protected project catalog is installed through public
			// plan/apply, using the production refresher shared by reads/writes.
			expect := []model.Precondition{model.PreconditionByRevision(b02InstalledSource, h.source.Revision)}
			var plan ontologyops.PlanResult
			if err := json.Unmarshal(h.call(t, "ontology.plan", b02InstalledRoot, false, map[string]any{"project": b02InstalledRoot, "document": b02InstalledDocument(), "expect": expect}), &plan); err != nil || !plan.Changes.Ready || plan.PlanDigest == "" {
				t.Fatalf("public install plan: %+v %v", plan, err)
			}
			h.write(t, "ontology.apply", b02InstalledRoot, map[string]any{"planDigest": plan.PlanDigest, "expect": expect, "humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}})
			// Read and bind the persisted source after public installation.
			h.api.Close()
			h = newB02InstalledHTTP(t, f, frames)
			h.catalogs(t)
			create := func(project, title string) model.Ref {
				return h.write(t, "bead.create", project, map[string]any{"type": "issue", "fields": map[string]any{"title": title}}).Results[0].Ref
			}
			root, ordinary, conditional := create(b02InstalledRoot, "installed blocker"), create(b02InstalledDependents, "ordinary dependent"), create(b02InstalledDependents, "conditional dependent")
			for _, edge := range []struct {
				from model.Ref
				kind string
			}{{ordinary, "blocks"}, {conditional, "conditional-blocks"}} {
				h.write(t, "bead.link", b02InstalledDependents, map[string]any{"from": edge.from, "to": root, "kind": edge.kind})
			}
			dependents := f.state(t)
			steps := []struct {
				name, method, target, status string
				ordinary, conditional        bool
				notices                      int
			}{
				{"active", "", "", "b02active", false, false, -1},
				{"wip", "bead.create", "b02wip", "b02wip", false, false, -1},
				{"done", "bead.close", "b02done", "b02done", true, false, 1},
				{"reopen-done", "bead.reopen", "b02active", "b02active", false, false, -1},
				{"frozen", "bead.defer", "", "b02frozen", false, false, -1},
				{"reopen-frozen", "bead.reopen", "b02active", "b02active", false, false, -1},
				{"failure", "bead.fail", "", "b02failed", true, true, 0},
				{"reopen-failure", "bead.reopen", "b02active", "b02active", false, false, -1},
				{"cancel", "bead.cancel", "", "b02canceled", true, false, 1},
			}
			for _, step := range steps {
				if step.name == "wip" {
					// Wip is the only blocker during this phase; an active
					// root must not mask either its status or its edge effect.
					wip := h.write(t, "bead.create", b02InstalledRoot, map[string]any{"type": "issue", "targetStatus": step.target, "fields": map[string]any{"title": "wip blocker"}}).Results[0].Ref
					for _, edge := range []struct {
						from model.Ref
						kind string
					}{{ordinary, "blocks"}, {conditional, "conditional-blocks"}} {
						h.write(t, "bead.unlink", b02InstalledDependents, map[string]any{"from": edge.from, "to": root, "kind": edge.kind})
						h.write(t, "bead.link", b02InstalledDependents, map[string]any{"from": edge.from, "to": wip, "kind": edge.kind})
					}
					h.observe(t, step.name, "b02wip", wip, ordinary, conditional, false, false)
					if engine != "memory" {
						before := f.state(t)
						h.api.Close()
						f.reopen(t)
						h = newB02InstalledHTTP(t, f, frames)
						requireSameState(t, before, f.state(t))
						h.observe(t, step.name+"-sql-reopened", "b02wip", wip, ordinary, conditional, false, false)
					}
					h.write(t, "bead.close", b02InstalledRoot, map[string]any{"id": wip, "targetStatus": "b02done"})
					h.observe(t, "wip-completed", "b02done", wip, ordinary, conditional, true, false)
					for _, edge := range []struct {
						from model.Ref
						kind string
					}{{ordinary, "blocks"}, {conditional, "conditional-blocks"}} {
						h.write(t, "bead.unlink", b02InstalledDependents, map[string]any{"from": edge.from, "to": wip, "kind": edge.kind})
						h.write(t, "bead.link", b02InstalledDependents, map[string]any{"from": edge.from, "to": root, "kind": edge.kind})
					}
					dependents = f.state(t)
					continue
				}
				var commit model.Commit
				beforeWrite := f.state(t)
				if step.method != "" {
					params := map[string]any{"id": root}
					if step.target != "" {
						params["targetStatus"] = step.target
					}
					commit = h.write(t, step.method, b02InstalledRoot, params)
				}
				h.observe(t, step.name, step.status, root, ordinary, conditional, step.ordinary, step.conditional)
				if engine != "memory" {
					before := f.state(t)
					h.api.Close()
					f.reopen(t)
					h = newB02InstalledHTTP(t, f, frames)
					requireSameState(t, before, f.state(t))
					h.observe(t, step.name+"-sql-reopened", step.status, root, ordinary, conditional, step.ordinary, step.conditional)
				}
				state := f.state(t)
				for _, ref := range []model.Ref{ordinary, conditional} {
					if !reflect.DeepEqual(dependents.Records[ref.ID], state.Records[ref.ID]) || !reflect.DeepEqual(dependents.Versions[ref.ID], state.Versions[ref.ID]) {
						t.Fatal("blocker lifecycle/read/reopen rewrote dependent record or history")
					}
				}
				if step.notices >= 0 {
					var retained *model.Changeset
					for i := range state.Log {
						if state.Log[i].Revision == commit.Revision {
							if retained != nil {
								t.Fatal("duplicate committed revision")
							}
							retained = &state.Log[i]
						}
					}
					if retained == nil || len(retained.Changes) != 1 || len(retained.Events) != 1+step.notices || retained.ValidateEvents() != nil {
						t.Fatal("terminal write lacks its exact retained event plan")
					}
					notices := 0
					for _, event := range retained.Events {
						if event.Type == model.EventDependencyConditionUnmet {
							notices++
							d := event.Details
							if d == nil || d.Dependent.Ref.ID != conditional.ID || d.Dependent.Revision != state.Records[conditional.ID].Revision || d.BlockerBefore.Ref.ID != root.ID || d.BlockerBefore.Revision != retained.Changes[0].FromRevision || d.BlockerAfter.Ref.ID != root.ID || d.BlockerAfter.Revision != commit.Revision || d.Ontology.Ref.ID != b02InstalledSource.ID || d.Ontology.Revision != h.source.Revision || d.Ontology.Digest != h.digest || d.Ontology.Project == nil || d.Ontology.Project.ID != b02InstalledRoot {
								t.Fatal("notice lost exact blocker/dependent/category-source binding")
							}
						}
					}
					beforeDoc := decodeDocument(t, beforeWrite.Records[root.ID])
					for _, event := range retained.Events {
						if event.Type != model.EventDependencyConditionUnmet {
							continue
						}
						wantBefore := model.ConditionDecision{Status: beforeDoc.Fields["status"].(string), Category: "active"}
						wantAfter := model.ConditionDecision{Status: step.status, Category: "done", Outcome: "success", Terminal: true}
						if step.method == "bead.cancel" {
							wantAfter.Category, wantAfter.Outcome = "canceled", ""
						}
						if !reflect.DeepEqual(event.Details.Before, wantBefore) || !reflect.DeepEqual(event.Details.After, wantAfter) {
							t.Fatalf("notice decision tuple: before=%+v after=%+v wantBefore=%+v wantAfter=%+v", event.Details.Before, event.Details.After, wantBefore, wantAfter)
						}
					}
					if notices != step.notices {
						t.Fatalf("%s notices=%d want=%d", step.name, notices, step.notices)
					}
				}
				t.Logf("B02_INSTALLED engine=%s phase=%s exact_status=%s ordinary=%t conditional=%t SQL_reopen=%t", engine, step.name, step.status, step.ordinary, step.conditional, engine != "memory")
			}
			fmt.Fprintln(frames, `{"qualification":"public installed category behavior only; reference/import/history/full-card remain separate"}`)
		})
	}
}
