package batchsql_test

import (
	"bytes"
	"encoding/json"
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
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	b02GraphProject = "b02-graph"
	b02RootProject  = "b02-root"
	b02GraphAgent   = "b02-public-graph"
)

var b02GraphOntology = model.Ref{ID: "ontology/b02-public-graph"}

type b02GraphReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
}

type b02GraphHTTP struct {
	f      *fixture
	api    *httptest.Server
	frames *os.File
}

// Only the protected catalog is a fixture. Every bead, edge and lifecycle
// transition below traverses the production HTTP/RPC/service/Writer path.
func b02GraphCatalog(deferStatus string) service.OntologySource {
	base := ontology.DefaultDocument()
	base.Statuses["b02-success"] = ontology.Status{Name: "Not terminal here", Category: ontology.Backlog}
	base.Statuses["b02-failure"] = ontology.Status{Name: "Success here", Category: ontology.Completed, Outcome: ontology.Success}
	base.Statuses["b02-canceled"] = ontology.Status{Name: "Not canceled here", Category: ontology.Backlog}
	base.Statuses["frozen"] = ontology.Status{Name: "Failure here", Category: ontology.Completed, Outcome: ontology.Failure}
	base.Statuses["pinned"] = ontology.Status{Name: "Success here too", Category: ontology.Completed, Outcome: ontology.Success}
	root := ontology.DefaultDocument()
	rename := map[string]string{"done": "b02-success", "failed": "b02-failure", "canceled": "b02-canceled", "backlog": deferStatus}
	for old, next := range rename {
		root.Statuses[next] = root.Statuses[old]
		delete(root.Statuses, old)
	}
	w := root.Workflows["issue"]
	for i, key := range w.Statuses {
		if next, ok := rename[key]; ok {
			w.Statuses[i] = next
		}
	}
	for slot, key := range w.Slots {
		if next, ok := rename[key]; ok {
			w.Slots[slot] = next
		}
	}
	for i := range w.Transitions {
		if next, ok := rename[w.Transitions[i].To]; ok {
			w.Transitions[i].To = next
		}
	}
	root.Workflows["issue"] = w
	return service.OntologySource{Base: base, Overlays: map[string]ontology.Document{b02RootProject: root}}
}

func newB02GraphHTTP(t *testing.T, f *fixture, frames *os.File) *b02GraphHTTP {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Get(f.ctx, []model.Ref{b02GraphOntology})
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(rows) != 1 || rows[0].Kind != "ontology" {
		t.Fatalf("durable catalog read: rows=%v err=%v close=%v", rows, err, closeErr)
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatalf("catalog does not compile: %+v", problems)
	}
	f.ontology = service.NewOntology(compiled.Effective)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(b02GraphOntology))),
	)
	svc := service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, svc); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: b02GraphProject, AuthMode: "anonymous"})
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
	h := &b02GraphHTTP{f: f, api: httptest.NewServer(handler), frames: frames}
	t.Cleanup(h.api.Close)
	return h
}

func (h *b02GraphHTTP) call(t *testing.T, method, project, key string, params map[string]any) b02GraphReply {
	t.Helper()
	meta := map[string]any{"project": project, "agent": b02GraphAgent}
	if key != "" {
		meta["idempotencyKey"] = key
	}
	params["_meta"] = meta
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "b02-graph", "method": method, "params": params})
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
	if err := json.NewEncoder(h.frames).Encode(struct {
		Request  json.RawMessage `json:"request"`
		Response json.RawMessage `json:"response"`
	}{body, raw}); err != nil {
		t.Fatal(err)
	}
	var reply b02GraphReply
	if err := json.Unmarshal(raw, &reply); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("invalid public response: HTTP %d %s (%v)", response.StatusCode, raw, err)
	}
	return reply
}

func (h *b02GraphHTTP) write(t *testing.T, method, project string, params map[string]any) model.Commit {
	t.Helper()
	reply := h.call(t, method, project, h.f.meta("b02-graph").IdempotencyKey, params)
	var commit model.Commit
	if reply.Error != nil || json.Unmarshal(reply.Result, &commit) != nil || len(commit.Results) == 0 {
		t.Fatalf("public %s refused: error=%+v result=%s", method, reply.Error, reply.Result)
	}
	return commit
}

func (h *b02GraphHTTP) create(t *testing.T, title, project string) model.Ref {
	t.Helper()
	return h.write(t, "bead.create", project, map[string]any{"type": "issue", "fields": map[string]any{"title": title}}).Results[0].Ref
}

func (h *b02GraphHTTP) ready(t *testing.T, want ...model.Ref) {
	t.Helper()
	// Maintain the host lease before the read-purity snapshot. The memory
	// driver's real lease clock follows the controlled fixture clock too.
	if err := h.f.lease.Renew(h.f.ctx); err != nil {
		t.Fatalf("renew fixture lease before readiness snapshot: %v", err)
	}
	before := h.f.state(t)
	// Cross a real clock boundary: a recomputation must not stamp a bead as
	// edited, publish a revision, append events, or rewrite its retained image.
	h.f.clock = h.f.clock.Add(time.Second)
	for attempt := 0; attempt < 2; attempt++ {
		reply := h.call(t, "bead.ready", b02GraphProject, "", map[string]any{"limit": 1000})
		var page struct {
			Items      []service.Bead `json:"items"`
			NextCursor string         `json:"nextCursor"`
		}
		if reply.Error != nil || json.Unmarshal(reply.Result, &page) != nil || page.NextCursor != "" {
			t.Fatalf("public ready did not return a complete set: %+v %s", reply.Error, reply.Result)
		}
		got, expected := []string{}, []string{}
		for _, bead := range page.Items {
			got = append(got, bead.ID)
		}
		for _, ref := range want {
			expected = append(expected, ref.ID)
		}
		sort.Strings(got)
		sort.Strings(expected)
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("public ready=%v want=%v", got, expected)
		}
	}
	requireSameState(t, before, h.f.state(t))
}

func b02GraphUnchangedDependents(t *testing.T, before, after durableState, refs ...model.Ref) {
	t.Helper()
	for _, ref := range refs {
		if !reflect.DeepEqual(before.Records[ref.ID], after.Records[ref.ID]) || !reflect.DeepEqual(before.Versions[ref.ID], after.Versions[ref.ID]) {
			t.Fatalf("blocker transition changed dependent %s record/timestamps/history", ref.ID)
		}
	}
}

func TestB02PublicCategoryDiamondAndReadPurityAcrossDrivers(t *testing.T) {
	for _, tc := range []struct {
		name, method, status, deferStatus string
		ordinary, conditional             bool
	}{
		{"completed_success", "bead.close", "b02-success", "frozen", true, false},
		{"completed_failure", "bead.fail", "b02-failure", "frozen", true, true},
		{"canceled", "bead.cancel", "b02-canceled", "frozen", true, false},
		{"frozen_backlog", "bead.defer", "frozen", "frozen", false, false},
		{"pinned_backlog", "bead.defer", "pinned", "pinned", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					f := newFixture(t, engine)
					directory := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
					if directory == "" {
						directory = t.TempDir()
					}
					frames, err := os.Create(filepath.Join(directory, strings.ReplaceAll(t.Name(), "/", "_")+".rpc.jsonl"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := frames.Close(); err != nil {
							t.Error(err)
						}
					})
					body, err := service.MarshalOntologySource(b02GraphCatalog(tc.deferStatus))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
						Ref: b02GraphOntology, Kind: "ontology", Op: model.OpCreate, Body: body,
					}}); err != nil {
						t.Fatal(err)
					}
					h := newB02GraphHTTP(t, f, frames)
					root := h.create(t, "root blocker", b02RootProject)
					left := h.create(t, "left branch", b02GraphProject)
					right := h.create(t, "right branch", b02GraphProject)
					leaf := h.create(t, "diamond leaf", b02GraphProject)
					conditional := h.create(t, "failure-only dependent", b02GraphProject)
					for _, edge := range []struct {
						from, to model.Ref
						kind     string
					}{
						{left, root, "blocks"}, {right, root, "blocks"},
						{leaf, left, "blocks"}, {leaf, right, "blocks"},
						{conditional, root, "conditional-blocks"},
					} {
						h.write(t, "bead.link", b02GraphProject, map[string]any{"from": edge.from, "to": edge.to, "kind": edge.kind})
					}
					h.ready(t)
					before := f.state(t)
					key := f.meta("blocked-leaf-close").IdempotencyKey
					refused := h.call(t, "bead.close", b02GraphProject, key, map[string]any{"id": leaf})
					if refused.Error == nil || refused.Error.Data.Code != "ontology_guard_failed" || len(refused.Result) != 0 {
						t.Fatalf("blocked diamond close must refuse: %+v %s", refused.Error, refused.Result)
					}
					requireSameState(t, before, f.state(t))
					meta, err := f.store.Meta(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
					if err != nil {
						t.Fatal(err)
					}
					receipt, err := r.IdempotencyByKey(f.ctx, principal, b02GraphAgent, "bead.close", key)
					closeErr := r.Close()
					if err != nil || closeErr != nil || receipt != nil {
						t.Fatalf("refused close retained a receipt: %+v err=%v close=%v", receipt, err, closeErr)
					}

					h.write(t, tc.method, b02RootProject, map[string]any{"id": root})
					after := f.state(t)
					if status := decodeDocument(t, after.Records[root.ID]).Fields["status"]; status != tc.status {
						t.Fatalf("public slot status=%v want=%s", status, tc.status)
					}
					b02GraphUnchangedDependents(t, before, after, left, right, leaf, conditional)
					want := []model.Ref{}
					if tc.ordinary {
						want = append(want, left, right)
					}
					if tc.conditional {
						want = append(want, conditional)
					}
					h.ready(t, want...)
					if engine != "memory" {
						h.api.Close()
						f.reopen(t)
						h = newB02GraphHTTP(t, f, frames)
						requireSameState(t, after, f.state(t))
						h.ready(t, want...)
						t.Log("SQL reopen reconstructed the public host and compiled catalog from its persisted source")
					} else {
						t.Log("memory assertion is in-process; no persistence reopen claimed")
					}

					h.write(t, "bead.reopen", b02RootProject, map[string]any{"id": root})
					h.ready(t)
					h.write(t, "bead.close", b02RootProject, map[string]any{"id": root})
					h.ready(t, left, right)
					h.write(t, "bead.close", b02GraphProject, map[string]any{"id": left})
					h.ready(t, right) // The other diamond branch must still block.
					h.write(t, "bead.close", b02GraphProject, map[string]any{"id": right})
					h.ready(t, leaf)
					// Reopening the root does not reopen its terminal intermediates.
					// The leaf reblocks only when its own immediate blocker reopens.
					h.write(t, "bead.reopen", b02RootProject, map[string]any{"id": root})
					h.ready(t, leaf)
					h.write(t, "bead.reopen", b02GraphProject, map[string]any{"id": left})
					h.ready(t)
					h.write(t, "bead.unlink", b02GraphProject, map[string]any{"from": left, "to": root, "kind": "blocks"})
					h.ready(t, left)
					h.write(t, "bead.close", b02GraphProject, map[string]any{"id": left})
					h.ready(t, leaf)
					h.write(t, "bead.unlink", b02GraphProject, map[string]any{"from": conditional, "to": root, "kind": "conditional-blocks"})
					h.ready(t, leaf, conditional)
					b02GraphUnchangedDependents(t, before, f.state(t), leaf)
					t.Logf("B02 public diamond category=%s engine=%s: own-project category/outcome, both branches, close/reopen/unlink, refusal conservation and read purity", tc.name, engine)
				})
			}
		})
	}
}
