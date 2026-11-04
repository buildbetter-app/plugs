package webacceptance_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const e3GrantKey = "e3.acceptance.readers"
const e3DefinitionKey = "e3.acceptance.definition"

// These adapters exist only in this acceptance package. They resolve real
// Writer-persisted metadata through service.Get and enforce explicit current
// stored grants. They do not supply an installed production authz plugin or
// add a production saved-view resource contract.
type e3Grant struct {
	Project   string             `json:"project"`
	Reader    model.PrincipalRef `json:"reader"`
	Resources []string           `json:"resources"`
	Presence  bool               `json:"presence"`
}

type e3StoredPolicy struct {
	f       *e3Fixture
	mu      sync.Mutex
	head    string
	grant   e3Grant
	checks  atomic.Int64
	allows  atomic.Int64
	denials atomic.Int64
}

func e3Failure(code model.ErrorCode) error { return &model.Error{Data: model.ErrorData{Code: code}} }

func (p *e3StoredPolicy) current(ctx context.Context) (e3Grant, error) {
	if err := ctx.Err(); err != nil {
		return e3Grant{}, err
	}
	meta, err := p.f.store.Meta(ctx)
	if err != nil {
		return e3Grant{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.head != meta.Head.String() {
		value, err := p.f.domain.Get(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, service.GetRequest{ID: model.RefByID(e3ID(100), model.Natural64(0))})
		if err != nil {
			return e3Grant{}, err
		}
		raw, err := json.Marshal(value.Bead.Metadata[e3GrantKey])
		var grant e3Grant
		if err != nil || json.Unmarshal(raw, &grant) != nil || grant.Project != e3Project || grant.Reader.Kind == "" || len(grant.Resources) == 0 {
			return e3Grant{}, e3Failure("authz_unavailable")
		}
		p.head, p.grant = meta.Head.String(), grant
	}
	return p.grant, nil
}

func e3EpicBinding(head string) string { return "e3-stored-policy-v1:" + head }

func e3GrantContains(grant e3Grant, id, project string) bool {
	if project != grant.Project {
		return false
	}
	for _, resource := range grant.Resources {
		if resource == id {
			return true
		}
	}
	return false
}

// e3EpicDocuments decodes only the pinned rows supplied by viewops. It never
// reads current bodies while producing counts, so status/parent membership is
// bound to the one content snapshot passed to AuthorizeScope.
func e3EpicDocuments(ctx context.Context, reader driver.Reader, refs []model.Ref) (map[string]pipeline.Document, error) {
	rows, err := reader.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	docs := make(map[string]pipeline.Document, len(rows))
	for _, row := range rows {
		if row.Kind != "bead" {
			return nil, e3Failure("authz_unavailable")
		}
		var doc pipeline.Document
		if err := json.Unmarshal(row.Body, &doc); err != nil || doc.ID == "" || doc.ID != row.Ref.ID {
			return nil, e3Failure("authz_unavailable")
		}
		docs[doc.ID] = doc
	}
	return docs, nil
}

// authorizeEpicProgress is a fixture-only current stored-grant adapter. It
// mirrors the producer contract: the parent must be readable, each child is
// checked in its own stored project, hidden children contribute only a count,
// and one current head binds the complete decision before status bucketing.
func (p *e3StoredPolicy) authorizeEpicProgress(ctx context.Context, by model.PrincipalRef, content driver.Reader, parent model.Ref, children []model.Ref) (viewops.EpicProgressAuthority, error) {
	if by.Kind != "anonymous" || by.ID != "anonymous" {
		return viewops.EpicProgressAuthority{}, e3Failure("forbidden")
	}
	meta, err := p.f.store.Meta(ctx)
	if err != nil {
		return viewops.EpicProgressAuthority{}, err
	}
	grant, err := p.current(ctx)
	if err != nil {
		return viewops.EpicProgressAuthority{}, err
	}
	if after, e := p.f.store.Meta(ctx); e != nil {
		return viewops.EpicProgressAuthority{}, e
	} else if after.Head != meta.Head {
		return viewops.EpicProgressAuthority{}, e3Failure("authz_unavailable")
	}
	docs, err := e3EpicDocuments(ctx, content, append([]model.Ref{parent}, children...))
	if err != nil {
		return viewops.EpicProgressAuthority{}, err
	}
	parentDoc, ok := docs[parent.ID]
	if !ok || !e3GrantContains(grant, parentDoc.ID, parentDoc.Project) {
		return viewops.EpicProgressAuthority{}, e3Failure("not_found")
	}
	visible := make([]model.Ref, 0, len(children))
	for _, child := range children {
		doc, ok := docs[child.ID]
		if !ok {
			return viewops.EpicProgressAuthority{}, e3Failure("authz_unavailable")
		}
		if e3GrantContains(grant, doc.ID, doc.Project) {
			visible = append(visible, child)
		}
	}
	return viewops.EpicProgressAuthority{
		Visible:       visible,
		Hidden:        model.Natural64(uint64(len(children) - len(visible))),
		PolicyBinding: e3EpicBinding(meta.Head.String()),
	}, nil
}

func (p *e3StoredPolicy) validateEpicProgress(ctx context.Context, by model.PrincipalRef, expected string) error {
	if by.Kind != "anonymous" || by.ID != "anonymous" {
		return e3Failure("forbidden")
	}
	meta, err := p.f.store.Meta(ctx)
	if err != nil {
		return err
	}
	grant, err := p.current(ctx)
	if err != nil {
		return err
	}
	if expected != e3EpicBinding(meta.Head.String()) || grant.Reader.Kind != by.Kind || grant.Reader.ID != by.ID {
		return e3Failure("authz_unavailable")
	}
	return nil
}

func (p *e3StoredPolicy) allowed(ctx context.Context, by model.PrincipalRef, id, project string) (bool, error) {
	p.checks.Add(1)
	grant, err := p.current(ctx)
	if err != nil {
		return false, err
	}
	a, err := json.Marshal(by)
	if err != nil {
		return false, err
	}
	b, _ := json.Marshal(grant.Reader)
	allowed := false
	if string(a) == string(b) && project == grant.Project {
		for _, resource := range grant.Resources {
			if id == resource {
				allowed = true
				break
			}
		}
	}
	if allowed {
		p.allows.Add(1)
	} else {
		p.denials.Add(1)
	}
	return allowed, nil
}

func (p *e3StoredPolicy) AuthorizeRead(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, project string) error {
	if resource.Kind != "bead" || !resource.Ref.IsID() || !resource.Ref.Branch.IsZero() {
		return e3Failure("forbidden")
	}
	allowed, err := p.allowed(ctx, by, resource.Ref.ID, project)
	if err != nil {
		return err
	}
	if !allowed {
		return e3Failure("forbidden")
	}
	return nil
}

func (p *e3StoredPolicy) AdmitPresence(ctx context.Context, by model.PrincipalRef, project string) (bool, error) {
	grant, err := p.current(ctx)
	if err != nil {
		return false, err
	}
	// The carrier must itself be readable; permission never follows from a
	// successful unfiltered service.Get alone.
	allowed, err := p.allowed(ctx, by, e3ID(100), project)
	return allowed && grant.Presence, err
}

func (p *e3StoredPolicy) CanRead(ctx context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
	return p.allowed(ctx, by, bead.ID, bead.Project)
}

func (p *e3StoredPolicy) Read(ctx context.Context, by model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	id, ok := strings.CutPrefix(event.Resource, "bead/")
	if !ok {
		return coreevents.Visibility{}, nil
	}
	allowed, err := p.allowed(ctx, by, id, e3Project)
	if err != nil || !allowed {
		return coreevents.Visibility{}, err
	}
	// The live service read proves current existence/project, even when the
	// event image is old. Deleted/foreign/uncertain resources fail closed.
	value, err := p.f.domain.Get(ctx, by, service.GetRequest{ID: model.RefByID(id, model.Natural64(0))})
	if err != nil {
		var typed *model.Error
		if errors.As(err, &typed) && typed.Data.Code == "not_found" {
			return coreevents.Visibility{}, nil
		}
		return coreevents.Visibility{}, err
	}
	if value.Bead.Project != e3Project {
		return coreevents.Visibility{}, nil
	}
	return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
}

func (p *e3StoredPolicy) ReadResource(ctx context.Context, by model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if resource.Kind != "bead" {
		return coreevents.ResourceVisibility{}, nil
	}
	value, err := p.f.domain.Get(ctx, by, service.GetRequest{ID: resource.Ref})
	if err != nil {
		return coreevents.ResourceVisibility{}, err
	}
	allowed, err := p.CanRead(ctx, by, value.Bead)
	return coreevents.ResourceVisibility{Readable: allowed}, err
}

type e3SavedViews struct{ policy *e3StoredPolicy }

func (v e3SavedViews) GetView(ctx context.Context, by model.PrincipalRef, id string) (viewops.Definition, error) {
	if id != e3ID(98) && id != e3ID(99) {
		return viewops.Definition{}, e3Failure("not_found")
	}
	value, err := v.policy.f.domain.Get(ctx, by, service.GetRequest{ID: model.RefByID(id, model.Natural64(0))})
	if err != nil {
		return viewops.Definition{}, err
	}
	allowed, err := v.policy.CanRead(ctx, by, value.Bead)
	if err != nil {
		return viewops.Definition{}, err
	}
	if !allowed {
		return viewops.Definition{}, e3Failure("not_found")
	}
	raw, err := json.Marshal(value.Bead.Metadata[e3DefinitionKey])
	var definition viewops.Definition
	if err != nil || json.Unmarshal(raw, &definition) != nil || definition.Filter == nil {
		return viewops.Definition{}, e3Failure("not_found")
	}
	return definition, nil
}

type e3BrowserHost struct {
	f       *e3Fixture
	policy  *e3StoredPolicy
	views   *viewops.Service
	watches *watch.Manager
	adapter *watchhttp.Adapter
	streams atomic.Int64
	mu      sync.Mutex
	lines   []json.RawMessage
	bridged map[string]bool
}

func newE3BrowserHost(f *e3Fixture) *e3BrowserHost {
	return &e3BrowserHost{f: f, policy: &e3StoredPolicy{f: f}, bridged: map[string]bool{}}
}

func (h *e3BrowserHost) bind(registry *rpc.Registry) error {
	var err error
	f := h.f
	var directBlockers viewops.DirectBlockerSource
	if f.ontologySource != nil {
		locate := service.StaticOntologySource(f.ontologyRef)
		directBlockers = func(ctx context.Context, reader driver.Reader, dependent model.Ref, limit int) (viewops.DirectBlockerVerdict, error) {
			verdict, err := service.ReadDirectBlockers(ctx, reader, service.DirectBlockerRequest{Dependent: dependent, Ontology: locate, Limit: limit})
			if err != nil {
				return viewops.DirectBlockerVerdict{}, err
			}
			out := viewops.DirectBlockerVerdict{Edges: make([]viewops.DirectBlockerEdge, 0, len(verdict.Edges))}
			for _, edge := range verdict.Edges {
				out.Edges = append(out.Edges, viewops.DirectBlockerEdge{Blocker: edge.Blocker, TargetKind: edge.TargetKind, Kind: edge.Kind, State: viewops.DirectBlockerState(edge.State), Reason: edge.Reason})
			}
			return out, nil
		}
	}
	progress := &viewops.EpicProgressConfig{
		ValidateAuthority: h.policy.validateEpicProgress,
		AuthorizeScope:    h.policy.authorizeEpicProgress,
		OntologySource:    service.StaticOntologySource(f.ontologyRef),
	}
	h.views, err = viewops.New(f.store, f.lease, viewops.Config{Views: e3SavedViews{h.policy}, AuthorizeRead: h.policy.AuthorizeRead, DirectBlockers: directBlockers, EpicProgress: progress, OntologyRevision: f.ontologyRevision, PolicyVersion: "e3-stored-policy-v1"})
	if err != nil {
		return err
	}
	if err := viewrpc.BindWithConfig(registry, h.views, viewrpc.Config{CursorKey: f.cursorKey}); err != nil {
		return err
	}
	codec, err := serverevents.NewCursorCodec(f.cursorKey)
	if err != nil {
		return err
	}
	reader := &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: h.policy}
	h.watches, err = watch.New(f.ctx, watch.Config{Events: &serverevents.Service{Reader: reader, Cursors: codec}, Presence: f.sessions})
	if err != nil {
		return err
	}
	h.adapter, err = watchhttp.New(h.watches, watchhttp.Config{Views: h.views, Ontology: model.Natural64(1), PolicyVersion: "e3-stored-policy-v1"})
	if err != nil {
		return err
	}
	if err := h.adapter.Register(registry); err != nil {
		return err
	}
	f.httpConfig.SessionBridge = func(ctx context.Context, by model.PrincipalRef, id, agent string) (context.Context, error) {
		lifetime, err := f.sessions.SessionContext(ctx, collab.Caller{Principal: by, Agent: agent}, id)
		if err != nil {
			return ctx, err
		}
		h.mu.Lock()
		h.bridged[id] = true
		h.mu.Unlock()
		return watchhttp.WithSession(ctx, id, lifetime), nil
	}
	// The new config fields come only from the source owner's normal merge.
	f.uiConfig = &webui.Config{Project: e3Project, MaxRows: 25, HomeViewID: e3ID(99), WatchPath: watchhttp.DefaultPath, BlockedViewID: e3ID(98)}
	f.mount = h.mount
	return nil
}

// wireWriter observes the actual bytes written by the adapter; it supplies no
// event payloads and implements flushing only by delegating to net/http.
type e3WireWriter struct {
	http.ResponseWriter
	host *e3BrowserHost
}

func (w e3WireWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.host.mu.Lock()
		w.host.lines = append(w.host.lines, append(json.RawMessage(nil), p[:n]...))
		w.host.mu.Unlock()
	}
	return n, err
}
func (w e3WireWriter) Flush() { w.ResponseWriter.(http.Flusher).Flush() }

func (h *e3BrowserHost) mount(mux *http.ServeMux) {
	admit, err := authn.New(authn.Config{})
	if err != nil {
		h.f.t.Fatal(err)
	}
	mux.HandleFunc(watchhttp.DefaultPath, func(w http.ResponseWriter, r *http.Request) {
		identity, err := admit.AdmitHTTP(r)
		if err != nil {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		ids, agents := r.Header.Values("Plug-Session"), r.Header.Values("Plug-Agent")
		if len(ids) != 1 || ids[0] == "" || len(agents) > 1 {
			http.Error(w, "session required", http.StatusBadRequest)
			return
		}
		caller := collab.Caller{Principal: identity.Principal()}
		if len(agents) == 1 {
			caller.Agent = agents[0]
		}
		lifetime, err := h.f.sessions.SessionContext(r.Context(), caller, ids[0])
		if err != nil {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		ctx := watchhttp.WithSession(watchhttp.WithPrincipal(r.Context(), identity.Principal()), ids[0], lifetime)
		h.streams.Add(1)
		defer h.streams.Add(-1)
		h.adapter.ServeHTTP(e3WireWriter{w, h}, r.WithContext(ctx))
	})
	mux.HandleFunc("/fixture/e3/stats", func(w http.ResponseWriter, r *http.Request) {
		rows, err := h.f.sessions.List(r.Context(), collab.Caller{Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}}, e3Project)
		if err != nil {
			http.Error(w, "presence unavailable", http.StatusServiceUnavailable)
			return
		}
		h.mu.Lock()
		watches := 0
		for id := range h.bridged {
			watches += h.watches.WatchCount(id)
		}
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"streams": h.streams.Load(), "watches": watches, "sessions": len(rows), "policyChecks": h.policy.checks.Load(), "policyAllows": h.policy.allows.Load(), "policyDenials": h.policy.denials.Load()})
	})
}

func (h *e3BrowserHost) close() {
	if h.watches != nil {
		if err := h.watches.Close(); err != nil {
			h.f.t.Error(err)
		}
	}
	if err := h.writeEvidence(); err != nil {
		h.f.t.Error(err)
	}
}

func (h *e3BrowserHost) writeEvidence() error {
	if h.f.evidence == "" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var wire strings.Builder
	for _, line := range h.lines {
		wire.Write(line)
	}
	return writeE3Bytes(h.f.evidence+"/watch.ndjson", []byte(wire.String()))
}

func (f *e3Fixture) seedBrowserMetadata() e3Grant {
	f.t.Helper()
	grant := e3Grant{Project: e3Project, Reader: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Presence: true}
	resources := []string{}
	for index := 1; index <= 60; index++ {
		resources = append(resources, e3ID(index))
		if index != 60 { // Real denied canary in the saved home membership.
			grant.Resources = append(grant.Resources, e3ID(index))
		}
	}
	grant.Resources = append(grant.Resources, e3ID(98), e3ID(99), e3ID(100))
	budget := model.Natural64(25)
	definitions := map[int]viewops.Definition{
		99: {Name: "E3 explicit stored membership", Filter: &viewops.Filter{Resources: resources}, Sort: []string{"id"}, BudgetTokens: &budget},
		98: {Name: "E3 dependent 52 candidate", Filter: &viewops.Filter{Resources: []string{e3ID(52)}}, Sort: []string{"id"}, BudgetTokens: &budget},
	}
	ops := []model.Op{}
	add := func(index int, key string, value any) {
		raw, err := json.Marshal(value)
		if err != nil {
			f.t.Fatal(err)
		}
		ops = append(ops, model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: model.RefByID(e3ID(index), model.Natural64(0)), Key: key, Value: raw}})
	}
	add(100, e3GrantKey, grant)
	for _, index := range []int{98, 99} {
		add(index, e3DefinitionKey, definitions[index])
	}
	f.call("tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: ops}, "_meta": map[string]any{"project": e3Project, "agent": "e3-populator", "idempotencyKey": e3Key("browser-metadata")}}, nil)
	f.receipt.Authorization = "fixture-only explicit current Writer-stored resource grant, actual allow/deny/revoke; not installed production authz qualification"
	return grant
}

func writeE3Bytes(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

// This is a small real-store/HTTP fixture gate, not the five-task E3 run.
func TestE3ConfiguredDirectBlockerFixture(t *testing.T) {
	root := os.Getenv("PLUG_WEB_ACCEPTANCE_EVIDENCE_DIR")
	if root == "" {
		root = os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	}
	if root == "" {
		root = t.TempDir()
	}
	// This small fixture also writes corpus/watch cleanup receipts. Keep them
	// separate from the full scripted runtime's durable root/e3 artifacts.
	t.Setenv("PLUG_WEB_ACCEPTANCE_EVIDENCE_DIR", filepath.Join(root, "configured-fixture"))
	f := newE3FixtureMode(t, true)
	if f.evidence != filepath.Join(root, "configured-fixture", "e3") || f.evidence == filepath.Join(root, "e3") {
		t.Fatalf("focused fixture shares the full runtime evidence directory: %s", f.evidence)
	}
	f.populate(1, 100)
	f.seedBrowserMetadata()
	meta := f.meta()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Get(f.ctx, []model.Ref{f.ontologyRef})
	_ = r.Close()
	if err != nil || len(rows) != 1 || rows[0].Kind != "ontology" || rows[0].Revision != f.ontologyRevision || rows[0].Ref != f.ontologyRef {
		t.Fatalf("actual protected source: rows=%+v err=%v", rows, err)
	}
	body, err := service.MarshalOntologySource(*f.ontologySource)
	if err != nil || string(rows[0].Body) != string(body) || f.ontologyDigest == "" {
		t.Fatalf("stored definition differs from Writer/audit source: %v", err)
	}
	for _, index := range []int{51, 60, 61} {
		f.call("tx.apply", map[string]any{
			"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: model.RefByID(e3ID(52), model.Natural64(0)), To: model.RefByID(e3ID(index), model.Natural64(0)), LinkKind: "blocks"}}}},
			"_meta":     map[string]any{"project": e3Project, "idempotencyKey": e3Key("fixture-blocker-" + e3ID(index))},
		}, nil)
	}
	// A real deletion leaves a dangling link; it must stay unknown instead of
	// becoming an active blocker or a denied existing target.
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock()}, []conformance.FixtureChange{{Ref: model.RefByID(e3ID(61), model.Natural64(0)), Kind: "bead", Op: model.OpDelete, Graph: &conformance.FixtureGraphSource{}}}); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"viewId": e3ID(98), "itemId": e3ID(52), "_meta": map[string]any{"project": e3Project}}
	var result viewops.ExplainResult
	wire := f.call("view.explain", params, &result)
	if !result.Included || result.DirectBlockers == nil || result.DirectBlockers.HiddenCount != 1 || len(result.DirectBlockers.Edges) != 2 || strings.Contains(string(wire.Result), e3ID(60)) {
		t.Fatalf("readable/unknown/count-only HTTP result: %s", wire.Result)
	}
	for _, edge := range result.DirectBlockers.Edges {
		switch edge.Blocker.ID {
		case e3ID(51):
			if edge.Blocker != model.RefByID(e3ID(51), model.Natural64(0)) || edge.Kind != "blocks" || edge.State != viewops.DirectBlockerUnsatisfied || edge.Reason != "blocker status does not satisfy blocks" {
				t.Fatalf("readable authoritative edge: %+v", edge)
			}
		case e3ID(61):
			if edge.State != viewops.DirectBlockerUnknown {
				t.Fatalf("missing target became active: %+v", edge)
			}
		default:
			t.Fatalf("unexpected exposed edge: %+v", edge)
		}
	}
	writeE3JSON(t, filepath.Join(f.evidence, "configured-blocker-http.json"), map[string]any{"sourceHead": f.receipt.SourceHead, "sourceTree": f.receipt.SourceTree, "sourceRef": f.ontologyRef, "sourceRevision": f.ontologyRevision, "snapshotHead": f.meta().Head, "response": wire})
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	partial, err := f.browser.views.Explain(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, viewops.ExplainRequest{ViewID: e3ID(98), ItemID: e3ID(52), Project: e3Project})
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "busy" || partial.DirectBlockers != nil || partial.Included {
		t.Fatalf("canceled explain did not refuse atomically: %+v %v", partial, err)
	}
	// Invalid protected source must refuse the whole HTTP result, even with
	// already-readable blocker data; no partial successful edge may escape.
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock()}, []conformance.FixtureChange{{Ref: f.ontologyRef, Kind: "ontology", Op: model.OpPatch, Body: []byte(`{"base":{"unexpected":true}}`)}}); err != nil {
		t.Fatal(err)
	}
	f.expectCode("view.explain", params, "invalid_params")
}
