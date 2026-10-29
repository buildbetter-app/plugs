package plugingrants_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
)

type rpcError struct{ code string }

func (e *rpcError) Error() string { return e.code }

type rpcBridge struct {
	mu        sync.Mutex
	registry  *rpc.Registry
	transport *inprocess.Transport
	calls     []dispatchCall
}

type dispatchCall struct {
	Method    string
	Principal pluginapi.PrincipalRef
}

func (b *rpcBridge) Effect(method string) (pluginhost.Effect, bool) {
	d, ok := b.registry.Catalog().Descriptor(method)
	if !ok || d.Effect != "read" && d.Effect != "write" {
		return "", false
	}
	return pluginhost.Effect(d.Effect), true
}

func (b *rpcBridge) Call(ctx context.Context, principal pluginapi.PrincipalRef, method string, params json.RawMessage) (json.RawMessage, error) {
	b.mu.Lock()
	b.calls = append(b.calls, dispatchCall{Method: method, Principal: principal})
	transport := b.transport
	b.mu.Unlock()
	if transport == nil {
		return nil, errors.New("RPC bridge is not connected")
	}
	var by model.PrincipalRef
	principalJSON, err := json.Marshal(principal)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(principalJSON, &by); err != nil {
		return nil, err
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		return nil, err
	}
	request := []byte(`{"jsonrpc":"2.0","id":"plugin-call","method":`)
	request = append(request, methodJSON...)
	request = append(request, []byte(`,"params":`)...)
	request = append(request, params...)
	request = append(request, '}')
	response, err := transport.RoundTrip(rpc.WithPrincipal(ctx, by), request)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *wire.Error     `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		return nil, fmt.Errorf("decode RPC response %s: %w", response, err)
	}
	if envelope.Error != nil {
		return nil, &rpcError{code: envelope.Error.Data.Code}
	}
	if len(envelope.Result) == 0 {
		return nil, errors.New("RPC response omitted result")
	}
	return envelope.Result, nil
}

// The baseline has no installed authorization provider. Use the real optional
// chain rather than inventing an allowlist or a plugin authorization decision.
type permissionChain struct {
	chain authz.Chain
	mu    sync.Mutex
	calls []permissionCall
}

type callerKey struct{}

type permissionCall struct {
	Permission string
	Refs       []pluginapi.Ref
	Principal  model.PrincipalRef
}

func (g *permissionChain) Authorize(ctx context.Context, permission string, refs []pluginapi.Ref) error {
	by, ok := ctx.Value(callerKey{}).(model.PrincipalRef)
	if !ok {
		by = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	}
	g.mu.Lock()
	g.calls = append(g.calls, permissionCall{Permission: permission, Refs: append([]pluginapi.Ref(nil), refs...), Principal: by})
	g.mu.Unlock()
	resources := make([]model.Ref, len(refs))
	for i, ref := range refs {
		data, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &resources[i]); err != nil {
			return err
		}
	}
	return g.check(ctx, by, permission, "", resources)
}

func (g *permissionChain) check(ctx context.Context, by model.PrincipalRef, permission, method string, refs []model.Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requests := make([]authz.CheckRequest, len(refs))
	for i, ref := range refs {
		requests[i] = authz.CheckRequest{Caller: authz.Caller{Principal: by}, Action: authz.Action(permission), Resource: authz.Resource{Kind: authz.Bead, Ref: ref}, Method: method}
	}
	decisions, err := g.chain.Check(ctx, nil, requests)
	if err != nil {
		return err
	}
	for _, decision := range decisions {
		if decision.Effect != authz.Allow {
			return &model.Error{Data: model.ErrorData{Code: "permission_denied"}}
		}
	}
	return nil
}

func (g *permissionChain) snapshot() []permissionCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]permissionCall, len(g.calls))
	copy(out, g.calls)
	return out
}

// The Get boundary receives the principal parsed by the actual RPC server.
// Embedding forwards all other verbs to the real core service and pipeline.
type checkedService struct {
	rpc.Service
	permissions *permissionChain
	mu          sync.Mutex
	readers     []model.PrincipalRef
}

func (s *checkedService) Get(ctx context.Context, by model.PrincipalRef, request service.GetRequest) (service.GetResult, error) {
	s.mu.Lock()
	s.readers = append(s.readers, by)
	s.mu.Unlock()
	if err := s.permissions.check(ctx, by, "read", "bead.get", []model.Ref{request.ID}); err != nil {
		var policy *authz.Error
		if errors.As(err, &policy) {
			return service.GetResult{}, &model.Error{Data: model.ErrorData{Code: model.ErrorCode(policy.Code)}}
		}
		return service.GetResult{}, err
	}
	return s.Service.Get(ctx, by, request)
}

type probeState struct {
	mu            sync.Mutex
	resource      pluginapi.Ref
	privateSeen   bool
	budgetProof   bool
	boundaryProof bool
	storedReads   int
	lastError     string
}

func (p *probeState) probe(ctx context.Context, host pluginapi.Host, proposal pluginapi.ProposedChange, reader pluginapi.SnapshotReader) error {
	if len(proposal.Changes) != 1 {
		return fmt.Errorf("expected one proposed change, got %d", len(proposal.Changes))
	}
	data, err := json.Marshal(proposal)
	if err != nil {
		return err
	}
	if strings.Contains(string(data), "PRIVATE-SENTINEL") || strings.Contains(string(data), "fields.description") {
		p.mu.Lock()
		p.privateSeen = true
		p.mu.Unlock()
		return fmt.Errorf("undeclared proposal data leaked: %s", data)
	}
	resource := proposal.Changes[0].Resource.Ref
	p.mu.Lock()
	p.resource = resource
	p.mu.Unlock()
	records, err := reader.Get(ctx, []pluginapi.Ref{resource})
	if err != nil {
		return fmt.Errorf("first guarded point read: %w", err)
	}
	for _, record := range records {
		if err := p.checkStored(record); err != nil {
			return err
		}
	}
	if _, err := reader.Get(ctx, []pluginapi.Ref{resource}); hostCode(err) != pluginhost.ErrBudget {
		return fmt.Errorf("second point read exceeded narrowed budget: code=%q err=%v", hostCode(err), err)
	}
	if _, err := reader.Query(ctx, pluginapi.Query{Filter: map[string]any{"fields.private": "x"}}); hostCode(err) != pluginhost.ErrRead {
		return fmt.Errorf("undeclared query code=%q err=%v", hostCode(err), err)
	}
	page, err := reader.Query(ctx, pluginapi.Query{})
	if err != nil {
		return fmt.Errorf("first guarded query: %w", err)
	}
	for _, record := range page.Items {
		if err := p.checkStored(record); err != nil {
			return err
		}
	}
	if _, err := reader.Query(ctx, pluginapi.Query{}); hostCode(err) != pluginhost.ErrBudget {
		return fmt.Errorf("second query code=%q err=%v", hostCode(err), err)
	}
	if hostCode(host.Call(ctx, "bead.get", map[string]any{}, nil)) != pluginhost.ErrMethod {
		return fmt.Errorf("hot RPC read was not explicitly refused")
	}
	if hostCode(host.Call(ctx, "tx.apply", map[string]any{}, nil)) != pluginhost.ErrHotWrite {
		return fmt.Errorf("hot RPC write was not explicitly refused")
	}
	if _, err := host.State().Put(ctx, "hot", true, ""); hostCode(err) != pluginhost.ErrHotWrite {
		return fmt.Errorf("hot state write code=%q err=%v", hostCode(err), err)
	}
	if _, err := host.HTTP().Get("https://example.com"); hostCode(err) != pluginhost.ErrHotEgress {
		return fmt.Errorf("hot egress code=%q err=%v", hostCode(err), err)
	}
	p.mu.Lock()
	p.budgetProof = true
	p.boundaryProof = true
	p.mu.Unlock()
	return nil
}

func (p *probeState) checkStored(record pluginapi.Record) error {
	var body struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(record.Body, &body); err != nil {
		return err
	}
	// A2/A4 grants the union of reads and owns. The stored normalized value
	// is visible to its owner; the unrelated description must remain hidden.
	if len(body.Fields) != 2 || body.Fields["title"] != "hello" || body.Fields["normalized"] != "HELLO" || body.Fields["description"] != nil {
		return fmt.Errorf("pre snapshot leaked undeclared fields: %s", record.Body)
	}
	p.mu.Lock()
	p.storedReads++
	p.mu.Unlock()
	return nil
}

type postState struct {
	calls     atomic.Int32
	completed atomic.Bool
	revision  atomic.Value
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	failure   error
}

func (p *postState) after(ctx context.Context, host pluginapi.Host, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
	p.calls.Add(1)
	if len(change.Changes) != 1 {
		return fmt.Errorf("expected one committed change, got %d", len(change.Changes))
	}
	ref := change.Changes[0].Resource.Ref
	records, err := reader.Get(ctx, []pluginapi.Ref{ref})
	if err != nil {
		return fmt.Errorf("committed snapshot read: %w", err)
	}
	if len(records) != 1 {
		return fmt.Errorf("post read returned %d records, want 1", len(records))
	}
	if records[0].Revision.String() != change.Revision.String() {
		return fmt.Errorf("post read revision mismatch: recordRevision=%s callback=%s", records[0].Revision, change.Revision)
	}
	var body map[string]any
	if err := json.Unmarshal(records[0].Body, &body); err != nil {
		return err
	}
	fields, _ := body["fields"].(map[string]any)
	if fields["normalized"] != "HELLO" || fields["title"] != nil || fields["description"] != nil {
		return fmt.Errorf("post snapshot was not narrowed to the committed grant: %s", records[0].Body)
	}
	p.revision.Store(change.Revision.String())
	if hostCode(host.Call(ctx, "bead.get", map[string]any{}, nil)) != pluginhost.ErrMethod {
		return fmt.Errorf("post RPC read was not explicitly refused")
	}
	if hostCode(host.Call(ctx, "tx.apply", map[string]any{}, nil)) != pluginhost.ErrReentrant {
		return fmt.Errorf("post graph write was not explicitly refused")
	}
	if _, err := host.HTTP().Get("https://example.com"); hostCode(err) != pluginhost.ErrEgress {
		return fmt.Errorf("post egress code=%q err=%v", hostCode(err), err)
	}
	if p.entered != nil {
		p.enterOnce.Do(func() { close(p.entered) })
		<-p.release
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("post context canceled after commit: %w", err)
		}
		if _, err := reader.Get(ctx, []pluginapi.Ref{ref}); err != nil {
			return fmt.Errorf("committed snapshot unusable after caller cancellation: %w", err)
		}
	}
	p.completed.Store(true)
	return p.failure
}

type runtime struct {
	store     *memory.Store
	host      *pluginhost.Host
	transport *inprocess.Transport
	bridge    *rpcBridge
	gate      *permissionChain
	service   *checkedService
	probe     *probeState
	post      *postState
}

func newRuntime(t *testing.T, post *postState, options ...conformance.Options) *runtime {
	t.Helper()
	ctx := context.Background()
	store, err := memory.NewWithOptions(memory.Options{StoreID: driver.StoreID("plug-grants"), Incarnation: "plug-grants-inc"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "plug-grants-acceptance", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = lease.Release(context.Background())
		_ = store.Close()
	})
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	bridge := &rpcBridge{registry: registry}
	gate := &permissionChain{}
	probe := &probeState{}
	var fixtureOptions conformance.Options
	if len(options) != 0 {
		fixtureOptions = options[0]
	}
	fixtureOptions.Probe = func(ctx context.Context, host pluginapi.Host, proposal pluginapi.ProposedChange, reader pluginapi.SnapshotReader) error {
		err := probe.probe(ctx, host, proposal, reader)
		if err != nil {
			probe.mu.Lock()
			probe.lastError = err.Error()
			probe.mu.Unlock()
		}
		return err
	}
	fixtureOptions.After = post.after
	native := conformance.Native(fixtureOptions)
	constructor := native.New
	native.New = func(host pluginapi.Host) (sdk.Registration, error) {
		registration, err := constructor(host)
		if err != nil {
			return sdk.Registration{}, err
		}
		if len(registration.Methods) != 1 {
			return sdk.Registration{}, fmt.Errorf("fixture registration changed: %d methods", len(registration.Methods))
		}
		registration.Methods[0].Handle = func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(params, &input); err != nil || input.ID == "" {
				return nil, fmt.Errorf("fixture method requires id: %w", err)
			}
			for _, method := range []string{"server.hello", "bead.update", "missing.method"} {
				if err := host.Call(ctx, method, map[string]any{}, nil); hostCode(err) != pluginhost.ErrMethod {
					return nil, fmt.Errorf("unsupported/undeclared outbound method %s was not refused: %v", method, err)
				}
			}
			var result map[string]any
			if err := host.Call(ctx, "bead.get", map[string]any{"id": map[string]any{"id": input.ID}}, &result); err != nil {
				return nil, err
			}
			return json.Marshal(result)
		}
		return registration, nil
	}
	host, err := pluginhost.New(pluginhost.Config{
		Natives:    []sdk.Native{native},
		Bindings:   nativeBindings(),
		Dispatcher: bridge,
		Authorize:  gate.Authorize,
	})
	if err != nil {
		t.Fatal(err)
	}
	document := ontology.DefaultDocument()
	document.Types["issue"].Fields["normalized"] = ontology.Field{Type: "string", Owner: "normalize"}
	compiled, problems := ontology.Compile(document, nil, ontology.PluginCatalog{"normalize": true})
	if len(problems) != 0 {
		t.Fatalf("compile fixture ontology: %+v", problems)
	}
	rules := service.NewOntology(func(project string) ontology.Effective { return compiled.Effective(project) })
	writer := pipeline.New(store, lease,
		pipeline.WithOntology(rules),
		pipeline.WithMutators(host.Mutators()...),
		pipeline.WithValidators(host.Validators()...),
		pipeline.WithPostSubscribers(host.Posts()...),
	)
	domain := service.New(store, lease, writer, service.WithOntology(rules))
	checked := &checkedService{Service: domain, permissions: gate}
	if err := rpc.BindService(registry, checked); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "demo", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	bridge.mu.Lock()
	bridge.transport = inprocess.New(server)
	bridge.mu.Unlock()
	return &runtime{store: store, host: host, transport: bridge.transport, bridge: bridge, gate: gate, service: checked, probe: probe, post: post}
}

func nativeBindings() []pluginhost.Binding {
	return []pluginhost.Binding{
		{ID: "normalize", Alias: "fixture", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}, MaxPointReads: 1},
		{ID: "check", Alias: "fixture", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.normalized"}},
		{ID: "observe", Alias: "fixture", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.normalized"}},
	}
}

func hostCode(err error) string {
	var e *pluginhost.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func callRPC(t *testing.T, transport *inprocess.Transport, ctx context.Context, method string, params any) (json.RawMessage, error) {
	t.Helper()
	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		return nil, err
	}
	request := []byte(`{"jsonrpc":"2.0","id":1,"method":`)
	request = append(request, methodJSON...)
	request = append(request, []byte(`,"params":`)...)
	request = append(request, encoded...)
	request = append(request, '}')
	response, err := transport.RoundTrip(ctx, request)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *wire.Error     `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		return nil, fmt.Errorf("decode response %s: %w", response, err)
	}
	if envelope.Error != nil {
		return nil, &rpcError{code: envelope.Error.Data.Code}
	}
	return envelope.Result, nil
}

func decodeObject(t *testing.T, data json.RawMessage) map[string]any {
	t.Helper()
	value, err := wire.DecodeJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object, got %T", value)
	}
	return object
}

func idempotencyKey(suffix string) string {
	return strings.Repeat("a", 64) + suffix
}

func createParams(key string) map[string]any {
	return map[string]any{
		"type": "issue",
		"fields": map[string]any{
			"title":       "hello",
			"description": "PRIVATE-SENTINEL",
		},
		"_meta": map[string]any{"project": "demo", "idempotencyKey": key},
	}
}

func createdID(t *testing.T, result map[string]any) string {
	t.Helper()
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("missing create result: %v", result)
	}
	row := rows[0].(map[string]any)
	ref := row["ref"].(map[string]any)
	id, ok := ref["id"].(string)
	if !ok || id == "" {
		t.Fatalf("missing created id: %v", row)
	}
	return id
}

func requireRPCCode(t *testing.T, err error, want string) {
	t.Helper()
	var got *rpcError
	if !errors.As(err, &got) || got.code != want {
		t.Fatalf("RPC error=%v, want %s", err, want)
	}
}

func TestNativeManifestGrantsThroughRPCAndDurablePost(t *testing.T) {
	post := &postState{failure: errors.New("post subscriber unavailable")}
	r := newRuntime(t, post)

	createdRaw, err := callRPC(t, r.transport, context.Background(), "bead.create", createParams(idempotencyKey("01")))
	if err != nil {
		t.Fatal(err)
	}
	created := decodeObject(t, createdRaw)
	id := createdID(t, created)
	if warnings, ok := created["warnings"].([]any); !ok || len(warnings) != 1 || warnings[0].(map[string]any)["code"] != "post_subscriber_failed" {
		t.Fatalf("committed post warning was not returned by RPC: %v", created)
	}
	if r.probe.privateSeen || !r.probe.budgetProof || !r.probe.boundaryProof {
		t.Fatalf("native pre-grant proof incomplete: %+v", r.probe)
	}
	if r.post.calls.Load() != 1 || !r.post.completed.Load() {
		t.Fatalf("post boundary checks did not complete: count=%d completed=%v", r.post.calls.Load(), r.post.completed.Load())
	}
	if got := r.post.revision.Load().(string); got != "1" {
		t.Fatalf("post read revision=%s, want committed revision 1", got)
	}
	// The bounded RPC registry does not yet publish native x.* methods.
	// Preserve its explicit refusal; enter the extension through the real host.
	_, err = callRPC(t, r.transport, context.Background(), "x.fixture.echo", map[string]any{"id": id})
	requireRPCCode(t, err, "method_not_found")

	// The embedded native extension method selects its declared params.id
	// resource, then reaches the real bead.get RPC with the plugin principal.
	methodResult, err := r.host.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(fmt.Sprintf(`{"id":%q}`, id)))
	if err != nil {
		t.Fatal(err)
	}
	methodObject := decodeObject(t, methodResult)
	bead := methodObject["bead"].(map[string]any)
	if bead["id"] != id || bead["fields"].(map[string]any)["normalized"] != "HELLO" || bead["fields"].(map[string]any)["description"] != "PRIVATE-SENTINEL" {
		t.Fatalf("extension RPC returned wrong scoped record: %v", methodObject)
	}
	calls := r.gate.snapshot()
	if len(calls) != 1 || calls[0].Permission != "read" || len(calls[0].Refs) != 1 || calls[0].Refs[0].ID != id {
		t.Fatalf("declared method resource grant was not enforced: %+v", calls)
	}
	r.bridge.mu.Lock()
	if len(r.bridge.calls) != 1 || r.bridge.calls[0].Method != "bead.get" || r.bridge.calls[0].Principal.Kind != "plugin" || r.bridge.calls[0].Principal.ID != "app.example.fixture/default" {
		t.Fatalf("extension did not use the real RPC dispatcher: %+v", r.bridge.calls)
	}
	r.bridge.mu.Unlock()
	r.service.mu.Lock()
	if len(r.service.readers) != 1 || r.service.readers[0].Kind != "plugin" || r.service.readers[0].ID != "app.example.fixture/default" {
		t.Fatalf("RPC did not preserve the plugin principal into core service: %+v", r.service.readers)
	}
	r.service.mu.Unlock()

	// Without an authorization provider, ordinary principals are allowed and
	// a missing resource reaches the core's real not_found result. A scoped
	// principal explicitly refuses rather than pretending to enforce caveats.
	_, err = r.host.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{"id":"pl-foreign"}`))
	requireRPCCode(t, err, "not_found")
	scoped := model.PrincipalRef{Kind: "user", ID: "reader", Caveats: map[string]any{"refs": []model.Ref{model.RefByID(id, model.Natural64(0))}}}
	_, err = r.host.Invoke(context.WithValue(context.Background(), callerKey{}, scoped), "x.fixture.echo", json.RawMessage(fmt.Sprintf(`{"id":%q}`, id)))
	var policy *authz.Error
	if !errors.As(err, &policy) || policy.Code != "authz_not_installed" {
		t.Fatalf("scoped extension caller bypassed optional authorization chain: %v", err)
	}
	_, err = callRPC(t, r.transport, rpc.WithPrincipal(context.Background(), scoped), "bead.get", map[string]any{"id": map[string]any{"id": id}})
	requireRPCCode(t, err, "authz_not_installed")

	// A retry is a durable receipt replay: it keeps the exact warning and does
	// not rerun the native post callback or graph transaction.
	replayedRaw, err := callRPC(t, r.transport, context.Background(), "bead.create", createParams(idempotencyKey("01")))
	if err != nil {
		t.Fatal(err)
	}
	if string(replayedRaw) != string(createdRaw) {
		t.Fatalf("durable replay changed response bytes:\nfirst=%s\nreplay=%s", createdRaw, replayedRaw)
	}
	if r.post.calls.Load() != 1 {
		t.Fatalf("durable replay reran post callback: %d", r.post.calls.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.bridge.mu.Lock()
	before := len(r.bridge.calls)
	r.bridge.mu.Unlock()
	if _, err := r.host.Invoke(ctx, "x.fixture.echo", json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native method was not refused: %v", err)
	}
	r.bridge.mu.Lock()
	if len(r.bridge.calls) != before {
		t.Fatal("canceled native method reached the RPC dispatcher")
	}
	r.bridge.mu.Unlock()
	meta, err := r.store.Meta(context.Background())
	if err != nil || meta.Head.String() != "1" {
		t.Fatalf("refused subscriber writes or replay changed graph head: %+v, %v", meta, err)
	}
}

func TestNativePostSurvivesCallerCancellationAfterCommit(t *testing.T) {
	post := &postState{entered: make(chan struct{}), release: make(chan struct{})}
	r := newRuntime(t, post)
	requestDone := make(chan struct{})
	var result json.RawMessage
	var callErr error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() {
		select {
		case <-post.release:
		default:
			close(post.release)
		}
	}()
	go func() {
		result, callErr = callRPC(t, r.transport, ctx, "bead.create", createParams(idempotencyKey("02")))
		close(requestDone)
	}()
	select {
	case <-post.entered:
	case <-time.After(time.Second):
		t.Fatal("native post callback did not receive the committed snapshot")
	}
	cancel()
	close(post.release)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("post completion did not return after caller cancellation")
	}
	if callErr != nil {
		t.Fatal(callErr)
	}
	if len(result) == 0 || post.calls.Load() != 1 || !post.completed.Load() {
		t.Fatalf("canceled caller lost committed post result: result=%s calls=%d", result, post.calls.Load())
	}
	if warnings, _ := decodeObject(t, result)["warnings"].([]any); len(warnings) != 0 {
		t.Fatalf("caller cancellation became a post failure: %v", warnings)
	}
	if got := post.revision.Load().(string); got != "1" {
		t.Fatalf("post snapshot revision=%s, want 1", got)
	}
}

func TestNativePreReadsMaskStoredFieldsThroughRPC(t *testing.T) {
	r := newRuntime(t, &postState{})
	created, err := callRPC(t, r.transport, context.Background(), "bead.create", createParams(idempotencyKey("03")))
	if err != nil {
		t.Fatal(err)
	}
	id := createdID(t, decodeObject(t, created))
	_, err = callRPC(t, r.transport, context.Background(), "bead.update", map[string]any{
		"id":     map[string]any{"id": id},
		"fields": map[string]any{"description": "PRIVATE-SENTINEL-UPDATED"},
		"_meta":  map[string]any{"project": "demo", "idempotencyKey": idempotencyKey("04")},
	})
	if err != nil {
		t.Fatalf("update: %v (pre probe: %s)", err, r.probe.lastError)
	}
	r.probe.mu.Lock()
	reads := r.probe.storedReads
	r.probe.mu.Unlock()
	if reads < 2 {
		t.Fatalf("point and query reads did not exercise stored-field masking: %d", reads)
	}
	if r.post.calls.Load() != 2 || r.post.revision.Load().(string) != "2" {
		t.Fatalf("post did not observe the second committed snapshot: calls=%d revision=%v", r.post.calls.Load(), r.post.revision.Load())
	}
}

func TestNativeMutatorCannotPatchUnownedFieldThroughRPC(t *testing.T) {
	r := newRuntime(t, &postState{}, conformance.Options{PatchPath: "fields.description"})
	_, err := callRPC(t, r.transport, context.Background(), "bead.create", createParams(idempotencyKey("05")))
	requireRPCCode(t, err, "subscriber_unavailable")
	if !r.probe.budgetProof || !r.probe.boundaryProof {
		t.Fatal("mutator failed before returning the unowned patch")
	}
	meta, err := r.store.Meta(context.Background())
	if err != nil || !meta.Head.IsZero() || r.post.calls.Load() != 0 {
		t.Fatalf("out-of-grant mutator reached commit/post: meta=%+v posts=%d err=%v", meta, r.post.calls.Load(), err)
	}
}

func TestNativeRegistrationAndResourceSelectorRefusals(t *testing.T) {
	cases := []struct {
		name    string
		change  func(*sdk.Registration)
		invoke  bool
		wantErr string
	}{
		{name: "undeclared method", change: func(r *sdk.Registration) { r.Methods[0].Descriptor.Name = "x.fixture.undeclared" }, wantErr: pluginhost.ErrMethod},
		{name: "wider permission", change: func(r *sdk.Registration) { r.Methods[0].Descriptor.Permission = "modify" }, wantErr: pluginhost.ErrMethod},
		{name: "missing resource selector", change: func(r *sdk.Registration) { r.Methods[0].Descriptor.Resource = nil }, wantErr: pluginhost.ErrMethod},
		{name: "missing subscriber", change: func(r *sdk.Registration) { delete(r.Mutators, "normalize") }, wantErr: pluginhost.ErrManifest},
		{name: "selector omits declared resource", change: func(r *sdk.Registration) {
			r.Methods[0].Descriptor.Resource = func(any) []pluginapi.Ref { return []pluginapi.Ref{pluginapi.RefByID("pl-other", "")} }
		}, invoke: true, wantErr: pluginhost.ErrMethod},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			native := conformance.Native(conformance.Options{})
			constructor := native.New
			native.New = func(host pluginapi.Host) (sdk.Registration, error) {
				registration, err := constructor(host)
				if err == nil {
					tc.change(&registration)
				}
				return registration, err
			}
			permissions := &permissionChain{}
			host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, Bindings: nativeBindings(), Authorize: permissions.Authorize})
			if tc.invoke {
				if err != nil {
					t.Fatal(err)
				}
				_, err = host.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{"id":"pl-declared"}`))
			}
			if hostCode(err) != tc.wantErr {
				t.Fatalf("registration/selector refusal=%v, want %s", err, tc.wantErr)
			}
			if calls := permissions.snapshot(); len(calls) != 0 {
				t.Fatalf("invalid registration/selector reached authorization: %+v", calls)
			}
		})
	}
}

func TestNativeBindingCannotWidenManifestGrant(t *testing.T) {
	cases := []struct {
		name   string
		change func([]pluginhost.Binding)
	}{
		{name: "read field", change: func(b []pluginhost.Binding) { b[0].Reads = []string{"fields.private"} }},
		{name: "owned field", change: func(b []pluginhost.Binding) { b[0].Owns = []string{"fields.private"} }},
		{name: "point budget", change: func(b []pluginhost.Binding) { b[0].MaxPointReads = 3 }},
		{name: "query budget", change: func(b []pluginhost.Binding) { b[0].MaxQueries = 2 }},
		{name: "callback timeout", change: func(b []pluginhost.Binding) { b[0].Timeout = 2 * time.Second }},
		{name: "unsupported project binding", change: func(b []pluginhost.Binding) { b[0].Project = "demo" }},
		{name: "unsupported type binding", change: func(b []pluginhost.Binding) { b[0].Type = "issue" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			native := conformance.Native(conformance.Options{})
			bindings := nativeBindings()
			tc.change(bindings)
			_, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, Bindings: bindings})
			if hostCode(err) != pluginhost.ErrManifest {
				t.Fatalf("grant widening was accepted: code=%q err=%v", hostCode(err), err)
			}
		})
	}
}
