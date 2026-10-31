package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
)

func fixtureBindings() []Binding {
	return []Binding{
		{ID: "normalize", Alias: "fixture", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}},
		{ID: "check", Alias: "fixture", Instance: "default", Subscriber: "check", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.normalized"}},
		{ID: "observe", Alias: "fixture", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.normalized"}},
	}
}

func proposal() port.ProposedChange {
	ref := model.RefByID("pl-a", "")
	return port.ProposedChange{RequestID: "request-1", Principal: model.PrincipalRef{Kind: "user", ID: "u"}, BaseRevision: model.Natural64(7), Changes: []port.Change{{Resource: model.ResourceRef{Kind: "bead", Ref: ref}, Op: model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: map[string]any{"title": "hello", "private": "PRIVATE-SENTINEL"}}}, Before: json.RawMessage(`{"id":"pl-a","type":"task","fields":{"title":"old","private":"PRIVATE-SENTINEL"}}`), After: json.RawMessage(`{"id":"pl-a","type":"task","fields":{"title":"hello","private":"PRIVATE-SENTINEL"}}`), SetBy: map[string]string{"fields.title": "request", "fields.private": "request"}}}}
}

type snapshot struct {
	at            model.Revision
	gets, queries int
	query         driver.Query
}

func (s *snapshot) Snapshot() driver.View {
	at := s.at
	if at == "" {
		at = model.Natural64(7)
	}
	return driver.View{At: &at, Incarnation: "inc-1"}
}
func (s *snapshot) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	s.gets++
	out := make([]driver.Record, len(refs))
	for j, ref := range refs {
		out[j] = driver.Record{Ref: ref, Kind: "bead", Revision: model.Natural64(7), Body: []byte(`{"id":"pl-a","type":"task","fields":{"title":"stored","private":"PRIVATE-SENTINEL"}}`)}
	}
	return out, nil
}
func (s *snapshot) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	s.queries++
	s.query = q
	values, _ := s.Get(ctx, []model.Ref{model.RefByID("pl-a", "")})
	return driver.Page[driver.Record]{Items: values, AsOf: *s.Snapshot().At}, nil
}
func (s *snapshot) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) {
	return nil, fmt.Errorf("Observe is not exposed to the guest")
}

type dispatcher struct{ calls int }

func (*dispatcher) Effect(method string) (Effect, bool) {
	switch method {
	case "tx.apply", "bead.update":
		return Write, true
	case "bead.get", "bead.ready":
		return Read, true
	}
	return "", false
}
func (d *dispatcher) Call(context.Context, pluginapi.PrincipalRef, string, json.RawMessage) (json.RawMessage, error) {
	d.calls++
	return json.RawMessage(`{}`), nil
}

func code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
func requireCode(err error, want string) error {
	if code(err) != want {
		return fmt.Errorf("got %v, want %s", err, want)
	}
	return nil
}

func TestNativeSubscriberConformance(t *testing.T) {
	store := &snapshot{}
	dispatch := &dispatcher{}
	probe := func(ctx context.Context, h pluginapi.Host, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) error {
		data, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "PRIVATE-SENTINEL") || strings.Contains(string(data), "fields.private") {
			return fmt.Errorf("undeclared data leaked: %s", data)
		}
		records, err := r.Get(context.Background(), []pluginapi.Ref{pluginapi.RefByID("pl-a", "")})
		if err != nil {
			return err
		}
		data, _ = json.Marshal(records)
		if strings.Contains(string(data), "PRIVATE-SENTINEL") {
			return fmt.Errorf("reader leaked undeclared field")
		}
		if err = requireCode(h.Call(context.Background(), "bead.get", map[string]any{"id": "pl-a"}, nil), ErrMethod); err != nil {
			return err
		}
		if _, err = r.Get(ctx, []pluginapi.Ref{pluginapi.RefByID("pl-a", "")}); err != nil {
			return err
		}
		_, err = r.Get(ctx, []pluginapi.Ref{pluginapi.RefByID("pl-b", "")})
		if err = requireCode(err, ErrBudget); err != nil {
			return err
		}
		_, err = r.Query(ctx, pluginapi.Query{Filter: map[string]any{"fields.private": "x"}})
		if err = requireCode(err, ErrRead); err != nil {
			return err
		}
		if _, err = r.Query(ctx, pluginapi.Query{}); err != nil {
			return err
		}
		_, err = r.Query(ctx, pluginapi.Query{})
		if err = requireCode(err, ErrBudget); err != nil {
			return err
		}
		if err = requireCode(h.Call(context.Background(), "tx.apply", map[string]any{}, nil), ErrHotWrite); err != nil {
			return err
		}
		if err = requireCode(h.Call(ctx, "bead.transitions", map[string]any{}, nil), ErrMethod); err != nil {
			return err
		}
		_, err = h.State().Put(context.Background(), "key", true, "")
		if err = requireCode(err, ErrHotWrite); err != nil {
			return err
		}
		_, err = h.HTTP().Get("https://example.com")
		return requireCode(err, ErrHotEgress)
	}
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{Probe: probe})}, Bindings: fixtureBindings(), Dispatcher: dispatch})
	if err != nil {
		t.Fatal(err)
	}
	var mutator port.Mutator
	for _, subscriber := range h.Subscribers() {
		if subscriber.Mutator != nil {
			mutator = subscriber.Mutator
		}
	}
	c := proposal()
	patches, err := mutator.Mutate(context.Background(), c, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(patches) != 1 || string(patches[0].Paths["fields.normalized"]) != `"HELLO"` {
		t.Fatalf("allowed patch not produced: %#v", patches)
	}
	if dispatch.calls != 0 || store.queries != 1 || store.gets != 3 {
		t.Fatalf("boundary escaped or over-budget read dispatched: calls=%d gets=%d queries=%d", dispatch.calls, store.gets, store.queries)
	}
	if store.query.AsOf == nil || store.query.AsOf.String() != "7" {
		t.Fatal("query was not pinned")
	}
	if string(c.Changes[0].After) != string(proposal().Changes[0].After) {
		t.Fatal("callback mutated core proposal")
	}
}

func TestUndeclaredPatchAndMethodRefused(t *testing.T) {
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{PatchPath: "fields.private"})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range h.Subscribers() {
		if s.Mutator != nil {
			patches, err := s.Mutator.Mutate(context.Background(), proposal(), &snapshot{})
			if code(err) != ErrPatch || len(patches) != 0 {
				t.Fatalf("undeclared patch accepted: %v %v", patches, err)
			}
		}
	}
	if _, err = h.Invoke(context.Background(), "x.fixture.undeclared", json.RawMessage(`{"id":"pl-a"}`)); code(err) != ErrMethod {
		t.Fatalf("undeclared method: %v", err)
	}
	result, err := h.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{"id":"pl-a","value":9007199254740993}`))
	if err != nil || !strings.Contains(string(result), "9007199254740993") {
		t.Fatalf("allowed method: %s %v", result, err)
	}
	if _, err = h.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{}`)); code(err) != ErrMethod {
		t.Fatal("missing resource accepted")
	}
	native := conformance.Native(conformance.Options{})
	constructor := native.New
	native.New = func(host pluginapi.Host) (sdk.Registration, error) {
		registration, err := constructor(host)
		registration.Methods[0].Descriptor.Name = "bead.echo"
		return registration, err
	}
	if _, err = New(Config{Natives: []sdk.Native{native}}); code(err) != ErrMethod {
		t.Fatalf("foreign method registered: %v", err)
	}
}

func TestNativeCallbackPatchScopeIsolated(t *testing.T) {
	native := conformance.Native(conformance.Options{Probe: func(_ context.Context, _ pluginapi.Host, c pluginapi.ProposedChange, _ pluginapi.SnapshotReader) error {
		// Callback input is scratch space; it cannot redefine the host's
		// original set of live resources eligible for returned patches.
		c.Changes[0].Resource.Ref = pluginapi.RefByID("pl-b", "")
		c.Changes[0].After = json.RawMessage(`{"fields":{"title":"other"}}`)
		return nil
	}})
	h, err := New(Config{Natives: []sdk.Native{native}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	c := proposal()
	before, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	patches, err := h.Mutators()[0].Mutator.Mutate(context.Background(), c, &snapshot{})
	if code(err) != ErrPatch || len(patches) != 0 {
		t.Fatalf("callback input changed patch scope: %v %v", patches, err)
	}
	after, err := json.Marshal(c)
	if err != nil || string(after) != string(before) {
		t.Fatalf("callback input changed the core proposal: %s, %v", after, err)
	}
}

func TestBindingLimitsAndSnapshotRefusal(t *testing.T) {
	for _, change := range []func(*Binding){func(b *Binding) { b.Reads = []string{"fields.private"} }, func(b *Binding) { b.Owns = []string{"fields.private"} }, func(b *Binding) { b.MaxPointReads = 3 }, func(b *Binding) { b.MaxQueries = 2 }, func(b *Binding) { b.Timeout = 2 * time.Second }, func(b *Binding) { b.Project = "unrouted" }} {
		bindings := fixtureBindings()
		change(&bindings[0])
		if _, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: bindings}); code(err) != ErrManifest {
			t.Fatalf("widened binding accepted: %v", err)
		}
	}
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range h.Subscribers() {
		if s.Mutator != nil {
			if _, err := s.Mutator.Mutate(context.Background(), proposal(), &snapshot{at: "8"}); code(err) != ErrRead {
				t.Fatalf("wrong snapshot accepted: %v", err)
			}
		}
	}
}

func TestMaskHidesUndeclaredRelationshipPayload(t *testing.T) {
	change := pluginapi.Change{Resource: pluginapi.ResourceRef{Kind: "bead", Ref: pluginapi.RefByID("pl-a", "")}, Op: pluginapi.Op{Kind: "link", Payload: json.RawMessage(`{"kind":"link","from":{"id":"pl-a"},"to":{"id":"PRIVATE-RELATED-ID"},"linkKind":"blocks"}`)}, After: json.RawMessage(`{"fields":{"title":"hello","private":"PRIVATE-FIELD"}}`)}
	masked, err := maskChange(change, []string{"fields.title"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(masked)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "PRIVATE-") {
		t.Fatalf("relationship payload leaked undeclared data: %s", data)
	}
}

func orderNative(alias, own, read string) sdk.Native {
	return sdk.Native{Manifest: pluginapi.PluginManifest{ID: "app.example." + alias, Alias: alias, Version: "1.0.0", PluginAPI: pluginapi.APIVersion, Runtime: "native", Provides: []pluginapi.Provision{{Kind: "subscriber", Name: "mutate", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}, Reads: []string{read}, Owns: []string{own}}}}, Instance: "default", New: func(pluginapi.Host) (sdk.Registration, error) {
		return sdk.Registration{Mutators: map[string]pluginapi.Mutator{"mutate": sdk.MutateFunc(func(context.Context, pluginapi.ProposedChange, pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
			return nil, nil
		})}}, nil
	}}
}
func orderBinding(alias, own, read string) Binding {
	return Binding{ID: alias, Alias: alias, Instance: "default", Subscriber: "mutate", Phase: pluginapi.PhaseMutate, Reads: []string{read}, Owns: []string{own}}
}

func TestDeterministicMutatorOrder(t *testing.T) {
	a := orderNative("alpha", "fields.a", "fields.b")
	b := orderNative("beta", "fields.b", "fields.title")
	bindings := []Binding{orderBinding("alpha", "fields.a", "fields.b"), orderBinding("beta", "fields.b", "fields.title")}
	for _, natives := range [][]sdk.Native{{a, b}, {b, a}} {
		h, err := New(Config{Natives: natives, Bindings: bindings})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(h.BindingOrder(), []string{"beta", "alpha"}) {
			t.Fatal(h.BindingOrder())
		}
	}
	for _, test := range []struct{ own, read string }{{"fields.a", "fields.title"}, {"fields.b", "fields.a"}} {
		native := orderNative("beta", test.own, test.read)
		bindings[1] = orderBinding("beta", test.own, test.read)
		if _, err := New(Config{Natives: []sdk.Native{a, native}, Bindings: bindings}); code(err) != ErrManifest {
			t.Fatalf("cycle or ownership conflict accepted: %v", err)
		}
	}
}

func TestBindingRuntimePoolUsesDistinctInstancesAndSharedState(t *testing.T) {
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.bindings) == 0 || h.bindings[0].pool == nil {
		t.Fatal("binding runtime pool was not initialized")
	}
	pool := h.bindings[0].pool
	members := make([]*instance, 0, maxRuntimeInstances)
	for i := 0; i < maxRuntimeInstances; i++ {
		select {
		case member := <-pool.available:
			members = append(members, member)
		default:
			t.Fatalf("pool exposed %d members, want %d", len(members), maxRuntimeInstances)
		}
	}
	for _, member := range members {
		pool.available <- member
	}
	for i := range members {
		if members[i].state != h.instances["fixture/default"].state {
			t.Fatalf("pool member %d did not share the host-side state store", i)
		}
		for j := i + 1; j < len(members); j++ {
			if members[i] == members[j] {
				t.Fatalf("pool members %d and %d alias the same runtime instance", i, j)
			}
		}
	}
}

func TestBindingRuntimePoolReturnsBusyWhenQueueIsFull(t *testing.T) {
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	pool := h.bindings[0].pool
	started := make(chan struct{}, maxRuntimeInstances)
	release := make(chan struct{})
	var group sync.WaitGroup
	for i := 0; i < maxRuntimeInstances; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			err := pool.run(context.Background(), pluginapi.PhaseMutate, time.Second, nil, func(_ *instance, _ context.Context) error {
				started <- struct{}{}
				<-release
				return nil
			})
			if err != nil {
				t.Errorf("pool member callback failed: %v", err)
			}
		}()
	}
	for i := 0; i < maxRuntimeInstances; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("pool member did not start")
		}
	}
	queueCalled := make(chan struct{}, 1)
	err = pool.run(context.Background(), pluginapi.PhaseMutate, time.Second, nil, func(*instance, context.Context) error {
		queueCalled <- struct{}{}
		return nil
	})
	if code(err) != ErrPluginBusy {
		t.Fatalf("queue overflow error=%v, want %s", err, ErrPluginBusy)
	}
	select {
	case <-queueCalled:
		t.Fatal("queue-overflow callback unexpectedly ran")
	default:
	}
	close(release)
	group.Wait()
}

func TestBindingRuntimePoolReplacesFailedMemberWithFreshGuest(t *testing.T) {
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	p := h.bindings[0].pool
	var failed *instance
	err = p.run(context.Background(), pluginapi.PhaseMutate, time.Second, nil, func(member *instance, _ context.Context) error {
		failed = member
		return errors.New("guest callback failed")
	}, "principal")
	if err == nil {
		t.Fatal("failed callback unexpectedly succeeded")
	}
	if failed == nil || !failed.isDisposed() {
		t.Fatalf("failed callback member was not disposed: member=%p", failed)
	}
	deadline := time.After(time.Second)
	for len(p.available) != maxRuntimeInstances {
		select {
		case <-deadline:
			t.Fatalf("pool did not refill after failed member: available=%d", len(p.available))
		case <-time.After(time.Millisecond):
		}
	}
	for i := 0; i < maxRuntimeInstances; i++ {
		member := <-p.available
		if member == failed {
			t.Fatal("disposed callback member returned to the pool")
		}
		if member.isDisposed() {
			t.Fatal("disposed replacement member returned to the pool")
		}
		p.available <- member
	}
}

func TestPostRestrictionsStateAndRetainedHTTP(t *testing.T) {
	var active *http.Client
	native := conformance.Native(conformance.Options{After: func(ctx context.Context, h pluginapi.Host, _ pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
		active = h.HTTP()
		if err := requireCode(h.Call(context.Background(), "tx.apply", map[string]any{}, nil), ErrReentrant); err != nil {
			return err
		}
		rev, err := h.State().Put(ctx, "delivery", true, "")
		if err != nil {
			return err
		}
		if rev.String() != "1" {
			return fmt.Errorf("bad state revision")
		}
		if _, err = h.State().Put(ctx, "delivery", false, ""); err == nil {
			return fmt.Errorf("stale CAS accepted")
		}
		var value bool
		found, err := h.State().Get(ctx, "delivery", &value)
		if err != nil || !found || !value {
			return fmt.Errorf("state did not roundtrip")
		}
		_, err = active.Get("https://not-declared.example")
		return requireCode(err, ErrEgress)
	}})
	h, err := New(Config{Natives: []sdk.Native{native}, Bindings: fixtureBindings(), Dispatcher: &dispatcher{}})
	if err != nil {
		t.Fatal(err)
	}
	c := proposal()
	for _, s := range h.Subscribers() {
		if s.Post != nil {
			if err = s.Post.After(context.Background(), port.CommittedChange{RequestID: c.RequestID, Principal: c.Principal, Revision: c.BaseRevision, Changes: c.Changes}, &snapshot{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if active == nil {
		t.Fatal("post callback did not expose its active host")
	}
	if _, err = active.Get("https://example.com"); code(err) != ErrInactive {
		t.Fatalf("retained client worked outside callback: %v", err)
	}
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "fe80::1", "::ffff:127.0.0.1"} {
		if publicIP(net.ParseIP(ip)) {
			t.Fatalf("non-public IP accepted: %s", ip)
		}
	}
}

func TestNativeDeadlineRevokesRetainedCapabilities(t *testing.T) {
	started := make(chan pluginapi.SnapshotReader, 1)
	release := make(chan struct{})
	exited := make(chan struct{})
	var retained pluginapi.Host
	native := conformance.Native(conformance.Options{Probe: func(_ context.Context, h pluginapi.Host, _ pluginapi.ProposedChange, r pluginapi.SnapshotReader) error {
		retained = h
		started <- r
		<-release
		close(exited)
		return nil
	}})
	bindings := fixtureBindings()
	bindings[0].Timeout = 10 * time.Millisecond
	h, err := New(Config{Natives: []sdk.Native{native}, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	store := &snapshot{}
	for _, subscriber := range h.Subscribers() {
		if subscriber.Mutator != nil {
			patches, err := subscriber.Mutator.Mutate(context.Background(), proposal(), store)
			if !errors.Is(err, context.DeadlineExceeded) || len(patches) != 0 {
				close(release)
				t.Fatalf("native callback ignored deadline: %v %v", patches, err)
			}
		}
	}
	r := <-started
	if _, err := r.Get(context.Background(), []pluginapi.Ref{pluginapi.RefByID("pl-a", "")}); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("retained reader outlived call: %v", err)
	}
	if _, err := retained.HTTP().Get("https://example.com"); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("retained HTTP outlived call: %v", err)
	}
	if store.gets != 0 {
		close(release)
		t.Fatal("expired read reached core")
	}
	close(release)
	<-exited
}

func TestExtensionAuthorizationUsesDeclaredResources(t *testing.T) {
	denied := errors.New("denied")
	var permission string
	var resources []pluginapi.Ref
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Authorize: func(_ context.Context, p string, r []pluginapi.Ref) error {
		permission = p
		resources = r
		return denied
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{"id":"pl-a"}`)); !errors.Is(err, denied) {
		t.Fatalf("authorization bypassed: %v", err)
	}
	if permission != "read" || len(resources) != 1 || resources[0].ID != "pl-a" {
		t.Fatalf("wrong permission/resources: %s %#v", permission, resources)
	}
	if _, err := h.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{"id":{"id":"pl-a"}}`)); code(err) != ErrMethod {
		t.Fatalf("resource extractor panic escaped: %v", err)
	}
}
