package claimrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type releaseProbe struct {
	called bool
	by     model.PrincipalRef
	ref    model.Ref
}

func TestReleaseRPCUsesRealServiceWriterAndDurableReplay(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "claimrpc", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	now := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	o := service.NewOntology(nil)
	w := pipeline.New(store, lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return now }))
	svc := service.New(store, lease, w, service.WithOntology(o), service.WithClock(func() time.Time { return now }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, svc); err != nil {
		t.Fatal(err)
	}
	if err := Bind(registry, svc); err != nil {
		t.Fatal(err)
	}
	if err := Bind(registry, svc); err == nil {
		t.Fatal("duplicate release binding succeeded")
	}
	m, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: m.Incarnation, OntologyRevision: model.Natural64(1), DefaultProject: "local"})
	if err != nil {
		t.Fatal(err)
	}
	sequence := 0
	call := func(callCtx context.Context, method string, params map[string]any, key string) map[string]any {
		t.Helper()
		sequence++
		if key == "" {
			key = fmt.Sprintf("%032x", sequence)
		}
		if params["_meta"] == nil {
			params["_meta"] = map[string]any{"project": "local", "agent": "alice", "idempotencyKey": key}
		}
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": sequence, "method": method, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		response := server.Dispatch(callCtx, raw)
		var envelope map[string]any
		if err := json.Unmarshal(response, &envelope); err != nil {
			t.Fatalf("response=%s err=%v", response, err)
		}
		return envelope
	}
	result := func(envelope map[string]any) map[string]any {
		t.Helper()
		if envelope["error"] != nil {
			t.Fatalf("RPC error: %+v", envelope)
		}
		v, ok := envelope["result"].(map[string]any)
		if !ok {
			t.Fatalf("missing result: %+v", envelope)
		}
		return v
	}
	created := result(call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "RPC recovery"}}, ""))
	id := created["results"].([]any)[0].(map[string]any)["ref"]
	result(call(ctx, "bead.claim", map[string]any{"id": id}, ""))
	// A caller cannot select system recovery through wire parameters.
	if call(ctx, "bead.release", map[string]any{"id": id, "actor": map[string]any{"kind": "system", "id": "core"}}, "")["error"] == nil {
		t.Fatal("wire actor was accepted")
	}
	now = now.Add(15*time.Minute + time.Nanosecond)
	bobParams := func() map[string]any {
		return map[string]any{"id": id, "_meta": map[string]any{"project": "local", "agent": "bob", "idempotencyKey": fmt.Sprintf("%032x", sequence+1)}}
	}
	if call(ctx, "bead.claim", bobParams(), "")["error"] == nil {
		t.Fatal("direct expired takeover bypassed claim slot")
	}
	coreCtx := rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "system", ID: "core"})
	key := "1234567890abcdef1234567890abcdef"
	first := call(coreCtx, "bead.release", map[string]any{"id": id, "reason": "expired worker"}, key)
	committed := result(first)
	if committed["revision"] != float64(3) || first["_meta"] != nil {
		t.Fatalf("first=%+v", first)
	}
	result(call(ctx, "bead.claim", bobParams(), ""))
	replay := call(coreCtx, "bead.release", map[string]any{"id": id, "reason": "expired worker"}, key)
	a, _ := json.Marshal(committed)
	b, _ := json.Marshal(result(replay))
	if string(a) != string(b) || replay["_meta"].(map[string]any)["replayed"] != true {
		t.Fatalf("replay=%+v", replay)
	}
	// Quiet heartbeat is still explicitly unsupported by this slice.
	if call(ctx, "bead.heartbeat", map[string]any{"id": id}, "")["error"] == nil {
		t.Fatal("unimplemented heartbeat returned success")
	}
}

func (p *releaseProbe) ReleaseWithOutcome(_ context.Context, by model.PrincipalRef, req service.ReleaseRequest, _ model.WriteMeta) (port.WriteOutcome, error) {
	p.called, p.by, p.ref = true, by, req.ID
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(7), Results: []model.OpResult{}, Warnings: []model.Warning{}}}, nil
}

func TestBindExposesOnlyCatalogedReleaseMethod(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	probe := &releaseProbe{}
	if err := Bind(registry, probe); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "claimrpc-test", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"id":    map[string]any{"id": "pl-recovery"},
		"_meta": map[string]any{"project": "local", "idempotencyKey": "0123456789abcdef0123456789abcdef"},
	}
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.release", "params": params})
	response := server.Dispatch(context.Background(), raw)
	var envelope map[string]any
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["error"] != nil || envelope["result"].(map[string]any)["revision"] != float64(7) {
		t.Fatalf("response=%s", response)
	}
	if !probe.called || probe.by.Kind != "anonymous" || probe.ref.ID != "pl-recovery" {
		t.Fatalf("probe=%+v", probe)
	}
	unsupported := server.Dispatch(context.Background(), []byte(`{"jsonrpc":"2.0","id":2,"method":"bead.reclaim","params":{}}`))
	if !json.Valid(unsupported) || string(unsupported) == "" {
		t.Fatal("unsupported method did not return a response")
	}
	var rejected map[string]any
	_ = json.Unmarshal(unsupported, &rejected)
	if rejected["error"] == nil {
		t.Fatalf("invented reclaim verb was accepted: %s", unsupported)
	}
}
