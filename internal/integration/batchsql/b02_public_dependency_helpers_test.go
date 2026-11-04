package batchsql_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The publication/receipt/hydrated-claim helpers preserve PR333's existing
// B01 fixture contribution while reusing the current-main public HTTP host.
// No legacy hierarchy product implementation or recording route is imported.
type b02PublicationProbe struct{ calls atomic.Uint64 }

func (p *b02PublicationProbe) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return nil
}

func b02InstallPublicationProbe(f *fixture, probe *b02PublicationProbe) {
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b02-publication", Subscriber: probe}),
	)
}

// Dependency operations enter the ordinary public no-plugin host. The fixed
// authorization host remains separately fail-closed for unsupported link verbs.
func b02LocalWrite(t *testing.T, h *b01Public, method, project string, params map[string]any) (b01Reply, string) {
	t.Helper()
	key := h.f.meta("b02-public-refusal").IdempotencyKey
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key}
	return h.call(t, h.slots, "", method, params), key
}

func b02AnonymousReceipt(t *testing.T, h *b01Public, method, key string) *driver.IdempotencyRecord {
	t.Helper()
	meta := h.f.state(t).Meta
	r, err := h.f.store.Read(h.f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(h.f.ctx, principal, "", method, key)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func b02HydratedClaim(t *testing.T, h *b01Public, reply b01Reply, want model.Ref) {
	t.Helper()
	commit := b01Committed(t, reply)
	state := h.f.state(t)
	if reply.Meta.Replayed || commit.Revision != state.Meta.Head || len(commit.Results) != 1 ||
		commit.Results[0].Revision != commit.Revision || !commit.Results[0].Ref.IsID() ||
		commit.Results[0].Ref.ID != want.ID || !commit.Results[0].Ref.Branch.IsZero() ||
		!bytes.Equal(commit.Results[0].Record, state.Records[want.ID].Body) {
		t.Fatalf("claim did not return the committed winner: %s", reply.Result)
	}
	if status := h.get(t, h.viewer(t, b01Projects...), want, nil)["status"]; status != "in_progress" {
		t.Fatalf("public winner status=%v", status)
	}
}
