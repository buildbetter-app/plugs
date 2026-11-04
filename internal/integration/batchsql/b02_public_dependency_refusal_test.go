package batchsql_test

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// PR333 endpoint assertions originate at 6cd771a932e2d87066d23d319ea3dfb3f01cf5d3
// (rebased as 47d485c14fb1497e02871bdd40d34b54fc959977). Their body is unchanged
// except for the B02-specific names of the support declarations below. The
// common public-host and durable-state fixtures already exist on fixed main b9.
//
// These requests use the supported public host without an authorization plugin.
// The installed fixed policy remains unchanged. Source/history and receipts are
// inspected through the actual memory, SQLite and PostgreSQL driver readers.
func TestB02PublicDependencyMissingEndpointsAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			probe := &b02EndpointPublicationProbe{}
			b02EndpointInstallPublicationProbe(f, probe)
			h := newB01Public(t, f)
			source := h.create(t, b01Child, "dependent", nil)
			target := h.create(t, b01Epic, "cross-project blocker", nil)
			claimed := h.create(t, b01Child, "separate claimed work", nil)
			reply, _ := b02EndpointLocalWrite(t, h, "bead.claim", b01Child, map[string]any{"id": claimed})
			b02EndpointHydratedClaim(t, h, reply, claimed)
			if decodeDocument(t, f.state(t).Records[claimed.ID]).Lease == nil {
				t.Fatal("positive public claim did not persist its lease")
			}

			// A successful request first proves that this public host reaches the
			// dependency writer, including a live target in a different project.
			before := f.state(t)
			published := probe.calls.Load()
			reply, validKey := b02EndpointLocalWrite(t, h, "bead.link", b01Child, map[string]any{
				"from": source, "to": target, "kind": "blocks",
			})
			commit := b01Committed(t, reply)
			linked := f.state(t)
			validReceipt := b02EndpointAnonymousReceipt(t, h, "bead.link", validKey)
			if commit.Revision != linked.Meta.Head || linked.Meta.Head == before.Meta.Head ||
				len(linked.Log) != len(before.Log)+1 || len(linked.Versions[source.ID]) != len(before.Versions[source.ID])+1 ||
				!reflect.DeepEqual(before.Records[target.ID], linked.Records[target.ID]) ||
				!reflect.DeepEqual(before.Versions[target.ID], linked.Versions[target.ID]) ||
				!reflect.DeepEqual(before.Records[claimed.ID], linked.Records[claimed.ID]) ||
				!reflect.DeepEqual(before.Versions[claimed.ID], linked.Versions[claimed.ID]) || probe.calls.Load() != published+1 {
				t.Fatal("positive cross-project link did not make exactly one source write and publication")
			}
			if validReceipt == nil || validReceipt.ResponsePending || validReceipt.Commit.Revision != commit.Revision {
				t.Fatal("positive public link has no completed receipt for its commit")
			}

			checkLinked := func(t *testing.T) {
				t.Helper()
				state := f.state(t)
				doc := decodeDocument(t, state.Records[source.ID])
				if doc.Project != b01Child || len(doc.Links) != 1 || doc.Links[0].Kind != "blocks" ||
					!doc.Links[0].To.IsID() || doc.Links[0].To.ID != target.ID || !doc.Links[0].To.Branch.IsZero() ||
					decodeDocument(t, state.Records[target.ID]).Project != b01Epic ||
					decodeDocument(t, state.Records[claimed.ID]).Lease == nil {
					t.Fatal("public refusal or reopen changed the cross-project edge or separate lease")
				}
				if !reflect.DeepEqual(validReceipt, b02EndpointAnonymousReceipt(t, h, "bead.link", validKey)) {
					t.Fatal("public refusal or reopen changed the successful link receipt")
				}
				h.ready(t, h.owner)
			}
			checkLinked(t)

			type failedRequest struct{ method, key string }
			failedKeys := []failedRequest{}
			checkFailedReceipts := func(t *testing.T) {
				t.Helper()
				for _, request := range failedKeys {
					if b02EndpointAnonymousReceipt(t, h, request.method, request.key) != nil {
						t.Fatal("missing-endpoint request left a receipt")
					}
				}
			}
			refusals := func(stage string) {
				for _, request := range []struct{ name, method string }{
					{"named-link", "bead.link"},
					{"atomic-batch", "tx.apply"},
				} {
					for _, kind := range []string{"blocks", "conditional-blocks"} {
						for _, endpoint := range []struct {
							name string
							from model.Ref
							to   model.Ref
						}{
							{"existing-source-missing-target", source, b01Ref("pl-b02-missing-target")},
							{"missing-source-existing-target", b01Ref("pl-b02-missing-source"), target},
						} {
							t.Run(stage+"/"+request.name+"/"+kind+"/"+endpoint.name, func(t *testing.T) {
								key := f.meta("b02-public-refusal").IdempotencyKey
								params := map[string]any{
									"from": endpoint.from, "to": endpoint.to, "kind": kind,
									"_meta": map[string]any{"project": b01Child, "idempotencyKey": key},
								}
								if request.method == "tx.apply" {
									// The valid patch precedes the missing endpoint. A
									// refusal must roll back this staged prefix as well.
									delete(params, "from")
									delete(params, "to")
									delete(params, "kind")
									params["changeSet"] = model.ChangeSet{Ops: []model.Op{
										patchOp(source, map[string]any{"title": "must not persist"}),
										{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: endpoint.from, To: endpoint.to, LinkKind: kind}},
									}}
								}
								original := f.state(t)
								published := probe.calls.Load()
								// The identical second public request must refuse again,
								// rather than replay a success/no-op receipt from the first.
								for attempt := 0; attempt < 2; attempt++ {
									h.unchanged(t, func() b01Reply {
										return h.call(t, h.slots, "", request.method, params)
									}, "not_found")
									requireSameState(t, original, f.state(t))
									if probe.calls.Load() != published || b02EndpointAnonymousReceipt(t, h, request.method, key) != nil {
										t.Fatal("missing-endpoint refusal published a change or created a receipt")
									}
								}
								failedKeys = append(failedKeys, failedRequest{request.method, key})
								checkLinked(t)
								t.Logf("B02 public refusal engine=%s stage=%s method=%s kind=%s endpoint=%s: two not_found replies; unchanged rows/lease/versions/log/head/derived state; no receipt or publication", engine, stage, request.method, kind, endpoint.name)
							})
						}
					}
				}
			}
			refusals("initial")
			checkFailedReceipts(t)

			if engine != "memory" {
				before := f.state(t)
				published := probe.calls.Load()
				h.api.Close()
				h.slots.Close()
				f.reopen(t)
				b02EndpointInstallPublicationProbe(f, probe)
				h.open(t)
				after := f.state(t)
				if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) ||
					!reflect.DeepEqual(before.Log, after.Log) || probe.calls.Load() != published {
					t.Fatal("SQL reopen changed the dependency source/history/lease or publication count")
				}
				checkLinked(t)
				checkFailedReceipts(t)
				refusals("reopened")
				checkFailedReceipts(t)
			}
		})
	}
}

// These support bodies are extracted from 1e602a2f8341b41de3c6462dc2f0923cf0672418
// internal/integration/batchsql/b01_legacy_hierarchy_test.go
// blob 2637e429c117cabcc2b54a07d6a75a16733c9f9f. Only their helper names change; no B01 cases are imported.
type b02EndpointPublicationProbe struct{ calls atomic.Uint64 }

func (p *b02EndpointPublicationProbe) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return nil
}

func b02EndpointInstallPublicationProbe(f *fixture, probe *b02EndpointPublicationProbe) {
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b01-publication", Subscriber: probe}),
	)
}

func b02EndpointLocalWrite(t *testing.T, h *b01Public, method, project string, params map[string]any) (b01Reply, string) {
	t.Helper()
	key := h.f.meta("b01-legacy").IdempotencyKey
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key}
	return h.call(t, h.slots, "", method, params), key
}

func b02EndpointAnonymousReceipt(t *testing.T, h *b01Public, method, key string) *driver.IdempotencyRecord {
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

func b02EndpointHydratedClaim(t *testing.T, h *b01Public, reply b01Reply, want model.Ref) {
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
