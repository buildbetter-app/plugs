package batchsql_test

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These requests use the supported public host without an authorization plugin.
// The installed fixed policy remains unchanged. Source/history and receipts are
// inspected through the actual memory, SQLite and PostgreSQL driver readers.
func TestB02PublicDependencyMissingEndpointsAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			probe := &b02PublicationProbe{}
			b02InstallPublicationProbe(f, probe)
			h := newB01Public(t, f)
			source := h.create(t, b01Child, "dependent", nil)
			target := h.create(t, b01Epic, "cross-project blocker", nil)
			claimed := h.create(t, b01Child, "separate claimed work", nil)
			reply, _ := b02LocalWrite(t, h, "bead.claim", b01Child, map[string]any{"id": claimed})
			b02HydratedClaim(t, h, reply, claimed)
			if decodeDocument(t, f.state(t).Records[claimed.ID]).Lease == nil {
				t.Fatal("positive public claim did not persist its lease")
			}

			// A successful request first proves that this public host reaches the
			// dependency writer, including a live target in a different project.
			before := f.state(t)
			published := probe.calls.Load()
			reply, validKey := b02LocalWrite(t, h, "bead.link", b01Child, map[string]any{
				"from": source, "to": target, "kind": "blocks",
			})
			commit := b01Committed(t, reply)
			linked := f.state(t)
			validReceipt := b02AnonymousReceipt(t, h, "bead.link", validKey)
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
				if !reflect.DeepEqual(validReceipt, b02AnonymousReceipt(t, h, "bead.link", validKey)) {
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
					if b02AnonymousReceipt(t, h, request.method, request.key) != nil {
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
									if probe.calls.Load() != published || b02AnonymousReceipt(t, h, request.method, key) != nil {
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
				b02InstallPublicationProbe(f, probe)
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
