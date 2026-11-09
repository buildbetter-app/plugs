package s1b05b08

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// The barrier runs immediately before the real SQL checkpoint transaction.
// Both publication paths retain their real persistence/lease/CAS behavior, so
// removing the service fence makes the refused operation publish and fail.
type b07EndpointFenceStore struct {
	driver.Store
	driver.PersistentCheckpointStore
	driver.HeadGuardedCheckpointStore
	beforeSave   func()
	fencedWrites int
}

func (s *b07EndpointFenceStore) barrier() {
	if fn := s.beforeSave; fn != nil {
		s.beforeSave = nil
		fn()
	}
}

func (s *b07EndpointFenceStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
	s.barrier()
	return s.PersistentCheckpointStore.SaveCheckpoint(ctx, lease, key, expected, payload)
}

func (s *b07EndpointFenceStore) SaveCheckpointAtHead(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, head driver.CoreSnapshotExpectation) (driver.Checkpoint, error) {
	s.fencedWrites++
	s.barrier()
	return s.HeadGuardedCheckpointStore.SaveCheckpointAtHead(ctx, lease, key, expected, payload, head)
}

func TestB07SQLWorkflowRelationsFenceOrdinaryEndpointsAtPublication(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, direction := range []string{"ordinary-source", "ordinary-target"} {
			for _, method := range []string{"bead.link", "bead.unlink"} {
				t.Run(engine+"/"+direction+"/"+method, func(t *testing.T) {
					f := newB07SQLPlaneFixture(t, engine)
					ordinary := f.createB08(t, "endpoint", "issue", nil, nil)
					item := b07InvokeSQLWorkflow(t, f, 1)[0]
					from, to := ordinary, item
					if direction == "ordinary-target" {
						from, to = item, ordinary
					}
					params := map[string]any{"from": from, "to": to, "kind": "related"}
					beforeKind := ""
					if method == "bead.unlink" {
						b08Commit(t, f.callB08(t, "bead.link", "seed-relation", params))
						beforeKind = "related"
					}
					base := f.store
					barrier := &b07EndpointFenceStore{
						Store:                      base,
						PersistentCheckpointStore:  base.(driver.PersistentCheckpointStore),
						HeadGuardedCheckpointStore: base.(driver.HeadGuardedCheckpointStore),
					}
					f.store = barrier
					f.install(t)
					before := f.conservation(t)
					var afterRace b08Conservation
					barrier.beforeSave = func() {
						// This is a real public ordinary mutation after validation,
						// before checkpoint publication, on the same store/lease.
						b08Commit(t, f.callB08(t, "bead.update", "advance-endpoint", map[string]any{"id": ordinary, "fields": map[string]any{"title": "changed before relation publication"}}))
						afterRace = f.conservation(t)
					}
					got := f.callB08(t, method, "fenced-relation", params)
					if got.Error == nil || got.Error.Data.Code != "write_conflict" || barrier.beforeSave != nil || barrier.fencedWrites != 1 {
						t.Fatalf("stale ordinary endpoint published: error=%+v fencedWrites=%d result=%s", got.Error, barrier.fencedWrites, got.Result)
					}
					if before.Meta.Head.String() == afterRace.Meta.Head.String() || !reflect.DeepEqual(before.Checkpoints, afterRace.Checkpoints) {
						t.Fatal("barrier did not isolate one real core mutation from workflow publication")
					}
					b08SameConservation(t, afterRace, f.conservation(t))
					f.reopenB08(t)
					b08SameConservation(t, afterRace, f.conservation(t))
					b07SQLRequireRelation(t, f, from, to, beforeKind, nil, false)
					// Refusal published no receipt: the exact request/key can
					// succeed against fresh observations and replay after reopen.
					retried := f.callB08(t, method, "fenced-relation", params)
					b08Commit(t, retried)
					afterKind := "related"
					if method == "bead.unlink" {
						afterKind = ""
					}
					b07SQLRequireRelation(t, f, from, to, afterKind, nil, false)
					b07SQLReplay(t, f, method, "fenced-relation", params, retried)
				})
			}
		}
	}
}

func TestB07SQLWorkflowAuxiliaryEndpointsFenceCompletePublication(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, method := range []string{"x.workflow.invoke", "bead.update"} {
			for _, auxiliary := range []string{"parent", "links"} {
				for _, mutation := range []string{"update", "delete"} {
					t.Run(engine+"/"+method+"/"+auxiliary+"/"+mutation, func(t *testing.T) {
						f := newB07SQLPlaneFixture(t, engine)
						ordinary := f.createB08(t, "auxiliary-endpoint", "issue", nil, nil)
						var source model.Ref
						if method == "bead.update" {
							source = b07InvokeSQLWorkflow(t, f, 1)[0]
						}
						params := func(target model.Ref) map[string]any {
							if method == "bead.update" {
								fields := map[string]any{"parent": target}
								if auxiliary == "links" {
									fields = map[string]any{"links": []workflowplugin.RelationSpec{{To: target, Kind: "related"}}}
								}
								return map[string]any{"id": source, "fields": fields}
							}
							item := workflowplugin.ChecklistItem{Label: "linked", Title: "linked", Type: "issue", Parent: &target}
							if auxiliary == "links" {
								item.Parent = nil
								item.Links = []workflowplugin.RelationSpec{{To: target, Kind: "related"}}
							}
							// The final item has no auxiliary refs: the invocation must
							// retain one observation for the complete checklist.
							return map[string]any{"template": "auxiliary-fence", "project": project, "mode": "ephemeral", "checklist": []workflowplugin.ChecklistItem{item, {Label: "plain", Title: "plain", Type: "issue"}}}
						}
						base := f.store
						// A competing public writer uses its own provider mutex while
						// sharing the real SQL store, lease and receipt census. Main's
						// delete path now reads the workflow checkpoint; re-entering
						// this provider from its publication barrier would deadlock.
						peerFixture := *f.fixture
						peer := *f
						peer.fixture = &peerFixture
						peer.install(t)
						barrier := &b07EndpointFenceStore{Store: base, PersistentCheckpointStore: base.(driver.PersistentCheckpointStore), HeadGuardedCheckpointStore: base.(driver.HeadGuardedCheckpointStore)}
						f.store = barrier
						f.install(t)
						before := f.conservation(t)
						var afterRace b08Conservation
						barrier.beforeSave = func() {
							if mutation == "delete" {
								b08Commit(t, peer.callB08(t, "tx.apply", "delete-auxiliary-endpoint", b08Batch(model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ordinary}})))
							} else {
								b08Commit(t, peer.callB08(t, "bead.update", "advance-auxiliary-endpoint", map[string]any{"id": ordinary, "fields": map[string]any{"title": "updated before publication"}}))
							}
							// Observe after the real competing public commit, before
							// workflow checkpoint, indexes, audit or receipt publication.
							afterRace = f.conservation(t)
						}
						got := f.callB08(t, method, "auxiliary-fence", params(ordinary))
						if got.Error == nil || got.Error.Data.Code != "write_conflict" || barrier.beforeSave != nil || barrier.fencedWrites != 1 {
							t.Fatalf("stale auxiliary endpoint published: error=%+v fencedWrites=%d result=%s", got.Error, barrier.fencedWrites, got.Result)
						}
						if before.Meta.Head.String() == afterRace.Meta.Head.String() || !reflect.DeepEqual(before.Checkpoints, afterRace.Checkpoints) {
							t.Fatal("barrier failed to isolate the real competing core mutation")
						}
						b08SameConservation(t, afterRace, f.conservation(t))
						f.reopenB08(t)
						b08SameConservation(t, afterRace, f.conservation(t))
						target := ordinary
						if mutation == "delete" {
							missing := f.callB08(t, method, "auxiliary-fence", params(ordinary))
							b08RequireRefusal(t, method, missing, "not_found", "")
							b08SameConservation(t, afterRace, f.conservation(t))
							target = f.createB08(t, "replacement-auxiliary-endpoint", "issue", nil, nil)
						}
						beforeRetry := f.conservation(t)
						retried := f.callB08(t, method, "auxiliary-fence", params(target))
						if method == "x.workflow.invoke" {
							var invoked workflowplugin.InvokeResult
							if retried.Error != nil || json.Unmarshal(retried.Result, &invoked) != nil || invoked.Run == nil || len(invoked.Items) != 2 {
								t.Fatalf("refused invocation consumed its key or partial checklist: error=%+v result=%s", retried.Error, retried.Result)
							}
							source = model.RefByID(invoked.Items[0].ID, model.Natural64(0))
						} else {
							b08Commit(t, retried)
						}
						b07RequireSameCoreState(t, beforeRetry, f.conservation(t))
						// Change the core head again before replay. The host-only
						// observation must not become part of request identity.
						b08Commit(t, f.callB08(t, "bead.update", "advance-before-auxiliary-replay", map[string]any{"id": target, "fields": map[string]any{"title": "fresh replay observation"}}))
						beforeReplay := f.conservation(t)
						f.reopenB08(t)
						b08SameConservation(t, beforeReplay, f.conservation(t))
						replayed := f.callB08(t, method, "auxiliary-fence", params(target))
						if replayed.Error != nil || !reflect.DeepEqual(retried.Result, replayed.Result) {
							t.Fatalf("auxiliary replay changed after SQL reopen/fresh core head: error=%+v first=%s replay=%s", replayed.Error, retried.Result, replayed.Result)
						}
						b08SameConservation(t, beforeReplay, f.conservation(t))
						var read struct {
							Bead struct {
								Parent string
								Links  []workflowplugin.RelationView
							}
						}
						public := f.callB08(t, "bead.get", "read-auxiliary-recovery", map[string]any{"id": source})
						if public.Error != nil || json.Unmarshal(public.Result, &read) != nil {
							t.Fatalf("recovered auxiliary body: error=%+v result=%s", public.Error, public.Result)
						}
						if auxiliary == "parent" {
							if read.Bead.Parent != target.ID || len(read.Bead.Links) != 0 {
								t.Fatalf("recovered parent body changed: %+v", read.Bead)
							}
						} else if read.Bead.Parent != "" || len(read.Bead.Links) != 1 || read.Bead.Links[0].From != source.ID || read.Bead.Links[0].To != target || read.Bead.Links[0].Kind != "related" {
							t.Fatalf("recovered relation body changed: %+v", read.Bead)
						}
						b08SameConservation(t, beforeReplay, f.conservation(t))
					})
				}
			}
		}
	}
}

// These are Plug RPC assertions, not native reference observations. Each SQL
// fixture has one store, lease, Writer and service for both ordinary beads and
// actual plugin-owned workflow items. No workflow reference or checkpoint is
// seeded, and no memory store is given a persistence marker.
func newB07SQLPlaneFixture(t *testing.T, engine string) *b08CloseFixture {
	t.Helper()
	f := newB08CloseFixture(t, engine, b08ContractOntology())
	capability, ok := f.store.(driver.PersistentCheckpointStore)
	if !ok || !capability.Persistent() {
		t.Fatal("positive SQL workflow proof requires the selected store's persistent capability")
	}
	f.workflow = true
	f.install(t)
	return f
}

func b07InvokeSQLWorkflow(t *testing.T, f *b08CloseFixture, count int) []model.Ref {
	t.Helper()
	items := make([]workflowplugin.ChecklistItem, count)
	for i := range items {
		items[i] = workflowplugin.ChecklistItem{Label: model.Natural64(uint64(i)).String(), Title: "private co-resident item", Type: "issue"}
	}
	before := f.conservation(t)
	got := f.callB08(t, "x.workflow.invoke", "b07-co-resident-invoke", map[string]any{
		"template": "b07-co-resident", "project": project, "mode": "ephemeral", "checklist": items,
	})
	var run workflowplugin.InvokeResult
	if got.Error != nil || json.Unmarshal(got.Result, &run) != nil || run.Plane != "ephemeral" || run.Run == nil || run.Run.ID == "" || len(run.Items) != count {
		t.Fatalf("actual public SQL workflow invocation: error=%+v result=%s", got.Error, got.Result)
	}
	refs := make([]model.Ref, count)
	for i, item := range run.Items {
		if item.ID == "" || item.Plane != "ephemeral" || item.Kind != "workflow.item" || item.Plugin != workflowplugin.Alias || item.RunID != run.Run.ID {
			t.Fatalf("public workflow identity lost: %+v", item)
		}
		refs[i] = model.RefByID(item.ID, model.Natural64(0))
		b07RequireLiveSQLWorkflowItem(t, f, refs[i])
	}
	after := f.conservation(t)
	b07RequireSameCoreState(t, before, after)
	checkpoint := after.Checkpoints["plugin/workflow/default/durable"]
	if checkpoint.Generation == 0 || len(checkpoint.Payload) == 0 {
		t.Fatal("public workflow producer did not persist the selected store's checkpoint")
	}
	return refs
}

// The workflow producer/lifecycle is expected to change its checkpoint only.
// Compare every observed core component independently rather than treating a
// stable head as evidence of an unchanged durable plane.
func b07RequireSameCoreState(t *testing.T, before, after b08Conservation) {
	t.Helper()
	for _, part := range []struct {
		name          string
		before, after any
	}{
		{"meta", before.Meta, after.Meta}, {"records", before.Rows, after.Rows},
		{"versions", before.Versions, after.Versions}, {"changes/events", before.Changes, after.Changes},
		{"derived", before.Derived, after.Derived}, {"locks", before.Locks, after.Locks},
		{"core receipts", before.Receipts, after.Receipts},
	} {
		if !reflect.DeepEqual(part.before, part.after) {
			t.Fatalf("workflow-only operation changed core %s", part.name)
		}
	}
}

func b07RequireLiveSQLWorkflowItem(t *testing.T, f *b08CloseFixture, ref model.Ref) {
	t.Helper()
	got := f.callB08(t, "bead.get", "b07-co-resident-get-"+ref.ID, map[string]any{"id": ref})
	var result struct {
		Bead struct {
			ID, Plane, Kind, Plugin string
		}
	}
	if got.Error != nil || json.Unmarshal(got.Result, &result) != nil || result.Bead.ID != ref.ID || result.Bead.Plane != "ephemeral" || result.Bead.Kind != "workflow.item" || result.Bead.Plugin != workflowplugin.Alias {
		t.Fatalf("actual workflow item unavailable through the same public service: error=%+v result=%s", got.Error, got.Result)
	}
}

func b07SQLReadyContains(t *testing.T, f *b08CloseFixture, id string, want bool) {
	t.Helper()
	got := f.callB08(t, "bead.ready", "b07-co-resident-ready", map[string]any{"limit": 1000, "filter": map[string]any{"includeEphemeral": true}})
	var page struct {
		Items      []struct{ ID string }
		NextCursor string
	}
	if got.Error != nil || json.Unmarshal(got.Result, &page) != nil || page.NextCursor != "" {
		t.Fatalf("same-store ready response was not complete: error=%+v result=%s", got.Error, got.Result)
	}
	found := false
	for _, item := range page.Items {
		found = found || item.ID == id
	}
	if found != want {
		t.Fatalf("same-store ready membership for %s=%t want=%t", id, found, want)
	}
}

// Both point reads expose the checkpoint-owned relation. Neither read nor the
// ready projection may publish a core or checkpoint mutation.
func b07SQLRequireRelation(t *testing.T, f *b08CloseFixture, from, to model.Ref, kind string, metadata *model.LinkMetadata, blocked bool) {
	t.Helper()
	before := f.conservation(t)
	for _, ref := range []model.Ref{from, to} {
		got := f.callB08(t, "bead.get", "b07-relation-view-"+ref.ID, map[string]any{"id": ref})
		var result struct {
			Bead struct {
				Blocked bool
				Links   []workflowplugin.RelationView
			}
		}
		if got.Error != nil || json.Unmarshal(got.Result, &result) != nil {
			t.Fatalf("public relation view: error=%+v result=%s", got.Error, got.Result)
		}
		wantBlocked := blocked && ref.ID == from.ID
		if result.Bead.Blocked != wantBlocked {
			t.Fatalf("public blocked state for %s=%t want=%t", ref.ID, result.Bead.Blocked, wantBlocked)
		}
		if kind == "" {
			if len(result.Bead.Links) != 0 {
				t.Fatalf("removed relation remains visible: %+v", result.Bead.Links)
			}
		} else if len(result.Bead.Links) != 1 || result.Bead.Links[0].From != from.ID || result.Bead.Links[0].To.ID != to.ID || !result.Bead.Links[0].To.Branch.IsZero() || result.Bead.Links[0].Kind != kind || !reflect.DeepEqual(result.Bead.Links[0].Metadata, metadata) {
			t.Fatalf("endpoint identity/metadata was duplicated or lost: %+v", result.Bead.Links)
		}
	}
	b07SQLReadyContains(t, f, from.ID, !blocked)
	b07SQLReadyContains(t, f, to.ID, true)
	b08SameConservation(t, before, f.conservation(t))
}

func b07SQLReplay(t *testing.T, f *b08CloseFixture, method, label string, params map[string]any, original b08Reply) {
	t.Helper()
	before := f.conservation(t)
	f.reopenB08(t)
	b08SameConservation(t, before, f.conservation(t))
	replay := f.callB08(t, method, label, params)
	b08Commit(t, replay)
	if !reflect.DeepEqual(original.Result, replay.Result) {
		t.Fatalf("workflow result was not replayed exactly after SQL reopen: first=%s replay=%s", original.Result, replay.Result)
	}
	// Workflow receipts live in the provider checkpoint. Its exact byte and
	// generation conservation proves replay without inventing a response flag.
	b08SameConservation(t, before, f.conservation(t))
}

func b07PublicExternalEdge(t *testing.T, f *b08CloseFixture, from, to model.Ref, kind string) {
	t.Helper()
	got := f.callB08(t, "bead.get", "public-external-domain", map[string]any{"id": from})
	var result struct {
		Bead struct{ Links []workflowplugin.RelationView }
	}
	if got.Error != nil || json.Unmarshal(got.Result, &result) != nil || len(result.Bead.Links) != 1 {
		t.Fatalf("public external edge projection: error=%+v result=%s", got.Error, got.Result)
	}
	link := result.Bead.Links[0]
	if link.From != from.ID || link.To.ID != to.ID || !link.To.Branch.IsZero() || link.Kind != kind || link.TargetKind != model.LinkTargetExternal {
		t.Fatalf("public read lost external target domain: %+v", link)
	}
}

func TestB07PublicLinkReadsKeepTargetDomainsAcrossCollisionAndReopen(t *testing.T) {
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				f := newB08CloseFixture(t, engine, b08ContractOntology())
				from := f.createB08(t, "read-domain-source", "issue", nil, nil)
				to := f.createB08(t, "read-domain-target", "issue", nil, nil)
				for _, targetKind := range []model.LinkTargetKind{model.LinkTargetLocal, model.LinkTargetExternal} {
					b08Commit(t, f.callB08(t, producer, "read-domain-"+string(targetKind), edgeTypedLinkParams(producer, model.OpLink, from, to, "relates-to", targetKind)))
				}
				check := func() {
					t.Helper()
					before := f.conservation(t)
					assertLinks := func(links []workflowplugin.RelationView) {
						t.Helper()
						seen := map[model.LinkTargetKind]bool{}
						for _, link := range links {
							kind, err := link.TargetKind.Normalized()
							if err != nil || seen[kind] || link.From != from.ID || link.To.ID != to.ID || !link.To.Branch.IsZero() || link.Kind != "relates-to" {
								t.Fatalf("public link identity conflated: %+v", links)
							}
							seen[kind] = true
						}
						// The local arm normalizes to omission on the wire.
						if len(links) != 2 || !seen[""] || !seen[model.LinkTargetExternal] {
							t.Fatalf("public links lost local/external distinction: %+v", links)
						}
					}
					got := f.callB08(t, "bead.get", "read-domain-get", map[string]any{"id": from})
					var result struct {
						Bead struct{ Links []workflowplugin.RelationView }
					}
					if got.Error != nil || json.Unmarshal(got.Result, &result) != nil {
						t.Fatalf("public point read: error=%+v result=%s", got.Error, got.Result)
					}
					assertLinks(result.Bead.Links)
					got = f.callB08(t, "bead.ready", "read-domain-ready", map[string]any{"limit": 1000})
					var page struct {
						Items []struct {
							ID    string
							Links []workflowplugin.RelationView
						}
						NextCursor string
					}
					if got.Error != nil || json.Unmarshal(got.Result, &page) != nil || page.NextCursor != "" {
						t.Fatalf("public ready read: error=%+v result=%s", got.Error, got.Result)
					}
					found := false
					for _, item := range page.Items {
						if item.ID == from.ID {
							assertLinks(item.Links)
							found = true
						}
					}
					if !found {
						t.Fatal("readable source absent from complete ready page")
					}
					b08SameConservation(t, before, f.conservation(t))
				}
				check()
				before := f.conservation(t)
				f.reopenB08(t)
				b08SameConservation(t, before, f.conservation(t))
				check()
			})
		}
	}
}

func TestB07SQLDemotionKeepsExternalIdentityOutsideLocalIncidence(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, outbound := range []bool{false, true} {
			name := "inbound-collision"
			if outbound {
				name = "outbound-capability-refusal"
			}
			t.Run(engine+"/"+name, func(t *testing.T) {
				f := newB07SQLPlaneFixture(t, engine)
				source := f.createB08(t, "demote-source", "issue", nil, nil)
				other := f.createB08(t, "demote-other", "issue", nil, nil)
				from, to := other, source
				if outbound {
					from, to = source, other
				}
				b08Commit(t, f.callB08(t, "bead.link", "demote-external-link", edgeTypedLinkParams("bead.link", model.OpLink, from, to, "relates-to", model.LinkTargetExternal)))
				request := map[string]any{"source": source, "template": "b07-demote", "label": "moved"}
				if outbound {
					f.refuseB08(t, "x.workflow.run.demote", "demote-external", request, "unsupported", "")
					b07PublicExternalEdge(t, f, from, to, "relates-to")
					return
				}
				got := f.callB08(t, "x.workflow.run.demote", "demote-external", request)
				var moved workflowplugin.DemoteResult
				if got.Error != nil || json.Unmarshal(got.Result, &moved) != nil || moved.Item.ID == "" || len(moved.Item.Links) != 0 {
					t.Fatalf("external collision entered demotion incidence: error=%+v result=%s", got.Error, got.Result)
				}
				b07PublicExternalEdge(t, f, from, to, "relates-to")
				before := f.conservation(t)
				f.reopenB08(t)
				b08SameConservation(t, before, f.conservation(t))
				replay := f.callB08(t, "x.workflow.run.demote", "demote-external", request)
				var replayed workflowplugin.DemoteResult
				if replay.Error != nil || json.Unmarshal(replay.Result, &replayed) != nil || replayed.Item.ID != moved.Item.ID || !reflect.DeepEqual(got.Result, replay.Result) {
					t.Fatalf("demotion result was not replayed exactly after SQL reopen: error=%+v first=%s replay=%s", replay.Error, got.Result, replay.Result)
				}
				b08SameConservation(t, before, f.conservation(t))
				b07PublicExternalEdge(t, f, from, to, "relates-to")
			})
		}
	}
}

func TestB07SQLNamedWorkflowEdgesHaveCanonicalEndpointIdentity(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, direction := range []string{"durable-to-workflow", "workflow-to-durable", "workflow-to-workflow"} {
			t.Run(engine+"/"+direction, func(t *testing.T) {
				f := newB07SQLPlaneFixture(t, engine)
				durable := f.createB08(t, "named-edge-durable", "issue", nil, nil)
				items := b07InvokeSQLWorkflow(t, f, 2)
				from, to := items[0], items[1]
				if direction == "durable-to-workflow" {
					from = durable
				} else if direction == "workflow-to-durable" {
					to = durable
				}
				params := func(kind string) map[string]any { return map[string]any{"from": from, "to": to, "kind": kind} }
				write := func(method, label string, request map[string]any) b08Reply {
					before := f.conservation(t)
					got := f.callB08(t, method, label, request)
					b08Commit(t, got)
					after := f.conservation(t)
					b07RequireSameCoreState(t, before, after)
					if reflect.DeepEqual(before.Checkpoints, after.Checkpoints) {
						t.Fatal("new named operation did not persist its workflow receipt/image")
					}
					return got
				}
				b07SQLRequireRelation(t, f, from, to, "", nil, false)
				first := write("bead.link", "named-block", params("blocks"))
				b07SQLRequireRelation(t, f, from, to, "blocks", nil, true)
				b07SQLReplay(t, f, "bead.link", "named-block", params("blocks"), first)
				again := write("bead.link", "named-block-again", params("blocks"))
				if !reflect.DeepEqual(b08Commit(t, first), b08Commit(t, again)) {
					t.Fatal("idempotent re-add changed workflow item generation")
				}
				b07SQLRequireRelation(t, f, from, to, "blocks", nil, true)
				f.refuseB08(t, "bead.link", "named-retype", params("related"), "ontology_guard_failed", "")
				b07SQLRequireRelation(t, f, from, to, "blocks", nil, true)
				removed := write("bead.unlink", "named-unblock", params("blocks"))
				b07SQLRequireRelation(t, f, from, to, "", nil, false)
				b07SQLReplay(t, f, "bead.unlink", "named-unblock", params("blocks"), removed)
				// The refused retype key remains available after explicit unlink.
				recovered := write("bead.link", "named-retype", params("related"))
				b07SQLRequireRelation(t, f, from, to, "related", nil, false)
				b07SQLReplay(t, f, "bead.link", "named-retype", params("related"), recovered)
				write("bead.unlink", "named-remove-related", params("related"))

				predicate := &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}
				wait := params("waits-for")
				wait["metadata"] = predicate
				withPredicate := write("bead.link", "named-wait", wait)
				b07SQLRequireRelation(t, f, from, to, "waits-for", predicate, true)
				omitted := write("bead.link", "named-wait-omitted", params("waits-for"))
				if !reflect.DeepEqual(b08Commit(t, withPredicate), b08Commit(t, omitted)) {
					t.Fatal("omitted metadata changed predicate or item generation")
				}
				b07SQLRequireRelation(t, f, from, to, "waits-for", predicate, true)
				replacement := params("waits-for")
				replacement["metadata"] = map[string]any{}
				updated := write("bead.link", "named-wait-replace", replacement)
				if reflect.DeepEqual(b08Commit(t, withPredicate), b08Commit(t, updated)) {
					t.Fatal("explicit predicate replacement did not change workflow generation")
				}
				b07SQLRequireRelation(t, f, from, to, "waits-for", &model.LinkMetadata{Gate: "all-children"}, false)
				b07SQLReplay(t, f, "bead.link", "named-wait-replace", replacement, updated)
				unchanged := write("bead.link", "named-wait-equal", replacement)
				if !reflect.DeepEqual(b08Commit(t, updated), b08Commit(t, unchanged)) {
					t.Fatal("equal normalized metadata changed workflow generation")
				}
				// Unlink has no metadata member and must remove the predicate edge.
				write("bead.unlink", "named-remove-wait", params("waits-for"))
				b07SQLRequireRelation(t, f, from, to, "", nil, false)
				f.refuseB08(t, "bead.link", "named-kind-recovery", params(strings.Repeat("x", 33)), "invalid_params", "")
				for i, kind := range []string{"  opaque kind  ", strings.Repeat("λ", 16)} {
					label := "named-kind-recovery"
					if i != 0 {
						label = "named-kind-32-utf8-bytes"
					}
					write("bead.link", label, params(kind))
					b07SQLRequireRelation(t, f, from, to, kind, nil, false)
					write("bead.unlink", label, params(kind))
				}
				b07SQLRequireRelation(t, f, from, to, "", nil, false)
			})
		}
	}
}

func TestB07SQLInvocationPublishesAuxiliaryIndexesOnce(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB07SQLPlaneFixture(t, engine)
			parent := b07InvokeSQLWorkflow(t, f, 1)[0]
			params := map[string]any{
				"template": "indexed-child", "project": project, "mode": "ephemeral",
				"checklist": []workflowplugin.ChecklistItem{{Label: "child", Title: "child", Type: "issue", Parent: &parent, Links: []workflowplugin.RelationSpec{{To: parent, Kind: "related"}}}},
			}
			before := f.conservation(t)
			invoked := f.callB08(t, "x.workflow.invoke", "indexed-child", params)
			var result workflowplugin.InvokeResult
			if invoked.Error != nil || json.Unmarshal(invoked.Result, &result) != nil || len(result.Items) != 1 {
				t.Fatalf("indexed invocation refused: error=%+v result=%s", invoked.Error, invoked.Result)
			}
			child := model.RefByID(result.Items[0].ID, model.Natural64(0))
			after := f.conservation(t)
			b07RequireSameCoreState(t, before, after)
			requireIndexes := func() {
				t.Helper()
				// Inspect the actual checkpoint arrays: public relation views
				// deduplicate mirrors and can hide duplicate stored indexes.
				checkpoint, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
				if err != nil {
					t.Fatal(err)
				}
				var namespace struct {
					Values map[string]struct{ Value json.RawMessage }
				}
				if err := json.Unmarshal(checkpoint.Payload, &namespace); err != nil {
					t.Fatal(err)
				}
				var stored struct {
					Items map[string]struct {
						Parent         *model.Ref
						ChildCounter   uint64
						Links, Inbound []workflowplugin.RelationView
					}
				}
				if err := json.Unmarshal(namespace.Values["workflow/state"].Value, &stored); err != nil {
					t.Fatal(err)
				}
				p, c := stored.Items[parent.ID], stored.Items[child.ID]
				want := []workflowplugin.RelationView{{From: child.ID, To: parent, Kind: "related"}}
				if len(stored.Items) != 2 || p.ChildCounter != 1 || !reflect.DeepEqual(p.Inbound, want) || !reflect.DeepEqual(c.Links, want) || c.Parent == nil || *c.Parent != parent {
					t.Fatalf("invocation duplicated or lost physical indexes: parent=%+v child=%+v", p, c)
				}
			}
			requireIndexes()
			b07SQLReplay(t, f, "x.workflow.invoke", "indexed-child", params, invoked)
			requireIndexes()
			b08SameConservation(t, after, f.conservation(t))
		})
	}
}

func TestB07SQLNamedOpaqueWorkflowEdgeCanJoinAtomicOrdinaryDelete(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, direction := range []string{"durable-to-workflow", "workflow-to-durable"} {
			t.Run(engine+"/"+direction, func(t *testing.T) {
				f := newB07SQLPlaneFixture(t, engine)
				ordinary := f.createB08(t, "opaque-delete-endpoint", "issue", nil, nil)
				item := b07InvokeSQLWorkflow(t, f, 1)[0]
				from, to := ordinary, item
				if direction == "workflow-to-durable" {
					from, to = item, ordinary
				}
				const kind = "  opaque kind  "
				b08Commit(t, f.callB08(t, "bead.link", "opaque-delete-link", map[string]any{"from": from, "to": to, "kind": kind}))
				b07SQLRequireRelation(t, f, from, to, kind, nil, false)
				batch := func(spelling string) map[string]any {
					return b08Batch(
						model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{From: from, To: to, LinkKind: spelling}},
						model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ordinary}},
					)
				}
				// Wrong spelling must conserve both planes and leave the key free.
				f.refuseB08(t, "tx.apply", "opaque-delete", batch(strings.TrimSpace(kind)), "not_found", "")
				b07SQLRequireRelation(t, f, from, to, kind, nil, false)
				params := batch(kind)
				deleted := f.callB08(t, "tx.apply", "opaque-delete", params)
				commit := b08Commit(t, deleted)
				if len(commit.Results) != 2 || commit.Results[0].Ref != item || commit.Results[1].Ref != ordinary {
					t.Fatalf("atomic unlink/delete lost ordered endpoint results: %+v", commit.Results)
				}
				b07SQLReplay(t, f, "tx.apply", "opaque-delete", params, deleted)
				before := f.conservation(t)
				missing := f.callB08(t, "bead.get", "opaque-deleted-endpoint", map[string]any{"id": ordinary})
				b08RequireRefusal(t, "bead.get", missing, "not_found", "")
				got := f.callB08(t, "bead.get", "opaque-retained-workflow", map[string]any{"id": item})
				var result struct{ Bead workflowplugin.ItemView }
				if got.Error != nil || json.Unmarshal(got.Result, &result) != nil || result.Bead.ID != item.ID || len(result.Bead.Links) != 0 {
					t.Fatalf("atomic delete lost workflow item or retained the opaque relation: error=%+v result=%s", got.Error, got.Result)
				}
				b08SameConservation(t, before, f.conservation(t))
			})
		}
	}
}

func TestB07SQLCoResidentWorkflowBatchRefusalsConserveBothPlanes(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB07SQLPlaneFixture(t, engine)
			from := f.createB08(t, "co-resident-source", "issue", nil, nil)
			to := f.createB08(t, "co-resident-target", "issue", nil, nil)
			other := f.createB08(t, "co-resident-neighbor", "issue", nil, nil)
			items := b07InvokeSQLWorkflow(t, f, 2)
			for _, pair := range []struct {
				name     string
				from, to model.Ref
			}{
				{"durable-to-workflow", from, items[0]},
				{"workflow-to-durable", items[0], to},
				{"workflow-to-workflow", items[0], items[1]},
			} {
				for _, attempt := range []struct {
					method string
					op     model.OpKind
				}{
					{"tx.apply", model.OpLink}, {"tx.apply", model.OpUnlink},
				} {
					label := "b07-co-resident/" + pair.name + "/" + attempt.method + "/" + string(attempt.op)
					t.Run(pair.name+"/"+attempt.method+"/"+string(attempt.op), func(t *testing.T) {
						params := edgeTypedLinkParams(attempt.method, attempt.op, pair.from, pair.to, "blocks", "")
						recovery := edgeTypedLinkParams(attempt.method, attempt.op, from, to, "blocks", "")
						if attempt.method == "tx.apply" {
							batch := func(source, target model.Ref) map[string]any {
								return b08Batch(
									edgePatchOp(other, label+"-prefix"),
									edgeLinkOp(from, other, "related"),
									edgeTypedLinkOp(attempt.op, source, target, "blocks", ""),
									edgePatchOp(to, label+"-tail"),
								)
							}
							params, recovery = batch(pair.from, pair.to), batch(from, to)
						}
						// This compares the complete same-store core/checkpoint image,
						// proves no receipt, reopens the same DSN and repeats refusal.
						f.refuseB08(t, attempt.method, label, params, "invalid_params", "")
						for _, item := range items {
							b07RequireLiveSQLWorkflowItem(t, f, item)
							b07SQLReadyContains(t, f, item.ID, true)
						}
						beforeRecovery := f.conservation(t)
						got := f.callB08(t, attempt.method, label, recovery)
						b08Commit(t, got)
						if got.Meta["replayed"] == true {
							t.Fatal("refused request consumed the recovery operation key")
						}
						if attempt.method == "tx.apply" && (f.document(t, other).Fields["title"] != label+"-prefix" || f.document(t, to).Fields["title"] != label+"-tail") {
							t.Fatal("corrected batch did not publish both valid images")
						}
						afterRecovery := f.conservation(t)
						if !reflect.DeepEqual(beforeRecovery.Checkpoints, afterRecovery.Checkpoints) {
							t.Fatal("durable recovery mutated co-resident workflow or checkpoint state")
						}
						f.reopenB08(t)
						b08SameConservation(t, afterRecovery, f.conservation(t))
						replay := f.callB08(t, attempt.method, label, recovery)
						b08Commit(t, replay)
						if replay.Meta["replayed"] != true || !reflect.DeepEqual(got.Result, replay.Result) {
							t.Fatalf("durable recovery was not replayed exactly after SQL reopen: first=%s replay=%s metadata=%+v", got.Result, replay.Result, replay.Meta)
						}
						b08SameConservation(t, afterRecovery, f.conservation(t))
						b07SQLReadyContains(t, f, from.ID, attempt.op == model.OpUnlink)
					})
				}
			}
		})
	}
}

func TestB07SQLMixedBlockingEdgesObserveAuthoritativeCompletion(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, direction := range []string{"durable-to-workflow", "workflow-to-durable", "workflow-to-workflow"} {
			t.Run(engine+"/"+direction, func(t *testing.T) {
				f := newB07SQLPlaneFixture(t, engine)
				durable := f.createB08(t, "settlement-durable", "issue", nil, nil)
				items := b07InvokeSQLWorkflow(t, f, 2)
				from, to := items[0], items[1]
				if direction == "durable-to-workflow" {
					from = durable
				} else if direction == "workflow-to-durable" {
					to = durable
				}
				beforeLink := f.conservation(t)
				b08Commit(t, f.callB08(t, "bead.link", "settlement-block", map[string]any{"from": from, "to": to, "kind": "blocks"}))
				b07RequireSameCoreState(t, beforeLink, f.conservation(t))
				b07SQLRequireRelation(t, f, from, to, "blocks", nil, true)
				beforeClose := f.conservation(t)
				request := map[string]any{"id": to}
				closed := f.callB08(t, "bead.close", "settlement-complete", request)
				b08Commit(t, closed)
				afterClose := f.conservation(t)
				if to.ID == durable.ID {
					if !reflect.DeepEqual(beforeClose.Checkpoints, afterClose.Checkpoints) || f.document(t, to).Fields["status"] != b08Completed {
						t.Fatal("durable blocker completion did not stay in its core plane")
					}
				} else {
					b07RequireSameCoreState(t, beforeClose, afterClose)
					if reflect.DeepEqual(beforeClose.Checkpoints, afterClose.Checkpoints) {
						t.Fatal("workflow blocker completion did not persist its checkpoint")
					}
				}
				b07SQLReadyContains(t, f, from.ID, true)
				b07SQLReadyContains(t, f, to.ID, false)
				got := f.callB08(t, "bead.get", "settlement-dependent", map[string]any{"id": from})
				var result struct{ Bead struct{ Blocked bool } }
				if got.Error != nil || json.Unmarshal(got.Result, &result) != nil || result.Bead.Blocked {
					t.Fatalf("public dependent did not observe authoritative completion: error=%+v result=%s", got.Error, got.Result)
				}
				b07SQLReplay(t, f, "bead.close", "settlement-complete", request, closed)
				b07SQLReadyContains(t, f, from.ID, true)
				b07SQLReadyContains(t, f, to.ID, false)
			})
		}
	}
}

func TestB07SQLWorkflowExternalTargetsRemainTypedUnsupported(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, method := range []string{"bead.link", "bead.unlink"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newB07SQLPlaneFixture(t, engine)
				to := f.createB08(t, "workflow-external-target", "issue", nil, nil)
				from := b07InvokeSQLWorkflow(t, f, 1)[0]
				local := map[string]any{"from": from, "to": to, "kind": "blocks"}
				if method == "bead.unlink" {
					b08Commit(t, f.callB08(t, "bead.link", "external-refusal-setup", local))
				}
				external := map[string]any{"from": from, "to": to, "kind": "blocks", "targetKind": "external"}
				f.refuseB08(t, method, "workflow-external-refusal", external, "unsupported", "")
				b07RequireLiveSQLWorkflowItem(t, f, from)
				beforeRecovery := f.conservation(t)
				recovered := f.callB08(t, method, "workflow-external-refusal", local)
				b08Commit(t, recovered)
				b07RequireSameCoreState(t, beforeRecovery, f.conservation(t))
				b07SQLReplay(t, f, method, "workflow-external-refusal", local, recovered)
				b07SQLReadyContains(t, f, from.ID, method == "bead.unlink")
			})
		}
	}
}

func TestB07SQLExternalWorkflowSpellingNeverJoinsTheWorkflowPlane(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				f := newB07SQLPlaneFixture(t, engine)
				from := f.createB08(t, "external-workflow-source", "issue", nil, nil)
				item := b07InvokeSQLWorkflow(t, f, 1)[0]
				before := f.conservation(t)
				b08Commit(t, f.callB08(t, producer, "external-workflow-link", edgeTypedLinkParams(producer, model.OpLink, from, item, "blocks", model.LinkTargetExternal)))
				links := f.document(t, from).Links
				if len(links) != 1 || links[0].To.ID != item.ID || !links[0].To.Branch.IsZero() || links[0].TargetKind != model.LinkTargetExternal || links[0].Kind != "blocks" {
					t.Fatalf("explicit external target lost exact identity: %+v", links)
				}
				b07PublicExternalEdge(t, f, from, item, "blocks")
				afterLink := f.conservation(t)
				if !reflect.DeepEqual(before.Checkpoints, afterLink.Checkpoints) {
					t.Fatal("external edge write mutated the real workflow checkpoint")
				}
				b07SQLReadyContains(t, f, from.ID, false)
				b07SQLReadyContains(t, f, item.ID, true)
				beforeClose := f.conservation(t)
				b08Commit(t, f.callB08(t, "bead.close", "external-workflow-complete", map[string]any{"id": item}))
				afterClose := f.conservation(t)
				b07RequireSameCoreState(t, beforeClose, afterClose)
				if reflect.DeepEqual(beforeClose.Checkpoints, afterClose.Checkpoints) {
					t.Fatal("actual final workflow close did not publish completion in the plugin plane")
				}
				b08RequireRefusal(t, "bead.get", f.callB08(t, "bead.get", "external-workflow-gone", map[string]any{"id": item}), "not_found", "")
				// The external endpoint remains unresolved: closing the actual
				// same-spelled workflow item must not settle the durable blocker.
				b07SQLReadyContains(t, f, from.ID, false)
				b07SQLReadyContains(t, f, item.ID, false)
				f.reopenB08(t)
				b08SameConservation(t, afterClose, f.conservation(t))
				if !reflect.DeepEqual(links, f.document(t, from).Links) {
					t.Fatal("external workflow-spelled identity changed on SQL reopen")
				}
				b07PublicExternalEdge(t, f, from, item, "blocks")
				b07SQLReadyContains(t, f, from.ID, false)
				unlink := "bead.unlink"
				if producer == "tx.apply" {
					unlink = producer
				}
				b08Commit(t, f.callB08(t, unlink, "external-workflow-unlink", edgeTypedLinkParams(unlink, model.OpUnlink, from, item, "blocks", model.LinkTargetExternal)))
				if len(f.document(t, from).Links) != 0 {
					t.Fatal("external endpoint unlink retained a durable edge")
				}
				b07SQLReadyContains(t, f, from.ID, true)
				if !reflect.DeepEqual(afterClose.Checkpoints, f.conservation(t).Checkpoints) {
					t.Fatal("external endpoint unlink mutated completed workflow state")
				}
			})
		}
	}
}

// This additional control uses the real authn/HTTP/RPC composition for the
// producer and every edge operation, with the same installed service/provider
// as the SQL fixture. Fixed authorization is not installed; this is neither
// an installed-policy write grant nor an installed-positive AZ9 claim.
func TestB07SQLAuthenticatedWorkflowProducerAndEdgesShareOneStore(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB07SQLPlaneFixture(t, engine)
			by := model.PrincipalRef{Kind: "user", ID: "user:b07-sql-owner"}
			keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return false }})
			compose := func() *httpjson.Handler {
				admission, err := authn.New(authn.Config{Provider: keys, AuthorizationInstalled: func() bool { return false }})
				if err != nil {
					t.Fatal(err)
				}
				handler, err := httpjson.New(f.server, admission, httpjson.Config{})
				if err != nil {
					t.Fatal(err)
				}
				return handler
			}
			app := compose()
			call := func(method, label string, params map[string]any) b08Reply {
				return f.callB08Authenticated(t, app, keys, by, method, label, params)
			}
			durable := b08Commit(t, call("bead.create", "http-durable", map[string]any{"type": "issue", "fields": map[string]any{"title": "co-resident HTTP durable"}})).Results[0].Ref
			beforeInvoke := f.conservation(t)
			invoked := call("x.workflow.invoke", "http-workflow", map[string]any{"template": "http-b07", "project": project, "mode": "ephemeral", "checklist": []workflowplugin.ChecklistItem{{Label: "first", Title: "first", Type: "issue"}, {Label: "second", Title: "second", Type: "issue"}}})
			var run workflowplugin.InvokeResult
			if invoked.Error != nil || json.Unmarshal(invoked.Result, &run) != nil || run.Run == nil || len(run.Items) != 2 || run.Plane != "ephemeral" {
				t.Fatalf("authenticated same-store workflow producer: error=%+v result=%s", invoked.Error, invoked.Result)
			}
			b07RequireSameCoreState(t, beforeInvoke, f.conservation(t))
			first := model.RefByID(run.Items[0].ID, model.Natural64(0))
			second := model.RefByID(run.Items[1].ID, model.Natural64(0))
			for _, item := range []model.Ref{first, second} {
				got := call("bead.get", "http-live-"+item.ID, map[string]any{"id": item})
				var result struct {
					Bead struct{ ID, Plane, Kind, Plugin string }
				}
				if got.Error != nil || json.Unmarshal(got.Result, &result) != nil || result.Bead.ID != item.ID || result.Bead.Plane != "ephemeral" || result.Bead.Kind != "workflow.item" || result.Bead.Plugin != workflowplugin.Alias {
					t.Fatalf("authenticated item not live on producer store: error=%+v result=%s", got.Error, got.Result)
				}
			}
			beforeLinks := f.conservation(t)
			local := map[string]any{"from": durable, "to": first, "kind": "blocks"}
			linked := call("bead.link", "http-mixed-link", local)
			b08Commit(t, linked)
			b08Commit(t, call("bead.link", "http-reverse-link", map[string]any{"from": second, "to": durable, "kind": "related"}))
			b07RequireSameCoreState(t, beforeLinks, f.conservation(t))
			external := map[string]any{"from": durable, "to": second, "kind": "blocks", "targetKind": "external"}
			beforeExternal := f.conservation(t)
			b08Commit(t, call("bead.link", "http-external-link", external))
			if !reflect.DeepEqual(beforeExternal.Checkpoints, f.conservation(t).Checkpoints) {
				t.Fatal("authenticated external write entered the workflow plane")
			}
			links := f.document(t, durable).Links
			if len(links) != 1 || links[0].To.ID != second.ID || links[0].TargetKind != model.LinkTargetExternal {
				t.Fatalf("authenticated external spelling lost typed identity: %+v", links)
			}
			beforeReplay := f.conservation(t)
			f.reopenB08(t)
			app = compose()
			b08SameConservation(t, beforeReplay, f.conservation(t))
			replay := call("bead.link", "http-mixed-link", local)
			b08Commit(t, replay)
			if !reflect.DeepEqual(linked.Result, replay.Result) {
				t.Fatal("authenticated mixed edge did not replay after SQL reopen")
			}
			b08SameConservation(t, beforeReplay, f.conservation(t))
			b08Commit(t, call("bead.unlink", "http-external-unlink", external))
			b08Commit(t, call("bead.unlink", "http-mixed-unlink", local))
			b08Commit(t, call("bead.unlink", "http-reverse-unlink", map[string]any{"from": second, "to": durable, "kind": "related"}))
			beforeReads := f.conservation(t)
			ready := call("bead.ready", "http-ready", map[string]any{"filter": map[string]any{"includeEphemeral": true}, "limit": 1000})
			var page struct {
				Items      []struct{ ID string }
				NextCursor string
			}
			if ready.Error != nil || json.Unmarshal(ready.Result, &page) != nil || len(page.Items) != 3 || page.NextCursor != "" {
				t.Fatalf("authenticated same-store ready membership: error=%+v result=%s", ready.Error, ready.Result)
			}
			want := map[string]bool{durable.ID: true, first.ID: true, second.ID: true}
			for _, item := range page.Items {
				if !want[item.ID] {
					t.Fatalf("unexpected/duplicate ready identity %s", item.ID)
				}
				delete(want, item.ID)
			}
			if len(want) != 0 {
				t.Fatalf("missing co-resident ready identities: %+v", want)
			}
			b08SameConservation(t, beforeReads, f.conservation(t))
		})
	}
}
