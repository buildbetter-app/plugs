package batchsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b01PublicationProbe struct{ calls atomic.Uint64 }

func (p *b01PublicationProbe) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return nil
}

func b01InstallPublicationProbe(f *fixture, probe *b01PublicationProbe) {
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b01-publication", Subscriber: probe}),
	)
}

// Legacy operations enter the ordinary public no-plugin host. The fixed
// authorization host remains separately fail-closed for unsupported link verbs.
func b01LocalWrite(t *testing.T, h *b01Public, method, project string, params map[string]any) (b01Reply, string) {
	t.Helper()
	key := h.f.meta("b01-legacy").IdempotencyKey
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key}
	return h.call(t, h.slots, "", method, params), key
}

func b01AnonymousReceipt(t *testing.T, h *b01Public, method, key string) *driver.IdempotencyRecord {
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

func b01LegacyLink(t *testing.T, h *b01Public, from, to model.Ref, kind string) {
	t.Helper()
	reply, _ := b01LocalWrite(t, h, "bead.link", b01Child, map[string]any{"from": from, "to": to, "kind": kind})
	b01Committed(t, reply)
}

func b01LegacyRefusal(t *testing.T, h *b01Public, probe *b01PublicationProbe, label, method, project string, params map[string]any, code model.ErrorCode) {
	t.Helper()
	// Close snapshot readers after each refusal, before subsequent writes.
	t.Run(label, func(t *testing.T) {
		published := probe.calls.Load()
		var key string
		h.unchanged(t, func() b01Reply {
			var reply b01Reply
			reply, key = b01LocalWrite(t, h, method, project, params)
			return reply
		}, code)
		if b01AnonymousReceipt(t, h, method, key) != nil || probe.calls.Load() != published {
			t.Fatal("refusal created a receipt or published a committed change")
		}
	})
}

// The oracle is the literal scenario set, never the implementation predicate.
// There is no ready-count RPC: cardinality is checked on the complete public
// ready response and every one-row page. Epic direct-child counts are separate.
func b01ReadyPages(t *testing.T, h *b01Public, want ...model.Ref) {
	t.Helper()
	viewer := h.viewer(t, b01Child)
	h.ready(t, viewer, want...)
	expected := make([]string, len(want))
	for i, ref := range want {
		expected[i] = ref.ID
	}
	sort.Strings(expected)
	got, seen, cursors := []string{}, map[string]bool{}, map[string]bool{}
	cursor := ""
	for {
		params := map[string]any{"limit": 1, "_meta": map[string]any{"project": b01Child}}
		if cursor != "" {
			params["cursor"] = cursor
		}
		reply := h.call(t, h.api, viewer, "bead.ready", params)
		b01Result(t, reply)
		var page struct {
			Items      []struct{ ID string } `json:"items"`
			NextCursor *string               `json:"nextCursor"`
		}
		if err := json.Unmarshal(reply.Result, &page); err != nil || len(page.Items) > 1 {
			t.Fatalf("invalid one-row public page: %s (%v)", reply.Result, err)
		}
		for _, item := range page.Items {
			if seen[item.ID] {
				t.Fatalf("ready page repeated %s", item.ID)
			}
			seen[item.ID] = true
			got = append(got, item.ID)
		}
		if len(got) > len(expected) {
			t.Fatalf("ready cardinality exceeds independent set: %v", got)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		if len(page.Items) != 1 || cursor == "" || cursors[cursor] {
			t.Fatal("ready page did not advance")
		}
		cursors[cursor] = true
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("paged ready set/count=%v/%d, want %v/%d", got, len(got), expected, len(expected))
	}
}

func b01StoredRepresentation(t *testing.T, h *b01Public, child, parent model.Ref, legacy bool) {
	t.Helper()
	record := h.f.state(t).Records[child.ID]
	var doc struct {
		Parent *model.Ref `json:"parent"`
		Links  []struct {
			To   model.Ref `json:"to"`
			Kind string    `json:"kind"`
		} `json:"links"`
	}
	// Decode JSON directly; neither the readiness nor canonicalization helper
	// supplies the expected persisted parent or legacy envelope.
	if err := json.Unmarshal(record.Body, &doc); err != nil || doc.Parent == nil || *doc.Parent != parent {
		t.Fatalf("canonical parent missing from stored postimage: %s (%v)", record.Body, err)
	}
	n := 0
	for _, link := range doc.Links {
		if link.Kind == "parent-child" {
			n++
			if link.To != parent {
				t.Fatalf("stored legacy parent differs: %+v", link)
			}
		}
	}
	if legacy != (n == 1) || n > 1 {
		t.Fatalf("stored legacy representation count=%d, expected legacy=%v", n, legacy)
	}
	if got := h.get(t, h.viewer(t, b01Projects...), child, nil)["parent"]; got != parent.ID {
		t.Fatalf("public parent=%v, want %s", got, parent.ID)
	}
}

func b01HierarchyPair(t *testing.T, h *b01Public, parent model.Ref, representation string) (model.Ref, model.Ref) {
	t.Helper()
	var canonical *model.Ref
	if representation != "legacy" {
		canonical = &parent
	}
	child := h.create(t, b01Child, representation+" child", canonical)
	if representation != "canonical" {
		b01LegacyLink(t, h, child, parent, "parent-child")
	}
	canonical = nil
	if representation != "legacy" {
		canonical = &child
	}
	grandchild := h.create(t, b01Child, representation+" grandchild", canonical)
	if representation != "canonical" {
		b01LegacyLink(t, h, grandchild, child, "parent-child")
	}
	b01StoredRepresentation(t, h, child, parent, representation != "canonical")
	b01StoredRepresentation(t, h, grandchild, child, representation != "canonical")
	return child, grandchild
}

func b01HydratedClaim(t *testing.T, h *b01Public, reply b01Reply, want model.Ref) {
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

func b01SuppressedClaimNext(t *testing.T, h *b01Public, child, grandchild model.Ref) {
	t.Helper()
	before := h.f.state(t)
	params := map[string]any{"filter": map[string]any{"resources": []string{child.ID, grandchild.ID}}}
	reply, key := b01LocalWrite(t, h, "bead.claimNext", b01Child, params)
	if reply.Error != nil || string(reply.Result) != "null" {
		t.Fatalf("fresh claimNext selected a suppressed descendant: %+v %s", reply.Error, reply.Result)
	}
	after := h.f.state(t)
	head, err := before.Meta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	if after.Meta.Head != model.Natural64(head+1) || !reflect.DeepEqual(before.Records, after.Records) ||
		!reflect.DeepEqual(before.Versions, after.Versions) || len(after.Log) != len(before.Log)+1 ||
		!reflect.DeepEqual(before.Log, after.Log[:len(before.Log)]) {
		t.Fatal("empty front changed beads/history beyond its single empty receipt")
	}
	last := after.Log[len(before.Log)]
	receipt := b01AnonymousReceipt(t, h, "bead.claimNext", key)
	if len(last.Changes) != 0 || last.IdempotencyKey != key || last.Revision != after.Meta.Head ||
		receipt == nil || receipt.ResponsePending || len(receipt.Commit.Results) != 0 || receipt.Commit.Revision != after.Meta.Head {
		t.Fatal("empty front has no complete empty H+1 receipt")
	}
	replay := h.call(t, h.slots, "", "bead.claimNext", params)
	if replay.Error != nil || string(replay.Result) != "null" || !replay.Meta.Replayed {
		t.Fatalf("suppressed front did not replay its null: %+v", replay)
	}
	requireSameState(t, after, h.f.state(t))
}

func TestB01PublicCanonicalAndLegacyHierarchyAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, representation := range []string{"canonical", "legacy", "matching-duplicate"} {
			for _, condition := range []string{"open-parent", "indefinite-defer-backlog", "blocked-ancestor"} {
				t.Run(engine+"/"+representation+"/"+condition, func(t *testing.T) {
					f := newFixture(t, engine)
					probe := &b01PublicationProbe{}
					b01InstallPublicationProbe(f, probe)
					h := newB01Public(t, f)
					parent := h.create(t, b01Epic, "ancestor", nil)
					blocker := h.create(t, b01New, "live blocker", nil)
					child, grandchild := b01HierarchyPair(t, h, parent, representation)
					free := h.create(t, b01Child, "unrelated ready work", nil)
					initial := f.state(t).Meta
					asOf := map[string]any{"revision": initial.Head, "incarnation": initial.Incarnation}
					b01ReadyPages(t, h, child, grandchild, free)
					if condition == "indefinite-defer-backlog" {
						h.slot(t, "bead.defer", b01Epic, parent)
						if status := h.get(t, h.viewer(t, b01Projects...), parent, nil)["status"]; status != "backlog" {
							t.Fatalf("defer did not enter the actual backlog category: %v", status)
						}
					} else if condition == "blocked-ancestor" {
						reply, _ := b01LocalWrite(t, h, "bead.link", b01Epic, map[string]any{"from": parent, "to": blocker, "kind": "blocks"})
						b01Committed(t, reply)
					}
					observe := func() {
						t.Helper()
						if condition == "open-parent" {
							b01ReadyPages(t, h, child, grandchild, free)
						} else {
							b01ReadyPages(t, h, free)
						}
						// A grandchild is never counted as a second direct child.
						h.progress(t, h.viewer(t, b01Projects...), parent, nil, map[string]int{"total": 1, "status:todo": 1}, 0)
						b01StoredRepresentation(t, h, child, parent, representation != "canonical")
						b01StoredRepresentation(t, h, grandchild, child, representation != "canonical")
					}
					observe()
					if engine != "memory" {
						before := f.state(t)
						h.api.Close()
						h.slots.Close()
						f.reopen(t)
						b01InstallPublicationProbe(f, probe)
						h.open(t)
						after := f.state(t)
						if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Log, after.Log) {
							t.Fatal("SQL reopen changed hierarchy source/history")
						}
						observe()
						if got := h.get(t, h.viewer(t, b01Projects...), child, asOf)["parent"]; got != parent.ID {
							t.Fatalf("SQL reopen lost the historical parent: %v", got)
						}
					}
					if condition == "open-parent" {
						reply, _ := b01LocalWrite(t, h, "bead.claim", b01Child, map[string]any{"id": child})
						b01HydratedClaim(t, h, reply, child)
						b01ReadyPages(t, h, grandchild, free)
						reply, _ = b01LocalWrite(t, h, "bead.claimNext", b01Child, map[string]any{"filter": map[string]any{"resources": []string{grandchild.ID}}})
						b01HydratedClaim(t, h, reply, grandchild)
					} else {
						for _, target := range []model.Ref{child, grandchild} {
							b01LegacyRefusal(t, h, probe, "suppressed-direct-claim-"+target.ID, "bead.claim", b01Child, map[string]any{"id": target}, "ontology_guard_failed")
						}
						b01SuppressedClaimNext(t, h, child, grandchild)
					}
					ancestorBefore := f.state(t)
					// Scheduling never permits closing a parent with a live child.
					b01LegacyRefusal(t, h, probe, "parent-close-keeps-child-guard", "bead.close", b01Epic, map[string]any{"id": parent}, "ontology_guard_failed")
					// Close the leaf despite its deferred/blocked ancestor.
					reply, _ := b01LocalWrite(t, h, "bead.close", b01Child, map[string]any{"id": grandchild})
					b01Committed(t, reply)
					b01LegacyLink(t, h, child, blocker, "blocks")
					b01LegacyRefusal(t, h, probe, "child-close-keeps-direct-blocker-guard", "bead.close", b01Child, map[string]any{"id": child}, "ontology_guard_failed")
					reply, _ = b01LocalWrite(t, h, "bead.unlink", b01Child, map[string]any{"from": child, "to": blocker, "kind": "blocks"})
					b01Committed(t, reply)
					reply, _ = b01LocalWrite(t, h, "bead.close", b01Child, map[string]any{"id": child})
					b01Committed(t, reply)
					for _, target := range []model.Ref{child, grandchild} {
						if status := h.get(t, h.viewer(t, b01Projects...), target, nil)["status"]; status != "done" {
							t.Fatalf("child close did not persist: %v", status)
						}
					}
					closed := f.state(t)
					if !reflect.DeepEqual(ancestorBefore.Records[parent.ID], closed.Records[parent.ID]) || !reflect.DeepEqual(ancestorBefore.Versions[parent.ID], closed.Versions[parent.ID]) {
						t.Fatal("child close changed the scheduling ancestor")
					}
					published := probe.calls.Load()
					reply, _ = b01LocalWrite(t, h, "bead.close", b01Child, map[string]any{"id": child})
					settledCommit := b01Committed(t, reply)
					settled := f.state(t)
					if reply.Meta.Replayed || settledCommit.Revision != closed.Meta.Head || settled.Meta.Head != closed.Meta.Head ||
						!reflect.DeepEqual(closed.Records, settled.Records) || !reflect.DeepEqual(closed.Versions, settled.Versions) ||
						!reflect.DeepEqual(closed.Log, settled.Log) || probe.calls.Load() != published {
						t.Fatal("close settlement changed graph, history or publication")
					}
					h.progress(t, h.viewer(t, b01Projects...), parent, nil, map[string]int{"total": 1, "status:done": 1}, 0)
					b01ReadyPages(t, h, free)
					verifyHierarchy(t, f)
				})
			}
		}
	}
}

func TestB01PublicHierarchyConflictsAndLegacyBoundaryAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, representation := range []string{"canonical", "legacy", "matching-duplicate"} {
			t.Run(engine+"/"+representation, func(t *testing.T) {
				f := newFixture(t, engine)
				probe := &b01PublicationProbe{}
				b01InstallPublicationProbe(f, probe)
				h := newB01Public(t, f)
				parent := h.create(t, b01Epic, "first parent", nil)
				other := h.create(t, b01Epic, "different parent", nil)
				child, grandchild := b01HierarchyPair(t, h, parent, representation)
				reply, _ := b01LocalWrite(t, h, "bead.create", b01Child, map[string]any{"type": "issue", "fields": map[string]any{"title": "unparented"}})
				orphan := b01Committed(t, reply).Results[0].Ref
				if probe.calls.Load() == 0 {
					t.Fatal("post-commit probe has no successful public-write call")
				}
				missing := b01Ref("pl-b01-parent-does-not-exist")
				b01LegacyRefusal(t, h, probe, "conflicting-parent-link", "bead.link", b01Child,
					map[string]any{"from": child, "to": other, "kind": "parent-child"}, "invalid_params")
				conflict := model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: child, To: other, LinkKind: "parent-child"}}
				b01LegacyRefusal(t, h, probe, "conflict-rolls-back-staged-prefix", "tx.apply", b01Child,
					map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{patchOp(orphan, map[string]any{"title": "must not persist"}), conflict}}}, "invalid_params")
				first := model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: orphan, To: parent, LinkKind: "parent-child"}}
				second := model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: orphan, To: other, LinkKind: "parent-child"}}
				b01LegacyRefusal(t, h, probe, "two-legacy-parents-refuse-atomically", "tx.apply", b01Child,
					map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{first, second}}}, "invalid_params")
				b01LegacyRefusal(t, h, probe, "missing-parent-link", "bead.link", b01Child,
					map[string]any{"from": orphan, "to": missing, "kind": "parent-child"}, "not_found")
				b01LegacyRefusal(t, h, probe, "missing-parent-reparent", "tx.apply", b01Child,
					map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{hierarchyMove(orphan, &missing)}}}, "not_found")
				b01LegacyRefusal(t, h, probe, "missing-parent-create", "bead.create", b01Child,
					map[string]any{"type": "issue", "fields": map[string]any{"title": "must not exist"}, "parent": missing}, "not_found")
				b01LegacyRefusal(t, h, probe, "mixed-representation-cycle-link", "bead.link", b01Epic,
					map[string]any{"from": parent, "to": grandchild, "kind": "parent-child"}, "ontology_guard_failed")
				b01LegacyRefusal(t, h, probe, "mixed-representation-cycle-reparent", "tx.apply", b01Epic,
					map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{hierarchyMove(parent, &grandchild)}}}, "ontology_guard_failed")
				b01LegacyRefusal(t, h, probe, "self-parent", "bead.link", b01Child,
					map[string]any{"from": orphan, "to": orphan, "kind": "parent-child"}, "ontology_guard_failed")
				branch := model.RefByID(parent.ID, model.Natural64(1))
				b01LegacyRefusal(t, h, probe, "nonzero-parent-branch", "bead.link", b01Child,
					map[string]any{"from": orphan, "to": branch, "kind": "parent-child"}, "invalid_params")
				move := hierarchyMove(orphan, &other)
				move.Reparent.Project = b01New
				b01LegacyRefusal(t, h, probe, "reparent-cannot-change-child-project", "tx.apply", b01Child,
					map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{move}}}, "invalid_params")
				if representation != "canonical" {
					before := f.state(t)
					published := probe.calls.Load()
					repeated, _ := b01LocalWrite(t, h, "bead.link", b01Child, map[string]any{"from": child, "to": parent, "kind": "parent-child"})
					repeatedCommit := b01Committed(t, repeated)
					after := f.state(t)
					if repeated.Meta.Replayed || repeatedCommit.Revision != before.Meta.Head || after.Meta.Head != before.Meta.Head ||
						!reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) ||
						!reflect.DeepEqual(before.Log, after.Log) || probe.calls.Load() != published {
						t.Fatal("matching legacy duplicate changed graph, history or publication")
					}
					b01LegacyRefusal(t, h, probe, "reparent-conflicts-with-retained-legacy-parent", "tx.apply", b01Child,
						map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &other)}}}, "invalid_params")
					b01LegacyRefusal(t, h, probe, "detach-cannot-leave-hidden-legacy-parent", "tx.apply", b01Child,
						map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{hierarchyMove(child, nil)}}}, "invalid_params")
					// B07/B11 keep one edge per endpoint pair; a kind change
					// requires unlinking the existing edge before relinking.
					b01LegacyRefusal(t, h, probe, "legacy-endpoint-retype-stays-refused", "bead.link", b01Child,
						map[string]any{"from": child, "to": parent, "kind": "blocks"}, "ontology_guard_failed")
				}
				// No new fixed-chain permit is inferred from compatibility input.
				for _, who := range []string{"owner", "child-only", "parent-only"} {
					t.Run("installed-authority-"+who, func(t *testing.T) {
						token := h.issue(t, b01Agent(who)).Token
						h.unchanged(t, func() b01Reply {
							return h.write(t, token, "bead.link", b01Child, map[string]any{"from": orphan, "to": parent, "kind": "parent-child"})
						}, "forbidden")
					})
				}
				b01ReadyPages(t, h, child, grandchild, orphan)
				b01StoredRepresentation(t, h, child, parent, representation != "canonical")
				// An explicit unlink followed by canonical reparent in one public
				// transaction is unambiguous and retains the child's project.
				before := f.state(t)
				unlink := model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: child, To: parent, LinkKind: "parent-child"}}
				reply, _ = b01LocalWrite(t, h, "tx.apply", b01Child, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{unlink, hierarchyMove(child, &other)}}})
				b01Committed(t, reply)
				b01StoredRepresentation(t, h, child, other, false)
				after := f.state(t)
				if len(after.Versions[child.ID]) != len(before.Versions[child.ID])+1 || len(after.Log) != len(before.Log)+1 {
					t.Fatal("atomic unlink/reparent did not make one child version and changeset")
				}
				if got := h.get(t, h.viewer(t, b01Projects...), child, nil)["project"]; got != b01Child {
					t.Fatalf("legacy transition moved the child's project: %v", got)
				}
				h.progress(t, h.viewer(t, b01Projects...), parent, nil, map[string]int{"total": 0}, 0)
				h.progress(t, h.viewer(t, b01Projects...), other, nil, map[string]int{"total": 1, "status:todo": 1}, 0)
				b01ReadyPages(t, h, child, grandchild, orphan)
				if engine != "memory" {
					h.api.Close()
					h.slots.Close()
					f.reopen(t)
					b01InstallPublicationProbe(f, probe)
					h.open(t)
					b01StoredRepresentation(t, h, child, other, false)
					b01StoredRepresentation(t, h, grandchild, child, representation != "canonical")
					b01LegacyRefusal(t, h, probe, "reopened-conflicting-parent", "bead.link", b01Child,
						map[string]any{"from": child, "to": parent, "kind": "parent-child"}, "invalid_params")
					b01ReadyPages(t, h, child, grandchild, orphan)
				}
				verifyHierarchy(t, f)
			})
		}
	}
}
