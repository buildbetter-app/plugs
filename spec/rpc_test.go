package spec

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/acceptance"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

func idRef(id string) map[string]any { return map[string]any{"id": id} }

func (f *stack) create(t *testing.T, title string, extra map[string]any) json.RawMessage {
	t.Helper()
	fields := map[string]any{"title": title}
	for k, v := range extra {
		fields[k] = v
	}
	return f.success(t, "bead.create", map[string]any{"type": "issue", "fields": fields})
}

func TestB01ReadyCategoriesAndPriorityOrder(t *testing.T) {
	f := newStack(t, []string{"pl-z", "pl-b", "pl-a", "pl-backlog", "pl-done"})
	f.create(t, "lower priority", map[string]any{"priority": 2})
	f.create(t, "tie b", map[string]any{"priority": 1})
	f.create(t, "tie a", map[string]any{"priority": 1})
	f.create(t, "not ready category", nil)
	f.create(t, "terminal", nil)
	f.core.slot(t, "bead.defer", "pl-backlog")
	f.success(t, "bead.close", map[string]any{"id": idRef("pl-done")})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-a", "pl-b", "pl-z")
	f.success(t, "bead.claim", map[string]any{"id": idRef("pl-a")})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-b", "pl-z")
	first := f.success(t, "bead.ready", map[string]any{"limit": 1})
	assertIDs(t, first, "pl-b")
	cursor := valueAt[string](t, first, "/result/nextCursor")
	if cursor == "" {
		t.Fatal("bounded ready result omitted its continuation")
	}
	assertIDs(t, f.success(t, "bead.ready", map[string]any{"limit": 1, "cursor": cursor}), "pl-z")
}

func TestB02OrdinaryBlockerCloseAndUnlink(t *testing.T) {
	f := newStack(t, []string{"pl-blocker", "pl-dependent", "pl-unlink"})
	f.create(t, "blocker", nil)
	f.create(t, "dependent", nil)
	f.create(t, "unlink-dependent", nil)
	blocker := model.RefByID("pl-blocker", model.Natural64(0))
	f.core.apply(t, "normal-links", model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpLink, Link: &model.LinkOp{From: model.RefByID("pl-dependent", model.Natural64(0)), To: blocker, LinkKind: "blocks"}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: model.RefByID("pl-unlink", model.Natural64(0)), To: blocker, LinkKind: "blocks"}},
	}})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-blocker")
	// Backlog is nonterminal, so it must continue blocking its dependents.
	f.core.slot(t, "bead.defer", "pl-blocker")
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}))
	f.core.apply(t, "normal-unlink", model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpUnlink, Link: &model.LinkOp{From: model.RefByID("pl-unlink", model.Natural64(0)), To: blocker, LinkKind: "blocks"}},
	}})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-unlink")
	f.success(t, "bead.close", map[string]any{"id": idRef("pl-blocker")})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-dependent", "pl-unlink")
}

func TestB02LiveBlockerRemovalAndConditionalOutcome(t *testing.T) {
	f := newStack(t, []string{"pl-blocker", "pl-dependent", "pl-conditional", "pl-failed", "pl-on-failure"})
	f.create(t, "blocker", nil)
	f.create(t, "dependent", nil)
	f.create(t, "conditional", nil)
	f.create(t, "already failed", nil)
	f.create(t, "failure-dependent", nil)
	f.core.slot(t, "bead.fail", "pl-failed")
	link := func(from, to, kind string) model.Op {
		return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: model.RefByID(from, model.Natural64(0)), To: model.RefByID(to, model.Natural64(0)), LinkKind: kind}}
	}
	f.core.apply(t, "links", model.ChangeSet{Ops: []model.Op{
		link("pl-dependent", "pl-blocker", "blocks"),
		link("pl-conditional", "pl-blocker", "conditional-blocks"),
		link("pl-on-failure", "pl-failed", "conditional-blocks"),
	}})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-blocker", "pl-on-failure")
	f.success(t, "bead.close", map[string]any{"id": idRef("pl-blocker")})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-dependent", "pl-on-failure")
	f.core.apply(t, "unlink-condition", model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{
		From: model.RefByID("pl-conditional", model.Natural64(0)), To: model.RefByID("pl-blocker", model.Natural64(0)), LinkKind: "conditional-blocks",
	}}}})
	assertIDs(t, f.success(t, "bead.ready", map[string]any{}), "pl-conditional", "pl-dependent", "pl-on-failure")
}

func TestB04DirectClaimLeaseAndForeignRefusal(t *testing.T) {
	f := newStack(t, []string{"pl-claim"})
	f.create(t, "claimable", nil)
	claimed := f.success(t, "bead.claim", map[string]any{"id": idRef("pl-claim")})
	assertPaths(t, claimed, map[string]any{
		"/result/revision":         2,
		"/result/results/0/ref/id": "pl-claim",
		"/result/leaseTtlMs":       300000,
		"/result/leaseExpiresAt":   f.core.inputs.Now().Add(5 * time.Minute).Format(time.RFC3339Nano),
	})
	get := f.success(t, "bead.get", map[string]any{"id": idRef("pl-claim")})
	assertPaths(t, get, map[string]any{"/result/bead/status": "in_progress"})
	lease := f.core.document(t, "pl-claim").Lease
	if lease == nil || !reflect.DeepEqual(lease.Principal, f.core.by) || lease.Agent != "acceptance-worker" {
		t.Fatalf("claim did not persist its actual principal and agent scope: %+v", lease)
	}
	// Reclaim by the same actor is a no-op even under a fresh request key.
	reclaimed := f.success(t, "bead.claim", map[string]any{"id": idRef("pl-claim")})
	assertPaths(t, reclaimed, map[string]any{
		"/result/revision": 2, "/result/results/0/revision": 2,
		"/result/leaseTtlMs":     300000,
		"/result/leaseExpiresAt": f.core.inputs.Now().Add(5 * time.Minute).Format(time.RFC3339Nano),
	})
	if !reflect.DeepEqual(f.core.document(t, "pl-claim").Lease, lease) {
		t.Fatal("same-owner fresh-key claim changed the durable lease")
	}
	f.core.assertLog(t, 2)
	foreign := rpc.WithPrincipal(f.core.ctx, model.PrincipalRef{Kind: "user", ID: "another-user"})
	response := f.callAs(t, foreign, "bead.claim", map[string]any{"id": idRef("pl-claim")})
	assertPaths(t, response, map[string]any{"/error/data/code": "busy"})
	response = f.callAs(t, foreign, "bead.update", map[string]any{"id": idRef("pl-claim"), "fields": map[string]any{"title": "must not land"}})
	assertPaths(t, response, map[string]any{"/error/data/code": "lease_not_held"})
	response = f.callAs(t, foreign, "bead.close", map[string]any{"id": idRef("pl-claim")})
	assertPaths(t, response, map[string]any{"/error/data/code": "lease_not_held"})
	assertPaths(t, f.success(t, "bead.get", map[string]any{"id": idRef("pl-claim")}), map[string]any{
		"/result/bead/status": "in_progress", "/result/bead/fields/title": "claimable",
	})
	f.core.assertLog(t, 2)
}

func TestB15CommentsVerbatimOrderedAndValidated(t *testing.T) {
	f := newStack(t, []string{"pl-comments"})
	f.create(t, "anchor", nil)
	before := f.core.document(t, "pl-comments")
	body := " \t**note**\n\nline with  two spaces \n"
	f.success(t, "bead.comment", map[string]any{"id": idRef("pl-comments"), "body": body})
	f.success(t, "bead.comment", map[string]any{"id": idRef("pl-comments"), "body": "second"})
	after := f.core.document(t, "pl-comments")
	if len(after.Comments) != 2 || after.Comments[0].Body != body || after.Comments[1].Body != "second" {
		t.Fatalf("comment text or thread order changed: %+v", after.Comments)
	}
	if !reflect.DeepEqual(after.Comments[0].Principal, f.core.by) || !reflect.DeepEqual(after.Comments[1].Principal, f.core.by) {
		t.Fatal("comment author was not preserved")
	}
	if !after.Comments[0].Time.Equal(f.core.inputs.Now()) || !after.Comments[1].Time.Equal(f.core.inputs.Now().Add(time.Second)) {
		t.Fatalf("same-clock comments did not advance past the thread head: %+v", after.Comments)
	}
	a, _ := json.Marshal(before.Fields)
	b, _ := json.Marshal(after.Fields)
	if err := acceptance.EqualJSON(a, b); err != nil {
		t.Fatalf("comment changed anchor fields: %v", err)
	}
	for _, body := range []string{"", " \t\n"} {
		response := f.call(t, "bead.comment", map[string]any{"id": idRef("pl-comments"), "body": body})
		assertPaths(t, response, map[string]any{"/error/data/code": "invalid_params"})
	}
	missing := f.call(t, "bead.comment", map[string]any{"id": idRef("pl-absent"), "body": "nonblank"})
	assertPaths(t, missing, map[string]any{"/error/data/code": "not_found"})
	f.core.assertLog(t, 3)
}

func TestB18HistoricalReadBoundaryAndNoOp(t *testing.T) {
	f := newStack(t, []string{"pl-history"})
	f.create(t, "original", nil)
	f.success(t, "bead.update", map[string]any{"id": idRef("pl-history"), "fields": map[string]any{"title": "edited"}})
	for _, version := range []struct {
		revision int
		title    string
	}{{1, "original"}, {2, "edited"}} {
		response := f.success(t, "history.query", map[string]any{"asOf": map[string]any{"revision": version.revision, "incarnation": f.incarnation}})
		assertIDs(t, response, "pl-history")
		assertPaths(t, response, map[string]any{"/result/items/0/fields/title": version.title, "/result/items/0/revision": version.revision})
	}
	missing := f.call(t, "bead.get", map[string]any{"id": idRef("pl-history"), "asOf": map[string]any{"revision": 0, "incarnation": f.incarnation}})
	assertPaths(t, missing, map[string]any{"/error/data/code": "not_found"})
	f.success(t, "bead.update", map[string]any{"id": idRef("pl-history"), "fields": map[string]any{"title": "edited"}})
	f.core.assertLog(t, 2)
	versions := f.core.versions(t, model.RefByID("pl-history", model.Natural64(0)))
	if len(versions) != 2 {
		t.Fatalf("no-op minted a version or a real write omitted one: %d", len(versions))
	}
}

func TestConnectedTraceRepeatability(t *testing.T) {
	run := func() []byte {
		f := newStack(t, []string{"pl-repeatable"})
		f.create(t, "original", nil)
		f.success(t, "bead.ready", map[string]any{})
		f.success(t, "bead.update", map[string]any{"id": idRef("pl-repeatable"), "fields": map[string]any{"title": "edited"}})
		f.success(t, "bead.get", map[string]any{"id": idRef("pl-repeatable")})
		bytes, err := json.Marshal(f.trace.Frames())
		if err != nil {
			t.Fatal(err)
		}
		return bytes
	}
	first, second := run(), run()
	if string(first) != string(second) {
		t.Fatalf("fresh real stacks with the same inputs changed their trace\nfirst: %s\nsecond: %s", first, second)
	}
}

func TestB28RPCWarningIsCommittedResult(t *testing.T) {
	f := newStack(t, []string{"pl-wire-warning"}, pipeline.WithPostSubscribers(pipeline.PostBinding{
		ID: "x.accept/post",
		Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
			return errors.New("controlled unavailable destination")
		}),
	}))
	response := f.create(t, "committed", nil)
	assertPaths(t, response, map[string]any{
		"/result/revision":        1,
		"/result/warnings/0/code": "post_subscriber_failed",
	})
	f.success(t, "bead.get", map[string]any{"id": idRef("pl-wire-warning")})
	f.core.assertLog(t, 1)
}
