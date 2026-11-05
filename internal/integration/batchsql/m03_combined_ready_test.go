package batchsql_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

var m03ReadyPrincipal = model.PrincipalRef{Kind: "user", ID: "m03-ready-owner"}

func m03ReadyService(t *testing.T, f *fixture) *service.Service {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, ok := f.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("combined ready control requires a checkpoint store")
	}
	provider, err := workflowplugin.New(pluginhost.NewDurableState(checkpoints, f.lease, workflowplugin.Alias, "default"))
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(f.now).WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	return service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.now), service.WithWorkflow(provider))
}

func m03Invoke(t *testing.T, f *fixture, svc *service.Service, key string, priorities ...int) []workflowplugin.ItemView {
	t.Helper()
	items := make([]workflowplugin.ChecklistItem, len(priorities))
	for i, priority := range priorities {
		items[i] = workflowplugin.ChecklistItem{
			Label: fmt.Sprintf("m03-step-%d", i), Title: fmt.Sprintf("m03 step %d", i), Type: "task",
			Fields: map[string]any{"priority": priority},
		}
	}
	result, err := svc.WorkflowInvoke(f.ctx, m03ReadyPrincipal, workflowplugin.InvokeRequest{Template: "m03", Project: "batch-local", Mode: "ephemeral", Checklist: items}, model.WriteMeta{Project: "batch-local", IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	for i, item := range result.Items {
		if item.CreatedAt.IsZero() {
			t.Fatalf("provider item %d omitted authoritative run creation time: %+v", i, item)
		}
	}
	return result.Items
}

func m03CreateDurable(t *testing.T, f *fixture, priority int) string {
	t.Helper()
	op := createOp("m03-durable", "m03 durable", "batch-local")
	op.Create.Fields["priority"] = priority
	commit, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{op}}, f.meta("m03-durable"))
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("durable M03 control create: commit=%+v err=%v", commit, err)
	}
	return commit.Results[0].Ref.ID
}

func m03ReadyIDs(page service.BeadPage) []string {
	ids := make([]string, len(page.Items))
	for i, item := range page.Items {
		ids[i] = item.ID
	}
	return ids
}

func m03RequireCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != want {
		t.Fatalf("want %s, got %T %v", want, err, err)
	}
}

// M03 binds a normal provider item with no user-createdAt field to the same
// hybrid order as durable rows, then resumes its aggregate cursor after a
// real SQLite/PostgreSQL close and reopen.
func TestM03CombinedReadyMixedOrderAndReopen(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			base := f.clock
			f.clock = base.Add(-72 * time.Hour)
			durableID := m03CreateDurable(t, f, 0)
			f.clock = base
			svc := m03ReadyService(t, f)
			provider := m03Invoke(t, f, svc, "m03-mixed-provider", 4)[0]
			first, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1})
			if err != nil || len(first.Items) != 1 || first.Next == nil || first.Next.Workflow || first.Items[0].ID != provider.ID {
				t.Fatalf("mixed first page did not use one hybrid aggregate: page=%+v err=%v", first, err)
			}
			if first.Items[0].CreatedAt.IsZero() || first.Items[0].CreatedAt.Before(provider.CreatedAt) {
				t.Fatalf("provider creation time was not consumed by ready order: page=%+v provider=%+v", first, provider)
			}
			position := *first.Next
			if position.WorkflowSnapshot == nil || position.ReadyMembership == "" {
				t.Fatal("aggregate continuation lost its provider snapshot or full membership")
			}
			f.reopen(t)
			svc = m03ReadyService(t, f)
			second, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: &position})
			if err != nil || !reflect.DeepEqual(m03ReadyIDs(second), []string{durableID}) || second.Next != nil {
				t.Fatalf("reopened aggregate continuation duplicated/omitted rows: page=%+v err=%v", second, err)
			}
		})
	}
}

// Provider-only pages must keep the new aggregate cursor shape. Unequal
// priorities distinguish the merged order from the provider's legacy ID sort.
func TestM03ProviderOnlyUnequalPriorityPagesNoDuplication(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			svc := m03ReadyService(t, f)
			items := m03Invoke(t, f, svc, "m03-provider-only", 4, 0)
			// The invocation order is [priority 4, priority 0]; the shared
			// hybrid order must put the priority-0 row first regardless of IDs.
			wantFirst, wantSecond := items[1].ID, items[0].ID
			first, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1})
			if err != nil || len(first.Items) != 1 || first.Next == nil || first.Next.Workflow || first.Items[0].ID != wantFirst {
				t.Fatalf("provider-only first page lost priority order or aggregate cursor: page=%+v err=%v want=%s", first, err, wantFirst)
			}
			position := *first.Next
			f.reopen(t)
			svc = m03ReadyService(t, f)
			second, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: &position})
			if err != nil || len(second.Items) != 1 || second.Items[0].ID != wantSecond || second.Next != nil || second.Items[0].ID == first.Items[0].ID {
				t.Fatalf("provider-only continuation duplicated/omitted rows after reopen: first=%+v second=%+v err=%v", first, second, err)
			}
		})
	}
}

func TestM03CombinedReadyMixedParentSnapshotAndReopen(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			svc := m03ReadyService(t, f)
			child := model.RefByID(m03CreateDurable(t, f, 0), model.Natural64(0))
			grandchild := model.RefByID(m03CreateDurable(t, f, 0), model.Natural64(0))
			invokeMeta := f.meta("m03-parent-invoke")
			invokeMeta.Method = "x.workflow.invoke"
			result, err := svc.WorkflowInvoke(f.ctx, m03ReadyPrincipal, workflowplugin.InvokeRequest{
				Template: "m03-parents", Project: "batch-local", Mode: "ephemeral",
				Checklist: []workflowplugin.ChecklistItem{
					{Label: "parent", Title: "blocked parent", Type: "task", Blocked: true},
					{Label: "free-one", Title: "free one", Type: "task"},
					{Label: "free-two", Title: "free two", Type: "task"},
				},
			}, invokeMeta)
			if err != nil || len(result.Items) != 3 {
				t.Fatalf("parent control invocation: %+v err=%v", result, err)
			}
			parent := model.RefByID(result.Items[0].ID, model.Natural64(0))
			for i, request := range []service.LinkRequest{
				{From: child, To: parent, Kind: "parent-child"},
				{From: grandchild, To: child, Kind: "parent-child"},
			} {
				linkMeta := f.meta(fmt.Sprintf("m03-parent-link-%d", i))
				linkMeta.Method = "bead.link"
				if _, err := svc.Link(f.ctx, m03ReadyPrincipal, request, linkMeta); err != nil {
					t.Fatal(err)
				}
			}
			first, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1})
			if err != nil || len(first.Items) != 1 || first.Next == nil {
				t.Fatalf("mixed parent first page: %+v err=%v", first, err)
			}
			f.reopen(t)
			svc = m03ReadyService(t, f)
			second, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: first.Next})
			if err != nil || len(second.Items) != 1 || second.Next != nil || second.Items[0].ID == first.Items[0].ID {
				t.Fatalf("mixed parent continuation: first=%+v second=%+v err=%v", first, second, err)
			}
			got := map[string]bool{first.Items[0].ID: true, second.Items[0].ID: true}
			if !got[result.Items[1].ID] || !got[result.Items[2].ID] {
				t.Fatalf("blocked mixed ancestors leaked into ready pages: %v", got)
			}
			updateMeta := f.meta("m03-parent-unblock")
			updateMeta.Method = "bead.update"
			if _, err := svc.Update(f.ctx, m03ReadyPrincipal, service.UpdateRequest{ID: parent, Fields: map[string]any{"blocked": false}}, updateMeta); err != nil {
				t.Fatal(err)
			}
			_, err = svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: first.Next})
			m03RequireCode(t, err, "cursor_invalid")
			fresh, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 10})
			if err != nil || len(fresh.Items) != 5 || fresh.Next != nil {
				t.Fatalf("unblocked mixed ancestry omitted ready descendants: %+v err=%v", fresh, err)
			}
			ids := map[string]bool{}
			for _, item := range fresh.Items {
				ids[item.ID] = true
			}
			if len(ids) != 5 || !ids[child.ID] || !ids[grandchild.ID] || !ids[parent.ID] {
				t.Fatalf("unblocked mixed membership duplicated or omitted rows: %v", ids)
			}
		})
	}
}

func TestM03AggregateBoundsAndIncarnationPrecedence(t *testing.T) {
	f := newFixture(t, "sqlite")
	svc := m03ReadyService(t, f)
	m03Invoke(t, f, svc, "m03-bounds", 0, 1)
	first, err := svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1})
	if err != nil || first.Next == nil {
		t.Fatalf("bounds control first page: %+v err=%v", first, err)
	}
	_, err = svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1001})
	m03RequireCode(t, err, "invalid_params")
	invalidOffset := *first.Next
	invalidOffset.Offset = -1
	_, err = svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: &invalidOffset})
	m03RequireCode(t, err, "cursor_invalid")
	incarnation := *first.Next
	incarnation.AsOf.Incarnation = "changed-incarnation"
	incarnation.WorkflowSnapshot.Revision = model.Natural64(999)
	_, err = svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: &incarnation})
	m03RequireCode(t, err, "incarnation_changed")
	// A provider checkpoint mismatch is lower precedence than a matching core
	// incarnation and is refused before any partial continuation is emitted.
	m03Invoke(t, f, svc, "m03-provider-mutation", 2)
	_, err = svc.Ready(f.ctx, m03ReadyPrincipal, service.ReadyRequest{Project: "batch-local", Limit: 1, Position: first.Next})
	m03RequireCode(t, err, "cursor_invalid")
}
