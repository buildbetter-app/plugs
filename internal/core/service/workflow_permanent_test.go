package service

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func permanentWorkflowService(t *testing.T, engine string) (*Service, driver.Store) {
	t.Helper()
	if engine == "memory" {
		f := setup(t)
		return f.svc, f.store
	}
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "permanent.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "permanent-workflow", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	ontology := NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	return New(store, lease, writer, WithOntology(ontology)), store
}

func permanentWorkflowRequest() workflowplugin.InvokeRequest {
	return workflowplugin.InvokeRequest{Template: "template", Project: "local", Mode: "permanent", Checklist: []workflowplugin.ChecklistItem{
		{Label: "first", Title: "first durable item"},
		{Label: "second", Title: "second durable item", Type: "issue"},
	}}
}

func permanentWorkflowUnchanged(t *testing.T, store driver.Store, key model.WriteMeta, head model.Revision) {
	t.Helper()
	ctx := context.Background()
	current, err := store.Meta(ctx)
	if err != nil || current.Head != head {
		t.Fatalf("refused/dry invocation changed head: %+v err=%v", current, err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: current.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := reader.IdempotencyByKey(ctx, principal, key.AgentLabel, "x.workflow.invoke", key.IdempotencyKey)
	if err != nil || receipt != nil {
		t.Fatalf("refused/dry invocation retained a receipt: %+v err=%v", receipt, err)
	}
	changes, err := reader.Changesets(ctx, nil, 1000)
	if err != nil || len(changes) != 0 {
		t.Fatalf("refused/dry invocation retained history: changes=%d err=%v", len(changes), err)
	}
}

func TestPermanentWorkflowInvokeAtomicRetry(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, returned := range []string{"full", "minimal"} {
			t.Run(engine+"/"+returned, func(t *testing.T) {
				svc, store := permanentWorkflowService(t, engine)
				ctx := context.Background()
				req, key := permanentWorkflowRequest(), meta(t)
				key.Return = returned
				first, err := svc.WorkflowInvoke(ctx, anonymous, req, key)
				if err != nil || first.Plane != "permanent" || len(first.Items) != 2 {
					t.Fatalf("ordinary workflow invocation: %+v err=%v", first, err)
				}
				after, err := store.Meta(ctx)
				if err != nil || after.Head.String() != "1" {
					t.Fatalf("checklist did not publish one atomic revision: %+v err=%v", after, err)
				}
				for i, item := range first.Items {
					if item.ID == "" || strings.HasPrefix(item.ID, workflowplugin.ItemPrefix) || item.Type != "issue" || item.Kind != "bead" || item.Plane != "permanent" || item.Revision != after.Head {
						t.Fatalf("permanent result is not an ordinary committed item: %+v", item)
					}
					got, err := svc.Get(ctx, anonymous, GetRequest{ID: model.RefByID(item.ID, model.Natural64(0))})
					if err != nil || got.Bead.Type != "issue" || got.Bead.Fields["title"] != req.Checklist[i].Title {
						t.Fatalf("ordinary persisted body: %+v err=%v", got, err)
					}
				}
				retry, err := svc.WorkflowInvoke(ctx, anonymous, req, key)
				beforeJSON, _ := json.Marshal(first)
				retryJSON, _ := json.Marshal(retry)
				if err != nil || !bytes.Equal(beforeJSON, retryJSON) {
					t.Fatalf("response-loss retry changed its complete result: first=%s retry=%s err=%v", beforeJSON, retryJSON, err)
				}
				for _, changed := range []string{"template", "project", "label", "title", "order", "count", "optional-type"} {
					next := permanentWorkflowRequest()
					switch changed {
					case "template":
						next.Template = "different-template"
					case "project":
						next.Project = "different-project"
					case "label":
						next.Checklist[0].Label = "different-label"
					case "title":
						next.Checklist[0].Title = "different title"
					case "order":
						next.Checklist[0], next.Checklist[1] = next.Checklist[1], next.Checklist[0]
					case "count":
						next.Checklist = append(next.Checklist, workflowplugin.ChecklistItem{Label: "third", Title: "new item", Type: "issue"})
					case "optional-type":
						next.Checklist[0].Type = "issue"
					}
					_, err := svc.WorkflowInvoke(ctx, anonymous, next, key)
					wantCode(t, err, "idempotency_key_reused")
				}
				current, err := store.Meta(ctx)
				if err != nil || current.Head != after.Head {
					t.Fatalf("retry or reused invocation changed head: %+v err=%v", current, err)
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: current.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				changes, err := reader.Changesets(ctx, nil, 1000)
				if err != nil || len(changes) != 1 || len(changes[0].Changes) != 2 {
					t.Fatalf("checklist history is not one complete ordinary batch: changes=%+v err=%v", changes, err)
				}
			})
		}
	}
}

func TestPermanentWorkflowInvokeInvalidLaterItemIsAtomic(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			svc, store := permanentWorkflowService(t, engine)
			req, key := permanentWorkflowRequest(), meta(t)
			req.Checklist[1].Type = "unconfigured-type"
			_, err := svc.WorkflowInvoke(context.Background(), anonymous, req, key)
			wantCode(t, err, "invalid_params")
			permanentWorkflowUnchanged(t, store, key, model.Natural64(0))
			page, err := svc.Ready(context.Background(), anonymous, ReadyRequest{Project: "local"})
			if err != nil || len(page.Items) != 0 {
				t.Fatalf("later failure left a partial checklist: %+v err=%v", page, err)
			}
			req.Checklist[1].Type = "issue"
			if _, err := svc.WorkflowInvoke(context.Background(), anonymous, req, key); err != nil {
				t.Fatalf("failed attempt reserved the invocation key: %v", err)
			}
		})
	}
}

type permanentWorkflowWriter struct {
	model.Writer
	metas []model.WriteMeta
}

func (w *permanentWorkflowWriter) Apply(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (model.Commit, error) {
	w.metas = append(w.metas, meta)
	return w.Writer.Apply(ctx, by, cs, meta)
}

func TestPermanentWorkflowInvokeDryRunPreservesMetadataAndStorage(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			svc, store := permanentWorkflowService(t, engine)
			writer := &permanentWorkflowWriter{Writer: svc.writer}
			svc.writer = writer
			req, key := permanentWorkflowRequest(), meta(t)
			key.DryRun, key.Return = true, "minimal"
			key.Traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
			key.Project = "stale-project"
			for range 2 {
				preview, err := svc.WorkflowInvoke(context.Background(), anonymous, req, key)
				if err != nil || len(preview.Items) != 2 {
					t.Fatalf("ordinary dry run: %+v err=%v", preview, err)
				}
				permanentWorkflowUnchanged(t, store, key, model.Natural64(0))
				for _, item := range preview.Items {
					_, err := svc.Get(context.Background(), anonymous, GetRequest{ID: model.RefByID(item.ID, model.Natural64(0))})
					wantCode(t, err, "not_found")
				}
			}
			if len(writer.metas) != 2 {
				t.Fatalf("dry invocation did not use one Writer proposal per call: %d", len(writer.metas))
			}
			for _, got := range writer.metas {
				if !got.DryRun || got.Return != key.Return || got.Traceparent != key.Traceparent || got.Agent != key.Agent || got.AgentLabel != key.AgentLabel || got.Project != req.Project || got.Method != "x.workflow.invoke" || got.IdempotencyKey != key.IdempotencyKey {
					t.Fatalf("permanent lowering dropped effective metadata: %+v", got)
				}
			}
			key.DryRun = false
			if _, err := svc.WorkflowInvoke(context.Background(), anonymous, req, key); err != nil {
				t.Fatalf("dry run retained a receipt or poisoned the real invocation: %v", err)
			}
		})
	}
}

func TestPermanentWorkflowInvokeValidatesOriginalKeyAndChecklist(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			svc, store := permanentWorkflowService(t, engine)
			for _, invalid := range []string{"", "not-a-key", strings.Repeat("a", 64) + "-0"} {
				key := meta(t)
				key.IdempotencyKey = invalid
				_, err := svc.WorkflowInvoke(context.Background(), anonymous, permanentWorkflowRequest(), key)
				wantCode(t, err, "invalid_params")
				permanentWorkflowUnchanged(t, store, key, model.Natural64(0))
			}
			for _, invalid := range []string{"label", "title", "duplicate-label"} {
				req, key := permanentWorkflowRequest(), meta(t)
				switch invalid {
				case "label":
					req.Checklist[1].Label = " "
				case "title":
					req.Checklist[1].Title = " "
				case "duplicate-label":
					req.Checklist[1].Label = req.Checklist[0].Label
				}
				_, err := svc.WorkflowInvoke(context.Background(), anonymous, req, key)
				wantCode(t, err, "invalid_params")
				permanentWorkflowUnchanged(t, store, key, model.Natural64(0))
			}
		})
	}
}
