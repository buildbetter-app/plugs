package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	coreservice "github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

var rollbackPostgresPrincipal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type rollbackPostgresRuntime struct {
	store    *Store
	lease    driver.ActiveLease
	service  *coreservice.Service
	ontology *coreservice.Ontology
	mapping  reverseconverter.KindMapping
	enabled  bool
	provider *workflowplugin.Provider
}

func newRollbackPostgresRuntime(t *testing.T, enabled bool, mapping reverseconverter.KindMapping) *rollbackPostgresRuntime {
	t.Helper()
	ctx := context.Background()
	store, err := Open(ctx, driver.Config{DSN: freshDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "rollback-window-postgres", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	ot := coreservice.NewOntology(nil)
	runtime := &rollbackPostgresRuntime{store: store, lease: lease, ontology: ot, mapping: mapping, enabled: enabled}
	state := pluginhost.NewDurableState(store, lease, workflowplugin.Alias, "default")
	provider, err := workflowplugin.New(state)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtime.provider = provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	runtime.setWindow(enabled)
	return runtime
}

func (r *rollbackPostgresRuntime) setWindow(enabled bool) {
	r.enabled = enabled
	writer := pipeline.New(r.store, r.lease,
		pipeline.WithOntology(r.ontology),
		pipeline.WithRollbackWindow(enabled),
		pipeline.WithRollbackKindMapping(r.mapping),
	)
	options := []coreservice.Option{
		coreservice.WithOntology(r.ontology),
		coreservice.WithRollbackWindow(enabled),
		coreservice.WithRollbackKindMapping(r.mapping),
	}
	if r.provider != nil {
		options = append(options, coreservice.WithWorkflow(r.provider))
	}
	r.service = coreservice.New(r.store, r.lease, writer, options...)
}

func rollbackPostgresWorkflowRequest() workflowplugin.InvokeRequest {
	return workflowplugin.InvokeRequest{
		Template: "rollback-window",
		Project:  "local",
		Mode:     "ephemeral",
		Checklist: []workflowplugin.ChecklistItem{
			{Label: "first", Title: "first native item"},
			{Label: "second", Title: "second native item"},
		},
	}
}

func rollbackPostgresMapping(kind string) (string, bool) {
	switch kind {
	case "workflow.run", "workflow.item":
		return "bead", true
	default:
		return "", false
	}
}

func rollbackPostgresMeta(n int) model.WriteMeta {
	return model.WriteMeta{Project: "local", Agent: "test-agent", AgentLabel: "test-agent", IdempotencyKey: fmt.Sprintf("%064d", n)}
}

func rollbackPostgresCheckpoint(t *testing.T, store *Store) driver.Checkpoint {
	t.Helper()
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/workflow/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func rollbackPostgresChangesets(t *testing.T, store *Store) []model.Changeset {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	entries, err := reader.Changesets(ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func rollbackPostgresWorkflowState(t *testing.T, checkpoint driver.Checkpoint) (int, int) {
	t.Helper()
	var envelope struct {
		Values map[string]struct {
			Value json.RawMessage `json:"value"`
		} `json:"values"`
	}
	if len(checkpoint.Payload) == 0 || json.Unmarshal(checkpoint.Payload, &envelope) != nil {
		t.Fatalf("invalid durable workflow checkpoint: %s", checkpoint.Payload)
	}
	var state struct {
		Receipts map[string]any `json:"receipts"`
		Audit    []any          `json:"audit"`
	}
	if err := json.Unmarshal(envelope.Values["workflow/state"].Value, &state); err != nil {
		t.Fatal(err)
	}
	return len(state.Receipts), len(state.Audit)
}

func TestRollbackWindowRealWritePathPostgreSQL(t *testing.T) {
	runtime := newRollbackPostgresRuntime(t, true, nil)
	ctx := context.Background()
	beforeMeta, err := runtime.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeChanges := rollbackPostgresChangesets(t, runtime.store)
	beforeCheckpoint := rollbackPostgresCheckpoint(t, runtime.store)
	refusalMeta := rollbackPostgresMeta(1)
	_, err = runtime.service.WorkflowInvoke(ctx, rollbackPostgresPrincipal, rollbackPostgresWorkflowRequest(), refusalMeta)
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != "workflow.run" {
		t.Fatalf("native refusal=%T %+v", err, err)
	}
	afterMeta, err := runtime.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterMeta.Head != beforeMeta.Head || !reflect.DeepEqual(rollbackPostgresChangesets(t, runtime.store), beforeChanges) || !reflect.DeepEqual(rollbackPostgresCheckpoint(t, runtime.store), beforeCheckpoint) {
		t.Fatal("denied native write changed PostgreSQL graph history or workflow checkpoint")
	}

	beadMeta := rollbackPostgresMeta(2)
	commit, err := runtime.service.Create(ctx, rollbackPostgresPrincipal, coreservice.CreateRequest{Type: "issue", Fields: map[string]any{"title": "window bead"}}, beadMeta)
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("ordinary write commit=%+v err=%v", commit, err)
	}
	read, err := runtime.service.Get(ctx, rollbackPostgresPrincipal, coreservice.GetRequest{ID: commit.Results[0].Ref, Project: "local"})
	if err != nil || read.Bead.Fields["title"] != "window bead" {
		t.Fatalf("ordinary write readback=%+v err=%v", read, err)
	}
	meta, err := runtime.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := runtime.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	principal, _ := driver.PrincipalKey(rollbackPostgresPrincipal)
	receipt, receiptErr := reader.IdempotencyByKey(ctx, principal, beadMeta.AgentLabel, "bead.create", beadMeta.IdempotencyKey)
	changes, changesErr := reader.Changesets(ctx, nil, 1000)
	_ = reader.Close()
	if receiptErr != nil || receipt == nil || changesErr != nil || len(changes) == 0 {
		t.Fatalf("operational receipt was not retained: receipt=%+v receiptErr=%v changes=%+v changesErr=%v", receipt, receiptErr, changes, changesErr)
	}

	runtime.mapping = rollbackPostgresMapping
	runtime.setWindow(true)
	mapped, err := runtime.service.WorkflowInvoke(ctx, rollbackPostgresPrincipal, rollbackPostgresWorkflowRequest(), rollbackPostgresMeta(3))
	if err != nil || len(mapped.Items) != 2 {
		t.Fatalf("mapped native write=%+v err=%v", mapped, err)
	}
	for _, item := range mapped.Items {
		got, getErr := runtime.service.Get(ctx, rollbackPostgresPrincipal, coreservice.GetRequest{ID: model.RefByID(item.ID, model.Natural64(0)), Project: "local", Agent: "test-agent"})
		if getErr != nil || got.Bead.ID != item.ID {
			t.Fatalf("mapped native readback item=%+v err=%v", got, getErr)
		}
	}
	checkpoint := rollbackPostgresCheckpoint(t, runtime.store)
	if checkpoint.Generation == 0 {
		t.Fatal("mapped native write did not publish its PostgreSQL checkpoint")
	}
	receipts, audit := rollbackPostgresWorkflowState(t, checkpoint)
	if receipts == 0 || audit == 0 {
		t.Fatalf("operational workflow metadata missing: receipts=%d audit=%d", receipts, audit)
	}

	// Switch off on the same PostgreSQL store and prove the previously refused
	// native kind can be written without a mapping.
	runtime.setWindow(false)
	off, err := runtime.service.WorkflowInvoke(ctx, rollbackPostgresPrincipal, rollbackPostgresWorkflowRequest(), rollbackPostgresMeta(4))
	if err != nil || len(off.Items) != 2 {
		t.Fatalf("disabled native write=%+v err=%v", off, err)
	}
	if rollbackPostgresCheckpoint(t, runtime.store).Generation <= checkpoint.Generation {
		t.Fatal("disabled native write did not advance its PostgreSQL checkpoint")
	}
}
