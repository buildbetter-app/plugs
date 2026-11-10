package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter/reversetest"
	coreservice "github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

var rollbackPostgresPrincipal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type rollbackPostgresRuntime struct {
	store     *Store
	lease     driver.ActiveLease
	service   *coreservice.Service
	ontology  *coreservice.Ontology
	converter *reverseconverter.Converter
	enabled   bool
	provider  *workflowplugin.Provider
}

func newRollbackPostgresRuntime(t *testing.T, enabled bool, converter *reverseconverter.Converter) *rollbackPostgresRuntime {
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
	runtime := &rollbackPostgresRuntime{store: store, lease: lease, ontology: ot, converter: converter, enabled: enabled}
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
		pipeline.WithReverseConverter(r.converter),
	)
	options := []coreservice.Option{
		coreservice.WithOntology(r.ontology),
		coreservice.WithRollbackWindow(enabled),
		coreservice.WithReverseConverter(r.converter),
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

	// Exercise the staged move through the real PostgreSQL Writer: refusal
	// cannot delete the source or publish native content, history or receipts.
	demoteHead := meta.Head
	demoteChanges := rollbackPostgresChangesets(t, runtime.store)
	demoteCheckpoint := rollbackPostgresCheckpoint(t, runtime.store)
	_, err = runtime.service.WorkflowDemote(ctx, rollbackPostgresPrincipal, workflowplugin.DemoteRequest{Source: commit.Results[0].Ref}, rollbackPostgresMeta(5))
	if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != workflowplugin.RunKind {
		t.Fatalf("demote refusal=%T %+v", err, err)
	}
	afterDemote, err := runtime.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retained, readErr := runtime.service.Get(ctx, rollbackPostgresPrincipal, coreservice.GetRequest{ID: commit.Results[0].Ref, Project: "local"})
	if readErr != nil || retained.Bead.Fields["title"] != "window bead" || afterDemote.Head != demoteHead || !reflect.DeepEqual(rollbackPostgresChangesets(t, runtime.store), demoteChanges) || !reflect.DeepEqual(rollbackPostgresCheckpoint(t, runtime.store), demoteCheckpoint) {
		t.Fatalf("denied atomic demote changed source, history or checkpoint: read=%+v err=%v", retained, readErr)
	}

	// A mapping for the run alone must still refuse the actual item kind,
	// without publishing either half of the operation.
	runtime.converter, err = reversetest.Converter(workflowplugin.RunKind)
	if err != nil {
		t.Fatal(err)
	}
	runtime.setWindow(true)
	partialMeta := rollbackPostgresMeta(7)
	partialMeta.Project = "local"
	_, err = runtime.service.WorkflowInvoke(ctx, rollbackPostgresPrincipal, rollbackPostgresWorkflowRequest(), partialMeta)
	if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != workflowplugin.ItemKind {
		t.Fatalf("partially mapped native refusal=%T %+v", err, err)
	}
	partialHead, err := runtime.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if partialHead.Head != demoteHead || !reflect.DeepEqual(rollbackPostgresChangesets(t, runtime.store), demoteChanges) || !reflect.DeepEqual(rollbackPostgresCheckpoint(t, runtime.store), demoteCheckpoint) {
		t.Fatal("partially mapped native refusal changed history or checkpoint")
	}

	runtime.converter, err = reversetest.Converter(workflowplugin.RunKind, workflowplugin.ItemKind)
	if err != nil {
		t.Fatal(err)
	}
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

	// Admission and reverse output use the same executable registry. The
	// complete persisted record, including every native field, must survive.
	dump, err := reversetest.WorkflowDump(checkpoint.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reverseconverter.Convert(dump); err == nil {
		t.Fatal("default reverse converter unexpectedly accepted unmapped native kinds")
	}
	converted, err := runtime.converter.Convert(dump)
	if err != nil || len(converted.Projection.Memories) != 3 {
		t.Fatalf("registered reverse conversion=%+v err=%v", converted, err)
	}
	expected := map[string]string{}
	for kind, records := range dump.Records {
		for _, record := range records {
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			expected[kind+"/"+record.ID] = string(encoded)
		}
	}
	lines := bytes.Split(bytes.TrimSpace(converted.JSONL), []byte("\n"))
	if len(lines) != len(expected)+1 {
		t.Fatalf("reverse JSONL=%s", converted.JSONL)
	}
	for _, line := range lines[1:] {
		var memory struct {
			Kind  string `json:"_type"`
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.Unmarshal(line, &memory); err != nil {
			t.Fatal(err)
		}
		if memory.Kind != "memory" || expected[memory.Key] == "" || memory.Value != expected[memory.Key] {
			t.Fatalf("reverse output lost native payload: %s", line)
		}
		delete(expected, memory.Key)
	}
	if len(expected) != 0 {
		t.Fatalf("reverse output omitted native records: %v", expected)
	}

	// Switch off on the same PostgreSQL store and prove the previously refused
	// native kind can be written without a mapping.
	runtime.converter = nil
	runtime.setWindow(true)
	// Existing content updates still publish new operational receipts/audit
	// with no native mapping, while creation remains refused below.
	beforeReceipts, beforeAudit := rollbackPostgresWorkflowState(t, rollbackPostgresCheckpoint(t, runtime.store))
	operationalMeta := rollbackPostgresMeta(8)
	operationalMeta.Project = "local"
	_, err = runtime.service.Update(ctx, rollbackPostgresPrincipal, coreservice.UpdateRequest{ID: model.RefByID(mapped.Items[0].ID, model.Natural64(0)), Fields: map[string]any{"title": "updated existing native item"}}, operationalMeta)
	if err != nil {
		t.Fatalf("existing native update with operational metadata: %v", err)
	}
	afterReceipts, afterAudit := rollbackPostgresWorkflowState(t, rollbackPostgresCheckpoint(t, runtime.store))
	if afterReceipts <= beforeReceipts || afterAudit <= beforeAudit {
		t.Fatal("window refused new operational receipts/audit on existing content")
	}
	reenabledCheckpoint := rollbackPostgresCheckpoint(t, runtime.store)
	_, err = runtime.service.WorkflowInvoke(ctx, rollbackPostgresPrincipal, rollbackPostgresWorkflowRequest(), rollbackPostgresMeta(6))
	if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != workflowplugin.RunKind || !reflect.DeepEqual(rollbackPostgresCheckpoint(t, runtime.store), reenabledCheckpoint) {
		t.Fatalf("unmapped write with window enabled=%T %+v", err, err)
	}
	runtime.setWindow(false)
	off, err := runtime.service.WorkflowInvoke(ctx, rollbackPostgresPrincipal, rollbackPostgresWorkflowRequest(), rollbackPostgresMeta(4))
	if err != nil || len(off.Items) != 2 {
		t.Fatalf("disabled native write=%+v err=%v", off, err)
	}
	for _, item := range off.Items {
		got, err := runtime.service.Get(ctx, rollbackPostgresPrincipal, coreservice.GetRequest{ID: model.RefByID(item.ID, model.Natural64(0)), Project: "local", Agent: "test-agent"})
		if err != nil || got.Bead.ID != item.ID {
			t.Fatalf("disabled write readback=%+v err=%v", got, err)
		}
	}
	if rollbackPostgresCheckpoint(t, runtime.store).Generation <= checkpoint.Generation {
		t.Fatal("disabled native write did not advance its PostgreSQL checkpoint")
	}
}
