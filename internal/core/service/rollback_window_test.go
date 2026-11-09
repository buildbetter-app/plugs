package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter/reversetest"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// memory checkpoints are real transactional checkpoints, but intentionally do
// not claim restart durability. The rollback-window test needs the workflow
// provider to exercise its actual checkpoint write path, so this narrow test
// adapter supplies only the stronger marker; all checkpoint bytes still go to
// the real memory store.
type rollbackPersistentCheckpointStore struct {
	driver.CheckpointStore
}

func (rollbackPersistentCheckpointStore) Persistent() bool { return true }

type rollbackWindowRuntime struct {
	store     driver.Store
	lease     driver.ActiveLease
	service   *Service
	ontology  *Ontology
	converter *reverseconverter.Converter
}

func newRollbackWindowRuntime(t *testing.T, engine string, enabled bool, converter *reverseconverter.Converter) *rollbackWindowRuntime {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	switch engine {
	case "memory":
		store = memory.New()
	case "sqlite":
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "rollback-window.db")})
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown rollback-window engine %q", engine)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "rollback-window-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	ot := NewOntology(nil)
	runtime := &rollbackWindowRuntime{store: store, lease: lease, ontology: ot, converter: converter}
	runtime.service = New(store, lease, nil, WithOntology(ot), WithRollbackWindow(enabled), WithReverseConverter(converter))
	runtime.setWindow(enabled)
	runtime.installWorkflow(t)
	return runtime
}

func (r *rollbackWindowRuntime) setWindow(enabled bool) {
	r.service.rollbackWindow = enabled
	r.service.reverseConverter = r.converter
	r.service.writer = pipeline.New(r.store, r.lease,
		pipeline.WithOntology(r.ontology),
		pipeline.WithRollbackWindow(enabled),
		pipeline.WithReverseConverter(r.converter),
	)
}

func (r *rollbackWindowRuntime) installWorkflow(t *testing.T) {
	t.Helper()
	checkpoint, ok := r.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("test store lacks the checkpoint capability required by workflow")
	}
	if _, persistent := checkpoint.(driver.PersistentCheckpointStore); !persistent {
		checkpoint = rollbackPersistentCheckpointStore{CheckpointStore: checkpoint}
	}
	state := pluginhost.NewDurableState(checkpoint, r.lease, workflowplugin.Alias, "default")
	provider, err := workflowplugin.New(state)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := r.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r.service = New(r.store, r.lease, r.service.writer,
		WithOntology(r.ontology),
		WithRollbackWindow(r.service.rollbackWindow),
		WithReverseConverter(r.converter),
		WithWorkflow(provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)),
	)
}

func rollbackWorkflowRequest() workflowplugin.InvokeRequest {
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

func rollbackCheckpoint(t *testing.T, store driver.Store) driver.Checkpoint {
	t.Helper()
	checkpoint, ok := store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("store lacks checkpoint capability")
	}
	value, err := checkpoint.LoadCheckpoint(context.Background(), "plugin/workflow/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func rollbackChangesets(t *testing.T, store driver.Store) []model.Changeset {
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

func rollbackWorkflowState(t *testing.T, checkpoint driver.Checkpoint) (map[string]any, []any) {
	t.Helper()
	var envelope struct {
		Values map[string]struct {
			Value json.RawMessage `json:"value"`
		} `json:"values"`
	}
	if len(checkpoint.Payload) == 0 || json.Unmarshal(checkpoint.Payload, &envelope) != nil {
		t.Fatalf("invalid durable workflow checkpoint: %s", checkpoint.Payload)
	}
	entry := envelope.Values["workflow/state"]
	var state struct {
		Receipts map[string]any `json:"receipts"`
		Audit    []any          `json:"audit"`
	}
	if err := json.Unmarshal(entry.Value, &state); err != nil {
		t.Fatal(err)
	}
	return state.Receipts, state.Audit
}

func TestRollbackWindowRealWritePathsMemoryAndSQLite(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			runtime := newRollbackWindowRuntime(t, engine, true, nil)
			ctx := context.Background()
			beforeMeta, err := runtime.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			beforeChanges := rollbackChangesets(t, runtime.store)
			beforeCheckpoint := rollbackCheckpoint(t, runtime.store)

			// The native checklist has two user-content items. The common service
			// write boundary refuses the complete request before the provider can
			// publish a run, item, receipt or audit checkpoint entry.
			refusalMeta := meta(t)
			refusalMeta.Project = "local"
			_, err = runtime.service.WorkflowInvoke(ctx, anonymous, rollbackWorkflowRequest(), refusalMeta)
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != "workflow.run" {
				t.Fatalf("native refusal=%T %+v", err, err)
			}
			afterMeta, err := runtime.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if afterMeta.Head != beforeMeta.Head || !reflect.DeepEqual(rollbackChangesets(t, runtime.store), beforeChanges) || !reflect.DeepEqual(rollbackCheckpoint(t, runtime.store), beforeCheckpoint) {
				t.Fatal("denied native write changed graph history or workflow checkpoint")
			}

			// A mapped ordinary kind remains writable while the window is open.
			beadMeta := meta(t)
			beadMeta.Project = "local"
			commit, err := runtime.service.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "window bead"}}, beadMeta)
			if err != nil || len(commit.Results) != 1 {
				t.Fatalf("mapped ordinary write commit=%+v err=%v", commit, err)
			}
			read, err := runtime.service.Get(ctx, anonymous, GetRequest{ID: commit.Results[0].Ref, Project: "local"})
			if err != nil || read.Bead.Fields["title"] != "window bead" {
				t.Fatalf("ordinary write readback=%+v err=%v", read, err)
			}
			readerMeta, err := runtime.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := runtime.store.Read(ctx, driver.View{Incarnation: readerMeta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			principal, _ := driver.PrincipalKey(anonymous)
			receipt, receiptErr := reader.IdempotencyByKey(ctx, principal, beadMeta.AgentLabel, "bead.create", beadMeta.IdempotencyKey)
			changes, changesErr := reader.Changesets(ctx, nil, 1000)
			_ = reader.Close()
			if receiptErr != nil || receipt == nil || changesErr != nil || len(changes) == 0 {
				t.Fatalf("operational receipt was not retained: receipt=%+v receiptErr=%v changes=%+v changesErr=%v", receipt, receiptErr, changes, changesErr)
			}

			// Demote stages an ordinary deletion and a native checkpoint in the
			// real common Writer transaction. A native refusal must preserve both.
			demoteHead := readerMeta.Head
			demoteChanges := rollbackChangesets(t, runtime.store)
			demoteCheckpoint := rollbackCheckpoint(t, runtime.store)
			demoteMeta := meta(t)
			demoteMeta.Project = "local"
			_, err = runtime.service.WorkflowDemote(ctx, anonymous, workflowplugin.DemoteRequest{Source: commit.Results[0].Ref}, demoteMeta)
			if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != workflowplugin.RunKind {
				t.Fatalf("demote refusal=%T %+v", err, err)
			}
			afterDemote, err := runtime.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			retained, readErr := runtime.service.Get(ctx, anonymous, GetRequest{ID: commit.Results[0].Ref, Project: "local"})
			if readErr != nil || retained.Bead.Fields["title"] != "window bead" || afterDemote.Head != demoteHead || !reflect.DeepEqual(rollbackChangesets(t, runtime.store), demoteChanges) || !reflect.DeepEqual(rollbackCheckpoint(t, runtime.store), demoteCheckpoint) {
				t.Fatalf("denied atomic demote changed source, history or checkpoint: read=%+v err=%v", retained, readErr)
			}

			// A mapping for the run alone must still refuse the actual item kind,
			// without publishing either half of the operation.
			runtime.converter, err = reversetest.Converter(workflowplugin.RunKind)
			if err != nil {
				t.Fatal(err)
			}
			runtime.setWindow(true)
			partialMeta := meta(t)
			partialMeta.Project = "local"
			_, err = runtime.service.WorkflowInvoke(ctx, anonymous, rollbackWorkflowRequest(), partialMeta)
			if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != workflowplugin.ItemKind {
				t.Fatalf("partially mapped native refusal=%T %+v", err, err)
			}
			partialHead, err := runtime.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if partialHead.Head != demoteHead || !reflect.DeepEqual(rollbackChangesets(t, runtime.store), demoteChanges) || !reflect.DeepEqual(rollbackCheckpoint(t, runtime.store), demoteCheckpoint) {
				t.Fatal("partially mapped native refusal changed history or checkpoint")
			}

			runtime.converter, err = reversetest.Converter(workflowplugin.RunKind, workflowplugin.ItemKind)
			if err != nil {
				t.Fatal(err)
			}
			runtime.setWindow(true)
			mappedMeta := meta(t)
			mappedMeta.Project = "local"
			mapped, err := runtime.service.WorkflowInvoke(ctx, anonymous, rollbackWorkflowRequest(), mappedMeta)
			if err != nil || len(mapped.Items) != 2 {
				t.Fatalf("newly mapped native write=%+v err=%v", mapped, err)
			}
			for _, item := range mapped.Items {
				got, getErr := runtime.service.Get(ctx, anonymous, GetRequest{ID: model.RefByID(item.ID, model.Natural64(0)), Project: "local", Agent: "test-agent"})
				if getErr != nil || got.Bead.ID != item.ID {
					t.Fatalf("mapped native readback item=%+v err=%v", got, getErr)
				}
			}
			checkpoint := rollbackCheckpoint(t, runtime.store)
			if checkpoint.Generation == 0 {
				t.Fatal("mapped native write did not publish its durable checkpoint")
			}
			receipts, audit := rollbackWorkflowState(t, checkpoint)
			if len(receipts) == 0 || len(audit) == 0 {
				t.Fatalf("operational workflow metadata missing: receipts=%d audit=%d", len(receipts), len(audit))
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

			// Disabling the setting on the same stores permits the native write even
			// without the trusted mapping, proving the switch is reversible.
			runtime.converter = nil
			runtime.setWindow(true)
			// Existing content updates still publish new operational receipts/audit
			// with no native mapping, while creation remains refused below.
			beforeReceipts, beforeAudit := rollbackWorkflowState(t, rollbackCheckpoint(t, runtime.store))
			operationalMeta := meta(t)
			operationalMeta.Project = "local"
			_, err = runtime.service.Update(ctx, anonymous, UpdateRequest{ID: model.RefByID(mapped.Items[0].ID, model.Natural64(0)), Fields: map[string]any{"title": "updated existing native item"}}, operationalMeta)
			if err != nil {
				t.Fatalf("existing native update with operational metadata: %v", err)
			}
			afterReceipts, afterAudit := rollbackWorkflowState(t, rollbackCheckpoint(t, runtime.store))
			if len(afterReceipts) <= len(beforeReceipts) || len(afterAudit) <= len(beforeAudit) {
				t.Fatal("window refused new operational receipts/audit on existing content")
			}
			reenabledCheckpoint := rollbackCheckpoint(t, runtime.store)
			reenabledMeta := meta(t)
			reenabledMeta.Project = "local"
			_, err = runtime.service.WorkflowInvoke(ctx, anonymous, rollbackWorkflowRequest(), reenabledMeta)
			if !errors.As(err, &typed) || typed.Data.Code != model.CodeUnsupported || typed.Data.Details["kind"] != workflowplugin.RunKind || !reflect.DeepEqual(rollbackCheckpoint(t, runtime.store), reenabledCheckpoint) {
				t.Fatalf("unmapped write with window enabled=%T %+v", err, err)
			}
			runtime.setWindow(false)
			offMeta := meta(t)
			offMeta.Project = "local"
			off, err := runtime.service.WorkflowInvoke(ctx, anonymous, rollbackWorkflowRequest(), offMeta)
			if err != nil || len(off.Items) != 2 {
				t.Fatalf("disabled native write=%+v err=%v", off, err)
			}
			for _, item := range off.Items {
				got, err := runtime.service.Get(ctx, anonymous, GetRequest{ID: model.RefByID(item.ID, model.Natural64(0)), Project: "local", Agent: "test-agent"})
				if err != nil || got.Bead.ID != item.ID {
					t.Fatalf("disabled write readback=%+v err=%v", got, err)
				}
			}
			if rollbackCheckpoint(t, runtime.store).Generation <= checkpoint.Generation {
				t.Fatal("disabled native write did not advance its durable checkpoint")
			}
		})
	}
}
