package service

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
	"sort"
	"strings"
)

func sortedMetadataKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func permanentWorkflowFields(item workflowplugin.ChecklistItem) map[string]any {
	fields := cloneMap(item.Fields)
	if fields == nil {
		fields = map[string]any{}
	}
	fields["title"] = item.Title
	if len(item.Labels) != 0 {
		fields["labels"] = append([]string(nil), item.Labels...)
	}
	if item.Blocked {
		fields["blocked"] = true
	}
	return fields
}

type workflowAtomicWriter interface {
	ApplyWorkflowMove(context.Context, model.PrincipalRef, model.ChangeSet, model.WriteMeta, driver.CheckpointMutation, map[string][]pipeline.Comment) (model.Commit, error)
}

func (s *Service) workflowMoveCommit(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, prepared workflowplugin.PreparedState, comments map[string][]pipeline.Comment) error {
	if err := s.admit(ctx, by); err != nil {
		return err
	}
	if strings.TrimSpace(meta.Project) == "" {
		return failure("project_required", "supply a project")
	}
	writer, ok := s.writer.(workflowAtomicWriter)
	if !ok {
		return failure("unsupported", "writer lacks atomic workflow move capability")
	}
	checkpoint, err := prepared.Checkpoint(ctx)
	if err != nil {
		return mapped(err)
	}
	_, err = writer.ApplyWorkflowMove(ctx, by, cs, meta, checkpoint, comments)
	return mapped(err)
}

// WorkflowPromote materializes the source into an existing, caller-authorized
// ordinary target. The target proposal and plugin CAS publish in one storage
// transaction; no ephemeral claim is copied into a core lease.
func (s *Service) WorkflowPromote(ctx context.Context, by model.PrincipalRef, req workflowplugin.PromoteRequest, meta model.WriteMeta) (workflowplugin.PromoteResult, error) {
	if err := validateWorkflowWrite(req.Item, nil, meta); err != nil {
		return workflowplugin.PromoteResult{}, err
	}
	if err := namedRef(req.Target); err != nil {
		return workflowplugin.PromoteResult{}, err
	}
	if s.workflow == nil {
		return workflowplugin.PromoteResult{}, failure("plugin_durable_state_unavailable", "ephemeral workflows require persistent checkpoint storage")
	}
	meta.Method = "x.workflow.run.promote"
	return s.workflow.PromoteAtomic(ctx, by, req, meta, func(item workflowplugin.ItemView, result workflowplugin.PromoteResult, prepared workflowplugin.PreparedState) error {
		if item.Claim != nil {
			return failure("busy", "release the workflow operational claim before promotion")
		}
		reader, err := s.reader(ctx, by, nil)
		if err != nil {
			return err
		}
		defer reader.Close()
		graph, err := scanBeads(ctx, reader)
		if err != nil {
			return err
		}
		target, ok := graph[req.Target.ID]
		if !ok {
			return failure("not_found", "promotion target was not found")
		}
		if target.doc.Project != item.Project || meta.Project != item.Project || target.doc.Type != item.Type {
			return failure("invalid_params", "promotion target must have the source project and type")
		}
		effective := s.ontology.effective(item.Project)
		category, known := effective.Category(state(target.doc).Status)
		compatible := known && (item.Status == "open" || item.Status == "claimed") && category == ontology.Unstarted
		if item.Status == "closed" {
			closeStatus, ok := effective.SlotStatus(item.Type, ontology.Close)
			compatible = ok && state(target.doc).Status == closeStatus
		}
		if !compatible {
			return failure("invalid_params", "prepare the promotion target in the equivalent ontology lifecycle state before moving")
		}
		if target.doc.Lease != nil {
			return failure("busy", "promotion target has an ordinary claim")
		}
		fields := cloneMap(item.Fields)
		if fields == nil {
			fields = map[string]any{}
		}
		fields["title"] = item.Title
		// Equivalent status remains owned by canonical ontology lifecycle slots.
		delete(fields, "status")
		fields["labels"] = append([]string{}, item.Labels...)
		if item.Blocked {
			fields["blocked"] = true
		} else {
			fields["blocked"] = nil
		}
		for name := range target.doc.Fields {
			if name == "status" {
				continue
			}
			if _, ok := fields[name]; !ok {
				fields[name] = nil
			}
		}
		ops := []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.Target, Fields: fields}}}
		if item.Parent != nil && workflowRef(*item.Parent) {
			return failure("unsupported", "promoting a workflow-parented item requires a durable hierarchy bridge")
		}
		if item.Parent != nil || target.doc.Parent != nil {
			ops = append(ops, model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: req.Target, Parent: item.Parent}})
		}
		for _, name := range sortedMetadataKeys(item.Metadata) {
			raw, err := json.Marshal(item.Metadata[name])
			if err != nil {
				return err
			}
			ops = append(ops, model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: req.Target, Key: name, Value: raw}})
		}
		for _, edge := range result.Relations {
			from := model.RefByID(edge.From, model.Natural64(0))
			if workflowRef(from) || workflowRef(edge.To) {
				continue
			}
			ops = append(ops, model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: edge.To, LinkKind: edge.Kind, Metadata: edge.Metadata}})
		}
		comments := map[string][]pipeline.Comment{}
		for _, comment := range item.Comments {
			comments[req.Target.ID] = append(comments[req.Target.ID], pipeline.Comment{Body: comment.Body, Principal: comment.Principal, Time: comment.Time})
		}
		revision := target.record.Revision
		return s.workflowMoveCommit(ctx, by, model.ChangeSet{Ops: ops, Expect: []model.Precondition{{Ref: req.Target, Revision: &revision}}}, meta, prepared, comments)
	})
}

// WorkflowDemote reads the source and all affected ordinary relations itself.
// It never trusts caller-supplied field/body projections. Unlinks, deletion and
// the new checkpoint image share the ordinary writer transaction.
func (s *Service) WorkflowDemote(ctx context.Context, by model.PrincipalRef, req workflowplugin.DemoteRequest, meta model.WriteMeta) (workflowplugin.DemoteResult, error) {
	if err := namedRef(req.Source); err != nil {
		return workflowplugin.DemoteResult{}, err
	}
	if meta.DryRun {
		return workflowplugin.DemoteResult{}, failure("invalid_params", "workflow moves do not support dryRun")
	}
	if strings.TrimSpace(meta.Project) == "" {
		return workflowplugin.DemoteResult{}, failure("project_required", "supply a project")
	}
	if s.workflow == nil {
		return workflowplugin.DemoteResult{}, failure("plugin_durable_state_unavailable", "ephemeral workflows require persistent checkpoint storage")
	}
	meta.Method = "x.workflow.run.demote"
	var cs model.ChangeSet
	return s.workflow.DemoteAtomic(ctx, by, req, meta, func() (workflowplugin.DemoteRequest, error) {
		reader, err := s.reader(ctx, by, nil)
		if err != nil {
			return workflowplugin.DemoteRequest{}, err
		}
		defer reader.Close()
		graph, err := scanBeads(ctx, reader)
		if err != nil {
			return workflowplugin.DemoteRequest{}, err
		}
		row, ok := graph[req.Source.ID]
		if !ok {
			return workflowplugin.DemoteRequest{}, failure("not_found", "demotion source was not found")
		}
		if row.doc.Project != meta.Project {
			return workflowplugin.DemoteRequest{}, failure("forbidden", "demotion source is outside the project")
		}
		if row.doc.Lease != nil {
			return workflowplugin.DemoteRequest{}, failure("busy", "release the ordinary claim before demotion")
		}
		if len(row.doc.Attachments) != 0 {
			return workflowplugin.DemoteRequest{}, failure("unsupported", "workflow items do not support attachment migration")
		}
		image := workflowplugin.DemoteRequest{Source: req.Source, Template: req.Template, Label: req.Label, Project: row.doc.Project, Type: row.doc.Type, Fields: cloneMap(row.doc.Fields), Parent: row.doc.Parent, Labels: append([]string(nil), row.doc.Labels...), Metadata: map[string]any{}}
		image.Title, _ = row.doc.Fields["title"].(string)
		if image.Label == "" {
			image.Label = req.Source.ID
		}
		category, known := s.ontology.effective(row.doc.Project).Category(state(row.doc).Status)
		if !known {
			return image, failure("invalid_params", "demotion source has no canonical status category")
		}
		switch category {
		case ontology.Unstarted, ontology.Started:
			// Unleased live work becomes an unclaimed workflow task. No core
			// lease or concrete ontology status is imported into plugin state.
			image.Status = "open"
		case ontology.Completed, ontology.Canceled:
			image.Status = "closed"
		default:
			return image, failure("unsupported", "demotion of deferred or triage work requires a workflow scheduling bridge")
		}
		delete(image.Fields, "status")
		image.Blocked, _ = row.doc.Fields["blocked"].(bool)
		for name, raw := range row.doc.Metadata {
			var value any
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				return image, err
			}
			image.Metadata[name] = value
		}
		for _, comment := range row.doc.Comments {
			if comment.Origin != "" {
				return image, failure("unsupported", "imported comment provenance requires a workflow provenance bridge")
			}
			image.Comments = append(image.Comments, workflowplugin.CommentView{Body: comment.Body, Principal: comment.Principal, Time: comment.Time})
		}
		for _, id := range sortedIDs(graph) {
			other := graph[id]
			if other.doc.Parent != nil && other.doc.Parent.ID == req.Source.ID {
				return image, failure("unsupported", "demoting a durable parent requires a cross-plane hierarchy bridge")
			}
			for _, edge := range other.doc.Links {
				if id != req.Source.ID && edge.To.ID != req.Source.ID {
					continue
				}
				from := model.RefByID(id, model.Natural64(0))
				cs.Ops = append(cs.Ops, model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{From: from, To: edge.To, LinkKind: edge.Kind}})
				if id == req.Source.ID {
					image.Links = append(image.Links, workflowplugin.RelationSpec{To: edge.To, Kind: edge.Kind, Metadata: edge.Metadata})
				} else {
					image.Inbound = append(image.Inbound, workflowplugin.RelationView{From: id, To: req.Source, Kind: edge.Kind, Metadata: edge.Metadata})
				}
				revision := other.record.Revision
				cs.Expect = append(cs.Expect, model.Precondition{Ref: from, Revision: &revision})
			}
		}
		// Read mixed edges without re-entering the provider fence held by the plan.
		revision := row.record.Revision
		cs.Expect = append(cs.Expect, model.Precondition{Ref: req.Source, Revision: &revision})
		cs.Ops = append(cs.Ops, model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: req.Source}})
		return image, nil
	}, func(result workflowplugin.DemoteResult, prepared workflowplugin.PreparedState) error {
		return s.workflowMoveCommit(ctx, by, cs, meta, prepared, nil)
	})
}
