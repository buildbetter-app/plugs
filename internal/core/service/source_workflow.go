package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type sourceWorkflow struct {
	provider    *workflowplugin.Provider
	digest      string
	profile     driver.RestoreImportProfile
	restoreTime time.Time
	active      map[string]string // exact target -> project, retained after completion
	addresses   map[sourceAddress]bool
}
type sourceAddress struct{ project, id string }

// InstallSourceWorkflow is a trusted host seam, never an RPC. The host first
// recovers the original joint receipt and immutable namespace origin. The final
// receipt binds verified import attribution separately from raw source authors.
// Installing preserves the method closures already bound by transports.
func (s *Service) InstallSourceWorkflow(ctx context.Context, provider *workflowplugin.Provider, profile driver.RestoreImportProfile, commit driver.SourcePlaneCommitBindingV1, receipt driver.IdempotencyRecord) error {
	bound, err := profile.Detached()
	if err != nil || bound.SourcePlanes == nil {
		return driver.ErrConflict
	}
	binding := bound.SourcePlanes
	plan, err := binding.Plan()
	if err != nil {
		return err
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return err
	}
	initial := driver.Meta{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Head: model.Natural64(1), HistoryFloor: model.Natural64(1)}
	if provider == nil || meta.Head.IsZero() || commit.Validate(bound, initial) != nil || len(commit.Providers) != 1 || len(plan.Providers) != 1 {
		return driver.ErrConflict
	}
	change := receipt.Changeset
	header := driver.ChangesetHeader{Branch: change.Branch, Incarnation: change.Incarnation, Kind: change.Kind, Time: change.Time, Principal: change.Principal, IdempotencyKey: change.IdempotencyKey, Method: bound.Method, AgentLabel: bound.AgentLabel(), RequestHash: receipt.RequestHash, ResponsePending: receipt.ResponsePending, Changes: change.Changes}
	if bound.ValidateHeader(header) != nil || !change.Branch.IsZero() || change.Incarnation != commit.Incarnation || change.Revision != commit.Revision || receipt.Commit.Revision != commit.Revision || receipt.RequestHash != commit.RequestHash {
		return driver.ErrConflict
	}
	evidence, err := json.Marshal(commit)
	if err != nil {
		return err
	}
	matched := 0
	for _, result := range receipt.Commit.Results {
		if result.Ref.ID != driver.SourcePlaneCommitRecordID {
			continue
		}
		if !result.Ref.Branch.IsZero() || result.Revision != commit.Revision || !bytes.Equal(result.Record, evidence) {
			return driver.ErrConflict
		}
		matched++
	}
	if matched != 1 {
		return driver.ErrConflict
	}
	installed := &sourceWorkflow{provider: provider, digest: commit.RequestHash, profile: bound, restoreTime: change.Time, active: map[string]string{}, addresses: map[sourceAddress]bool{}}
	for _, entity := range plan.Entities {
		installed.addresses[sourceAddress{entity.Source.Project, entity.Source.ID}] = true
		if entity.TargetKind == "workflow:item" {
			installed.active[entity.TargetID] = entity.Source.Project
		}
	}
	for {
		prior := s.source.Load()
		if prior != nil && (prior.digest != installed.digest || !prior.restoreTime.Equal(installed.restoreTime)) {
			return driver.ErrConflict
		}
		if s.source.CompareAndSwap(prior, installed) {
			break
		}
	}
	return nil
}

func (s *Service) workflowProvider() *workflowplugin.Provider {
	if source := s.source.Load(); source != nil {
		return source.provider
	}
	return s.workflow
}

// SourceRuntimeInstalled lets the guarded transport select the canonical
// writer authorization path instead of its legacy standalone provider path.
func (s *Service) SourceRuntimeInstalled() bool { return s.source.Load() != nil }

// SourceWorkflowProject exposes only the immutable imported target binding to
// the guarded transport. The provider still checks current presence and owner.
func (s *Service) SourceWorkflowProject(ref model.Ref) (string, bool) {
	source := s.source.Load()
	if source == nil || !ref.IsID() || !ref.Branch.IsZero() {
		return "", false
	}
	project, ok := source.active[ref.ID]
	return project, ok
}

// ResolveSourceAddress is an active-first current-view address lookup. A
// known source with no remaining target refuses instead of falling through to
// an unrelated ordinary record with the same naked ID.
func (s *Service) ResolveSourceAddress(ctx context.Context, by model.PrincipalRef, project string, ref model.Ref) (model.Ref, bool, error) {
	source := s.source.Load()
	if source == nil || !ref.IsID() {
		return ref, false, nil
	}
	if !ref.Branch.IsZero() {
		return model.Ref{}, false, failure("invalid_params", "source addresses require branch zero")
	}
	if !source.addresses[sourceAddress{project, ref.ID}] {
		return ref, false, nil
	}
	resolved, found, err := source.provider.ResolveSourceRef(ctx, by, project, ref.ID, workflowMeta(by, "bead.get", project, ""))
	if err != nil {
		return model.Ref{}, true, err
	}
	if !found {
		return model.Ref{}, true, failure("not_found", "source address has no current target")
	}
	return resolved, true, nil
}

func sourceOpTarget(op model.Op) model.Ref {
	switch op.Kind {
	case model.OpPatch:
		return op.Patch.Target
	case model.OpClose:
		return op.Close.Target
	case model.OpComment:
		return op.Comment.Target
	case model.OpLink, model.OpUnlink:
		return op.Link.From
	case model.OpDelete:
		return op.Delete.Target
	case model.OpSetMeta:
		return op.SetMeta.Target
	case model.OpReparent:
		return op.Reparent.Target
	}
	return model.Ref{}
}

// Copy the selected union arm before rewriting it. The original ChangeSet
// remains the request hash and receipt replay identity across every retry.
func sourceOpWithTarget(op model.Op, target model.Ref) model.Op {
	switch op.Kind {
	case model.OpPatch:
		v := *op.Patch
		v.Target = target
		op.Patch = &v
	case model.OpClose:
		v := *op.Close
		v.Target = target
		op.Close = &v
	case model.OpComment:
		v := *op.Comment
		v.Target = target
		op.Comment = &v
	case model.OpDelete:
		v := *op.Delete
		v.Target = target
		op.Delete = &v
	case model.OpSetMeta:
		v := *op.SetMeta
		v.Target = target
		op.SetMeta = &v
	case model.OpReparent:
		v := *op.Reparent
		v.Target = target
		op.Reparent = &v
	case model.OpLink, model.OpUnlink:
		v := *op.Link
		v.From = target
		op.Link = &v
	}
	return op
}

func (s *Service) sourcePreparation(source *sourceWorkflow, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) pipeline.SourcePreparation {
	cs.Expect = append([]model.Precondition(nil), cs.Expect...)
	return func(ctx context.Context, reader driver.Reader, prepare func(pipeline.SourceOperationHandler) error) (pipeline.SourcePublication, error) {
		var namespace driver.CheckpointExpectation
		index := 0
		prepared, err := source.provider.PrepareSourceOperations(ctx, func(staged *workflowplugin.Provider) error {
			var err error
			_, namespace, err = staged.OrdinaryBlockerSnapshot(ctx, by, nil, meta)
			if err != nil {
				return err
			}
			resolve := func(ref model.Ref) (model.Ref, error) {
				if !ref.IsID() {
					return ref, nil
				}
				if !ref.Branch.IsZero() {
					return model.Ref{}, failure("invalid_params", "source operations require branch zero")
				}
				if source.addresses[sourceAddress{meta.Project, ref.ID}] {
					mapped, found, err := staged.ResolveSourceRef(ctx, by, meta.Project, ref.ID, meta)
					if err != nil {
						return model.Ref{}, err
					}
					if !found {
						return model.Ref{}, failure("not_found", "source address has no current target")
					}
					return mapped, nil
				}
				if workflowRef(ref) && source.active[ref.ID] != meta.Project {
					return model.Ref{}, failure("not_found", "workflow item was not imported into this project")
				}
				return ref, nil
			}
			for _, expect := range cs.Expect {
				ref, err := resolve(expect.Ref)
				if err != nil {
					return err
				}
				if workflowRef(ref) || ref.ID != expect.Ref.ID {
					return failure("unsupported", "source address expectations require an explicit ordinary target")
				}
			}
			return prepare(func(ctx context.Context, original model.Op, image pipeline.SourceCoreImage) (model.Op, *pipeline.SourceOperation, error) {
				opIndex := index
				index++
				target, err := resolve(sourceOpTarget(original))
				if err != nil {
					return model.Op{}, nil, err
				}
				op := sourceOpWithTarget(original, target)
				docs, err := image(ctx)
				if err != nil {
					return model.Op{}, nil, err
				}
				graph := make(map[string]storedBead, len(docs))
				for id, doc := range docs {
					graph[id] = storedBead{doc: doc}
				}
				resolver := &workflowCoreResolver{service: s, principal: by, graph: graph, head: driver.CoreSnapshotExpectation{StoreID: namespace.StoreID, Incarnation: reader.Snapshot().Incarnation, Head: *reader.Snapshot().At}}
				if !workflowRef(target) {
					if original.Kind == model.OpLink || original.Kind == model.OpUnlink {
						kind, err := original.Link.TargetKind.Normalized()
						if err != nil {
							return model.Op{}, nil, failure("invalid_params", err.Error())
						}
						if kind != model.LinkTargetExternal {
							to, err := resolve(original.Link.To)
							if err != nil {
								return model.Op{}, nil, err
							}
							if workflowRef(to) {
								return model.Op{}, nil, failure("unsupported", "mixed source relation edits require provider preparation")
							}
							op.Link.To = to
						}
					}
					if original.Kind == model.OpClose || isSourceClaim(meta.Method) || meta.Method == "bead.close" {
						if err := s.sourceOrdinaryGuard(ctx, staged, by, target, meta, resolver); err != nil {
							return model.Op{}, nil, err
						}
					}
					return op, nil, nil
				}
				if meta.DryRun || len(cs.Expect) != 0 || original.Kind == model.OpClose && len(original.Close.Expect) != 0 {
					return model.Op{}, nil, failure("unsupported", "active source operations do not support dryRun or expect")
				}
				before, err := staged.GetWithResolver(ctx, by, target, meta, resolver)
				if err != nil {
					return model.Op{}, nil, err
				}
				operationMeta := meta
				digest := sha256.Sum256([]byte(meta.Method + "\x00" + meta.IdempotencyKey + "\x00" + strconv.Itoa(opIndex)))
				operationMeta.IdempotencyKey = hex.EncodeToString(digest[:])
				var commit model.Commit
				switch op.Kind {
				case model.OpPatch:
					if isSourceClaim(meta.Method) {
						operationMeta.Method = "bead.claim"
						item, err := staged.ClaimWithResolver(ctx, by, target, operationMeta, resolver)
						if err != nil {
							return model.Op{}, nil, err
						}
						commit = workflowClaimCommit(item)
					} else if meta.Method == "bead.close" {
						commit, err = staged.CloseWithResolver(ctx, by, target, "", operationMeta, resolver)
					} else {
						refs, e := workflowUpdateRefs(op.Patch.Fields)
						if e != nil {
							return model.Op{}, nil, e
						}
						for _, ref := range refs {
							if !workflowRef(ref) && (graph[ref.ID].doc.ID == "" || graph[ref.ID].doc.Project != meta.Project) {
								return model.Op{}, nil, failure("not_found", "ordinary workflow endpoint was not found in the staged project")
							}
						}
						operationMeta.Method = "bead.update"
						commit, err = staged.Update(ctx, by, target, op.Patch.Fields, operationMeta)
					}
				case model.OpComment:
					operationMeta.Method = "bead.comment"
					commit, err = staged.Comment(ctx, by, target, op.Comment.Body, operationMeta)
				case model.OpClose:
					operationMeta.Method = "bead.close"
					commit, err = staged.CloseWithResolver(ctx, by, target, op.Close.Reason, operationMeta, resolver)
				default:
					return model.Op{}, nil, failure("unsupported", "unsupported active source operation")
				}
				if err != nil {
					return model.Op{}, nil, err
				}
				if len(commit.Results) != 1 {
					return model.Op{}, nil, failure("internal_error", "active operation returned an invalid result")
				}
				active := &pipeline.SourceOperation{Result: commit.Results[0], LeaseExpiresAt: commit.LeaseExpiresAt, LeaseTtlMs: commit.LeaseTtlMs}
				active.Change = port.Change{Resource: model.ResourceRef{Kind: "workflow_item", Ref: target}, Op: op}
				active.Change.Before, err = json.Marshal(before)
				if err != nil {
					return model.Op{}, nil, err
				}
				after, err := staged.GetWithResolver(ctx, by, target, meta, resolver)
				if err == nil {
					active.Change.After, err = json.Marshal(after)
					active.Result.Record = active.Change.After
				} else {
					var typed *model.Error
					if !errors.As(err, &typed) || typed.Data.Code != "not_found" || !(op.Kind == model.OpClose || meta.Method == "bead.close") {
						return model.Op{}, nil, err
					}
					err = nil // completion removes the operational body; receipt retains the result
				}
				return op, active, err
			})
		})
		if errors.Is(err, driver.ErrCheckpointConflict) {
			return pipeline.SourcePublication{}, driver.ErrConflict
		}
		if err != nil {
			return pipeline.SourcePublication{}, err
		}
		head, ok := prepared.CoreSnapshot()
		if !ok {
			return pipeline.SourcePublication{}, driver.ErrUnsupported
		}
		publication := pipeline.SourcePublication{Head: head, Namespace: namespace}
		if !prepared.Empty() {
			mutation, err := prepared.Checkpoint(ctx)
			if err != nil {
				return pipeline.SourcePublication{}, err
			}
			publication.Mutation = &mutation
		}
		return publication, nil
	}
}

func isSourceClaim(method string) bool { return method == "bead.claim" || method == "bead.claimNext" }

func (s *Service) sourceOrdinaryGuard(ctx context.Context, staged *workflowplugin.Provider, by model.PrincipalRef, target model.Ref, meta model.WriteMeta, resolver *workflowCoreResolver) error {
	blocked, _, err := staged.OrdinaryBlockerSnapshot(ctx, by, []string{target.ID}, meta)
	if err != nil {
		return err
	}
	if blocked[target.ID] {
		return failure("ontology_guard_failed", "an active source dependency is not satisfied")
	}
	if isSourceClaim(meta.Method) {
		facts, _, err := staged.OrdinaryParentSnapshot(ctx, by, []string{target.ID}, meta, resolver)
		if err != nil {
			return err
		}
		if facts[target.ID].Blocked {
			return failure("ontology_guard_failed", "an active source parent is not ready")
		}
	}
	return nil
}

// IsParentBlocked follows the pinned ordinary graph while using the supplied
// lookup for typed active ancestors. The provider owns mixed cycle detection.
func (r *workflowCoreResolver) IsParentBlocked(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta, target model.Ref, lookup func(string) (workflowplugin.OrdinaryParentFact, error)) (bool, error) {
	if r.graph == nil {
		if _, err := r.IsBlocked(ctx, by, meta, target, "blocks", nil); err != nil {
			return true, err
		}
	}
	seen := map[string]bool{}
	for id := target.ID; id != ""; {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		if seen[id] {
			return true, nil
		}
		seen[id] = true
		row, ok := r.graph[id]
		if !ok || row.doc.Project != meta.Project {
			return true, nil
		}
		category, known := r.service.ontology.effective(row.doc.Project).Category(state(row.doc).Status)
		if !known || category == "backlog" {
			return true, nil
		}
		ready, err := r.service.ontology.noOpenBlockers(row.doc, r.graph)
		if err != nil || !ready {
			return true, err
		}
		fact, err := lookup(id)
		if err != nil || fact.Blocked {
			return true, err
		}
		if fact.Parent != nil {
			return false, nil
		} // same-image lookup already checked its active ancestry
		parent, err := pipeline.HierarchyParent(row.doc)
		if err != nil {
			return true, err
		}
		if parent == nil {
			return false, nil
		}
		id = parent.ID
	}
	return false, nil
}

var _ workflowplugin.ParentBlockerResolver = (*workflowCoreResolver)(nil)

func (s *Service) knownSourceWorkflowRef(project string, ref model.Ref) bool {
	source := s.source.Load()
	return source != nil && ref.Validate() == nil && ref.IsID() && ref.Branch.IsZero() && source.active[ref.ID] == project
}
