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
	provider     *workflowplugin.Provider
	binding      driver.SourcePlaneProviderCommitV1
	commitRecord []byte
	originDigest string
	digest       string
	profile      driver.RestoreImportProfile
	restoreTime  time.Time
	active       map[string]string // exact target -> project, retained after completion
	addresses    map[sourceAddress]bool
	allocatorIDs []string // global identity occupancy, retained after completion/deletion
}
type sourceAddress struct{ project, id string }

// InstallSourceWorkflow is a trusted host seam, never an RPC. The host first
// recovers the original joint receipt and immutable namespace origin. The final
// receipt binds verified import attribution separately from raw source authors.
// Installing preserves the method closures already bound by transports.
func (s *Service) InstallSourceWorkflow(ctx context.Context, provider *workflowplugin.Provider, profile driver.RestoreImportProfile, commit driver.SourcePlaneCommitBindingV1, receipt driver.IdempotencyRecord) error {
	return s.InstallVerifiedSourceWorkflow(ctx, provider, profile, commit, receipt, "")
}

// InstallVerifiedSourceWorkflow retains the immutable digest already verified
// by the host against the admitted archive. An empty digest requires full
// recovery before reusing the binding under another lease.
func (s *Service) InstallVerifiedSourceWorkflow(ctx context.Context, provider *workflowplugin.Provider, profile driver.RestoreImportProfile, commit driver.SourcePlaneCommitBindingV1, receipt driver.IdempotencyRecord, originDigest string) error {
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
	floor := model.Natural64(1)
	if bound.HistoryMode == driver.RestoreHistoryModeFull ||
		bound.HistoryMode == driver.RestoreHistoryModePartial && bound.NativeDeltaHistory != nil && bound.NativeDeltaHistory.FirstSequence == 1 {
		floor = model.Natural64(0)
	}
	initial := driver.Meta{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Head: receipt.Commit.Revision, HistoryFloor: floor}
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
	installed := &sourceWorkflow{
		provider: provider, binding: commit.Providers[0], commitRecord: evidence, originDigest: originDigest,
		digest: commit.RequestHash, profile: bound, restoreTime: change.Time,
		active: map[string]string{}, addresses: map[sourceAddress]bool{},
	}
	for _, entity := range plan.Entities {
		installed.addresses[sourceAddress{entity.Source.Project, entity.Source.ID}] = true
		installed.allocatorIDs = append(installed.allocatorIDs, entity.Source.ID, entity.TargetID)
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

// RebindSourceWorkflow reuses verified routing on the same open store.
// The protected marker must still match, and the provider is constructed
// against the replacement lease and current checkpoint.
func (s *Service) RebindSourceWorkflow(ctx context.Context, previous *Service, buildProvider func(context.Context, driver.SourcePlaneProviderCommitV1, string) (*workflowplugin.Provider, error)) (bool, error) {
	bound := previous.source.Load()
	if bound == nil || bound.originDigest == "" {
		return false, nil
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return false, err
	}
	origin := bound.binding.Origin
	if meta.StoreID != origin.StoreID || meta.Incarnation != origin.Incarnation {
		return false, nil
	}
	reader, err := s.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return false, err
	}
	rows, readErr := driver.GetNativeHeadRecords(driver.WithRetainedSourceRead(ctx), reader, []model.Ref{model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0))})
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return false, err
	}
	if len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, bound.commitRecord) {
		return false, driver.ErrRestoreBindingMismatch{Field: "source.commitRecord"}
	}
	provider, err := buildProvider(ctx, bound.binding, bound.originDigest)
	if err != nil {
		return false, err
	}
	rebound := *bound
	rebound.provider = provider
	if !s.source.CompareAndSwap(nil, &rebound) {
		return false, driver.ErrConflict
	}
	return true, nil
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

// SourceAddressProject reports whether a source-facing address was admitted
// for the requested project. It exposes only the immutable address binding so
// the guarded RPC can authorize the alias before resolving its current target.
func (s *Service) SourceAddressProject(project string, ref model.Ref) (string, bool) {
	source := s.source.Load()
	if source == nil || project == "" || !ref.IsID() || !ref.Branch.IsZero() {
		return "", false
	}
	if !source.addresses[sourceAddress{project, ref.ID}] {
		return "", false
	}
	return project, true
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

// Source edits need the general source participant, which stages each operation
// in request order. The joined delete participant handles ordinary operations
// and explicit incident unlinks; it must not replace a source edit with a core
// operation. Use immutable import bindings so absent aliases cannot fall through.
func sourceDeleteBatchHasSourceEdits(source *sourceWorkflow, cs model.ChangeSet, project string) bool {
	if source == nil {
		return false
	}
	for _, op := range cs.Ops {
		if err := op.Validate(); err != nil {
			return true // The canonical writer reports malformed operations.
		}
		if op.Kind == model.OpDelete || op.Kind == model.OpUnlink {
			continue
		}
		target := sourceOpTarget(op)
		if workflowRef(target) || source.addresses[sourceAddress{project, target.ID}] {
			return true
		}
		if op.Kind == model.OpLink && op.Link != nil && (workflowRef(op.Link.To) || source.addresses[sourceAddress{project, op.Link.To.ID}]) {
			return true
		}
	}
	return false
}

func (s *Service) sourcePreparation(source *sourceWorkflow, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) pipeline.SourcePreparation {
	cs.Expect = append([]model.Precondition(nil), cs.Expect...)
	return func(ctx context.Context, reader driver.Reader, prepare func(pipeline.SourceOperationHandler) error) (pipeline.SourcePublication, error) {
		var namespace driver.CheckpointExpectation
		index := 0
		provider := s.workflow
		if source != nil {
			provider = source.provider
		}
		prepareOperations := provider.PrepareOperations
		if source != nil {
			prepareOperations = provider.PrepareSourceOperations
		}
		prepared, err := prepareOperations(ctx, func(staged *workflowplugin.Provider) error {
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
				if source != nil && source.addresses[sourceAddress{meta.Project, ref.ID}] {
					mapped, found, err := staged.ResolveSourceRef(ctx, by, meta.Project, ref.ID, meta)
					if err != nil {
						return model.Ref{}, err
					}
					if !found {
						return model.Ref{}, failure("not_found", "source address has no current target")
					}
					return mapped, nil
				}
				if source != nil && workflowRef(ref) && source.active[ref.ID] != meta.Project {
					return model.Ref{}, failure("not_found", "workflow item was not imported into this project")
				}
				if source == nil && workflowRef(ref) {
					if err := validateWorkflowRef(ref); err != nil {
						return model.Ref{}, err
					}
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
			closeTargets := []string{}
			err = prepare(func(ctx context.Context, original model.Op, image pipeline.SourceCoreImage) (model.Op, *pipeline.SourceOperation, error) {
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
				if op.Kind == model.OpLink || op.Kind == model.OpUnlink {
					linked, active, err := s.prepareWorkflowRelation(ctx, staged, by, op, opIndex, cs, meta, graph, resolver, resolve)
					if err != nil || active != nil {
						return linked, active, err
					}
					op = linked
				}
				if !workflowRef(target) {
					if op.Kind == model.OpDelete {
						// Preserve ordinary deletes on the installed source path, but
						// reject incident active references against this staged image.
						// Replacing the participant with the native provider would lose
						// both the source address mapping and its snapshot fence.
						_, err := staged.PrepareOrdinaryDelete(ctx, by, model.ChangeSet{Ops: []model.Op{op}}, meta, func(check func(model.Op) (*workflowplugin.OrdinaryDeleteImage, error)) error {
							_, err := check(op)
							return err
						})
						if err != nil {
							return model.Op{}, nil, err
						}
					}
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
					if isSourceClaim(meta.Method) {
						if err := s.sourceOrdinaryGuard(ctx, staged, by, target, meta, resolver); err != nil {
							return model.Op{}, nil, err
						}
					} else if original.Kind == model.OpClose || meta.Method == "bead.close" {
						if err := s.sourceOrdinaryCloseGuard(ctx, staged, by, graph[target.ID].doc, op, meta); err != nil {
							return model.Op{}, nil, err
						}
						closeTargets = append(closeTargets, target.ID)
					}
					return op, nil, nil
				}
				if meta.TargetStatus != nil || original.Kind == model.OpClose && original.Close.TargetStatus != nil {
					return model.Op{}, nil, failure("unsupported", "active source provider does not support targetStatus")
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
				case model.OpSetMeta:
					metadata, changed, err := workflowMetadata(before.Metadata, *op.SetMeta)
					if err != nil {
						return model.Op{}, nil, err
					}
					if changed {
						operationMeta.Method = "bead.update"
						commit, err = staged.Update(ctx, by, target, map[string]any{"metadata": metadata}, operationMeta)
					} else {
						commit = model.Commit{Revision: before.Revision, Results: []model.OpResult{{Ref: target, Revision: before.Revision}}}
					}
					if err != nil {
						return model.Op{}, nil, err
					}
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
			if err != nil || len(closeTargets) == 0 {
				return err
			}
			// Final ontology validation consumes the final staged child image;
			// each original close prefix was checked above before advancing it.
			_, children, _, err := staged.OrdinaryCloseSnapshot(ctx, by, closeTargets, meta)
			if err == nil {
				setWorkflowCloseFacts(ctx, children)
			}
			return err
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

var _ workflowplugin.ParentBlockerResolver = (*workflowCoreResolver)(nil)

func (s *Service) knownSourceWorkflowRef(project string, ref model.Ref) bool {
	source := s.source.Load()
	return source != nil && ref.Validate() == nil && ref.IsID() && ref.Branch.IsZero() && source.active[ref.ID] == project
}
