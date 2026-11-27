package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type Service struct {
	store                  driver.Store
	lease                  driver.ActiveLease
	writer                 model.Writer
	ontology               *Ontology
	now                    func() time.Time
	memoryReadAuthorizer   MemoryReadAuthorizer
	authorizationInstalled func() bool
	events                 EventProvider
	workflow               *workflowplugin.Provider
	rollbackWindow         bool
	reverseConverter       *reverseconverter.Converter
	source                 atomic.Pointer[sourceWorkflow]
	compaction             CompactionConfig
}

// claimNextConflictRestartBudget bounds only the extra full-walk restart used
// after the terminal ready probe observes a candidate that has repeatedly
// exhausted the Writer's own snapshot attempts. Candidate enumeration itself
// remains uncapped; the bound prevents one unchanged candidate from spinning a
// request forever while preserving one restart for a newly ready candidate.
const claimNextConflictRestartBudget = 1

type Option func(*Service)

// WithOntology shares the same project ontology resolver used by the pipeline.
func WithOntology(o *Ontology) Option       { return func(s *Service) { s.ontology = o } }
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// WithMemoryReadAuthorizer installs the host's current-read policy for core
// memories. Denied memories are omitted from lists/briefs and appear as
// not_found on point reads.
func WithMemoryReadAuthorizer(authorizer MemoryReadAuthorizer) Option {
	return func(s *Service) { s.memoryReadAuthorizer = authorizer }
}

// WithAuthorizationInstalled supplies the host's live authorization
// installation boundary. Scoped principals fail closed whenever it reports
// false, even if a stale memory policy remains configured.
func WithAuthorizationInstalled(installed func() bool) Option {
	return func(s *Service) { s.authorizationInstalled = installed }
}

func WithEventProvider(provider EventProvider) Option {
	return func(s *Service) { s.events = provider }
}

// WithWorkflow installs the compiled-in workflow plugin's Option C provider.
// The service keeps the standard bead tools as the public adapter while the
// provider owns only its unversioned active-run state.
func WithWorkflow(provider *workflowplugin.Provider) Option {
	return func(s *Service) {
		s.workflow = provider
		if provider != nil {
			// The provider owns the native checkpoint mutation. Install the same
			// classifier used by the common Writer at that boundary so every
			// provider-created user-content kind is checked immediately before
			// publication. The closure reads the service's live switch/mapping so
			// host-owned tests and reload seams retain ordinary toggle semantics.
			provider.WithUserContentGuard(func(kinds []string) error {
				if !s.rollbackWindow {
					return nil
				}
				for _, kind := range kinds {
					if err := s.rollbackWindowKind(kind); err != nil {
						return err
					}
				}
				return nil
			})
		}
	}
}

// WithRollbackWindow enables the server-wide admission guard for newly
// created user-content kinds that the canonical reverse path cannot represent.
// It is disabled by default and is intended for the prepared switch-day
// rollback window only.
func WithRollbackWindow(enabled bool) Option {
	return func(s *Service) { s.rollbackWindow = enabled }
}

// WithReverseConverter shares the actual reverse converter with the native
// creation guard. A kind is admitted only through an executable conversion.
func WithReverseConverter(converter *reverseconverter.Converter) Option {
	return func(s *Service) { s.reverseConverter = converter }
}

// New never opens a transaction or storage driver. The composition root owns
// the store, active lease and the single Writer, and closes their lifetime.
func New(store driver.Store, lease driver.ActiveLease, writer model.Writer, options ...Option) *Service {
	s := &Service{store: store, lease: lease, writer: writer, ontology: NewOntology(nil), now: time.Now}
	for _, option := range options {
		option(s)
	}
	return s
}

func failure(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint, Retryable: code == "busy" || code == "write_conflict"}}
}

// ClaimNextSelectionSeed derives the stable internal ordering seed used by
// claimNext's ready walk. The selector identity keeps equivalent requests
// aligned, while the idempotency key spreads concurrent requests. Hashing
// keeps filter values out of the internal pager state and any diagnostics.
func ClaimNextSelectionSeed(identity, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(identity + "\x00" + idempotencyKey))
	return hex.EncodeToString(digest[:])
}

func mapped(err error) error {
	if err == nil {
		return nil
	}
	if unknown, ok := driver.Indeterminate(err); ok {
		return unknown
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Details: details, Retryable: code == "busy" || code == "write_conflict"}}
	}
	return err
}

func (s *Service) admit(ctx context.Context, by model.PrincipalRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.store == nil || s.lease == nil || s.writer == nil || s.ontology == nil || s.now == nil {
		return failure("internal_error", "service is not configured")
	}
	if _, err := driver.PrincipalKey(by); err != nil {
		return failure("invalid_params", "invalid principal")
	}
	select {
	case <-s.lease.Lost():
		return failure("busy", "active storage lease was lost")
	default:
		return nil
	}
}

func (s *Service) rollbackWindowKind(kind string) error {
	if !s.rollbackWindow {
		return nil
	}
	classification := s.reverseConverter.ClassifyKind(kind)
	if classification.Disposition == reverseconverter.KindUnrepresentable {
		return reverseconverter.RollbackWindowRefusal(kind)
	}
	return nil
}

func namedRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
		return failure("invalid_params", "named verbs require a nonempty bead ID on the main branch")
	}
	if strings.HasPrefix(ref.ID, "workflow:") {
		return failure("invalid_params", "workflow references require a supported workflow item operation")
	}
	return nil
}

func (s *Service) apply(ctx context.Context, by model.PrincipalRef, method string, cs model.ChangeSet, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	if strings.TrimSpace(meta.Project) == "" {
		return port.WriteOutcome{}, failure("project_required", "supply a project")
	}
	meta.Method = method
	if method != "bead.close" && method != "bead.reopen" {
		meta.TargetStatus = nil
	}
	source := s.source.Load()
	deletes := ordinaryDeletes(cs)
	ordinaryMixed := source == nil && (method == "tx.apply" || method == "bead.link") && len(deletes) == 0 && s.workflow.CanPrepareOrdinaryOperations() && (ordinaryBatchUsesWorkflow(cs) || cycleLinkChanges(cs))
	if ordinaryMixed {
		ctx = pipeline.WithSourceOperations(ctx, s.sourcePreparation(nil, by, cs, meta), ordinaryWorkflowKind)
	}
	if source != nil {
		ctx = pipeline.WithAllocatorReservations(ctx, source.allocatorIDs)
		ctx = pipeline.WithSourceOperations(ctx, s.sourcePreparation(source, by, cs, meta), func(ref model.Ref) string {
			if source.active[ref.ID] == meta.Project && ref.IsID() && ref.Branch.IsZero() {
				return "workflow_item"
			}
			return ""
		})
	}
	jointDelete := method == "tx.apply" && s.workflowProvider() != nil && len(deletes) != 0 && !sourceDeleteBatchHasSourceEdits(source, cs, meta.Project)
	if s.workflowProvider() != nil && (method == "bead.claim" || method == "bead.claimNext" || method == "bead.close" || method == "tx.apply") {
		ctx = withWorkflowCloseFacts(ctx)
		ctx = pipeline.WithWorkflowGuard(ctx, s.workflowWriteGuard)
	}
	if jointDelete {
		writer, ok := s.writer.(interface {
			ApplyWithSourceOperations(context.Context, model.PrincipalRef, model.ChangeSet, model.WriteMeta, pipeline.SourcePreparation, func(model.Ref) string) (port.WriteOutcome, error)
		})
		if !ok {
			return port.WriteOutcome{}, failure("unsupported", "writer lacks atomic workflow delete capability")
		}
		var preview map[int]model.OpResult
		v, err := writer.ApplyWithSourceOperations(ctx, by, cs, meta, s.ordinaryDeletePreparation(by, cs, meta, &preview), func(ref model.Ref) string {
			if s.source.Load() != nil && workflowRef(ref) {
				return "workflow_item"
			}
			return ordinaryDeleteKind(ref)
		})
		if err == nil {
			if meta.DryRun {
				for position, result := range preview {
					v.Commit.Results[position] = result
				}
			} else {
				v.Commit, err = s.workflowProvider().ProjectOrdinaryDeleteResults(ctx, by, cs, meta, v.Commit)
			}
		}
		if err != nil {
			return port.WriteOutcome{}, mapped(err)
		}
		return v, nil
	}
	if source == nil && method == "tx.apply" && !ordinaryMixed {
		for _, op := range cs.Ops {
			if op.Kind != model.OpPatch || op.Patch == nil || !workflowRef(op.Patch.Target) {
				continue
			}
			// This route has no workflow patch participant. Use the existing
			// scoped reader to distinguish an absent item from an unsupported
			// write to an item that exists, before any draft can be published.
			if _, err := s.Get(ctx, by, GetRequest{ID: op.Patch.Target, Project: meta.Project, Agent: meta.Agent}); err != nil {
				return port.WriteOutcome{}, mapped(err)
			}
			return port.WriteOutcome{}, failure("unsupported", "tx.apply cannot patch workflow items without source operation support")
		}
	}
	if writer, ok := s.writer.(port.OutcomeWriter); ok {
		v, err := writer.ApplyWithOutcome(ctx, by, cs, meta)
		return v, mapped(err)
	}
	if outcomeRequired {
		return port.WriteOutcome{}, failure("internal_error", "writer does not supply transient write outcomes")
	}
	// Legacy methods keep model.Writer compatibility. An outcome-aware caller
	// must use an actual OutcomeWriter; replay is never inferred from a commit.
	v, err := s.writer.Apply(ctx, by, cs, meta)
	return port.WriteOutcome{Commit: v}, mapped(err)
}

func (s *Service) applyMemory(ctx context.Context, by model.PrincipalRef, method string, cs model.ChangeSet, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	meta.Method = method
	if writer, ok := s.writer.(port.OutcomeWriter); ok {
		v, err := writer.ApplyWithOutcome(ctx, by, cs, meta)
		return v, mapped(err)
	}
	if outcomeRequired {
		return port.WriteOutcome{}, failure("internal_error", "writer does not supply transient write outcomes")
	}
	v, err := s.writer.Apply(ctx, by, cs, meta)
	return port.WriteOutcome{Commit: v}, mapped(err)
}

// ApplyWithOutcome is the optional direct transaction binding. Named service
// verbs continue to lower through the same method; the RPC layer binds this
// entry point only in compositions that explicitly install tx.apply.
func (s *Service) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.apply(ctx, by, "tx.apply", cs, meta, true)
}

func workflowMeta(by model.PrincipalRef, method, project, agent string) model.WriteMeta {
	meta := model.WriteMeta{Method: method, Project: project, Agent: agent}
	if by.Kind == "anonymous" {
		meta.AgentLabel = agent
	}
	return meta
}

func workflowRef(ref model.Ref) bool {
	return ref.IsID() && strings.HasPrefix(ref.ID, workflowplugin.ItemPrefix)
}

func workflowTypes(filter *Filter) []string {
	if filter == nil {
		return nil
	}
	return filter.Types
}

// WorkflowInvoke is the typed extension binding used by x.workflow.invoke.
// Permanent invocations lower through the ordinary bead writer; ephemeral
// invocations remain entirely in the workflow provider.
func (s *Service) WorkflowInvoke(ctx context.Context, by model.PrincipalRef, req workflowplugin.InvokeRequest, meta model.WriteMeta) (workflowplugin.InvokeResult, error) {
	if req.Mode == "ephemeral" {
		if err := workflowplugin.ValidateInvokeRequest(req); err != nil {
			return workflowplugin.InvokeResult{}, err
		}
		if meta.DryRun {
			return workflowplugin.InvokeResult{}, failure("invalid_params", "ephemeral workflow invocation does not support dryRun")
		}
		if s.workflowProvider() == nil {
			return workflowplugin.InvokeResult{}, failure("plugin_durable_state_unavailable", "ephemeral workflows require persistent checkpoint storage")
		}
		meta.Method = "x.workflow.invoke"
		meta.Project = req.Project
		for _, item := range req.Checklist {
			if err := s.validateWorkflowEndpoints(ctx, by, req.Project, workflowAuxiliaryRefs(item.Parent, item.Links)...); err != nil {
				return workflowplugin.InvokeResult{}, err
			}
		}
		return s.workflowProvider().Invoke(ctx, by, req, meta)
	}
	if req.Mode != "permanent" {
		return workflowplugin.InvokeResult{}, failure("invalid_params", "workflow invocation mode is required")
	}
	if err := workflowplugin.ValidateInvokeRequest(req); err != nil {
		return workflowplugin.InvokeResult{}, err
	}
	// One ordinary Writer proposal keeps the checklist atomic and validates the
	// original request key in its own method namespace. Local create aliases
	// bind the complete invocation to the Writer request hash without adding
	// workflow metadata to any durable bead body.
	request, err := json.Marshal(req)
	if err != nil {
		return workflowplugin.InvokeResult{}, failure("invalid_params", "invalid workflow invocation")
	}
	digest := sha256.Sum256(append([]byte("plug/workflow/permanent/1\x00"), request...))
	ops := make([]model.Op, len(req.Checklist))
	types := make([]string, len(req.Checklist))
	labels := make(map[string]bool, len(req.Checklist))
	for index, item := range req.Checklist {
		if strings.TrimSpace(item.Label) == "" || strings.TrimSpace(item.Title) == "" || labels[item.Label] {
			return workflowplugin.InvokeResult{}, failure("invalid_params", "checklist labels must be unique and labels and titles must be nonempty")
		}
		labels[item.Label] = true
		types[index] = item.Type
		if types[index] == "" {
			// The ordinary built-in type is issue. The configured ontology
			// still decides whether that type is active for this project.
			types[index] = "issue"
		}
		ops[index] = model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: fmt.Sprintf("workflow-%x-%d", digest, index), Type: types[index], Project: req.Project, Fields: permanentWorkflowFields(item), Parent: item.Parent}}
	}
	for index, item := range req.Checklist {
		target := model.RefByKey(fmt.Sprintf("workflow-%x-%d", digest, index))
		if item.Parent != nil {
			if err := namedRef(*item.Parent); err != nil {
				return workflowplugin.InvokeResult{}, err
			}
		}
		for _, link := range item.Links {
			if err := namedRef(link.To); err != nil {
				return workflowplugin.InvokeResult{}, err
			}
			ops = append(ops, model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: target, To: link.To, LinkKind: link.Kind, Metadata: link.Metadata}})
		}
		for _, name := range sortedMetadataKeys(item.Metadata) {
			raw, err := json.Marshal(item.Metadata[name])
			if err != nil {
				return workflowplugin.InvokeResult{}, failure("invalid_params", "invalid metadata")
			}
			ops = append(ops, model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: target, Key: name, Value: raw}})
		}
	}
	meta.Project = req.Project
	out, err := s.apply(ctx, by, "x.workflow.invoke", model.ChangeSet{Ops: ops}, meta, false)
	if err != nil {
		return workflowplugin.InvokeResult{}, err
	}
	if len(out.Commit.Results) < len(req.Checklist) {
		return workflowplugin.InvokeResult{}, failure("internal_error", "permanent workflow invocation did not return every item")
	}
	result := workflowplugin.InvokeResult{Plane: "permanent", Items: make([]workflowplugin.ItemView, len(req.Checklist))}
	for index, item := range req.Checklist {
		created := out.Commit.Results[index]
		result.Items[index] = workflowplugin.ItemView{ID: created.Ref.ID, Label: item.Label, Title: item.Title, Type: types[index], Project: req.Project, Status: "open", Plane: "permanent", Kind: "bead", Revision: created.Revision, Fields: permanentWorkflowFields(item), Labels: append([]string(nil), item.Labels...), Metadata: cloneMap(item.Metadata), Parent: item.Parent, Blocked: item.Blocked}
	}
	return result, nil
}

func (s *Service) Create(ctx context.Context, by model.PrincipalRef, req CreateRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.create(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.create(ctx, by, req, meta, true)
}

func (s *Service) create(ctx context.Context, by model.PrincipalRef, req CreateRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if req.Type == "" || req.Fields == nil {
		return port.WriteOutcome{}, failure("invalid_params", "type and fields are required")
	}
	if req.Parent != nil {
		if err := namedRef(*req.Parent); err != nil {
			return port.WriteOutcome{}, err
		}
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: req.Type, Fields: req.Fields, Parent: req.Parent, Project: meta.Project, TargetStatus: req.TargetStatus}}}}
	return s.apply(ctx, by, "bead.create", cs, meta, outcomeRequired)
}

func (s *Service) Update(ctx context.Context, by model.PrincipalRef, req UpdateRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.update(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) UpdateWithOutcome(ctx context.Context, by model.PrincipalRef, req UpdateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.update(ctx, by, req, meta, true)
}

func (s *Service) update(ctx context.Context, by model.PrincipalRef, req UpdateRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if s.SourceRuntimeInstalled() {
		if len(req.Fields) == 0 {
			return port.WriteOutcome{}, failure("invalid_params", "update requires fields")
		}
		return s.apply(ctx, by, "bead.update", model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.ID, Fields: req.Fields}}}, Expect: req.Expect}, meta, outcomeRequired)
	}
	if workflowRef(req.ID) {
		if err := validateWorkflowWrite(req.ID, req.Expect, meta); err != nil {
			return port.WriteOutcome{}, err
		}
		if s.workflowProvider() == nil {
			return port.WriteOutcome{}, failure("not_found", "workflow item was not found")
		}
		refs, err := workflowUpdateRefs(req.Fields)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		if err := s.validateWorkflowEndpoints(ctx, by, meta.Project, refs...); err != nil {
			return port.WriteOutcome{}, err
		}
		commit, err := s.workflowProvider().Update(ctx, by, req.ID, req.Fields, meta)
		return port.WriteOutcome{Commit: commit}, err
	}
	if err := namedRef(req.ID); err != nil {
		return port.WriteOutcome{}, err
	}
	if len(req.Fields) == 0 {
		return port.WriteOutcome{}, failure("invalid_params", "update requires fields")
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.ID, Fields: req.Fields}}}, Expect: req.Expect}
	return s.apply(ctx, by, "bead.update", cs, meta, outcomeRequired)
}

func (s *Service) Claim(ctx context.Context, by model.PrincipalRef, req ClaimRequest, meta model.WriteMeta) (ClaimResult, error) {
	v, err := s.claim(ctx, by, req, meta, false)
	if err != nil {
		return ClaimResult{}, err
	}
	return claimResult(v.Commit)
}

func (s *Service) ClaimWithOutcome(ctx context.Context, by model.PrincipalRef, req ClaimRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if s.SourceRuntimeInstalled() {
		return s.claimWithMethod(ctx, by, req, meta, true, meta.Method)
	}
	if workflowRef(req.ID) {
		if err := validateWorkflowWrite(req.ID, req.Expect, meta); err != nil {
			return port.WriteOutcome{}, err
		}
		if s.workflowProvider() == nil {
			return port.WriteOutcome{}, failure("not_found", "workflow item was not found")
		}
		if meta.Method == "bead.claimNext" {
			// claimNext candidates are selected and authorized by the queue walk.
			// Preserve that selector when committing the provider receipt; the
			// provider intentionally refuses the legacy bare bead.claim shape.
			item, found, replayed, err := s.workflowProvider().ClaimNextSelectedWithResolver(ctx, by, req.ID, meta.Project, nil, meta, s.workflowBlockerResolver(ctx, by, meta))
			if err != nil {
				return port.WriteOutcome{}, err
			}
			if !found {
				return port.WriteOutcome{Replayed: replayed, NullResult: true}, nil
			}
			return port.WriteOutcome{Commit: workflowClaimCommit(item), Replayed: replayed}, nil
		}
		item, err := s.workflowProvider().ClaimWithResolver(ctx, by, req.ID, meta, s.workflowBlockerResolver(ctx, by, meta))
		if err != nil {
			return port.WriteOutcome{}, err
		}
		return port.WriteOutcome{Commit: workflowClaimCommit(item)}, nil
	}
	method := ""
	if meta.Method == "bead.claimNext" {
		method = meta.Method
	}
	v, err := s.claimWithMethod(ctx, by, req, meta, true, method)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	_, err = claimResult(v.Commit)
	return v, err
}

// ReplayWorkflowClaimNext checks only the provider-owned durable receipt for
// this exact selector. It is an optional adapter seam used by the guarded RPC
// service before it walks the live ready set; a miss must never consume a new
// candidate. The bool reports whether the provider had a retained receipt.
func (s *Service) ReplayWorkflowClaimNext(ctx context.Context, by model.PrincipalRef, req ClaimNextRequest, meta model.WriteMeta) (*port.WriteOutcome, bool, error) {
	if s.SourceRuntimeInstalled() {
		return nil, false, nil
	}
	if s.workflowProvider() == nil {
		return nil, false, nil
	}
	if req.Filter != nil && req.Filter.IncludeEphemeral != nil && !*req.Filter.IncludeEphemeral {
		return nil, false, nil
	}
	if meta.DryRun {
		return nil, false, failure("invalid_params", "workflow claimNext does not support dryRun")
	}
	if strings.TrimSpace(req.Project) == "" {
		return nil, false, failure("project_required", "supply a project")
	}
	identity, err := ClaimNextRequestIdentity(req)
	if err != nil {
		return nil, false, failure("invalid_params", "invalid claimNext selector")
	}
	meta.ClaimNextIdentity = identity
	meta.Method = "bead.claimNext"
	meta.Project = req.Project
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	} else if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	item, found, replayed, err := s.workflowProvider().ReplayClaimNext(ctx, by, req.Project, nil, meta)
	if err != nil {
		return nil, false, err
	}
	if !replayed {
		return nil, false, nil
	}
	if !found {
		return &port.WriteOutcome{Replayed: true, NullResult: true}, true, nil
	}
	return &port.WriteOutcome{Commit: workflowClaimCommit(item), Replayed: true}, true, nil
}

func (s *Service) claim(ctx context.Context, by model.PrincipalRef, req ClaimRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if workflowRef(req.ID) {
		return s.ClaimWithOutcome(ctx, by, req, meta)
	}
	return s.claimWithMethod(ctx, by, req, meta, outcomeRequired, "")
}

// ClaimNextWithOutcome walks the live ready set and lowers the first candidate
// through the same claim pipeline as bead.claim. A candidate can disappear or
// lose its claim race while the queue is being walked; those ordinary races
// advance to the next candidate. NullResult retains the cataloged JSON-null
// outcome and its replay metadata when the ready set is empty at commit.
func (s *Service) ClaimNextWithOutcome(ctx context.Context, by model.PrincipalRef, req ClaimNextRequest, meta model.WriteMeta) (*port.WriteOutcome, error) {
	if err := s.admit(ctx, by); err != nil {
		return nil, err
	}
	if s.workflowProvider() != nil && meta.DryRun {
		return nil, failure("invalid_params", "workflow claimNext does not support dryRun")
	}
	if strings.TrimSpace(req.Project) == "" {
		return nil, failure("project_required", "supply a project")
	}
	identity, err := ClaimNextRequestIdentity(req)
	if err != nil {
		return nil, failure("invalid_params", "invalid claimNext selector")
	}
	meta.ClaimNextIdentity = identity
	meta.Method = "bead.claimNext"
	meta.Project = req.Project
	selectionSeed := ClaimNextSelectionSeed(identity, meta.IdempotencyKey)
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	} else if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}

	// A successful claim has a durable receipt under the claimNext method. On a
	// retry after the bead is no longer ready, recover its target and re-enter
	// the ordinary pipeline so replay authorization remains current.
	if meta.IdempotencyKey != "" {
		principal, err := driver.PrincipalKey(by)
		if err != nil {
			return nil, failure("invalid_params", "invalid principal")
		}
		reader, err := s.reader(ctx, by, nil)
		if err != nil {
			return nil, err
		}
		entry, lookupErr := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, "bead.claimNext", meta.IdempotencyKey)
		_ = reader.Close()
		if lookupErr != nil {
			return nil, mapped(lookupErr)
		}
		if entry != nil && len(entry.Commit.Results) > 0 {
			id := entry.Commit.Results[0].Ref
			out, claimErr := s.claimWithMethod(ctx, by, ClaimRequest{ID: id}, meta, true, "bead.claimNext")
			if claimErr != nil {
				return nil, claimErr
			}
			return &out, nil
		}
		if entry != nil {
			// A stored null is terminal for this exact selector. Replay must not
			// probe today's ready set and accidentally claim a later candidate.
			// Re-enter only the core Writer that owns this retained receipt;
			// the provider fallback may now contain a different live candidate.
			out, err := s.commitCoreClaimNextEmpty(ctx, by, meta, nil)
			if err != nil {
				return nil, err
			}
			out.NullResult = true
			return &out, nil
		}
	}
	if out, replayed, replayErr := s.ReplayWorkflowClaimNext(ctx, by, req, meta); replayErr != nil {
		return nil, replayErr
	} else if replayed {
		return out, nil
	}

	position := (*PagePosition)(nil)
	fresh := false
	exhausted := map[string]error{}
	var exhaustedConflict error
	repeatedConflictThisPass := false
	conflictRestarts := 0
	for {
		page, err := s.ClaimNextReadyPage(ctx, by, ReadyRequest{Project: req.Project, Agent: meta.Agent, ViewID: req.ViewID, Filter: req.Filter, Limit: 1000, Position: position, ClaimNextSeed: selectionSeed, claimNext: true})
		if err != nil {
			return nil, err
		}
		for _, bead := range page.Items {
			candidateMeta := meta
			candidateMeta.Method = "bead.claimNext"
			var out port.WriteOutcome
			var claimErr error
			if workflowRef(model.RefByID(bead.ID, model.Natural64(0))) {
				out, claimErr = s.ClaimWithOutcome(ctx, by, ClaimRequest{ID: model.RefByID(bead.ID, model.Natural64(0))}, candidateMeta)
			} else {
				out, claimErr = s.claimWithMethod(ctx, by, ClaimRequest{ID: model.RefByID(bead.ID, model.Natural64(0))}, candidateMeta, true, "bead.claimNext")
			}
			if claimErr == nil {
				return &out, nil
			}
			if !claimNextRace(claimErr) {
				return nil, claimErr
			}
			if claimNextCandidateConflict(claimErr) {
				if _, seen := exhausted[bead.ID]; seen {
					repeatedConflictThisPass = true
				} else {
					exhausted[bead.ID] = claimErr
					if exhaustedConflict == nil {
						exhaustedConflict = claimErr
					}
				}
			}
		}
		if page.Next == nil {
			if !fresh {
				// The queue is live. Re-run the complete selection from a new
				// snapshot before recording a terminal null, so a bead that became
				// ready while the first walk was finishing is not missed.
				fresh = true
				position = nil
				repeatedConflictThisPass = false
				continue
			}
			out, err := s.CommitClaimNextEmpty(ctx, by, req, meta, s.claimNextReadyProbe(by, req))
			if err != nil {
				if errors.Is(err, port.ErrClaimNextReady) {
					if repeatedConflictThisPass {
						if conflictRestarts >= claimNextConflictRestartBudget {
							return nil, exhaustedConflict
						}
						conflictRestarts++
					}
					repeatedConflictThisPass = false
					fresh = false
					position = nil
					continue
				}
				return nil, err
			}
			if len(out.Commit.Results) == 0 {
				out.NullResult = true
			}
			return &out, nil
		}
		position = page.Next
	}
}

// claimNextReadyProbe observes the complete ready set on the transaction's
// post-head snapshot. It is deliberately separate from the live walk so the
// transaction can release and restart if a candidate appeared in between.
func (s *Service) claimNextReadyProbe(by model.PrincipalRef, req ClaimNextRequest) port.ClaimNextReadyProbe {
	return func(ctx context.Context, reader driver.Reader) (bool, error) {
		items, err := s.ReadyCandidates(ctx, by, ReadyRequest{Project: req.Project, ViewID: req.ViewID, Filter: req.Filter}, reader)
		return len(items) != 0, err
	}
}

// CommitClaimNextEmpty is the narrow guarded-service seam for a terminal
// claimNext selection. It records a durable H+1 receipt only after the
// supplied transaction-level probe observes no ready candidate. A nil probe
// is reserved for replay of an already durable empty receipt.
func (s *Service) CommitClaimNextEmpty(ctx context.Context, by model.PrincipalRef, req ClaimNextRequest, meta model.WriteMeta, probe port.ClaimNextReadyProbe) (port.WriteOutcome, error) {
	if s.SourceRuntimeInstalled() {
		return port.WriteOutcome{}, failure("unsupported", "source claimNext requires a joint terminal readiness proof")
	}
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	if strings.TrimSpace(req.Project) == "" {
		return port.WriteOutcome{}, failure("project_required", "supply a project")
	}
	identity, err := ClaimNextRequestIdentity(req)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", "invalid claimNext selector")
	}
	meta.ClaimNextIdentity = identity
	meta.Method = "bead.claimNext"
	meta.Project = req.Project
	includeEphemeral := true
	if req.Filter != nil && req.Filter.IncludeEphemeral != nil {
		includeEphemeral = *req.Filter.IncludeEphemeral
	}
	if s.workflowProvider() != nil {
		if meta.DryRun {
			return port.WriteOutcome{}, failure("invalid_params", "workflow claimNext does not support dryRun")
		}
		types := []string(nil)
		if req.Filter != nil {
			types = req.Filter.Types
		}
		resources := []string(nil)
		if req.Filter != nil {
			resources = req.Filter.Resources
		}
		if !includeEphemeral {
			return s.commitCoreClaimNextEmpty(ctx, by, meta, probe)
		}
		item, found, replayed, err := s.workflowProvider().ClaimNextWithResourcesWithResolver(ctx, by, req.Project, types, resources, meta, s.workflowBlockerResolver(ctx, by, meta))
		if err != nil {
			return port.WriteOutcome{}, err
		}
		if replayed {
			if !found {
				return port.WriteOutcome{Replayed: true, NullResult: true}, nil
			}
			return port.WriteOutcome{Commit: workflowClaimCommit(item), Replayed: true}, nil
		}
		if found {
			return port.WriteOutcome{Commit: workflowClaimCommit(item)}, nil
		}
	}
	return s.commitCoreClaimNextEmpty(ctx, by, meta, probe)
}

// commitCoreClaimNextEmpty never visits the provider queue. In particular, a
// retained core null must replay through its original Writer and authorization
// boundary even when a workflow provider was installed or gained items later.
func (s *Service) commitCoreClaimNextEmpty(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta, probe port.ClaimNextReadyProbe) (port.WriteOutcome, error) {
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	} else if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	writer, ok := s.writer.(interface {
		ApplyClaimNextEmpty(context.Context, model.PrincipalRef, model.ChangeSet, model.WriteMeta, port.ClaimNextReadyProbe) (port.WriteOutcome, error)
	})
	if !ok {
		return port.WriteOutcome{}, failure("internal_error", "writer does not support claimNext terminal probes")
	}
	out, err := writer.ApplyClaimNextEmpty(ctx, by, model.ChangeSet{Ops: []model.Op{}}, meta, probe)
	return out, mapped(err)
}

func claimNextRace(err error) bool {
	var value *model.Error
	if !errors.As(err, &value) {
		return false
	}
	switch value.Data.Code {
	case "forbidden", "not_found", "precondition_failed", "ontology_guard_failed":
		return true
	case "busy":
		// Only a candidate's live-lease race is skippable. Pre-phase timeout,
		// active-store lease loss and every other busy condition remain a wire
		// error rather than being converted into terminal null.
		return value.Message == "bead already has a live claim"
	case "write_conflict":
		// The shared writer has already exhausted its bounded snapshot retries
		// for this selected candidate. Advance within the same claimNext walk;
		// unrelated/global write failures retain their typed error and do not
		// become a terminal null.
		return claimNextCandidateConflict(value)
	default:
		return false
	}
}

func claimNextCandidateConflict(err error) bool {
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != "write_conflict" {
		return false
	}
	return value.Message == "snapshot changed during all four write attempts" || value.Data.Hint == "snapshot changed during all four write attempts"
}

func (s *Service) claimWithMethod(ctx context.Context, by model.PrincipalRef, req ClaimRequest, meta model.WriteMeta, outcomeRequired bool, method string) (port.WriteOutcome, error) {
	if err := namedRef(req.ID); err != nil && !s.SourceRuntimeInstalled() {
		return port.WriteOutcome{}, err
	}
	// Slot normalization belongs after the pipeline's idempotency lookup. This
	// stable request replays even after the current bead has changed status.
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.ID, Fields: map[string]any{}}}}, Expect: req.Expect}
	if method == "" {
		method = "bead.claim"
	}
	return s.apply(ctx, by, method, cs, meta, outcomeRequired)
}

func claimResult(v model.Commit) (ClaimResult, error) {
	if v.LeaseExpiresAt == nil || v.LeaseTtlMs == nil {
		return ClaimResult{}, failure("internal_error", "claim commit is missing its durable lease result")
	}
	ttl, err := durationMS(*v.LeaseTtlMs)
	if err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{Commit: v, LeaseExpiresAt: *v.LeaseExpiresAt, LeaseTTL: ttl}, nil
}

func durationMS(n model.Natural) (time.Duration, error) {
	ms, err := n.Int64()
	if err != nil || ms < 1 || ms > int64((1<<63-1)/time.Millisecond) {
		return 0, failure("internal_error", "invalid durable claim lease TTL")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func (s *Service) Comment(ctx context.Context, by model.PrincipalRef, req CommentRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.comment(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) CommentWithOutcome(ctx context.Context, by model.PrincipalRef, req CommentRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if s.SourceRuntimeInstalled() {
		return s.comment(ctx, by, req, meta, true)
	}
	if workflowRef(req.ID) {
		if err := validateWorkflowWrite(req.ID, req.Expect, meta); err != nil {
			return port.WriteOutcome{}, err
		}
		if s.workflowProvider() == nil {
			return port.WriteOutcome{}, failure("not_found", "workflow item was not found")
		}
		commit, err := s.workflowProvider().Comment(ctx, by, req.ID, req.Body, meta)
		return port.WriteOutcome{Commit: commit}, err
	}
	return s.comment(ctx, by, req, meta, true)
}

func (s *Service) comment(ctx context.Context, by model.PrincipalRef, req CommentRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := namedRef(req.ID); err != nil && !s.SourceRuntimeInstalled() {
		return port.WriteOutcome{}, err
	}
	if strings.TrimSpace(req.Body) == "" {
		return port.WriteOutcome{}, failure("invalid_params", "comment body must not be empty")
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpComment, Comment: &model.CommentOp{Target: req.ID, Body: req.Body}}}, Expect: req.Expect}
	return s.apply(ctx, by, "bead.comment", cs, meta, outcomeRequired)
}

func (s *Service) Close(ctx context.Context, by model.PrincipalRef, req CloseRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.close(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, req CloseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if workflowRef(req.ID) && req.TargetStatus != nil {
		return port.WriteOutcome{}, failure("unsupported", "workflow provider does not support targetStatus")
	}
	if s.SourceRuntimeInstalled() {
		return s.close(ctx, by, req, meta, true)
	}
	if workflowRef(req.ID) {
		if err := validateWorkflowWrite(req.ID, req.Expect, meta); err != nil {
			return port.WriteOutcome{}, err
		}
		if s.workflowProvider() == nil {
			return port.WriteOutcome{}, failure("not_found", "workflow item was not found")
		}
		commit, err := s.workflowProvider().CloseWithResolver(ctx, by, req.ID, req.Reason, meta, s.workflowBlockerResolver(ctx, by, meta))
		return port.WriteOutcome{Commit: commit}, err
	}
	return s.close(ctx, by, req, meta, true)
}

func workflowClaimCommit(item workflowplugin.ItemView) model.Commit {
	commit := model.Commit{Revision: model.Natural64(0), Results: []model.OpResult{{Ref: model.RefByID(item.ID, model.Natural64(0)), Revision: item.Revision}}, Warnings: []model.Warning{}}
	if item.Claim != nil {
		commit.LeaseExpiresAt = &item.Claim.ExpiresAt
		value := model.Natural64(uint64(workflowplugin.ClaimTTL / time.Millisecond))
		commit.LeaseTtlMs = &value
	}
	return commit
}

func (s *Service) close(ctx context.Context, by model.PrincipalRef, req CloseRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if s.SourceRuntimeInstalled() {
		return s.apply(ctx, by, "bead.close", model.ChangeSet{Ops: []model.Op{{Kind: model.OpClose, Close: &model.CloseOp{Target: req.ID, Reason: req.Reason, TargetStatus: req.TargetStatus}}}, Expect: req.Expect}, meta, outcomeRequired)
	}
	meta.TargetStatus = req.TargetStatus
	return s.lifecycle(ctx, by, "bead.close", req.ID, req.Reason, req.Expect, meta, outcomeRequired)
}

// Reopen moves an existing bead through the ontology's reopen slot. The
// transition, lease settlement, guard evaluation and history publication all
// remain in the shared Writer pipeline used by every named lifecycle verb.
func (s *Service) Reopen(ctx context.Context, by model.PrincipalRef, req ReopenRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.reopen(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) ReopenWithOutcome(ctx context.Context, by model.PrincipalRef, req ReopenRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.reopen(ctx, by, req, meta, true)
}

func (s *Service) reopen(ctx context.Context, by model.PrincipalRef, req ReopenRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	meta.TargetStatus = req.TargetStatus
	return s.lifecycle(ctx, by, "bead.reopen", req.ID, req.Reason, req.Expect, meta, outcomeRequired)
}

// Defer moves an existing bead through the ontology's defer slot.
func (s *Service) Defer(ctx context.Context, by model.PrincipalRef, req DeferRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.deferBead(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) DeferWithOutcome(ctx context.Context, by model.PrincipalRef, req DeferRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.deferBead(ctx, by, req, meta, true)
}

func (s *Service) deferBead(ctx context.Context, by model.PrincipalRef, req DeferRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	return s.lifecycle(ctx, by, "bead.defer", req.ID, req.Reason, req.Expect, meta, outcomeRequired)
}

// Cancel moves an existing bead through the ontology's cancel slot.
func (s *Service) Cancel(ctx context.Context, by model.PrincipalRef, req CancelRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.cancel(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) CancelWithOutcome(ctx context.Context, by model.PrincipalRef, req CancelRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.cancel(ctx, by, req, meta, true)
}

func (s *Service) cancel(ctx context.Context, by model.PrincipalRef, req CancelRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	return s.lifecycle(ctx, by, "bead.cancel", req.ID, req.Reason, req.Expect, meta, outcomeRequired)
}

// Fail moves an existing bead through the ontology's failure slot.
func (s *Service) Fail(ctx context.Context, by model.PrincipalRef, req FailRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.fail(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) FailWithOutcome(ctx context.Context, by model.PrincipalRef, req FailRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.fail(ctx, by, req, meta, true)
}

func (s *Service) fail(ctx context.Context, by model.PrincipalRef, req FailRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	return s.lifecycle(ctx, by, "bead.fail", req.ID, req.Reason, req.Expect, meta, outcomeRequired)
}

func (s *Service) lifecycle(ctx context.Context, by model.PrincipalRef, method string, id model.Ref, reason string, expect []model.Precondition, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := namedRef(id); err != nil {
		return port.WriteOutcome{}, err
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: id, Fields: map[string]any{}}}}, Expect: expect}
	if reason != "" {
		cs.Ops = append(cs.Ops, model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: id, Body: reason}})
	}
	return s.apply(ctx, by, method, cs, meta, outcomeRequired)
}

func (s *Service) Apply(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.apply(ctx, by, "tx.apply", cs, meta, false)
	return v.Commit, err
}

func linkChangeSet(req LinkRequest, kind model.OpKind) (model.ChangeSet, error) {
	if err := namedRef(req.From); err != nil {
		return model.ChangeSet{}, err
	}
	targetKind, err := req.TargetKind.Normalized()
	if err != nil {
		return model.ChangeSet{}, failure("invalid_params", err.Error())
	}
	if err := model.ValidateLinkTarget(targetKind, req.To); err != nil {
		return model.ChangeSet{}, failure("invalid_params", err.Error())
	}
	if targetKind != model.LinkTargetExternal {
		if err := namedRef(req.To); err != nil {
			return model.ChangeSet{}, err
		}
	}
	if req.Kind == "" {
		return model.ChangeSet{}, failure("invalid_params", "link kind must not be empty")
	}
	return model.ChangeSet{
		Ops:    []model.Op{{Kind: kind, Link: &model.LinkOp{Kind: kind, From: req.From, To: req.To, LinkKind: req.Kind, TargetKind: targetKind, Metadata: req.Metadata}}},
		Expect: req.Expect,
	}, nil
}

func workflowLinkRequest(req LinkRequest) (workflowplugin.LinkRequest, bool, error) {
	targetKind, err := req.TargetKind.Normalized()
	if err != nil {
		return workflowplugin.LinkRequest{}, false, failure("invalid_params", err.Error())
	}
	if err := model.ValidateLinkTarget(targetKind, req.To); err != nil {
		return workflowplugin.LinkRequest{}, false, failure("invalid_params", err.Error())
	}
	// The target domain is authoritative before any local workflow routing.
	// Opaque external IDs may spell a live workflow item without binding to it.
	if targetKind == model.LinkTargetExternal {
		if workflowRef(req.From) {
			if err := validateWorkflowRef(req.From); err != nil {
				return workflowplugin.LinkRequest{}, false, err
			}
			return workflowplugin.LinkRequest{}, false, failure("unsupported", "workflow external targets require a typed workflow storage contract")
		}
		return workflowplugin.LinkRequest{}, false, nil
	}
	fromWorkflow := workflowRef(req.From)
	toWorkflow := workflowRef(req.To)
	if !fromWorkflow && !toWorkflow {
		return workflowplugin.LinkRequest{}, false, nil
	}
	if err := validateRefForWorkflowEndpoint(req.From, fromWorkflow); err != nil {
		return workflowplugin.LinkRequest{}, false, err
	}
	if err := validateRefForWorkflowEndpoint(req.To, toWorkflow); err != nil {
		return workflowplugin.LinkRequest{}, false, err
	}
	return workflowplugin.LinkRequest{From: req.From, To: req.To, Kind: req.Kind, Metadata: req.Metadata}, true, nil
}

func validateRefForWorkflowEndpoint(ref model.Ref, workflow bool) error {
	if workflow {
		return validateWorkflowRef(ref)
	}
	return namedRef(ref)
}

func (s *Service) LinkWithOutcome(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	workflowReq, mixed, err := workflowLinkRequest(req)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	if mixed {
		if s.SourceRuntimeInstalled() {
			return port.WriteOutcome{}, failure("unsupported", "source relation edits require canonical batch preparation")
		}
		if s.workflowProvider() == nil {
			return port.WriteOutcome{}, failure("invalid_params", "workflow references require a supported workflow item operation")
		}
		if err := validateWorkflowWrite(req.From, req.Expect, meta); err != nil && workflowRef(req.From) {
			return port.WriteOutcome{}, err
		}
		if err := validateWorkflowWrite(req.To, req.Expect, meta); err != nil && workflowRef(req.To) {
			return port.WriteOutcome{}, err
		}
		if s.workflow.CanPrepareOrdinaryOperations() {
			return s.linkWorkflowChecked(ctx, by, req, workflowReq, meta)
		}
		if err := s.validateWorkflowEndpoints(ctx, by, meta.Project, req.From, req.To); err != nil {
			return port.WriteOutcome{}, err
		}
		commit, err := s.workflowProvider().Link(ctx, by, workflowReq, meta)
		return port.WriteOutcome{Commit: commit}, err
	}
	cs, err := linkChangeSet(req, model.OpLink)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	return s.apply(ctx, by, "bead.link", cs, meta, true)
}

func (s *Service) Link(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta) (model.Commit, error) {
	_, mixed, err := workflowLinkRequest(req)
	if err != nil {
		return model.Commit{}, err
	}
	if mixed {
		v, err := s.LinkWithOutcome(ctx, by, req, meta)
		return v.Commit, err
	}
	cs, err := linkChangeSet(req, model.OpLink)
	if err != nil {
		return model.Commit{}, err
	}
	v, err := s.apply(ctx, by, "bead.link", cs, meta, false)
	return v.Commit, err
}

func (s *Service) UnlinkWithOutcome(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	workflowReq, mixed, err := workflowLinkRequest(req)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	if mixed {
		if s.SourceRuntimeInstalled() {
			return port.WriteOutcome{}, failure("unsupported", "source relation edits require canonical batch preparation")
		}
		if s.workflowProvider() == nil {
			return port.WriteOutcome{}, failure("invalid_params", "workflow references require a supported workflow item operation")
		}
		if err := validateWorkflowWrite(req.From, req.Expect, meta); err != nil && workflowRef(req.From) {
			return port.WriteOutcome{}, err
		}
		if err := validateWorkflowWrite(req.To, req.Expect, meta); err != nil && workflowRef(req.To) {
			return port.WriteOutcome{}, err
		}
		if err := s.validateWorkflowEndpoints(ctx, by, meta.Project, req.From, req.To); err != nil {
			return port.WriteOutcome{}, err
		}
		commit, err := s.workflowProvider().Unlink(ctx, by, workflowReq, meta)
		return port.WriteOutcome{Commit: commit}, err
	}
	cs, err := linkChangeSet(req, model.OpUnlink)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	return s.apply(ctx, by, "bead.unlink", cs, meta, true)
}

func (s *Service) Unlink(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta) (model.Commit, error) {
	_, mixed, err := workflowLinkRequest(req)
	if err != nil {
		return model.Commit{}, err
	}
	if mixed {
		v, err := s.UnlinkWithOutcome(ctx, by, req, meta)
		return v.Commit, err
	}
	cs, err := linkChangeSet(req, model.OpUnlink)
	if err != nil {
		return model.Commit{}, err
	}
	v, err := s.apply(ctx, by, "bead.unlink", cs, meta, false)
	return v.Commit, err
}

func (s *Service) reader(ctx context.Context, by model.PrincipalRef, asOf *AsOf) (driver.Reader, error) {
	if err := s.admit(ctx, by); err != nil {
		return nil, err
	}
	r, err := ReadAsOf(ctx, s.store, asOf)
	if err != nil {
		return nil, err
	}
	if err := s.admit(ctx, by); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

// ValidateAsOf validates the input union before a trusted continuation replaces
// it with its resolved concrete address. It does not authorize a read.
func ValidateAsOf(asOf AsOf) error {
	if asOf.Instant != nil {
		if asOf.Revision != "" {
			return failure("invalid_params", "asOf must select either a revision or an instant")
		}
		if _, err := asOf.Instant.MarshalText(); err != nil {
			return failure("invalid_params", "invalid asOf instant")
		}
		return nil
	}
	if err := asOf.Revision.Validate(); err != nil {
		return failure("invalid_params", "invalid asOf revision")
	}
	return nil
}

// ReadAsOf is the shared content-snapshot boundary for domain reads and their
// authorization adapter. The caller owns principal admission and current read
// permission; this function resolves content only. An instant is resolved from
// one complete committed log snapshot, then reopened at its concrete address.
func ReadAsOf(ctx context.Context, store driver.Store, asOf *AsOf) (driver.Reader, error) {
	if asOf != nil {
		if err := ValidateAsOf(*asOf); err != nil {
			return nil, err
		}
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return nil, mapped(err)
	}
	view := driver.View{Incarnation: meta.Incarnation}
	if asOf != nil {
		if asOf.Instant == nil {
			revision := asOf.Revision
			view.At = &revision
		}
		if asOf.Incarnation != "" {
			view.Incarnation = asOf.Incarnation
		}
	}
	r, err := store.Read(ctx, view)
	if err != nil {
		return nil, mapped(err)
	}
	snapshot := r.Snapshot()
	if snapshot.At == nil {
		_ = r.Close()
		return nil, fmt.Errorf("driver returned an unpinned read snapshot")
	}
	if asOf == nil || asOf.Instant == nil {
		return r, nil
	}
	revision, err := historyInstantRevision(ctx, r, *asOf.Instant)
	if err != nil {
		_ = r.Close()
		return nil, historyGone(err)
	}
	if revision.String() == snapshot.At.String() {
		return r, nil
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	// Preserve the incarnation and exact revision observed by the resolver.
	// A concurrent append cannot advance this read, and a restore must refuse.
	view = driver.View{Branch: snapshot.Branch, At: &revision, Incarnation: snapshot.Incarnation}
	r, err = store.Read(ctx, view)
	if err != nil {
		return nil, historyGone(err)
	}
	if r.Snapshot().At == nil || r.Snapshot().At.String() != revision.String() || r.Snapshot().Incarnation != snapshot.Incarnation {
		_ = r.Close()
		return nil, failure("internal_error", "driver changed the resolved instant snapshot")
	}
	return r, nil
}
