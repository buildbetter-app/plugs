package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type Service struct {
	store    driver.Store
	lease    driver.ActiveLease
	writer   model.Writer
	ontology *Ontology
	now      func() time.Time
}

type Option func(*Service)

// WithOntology shares the same project ontology resolver used by the pipeline.
func WithOntology(o *Ontology) Option       { return func(s *Service) { s.ontology = o } }
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

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

func mapped(err error) error {
	if err == nil {
		return nil
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

func namedRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
		return failure("invalid_params", "named verbs require a nonempty bead ID on the main branch")
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
		return port.WriteOutcome{}, failure("invalid_params", "parent creation is not supported by this slice")
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: req.Type, Fields: req.Fields, Project: meta.Project}}}}
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
	v, err := s.claim(ctx, by, req, meta, true)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	_, err = claimResult(v.Commit)
	return v, err
}

func (s *Service) claim(ctx context.Context, by model.PrincipalRef, req ClaimRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := namedRef(req.ID); err != nil {
		return port.WriteOutcome{}, err
	}
	// Slot normalization belongs after the pipeline's idempotency lookup. This
	// stable request replays even after the current bead has changed status.
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.ID, Fields: map[string]any{}}}}, Expect: req.Expect}
	return s.apply(ctx, by, "bead.claim", cs, meta, outcomeRequired)
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
	return s.comment(ctx, by, req, meta, true)
}

func (s *Service) comment(ctx context.Context, by model.PrincipalRef, req CommentRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := namedRef(req.ID); err != nil {
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
	return s.close(ctx, by, req, meta, true)
}

func (s *Service) close(ctx context.Context, by model.PrincipalRef, req CloseRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := namedRef(req.ID); err != nil {
		return port.WriteOutcome{}, err
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.ID, Fields: map[string]any{}}}}, Expect: req.Expect}
	if req.Reason != "" {
		cs.Ops = append(cs.Ops, model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: req.ID, Body: req.Reason}})
	}
	return s.apply(ctx, by, "bead.close", cs, meta, outcomeRequired)
}

func (s *Service) reader(ctx context.Context, by model.PrincipalRef, asOf *AsOf) (driver.Reader, error) {
	if err := s.admit(ctx, by); err != nil {
		return nil, err
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return nil, mapped(err)
	}
	view := driver.View{Incarnation: meta.Incarnation}
	if asOf != nil {
		if err := asOf.Revision.Validate(); err != nil {
			return nil, failure("invalid_params", "invalid asOf revision")
		}
		view.At = &asOf.Revision
		if asOf.Incarnation != "" {
			view.Incarnation = asOf.Incarnation
		}
	}
	r, err := s.store.Read(ctx, view)
	if err != nil {
		return nil, mapped(err)
	}
	if err := s.admit(ctx, by); err != nil {
		_ = r.Close()
		return nil, err
	}
	if r.Snapshot().At == nil {
		_ = r.Close()
		return nil, fmt.Errorf("driver returned an unpinned read snapshot")
	}
	return r, nil
}
