package ontologyops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func New(store ReadStore, lease driver.ActiveLease, cfg Config) (*Service, error) {
	if store == nil || lease == nil || cfg.Source == nil {
		return nil, fmt.Errorf("ontology operations require a store, active lease and protected source locator")
	}
	if cfg.MaxPlans == 0 {
		cfg.MaxPlans = 64
	}
	if cfg.MaxPlans < 1 || cfg.MaxPlans > 1024 {
		return nil, fmt.Errorf("MaxPlans must be between 1 and 1024")
	}
	return &Service{store: store, lease: lease, cfg: cfg, plans: map[string]retainedPlan{}}, nil
}

func failure(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint, Retryable: code == "busy"}}
}

func mapped(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return failure("busy", "ontology read was canceled or exceeded its deadline")
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Details: details}}
	}
	return err
}

func (s *Service) admit(ctx context.Context, by model.PrincipalRef) error {
	if err := ctx.Err(); err != nil {
		return mapped(err)
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

func (s *Service) authorize(ctx context.Context, by model.PrincipalRef, ref model.Ref, kind, project string) error {
	if s.cfg.AuthorizeRead != nil {
		return s.cfg.AuthorizeRead(ctx, by, model.ResourceRef{Kind: kind, Ref: ref}, project)
	}
	return nil
}

func (s *Service) reader(ctx context.Context, by model.PrincipalRef, at *model.Revision) (driver.Meta, driver.Reader, error) {
	if err := s.admit(ctx, by); err != nil {
		return driver.Meta{}, nil, err
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return driver.Meta{}, nil, mapped(err)
	}
	if at != nil {
		a, aerr := at.Uint64()
		h, herr := meta.Head.Uint64()
		if aerr != nil || herr != nil || a > h {
			return driver.Meta{}, nil, failure("not_found", "ontology revision is beyond the stored history")
		}
	}
	r, err := s.store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	return meta, r, mapped(err)
}

type source struct {
	document service.OntologySource
	compiled ontology.Snapshot
	problems []Problem
	guard    SourceGuard
}

func sameRef(a, b model.Ref) bool {
	return a.IsID() && b.IsID() && a.ID == b.ID && a.Branch.String() == b.Branch.String()
}

func (s *Service) source(ctx context.Context, by model.PrincipalRef, r driver.Reader, project string) (source, error) {
	ref, err := s.cfg.Source(project)
	if err != nil {
		return source{}, err
	}
	if err := ref.Validate(); err != nil || !ref.IsID() || !ref.Branch.IsZero() || (ref.ID != "ontology" && !strings.HasPrefix(ref.ID, "ontology/")) {
		return source{}, failure("invalid_params", "source locator must select a protected branch-zero ontology ID")
	}
	if err := s.authorize(ctx, by, ref, "ontology", project); err != nil {
		return source{}, err
	}
	rows, err := r.Get(ctx, []model.Ref{ref})
	if err != nil {
		return source{}, mapped(err)
	}
	if len(rows) == 0 {
		return source{}, failure("not_found", "protected ontology source is absent")
	}
	if len(rows) != 1 || !sameRef(rows[0].Ref, ref) || rows[0].Kind != "ontology" || rows[0].Revision == "" || rows[0].Revision.Validate() != nil {
		return source{}, failure("invalid_params", "unsupported protected ontology source identity, kind or revision")
	}
	// The service owner supplies the one strict source-envelope decoder.
	doc, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		return source{}, failure("invalid_params", "protected ontology source cannot be decoded: "+err.Error())
	}
	compiled, problems := ontology.Compile(doc.Base, doc.Overlays, doc.Plugins)
	out := source{document: doc, compiled: compiled, problems: []Problem{}, guard: SourceGuard{Ref: ref, Revision: rows[0].Revision, BodyHash: bytesDigest(rows[0].Body), Digest: compiled.Digest()}}
	for _, p := range problems {
		out.problems = append(out.problems, Problem{Path: p.Path, Code: string(p.Code), Message: p.Message})
	}
	return out, nil
}

func bytesDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func digest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return bytesDigest(raw), nil
}

func (s source) valid() error {
	if len(s.problems) != 0 || s.compiled.Digest() == "" {
		return failure("invalid_params", "protected ontology source does not compile")
	}
	return nil
}
