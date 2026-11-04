package authzrpc

import (
	"context"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (c *Checker) ontologyManagementInstalled() bool {
	if c != nil {
		for _, rules := range c.chain.Rules {
			if _, ok := rules.(*authzfixed.OntologyRuleSet); ok {
				return true
			}
		}
	}
	return false
}

// AuthorizeOntologyRead is the current-policy adapter consumed by ontologyops,
// including reads of historical sources and removed overlays. Authorization
// always uses the current store image; content history supplies no grant.
func (c *Checker) AuthorizeOntologyRead(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
	if !c.ontologyManagementInstalled() {
		return forbidden("ontology management policy is not installed")
	}
	meta, err := c.store.Meta(ctx)
	if err != nil {
		return err
	}
	reader, err := c.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return err
	}
	defer reader.Close()
	kind, method := authz.Ontology, ""
	if resource.Kind == "bead" {
		kind, method = authz.Bead, "bead.get"
	} else if resource.Kind != "ontology" {
		return forbidden("unsupported ontology read resource")
	}
	allowed, err := c.decision(ctx, reader, by, authz.Read, kind, resource.Ref, method)
	if err != nil {
		return err
	}
	if !allowed {
		return forbidden("current ontology read authority is absent")
	}
	return nil
}

// AuthorizeProtected reuses this installation's actual chain, verified caller,
// and current facts. It never maps ontology.apply to a generic write. The
// Writer tracks every consumed dependency and checks it under publication's
// locks; both replay and the final locked check require a live User session.
func (c *Checker) AuthorizeProtected(ctx context.Context, stage port.AuthorizationStage, by model.PrincipalRef, req pipeline.ProtectedApplyRequest, meta model.WriteMeta, reader port.SnapshotReader) (port.EventContext, error) {
	if !c.ontologyManagementInstalled() {
		return port.EventContext{}, forbidden("ontology management policy is not installed")
	}
	if stage != port.AuthorizeBefore && stage != port.AuthorizeAfter && stage != port.AuthorizeReplay {
		return port.EventContext{}, forbidden("unsupported protected authorization stage")
	}
	if meta.Method != "ontology.apply" || reader == nil || !req.Source.Ref.IsID() || !req.Source.Ref.Branch.IsZero() ||
		(req.Source.Ref.ID != "ontology" && !strings.HasPrefix(req.Source.Ref.ID, "ontology/")) {
		return port.EventContext{}, forbidden("invalid protected ontology operation")
	}
	if err := authn.RequireInteractiveUser(ctx, by); err != nil {
		return port.EventContext{}, err
	}
	rows, err := reader.Get(ctx, []model.Ref{req.Source.Ref})
	if err != nil {
		return port.EventContext{}, err
	}
	if len(rows) != 1 || rows[0].Kind != "ontology" || rows[0].Ref.ID != req.Source.Ref.ID || rows[0].Ref.Branch.String() != req.Source.Ref.Branch.String() || rows[0].Revision.Validate() != nil {
		return port.EventContext{}, forbidden("protected ontology source is unavailable")
	}
	allowed, err := c.decision(ctx, reader, by, authz.ManageOntology, authz.Ontology, req.Source.Ref, "ontology.apply")
	if err != nil {
		return port.EventContext{}, err
	}
	if !allowed {
		return port.EventContext{}, forbidden("current protected ontology authority is absent")
	}
	return port.EventContext{
		Cause: model.EventCause{Root: meta.IdempotencyKey, Depth: model.Natural64(0), Chain: []string{}},
		Chain: []string{"authz/fixed", "authz/ontology-management"},
	}, nil
}

var _ pipeline.ProtectedAuthorizer = (*Checker)(nil)

func (c *Checker) RevalidateProtectedContext(ctx context.Context, by model.PrincipalRef) error {
	if !c.ontologyManagementInstalled() {
		return forbidden("ontology management policy is not installed")
	}
	return authn.RequireInteractiveUser(ctx, by)
}

// Discovery uses the same installed management rule, current source and live
// interactive identity as apply. A generic write grant or historical receipt
// cannot make the protected method visible. This remains an inventory check;
// the Writer independently reauthorizes the actual proposed source.
func (c *Checker) ontologyManagementVisible(ctx context.Context, by model.PrincipalRef) (bool, error) {
	if !c.ontologyManagementInstalled() || authn.RequireInteractiveUser(ctx, by) != nil {
		return false, nil
	}
	meta, err := c.store.Meta(ctx)
	if err != nil {
		return false, err
	}
	at := meta.Head
	reader, err := c.store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		return false, err
	}
	defer reader.Close()
	query := driver.Query{Filter: map[string]any{"kind": "ontology"}, Limit: 1000}
	for {
		page, err := reader.Query(ctx, query)
		if err != nil {
			return false, err
		}
		for _, row := range page.Items {
			if row.Kind != "ontology" || !row.Ref.IsID() || !row.Ref.Branch.IsZero() || row.Revision.Validate() != nil ||
				(row.Ref.ID != "ontology" && !strings.HasPrefix(row.Ref.ID, "ontology/")) {
				continue
			}
			allowed, err := c.decision(ctx, reader, by, authz.ManageOntology, authz.Ontology, row.Ref, "ontology.apply")
			if err != nil || allowed {
				return allowed, err
			}
		}
		if page.NextCursor == nil {
			return false, nil
		}
		query.Cursor = page.NextCursor
	}
}
