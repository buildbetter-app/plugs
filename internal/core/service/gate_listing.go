package service

import (
	"context"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// GateListRequest asks for exact installed gate nodes. Limit zero is unlimited.
// Check callers exclude templates before the limit and select AwaitType after it.
type GateListRequest struct {
	Project          string
	Agent            string
	All              bool
	ExcludeTemplates bool
	Limit            int64
	AwaitType        string
}

type GateVisibility func(context.Context, *Bead) (bool, error)

func (s *Service) GateList(ctx context.Context, by model.PrincipalRef, req GateListRequest) (BeadPage, error) {
	return s.GateListWithFilter(ctx, by, req, nil)
}

// GateListWithFilter keeps visibility and query selection before the native
// limit. It never uses ordinary pageOptions, whose default and cap differ.
func (s *Service) GateListWithFilter(ctx context.Context, by model.PrincipalRef, req GateListRequest, visible GateVisibility) (BeadPage, error) {
	if s == nil || s.store == nil {
		return BeadPage{}, failure("unsupported", "gate listing requires a query source")
	}
	if strings.TrimSpace(req.Project) == "" {
		return BeadPage{}, failure("project_required", "supply a project")
	}
	if req.Limit < 0 {
		return BeadPage{}, failure("invalid_params", "gate limit must not be negative")
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return BeadPage{}, err
	}
	defer r.Close()
	view := r.Snapshot()
	if view.At == nil {
		return BeadPage{}, failure("internal_error", "gate reader has no snapshot revision")
	}
	page := BeadPage{Items: []Bead{}, AsOf: AsOf{Revision: *view.At, Incarnation: view.Incarnation}}
	type candidate struct {
		bead     Bead
		priority int
	}
	candidates := map[string]candidate{}
	accept := func(bead Bead) error {
		if bead.Project != req.Project || bead.Type != ontology.GateType {
			return nil
		}
		closed, _ := s.ontology.effective(bead.Project).SlotStatus(bead.Type, ontology.Close)
		isClosed := closed != "" && bead.Status == closed
		if bead.Kind == "workflow.item" {
			isClosed = bead.Status == "closed"
		}
		if !req.All && isClosed {
			return nil
		}
		if template, _ := bead.Fields["template"].(bool); req.ExcludeTemplates && template {
			return nil
		}
		if visible != nil {
			allowed, err := visible(ctx, &bead)
			if err != nil {
				return err
			}
			if !allowed {
				return nil
			}
		}
		priority, err := beadPriority(bead)
		if err != nil {
			return err
		}
		candidates[bead.ID] = candidate{bead: bead, priority: priority}
		return nil
	}
	var cursor *driver.Cursor
	for {
		rows, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return BeadPage{}, mapped(err)
		}
		for _, row := range rows.Items {
			// A row without an issue is absent, not a failed query.
			if row.Kind != "bead" || len(row.Body) == 0 {
				continue
			}
			doc, err := decodeDocument(row.Body)
			if err != nil || doc.ID != row.Ref.ID || !row.Ref.Branch.IsZero() {
				return BeadPage{}, failure("internal_error", "invalid persisted gate query row")
			}
			bead, err := beadValue(storedBead{row, doc})
			if err != nil {
				return BeadPage{}, err
			}
			if err := accept(bead); err != nil {
				return BeadPage{}, err
			}
		}
		if rows.NextCursor == nil {
			break
		}
		if len(rows.Items) == 0 {
			return BeadPage{}, failure("internal_error", "gate query did not advance")
		}
		cursor = rows.NextCursor
	}
	if provider := s.workflowProvider(); provider != nil {
		items, err := provider.ListItems(ctx, by, req.Project, workflowMeta(by, "gate.list", req.Project, req.Agent))
		if err != nil {
			return BeadPage{}, err
		}
		for _, item := range items {
			if err := accept(workflowBead(item)); err != nil {
				return BeadPage{}, err
			}
		}
		// As in the combined ready reader, refuse a mixed ordinary head if
		// it moved while reading the provider's single checkpoint image.
		current, err := s.store.Meta(ctx)
		if err != nil {
			return BeadPage{}, mapped(err)
		}
		if current.Incarnation != view.Incarnation || current.Head.String() != view.At.String() {
			return BeadPage{}, failure("cursor_invalid", "gate source changed while listing; repeat the read")
		}
	}
	// The native Querier's default order is priority ASC, created_at DESC,
	// id ASC. Merge both complete planes before cutting the selected bound;
	// a higher-ranked workflow gate must be able to displace a durable gate.
	ordered := make([]candidate, 0, len(candidates))
	for _, item := range candidates {
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.priority != b.priority {
			return a.priority < b.priority
		}
		if !a.bead.CreatedAt.Equal(b.bead.CreatedAt) {
			return a.bead.CreatedAt.After(b.bead.CreatedAt)
		}
		return a.bead.ID < b.bead.ID
	})
	for i, item := range ordered {
		if req.Limit > 0 && int64(i) >= req.Limit {
			break
		}
		bead := item.bead
		awaitType, _ := bead.Fields["await_type"].(string)
		if req.AwaitType == "" || req.AwaitType == "all" || req.AwaitType == awaitType || req.AwaitType == "gh" && strings.HasPrefix(awaitType, "gh:") {
			page.Items = append(page.Items, bead)
		}
	}
	return page, s.admit(ctx, by)
}
