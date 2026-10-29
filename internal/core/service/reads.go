package service

import (
	"context"
	"slices"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func (s *Service) Get(ctx context.Context, by model.PrincipalRef, req GetRequest) (GetResult, error) {
	if err := namedRef(req.ID); err != nil {
		return GetResult{}, err
	}
	r, err := s.reader(ctx, by, req.AsOf)
	if err != nil {
		return GetResult{}, err
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{req.ID})
	if err != nil {
		return GetResult{}, mapped(err)
	}
	if len(rows) == 0 || rows[0].Kind != "bead" {
		return GetResult{}, failure("not_found", "bead was not found")
	}
	doc, err := decodeDocument(rows[0].Body)
	if err != nil || doc.ID != rows[0].Ref.ID {
		return GetResult{}, failure("internal_error", "invalid persisted bead")
	}
	bead, err := beadValue(storedBead{rows[0], doc})
	if err != nil {
		return GetResult{}, err
	}
	result := GetResult{Bead: bead, Attachments: doc.Attachments}
	if doc.Lease != nil {
		ttl, err := durationMS(doc.Lease.TtlMs)
		if err != nil {
			return GetResult{}, err
		}
		result.Lease = &Lease{Principal: doc.Lease.Principal, Agent: doc.Lease.Agent, ExpiresAt: doc.Lease.ExpiresAt, TTL: ttl, Epoch: doc.Lease.Epoch}
	}
	if err := s.admit(ctx, by); err != nil {
		return GetResult{}, err
	}
	return result, nil
}

func pageOptions(project, viewID, cursor string, filter *Filter, limit int) (int, error) {
	if strings.TrimSpace(project) == "" {
		return 0, failure("project_required", "supply a project")
	}
	if cursor != "" {
		return 0, failure("cursor_invalid", "RPC must validate the opaque cursor before passing a domain page position")
	}
	if viewID != "" || (filter != nil && (filter.Query != "" || filter.ViewID != "" || len(filter.Events) != 0)) {
		return 0, failure("invalid_params", "views, query strings and event filters are not supported by this slice")
	}
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return 0, failure("invalid_params", "limit must be between 1 and 1000")
	}
	return limit, nil
}

func matchesFilter(row storedBead, project string, filter *Filter) bool {
	if row.doc.Project != project {
		return false
	}
	if filter == nil {
		return true
	}
	return (len(filter.Types) == 0 || slices.Contains(filter.Types, row.doc.Type)) &&
		(len(filter.Resources) == 0 || slices.Contains(filter.Resources, row.doc.ID))
}

func pageResult(items []Bead, limit int, position *PagePosition, asOf AsOf) (BeadPage, error) {
	offset := 0
	if position != nil {
		if position.Offset < 0 || position.Offset > len(items) || position.AsOf.Incarnation != asOf.Incarnation || position.AsOf.Revision.String() != asOf.Revision.String() {
			return BeadPage{}, failure("cursor_invalid", "page position does not match the snapshot")
		}
		offset = position.Offset
	}
	end := min(offset+limit, len(items))
	page := BeadPage{Items: items[offset:end], AsOf: asOf}
	if end < len(items) {
		page.Next = &PagePosition{Offset: end, AsOf: asOf}
	}
	return page, nil
}

func validatePosition(position *PagePosition) error {
	if position != nil && (position.Offset < 0 || position.Offset > scanLimit || position.AsOf.Incarnation == "" || position.AsOf.Revision.Validate() != nil) {
		return failure("cursor_invalid", "invalid domain page position")
	}
	return nil
}

func (s *Service) Ready(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (BeadPage, error) {
	limit, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, req.Limit)
	if err != nil {
		return BeadPage{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return BeadPage{}, err
	}
	var asOf *AsOf
	if req.Position != nil {
		asOf = &req.Position.AsOf
	}
	r, err := s.reader(ctx, by, asOf)
	if err != nil {
		return BeadPage{}, err
	}
	defer r.Close()
	graph, err := scanBeads(ctx, r)
	if err != nil {
		return BeadPage{}, err
	}
	items := []Bead{}
	now := s.now()
	ids := sortedIDs(graph)
	priorities := map[string]int{}
	for _, id := range ids {
		p, err := priority(graph[id].doc)
		if err != nil {
			return BeadPage{}, err
		}
		priorities[id] = p
	}
	sort.SliceStable(ids, func(i, j int) bool { return priorities[ids[i]] < priorities[ids[j]] })
	for _, id := range ids {
		row := graph[id]
		st := state(row.doc)
		if !matchesFilter(row, req.Project, req.Filter) || st.Template || !s.ontology.effective(st.Project).ReadyEligible(st.Type, st.Status) {
			continue
		}
		if row.doc.Lease != nil && now.Before(row.doc.Lease.ExpiresAt) {
			continue
		}
		unblocked, err := s.ontology.noOpenBlockers(row.doc, graph)
		if err != nil {
			return BeadPage{}, err
		}
		if !unblocked {
			continue
		}
		bead, err := beadValue(row)
		if err != nil {
			return BeadPage{}, err
		}
		items = append(items, bead)
	}
	if err := s.admit(ctx, by); err != nil {
		return BeadPage{}, err
	}
	view := r.Snapshot()
	return pageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation})
}

func (s *Service) History(ctx context.Context, by model.PrincipalRef, req HistoryRequest) (BeadPage, error) {
	limit, err := pageOptions(req.Project, "", req.Cursor, req.Filter, req.Limit)
	if err != nil {
		return BeadPage{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return BeadPage{}, err
	}
	if req.Position != nil && (req.Position.AsOf.Revision.String() != req.AsOf.Revision.String() || (req.AsOf.Incarnation != "" && req.Position.AsOf.Incarnation != req.AsOf.Incarnation)) {
		return BeadPage{}, failure("cursor_invalid", "history page does not match requested asOf")
	}
	if req.Position != nil {
		req.AsOf = req.Position.AsOf
	}
	r, err := s.reader(ctx, by, &req.AsOf)
	if err != nil {
		return BeadPage{}, err
	}
	defer r.Close()
	graph, err := scanBeads(ctx, r)
	if err != nil {
		return BeadPage{}, err
	}
	items := []Bead{}
	for _, id := range sortedIDs(graph) {
		row := graph[id]
		if !matchesFilter(row, req.Project, req.Filter) {
			continue
		}
		bead, err := beadValue(row)
		if err != nil {
			return BeadPage{}, err
		}
		items = append(items, bead)
	}
	if err := s.admit(ctx, by); err != nil {
		return BeadPage{}, err
	}
	view := r.Snapshot()
	return pageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation})
}
