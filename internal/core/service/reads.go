package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func (s *Service) Get(ctx context.Context, by model.PrincipalRef, req GetRequest) (GetResult, error) {
	if workflowRef(req.ID) {
		if err := validateWorkflowRef(req.ID); err != nil {
			return GetResult{}, err
		}
		if req.AsOf != nil {
			return GetResult{}, failure("invalid_params", "workflow items have no historical asOf view")
		}
		if s.workflow == nil {
			return GetResult{}, failure("not_found", "workflow item was not found")
		}
		item, err := s.workflow.Get(ctx, by, req.ID, workflowMeta(by, "bead.get", req.Project, req.Agent))
		if err != nil {
			return GetResult{}, err
		}
		return workflowGetResult(item), nil
	}
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
	if s.workflow != nil {
		links, err := s.workflow.Relations(ctx, by, req.ID, workflowMeta(by, "bead.get", req.Project, req.Agent))
		if err != nil {
			return GetResult{}, err
		}
		result.Bead.Links = links
	}
	if len(doc.Comments) != 0 {
		result.Comments = make([]Comment, len(doc.Comments))
		for i, comment := range doc.Comments {
			principal, err := clonePrincipalValue(comment.Principal)
			if err != nil {
				return GetResult{}, failure("internal_error", "invalid persisted comment actor")
			}
			value := Comment{Body: comment.Body, Principal: principal, Time: comment.Time, Origin: comment.Origin, SourceAuthor: comment.SourceAuthor}
			if comment.SourceTime != nil {
				at := *comment.SourceTime
				value.SourceTime = &at
			}
			result.Comments[i] = value
		}
	}
	if doc.Lease != nil {
		lease, err := s.effectiveLease(ctx, doc.ID, doc.Lease)
		if err != nil {
			return GetResult{}, err
		}
		ttl, err := durationMS(lease.TtlMs)
		if err != nil {
			return GetResult{}, err
		}
		result.Lease = &Lease{Principal: lease.Principal, Agent: lease.Agent, ExpiresAt: lease.ExpiresAt, TTL: ttl, Epoch: lease.Epoch}
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

func pageResult(items []Bead, limit int, position *PagePosition, asOf AsOf, pinned, restartOnRevision bool) (BeadPage, error) {
	offset := 0
	if position != nil {
		if position.AsOf.Incarnation != asOf.Incarnation {
			return BeadPage{}, mapped(driver.ErrIncarnationChanged{From: position.AsOf.Incarnation, To: asOf.Incarnation})
		}
		if position.Offset < 0 || (pinned && (position.Offset > len(items) || position.AsOf.Revision.String() != asOf.Revision.String())) {
			return BeadPage{}, failure("cursor_invalid", "page position does not match the snapshot")
		}
		// ClaimNext pages are ordered candidates from a live snapshot. If a
		// fresh snapshot changed the revision, restart at the beginning rather
		// than applying an offset from the old candidate ordering.
		if !restartOnRevision || pinned || position.AsOf.Revision.String() == asOf.Revision.String() {
			offset = position.Offset
		}
	}
	end := offset + limit
	if end < offset || end > len(items) {
		end = len(items)
	}
	// A live list may shrink below its previous cutoff. Its tail is then an
	// empty page, while snapshot history retains strict position validation.
	page := BeadPage{Items: items[min(offset, len(items)):end], AsOf: asOf}
	if end < len(items) {
		page.Next = &PagePosition{Offset: end, AsOf: asOf}
	}
	return page, nil
}

func validatePosition(position *PagePosition) error {
	if position != nil && (position.Offset < 0 || position.AsOf.Incarnation == "" || position.AsOf.Revision.Validate() != nil) {
		return failure("cursor_invalid", "invalid domain page position")
	}
	return nil
}

func (s *Service) readyItems(ctx context.Context, req ReadyRequest, r queryReader) ([]Bead, error) {
	graph, err := scanBeads(ctx, r)
	if err != nil {
		return nil, err
	}
	items := []Bead{}
	now := s.now()
	ids := sortedIDs(graph)
	priorities := map[string]int{}
	for _, id := range ids {
		p, err := priority(graph[id].doc)
		if err != nil {
			return nil, err
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
		if row.doc.Lease != nil {
			lease, err := s.effectiveLease(ctx, row.doc.ID, row.doc.Lease)
			if err != nil {
				return nil, err
			}
			if now.Before(lease.ExpiresAt) {
				continue
			}
		}
		unblocked, err := s.ontology.readyDependencies(row.doc, graph)
		if err != nil {
			return nil, err
		}
		if !unblocked {
			continue
		}
		bead, err := beadValue(row)
		if err != nil {
			return nil, err
		}
		items = append(items, bead)
	}
	return items, nil
}

func (s *Service) readyPage(ctx context.Context, by model.PrincipalRef, req ReadyRequest, restartOnRevision bool) (BeadPage, error) {
	limit, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, req.Limit)
	if err != nil {
		return BeadPage{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return BeadPage{}, err
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return BeadPage{}, err
	}
	defer r.Close()
	items, err := s.readyItems(ctx, req, r)
	if err != nil {
		return BeadPage{}, err
	}
	if s.workflow != nil {
		active, err := s.workflow.ActiveIDs(ctx)
		if err != nil {
			return BeadPage{}, err
		}
		filtered := items[:0]
		for _, item := range items {
			if _, collides := active[item.ID]; collides {
				continue
			}
			filtered = append(filtered, item)
		}
		items = filtered
	}
	if restartOnRevision && req.ClaimNextSeed != "" {
		items, err = spreadClaimNextItems(items, req.ClaimNextSeed)
		if err != nil {
			return BeadPage{}, err
		}
	}
	if err := s.admit(ctx, by); err != nil {
		return BeadPage{}, err
	}
	view := r.Snapshot()
	return pageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation}, false, restartOnRevision)
}

// spreadClaimNextItems rotates each equal-priority group by a stable offset.
// Priority groups stay contiguous, so the queue still drains every lower
// priority before considering a higher one; only ID ties are spread between
// concurrent request keys. The ordinary bead.ready path never calls this.
func spreadClaimNextItems(items []Bead, seed string) ([]Bead, error) {
	if len(items) < 2 || seed == "" {
		return items, nil
	}
	ordered := append([]Bead(nil), items...)
	for start := 0; start < len(ordered); {
		groupPriority, err := beadPriority(ordered[start])
		if err != nil {
			return nil, err
		}
		end := start + 1
		for end < len(ordered) {
			priority, err := beadPriority(ordered[end])
			if err != nil {
				return nil, err
			}
			if priority != groupPriority {
				break
			}
			end++
		}
		if end-start > 1 {
			groupSeed := sha256.Sum256([]byte(seed + "\x00" + strconv.Itoa(groupPriority)))
			offset := int(binary.BigEndian.Uint64(groupSeed[:8]) % uint64(end-start))
			if offset != 0 {
				rotated := append([]Bead(nil), ordered[start+offset:end]...)
				rotated = append(rotated, ordered[start:start+offset]...)
				copy(ordered[start:end], rotated)
			}
		}
		start = end
	}
	return ordered, nil
}

func (s *Service) Ready(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (BeadPage, error) {
	return s.readyCombined(ctx, by, req, false)
}

func (s *Service) readyCombined(ctx context.Context, by model.PrincipalRef, req ReadyRequest, restartOnRevision bool) (BeadPage, error) {
	limit, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, req.Limit)
	if err != nil {
		return BeadPage{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return BeadPage{}, err
	}
	if s.workflow == nil {
		if req.Position != nil && (req.Position.Workflow || req.Position.WorkflowSnapshot != nil) {
			return BeadPage{}, failure("cursor_invalid", "workflow provider is unavailable; restart paging")
		}
		return s.readyPage(ctx, by, req, restartOnRevision)
	}
	workflowPhase := req.Position != nil && req.Position.Workflow
	coreReq := req
	if workflowPhase {
		// Observe the current core incarnation without applying a provider
		// offset to core content or re-emitting the first phase.
		coreReq.Position = nil
		coreReq.Limit = 1
	}
	page, err := s.readyPage(ctx, by, coreReq, restartOnRevision)
	if err != nil {
		return BeadPage{}, err
	}
	if workflowPhase && req.Position.AsOf.Incarnation != page.AsOf.Incarnation {
		return BeadPage{}, mapped(driver.ErrIncarnationChanged{From: req.Position.AsOf.Incarnation, To: page.AsOf.Incarnation})
	}
	// Core rows retain their existing live pagination. Only after consuming
	// them do we capture a provider snapshot for the second phase.
	if !workflowPhase && page.Next != nil {
		return page, nil
	}
	types := []string(nil)
	if req.Filter != nil {
		types = req.Filter.Types
	}
	includeEphemeral := true
	if req.Filter != nil && req.Filter.IncludeEphemeral != nil {
		includeEphemeral = *req.Filter.IncludeEphemeral
	}
	snapshot, err := s.workflow.ReadySnapshot(ctx, by, req.Project, types, workflowMeta(by, "bead.ready", req.Project, req.Agent), includeEphemeral)
	if err != nil {
		return BeadPage{}, err
	}
	binding := &WorkflowPageSnapshot{Revision: model.Revision(snapshot.Revision), ValidUntil: snapshot.ValidUntil}
	if !binding.Valid() {
		return BeadPage{}, failure("internal_error", "invalid workflow ready snapshot")
	}
	items := snapshot.Items
	if req.Filter != nil && len(req.Filter.Resources) != 0 {
		filtered := make([]workflowplugin.ItemView, 0, len(items))
		for _, item := range items {
			if slices.Contains(req.Filter.Resources, item.ID) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	page.WorkflowSnapshot = binding
	offset := 0
	if workflowPhase {
		if req.Position.WorkflowSnapshot == nil || !req.Position.WorkflowSnapshot.Valid() {
			return BeadPage{}, failure("cursor_invalid", "workflow cursor is not snapshot-bound; restart paging")
		}
		if !req.Position.WorkflowSnapshot.Equal(binding) {
			if !restartOnRevision {
				return BeadPage{}, failure("cursor_invalid", "workflow ready membership changed; restart paging")
			}
			// ClaimNext is an internal live walker. Its own successful claims
			// change the provider generation; restart rather than skipping rows.
		} else {
			offset = req.Position.Offset
		}
		if offset > len(items) {
			return BeadPage{}, failure("cursor_invalid", "workflow cursor is beyond the captured ready set")
		}
		page.Items = nil
		page.Next = nil
	}
	remaining := limit - len(page.Items)
	end := min(offset+remaining, len(items))
	for _, item := range items[offset:end] {
		page.Items = append(page.Items, workflowBead(item))
	}
	if end < len(items) {
		page.Next = &PagePosition{Offset: end, AsOf: page.AsOf, Workflow: true, WorkflowSnapshot: binding}
	}
	return page, nil
}

func workflowBead(item workflowplugin.ItemView) Bead {
	fields := map[string]any{"title": item.Title}
	return Bead{ID: item.ID, Type: item.Type, Project: item.Project, Revision: item.Revision, Status: item.Status,
		Fields: fields, Metadata: item.Metadata, Parent: refID(item.Parent), Labels: append([]string(nil), item.Labels...), Links: append([]workflowplugin.RelationView(nil), item.Links...),
		Blocked: item.Blocked, ChildCounter: item.ChildCounter, Plane: item.Plane, Kind: item.Kind, Plugin: item.Plugin, WorkflowRunID: item.RunID}
}

func refID(ref *model.Ref) string {
	if ref == nil {
		return ""
	}
	return ref.ID
}

func workflowGetResult(item workflowplugin.ItemView) GetResult {
	result := GetResult{Bead: workflowBead(item), Comments: []Comment{}}
	for _, comment := range item.Comments {
		result.Comments = append(result.Comments, Comment{Body: comment.Body, Principal: comment.Principal, Time: comment.Time})
	}
	return result
}

// ClaimNextReadyPage is the queue-specific live pager. A revision change
// restarts its offset from the beginning of the newly observed ready set.
func (s *Service) ClaimNextReadyPage(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (BeadPage, error) {
	return s.readyCombined(ctx, by, req, true)
}

// ReadyCandidates evaluates the complete ready set on a supplied snapshot.
// ClaimNext's terminal transaction probe uses this to inspect the same fresh
// write snapshot after head acquisition without opening a second reader.
func (s *Service) ReadyCandidates(ctx context.Context, req ReadyRequest, reader driver.Reader) ([]Bead, error) {
	if _, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, 1000); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, failure("internal_error", "ready probe reader is unavailable")
	}
	return s.readyItems(ctx, req, reader)
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
	return pageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation}, true, false)
}
