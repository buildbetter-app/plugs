package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func coreReadRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
		return failure("invalid_params", "point reads require a nonempty ID on the main branch")
	}
	if strings.HasPrefix(ref.ID, "workflow:") {
		return validateWorkflowRef(ref)
	}
	return nil
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	copy := make(map[string]any, len(value))
	for key, item := range value {
		copy[key] = item
	}
	return copy
}

func (s *Service) Get(ctx context.Context, by model.PrincipalRef, req GetRequest) (GetResult, error) {
	if workflowRef(req.ID) {
		if err := validateWorkflowRef(req.ID); err != nil {
			return GetResult{}, err
		}
		if s.workflow != nil {
			item, err := s.workflow.GetWithResolver(ctx, by, req.ID, workflowMeta(by, "bead.get", req.Project, req.Agent), s.workflowBlockerResolver(ctx, by, workflowMeta(by, "bead.get", req.Project, req.Agent)))
			if err == nil {
				if req.AsOf != nil {
					return GetResult{}, failure("invalid_params", "workflow items have no historical asOf view")
				}
				return workflowGetResult(item), nil
			}
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "not_found" {
				return GetResult{}, err
			}
		}
	}
	if err := coreReadRef(req.ID); err != nil {
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
	if s.workflow != nil && req.AsOf == nil {
		links, err := s.workflow.Relations(ctx, by, req.ID, workflowMeta(by, "bead.get", req.Project, req.Agent))
		if err != nil {
			return GetResult{}, err
		}
		result.Bead.Links = append(result.Bead.Links, links...)
		// These visible relations come from the actual checkpoint index. The
		// ordinary row remains in its own graph without a shadow parent row.
		for _, relation := range links {
			if relation.From != req.ID.ID || relation.Kind != "parent-child" {
				continue
			}
			if err := validateWorkflowRef(relation.To); err != nil {
				return GetResult{}, failure("internal_error", "invalid persisted mixed hierarchy parent")
			}
			if result.Bead.Parent != "" && result.Bead.Parent != relation.To.ID {
				return GetResult{}, failure("internal_error", "conflicting persisted mixed hierarchy parents")
			}
			result.Bead.Parent = relation.To.ID
		}
		blocked, err := s.workflow.OrdinaryBlocked(ctx, by, []string{req.ID.ID}, workflowMeta(by, "bead.get", doc.Project, req.Agent))
		if err != nil {
			return GetResult{}, err
		}
		result.Bead.Blocked = result.Bead.Blocked || blocked[req.ID.ID]
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

// Only a trusted ReadyWithFilter traversal carries this detached graph between
// internal pages. Readers, authorization facts and membership decisions are
// never retained here. Every page validates the source and reevaluates its live
// clock, leases, policy and ordering on that page's own reader.
type readyGraphSelection struct {
	owner *Service
	asOf  AsOf
	graph map[string]storedBead
}

func (s *Service) readyItems(ctx context.Context, by model.PrincipalRef, req ReadyRequest, r driver.Reader) ([]Bead, map[string]readyOrderKey, error) {
	var graph map[string]storedBead
	if req.readySourceBinding && req.readyGraph != nil {
		selection := req.readyGraph
		view := r.Snapshot()
		if selection.owner != s || view.At == nil {
			return nil, nil, failure("cursor_invalid", "invalid guarded ready source")
		}
		if selection.graph != nil {
			if selection.asOf.Incarnation != view.Incarnation || selection.asOf.Revision.String() != view.At.String() {
				return nil, nil, failure("cursor_invalid", "durable ready source changed while collecting candidates; restart paging")
			}
			graph = selection.graph
		} else {
			var err error
			graph, err = scanBeads(ctx, r)
			if err != nil {
				return nil, nil, err
			}
			selection.graph = graph
			selection.asOf = AsOf{Revision: *view.At, Incarnation: view.Incarnation}
		}
	} else {
		var err error
		graph, err = scanBeads(ctx, r)
		if err != nil {
			return nil, nil, err
		}
	}
	graphIDs := sortedIDs(graph)
	workflowBlocked := map[string]bool{}
	if s.workflow != nil {
		resolver := &workflowCoreResolver{service: s, principal: by, graph: graph}
		facts, _, err := s.workflow.OrdinaryParentSnapshot(ctx, by, graphIDs, workflowMeta(by, "bead.ready", req.Project, req.Agent), resolver)
		if err != nil {
			return nil, nil, err
		}
		lookup := func(id string) (workflowplugin.OrdinaryParentFact, error) { return facts[id], nil }
		for _, id := range graphIDs {
			blocked, err := resolver.mixedParentBlocked(ctx, model.RefByID(id, model.Natural64(0)), lookup)
			if err != nil {
				return nil, nil, err
			}
			workflowBlocked[id] = blocked
		}
	}
	// Membership is always evaluated against the complete current graph. P74
	// applies only to the order of this exact set, before any page is cut.
	now := s.now().UTC()
	eligible := map[string]storedBead{}
	for _, id := range graphIDs {
		row := graph[id]
		st := state(row.doc)
		if req.claiming && !pipeline.ClaimAssignmentEligible(row.doc.Assignment, by, req.Agent, s.claimPools) {
			continue
		}
		if !matchesFilter(row, req.Project, req.Filter) || st.Template || !s.ontology.effective(st.Project).ReadyEligible(st.Type, st.Status) {
			continue
		}
		if workflowBlocked[row.doc.ID] || row.doc.Fields["blocked"] == true {
			continue
		}
		if row.doc.Lease != nil {
			lease, err := s.effectiveLease(ctx, row.doc.ID, row.doc.Lease)
			if err != nil {
				return nil, nil, err
			}
			if now.Before(lease.ExpiresAt) {
				continue
			}
		}
		unblocked, err := s.ontology.readyDependencies(row.doc, graph)
		if err != nil {
			return nil, nil, err
		}
		if !unblocked {
			continue
		}
		// Only the authorized membership needs ordering inputs. In particular,
		// a denied legacy row with unknown creation time must not prevent an
		// otherwise valid authorized set from being read.
		if req.authorizationFilter != nil {
			bead, err := beadValue(row)
			if err != nil {
				return nil, nil, err
			}
			allowed, err := req.authorizationFilter(ctx, r, bead)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
		}
		eligible[id] = row
	}
	keys, err := readyOrderKeys(ctx, r, eligible, now)
	if err != nil {
		return nil, nil, err
	}
	ids := sortedIDs(eligible)
	sortReadyIDs(ids, keys)
	items := make([]Bead, 0, len(ids))
	for _, id := range ids {
		row := eligible[id]
		bead, err := beadValue(row)
		if err != nil {
			return nil, nil, err
		}
		bead.CreatedAt = keys[id].CreatedAt
		items = append(items, bead)
	}
	return items, keys, nil
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
	items, keys, err := s.readyItems(ctx, by, req, r)
	if err != nil {
		return BeadPage{}, err
	}
	includeEphemeral := true
	if req.Filter != nil && req.Filter.IncludeEphemeral != nil {
		includeEphemeral = *req.Filter.IncludeEphemeral
	}
	if s.workflow != nil && includeEphemeral {
		active, err := s.workflow.ActiveIDsFor(ctx, by, workflowMeta(by, "bead.ready", req.Project, req.Agent))
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
		items, err = spreadClaimNextItems(items, req.ClaimNextSeed, keys)
		if err != nil {
			return BeadPage{}, err
		}
	}
	if err := s.admit(ctx, by); err != nil {
		return BeadPage{}, err
	}
	view := r.Snapshot()
	membership := ""
	if req.readySourceBinding {
		// Internal guarded collection binds the entire core order before any
		// slice, including a cutoff change without a durable revision. The
		// guard replaces this internal value with its authorized public digest.
		h := sha256.New()
		var length [8]byte
		for _, item := range items {
			binary.BigEndian.PutUint64(length[:], uint64(len(item.ID)))
			_, _ = h.Write(length[:])
			_, _ = h.Write([]byte(item.ID))
		}
		membership = hex.EncodeToString(h.Sum(nil))
	}
	page, err := pageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation}, false, restartOnRevision)
	if err == nil {
		page.ReadyMembership = membership
	}
	return page, err
}

// spreadClaimNextItems rotates equal hybrid-key groups by a stable offset.
// It never crosses a bucket, rank or creation-time boundary. The optional
// projection preserves the existing helper's priority-only test callers;
// the live queue always supplies the complete keys from its pinned snapshot.
// The ordinary bead.ready path never calls this.
func spreadClaimNextItems(items []Bead, seed string, order ...map[string]readyOrderKey) ([]Bead, error) {
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
			same := priority == groupPriority
			if len(order) != 0 {
				a, b := order[0][ordered[start].ID], order[0][ordered[end].ID]
				same = a.Bucket == b.Bucket && a.Rank == b.Rank && a.CreatedAt.Equal(b.CreatedAt)
			}
			if !same {
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

// ReadyWithFilter is the composition-root entry point for a compiled finite
// authorization predicate. Keeping this capability separate from the public
// ReadyRequest prevents a transport caller from supplying an arbitrary
// predicate while allowing the guarded adapter to filter before pageResult.
func (s *Service) ReadyWithFilter(ctx context.Context, by model.PrincipalRef, req ReadyRequest, filter ReadyAuthorizer) (BeadPage, error) {
	req.authorizationFilter = filter
	req.readySourceBinding = true
	req.readyGraph = &readyGraphSelection{owner: s}
	if req.Position != nil && req.Position.readyGraph != nil {
		req.readyGraph = req.Position.readyGraph
	}
	page, err := s.Ready(ctx, by, req)
	if err == nil && page.Next != nil {
		page.Next.readyGraph = req.readyGraph
	}
	return page, err
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
	snapshot, err := s.workflow.ReadySnapshotWithResolver(ctx, by, req.Project, types, workflowMeta(by, "bead.ready", req.Project, req.Agent), includeEphemeral, s.workflowBlockerResolver(ctx, by, workflowMeta(by, "bead.ready", req.Project, req.Agent)))
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
	fields := cloneMap(item.Fields)
	if fields == nil {
		fields = map[string]any{}
	}
	fields["title"] = item.Title
	bead := Bead{ID: item.ID, Type: item.Type, Project: item.Project, Revision: item.Revision, Status: item.Status,
		Fields: fields, Metadata: item.Metadata, Parent: refID(item.Parent), Labels: append([]string(nil), item.Labels...), Links: append([]workflowplugin.RelationView(nil), item.Links...),
		Blocked: item.Blocked, ChildCounter: item.ChildCounter, Plane: item.Plane, Kind: item.Kind, Plugin: item.Plugin, WorkflowRunID: item.RunID}
	if item.Claim != nil {
		bead.Assignee = item.Claim.Agent
	}
	return bead
}

func refID(ref *model.Ref) string {
	if ref == nil {
		return ""
	}
	return ref.ID
}

func workflowGetResult(item workflowplugin.ItemView) GetResult {
	result := GetResult{Bead: workflowBead(item), Comments: []Comment{}}
	if item.Claim != nil {
		result.Lease = &Lease{Principal: item.Claim.Principal, Agent: item.Claim.Agent, ExpiresAt: item.Claim.ExpiresAt, TTL: workflowplugin.ClaimTTL, Epoch: item.Claim.Generation}
	}
	for _, comment := range item.Comments {
		result.Comments = append(result.Comments, Comment{Body: comment.Body, Principal: comment.Principal, Time: comment.Time})
	}
	return result
}

// ClaimNextReadyPage is the queue-specific live pager. A revision change
// restarts its offset from the beginning of the newly observed ready set.
func (s *Service) ClaimNextReadyPage(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (BeadPage, error) {
	req.claiming = true
	return s.readyCombined(ctx, by, req, true)
}

// ReadyCandidates evaluates the complete ready set on a supplied snapshot.
// ClaimNext's terminal transaction probe uses this to inspect the same fresh
// write snapshot after head acquisition without opening a second reader.
func (s *Service) ReadyCandidates(ctx context.Context, by model.PrincipalRef, req ReadyRequest, reader driver.Reader) ([]Bead, error) {
	req.claiming = true
	if _, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, 1000); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, failure("internal_error", "ready probe reader is unavailable")
	}
	items, _, err := s.readyItems(ctx, by, req, reader)
	return items, err
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
