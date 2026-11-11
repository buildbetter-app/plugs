package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

type BlockedBead struct {
	Bead           Bead
	BlockedBy      []model.Ref
	BlockedByCount int
	HiddenBlockers int
}

type BlockedPage struct {
	Items            []BlockedBead
	AsOf             AsOf
	Next             *PagePosition
	WorkflowSnapshot *WorkflowPageSnapshot
	Membership       string
}

// Blocked derives a listing from the current graph. Explicit stored blocked
// flags remain write guards; they are neither trusted nor repaired here.
func (s *Service) Blocked(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (BlockedPage, error) {
	return s.BlockedWithFilter(ctx, by, req, nil)
}

// BlockedWithFilter applies current subject and reason visibility before the
// page boundary. A hidden or unresolved reason contributes only to counts.
func (s *Service) BlockedWithFilter(ctx context.Context, by model.PrincipalRef, req ReadyRequest, allowed ReadyAuthorizer) (BlockedPage, error) {
	limit, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, req.Limit)
	if err != nil {
		return BlockedPage{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return BlockedPage{}, err
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return BlockedPage{}, err
	}
	defer r.Close()
	graph, err := scanBeads(ctx, r)
	if err != nil {
		return BlockedPage{}, err
	}
	workflow := map[string]Bead{}
	var binding *WorkflowPageSnapshot
	if provider := s.workflowProvider(); provider != nil {
		observed, err := provider.BlockedGraph(ctx, by, workflowMeta(by, "bead.blocked", req.Project, req.Agent))
		if err != nil {
			return BlockedPage{}, err
		}
		binding = &WorkflowPageSnapshot{Revision: model.Revision(observed.Revision)}
		for _, item := range observed.Items {
			bead := workflowBead(item)
			workflow[item.ID] = bead
			var parent *model.Ref
			if bead.Parent != "" {
				ref := model.RefByID(bead.Parent, model.Natural64(0))
				parent = &ref
			}
			graph[item.ID] = storedBead{doc: pipeline.Document{ID: item.ID, Type: item.Type, Project: item.Project, Parent: parent, Fields: map[string]any{"status": item.Status, "title": item.Title}}}
		}
		for _, relation := range observed.Relations {
			row, exists := graph[relation.From]
			if !exists {
				continue
			}
			if relation.Kind == "parent-child" {
				if row.doc.Parent != nil && row.doc.Parent.ID != relation.To.ID {
					return BlockedPage{}, failure("internal_error", "conflicting mixed graph parents")
				}
				ref := relation.To
				row.doc.Parent = &ref
			} else {
				row.doc.Links = append(row.doc.Links, pipeline.Link{To: relation.To, Kind: relation.Kind, Metadata: relation.Metadata})
			}
			graph[relation.From] = row
		}
	}
	reasons, err := s.blockedReasons(ctx, graph, workflow)
	if err != nil {
		return BlockedPage{}, err
	}
	parents, err := s.parentFilter(ctx, r, req.Filter, true)
	if err != nil {
		return BlockedPage{}, err
	}
	visible := map[string]bool{}
	beads := map[string]Bead{}
	for _, id := range sortedIDs(graph) {
		bead, isWorkflow := workflow[id]
		if !isWorkflow {
			bead, err = beadValue(graph[id])
			if err != nil {
				return BlockedPage{}, err
			}
		}
		beads[id] = bead
		visible[id] = true
		if allowed != nil {
			visible[id], err = allowed(ctx, r, bead)
			if err != nil {
				return BlockedPage{}, err
			}
		}
	}
	items := []BlockedBead{}
	for _, id := range sortedIDs(graph) {
		row := graph[id]
		_, isWorkflow := workflow[id]
		if !visible[id] || len(reasons[id]) == 0 || !matchesFilter(row, req.Project, req.Filter) || parents != nil && !parents[id] || isWorkflow && req.Filter != nil && req.Filter.IncludeEphemeral != nil && !*req.Filter.IncludeEphemeral {
			continue
		}
		bead := beads[id]
		bead.Blocked = true
		// Relations are a projection too: a hidden reason must not escape
		// through the embedded bead's parent or links.
		bead.Links = nil
		for _, link := range row.doc.Links {
			if link.TargetKind != model.LinkTargetExternal && link.To.IsID() && link.To.Branch.IsZero() && visible[link.To.ID] {
				for _, relation := range beads[id].Links {
					if relation.To == link.To && relation.Kind == link.Kind {
						bead.Links = append(bead.Links, relation)
						break
					}
				}
			}
		}
		if bead.Parent != "" && !visible[bead.Parent] {
			bead.Parent, bead.ParentHidden = "", true
		}
		item := BlockedBead{Bead: bead, BlockedBy: []model.Ref{}, BlockedByCount: len(reasons[id])}
		for _, reason := range reasons[id] {
			if !reason.Unresolved && visible[reason.Ref.ID] {
				item.BlockedBy = append(item.BlockedBy, reason.Ref)
			} else {
				item.HiddenBlockers++
			}
		}
		items = append(items, item)
	}
	view := r.Snapshot()
	current, err := s.store.Meta(ctx)
	if err != nil {
		return BlockedPage{}, mapped(err)
	}
	if current.Incarnation != view.Incarnation || current.Head != *view.At {
		return BlockedPage{}, failure("cursor_invalid", "blocked graph changed during collection; restart paging")
	}
	if provider := s.workflowProvider(); provider != nil {
		check, err := provider.BlockedGraph(ctx, by, workflowMeta(by, "bead.blocked", req.Project, req.Agent))
		if err != nil {
			return BlockedPage{}, err
		}
		if binding.Revision != model.Revision(check.Revision) {
			return BlockedPage{}, failure("cursor_invalid", "workflow blocked graph changed during collection; restart paging")
		}
	}
	data, err := json.Marshal(items)
	if err != nil {
		return BlockedPage{}, err
	}
	sum := sha256.Sum256(data)
	membership := hex.EncodeToString(sum[:])
	asOf := AsOf{Revision: *view.At, Incarnation: view.Incarnation}
	offset := 0
	if req.Position != nil {
		position := req.Position
		if position.AsOf != asOf || position.Offset > len(items) || position.ReadyMembership != membership || !sameBlockedWorkflow(position.WorkflowSnapshot, binding) {
			return BlockedPage{}, failure("cursor_invalid", "blocked cursor does not match the graph and visibility snapshot")
		}
		offset = position.Offset
	}
	end := min(offset+limit, len(items))
	page := BlockedPage{Items: items[offset:end], AsOf: asOf, WorkflowSnapshot: binding, Membership: membership}
	if end < len(items) {
		page.Next = &PagePosition{Offset: end, AsOf: asOf, WorkflowSnapshot: binding, ReadyMembership: membership}
	}
	if err := s.admit(ctx, by); err != nil {
		return BlockedPage{}, err
	}
	return page, nil
}

func sameBlockedWorkflow(a, b *WorkflowPageSnapshot) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Equal(b)
}

// blockedReasons never follows its own public API or the stored projection.
// Direct reasons win over parent fallback. A queue propagates blocked parents
// once through their children, so deep chains never consume the call stack.
type blockedReason struct {
	Ref        model.Ref
	Unresolved bool
}

func (s *Service) blockedReasons(ctx context.Context, graph map[string]storedBead, workflow map[string]Bead) (map[string][]blockedReason, error) {
	terminal := func(row storedBead) bool {
		if _, ok := workflow[row.doc.ID]; ok {
			return state(row.doc).Status == "closed"
		}
		return s.ontology.effective(row.doc.Project).SatisfiesBlocks(state(row.doc).Status)
	}
	closedSubject := func(row storedBead) bool { return state(row.doc).Status == "pinned" || terminal(row) }
	reasons := map[string][]blockedReason{}
	children := map[string][]string{}
	allChildren := map[string][]storedBead{}
	for _, row := range graph {
		if row.doc.Parent != nil {
			allChildren[row.doc.Parent.ID] = append(allChildren[row.doc.Parent.ID], row)
		}
	}
	queue := []string{}
	for _, id := range sortedIDs(graph) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row := graph[id]
		if closedSubject(row) {
			continue
		}
		if row.doc.Parent != nil {
			children[row.doc.Parent.ID] = append(children[row.doc.Parent.ID], id)
		}
		unique := map[string]blockedReason{}
		for _, link := range row.doc.Links {
			if link.Kind != "blocks" && link.Kind != "conditional-blocks" && link.Kind != "waits-for" && !(link.Kind == "parent-child" && link.TargetKind == model.LinkTargetExternal) {
				continue
			}
			blocker, exists := graph[link.To.ID]
			unresolved := !exists || link.TargetKind == model.LinkTargetExternal || !link.To.IsID() || !link.To.Branch.IsZero()
			satisfied := false
			if !unresolved {
				satisfied = terminal(blocker)
				if link.Kind == "conditional-blocks" {
					_, ephemeral := workflow[blocker.doc.ID]
					satisfied = !ephemeral && s.ontology.effective(blocker.doc.Project).SatisfiesConditionalBlocks(state(blocker.doc).Status)
				}
				if link.Kind == "waits-for" {
					// The public blocked listing follows native's default
					// all-children gate, including a closed spawner.
					if link.Metadata != nil && link.Metadata.AlsoBlocks && !terminal(blocker) {
						satisfied = false
					} else {
						openChild, terminalChild := false, false
						for _, child := range allChildren[blocker.doc.ID] {
							if terminal(child) {
								terminalChild = true
							} else {
								openChild = true
							}
						}
						satisfied = !openChild || link.Metadata != nil && link.Metadata.Gate == "any-children" && terminalChild
					}
				}
			}
			if !satisfied {
				key, _ := json.Marshal(link.To)
				previous := unique[string(key)]
				unique[string(key)] = blockedReason{Ref: link.To, Unresolved: unresolved || previous.Unresolved}
			}
		}
		if len(unique) == 0 && row.doc.Parent != nil {
			if _, exists := graph[row.doc.Parent.ID]; !exists {
				key, _ := json.Marshal(row.doc.Parent)
				unique[string(key)] = blockedReason{Ref: *row.doc.Parent, Unresolved: true}
			}
		}
		keys := make([]string, 0, len(unique))
		for key := range unique {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			reasons[id] = append(reasons[id], unique[key])
		}
		if len(reasons[id]) != 0 {
			queue = append(queue, id)
		}
	}
	for index := 0; index < len(queue); index++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		parent := queue[index]
		for _, id := range children[parent] {
			if len(reasons[id]) == 0 {
				reasons[id] = []blockedReason{{Ref: model.RefByID(parent, model.Natural64(0))}}
				queue = append(queue, id)
			}
		}
	}
	return reasons, nil
}
