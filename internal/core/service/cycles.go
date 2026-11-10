package service

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type CyclesRequest struct {
	Project          string
	IncludeTracks    bool
	IncludeEphemeral bool
	Limit            int
}

// An unreadable member keeps its position without disclosing its identity.
type CycleMember struct {
	Ref        *model.Ref `json:"ref,omitempty"`
	Unreadable bool       `json:"unreadable,omitempty"`
}

type Cycle struct {
	Path            []CycleMember `json:"path"`
	MemberCount     model.Natural `json:"memberCount"`
	UnreadableCount model.Natural `json:"unreadableCount"`
	Partial         bool          `json:"partial"`
}

type CyclesResult struct {
	Items      []Cycle       `json:"items"`
	CycleCount model.Natural `json:"cycleCount"`
	Truncated  bool          `json:"truncated"`
	AsOf       AsOf          `json:"asOf"`
}

// cycleRow separates report topology from durable record envelopes. Only the
// adopted durable graph supplies these rows; no provider state is substituted.
type cycleRow struct {
	project string
	links   []pipeline.Link
}

func (s *Service) Cycles(ctx context.Context, by model.PrincipalRef, req CyclesRequest) (CyclesResult, error) {
	if err := s.admit(ctx, by); err != nil {
		return CyclesResult{}, err
	}
	// Installed-policy composition uses GuardedService.Cycles. Never fall
	// back to unfiltered disclosure if that adapter was omitted by a host.
	if s.authorizationInstalled != nil && s.authorizationInstalled() {
		return CyclesResult{}, failure("authz_unavailable", "cycle read authorization is unavailable")
	}
	if err := CheckCycleReportCapability(s.store, req); err != nil {
		return CyclesResult{}, err
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return CyclesResult{}, err
	}
	closed := false
	closeContent := func() error {
		if closed {
			return nil
		}
		closed = true
		return r.Close()
	}
	defer closeContent()
	result, err := ReadCycles(ctx, r, req, nil)
	if err != nil {
		return CyclesResult{}, err
	}
	// Closing the real content reader can observe cancellation or lease loss.
	// Finish that boundary before the final publication admission check.
	if err := closeContent(); err != nil {
		return CyclesResult{}, mapped(err)
	}
	if err := s.admit(ctx, by); err != nil {
		return CyclesResult{}, err
	}
	if s.authorizationInstalled != nil && s.authorizationInstalled() {
		return CyclesResult{}, failure("authz_unavailable", "cycle read authorization is unavailable")
	}
	return result, nil
}

// CheckCycleReportCapability validates the selector before reporting a missing
// combined graph snapshot. Workflow links and persistence alone do not provide
// a complete durable/workflow graph at one read generation.
func CheckCycleReportCapability(_ driver.Store, req CyclesRequest) error {
	if _, err := cycleLimit(req); err != nil {
		return err
	}
	if !req.IncludeEphemeral {
		return nil
	}
	return ephemeralCycleCapabilityFailure()
}

func ephemeralCycleCapabilityFailure() error {
	return failure("plugin_durable_state_unavailable", "ephemeral cycle reporting requires a supported combined graph snapshot")
}

func cycleLimit(req CyclesRequest) (int, error) {
	if strings.TrimSpace(req.Project) == "" {
		return 0, failure("project_required", "supply a project")
	}
	limit := req.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return 0, failure("invalid_params", "cycle reporting requires limit 1..1000")
	}
	return limit, nil
}

// ReadCycles uses one pinned content snapshot. An installed-policy caller
// supplies a current-read decision callback and fences that authority before
// publishing the result. No graph write, receipt or lease renewal occurs.
func ReadCycles(ctx context.Context, reader driver.Reader, req CyclesRequest, readable func(context.Context, model.Ref) (bool, error)) (CyclesResult, error) {
	if err := ctx.Err(); err != nil {
		return CyclesResult{}, err
	}
	limit, err := cycleLimit(req)
	if err != nil {
		return CyclesResult{}, err
	}
	if reader == nil || reader.Snapshot().At == nil {
		return CyclesResult{}, failure("invalid_params", "cycle reporting requires a pinned snapshot")
	}
	if req.IncludeEphemeral {
		return CyclesResult{}, ephemeralCycleCapabilityFailure()
	}
	rows, err := scanBeads(ctx, reader)
	if err != nil {
		return CyclesResult{}, err
	}
	graph := make(map[string]cycleRow, len(rows))
	for id, row := range rows {
		graph[id] = cycleRow{project: row.doc.Project, links: row.doc.Links}
	}
	view := reader.Snapshot()
	return readCycleGraph(ctx, graph, req, limit, AsOf{Revision: *view.At, Incarnation: view.Incarnation}, readable)
}

func readCycleGraph(ctx context.Context, rows map[string]cycleRow, req CyclesRequest, limit int, asOf AsOf, readable func(context.Context, model.Ref) (bool, error)) (CyclesResult, error) {
	if err := ctx.Err(); err != nil {
		return CyclesResult{}, err
	}
	base, combined := map[string]map[string]bool{}, map[string]map[string]bool{}
	add := func(graph map[string]map[string]bool, from, to string) {
		if graph[from] == nil {
			graph[from] = map[string]bool{}
		}
		graph[from][to] = true
		if graph[to] == nil {
			graph[to] = map[string]bool{}
		}
	}
	for id, row := range rows {
		for _, edge := range row.links {
			if edge.TargetKind == model.LinkTargetExternal || !edge.To.IsID() || !edge.To.Branch.IsZero() || edge.To.ID == "" {
				continue
			}
			if edge.Kind == "blocks" || edge.Kind == "conditional-blocks" {
				add(base, id, edge.To.ID)
				add(combined, id, edge.To.ID)
			} else if req.IncludeTracks && edge.Kind == "tracks" {
				add(combined, id, edge.To.ID)
			}
		}
	}
	paths, err := cyclePaths(ctx, base, combined, req.IncludeTracks)
	if err != nil {
		return CyclesResult{}, err
	}
	visible := map[string]bool{}
	known := map[string]bool{}
	result := CyclesResult{Items: []Cycle{}, CycleCount: model.Natural64(0), AsOf: asOf}
	count := uint64(0)
	for _, path := range paths {
		item := Cycle{Path: make([]CycleMember, 0, len(path)), MemberCount: model.Natural64(uint64(len(path)))}
		anchored, hidden := false, uint64(0)
		for _, id := range path {
			ref := model.RefByID(id, model.Natural64(0))
			if !known[id] {
				_, visible[id] = rows[id]
				if visible[id] && readable != nil {
					visible[id], err = readable(ctx, ref)
					if err != nil {
						return CyclesResult{}, err
					}
				}
				known[id] = true
			}
			if visible[id] {
				item.Path = append(item.Path, CycleMember{Ref: &ref})
				anchored = anchored || rows[id].project == req.Project
			} else {
				item.Path = append(item.Path, CycleMember{Unreadable: true})
				hidden++
			}
		}
		// Counts have the same visible-project scope as the returned items.
		// Wholly hidden cycles do not become an existence oracle.
		if !anchored {
			continue
		}
		count++
		item.UnreadableCount, item.Partial = model.Natural64(hidden), hidden != 0
		if len(result.Items) < limit {
			result.Items = append(result.Items, item)
		}
	}
	if err := ctx.Err(); err != nil {
		return CyclesResult{}, err
	}
	result.CycleCount, result.Truncated = model.Natural64(count), count > uint64(len(result.Items))
	return result, nil
}

// The base report is a sorted DFS back-edge report, not an enumeration of all
// simple cycles. Optional tracks adds shortest return paths for each blocking
// edge; a tracks-only loop cannot enter that set. Parallel endpoints collapse.
func cyclePaths(ctx context.Context, base, combined map[string]map[string]bool, includeTracks bool) ([][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sortedKeys := func(values map[string]bool) []string {
		out := make([]string, 0, len(values))
		for value := range values {
			out = append(out, value)
		}
		sort.Strings(out)
		return out
	}
	ordered := func(graph map[string]map[string]bool) (map[string][]string, []string) {
		out, nodes := map[string][]string{}, map[string]bool{}
		for id, edges := range graph {
			out[id], nodes[id] = sortedKeys(edges), true
		}
		return out, sortedKeys(nodes)
	}
	adjacent, roots := ordered(base)
	paths := map[string][]string{}
	retain := func(path []string) {
		lowest := 0
		for i := range path {
			if path[i] < path[lowest] {
				lowest = i
			}
		}
		rotated := append(append([]string{}, path[lowest:]...), path[:lowest]...)
		encoded, _ := json.Marshal(rotated)
		paths[string(encoded)] = rotated
	}
	active, done := map[string]int{}, map[string]bool{}
	stack := []string{}
	var walk func(string) error
	walk = func(id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if position, exists := active[id]; exists {
			retain(stack[position:])
			return nil
		}
		if done[id] {
			return nil
		}
		active[id] = len(stack)
		stack = append(stack, id)
		for _, next := range adjacent[id] {
			if err := walk(next); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		delete(active, id)
		done[id] = true
		return nil
	}
	for _, id := range roots {
		if err := walk(id); err != nil {
			return nil, err
		}
	}
	if includeTracks {
		mixed, _ := ordered(combined)
		for _, from := range roots {
			for _, to := range adjacent[from] {
				previous := map[string]string{to: ""}
				queue := []string{to}
				for i := 0; i < len(queue); i++ {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					at := queue[i]
					if at == from {
						path := []string{at}
						for at != to {
							at = previous[at]
							path = append(path, at)
						}
						slices.Reverse(path)
						retain(append([]string{from}, path[:len(path)-1]...))
						break
					}
					for _, next := range mixed[at] {
						if _, visited := previous[next]; !visited {
							previous[next] = at
							queue = append(queue, next)
						}
					}
				}
			}
		}
	}
	out := make([][]string, 0, len(paths))
	for _, path := range paths {
		out = append(out, path)
	}
	sort.Slice(out, func(i, j int) bool { return slices.Compare(out[i], out[j]) < 0 })
	return out, nil
}
