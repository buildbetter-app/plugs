package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// DirectBlockerState is the authoritative state of one stored direct edge in
// the supplied reader snapshot. Unknown is deliberately distinct from an
// unsatisfied status: a missing blocker or unavailable ontology cannot be
// presented as a readable reason.
type DirectBlockerState string

const (
	DirectBlockerSatisfied   DirectBlockerState = "satisfied"
	DirectBlockerUnsatisfied DirectBlockerState = "unsatisfied"
	DirectBlockerUnknown     DirectBlockerState = "unknown"
)

// DirectBlockerEdge is a read-only verdict for one direct blocking edge. The
// blocker reference is retained for the consumer to apply its own current-read
// authorization; this service never authorizes or redacts a resource.
type DirectBlockerEdge struct {
	TargetKind model.LinkTargetKind
	Blocker    model.Ref
	Kind       string
	Project    string
	Status     string
	State      DirectBlockerState
	Reason     string
}

// DirectBlockerVerdict is bounded to the dependent's stored direct edges.
// Waits-for evaluates their direct child fanout, never transitive blockers,
// and all source reads remain on the supplied pinned reader.
type DirectBlockerVerdict struct {
	Dependent model.Ref
	Edges     []DirectBlockerEdge
}

// DirectBlockerRequest identifies one dependent and the durable ontology
// source used for each blocker project in the same reader snapshot.
type DirectBlockerRequest struct {
	Dependent model.Ref
	Ontology  OntologySourceLocator
	Limit     int
}

const (
	directBlockerDefaultLimit = 100
	directBlockerMaxLimit     = 1000
)

// ReadDirectBlockers evaluates the dependent's direct blocks,
// conditional-blocks and waits-for links against the caller's pinned reader.
// Waits-for uses the same child predicate as readiness and close guards.
// Ontology, blocker and child records use that same snapshot; no service
// store, live readiness query, writer, receipt or authorization callback is
// consulted.
func ReadDirectBlockers(ctx context.Context, reader driver.Reader, req DirectBlockerRequest) (DirectBlockerVerdict, error) {
	if err := ctx.Err(); err != nil {
		return DirectBlockerVerdict{}, err
	}
	if reader == nil {
		return DirectBlockerVerdict{}, failure("invalid_params", "direct blocker verdict requires a pinned reader")
	}
	if err := namedRef(req.Dependent); err != nil {
		return DirectBlockerVerdict{}, err
	}
	if req.Ontology == nil {
		return DirectBlockerVerdict{}, failure("invalid_params", "direct blocker verdict requires an ontology source locator")
	}
	limit := req.Limit
	if limit == 0 {
		limit = directBlockerDefaultLimit
	}
	if limit < 1 || limit > directBlockerMaxLimit {
		return DirectBlockerVerdict{}, failure("invalid_params", "direct blocker limit must be between 1 and 1000")
	}
	rows, err := reader.Get(ctx, []model.Ref{req.Dependent})
	if err != nil {
		return DirectBlockerVerdict{}, mapped(err)
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		return DirectBlockerVerdict{}, failure("not_found", "dependent bead was not found")
	}
	dependent, err := decodeDocument(rows[0].Body)
	if err != nil || dependent.ID != req.Dependent.ID {
		return DirectBlockerVerdict{}, failure("internal_error", "invalid persisted dependent bead")
	}
	links := make([]pipeline.Link, 0, len(dependent.Links))
	for _, link := range dependent.Links {
		if link.Kind != "blocks" && link.Kind != "conditional-blocks" && link.Kind != "waits-for" {
			continue
		}
		if len(links) == limit {
			return DirectBlockerVerdict{}, failure("closure_too_large", "direct blocker candidate bound exceeded")
		}
		links = append(links, link)
	}
	verdict := DirectBlockerVerdict{Dependent: req.Dependent, Edges: make([]DirectBlockerEdge, 0, len(links))}
	if len(links) == 0 {
		return verdict, nil
	}
	refs := make([]model.Ref, 0, len(links))
	seenRefs := make(map[string]bool, len(links))
	for _, link := range links {
		if link.TargetKind == model.LinkTargetExternal || !link.To.IsID() || !link.To.Branch.IsZero() {
			continue
		}
		if seenRefs[link.To.ID] {
			continue
		}
		seenRefs[link.To.ID] = true
		refs = append(refs, link.To)
	}
	blockerRows := []driver.Record{}
	if len(refs) != 0 {
		blockerRows, err = reader.Get(ctx, refs)
		if err != nil {
			return DirectBlockerVerdict{}, mapped(err)
		}
	}
	byID := make(map[string]driver.Record, len(blockerRows))
	for _, row := range blockerRows {
		if row.Kind == "bead" && row.Ref.IsID() && row.Ref.Branch.IsZero() {
			byID[row.Ref.ID] = row
		}
	}
	evaluation := directBlockerEvaluation{reader: reader, locate: req.Ontology,
		compiled: map[string]ontology.Effective{}, known: map[string]bool{}, available: map[string]bool{}}
	for _, link := range links {
		edge := DirectBlockerEdge{Blocker: link.To, Kind: link.Kind, TargetKind: link.TargetKind}
		if link.TargetKind == model.LinkTargetExternal || !link.To.IsID() || !link.To.Branch.IsZero() {
			edge.State, edge.Reason = DirectBlockerUnknown, "blocker reference is unresolved"
			verdict.Edges = append(verdict.Edges, edge)
			continue
		}
		row, ok := byID[link.To.ID]
		if !ok {
			edge.State, edge.Reason = DirectBlockerUnknown, "blocker is missing from the snapshot"
			verdict.Edges = append(verdict.Edges, edge)
			continue
		}
		blocker, decodeErr := decodeDocument(row.Body)
		if decodeErr != nil || blocker.ID != row.Ref.ID {
			return DirectBlockerVerdict{}, failure("internal_error", "invalid persisted blocker bead")
		}
		edge.Project = blocker.Project
		edge.Status, _ = blocker.Fields["status"].(string)
		effective, available, err := evaluation.effective(ctx, blocker.Project)
		if err != nil {
			return DirectBlockerVerdict{}, err
		}
		if !available {
			edge.State, edge.Reason = DirectBlockerUnknown, "blocker ontology is unavailable in the snapshot"
			verdict.Edges = append(verdict.Edges, edge)
			continue
		}
		if _, statusKnown := effective.Status(edge.Status); !statusKnown {
			edge.State, edge.Reason = DirectBlockerUnknown, "blocker status is unknown to its project ontology"
			verdict.Edges = append(verdict.Edges, edge)
			continue
		}
		satisfied := effective.SatisfiesBlocks(edge.Status)
		if link.Kind == "conditional-blocks" {
			satisfied = effective.SatisfiesConditionalBlocks(edge.Status)
		}
		if link.Kind == "waits-for" {
			state, err := evaluation.waitsFor(ctx, link, storedBead{row, blocker})
			if err != nil {
				return DirectBlockerVerdict{}, err
			}
			if state == DirectBlockerUnknown {
				edge.State, edge.Reason = state, "waits-for child state is unknown"
				verdict.Edges = append(verdict.Edges, edge)
				continue
			}
			satisfied = state == DirectBlockerSatisfied
		}
		if satisfied {
			edge.State = DirectBlockerSatisfied
		} else {
			edge.State = DirectBlockerUnsatisfied
			if link.Kind == "conditional-blocks" {
				edge.Reason = "blocker status does not satisfy conditional-blocks"
			} else if link.Kind == "waits-for" {
				edge.Reason = "waits-for predicate is unsatisfied"
			} else {
				edge.Reason = "blocker status does not satisfy blocks"
			}
		}
		verdict.Edges = append(verdict.Edges, edge)
	}
	return verdict, nil
}

type directBlockerEvaluation struct {
	reader    driver.Reader
	locate    OntologySourceLocator
	compiled  map[string]ontology.Effective
	known     map[string]bool
	available map[string]bool
	graph     map[string]storedBead
}

func (e *directBlockerEvaluation) effective(ctx context.Context, project string) (ontology.Effective, bool, error) {
	if !e.known[project] {
		effective, ok, err := directBlockerEffective(ctx, e.reader, e.locate, project)
		if err != nil {
			return ontology.Effective{}, false, err
		}
		e.compiled[project], e.available[project], e.known[project] = effective, ok, true
	}
	return e.compiled[project], e.available[project], nil
}

func (e *directBlockerEvaluation) waitsFor(ctx context.Context, link pipeline.Link, spawner storedBead) (DirectBlockerState, error) {
	terminal := e.compiled[spawner.doc.Project].SatisfiesBlocks(state(spawner.doc).Status)
	if link.Metadata == nil && terminal {
		return DirectBlockerSatisfied, nil
	}
	if link.Metadata != nil && link.Metadata.AlsoBlocks && !terminal {
		return DirectBlockerUnsatisfied, nil
	}
	if e.graph == nil {
		// A complete paginated scan on this reader is necessary for both Parent
		// and legacy parent-child edges. A failed page cannot mean no children.
		graph, err := scanBeads(ctx, e.reader)
		if err != nil {
			return "", err
		}
		e.graph = graph
	}
	for _, child := range e.graph {
		member := child.doc.Parent != nil && child.doc.Parent.ID == spawner.doc.ID
		for _, edge := range child.doc.Links {
			member = member || edge.TargetKind != model.LinkTargetExternal && edge.Kind == "parent-child" && edge.To.ID == spawner.doc.ID
		}
		if !member {
			continue
		}
		effective, available, err := e.effective(ctx, child.doc.Project)
		if err != nil {
			return "", err
		}
		if !available {
			return DirectBlockerUnknown, nil
		}
		if _, known := effective.Status(state(child.doc).Status); !known {
			return DirectBlockerUnknown, nil
		}
	}
	// Compile every relevant child's own project before reusing the canonical
	// graph predicate. Unknown ontology/status cannot satisfy a child predicate.
	o := NewOntology(func(project string) ontology.Effective { return e.compiled[project] })
	if o.waitsForSatisfied(link, spawner, e.graph) {
		return DirectBlockerSatisfied, nil
	}
	return DirectBlockerUnsatisfied, nil
}

func directBlockerEffective(ctx context.Context, reader driver.Reader, locate OntologySourceLocator, project string) (ontology.Effective, bool, error) {
	ref, err := locate(project)
	if err != nil {
		return ontology.Effective{}, false, err
	}
	if err := ref.Validate(); err != nil || !protectedOntologyRef(ref) {
		return ontology.Effective{}, false, failure("invalid_params", "ontology source locator returned an unprotected branch-zero ID")
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return ontology.Effective{}, false, mapped(err)
	}
	if len(rows) == 0 {
		return ontology.Effective{}, false, nil
	}
	if len(rows) != 1 || rows[0].Kind != "ontology" || !sameResolvedRef(rows[0].Ref, ref) {
		return ontology.Effective{}, false, failure("invalid_params", "ontology source record has an unsupported kind or identity")
	}
	source, err := DecodeOntologySource(rows[0].Body)
	if err != nil {
		return ontology.Effective{}, false, failure("invalid_params", "unsupported protected ontology source")
	}
	snapshot, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return ontology.Effective{}, false, failure("invalid_params", "protected ontology source does not compile")
	}
	return snapshot.Effective(project), true, nil
}
