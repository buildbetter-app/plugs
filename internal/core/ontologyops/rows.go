package ontologyops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func decodeRow(body []byte) (pipeline.Document, error) {
	// Keep the existing stored model, while refusing unsupported outer
	// relationships instead of projecting them away for graph observations.
	var doc pipeline.Document
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return doc, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return doc, fmt.Errorf("stored bead has trailing JSON")
	}
	return pipeline.DecodeDocument(body)
}

func readRows(ctx context.Context, r driver.Reader) ([]ontologymigration.Row, error) {
	rows := []ontologymigration.Row{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, mapped(err)
		}
		if r.Snapshot().At == nil || page.AsOf.String() != r.Snapshot().At.String() {
			return nil, failure("invalid_params", "bead enumeration changed storage snapshot")
		}
		for _, record := range page.Items {
			doc, err := decodeRow(record.Body)
			if err != nil || record.Kind != "bead" || !record.Ref.IsID() || !record.Ref.Branch.IsZero() || doc.ID != record.Ref.ID {
				return nil, failure("invalid_params", "unsupported or corrupt stored bead image")
			}
			status, ok := doc.Fields["status"].(string)
			if !ok || status == "" {
				return nil, failure("invalid_params", "stored bead is missing its status")
			}
			fields := make(map[string]any, len(doc.Fields))
			for k, v := range doc.Fields {
				if k != "status" {
					fields[k] = v
				}
			}
			template, _ := fields["template"].(bool)
			rows = append(rows, ontologymigration.Row{Revision: record.Revision, State: ontology.BeadState{ID: doc.ID, Project: doc.Project, Type: doc.Type, Status: status, Template: template, Fields: fields}})
		}
		if page.NextCursor == nil {
			return rows, nil
		}
		if cursor != nil && page.NextCursor.Index.String() == cursor.Index.String() {
			return nil, failure("invalid_params", "bead enumeration did not advance")
		}
		cursor = page.NextCursor
	}
}

type rowSource struct {
	s         *Service
	by        model.PrincipalRef
	r         driver.Reader
	current   source
	candidate ontology.Snapshot
	graph     *storedGraph
}

type storedGraph struct {
	docs      map[string]pipeline.Document
	supported bool
}

func (r rowSource) loadGraph(ctx context.Context, rows []ontologymigration.Row) (*storedGraph, error) {
	graph := r.graph
	if graph == nil {
		graph = &storedGraph{}
	}
	if graph.docs != nil {
		return graph, nil
	}
	graph.docs, graph.supported = map[string]pipeline.Document{}, true
	for start := 0; start < len(rows); start += 1000 {
		end := min(start+1000, len(rows))
		refs := make([]model.Ref, 0, end-start)
		for _, row := range rows[start:end] {
			refs = append(refs, model.Ref{ID: row.State.ID})
		}
		records, err := r.r.Get(ctx, refs)
		if err != nil {
			return nil, mapped(err)
		}
		if len(records) != len(refs) {
			return nil, failure("invalid_params", "graph enumeration omitted an inventoried bead")
		}
		for _, record := range records {
			doc, err := decodeRow(record.Body)
			if err != nil || record.Kind != "bead" || doc.ID != record.Ref.ID {
				return nil, failure("invalid_params", "graph enumeration returned an invalid bead image")
			}
			graph.docs[doc.ID] = doc
			for _, link := range doc.Links {
				switch link.Kind {
				case "relates-to", "blocks", "conditional-blocks":
				default:
					graph.supported = false
				}
			}
		}
	}
	return graph, nil
}

func (r rowSource) read(ctx context.Context) ([]ontologymigration.Row, error) {
	rows, err := readRows(ctx, r.r)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		// A single compiled envelope must really be authoritative for every
		// project in this complete inventory. A multi-source deployment needs
		// a future coordinated planner; it cannot silently reuse this source.
		ref, err := r.s.cfg.Source(row.State.Project)
		if err != nil {
			return nil, err
		}
		if !sameRef(ref, r.current.guard.Ref) {
			return nil, failure("invalid_params", "planning across separate protected ontology sources is unsupported")
		}
		if err := r.s.authorize(ctx, r.by, model.Ref{ID: row.State.ID}, "bead", row.State.Project); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (r rowSource) Inventory(ctx context.Context) (ontologymigration.Inventory, error) {
	rows, err := r.read(ctx)
	if err != nil {
		return ontologymigration.Inventory{}, err
	}
	digest, err := ontologymigration.RowsDigest(rows)
	if err != nil {
		return ontologymigration.Inventory{}, failure("invalid_params", err.Error())
	}
	return ontologymigration.Inventory{DataRevision: *r.r.Snapshot().At, Ontology: ontologymigration.Reference{Revision: r.current.guard.Revision, Digest: r.current.guard.Digest}, Count: model.Natural64(uint64(len(rows))), RowsDigest: digest}, nil
}

// Scan enumerates storage again at the same immutable snapshot, independently
// of the inventory pass. The planner checks count, content, duplicates and pins.
func (r rowSource) Scan(ctx context.Context, inventory ontologymigration.Inventory, yield func(ontologymigration.Row) error) error {
	if inventory.DataRevision.String() != r.r.Snapshot().At.String() {
		return fmt.Errorf("migration scan snapshot differs from inventory")
	}
	rows, err := r.read(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := yield(row); err != nil {
			return err
		}
	}
	return nil
}

// Observe obtains graph/evidence facts from the actual complete pinned store.
// A configured authorizer still needs a future transition-decision adapter;
// a missing permission or gate observation is deliberately left unobserved.
func (r rowSource) Observe(ctx context.Context, inventory ontologymigration.Inventory, change ontologymigration.Change) (ontologymigration.Observation, error) {
	if inventory.DataRevision.String() != r.r.Snapshot().At.String() || change.Candidate.Digest != r.candidate.Digest() {
		return ontologymigration.Observation{}, failure("ontology_plan_stale", "guard observation snapshot or candidate changed")
	}
	graph := r.graph
	if graph == nil || graph.docs == nil {
		rows, err := r.read(ctx)
		if err != nil {
			return ontologymigration.Observation{}, err
		}
		graph, err = r.loadGraph(ctx, rows)
		if err != nil {
			return ontologymigration.Observation{}, err
		}
	}
	doc, exists := graph.docs[change.After.State.ID]
	if !exists {
		return ontologymigration.Observation{}, failure("invalid_params", "guard target image is absent")
	}
	installed := r.s.cfg.AuthorizeRead != nil
	facts := ontology.GuardFacts{AuthzInstalled: installed, Evidence: map[string]bool{}}
	for _, attachment := range doc.Attachments {
		if attachment.URI != "" || attachment.ContentRef != "" {
			facts.Evidence[attachment.Kind] = true
		}
	}
	// Explicit false observations distinguish absent evidence from an
	// unimplemented gate resolver, whose observations remain nil.
	definitions := r.candidate.Effective(doc.Project).Document()
	for _, rule := range definitions.Workflows[definitions.Types[doc.Type].Workflow].Transitions {
		for _, guard := range rule.Guards {
			if guard.Kind == ontology.EvidenceRequired {
				for _, kind := range guard.Kinds {
					if _, ok := facts.Evidence[kind]; !ok {
						facts.Evidence[kind] = false
					}
				}
			}
		}
	}
	noBlockers, graphSupported := true, graph.supported
	for _, link := range doc.Links {
		switch link.Kind {
		case "relates-to":
		case "blocks", "conditional-blocks":
			blocker, exists := graph.docs[link.To.ID]
			satisfied := false
			if link.TargetKind != model.LinkTargetExternal && exists && link.To.IsID() && link.To.Branch.IsZero() {
				e := r.candidate.Effective(blocker.Project)
				status, _ := blocker.Fields["status"].(string)
				satisfied = e.SatisfiesBlocks(status)
				if link.Kind == "conditional-blocks" {
					satisfied = e.SatisfiesConditionalBlocks(status)
				}
			}
			noBlockers = noBlockers && satisfied
		default:
			graphSupported = false
		}
	}
	// The admitted pipeline.Document has no parent member, and this read
	// slice refuses unknown members. Known link kinds cannot encode children.
	if graphSupported {
		noChildren := true
		facts.NoOpenBlockers, facts.NoOpenChildren = &noBlockers, &noChildren
	}
	rowDigest, err := ontologymigration.RowDigest(change.After)
	return ontologymigration.Observation{Candidate: change.Candidate, RowDigest: rowDigest, Facts: facts, AuthzInstalled: &installed}, err
}

// NoValidatingPre is an explicit host declaration for a deployment with no
// validating-pre callbacks. Do not use it when a plugin host has any bindings.
func NoValidatingPre() ontologymigration.ValidatingPre { return noValidatingPre{} }

type noValidatingPre struct{}

func (noValidatingPre) Inventory(ctx context.Context, candidate ontologymigration.Reference) (ontologymigration.PreInventory, error) {
	if err := ctx.Err(); err != nil {
		return ontologymigration.PreInventory{}, err
	}
	bindings := []ontologymigration.Binding{}
	digest, err := ontologymigration.BindingsDigest(candidate, bindings)
	return ontologymigration.PreInventory{Candidate: candidate, Bindings: bindings, Digest: digest}, err
}

func (noValidatingPre) Validate(context.Context, ontologymigration.Binding, ontologymigration.Change) ([]ontology.Violation, error) {
	return nil, fmt.Errorf("no validating-pre callback is bound")
}
