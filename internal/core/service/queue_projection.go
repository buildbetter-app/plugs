package service

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// queueReadyItems evaluates the complete thin graph before priority spreading.
// It is never a raw rotated superset or an authorized/capped Select result.
// Memory/legacy/uncertified readers retain the exact existing interpreter.
func (s *Service) queueReadyItems(ctx context.Context, req ReadyRequest, reader driver.Reader) ([]Bead, *driver.QueueProjection, error) {
	queue, ok := reader.(driver.QueueProjectionReader)
	if !ok {
		return nil, nil, nil
	}
	projection, err := queue.QueueProjection(ctx)
	if errors.Is(err, driver.ErrSelectionUnavailable) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, mapped(err)
	}
	graph := make(map[string]storedBead, len(projection.Beads))
	sources := make(map[string]driver.QueueBead, len(projection.Beads))
	for _, source := range projection.Beads {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		b := source.Bead
		fields := map[string]any{"status": b.Status, "title": b.Title, "template": b.Template, "priority": json.Number(strconv.FormatInt(b.Priority, 10))}
		for _, absent := range b.Absent {
			delete(fields, string(absent))
		}
		doc := pipeline.Document{ID: b.Ref.ID, Type: b.Type, Fields: fields, Parent: b.Parent}
		if b.Project != nil {
			doc.Project = *b.Project
		}
		for _, link := range source.Facts.Links {
			doc.Links = append(doc.Links, pipeline.Link{Kind: link.Kind, To: link.To, Metadata: link.Metadata})
		}
		graph[doc.ID] = storedBead{record: driver.Record{Ref: b.Ref, Kind: "bead", Revision: source.Source.Revision}, doc: doc}
		sources[doc.ID] = source
	}
	// Reuse the existing readiness/dependency interpreter over the full graph.
	// Only eligible lease-bearing images need actual checkpoint/clock evaluation.
	// Every such Get is counted, even if it later excludes the bead.
	ids := sortedIDs(graph)
	priorities := map[string]int{}
	for _, id := range ids {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		p, err := priority(graph[id].doc)
		if err != nil {
			return nil, nil, err
		}
		priorities[id] = p
	}
	// The old interpreter orders by numeric priority then bytewise ID.
	sort.SliceStable(ids, func(i, j int) bool { return priorities[ids[i]] < priorities[ids[j]] })
	items := []Bead{}
	now := s.now()
	for _, id := range ids {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		row := graph[id]
		st := state(row.doc)
		if !matchesFilter(row, req.Project, req.Filter) || st.Template || !s.ontology.effective(st.Project).ReadyEligible(st.Type, st.Status) {
			continue
		}
		if sources[id].Facts.LeasePresent {
			loaded, err := queueImages(ctx, queue, &projection, []driver.QueueBead{sources[id]})
			if err != nil {
				return nil, nil, err
			}
			row = loaded[0]
			graph[id] = row
			lease, err := s.effectiveLease(ctx, id, row.doc.Lease)
			if err != nil {
				return nil, nil, err
			}
			if lease != nil && now.Before(lease.ExpiresAt) {
				continue
			}
		}
		ready, err := s.ontology.readyDependencies(row.doc, graph)
		if err != nil {
			return nil, nil, err
		}
		if !ready {
			continue
		}
		bead, err := beadValue(row)
		if err != nil {
			return nil, nil, err
		}
		items = append(items, bead)
	}
	return items, &projection, nil
}

func queueImages(ctx context.Context, reader driver.QueueProjectionReader, projection *driver.QueueProjection, sources []driver.QueueBead) ([]storedBead, error) {
	refs := make([]model.Ref, len(sources))
	for i, source := range sources {
		refs[i] = source.Bead.Ref
	}
	records, err := reader.QueueProjectionImages(ctx, projection.View, refs)
	if err != nil {
		return nil, mapped(err)
	}
	if len(records) != len(sources) {
		return nil, mapped(driver.QueueUnavailable("selected source image missing"))
	}
	result := make([]storedBead, len(records))
	for i, record := range records {
		source := sources[i]
		if !driver.SelectionSameRef(record.Ref, source.Bead.Ref) || record.Kind != "bead" || record.Revision.String() != source.Source.Revision.String() || driver.SelectionDigest(record.Body) != source.Source.Digest {
			return nil, mapped(driver.QueueUnavailable("selected source image changed"))
		}
		doc, err := decodeDocument(record.Body)
		if err != nil || doc.ID != source.Bead.Ref.ID {
			return nil, failure("internal_error", "invalid persisted bead")
		}
		result[i] = storedBead{record: record, doc: doc}
	}
	return result, nil
}

// queueReadyPage has the old live paging/restart contract. Bodies are fetched
// only after exact READY membership, priority spreading and the page cutoff.
// The complete N-thin census remains visible and is not a W7/SLO result.
func (s *Service) queueReadyPage(ctx context.Context, by model.PrincipalRef, req ReadyRequest, restartOnRevision bool) (BeadPage, error) {
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
	items, projection, err := s.queueReadyItems(ctx, req, r)
	if err != nil {
		return BeadPage{}, err
	}
	if projection == nil {
		items, err = s.readyItems(ctx, req, r)
		if err != nil {
			return BeadPage{}, err
		}
	}
	if req.ClaimNextSeed != "" {
		items, err = spreadClaimNextItems(items, req.ClaimNextSeed)
		if err != nil {
			return BeadPage{}, err
		}
	}
	view := r.Snapshot()
	page, err := pageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation}, false, restartOnRevision)
	if err != nil {
		return BeadPage{}, err
	}
	if projection != nil {
		byID := map[string]driver.QueueBead{}
		for _, source := range projection.Beads {
			byID[source.Bead.Ref.ID] = source
		}
		sources := make([]driver.QueueBead, len(page.Items))
		for i, bead := range page.Items {
			sources[i] = byID[bead.ID]
		}
		if len(sources) != 0 {
			rows, err := queueImages(ctx, r.(driver.QueueProjectionReader), projection, sources)
			if err != nil {
				return BeadPage{}, err
			}
			for i, row := range rows {
				page.Items[i], err = beadValue(row)
				if err != nil {
					return BeadPage{}, err
				}
			}
		}
	}
	if err := s.admit(ctx, by); err != nil {
		return BeadPage{}, err
	}
	return page, nil
}
