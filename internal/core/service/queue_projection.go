package service

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// queueReadyItems evaluates the complete thin graph before priority spreading.
// It is never a raw rotated superset or an authorized/capped Select result.
// Memory/legacy/uncertified readers retain the exact existing interpreter.
// readinessVisit adds work only for the queue interpreter; existing guard and
// ordinary Ready callers supply no context and retain their accounting.
func readinessVisit(work []context.Context) {
	if len(work) != 0 && work[0] != nil {
		driver.ReportReadWork(work[0], driver.ReadSelectorKeys, 1)
	}
}

func (s *Service) queueReadyItems(ctx context.Context, req ReadyRequest, reader driver.Reader) ([]Bead, *driver.QueueProjection, error) {
	return s.queueReadyItemsWithBlocked(ctx, req, reader, model.PrincipalRef{}, nil)
}

// queueReadyItemsBy is the integration seam for the main queue caller. The
// caller evaluates workflow parent/dependency facts against the complete thin
// graph from this pinned projection. A nil callback preserves standalone core
// behavior; the principal, IDs and graph belong to the same request.
func (s *Service) queueReadyItemsBy(ctx context.Context, by model.PrincipalRef, req ReadyRequest, reader driver.Reader, ordinaryBlocked func(context.Context, model.PrincipalRef, []string, map[string]storedBead) (map[string]bool, error)) ([]Bead, *driver.QueueProjection, error) {
	return s.queueReadyItemsWithBlocked(ctx, req, reader, by, ordinaryBlocked)
}

func (s *Service) queueReadyItemsWithBlocked(ctx context.Context, req ReadyRequest, reader driver.Reader, by model.PrincipalRef, ordinaryBlocked func(context.Context, model.PrincipalRef, []string, map[string]storedBead) (map[string]bool, error)) ([]Bead, *driver.QueueProjection, error) {
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
	// Hybrid ordering and workflow membership both require their typed source
	// inputs. A legacy/partial annotation stays on the exact graph fallback;
	// this path never invents a creation instant or silently treats an unknown
	// block state as false.
	for _, source := range projection.Beads {
		if source.Facts.CreatedAt.IsZero() {
			return nil, nil, nil
		}
	}
	graph := make(map[string]storedBead, len(projection.Beads))
	sources := make(map[string]driver.QueueBead, len(projection.Beads))
	for _, source := range projection.Beads {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		b := source.Bead
		fields := map[string]any{"status": b.Status, "title": b.Title, "template": b.Template, "priority": json.Number(strconv.FormatInt(b.Priority, 10))}
		for _, absent := range b.Absent {
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
			delete(fields, string(absent))
		}
		doc := pipeline.Document{ID: b.Ref.ID, Type: b.Type, Fields: fields, Parent: b.Parent}
		if b.Project != nil {
			doc.Project = *b.Project
		}
		for _, link := range source.Facts.Links {
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
			decoded, err := queueDocumentLink(link)
			if err != nil {
				return nil, nil, failure("internal_error", "invalid persisted queue link")
			}
			doc.Links = append(doc.Links, decoded)
		}
		createdAt := source.Facts.CreatedAt.UTC()
		doc.CreatedAt = &createdAt
		if source.Facts.Blocked {
			doc.Fields["blocked"] = true
		}
		graph[doc.ID] = storedBead{record: driver.Record{Ref: b.Ref, Kind: "bead", Revision: source.Source.Revision}, doc: doc}
		sources[doc.ID] = source
	}
	// Reuse the existing readiness/dependency interpreter over the full graph.
	// Only eligible lease-bearing images need actual checkpoint/clock evaluation.
	// Every such Get is counted, even if it later excludes the bead.
	ids := make([]string, 0, len(graph))
	for id := range graph {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		return ids[i] < ids[j]
	})
	blocked := map[string]bool{}
	if ordinaryBlocked != nil {
		var err error
		blocked, err = ordinaryBlocked(ctx, by, append([]string(nil), ids...), graph)
		if err != nil {
			return nil, nil, err
		}
	}
	priorities := map[string]int{}
	for _, id := range ids {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		p, err := priority(graph[id].doc)
		if err != nil {
			return nil, nil, err
		}
		priorities[id] = p
	}
	now := s.now()
	// Match the main hybrid ReadySortKey rule: recent rows keep their ontology
	// rank, older rows share the deferred bucket, then creation time and ID are
	// deterministic tie breakers.
	order := map[string]queueReadyOrderKey{}
	for _, id := range ids {
		createdAt := graph[id].doc.CreatedAt
		if createdAt == nil || createdAt.IsZero() {
			return nil, nil, nil
		}
		order[id] = queueReadyOrderKeyFor(*createdAt, now, priorities[id])
	}
	sort.SliceStable(ids, func(i, j int) bool {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		a, b := order[ids[i]], order[ids[j]]
		if a.Bucket != b.Bucket {
			return a.Bucket < b.Bucket
		}
		if a.Rank != b.Rank {
			return a.Rank < b.Rank
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return ids[i] < ids[j]
	})
	items := []Bead{}
	for _, id := range ids {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		row := graph[id]
		st := state(row.doc)
		if !matchesFilter(row, req.Project, req.Filter) || st.Template || sources[id].Facts.Blocked || blocked[id] || !s.ontology.effective(st.Project).ReadyEligible(st.Type, st.Status) {
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
		ready, err := s.ontology.readyDependencies(row.doc, graph, ctx)
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

type queueReadyOrderKey struct {
	Bucket    int
	Rank      int
	CreatedAt time.Time
}

func queueReadyOrderKeyFor(createdAt, at time.Time, priority int) queueReadyOrderKey {
	key := queueReadyOrderKey{CreatedAt: createdAt.UTC()}
	if createdAt.Before(at.Add(-48 * time.Hour)) {
		key.Bucket, key.Rank = 1, 999
	} else {
		key.Rank = priority
	}
	return key
}

// queueDocumentLink round-trips the trusted queue annotation through the
// pipeline link codec. The W3 branch has no target-kind member yet; the main
// composition does, and this keeps that field when the destination struct
// advertises it without changing the legacy link schema here.
func queueDocumentLink(link driver.QueueLink) (pipeline.Link, error) {
	wire := struct {
		TargetKind string              `json:"targetKind,omitempty"`
		To         model.Ref           `json:"to"`
		Kind       string              `json:"kind"`
		Metadata   *model.LinkMetadata `json:"metadata,omitempty"`
	}{TargetKind: link.TargetKind, To: link.To, Kind: link.Kind, Metadata: link.Metadata}
	raw, err := json.Marshal(wire)
	if err != nil {
		return pipeline.Link{}, err
	}
	var decoded pipeline.Link
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return pipeline.Link{}, err
	}
	return decoded, nil
}

func queueImages(ctx context.Context, reader driver.QueueProjectionReader, projection *driver.QueueProjection, sources []driver.QueueBead) ([]storedBead, error) {
	refs := make([]model.Ref, len(sources))
	for i, source := range sources {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
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
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
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
	items, keys, projection, err := s.queueReadyCandidates(ctx, by, req, r)
	if err != nil {
		return BeadPage{}, err
	}
	includeEphemeral := true
	if req.Filter != nil && req.Filter.IncludeEphemeral != nil {
		includeEphemeral = *req.Filter.IncludeEphemeral
	}
	if provider := s.workflowProvider(); provider != nil && includeEphemeral {
		active, err := provider.ActiveIDsFor(ctx, by, workflowMeta(by, "bead.ready", req.Project, req.Agent))
		if err != nil {
			return BeadPage{}, err
		}
		filtered := items[:0]
		for _, item := range items {
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
			if _, collides := active[item.ID]; !collides {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	if restartOnRevision && req.ClaimNextSeed != "" {
		items, err = spreadClaimNextItemsObserved(ctx, items, req.ClaimNextSeed, keys)
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
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
			byID[source.Bead.Ref.ID] = source
		}
		sources := make([]driver.QueueBead, len(page.Items))
		for i, bead := range page.Items {
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
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
