package viewops

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The bound is an integer/time arithmetic limit, not a policy on useful ages.
const maxStaleDays = 106751 // largest whole-day duration representable by time.Duration

func validateStale(filter *StaleFilter) error {
	if filter == nil {
		return nil
	}
	if filter.Days < 1 || filter.Days > maxStaleDays {
		return failure("invalid_params", "stale.days must be a positive supported integer")
	}
	if filter.Status != nil && *filter.Status != strings.TrimSpace(*filter.Status) {
		return failure("invalid_params", "stale.status must be an exact trimmed status")
	}
	return nil
}

func (s *Service) staleCutoff(filter *StaleFilter, cursor *Cursor) (*time.Time, error) {
	if filter == nil {
		return nil, nil
	}
	if cursor != nil {
		cutoff := cursor.StaleCutoff.UTC()
		return &cutoff, nil
	}
	now := time.Now().UTC()
	if s.cfg.Now != nil {
		now = s.cfg.Now().UTC()
	}
	if now.IsZero() {
		return nil, failure("invalid_params", "stale selection requires a nonzero server clock")
	}
	cutoff := now.AddDate(0, 0, -filter.Days)
	return &cutoff, nil
}

// staleCandidates receives only currently authorized ordinary candidates.
// Each version and ontology source is read from the same pinned content reader
// as the row. It neither creates a transaction nor consults process-local
// ontology, lease expiry, body fields or a reconstructed final-image time.
func (s *Service) staleCandidates(ctx context.Context, reader driver.Reader, candidates []stored, filter StaleFilter, cutoff time.Time) ([]stored, error) {
	byProject := map[string]ontology.Effective{}
	selected := make([]stored, 0, len(candidates))
	for _, row := range candidates {
		effective, ok := byProject[row.doc.Project]
		if !ok {
			var err error
			effective, err = s.staleOntology(ctx, reader, row.doc.Project)
			if err != nil {
				return nil, err
			}
			byProject[row.doc.Project] = effective
		}
		status, ok := row.doc.Fields["status"].(string)
		if !ok || status == "" {
			return nil, failure("invalid_params", "persisted bead has no status for stale selection")
		}
		category, known := effective.Category(status)
		if !known {
			continue
		}
		// A present status is an exact status selector. In particular, a
		// present empty string is not the same as omission and therefore
		// cannot fall back to the default active categories.
		if filter.Status != nil {
			if status != *filter.Status {
				continue
			}
		} else if !category.ReadyEligible() && !category.Started() {
			continue
		}
		updated, err := latestPersistedUpdate(ctx, reader, row.record)
		if err != nil {
			return nil, err
		}
		if updated.Before(cutoff) {
			selected = append(selected, row)
		}
	}
	return selected, nil
}

func (s *Service) staleOntology(ctx context.Context, reader driver.Reader, project string) (ontology.Effective, error) {
	if s.cfg.StaleOntologySource == nil {
		effective, ok := s.cfg.StaleDefaultOntology.ImmutableDefault()
		if !ok {
			return ontology.Effective{}, failure("invalid_params", "stale selection requires a pinned ontology source after ontology refresh")
		}
		return effective, nil
	}
	ref, err := s.cfg.StaleOntologySource(project)
	if err != nil {
		return ontology.Effective{}, mapped(err)
	}
	if err := ref.Validate(); err != nil || !ref.IsID() || !ref.Branch.IsZero() || ref.ID != "ontology" && !strings.HasPrefix(ref.ID, "ontology/") {
		return ontology.Effective{}, failure("invalid_params", "stale ontology locator returned an invalid protected source")
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return ontology.Effective{}, mapped(err)
	}
	if len(rows) != 1 || rows[0].Kind != "ontology" || rows[0].Ref.ID != ref.ID {
		return ontology.Effective{}, failure("invalid_params", "stale selection requires a pinned protected ontology source")
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		return ontology.Effective{}, failure("invalid_params", "stale ontology source is invalid")
	}
	snapshot, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return ontology.Effective{}, failure("invalid_params", "stale ontology source does not compile")
	}
	return snapshot.Effective(project), nil
}

func latestPersistedUpdate(ctx context.Context, reader driver.Reader, record driver.Record) (time.Time, error) {
	var cursor *driver.Cursor
	var latest *driver.Version
	for {
		page, err := reader.Versions(ctx, record.Ref, driver.PageReq{Cursor: cursor, Limit: 1000})
		if err != nil {
			return time.Time{}, mapped(err)
		}
		if reader.Snapshot().At == nil || !sameRevision(page.AsOf, *reader.Snapshot().At) {
			return time.Time{}, failure("invalid_params", "stale version page differs from the pinned snapshot")
		}
		for i := range page.Items {
			version := page.Items[i]
			latest = &version
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			return time.Time{}, failure("invalid_params", "stale version pagination did not advance")
		}
		cursor = page.NextCursor
	}
	if latest == nil || latest.Deleted || latest.CreatedAt.IsZero() || !sameRevision(latest.Revision, record.Revision) {
		return time.Time{}, failure("invalid_params", fmt.Sprintf("stale update time is unavailable for record %q", record.Ref.ID))
	}
	return latest.CreatedAt.UTC(), nil
}
