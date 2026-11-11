package plugdump

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

// ReadSourceCommits authenticates stored parent edges and every admitted image.
// SQL metadata stays exact on complete histories. SQL census failure alone is
// never sufficient to admit an archive with a boundary.
func (s OwnVersionCommitSource) ReadSourceCommits(ctx context.Context) (result SourceCommitLog, resultErr error) {
	if s.PhysicalDB == nil || s.SQLSource.DB == nil {
		return result, ErrSourceHistoryUnavailable{Reason: "missing-scoped-reader"}
	}
	head, err := s.historyTable(ctx, "SELECT DOLT_HASHOF('HEAD')")
	if err != nil {
		return result, err
	}
	if len(head.Rows) != 1 || len(head.Columns) != 1 {
		return result, ErrSourceHistoryUnavailable{Reason: "invalid-head"}
	}
	result.Head = historyText(head.Rows[0][head.Columns[0]])
	if !validCommitID(result.Head) {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "invalid-head"}
	}
	status, err := s.historyTable(ctx, "SELECT table_name, staged, status FROM dolt_status")
	if err != nil {
		return SourceCommitLog{}, err
	}
	result.Clean = len(status.Rows) == 0
	sqlLog, sqlErr := s.SQLSource.ReadSourceCommits(ctx)
	conn, err := s.PhysicalDB.Conn(ctx)
	if err != nil {
		return SourceCommitLog{}, err
	}
	physical, readErr := physicalcapture.ReadHistory(ctx, conn, result.Head)
	if err := errors.Join(readErr, conn.Close()); err != nil {
		return SourceCommitLog{}, locatedCommitFailure(err, 1)
	}
	if !physical.ReadOnlyScope || physical.Head != result.Head {
		return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "unauthenticated-history"}
	}
	all := map[string]SourceCommit{}
	for _, c := range physical.Commits {
		metadata := Row{}
		for key, value := range c.Metadata {
			v, err := sqlValue(value, "")
			if err != nil {
				return SourceCommitLog{}, err
			}
			metadata[key] = v
		}
		all[c.ID] = SourceCommit{ID: c.ID, Parents: c.Parents, Metadata: metadata}
	}
	// Preserve the original SQL metadata and refuse a contradictory parent edge.
	if sqlErr == nil {
		if sqlLog.Head != result.Head || sqlLog.Clean != result.Clean {
			return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
		}
		seen := map[string]bool{}
		for _, c := range sqlLog.Commits {
			if seen[c.ID] {
				return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "duplicate-commit"}
			}
			seen[c.ID] = true
			if observed, present := all[c.ID]; present {
				if !equalJSON(observed.Parents, c.Parents) {
					return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "contradictory-physical-ancestry"}
				}
				all[c.ID] = c
			}
		}
		for id := range all {
			if !seen[id] {
				return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "contradictory-physical-census"}
			}
		}
		if len(physical.Boundaries) == 0 && len(sqlLog.Commits) != len(all) {
			return SourceCommitLog{}, ErrSourceHistoryUnavailable{Reason: "contradictory-physical-census"}
		}
	}
	initial := SourceCommitLog{Head: result.Head, Clean: result.Clean, Commits: []SourceCommit{}}
	for _, c := range all {
		initial.Commits = append(initial.Commits, c)
	}
	frontier := map[string]NativeHistoryBoundary{}
	for _, b := range physical.Boundaries {
		boundary := NativeHistoryBoundary{CommitID: b.CommitID, Reason: b.Reason, SQLDoltCodes: b.SQLCodes}
		frontier[b.CommitID] = boundary
		initial.Boundaries = append(initial.Boundaries, boundary)
	}
	if len(initial.Boundaries) > 0 {
		if err := locateHistoryBoundaries(&initial); err != nil {
			return SourceCommitLog{}, err
		}
	}
	order, err := historyCommitOrder(initial)
	if err != nil {
		return SourceCommitLog{}, err
	}
	ordinals := map[string]uint64{}
	if len(initial.Boundaries) > 0 {
		_, ordinals, err = boundaryCommitOrder(initial, true)
	} else {
		for i, c := range order {
			ordinals[c.ID] = uint64(len(order) - i)
		}
	}
	if err != nil {
		return SourceCommitLog{}, err
	}
	verified := map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if verified[id] {
			return nil
		}
		if _, missing := frontier[id]; missing {
			return nil
		}
		c, present := all[id]
		if !present {
			return ErrSourceHistoryUnavailable{Reason: "unobserved-parent"}
		}
		_, err := s.ReadNativeSourceImage(ctx, id)
		if err != nil {
			reason := physicalcapture.UnavailableReason(err)
			if reason == "" || id == result.Head {
				return locatedCommitFailure(err, ordinals[id])
			}
			var failure *NativeCommitFailure
			errors.As(err, &failure)
			boundary := NativeHistoryBoundary{CommitID: id, Reason: reason, SQLDoltCodes: physicalcapture.UnavailableSQLCodes(err)}
			if failure != nil {
				boundary.CommitSchemaVersion = failure.SchemaVersion
			}
			frontier[id] = boundary
			return nil
		}
		verified[id] = true
		for _, p := range c.Parents {
			if err := visit(p); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(result.Head); err != nil {
		return SourceCommitLog{}, err
	}
	result.Commits = []SourceCommit{}
	usedFrontier := map[string]bool{}
	for id := range verified {
		c := all[id]
		result.Commits = append(result.Commits, c)
		for _, p := range c.Parents {
			if _, missing := frontier[p]; missing {
				usedFrontier[p] = true
			}
		}
	}
	for id := range usedFrontier {
		result.Boundaries = append(result.Boundaries, frontier[id])
	}
	sort.Slice(result.Commits, func(i, j int) bool { return result.Commits[i].ID < result.Commits[j].ID })
	if len(result.Boundaries) > 0 {
		if err := locateHistoryBoundaries(&result); err != nil {
			return SourceCommitLog{}, err
		}
	} else if sqlErr != nil {
		return SourceCommitLog{}, sqlErr
	}
	if sqlErr != nil {
		var unavailable ErrSourceHistoryUnavailable
		if !errors.As(sqlErr, &unavailable) || unavailable.Reason != "ancestry-unavailable" && unavailable.Reason != "commit-census-unavailable" {
			return SourceCommitLog{}, sqlErr
		}
	}
	return result, nil
}

func locatedCommitFailure(err error, ordinal uint64) error {
	var failure *NativeCommitFailure
	errors.As(err, &failure)
	if reason := physicalcapture.UnavailableReason(err); reason != "" {
		var version *uint64
		if failure != nil {
			version = failure.SchemaVersion
		}
		return errors.Join(ErrSourceHistoryUnavailable{Reason: "native-history-head-unavailable", BoundaryReason: reason, SchemaVersion: version, OrdinalFromHead: ordinal, SQLCodes: physicalcapture.UnavailableSQLCodes(err)}, err)
	}
	if failure != nil {
		return &NativeCommitFailure{SchemaVersion: failure.SchemaVersion, OrdinalFromHead: ordinal, Cause: err}
	}
	return fmt.Errorf("native physical history: %w", err)
}
