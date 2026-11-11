package physicalcapture

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	embedded "github.com/dolthub/driver/v2"
)

// History contains only detached observations. Frontier commits have no image,
// metadata or invented parents. The driver authenticates actual stored edges.
type HistoryCommit struct {
	ID             string
	Parents        []string
	OrderedParents []string
	Metadata       map[string]driver.Value
}

type HistoryBoundary struct {
	CommitID, Reason string
	SQLCodes         []uint16
	Cause            error
}

type History struct {
	Head          string
	ReadOnlyScope bool
	Commits       []HistoryCommit
	Boundaries    []HistoryBoundary
}

func ReadHistory(ctx context.Context, conn *sql.Conn, head string) (History, error) {
	observed, err := embedded.ReadPhysicalHistory(ctx, conn, head)
	if err != nil {
		return History{}, err
	}
	result := History{Head: observed.Head, ReadOnlyScope: observed.ReadOnlyScope, Commits: []HistoryCommit{}, Boundaries: []HistoryBoundary{}}
	for _, commit := range observed.Commits {
		result.Commits = append(result.Commits, HistoryCommit{ID: commit.ID, Parents: commit.Parents, OrderedParents: commit.OrderedParents, Metadata: commit.Metadata})
	}
	for _, boundary := range observed.Boundaries {
		result.Boundaries = append(result.Boundaries, HistoryBoundary{CommitID: boundary.CommitID, Reason: boundary.Reason, SQLCodes: boundary.SQLCodes, Cause: boundary.Cause})
	}
	return result, nil
}

// UnavailableReason returns only positive typed missing-storage evidence, never
// a classification inferred from SQL error text or a failed arbitrary query.
func UnavailableReason(err error) string { return embedded.PhysicalHistoryUnavailableReason(err) }

func UnavailableSQLCodes(err error) []uint16 { return embedded.PhysicalHistoryUnavailableCodes(err) }

// ProbeCompleteReadFailure preserves a refusal after the caller authenticated
// presence. A later missing sentinel cannot retroactively establish a boundary.
func ProbeCompleteReadFailure(err error) error {
	return &embedded.PhysicalStoreInconsistent{Cause: err}
}

func IsInconsistentStore(err error) bool {
	var inconsistent *embedded.PhysicalStoreInconsistent
	return errors.As(err, &inconsistent)
}
