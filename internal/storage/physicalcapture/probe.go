package physicalcapture

import (
	"context"
	"database/sql"
	"errors"

	embedded "github.com/dolthub/driver/v2"
)

// HistoryProbe contains detached presence observations. The first missing ID
// and original cause stay internal to the history reader; the CLI explicitly
// emits only counts, safe classification and schema/ordinal coordinates.
// Nil layout counts are unmeasured.
type HistoryProbe struct {
	ReachableCommitCount               uint64
	ReachableCountIsLowerBound         bool
	ReadableRootCount                  uint64
	ChunkCompleteCommitCount           uint64
	FirstUnreadableOrdinal             *uint64
	FirstUnreadableSchemaVersion       *uint64
	ShallowGhostMarkerCount            uint64
	ArchiveChunkCount, LooseChunkCount *uint64
	ArchiveFileCount, LooseFileCount   *uint64
	ReasonSubcode                      *string
	GoErrorTypes                       []string
	SQLDoltCodes                       []uint16
	FirstUnreadableCommitID            string `json:"-"`
	FirstUnreadableCause               error  `json:"-"`
}

func ProbeHistory(ctx context.Context, conn *sql.Conn, head string) (HistoryProbe, error) {
	observed, err := embedded.ReadPhysicalHistoryProbe(ctx, conn, head)
	if err != nil {
		return HistoryProbe{}, err
	}
	if !observed.ReadOnlyScope {
		return HistoryProbe{}, errors.New("history probe read-only scope not authenticated")
	}
	return HistoryProbe{
		ReachableCommitCount:         observed.ReachableCommitCount,
		ReachableCountIsLowerBound:   observed.ReachableCountIsLowerBound,
		ReadableRootCount:            observed.ReadableRootCount,
		ChunkCompleteCommitCount:     observed.ChunkCompleteCommitCount,
		FirstUnreadableOrdinal:       observed.FirstUnreadableOrdinal,
		FirstUnreadableSchemaVersion: observed.FirstUnreadableSchemaVersion,
		ShallowGhostMarkerCount:      observed.ShallowGhostMarkerCount,
		ArchiveChunkCount:            observed.ArchiveChunkCount, LooseChunkCount: observed.LooseChunkCount,
		ArchiveFileCount: observed.ArchiveFileCount, LooseFileCount: observed.LooseFileCount,
		ReasonSubcode: observed.ReasonSubcode, GoErrorTypes: observed.GoErrorTypes, SQLDoltCodes: observed.SQLDoltCodes,
		FirstUnreadableCommitID: observed.FirstUnreadableCommitID, FirstUnreadableCause: observed.FirstUnreadableCause,
	}, nil
}
