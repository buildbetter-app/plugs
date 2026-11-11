package main

import (
	"context"
	"database/sql"
	"errors"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

type historyProbeReport struct {
	Format                       string   `json:"format"`
	ReachableCommitCount         uint64   `json:"reachable_commit_count"`
	ReachableCountIsLowerBound   bool     `json:"reachable_count_is_lower_bound"`
	ReadableRootCount            uint64   `json:"readable_root_count"`
	ChunkCompleteCommitCount     uint64   `json:"chunk_complete_commit_count"`
	FirstUnreadableOrdinal       *uint64  `json:"first_unreadable_ordinal"`
	FirstUnreadableSchemaVersion *uint64  `json:"first_unreadable_schema_version"`
	ShallowGhostMarkerCount      uint64   `json:"shallow_ghost_marker_count"`
	ArchiveChunkCount            *uint64  `json:"archive_chunk_count"`
	LooseChunkCount              *uint64  `json:"loose_chunk_count"`
	ArchiveFileCount             *uint64  `json:"archive_file_count"`
	LooseFileCount               *uint64  `json:"loose_file_count"`
	ReasonSubcode                *string  `json:"reason_subcode"`
	GoErrorTypes                 []string `json:"go_error_types"`
	SQLDoltCodes                 []uint16 `json:"sql_dolt_codes"`
}

func probeHistory(ctx context.Context, conn *sql.Conn) (historyProbeReport, error) {
	var head string
	if err := conn.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&head); err != nil {
		return historyProbeReport{}, err
	}
	observed, err := physicalcapture.ProbeHistory(ctx, conn, head)
	if err != nil {
		return historyProbeReport{}, err
	}
	if observed.ReasonSubcode != nil && safeHistoryReason(*observed.ReasonSubcode) == "" {
		return historyProbeReport{}, errors.New("unregistered history probe classification")
	}
	return historyProbeReport{
		Format:                       "beads-history-completeness/1",
		ReachableCommitCount:         observed.ReachableCommitCount,
		ReachableCountIsLowerBound:   observed.ReachableCountIsLowerBound,
		ReadableRootCount:            observed.ReadableRootCount,
		ChunkCompleteCommitCount:     observed.ChunkCompleteCommitCount,
		FirstUnreadableOrdinal:       observed.FirstUnreadableOrdinal,
		FirstUnreadableSchemaVersion: observed.FirstUnreadableSchemaVersion,
		ShallowGhostMarkerCount:      observed.ShallowGhostMarkerCount,
		ArchiveChunkCount:            observed.ArchiveChunkCount, LooseChunkCount: observed.LooseChunkCount,
		ArchiveFileCount: observed.ArchiveFileCount, LooseFileCount: observed.LooseFileCount,
		ReasonSubcode: observed.ReasonSubcode,
		GoErrorTypes:  append([]string{}, observed.GoErrorTypes...), SQLDoltCodes: append([]uint16{}, observed.SQLDoltCodes...),
	}, nil
}
