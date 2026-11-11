package embedded

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

func TestPhysicalHistoryBoundaryRequiresPositiveMissingStorageEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, kind, want string
		err              error
	}{
		{"actual-ghost", "resolve", "ghost_parent", doltdb.ErrGhostCommitEncountered},
		{"actual-root", "root", "missing_root_chunk", doltdb.ErrHashNotFound},
		{"actual-table", "table", "missing_table_chunk", fmt.Errorf("wrapped: %w", fs.ErrNotExist)},
		{"actual-schema", "schema", "unreadable_schema", doltdb.ErrHashNotFound},
		{"cursor-row-chunk-absent", "schema", "unreadable_schema", &storedChunkAbsent{reason: "missing_table_chunk"}},
		{"actual-archive-file", "root", "missing_archive_chunk", &os.PathError{Op: "open", Path: "/public/file.darc", Err: fs.ErrNotExist}},
		{"archive-permission", "root", "", &os.PathError{Op: "open", Path: "/public/file.darc", Err: fs.ErrPermission}},
		{"archive-text-only", "root", "", errors.New("missing /public/file.darc")},
		{"archive-joined-corrupt", "root", "", errors.Join(&os.PathError{Op: "open", Path: "/public/file.darc", Err: fs.ErrNotExist}, errors.New("corrupt present bytes"))},
		{"text-only", "rows", "", errors.New("missing table chunk")},
		{"corrupt", "schema", "", errors.New("invalid stored schema")},
		{"wrong-surface", "permission", "", fs.ErrNotExist},
		{"cancelled", "root", "", context.Canceled},
		{"deadline", "root", "", context.DeadlineExceeded},
		{"joined-corrupt", "root", "", errors.Join(doltdb.ErrHashNotFound, errors.New("corrupt present rows"))},
		{"joined-cancelled", "root", "", errors.Join(doltdb.ErrHashNotFound, context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := missingPhysicalHistory(tc.kind, tc.err); got != tc.want {
				t.Fatalf("reason=%s want=%s", got, tc.want)
			}
		})
	}
	missing := physicalReadError("root", "public", "", doltdb.ErrHashNotFound)
	if PhysicalHistoryUnavailableReason(errors.Join(missing)) != "missing_root_chunk" || PhysicalHistoryUnavailableReason(errors.Join(missing, errors.New("close failure"))) != "" {
		t.Fatal("cleanup/integrity failure became a successful boundary")
	}
	if len(PhysicalHistoryUnavailableCodes(missing)) != 0 {
		t.Fatal("translated generic SQL code invented")
	}
}

func TestPhysicalHistoryErrorEvidenceKeepsOriginalNumbersAndInnermostTypes(t *testing.T) {
	err := physicalReadError("root", "public", "", &storedChunkAbsent{reason: "missing_root_chunk"})
	var evidence *physicalErrorEvidence
	if !errors.As(err, &evidence) || len(evidence.SafeSQLCodes()) != 0 || evidence.SafeInnermostGoErrorType() != "*embedded.storedChunkAbsent" {
		t.Fatal("original absent-root evidence erased or synthetic SQL code invented")
	}
	if len(evidence.SafeGoErrorTypes()) != 1 {
		t.Fatal("original absent-root type census changed")
	}
}
