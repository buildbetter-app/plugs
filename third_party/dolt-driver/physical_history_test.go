package embedded

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
