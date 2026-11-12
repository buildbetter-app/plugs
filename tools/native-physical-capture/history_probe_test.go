package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	embedded "github.com/dolthub/driver/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/plugdump"
)

func TestSafeUnavailableHistoryRetainsCauseAndExplicitUnknownCoordinates(t *testing.T) {
	for _, test := range []struct {
		name, reason, boundaryReason, want string
	}{
		{"missing-counter-parent", "child-counter-parent-unresolved", "", "child_counter_parent_unresolved"},
		{"", "commit-census-unavailable", "", "commit_census_unavailable"},
		{"ghost_parent", "commit-census-unavailable", "ghost_parent", "ghost_parent"},
		{"missing_root_chunk", "commit-census-unavailable", "missing_root_chunk", "missing_root_chunk"},
		{"missing_table_chunk", "commit-census-unavailable", "missing_table_chunk", "missing_table_chunk"},
		{"missing_archive_chunk", "commit-census-unavailable", "missing_archive_chunk", "missing_archive_chunk"},
		{"unreadable_schema", "commit-census-unavailable", "unreadable_schema", "unreadable_schema"},
		{"other", "commit-census-unavailable", "other", "other"},
		{"private-error-text", "commit-census-unavailable", "private-error-text", "commit_census_unavailable"},
		{"empty-source-and-boundary", "", "", "other"},
		{"unknown-source-and-boundary", "private-source-reason", "private-boundary-reason", "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cause := errors.Join(&messageTrap{}, &mysql.MySQLError{Number: 1146, Message: "private-value"})
			err := plugdump.ErrSourceHistoryUnavailable{Reason: test.reason, BoundaryReason: test.boundaryReason, Cause: cause}
			if !errors.Is(err, cause) {
				t.Fatal("source-history wrapper discarded its cause")
			}
			data, encodeErr := json.Marshal(safeReport(err))
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"commit_schema_version", "commit_ordinal_from_head", "verified_commit_count"} {
				if !bytes.Equal(fields[key], []byte("null")) {
					t.Fatal("unobserved history coordinate omitted or invented")
				}
			}
			for _, key := range []string{"go_error_types", "sql_dolt_codes"} {
				if !bytes.HasPrefix(fields[key], []byte("[")) {
					t.Fatal("history type/code array omitted or null")
				}
			}
			var got string
			if err := json.Unmarshal(fields["reason_subcode"], &got); err != nil || got != test.want {
				t.Fatal("history reason is not a closed safe subcode")
			}
			if bytes.Contains(data, []byte("private-")) {
				t.Fatal("private cause/reason content escaped safe diagnostics")
			}
			report := safeReport(&err)
			if report.ReasonSubcode == nil || *report.ReasonSubcode != test.want || !reflect.DeepEqual(report.SQLCodes, []uint16{1146}) {
				t.Fatal("pointer unavailable wrapper lost original cause/code")
			}
		})
	}
}

func TestSafeUnavailableHistoryDriverEvidenceDoesNotInventGenericSQLCode(t *testing.T) {
	// The actual missing-root fixture exercises the driver's original evidence.
	// This verifies that other consumers cannot turn its translated fallback
	// into a source SQL number in the safe report.
	err := &embedded.PhysicalHistoryUnavailable{Reason: "missing_archive_chunk", Cause: &messageTrap{}}
	report := safeReport(plugdump.ErrSourceHistoryUnavailable{Reason: "native-history-unavailable", Cause: err})
	if report.ReasonSubcode == nil || *report.ReasonSubcode != "missing_archive_chunk" || len(report.SQLCodes) != 0 || report.InnermostGoErrorType != "" || report.UnrecognizedGoErrorTypeCount != 1 {
		t.Fatal("typed archive absence or omitted class count lost")
	}
}

func TestSafeProbeCompleteReadFailureIsTypedRefusalWithoutBoundary(t *testing.T) {
	err := &embedded.PhysicalReadError{Kind: "inconsistent-store", Cause: &embedded.PhysicalStoreInconsistent{Cause: &messageTrap{}, PanicType: "runtime.errorString"}}
	report := safeReport(err)
	if report.ReasonSubcode == nil || *report.ReasonSubcode != "inconsistent_store" || report.HistoryBoundaryReason != "" || report.CommitOrdinalFromHead != nil || report.VerifiedCommitCount != nil || report.Stage != stageCell {
		t.Fatal("probe-complete refusal was given a history boundary or invented coordinates")
	}
	data, marshalErr := json.Marshal(report)
	if marshalErr != nil || bytes.Contains(data, []byte("private")) {
		t.Fatal("inconsistent-store diagnostic formatted a private cause")
	}
}

func TestPublicHistoryProbeDoesNotReadUserCellsOrMigrationCursor(t *testing.T) {
	ctx := context.Background()
	home := diagnosticDir(t)
	t.Setenv("HOME", home)
	t.Setenv("DOLT_ROOT_PATH", home)
	t.Setenv("DOLT_METRICS_DISABLED", "1")
	dir := diagnosticDir(t)
	db, closeDriver, err := openPublicMigrationFixture(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(); _ = closeDriver() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE public_commit_history")
	ownVersionExec(t, ctx, conn, "USE public_commit_history")
	ownVersionExec(t, ctx, conn, "CREATE TABLE unregistered_probe_only (id INT PRIMARY KEY, secret VARCHAR(32))")
	ownVersionExec(t, ctx, conn, "INSERT INTO unregistered_probe_only VALUES (1,'never_decode_this_cell')")
	commitHistoryCommit(t, ctx, conn)
	// This root has neither a Beads cursor nor a supported user-table census.
	// The count-only probe must still work without invoking those readers.
	report, err := probeHistory(ctx, conn)
	if err != nil {
		t.Fatalf("counts-only non-Beads root probe: %T", err)
	}
	if report.ReachableCommitCount < 2 || report.ReadableRootCount != report.ReachableCommitCount || report.ChunkCompleteCommitCount != report.ReachableCommitCount || report.ReachableCountIsLowerBound || report.FirstUnreadableOrdinal != nil || report.FirstUnreadableSchemaVersion != nil || report.ReasonSubcode != nil {
		t.Fatal("complete graph/root probe inferred unavailable history")
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("never_decode")) || bytes.Contains(data, []byte("unregistered_probe_only")) {
		t.Fatal("counts-only probe disclosed user table/cell")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || !bytes.Equal(fields["first_unreadable_schema_version"], []byte("null")) || fields["chunk_complete_commit_count"] == nil {
		t.Fatal("probe omitted explicit presence/schema observations")
	}
	if report.GoErrorTypes == nil || report.SQLDoltCodes == nil {
		t.Fatal("complete probe must emit empty arrays")
	}
	if _, err := probeSchema(ctx, conn); err == nil {
		t.Fatal("fixture unexpectedly has a migration cursor")
	}
	_, err = probeHistory(ctx, conn)
	if err != nil {
		t.Fatal(fmt.Errorf("schema read failure contaminated a fresh counts-only probe: %w", err))
	}
}
