package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

type captureStage string

const (
	stageOpen    captureStage = "archive-read"
	stageSchema  captureStage = "schema"
	stageCensus  captureStage = "table-scan"
	stageHistory captureStage = "history-walk"
	stageCell    captureStage = "row-decode"
	stageReport  captureStage = "report"
	stageProbe                = stageHistory
)

type schemaObservation struct {
	FoundVersion    *uint64 `json:"found_version,omitempty"`
	ExpectedVersion uint64  `json:"expected_version"`
	RowCountClass   string  `json:"row_count_class"`
}

type captureFailure struct {
	stage         captureStage
	table, column string
	schema        schemaObservation
	cause         error
}

func (*captureFailure) Error() string   { return "native capture failed" }
func (e *captureFailure) Unwrap() error { return e.cause }

type unsupportedCaptureSchema struct{}

func (*unsupportedCaptureSchema) Error() string { return "capture schema unsupported" }

// Only fixed schema identifiers are selected, before inventory/history reads.
// Missing tables are errors, never synthesized version zero or empty data.
func probeSchema(ctx context.Context, conn *sql.Conn) (schemaObservation, error) {
	observation := schemaObservation{ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "unmeasured"}
	var count, version uint64
	err := conn.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&count, &version)
	if err != nil {
		return observation, err
	}
	observation.FoundVersion = &version
	switch count {
	case 0:
		observation.RowCountClass = "empty"
	case 1:
		observation.RowCountClass = "single"
	default:
		observation.RowCountClass = "series"
	}
	return observation, nil
}

type safeCaptureReport struct {
	ReasonSubcode                *string           `json:"reason_subcode"`
	HistoryBoundaryReason        string            `json:"history_boundary_reason,omitempty"`
	VerifiedCommitCount          *uint64           `json:"verified_commit_count"`
	InnermostGoErrorType         string            `json:"innermost_go_error_type,omitempty"`
	CommitSchemaVersion          *uint64           `json:"commit_schema_version"`
	CommitOrdinalFromHead        *uint64           `json:"commit_ordinal_from_head"`
	Format                       string            `json:"format"`
	Stage                        captureStage      `json:"stage"`
	GoErrorTypes                 []string          `json:"go_error_types"`
	UnrecognizedGoErrorTypeCount uint64            `json:"unrecognized_go_error_type_count"`
	SQLCodes                     []uint16          `json:"sql_dolt_codes"`
	Schema                       schemaObservation `json:"schema_migrations"`
	Table                        *string           `json:"table"`
	Column                       string            `json:"column,omitempty"`
}

// Never call Error, format a value, parse a message or serialize an error.
// Unwrap only to retain Go type names and the driver's typed numeric SQL code.
func safeReport(err error) safeCaptureReport {
	report := safeCaptureReport{Format: "beads-capture-diagnostic/1", Stage: stageReport,
		GoErrorTypes: []string{}, SQLCodes: []uint16{},
		Schema: schemaObservation{ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "unmeasured"}}
	var failure *captureFailure
	// Every failure has a genuine known wrapper even when its original class
	// is not in the public type vocabulary. The original cause is unchanged.
	if err != nil && !errors.As(err, &failure) {
		err = &captureFailure{stage: stageReport, schema: report.Schema, cause: err}
	}
	if errors.As(err, &failure) {
		report.Stage, report.Schema = safeStage(failure.stage), safeSchemaObservation(failure.schema)
		table, column := plugdump.PublicCaptureIdentifiers(failure.table, failure.column)
		if table != "" {
			report.Table, report.Column = &table, column
		}
	}
	if stage, table := physicalcapture.FailureLocation(err); stage != "" {
		report.Stage = safeStage(captureStage(stage))
		if table, _ := plugdump.PublicCaptureIdentifiers(table, ""); table != "" {
			report.Table, report.Column = &table, ""
		}
	}
	var commitFailure *plugdump.NativeCommitFailure
	if errors.As(err, &commitFailure) {
		report.CommitSchemaVersion = commitFailure.SchemaVersion
		if commitFailure.OrdinalFromHead != 0 {
			ordinal := commitFailure.OrdinalFromHead
			report.CommitOrdinalFromHead = &ordinal
		}
	}
	deepestLeaf := -1
	seenTypes, seenCodes := map[string]bool{}, map[uint16]bool{}
	unknownTypes := map[string]bool{}
	addType := func(name string) string {
		if name == "" {
			return ""
		}
		known := safeGoErrorType(name)
		if known == "" {
			if !unknownTypes[name] {
				unknownTypes[name] = true
				report.UnrecognizedGoErrorTypeCount++
			}
		} else if !seenTypes[known] {
			report.GoErrorTypes = append(report.GoErrorTypes, known)
			seenTypes[known] = true
		}
		return known
	}
	var walk func(error, int)
	walk = func(cause error, depth int) {
		if cause == nil {
			return
		}
		if depth > 32 {
			// A truncated branch has no observed deepest leaf. Do not report a
			// shallower sibling as the original innermost error class.
			deepestLeaf, report.InnermostGoErrorType = 33, ""
			return
		}
		name := addType(reflect.TypeOf(cause).String())
		// The driver snapshots original types/codes before translating errors.
		// Do not walk the translated fallback, whose generic 1105 is not an
		// observed SQL/Dolt number from the source.
		if evidence, ok := cause.(interface {
			SafeGoErrorTypes() []string
			SafeSQLCodes() []uint16
		}); ok {
			for _, original := range evidence.SafeGoErrorTypes() {
				addType(original)
			}
			if depth+1 > deepestLeaf {
				deepestLeaf = depth + 1
				report.InnermostGoErrorType = ""
				if original, ok := cause.(interface{ SafeInnermostGoErrorType() string }); ok {
					report.InnermostGoErrorType = addType(original.SafeInnermostGoErrorType())
				}
			}
			for _, code := range evidence.SafeSQLCodes() {
				if code != 0 && !seenCodes[code] {
					report.SQLCodes = append(report.SQLCodes, code)
					seenCodes[code] = true
				}
			}
			return
		}
		if sqlError, ok := cause.(*mysql.MySQLError); ok && sqlError.Number != 0 && !seenCodes[sqlError.Number] {
			report.SQLCodes = append(report.SQLCodes, sqlError.Number)
			seenCodes[sqlError.Number] = true
		}
		if numbered, ok := cause.(interface{ Number() int }); ok {
			if code := numbered.Number(); code > 0 && code <= 65535 && !seenCodes[uint16(code)] {
				report.SQLCodes = append(report.SQLCodes, uint16(code))
				seenCodes[uint16(code)] = true
			}
		}
		hasChild := false
		switch wrapped := cause.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				if child != nil {
					hasChild = true
					walk(child, depth+1)
				}
			}
		case interface{ Unwrap() error }:
			if child := wrapped.Unwrap(); child != nil {
				hasChild = true
				walk(child, depth+1)
			}
		}
		if !hasChild && depth > deepestLeaf {
			deepestLeaf = depth
			report.InnermostGoErrorType = name
		}
	}
	walk(err, 0)
	var boundary plugdump.ErrSourceHistoryUnavailable
	var boundaryPointer *plugdump.ErrSourceHistoryUnavailable
	unavailable := errors.As(err, &boundary)
	if !unavailable && errors.As(err, &boundaryPointer) {
		boundary = *boundaryPointer
		unavailable = true
	}
	if unavailable {
		report.Stage = stageHistory
		reason := safeHistoryReason(boundary.BoundaryReason)
		if reason == "" {
			reason = safeHistoryReason(physicalcapture.UnavailableReason(err))
		}
		if reason == "" {
			reason = safeSourceHistoryReason(boundary.Reason)
		}
		if reason == "" {
			reason = "other"
		}
		report.ReasonSubcode = &reason
		if boundary.BoundaryReason != "" {
			report.HistoryBoundaryReason = reason
		}
		if boundary.SchemaVersion != nil {
			report.CommitSchemaVersion = boundary.SchemaVersion
		}
		if boundary.OrdinalFromHead > 0 {
			ordinal := boundary.OrdinalFromHead
			report.CommitOrdinalFromHead = &ordinal
		}
		report.VerifiedCommitCount = boundary.VerifiedCommitCount
		for _, code := range boundary.SQLCodes {
			if code != 0 && !seenCodes[code] {
				report.SQLCodes = append(report.SQLCodes, code)
				seenCodes[code] = true
			}
		}
	} else if err != nil {
		reason := safeHistoryReason(physicalcapture.UnavailableReason(err))
		if physicalcapture.IsInconsistentStore(err) {
			reason = "inconsistent_store"
		} else if reason == "" {
			reason = "other"
		}
		report.ReasonSubcode = &reason
	}
	sort.Slice(report.SQLCodes, func(i, j int) bool { return report.SQLCodes[i] < report.SQLCodes[j] })
	return report
}

func safeHistoryReason(reason string) string {
	switch reason {
	case "ghost_parent", "missing_root_chunk", "missing_table_chunk", "missing_archive_chunk", "unreadable_schema", "other":
		return reason
	}
	return ""
}

func writeSafeReport(output io.Writer, err error) {
	// Encoding this closed report has no data-dependent marshaler. If the
	// destination is unavailable, never fall back to printing the original error.
	_ = json.NewEncoder(output).Encode(safeReport(err))
}

// Stage current SQL reads without parsing error text. NativeInventory performs
// the same reads again in the existing transaction; the preflight localizes a
// failed census or cell read before its higher-level validation runs.
func preflightCurrent(ctx context.Context, source plugdump.SQLSource, observation schemaObservation) error {
	names, err := source.Tables(ctx)
	if err != nil {
		return &captureFailure{stage: stageCensus, schema: observation, cause: err}
	}
	for _, name := range names {
		allowed, _ := plugdump.PublicCaptureIdentifiers(name, "")
		if allowed == "" {
			return &captureFailure{stage: stageCensus, schema: observation, cause: errors.New("unregistered source family")}
		}
		if _, err := source.ReadTable(ctx, name); err != nil {
			return &captureFailure{stage: stageCell, table: name, schema: observation, cause: err}
		}
	}
	return nil
}

type observedNativeSource struct {
	plugdump.OwnVersionCommitSource
	schema schemaObservation
}

func (s observedNativeSource) Tables(ctx context.Context) ([]string, error) {
	names, err := s.SQLSource.Tables(ctx)
	return names, s.failure(stageCensus, "", err)
}

func (s observedNativeSource) ReadTable(ctx context.Context, name string) (plugdump.Table, error) {
	table, err := s.SQLSource.ReadTable(ctx, name)
	return table, s.failure(stageCell, name, err)
}

func (s observedNativeSource) ReadSourceCommits(ctx context.Context) (plugdump.SourceCommitLog, error) {
	log, err := s.OwnVersionCommitSource.ReadSourceCommits(ctx)
	return log, s.failure(stageHistory, "", err)
}

func (s observedNativeSource) ReadSourceIgnore(ctx context.Context) (plugdump.Table, error) {
	table, err := s.SQLSource.ReadSourceIgnore(ctx)
	return table, s.failure(stageCensus, "dolt_ignore", err)
}

func (s observedNativeSource) ReadNativeSourceImage(ctx context.Context, id string) (map[string]plugdump.Table, error) {
	image, err := s.OwnVersionCommitSource.ReadNativeSourceImage(ctx, id)
	return image, s.failure(stageHistory, "", err)
}

func (s observedNativeSource) failure(stage captureStage, table string, err error) error {
	if err == nil {
		return nil
	}
	return &captureFailure{stage: stage, table: table, schema: s.schema, cause: err}
}
