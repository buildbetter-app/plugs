package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"

	embedded "github.com/dolthub/driver/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/plugdump"
)

type messageTrap struct{}

func (*messageTrap) Error() string { panic("an error message must never be read") }

func TestSourceHistoryRefusalRetainsItsFixedCodeAndCoordinates(t *testing.T) {
	version := uint64(66)
	for _, test := range []struct{ reason, code string }{
		{"ancestry-unavailable", "ancestry_unavailable"},
		{"native-image-census-limit", "native_image_census_limit"},
		{"head-image-differs-versioned-family:public_fixture_identifier", "head_image_differs_versioned_family"},
	} {
		t.Run(test.code, func(t *testing.T) {
			report := safeReport(plugdump.ErrSourceHistoryUnavailable{
				Reason: test.reason, SchemaVersion: &version, OrdinalFromHead: 7,
			})
			if report.ReasonSubcode == nil || *report.ReasonSubcode != test.code ||
				report.CommitSchemaVersion == nil || *report.CommitSchemaVersion != version ||
				report.CommitOrdinalFromHead == nil || *report.CommitOrdinalFromHead != 7 ||
				report.HistoryBoundaryReason != "" || report.VerifiedCommitCount != nil {
				t.Fatalf("ordinary refusal lost its own diagnostic or acquired boundary credit: %+v", report)
			}
			encoded, err := json.Marshal(report)
			if err != nil || bytes.Contains(encoded, []byte("public_fixture_identifier")) {
				t.Fatal("typed reason suffix was serialized")
			}
		})
	}
}

func TestSafeCaptureFailureTaxonomy(t *testing.T) {
	version := uint64(66)
	observation := schemaObservation{FoundVersion: &version, ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "series"}
	for _, stage := range []captureStage{stageOpen, stageSchema, stageCensus, stageHistory, stageCell, stageReport} {
		t.Run(string(stage), func(t *testing.T) {
			var output bytes.Buffer
			writeSafeReport(&output, &captureFailure{stage: stage, table: "private_table_abc", column: "private_column_xyz", schema: observation,
				cause: errors.Join(&messageTrap{}, &os.PathError{Op: "open", Path: "/private/secret_path", Err: &mysql.MySQLError{Number: 1146, Message: "private_value_9007199254740993"}})})
			for _, private := range []string{"private_table_abc", "private_column_xyz", "secret_path", "private_value", "9007199254740993"} {
				if strings.Contains(output.String(), private) {
					t.Fatalf("unsafe output: %s", output.Bytes())
				}
			}
			var report safeCaptureReport
			if err := json.Unmarshal(output.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			if report.Stage != stage || report.Table != nil || report.Column != "" || !reflect.DeepEqual(report.SQLCodes, []uint16{1146}) || *report.Schema.FoundVersion != version {
				t.Fatalf("typed evidence changed: %+v", report)
			}
			if strings.Contains(output.String(), "*main.messageTrap") || report.UnrecognizedGoErrorTypeCount != 1 || !strings.Contains(output.String(), "*mysql.MySQLError") || !strings.Contains(output.String(), "*main.captureFailure") {
				t.Fatal("closed Go error evidence lost or unknown name emitted")
			}
		})
	}
}

func TestSafeCaptureIdentifiersAndPhysicalFailures(t *testing.T) {
	for _, test := range []struct{ table, column, wantTable, wantColumn string }{
		{"issues", "metadata", "issues", "metadata"},
		{"issues", "private_value", "issues", ""},
		{"private_table", "metadata", "", ""},
		{"dolt_nonlocal_tables", "ref_table", "dolt_nonlocal_tables", "ref_table"},
	} {
		t.Run(test.table+"/"+test.column, func(t *testing.T) {
			report := safeReport(&captureFailure{stage: stageCell, table: test.table, column: test.column, cause: &messageTrap{}})
			if (report.Table == nil) != (test.wantTable == "") || report.Table != nil && *report.Table != test.wantTable || report.Column != test.wantColumn {
				t.Fatalf("identifier boundary: %+v", report)
			}
		})
	}
	for _, test := range []struct {
		kind  string
		stage captureStage
	}{
		{"database", stageOpen}, {"schema", stageSchema}, {"census", stageCensus},
		{"resolve", stageHistory}, {"rows", stageCell},
	} {
		t.Run(test.kind, func(t *testing.T) {
			report := safeReport(&captureFailure{stage: stageHistory, cause: &embedded.PhysicalReadError{
				Kind: test.kind, CommitID: "private_commit_reference", Table: "issues", Cause: &mysql.MySQLError{Number: 1105, Message: "private message"},
			}})
			if report.Stage != test.stage || report.Table == nil || *report.Table != "issues" || !reflect.DeepEqual(report.SQLCodes, []uint16{1105}) {
				t.Fatalf("physical error classification: %+v", report)
			}
			data, err := json.Marshal(report)
			if err != nil || bytes.Contains(data, []byte("private")) {
				t.Fatal("physical error data leaked")
			}
		})
	}
}

func TestStructuredCaptureFailureKeepsKnownTypesNumbersAndCoordinates(t *testing.T) {
	schema := uint64(39)
	observed := uint64(66)
	cause := &plugdump.NativeCommitFailure{SchemaVersion: &schema, OrdinalFromHead: 7,
		Cause: errors.Join(&os.PathError{Op: "read", Path: "/public-fixture/source", Err: syscall.EACCES},
			&mysql.MySQLError{Number: 65535, Message: "not diagnostic data"},
			&mysql.MySQLError{Number: 1146}, &mysql.MySQLError{Number: 1146})}
	report := safeReport(&captureFailure{stage: stageCell, table: "comments", column: "text",
		schema: schemaObservation{FoundVersion: &observed, RowCountClass: "series"}, cause: cause})
	if report.Stage != stageCell || report.Table == nil || *report.Table != "comments" || report.Column != "text" ||
		report.CommitSchemaVersion == nil || *report.CommitSchemaVersion != schema ||
		report.CommitOrdinalFromHead == nil || *report.CommitOrdinalFromHead != 7 ||
		report.Schema.FoundVersion == nil || *report.Schema.FoundVersion != observed ||
		report.InnermostGoErrorType != "syscall.Errno" || report.UnrecognizedGoErrorTypeCount != 0 ||
		!reflect.DeepEqual(report.SQLCodes, []uint16{1146, 65535}) {
		t.Fatalf("structured capture evidence changed: %+v", report)
	}
	for _, name := range report.GoErrorTypes {
		if safeGoErrorType(name) == "" {
			t.Fatal("non-public Go type emitted")
		}
	}
	if report.VerifiedCommitCount != nil || report.HistoryBoundaryReason != "" || report.ReasonSubcode == nil || *report.ReasonSubcode != "other" {
		t.Fatal("ordinary read failure acquired history-boundary evidence")
	}
}

func TestStructuredCaptureUnknownClassesAreCountedOnceWithoutChangingCause(t *testing.T) {
	original := &messageTrap{}
	cause := errors.Join(original, original)
	report := safeReport(cause)
	if !errors.Is(cause, original) || report.UnrecognizedGoErrorTypeCount != 1 || report.InnermostGoErrorType != "" {
		t.Fatal("unknown class multiplicity or original cause changed")
	}
	if !reflect.DeepEqual(report.GoErrorTypes, []string{"*main.captureFailure", "*errors.joinError"}) {
		t.Fatal("known wrapper evidence changed")
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"table", "commit_schema_version", "commit_ordinal_from_head", "verified_commit_count"} {
		if !bytes.Equal(fields[name], []byte("null")) {
			t.Fatal("unobserved coordinate was omitted or invented")
		}
	}
	if bytes.Contains(data, []byte("messageTrap")) || bytes.Contains(data, []byte("UNRECOGNIZED_CODE")) {
		t.Fatal("reader emitted an unknown name or the parser-owned substitution")
	}
}
