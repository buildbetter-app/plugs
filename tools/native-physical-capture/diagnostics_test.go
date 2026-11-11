package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	embedded "github.com/dolthub/driver/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/plugdump"
)

type messageTrap struct{}

func (*messageTrap) Error() string { panic("an error message must never be read") }

func TestSafeCaptureFailureTaxonomy(t *testing.T) {
	version := uint64(66)
	observation := schemaObservation{FoundVersion: &version, ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "series"}
	for _, stage := range []captureStage{stageOpen, stageSchema, stageCensus, stageHistory, stageCell} {
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
			if report.Stage != stage || report.Table != "" || report.Column != "" || !reflect.DeepEqual(report.SQLCodes, []uint16{1146}) || *report.Schema.FoundVersion != version {
				t.Fatalf("typed evidence changed: %+v", report)
			}
			if !strings.Contains(output.String(), "*main.messageTrap") || !strings.Contains(output.String(), "*mysql.MySQLError") {
				t.Fatal("Go error type evidence omitted")
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
			if report.Table != test.wantTable || report.Column != test.wantColumn {
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
			if report.Stage != test.stage || report.Table != "issues" || !reflect.DeepEqual(report.SQLCodes, []uint16{1105}) {
				t.Fatalf("physical error classification: %+v", report)
			}
			data, err := json.Marshal(report)
			if err != nil || bytes.Contains(data, []byte("private")) {
				t.Fatal("physical error data leaked")
			}
		})
	}
}
