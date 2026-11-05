package plugdump

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/steveyegge/beads/internal/captureclock"
)

func testClockInput(t *testing.T) *captureclock.Snapshot {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "clock.json")
	input := `{"version":"beads-capture-clock/1","step":"independent-readback","go_time":"2026-11-03T12:00:00.123456789Z","sql_time":"2026-11-03T12:00:10.123456Z"}`
	if err := os.WriteFile(path, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(captureclock.FileEnv, path)
	t.Setenv(captureclock.PublicEnv, "1")
	clock, err := captureclock.Read()
	if err != nil {
		t.Fatalf("valid input precondition: %v", err)
	}
	return clock
}

func TestNativeClockReadbackAndRefusals(t *testing.T) {
	for _, tc := range []struct{ name, reason string }{
		{"valid", ""},
		{"wrong-sql-microseconds", "differs from explicit input"},
		{"wrong-sql-default", "differs from explicit input"},
		{"empty-readback", "requires one row"},
		{"sql-error", "SQL clock readback"},
		{"missing-column-precision", "column precision census"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := testClockInput(t)
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
				_ = db.Close()
			})
			microseconds, seconds := "2026-11-03T12:00:10.123456Z", "2026-11-03T12:00:10.000000Z"
			if tc.name == "wrong-sql-microseconds" {
				microseconds = "2026-11-03T12:00:10.123457Z"
			}
			if tc.name == "wrong-sql-default" {
				seconds = "2026-11-03T12:00:11.000000Z"
			}
			rows := sqlmock.NewRows([]string{"utc_microseconds", "utc_default", "session_time_zone", "server_version"})
			if tc.name != "empty-readback" {
				rows.AddRow(microseconds, seconds, "+00:00", "unit-readback")
			}
			expect := mock.ExpectQuery(nativeClockQuery)
			if tc.name == "sql-error" {
				expect.WillReturnError(errors.New("unit query refusal"))
			} else {
				expect.WillReturnRows(rows)
			}
			if tc.name == "valid" || tc.name == "missing-column-precision" {
				columns := sqlmock.NewRows([]string{"table_name", "column_name", "data_type", "datetime_precision"})
				if tc.name == "valid" {
					for _, table := range []string{"issues", "wisps"} {
						for _, column := range []string{"created_at", "defer_until", "updated_at"} {
							columns.AddRow(table, column, "datetime", 0)
						}
					}
				}
				mock.ExpectQuery(nativeClockColumnsQuery).WillReturnRows(columns)
			}
			evidence, err := (SQLSource{DB: db}).observeNativeClock(context.Background(), input)
			if tc.reason != "" {
				if err == nil || !strings.Contains(err.Error(), tc.reason) || evidence != nil {
					t.Fatalf("specific refusal %q: %+v %v", tc.reason, evidence, err)
				}
				return
			}
			if err != nil || evidence == nil || !evidence.GoObserved.Equal(input.GoTime) || evidence.GoObserved.Equal(input.SQLTime) || evidence.SQLInputResolutionNS != 1000 || evidence.SQLDefaultUTCResolutionNS != 1000000000 {
				t.Fatalf("independent clock evidence: %+v %v", evidence, err)
			}
			if err := unchangedNativeClock(input); err != nil {
				t.Fatalf("stable clock precondition: %v", err)
			}
			data := strings.Replace(string(input.Input), "independent-readback", "changed-step", 1)
			if err := os.WriteFile(input.Path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := unchangedNativeClock(input); err == nil || !strings.Contains(err.Error(), "changed during census") {
				t.Fatalf("changed-step refusal: %v", err)
			}
		})
	}
}

func TestNativeClockAbsenceDoesNotQuerySQL(t *testing.T) {
	// A nil DB deliberately makes any attempted SQL read fail; the ordinary
	// legacy native inventory must issue no additional clock observation.
	evidence, err := (SQLSource{}).observeNativeClock(context.Background(), nil)
	if err != nil || evidence != nil {
		t.Fatalf("ordinary absence: %+v %v", evidence, err)
	}
}
