package issueops

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysql "github.com/go-sql-driver/mysql"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/sqlbuild"
)

// missingTableError builds the driver error a Dolt query returns when its FROM
// clause names a table that does not exist. Both the embedded driver
// (dolthub/driver/v2 translateError) and the sql-server wire protocol surface
// it as a *mysql.MySQLError carrying 1146 and go-mysql-server's
// "table not found: %s" text.
func missingTableError(table string) error {
	return &mysql.MySQLError{Number: 1146, Message: "table not found: " + table}
}

// missingTableErrorMySQLText is the same condition worded the way stock MySQL
// (and Dolt's MySQL-compatible error mode) words it.
func missingTableErrorMySQLText(schema, table string) error {
	return &mysql.MySQLError{Number: 1146, Message: "Table '" + schema + "." + table + "' doesn't exist"}
}

func expectHydrationQuery(mock sqlmock.Sqlmock, table, id string, err error) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + IssueSelectColumns + " FROM " + table + " " + sqlbuild.LeaseJoin(table) + " WHERE id = ?")).
		WithArgs(id).
		WillReturnError(err)
}

// TestGetIssueInTxMissingLeasesTableIsAnError pins the narrowed tolerance. The
// hydration FROM clause carries sqlbuild.LeaseJoin, so a database missing the
// leases table fails the query for a row that is demonstrably present. The
// table-not-exist tolerance exists for the optional wisps plane only; folding
// a missing leases table into "row absent" hands the caller a 404 it cannot
// tell apart from a deletion.
func TestGetIssueInTxMissingLeasesTableIsAnError(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "dolt wording", err: missingTableError("leases")},
		{name: "mysql wording", err: missingTableErrorMySQLText("beads", "leases")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, mock, tx := beginMockTx(t)

			// A broken active hydration must not be treated as absence or
			// hidden by a durable image. No durable query is permitted.
			expectHydrationQuery(mock, "wisps", "bd-1", tc.err)

			_, err := GetIssueInTx(context.Background(), tx, "bd-1")
			if err == nil {
				t.Fatal("GetIssueInTx succeeded with no leases table")
			}
			if errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("GetIssueInTx reported the row absent for a broken lease join: %v", err)
			}
			if !strings.Contains(err.Error(), "leases") {
				t.Fatalf("error does not name the missing table: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
	}
}

// TestGetIssueInTxMissingWispsTableIsNotFound is the control: the tolerance
// the narrowed guard must keep. A pre-migration database has no wisps table,
// and an ID on neither plane is genuinely absent.
func TestGetIssueInTxMissingWispsTableIsNotFound(t *testing.T) {
	_, mock, tx := beginMockTx(t)

	expectHydrationQuery(mock, "wisps", "bd-1", missingTableError("wisps"))
	expectHydrationQuery(mock, "issues", "bd-1", sql.ErrNoRows)

	_, err := GetIssueInTx(context.Background(), tx, "bd-1")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetIssueInTx = %v, want ErrNotFound for a pre-migration database", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestGetIssueInTxAbsentRowIsNotFound is the second control: an ID on neither
// plane of a healthy database still answers not-found.
func TestGetIssueInTxAbsentRowIsNotFound(t *testing.T) {
	_, mock, tx := beginMockTx(t)

	expectHydrationQuery(mock, "wisps", "bd-1", sql.ErrNoRows)
	expectHydrationQuery(mock, "issues", "bd-1", sql.ErrNoRows)

	_, err := GetIssueInTx(context.Background(), tx, "bd-1")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetIssueInTx = %v, want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestUpdateIssueInTxMissingLeasesTableIsAnError covers the mutation path.
// updateIssueInTx reads the pre-update row through the same hydration query,
// so the same swallowed error told a writer its live issue did not exist.
func TestUpdateIssueInTxMissingLeasesTableIsAnError(t *testing.T) {
	_, mock, tx := beginMockTx(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM wisps WHERE id = ? LIMIT 1")).
		WithArgs("bd-1").
		WillReturnError(sql.ErrNoRows)
	expectHydrationQuery(mock, "wisps", "bd-1", sql.ErrNoRows)
	expectHydrationQuery(mock, "issues", "bd-1", missingTableError("leases"))

	_, err := UpdateIssueInTx(context.Background(), tx, "bd-1", map[string]interface{}{"title": "new"}, "tester")
	if err == nil {
		t.Fatal("UpdateIssueInTx succeeded with no leases table")
	}
	if errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("UpdateIssueInTx reported the row absent for a broken lease join: %v", err)
	}
	if !strings.Contains(err.Error(), "leases") {
		t.Fatalf("error does not name the missing table: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// These SQL boundary controls forbid the wrong-plane query. Persisted collision
// and label identity are also checked by TestGetIssueActiveImageWinsStoredCollision
// in the Dolt package; mock rows are not native runtime qualification.
func TestGetIssueInTxActiveImageAndDurableFallback(t *testing.T) {
	for _, test := range []struct {
		name, table, labels string
		activeAbsent        error
	}{
		{name: "active image", table: "wisps", labels: "wisp_labels"},
		{name: "durable only", table: "issues", labels: "labels", activeAbsent: sql.ErrNoRows},
		{name: "pre migration durable", table: "issues", labels: "labels", activeAbsent: missingTableError("wisps")},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, mock, tx := beginMockTx(t)
			if test.activeAbsent != nil {
				expectHydrationQuery(mock, "wisps", "bd-image", test.activeAbsent)
			}
			values := issueRowValues("bd-image", test.table+" title")
			for i, column := range issueColumns() {
				switch column {
				case "ephemeral":
					values[i] = 0
					if test.table == "wisps" {
						values[i] = 1
					}
				case "description":
					values[i] = test.table + " description"
				case "metadata":
					values[i] = `{"source":"` + test.table + `"}`
				}
			}
			mock.ExpectQuery(regexp.QuoteMeta("SELECT " + IssueSelectColumns + " FROM " + test.table + " " + sqlbuild.LeaseJoin(test.table) + " WHERE id = ?")).
				WithArgs("bd-image").WillReturnRows(issueRows().AddRow(values...))
			mock.ExpectQuery(regexp.QuoteMeta("SELECT label FROM " + test.labels + " WHERE issue_id = ? ORDER BY label")).
				WithArgs("bd-image").WillReturnRows(sqlmock.NewRows([]string{"label"}).AddRow(test.table + " label"))
			got, err := GetIssueInTx(context.Background(), tx, "bd-image")
			if err != nil {
				t.Fatal(err)
			}
			if got.ID != "bd-image" || got.Title != test.table+" title" || got.Description != test.table+" description" || got.Ephemeral != (test.table == "wisps") || string(got.Metadata) != `{"source":"`+test.table+`"}` || len(got.Labels) != 1 || got.Labels[0] != test.table+" label" {
				t.Fatalf("wrong source image: %+v", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
	}
}

func TestGetIssueInTxActiveErrorsDoNotFallBack(t *testing.T) {
	for _, labelsFail := range []bool{false, true} {
		t.Run(fmt.Sprint("labels=", labelsFail), func(t *testing.T) {
			_, mock, tx := beginMockTx(t)
			cause := errors.New("active source read failed")
			if labelsFail {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT " + IssueSelectColumns + " FROM wisps " + sqlbuild.LeaseJoin("wisps") + " WHERE id = ?")).
					WithArgs("bd-image").WillReturnRows(issueRows().AddRow(issueRowValues("bd-image", "active title")...))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT label FROM wisp_labels WHERE issue_id = ? ORDER BY label")).WithArgs("bd-image").WillReturnError(cause)
			} else {
				expectHydrationQuery(mock, "wisps", "bd-image", cause)
			}
			if _, err := GetIssueInTx(context.Background(), tx, "bd-image"); !errors.Is(err, cause) || errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("active failure lost or treated as absence: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet sql expectations: %v", err)
			}
		})
	}
}

func TestGetIssueInTxDurableErrorAfterActiveAbsence(t *testing.T) {
	_, mock, tx := beginMockTx(t)
	cause := errors.New("durable source read failed")
	expectHydrationQuery(mock, "wisps", "bd-image", sql.ErrNoRows)
	expectHydrationQuery(mock, "issues", "bd-image", cause)
	if _, err := GetIssueInTx(context.Background(), tx, "bd-image"); !errors.Is(err, cause) || errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("durable failure lost or treated as absence: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
