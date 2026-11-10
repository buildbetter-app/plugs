package embedded

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestExecContextPreservesConcreteMySQLErrorAfterStatementClose(t *testing.T) {
	connector, err := NewConnector(&Config{Directory: t.TempDir(), CommitName: "Public fixture", CommitEmail: "fixture@example.test"})
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close(); _ = connector.Close() })
	_, err = db.ExecContext(t.Context(), "USE landing_missing_database")
	// Clients use the concrete error to inspect the wire-compatible code.
	// A nil statement-close result must not replace it with a join wrapper.
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok || mysqlErr.Number != 1049 {
		t.Fatalf("missing database error = %T %v; want concrete MySQLError 1049", err, err)
	}
}

func TestQuerySplitterPreservesQuotedAndNestedSemicolons(t *testing.T) {
	splitter := NewQuerySplitter("SELECT 'a;b', (1 + 2); SELECT `x;y`; SELECT \"c;d\"")
	for _, want := range []string{"SELECT 'a;b', (1 + 2);", "SELECT `x;y`;", "SELECT \"c;d\""} {
		if got, err := splitter.Next(); err != nil || got != want {
			t.Fatalf("Next() = %q, %v; want %q", got, err, want)
		}
	}
	if got, err := splitter.Next(); got != "" || err != io.EOF {
		t.Fatalf("exhausted Next() = %q, %v", got, err)
	}
}

func TestMetricsTimestampIsPrivateAndHonorsInterval(t *testing.T) {
	dir := t.TempDir()
	if _, tooSoon, err := tooSoonToEmitMetrics(dir); err != nil || !tooSoon {
		t.Fatalf("new timestamp: tooSoon=%v err=%v", tooSoon, err)
	}
	path := filepath.Join(dir, "dolt_embedded_metrics")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("timestamp mode: %v %v", info, err)
	}
	old := time.Now().Add(-2 * metricsInterval)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if got, tooSoon, err := tooSoonToEmitMetrics(dir); err != nil || tooSoon || got != path {
		t.Fatalf("old timestamp: path=%q tooSoon=%v err=%v", got, tooSoon, err)
	}
}

func TestMetricsTimestampRefusesMissingRootAndEscapingSymlink(t *testing.T) {
	if _, tooSoon, err := tooSoonToEmitMetrics(filepath.Join(t.TempDir(), "missing")); err == nil || !tooSoon {
		t.Fatalf("missing root: tooSoon=%v err=%v", tooSoon, err)
	}
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "untouched")
	if err := os.Symlink(outside, filepath.Join(dir, "dolt_embedded_metrics")); err != nil {
		t.Fatal(err)
	}
	if _, tooSoon, err := tooSoonToEmitMetrics(dir); err == nil || !tooSoon {
		t.Fatalf("escaping symlink: tooSoon=%v err=%v", tooSoon, err)
	}
	if _, err := os.Lstat(outside); !os.IsNotExist(err) {
		t.Fatalf("escaping symlink created its target: %v", err)
	}
}
