// Package testclock binds application time and real SQL time in disposable
// acceptance stores. It is a test-only Bazel dependency, never server config.
package testclock

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"
)

// Clock advances explicitly. SQL sessions read the same instant from an owned
// database row; neither query results nor driver responses are substituted.
type Clock struct {
	mu       sync.RWMutex
	now      time.Time
	postgres *sql.DB
}

func New(t testing.TB, at time.Time) *Clock {
	t.Helper()
	if at.IsZero() || at.Nanosecond()%1000 != 0 {
		t.Fatal("paired SQL clock requires an explicit, microsecond-aligned instant")
	}
	return &Clock{now: at.UTC()}
}

func (c *Clock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *Clock) Advance(t testing.TB, delta time.Duration) {
	t.Helper()
	if delta < 0 || delta%time.Microsecond != 0 {
		t.Fatal("paired SQL clock advances must be nonnegative whole microseconds")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	next := c.now.Add(delta)
	if c.postgres != nil {
		if _, err := c.postgres.ExecContext(context.Background(), `UPDATE plug_test_clock.instant SET at=$1 WHERE singleton`, next); err != nil {
			t.Fatal(err)
		}
	}
	c.now = next
}

// BindPostgres installs a VOLATILE SQL clock in a fresh disposable database.
// Explicitly listing pg_catalog after the test schema makes unqualified
// clock_timestamp() in the unchanged driver resolve to this SQL function on
// every pooled/reserved connection. pg_catalog.clock_timestamp() stays real.
func (c *Clock) BindPostgres(t testing.TB, dsn string) string {
	t.Helper()
	if c.postgres != nil {
		t.Fatal("SQL clock is already bound")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	_, err = db.ExecContext(context.Background(), `
CREATE SCHEMA plug_test_clock;
CREATE TABLE plug_test_clock.instant (singleton boolean PRIMARY KEY CHECK(singleton), at timestamptz NOT NULL);
CREATE FUNCTION plug_test_clock.clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE
AS 'SELECT at FROM plug_test_clock.instant WHERE singleton';`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(context.Background(), `INSERT INTO plug_test_clock.instant VALUES (true,$1)`, c.Now()); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		t.Fatal("SQL clock requires a PostgreSQL URL")
	}
	q := u.Query()
	q.Set("search_path", "plug_test_clock,pg_catalog")
	// Deliberately use a non-UTC session timezone. timestamptz must preserve
	// the instant; application outputs still use their explicit UTC clock.
	q.Set("timezone", "America/St_Johns")
	u.RawQuery = q.Encode()
	c.postgres = db
	return u.String()
}

// PostgresNow checks the reached SQL function through an independent session,
// including the distinction from the production catalog clock.
func PostgresNow(t testing.TB, dsn string) time.Time {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var at, wall time.Time
	if err = db.QueryRowContext(context.Background(), `SELECT clock_timestamp(),pg_catalog.clock_timestamp()`).Scan(&at, &wall); err != nil {
		t.Fatal(err)
	}
	if at.Equal(wall) {
		t.Fatal("controlled SQL clock unexpectedly equals catalog wall time")
	}
	return at.UTC()
}

func (c *Clock) String() string {
	return fmt.Sprintf("controlled UTC %s", c.Now().Format(time.RFC3339Nano))
}
