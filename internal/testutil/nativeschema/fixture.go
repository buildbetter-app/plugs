// Package schema assembles the frozen native schema for disposable tests only.
// It exposes no production migration or existing-store upgrade operation.
package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.up.sql migrations/ignored/*.up.sql
var migrations embed.FS

type migrationFile struct {
	version int
	name    string
}

type migrationSource struct {
	files            fs.FS
	dir, cursorTable string
}

var mainSource = migrationSource{migrations, "migrations", "schema_migrations"}
var ignoredSource = migrationSource{migrations, "migrations/ignored", "ignored_schema_migrations"}

func (m migrationSource) list() []migrationFile {
	entries, err := fs.ReadDir(m.files, m.dir)
	if err != nil {
		panic(err)
	}
	var result []migrationFile
	seen := map[int]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		version, err := strconv.Atoi(strings.SplitN(entry.Name(), "_", 2)[0])
		if err != nil || seen[version] {
			panic("invalid frozen migration version")
		}
		seen[version] = true
		result = append(result, migrationFile{version, entry.Name()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].version < result[j].version })
	return result
}

func (m migrationSource) bootstrapSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	version INT PRIMARY KEY,
	applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	content_hash CHAR(64)
)`, m.cursorTable)
}

func LatestVersion() int {
	files := mainSource.list()
	return files[len(files)-1].version
}

// AllMigrationsSQL retains the original fresh-CLI bundle algorithm and SQL.
func AllMigrationsSQL() string {
	var b strings.Builder
	b.WriteString(mainSource.bootstrapSQL())
	b.WriteString(";\n")
	for _, f := range mainSource.list() {
		data, err := mainSource.files.ReadFile(mainSource.dir + "/" + f.name)
		if err != nil {
			panic(err)
		}
		b.WriteString(cliCompatibleMigrationSQL(f.name, string(data)))
		sum := sha256.Sum256(data)
		fmt.Fprintf(&b, "\nINSERT IGNORE INTO %s (version, content_hash) VALUES (%d, '%s');\n",
			mainSource.cursorTable, f.version, hex.EncodeToString(sum[:]))
	}
	return b.String()
}

// MigrateUpTo applies the frozen numbered SQL to a fresh, disposable fixture.
// Its name is retained for the original capture controls; it is testonly.
func MigrateUpTo(ctx context.Context, conn *sql.Conn, version int) (int, error) {
	return seed(ctx, conn, mainSource, version)
}

// MigrateUp adds the frozen clone-local series to a disposable fixture.
// Production upgrade guards, repairs and history are outside this test helper.
func MigrateUp(ctx context.Context, conn *sql.Conn) (int, error) {
	count, err := seed(ctx, conn, mainSource, LatestVersion())
	if err != nil {
		return count, err
	}
	n, err := seed(ctx, conn, ignoredSource, 0)
	return count + n, err
}

func seed(ctx context.Context, conn *sql.Conn, source migrationSource, limit int) (int, error) {
	if _, err := conn.ExecContext(ctx, source.bootstrapSQL()); err != nil {
		return 0, err
	}
	var current int
	if err := conn.QueryRowContext(ctx, "SELECT COALESCE(MAX(version),0) FROM "+source.cursorTable).Scan(&current); err != nil {
		return 0, err
	}
	count := 0
	for _, file := range source.list() {
		if file.version <= current || limit > 0 && file.version > limit {
			continue
		}
		data, err := source.files.ReadFile(source.dir + "/" + file.name)
		if err != nil {
			return count, err
		}
		if err := DrainCall(ctx, conn, string(data)); err != nil {
			return count, fmt.Errorf("frozen fixture %s: %w", file.name, err)
		}
		sum := sha256.Sum256(data)
		if _, err := conn.ExecContext(ctx, "INSERT IGNORE INTO "+source.cursorTable+" (version,content_hash) VALUES (?,?)", file.version, hex.EncodeToString(sum[:])); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func CurrentCommit(ctx context.Context, conn *sql.Conn) (string, error) {
	var head string
	err := conn.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&head)
	return head, err
}

// DrainCall consumes every result set and preserves both read and close errors.
func DrainCall(ctx context.Context, conn *sql.Conn, query string, args ...any) (resultErr error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for {
		for rows.Next() {
		}
		if !rows.NextResultSet() {
			break
		}
	}
	return rows.Err()
}
