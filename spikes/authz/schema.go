package authz

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
)

// Dialect selects the small set of SQL spelling differences used by the spike.
type Dialect string

const (
	DialectSQLite   Dialect = "sqlite"
	DialectPostgres Dialect = "postgres"
)

const (
	BeadCount    = 100_000
	ReparentSize = 50_000
	// SparseGrantStart is the first of the shallow, leaf-only rows. Grants in
	// this range do not overlap one another or the large epic subtree, so a
	// sparse grant set exposes exactly one bead per grant.
	SparseGrantStart = ReparentSize + 3
	ClosureLimit     = 5_000
	CountP95TargetMS = 250.0
)

// Store owns the synthetic graph and its authorization indexes.
type Store struct {
	DB      *sql.DB
	Dialect Dialect
}

func OpenSQLite(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY;`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{DB: db, Dialect: DialectSQLite}, nil
}

func OpenPostgres(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{DB: db, Dialect: DialectPostgres}, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) Reset(ctx context.Context) error {
	stmts := []string{
		`DROP TABLE IF EXISTS plugin_grant`, `DROP TABLE IF EXISTS authz_cut_closure`,
		`DROP TABLE IF EXISTS authz_closure`, `DROP TABLE IF EXISTS bead`,
		`CREATE TABLE bead (
			id INTEGER PRIMARY KEY, parent_id INTEGER, depth INTEGER NOT NULL,
			restricted INTEGER NOT NULL DEFAULT 0, project_id INTEGER NOT NULL,
			assignee INTEGER, ready INTEGER NOT NULL DEFAULT 1,
			sort_bucket INTEGER NOT NULL, sort_rank INTEGER NOT NULL,
			sort_created INTEGER NOT NULL, claimed_by TEXT
		)`,
		`CREATE TABLE authz_closure (descendant INTEGER NOT NULL, ancestor INTEGER NOT NULL, depth INTEGER NOT NULL, PRIMARY KEY (descendant, ancestor))`,
		`CREATE TABLE authz_cut_closure (descendant INTEGER NOT NULL, ancestor INTEGER NOT NULL, depth INTEGER NOT NULL, PRIMARY KEY (descendant, ancestor))`,
		`CREATE TABLE plugin_grant (subject TEXT NOT NULL, scope INTEGER NOT NULL, role TEXT NOT NULL, PRIMARY KEY(subject, scope, role))`,
		`CREATE INDEX bead_sort ON bead(sort_bucket, sort_rank, sort_created, id)`,
		`CREATE INDEX bead_project ON bead(project_id)`,
		`CREATE INDEX closure_ancestor_descendant ON authz_closure(ancestor, descendant)`,
		`CREATE INDEX cut_ancestor_descendant ON authz_cut_closure(ancestor, descendant)`,
		`CREATE INDEX cut_descendant_ancestor ON authz_cut_closure(descendant, ancestor)`,
		`CREATE INDEX grant_subject_scope ON plugin_grant(subject, scope)`,
	}
	for _, stmt := range stmts {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

// Seed writes 100,000 beads. IDs 1..50,002 form a depth-10 subtree rooted at
// epic 2; the remaining beads are shallow children of root 1. This gives a
// real 50,000-descendant closure rewrite while keeping every row at depth <=10.
func (s *Store) Seed(ctx context.Context, visibleFraction float64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	beadStmt, err := tx.PrepareContext(ctx, s.bind(`INSERT INTO bead(id,parent_id,depth,restricted,project_id,assignee,ready,sort_bucket,sort_rank,sort_created) VALUES(?,?,?,?,?,?,?,?,?,?)`))
	if err != nil {
		return err
	}
	for id := 1; id <= BeadCount; id++ {
		parent, depth := parentAndDepth(id)
		bucket := id % 3 // persisted hybrid order key; no CASE or moving cutoff in query
		project := 1 + (id % 100)
		if _, err = beadStmt.ExecContext(ctx, id, parent, depth, 0, project, id%17, 1, bucket, id, id); err != nil {
			beadStmt.Close()
			return fmt.Errorf("bead %d: %w", id, err)
		}
	}
	beadStmt.Close()
	if err = insertClosure(ctx, tx, s.Dialect); err != nil {
		return err
	}
	if err = insertGrants(ctx, tx, s.Dialect, visibleFraction); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// SetVisibleFraction changes only the grant rows after a corpus has been
// seeded. The graph and closure tables are immutable benchmark inputs, so
// reusing them makes each visibility case independent without rebuilding
// 100,000 rows of closure data.
func (s *Store) SetVisibleFraction(ctx context.Context, visibleFraction float64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DELETE FROM plugin_grant`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE bead SET claimed_by=NULL WHERE claimed_by IS NOT NULL`); err != nil {
		return err
	}
	if err = insertGrants(ctx, tx, s.Dialect, visibleFraction); err != nil {
		return err
	}
	return tx.Commit()
}

func parentAndDepth(id int) (int, int) {
	if id == 1 {
		return 0, 0
	}
	if id <= 10 {
		return id - 1, id - 1
	}
	if id <= 50_002 {
		// Spread the large epic's descendants across levels 2..9, anchored on
		// IDs 3..10. Every such row is under epic 2. The stored depth is the
		// edge distance from root 1, so it is one greater than the level used
		// to select the parent (the old fixture stored this one level too low).
		level := 2 + ((id - 11) % 8)
		return level + 1, level + 1
	}
	return 1, 1
}

func insertClosure(ctx context.Context, tx *sql.Tx, d Dialect) error {
	stmt, err := tx.PrepareContext(ctx, insertSQL(d, "authz_closure"))
	if err != nil {
		return err
	}
	cut, err := tx.PrepareContext(ctx, insertSQL(d, "authz_cut_closure"))
	if err != nil {
		stmt.Close()
		return err
	}
	defer stmt.Close()
	defer cut.Close()
	for id := 1; id <= BeadCount; id++ {
		cur, depth := id, 0
		for cur != 0 {
			if _, err = stmt.ExecContext(ctx, id, cur, depth); err != nil {
				return fmt.Errorf("closure %d/%d: %w", id, cur, err)
			}
			if _, err = cut.ExecContext(ctx, id, cur, depth); err != nil {
				return fmt.Errorf("cut closure %d/%d: %w", id, cur, err)
			}
			p, _ := parentAndDepth(cur)
			cur, depth = p, depth+1
		}
	}
	return nil
}

func insertGrants(ctx context.Context, tx *sql.Tx, d Dialect, fraction float64) error {
	q := `INSERT INTO plugin_grant(subject,scope,role) VALUES(?,?,?)`
	if d == DialectPostgres {
		q = `INSERT INTO plugin_grant(subject,scope,role) VALUES($1,$2,$3)`
	}
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if fraction < 0 || fraction > 1 {
		return fmt.Errorf("visible fraction %.6f is outside [0,1]", fraction)
	}
	// A root grant is the 100% case. Sparse grants use disjoint shallow leaf
	// scopes, rather than IDs starting at 1: granting root 1 (or any ancestor
	// of the large epic) would make every sparse run visible at 100%.
	subject := "principal:sample"
	if fraction == 1 {
		_, err = stmt.ExecContext(ctx, subject, 1, "reader")
	} else {
		want := int(float64(BeadCount)*fraction + 0.5)
		if want > BeadCount-SparseGrantStart+1 {
			return fmt.Errorf("visible fraction %.6f needs %d disjoint leaf grants, only %d available", fraction, want, BeadCount-SparseGrantStart+1)
		}
		for i := 0; i < want; i++ {
			if _, err = stmt.ExecContext(ctx, subject, SparseGrantStart+i, "reader"); err != nil {
				break
			}
		}
	}
	return err
}

func insertSQL(d Dialect, table string) string {
	if d == DialectPostgres {
		return fmt.Sprintf("INSERT INTO %s(descendant,ancestor,depth) VALUES($1,$2,$3)", table)
	}
	return fmt.Sprintf("INSERT INTO %s(descendant,ancestor,depth) VALUES(?,?,?)", table)
}

func (s *Store) bind(q string) string {
	if s.Dialect != DialectPostgres {
		return q
	}
	// Only the bead insert uses ten positional placeholders. Keep this tiny
	// converter local to the spike instead of depending on a query builder.
	n := 0
	out := make([]byte, 0, len(q)+8)
	for i := 0; i < len(q); i++ {
		if q[i] == '?' {
			n++
			out = append(out, fmt.Sprintf("$%d", n)...)
		} else {
			out = append(out, q[i])
		}
	}
	return string(out)
}

// MakeVisibleSubjects creates deterministic principal sets for differential
// checks without changing the benchmark's grant rows.
func MakeVisibleSubjects() []string {
	return []string{"principal:sample", "principal:none", "principal:admin"}
}

// SampleIDs returns stable IDs distributed across the graph.
func SampleIDs(n int) []int {
	r := rand.New(rand.NewSource(7))
	out := make([]int, 0, n)
	for len(out) < n {
		out = append(out, 1+r.Intn(BeadCount))
	}
	return out
}
