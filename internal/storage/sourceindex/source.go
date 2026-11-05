// Package sourceindex maintains the private typed SQL source indexes. It never
// interprets document JSON or supplies a general query language.
package sourceindex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type SQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type Dialect bool

const (
	SQLite          Dialect = false
	Postgres        Dialect = true
	publicationRows         = 128
)

func (d Dialect) table(name string) string {
	if d == Postgres {
		return "plug_postgres." + name
	}
	return name
}
func (d Dialect) key(key string) any {
	if d == Postgres {
		return []byte(key)
	}
	return key
}
func (d Dialect) revision(at uint64) string {
	if d == Postgres {
		return strconv.FormatUint(at, 10)
	}
	return fmt.Sprintf("%020d", at)
}

type bindings struct {
	d    Dialect
	args []any
}

func (b *bindings) add(value any) string {
	b.args = append(b.args, value)
	if b.d == Postgres {
		return "$" + strconv.Itoa(len(b.args))
	}
	return "?"
}
func (b *bindings) scope(incarnation string) string {
	return "incarnation=" + b.add(incarnation) + " AND branch=" + b.add(b.d.revision(0))
}
func key(ref model.Ref) string {
	value, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	return value
}

type Declaration struct {
	ParentKey string
	Graph     bool
	Links     []driver.SourceLink
}

func Declare(record driver.Record, parent *model.Ref, links []driver.SourceLink, graph bool) (Declaration, error) {
	if err := driver.ValidateGraphSourceRecord(record, parent, links); err != nil {
		return Declaration{}, err
	}
	declaration := Declaration{Graph: graph, Links: slices.Clone(links)}
	if parent != nil {
		declaration.ParentKey = key(*parent)
	}
	return declaration, nil
}

type Bucket struct{ Kind, Target string }
type Overlay struct {
	Declarations map[string]Declaration
	Parents      map[string][]string
	Incoming     map[Bucket][]string
}

func remove(keys []string, id string) []string {
	if i, ok := slices.BinarySearch(keys, id); ok {
		return slices.Delete(keys, i, i+1)
	}
	return keys
}
func insert(keys []string, id string) []string {
	i, ok := slices.BinarySearch(keys, id)
	if ok {
		return keys
	}
	return slices.Insert(keys, i, id)
}
func (o *Overlay) Replace(id string, declaration *Declaration) {
	if old, ok := o.Declarations[id]; ok {
		if old.ParentKey != "" {
			o.Parents[old.ParentKey] = remove(o.Parents[old.ParentKey], id)
		}
		for _, link := range old.Links {
			bucket := Bucket{link.Kind, key(link.To)}
			o.Incoming[bucket] = remove(o.Incoming[bucket], id)
		}
		delete(o.Declarations, id)
	}
	if declaration == nil {
		return
	}
	if o.Declarations == nil {
		o.Declarations = make(map[string]Declaration)
		o.Parents = make(map[string][]string)
		o.Incoming = make(map[Bucket][]string)
	}
	o.Declarations[id] = *declaration
	if declaration.ParentKey != "" {
		o.Parents[declaration.ParentKey] = insert(o.Parents[declaration.ParentKey], id)
	}
	for _, link := range declaration.Links {
		bucket := Bucket{link.Kind, key(link.To)}
		o.Incoming[bucket] = insert(o.Incoming[bucket], id)
	}
}

type Coverage struct {
	Ready          bool
	First, Through uint64
}
type CoveragePair struct{ Parent, Links Coverage }

func (c Coverage) Require(at uint64) error {
	if !c.Ready || at < c.First || at > c.Through {
		return fmt.Errorf("%w: typed source coverage unavailable", driver.ErrUnsupported)
	}
	return nil
}
func (d Dialect) ReadCoverage(ctx context.Context, q SQL, incarnation string) (CoveragePair, error) {
	result := CoveragePair{}
	for i, name := range []string{"hierarchy_index_coverage", "graph_link_index_coverage"} {
		b := bindings{d: d}
		where := b.scope(incarnation)
		var format int
		var first, through, state string
		err := q.QueryRowContext(ctx, "SELECT format,CAST(built_from AS TEXT),CAST(certified_through AS TEXT),state FROM "+d.table(name)+" WHERE "+where, b.args...).Scan(&format, &first, &through, &state)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return CoveragePair{}, err
		}
		if format != 1 || state != "ready" && state != "unbuilt" {
			return CoveragePair{}, fmt.Errorf("%w: incompatible typed source coverage", driver.ErrUnsupported)
		}
		from, e := strconv.ParseUint(first, 10, 64)
		if e != nil {
			return CoveragePair{}, e
		}
		to, e := strconv.ParseUint(through, 10, 64)
		if e != nil || from > to {
			return CoveragePair{}, fmt.Errorf("%w: malformed typed source coverage", driver.ErrUnsupported)
		}
		coverage := Coverage{Ready: state == "ready", First: from, Through: to}
		if i == 0 {
			result.Parent = coverage
		} else {
			result.Links = coverage
		}
	}
	return result, nil
}

// Empty checks private index families in addition to the caller's existing
// complete durable-target emptiness fence. Neither proof substitutes for the other.
func (d Dialect) Empty(ctx context.Context, q SQL) (bool, error) {
	parts := make([]string, 0, 8)
	for _, name := range []string{"hierarchy_index_coverage", "hierarchy_source_current", "hierarchy_source_history", "graph_link_index_coverage", "graph_link_source_current", "graph_link_current", "graph_link_source_history", "graph_link_history"} {
		parts = append(parts, "EXISTS(SELECT 1 FROM "+d.table(name)+")")
	}
	var occupied bool
	err := q.QueryRowContext(ctx, "SELECT "+strings.Join(parts, " OR ")).Scan(&occupied)
	return !occupied, err
}

// SourcesEmpty proves that no live bead predates this publication. It checks
// storage envelopes only, never document JSON or derived closure membership.
func (d Dialect) SourcesEmpty(ctx context.Context, q SQL) (bool, error) {
	query := "SELECT NOT EXISTS(SELECT 1 FROM records r JOIN objects o ON o.key=r.key WHERE o.kind='bead')"
	if d == Postgres {
		query = "SELECT NOT EXISTS(SELECT 1 FROM plug_postgres.records WHERE kind='bead' AND NOT deleted)"
	}
	var empty bool
	err := q.QueryRowContext(ctx, query).Scan(&empty)
	return empty, err
}

// Plan is prepared before record publication while Head is held. Its marker
// and all source rows publish afterward inside that same fenced SQL transaction.
type Plan struct {
	Dialect                   Dialect
	Incarnation               string
	At                        uint64
	Parent, Links             Coverage
	ParentActive, LinksActive bool
	// empty is the same-connection proof that both typed source index domains
	// were empty when this plan was prepared. It lets the first publication
	// avoid cleanup statements that cannot affect a row; later plans retain the
	// legacy close/delete boundary.
	empty        bool
	Records      map[string]*driver.Record
	Declarations map[string]Declaration
	backfill     bool // preserve source revisions; publish markers only after every page
}

func (d Dialect) Prepare(ctx context.Context, q SQL, current, next driver.Meta, durableEmpty bool, records map[string]*driver.Record, identities map[string]driver.Record, overlay *Overlay, graphWritable func() error, graphBumped bool) (Plan, error) {
	coverage, err := d.ReadCoverage(ctx, q, current.Incarnation)
	if err != nil {
		return Plan{}, err
	}
	head, err := current.Head.Uint64()
	if err != nil {
		return Plan{}, err
	}
	at, err := next.Head.Uint64()
	if err != nil {
		return Plan{}, err
	}
	parentComplete, linksComplete, parentDeclared, linksDeclared := true, true, false, false
	for id, record := range records {
		kind := identities[id].Kind
		if record != nil {
			kind = record.Kind
		}
		if kind != "bead" {
			continue
		}
		declaration, typed := overlay.Declarations[id]
		if typed || coverage.Parent.Ready || coverage.Links.Ready {
			if err = graphWritable(); err != nil {
				return Plan{}, err
			}
			if !graphBumped {
				return Plan{}, fmt.Errorf("typed source publication requires graph/blocking bump")
			}
		}
		if record == nil {
			continue
		}
		if !typed {
			parentComplete, linksComplete = false, false
			continue
		}
		if key(record.Ref) != id || declaration.ParentKey == id || driver.ValidateGraphSourceRecord(*record, nil, declaration.Links) != nil {
			return Plan{}, fmt.Errorf("%w: source key/image/declaration mismatch", driver.ErrUnsupported)
		}
		parentDeclared = true
		if declaration.Graph {
			linksDeclared = true
		} else {
			linksComplete = false
		}
	}
	if coverage.Parent.Ready && (!parentComplete || coverage.Parent.Through != head) || coverage.Links.Ready && (!linksComplete || coverage.Links.Through != head) {
		return Plan{}, fmt.Errorf("%w: incomplete or gapped typed source publication", driver.ErrUnsupported)
	}
	// Ontology, allocator and other source-free publications do not make an
	// empty graph incomplete. Prove source absence under the same Head fence;
	// never infer it from Head zero or from the staged final images.
	empty := false
	if !coverage.Parent.Ready || !coverage.Links.Ready {
		empty, err = d.SourcesEmpty(ctx, q)
		if err == nil && empty {
			empty, err = d.Empty(ctx, q)
		}
		if err != nil {
			return Plan{}, err
		}
	}
	parentActive := coverage.Parent.Ready || empty && parentComplete && parentDeclared
	linksActive := coverage.Links.Ready || empty && linksComplete && linksDeclared
	parent, links := coverage.Parent, coverage.Links
	first := head
	if durableEmpty {
		first = 0
	}
	if !parent.Ready {
		parent.First = first
	}
	if !links.Ready {
		links.First = first
	}
	if current.Incarnation != next.Incarnation {
		// Native current images have no revision-zero history in the new incarnation.
		floor, err := next.HistoryFloor.Uint64()
		if err != nil {
			return Plan{}, err
		}
		parent, links = Coverage{First: floor}, Coverage{First: floor}
		parentActive = empty && parentComplete && parentDeclared
		linksActive = empty && linksComplete && linksDeclared
	}
	parent.Through, links.Through = at, at
	return Plan{Dialect: d, Incarnation: next.Incarnation, At: at, Parent: parent, Links: links, ParentActive: parentActive, LinksActive: linksActive, empty: empty, Records: records, Declarations: overlay.Declarations}, nil
}

func sortedRecordKeys(records map[string]*driver.Record) []string {
	keys := make([]string, 0, len(records))
	for id := range records {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	return keys
}
