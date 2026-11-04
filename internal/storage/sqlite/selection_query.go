package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type selectionFactBinding struct {
	start   string
	kind    driver.SelectionFactKind
	staged  []driver.SelectionFact
	overlay bool
}
type selectionFactID struct {
	row       string
	at        uint64
	committed bool
}

// A fact envelope binds the actual source observation at its selected token.
// Data sources use only publication-time metadata. In particular, a
// Count/Summary preflight must not decode key_history's complete derived row.
func (r *reader) selectionSourceState(ctx context.Context, id string, at uint64, overlay bool) (driver.LockState, string, error) {
	state := driver.LockState{Revision: model.Natural64(0)}
	if !strings.HasPrefix(id, "0:") && !strings.HasPrefix(id, "3:") {
		// Read-fact and sentinel histories contain metadata only. Their bumps
		// retain the prepublication token, unlike record/derived replacement.
		var raw []byte
		err := r.tx.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`, id, revKey(at)).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return driver.LockState{}, "", err
		}
		if err == nil {
			if err = json.Unmarshal(raw, &state); err != nil {
				return driver.LockState{}, "", err
			}
			if keyID(state.Key) != id {
				return driver.LockState{}, "", fmt.Errorf("%w: SQLite fact source key mismatch", driver.ErrUnsupported)
			}
		}
		if overlay && r.owner != nil && at == r.at {
			if key, ok := r.owner.bumps[id]; ok {
				state.Key, state.Present = key, true
			}
		}
		return state, "", nil
	}
	var digest string
	var err error
	if strings.HasPrefix(id, "3:") {
		state, digest, err = projectedDerivedSource(ctx, r.tx, r.meta.Incarnation, id, at)
		if err != nil {
			return driver.LockState{}, "", err
		}
	} else {
		var revision string
		var present int
		err = r.tx.QueryRowContext(ctx, `SELECT source_revision,source_present,source_digest
			FROM selection_source_history
			WHERE incarnation=? AND branch=? AND source_key=? AND start_revision<=?
			AND (end_revision IS NULL OR end_revision>?)
			ORDER BY start_revision DESC LIMIT 1`, r.meta.Incarnation, revKey(0), id, revKey(at), revKey(at)).Scan(&revision, &present, &digest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return driver.LockState{}, "", err
		}
		if err == nil {
			observed, parseErr := parseRev(revision)
			if parseErr != nil {
				return driver.LockState{}, "", parseErr
			}
			if present != 0 && present != 1 {
				return driver.LockState{}, "", fmt.Errorf("%w: SQLite malformed fact source presence", driver.ErrUnsupported)
			}
			state.Revision, state.Present = observed, present == 1
		}
	}
	if overlay && r.owner != nil && at == r.at {
		if staged, ok := r.owner.derived[id]; ok {
			state.Present = staged.Present
			if staged.Present && staged.Derived != nil {
				return state, driver.SelectionDigest(staged.Derived.Value), nil
			}
			return state, "", nil
		}
		if staged, ok := r.owner.records[id]; ok {
			state.Present = staged != nil
			if staged != nil {
				return state, driver.SelectionDigest(staged.Body), nil
			}
			return state, "", nil
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		return state, "", nil // a true never-present source is miss at revision zero
	}
	return state, digest, nil
}

func (r *reader) validateSelectionFactSource(ctx context.Context, id, revision, digest string, present int, at uint64, committed bool) error {
	if present != 0 && present != 1 {
		return fmt.Errorf("%w: SQLite malformed fact source presence", driver.ErrUnsupported)
	}
	state, actual, err := r.selectionSourceState(ctx, id, at, !committed)
	if err != nil {
		return err
	}
	want, err := parseRev(revision)
	if err != nil {
		return err
	}
	if state.Present != (present == 1) || state.Revision.String() != want.String() {
		return fmt.Errorf("%w: SQLite fact source observation changed", driver.ErrSelectionUnavailable)
	}
	if actual != digest {
		return fmt.Errorf("%w: SQLite fact source bytes changed", driver.ErrSelectionUnavailable)
	}
	return nil
}

// Every referenced fact envelope is checked before evaluating any branch of
// either expression. A missing complete set cannot become true under NOT.
func (r *reader) selectionFacts(ctx context.Context, e driver.SelectionExpr, at uint64, committed bool, out map[selectionFactID]selectionFactBinding) error {
	if e.Exists != nil {
		id := selectionFactID{row: keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &e.Exists.Row}), at: at, committed: committed}
		if _, ok := out[id]; !ok {
			if r.owner != nil && at == r.at && !committed {
				if state, changed := r.owner.derived[id.row]; changed {
					if !state.Present || state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Facts == nil || state.Derived.Index.Facts.Kind != e.Exists.Kind {
						return fmt.Errorf("%w: SQLite selection staged fact coverage unavailable", driver.ErrSelectionUnavailable)
					}
					if family, sourceBuilt := sqliteSourceBuiltFamily(e.Exists.Row, e.Exists.Kind); sourceBuilt && state.Derived.Index.Source.Key.Kind == driver.LockRecord {
						if err := r.requireSelectionFamily(ctx, family, at); err != nil {
							return err
						}
						complete, err := r.owner.selectionFamilyCompleteForCommit(ctx, r.tx, r.meta.Incarnation, family, r.owner.derived)
						if err != nil {
							return err
						}
						if !complete {
							return fmt.Errorf("%w: SQLite staged source family incomplete", driver.ErrSelectionUnavailable)
						}
					}
					out[id] = selectionFactBinding{kind: e.Exists.Kind, staged: state.Derived.Index.Facts.Rows, overlay: true}
					goto children
				}
			}
			var start, table, kind, source, revision, digest string
			var present int
			err := r.tx.QueryRowContext(ctx, `SELECT start_revision,row_table,kind,source_key,source_revision,source_present,source_digest FROM selection_fact_sets
			 WHERE incarnation=? AND branch=? AND row_key=? AND start_revision<=?
			 AND (end_revision IS NULL OR end_revision>?)
			 ORDER BY start_revision DESC LIMIT 1`, r.meta.Incarnation, revKey(0), id.row, revKey(at), revKey(at)).Scan(&start, &table, &kind, &source, &revision, &present, &digest)
			if err == sql.ErrNoRows {
				return fmt.Errorf("%w: SQLite selection fact coverage unavailable", driver.ErrSelectionUnavailable)
			}
			if err != nil {
				return err
			}
			if table != string(e.Exists.Row.Table) || kind != string(e.Exists.Kind) {
				return fmt.Errorf("%w: SQLite selection fact domain unavailable", driver.ErrSelectionUnavailable)
			}
			if family, sourceBuilt := sqliteSourceBuiltFamily(e.Exists.Row, e.Exists.Kind); sourceBuilt && strings.HasPrefix(source, "0:") {
				if err := r.requireSelectionFamily(ctx, family, at); err != nil {
					return err
				}
				if r.owner != nil && at == r.at && !committed {
					complete, err := r.owner.selectionFamilyCompleteForCommit(ctx, r.tx, r.meta.Incarnation, family, r.owner.derived)
					if err != nil {
						return err
					}
					if !complete {
						return fmt.Errorf("%w: SQLite staged source family incomplete", driver.ErrSelectionUnavailable)
					}
				}
			}
			if err = r.validateSelectionFactSource(ctx, source, revision, digest, present, at, committed); err != nil {
				return err
			}
			out[id] = selectionFactBinding{kind: e.Exists.Kind, start: start}
		}
		if out[id].kind != e.Exists.Kind {
			return fmt.Errorf("%w: SQLite selection fact domain mismatch", driver.ErrSelectionUnavailable)
		}
	}
children:
	for _, child := range e.Args {
		if err := r.selectionFacts(ctx, child, at, committed, out); err != nil {
			return err
		}
	}
	return nil
}

type sqliteSelectionSQL struct {
	args     []any
	argIndex map[selectionArgKey]int
	facts    map[selectionFactID]selectionFactBinding
	owner    *transaction
	meta     driver.Meta
}

type selectionArgKey struct {
	kind  byte
	value string
}

func selectionBindKey(v any) (selectionArgKey, bool) {
	switch v := v.(type) {
	case nil:
		return selectionArgKey{kind: '0'}, true
	case string:
		return selectionArgKey{kind: 's', value: v}, true
	case []byte:
		return selectionArgKey{kind: 'b', value: string(v)}, true
	case int:
		return selectionArgKey{kind: 'i', value: strconv.FormatInt(int64(v), 10)}, true
	case int64:
		return selectionArgKey{kind: 'I', value: strconv.FormatInt(v, 10)}, true
	case uint64:
		return selectionArgKey{kind: 'u', value: strconv.FormatUint(v, 10)}, true
	default:
		return selectionArgKey{}, false
	}
}

func (b *sqliteSelectionSQL) arg(v any) string {
	key, reusable := selectionBindKey(v)
	if reusable {
		if n := b.argIndex[key]; n != 0 {
			return "?" + strconv.Itoa(n)
		}
	}
	b.args = append(b.args, v)
	n := len(b.args)
	if reusable {
		if b.argIndex == nil {
			b.argIndex = make(map[selectionArgKey]int)
		}
		b.argIndex[key] = n
	}
	// Explicit numbers preserve the binding when SQL fragments are assembled
	// later and let both staged source planes share the same immutable values.
	return "?" + strconv.Itoa(n)
}
func (b *sqliteSelectionSQL) refValue(ref model.Ref) string {
	return keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
}
func (b *sqliteSelectionSQL) value(v driver.SelectionValue) any {
	switch {
	case v.String != nil:
		return *v.String
	case v.Int != nil:
		return *v.Int
	case v.Bool != nil:
		if *v.Bool {
			return 1
		}
		return 0
	case v.Ref != nil:
		return b.refValue(*v.Ref)
	default:
		return nil // SelectionPlan.Validate rejects this before compilation.
	}
}
func selectionColumnSQL(alias string, col driver.SelectionColumn, ref bool) string {
	switch col {
	case driver.SelectionID:
		if ref {
			return "c.record_key"
		}
		return alias + ".bead_id"
	case driver.SelectionType:
		return alias + ".bead_type"
	case driver.SelectionProject:
		return alias + ".project"
	case driver.SelectionStatus:
		return alias + ".status"
	case driver.SelectionTitle:
		return alias + ".title"
	case driver.SelectionRevision:
		return alias + ".revision_text"
	case driver.SelectionParent:
		return alias + ".parent_key"
	case driver.SelectionTemplate:
		return alias + ".template"
	case driver.SelectionPriority:
		return alias + ".priority"
	default:
		return "NULL"
	}
}

// The candidate identity comes from historical content even when current
// authority has no bead image. An authorization fact comparison to ID must
// therefore use c, not the nullable current-authority alias.
func selectionCandidateSQL(alias string, col driver.SelectionColumn, ref bool) string {
	if col == driver.SelectionID {
		if ref {
			return "c.record_key"
		}
		return "c.bead_id"
	}
	return selectionColumnSQL(alias, col, ref)
}
func selectionFactColumnSQL(col driver.SelectionFactColumn) string {
	switch col {
	case driver.SelectionFactRef:
		return "ref_key"
	case driver.SelectionFactTarget:
		return "target_key"
	case driver.SelectionFactLinkKind:
		return "link_kind"
	case driver.SelectionFactSubject:
		return "subject"
	case driver.SelectionFactScope:
		return "scope"
	case driver.SelectionFactScopeKind:
		return "scope_kind"
	case driver.SelectionFactProjectName:
		return "project_name"
	case driver.SelectionFactRole:
		return "role"
	case driver.SelectionFactName:
		return "name"
	case driver.SelectionFactValue:
		return "value"
	case driver.SelectionFactActive:
		return "active"
	case driver.SelectionFactDepth:
		return "depth"
	default:
		return "NULL"
	}
}
func stagedFactValue(f driver.SelectionFact, col driver.SelectionFactColumn) driver.SelectionValue {
	switch col {
	case driver.SelectionFactRef:
		if f.Ref != nil {
			return driver.SelectionRef(*f.Ref)
		}
	case driver.SelectionFactTarget:
		if f.Link != nil {
			return driver.SelectionRef(f.Link.To)
		}
		if f.Target != nil {
			return driver.SelectionRef(*f.Target)
		}
	case driver.SelectionFactLinkKind:
		if f.Link != nil {
			return driver.SelectionString(f.Link.Kind)
		}
	case driver.SelectionFactSubject:
		return driver.SelectionString(f.Subject)
	case driver.SelectionFactScope:
		return driver.SelectionString(f.Scope)
	case driver.SelectionFactScopeKind:
		return driver.SelectionString(string(f.ScopeKind))
	case driver.SelectionFactProjectName:
		if f.ProjectName != nil {
			return driver.SelectionString(*f.ProjectName)
		}
	case driver.SelectionFactRole:
		return driver.SelectionString(f.Role)
	case driver.SelectionFactName:
		return driver.SelectionString(f.Name)
	case driver.SelectionFactValue:
		return driver.SelectionString(f.Value)
	case driver.SelectionFactActive:
		return driver.SelectionBool(f.Active)
	case driver.SelectionFactDepth:
		return driver.SelectionInt(int64(f.Depth))
	}
	return driver.SelectionValue{}
}
func (b *sqliteSelectionSQL) exists(e *driver.SelectionExists, alias string, at uint64, committed bool) (string, error) {
	id := selectionFactID{row: keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &e.Row}), at: at, committed: committed}
	bound, ok := b.facts[id]
	if !ok {
		return "", fmt.Errorf("%w: SQLite selection fact was not preflighted", driver.ErrUnsupported)
	}
	if bound.overlay {
		if len(bound.staged) > 2048 {
			return "", fmt.Errorf("%w: SQLite staged fact vector too large", driver.ErrUnsupported)
		}
		terms := make([]string, 0, len(bound.staged))
		for _, fact := range bound.staged {
			checks := make([]string, 0, len(e.Tests))
			for _, test := range e.Tests {
				actual := stagedFactValue(fact, test.Column)
				if actual == (driver.SelectionValue{}) {
					checks = append(checks, "0")
					continue
				}
				var want string
				if test.Value != nil {
					want = b.arg(b.value(*test.Value))
				} else {
					want = selectionCandidateSQL(alias, test.Candidate, actual.Ref != nil)
				}
				checks = append(checks, "COALESCE("+want+"="+b.arg(b.value(actual))+",0)")
			}
			if len(checks) == 0 {
				return "1", nil
			}
			terms = append(terms, "("+strings.Join(checks, " AND ")+")")
		}
		if len(terms) == 0 {
			return "0", nil
		}
		return "(" + strings.Join(terms, " OR ") + ")", nil
	}
	checks := make([]string, 0, len(e.Tests)+5)
	checks = append(checks, "f.incarnation="+b.arg(b.meta.Incarnation), "f.branch="+b.arg(revKey(0)), "f.row_key="+b.arg(id.row), "f.start_revision="+b.arg(bound.start))
	if e.Kind == driver.SelectionScopedGrantFact {
		// The literal makes the scoped-only partial index eligible. The fact
		// envelope is also checked before SQL, including the empty-set case.
		checks = append(checks, "f.kind='scoped_grant_v1'")
	} else {
		checks = append(checks, "f.kind="+b.arg(string(e.Kind)))
	}
	for _, test := range e.Tests {
		var want string
		if test.Value != nil {
			want = b.arg(b.value(*test.Value))
		} else {
			want = selectionCandidateSQL(alias, test.Candidate, test.Column == driver.SelectionFactRef || test.Column == driver.SelectionFactTarget)
		}
		// A bare equality remains an indexable constraint. SQL NULL cannot
		// satisfy EXISTS, which matches the closed optional-value semantics.
		checks = append(checks, "f."+selectionFactColumnSQL(test.Column)+"="+want)
	}
	return "EXISTS(SELECT 1 FROM selection_facts AS f WHERE " + strings.Join(checks, " AND ") + ")", nil
}

// Source predicates read the existing temporal Parent and outgoing-link
// indexes. No second edge relation or source Body enters the selector.
func (b *sqliteSelectionSQL) sourceExpr(e driver.SelectionExpr, at uint64, committed bool) (string, error) {
	if len(e.Sources.Refs) == 0 || e.Op == driver.SelectionIncomingOp && len(e.Sources.Kinds) == 0 {
		return "0", nil
	}
	refs := map[string]bool{}
	for _, ref := range e.Sources.Refs {
		refs[b.refValue(ref)] = true
	}
	refKeys := make([]string, 0, len(refs))
	for id := range refs {
		refKeys = append(refKeys, id)
	}
	sort.Strings(refKeys)
	var committedSQL string
	if e.Op == driver.SelectionChildrenOp {
		committedSQL = `EXISTS(SELECT 1 FROM hierarchy_source_history AS h WHERE h.incarnation=` + b.arg(b.meta.Incarnation) +
			` AND h.branch=` + b.arg(revKey(0)) + ` AND h.child_key=c.record_key AND h.parent_key IN (`
		boundRefs := make([]string, 0, len(refKeys))
		for _, id := range refKeys {
			boundRefs = append(boundRefs, b.arg(id))
		}
		committedSQL += strings.Join(boundRefs, ",") +
			`) AND h.start_revision<=` + b.arg(revKey(at)) + ` AND (h.end_revision IS NULL OR h.end_revision>` + b.arg(revKey(at)) + `))`
	} else {
		kinds := map[string]bool{}
		for _, kind := range e.Sources.Kinds {
			kinds[string(kind)] = true
		}
		kindKeys := make([]string, 0, len(kinds))
		for kind := range kinds {
			kindKeys = append(kindKeys, kind)
		}
		sort.Strings(kindKeys)
		committedSQL = `EXISTS(SELECT 1 FROM graph_link_history AS l JOIN graph_link_source_history AS h
			ON h.incarnation=l.incarnation AND h.branch=l.branch AND h.source_key=l.source_key AND h.start_revision=l.start_revision
			WHERE l.incarnation=` + b.arg(b.meta.Incarnation) + ` AND l.branch=` + b.arg(revKey(0)) +
			` AND l.source_key=c.record_key AND l.target_key IN (`
		boundRefs := make([]string, 0, len(refKeys))
		for _, id := range refKeys {
			boundRefs = append(boundRefs, b.arg(id))
		}
		committedSQL += strings.Join(boundRefs, ",") + `) AND l.link_kind IN (`
		boundKinds := make([]string, 0, len(kindKeys))
		for _, kind := range kindKeys {
			boundKinds = append(boundKinds, b.arg([]byte(kind)))
		}
		committedSQL += strings.Join(boundKinds, ",") +
			`) AND h.start_revision<=` + b.arg(revKey(at)) + ` AND (h.end_revision IS NULL OR h.end_revision>` + b.arg(revKey(at)) + `))`
	}
	if committed || b.owner == nil || at != mustUint(b.meta.Head) || len(b.owner.records) == 0 {
		return committedSQL, nil
	}
	shadow := make([]string, 0, len(b.owner.records))
	staged := make([]string, 0)
	for id, record := range b.owner.records {
		shadow = append(shadow, id)
		if record == nil || record.Kind != "bead" {
			continue
		}
		declaration := b.owner.sourceOverlay.Declarations[id]
		match := false
		if e.Op == driver.SelectionChildrenOp {
			match = refs[declaration.ParentKey]
		} else {
			for _, link := range declaration.Links {
				if refs[b.refValue(link.To)] {
					for _, kind := range e.Sources.Kinds {
						if link.Kind == string(kind) {
							match = true
							break
						}
					}
				}
			}
		}
		if match {
			staged = append(staged, id)
		}
	}
	sort.Strings(shadow)
	sort.Strings(staged)
	shadowArgs := make([]string, 0, len(shadow))
	for _, id := range shadow {
		shadowArgs = append(shadowArgs, b.arg(id))
	}
	committedSQL = "(" + committedSQL + " AND c.record_key NOT IN (" + strings.Join(shadowArgs, ",") + "))"
	if len(staged) == 0 {
		return committedSQL, nil
	}
	stagedArgs := make([]string, 0, len(staged))
	for _, id := range staged {
		stagedArgs = append(stagedArgs, b.arg(id))
	}
	return "(" + committedSQL + " OR c.record_key IN (" + strings.Join(stagedArgs, ",") + "))", nil
}
func (b *sqliteSelectionSQL) expr(e driver.SelectionExpr, alias string, at uint64, committed bool) (string, error) {
	switch e.Op {
	case driver.SelectionTrue:
		return "1", nil
	case driver.SelectionFalse:
		return "0", nil
	case driver.SelectionResourceExists:
		return alias + ".record_key IS NOT NULL", nil
	case driver.SelectionAll, driver.SelectionAny:
		parts := make([]string, 0, len(e.Args))
		for _, child := range e.Args {
			part, err := b.expr(child, alias, at, committed)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		if len(parts) == 0 {
			if e.Op == driver.SelectionAll {
				return "1", nil
			}
			return "0", nil
		}
		join := " AND "
		if e.Op == driver.SelectionAny {
			join = " OR "
		}
		return "(" + strings.Join(parts, join) + ")", nil
	case driver.SelectionNot:
		part, err := b.expr(e.Args[0], alias, at, committed)
		return "NOT (" + part + ")", err
	case driver.SelectionCompareOp:
		c := e.Compare
		column := selectionCandidateSQL(alias, c.Column, c.Column == driver.SelectionParent)
		if c.Op == "absent" {
			return column + " IS NULL", nil
		}
		if c.Op == "contains" {
			return "COALESCE(instr(" + column + "," + b.arg(*c.Value.String) + ")>0,0)", nil
		}
		return "COALESCE(" + column + "=" + b.arg(b.value(*c.Value)) + ",0)", nil
	case driver.SelectionRefs:
		seen := map[string]bool{}
		for _, ref := range e.Refs {
			seen[b.refValue(ref)] = true
		}
		if len(seen) == 0 {
			return "0", nil
		}
		keys := make([]string, 0, len(seen))
		for key := range seen {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		items := make([]string, 0, len(keys))
		for _, key := range keys {
			items = append(items, b.arg(key))
		}
		return "c.record_key IN (" + strings.Join(items, ",") + ")", nil
	case driver.SelectionExistsOp:
		return b.exists(e.Exists, alias, at, committed)
	case driver.SelectionChildrenOp, driver.SelectionIncomingOp:
		return b.sourceExpr(e, at, committed)
	default:
		return "", fmt.Errorf("%w: SQLite selection unknown expression", driver.ErrUnsupported)
	}
}

const selectionColumns = "record_key,bead_id,bead_type,project,status,title,template,parent_key,priority,membership_leaf,revision_text"

func selectionNulls(n int) string { return strings.TrimSuffix(strings.Repeat("NULL,", n), ",") }

// A positive refs term in an AND branch is a safe candidate seed. The final
// expression still runs on every driven candidate, so other terms and
// authorization cannot be lost. OR and NOT cannot provide that guarantee.
func selectionRequiredRefs(e driver.SelectionExpr) map[string]bool {
	if e.Op == driver.SelectionRefs {
		out := make(map[string]bool, len(e.Refs))
		for _, ref := range e.Refs {
			out[keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})] = true
		}
		return out
	}
	if e.Op != driver.SelectionAll {
		return nil
	}
	var smallest map[string]bool
	for _, child := range e.Args {
		candidate := selectionRequiredRefs(child)
		if candidate != nil && (smallest == nil || len(candidate) < len(smallest)) {
			smallest = candidate
		}
	}
	return smallest
}

func (b *sqliteSelectionSQL) source(name string, at uint64, overlay bool, required map[string]bool) (string, error) {
	from := "selection_bead_history"
	if required != nil {
		// This index turns sparse refs into point lookups rather than a
		// selection-wide temporal scan before the authorization predicate.
		from += " INDEXED BY selection_bead_history_record"
	}
	base := "SELECT " + selectionColumns + " FROM " + from + " WHERE incarnation=" + b.arg(b.meta.Incarnation) + " AND branch=" + b.arg(revKey(0)) + " AND start_revision<=" + b.arg(revKey(at)) + " AND (end_revision IS NULL OR end_revision>" + b.arg(revKey(at)) + ")"
	if required != nil {
		if len(required) == 0 {
			base += " AND 0"
		} else {
			keys := make([]string, 0, len(required))
			for id := range required {
				keys = append(keys, id)
			}
			sort.Strings(keys)
			bound := make([]string, 0, len(keys))
			for _, id := range keys {
				bound = append(bound, b.arg(id))
			}
			base += " AND record_key IN (" + strings.Join(bound, ",") + ")"
		}
	}
	if !overlay || b.owner == nil || len(b.owner.derived) == 0 {
		return name + " AS (" + base + ")", nil
	}
	keys := make([]string, 0)
	for id, state := range b.owner.derived {
		if state.Key.Derived != nil && state.Key.Derived.Table == driver.DerivedSortKey {
			keys = append(keys, id)
		}
	}
	if len(keys) == 0 {
		return name + " AS (" + base + ")", nil
	}
	if len(keys) > 2048 {
		return "", fmt.Errorf("%w: SQLite staged selection vector too large", driver.ErrUnsupported)
	}
	sort.Strings(keys)
	shadow := make([]string, 0, len(keys))
	for _, id := range keys {
		shadow = append(shadow, b.arg(id))
	}
	base += " AND derived_key NOT IN (" + strings.Join(shadow, ",") + ")"
	stagedRows := make([]string, 0, len(keys))
	for _, id := range keys {
		state := b.owner.derived[id]
		if !state.Present || state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Bead == nil {
			continue
		}
		index := state.Derived.Index
		bead := index.Bead
		if required != nil && !required[b.refValue(bead.Ref)] {
			continue
		}
		var project, parent any
		var status any = bead.Status
		var title any = bead.Title
		var template any = bead.Template
		var priority any = bead.Priority
		for _, absent := range bead.Absent {
			switch absent {
			case driver.SelectionStatus:
				status = nil
			case driver.SelectionTitle:
				title = nil
			case driver.SelectionTemplate:
				template = nil
			case driver.SelectionPriority:
				priority = nil
			}
		}
		if bead.Project != nil {
			project = *bead.Project
		}
		if bead.Parent != nil {
			parent = b.refValue(*bead.Parent)
		}
		values := []any{b.refValue(bead.Ref), bead.Ref.ID, bead.Type, project, status, title, template, parent, priority, bead.MembershipLeaf, index.Source.Revision.String()}
		columns := make([]string, len(values))
		for i, value := range values {
			columns[i] = b.arg(value)
		}
		stagedRows = append(stagedRows, "("+strings.Join(columns, ",")+")")
	}
	if len(stagedRows) == 0 {
		return name + " AS (" + base + ")", nil
	}
	// A VALUES table is one compound term regardless of the staged row
	// count. One SELECT per bead hits SQLite's 500-term compound limit well
	// below the admitted staged vector bound.
	return name + " AS (" + base + " UNION ALL SELECT * FROM (VALUES " + strings.Join(stagedRows, ",") + "))", nil
}

func selectionOrderSQL(order []driver.SelectionOrder) string {
	parts := make([]string, 0, len(order)+1)
	for _, item := range order {
		col := selectionColumnSQL("capped", item.Column, false)
		if item.Column == driver.SelectionProject || item.Column == driver.SelectionStatus || item.Column == driver.SelectionTitle {
			col = "COALESCE(" + col + ",'')"
		} else if item.Column == driver.SelectionPriority {
			col = "COALESCE(" + col + ",0)"
		}
		if item.Column != driver.SelectionPriority {
			col += " COLLATE BINARY"
		}
		if item.Desc {
			col += " DESC"
		}
		parts = append(parts, col)
	}
	parts = append(parts, "capped.bead_id COLLATE BINARY")
	return strings.Join(parts, ",")
}

func (b *sqliteSelectionSQL) query(p driver.SelectionPlan) (string, error) {
	c, _ := p.ContentRevision.Uint64()
	a, _ := p.AuthorityRevision.Uint64()
	content, err := b.source("content", c, c == a && !p.ContentCommitted, selectionRequiredRefs(p.Membership))
	if err != nil {
		return "", err
	}
	authority, err := b.source("authority", a, true, nil)
	if err != nil {
		return "", err
	}
	excluded := "1"
	if len(p.Excluded) > 0 {
		seen := map[string]bool{}
		for _, ref := range p.Excluded {
			seen[b.refValue(ref)] = true
		}
		keys := make([]string, 0, len(seen))
		for key := range seen {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		args := make([]string, 0, len(keys))
		for _, key := range keys {
			args = append(args, b.arg(key))
		}
		excluded = "c.record_key NOT IN (" + strings.Join(args, ",") + ")"
	}
	// SQLite's anonymous parameters bind in statement order, not in the order
	// independent SQL fragments were built. Exclusions occur before both ASTs.
	member, err := b.expr(p.Membership, "c", c, p.ContentCommitted)
	if err != nil {
		return "", err
	}
	authorized, err := b.expr(p.Authorization, "a", a, false)
	if err != nil {
		return "", err
	}
	boundary, err := p.Boundary.Description()
	if err != nil {
		return "", err
	}
	if boundary.HostExpression != nil {
		host, err := b.expr(*boundary.HostExpression, "a", a, false)
		if err != nil {
			return "", err
		}
		authorized = "(" + authorized + ") AND (" + host + ")"
	}
	ctes := []string{content, authority, "capped AS MATERIALIZED (SELECT c.* FROM content AS c LEFT JOIN authority AS a ON a.record_key=c.record_key WHERE (" + excluded + ") AND (" + member + ") AND (" + authorized + ") LIMIT " + b.arg(p.EffectiveCap()+1) + ")"}
	if p.Mode == driver.SelectionCountMode {
		return "WITH " + strings.Join(ctes, ",") + " SELECT 0,(SELECT COUNT(*) FROM capped)," + selectionNulls(15), nil
	}
	if p.Mode == driver.SelectionTypeSummary {
		return "WITH " + strings.Join(ctes, ",") + " SELECT 0,(SELECT COUNT(*) FROM capped)," + selectionNulls(15) + " UNION ALL SELECT 1,NULL,bead_type,COUNT(*),0," + selectionNulls(12) + " FROM capped GROUP BY bead_type ORDER BY 1,3", nil
	}
	ctes = append(ctes, "ordered AS MATERIALIZED (SELECT capped.*,ROW_NUMBER() OVER (ORDER BY "+selectionOrderSQL(p.Order)+") AS rn FROM capped)")
	start := ""
	if p.Page.After != nil {
		key := p.Page.After.Key
		terms := []string{"o.record_key=" + b.arg(b.refValue(key.Ref)), "o.revision_text=" + b.arg(key.Revision.String()), "o.membership_leaf=" + b.arg(key.MembershipLeaf)}
		for i, o := range p.Order {
			col := selectionColumnSQL("o", o.Column, false)
			if o.Column == driver.SelectionProject || o.Column == driver.SelectionStatus || o.Column == driver.SelectionTitle {
				col = "COALESCE(" + col + ",'')"
			} else if o.Column == driver.SelectionPriority {
				col = "COALESCE(" + col + ",0)"
			}
			terms = append(terms, col+"="+b.arg(b.value(key.Order[i])))
		}
		start = "(SELECT rn FROM ordered AS o WHERE " + strings.Join(terms, " AND ") + " LIMIT 1)"
	} else {
		start = b.arg(int64(p.Page.Offset))
	}
	ctes = append(ctes, "page_start AS (SELECT "+start+" AS first)", "page_end AS (SELECT first,first+"+b.arg(p.Page.Limit)+" AS last FROM page_start)")
	// The first row always carries total/start, even for an empty page. Group
	// rows and thin emitted keys are derived from the SAME bounded authorized
	// CTE; no body or lookahead payload enters this query.
	meta := "SELECT 0,(SELECT COUNT(*) FROM capped),NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,first FROM page_end"
	groups := ""
	if p.GroupBy == driver.SelectionGroupType {
		groups = " UNION ALL SELECT 1,NULL,o.bead_type,COUNT(*),SUM(CASE WHEN o.rn>p.last THEN 1 ELSE 0 END),NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL FROM ordered AS o CROSS JOIN page_end AS p GROUP BY o.bead_type"
	}
	keys := " UNION ALL SELECT 2,NULL,NULL,NULL,NULL,o.record_key,o.bead_id,o.bead_type,o.project,o.status,o.title,o.template,o.parent_key,o.priority,o.membership_leaf,o.revision_text,o.rn FROM ordered AS o CROSS JOIN page_end AS p WHERE o.rn>p.first AND o.rn<=p.last"
	return "WITH " + strings.Join(ctes, ",") + " " + meta + groups + keys + " ORDER BY 1,3,17", nil
}

// The fresh terminal fence asks only whether one committed candidate exists.
// It shares the eager coverage/source/fact preflight with Select, but has no
// authorized candidate cap, grouping, page or staged overlay.
func (b *sqliteSelectionSQL) existsQuery(p driver.SelectionPlan) (string, error) {
	at, _ := p.ContentRevision.Uint64()
	content, err := b.source("content", at, false, selectionRequiredRefs(p.Membership))
	if err != nil {
		return "", err
	}
	authority, err := b.source("authority", at, false, nil)
	if err != nil {
		return "", err
	}
	member, err := b.expr(p.Membership, "c", at, true)
	if err != nil {
		return "", err
	}
	authorized, err := b.expr(p.Authorization, "a", at, false)
	if err != nil {
		return "", err
	}
	boundary, err := p.Boundary.Description()
	if err != nil {
		return "", err
	}
	if boundary.HostExpression != nil {
		host, err := b.expr(*boundary.HostExpression, "a", at, false)
		if err != nil {
			return "", err
		}
		authorized = "(" + authorized + ") AND (" + host + ")"
	}
	return "WITH " + content + "," + authority +
		" SELECT EXISTS(SELECT 1 FROM content AS c LEFT JOIN authority AS a ON a.record_key=c.record_key WHERE (" +
		member + ") AND (" + authorized + ") LIMIT 1)", nil
}
