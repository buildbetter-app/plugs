package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type selectionPlane struct {
	at      uint64
	overlay bool
}

type selectionCompiler struct {
	reader *reader
	planes map[string]selectionPlane
	args   []any
	ctes   []string
	refs   map[string]string
}

func (c *selectionCompiler) bind(v any) string {
	c.args = append(c.args, v)
	return "$" + strconv.Itoa(len(c.args))
}
func (c *selectionCompiler) interval(alias string, at uint64) string {
	p := c.bind(decimal(at))
	return alias + `.start_revision<=` + p + ` AND (` + alias + `.end_revision IS NULL OR ` + alias + `.end_revision>` + p + `)`
}
func (c *selectionCompiler) relation(r *reader, name string, at uint64, overlay bool) error {
	c.planes[name] = selectionPlane{at: at, overlay: overlay}
	scope := c.bind(r.meta.Incarnation)
	interval := c.interval("s", at)
	shadow := [][]byte{}
	staged := []selectionDBRow{}
	facts := []selectionDBFact{}
	if overlay && r.writer != nil && at == r.at {
		for id, s := range r.writer.derived {
			shadow = append(shadow, []byte(id))
			if !s.Present || s.Derived == nil || s.Derived.Index == nil {
				continue
			}
			row, err := selectionRow(id, *s.Derived)
			if err != nil {
				return err
			}
			staged = append(staged, row)
			if s.Derived.Index.Facts != nil {
				facts = append(facts, selectionFactsRows(id, s.Derived.Index.Facts)...)
			}
		}
	}
	exclude := ""
	if len(shadow) > 0 {
		exclude = ` AND NOT(s.row_key=ANY(` + c.bind(shadow) + `::bytea[]))`
	}
	base := `SELECT ` + selectionColumns + ` FROM plug_postgres.selection_rows s WHERE s.incarnation=` + scope + ` AND s.layout=2 AND ` + interval + exclude
	if len(staged) > 0 {
		base += ` UNION ALL SELECT ` + selectionJSONColumns + ` FROM jsonb_to_recordset(` + c.bind(selectionJSON(staged)) + `::jsonb) AS p(` + selectionJSONLayout + `)`
	}
	c.ctes = append(c.ctes, name+`_rows AS NOT MATERIALIZED (`+base+`)`)
	f := `SELECT ` + prefixSelectionFactColumns("f") + ` FROM plug_postgres.selection_facts f JOIN plug_postgres.selection_rows s ON s.incarnation=f.incarnation AND s.row_key=f.row_key AND s.start_revision=f.start_revision WHERE s.incarnation=` + scope + ` AND s.layout=2 AND ` + interval + exclude
	if len(facts) > 0 {
		f += ` UNION ALL SELECT ` + selectionFactJSONColumns + ` FROM jsonb_to_recordset(` + c.bind(selectionJSON(facts)) + `::jsonb) AS p(` + selectionFactJSONLayout + `)`
	}
	c.ctes = append(c.ctes, name+`_facts AS NOT MATERIALIZED (`+f+`)`)
	return nil
}

// A finite necessary-ref set is a safe driving domain, never a substitute for
// the complete membership/authorization predicates below. OR with any open
// branch stays unbounded; NOT never manufactures a positive ref restriction.
func selectionNecessaryRefs(e driver.SelectionExpr) map[string]bool {
	switch e.Op {
	case driver.SelectionFalse:
		return map[string]bool{}
	case driver.SelectionRefs:
		refs := map[string]bool{}
		for _, ref := range e.Refs {
			refs[selectionRefKey(ref)] = true
		}
		return refs
	case driver.SelectionAll:
		var refs map[string]bool
		for _, child := range e.Args {
			refs = intersectSelectionRefs(refs, selectionNecessaryRefs(child))
		}
		return refs
	case driver.SelectionAny:
		refs := map[string]bool{}
		for _, child := range e.Args {
			next := selectionNecessaryRefs(child)
			if next == nil {
				return nil
			}
			for key := range next {
				refs[key] = true
				if len(refs) > driver.SelectionMaxNodes {
					return nil // avoid an expanded extra request-ref transport
				}
			}
		}
		return refs
	default:
		return nil
	}
}
func intersectSelectionRefs(left, right map[string]bool) map[string]bool {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	for key := range left {
		if !right[key] {
			delete(left, key)
		}
	}
	return left
}
func (c *selectionCompiler) requestRefs(ordered []string) string {
	var identity strings.Builder
	for _, key := range ordered {
		identity.WriteString(strconv.Itoa(len(key)))
		identity.WriteByte(':')
		identity.WriteString(key)
	}
	fingerprint := identity.String()
	if name := c.refs[fingerprint]; name != "" {
		return name
	}
	name := fmt.Sprintf("request_refs_%d", len(c.refs))
	c.refs[fingerprint] = name
	keys := make([][]byte, 0, len(ordered))
	for _, key := range ordered {
		keys = append(keys, []byte(key))
	}
	c.ctes = append(c.ctes, name+` AS MATERIALIZED (SELECT DISTINCT key FROM unnest(`+c.bind(keys)+`::bytea[]) AS refs(key))`)
	return name
}

func prefixSelectionFactColumns(alias string) string {
	cols := strings.Split(selectionFactColumns, ",")
	for i := range cols {
		cols[i] = alias + "." + cols[i]
	}
	return strings.Join(cols, ",")
}
func selectionColumn(alias string, col driver.SelectionColumn, order bool) string {
	var s string
	switch col {
	case driver.SelectionID:
		s = "c.bead_id" // identity survives a missing current authority image
	case driver.SelectionType:
		s = alias + ".bead_type"
	case driver.SelectionProject:
		s = alias + ".project"
	case driver.SelectionStatus:
		s = alias + ".status"
	case driver.SelectionTitle:
		s = alias + ".title"
	case driver.SelectionRevision:
		s = "convert_to(" + alias + ".source_revision::text,'UTF8')"
	case driver.SelectionParent:
		s = alias + ".parent_key"
	case driver.SelectionTemplate:
		s = alias + ".template_value"
	case driver.SelectionPriority:
		s = alias + ".priority"
	}
	if order {
		if col == driver.SelectionPriority {
			s = "coalesce(" + s + ",0)"
		} else {
			s = "coalesce(" + s + ",''::bytea)"
		}
	}
	return s
}
func (c *selectionCompiler) value(v driver.SelectionValue) string {
	switch {
	case v.String != nil:
		return c.bind([]byte(*v.String)) + "::bytea"
	case v.Ref != nil:
		return c.bind([]byte(selectionRefKey(*v.Ref))) + "::bytea"
	case v.Bool != nil:
		return c.bind(*v.Bool) + "::boolean"
	case v.Int != nil:
		return c.bind(*v.Int) + "::bigint"
	}
	return "NULL"
}
func (c *selectionCompiler) expression(e driver.SelectionExpr, alias, domain string) string {
	switch e.Op {
	case driver.SelectionTrue:
		return "TRUE"
	case driver.SelectionFalse:
		return "FALSE"
	case driver.SelectionResourceExists:
		return alias + ".bead_key IS NOT NULL"
	case driver.SelectionAll, driver.SelectionAny:
		parts := make([]string, 0, len(e.Args))
		for _, a := range e.Args {
			parts = append(parts, "("+c.expression(a, alias, domain)+")")
		}
		if len(parts) == 0 {
			if e.Op == driver.SelectionAll {
				return "TRUE"
			}
			return "FALSE"
		}
		sep := " AND "
		if e.Op == driver.SelectionAny {
			sep = " OR "
		}
		return strings.Join(parts, sep)
	case driver.SelectionNot:
		return "NOT(" + c.expression(e.Args[0], alias, domain) + ")"
	case driver.SelectionCompareOp:
		x := e.Compare
		col := selectionColumn(alias, x.Column, false)
		if x.Op == "absent" {
			return col + " IS NULL"
		}
		value := c.value(*x.Value)
		if x.Op == "contains" {
			return "coalesce(plug_postgres.strpos(" + col + "," + value + ")>0,FALSE)"
		}
		return "coalesce(" + col + "=" + value + ",FALSE)"
	case driver.SelectionRefs:
		ids := map[string]bool{}
		for _, r := range e.Refs {
			ids[selectionRefKey(r)] = true
		}
		ordered := make([]string, 0, len(ids))
		for k := range ids {
			ordered = append(ordered, k)
		}
		sort.Strings(ordered)
		name := c.requestRefs(ordered)
		return `EXISTS(SELECT 1 FROM ` + name + ` req WHERE req.key=c.bead_key)`
	case driver.SelectionChildrenOp, driver.SelectionIncomingOp:
		return c.sourceExpression(e, alias, domain)
	case driver.SelectionExistsOp:
		x := e.Exists
		key, _ := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &x.Row})
		parts := []string{"f.row_key=" + c.bind([]byte(key)) + "::bytea"}
		for _, t := range x.Tests {
			col := "f." + string(t.Column)
			switch t.Column {
			case driver.SelectionFactRef:
				col = "f.ref_key"
			case driver.SelectionFactTarget:
				col = "f.target_key"
			}
			var operand string
			if t.Value != nil {
				operand = c.value(*t.Value)
			} else if t.Candidate == driver.SelectionID && (t.Column == driver.SelectionFactRef || t.Column == driver.SelectionFactTarget) {
				operand = "c.bead_key"
			} else {
				operand = selectionColumn(alias, t.Candidate, false)
			}
			parts = append(parts, col+"="+operand)
		}
		return `EXISTS(SELECT 1 FROM ` + domain + `_facts f WHERE ` + strings.Join(parts, " AND ") + `)`
	}
	return "FALSE" // Validate rejects all unknown nodes before compilation.
}

// The envelope preflight is eager across ALL expression branches. Even an
// empty page/false expression cannot turn missing fact coverage into permission.
func (r *reader) selectionFactCoverage(ctx context.Context, e driver.SelectionExpr, at uint64, overlay bool) error {
	wanted := map[string]driver.SelectionFactKind{}
	var visit func(driver.SelectionExpr) error
	visit = func(e driver.SelectionExpr) error {
		if e.Exists != nil {
			k, _ := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &e.Exists.Row})
			if kind, ok := wanted[k]; ok && kind != e.Exists.Kind {
				return selectionUnsupported("conflicting fact domains")
			}
			wanted[k] = e.Exists.Kind
		}
		for _, a := range e.Args {
			if err := visit(a); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(e); err != nil {
		return err
	}
	if len(wanted) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(wanted))
	for k := range wanted {
		keys = append(keys, []byte(k))
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT row_key,fact_kind,source_key,source_revision::text,source_present,source_digest,family_table,source_kind FROM plug_postgres.selection_rows WHERE incarnation=$1 AND layout=2 AND row_key=ANY($2::bytea[]) AND start_revision<=$3 AND (end_revision IS NULL OR end_revision>$3)`, r.meta.Incarnation, keys, decimal(at))
	if err != nil {
		return err
	}
	found := map[string]selectionDBRow{}
	for rows.Next() {
		var key, source, digest []byte
		var kind *string
		var rev, table, sourceKind string
		var present bool
		if err = rows.Scan(&key, &kind, &source, &rev, &present, &digest, &table, &sourceKind); err != nil {
			break
		}
		found[string(key)] = selectionDBRow{FamilyTable: table, SourceKind: sourceKind, FactKind: kind, SourceKey: selectionHex(string(source)), SourceRevision: rev, SourcePresent: present, SourceDigest: selectionHex(string(digest))}
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return err
	}
	if overlay && r.writer != nil && at == r.at {
		for id, s := range r.writer.derived {
			if _, needed := wanted[id]; !needed {
				continue
			}
			delete(found, id)
			if s.Present && s.Derived != nil && s.Derived.Index != nil {
				row, err := selectionRow(id, *s.Derived)
				if err != nil {
					return err
				}
				found[id] = row
			}
		}
	}
	sources := map[string]bool{}
	var stagedValidity map[driver.SelectionProjectionFamily]bool
	for id, kind := range wanted {
		x, ok := found[id]
		if !ok || x.FactKind == nil || *x.FactKind != string(kind) {
			return selectionUnsupported("complete fact domain unavailable")
		}
		if x.SourceKind == string(driver.LockRecord) {
			if family, core := selectionCoreFamily(x.FamilyTable, string(kind)); core {
				if err := r.requireSelectionCoverage(ctx, family, at); err != nil {
					return err
				}
				if overlay && r.writer != nil && at == r.at {
					if stagedValidity == nil {
						var err error
						stagedValidity, err = r.selectionFamilyValidity(ctx, at, true)
						if err != nil {
							return err
						}
					}
					if !stagedValidity[family] {
						return selectionUnsupported("incomplete staged fact family")
					}
				}
			}
		}
		raw, _ := hex.DecodeString(x.SourceKey)
		sources[string(raw)] = true
	}
	sourceKeys := make([][]byte, 0, len(sources))
	for k := range sources {
		sourceKeys = append(sourceKeys, []byte(k))
	}
	state, err := r.selectionSourceStates(ctx, sourceKeys, at, overlay)
	if err != nil {
		return err
	}

	for _, x := range found {
		raw, _ := hex.DecodeString(x.SourceKey)
		h, ok := state[string(raw)]
		if !ok {
			h.Revision = "0"
		}
		digest, _ := hex.DecodeString(x.SourceDigest)
		if h.Revision != x.SourceRevision || h.Present != x.SourcePresent || h.Digest != string(digest) {
			return selectionUnsupported("fact source changed")
		}
	}
	return nil
}

func (r *reader) Select(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	zero := driver.SelectionResult{}
	if err := r.check(ctx); err != nil {
		return zero, err
	}
	if err := p.Validate(); err != nil {
		return zero, err
	}
	if p.Incarnation != r.meta.Incarnation {
		return zero, driver.ErrIncarnationChanged{From: p.Incarnation, To: r.meta.Incarnation}
	}
	a, _ := p.AuthorityRevision.Uint64()
	at, _ := p.ContentRevision.Uint64()
	head, _ := r.meta.Head.Uint64()
	floor, _ := r.meta.HistoryFloor.Uint64()
	if a != r.at || a != head {
		return zero, selectionUnsupported("requires bound current authority")
	}
	if at < floor {
		return zero, driver.ErrTruncated{Floor: r.meta.HistoryFloor}
	}
	for _, rev := range []uint64{at, a} {
		ready, err := r.selectionCoverage(ctx, rev)
		if err != nil {
			return zero, err
		}
		if !ready {
			return zero, selectionUnsupported("complete bead coverage unavailable")
		}
	}
	if r.writer != nil {
		covered, err := r.writer.selectionOverlayCovered(ctx)
		if err != nil {
			return zero, err
		}
		if !covered {
			return zero, selectionUnsupported("incomplete staged bead domain")
		}
	}
	boundary, err := p.Boundary.Description()
	if err != nil {
		return zero, err
	}
	if err = r.selectionRequireSources(ctx, boundary.Sources, a, true); err != nil {
		return zero, err
	}
	contentOverlay := !p.ContentCommitted && at == a
	checks := []struct {
		expr    driver.SelectionExpr
		at      uint64
		overlay bool
	}{{p.Membership, at, contentOverlay}, {p.Authorization, a, true}}
	if boundary.HostExpression != nil {
		checks = append(checks, struct {
			expr    driver.SelectionExpr
			at      uint64
			overlay bool
		}{*boundary.HostExpression, a, true})
	}
	for _, check := range checks {
		if err = r.selectionRequireGraphDomains(ctx, check.expr, check.at); err != nil {
			return zero, err
		}
		if err = r.selectionRequireDeclarations(check.expr, check.at, check.overlay); err != nil {
			return zero, err
		}
		if err = r.selectionFactCoverage(ctx, check.expr, check.at, check.overlay); err != nil {
			return zero, err
		}
	}
	query, args, err := r.compileSelection(p)
	if err != nil {
		return zero, err
	}
	return r.runSelection(ctx, p, query, args)
}

// Build relations once per content plane. Authority always retains the current
// transaction overlay, independently of an equal-token committed content plane.
func (r *reader) selectionCompiler(p driver.SelectionPlan, committed bool) (*selectionCompiler, error) {
	c := &selectionCompiler{reader: r, refs: map[string]string{}, planes: map[string]selectionPlane{}}
	at, _ := p.ContentRevision.Uint64()
	a, _ := p.AuthorityRevision.Uint64()
	contentOverlay := !committed && !p.ContentCommitted && at == a
	authorityOverlay := !committed
	if err := c.relation(r, "content", at, contentOverlay); err != nil {
		return nil, err
	}
	if at == a && contentOverlay == authorityOverlay {
		c.planes["authority"] = selectionPlane{at: a, overlay: authorityOverlay}
		c.ctes = append(c.ctes, `authority_rows AS NOT MATERIALIZED (SELECT * FROM content_rows)`, `authority_facts AS NOT MATERIALIZED (SELECT * FROM content_facts)`)
	} else if err := c.relation(r, "authority", a, authorityOverlay); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *selectionCompiler) selectionMatch(p driver.SelectionPlan) (string, string, error) {
	boundary, err := p.Boundary.Description()
	if err != nil {
		return "", "", err
	}
	member := c.expression(p.Membership, "c", "content")
	allowed := c.expression(p.Authorization, "a", "authority")
	refs := intersectSelectionRefs(selectionNecessaryRefs(p.Membership), selectionNecessaryRefs(p.Authorization))
	if boundary.HostExpression != nil {
		allowed = "(" + allowed + ") AND (" + c.expression(*boundary.HostExpression, "a", "authority") + ")"
		refs = intersectSelectionRefs(refs, selectionNecessaryRefs(*boundary.HostExpression))
	}
	from := "content_rows c"
	if refs != nil {
		keys := make([]string, 0, len(refs))
		for key := range refs {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		from = c.requestRefs(keys) + " driving_refs JOIN content_rows c ON c.bead_key=driving_refs.key"
	}
	where := "c.bead_key IS NOT NULL AND (" + member + ") AND (" + allowed + ")"
	if len(p.Excluded) > 0 {
		keys := make([][]byte, 0, len(p.Excluded))
		for _, ref := range p.Excluded {
			keys = append(keys, []byte(selectionRefKey(ref)))
		}
		where += " AND NOT(c.bead_key=ANY(" + c.bind(keys) + "::bytea[]))"
	}
	return from + " LEFT JOIN authority_rows a ON a.bead_key=c.bead_key AND a.bead_key IS NOT NULL", where, nil
}

func (r *reader) compileSelection(p driver.SelectionPlan) (string, []any, error) {
	c, err := r.selectionCompiler(p, false)
	if err != nil {
		return "", nil, err
	}
	from, where, err := c.selectionMatch(p)
	if err != nil {
		return "", nil, err
	}
	cap := p.EffectiveCap()
	projection := "c.*"
	if p.Page == nil {
		// Aggregate transport never materializes unused title/order/leaf values.
		projection = "c.bead_type"
	}
	c.ctes = append(c.ctes, `matched AS MATERIALIZED (SELECT `+projection+` FROM `+from+` WHERE `+where+` LIMIT `+strconv.Itoa(cap+1)+`)`, `totals AS (SELECT count(*) total FROM matched)`)
	start := "0"
	end := "0"
	domain := "matched"
	suffix := "0::bigint"
	if p.Page != nil {
		order := make([]string, 0, len(p.Order)+1)
		for _, o := range p.Order {
			s := selectionColumn("c", o.Column, true)
			if o.Desc {
				s += " DESC"
			}
			order = append(order, s)
		}
		order = append(order, "c.bead_id")
		c.ctes = append(c.ctes, `ranked AS MATERIALIZED (SELECT c.*,row_number() OVER(ORDER BY `+strings.Join(order, ",")+`) rn FROM matched c WHERE (SELECT total FROM totals)<=`+strconv.Itoa(cap)+`)`)
		domain = "ranked"
		if p.Page.Offset > uint64(cap) {
			start = "-1"
		} else {
			start = strconv.FormatUint(p.Page.Offset, 10)
		}
		if after := p.Page.After; after != nil {
			k := after.Key
			pred := []string{"c.bead_key=" + c.bind([]byte(selectionRefKey(k.Ref))) + "::bytea", "c.source_revision=" + c.bind(k.Revision.String()) + "::numeric", "c.leaf=" + c.bind([]byte(k.MembershipLeaf)) + "::bytea"}
			for i, v := range k.Order {
				pred = append(pred, selectionColumn("c", p.Order[i].Column, true)+"="+c.value(v))
			}
			start = `coalesce((SELECT rn FROM ranked c WHERE ` + strings.Join(pred, " AND ") + `),-1)`
		}
		end = "least(b.start+" + strconv.Itoa(p.Page.Limit) + ",t.total)"
		suffix = "count(*) FILTER(WHERE rn>" + end + ")"
	}
	c.ctes = append(c.ctes, `bounds AS (SELECT `+start+`::bigint start)`)
	if p.GroupBy == driver.SelectionGroupType {
		c.ctes = append(c.ctes, `grouped AS (SELECT bead_type,count(*) amount,`+suffix+` elided FROM `+domain+` CROSS JOIN bounds b CROSS JOIN totals t GROUP BY bead_type)`)
	} else {
		c.ctes = append(c.ctes, `grouped AS (SELECT NULL::bytea bead_type,0::bigint amount,0::bigint elided WHERE FALSE)`)
	}
	q := `WITH ` + strings.Join(c.ctes, ",\n") + ` SELECT tag,payload FROM (` +
		`SELECT 0 tag,0::bigint seq,jsonb_build_object('total',t.total,'groups',(SELECT count(*) FROM grouped),'start',b.start)::text payload FROM totals t CROSS JOIN bounds b`
	q += ` UNION ALL SELECT 1,0,jsonb_build_object('type',encode(g.bead_type,'hex'),'count',g.amount,'elided',g.elided)::text FROM grouped g CROSS JOIN totals t WHERE t.total<=` + strconv.Itoa(cap) + ` AND (SELECT count(*) FROM grouped)<=` + strconv.Itoa(driver.SelectionGroupCap)
	if p.Page != nil {
		q += ` UNION ALL SELECT 2,c.rn,jsonb_build_object('ref',encode(c.ref,'hex'),'revision',c.source_revision::text,'id',encode(c.bead_id,'hex'),'type',encode(c.bead_type,'hex'),'project',encode(c.project,'hex'),'status',encode(c.status,'hex'),'title',encode(c.title,'hex'),'priority',c.priority,'leaf',encode(c.leaf,'hex'))::text FROM ranked c CROSS JOIN bounds b CROSS JOIN totals t WHERE b.start>=0 AND b.start<=t.total AND c.rn>b.start AND c.rn<=` + end + ` AND (SELECT count(*) FROM grouped)<=` + strconv.Itoa(driver.SelectionGroupCap)
	}
	q += `) output ORDER BY tag,seq`
	return q, c.args, nil
}

type selectionKeyJSON struct {
	Ref, Revision, ID, Type, Status, Title, Leaf string
	Project                                      *string
	Priority                                     int64
}

func selectionUnhex(s string) string { b, _ := hex.DecodeString(s); return string(b) }
func (r *reader) runSelection(ctx context.Context, p driver.SelectionPlan, query string, args []any) (driver.SelectionResult, error) {
	zero := driver.SelectionResult{}
	rows, err := r.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()
	result := driver.SelectionResult{}
	var control struct {
		Total, Groups uint64
		Start         int64
	}
	haveControl := false
	for rows.Next() {
		var tag int
		var payload string
		if err = rows.Scan(&tag, &payload); err != nil {
			return zero, err
		}
		switch tag {
		case 0:
			if err = json.Unmarshal([]byte(payload), &control); err != nil {
				return zero, err
			}
			haveControl = true
			result.Total = control.Total
		case 1:
			var g struct {
				Type          string
				Count, Elided uint64
			}
			if err = json.Unmarshal([]byte(payload), &g); err != nil {
				return zero, err
			}
			driver.ReportReadWork(ctx, driver.ReadAggregateGroups, 1)
			result.Groups = append(result.Groups, driver.SelectionTypeGroup{Type: selectionUnhex(g.Type), Count: g.Count, Elided: g.Elided})
		case 2:
			var row selectionKeyJSON
			if err = json.Unmarshal([]byte(payload), &row); err != nil {
				return zero, err
			}
			raw, err := hex.DecodeString(row.Ref)
			if err != nil {
				return zero, err
			}
			var ref model.Ref
			if err = decode(raw, &ref); err != nil {
				return zero, err
			}
			rev, err := parseCounter(row.Revision)
			if err != nil {
				return zero, err
			}
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
			key := driver.SelectionKey{Ref: ref, Revision: model.Natural64(rev), MembershipLeaf: selectionUnhex(row.Leaf), Order: make([]driver.SelectionValue, 0, len(p.Order))}
			for _, o := range p.Order {
				var value driver.SelectionValue
				switch o.Column {
				case driver.SelectionID:
					value = driver.SelectionString(selectionUnhex(row.ID))
				case driver.SelectionType:
					value = driver.SelectionString(selectionUnhex(row.Type))
				case driver.SelectionProject:
					s := ""
					if row.Project != nil {
						s = selectionUnhex(*row.Project)
					}
					value = driver.SelectionString(s)
				case driver.SelectionStatus:
					value = driver.SelectionString(selectionUnhex(row.Status))
				case driver.SelectionTitle:
					value = driver.SelectionString(selectionUnhex(row.Title))
				case driver.SelectionRevision:
					value = driver.SelectionString(row.Revision)
				case driver.SelectionPriority:
					value = driver.SelectionInt(row.Priority)
				}
				key.Order = append(key.Order, value)
			}
			result.Keys = append(result.Keys, key)
		}
	}
	if err = rows.Err(); err != nil {
		return zero, err
	}
	if !haveControl {
		return zero, selectionUnsupported("missing control row")
	}
	if control.Total > uint64(p.EffectiveCap()) {
		return zero, driver.ErrSelectionCap{Limit: p.EffectiveCap()}
	}
	if control.Groups > driver.SelectionGroupCap {
		return zero, driver.ErrSelectionCap{Limit: driver.SelectionGroupCap, Groups: true}
	}
	if p.Page != nil {
		if control.Start < 0 || uint64(control.Start) > control.Total {
			return zero, selectionUnsupported("offset or keyset anchor outside membership")
		}
		if result.Keys == nil {
			result.Keys = []driver.SelectionKey{}
		}
		result.More = uint64(control.Start)+uint64(len(result.Keys)) < result.Total
		if result.More && len(result.Keys) > 0 {
			binding, err := p.Binding()
			if err != nil {
				return zero, err
			}
			result.Next = &driver.SelectionKeyset{Binding: binding, Key: result.Keys[len(result.Keys)-1]}
		}
	}
	sort.Slice(result.Groups, func(i, j int) bool { return result.Groups[i].Type < result.Groups[j].Type })
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	return result, nil
}

// Optional writer support is deliberately separate from the read capability.
func (t *transaction) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	return t.PutDerived(ctx, table, del, add)
}

var _ driver.SelectionReader = (*reader)(nil)
var _ driver.SelectionReader = (*transaction)(nil)
var _ driver.SelectionIndexWriter = (*transaction)(nil)
