package postgres

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The SQL representation contains only typed semantic values supplied by core,
// never values inferred by a driver from a bead document. Hex transports opaque
// string bytes through jsonb without replacing invalid UTF-8 or rejecting NUL.
type selectionDBRow struct {
	FamilyTable                          string  `json:"family_table"`
	SourceKind                           string  `json:"source_kind"`
	SourceTable                          string  `json:"source_table"`
	ValueDigest                          string  `json:"value_digest"`
	Layout                               int     `json:"layout"`
	Annotation                           []byte  `json:"-"`
	RowKey                               string  `json:"row_key"`
	SourceKey                            string  `json:"source_key"`
	SourceRevision                       string  `json:"source_revision"`
	SourcePresent                        bool    `json:"source_present"`
	SourceDigest                         string  `json:"source_digest"`
	BeadKey                              *string `json:"bead_key"`
	Ref, ID, Type                        *string
	Project, Parent, Status, Title, Leaf *string
	Template                             *bool
	Priority                             *int64
	FactKind                             *string `json:"fact_kind"`
}

const selectionJSONLayout = `row_key text,source_key text,source_revision text,source_present boolean,source_digest text,bead_key text,"Ref" text,"ID" text,"Type" text,"Project" text,"Parent" text,"Status" text,"Title" text,"Template" boolean,"Leaf" text,"Priority" bigint,fact_kind text,annotation text,family_table text,source_kind text,source_table text,value_digest text,layout integer`
const selectionJSONColumns = `decode(row_key,'hex') row_key,decode(source_key,'hex') source_key,source_revision::numeric source_revision,source_present,decode(source_digest,'hex') source_digest,decode(bead_key,'hex') bead_key,decode("Ref",'hex') ref,decode("ID",'hex') bead_id,decode("Type",'hex') bead_type,decode("Project",'hex') project,decode("Parent",'hex') parent_key,decode("Status",'hex') status,decode("Title",'hex') title,"Template" template_value,"Priority" priority,decode("Leaf",'hex') leaf,fact_kind,family_table,source_kind,source_table,decode(value_digest,'hex') value_digest,layout`
const selectionColumns = `row_key,source_key,source_revision,source_present,source_digest,bead_key,ref,bead_id,bead_type,project,parent_key,status,title,template_value,priority,leaf,fact_kind,family_table,source_kind,source_table,value_digest,layout`

func selectionHex(s string) string       { return hex.EncodeToString([]byte(s)) }
func selectionHexPtr(s string) *string   { x := selectionHex(s); return &x }
func selectionRefKey(r model.Ref) string { k, _ := recordKey(r); return k }
func selectionUnsupported(reason string) error {
	return fmt.Errorf("%w: PostgreSQL selection %s", driver.ErrSelectionUnavailable, reason)
}
func selectionJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// Gob preserves Ref's public fields but omits its private arm discriminator.
// Restore valid ID refs to the same representation as canonical source/JSON
// reads, without changing opaque identity bytes or repairing malformed refs.
// The callers still validate the entire annotation against its logical row.
func decodeSelectionAnnotation(raw []byte) (driver.SelectionIndex, error) {
	var index driver.SelectionIndex
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&index); err != nil {
		return driver.SelectionIndex{}, err
	}
	restore := func(ref *model.Ref) {
		if ref != nil && ref.Validate() == nil && ref.IsID() {
			*ref = model.RefByID(ref.ID, ref.Branch)
		}
	}
	restore(index.Source.Key.Ref)
	if index.Bead != nil {
		restore(&index.Bead.Ref)
		restore(index.Bead.Parent)
	}
	if index.Facts != nil {
		for i := range index.Facts.Rows {
			fact := &index.Facts.Rows[i]
			restore(fact.Ref)
			restore(fact.Target)
			restore(fact.ScopeRef)
			if fact.Link != nil {
				restore(&fact.Link.To)
			}
		}
	}
	if index.Queue != nil {
		for i := range index.Queue.Links {
			restore(&index.Queue.Links[i].To)
		}
	}
	return index, nil
}

// Cache only decoding, never a database observation. Every caller still reads
// the current SQL image and validates the annotation against that logical row.
// Exact binary keys also work for ReadCommitted writers and same-token physical
// replacements; neither row identity nor a revision alone may authorize reuse.
// Keep the optional optimization bounded for long-lived, large-domain readers.
const selectionAnnotationCacheBytes = 4 << 20
const selectionAnnotationCacheEntries = 4096

func (r *reader) decodedSelectionAnnotation(raw []byte) (driver.SelectionIndex, error) {
	index, ok := r.annotationCache[string(raw)]
	if !ok {
		var err error
		index, err = decodeSelectionAnnotation(raw)
		if err != nil {
			return driver.SelectionIndex{}, err
		}
		if len(r.annotationCache) < selectionAnnotationCacheEntries && len(raw) <= selectionAnnotationCacheBytes-r.annotationCacheBytes {
			if r.annotationCache == nil {
				r.annotationCache = make(map[string]driver.SelectionIndex)
			}
			r.annotationCache[string(raw)] = index
			r.annotationCacheBytes += len(raw)
		}
	}
	// The cache owns every nested pointer/slice. Caller mutation must not alter
	// a subsequent read even when the database returned the same encoded bytes.
	return *(driver.DerivedRow{Index: &index}).Clone().Index, nil
}

func selectionRow(id string, row driver.DerivedRow) (selectionDBRow, error) {
	x := row.Index
	s := selectionDBRow{RowKey: selectionHex(id), SourceRevision: x.Source.Revision.String(), SourcePresent: x.Source.Present, SourceDigest: selectionHex(x.Source.Digest)}
	key, err := keyID(x.Source.Key)
	if err != nil {
		return s, err
	}
	s.SourceKey = selectionHex(key)
	s.FamilyTable = string(x.Row.Table)
	s.SourceKind = string(x.Source.Key.Kind)
	if x.Source.Key.Derived != nil {
		s.SourceTable = string(x.Source.Key.Derived.Table)
	}
	s.ValueDigest = selectionHex(x.ValueDigest)
	s.Layout = int(x.Version)
	if b := x.Bead; b != nil {
		s.BeadKey = selectionHexPtr(selectionRefKey(b.Ref))
		raw, err := encode(b.Ref)
		if err != nil {
			return s, err
		}
		s.Ref = selectionHexPtr(string(raw))
		s.ID = selectionHexPtr(b.Ref.ID)
		s.Type = selectionHexPtr(b.Type)
		s.Status = selectionHexPtr(b.Status)
		s.Title = selectionHexPtr(b.Title)
		template, priority := b.Template, b.Priority
		s.Template, s.Priority = &template, &priority
		for _, absent := range b.Absent {
			switch absent {
			case driver.SelectionStatus:
				s.Status = nil
			case driver.SelectionTitle:
				s.Title = nil
			case driver.SelectionTemplate:
				s.Template = nil
			case driver.SelectionPriority:
				s.Priority = nil
			}
		}
		s.Leaf = selectionHexPtr(b.MembershipLeaf)
		if b.Project != nil {
			s.Project = selectionHexPtr(*b.Project)
		}
		if b.Parent != nil {
			s.Parent = selectionHexPtr(selectionRefKey(*b.Parent))
		}
	} else {
		k := string(x.Facts.Kind)
		s.FactKind = &k
	}
	return s, nil
}

// Validate the entire proposed derived overlay against its locked, final source
// images. Tokens alone cannot detect a same-revision staged replacement.
func (t *transaction) validateSelectionWrites(ctx context.Context, proposed map[string]driver.LockState) error {
	keys := map[string]driver.LockKey{}
	for _, s := range proposed {
		if !s.Present || s.Derived == nil || s.Derived.Index == nil {
			continue
		}
		x := s.Derived.Index
		if err := x.Validate(s.Key.Derived.Table, *s.Derived); err != nil {
			return err
		}
		expectedIncarnation := t.meta.Incarnation
		if t.restore != nil {
			expectedIncarnation = t.restore.Incarnation
		}
		if x.Incarnation != expectedIncarnation {
			return selectionUnsupported("index incarnation mismatch")
		}
		id, err := keyID(x.Source.Key)
		if err != nil {
			return err
		}
		i := sort.Search(len(t.acquired), func(i int) bool { k, _ := keyID(t.acquired[i].Key); return k >= id })
		if i == len(t.acquired) {
			return selectionUnsupported("source absent from lock census")
		}
		got, _ := keyID(t.acquired[i].Key)
		if got != id {
			return selectionUnsupported("source absent from lock census")
		}
		keys[id] = x.Source.Key
	}
	if len(keys) == 0 {
		return nil
	}
	ids := make([]string, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordered := make([]driver.LockKey, 0, len(ids))
	for _, id := range ids {
		ordered = append(ordered, keys[id])
	}
	original := t.derived
	t.derived = proposed
	states, err := t.observeKeys(ctx, ordered)
	t.derived = original
	if err != nil {
		return err
	}
	observations := map[string]driver.LockState{}
	for i, s := range states {
		if _, ok := t.bumps[ids[i]]; ok {
			s.Present = true
		}
		observations[ids[i]] = s
	}
	for _, s := range proposed {
		if !s.Present || s.Derived == nil || s.Derived.Index == nil {
			continue
		}
		x := s.Derived.Index
		id, _ := keyID(x.Source.Key)
		source := observations[id]
		if source.Present != x.Source.Present || source.Revision.String() != x.Source.Revision.String() {
			return selectionUnsupported("source observation changed")
		}
		digest := ""
		if source.Present {
			switch x.Source.Key.Kind {
			case driver.LockRecord:
				if source.Record == nil {
					return selectionUnsupported("missing source record")
				}
				digest = driver.SelectionDigest(source.Record.Body)
			case driver.LockDerived:
				if source.Derived == nil {
					return selectionUnsupported("missing source row")
				}
				digest = driver.SelectionDigest(source.Derived.Value)
			}
		}
		if digest != x.Source.Digest {
			return selectionUnsupported("source bytes changed")
		}
		if x.Bead != nil && (source.Record == nil || source.Record.Kind != "bead") {
			return selectionUnsupported("bead source kind")
		}
	}
	return nil
}
func (t *transaction) selectionSourceChanged(k driver.LockKey) bool {
	id, _ := keyID(k)
	_, a := t.records[id]
	_, b := t.derived[id]
	_, c := t.bumps[id]
	return a || b || c
}

// Staged admission either carries a mutation-fenced complete proof over the
// full impacted closure, or executes the original full thin census.
func (t *transaction) selectionOverlayCovered(ctx context.Context) (bool, error) {
	valid, err := t.reader.selectionAdmissionValidity(ctx, t.at)
	if err != nil {
		return false, err
	}
	return valid[selectionBeadFamily()], nil
}

// Logical state JSON predates typed annotations. Preserve opaque annotation
// string bytes in a separate binary encoding, while keeping the logical Value
// and every existing state/version byte unchanged.
func (r *reader) selectionAnnotations(ctx context.Context, values []driver.DerivedRow, table driver.DerivedTable) error {
	ids := [][]byte{}
	positions := map[string]int{}
	for i, row := range values {
		key := driver.DerivedRowKey{Table: table, Key: row.Key}
		id, _ := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &key})
		if r.writer != nil {
			if _, staged := r.writer.derived[id]; staged {
				continue
			}
		}
		ids = append(ids, []byte(id))
		positions[id] = i
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT row_key,annotation FROM plug_postgres.selection_rows WHERE incarnation=$1 AND layout=2 AND row_key=ANY($2::bytea[]) AND start_revision<=$3 AND (end_revision IS NULL OR end_revision>$3)`, r.meta.Incarnation, ids, decimal(r.at))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return err
		}
		x, err := r.decodedSelectionAnnotation(raw)
		if err != nil {
			return err
		}
		pos, ok := positions[string(id)]
		if !ok {
			return selectionUnsupported("unexpected annotation identity")
		}
		// Physical startup metadata never changes logical bytes or optimistic
		// tokens. A different Value cannot inherit this annotation.
		values[pos].Index = nil
		if x.Incarnation == r.meta.Incarnation && x.Validate(table, values[pos]) == nil {
			values[pos].Index = &x
		}
	}
	return rows.Err()
}
