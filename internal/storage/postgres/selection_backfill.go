package postgres

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

type selectionBackfill struct {
	tx       *transaction
	projects []driver.GraphSourceProjection
	families map[driver.SelectionProjectionFamily]bool // true only for unavailable families
	expected map[string]bool
	sources  map[string]bool
}

func (t *transaction) prepareSelectionBackfill(ctx context.Context, projects []driver.GraphSourceProjection) (*selectionBackfill, error) {
	b := &selectionBackfill{tx: t, projects: projects, families: map[driver.SelectionProjectionFamily]bool{}, expected: map[string]bool{}, sources: map[string]bool{}}
	building := false
	clean, err := t.selectionCertificateClean(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range projects {
		if err := p.Validate(); err != nil {
			return nil, err
		}
		for _, f := range p.Families {
			if _, duplicate := b.families[f]; duplicate {
				return nil, selectionUnsupported("duplicate startup family")
			}
			err := t.requireSelectionCoverage(ctx, f, t.at)
			if err != nil && !isSelectionUnavailable(err) {
				return nil, err
			}
			b.families[f] = err != nil || !clean
			building = building || b.families[f]
		}
	}
	if !building {
		return nil, nil
	}
	return b, nil
}

func (b *selectionBackfill) hooks() sourceindex.BackfillSelection {
	return sourceindex.BackfillSelection{Clear: b.clear, Page: b.page, Activate: b.activate}
}
func (b *selectionBackfill) clear(ctx context.Context, publish sourceindex.BackfillPublication) error {
	return publish(ctx, func(q sourceindex.SQL) error {
		for family, building := range b.families {
			if !building {
				continue
			}
			// Rebuild only our record/bead domain. Independently complete derived
			// sources sharing a table/kind retain their exact annotations and history.
			where := `incarnation=$1 AND layout=2 AND source_kind='record' AND family_table=$2 AND coalesce(fact_kind,'')=$3 AND EXISTS(SELECT 1 FROM plug_postgres.selection_sources h WHERE h.incarnation=$1 AND h.key=selection_rows.source_key AND h.kind='bead')`
			args := []any{b.tx.meta.Incarnation, string(family.Table), string(family.Kind), b.tx.meta.Head.String()}
			if err := deleteSelectionRowsWithFacts(ctx, q, where+` AND start_revision=$4`, args...); err != nil {
				return err
			}
			if _, err := q.ExecContext(ctx, `UPDATE plug_postgres.selection_rows SET end_revision=$4 WHERE `+where+` AND start_revision<$4 AND (end_revision IS NULL OR end_revision>$4)`, args...); err != nil {
				return err
			}
		}
		return nil
	})
}

// The fact FK is immediate. Delete children using the exact same owned parent
// identities before deleting those parents, in the existing Head-locked SQL
// transaction. Earlier intervals and independent source domains stay intact.
func deleteSelectionRowsWithFacts(ctx context.Context, q sourceindex.SQL, where string, args ...any) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM plug_postgres.selection_facts WHERE (incarnation,row_key,start_revision) IN (SELECT incarnation,row_key,start_revision FROM plug_postgres.selection_rows WHERE `+where+`)`, args...); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `DELETE FROM plug_postgres.selection_rows WHERE `+where, args...)
	return err
}

func selectionOwnedDBRow(id string, row driver.DerivedRow) (selectionDBRow, error) {
	result, err := selectionRow(id, row)
	if err != nil {
		return result, err
	}
	var encoded bytes.Buffer
	if err = gob.NewEncoder(&encoded).Encode(row.Index); err != nil {
		return result, err
	}
	result.Annotation = encoded.Bytes()
	return result, nil
}
func writeSelectionRows(ctx context.Context, q sourceindex.SQL, incarnation string, at model.Revision, rows []selectionDBRow, facts []selectionDBFact) error {
	for start := 0; start < len(rows); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(rows))
		// Binary annotations dominate large closure publications. Transport the
		// exact existing Gob bytes directly, outside the scalar JSON recordset.
		// WITH ORDINALITY binds each bytea to its original JSON array element;
		// both slices have the same bounded length and no identity join can drop
		// or multiply a row. All semantic columns and SQL triggers are unchanged.
		annotations := make([][]byte, end-start)
		for i, row := range rows[start:end] {
			annotations[i] = row.Annotation
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO plug_postgres.selection_rows(incarnation,start_revision,`+selectionColumns+`,annotation) SELECT $1,$2,`+selectionJSONColumns+`,($4::bytea[])[p.ordinality::int] FROM ROWS FROM (jsonb_to_recordset($3::jsonb) AS (`+selectionJSONLayout+`)) WITH ORDINALITY AS p`, incarnation, at.String(), selectionJSON(rows[start:end]), annotations); err != nil {
			return err
		}
	}
	for start := 0; start < len(facts); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(facts))
		columns, err := selectionFactArrayArgs(facts[start:end])
		if err != nil {
			return err
		}
		args := append([]any{incarnation, at.String()}, columns...)
		if _, err := q.ExecContext(ctx, `INSERT INTO plug_postgres.selection_facts(incarnation,start_revision,`+selectionFactColumns+`) SELECT $1,$2,* FROM `+selectionFactArrayRelation, args...); err != nil {
			return err
		}
	}
	return nil
}

func (b *selectionBackfill) page(ctx context.Context, q sourceindex.SQL, records []driver.Record, metadata []driver.GraphSourceMetadata) error {
	outputs := []driver.DerivedSelectionRow{}
	for _, project := range b.projects {
		page, err := driver.NewSelectionProjectionPage(records, metadata, project)
		if err != nil {
			return err
		}
		// One bounded range statement per projection/source page; never one query
		// per source. Range and source reads share this Head-locked SQL transaction.
		inputs, err := sourceindex.Postgres.SelectionRangePage(ctx, q, b.tx.meta.Head, page.Ranges, project.MaxPageRows)
		if err != nil {
			return err
		}
		decoded, err := page.Decode(inputs, b.tx.meta.Incarnation)
		if err != nil {
			return err
		}
		for _, rows := range decoded {
			outputs = append(outputs, rows...)
		}
	}
	ids := make([][]byte, 0, len(outputs))
	for _, row := range outputs {
		id, err := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}})
		if err != nil {
			return err
		}
		if b.expected[id] {
			return selectionUnsupported("duplicate cross-page startup identity")
		}
		b.expected[id] = true
		ids = append(ids, []byte(id))
	}
	// Existing opaque logical bytes are never changed by startup. Closure input
	// must exist; generated sort/label/link projections may have no logical row.
	actual, err := selectionStartupValues(ctx, q, b.tx.meta.Head, ids)
	if err != nil {
		return err
	}
	physical := []selectionDBRow{}
	facts := []selectionDBFact{}
	for i, row := range outputs {
		id := string(ids[i])
		state, exists := actual[id]
		if exists && state.Present {
			if state.Derived == nil || state.Key.Derived == nil || state.Key.Derived.Table != row.Table || state.Derived.Key != row.Row.Key || !bytes.Equal(state.Derived.Value, row.Row.Value) {
				return selectionUnsupported("startup logical value differs")
			}
			if x := state.Derived.Index; x != nil && (x.Source.Key.Kind != driver.LockRecord || x.Source.Key.Ref == nil || !driver.SelectionSameRef(*x.Source.Key.Ref, *row.Row.Index.Source.Key.Ref)) {
				return selectionUnsupported("startup identity belongs to an independent source")
			}
		} else if row.Table == driver.DerivedAncestorClosure {
			return selectionUnsupported("startup closure input missing")
		}
		kind := ""
		if row.Row.Index.Facts != nil {
			kind = string(row.Row.Index.Facts.Kind)
		}
		family, ok := selectionCoreFamily(string(row.Table), kind)
		if !ok {
			return selectionUnsupported("undeclared startup output family")
		}
		if !b.families[family] {
			continue
		}
		encoded, err := selectionOwnedDBRow(id, row.Row)
		if err != nil {
			return err
		}
		physical = append(physical, encoded)
		if row.Row.Index.Facts != nil {
			facts = append(facts, selectionFactsRows(id, row.Row.Index.Facts)...)
		}
	}
	headers := []selectionSourceHeader{}
	sourceIDs := [][]byte{}
	for _, record := range records {
		id, err := recordKey(record.Ref)
		if err != nil {
			return err
		}
		if b.sources[id] {
			return selectionUnsupported("duplicate startup source")
		}
		b.sources[id] = true
		sourceIDs = append(sourceIDs, []byte(id))
		headers = append(headers, selectionSourceHeader{Key: selectionHex(id), Revision: record.Revision.String(), Present: true, Digest: selectionHex(driver.SelectionDigest(record.Body)), Kind: record.Kind})
	}
	// Source revision is the actual committed revision, not this availability
	// floor. The physical header at H does not rewrite logical state/history.
	if len(sourceIDs) > 0 {
		if _, err = q.ExecContext(ctx, `UPDATE plug_postgres.selection_sources SET end_revision=$2 WHERE incarnation=$1 AND key=ANY($3::bytea[]) AND start_revision<$2 AND (end_revision IS NULL OR end_revision>$2)`, b.tx.meta.Incarnation, b.tx.meta.Head.String(), sourceIDs); err != nil {
			return err
		}
		if _, err = q.ExecContext(ctx, `INSERT INTO plug_postgres.selection_sources(incarnation,key,start_revision,revision,present,digest,kind) SELECT $1,decode(key,'hex'),$2,revision::numeric,present,decode(digest,'hex'),kind FROM jsonb_to_recordset($3::jsonb) AS h(key text,revision text,present boolean,digest text,kind text) ON CONFLICT(incarnation,key,start_revision) DO UPDATE SET revision=excluded.revision,present=excluded.present,digest=excluded.digest,kind=excluded.kind`, b.tx.meta.Incarnation, b.tx.meta.Head.String(), selectionJSON(headers)); err != nil {
			return err
		}
	}
	// An old unqualified physical layout may occupy this exact Head/key. It
	// cannot block replacement of an owned row or qualify the new layout.
	// Earlier versions and independently owned rows remain untouched.
	ownedIDs := make([][]byte, 0, len(physical))
	for _, row := range physical {
		ownedIDs = append(ownedIDs, []byte(selectionUnhex(row.RowKey)))
	}
	for start := 0; start < len(ownedIDs); start += publicationBatchRows {
		if err = deleteSelectionRowsWithFacts(ctx, q, `incarnation=$1 AND start_revision=$2 AND layout<>2 AND row_key=ANY($3::bytea[])`, b.tx.meta.Incarnation, b.tx.meta.Head.String(), ownedIDs[start:min(start+publicationBatchRows, len(ownedIDs))]); err != nil {
			return err
		}
	}
	return writeSelectionRows(ctx, q, b.tx.meta.Incarnation, b.tx.meta.Head, physical, facts)
}

// Decode only bounded driver envelopes, never source document JSON. Key and
// revision identity checks prevent a stored payload from naming a different row.
func selectionStartupValues(ctx context.Context, q sourceindex.SQL, at model.Revision, ids [][]byte) (map[string]driver.LockState, error) {
	result := map[string]driver.LockState{}
	for start := 0; start < len(ids); start += publicationBatchRows {
		rows, err := q.QueryContext(ctx, `SELECT DISTINCT ON(key) key,value FROM plug_postgres.states WHERE key=ANY($1::bytea[]) AND revision<=$2 ORDER BY key,revision DESC`, ids[start:min(start+publicationBatchRows, len(ids))], at.String())
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, raw []byte
			var state driver.LockState
			if err = rows.Scan(&id, &raw); err != nil {
				break
			}
			if err = decode(raw, &state); err != nil {
				break
			}
			key, keyErr := keyID(state.Key)
			revision, revisionErr := state.Revision.Uint64()
			if keyErr != nil || key != string(id) || revisionErr != nil || revision > mustRevision(at) {
				err = selectionUnsupported("startup state envelope mismatch")
				break
			}
			result[key] = state
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (b *selectionBackfill) activate(ctx context.Context, q sourceindex.SQL) error {
	meta, err := readMeta(ctx, b.tx.sql, "")
	if err != nil {
		return err
	}
	if meta.Incarnation != b.tx.meta.Incarnation || meta.Head.String() != b.tx.meta.Head.String() {
		return selectionUnsupported("startup final authority changed")
	}
	var count int
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.records WHERE kind='bead' AND NOT deleted`).Scan(&count); err != nil {
		return err
	}
	if count != len(b.sources) {
		return selectionUnsupported("startup source census incomplete")
	}
	valid, err := b.tx.reader.selectionFamilyValidity(ctx, b.tx.at, false)
	if err != nil {
		return err
	}
	for family := range b.families {
		if !valid[family] {
			return selectionUnsupported("startup family/source/Parent census incomplete")
		}
	}
	if err = b.requireNoOrphans(ctx, q); err != nil {
		return err
	}
	generation, err := b.tx.reader.selectionProofGeneration(ctx)
	if err != nil {
		return err
	}
	statements := 0
	for family, building := range b.families {
		if !building {
			continue
		}
		if _, err = q.ExecContext(ctx, `INSERT INTO plug_postgres.selection_coverage(incarnation,branch,family_table,fact_kind,format,start_revision,built_from,certified_through,ready) VALUES($1,0,$2,$3,$4,$5,$5,$5,true) ON CONFLICT(incarnation,branch,family_table,fact_kind,format,start_revision) DO UPDATE SET built_from=excluded.built_from,certified_through=excluded.certified_through,ready=true`, meta.Incarnation, string(family.Table), string(family.Kind), family.Version, meta.Head.String()); err != nil {
			return err
		}
		// PostgreSQL fires both INSERT and UPDATE statement triggers for
		// ON CONFLICT DO UPDATE, including when no row conflicts.
		statements += 2
	}
	if err = b.tx.reader.requireSelectionProofGeneration(ctx, generation, statements); err != nil {
		return err
	}
	complete := true
	for _, family := range selectionFamilies() {
		complete = complete && b.families[family]
	}
	if complete {
		if err = b.tx.reader.certifySelectionProof(ctx, valid); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Bound the orphan census by driver state pages. Only record/bead-owned core
// rows belong to this build; derived-source and non-bead complete envelopes are
// left byte-for-byte intact. A live row bound to a deleted bead is still orphaned.
func (b *selectionBackfill) requireNoOrphans(ctx context.Context, q sourceindex.SQL) error {
	after := []byte{}
	for {
		rows, err := q.QueryContext(ctx, `SELECT key,value FROM (SELECT DISTINCT ON(key) key,value FROM plug_postgres.states WHERE key>$1 AND revision<=$2 ORDER BY key,revision DESC) latest WHERE convert_from(value,'UTF8')::jsonb->>'Present'='true' AND convert_from(value,'UTF8')::jsonb->'Derived'->'Index'->'Source'->'Key'->>'Kind'='record' ORDER BY key LIMIT 128`, after, b.tx.meta.Head.String())
		if err != nil {
			return err
		}
		states := map[string]driver.LockState{}
		sources := [][]byte{}
		pageCount := 0
		for rows.Next() {
			pageCount++
			var raw, id []byte
			var state driver.LockState
			if err = rows.Scan(&id, &raw); err != nil {
				break
			}
			after = bytes.Clone(id)
			if err = decode(raw, &state); err != nil {
				break
			}
			encoded, e := keyID(state.Key)
			if e != nil || encoded != string(id) {
				err = selectionUnsupported("orphan census envelope identity")
				break
			}
			if state.Derived == nil || state.Key.Derived == nil || state.Derived.Index == nil {
				err = selectionUnsupported("orphan census envelope missing")
				break
			}
			x := state.Derived.Index
			kind := ""
			if x.Facts != nil {
				kind = string(x.Facts.Kind)
			}
			family, core := selectionCoreFamily(string(state.Key.Derived.Table), kind)
			if _, declared := b.families[family]; !core || !declared {
				continue
			}
			source, e := keyID(x.Source.Key)
			if e != nil {
				err = e
				break
			}
			states[encoded] = state
			sources = append(sources, []byte(source))
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return err
		}
		if pageCount == 0 {
			return nil
		}
		type sourceOwner struct {
			kind    string
			deleted bool
		}
		kinds := map[string]sourceOwner{}
		if len(sources) > 0 {
			rs, e := q.QueryContext(ctx, `SELECT key,kind,deleted FROM plug_postgres.records WHERE key=ANY($1::bytea[])`, sources)
			if e != nil {
				return e
			}
			for rs.Next() {
				var id []byte
				var owner sourceOwner
				if err = rs.Scan(&id, &owner.kind, &owner.deleted); err != nil {
					break
				}
				kinds[string(id)] = owner
			}
			if err == nil {
				err = rs.Err()
			}
			_ = rs.Close()
			if err != nil {
				return err
			}
		}
		for id, state := range states {
			x := state.Derived.Index
			source, _ := keyID(x.Source.Key)
			owner, known := kinds[source]
			if !known || owner.deleted || owner.kind == "bead" && !b.expected[id] {
				return selectionUnsupported("orphan current source-built row")
			}
		}
		if pageCount < 128 {
			return nil
		}
	}
}
