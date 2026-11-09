package postgres

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) QueueProjection(ctx context.Context) (driver.QueueProjection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.QueueProjection{}, err
	}
	if r.writer != nil && !r.writer.hasHead {
		return driver.QueueProjection{}, driver.QueueUnavailable("transaction requires Head")
	}
	if decimal(r.at) != r.meta.Head.String() {
		return driver.QueueProjection{}, driver.QueueUnavailable("historical reader")
	}
	lo, hi := driver.QueueProjectionRanges()
	// One fresh statement also serves READ COMMITTED post-Head transactions.
	// Authoritative records headers, not selection_sources, drive the census.
	// Raw logical state and active physical annotations are independently read
	// and matched, so a deleted hidden header+both projections cannot disappear
	// from the proof, and orphan physical rows cannot inherit a ready marker.
	rows, err := r.sql.QueryContext(ctx, `WITH
		wanted(family,kind) AS (VALUES ($8::text,''::text),($9::text,$10::text)),
		logical AS (SELECT DISTINCT ON(key) key,value FROM plug_postgres.states
		 WHERE ((key>=$4 AND key<$5) OR (key>=$6 AND key<$7)) AND revision<=$2::numeric
		 ORDER BY key,revision DESC)
		SELECT 'meta',convert_to(m.incarnation,'UTF8'),''::bytea,m.head::text,''::bytea,
		 m.incarnation=$1 AND m.head=$2::numeric AND NOT EXISTS(SELECT 1 FROM wanted w
		 WHERE NOT COALESCE((SELECT c.format=2 AND c.ready AND c.built_from<=$2::numeric AND c.certified_through>=$2::numeric
		 FROM plug_postgres.selection_coverage c
		 WHERE c.incarnation=$1 AND c.branch=$3 AND c.family_table=w.family AND c.fact_kind=w.kind
		 AND c.start_revision<=$2::numeric AND (c.end_revision IS NULL OR c.end_revision>$2::numeric)
		 ORDER BY c.start_revision DESC LIMIT 1),FALSE)),''::bytea,''::bytea
		 FROM plug_postgres.meta m
		UNION ALL SELECT 'source',r.key,''::bytea,r.revision::text,COALESCE(s.digest,''::bytea),
		 COALESCE(s.present AND s.revision=r.revision AND s.kind='bead' AND octet_length(s.digest)>0,FALSE),''::bytea,''::bytea
		 FROM plug_postgres.records r LEFT JOIN plug_postgres.selection_sources s
		 ON s.key=r.key AND s.incarnation=$1 AND s.start_revision<=$2::numeric
		 AND (s.end_revision IS NULL OR s.end_revision>$2::numeric)
		 WHERE r.kind='bead' AND NOT r.deleted
		UNION ALL SELECT 'row',key,value,'',''::bytea,TRUE,''::bytea,''::bytea FROM logical
		UNION ALL SELECT 'index',row_key,annotation,source_revision::text,source_digest,source_present,source_key,value_digest FROM plug_postgres.selection_rows
		 WHERE incarnation=$1 AND layout=2 AND family_table IN ($8,$9) AND start_revision<=$2::numeric
		 AND (end_revision IS NULL OR end_revision>$2::numeric)`, r.meta.Incarnation, decimal(r.at), 0,
		[]byte(lo[0]), []byte(hi[0]), []byte(lo[1]), []byte(hi[1]),
		string(driver.DerivedSortKey), string(driver.DerivedReferenceIndex), string(driver.SelectionLinkFact))
	if err != nil {
		return driver.QueueProjection{}, err
	}
	defer rows.Close()
	ready, metaSeen := false, false
	var sources []driver.QueueSourceHeader
	logical := map[string]driver.DerivedSelectionRow{}
	indexes := map[string]*driver.SelectionIndex{}
	for rows.Next() {
		var kind, revision string
		var key, raw, digest, sourceKey, valueDigest []byte
		var valid bool
		if err := rows.Scan(&kind, &key, &raw, &revision, &digest, &valid, &sourceKey, &valueDigest); err != nil {
			return driver.QueueProjection{}, err
		}
		switch kind {
		case "meta":
			if metaSeen {
				return driver.QueueProjection{}, driver.QueueUnavailable("duplicate meta")
			}
			metaSeen, ready = true, valid
		case "source":
			driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
			driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(key)+len(revision)+len(digest)))
			if !valid {
				return driver.QueueProjection{}, driver.QueueUnavailable("live source header missing or stale")
			}
			sources = append(sources, driver.QueueSourceHeader{Key: string(key), Revision: model.Natural(revision), Digest: string(digest)})
		case "row":
			driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
			driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(raw)))
			var state driver.LockState
			if err := decode(raw, &state); err != nil {
				return driver.QueueProjection{}, err
			}
			if !state.Present {
				continue
			}
			id, err := keyID(state.Key)
			if err != nil || id != string(key) || state.Key.Derived == nil || state.Derived == nil || state.Key.Derived.Key != state.Derived.Key {
				return driver.QueueProjection{}, driver.QueueUnavailable("logical state identity mismatch")
			}
			logical[id] = driver.DerivedSelectionRow{Table: state.Key.Derived.Table, Row: *state.Derived}
		case "index":
			driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
			driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(raw)))
			index, err := decodeSelectionAnnotation(raw)
			if err != nil {
				return driver.QueueProjection{}, err
			}
			id, err := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &index.Row})
			sourceID, sourceErr := keyID(index.Source.Key)
			if err != nil || sourceErr != nil || id != string(key) || indexes[id] != nil || sourceID != string(sourceKey) || index.Source.Revision.String() != revision || index.Source.Present != valid || index.Source.Digest != string(digest) || index.ValueDigest != string(valueDigest) {
				return driver.QueueProjection{}, driver.QueueUnavailable("duplicate or misbound physical annotation")
			}
			indexes[id] = &index
		}
	}
	if err := rows.Err(); err != nil {
		return driver.QueueProjection{}, err
	}
	inputs := make([]driver.DerivedSelectionRow, 0, len(logical))
	for id, row := range logical {
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		row.Row.Index = indexes[id]
		delete(indexes, id)
		inputs = append(inputs, row)
	}
	if len(indexes) != 0 {
		return driver.QueueProjection{}, driver.QueueUnavailable("orphan physical annotation")
	}
	at := model.Natural64(r.at)
	view := driver.View{Incarnation: r.meta.Incarnation, Branch: model.Natural64(0), At: &at}
	return driver.CompleteQueueProjection(ctx, view, metaSeen && ready, sources, inputs)
}

var _ driver.QueueProjectionReader = (*reader)(nil)
var _ driver.QueueProjectionReader = (*transaction)(nil)

func (r *reader) QueueProjectionImages(ctx context.Context, view driver.View, refs []model.Ref) ([]driver.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if view.Incarnation != r.meta.Incarnation || view.At == nil || view.At.String() != decimal(r.at) || !view.Branch.IsZero() || (r.writer != nil && !r.writer.hasHead) {
		return nil, driver.QueueUnavailable("image view changed")
	}
	committed := &reader{store: r.store, sql: r.sql, meta: r.meta, at: r.at}
	return committed.Get(ctx, refs)
}
