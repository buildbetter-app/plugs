package sqlite

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) QueueProjection(ctx context.Context) (driver.QueueProjection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.QueueProjection{}, err
	}
	if r.owner != nil && !r.owner.hasHead {
		return driver.QueueProjection{}, driver.QueueUnavailable("transaction requires Head")
	}
	if r.at != mustUint(r.meta.Head) {
		return driver.QueueProjection{}, driver.QueueUnavailable("historical reader")
	}
	lo, hi := driver.QueueProjectionRanges()
	// All branches share one pinned committed statement. Neither staged rows
	// nor source bodies are read. Live keys come from records/objects, not the
	// maintained selection header or projection domains. The authoritative key
	// token comes from key_history's revision index without decoding its image.
	rows, err := r.tx.QueryContext(ctx, `WITH
		wanted(family,kind) AS (VALUES (?8,''),(?9,?10)),
		logical AS (
			SELECT h.key,h.data FROM key_history h JOIN
			(SELECT key,MAX(rev) rev FROM key_history
			 WHERE ((key>=?4 AND key<?5) OR (key>=?6 AND key<?7)) AND rev<=?2 GROUP BY key) x
			 ON h.key=x.key AND h.rev=x.rev),
		live AS (SELECT r.key,(SELECT MAX(k.rev) FROM key_history k WHERE k.key=r.key AND k.rev<=?2) revision
			FROM records r JOIN objects o ON o.key=r.key WHERE o.kind='bead')
		SELECT 'meta',m.incarnation,X'',m.head,'',m.incarnation=?1 AND m.head=?2 AND
		 NOT EXISTS(SELECT 1 FROM wanted w WHERE NOT COALESCE((
			SELECT c.format=2 AND c.state='ready' AND c.certified_through>=?2
			FROM selection_family_coverage c WHERE c.incarnation=?1 AND c.branch=?3
			AND c.family_table=w.family AND c.family_kind=w.kind AND c.built_from<=?2
			ORDER BY c.built_from DESC LIMIT 1),0)) FROM meta m
		UNION ALL
		SELECT 'source',l.key,X'',COALESCE(l.revision,''),COALESCE(s.source_digest,''),
		 COALESCE(s.source_present=1 AND s.source_revision=l.revision AND s.source_digest<>'',0)
		 FROM live l LEFT JOIN selection_source_history s ON s.source_key=l.key
		 AND s.incarnation=?1 AND s.branch=?3 AND s.start_revision<=?2
		 AND (s.end_revision IS NULL OR s.end_revision>?2)
		UNION ALL SELECT 'row',key,data,'','',1 FROM logical`,
		r.meta.Incarnation, revKey(r.at), revKey(0), lo[0], hi[0], lo[1], hi[1],
		string(driver.DerivedSortKey), string(driver.DerivedReferenceIndex), string(driver.SelectionLinkFact))
	if err != nil {
		return driver.QueueProjection{}, err
	}
	defer rows.Close()
	ready, metaSeen := false, false
	var sources []driver.QueueSourceHeader
	var logical []driver.DerivedSelectionRow
	for rows.Next() {
		var kind, key, revision, digest string
		var raw []byte
		var valid bool
		if err := rows.Scan(&kind, &key, &raw, &revision, &digest, &valid); err != nil {
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
			token, err := parseRev(revision)
			if err != nil {
				return driver.QueueProjection{}, err
			}
			sources = append(sources, driver.QueueSourceHeader{Key: key, Revision: token, Digest: digest})
		case "row":
			driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
			driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(raw)))
			var state driver.LockState
			if err := json.Unmarshal(raw, &state); err != nil {
				return driver.QueueProjection{}, err
			}
			if !state.Present {
				continue
			}
			if state.Key.Derived == nil || state.Derived == nil || keyID(state.Key) != key || state.Key.Derived.Key != state.Derived.Key {
				return driver.QueueProjection{}, driver.QueueUnavailable("logical state identity mismatch")
			}
			logical = append(logical, driver.DerivedSelectionRow{Table: state.Key.Derived.Table, Row: *state.Derived})
		}
	}
	if err := rows.Err(); err != nil {
		return driver.QueueProjection{}, err
	}
	at := model.Natural64(r.at)
	view := driver.View{Incarnation: r.meta.Incarnation, Branch: model.Natural64(0), At: &at}
	return driver.CompleteQueueProjection(ctx, view, metaSeen && ready, sources, logical)
}

var _ driver.QueueProjectionReader = (*reader)(nil)
var _ driver.QueueProjectionReader = (*transaction)(nil)

func (r *reader) QueueProjectionImages(ctx context.Context, view driver.View, refs []model.Ref) ([]driver.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if view.Incarnation != r.meta.Incarnation || view.At == nil || view.At.String() != model.Natural64(r.at).String() || !view.Branch.IsZero() || (r.owner != nil && !r.owner.hasHead) {
		return nil, driver.QueueUnavailable("image view changed")
	}
	// A detached reader shares only the pinned SQL snapshot, never owner maps.
	committed := &reader{store: r.store, tx: r.tx, meta: r.meta, at: r.at}
	return committed.Get(ctx, refs)
}
