package pluginhost

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// coreReader is the only public/internal read conversion. All accesses go
// through the pipeline's guarded port; a plugin cannot supply a new AsOf.
type coreReader struct {
	reader   port.SnapshotReader
	revision model.Revision
}

func (r *coreReader) Get(ctx context.Context, refs []pluginapi.Ref) ([]pluginapi.Record, error) {
	internal := make([]model.Ref, len(refs))
	for j, ref := range refs {
		if err := assign(ref, &internal[j]); err != nil {
			return nil, err
		}
	}
	records, err := r.reader.Get(ctx, internal)
	if err != nil {
		return nil, err
	}
	out := make([]pluginapi.Record, len(records))
	for j, record := range records {
		var ref pluginapi.Ref
		if err := assign(record.Ref, &ref); err != nil {
			return nil, err
		}
		out[j] = pluginapi.Record{Ref: ref, Kind: record.Kind, Revision: pluginapi.Revision(record.Revision), Body: append([]byte(nil), record.Body...)}
	}
	return out, nil
}
func (r *coreReader) Query(ctx context.Context, q pluginapi.Query) (pluginapi.Page[pluginapi.Record], error) {
	view := r.reader.Snapshot()
	if q.Cursor != nil && (q.Cursor.Incarnation != view.Incarnation || q.Cursor.Revision.String() != r.revision.String()) {
		return pluginapi.Page[pluginapi.Record]{}, failure(ErrRead, "cursor is not bound to the callback snapshot")
	}
	query := driver.Query{Filter: q.Filter, Limit: q.Limit, AsOf: &r.revision}
	if query.Limit == 0 {
		query.Limit = 1000
	}
	if q.Cursor != nil {
		query.Cursor = &driver.Cursor{Incarnation: q.Cursor.Incarnation, Revision: model.Revision(q.Cursor.Revision), Index: model.Natural(q.Cursor.Index), After: q.Cursor.After, Method: q.Cursor.Method, FilterDigest: q.Cursor.FilterDigest, PrincipalDigest: q.Cursor.PrincipalDigest, OntologyRevision: model.Revision(q.Cursor.OntologyRevision)}
	}
	page, err := r.reader.Query(ctx, query)
	if err != nil {
		return pluginapi.Page[pluginapi.Record]{}, err
	}
	if page.AsOf.String() != r.revision.String() {
		return pluginapi.Page[pluginapi.Record]{}, failure(ErrRead, "query returned a different snapshot revision")
	}
	out := pluginapi.Page[pluginapi.Record]{AsOf: pluginapi.Revision(page.AsOf), Items: make([]pluginapi.Record, len(page.Items))}
	for j, record := range page.Items {
		var ref pluginapi.Ref
		if err := assign(record.Ref, &ref); err != nil {
			return out, err
		}
		out.Items[j] = pluginapi.Record{Ref: ref, Kind: record.Kind, Revision: pluginapi.Revision(record.Revision), Body: append([]byte(nil), record.Body...)}
	}
	if page.NextCursor != nil {
		c := page.NextCursor
		out.NextCursor = &pluginapi.Cursor{Incarnation: c.Incarnation, Revision: pluginapi.Revision(c.Revision), Index: pluginapi.Natural(c.Index), After: c.After, Method: c.Method, FilterDigest: c.FilterDigest, PrincipalDigest: c.PrincipalDigest, OntologyRevision: pluginapi.Revision(c.OntologyRevision)}
	}
	return out, nil
}
func (r *coreReader) Beads(context.Context, []pluginapi.Ref) ([]pluginapi.View, error) {
	return nil, fmt.Errorf("bead views are provided by the scoped reader")
}
func (r *coreReader) Closure(context.Context, pluginapi.Ref, bool) ([]pluginapi.Ref, error) {
	return nil, failure(ErrRead, "guarded closure reads are not implemented")
}
func (r *coreReader) Grants(context.Context, []pluginapi.SubjectRef, []pluginapi.Ref) ([]pluginapi.Grant, error) {
	return nil, failure(ErrRead, "guarded grant reads are not implemented")
}
