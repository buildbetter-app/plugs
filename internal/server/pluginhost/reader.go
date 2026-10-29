package pluginhost

import (
	"context"
	"sync"

	"github.com/buildbetter-app/plugs/pluginapi"
)

// scopedReader shares its counters across every accessor (including Host.Call)
// for one callback. It never exposes an engine handle or unguarded reader.
type scopedReader struct {
	ctx                   context.Context
	base                  pluginapi.SnapshotReader
	reads                 []string
	mu                    sync.Mutex
	points, queries       int
	maxPoints, maxQueries int
}

func (r *scopedReader) charge(ctx context.Context, points, queries int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if points < 0 || queries < 0 || points > r.maxPoints-r.points || queries > r.maxQueries-r.queries {
		return failure(ErrBudget, "maximum %d point reads and %d queries", r.maxPoints, r.maxQueries)
	}
	r.points += points
	r.queries += queries
	return nil
}
func (r *scopedReader) Get(ctx context.Context, refs []pluginapi.Ref) ([]pluginapi.Record, error) {
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			return nil, err
		}
	}
	if err := r.charge(ctx, len(refs), 0); err != nil {
		return nil, err
	}
	values, err := r.base.Get(r.ctx, append([]pluginapi.Ref(nil), refs...))
	if err != nil {
		return nil, err
	}
	out := make([]pluginapi.Record, len(values))
	for index, value := range values {
		value.Body, err = mask(value.Body, r.reads)
		if err != nil {
			return nil, err
		}
		out[index] = value
	}
	return out, nil
}
func (r *scopedReader) Query(ctx context.Context, query pluginapi.Query) (pluginapi.Page[pluginapi.Record], error) {
	for field := range query.Filter {
		if !granted(r.reads, field) {
			return pluginapi.Page[pluginapi.Record]{}, failure(ErrRead, "query field %s is not declared", field)
		}
	}
	if query.Limit < 0 || query.Limit > 1000 {
		return pluginapi.Page[pluginapi.Record]{}, failure(ErrRead, "query limit must be between 0 and 1000")
	}
	if err := r.charge(ctx, 0, 1); err != nil {
		return pluginapi.Page[pluginapi.Record]{}, err
	}
	copy, err := pluginapi.CloneJSON(query)
	if err != nil {
		return pluginapi.Page[pluginapi.Record]{}, err
	}
	page, err := r.base.Query(r.ctx, copy)
	if err != nil {
		return page, err
	}
	for index := range page.Items {
		page.Items[index].Body, err = mask(page.Items[index].Body, r.reads)
		if err != nil {
			return pluginapi.Page[pluginapi.Record]{}, err
		}
	}
	return page, nil
}
func (r *scopedReader) Beads(ctx context.Context, refs []pluginapi.Ref) ([]pluginapi.View, error) {
	records, err := r.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	out := make([]pluginapi.View, len(records))
	for j, record := range records {
		var fields map[string]any
		if err := decode(record.Body, &fields); err != nil {
			return nil, err
		}
		out[j] = pluginapi.View{ID: record.Ref.ID, Fields: fields}
	}
	return out, nil
}
func (r *scopedReader) Closure(context.Context, pluginapi.Ref, bool) ([]pluginapi.Ref, error) {
	return nil, failure(ErrRead, "guarded closure reads are not implemented")
}
func (r *scopedReader) Grants(context.Context, []pluginapi.SubjectRef, []pluginapi.Ref) ([]pluginapi.Grant, error) {
	return nil, failure(ErrRead, "guarded grant reads are not implemented")
}

var _ pluginapi.SnapshotReader = (*scopedReader)(nil)
