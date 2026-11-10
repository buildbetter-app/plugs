package sourceindex

import (
	"bytes"
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	// This is ONE total per request, shared by every bucket, keyset prefix,
	// duplicate, excluded/shadowed key and historical interval, including probe.
	MaxHeaders       = 65536
	MaxStagedSources = 25000
	headerChunk      = 64
	bucketBatch      = 64
)

type Work struct{ Headers, Images, Queries int }
type Request struct {
	Refs                []model.Ref
	Kinds               []driver.IncomingLinkKind
	Excluded            []model.Ref
	Limit               int
	Inverse, Historical bool
}
type sourceHeader struct {
	key        string
	start      uint64
	historical bool
	end        *uint64
	staged     bool
}
type sourceCursor struct {
	id             int
	parent         string
	bucket         Bucket
	rows           []sourceHeader
	position       int
	complete       bool
	last           sourceHeader
	staged         []string
	stagedPosition int
	stagedMode     bool
}

func (c *sourceCursor) current() sourceHeader {
	if c.stagedMode {
		return sourceHeader{key: c.staged[c.stagedPosition], staged: true}
	}
	return c.rows[c.position]
}

type cursorHeap []*sourceCursor

func (h cursorHeap) Len() int           { return len(h) }
func (h cursorHeap) Less(i, j int) bool { return h[i].current().key < h[j].current().key }
func (h cursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(v any)        { *h = append(*h, v.(*sourceCursor)) }
func (h *cursorHeap) Pop() any          { n := len(*h) - 1; v := (*h)[n]; *h = (*h)[:n]; return v }
func unavailableBudget() error {
	return fmt.Errorf("%w: typed source request exhausted its shared header budget", driver.ErrUnsupported)
}

// Every SQL prefix is ordered on the physical parent/incoming index. A
// continuation seeks strictly after its full last key/start tuple. Interval,
// exclusion and shadow filters run only on these budgeted key-only headers;
// they cannot hide an unbounded physical scan behind a filtered SQL LIMIT.
func (d Dialect) fetchHeaders(ctx context.Context, q SQL, incarnation string, request Request, cursors []*sourceCursor, take int, work *Work) error {
	if take <= 0 || len(cursors)*take > MaxHeaders-work.Headers {
		return unavailableBudget()
	}
	b := bindings{d: d}
	queries := make([]string, 0, len(cursors))
	for _, cursor := range cursors {
		table, column, index := "hierarchy_source_current", "child_key", "hierarchy_source_current_parent"
		if request.Inverse {
			table, column, index = "graph_link_current", "source_key", "graph_link_current_incoming"
		}
		if request.Historical {
			if request.Inverse {
				table, index = "graph_link_history", "graph_link_history_incoming"
			} else {
				table, index = "hierarchy_source_history", "hierarchy_source_history_parent"
			}
		}
		where := b.scope(incarnation)
		if request.Inverse {
			where += " AND target_key=" + b.add(d.key(cursor.bucket.Target)) + " AND link_kind=" + b.add([]byte(cursor.bucket.Kind))
		} else {
			where += " AND parent_key=" + b.add(d.key(cursor.parent))
		}
		if cursor.last.key != "" {
			if request.Historical {
				where += " AND (" + column + ",start_revision)>(" + b.add(d.key(cursor.last.key)) + "," + b.add(d.revision(cursor.last.start)) + ")"
			} else {
				where += " AND " + column + ">" + b.add(d.key(cursor.last.key))
			}
		}
		from := d.table(table)
		if d == SQLite {
			from += " INDEXED BY " + index
		}
		columns := column + " AS source_key,CAST(NULL AS TEXT) AS start_revision,CAST(NULL AS TEXT) AS end_revision"
		order := column
		if request.Historical {
			columns = column + " AS source_key,start_revision"
			if !request.Inverse {
				columns += ",end_revision"
			}
			order += ",start_revision"
		}
		inner := "SELECT " + columns + " FROM " + from + " WHERE " + where + " ORDER BY " + order + " LIMIT " + b.add(take)
		outer := "SELECT " + strconv.Itoa(cursor.id) + " AS bucket_id,x.source_key,CAST(x.start_revision AS TEXT),CAST(x.end_revision AS TEXT) FROM (" + inner + ") AS x"
		if request.Historical && request.Inverse {
			// End belongs to the exact source interval, never to a mutable edge.
			outer = "SELECT " + strconv.Itoa(cursor.id) + ",x.source_key,CAST(x.start_revision AS TEXT),CAST(s.end_revision AS TEXT) FROM (" + inner + ") AS x JOIN " + d.table("graph_link_source_history") + " AS s ON s.incarnation=" + b.add(incarnation) + " AND s.branch=" + b.add(d.revision(0)) + " AND s.source_key=x.source_key AND s.start_revision=x.start_revision"
		}
		queries = append(queries, outer)
		cursor.rows = nil
		cursor.position = 0
	}
	rows, err := q.QueryContext(ctx, strings.Join(queries, " UNION ALL "), b.args...)
	if err != nil {
		return err
	}
	work.Queries++
	driver.ReportReadWork(ctx, driver.ReadSourceQueries, 1)
	byID := make(map[int]*sourceCursor, len(cursors))
	for _, c := range cursors {
		byID[c.id] = c
	}
	for rows.Next() {
		var id int
		var raw []byte
		var start, end sql.NullString
		if err = rows.Scan(&id, &raw, &start, &end); err != nil {
			break
		}
		work.Headers++
		driver.ReportReadWork(ctx, driver.ReadSourceHeaders, 1)
		if work.Headers > MaxHeaders {
			err = unavailableBudget()
			break
		}
		cursor := byID[id]
		if cursor == nil {
			err = fmt.Errorf("%w: unknown source bucket", driver.ErrUnsupported)
			break
		}
		header := sourceHeader{key: string(raw), historical: request.Historical}
		if request.Historical {
			if !start.Valid {
				err = fmt.Errorf("%w: missing source interval start", driver.ErrUnsupported)
				break
			}
			header.start, err = strconv.ParseUint(start.String, 10, 64)
			if err != nil {
				break
			}
			if end.Valid {
				n, e := strconv.ParseUint(end.String, 10, 64)
				if e != nil {
					err = e
					break
				}
				header.end = &n
			}
		}
		cursor.rows = append(cursor.rows, header)
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, cursor := range cursors {
		sort.Slice(cursor.rows, func(i, j int) bool {
			a, b := cursor.rows[i], cursor.rows[j]
			if a.key != b.key {
				return a.key < b.key
			}
			return a.start < b.start
		})
		cursor.complete = len(cursor.rows) < take
		if len(cursor.rows) != 0 {
			cursor.last = cursor.rows[len(cursor.rows)-1]
		}
	}
	return nil
}

func (d Dialect) advanceCursor(ctx context.Context, q SQL, incarnation string, request Request, cursor *sourceCursor, work *Work) (bool, error) {
	if cursor.stagedMode {
		cursor.stagedPosition++
		if cursor.stagedPosition >= len(cursor.staged) {
			return false, nil
		}
		if work.Headers >= MaxHeaders {
			return false, unavailableBudget()
		}
		work.Headers++
		driver.ReportReadWork(ctx, driver.ReadSourceHeaders, 1)
		return true, nil
	}
	cursor.position++
	if cursor.position < len(cursor.rows) {
		return true, nil
	}
	if cursor.complete {
		return false, nil
	}
	take := min(headerChunk, MaxHeaders-work.Headers)
	if err := d.fetchHeaders(ctx, q, incarnation, request, []*sourceCursor{cursor}, take, work); err != nil {
		return false, err
	}
	return len(cursor.rows) != 0, nil
}

func (d Dialect) recheck(ctx context.Context, q SQL, meta driver.Meta, at uint64, inverse bool) error {
	var incarnation, head, floor string
	if err := q.QueryRowContext(ctx, "SELECT incarnation,CAST(head AS TEXT),CAST(history_floor AS TEXT) FROM "+d.table("meta")).Scan(&incarnation, &head, &floor); err != nil {
		return err
	}
	if incarnation != meta.Incarnation {
		return driver.ErrIncarnationChanged{From: meta.Incarnation, To: incarnation}
	}
	n, err := strconv.ParseUint(floor, 10, 64)
	if err != nil {
		return err
	}
	if at < n {
		return driver.ErrTruncated{Floor: model.Natural64(n)}
	}
	n, err = strconv.ParseUint(head, 10, 64)
	if err != nil {
		return err
	}
	if at > n {
		return driver.ErrFutureRevision{Requested: model.Natural64(at), Head: model.Natural64(n)}
	}
	coverage, err := d.ReadCoverage(ctx, q, incarnation)
	if err != nil {
		return err
	}
	if inverse {
		return coverage.Links.Require(at)
	}
	return coverage.Parent.Require(at)
}

func (d Dialect) Select(ctx context.Context, q SQL, meta driver.Meta, at uint64, request Request, overlay *Overlay, staged map[string]*driver.Record, work *Work) ([]driver.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if request.Inverse {
		if _, err := driver.NormalizeInverseSourceLinksRequest(request.Refs, request.Kinds, request.Excluded, request.Limit); err != nil {
			return nil, false, err
		}
	} else if _, err := driver.NormalizeHierarchyExcludingRequest(request.Refs, request.Excluded, request.Limit); err != nil {
		return nil, false, err
	}
	if len(staged) > MaxStagedSources {
		return nil, false, fmt.Errorf("%w: typed source staged vector too large", driver.ErrUnsupported)
	}
	if work == nil {
		work = &Work{}
	}
	if err := d.recheck(ctx, q, meta, at, request.Inverse); err != nil {
		return nil, false, err
	}
	coverage, err := d.ReadCoverage(ctx, q, meta.Incarnation)
	if err != nil {
		return nil, false, err
	}
	c := coverage.Parent
	if request.Inverse {
		c = coverage.Links
	}
	if err = c.Require(at); err != nil {
		return nil, false, err
	}
	refs := make(map[string]bool, len(request.Refs))
	for _, ref := range request.Refs {
		refs[key(ref)] = true
	}
	kinds := make(map[string]bool, len(request.Kinds))
	for _, kind := range request.Kinds {
		kinds[string(kind)] = true
	}
	excluded := make(map[string]bool, len(request.Excluded))
	for _, ref := range request.Excluded {
		excluded[key(ref)] = true
	}
	cursors := make([]*sourceCursor, 0)
	stagedCursors := make([]*sourceCursor, 0)
	for ref := range refs {
		if request.Inverse {
			for kind := range kinds {
				bucket := Bucket{kind, ref}
				cursors = append(cursors, &sourceCursor{id: len(cursors), bucket: bucket})
				if overlay != nil && len(overlay.Incoming[bucket]) != 0 {
					stagedCursors = append(stagedCursors, &sourceCursor{staged: overlay.Incoming[bucket], stagedMode: true})
				}
			}
		} else {
			cursors = append(cursors, &sourceCursor{id: len(cursors), parent: ref})
			if overlay != nil && len(overlay.Parents[ref]) != 0 {
				stagedCursors = append(stagedCursors, &sourceCursor{staged: overlay.Parents[ref], stagedMode: true})
			}
		}
	}
	h := make(cursorHeap, 0, len(cursors)+len(stagedCursors))
	if len(cursors) != 0 {
		initial := min(headerChunk, max(1, MaxHeaders/(2*len(cursors))))
		for first := 0; first < len(cursors); first += bucketBatch {
			end := min(first+bucketBatch, len(cursors))
			if err = d.fetchHeaders(ctx, q, meta.Incarnation, request, cursors[first:end], initial, work); err != nil {
				return nil, false, err
			}
		}
		for _, cursor := range cursors {
			if len(cursor.rows) != 0 {
				h = append(h, cursor)
			}
		}
	}
	for _, cursor := range stagedCursors {
		if work.Headers >= MaxHeaders {
			return nil, false, unavailableBudget()
		}
		work.Headers++
		driver.ReportReadWork(ctx, driver.ReadSourceHeaders, 1)
		h = append(h, cursor)
	}
	heap.Init(&h)
	selected := make([]sourceHeader, 0, request.Limit+1)
	lastSelected := ""
	for len(h) != 0 && len(selected) < request.Limit+1 {
		if err = ctx.Err(); err != nil {
			return nil, false, err
		}
		cursor := heap.Pop(&h).(*sourceCursor)
		header := cursor.current()
		_, shadowed := staged[header.key]
		active := !header.historical || header.start <= at && (header.end == nil || at < *header.end)
		if !excluded[header.key] && (header.staged || !shadowed) && active && header.key != lastSelected {
			selected = append(selected, header)
			lastSelected = header.key
			if len(selected) == request.Limit+1 {
				break
			}
		}
		present, e := d.advanceCursor(ctx, q, meta.Incarnation, request, cursor, work)
		if e != nil {
			return nil, false, e
		}
		if present {
			heap.Push(&h, cursor)
		}
	}
	images, err := d.fetchImages(ctx, q, meta.Incarnation, at, request, selected, staged, work)
	if err != nil {
		return nil, false, err
	}
	if err = d.recheck(ctx, q, meta, at, request.Inverse); err != nil {
		return nil, false, err
	}
	if err = ctx.Err(); err != nil {
		return nil, false, err
	}
	more := len(images) > request.Limit
	if more {
		images = images[:request.Limit]
	}
	return images, more, nil
}

func (d Dialect) fetchImages(ctx context.Context, q SQL, incarnation string, at uint64, request Request, selected []sourceHeader, staged map[string]*driver.Record, work *Work) ([]driver.Record, error) {
	found := make(map[string]driver.Record, len(selected))
	stored := make([]sourceHeader, 0, len(selected))
	for _, header := range selected {
		if header.staged {
			r := staged[header.key]
			if r == nil {
				return nil, fmt.Errorf("%w: missing staged source image", driver.ErrUnsupported)
			}
			found[header.key] = *r
			work.Images++
			driver.ReportReadWork(ctx, driver.ReadSourceImages, 1)
		} else {
			stored = append(stored, header)
		}
	}
	for first := 0; first < len(stored); first += publicationRows {
		end := min(first+publicationRows, len(stored))
		b := bindings{d: d}
		values := make([]string, 0, end-first)
		for _, header := range stored[first:end] {
			id := b.add(d.key(header.key))
			if d == Postgres {
				id += "::bytea"
			}
			if request.Historical {
				start := b.add(d.revision(header.start))
				if d == Postgres {
					start += "::numeric(20,0)"
				}
				values = append(values, "("+id+","+start+")")
			} else {
				values = append(values, "("+id+")")
			}
		}
		table, column := "hierarchy_source_current", "child_key"
		if request.Inverse {
			table, column = "graph_link_source_current", "source_key"
		}
		columns := "source_key"
		if request.Historical {
			if request.Inverse {
				table = "graph_link_source_history"
			} else {
				table = "hierarchy_source_history"
			}
			columns += ",start_revision"
		}
		query := "WITH selected(" + columns + ") AS (VALUES " + strings.Join(values, ",") + ") SELECT s.source_key,h.ref,h.kind,CAST(h.revision AS TEXT),h.body FROM selected AS s JOIN " + d.table(table) + " AS h ON h." + column + "=s.source_key AND h.incarnation=" + b.add(incarnation) + " AND h.branch=" + b.add(d.revision(0))
		if request.Historical {
			query += " AND h.start_revision=s.start_revision"
		}
		rows, err := q.QueryContext(ctx, query, b.args...)
		if err != nil {
			return nil, err
		}
		work.Queries++
		driver.ReportReadWork(ctx, driver.ReadSourceQueries, 1)
		for rows.Next() {
			var id, ref, body []byte
			var kind, revision string
			if err = rows.Scan(&id, &ref, &kind, &revision, &body); err != nil {
				break
			}
			work.Images++
			driver.ReportReadWork(ctx, driver.ReadSourceImages, 1)
			// SQL Scan allocated a body even if identity validation later
			// rejects this image or it is only the page lookahead.
			driver.ReportReadPayload(ctx, body)
			n, e := strconv.ParseUint(revision, 10, 64)
			if e != nil {
				err = e
				break
			}
			if _, duplicate := found[string(id)]; duplicate {
				err = fmt.Errorf("%w: duplicate source image", driver.ErrUnsupported)
				break
			}
			found[string(id)] = driver.Record{Ref: model.RefByID(string(ref), model.Natural64(0)), Kind: kind, Revision: model.Natural64(n), Body: body}
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	images := make([]driver.Record, 0, len(selected))
	for _, header := range selected {
		image, exists := found[header.key]
		revision, err := image.Revision.Uint64()
		if !exists || err != nil || revision > at || key(image.Ref) != header.key || driver.ValidateHierarchyRecord(image, nil) != nil {
			return nil, fmt.Errorf("%w: malformed indexed source image", driver.ErrUnsupported)
		}
		image.Body = bytes.Clone(image.Body)
		// This second allocation is distinct from the stored row Scan. A
		// staged alias did not allocate a body before this clone.
		driver.ReportReadPayload(ctx, image.Body)
		images = append(images, image)
	}
	return images, nil
}
