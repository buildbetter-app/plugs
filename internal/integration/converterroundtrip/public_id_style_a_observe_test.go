package converterroundtrip

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type publicIDStoredComment struct {
	ID    uint64          `json:"id"`
	Value publicIDComment `json:"value"`
}

type publicIDObservation struct {
	Current   driver.Meta             `json:"current_target"`
	Items     []pipeline.Document     `json:"items"`
	Comments  []publicIDStoredComment `json:"comments"`
	Events    []model.Changeset       `json:"events"`
	Allocator driver.OfflineAllocator `json:"allocator"`
}

func publicIDReader(ctx context.Context, store driver.Store) (driver.Reader, error) {
	meta, err := store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
}

// Exhaust a fixed unfiltered snapshot. Native namespaces and wrong-Kind
// extras cannot evade the initial-row comparison through a Kind selector.
func publicIDCensus(ctx context.Context, store driver.Store) ([]driver.Record, error) {
	r, err := publicIDReader(ctx, store)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	at := *r.Snapshot().At
	rows := []driver.Record{}
	seen, cursors := map[string]bool{}, map[string]bool{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Limit: 256, AsOf: &at, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		if page.AsOf.String() != at.String() {
			return nil, fmt.Errorf("consumer census snapshot changed")
		}
		for _, row := range page.Items {
			key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref})
			if err != nil || seen[key] {
				return nil, fmt.Errorf("consumer census duplicate/invalid Ref")
			}
			seen[key] = true
			rows = append(rows, row)
		}
		if page.NextCursor == nil {
			break
		}
		next := page.NextCursor
		if len(page.Items) == 0 || next.Incarnation != r.Snapshot().Incarnation || next.Revision.String() != at.String() || next.Method != "query" {
			return nil, fmt.Errorf("consumer census cursor binding differs")
		}
		encoded, err := json.Marshal(next)
		if err != nil || cursors[string(encoded)] {
			return nil, fmt.Errorf("consumer census cursor repeats")
		}
		cursors[string(encoded)] = true
		cursor = next
	}
	sort.Slice(rows, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &rows[i].Ref})
		b, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &rows[j].Ref})
		return a < b
	})
	return rows, nil
}

func publicIDPreserved(initial, actual []driver.Record) error {
	byKey := map[string]driver.Record{}
	for _, row := range actual {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref})
		if err != nil {
			return err
		}
		byKey[key] = row
	}
	for _, row := range initial {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref})
		if err != nil {
			return err
		}
		got, found := byKey[key]
		if !found || !reflect.DeepEqual(row, got) {
			return fmt.Errorf("imported row changed after continuation: %s", key)
		}
	}
	return nil
}

func publicIDAllocator(ctx context.Context, store driver.Store, project string, want driver.OfflineAllocator) error {
	r, err := publicIDReader(ctx, store)
	if err != nil {
		return err
	}
	defer r.Close()
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		return fmt.Errorf("actual allocator capability absent")
	}
	got, err := allocators.LoadAllocator(ctx, project)
	if err != nil || !reflect.DeepEqual(got, want) {
		return fmt.Errorf("native allocator differs: got=%+v want=%+v error=%v", got, want, err)
	}
	return nil
}

func publicIDEvent(ctx context.Context, store driver.Store, operation publicIDOperation, meta model.WriteMeta) (model.Changeset, error) {
	r, err := publicIDReader(ctx, store)
	if err != nil {
		return model.Changeset{}, err
	}
	defer r.Close()
	actor, err := driver.PrincipalKey(operation.Actor)
	if err != nil {
		return model.Changeset{}, err
	}
	change, err := r.ChangesetByKey(ctx, actor, "", meta.Method, meta.IdempotencyKey)
	if err != nil {
		return model.Changeset{}, err
	}
	if err := publicIDCheckEvent(change, operation, meta); err != nil {
		return model.Changeset{}, err
	}
	return *change, nil
}

func publicIDCheckEvent(change *model.Changeset, operation publicIDOperation, meta model.WriteMeta) error {
	id, kind := operation.ExpectedID, model.EventBeadCreated
	if operation.Kind == "comment" {
		id, kind = operation.Target, model.EventBeadCommented
	}
	if change == nil || change.ValidateEvents() != nil || change.Kind != model.ChangesetWrite || !reflect.DeepEqual(change.Principal, operation.Actor) || !change.Time.Equal(operation.Clock) || len(change.Events) != 1 || len(change.Changes) != 1 || change.Events[0].Type != kind || change.Events[0].Resource.Ref.ID != id || change.Events[0].Resource.Kind != "bead" || change.Events[0].Provenance.Method != meta.Method || change.IdempotencyKey != meta.IdempotencyKey {
		return fmt.Errorf("actual Plug event(kind,actor,item) missing/misattributed for %s", meta.IdempotencyKey)
	}
	return nil
}

func (c *publicIDConsumer) observe(t *testing.T, ctx context.Context, store driver.Store) (publicIDObservation, error) {
	observation := publicIDObservation{Items: []pipeline.Document{}, Comments: []publicIDStoredComment{}, Events: []model.Changeset{}}
	meta, err := store.Meta(ctx)
	if err != nil {
		return observation, err
	}
	observation.Current = meta
	rows, err := publicIDCensus(ctx, store)
	if err != nil {
		return observation, err
	}
	if err = publicIDPreserved(c.initial, rows); err != nil {
		return observation, err
	}
	byID := map[string]driver.Record{}
	for _, row := range rows {
		byID[row.Ref.ID] = row
	}
	if len(rows) != len(c.initial)+len(c.request.Items) {
		return observation, fmt.Errorf("continuation added/deleted unexpected rows")
	}
	for _, expected := range c.request.Items {
		row, found := byID[expected.Document.ID]
		if !found || row.Kind != "bead" {
			return observation, fmt.Errorf("continued native item missing/wrong kind")
		}
		document, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			return observation, err
		}
		// Comments have the explicitly separate 12:53 tuple/ID contract. Every
		// remaining Document field is compared exactly, including key presence.
		fields := document
		fields.Comments = nil
		if !reflect.DeepEqual(fields, expected.Document) {
			return observation, fmt.Errorf("continued item fields differ for %s: got=%+v want=%+v", fields.ID, fields, expected.Document)
		}
		latest := -1
		for index, operation := range c.request.Operations {
			if operation.ExpectedID == document.ID || operation.Target == document.ID {
				latest = index
			}
		}
		if latest < 0 {
			return observation, fmt.Errorf("continued item has no original write")
		}
		lastWrite, err := publicIDEvent(ctx, store, c.request.Operations[latest], publicIDMeta(c.request.Case, latest, c.request.Operations[latest].Project))
		if err != nil || lastWrite.Revision.String() != row.Revision.String() || !lastWrite.Time.Equal(c.request.Operations[latest].Clock) {
			return observation, fmt.Errorf("continued item latest write/time differs: %v", err)
		}
		if c.request.Operations[latest].Kind != "comment" && (document.CreatedAt == nil || !lastWrite.Time.Equal(*document.CreatedAt)) {
			return observation, fmt.Errorf("new item last write time differs from its exact created_at")
		}
		observation.Items = append(observation.Items, fields)
		var after *service.CommentPosition
		seen := map[uint64]bool{}
		comments := make([]service.Comment, len(document.Comments))
		for index, comment := range document.Comments {
			comments[index] = service.Comment(comment)
		}
		for {
			page, err := service.PageComments(comments, 1, after)
			if err != nil {
				return observation, err
			}
			for _, comment := range page.Items {
				if comment.ID == 0 || seen[comment.ID] {
					return observation, fmt.Errorf("Plug comment ID missing/duplicate")
				}
				seen[comment.ID] = true
				author, err := driver.PrincipalKey(comment.Comment.Principal)
				if err != nil {
					return observation, err
				}
				if comment.Comment.Origin != "" || comment.Comment.SourceAuthor != "" || comment.Comment.SourceTime != nil {
					return observation, fmt.Errorf("new comment acquired invented source provenance")
				}
				observation.Comments = append(observation.Comments, publicIDStoredComment{ID: comment.ID, Value: publicIDComment{Item: document.ID, Body: comment.Comment.Body, Author: author, Time: comment.Comment.Time}})
			}
			if page.Next == nil {
				break
			}
			after = page.Next
		}
	}
	want, got := map[string]int{}, map[string]int{}
	for _, comment := range c.request.Comments {
		raw, err := json.Marshal(comment)
		if err != nil {
			return observation, err
		}
		want[string(raw)]++
	}
	for _, comment := range observation.Comments {
		raw, err := json.Marshal(comment.Value)
		if err != nil {
			return observation, err
		}
		got[string(raw)]++
	}
	if !reflect.DeepEqual(want, got) {
		return observation, fmt.Errorf("new native comment(item,body,author,time) multiset differs")
	}
	for index, operation := range c.request.Operations {
		event, err := publicIDEvent(ctx, store, operation, publicIDMeta(c.request.Case, index, operation.Project))
		if err != nil {
			return observation, err
		}
		observation.Events = append(observation.Events, event)
	}
	last := c.request.Operations[len(c.request.Operations)-1]
	if err := publicIDAllocator(ctx, store, last.Project, last.After); err != nil {
		return observation, err
	}
	observation.Allocator = last.After
	return observation, nil
}

// The existing shared lifecycle owner invokes this on its second reopened
// target. It observes only; it never repeats writes or owns target handles.
func (c *publicIDConsumer) assertReopened(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
	if !c.wrote || c.asserted || report.Archive == nil || report.Archive.SHA256() != c.request.CaptureSHA256 || profile.HistoryMode != driver.RestoreHistoryModeFull {
		return fmt.Errorf("post-write assertion phase/source differs")
	}
	after, err := c.observe(t, ctx, store)
	if err != nil {
		return err
	}
	publicProofJSON(t, filepath.Join(os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT"), "id-after-postwrite-reopen.json"), after)
	if !reflect.DeepEqual(c.beforeReopen, after) {
		return fmt.Errorf("continued items/comments/IDs/events/counters changed on actual post-write reopen")
	}
	c.asserted = true
	return nil
}
