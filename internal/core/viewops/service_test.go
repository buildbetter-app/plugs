package viewops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type testLease struct{ lost chan struct{} }

func (l *testLease) Epoch() uint64                 { return 1 }
func (l *testLease) Renew(context.Context) error   { return nil }
func (l *testLease) Lost() <-chan struct{}         { return l.lost }
func (l *testLease) Release(context.Context) error { return nil }

type syntheticStore struct {
	rows []driver.Record
	meta driver.Meta
}

func (s *syntheticStore) Meta(context.Context) (driver.Meta, error) { return s.meta, nil }
func (s *syntheticStore) Read(_ context.Context, view driver.View) (driver.Reader, error) {
	if view.Incarnation != s.meta.Incarnation {
		return nil, driver.ErrIncarnationChanged{From: view.Incarnation, To: s.meta.Incarnation}
	}
	return &syntheticReader{rows: s.rows, snapshot: driver.View{At: ptr(model.Natural64(1)), Incarnation: s.meta.Incarnation}}, nil
}

func ptr(v model.Natural) *model.Natural { return &v }

type syntheticReader struct {
	rows     []driver.Record
	snapshot driver.View
}

func (r *syntheticReader) Snapshot() driver.View { return r.snapshot }
func (r *syntheticReader) Get(context.Context, []driver.Ref) ([]driver.Record, error) {
	return nil, errors.New("synthetic Get is not used")
}
func (r *syntheticReader) Blob(context.Context, driver.Hash) ([]byte, error) {
	return nil, errors.New("synthetic Blob is not used")
}
func (r *syntheticReader) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) {
	return nil, errors.New("synthetic Observe is not used")
}
func (r *syntheticReader) Query(_ context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	start := uint64(0)
	if q.Cursor != nil {
		var err error
		start, err = q.Cursor.Index.Uint64()
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
	}
	if start > uint64(len(r.rows)) {
		return driver.Page[driver.Record]{}, errors.New("synthetic cursor out of range")
	}
	limit := q.Limit
	if limit == 0 {
		limit = 1000
	}
	end := start + uint64(limit)
	if end > uint64(len(r.rows)) {
		end = uint64(len(r.rows))
	}
	page := driver.Page[driver.Record]{Items: append([]driver.Record(nil), r.rows[start:end]...), AsOf: *r.snapshot.At}
	if end < uint64(len(r.rows)) {
		page.NextCursor = &driver.Cursor{Incarnation: r.snapshot.Incarnation, Revision: *r.snapshot.At, Index: model.Natural64(end), Method: "query"}
	}
	return page, nil
}
func (r *syntheticReader) Versions(context.Context, driver.Ref, driver.PageReq) (driver.Page[driver.Version], error) {
	return driver.Page[driver.Version]{}, errors.New("synthetic Versions is not used")
}
func (r *syntheticReader) Changesets(context.Context, *driver.Cursor, int) ([]model.Changeset, error) {
	return nil, errors.New("synthetic Changesets is not used")
}
func (r *syntheticReader) ChangesetByKey(context.Context, string, string, string, string) (*model.Changeset, error) {
	return nil, errors.New("synthetic ChangesetByKey is not used")
}
func (r *syntheticReader) IdempotencyByKey(context.Context, string, string, string, string) (*driver.IdempotencyRecord, error) {
	return nil, errors.New("synthetic IdempotencyByKey is not used")
}
func (r *syntheticReader) Close() error { return nil }

func TestRunRefusesAfterAuthorizationCandidateCap(t *testing.T) {
	rows := make([]driver.Record, 0, OrdinaryCandidateCap+1)
	bodyFor := func(id string) []byte {
		body, _ := json.Marshal(pipeline.Document{ID: id, Type: "issue", Fields: map[string]any{"status": "open"}})
		return body
	}
	for i := 0; i <= OrdinaryCandidateCap; i++ {
		id := fmt.Sprintf("issue-%05d", i)
		rows = append(rows, driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: bodyFor(id)})
	}
	store := &syntheticStore{rows: rows, meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)}}
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Run(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, RunRequest{Definition: &Definition{Filter: &Filter{}}})
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "scope_too_large" {
		t.Fatalf("got %T %v, want scope_too_large", err, err)
	}
}

func TestRunCountsUnreadableBlockersWithoutLeakingRows(t *testing.T) {
	blocker := model.RefByID("blocker", model.Natural64(0))
	dependent := model.RefByID("dependent", model.Natural64(0))
	body := func(doc pipeline.Document) []byte {
		raw, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	store := &syntheticStore{
		rows: []driver.Record{
			{Ref: blocker, Kind: "bead", Revision: model.Natural64(1), Body: body(pipeline.Document{ID: blocker.ID, Type: "issue", Project: "p", Fields: map[string]any{"status": "open"}})},
			{Ref: dependent, Kind: "bead", Revision: model.Natural64(1), Body: body(pipeline.Document{ID: dependent.ID, Type: "issue", Project: "p", Fields: map[string]any{"status": "open"}, Links: []pipeline.Link{{To: blocker, Kind: "blocks"}}})},
		},
		meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
	}
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{
		AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
			if resource.Ref.ID == blocker.ID {
				return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "hidden blocker"}}
			}
			return nil
		},
		OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, RunRequest{
		Definition: &Definition{Filter: &Filter{Types: []string{"issue"}}, Fields: []string{"id"}},
		Project:    "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != dependent.ID || result.Items[0].HiddenBlockers != 1 {
		t.Fatalf("unexpected authorized dependent result: %+v", result)
	}
}

func TestParseQueryRefusesSymbolicAndAlias(t *testing.T) {
	_, err := parseQuery("id=a&&type=issue")
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "invalid_params" {
		t.Fatalf("got %T %v, want invalid_params", err, err)
	}
}
