package viewops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
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
	rows       []driver.Record
	meta       driver.Meta
	lastReader *syntheticReader
}

type savedViewMap map[string]Definition

func (m savedViewMap) GetView(_ context.Context, _ model.PrincipalRef, id string) (Definition, error) {
	definition, ok := m[id]
	if !ok {
		return Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "saved view is absent"}}
	}
	return definition, nil
}

type readinessFunc func(context.Context, driver.Reader, []model.Ref) (map[string]ReadinessState, error)

func (fn readinessFunc) Pending(ctx context.Context, reader driver.Reader, refs []model.Ref) (map[string]ReadinessState, error) {
	return fn(ctx, reader, refs)
}

type projectionStatusFunc func(context.Context, string) (coreevents.ProjectionStatus, error)

func (fn projectionStatusFunc) Status(ctx context.Context, binding string) (coreevents.ProjectionStatus, error) {
	return fn(ctx, binding)
}

func (s *syntheticStore) Meta(context.Context) (driver.Meta, error) { return s.meta, nil }
func (s *syntheticStore) Read(_ context.Context, view driver.View) (driver.Reader, error) {
	if view.Incarnation != s.meta.Incarnation {
		return nil, driver.ErrIncarnationChanged{From: view.Incarnation, To: s.meta.Incarnation}
	}
	r := &syntheticReader{rows: s.rows, snapshot: driver.View{At: ptr(model.Natural64(1)), Incarnation: s.meta.Incarnation}}
	s.lastReader = r
	return r, nil
}

func ptr(v model.Natural) *model.Natural { return &v }

type syntheticReader struct {
	rows     []driver.Record
	snapshot driver.View
}

func (r *syntheticReader) Snapshot() driver.View { return r.snapshot }
func (r *syntheticReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wanted := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref.IsID() && ref.Branch.IsZero() {
			wanted[ref.ID] = struct{}{}
		}
	}
	rows := make([]driver.Record, 0, len(wanted))
	for _, row := range r.rows {
		if _, ok := wanted[row.Ref.ID]; ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
func (r *syntheticReader) Blob(context.Context, driver.Hash) ([]byte, error) {
	return nil, errors.New("synthetic Blob is not used")
}
func (r *syntheticReader) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) {
	return nil, errors.New("synthetic Observe is not used")
}
func (r *syntheticReader) DerivedRows(context.Context, driver.DerivedTable, string) ([]driver.DerivedRow, error) {
	return nil, errors.New("synthetic DerivedRows is not used")
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

func explainDocument(t *testing.T, id, project string, links ...pipeline.Link) []byte {
	t.Helper()
	body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: project, Fields: map[string]any{"status": "open"}, Links: links})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func explainService(t *testing.T, store *syntheticStore, source DirectBlockerSource, authorize AuthorizeRead) *Service {
	t.Helper()
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{
		Views:            savedViewMap{"issues": {Filter: &Filter{Types: []string{"issue"}}}},
		AuthorizeRead:    authorize,
		DirectBlockers:   source,
		OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestExplainDirectBlockersOptionalProjectionAndPinnedReader(t *testing.T) {
	dependent := model.RefByID("dependent", model.Natural64(0))
	store := &syntheticStore{
		rows: []driver.Record{{Ref: dependent, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, dependent.ID, "dependent-project")}},
		meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
	}
	var gotReader driver.Reader
	var gotDependent model.Ref
	var gotLimit int
	svc := explainService(t, store, func(_ context.Context, reader driver.Reader, ref model.Ref, limit int) (DirectBlockerVerdict, error) {
		gotReader, gotDependent, gotLimit = reader, ref, limit
		return DirectBlockerVerdict{Edges: []DirectBlockerEdge{}}, nil
	}, nil)
	result, err := svc.Explain(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, ExplainRequest{ViewID: "issues", ItemID: dependent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if result.DirectBlockers == nil || result.DirectBlockers.Edges == nil || result.DirectBlockers.HiddenCount != 0 {
		t.Fatalf("configured empty direct blocker result=%+v", result.DirectBlockers)
	}
	if gotReader != store.lastReader || gotDependent != dependent || gotLimit != DirectBlockerExplainLimit {
		t.Fatalf("source did not receive the pinned reader/dependent/limit: reader=%p want=%p dependent=%+v limit=%d", gotReader, store.lastReader, gotDependent, gotLimit)
	}
	raw, err := json.Marshal(result.DirectBlockers)
	if err != nil || string(raw) != `{"edges":[],"hiddenCount":0}` {
		t.Fatalf("explicit empty wire result=%s err=%v", raw, err)
	}

	withoutSource := explainService(t, store, nil, nil)
	result, err = withoutSource.Explain(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, ExplainRequest{ViewID: "issues", ItemID: dependent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if result.DirectBlockers != nil {
		t.Fatalf("unconfigured source must be omitted: %+v", result.DirectBlockers)
	}
}

func TestExplainDirectBlockersUsesCurrentTargetProjectAndRedactsDeniedEdges(t *testing.T) {
	dependent := model.RefByID("dependent", model.Natural64(0))
	visible := model.RefByID("visible", model.Natural64(0))
	hidden := model.RefByID("hidden", model.Natural64(0))
	missing := model.RefByID("missing", model.Natural64(0))
	unresolved := model.RefByKey("unresolved")
	store := &syntheticStore{
		rows: []driver.Record{
			{Ref: dependent, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, dependent.ID, "dependent-project")},
			{Ref: visible, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, visible.ID, "target-project")},
			{Ref: hidden, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, hidden.ID, "target-project")},
		},
		meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
	}
	var checkedProjects []string
	svc := explainService(t, store, func(_ context.Context, _ driver.Reader, _ model.Ref, _ int) (DirectBlockerVerdict, error) {
		return DirectBlockerVerdict{Edges: []DirectBlockerEdge{
			{Blocker: visible, Kind: "blocks", State: DirectBlockerSatisfied},
			{Blocker: hidden, Kind: "blocks", State: DirectBlockerUnsatisfied},
			{Blocker: missing, Kind: "blocks", State: DirectBlockerUnknown, Reason: "missing"},
			{Blocker: unresolved, Kind: "blocks", State: DirectBlockerUnknown, Reason: "unresolved"},
		}}, nil
	}, nil)
	// The source above is intentionally independent of authorization and does
	// not consult the dependent's project. The consumer must authorize each
	// readable target with its stored project.
	svc.cfg.AuthorizeRead = func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, project string) error {
		if resource.Ref.ID == visible.ID || resource.Ref.ID == hidden.ID {
			checkedProjects = append(checkedProjects, project)
			if project != "target-project" {
				return fmt.Errorf("target authorization used %q", project)
			}
		}
		if resource.Ref.ID == hidden.ID {
			return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "hidden blocker"}}
		}
		return nil
	}
	result, err := svc.Explain(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, ExplainRequest{ViewID: "issues", ItemID: dependent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(checkedProjects) != 2 || checkedProjects[0] != "target-project" || checkedProjects[1] != "target-project" {
		t.Fatalf("target authorization projects=%v", checkedProjects)
	}
	if result.DirectBlockers == nil || result.DirectBlockers.HiddenCount != 1 || result.HiddenBlockers != 1 {
		t.Fatalf("redaction counts=%+v hidden=%d", result.DirectBlockers, result.HiddenBlockers)
	}
	if len(result.DirectBlockers.Edges) != 3 || result.DirectBlockers.Edges[0].Blocker.ID != visible.ID || result.DirectBlockers.Edges[1].Blocker.ID != missing.ID || result.DirectBlockers.Edges[2].Blocker.IsID() {
		t.Fatalf("projected edges=%+v", result.DirectBlockers.Edges)
	}
	if result.DirectBlockers.Edges[1].State != DirectBlockerUnknown || result.DirectBlockers.Edges[2].State != DirectBlockerUnknown {
		t.Fatalf("unknown edges were not preserved=%+v", result.DirectBlockers.Edges)
	}
}

func TestExplainDirectBlockersRefusesOverflowAndMapsCancellationWithoutPartial(t *testing.T) {
	dependent := model.RefByID("dependent", model.Natural64(0))
	store := &syntheticStore{
		rows: []driver.Record{{Ref: dependent, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, dependent.ID, "project")}},
		meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
	}
	mode := "overflow"
	svc := explainService(t, store, func(context.Context, driver.Reader, model.Ref, int) (DirectBlockerVerdict, error) {
		if mode == "cancel" {
			return DirectBlockerVerdict{}, context.Canceled
		}
		edges := make([]DirectBlockerEdge, DirectBlockerExplainLimit+1)
		for i := range edges {
			edges[i] = DirectBlockerEdge{Blocker: model.RefByID(fmt.Sprintf("b-%d", i), model.Natural64(0)), Kind: "blocks", State: DirectBlockerUnknown}
		}
		return DirectBlockerVerdict{Edges: edges}, nil
	}, nil)
	result, err := svc.Explain(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, ExplainRequest{ViewID: "issues", ItemID: dependent.ID})
	if err == nil || result.ViewID != "" || result.DirectBlockers != nil || result.Reasons != nil {
		t.Fatalf("overflow result=%+v err=%v", result, err)
	}
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "closure_too_large" {
		t.Fatalf("overflow err=%T %v", err, err)
	}
	mode = "cancel"
	result, err = svc.Explain(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, ExplainRequest{ViewID: "issues", ItemID: dependent.ID})
	if err == nil || result.ViewID != "" || result.DirectBlockers != nil || result.Reasons != nil || !errors.As(err, &typed) || typed.Data.Code != "busy" {
		t.Fatalf("cancellation result=%+v err=%T %v", result, err, err)
	}
}

func TestParseQueryRefusesSymbolicAndAlias(t *testing.T) {
	_, err := parseQuery("id=a&&type=issue")
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "invalid_params" {
		t.Fatalf("got %T %v, want invalid_params", err, err)
	}
}

func TestWatchBindingCompilesExactResourceCheckpoint(t *testing.T) {
	store := &syntheticStore{meta: driver.Meta{StoreID: "store", Incarnation: "inc"}}
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{
		Views:            savedViewMap{"one": {Filter: &Filter{Resources: []string{"bead-one"}}}},
		OntologyRevision: model.Natural64(7),
		PolicyVersion:    "policy-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := svc.WatchBinding(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, "one")
	if err != nil {
		t.Fatal(err)
	}
	if binding.Ontology.String() != "7" || binding.PolicyVersion != "policy-3" || binding.Incarnation != "inc" {
		t.Fatalf("checkpoint generations=%+v", binding)
	}
	if len(binding.Filter.Resources) != 1 || binding.Filter.Resources[0] != "bead/bead-one" {
		t.Fatalf("compiled event filter=%+v", binding.Filter)
	}
}

func TestWatchBindingCompilesBeadTypeMembershipSeparatelyFromEventTypes(t *testing.T) {
	store := &syntheticStore{meta: driver.Meta{StoreID: "store", Incarnation: "inc"}}
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{
		Views:            savedViewMap{"issues": {Filter: &Filter{Types: []string{"issue"}}}},
		OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := svc.WatchBinding(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, "issues")
	if err != nil {
		t.Fatal(err)
	}
	if len(binding.Filter.BeadTypes) != 1 || binding.Filter.BeadTypes[0] != "issue" || len(binding.Filter.Types) != 0 {
		t.Fatalf("bead type membership=%+v", binding.Filter)
	}
}

func TestWatchBindingRefusesUnrepresentableMembershipShapes(t *testing.T) {
	store := &syntheticStore{meta: driver.Meta{StoreID: "store", Incarnation: "inc"}}
	views := savedViewMap{
		"query": {Filter: &Filter{Query: "status=open"}},
		"group": {Filter: &Filter{}, GroupBy: []string{"type"}},
	}
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{Views: views, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"query", "group"} {
		t.Run(id, func(t *testing.T) {
			_, err := svc.WatchBinding(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, id)
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "invalid_params" {
				t.Fatalf("watch binding error=%T %v", err, err)
			}
		})
	}
	var _ coreevents.ViewResolver = svc
}

func TestRunConsumesPinnedReadinessAndProjectionStatus(t *testing.T) {
	ref := model.RefByID("pending", model.Natural64(0))
	body, err := json.Marshal(pipeline.Document{ID: ref.ID, Type: "issue", Project: "p", Fields: map[string]any{"status": "open"}})
	if err != nil {
		t.Fatal(err)
	}
	store := &syntheticStore{
		rows: []driver.Record{{Ref: ref, Kind: "bead", Revision: model.Natural64(1), Body: body}},
		meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
	}
	projection := coreevents.ProjectionStatus{
		Binding:  "core",
		Position: coreevents.Position{Incarnation: "inc", Revision: "1"},
		Stalled:  true,
		Failure:  &coreevents.Failure{Reason: "projection_failed", Attempts: 2},
	}
	var gotRefs []model.Ref
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{
		AuthorizeRead: func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil },
		Readiness: readinessFunc(func(_ context.Context, reader driver.Reader, refs []model.Ref) (map[string]ReadinessState, error) {
			if reader.Snapshot().At == nil || len(refs) != 1 || refs[0].ID != ref.ID {
				return nil, fmt.Errorf("readiness did not receive the pinned candidate snapshot")
			}
			gotRefs = append([]model.Ref(nil), refs...)
			return map[string]ReadinessState{ref.ID: {Pending: true}, "outside": {Pending: true}}, nil
		}),
		ProjectionStatus: projectionStatusFunc(func(_ context.Context, binding string) (coreevents.ProjectionStatus, error) {
			if binding != "core" {
				return coreevents.ProjectionStatus{}, fmt.Errorf("unexpected projection binding %q", binding)
			}
			return projection, nil
		}),
		ProjectionBinding: "core",
		OntologyRevision:  model.Natural64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{
		Definition: &Definition{Filter: &Filter{Types: []string{"issue"}}, Fields: []string{"id"}},
		Project:    "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(gotRefs) != 1 || gotRefs[0].ID != ref.ID || len(result.Items) != 1 || !result.Items[0].ReadinessPending {
		t.Fatalf("pinned readiness result=%+v refs=%+v", result.Items, gotRefs)
	}
	if result.Projection == nil || !result.Projection.Stalled || result.Projection.Failure == nil || result.Projection.Resume == nil || result.Projection.Resume.Position.Revision.String() != "1" {
		t.Fatalf("projection status result=%+v", result.Projection)
	}
	result.Projection.Failure.Reason = "mutated-result"
	if projection.Failure.Reason != "projection_failed" {
		t.Fatalf("projection status was not copied: %+v", projection)
	}
}

func TestCountOnlyFailsClosedOnAuthorizationAndFinalAdmission(t *testing.T) {
	by := model.PrincipalRef{Kind: "user", ID: "reader"}
	for _, code := range []model.ErrorCode{"forbidden", "not_found", "busy", "internal_error", "canceled", "lease_lost"} {
		t.Run(string(code), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lease := &testLease{lost: make(chan struct{})}
			store := &syntheticStore{
				rows: []driver.Record{{Ref: model.RefByID("candidate", model.Natural64(0)), Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, "candidate", "p")}},
				meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
			}
			svc, err := New(store, lease, Config{AuthorizeRead: func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
				switch code {
				case "canceled":
					cancel()
					return nil
				case "lease_lost":
					close(lease.lost)
					return nil
				default:
					return &model.Error{Data: model.ErrorData{Code: code}}
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := svc.Run(ctx, by, RunRequest{CountOnly: true, Definition: &Definition{Filter: &Filter{}}})
			if code == "forbidden" || code == "not_found" {
				if err != nil || result.Count == nil || *result.Count != 0 {
					t.Fatalf("denied candidate must contribute zero: result=%+v err=%v", result, err)
				}
				return
			}
			want := code
			if code == "canceled" || code == "lease_lost" {
				want = "busy"
			}
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != want || result.Count != nil {
				t.Fatalf("count escaped failed authorization/admission: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestCountOnlyKeepsProjectionWithoutPayloadDependencies(t *testing.T) {
	dependent, blocker := model.RefByID("dependent", model.Natural64(0)), model.RefByID("blocker", model.Natural64(0))
	store := &syntheticStore{
		rows: []driver.Record{
			{Ref: dependent, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, dependent.ID, "p", pipeline.Link{To: blocker, Kind: "blocks"})},
			{Ref: blocker, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, blocker.ID, "p")},
		},
		meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)},
	}
	svc, err := New(store, &testLease{lost: make(chan struct{})}, Config{
		AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
			if resource.Ref.ID != dependent.ID {
				t.Fatal("count-only authorized a payload-only blocker target")
			}
			return nil
		},
		Readiness: readinessFunc(func(context.Context, driver.Reader, []model.Ref) (map[string]ReadinessState, error) {
			t.Fatal("count-only requested item readiness")
			return nil, nil
		}),
		ProjectionStatus: projectionStatusFunc(func(context.Context, string) (coreevents.ProjectionStatus, error) {
			return coreevents.ProjectionStatus{Binding: "core", Position: coreevents.Position{Incarnation: "inc", Revision: model.Natural64(1)}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, RunRequest{
		CountOnly: true, Definition: &Definition{Filter: &Filter{Resources: []string{dependent.ID}}},
	})
	if err != nil || result.Count == nil || *result.Count != 1 || result.Items == nil || len(result.Items) != 0 || len(result.Groups) != 0 || result.NextCursor != nil || result.Projection == nil || result.Projection.Binding != "core" {
		t.Fatalf("count projection=%+v err=%v", result, err)
	}
}
