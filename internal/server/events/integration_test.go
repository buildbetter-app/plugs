package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type fixture struct {
	ctx         context.Context
	store       *memory.Store
	lease       driver.ActiveLease
	incarnation string
	reader      *core.Reader
	service     Service
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "events-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := &core.Reader{Log: Log{Store: store}, Materializer: HistoryMaterializer{Store: store}, Authorization: core.AllowAll{}}
	return &fixture{ctx: ctx, store: store, lease: lease, incarnation: meta.Incarnation, reader: reader, service: Service{Reader: reader, Cursors: codecForTest(t)}}
}

type change struct {
	id     string
	kind   string
	op     model.OpKind
	fields map[string]any
	paths  []string
}

// The shared storage fixture owns transactions and real log publication.
// Event source and tests receive no transaction handles.
func (f *fixture) commit(t *testing.T, at time.Time, changes ...change) model.Changeset {
	t.Helper()
	fixtures := make([]conformance.FixtureChange, 0, len(changes))
	for _, item := range changes {
		data, err := json.Marshal(item.fields)
		if err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, conformance.FixtureChange{Ref: model.RefByID(item.id, model.Natural64(0)), Kind: item.kind, Op: item.op, Body: data, ChangedPaths: append([]string{}, item.paths...)})
	}
	set, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Incarnation: f.incarnation, Kind: model.ChangesetWrite, Time: at, Principal: model.PrincipalRef{Kind: "user", ID: "author"}, Chain: []string{}}, fixtures)
	if err != nil {
		t.Fatal(err)
	}
	return set
}
func bead(id, title string) change {
	return change{id: id, kind: "bead", op: model.OpCreate, fields: map[string]any{"title": title}, paths: []string{"/title"}}
}
func request() Request {
	return Request{Principal: model.PrincipalRef{Kind: "user", ID: "reader"}, Limit: 1000, Ontology: model.Natural64(1), PolicyVersion: "policy-1", SubscriptionID: "sub-1"}
}
func (f *fixture) checkpoints(t *testing.T) *Checkpoints {
	t.Helper()
	cp, err := NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	return cp
}
func (f *fixture) dispatcher(t *testing.T) *core.Dispatcher {
	return &core.Dispatcher{Reader: f.reader, Checkpoints: f.checkpoints(t), Active: f.lease}
}

func TestPullCommittedHistoryAndPartialChangesetResume(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-b", "first-b"), bead("pl-a", "first-a"))
	f.commit(t, time.Time{}, change{id: "pl-a", kind: "bead", op: model.OpPatch, fields: map[string]any{"title": "second-a", "number": json.Number("9007199254740993")}, paths: []string{"/title", "/number"}})
	f.commit(t, time.Time{}, change{id: "pl-b", kind: "bead", op: model.OpDelete, paths: []string{"/title"}})
	req := request()
	req.Limit = 1
	var all []model.Event
	for i := 0; i < 5; i++ {
		page, err := f.service.List(f.ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Events...)
		req.After = page.Cursor
	}
	if len(all) != 4 {
		t.Fatalf("events=%d want4", len(all))
	}
	for i, want := range []string{"bead/pl-b", "bead/pl-a", "bead/pl-a", "bead/pl-b"} {
		if all[i].Durable.Resource != want {
			t.Fatalf("event%d=%+v", i, all[i])
		}
	}
	if all[0].Durable.Revision.String() != "1" || all[0].Durable.Index.String() != "0" || all[1].Durable.Revision.String() != "1" || all[1].Durable.Index.String() != "1" {
		t.Fatal("lost stable index order")
	}
	updated := all[2].Durable
	if updated.Type != model.EventBeadUpdated || updated.Before["title"] != "first-a" || updated.After["title"] != "second-a" || updated.After["number"].(json.Number).String() != "9007199254740993" {
		t.Fatalf("patch images=%+v", updated)
	}
	deleted := all[3].Durable
	if deleted.Type != model.EventBeadDeleted || deleted.Before["title"] != "first-b" || deleted.After != nil {
		t.Fatalf("delete=%+v", deleted)
	}
	if updated.SetBy["title"] != "request" || len(updated.Chain) != 0 || updated.Cause.Root == "" {
		t.Fatal("missing durable envelope metadata")
	}
	_, err := f.reader.Read(f.ctx, core.ReadRequest{After: core.Position{Incarnation: f.incarnation, Revision: "1", Index: "2"}, Principal: req.Principal, Limit: 1})
	if !errors.Is(err, core.ErrCursorInvalid) {
		t.Fatalf("impossible index: %v", err)
	}
	meta, _ := f.store.Meta(f.ctx)
	if meta.Head.String() != "3" {
		t.Fatal("delivery changed graph head")
	}
}

func TestPullAcrossChangesetScanPageBoundaries(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 65; i++ {
		f.commit(t, time.Time{}, bead(fmt.Sprintf("pl-%03d-b", i), "b"), bead(fmt.Sprintf("pl-%03d-a", i), "a"))
	}
	req := request()
	seen := map[string]bool{}
	count := 0
	for i := 0; i < 4; i++ {
		page, err := f.service.List(f.ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Events {
			key := event.Durable.Revision.String() + ":" + event.Durable.Index.String()
			if seen[key] {
				t.Fatalf("duplicate pull event %s", key)
			}
			seen[key] = true
			count++
		}
		req.After = page.Cursor
	}
	if count != 130 || !seen["65:1"] {
		t.Fatalf("count=%d last=%v", count, seen["65:1"])
	}
}

// This injected test ontology accepts ordinary title writes. The Writer,
// transaction publication, history, event reader and checkpoints are real.
type titleOntology struct{}

func (titleOntology) Normalize(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return []port.Patch{}, nil
}
func (titleOntology) Validate(_ context.Context, proposal port.ProposedChange, _ port.SnapshotReader) error {
	for _, change := range proposal.Changes {
		doc, err := pipeline.DecodeDocument(change.After)
		if err != nil {
			return err
		}
		if title, ok := doc.Fields["title"].(string); !ok || title == "" {
			return errors.New("test ontology requires a title")
		}
	}
	return nil
}

func TestWriterToLogDeliveryAndReplay(t *testing.T) {
	f := newFixture(t)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(titleOntology{}), pipeline.WithIDGenerator(func() (string, error) { return "pl-connected", nil }))
	by := model.PrincipalRef{Kind: "user", ID: "author"}
	create := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "initial"}}}}}
	meta := model.WriteMeta{Method: "tx.apply", IdempotencyKey: strings.Repeat("1", 32)}
	created, err := writer.Apply(f.ctx, by, create, meta)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := writer.Apply(f.ctx, by, create, meta)
	if err != nil || replayed.Revision.String() != created.Revision.String() {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	first, err := f.service.List(f.ctx, request())
	if err != nil || len(first.Events) != 1 || first.Events[0].Durable.Type != model.EventBeadCreated {
		t.Fatalf("create/replay events=%+v err=%v", first, err)
	}
	patch := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: created.Results[0].Ref, Fields: map[string]any{"title": "updated"}}}}}
	meta.IdempotencyKey = strings.Repeat("2", 32)
	updated, err := writer.Apply(f.ctx, by, patch, meta)
	if err != nil {
		t.Fatal(err)
	}
	req := request()
	req.After = first.Cursor
	second, err := f.service.List(f.ctx, req)
	if err != nil || len(second.Events) != 1 {
		t.Fatalf("patch events=%+v err=%v", second, err)
	}
	event := second.Events[0].Durable
	if event.Type != model.EventBeadUpdated || event.Revision.String() != updated.Revision.String() || event.Before["fields"].(map[string]any)["title"] != "initial" || event.After["fields"].(map[string]any)["title"] != "updated" {
		t.Fatalf("writer history=%+v", event)
	}
	if len(event.Changed) != 1 || event.Changed[0] != "/fields/title" {
		t.Fatalf("changed paths must be JSON Pointers: %v", event.Changed)
	}
	var delivered []core.DeliveryKey
	binding := core.Binding{ID: "writer-listener", Class: core.Listener, Principal: req.Principal, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
		delivered = append(delivered, item.Key)
		return nil
	})}
	dispatcher := f.dispatcher(t)
	for i := 0; i < 3; i++ {
		if _, err := dispatcher.Step(f.ctx, binding); err != nil {
			t.Fatal(err)
		}
	}
	state, _, err := f.checkpoints(t).Load(f.ctx, binding.ID)
	if err != nil || len(delivered) != 2 || delivered[0].Revision.String() != created.Revision.String() || delivered[1].Revision.String() != updated.Revision.String() || state.Position.Revision.String() != updated.Revision.String() {
		t.Fatalf("delivery=%v checkpoint=%+v err=%v", delivered, state, err)
	}
}

type authorizationFunc func(context.Context, model.PrincipalRef, model.DurableEvent) (core.Visibility, error)

func (fn authorizationFunc) Read(ctx context.Context, p model.PrincipalRef, e model.DurableEvent) (core.Visibility, error) {
	return fn(ctx, p, e)
}

func TestCurrentAuthorizationMasksHistoryAndVisibilityControls(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, change{id: "pl-secret", kind: "bead", op: model.OpCreate, fields: map[string]any{"title": "visible-title", "secret": "must-not-leak"}, paths: []string{"/title", "/secret"}})
	allowed := true
	f.reader.Authorization = authorizationFunc(func(_ context.Context, p model.PrincipalRef, e model.DurableEvent) (core.Visibility, error) {
		if p.ID != "reader" {
			t.Fatal("wrong effective principal")
		}
		if !allowed {
			return core.Visibility{PreviouslyVisible: true}, nil
		}
		return core.Visibility{Readable: true, After: map[string]any{"title": e.After["title"]}}, nil
	})
	page, err := f.service.List(f.ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(page)
	if strings.Contains(string(encoded), "must-not-leak") {
		t.Fatal("unmasked image leaked")
	}
	allowed = false
	req := request()
	req.Filter = core.Filter{Types: []string{"visibility.lost"}, Resources: []string{"other"}}
	page, err = f.service.List(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Control == nil || page.Events[0].Control.Type != model.EventVisibilityLost {
		t.Fatalf("control=%+v", page)
	}
	encoded, err = json.Marshal(page.Events[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"before", "after", "must-not-leak", "visible-title"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("control leaked %s", forbidden)
		}
	}
	f.reader.Authorization = authorizationFunc(func(context.Context, model.PrincipalRef, model.DurableEvent) (core.Visibility, error) {
		return core.Visibility{}, errors.New("authorization unavailable")
	})
	if _, err = f.service.List(f.ctx, request()); err == nil {
		t.Fatal("authorization failure became a successful page")
	}
}

func TestMandatoryGapAndPolicyResetBoundaries(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-before", "before"))
	f.commit(t, time.Time{}, change{id: "ontology", kind: "ontology", op: model.OpCreate, fields: map[string]any{"version": 1}, paths: []string{"/version"}})
	f.commit(t, time.Time{}, bead("pl-after", "after"))
	req := request()
	req.Filter = core.Filter{Types: []string{"bead.created", "watch.reset"}, Resources: []string{"bead/pl-before", "bead/pl-after"}}
	page, err := f.service.Poll(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Reset || len(page.Events) != 2 || page.Events[0].Durable.Resource != "bead/pl-before" || page.Events[1].Control.Type != model.EventWatchReset {
		t.Fatalf("boundary=%+v", page)
	}
	if _, err = json.Marshal(page.Events[1]); err != nil {
		t.Fatal(err)
	}
	// A new actual memory store supplies a different incarnation. Its graph
	// does not need a matching resource for the obligatory gap to be sent.
	other := newFixture(t)
	req.After = page.Cursor
	gap, err := other.service.Poll(other.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !gap.Reset || len(gap.Events) != 1 || gap.Events[0].Control.Type != model.EventGap {
		t.Fatalf("gap=%+v", gap)
	}
	encoded, err := json.Marshal(gap.Events[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "before") || strings.Contains(string(encoded), "after") || !strings.Contains(string(encoded), `"derived":false`) {
		t.Fatal("invalid gap wire union")
	}
	// Pull reports the incarnation boundary as a typed error.
	binding, _ := BindingFor("event.list", req.Filter, req.Principal, req.Ontology, req.PolicyVersion)
	req.After, _ = other.service.Cursors.Encode(core.Position{Incarnation: f.incarnation, Revision: "1"}, binding)
	var boundary *core.BoundaryError
	if _, err = other.service.List(other.ctx, req); !errors.As(err, &boundary) || boundary.Code != "incarnation_changed" {
		t.Fatalf("pull boundary=%v", err)
	}
}

func TestRolledBackStorageFixtureEmitsNoEvent(t *testing.T) {
	f := newFixture(t)
	err := conformance.SeedRollback(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "author"}}, []conformance.FixtureChange{{Ref: model.RefByID("pl-rollback", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"title":"never committed"}`), ChangedPaths: []string{"/title"}}})
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.service.List(f.ctx, request())
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("rollback events=%+v %v", page, err)
	}
	set := f.commit(t, time.Time{}, bead("pl-real", "real"))
	if set.Revision.String() != "1" {
		t.Fatal("rollback consumed a revision")
	}
	page, err = f.service.List(f.ctx, request())
	if err != nil || len(page.Events) != 1 || page.Events[0].Durable.Resource != "bead/pl-real" {
		t.Fatalf("committed events=%+v %v", page, err)
	}
}

type describerFunc func(context.Context, model.Changeset, int, string) (Description, error)

func (fn describerFunc) Describe(ctx context.Context, set model.Changeset, index int, kind string) (Description, error) {
	return fn(ctx, set, index, kind)
}

func TestUnsupportedProvenanceAndFreeFormTypesRefuse(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-a", "a"))
	f.commit(t, time.Time{}, change{id: "pl-a", kind: "bead", op: model.OpPatch, fields: map[string]any{"status": "done"}, paths: []string{"/status"}})
	if page, err := f.service.List(f.ctx, request()); !errors.Is(err, driver.ErrUnsupported) || len(page.Events) != 0 {
		t.Fatalf("invented slot event: %+v %v", page, err)
	}
	// Even an injected host description must select the closed event union.
	f.reader.Materializer = HistoryMaterializer{Store: f.store, Describer: describerFunc(func(context.Context, model.Changeset, int, string) (Description, error) {
		return Description{Type: "plugin.custom", SetBy: map[string]string{}, Cause: model.EventCause{Root: "root", Chain: []string{}}}, nil
	})}
	if _, err := f.service.List(f.ctx, request()); err == nil {
		t.Fatal("free-form plugin event accepted")
	}
	set := model.Changeset{Incarnation: f.incarnation, Revision: "1", Principal: model.PrincipalRef{Kind: "user", ID: "author"}, Mutators: []string{"mutator"}, Changes: []model.ChangeRecord{{Op: model.OpCreate}}}
	if _, err := describeBasic(set, 0, "bead"); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("fabricated missing mutator attribution")
	}
	set.Mutators = nil
	set.Principal = model.PrincipalRef{Kind: "plugin", ID: "plugin:x/default"}
	if _, err := describeBasic(set, 0, "bead"); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("fabricated missing causal provenance")
	}
	set.Principal = model.PrincipalRef{Kind: "user", ID: "author"}
	for _, path := range []string{"fields.title", "title", "/title~", "/title~2"} {
		set.Changes[0] = model.ChangeRecord{Ref: model.RefByID("pl-a", model.Natural64(0)), Op: model.OpPatch, ChangedPaths: []string{path}}
		if _, err := (HistoryMaterializer{Store: f.store}).Materialize(f.ctx, set, 0); !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("accepted malformed changed path %s: %v", path, err)
		}
	}
	for _, path := range []string{"/fields", "/fields/status", "/fields/status/label"} {
		set.Changes[0] = model.ChangeRecord{Op: model.OpPatch, ChangedPaths: []string{path}}
		if _, err := describeBasic(set, 0, "bead"); !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("fabricated slot provenance for %s", path)
		}
	}
	set.Changes[0].Op = "arbitrary"
	if _, err := describeBasic(set, 0, "memory"); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("invented event for unknown operation")
	}
}

func TestReadRequiresAuthorizationAndIncarnation(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-a", "a"))
	if _, err := f.reader.Read(f.ctx, core.ReadRequest{After: core.Position{Revision: "1"}, Principal: request().Principal, Limit: 1}); !errors.Is(err, core.ErrCursorInvalid) {
		t.Fatalf("unbound revision=%v", err)
	}
	f.reader.Authorization = nil
	if _, err := f.service.List(f.ctx, request()); err == nil {
		t.Fatal("missing authorization silently allowed")
	}
}

type readerTracker struct {
	ReadStore
	opened, closed int
}

func (s *readerTracker) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.ReadStore.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.opened++
	return &trackedReader{Reader: reader, closed: &s.closed}, nil
}

type trackedReader struct {
	driver.Reader
	closed *int
}

func (r *trackedReader) Close() error {
	*r.closed += 1
	return r.Reader.Close()
}

func TestStorageReadersCloseOnSuccessAndFailure(t *testing.T) {
	f := newFixture(t)
	set := f.commit(t, time.Time{}, bead("pl-a", "a"))
	for _, test := range []struct {
		name    string
		run     func(*readerTracker) error
		wantErr bool
	}{
		{"scan", func(store *readerTracker) error {
			_, err := (Log{Store: store}).Scan(f.ctx, core.Position{Incarnation: f.incarnation}, 10)
			return err
		}, false},
		{"scan-invalid-limit", func(store *readerTracker) error {
			_, err := (Log{Store: store}).Scan(f.ctx, core.Position{Incarnation: f.incarnation}, -1)
			return err
		}, true},
		{"scan-overflow", func(store *readerTracker) error {
			_, err := (Log{Store: store}).Scan(f.ctx, core.Position{Incarnation: f.incarnation, Revision: "18446744073709551616"}, 10)
			return err
		}, true},
		{"image", func(store *readerTracker) error {
			_, err := (HistoryMaterializer{Store: store}).image(f.ctx, f.incarnation, model.RefByID("pl-a", model.Natural64(0)), set.Revision)
			return err
		}, false},
		{"missing-image", func(store *readerTracker) error {
			_, err := (HistoryMaterializer{Store: store}).image(f.ctx, f.incarnation, model.RefByID("pl-missing", model.Natural64(0)), set.Revision)
			return err
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &readerTracker{ReadStore: f.store}
			if err := test.run(store); (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
			if store.opened != 1 || store.closed != store.opened {
				t.Fatalf("readers opened=%d closed=%d", store.opened, store.closed)
			}
		})
	}
}
