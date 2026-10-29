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
	req.Filter = core.Filter{Types: []string{"bead.created"}, Resources: []string{"bead/pl-before", "bead/pl-after"}}
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
