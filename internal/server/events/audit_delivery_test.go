package events

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Only the supplied records are changed. Descriptor publication, historical
// reads and checkpoint CAS all use the actual adopted memory implementation.
func commitAudit(t *testing.T, f *fixture, plan []model.EventDescriptor, changes ...change) model.Changeset {
	t.Helper()
	fixtures := make([]conformance.FixtureChange, 0, len(changes))
	for _, item := range changes {
		body, err := json.Marshal(item.fields)
		if err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, conformance.FixtureChange{Ref: auditRef(item.id), Kind: item.kind, Op: item.op, Body: body, ChangedPaths: item.paths})
	}
	set, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{
		Incarnation: f.incarnation, Kind: model.ChangesetWrite,
		Principal: model.PrincipalRef{Kind: "user", ID: "author"}, Chain: []string{}, Events: plan,
	}, fixtures)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// auditImages creates six source records in revision 1, then an audited batch
// in revision 2. Start after the setup rather than delivering its ontology edit.
func auditStart(f *fixture) core.Position {
	return core.Position{Incarnation: f.incarnation, Revision: "1", Index: "5"}
}

func auditRequest(t *testing.T, f *fixture, method string) Request {
	t.Helper()
	req := request()
	binding, err := BindingFor(method, req.Filter, req.Principal, req.Ontology, req.PolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	req.After, err = f.service.Cursors.Encode(auditStart(f), binding)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func eventOrdinals(events []model.Event) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		p := core.PositionOf(event)
		out = append(out, p.Revision.String()+":"+p.Index.String())
	}
	return out
}

func TestPersistedDescriptorPullSizeOneAndLegacySuccessor(t *testing.T) {
	f, set := auditImages(t)
	f.commit(t, time.Time{}, bead("pl-legacy-a", "legacy-a"), bead("pl-legacy-b", "legacy-b"))
	store := &readerTracker{ReadStore: f.store}
	f.reader.Log, f.reader.Materializer = Log{Store: store}, HistoryMaterializer{Store: store}

	sets, err := (Log{Store: store}).Scan(f.ctx, core.Position{Incarnation: f.incarnation, Revision: "2"}, 1)
	if err != nil || len(sets) != 1 || !reflect.DeepEqual(sets[0].Events, set.Events) || len(sets[0].Changes) != 2 {
		t.Fatalf("actual persisted batch=%+v %v", sets, err)
	}
	req := auditRequest(t, f, "event.list")
	req.Limit = 1
	var all []model.Event
	for range 7 {
		page, err := f.service.List(f.ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) > 1 {
			t.Fatal("size-one page exceeded its visible limit")
		}
		all = append(all, page.Events...)
		req.After = page.Cursor
	}
	if got, want := eventOrdinals(all), []string{"2:0", "2:1", "2:2", "2:3", "3:0", "3:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted logical/legacy sequence=%v want %v", got, want)
	}
	for _, event := range all[1:3] {
		e := event.Durable
		if e.Type != model.EventDependencyConditionUnmet || e.Before != nil || e.After != nil || e.Derived || len(e.Changed) != 0 || e.Details.Dependent.Revision.String() != "1" {
			t.Fatalf("notice changed an unchanged dependent: %+v", e)
		}
	}
	if e := all[3].Durable; e.Resource != "bead/pl-other" || e.Before["title"] != "before" || e.After["title"] != "after" {
		t.Fatalf("logical index 3 lost change anchor 1: %+v", e)
	}
	position := auditStart(f)
	for _, revision := range []string{"2", "3"} {
		page, err := f.reader.Read(f.ctx, core.ReadRequest{After: position, Principal: req.Principal, Limit: 1000, Changesets: 1})
		if err != nil || page.Position.Revision.String() != revision {
			t.Fatalf("bounded size-one scan did not advance to %s: %+v %v", revision, page, err)
		}
		position = page.Position
	}
	if store.opened == 0 || store.opened != store.closed {
		t.Fatalf("readers opened=%d closed=%d", store.opened, store.closed)
	}
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, id := range []string{"pl-dependent-a", "pl-dependent-b"} {
		rows, err := reader.Get(f.ctx, []driver.Ref{auditRef(id)})
		if err != nil || len(rows) != 1 || rows[0].Revision.String() != "1" {
			t.Fatalf("notice changed dependent %s: %+v %v", id, rows, err)
		}
		versions, err := reader.Versions(f.ctx, auditRef(id), driver.PageReq{})
		if err != nil || len(versions.Items) != 1 {
			t.Fatalf("notice invented a dependent version: %+v %v", versions, err)
		}
	}
}

type auditLostAck struct {
	core.Checkpoints
	failed bool
}

func (c *auditLostAck) Save(ctx context.Context, binding string, generation uint64, state core.State) (uint64, error) {
	if !c.failed && state.Position.Revision.String() == "2" && state.Position.Index.String() == "1" {
		c.failed = true
		return generation, errors.New("lost logical index 1 acknowledgement")
	}
	return c.Checkpoints.Save(ctx, binding, generation, state)
}

func TestPersistedNoticeRecoveryReplaysIndexOneAndRetainsLaterChange(t *testing.T) {
	f, _ := auditImages(t)
	f.commit(t, time.Time{}, bead("pl-legacy-a", "legacy-a"), bead("pl-legacy-b", "legacy-b"))
	var items []core.Delivery
	b := core.Binding{ID: "audit-recovery", Class: core.Listener, Principal: request().Principal, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
		if item.Partition != item.Event.Durable.Resource || !item.Late {
			t.Errorf("invalid recovery partition/late: %+v", item)
		}
		items = append(items, item)
		return nil
	})}
	cp := f.checkpoints(t)
	if _, err := cp.Save(f.ctx, b.ID, 0, core.State{Position: auditStart(f), Class: b.Class}); err != nil {
		t.Fatal(err)
	}
	d := f.dispatcher(t)
	d.Checkpoints = &auditLostAck{Checkpoints: cp}
	if _, err := d.Step(f.ctx, b); err == nil {
		t.Fatal("lost notice acknowledgement became success")
	}
	state, _, err := cp.Load(f.ctx, b.ID)
	if err != nil || state.Position.Revision.String() != "2" || !state.Position.Index.IsZero() {
		t.Fatalf("unacknowledged notice advanced durable position: %+v %v", state, err)
	}
	// Reconstruct all adapters and the dispatcher, retaining only actual store
	// records, log and fenced metadata. Index 1 must replay with the same key.
	f.reader = &core.Reader{Log: Log{Store: f.store}, Materializer: HistoryMaterializer{Store: f.store}, Authorization: core.AllowAll{}}
	restarted := f.dispatcher(t)
	for range 3 {
		if _, err := restarted.Step(f.ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	var events []model.Event
	for _, item := range items {
		events = append(events, item.Event)
	}
	if got, want := eventOrdinals(events), []string{"2:0", "2:1", "2:1", "2:2", "2:3", "3:0", "3:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered sequence=%v want %v", got, want)
	}
	if items[1].Key.String() != items[2].Key.String() || !reflect.DeepEqual(items[1].Event, items[2].Event) {
		t.Fatal("notice replay changed key or immutable payload")
	}
	state, generation, err := f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || generation != 7 || state.Position.Revision.String() != "3" || state.Position.Index.String() != "1" {
		t.Fatalf("durable recovery checkpoint=%+v generation=%d %v", state, generation, err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil || meta.Head.String() != "3" {
		t.Fatalf("delivery metadata changed graph head: %+v %v", meta, err)
	}
}

func TestDescriptorSubscriptionGapReopensAfterCompleteLogicalPlan(t *testing.T) {
	f, _ := auditImages(t)
	req := request()
	binding, err := BindingFor("event.subscribe", req.Filter, req.Principal, req.Ontology, req.PolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	req.After, err = f.service.Cursors.Encode(core.Position{Incarnation: "previous-incarnation", Revision: "1"}, binding)
	if err != nil {
		t.Fatal(err)
	}
	gap, err := f.service.Poll(f.ctx, req)
	if err != nil || !gap.Reset || len(gap.Events) != 1 || gap.Events[0].Control == nil || gap.Events[0].Control.Type != model.EventGap {
		t.Fatalf("mandatory gap=%+v %v", gap, err)
	}
	position, err := f.service.Cursors.Decode(gap.Cursor, binding)
	if err != nil || position.Revision.String() != "2" || position.Index.String() != "3" {
		t.Fatalf("gap reopen cursor splits committed descriptor plan: %+v %v", position, err)
	}
	req.After = gap.Cursor
	page, err := f.service.Poll(f.ctx, req)
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("gap reopen redelivered old notices: %+v %v", page, err)
	}
}
