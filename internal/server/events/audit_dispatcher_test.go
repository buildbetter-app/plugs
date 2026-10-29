package events

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestPersistedNoticePostFailureWarnsAndRecoversExactLogicalKey(t *testing.T) {
	f, set := auditImages(t)
	var items []core.Delivery
	b := core.Binding{ID: "audit-post", Class: core.Post, Principal: request().Principal, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
		items = append(items, item)
		if item.Key.Index.String() == "1" {
			return errors.New("notice destination failed")
		}
		return nil
	})}
	cp := f.checkpoints(t)
	if _, err := cp.Save(f.ctx, b.ID, 0, core.State{Position: auditStart(f), Class: b.Class}); err != nil {
		t.Fatal(err)
	}
	if warnings := f.dispatcher(t).RunPost(f.ctx, []core.Binding{b}, set.Revision); len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("committed notice post failure warnings=%+v", warnings)
	}
	state, _, err := cp.Load(f.ctx, b.ID)
	if err != nil || state.Failure == nil || !state.Failure.DeadLetter || state.Failure.Key.Index.String() != "1" || state.Position.Revision.String() != "2" || !state.Position.Index.IsZero() {
		t.Fatalf("notice failure lost logical checkpoint: %+v %v", state, err)
	}
	raw, err := json.Marshal(state)
	if err != nil || strings.Contains(string(raw), "details") || strings.Contains(string(raw), "pl-blocker") || strings.Contains(string(raw), "pl-dependent") || strings.Contains(string(raw), "done") {
		t.Fatalf("dead letter retained audit facts: %s %v", raw, err)
	}
	b.Handler = handlerFunc(func(_ context.Context, item core.Delivery) error {
		items = append(items, item)
		if !item.Late {
			t.Error("post recovery missing late marker")
		}
		return nil
	})
	if _, err := f.dispatcher(t).Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	var events []model.Event
	for _, item := range items {
		events = append(events, item.Event)
	}
	if got := eventOrdinals(events); !reflect.DeepEqual(got, []string{"2:0", "2:1", "2:1", "2:2", "2:3"}) || items[0].Late || items[1].Late || items[1].Key.String() != items[2].Key.String() {
		t.Fatalf("post recovery sequence=%v", got)
	}
	state, _, err = cp.Load(f.ctx, b.ID)
	if err != nil || state.Failure != nil || state.Position.Index.String() != "3" {
		t.Fatalf("post recovery durable position=%+v %v", state, err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil || meta.Head.String() != "2" {
		t.Fatalf("post warning/recovery changed committed head: %+v %v", meta, err)
	}
}

func TestPersistedDescriptorProjectionReceivesWholeOrderedBatch(t *testing.T) {
	f, _ := auditImages(t)
	f.commit(t, time.Time{}, bead("pl-legacy-a", "legacy-a"), bead("pl-legacy-b", "legacy-b"))
	var groups []core.ChangeDelivery
	fail := true
	b := core.Binding{ID: "audit-projection", Class: core.Projection, Principal: request().Principal, Projector: projectorFunc(func(_ context.Context, group core.ChangeDelivery) error {
		groups = append(groups, group)
		if fail {
			return errors.New("projection failed")
		}
		return nil
	})}
	cp := f.checkpoints(t)
	if _, err := cp.Save(f.ctx, b.ID, 0, core.State{Position: auditStart(f), Class: b.Class}); err != nil {
		t.Fatal(err)
	}
	d := f.dispatcher(t)
	d.Admin = admin()
	if _, err := d.Step(f.ctx, b); err == nil {
		t.Fatal("failed projection was acknowledged")
	}
	if len(groups) != 1 || len(groups[0].Events) != 4 {
		t.Fatalf("projection split the audited batch: %+v", groups)
	}
	for i, item := range groups[0].Events {
		if item.Key.Index.String() != model.Natural64(uint64(i)).String() {
			t.Fatalf("projection reordered descriptor %d: %+v", i, item)
		}
	}
	if _, err := f.dispatcher(t).Step(f.ctx, b); !errors.Is(err, core.ErrHalted) {
		t.Fatalf("projection halt not persisted: %v", err)
	}
	if err := d.Skip(f.ctx, b, adminUser(), "skip notice"); !errors.Is(err, core.ErrSkipForbidden) {
		t.Fatalf("projection skipped part of a descriptor group: %v", err)
	}
	if err := d.Resume(f.ctx, b, adminUser()); err != nil {
		t.Fatal(err)
	}
	fail = false
	for range 2 {
		if _, err := d.Step(f.ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if len(groups) != 3 || !reflect.DeepEqual(groups[0], groups[1]) || groups[2].Revision.String() != "3" || len(groups[2].Events) != 2 {
		t.Fatalf("whole revision recovery/legacy successor=%+v", groups)
	}
	state, _, err := cp.Load(f.ctx, b.ID)
	if err != nil || state.Halted || state.Failure != nil || state.Position.Revision.String() != "3" || state.Position.Index.String() != "1" {
		t.Fatalf("projection did not retain final logical checkpoint: %+v %v", state, err)
	}
}
