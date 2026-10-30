package service

import (
	"context"
	"errors"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type eventProvider struct{ service serverevents.Service }

func (p eventProvider) List(ctx context.Context, req EventProviderRequest) (EventPage, error) {
	page, err := p.service.List(ctx, serverevents.Request{
		Principal: req.Principal,
		Filter:    coreevents.Filter{Types: req.Filter.Types, Resources: req.Filter.Resources},
		After:     req.After,
		Limit:     req.Limit,
	})
	return EventPage{Events: page.Events, Cursor: page.Cursor}, err
}

func TestHistoryEventAndHealthReadsUsePinnedStorageWithoutMutation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	storeMeta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cursorCodec, err := serverevents.NewCursorCodec([]byte("history-read-test-cursor-key-000000"))
	if err != nil {
		t.Fatal(err)
	}
	f.svc.events = eventProvider{service: serverevents.Service{
		Reader:  &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: coreevents.AllowAll{}},
		Cursors: cursorCodec,
	}}

	ref, first := create(t, f, "first")
	second, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: ref, Fields: map[string]any{"title": "second"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.EventList(ctx, anonymous, EventListRequest{Filter: &Filter{}, Limit: 1})
	if err != nil || len(page.Events) != 1 || page.Cursor == "" {
		t.Fatalf("first event page=%+v err=%v", page, err)
	}
	resumed, err := f.svc.EventList(ctx, anonymous, EventListRequest{Filter: &Filter{}, After: page.Cursor, Limit: 1})
	if err != nil || len(resumed.Events) != 1 || resumed.Events[0].Durable == nil || resumed.Events[0].Durable.Revision.String() != second.Revision.String() {
		t.Fatalf("resumed event page=%+v err=%v", resumed, err)
	}

	version, err := f.svc.HistoryGet(ctx, anonymous, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: first.Revision, Incarnation: storeMeta.Incarnation}})
	if err != nil || version.Body["fields"].(map[string]any)["title"] != "first" || !version.Base {
		t.Fatalf("history version=%+v err=%v", version, err)
	}
	diff, err := f.svc.HistoryDiff(ctx, anonymous, HistoryDiffRequest{Ref: ref, FromRevision: first.Revision, ToRevision: second.Revision})
	if err != nil || len(diff.Patch) != 1 || diff.Patch[0].Path != "/fields/title" || diff.Patch[0].Value != "second" {
		t.Fatalf("history diff=%+v err=%v", diff, err)
	}
	healthBefore, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	health, err := f.svc.Health(ctx, anonymous)
	if err != nil || health.Head.String() != healthBefore.Head.String() || health.Incarnation != healthBefore.Incarnation || health.Role != "active" {
		t.Fatalf("health=%+v err=%v meta=%+v", health, err, healthBefore)
	}
	if health.Head.String() != second.Revision.String() {
		t.Fatalf("health head=%s want %s", health.Head, second.Revision)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: storeMeta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
}

func TestHistoryReadsMapFutureRevisionToNotFound(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	ref, created := create(t, f, "future revision")
	storeMeta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	head, err := storeMeta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	future := model.Natural64(head + 1)

	assertFutureHistoryNotFound := func(name string, err error) {
		t.Helper()
		var typed *model.Error
		if !errors.As(err, &typed) || typed.Data.Code != "not_found" {
			t.Fatalf("%s error=%T %v, want typed not_found", name, err, err)
		}
		if typed.Data.Details["requestedRevision"] != future || typed.Data.Details["headRevision"] != storeMeta.Head {
			t.Fatalf("%s details=%#v, want requested=%s head=%s", name, typed.Data.Details, future, storeMeta.Head)
		}
	}

	_, err = f.svc.HistoryGet(ctx, anonymous, HistoryVersionRequest{
		Ref:  ref,
		AsOf: AsOf{Revision: future, Incarnation: storeMeta.Incarnation},
	})
	assertFutureHistoryNotFound("history.get", err)

	_, err = f.svc.HistoryDiff(ctx, anonymous, HistoryDiffRequest{
		Ref:          ref,
		FromRevision: created.Revision,
		ToRevision:   future,
	})
	assertFutureHistoryNotFound("history.diff", err)
}
