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
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The policy test adapter checks current source existence through real store
// reads; configured denial/redaction exercises the host authorization contract.
type auditAuthorization struct {
	core.AllowAll
	store         ReadStore
	deny, redact  string
	primaryRedact bool
	fail          error
	resources     []string
}

func (a *auditAuthorization) ReadResource(ctx context.Context, _ model.PrincipalRef, resource model.AuditResource) (core.ResourceVisibility, error) {
	key := resource.Kind + "/" + resource.Ref.ID
	a.resources = append(a.resources, key)
	if a.fail != nil {
		return core.ResourceVisibility{}, a.fail
	}
	meta, err := a.store.Meta(ctx)
	if err != nil {
		return core.ResourceVisibility{}, err
	}
	r, err := a.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return core.ResourceVisibility{}, err
	}
	defer r.Close()
	rows, err := r.Get(ctx, []driver.Ref{resource.Ref})
	if errors.Is(err, driver.ErrNotFound) {
		return core.ResourceVisibility{}, nil
	}
	if err != nil {
		return core.ResourceVisibility{}, err
	}
	return core.ResourceVisibility{Readable: len(rows) == 1 && rows[0].Kind == resource.Kind && key != a.deny, Redacted: key == a.redact}, nil
}

func (a *auditAuthorization) Read(ctx context.Context, principal model.PrincipalRef, event model.DurableEvent) (core.Visibility, error) {
	if event.Type == model.EventDependencyConditionUnmet && a.primaryRedact {
		return core.Visibility{Readable: true, PreviouslyVisible: true, Redacted: true}, nil
	}
	return a.AllowAll.Read(ctx, principal, event)
}

func TestPersistedNoticesRequireAllCurrentSourcesWithoutDisclosure(t *testing.T) {
	for _, action := range []string{"deny", "redact"} {
		for _, resource := range []string{"bead/pl-blocker", "bead/pl-dependent-a", "bead/pl-dependent-b", "ontology/pl-ontology", "project/pl-project"} {
			t.Run(action+"/"+resource, func(t *testing.T) {
				f, _ := auditImages(t)
				store := &readerTracker{ReadStore: f.store}
				policy := &auditAuthorization{store: store}
				if action == "deny" {
					policy.deny = resource
				} else {
					policy.redact = resource
				}
				f.reader.Authorization = policy
				page, err := f.service.List(f.ctx, auditRequest(t, f, "event.list"))
				want := 2
				if strings.Contains(resource, "dependent-") {
					want = 3
				}
				if err != nil || len(page.Events) != want {
					t.Fatalf("masked notice page=%+v %v", page, err)
				}
				for _, event := range page.Events {
					if event.Control != nil {
						t.Fatal("hidden notice disclosed itself through a marker")
					}
					if event.Durable.Details != nil && event.Durable.Resource == resource {
						t.Fatal("hidden dependent details escaped masking")
					}
				}
				binding, _ := BindingFor("event.list", request().Filter, request().Principal, request().Ontology, request().PolicyVersion)
				position, err := f.service.Cursors.Decode(page.Cursor, binding)
				if err != nil || position.Index.String() != "3" {
					t.Fatalf("masked logical scan did not progress: %+v %v", position, err)
				}
				if store.opened == 0 || store.opened != store.closed {
					t.Fatalf("policy readers opened=%d closed=%d", store.opened, store.closed)
				}
			})
		}
	}
	f, _ := auditImages(t)
	policy := &auditAuthorization{store: f.store}
	f.reader.Authorization = policy
	if _, err := f.service.List(f.ctx, auditRequest(t, f, "event.list")); err != nil {
		t.Fatal(err)
	}
	want := []string{"bead/pl-blocker", "bead/pl-dependent-a", "ontology/pl-ontology", "project/pl-project", "bead/pl-blocker", "bead/pl-dependent-b", "ontology/pl-ontology", "project/pl-project"}
	if !reflect.DeepEqual(policy.resources, want) {
		t.Fatalf("actual protection reads=%v want %v", policy.resources, want)
	}
	policy.primaryRedact = true
	page, err := f.service.List(f.ctx, auditRequest(t, f, "event.list"))
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("primary audit redaction leaked: %+v %v", page, err)
	}
}

func TestPersistedNoticeAuthorizationUnavailableRefusesPageAndCheckpoint(t *testing.T) {
	for _, failure := range []string{"missing capability", "policy unavailable"} {
		t.Run(failure, func(t *testing.T) {
			f, _ := auditImages(t)
			want := core.ErrResourceAuthorization
			f.reader.Authorization = authorizationFunc(core.AllowAll{}.Read)
			if failure == "policy unavailable" {
				want = errors.New("current policy unavailable")
				f.reader.Authorization = &auditAuthorization{store: f.store, fail: want}
			}
			page, err := f.service.List(f.ctx, auditRequest(t, f, "event.list"))
			if !errors.Is(err, want) || len(page.Events) != 0 || page.Cursor != "" {
				t.Fatalf("authorization failure became a partial page: %+v %v", page, err)
			}
			calls := 0
			b := core.Binding{ID: "audit-auth-unavailable", Class: core.Listener, Principal: request().Principal, Handler: handlerFunc(func(context.Context, core.Delivery) error { calls++; return nil })}
			cp := f.checkpoints(t)
			if _, err := cp.Save(f.ctx, b.ID, 0, core.State{Position: auditStart(f), Class: b.Class}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.dispatcher(t).Step(f.ctx, b); !errors.Is(err, want) {
				t.Fatalf("unavailable authorization became delivery success: %v", err)
			}
			state, generation, err := cp.Load(f.ctx, b.ID)
			if err != nil || calls != 0 || generation != 1 || !reflect.DeepEqual(state.Position, auditStart(f)) {
				t.Fatalf("unavailable authorization advanced delivery: %+v %d calls=%d %v", state, generation, calls, err)
			}
		})
	}
}

func TestMissingPersistedNoticePinsSuppressWithoutSkippingOrdinaryEvents(t *testing.T) {
	plan := auditPlan("2")
	plan[1].Details.Ontology.Ref = auditRef("pl-unavailable")
	f, _ := auditImagesWithPlan(t, plan)
	page, err := f.service.List(f.ctx, auditRequest(t, f, "event.list"))
	if err != nil || !reflect.DeepEqual(eventOrdinals(page.Events), []string{"2:0", "2:2", "2:3"}) {
		t.Fatalf("absent retained source suppressed the wrong events: %+v %v", page, err)
	}
	raw, err := json.Marshal(page)
	if err != nil || strings.Contains(string(raw), "pl-unavailable") || strings.Contains(string(raw), "pl-dependent-a") {
		t.Fatalf("suppressed source or dependent disclosed: %s %v", raw, err)
	}
}

func TestPersistedNoticePolicyHidesDeletedCurrentSource(t *testing.T) {
	f, _ := auditImages(t)
	f.commit(t, time.Time{}, change{id: "pl-project", kind: "project", op: model.OpDelete})
	store := &readerTracker{ReadStore: f.store}
	f.reader.Authorization = &auditAuthorization{store: store}
	page, err := f.reader.Read(f.ctx, core.ReadRequest{After: auditStart(f), Principal: request().Principal, Limit: 1000, Changesets: 1})
	if err != nil || !reflect.DeepEqual(eventOrdinals(page.Events()), []string{"2:0", "2:3"}) || page.Position.Index.String() != "3" {
		t.Fatalf("deleted current protection source leaked pinned notice: %+v %v", page, err)
	}
	if store.opened != store.closed {
		t.Fatalf("policy readers opened=%d closed=%d", store.opened, store.closed)
	}
}
