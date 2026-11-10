package cardb20

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var engines = []string{"memory", "sqlite", "postgres"}

type storeFixture struct {
	store driver.Store
	open  func() (driver.Store, error)
}

func openStore(t *testing.T, engine string) storeFixture {
	t.Helper()
	ctx := context.Background()
	var open func() (driver.Store, error)
	switch engine {
	case "memory":
		open = func() (driver.Store, error) {
			return memory.NewWithOptions(memory.Options{StoreID: "b20-public", Incarnation: "b20-current"})
		}
	case "sqlite":
		path := filepath.Join(t.TempDir(), "b20.db")
		open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path}) }
	case "postgres":
		dsn := startPostgres(t)
		open = func() (driver.Store, error) { return (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn}) }
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	s, err := open()
	if err != nil {
		t.Fatal(err)
	}
	return storeFixture{s, open}
}

type stableEvent struct {
	Sequence  string `json:"sequence"`
	Operation string `json:"operation"`
	ID        string `json:"id"`
	Actor     string `json:"actor"`
	Title     string `json:"title"`
	Status    string `json:"status"`
}

func projectEvents(t *testing.T, events []model.Event) []stableEvent {
	t.Helper()
	result := make([]stableEvent, 0, len(events))
	for _, event := range events {
		d := event.Durable
		if d == nil || d.After == nil {
			t.Fatalf("expected durable event with after image: %+v", event)
		}
		fields, ok := d.After["fields"].(map[string]any)
		if !ok {
			t.Fatalf("event has no fields: %+v", d)
		}
		op := map[model.DurableEventType]string{model.EventBeadCreated: "create", model.EventBeadUpdated: "update", model.EventBeadClosed: "close"}[d.Type]
		if op == "" {
			t.Fatalf("unexpected event kind %s", d.Type)
		}
		result = append(result, stableEvent{d.Revision.String(), op, fmt.Sprint(d.After["id"]), d.Principal.ID, fmt.Sprint(fields["title"]), fmt.Sprint(fields["status"])})
	}
	return result
}

func create(t *testing.T, r *runtime, title string) model.Commit {
	t.Helper()
	return success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, true))
}
func update(t *testing.T, r *runtime, id, title string) model.Commit {
	t.Helper()
	return success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": title}}, true))
}
func closeBead(t *testing.T, r *runtime, id string) model.Commit {
	t.Helper()
	return success[model.Commit](t, "bead.close", r.call(t, "", "bead.close", map[string]any{"id": map[string]any{"id": id}, "reason": "complete"}, true))
}
func list(t *testing.T, r *runtime, after string, limit int) eventPage {
	t.Helper()
	params := map[string]any{"filter": map[string]any{}, "limit": limit}
	if after != "" {
		params["after"] = after
	}
	return success[eventPage](t, "event.list", r.call(t, "", "event.list", params, false))
}
func journal(t *testing.T, store driver.Store) (driver.Meta, []model.Changeset) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	sets, err := reader.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return meta, sets
}

// This is an ordinary atomic admission refusal. The real Writer resolves the
// absent target during preparation, before opening the write transaction; it
// does not certify rollback after physical storage writes. Claim interleaving,
// authorization overlap and compare-and-set preconditions remain out of scope.
func refuseBatch(t *testing.T, r *runtime, id string) {
	t.Helper()
	response := r.call(t, "", "tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
		map[string]any{"kind": "patch", "target": map[string]any{"id": id}, "fields": map[string]any{"title": "must roll back"}},
		map[string]any{"kind": "patch", "target": map[string]any{"id": "pl-b20-absent"}, "fields": map[string]any{"title": "missing"}},
	}}}, true)
	if response.Error == nil || response.Error.Data.Code != "not_found" {
		t.Fatalf("missing target refusal = %+v", response)
	}
}

// replayCurrent records Plug separately. A native recorder may compare this
// actual projection to its observed reference; no source counter is restored
// or interpreted as a Plug revision.
func replayCurrent(t *testing.T, engine string) []stableEvent {
	t.Helper()
	f := openStore(t, engine)
	r := newRuntime(t, f.store, false, "b20-1")
	if got := create(t, r, "journal anchor"); got.Revision.String() != "1" {
		t.Fatal(got)
	}
	if got := update(t, r, "b20-1", "journal edited"); got.Revision.String() != "2" {
		t.Fatal(got)
	}
	before, sets := journal(t, r.store)
	if got := update(t, r, "b20-1", "journal edited"); got.Revision != before.Head {
		t.Fatal(got)
	}
	refuseBatch(t, r, "b20-1")
	after, afterSets := journal(t, r.store)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sets, afterSets) {
		t.Fatal("no-op/refusal changed committed journal or counter")
	}
	if got := closeBead(t, r, "b20-1"); got.Revision.String() != "3" {
		t.Fatalf("post-refusal write burned a revision: %+v", got)
	}
	page := list(t, r, "", 100)
	projection := projectEvents(t, page.Events)
	want := []stableEvent{{"1", "create", "b20-1", "cardb20-reader", "journal anchor", "todo"}, {"2", "update", "b20-1", "cardb20-reader", "journal edited", "todo"}, {"3", "close", "b20-1", "cardb20-reader", "journal edited", "done"}}
	if !reflect.DeepEqual(projection, want) {
		t.Fatalf("actual current journal = %+v", projection)
	}
	finalMeta, sets := journal(t, r.store)
	if len(sets) != 3 {
		t.Fatalf("journal rows=%d", len(sets))
	}
	// Ordinary writes retain the supported legacy Changes plan; Events may be nil.
	// The real event reader above materializes and checks the complete payload.
	for i, set := range sets {
		if err := set.ValidateEvents(); err != nil {
			t.Fatal(err)
		}
		if set.Revision.String() != fmt.Sprint(i+1) || len(set.Changes) != 1 {
			t.Fatalf("journal row%d=%+v", i, set)
		}
	}
	afterCursor := ""
	var paged []model.Event
	for range 3 {
		p := list(t, r, afterCursor, 1)
		if len(p.Events) != 1 || p.Cursor == afterCursor {
			t.Fatalf("one-row since-exclusive page=%+v", p)
		}
		paged = append(paged, p.Events...)
		afterCursor = p.Cursor
	}
	if !reflect.DeepEqual(projectEvents(t, paged), projection) {
		t.Fatal("bounded pages changed row order or payload")
	}
	caught := list(t, r, afterCursor, 1)
	if len(caught.Events) != 0 || caught.Cursor != afterCursor {
		t.Fatalf("caught-up page=%+v", caught)
	}
	retain(t, "plug-"+engine+"-current.json", map[string]any{"engine": engine, "projection": projection, "meta": finalMeta, "changesets": sets, "no_op_and_atomic_admission_refusal": "UNCHANGED", "post_refusal_revision": "3", "physical_storage_rollback": "UNRUN", "unbounded_compat_caller": "EXERCISED_SEPARATELY_THROUGH_REAL_CLI", "native_prune": "FOLDED_OPERATOR_RETENTION_DESIGN_DIFFERENCE", "policy": "NO_POLICY_PLUGIN"})
	observeCurrentCallers(t, r, engine, projection)
	return projection
}

func TestB20CurrentRowsNoOpAbortAndSinceExclusivePages(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) { replayCurrent(t, engine) })
	}
}

func TestB20LiveDeliveryAndReconnectWithinSameSigner(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			f := openStore(t, engine)
			r := newRuntime(t, f.store, true, "b20-1")
			open := func(session, from string) (subscriptionResult, *watch.Subscription) {
				params := map[string]any{"filter": map[string]any{}}
				if from != "" {
					params["from"] = from
				}
				opened := success[subscriptionResult](t, "event.subscribe", r.call(t, session, "event.subscribe", params, false))
				s, err := r.manager.AttachSession(r.ctx, opened.SubscriptionID, session, r.principal)
				if err != nil {
					t.Fatal(err)
				}
				return opened, s
			}
			opened, s := open("first", "")
			initial := nextFrame(t, s)
			if initial.Kind != watch.FrameSnapshot || len(initial.Events) != 0 || initial.Cursor != opened.Cursor {
				t.Fatalf("initial=%+v", initial)
			}
			create(t, r, "journal anchor")
			first := nextFrame(t, s)
			if first.Kind != watch.FrameDelta || len(first.Events) != 1 {
				t.Fatalf("first=%+v", first)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			update(t, r, "b20-1", "journal edited")
			_, resumed := open("second", first.Cursor)
			catchup := nextFrame(t, resumed)
			if catchup.Kind != watch.FrameSnapshot || len(catchup.Events) != 1 || catchup.Cursor == first.Cursor {
				t.Fatalf("catchup=%+v", catchup)
			}
			closeBead(t, r, "b20-1")
			last := nextFrame(t, resumed)
			if last.Kind != watch.FrameDelta || len(last.Events) != 1 {
				t.Fatalf("last=%+v", last)
			}
			delivered := append(append(first.Events, catchup.Events...), last.Events...)
			if !reflect.DeepEqual(projectEvents(t, delivered), projectEvents(t, list(t, r, "", 100).Events)) {
				t.Fatal("live/reconnected order diverged from committed rows")
			}
			if err := resumed.Close(); err != nil {
				t.Fatal(err)
			}
			retain(t, "plug-"+engine+"-live.json", map[string]any{"first": first, "catchup": catchup, "last": last, "signer_lifetime": "SAME_RUNTIME"})
		})
	}
}

func TestB20SQLReopenJournalContinuityAndMemorySameStore(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			f := openStore(t, engine)
			r := newRuntime(t, f.store, false, "b20-1")
			create(t, r, "journal anchor")
			update(t, r, "b20-1", "journal edited")
			before, sets := journal(t, r.store)
			if engine == "memory" {
				// This is same-store readback. Opening another memory store would
				// be a different store, not evidence of memory persistence.
				after, again := journal(t, r.store)
				if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sets, again) {
					t.Fatal("same-store readback changed")
				}
				return
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := f.open()
			if err != nil {
				t.Fatal(err)
			}
			after, again := journal(t, reopened)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sets, again) {
				t.Fatal("SQL reopen changed durable journal")
			}
			// A new runtime starts a new cursor lifetime. No old signed cursor
			// crosses the reopen; the assertion concerns durable rows only.
			next := newRuntime(t, reopened, false, "unused")
			closeBead(t, next, "b20-1")
			final, all := journal(t, reopened)
			if final.Head.String() != "3" || len(all) != 3 || !reflect.DeepEqual(all[:2], sets) {
				t.Fatal("post-reopen journal did not continue gaplessly")
			}
			if len(list(t, next, "", 100).Events) != 3 {
				t.Fatal("reopened journal projection omitted rows")
			}
			retain(t, "plug-"+engine+"-reopen.json", map[string]any{"before": before, "after": after, "continued": final, "changesets": all, "old_signed_cursor_reused": false})
		})
	}
}

func retain(t *testing.T, name string, value any) {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if root == "" {
		t.Fatal("Bazel undeclared output directory required")
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
