package s1b17b20

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const cursorKey = "cardb20-same-process-event-key-1"

//go:embed reference/b20/*.json reference/b20/record_reference.py
var retained embed.FS

type eventProvider struct{ events *serverevents.Service }

func (p eventProvider) List(ctx context.Context, req service.EventProviderRequest) (service.EventPage, error) {
	page, err := p.events.List(ctx, serverevents.Request{
		Principal: req.Principal,
		Filter: coreevents.Filter{
			Types:     req.Filter.Types,
			Resources: req.Filter.Resources,
		},
		After: req.After,
		Limit: req.Limit,
	})
	return service.EventPage{Events: page.Events, Cursor: page.Cursor}, err
}

type runtime struct {
	ctx       context.Context
	cancel    context.CancelFunc
	store     driver.Store
	lease     driver.ActiveLease
	server    *rpc.Server
	manager   *watch.Manager
	principal model.PrincipalRef
	requests  int
	once      sync.Once
	closeErr  error
}

func newRuntime(t *testing.T, store driver.Store, live bool, id string) *runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := store.Lease(ctx, "cardb20-runtime", driver.LeaseTTL)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	issued := false
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease,
		pipeline.WithOntology(ontology),
		pipeline.WithIDGenerator(func() (string, error) {
			if issued {
				return "", fmt.Errorf("unexpected second ID request")
			}
			issued = true
			return id, nil
		}),
	)
	reader := &coreevents.Reader{
		Log:           serverevents.Log{Store: store},
		Materializer:  serverevents.HistoryMaterializer{Store: store},
		Authorization: coreevents.AllowAll{},
	}
	codec, err := serverevents.NewCursorCodec([]byte(cursorKey))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: reader, Cursors: codec}
	domain := service.New(store, lease, writer,
		service.WithOntology(ontology), service.WithEventProvider(eventProvider{events: events}))
	registry, err := rpc.NewRegistry()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var manager *watch.Manager
	if live {
		manager, err = watch.New(ctx, watch.Config{
			Events: events, Coalesce: time.Millisecond,
			IDGenerator: func() (string, error) { return fmt.Sprintf("sub-%s", id), nil },
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		adapter, adapterErr := watchhttp.New(manager, watchhttp.Config{
			Ontology: model.Natural64(1), PolicyVersion: "cardb20-no-policy-plugin",
		})
		if adapterErr != nil {
			cancel()
			t.Fatal(adapterErr)
		}
		if err = adapter.Register(registry); err != nil {
			cancel()
			t.Fatal(err)
		}
	}
	if err = rpc.BindService(registry, domain); err != nil {
		cancel()
		t.Fatal(err)
	}
	if err = rpc.BindTxApply(registry, domain); err != nil {
		cancel()
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{
		DefaultProject: "cardb20", Incarnation: meta.Incarnation,
		OntologyRevision: model.Natural64(1), CursorKey: []byte(cursorKey),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	r := &runtime{ctx: ctx, cancel: cancel, store: store, lease: lease, server: server,
		manager: manager, principal: model.PrincipalRef{Kind: "user", ID: "cardb20-reader"}}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func memoryRuntime(t *testing.T, live bool, id string) *runtime {
	t.Helper()
	store, err := memory.NewWithOptions(memory.Options{
		StoreID: driver.StoreID("cardb20-store"), Incarnation: "cardb20-incarnation",
	})
	if err != nil {
		t.Fatal(err)
	}
	return newRuntime(t, store, live, id)
}

func (r *runtime) Close() error {
	r.once.Do(func() {
		if r.manager != nil {
			r.closeErr = r.manager.Close()
		}
		r.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r.closeErr = errors.Join(r.closeErr, r.lease.Release(ctx), r.store.Close())
	})
	return r.closeErr
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error,omitempty"`
}

func (r *runtime) call(t *testing.T, session, method string, params map[string]any, write bool) response {
	t.Helper()
	r.requests++
	copy := make(map[string]any, len(params)+1)
	for key, value := range params {
		copy[key] = value
	}
	meta := map[string]any{"project": "cardb20", "agent": "cardb20-recorder"}
	if write {
		meta["idempotencyKey"] = fmt.Sprintf("%032x", r.requests)
	}
	copy["_meta"] = meta
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": r.requests, "method": method, "params": copy,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpc.WithPrincipal(r.ctx, r.principal)
	if session != "" {
		ctx = watchhttp.WithSession(ctx, session, r.ctx)
	}
	raw := r.server.Dispatch(ctx, request)
	var envelope response
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("%s response %s: %v", method, raw, err)
	}
	return envelope
}

func success[T any](t *testing.T, method string, envelope response) T {
	t.Helper()
	if envelope.Error != nil {
		t.Fatalf("%s refused with %s", method, envelope.Error.Data.Code)
	}
	var value T
	if err := json.Unmarshal(envelope.Result, &value); err != nil {
		t.Fatalf("decode %s result %s: %v", method, envelope.Result, err)
	}
	return value
}

func nextFrame(t *testing.T, subscription *watch.Subscription) watch.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	frame, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

type subscriptionResult struct {
	SubscriptionID string `json:"subscriptionId"`
	Cursor         string `json:"cursor"`
}

type eventPage struct {
	Events []model.Event `json:"events"`
	Cursor string        `json:"cursor"`
}

func TestB20CommittedEventsAreOrderedLiveAndReconnectWithinOneKeyLifetime(t *testing.T) {
	r := memoryRuntime(t, true, "pl-b20-live")
	opened := success[subscriptionResult](t, "event.subscribe", r.call(t, "session-a", "event.subscribe", map[string]any{"filter": map[string]any{}}, false))
	stream, err := r.manager.AttachSession(r.ctx, opened.SubscriptionID, "session-a", r.principal)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := nextFrame(t, stream)
	if snapshot.Kind != watch.FrameSnapshot || snapshot.Cursor != opened.Cursor || len(snapshot.Events) != 0 {
		t.Fatalf("opening snapshot = %+v", snapshot)
	}

	created := success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "first"},
	}, true))
	firstLive := nextFrame(t, stream)
	if created.Revision.String() != "1" || firstLive.Kind != watch.FrameDelta || len(firstLive.Events) != 1 ||
		firstLive.Events[0].Durable == nil || firstLive.Events[0].Durable.Type != model.EventBeadCreated ||
		firstLive.Events[0].Durable.Revision.String() != "1" || firstLive.Events[0].Durable.Index.String() != "0" {
		t.Fatalf("created=%+v live=%+v", created, firstLive)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}

	updated := success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{
		"id": map[string]any{"id": "pl-b20-live"}, "fields": map[string]any{"title": "second"},
	}, true))
	resumed := success[subscriptionResult](t, "event.subscribe", r.call(t, "session-b", "event.subscribe", map[string]any{
		"filter": map[string]any{}, "from": firstLive.Cursor,
	}, false))
	reconnected, err := r.manager.AttachSession(r.ctx, resumed.SubscriptionID, "session-b", r.principal)
	if err != nil {
		t.Fatal(err)
	}
	catchup := nextFrame(t, reconnected)
	if updated.Revision.String() != "2" || catchup.Kind != watch.FrameSnapshot || len(catchup.Events) != 1 ||
		catchup.Events[0].Durable == nil || catchup.Events[0].Durable.Type != model.EventBeadUpdated ||
		catchup.Events[0].Durable.Revision.String() != "2" || catchup.Cursor == firstLive.Cursor {
		t.Fatalf("updated=%+v catchup=%+v", updated, catchup)
	}
	closed := success[model.Commit](t, "bead.close", r.call(t, "", "bead.close", map[string]any{
		"id": map[string]any{"id": "pl-b20-live"}, "reason": "complete",
	}, true))
	closeLive := nextFrame(t, reconnected)
	if closed.Revision.String() != "3" || closeLive.Kind != watch.FrameDelta || len(closeLive.Events) != 1 {
		t.Fatalf("bead.close with a reason: revision=%s frame kind=%s event count=%d, want revision=3 delta with one event",
			closed.Revision, closeLive.Kind, len(closeLive.Events))
	}
	closeEvent := closeLive.Events[0].Durable
	if closeEvent == nil {
		t.Fatal("bead.close with a reason published a control event, want bead.closed")
	}
	if closeEvent.Type != model.EventBeadClosed {
		t.Fatalf("bead.close with a reason published %s at revision=%s index=%s, want bead.closed",
			closeEvent.Type, closeEvent.Revision, closeEvent.Index)
	}
	if closeEvent.Revision.String() != "3" || closeEvent.Index.String() != "0" || closeLive.Cursor == catchup.Cursor {
		t.Fatalf("bead.closed position/cursor = revision=%s index=%s advanced=%t, want revision=3 index=0 advanced=true",
			closeEvent.Revision, closeEvent.Index, closeLive.Cursor != catchup.Cursor)
	}
	if err := reconnected.Close(); err != nil {
		t.Fatal(err)
	}

	firstPage := success[eventPage](t, "event.list", r.call(t, "", "event.list", map[string]any{"filter": map[string]any{}, "limit": 1}, false))
	secondPage := success[eventPage](t, "event.list", r.call(t, "", "event.list", map[string]any{"filter": map[string]any{}, "after": firstPage.Cursor, "limit": 1}, false))
	thirdPage := success[eventPage](t, "event.list", r.call(t, "", "event.list", map[string]any{"filter": map[string]any{}, "after": secondPage.Cursor, "limit": 1}, false))
	if len(firstPage.Events) != 1 || len(secondPage.Events) != 1 || len(thirdPage.Events) != 1 ||
		firstPage.Events[0].Durable == nil || secondPage.Events[0].Durable == nil || thirdPage.Events[0].Durable == nil ||
		firstPage.Events[0].Durable.Type != model.EventBeadCreated || secondPage.Events[0].Durable.Type != model.EventBeadUpdated ||
		thirdPage.Events[0].Durable.Type != model.EventBeadClosed ||
		firstPage.Events[0].Durable.Revision.String() != "1" || secondPage.Events[0].Durable.Revision.String() != "2" ||
		thirdPage.Events[0].Durable.Revision.String() != "3" || thirdPage.Events[0].Durable.Index.String() != "0" {
		t.Fatalf("pull pages out of order: first=%+v second=%+v third=%+v", firstPage, secondPage, thirdPage)
	}
}

func TestB20NoOpAndAtomicRefusalPublishNoEvent(t *testing.T) {
	r := memoryRuntime(t, false, "pl-b20-abort")
	created := success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "anchor"},
	}, true))
	noop := success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{
		"id": map[string]any{"id": "pl-b20-abort"}, "fields": map[string]any{"title": "anchor"},
	}, true))
	refusal := r.call(t, "", "tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
		map[string]any{"kind": "patch", "target": map[string]any{"id": "pl-b20-abort"}, "fields": map[string]any{"title": "must roll back"}},
		map[string]any{"kind": "link", "from": map[string]any{"id": "pl-b20-abort"}, "to": map[string]any{"id": "pl-b20-abort"}, "linkKind": "blocks"},
	}}}, true)
	if created.Revision.String() != "1" || noop.Revision.String() != "1" || refusal.Error == nil || refusal.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("created=%+v noop=%+v refusal=%+v", created, noop, refusal.Error)
	}
	page := success[eventPage](t, "event.list", r.call(t, "", "event.list", map[string]any{"filter": map[string]any{}, "limit": 100}, false))
	if len(page.Events) != 1 || page.Events[0].Durable == nil || page.Events[0].Durable.Type != model.EventBeadCreated {
		t.Fatalf("no-op or refusal published an event: %+v", page.Events)
	}
	reader, err := r.store.Read(r.ctx, driver.View{Incarnation: "cardb20-incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	sets, err := reader.Changesets(r.ctx, nil, 10)
	if err != nil || len(sets) != 1 {
		t.Fatalf("changesets after no-op/refusal = %d, %v", len(sets), err)
	}
	rows, err := reader.Get(r.ctx, []model.Ref{model.RefByID("pl-b20-abort", model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read rolled-back bead: %d, %v", len(rows), err)
	}
	var body map[string]any
	if err := json.Unmarshal(rows[0].Body, &body); err != nil {
		t.Fatal(err)
	}
	if fields := body["fields"].(map[string]any); fields["title"] != "anchor" {
		t.Fatalf("refused patch landed: %+v", body)
	}
}

func TestB20SQLiteReopenPreservesJournalContinuity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cardb20.db")
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := newRuntime(t, store, false, "pl-b20-sqlite")
	success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "before reopen"},
	}, true))
	success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{
		"id": map[string]any{"id": "pl-b20-sqlite"}, "fields": map[string]any{"title": "after write"},
	}, true))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := reopened.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	sets, err := reader.Changesets(ctx, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if before.Incarnation != after.Incarnation || len(sets) != 2 || sets[0].Revision.String() != "1" || sets[1].Revision.String() != "2" {
		t.Fatalf("before=%+v after=%+v sets=%+v", before, after, sets)
	}
	for i, set := range sets {
		if len(set.Changes) != 1 || set.Changes[0].Ref.ID != "pl-b20-sqlite" {
			t.Fatalf("changeset %d durable journal = %+v", i, set.Changes)
		}
	}

	// Reopening proves the durable log, not signed-cursor persistence. Build a
	// fresh event projection and cursor signer over the reopened store.
	codec, err := serverevents.NewCursorCodec([]byte(cursorKey))
	if err != nil {
		t.Fatal(err)
	}
	projected := &serverevents.Service{Reader: &coreevents.Reader{
		Log:           serverevents.Log{Store: reopened},
		Materializer:  serverevents.HistoryMaterializer{Store: reopened},
		Authorization: coreevents.AllowAll{},
	}, Cursors: codec}
	page, err := projected.List(ctx, serverevents.Request{
		Principal: r.principal, Filter: coreevents.Filter{}, Limit: 10,
		Ontology: model.Natural64(1), PolicyVersion: "cardb20-no-policy-plugin",
	})
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("project reopened journal: %d events, %v", len(page.Events), err)
	}
	want := []model.DurableEventType{model.EventBeadCreated, model.EventBeadUpdated}
	for i, event := range page.Events {
		if event.Durable == nil || event.Durable.Index.String() != "0" || event.Durable.Revision.String() != fmt.Sprint(i+1) || event.Durable.Type != want[i] {
			t.Fatalf("projected event %d = %+v", i, event)
		}
	}
}

type referenceRecording struct {
	SourceCommit string `json:"sourceCommit"`
	BinarySHA    string `json:"binarySha256"`
	Observations struct {
		BeforeNoOpRows int    `json:"beforeNoOpRows"`
		AfterNoOpRows  int    `json:"afterNoOpRows"`
		ProjectionSHA  string `json:"projectionSha256"`
	} `json:"observations"`
	Projection []map[string]any `json:"projection"`
}

type referenceProvenance struct {
	Source struct {
		Commit string `json:"commit"`
		Tree   string `json:"tree"`
	} `json:"source"`
	Recorder struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"recorder"`
	Recordings []struct {
		Path             string `json:"path"`
		FileSHA256       string `json:"fileSha256"`
		ProjectionSHA256 string `json:"projectionSha256"`
	} `json:"recordings"`
}

func shaB20(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func TestB20RetainedBeadsTwinsSelectOnlyStableJournalFacts(t *testing.T) {
	load := func(name string) referenceRecording {
		data, err := retained.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var value referenceRecording
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	a := load("reference/b20/beads-run-a.json")
	b := load("reference/b20/beads-run-b.json")
	const pin = "9d6fff760b7a314b476539198504d77ce0959656"
	const binarySHA = "174083f0816e08f94e65c05362d713b913dac036e7a6c0cea2ab0b054f3ee083"
	if a.SourceCommit != pin || b.SourceCommit != pin || a.BinarySHA != binarySHA || b.BinarySHA != binarySHA ||
		a.Observations.BeforeNoOpRows != 2 || a.Observations.AfterNoOpRows != 2 ||
		b.Observations.BeforeNoOpRows != 2 || b.Observations.AfterNoOpRows != 2 || a.Observations.ProjectionSHA != b.Observations.ProjectionSHA ||
		!reflect.DeepEqual(a.Projection, b.Projection) {
		t.Fatalf("reference twins diverged: a=%+v b=%+v", a, b)
	}
	want := []map[string]any{
		{"seq": float64(1), "op": "create", "issueId": "b20-1", "actor": "b20-recorder", "title": "journal anchor", "status": "open"},
		{"seq": float64(2), "op": "update", "issueId": "b20-1", "actor": "b20-recorder", "title": "journal edited", "status": "open"},
		{"seq": float64(3), "op": "close", "issueId": "b20-1", "actor": "b20-recorder", "title": "journal edited", "status": "closed", "closeReason": "complete"},
	}
	if !reflect.DeepEqual(a.Projection, want) {
		t.Fatalf("unexpected retained projection: %+v", a.Projection)
	}
	provenanceData, err := retained.ReadFile("reference/b20/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var provenance referenceProvenance
	if err := json.Unmarshal(provenanceData, &provenance); err != nil {
		t.Fatal(err)
	}
	recorder, err := retained.ReadFile("reference/b20/record_reference.py")
	if err != nil {
		t.Fatal(err)
	}
	if provenance.Source.Commit != pin || provenance.Source.Tree != "cde121d6e4119e454857ad1c3ca293e2791be840" ||
		provenance.Recorder.Path != "spec/cardb20/record_reference.py" || provenance.Recorder.SHA256 != shaB20(recorder) || len(provenance.Recordings) != 2 {
		t.Fatalf("invalid retained provenance: %+v", provenance)
	}
	for _, recording := range provenance.Recordings {
		data, err := retained.ReadFile("reference/b20/" + filepath.Base(recording.Path))
		if err != nil {
			t.Fatal(err)
		}
		if recording.FileSHA256 != shaB20(data) || recording.ProjectionSHA256 != a.Observations.ProjectionSHA {
			t.Fatalf("recording provenance mismatch: %+v", recording)
		}
	}
}
