// These disposable public-route controls are separate from native import proof.
package b15comments_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

const project = "b15-public"

var actor = model.PrincipalRef{Kind: "user", ID: "b15-comment-author"}

type fixture struct {
	t           *testing.T
	engine, dsn string
	clock       *testclock.Clock
	store       driver.Store
	lease       driver.ActiveLease
	domain      *service.Service
	server      *rpc.Server
	serial      int
	frames      []json.RawMessage
}

func newFixture(t *testing.T, engine string) *fixture {
	f := &fixture{t: t, engine: engine, clock: testclock.New(t, time.Date(2026, 11, 11, 13, 29, 0, 0, time.UTC))}
	if engine == "sqlite" {
		f.dsn = filepath.Join(t.TempDir(), "comments.db")
	}
	if engine == "postgres" {
		f.dsn = f.clock.BindPostgres(t, testclock.PostgresDSN(t))
	}
	f.open()
	t.Cleanup(func() {
		f.close()
		root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
		if root == "" {
			return
		}
		name := strings.ReplaceAll(t.Name(), "/", "-") + ".json"
		raw, err := json.MarshalIndent(f.frames, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, name), append(raw, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *fixture) open() {
	t := f.t
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: f.clock.Now})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(context.Background(), driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = postgres.Open(context.Background(), driver.Config{DSN: f.dsn})
	default:
		t.Fatalf("unknown engine %s", f.engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(context.Background(), "b15-public", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	f.bind()
}

func (f *fixture) bind() {
	t := f.t
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithClock(f.clock.Now), pipeline.WithOntology(ontology), pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)), pipeline.WithIDGenerator(func() (string, error) { return "b15-anchor", nil }))
	checkpoints, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(checkpoints, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := workflow.New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider = provider.WithClock(f.clock.Now).WithIdentity(workflow.Alias, "default", meta.Incarnation)
	f.domain = service.New(f.store, f.lease, writer, service.WithClock(f.clock.Now), service.WithOntology(ontology), service.WithWorkflow(provider))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, AuthMode: "authenticated", Incarnation: meta.Incarnation, CursorKey: bytes.Repeat([]byte{0x15}, 32)})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) close() {
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) reopen() {
	if f.engine == "memory" {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Fatal(err)
		}
		var err error
		f.lease, err = f.store.Lease(context.Background(), "b15-public-reopen", driver.LeaseTTL)
		if err != nil {
			f.t.Fatal(err)
		}
		f.bind()
		return
	}
	f.close()
	f.open()
}

func marshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error"`
}

func (f *fixture) call(method string, params map[string]any, write bool) response {
	f.t.Helper()
	before := marshal(f.t, params)
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	f.serial++
	if write {
		key := sha256.Sum256([]byte(fmt.Sprintf("b15/%s/%d", f.engine, f.serial)))
		copyParams["_meta"] = map[string]any{"project": project, "agent": "b15-public", "idempotencyKey": hex.EncodeToString(key[:])}
	}
	request := marshal(f.t, map[string]any{"jsonrpc": "2.0", "id": f.serial, "method": method, "params": copyParams})
	raw := f.server.Dispatch(rpc.WithPrincipal(context.Background(), actor), request)
	f.frames = append(f.frames, marshal(f.t, map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(raw)}))
	if !bytes.Equal(before, marshal(f.t, params)) {
		f.t.Fatal("public request map was mutated")
	}
	var got response
	if err := json.Unmarshal(raw, &got); err != nil {
		f.t.Fatalf("%s invalid response %s: %v", method, raw, err)
	}
	return got
}

func (f *fixture) success(method string, params map[string]any, write bool) json.RawMessage {
	f.t.Helper()
	r := f.call(method, params, write)
	if r.Error != nil {
		f.t.Fatalf("%s refused %s: %s", method, marshal(f.t, params), marshal(f.t, r))
	}
	return r.Result
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func (f *fixture) create() {
	f.success("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "comment anchor", "status": "todo", "priority": 1}}, true)
}

func (f *fixture) document() pipeline.Document {
	f.t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(context.Background(), []model.Ref{ref("b15-anchor")})
	if err != nil || len(rows) != 1 {
		f.t.Fatalf("anchor readback: %v count=%d", err, len(rows))
	}
	var doc pipeline.Document
	if err := json.Unmarshal(rows[0].Body, &doc); err != nil {
		f.t.Fatal(err)
	}
	return doc
}

func (f *fixture) changes() []model.Changeset {
	f.t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	entries, err := r.Changesets(context.Background(), nil, 1000)
	if err != nil || len(entries) == 1000 {
		f.t.Fatalf("history read: %v count=%d", err, len(entries))
	}
	return entries
}

type pageItem struct {
	ID      uint64           `json:"id"`
	Comment pipeline.Comment `json:"comment"`
}

type page struct {
	Comments []pageItem `json:"comments"`
	Next     string     `json:"nextCursor"`
}

func (f *fixture) page(id string, limit int, cursor string) page {
	selection := map[string]any{}
	if limit != 0 {
		selection["limit"] = limit
	}
	if cursor != "" {
		selection["cursor"] = cursor
	}
	raw := f.success("bead.get", map[string]any{"id": ref(id), "commentPage": selection}, false)
	var got struct {
		Page page `json:"commentPage"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		f.t.Fatal(err)
	}
	if got.Page.Comments == nil {
		f.t.Fatalf("public page missing: %s", raw)
	}
	return got.Page
}

func TestB15PublicDurableClockHistoryAndRequestIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.create()
			before := f.document()
			bodies := []string{" \t**literal**\n東京 e\u0301  ", "second"}
			for _, body := range bodies {
				f.success("bead.comment", map[string]any{"id": ref(before.ID), "body": body}, true)
			}
			got := f.document()
			want := []pipeline.Comment{{Body: bodies[0], Principal: actor, Time: f.clock.Now()}, {Body: bodies[1], Principal: actor, Time: f.clock.Now().Add(time.Second)}}
			if !reflect.DeepEqual(got.Comments, want) {
				t.Errorf("same-clock exact comments: got=%+v want=%+v", got.Comments, want)
			}
			got.Comments = nil
			if !reflect.DeepEqual(got, before) {
				t.Error("comment changed the anchor's non-comment members")
			}
			entries := f.changes()
			if len(entries) != 3 {
				t.Fatalf("expected create and two comment history entries, got %d", len(entries))
			}
			for _, entry := range entries[1:] {
				if len(entry.Changes) != 1 || entry.Changes[0].Op != model.OpComment || entry.Changes[0].Ref.ID != before.ID || !reflect.DeepEqual(entry.Changes[0].ChangedPaths, []string{"/comments"}) || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadCommented {
					t.Errorf("not one comment history/event: %+v", entry)
				}
			}
			// An explicitly authored future thread is a clock-boundary fixture,
			// never a native import or reference result.
			future := f.document()
			future.Comments[1].Time = f.clock.Now().Add(time.Hour)
			_, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref(before.ID), Kind: "bead", Op: model.OpPatch, Body: marshal(t, future)}})
			if err != nil {
				t.Fatal(err)
			}
			request := service.CommentRequest{ID: ref(before.ID), Body: "after future"}
			requestBefore := marshal(t, request)
			_, err = f.domain.CommentWithOutcome(context.Background(), actor, request, model.WriteMeta{Project: project, Agent: "b15-public", IdempotencyKey: strings.Repeat("f", 32)})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(requestBefore, marshal(t, request)) {
				t.Error("public typed comment request mutated")
			}
			want = append(future.Comments, pipeline.Comment{Body: request.Body, Principal: actor, Time: f.clock.Now().Add(time.Hour + time.Second)})
			if !reflect.DeepEqual(f.document().Comments, want) {
				t.Error("future newest timestamp did not advance exactly one second")
			}
			stable := f.document()
			history := f.changes()
			for _, tc := range []struct{ id, body, code string }{{before.ID, "", "invalid_params"}, {before.ID, " \t\n", "invalid_params"}, {"", "body", "invalid_params"}, {"b15-unknown", "body", "not_found"}, {"b15-anch", "body", "not_found"}} {
				r := f.call("bead.comment", map[string]any{"id": map[string]any{"id": tc.id}, "body": tc.body}, true)
				if r.Error == nil || r.Error.Data.Code != tc.code {
					t.Errorf("refusal %+v: %s", tc, marshal(t, r))
				}
				if !reflect.DeepEqual(stable, f.document()) || !reflect.DeepEqual(history, f.changes()) {
					t.Error("refused comment changed committed row/history")
				}
			}
			f.reopen()
			if !reflect.DeepEqual(stable, f.document()) || !reflect.DeepEqual(history, f.changes()) {
				t.Error("SQL reopen changed exact comment document/history")
			}
			if got := f.page(before.ID, 500, ""); len(got.Comments) != 3 || got.Comments[2].ID != 3 || !reflect.DeepEqual(got.Comments[2].Comment, want[2]) {
				t.Errorf("public reopened comment IDs/tuple: %+v", got)
			}
		})
	}
}

func TestB15PublicDefaultAndMaximumPagesOrderTiesWithoutMutation(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.create()
			if empty := f.page("b15-anchor", 0, ""); len(empty.Comments) != 0 || empty.Next != "" {
				t.Fatal("new thread is not an empty page")
			}
			doc := f.document()
			want := make([]pageItem, 502)
			for i := range want {
				// Reverse groups of three: every tie crosses an independent ID
				// order, and the 100/500 boundaries cut through tie groups.
				comment := pipeline.Comment{Body: fmt.Sprintf(" exact-%03d \n", i), Principal: actor, Time: f.clock.Now().Add(time.Duration((501-i)/3) * time.Second)}
				doc.Comments = append(doc.Comments, comment)
				want[i] = pageItem{ID: uint64(i + 1), Comment: comment}
			}
			sort.Slice(want, func(i, j int) bool {
				if want[i].Comment.Time.Equal(want[j].Comment.Time) {
					return want[i].ID < want[j].ID
				}
				return want[i].Comment.Time.Before(want[j].Comment.Time)
			})
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref(doc.ID), Kind: "bead", Op: model.OpPatch, Body: marshal(t, doc)}}); err != nil {
				t.Fatal(err)
			}
			state, history := f.document(), f.changes()
			first := f.page(doc.ID, 0, "")
			if len(first.Comments) != 100 || first.Next == "" || !reflect.DeepEqual(first.Comments, want[:100]) {
				t.Fatal("default 100 page lost exact tuple order")
			}
			max := f.page(doc.ID, 500, "")
			if len(max.Comments) != 500 || max.Next == "" || !reflect.DeepEqual(max.Comments, want[:500]) {
				t.Fatal("maximum 500 page lost exact tuple order")
			}
			f.reopen()
			var walk []pageItem
			walk = append(walk, first.Comments...)
			cursor := first.Next
			for cursor != "" {
				p := f.page(doc.ID, 100, cursor)
				if p.Next == cursor || len(p.Comments) == 0 {
					t.Fatal("keyset walk stalled")
				}
				walk = append(walk, p.Comments...)
				cursor = p.Next
			}
			if !reflect.DeepEqual(walk, want) {
				t.Error("reopened keyset walk lost, duplicated or reordered tuples")
			}
			tail := f.page(doc.ID, 500, max.Next)
			if !reflect.DeepEqual(tail.Comments, want[500:]) || tail.Next != "" {
				t.Error("500-page continuation lost exact tail")
			}
			r := f.call("bead.get", map[string]any{"id": ref(doc.ID), "commentPage": map[string]any{"limit": 501}}, false)
			if r.Error == nil || r.Error.Data.Code != "invalid_params" {
				t.Error("501 page limit did not refuse")
			}
			if !reflect.DeepEqual(state, f.document()) || !reflect.DeepEqual(history, f.changes()) {
				t.Error("paging mutated committed thread/history")
			}
		})
	}
}

func TestB15PublicWispCommentClockAndNoDurableHistory(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.create()
			anchor, durable := f.document(), f.changes()
			raw := f.success("x.workflow.invoke", map[string]any{"template": "b15-public", "project": project, "mode": "ephemeral", "checklist": []map[string]any{{"label": "step", "title": "comment thread", "type": "task"}}}, true)
			var invoked workflow.InvokeResult
			if err := json.Unmarshal(raw, &invoked); err != nil || len(invoked.Items) != 1 {
				t.Fatalf("wisp invocation: %v %s", err, raw)
			}
			id := invoked.Items[0].ID
			want := []pipeline.Comment{}
			for i, body := range []string{" \twisp text\n東京  ", "same clock"} {
				f.success("bead.comment", map[string]any{"id": ref(id), "body": body}, true)
				want = append(want, pipeline.Comment{Body: body, Principal: actor, Time: f.clock.Now().Add(time.Duration(i) * time.Second)})
			}
			read := func() []pipeline.Comment {
				raw := f.success("bead.get", map[string]any{"id": ref(id)}, false)
				var thread struct {
					Comments []pipeline.Comment `json:"comments"`
				}
				if err := json.Unmarshal(raw, &thread); err != nil {
					t.Fatal(err)
				}
				return thread.Comments
			}
			got := read()
			if !reflect.DeepEqual(got, want) {
				t.Errorf("wisp exact tuple/monotonic time: got=%+v want=%+v", got, want)
			}
			if !reflect.DeepEqual(anchor, f.document()) || !reflect.DeepEqual(durable, f.changes()) {
				t.Error("wisp comment changed durable anchor/history")
			}
			f.reopen()
			if reopened := read(); !reflect.DeepEqual(got, reopened) {
				t.Error("SQL reopen changed wisp comment tuples")
			}
			if !reflect.DeepEqual(anchor, f.document()) || !reflect.DeepEqual(durable, f.changes()) {
				t.Error("reopened wisp introduced durable history")
			}
			paged := f.page(id, 500, "")
			for i := range want {
				if len(paged.Comments) != 2 || paged.Comments[i].ID != uint64(i+1) || !reflect.DeepEqual(paged.Comments[i].Comment, got[i]) {
					t.Errorf("reopened public wisp page IDs/tuples: %+v", paged)
					break
				}
			}
		})
	}
}
