package s1b05b08

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This consumer calls the existing production import adapter. Its operation-
// bound test presence verifier supplies no protected source, user, grant or
// hardware-key evidence. Anonymous/no-policy execution cannot earn AZ9 credit.
type b07NativeSQLHost struct {
	t            *testing.T
	ctx          context.Context
	runtime      *composition.Runtime
	server       *httpjson.Server
	done         chan error
	config       composition.Config
	url          string
	output       string
	count        int
	observations int
	scopes       map[string][2]string
}

type b07NativeReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    model.ErrorData `json:"data"`
	} `json:"error"`
	Meta map[string]any `json:"_meta"`
}

func b07NativeImportAndExercise(t *testing.T, engine, root, twin string, capture b07NativeCapture) {
	output := filepath.Join(root, twin, "consumer-"+engine)
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	h := &b07NativeSQLHost{t: t, ctx: b07NativeContext(t), output: output, scopes: map[string][2]string{}}
	h.config = composition.Config{Driver: engine, Project: "b07", AuthMode: "anonymous"}
	if engine == "sqlite" {
		h.config.DSN = filepath.Join(t.TempDir(), "b07-native.db")
	} else {
		_, h.config.DSN = postgresFixture(t)
	}
	h.config.RestoreImportPlanner = converterroundtrip.InspectImport
	h.config.RestorePresence = restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if method != "admin.restore.begin" || len(digest) != 64 || p["proof"] != b07NativeSHA(capture.Archive) {
			return fmt.Errorf("B07 test presence is not bound to its actual native archive")
		}
		b07NativeJSON(t, filepath.Join(output, "test-presence-"+digest+".json"), map[string]any{
			"method": method, "operationDigest": digest, "archiveSHA256": b07NativeSHA(capture.Archive),
			"scope": "host-private consumer test verifier; no protected/hardware/native-authority credit",
		})
		return nil
	})
	h.open()
	t.Cleanup(h.close)
	if !h.snapshot().Meta.Head.IsZero() {
		t.Fatal("native import target is not empty")
	}
	inspection, err := converterroundtrip.InspectImport(h.ctx, bytes.NewReader(capture.Archive))
	if err != nil {
		t.Fatalf("intact native archive inspection refused: %v", err)
	}
	report, err := h.runtime.RestoreImport(h.ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, bytes.NewReader(capture.Archive), composition.RestoreImportRequest{
		IdempotencyKey: b07NativeSHA([]byte(engine + "/" + twin + "/import"))[:32], AgentLabel: "b07-native",
		HistoryMode: driver.RestoreHistoryModeSnapshot,
		Presence:    restore.Presence{"proof": b07NativeSHA(capture.Archive)},
	})
	if err != nil {
		t.Fatalf("existing production adapter refused intact native archive: %v", err)
	}
	b07NativeJSON(t, filepath.Join(output, "native-import.json"), map[string]any{
		"inspection": inspection, "restored": report.Restored, "requestHash": report.RequestHash,
		"idempotencyKey": report.IdempotencyKey, "archiveSHA256": b07NativeSHA(capture.Archive),
		"authorizationInstalled": false, "history": "snapshot", "AZ9": "NOT_PROVED",
	})
	h.reopen()
	h.assertImported(capture)
	edge := capture.Edges[0]
	from, to := model.RefByID(edge.From, model.Natural64(0)), model.RefByID(edge.To, model.Natural64(0))
	if edge.Kind != "blocks" {
		t.Fatal("B07 mutation control must be a genuine native blocks edge")
	}
	for _, id := range capture.Ready {
		if id == edge.From {
			t.Fatal("the genuine native blocks control is unexpectedly ready")
		}
	}
	unblocked := append(append([]string(nil), capture.Ready...), edge.From)
	sort.Strings(unblocked)
	sub := h.subscribe(edge.From)
	defer sub.Close()
	params := map[string]any{"from": from, "to": to, "kind": "blocks"}
	first := h.call("bead.unlink", "native-unlink", params)
	commit := h.commit(first)
	h.delivered(sub, commit.Revision, model.EventBeadUnlinked)
	h.assertReady(unblocked)
	after := h.snapshot()
	replay := h.call("bead.unlink", "native-unlink", params)
	if replay.Meta["replayed"] != true || !bytes.Equal(first.Result, replay.Result) {
		t.Fatal("actual HTTP unlink did not replay its exact native-imported outcome")
	}
	h.same(after)
	h.quiet(sub)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	h.reopen()
	h.same(after)
	h.assertReady(unblocked)
	sub = h.subscribe(edge.From)
	defer sub.Close()
	replay = h.call("bead.unlink", "native-unlink", params)
	if replay.Meta["replayed"] != true || !bytes.Equal(first.Result, replay.Result) {
		t.Fatal("physical SQL reopen lost exact unlink replay")
	}
	h.same(after)
	h.quiet(sub)
	relinked := h.call("bead.link", "native-relink", params)
	commit = h.commit(relinked)
	h.delivered(sub, commit.Revision, model.EventBeadLinked)
	h.assertReady(capture.Ready)
	beforeRefusal := h.snapshot()
	batch := func(kind string) map[string]any {
		return map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: from, Fields: map[string]any{"title": "must roll back with retype"}}},
			{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: kind}},
		}}}
	}
	refused := h.call("tx.apply", "native-late-retype", batch("tracks"))
	if refused.Error == nil || refused.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("late native-edge retype refusal=%+v", refused)
	}
	h.same(beforeRefusal)
	h.quiet(sub)
	h.assertReady(capture.Ready)
	corrected := h.call("tx.apply", "native-late-retype", batch("blocks"))
	h.commit(corrected)
	if corrected.Meta["replayed"] == true {
		t.Fatal("refused late batch consumed the correction key")
	}
	afterCorrection := h.snapshot()
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	h.reopen()
	h.same(afterCorrection)
	h.assertReady(capture.Ready)
	sub = h.subscribe(edge.From)
	defer sub.Close()
	correctedReplay := h.call("tx.apply", "native-late-retype", batch("blocks"))
	if correctedReplay.Meta["replayed"] != true || !bytes.Equal(corrected.Result, correctedReplay.Result) {
		t.Fatal("corrected imported-edge batch receipt did not survive physical SQL reopen")
	}
	h.same(afterCorrection)
	h.quiet(sub)
	b07NativeJSON(t, filepath.Join(output, "consumer-terminal.json"), map[string]any{
		"engine": engine, "archiveSHA256": b07NativeSHA(capture.Archive), "rows": len(afterCorrection.Rows),
		"head": afterCorrection.Meta.Head, "scope": "actual anonymous no-policy native import/HTTP/SQL/reopen/watch; no installed-positive AZ9",
	})
}

func (h *b07NativeSQLHost) open() {
	h.t.Helper()
	r, err := composition.Open(h.ctx, h.config)
	if err != nil {
		h.t.Fatal(err)
	}
	h.runtime = r
	s, err := r.Listen(h.ctx, "127.0.0.1:0", nil)
	if err != nil {
		_ = r.Close(context.Background())
		h.t.Fatal(err)
	}
	h.server, h.url = s, "http://"+s.Addr().String()+httpjson.Path
	h.done = make(chan error, 1)
	go func() { h.done <- s.Serve() }()
	b07NativeJSON(h.t, filepath.Join(h.output, fmt.Sprintf("runtime-open-%03d.json", h.count)), map[string]any{
		"engine": h.config.Driver, "listener": s.Addr().String(), "openedUTC": time.Now().UTC(),
		"sqlLocatorSHA256": b07NativeSHA([]byte(h.config.DSN)), "authorizationInstalled": h.config.Auth.AuthorizationInstalled != nil && h.config.Auth.AuthorizationInstalled(),
	})
}

func (h *b07NativeSQLHost) close() {
	if h.runtime == nil {
		return
	}
	if err := h.server.Close(); err != nil {
		h.t.Error(err)
	}
	select {
	case err := <-h.done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.t.Error(err)
		}
	case <-time.After(5 * time.Second):
		h.t.Error("owned HTTP listener did not stop")
	}
	if err := h.runtime.Close(context.Background()); err != nil {
		h.t.Error(err)
	}
	h.runtime = nil
}

func (h *b07NativeSQLHost) reopen() { h.close(); h.open() }

func (h *b07NativeSQLHost) call(method, label string, params map[string]any) b07NativeReply {
	h.t.Helper()
	h.count++
	key := b07NativeSHA([]byte(label))[:32]
	h.scopes[method+"/"+key] = [2]string{method, key}
	copyParams := map[string]any{}
	for k, v := range params {
		copyParams[k] = v
	}
	copyParams["_meta"] = map[string]any{"project": "b07", "agent": "b07-native", "idempotencyKey": key}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": h.count, "method": method, "params": copyParams})
	if err != nil {
		h.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost, h.url, bytes.NewReader(request))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	name := fmt.Sprintf("http-%03d-%s", h.count, label)
	b07NativeWrite(h.t, filepath.Join(h.output, name+".request.json"), request)
	b07NativeWrite(h.t, filepath.Join(h.output, name+".response.json"), data)
	if err != nil || closeErr != nil || response.StatusCode != http.StatusOK {
		h.t.Fatalf("actual HTTP %s status=%d read=%v close=%v", method, response.StatusCode, err, closeErr)
	}
	var reply b07NativeReply
	b07NativeDecode(h.t, data, &reply)
	return reply
}

func (h *b07NativeSQLHost) commit(reply b07NativeReply) model.Commit {
	h.t.Helper()
	if reply.Error != nil || len(reply.Result) == 0 {
		h.t.Fatalf("public native-imported mutation refused: %+v", reply)
	}
	var commit model.Commit
	b07NativeDecode(h.t, reply.Result, &commit)
	return commit
}

func (h *b07NativeSQLHost) assertImported(capture b07NativeCapture) {
	h.t.Helper()
	state := h.snapshot()
	docs := map[string]pipeline.Document{}
	for _, row := range state.Rows {
		if row.Kind != "bead" {
			continue
		}
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			h.t.Fatal(err)
		}
		docs[row.Ref.ID] = doc
	}
	for _, edge := range capture.Edges {
		doc, ok := docs[edge.From]
		if !ok {
			h.t.Fatalf("genuine source %s missing after import", edge.From)
		}
		if edge.Kind == "parent-child" {
			if doc.Parent == nil || doc.Parent.ID != edge.To {
				h.t.Fatalf("native parent edge lost: %+v", doc)
			}
			continue
		}
		kind := edge.Kind
		if kind == "related" {
			kind = "relates-to"
		}
		if len(doc.Links) != 1 || doc.Links[0].To.ID != edge.To || doc.Links[0].Kind != kind || (doc.Links[0].TargetKind == model.LinkTargetExternal) != strings.HasPrefix(edge.To, "external:") {
			h.t.Fatalf("native typed edge not retained: edge=%+v doc=%+v", edge, doc)
		}
	}
	h.assertReady(capture.Ready)
}

func (h *b07NativeSQLHost) assertReady(want []string) {
	h.t.Helper()
	reply := h.call("bead.ready", "import-ready", map[string]any{"limit": 1000})
	if reply.Error != nil {
		h.t.Fatalf("imported public ready refused: %+v", reply)
	}
	var page struct {
		Items      []struct{ ID string }
		NextCursor string
	}
	b07NativeDecode(h.t, reply.Result, &page)
	ids := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	if page.NextCursor != "" || !reflect.DeepEqual(ids, want) {
		h.t.Fatalf("complete native/Plug ready membership differs: expected=%v Plug=%v next=%q", want, ids, page.NextCursor)
	}
}

func (h *b07NativeSQLHost) subscribe(id string) *watch.Subscription {
	h.t.Helper()
	sub, err := h.runtime.Watches.Open(h.ctx, watch.Request{SessionID: "b07-native-owned-session", Method: watch.MethodEventSubscribe,
		Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Agent: "b07-native", Project: "b07",
		Filter: coreevents.Filter{Resources: []string{"bead/" + id}}, Ontology: model.Natural64(1)})
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	frame, err := sub.Next(ctx)
	if err != nil || frame.Kind != watch.FrameSnapshot {
		h.t.Fatalf("real watch initial snapshot failed: frame=%+v error=%v", frame, err)
	}
	b07NativeJSON(h.t, filepath.Join(h.output, fmt.Sprintf("watch-initial-%03d.json", h.count)), frame)
	return sub
}

func (h *b07NativeSQLHost) delivered(sub *watch.Subscription, revision model.Revision, kind model.DurableEventType) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	frame, err := sub.Next(ctx)
	if err != nil || frame.Kind != watch.FrameDelta || len(frame.Events) != 1 || frame.Events[0].Durable == nil || frame.Events[0].Durable.Type != kind || frame.Events[0].Durable.Revision.String() != revision.String() {
		h.t.Fatalf("real subscriber missed exact public commit: frame=%+v error=%v revision=%s kind=%s", frame, err, revision, kind)
	}
	b07NativeJSON(h.t, filepath.Join(h.output, fmt.Sprintf("watch-delivery-%03d.json", h.count)), frame)
}

func (h *b07NativeSQLHost) quiet(sub *watch.Subscription) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 2*watch.CoalesceWindow)
	defer cancel()
	frame, err := sub.Next(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		h.t.Fatalf("refusal/replay delivered another frame: %+v error=%v", frame, err)
	}
}

type b07NativeConservation struct {
	Meta     driver.Meta
	Rows     []driver.Record
	Versions map[string][]driver.Version
	Changes  []model.Changeset
	Derived  map[driver.DerivedTable][]driver.DerivedRow
	Receipts map[string]driver.IdempotencyRecord
}

func (h *b07NativeSQLHost) snapshot() b07NativeConservation {
	h.t.Helper()
	meta, err := h.runtime.Store.Meta(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	r, err := h.runtime.Store.Read(h.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		h.t.Fatal(err)
	}
	defer r.Close()
	if view := r.Snapshot(); view.At == nil || view.At.String() != meta.Head.String() || view.Incarnation != meta.Incarnation {
		h.t.Fatalf("native consumer observation is not pinned: %+v", view)
	}
	s := b07NativeConservation{Meta: meta, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]driver.IdempotencyRecord{}}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(h.ctx, driver.Query{Limit: 128, Cursor: cursor})
		if err != nil {
			h.t.Fatal(err)
		}
		s.Rows = append(s.Rows, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
			h.t.Fatal("complete imported-row census did not advance")
		}
		cursor = page.NextCursor
	}
	sort.Slice(s.Rows, func(i, j int) bool { return s.Rows[i].Ref.ID < s.Rows[j].Ref.ID })
	for _, row := range s.Rows {
		cursor = nil
		for {
			page, err := r.Versions(h.ctx, row.Ref, driver.PageReq{Limit: 128, Cursor: cursor})
			if err != nil {
				h.t.Fatal(err)
			}
			s.Versions[row.Ref.ID] = append(s.Versions[row.Ref.ID], page.Items...)
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
				h.t.Fatal("complete imported-version census did not advance")
			}
			cursor = page.NextCursor
		}
	}
	cursor = nil
	for {
		changes, err := r.Changesets(h.ctx, cursor, 128)
		if err != nil {
			h.t.Fatal(err)
		}
		s.Changes = append(s.Changes, changes...)
		if len(changes) < 128 {
			break
		}
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: changes[len(changes)-1].Revision, Index: model.Natural64(0)}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(h.ctx, table, "")
		if err != nil {
			h.t.Fatal(err)
		}
		s.Derived[table] = rows
	}
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		h.t.Fatal(err)
	}
	for name, scope := range h.scopes {
		receipt, err := r.IdempotencyByKey(h.ctx, principal, "b07-native", scope[0], scope[1])
		if err != nil {
			h.t.Fatal(err)
		}
		if receipt != nil {
			s.Receipts[name] = *receipt
		}
	}
	h.observations++
	b07NativeJSON(h.t, filepath.Join(h.output, fmt.Sprintf("conservation-%03d.json", h.observations)), s)
	return s
}

func (h *b07NativeSQLHost) same(want b07NativeConservation) {
	h.t.Helper()
	got := h.snapshot()
	if !reflect.DeepEqual(want, got) {
		h.t.Fatalf("native-imported SQL conservation failed: head=%s/%s rows=%t versions=%t changes=%t derived=%t receipts=%t", want.Meta.Head, got.Meta.Head,
			reflect.DeepEqual(want.Rows, got.Rows), reflect.DeepEqual(want.Versions, got.Versions), reflect.DeepEqual(want.Changes, got.Changes), reflect.DeepEqual(want.Derived, got.Derived), reflect.DeepEqual(want.Receipts, got.Receipts))
	}
}
