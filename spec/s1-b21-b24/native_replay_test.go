package s1b21b24

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Snapshot import deliberately has a nonzero history floor. Keep the ordinary
// reviewed fixture untouched and observe every native-imported plane directly,
// using the advertised floor as the changeset cursor rather than inventing a
// revision-zero history window.
func nativeImportedState(t *testing.T, f *fixture) storedState {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	at := meta.Head
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	state := storedState{Meta: meta, Records: []driver.Record{}, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]*driver.IdempotencyRecord{}}
	var recordCursor *driver.Cursor
	seenRecordCursors := map[string]bool{}
	for {
		page, err := r.Query(f.ctx, driver.Query{Limit: 10000, Cursor: recordCursor})
		if err != nil || page.AsOf != at {
			t.Fatalf("native imported record census page: %+v %v", page, err)
		}
		state.Records = append(state.Records, page.Items...)
		if page.NextCursor == nil {
			break
		}
		encoded, err := json.Marshal(page.NextCursor)
		if err != nil || len(page.Items) == 0 || seenRecordCursors[string(encoded)] {
			t.Fatal("native imported record census did not advance", err)
		}
		seenRecordCursors[string(encoded)] = true
		recordCursor = page.NextCursor
	}

	for i := range state.Records {
		state.Records[i].Body = append([]byte(nil), state.Records[i].Body...)
		var cursor *driver.Cursor
		for {
			versions, err := r.Versions(f.ctx, state.Records[i].Ref, driver.PageReq{Cursor: cursor, Limit: 10000})
			if err != nil {
				t.Fatalf("native imported versions %s: %v", state.Records[i].Ref.ID, err)
			}
			state.Versions[state.Records[i].Ref.ID] = append(state.Versions[state.Records[i].Ref.ID], versions.Items...)
			if versions.NextCursor == nil {
				break
			}
			if len(versions.Items) == 0 {
				t.Fatal("native imported version census did not advance")
			}
			cursor = versions.NextCursor
		}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatalf("native imported derived %s: %v", table, err)
		}
		state.Derived[table] = append([]driver.DerivedRow(nil), rows...)
	}
	cursor := &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Index: model.Natural64(0), Method: "changesets", OntologyRevision: model.Natural64(0)}
	for {
		rows, err := r.Changesets(f.ctx, cursor, 10000)
		if err != nil {
			t.Fatalf("native imported post-floor changeset census: %v", err)
		}
		state.Changesets = append(state.Changesets, rows...)
		if len(rows) < 10000 {
			break
		}
		next := rows[len(rows)-1].Revision
		if next == cursor.Revision {
			t.Fatal("native imported changeset census did not advance")
		}
		cursor.Revision = next
	}
	f.mu.Lock()
	requests := append([]frame(nil), f.frames...)
	f.mu.Unlock()
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	for _, captured := range requests {
		var request struct {
			Method string `json:"method"`
			Params struct {
				Meta struct {
					Agent          string `json:"agent"`
					IdempotencyKey string `json:"idempotencyKey"`
				} `json:"_meta"`
			} `json:"params"`
		}
		if err := json.Unmarshal(captured.Request, &request); err != nil || request.Method == "" || request.Params.Meta.IdempotencyKey == "" {
			continue
		}
		receipt, err := r.IdempotencyByKey(f.ctx, principal, request.Params.Meta.Agent, request.Method, request.Params.Meta.IdempotencyKey)
		if err != nil {
			t.Fatalf("native imported receipt %s/%s: %v", request.Method, request.Params.Meta.IdempotencyKey, err)
		}
		if receipt != nil {
			copy := *receipt
			state.Receipts[request.Method+"/"+request.Params.Meta.IdempotencyKey] = &copy
		}
	}
	return state
}

// No-op receipts deliberately use a zero Changeset as an absence marker.
// Keep the original typed census for every comparison; only the evidence wire
// form uses an explicit, independently verified null for that absent changeset.
// All four driver receipt fields remain present, including the complete Commit.
type nativeReceiptRecording struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       *model.Changeset
	ResponsePending bool
	ChangesetAbsent bool
}

type nativeStateRecording struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   map[string][]driver.Version
	Derived    map[driver.DerivedTable][]driver.DerivedRow
	Changesets []model.Changeset
	Receipts   map[string]nativeReceiptRecording
}

func nativeRecordState(t *testing.T, state storedState) nativeStateRecording {
	t.Helper()
	out := nativeStateRecording{Meta: state.Meta, Records: state.Records, Versions: state.Versions, Derived: state.Derived, Changesets: state.Changesets, Receipts: map[string]nativeReceiptRecording{}}
	for key, receipt := range state.Receipts {
		if receipt == nil {
			t.Fatalf("native receipt %s is unexpectedly nil", key)
		}
		value := nativeReceiptRecording{RequestHash: receipt.RequestHash, Commit: receipt.Commit, ResponsePending: receipt.ResponsePending}
		if receipt.Changeset.Kind == "" {
			if !reflect.DeepEqual(receipt.Changeset, model.Changeset{}) {
				t.Fatalf("native receipt %s has a nonzero changeset without a kind", key)
			}
			value.ChangesetAbsent = true
		} else {
			entry := receipt.Changeset
			value.Changeset = &entry
		}
		out.Receipts[key] = value
	}
	return out
}

func nativeAssertSnapshotHistoryBoundary(t *testing.T, root string, f *fixture) {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Head != model.Natural64(1) || meta.HistoryFloor != model.Natural64(1) {
		t.Fatalf("native import did not establish the explicit snapshot floor: %+v", meta)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := r.Changesets(f.ctx, nil, 10000)
	closeErr := r.Close()
	var truncated driver.ErrTruncated
	if !errors.As(readErr, &truncated) || truncated.Floor != meta.HistoryFloor || closeErr != nil {
		t.Fatalf("native below-floor history did not refuse with its exact floor: %v / %v", readErr, closeErr)
	}
	state := nativeImportedState(t, f)
	if len(state.Changesets) != 0 {
		t.Fatal("native snapshot import fabricated source changesets")
	}
	nativeWriteJSON(t, filepath.Join(root, f.engine+"-history-boundary.json"), map[string]any{"meta": meta, "nil_cursor_refusal": map[string]any{"error": readErr.Error(), "floor": truncated.Floor}, "post_floor_changesets": state.Changesets, "source_history_claim": false})
}

type nativePostObservation struct {
	Change   port.CommittedChange `json:"change"`
	Snapshot driver.View          `json:"snapshot"`
	Rows     []driver.Record      `json:"rows"`
}
type nativePostObserver struct {
	mu           sync.Mutex
	observations []nativePostObservation
	fail         atomic.Bool
	leaseRoot    string
	leaseSerial  int
	hostLease    *nativeHostLease
}

// This test host owns the same maintenance responsibility as Runtime.renew.
// Keep the concrete lease passed to Writer unchanged: PostgreSQL fences it by
// concrete identity as well as epoch. No request retries or longer TTLs apply.
type nativeHostLease struct {
	lease   driver.ActiveLease
	epoch   uint64
	cancel  context.CancelFunc
	done    chan struct{}
	path    string
	events  []map[string]any
	failure error
}

func (o *nativePostObserver) startHostLease(t *testing.T, f *fixture) {
	t.Helper()
	if o.hostLease != nil || f.lease == nil || o.leaseRoot == "" {
		t.Fatal("native host maintenance requires one actual bound lease")
	}
	ctx, cancel := context.WithCancel(f.ctx)
	o.leaseSerial++
	h := &nativeHostLease{lease: f.lease, epoch: f.lease.Epoch(), cancel: cancel, done: make(chan struct{}), path: filepath.Join(o.leaseRoot, fmt.Sprintf("%s-host-lease-%02d.json", f.engine, o.leaseSerial))}
	o.hostLease = h
	renewTimeout := driver.LeaseTTL - 2*driver.LeaseRenewEvery
	if renewTimeout <= 0 {
		renewTimeout = driver.LeaseRenewEvery
	}
	h.events = append(h.events, map[string]any{"event": "start", "at_utc": time.Now().UTC(), "engine": f.engine, "generation": o.leaseSerial, "epoch": h.epoch, "ttl": driver.LeaseTTL.String(), "renew_every": driver.LeaseRenewEvery.String(), "renew_timeout": renewTimeout.String()})
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				h.events = append(h.events, map[string]any{"event": "stop_requested", "at_utc": time.Now().UTC()})
				return
			case <-h.lease.Lost():
				h.failure = errors.New("native host lost its active storage lease")
				h.events = append(h.events, map[string]any{"event": "lost", "at_utc": time.Now().UTC(), "error": h.failure.Error()})
				return
			case <-ticker.C:
				started := time.Now().UTC()
				renewCtx, renewCancel := context.WithTimeout(ctx, renewTimeout)
				err := h.lease.Renew(renewCtx)
				renewCancel()
				event := map[string]any{"event": "renew", "started_utc": started, "ended_utc": time.Now().UTC(), "epoch": h.lease.Epoch()}
				if err != nil {
					event["error"] = err.Error()
					if ctx.Err() != nil && errors.Is(err, context.Canceled) {
						event["event"] = "renew_canceled_on_stop"
					} else {
						h.failure = fmt.Errorf("native host lease renewal: %w", err)
					}
				} else if h.lease.Epoch() != h.epoch {
					h.failure = errors.New("native host renewal changed the active epoch")
					event["error"] = h.failure.Error()
				}
				h.events = append(h.events, event)
				if err != nil || h.failure != nil {
					return
				}
			}
		}
	}()
}

func (o *nativePostObserver) stopHostLease(t *testing.T) {
	t.Helper()
	h := o.hostLease
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
	// Check before Release: planned teardown must never hide an already lost
	// lease. The goroutine is drained before inspecting or writing its events.
	select {
	case <-h.lease.Lost():
		if h.failure == nil {
			h.failure = errors.New("native host lease was lost before planned release")
		}
	default:
	}
	h.events = append(h.events, map[string]any{"event": "drained_before_release", "at_utc": time.Now().UTC(), "epoch": h.lease.Epoch(), "active": h.failure == nil})
	nativeWriteJSON(t, h.path, h.events)
	o.hostLease = nil
	if h.failure != nil {
		t.Fatal(h.failure)
	}
}

func (o *nativePostObserver) After(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
	snapshot := r.Snapshot()
	if snapshot.At == nil || *snapshot.At != c.Revision {
		return errors.New("native imported subscriber snapshot is not committed revision")
	}
	refs := []model.Ref{}
	for _, change := range c.Changes {
		refs = append(refs, change.Resource.Ref)
	}
	rows, err := r.Get(ctx, refs)
	if err != nil {
		return err
	}
	byID := map[string]driver.Record{}
	for _, row := range rows {
		byID[row.Ref.ID] = row
	}
	last := map[string]port.Change{}
	for _, change := range c.Changes {
		last[change.Resource.Ref.ID] = change
	}
	for id, change := range last {
		if !bytes.Equal(byID[id].Body, change.After) {
			return errors.New("native imported subscriber did not read exact committed after image")
		}
	}
	o.mu.Lock()
	o.observations = append(o.observations, nativePostObservation{c, snapshot, rows})
	o.mu.Unlock()
	if o.fail.Load() {
		return errors.New("deliberate native acceptance post failure after durable commit")
	}
	return nil
}
func (o *nativePostObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.observations)
}

// This trusted test host revokes current authority only for the two native-
// imported race targets. The production Writer must enforce it for both stale
// guard disclosure and stored replay, including after physical SQL reopen.
type nativeRaceAuthorizer struct{ denied *atomic.Bool }

func (a nativeRaceAuthorizer) Authorize(_ context.Context, _ port.AuthorizationStage, p port.ProposedChange, _ port.SnapshotReader) error {
	if !a.denied.Load() {
		return nil
	}
	for _, expect := range p.Expect {
		if expect.Ref.ID == nativeID("plugracecc") || expect.Ref.ID == nativeID("plugraceuc") {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
	}
	return nil
}

// Bind to the actually restored ontology. The ordinary fixture's default issue
// ontology cannot stand in for the actually stored imported project vocabulary.
func nativeBindImported(t *testing.T, f *fixture, observer *nativePostObserver, denied *atomic.Bool, trace ...*nativeReadTrace) {
	t.Helper()
	observer.stopHostLease(t)
	if f.lease != nil {
		if err := f.lease.Release(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.lease = nil
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, readErr := reader.DerivedRows(f.ctx, driver.DerivedOntology, "")
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(rows) != 1 || rows[0].Key != "builtin" {
		t.Fatalf("imported ontology: %v %v %v", rows, readErr, closeErr)
	}
	doc, err := ontology.Decode(rows[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	rules := service.NewOntology(snapshot.Effective)
	f.ontology = rules
	f.lease, err = f.store.Lease(f.ctx, "s1-native-imported-"+f.engine, driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	observer.startHostLease(t, f)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(rules), pipeline.WithClock(f.clock.Now), pipeline.WithIDGenerator(f.nextID), pipeline.WithAuthorizer(nativeRaceAuthorizer{denied}), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "native-imported-observer", Subscriber: observer}))
	readStore := f.store
	if len(trace) == 1 && trace[0] != nil {
		readStore = &nativeObservedStore{Store: f.store, trace: trace[0]}
	} else if len(trace) != 0 {
		t.Fatal("native imported read observation requires exactly one nonnil trace")
	}
	f.service = service.New(readStore, f.lease, f.writer, service.WithOntology(rules), service.WithClock(f.clock.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err = rpc.BindService(registry, f.service); err != nil {
		t.Fatal(err)
	}
	view, err := viewops.New(readStore, f.lease, viewops.Config{OntologyRevision: model.Natural64(1), StaleDefaultOntology: rules, Now: f.clock.Now, AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
		if denied.Load() && ref.Ref.ID == nativeID("betaaa") {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = viewrpc.BindWithConfig(registry, view, viewrpc.Config{CursorKey: fixedCursorKey}); err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: fixedCursorKey, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
}
func nativeReopenImported(t *testing.T, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	if f.engine == "memory" {
		return
	}
	o.stopHostLease(t)
	f.close(t)
	var err error
	if f.engine == "sqlite" {
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.path})
	} else {
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	}
	if err != nil {
		t.Fatal(err)
	}
	nativeBindImported(t, f, o, denied)
}
func nativeReplayAll(t *testing.T, root, archivePath string, expected []byte, config nativePublicConfigExpectation, protocol nativeRecordingFixture, historyOff nativeHistoryOffObservation) {
	t.Helper()
	data, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := converterroundtrip.InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("actual native archive inspection: %v", err)
	}
	archive, err := dumpformat.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	source := nativeAssertHistoryOffArchive(t, archive, historyOff)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "s1-native-recorder"}, Authority: "restore-admin", AdmissionDigest: nativeHashBytes([]byte("s1-b21-b24 disposable native acceptance"))}, SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "s1-native-" + *nativeRecordingID, ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot}
	nativeWriteJSON(t, filepath.Join(root, "import-profile.json"), profile)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		if !t.Run("native-replay-"+engine, func(t *testing.T) {
			f := newFixture(t, engine)
			if err := f.lease.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			f.lease = nil
			f.clock.Advance(t, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).Sub(f.clock.Now()))
			f.bind(t)
			f.ids = []string{nativeID("newbatch"), nativeID("capacity"), nativeID("capacity2")}
			report, err := converterroundtrip.RestoreImportWithLease(f.ctx, bytes.NewReader(data), f.store, f.lease, profile)
			if err != nil {
				t.Fatalf("native archive %s snapshot restore: %v", engine, err)
			}
			nativeWriteJSON(t, filepath.Join(root, engine+"-restore.json"), report.Restored)
			observer := &nativePostObserver{leaseRoot: root}
			t.Cleanup(func() { observer.stopHostLease(t) })
			var denied atomic.Bool
			nativeBindImported(t, f, observer, &denied)
			compare := func(label string) {
				live, err := converterroundtrip.ReverseRestored(f.ctx, f.store)
				if err != nil {
					t.Fatalf("native %s live reverse %s: %v", engine, label, err)
				}
				nativeWriteRecordingBytes(t, filepath.Join(root, engine+"-"+label+"-reverse.jsonl"), live.Result.JSONL)
				nativeWriteJSON(t, filepath.Join(root, engine+"-"+label+"-reverse-report.json"), live.Result.Report)
				projection, err := nativeProjectJSONL(live.Result.JSONL, protocol.ProjectionFields, protocol.StatusCategories)
				if err != nil {
					t.Fatal(err)
				}
				nativeWriteRecordingBytes(t, filepath.Join(root, engine+"-"+label+"-projection.json"), projection)
				if err = nativeComparePublicReverse(expected, projection, config, live.Result.Report.ConfigReferences, live.Result.Report.ConfigAsMemories); err != nil {
					t.Fatalf("native %s %s semantic/config/source readback: %v", engine, label, err)
				}
			}
			compare("import")
			nativeImportedHistoryOffParity(t, root, f, observer, &denied, "import", source, historyOff)
			nativeReopenImported(t, f, observer, &denied)
			compare("reopen")
			nativeImportedHistoryOffParity(t, root, f, observer, &denied, "reopen", source, historyOff)
			nativeAssertSnapshotHistoryBoundary(t, root, f)
			defer func() {
				observer.mu.Lock()
				defer observer.mu.Unlock()
				nativeWriteJSON(t, filepath.Join(root, engine+"-subscriber.json"), observer.observations)
				nativeWriteJSON(t, filepath.Join(root, engine+"-final-state.json"), nativeRecordState(t, nativeImportedState(t, f)))
			}()
			nativeImportedBatch(t, f, observer, &denied)
			if t.Failed() {
				return
			}
			nativeImportedCAS(t, f, observer, &denied)
			if t.Failed() {
				return
			}
			nativeImportedViews(t, f, observer, &denied)
			if t.Failed() {
				return
			}
			nativeImportedLifecycleRaces(t, root, f, observer, &denied)
			if t.Failed() {
				return
			}
			nativeImportedDefer(t, root, f, observer, &denied)
			if t.Failed() {
				return
			}
			nativeImportedHistoryOffLifecycle(t, root, f, observer, &denied, source, historyOff)
		}) {
			t.Fatalf("native %s replay failed; subsequent driver qualification stops", engine)
		}
	}
}
func nativePatch(ref model.Ref, fields map[string]any) any {
	return map[string]any{"kind": "patch", "target": ref, "fields": fields}
}
func nativeChanges(ops ...any) map[string]any {
	return map[string]any{"changeSet": map[string]any{"ops": ops}}
}
func nativeImportedRef(name string) model.Ref {
	return model.RefByID(nativeID(name), model.Natural64(0))
}
func nativeDocument(t *testing.T, f *fixture, ref model.Ref) pipeline.Document {
	t.Helper()
	for _, r := range nativeImportedState(t, f).Records {
		if r.Ref.ID == ref.ID {
			d, e := pipeline.DecodeDocument(r.Body)
			if e != nil {
				t.Fatal(e)
			}
			return d
		}
	}
	t.Fatalf("imported row missing: %s", ref.ID)
	return pipeline.Document{}
}
func nativeRefuse(t *testing.T, f *fixture, o *nativePostObserver, method, label string, params map[string]any, code string) {
	t.Helper()
	before := nativeImportedState(t, f)
	posts := o.count()
	got := f.call(method, label, params)
	if got.Error == nil || got.Error.Data.Code != code || got.Meta.Replayed || len(got.Result) != 0 {
		t.Fatalf("native imported %s refusal=%+v want=%s", label, got, code)
	}
	sameState(t, before, nativeImportedState(t, f))
	if posts != o.count() {
		t.Fatal("native imported refusal ran post subscriber")
	}
}
func nativeImportedBatch(t *testing.T, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	anchor := nativeImportedRef("anchor")
	before := nativeImportedState(t, f)
	posts := o.count()
	o.fail.Store(true)
	request := f.request("tx.apply", "native-b21-batch", nativeChanges(map[string]any{"kind": "create", "key": "new", "type": "issue", "fields": map[string]any{"title": "native imported batch"}}, nativePatch(model.RefByKey("new"), map[string]any{"description": "backward key"}), map[string]any{"kind": "close", "target": model.RefByKey("new"), "reason": "one typed atomic revision"}, nativePatch(anchor, map[string]any{"title": "native imported final"})))
	first := f.dispatch(request)
	c := commit(t, first)
	o.fail.Store(false)
	after := nativeImportedState(t, f)
	if len(c.Results) != 4 || len(c.Warnings) != 1 || c.Warnings[0].Code != "post_subscriber_failed" || len(after.Changesets) != len(before.Changesets)+1 || o.count() != posts+1 {
		t.Fatalf("imported batch result/history/posts: %+v", c)
	}
	for _, r := range c.Results {
		if r.Revision != c.Revision {
			t.Fatal("imported batch split revision")
		}
	}
	if nativeDocument(t, f, anchor).Fields["title"] != "native imported final" {
		t.Fatal("imported declaration order lost")
	}
	for _, phase := range []string{"same-process", "sql-reopen"} {
		if phase == "sql-reopen" {
			nativeReopenImported(t, f, o, denied)
		}
		replay := f.dispatch(request)
		if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) || o.count() != posts+1 {
			t.Fatalf("imported stored warning/commit replay %s: %+v", phase, replay)
		}
		sameState(t, after, nativeImportedState(t, f))
	}
	nativeRefuse(t, f, o, "tx.apply", "native-late-refusal", nativeChanges(nativePatch(anchor, map[string]any{"title": "rollback"}), nativePatch(nativeImportedRef("missing"), map[string]any{"title": "refuse"})), "not_found")
	nativeRefuse(t, f, o, "tx.apply", "native-forward-refusal", nativeChanges(nativePatch(model.RefByKey("later"), map[string]any{"title": "forward"}), map[string]any{"kind": "create", "key": "later", "type": "issue", "fields": map[string]any{"title": "later"}}), "invalid_params")
	nativeRefuse(t, f, o, "tx.apply", "native-unknown-close-field", nativeChanges(nativePatch(anchor, map[string]any{"title": "rollback"}), map[string]any{"kind": "close", "target": anchor, "override_guard": true}), "invalid_params")
	// The native partial BatchCloser is deliberately represented by an atomic
	// ChangeSet here: a missing middle target commits none of the survivors.
	nativeRefuse(t, f, o, "tx.apply", "native-atomic-close-deviation", nativeChanges(map[string]any{"kind": "close", "target": nativeImportedRef("alphaa"), "reason": "must rollback"}, map[string]any{"kind": "close", "target": nativeImportedRef("missing")}, map[string]any{"kind": "close", "target": nativeImportedRef("spawner")}), "not_found")
	ops := make([]any, 1000)
	for i := range ops {
		ops[i] = nativePatch(anchor, map[string]any{"title": "native imported final"})
	}
	noopBefore := nativeImportedState(t, f)
	noopPosts := o.count()
	noop := commit(t, f.call("tx.apply", "native-boundary1000", nativeChanges(ops...)))
	sameMutationState(t, noopBefore, nativeImportedState(t, f))
	if noop.Revision != noopBefore.Meta.Head || o.count() != noopPosts {
		t.Fatal("imported noop boundary minted mutation or post")
	}
	nativeRefuse(t, f, o, "tx.apply", "native-boundary1001", nativeChanges(append(ops, ops[0])...), "invalid_params")
}
func nativeImportedCAS(t *testing.T, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	target := nativeImportedRef("casrow")
	before := nativeImportedState(t, f)
	d := nativeDocument(t, f, target)
	var current model.Revision
	for _, r := range before.Records {
		if r.Ref.ID == target.ID {
			current = r.Revision
		}
	}
	fields := []string{"title", "description"}
	results := make([]reply, 2)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(2)
	done.Add(2)
	for i, field := range fields {
		go func(i int, field string) {
			defer done.Done()
			ready.Done()
			<-start
			results[i] = f.call("bead.update", "native-cas-"+field, map[string]any{"id": target, "fields": map[string]any{field: "plug-winner-" + field}, "expect": []model.Precondition{model.PreconditionByRevision(target, current)}})
		}(i, field)
	}
	ready.Wait()
	close(start)
	done.Wait()
	winner, loser := -1, -1
	for i, r := range results {
		if r.Error == nil {
			if winner != -1 {
				t.Fatal("two imported CAS winners")
			}
			winner = i
		} else {
			loser = i
			if r.Error.Data.Code != "precondition_failed" {
				t.Fatalf("imported CAS loser=%+v", r)
			}
		}
	}
	if winner < 0 || loser < 0 {
		t.Fatal("imported CAS lacks winner/loser")
	}
	after := nativeImportedState(t, f)
	winningCommit := commit(t, results[winner])
	losingReply := results[loser]
	observed, typed := losingReply.Error.Data.Details["currentRevision"].(json.Number)
	if !typed || observed.String() != winningCommit.Revision.String() || losingReply.Error.Data.Details["reason"] != "revision" || losingReply.Error.Data.Retryable || losingReply.Meta.Replayed || len(losingReply.Result) != 0 {
		t.Fatalf("native imported typed CAS loser does not name the actual winning revision: %+v / %+v", losingReply, winningCommit)
	}
	got := nativeDocument(t, f, target)
	if got.Fields[fields[winner]] != "plug-winner-"+fields[winner] || !nativeEqualJSON(got.Fields[fields[loser]], d.Fields[fields[loser]]) || len(after.Changesets) != len(before.Changesets)+1 {
		t.Fatal("imported CAS merged disjoint loser or split commit")
	}
	nativeRefuse(t, f, o, "bead.update", "native-stale-cas", map[string]any{"id": target, "fields": map[string]any{fields[loser]: "refused"}, "expect": []model.Precondition{model.PreconditionByRevision(target, current)}}, "precondition_failed")
	nativeReopenImported(t, f, o, denied)
	sameState(t, after, nativeImportedState(t, f))
}

// These targets were created by the genuine native producer and are imported
// open, independently of the rows consumed by its own lifecycle races.
func nativeImportedLifecycleRaces(t *testing.T, root string, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	for _, tc := range []struct {
		name, target string
		racers       int
		mixed        bool
	}{{"close-close", "plugracecc", 16, false}, {"update-close", "plugraceuc", 8, true}} {
		if !t.Run("B22-imported-lifecycle-"+tc.name, func(t *testing.T) {
			label := "native-b22-" + tc.name
			target := nativeImportedRef(tc.target)
			before := nativeImportedState(t, f)
			seed := nativeDocument(t, f, target)
			if seed.Fields["status"] != "todo" {
				t.Fatal("imported lifecycle race requires its actual open native row")
			}
			var token model.Revision
			for _, row := range before.Records {
				if row.Ref.ID == target.ID && row.Ref.Branch.IsZero() {
					token = row.Revision
				}
			}
			if token.String() != "1" {
				t.Fatal("imported race target is not the untouched snapshot row")
			}
			requests := make([][]byte, tc.racers)
			methods, labels := make([]string, tc.racers), make([]string, tc.racers)
			for i := range requests {
				methods[i] = "bead.close"
				labels[i] = fmt.Sprintf("%s-racer-%02d", label, i)
				params := map[string]any{"id": target, "reason": fmt.Sprintf("%s-close-%d", label, i), "expect": []model.Precondition{model.PreconditionByRevision(target, token)}}
				if tc.mixed && i%2 == 0 {
					methods[i] = "bead.update"
					params = map[string]any{"id": target, "fields": map[string]any{"description": fmt.Sprintf("%s-update-%d", label, i)}, "expect": []model.Precondition{model.PreconditionByRevision(target, token)}}
				}
				requests[i] = f.request(methods[i], labels[i], params)
			}
			posts := o.count()
			results := make([]reply, tc.racers)
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			ready.Add(tc.racers)
			done.Add(tc.racers)
			for i := range results {
				go func(i int) {
					defer done.Done()
					ready.Done()
					<-start
					results[i] = f.dispatch(requests[i])
				}(i)
			}
			ready.Wait()
			close(start)
			done.Wait()
			winner := -1
			for i, result := range results {
				if result.Error == nil {
					if winner >= 0 {
						t.Fatal("imported lifecycle race admitted multiple winners")
					}
					winner = i
				}
			}
			if winner < 0 {
				t.Fatal("imported lifecycle race admitted no winner")
			}
			winning := commit(t, results[winner])
			after := nativeImportedState(t, f)
			// Named close lowers to an empty patch plus a reason comment. The
			// writer therefore returns two ordered OpResults for the same target
			// in one Commit/revision, while update has one result. Both results
			// must carry the exact final record; result cardinality never denotes
			// multiple durable commits.
			expectedResults := 1
			if methods[winner] == "bead.close" {
				expectedResults = 2 // every close racer below carries a nonempty reason
			}
			if results[winner].Meta.Replayed || len(winning.Results) != expectedResults || winning.Revision == before.Meta.Head {
				t.Fatal("imported lifecycle winner has the wrong verb-specific result cardinality")
			}
			var winnerRecord driver.Record
			foundWinnerRecord := false
			for _, row := range after.Records {
				if nativeEqualJSON(row.Ref, target) {
					winnerRecord, foundWinnerRecord = row, true
					break
				}
			}
			if !foundWinnerRecord {
				t.Fatal("imported lifecycle winner record disappeared")
			}
			for _, result := range winning.Results {
				if !nativeEqualJSON(result.Ref, target) || result.Revision != winning.Revision || len(result.Record) == 0 || !bytes.Equal(result.Record, winnerRecord.Body) {
					t.Fatal("imported lifecycle winner result lost its target, revision, or final record")
				}
			}
			for i, result := range results {
				if i != winner {
					nativeRequireImportedVersionRefusal(t, result, winning.Revision)
				}
			}
			oldHead, err := before.Meta.Head.Uint64()
			if err != nil || after.Meta.Head != model.Natural64(oldHead+1) || after.Meta.Head != winning.Revision {
				t.Fatal("imported lifecycle race must advance the actual head exactly once")
			}
			if len(after.Changesets) != len(before.Changesets)+1 || len(after.Versions[target.ID]) != len(before.Versions[target.ID])+1 || len(after.Receipts) != len(before.Receipts)+1 || o.count() != posts+1 {
				t.Fatal("imported lifecycle race split its history/version/receipt/subscriber commit")
			}
			if len(before.Records) != len(after.Records) || len(before.Versions) != len(after.Versions) || !reflect.DeepEqual(before.Changesets, after.Changesets[:len(before.Changesets)]) {
				t.Fatal("imported lifecycle race changed the record set or prior history")
			}
			for key, receipt := range before.Receipts {
				if !reflect.DeepEqual(receipt, after.Receipts[key]) {
					t.Fatal("imported lifecycle race rewrote a prior receipt")
				}
			}
			oldVersions := map[string]bool{}
			for _, version := range before.Versions[target.ID] {
				oldVersions[nativeCanonical(version)] = true
			}
			newVersions := 0
			for _, version := range after.Versions[target.ID] {
				if oldVersions[nativeCanonical(version)] {
					delete(oldVersions, nativeCanonical(version))
				} else {
					newVersions++
				}
			}
			if len(oldVersions) != 0 || newVersions != 1 {
				t.Fatal("imported lifecycle race rewrote its target's prior versions")
			}
			for i := range results {
				receipt := after.Receipts[methods[i]+"/"+key(labels[i])]
				if i == winner {
					if receipt == nil || receipt.RequestHash == "" || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, winning) || !reflect.DeepEqual(receipt.Changeset, after.Changesets[len(after.Changesets)-1]) {
						t.Fatal("imported winner receipt is not the exact winning Commit/history")
					}
				} else if receipt != nil {
					t.Fatal("refused lifecycle racer persisted a receipt")
				}
			}
			got := nativeDocument(t, f, target)
			if methods[winner] == "bead.close" {
				if got.Fields["status"] != "done" || !nativeEqualJSON(got.Fields["description"], seed.Fields["description"]) || len(got.Comments) != len(seed.Comments)+1 || got.Comments[len(got.Comments)-1].Body != fmt.Sprintf("%s-close-%d", label, winner) {
					t.Fatal("imported close winner mixed a losing field/reason")
				}
			} else if got.Fields["status"] != seed.Fields["status"] || got.Fields["description"] != fmt.Sprintf("%s-update-%d", label, winner) || !reflect.DeepEqual(got.Comments, seed.Comments) {
				t.Fatal("imported update winner mixed a losing close/field")
			}
			for _, row := range before.Records {
				if row.Ref.ID == target.ID && row.Ref.Branch.IsZero() {
					continue
				}
				found := false
				for _, actual := range after.Records {
					if nativeEqualJSON(actual.Ref, row.Ref) {
						found = reflect.DeepEqual(actual, row)
					}
				}
				if !found || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID]) {
					t.Fatal("imported race changed an unrelated row/version")
				}
			}
			for _, phase := range []string{"same-process", "physical-sql-reopen"} {
				if phase == "physical-sql-reopen" {
					nativeReopenImported(t, f, o, denied)
					sameState(t, after, nativeImportedState(t, f))
				}
				denied.Store(true)
				for _, i := range []int{winner, (winner + 1) % tc.racers} {
					refused := f.dispatch(requests[i])
					if refused.Error == nil || refused.Error.Data.Code != "forbidden" || len(refused.Error.Data.Details) != 0 || len(refused.Result) != 0 || refused.Meta.Replayed || o.count() != posts+1 {
						t.Fatal("revoked current authority disclosed a lifecycle receipt or stale version", phase)
					}
					sameState(t, after, nativeImportedState(t, f))
				}
				denied.Store(false)
				replayed := f.dispatch(requests[winner])
				if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(results[winner].Result, replayed.Result) || o.count() != posts+1 {
					t.Fatal("imported lifecycle stored Commit changed or reran a subscriber", phase)
				}
				sameState(t, after, nativeImportedState(t, f))
			}
			for i := range results {
				if i == winner {
					continue
				}
				// Reuse the same refused request/key after physical reopen. On a
				// closed winner this proves the guard runs before close's no-op.
				state := nativeImportedState(t, f)
				nativeRequireImportedVersionRefusal(t, f.dispatch(requests[i]), winning.Revision)
				sameState(t, state, nativeImportedState(t, f))
				if o.count() != posts+1 {
					t.Fatal("imported stale lifecycle refusal ran a subscriber")
				}
			}
			nativeWriteJSON(t, filepath.Join(root, f.engine+"-b22-"+tc.name+".json"), map[string]any{"racers": tc.racers, "expected_revision": token, "winner": winner, "winner_method": methods[winner], "winning_commit": winning, "before": nativeRecordState(t, before), "after": nativeRecordState(t, after), "same_after_replay_and_refusal": nativeRecordState(t, nativeImportedState(t, f)), "post_before": posts, "post_after": o.count(), "reopen": map[string]any{"engine": f.engine, "physical": f.engine != "memory", "sqlite_path": f.path, "postgres_dsn_sha256": nativeHashBytes([]byte(f.dsn))}})
		}) {
			return
		}
	}
}

func nativeRequireImportedVersionRefusal(t *testing.T, got reply, revision model.Revision) {
	t.Helper()
	if got.Error == nil || got.Error.Data.Code != "precondition_failed" || got.Error.Data.Retryable || got.Meta.Replayed || len(got.Result) != 0 || got.Error.Data.Details["reason"] != "revision" {
		t.Fatal("imported lifecycle loser must be a typed nonretryable refusal", got)
	}
	current, ok := got.Error.Data.Details["currentRevision"].(json.Number)
	if !ok || current.String() != revision.String() {
		t.Fatal("imported lifecycle refusal did not name the actual numeric winning revision")
	}
}

func nativeViewDefinition(query string, resources []string, budget int, sortKeys ...string) map[string]any {
	return map[string]any{"filter": map[string]any{"query": query, "resources": resources}, "fields": []string{"id", "revision", "fields.title", "status"}, "sort": sortKeys, "budgetTokens": budget}
}
func nativeViewIDs(t *testing.T, r reply) ([]string, string) {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("imported view refusal: %+v", r.Error)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if e := json.Unmarshal(r.Result, &page); e != nil {
		t.Fatal(e)
	}
	ids := []string{}
	for _, v := range page.Items {
		ids = append(ids, v.ID)
	}
	return ids, page.NextCursor
}
func nativeImportedViews(t *testing.T, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	resources := []string{nativeID("alphaa"), nativeID("betaaa"), nativeID("gammaa")}
	before := nativeImportedState(t, f)
	unknownProjection := nativeViewDefinition("title=Needle", resources, 100, "id")
	unknownProjection["fields"] = []string{"title"}
	nativeRefuse(t, f, o, "view.run", "native-unknown-projection", map[string]any{"definition": unknownProjection}, "unknown_field")
	for _, query := range []string{"title=Needle", "label=Alpha", "label=alpha", "type=does-not-exist", "status=does-not-exist"} {
		def := nativeViewDefinition(query, resources, 100, "id")
		ids, _ := nativeViewIDs(t, f.call("view.run", "native-list-"+query, map[string]any{"definition": def}))
		r := f.call("view.run", "native-count-"+query, map[string]any{"countOnly": true, "definition": nativeViewDefinition(query, resources, 1, "id")})
		if r.Error != nil {
			t.Fatalf("imported count: %+v", r.Error)
		}
		var count struct {
			Count      *int   `json:"count"`
			Items      []any  `json:"items"`
			NextCursor string `json:"nextCursor"`
		}
		if e := json.Unmarshal(r.Result, &count); e != nil {
			t.Fatal(e)
		}
		if count.Count == nil || *count.Count != len(ids) || len(count.Items) != 0 || count.NextCursor != "" {
			t.Fatal("imported count was page limited or lost zero")
		}
		want := map[string][]string{"title=Needle": {nativeID("alphaa"), nativeID("betaaa")}, "label=Alpha": {nativeID("alphaa")}, "label=alpha": {nativeID("betaaa")}, "type=does-not-exist": {}, "status=does-not-exist": {}}[query]
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf("imported predicate %s got=%v want=%v", query, ids, want)
		}
	}
	denied.Store(true)
	ids, _ := nativeViewIDs(t, f.call("view.run", "native-auth-list", map[string]any{"definition": nativeViewDefinition("title=Needle", resources, 100, "id")}))
	r := f.call("view.run", "native-auth-count", map[string]any{"countOnly": true, "definition": nativeViewDefinition("title=Needle", resources, 1, "id")})
	var count struct {
		Count int `json:"count"`
	}
	_ = json.Unmarshal(r.Result, &count)
	if r.Error != nil || !reflect.DeepEqual(ids, []string{nativeID("alphaa")}) || count.Count != 1 {
		t.Fatal("imported current authorization leaked count or page")
	}
	denied.Store(false)
	sameState(t, before, nativeImportedState(t, f))
	// Historical content stays pinned while authorization uses the currently
	// configured policy. A later title edit must not rewrite the old predicate.
	commit(t, f.call("bead.update", "native-auth-later-title", map[string]any{"id": nativeImportedRef("betaaa"), "fields": map[string]any{"title": "current beta no longer matches"}}))
	historical := map[string]any{"revision": before.Meta.Head, "incarnation": before.Meta.Incarnation}
	def := nativeViewDefinition("title=Needle", resources, 100, "id")
	oldIDs, _ := nativeViewIDs(t, f.call("view.run", "native-historical-allowed", map[string]any{"definition": def, "asOf": historical}))
	if !reflect.DeepEqual(oldIDs, []string{nativeID("alphaa"), nativeID("betaaa")}) {
		t.Fatalf("imported historical content was replaced by current content: %v", oldIDs)
	}
	currentIDs, _ := nativeViewIDs(t, f.call("view.run", "native-historical-current-control", map[string]any{"definition": def}))
	if !reflect.DeepEqual(currentIDs, []string{nativeID("alphaa")}) {
		t.Fatalf("imported current title control: %v", currentIDs)
	}
	authBefore := nativeImportedState(t, f)
	denied.Store(true)
	oldIDs, _ = nativeViewIDs(t, f.call("view.run", "native-historical-current-deny", map[string]any{"definition": def, "asOf": historical}))
	r = f.call("view.run", "native-historical-denied-count", map[string]any{"countOnly": true, "definition": def, "asOf": historical})
	if err := json.Unmarshal(r.Result, &count); err != nil || r.Error != nil || count.Count != 1 || !reflect.DeepEqual(oldIDs, []string{nativeID("alphaa")}) {
		t.Fatalf("imported historical view restored an old grant: ids=%v count=%s error=%v", oldIDs, r.Result, r.Error)
	}
	denied.Store(false)
	sameState(t, authBefore, nativeImportedState(t, f))
	// Produce actual native-imported live revisions crossing 9 -> 10. The literal
	// expected sequence cannot be learned from the comparator or unpaged result.
	first, second, third := nativeImportedRef("alphaa"), nativeImportedRef("betaaa"), nativeImportedRef("gammaa")
	for {
		meta, e := f.store.Meta(f.ctx)
		if e != nil {
			t.Fatal(e)
		}
		n, e := meta.Head.Uint64()
		if e != nil {
			t.Fatal(e)
		}
		if n >= 8 {
			break
		}
		commit(t, f.call("bead.update", fmt.Sprintf("native-pad-%s", meta.Head), map[string]any{"id": nativeImportedRef("anchor"), "fields": map[string]any{"description": meta.Head.String()}}))
	}
	commit(t, f.call("bead.update", "native-rev9", map[string]any{"id": first, "fields": map[string]any{"description": "revision-nine"}}))
	commit(t, f.call("bead.update", "native-rev10", map[string]any{"id": second, "fields": map[string]any{"description": "revision-ten"}}))
	commit(t, f.call("bead.update", "native-rev11", map[string]any{"id": third, "fields": map[string]any{"description": "revision-eleven"}}))
	for _, spec := range []struct {
		sort string
		want []string
	}{{"revision", []string{first.ID, second.ID, third.ID}}, {"-revision", []string{third.ID, second.ID, first.ID}}} {
		def := nativeViewDefinition("", resources, 1, spec.sort)
		pageBefore := nativeImportedState(t, f)
		cursor := ""
		got := []string{}
		seen := map[string]bool{}
		for i := 0; i < 10; i++ {
			p := map[string]any{"definition": def}
			if cursor != "" {
				p["cursor"] = cursor
			}
			ids, next := nativeViewIDs(t, f.call("view.run", fmt.Sprintf("native-page-%s-%d", spec.sort, i), p))
			got = append(got, ids...)
			if next == "" {
				break
			}
			if seen[next] {
				t.Fatal("imported cursor repeated")
			}
			seen[next] = true
			cursor = next
			if i == 0 && f.engine != "memory" {
				nativeWriteJSON(t, filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "native-"+f.engine+"-saved-cursor-"+spec.sort+".json"), map[string]any{"cursor": cursor, "first_ids": ids, "definition": def, "incarnation": pageBefore.Meta.Incarnation, "revision": pageBefore.Meta.Head})
				nativeReopenImported(t, f, o, denied)
				sameState(t, pageBefore, nativeImportedState(t, f))
			}
		}
		if !reflect.DeepEqual(got, spec.want) {
			t.Fatalf("imported numeric paging %s got=%v want=%v", spec.sort, got, spec.want)
		}
		sameState(t, pageBefore, nativeImportedState(t, f))
	}
}
func nativeImportedDefer(t *testing.T, root string, f *fixture, o *nativePostObserver, denied *atomic.Bool) {
	t.Helper()
	target := nativeImportedRef("timedaa")
	nativeRefuse(t, f, o, "bead.defer", "native-plug-blank-defer", map[string]any{"id": target, "reason": " "}, "invalid_params")
	c := commit(t, f.call("bead.defer", "native-plug-defer", map[string]any{"id": target, "reason": "explicit waiting"}))
	if len(c.Results) == 0 {
		t.Fatal("imported defer missing result")
	}
	before := nativeImportedState(t, f)
	d := nativeDocument(t, f, target)
	if d.Fields["status"] != "backlog" {
		t.Fatalf("imported untimed defer status=%v", d.Fields["status"])
	}
	o.stopHostLease(t)
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	clockBefore := f.clock.Now()
	f.clock.Advance(t, 400*24*time.Hour)
	nativeBindImported(t, f, o, denied)
	ready, _ := nativeViewIDs(t, f.call("bead.ready", "native-defer-after-clock", map[string]any{"limit": 100, "filter": map[string]any{"resources": []string{target.ID}}}))
	if len(ready) != 0 {
		t.Fatal("Plug imported untimed defer auto-woke")
	}
	afterClock := nativeImportedState(t, f)
	// Preserve the 400-day advance and the 24-hour G4 visibility contract. Every
	// prior receipt must now be hidden; all durable state remains byte exact.
	if f.clock.Now().Sub(clockBefore) != 400*24*time.Hour || len(before.Receipts) == 0 || len(afterClock.Receipts) != 0 {
		t.Fatalf("native imported clock/receipt visibility: before=%d after=%d delta=%s", len(before.Receipts), len(afterClock.Receipts), f.clock.Now().Sub(clockBefore))
	}
	durableBefore, durableAfter := before, afterClock
	durableBefore.Receipts, durableAfter.Receipts = nil, nil
	sameState(t, durableBefore, durableAfter)
	nativeWriteJSON(t, filepath.Join(root, f.engine+"-clock-visibility.json"), map[string]any{"clock_before": clockBefore, "clock_after": f.clock.Now(), "idempotency_window": driver.IdempotencyWindow.String(), "before": nativeRecordState(t, before), "after": nativeRecordState(t, afterClock), "durable_equal": true})
	nativeReopenImported(t, f, o, denied)
	sameState(t, afterClock, nativeImportedState(t, f))
	commit(t, f.call("bead.reopen", "native-plug-reopen", map[string]any{"id": target, "reason": "explicit reopen"}))
	if nativeDocument(t, f, target).Fields["status"] != "todo" {
		t.Fatal("imported explicit reopen did not restore initial category")
	}
	// A controlled stale view reads the same imported rows; it may not mutate
	// timestamps or storage while advancing the application and SQL test clock.
	before = nativeImportedState(t, f)
	stale := f.call("view.run", "native-imported-stale", map[string]any{"definition": nativeViewDefinition("", []string{target.ID}, 100, "id"), "stale": map[string]any{"days": 1}})
	if stale.Error == nil || stale.Error.Data.Code != "invalid_params" {
		t.Fatalf("imported configured stale missing-source refusal: %+v", stale)
	}
	sameState(t, before, nativeImportedState(t, f))
	nativeWriteJSON(t, filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "native-"+f.engine+"-defer.json"), map[string]any{"deferred": d, "reopened": nativeDocument(t, f, target), "stale_missing_source_refusal": stale.Error, "stale_qualification": "WAIT: configured imported ontology has no protected source record; no synthetic fallback", "adopted": "untimed with explicit reopen"})
}
