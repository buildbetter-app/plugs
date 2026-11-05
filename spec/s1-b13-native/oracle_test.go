package b13native_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

var b13DerivedTables = []driver.DerivedTable{
	driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure,
	driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex,
	driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins,
}

var b13Anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

const b13Agent = "b13-native-recorder"

type b13Scope struct {
	Principal, Agent, Method, Key string
}

type b13Post struct {
	Change port.CommittedChange
	View   driver.View
	Rows   []driver.Record
	Error  any
}

// These are original service returns, not objects reconstructed from HTTP or
// normalized into valid model.Commit values. Failure commits may be invalid
// public JSON; the method-free companion is always saved first.
type b13Return struct {
	Method    string
	Principal model.PrincipalRef
	Request   any
	Meta      model.WriteMeta
	Value     any
	Error     error
}

type b13Image struct {
	Meta            driver.Meta
	View            driver.View
	Rows            []driver.Record
	Versions        map[string][]driver.Version
	RecordKeys      []driver.LockKey
	RecordStates    []driver.LockState
	Derived         map[driver.DerivedTable][]driver.DerivedRow
	DerivedStates   map[driver.DerivedTable][]driver.LockState
	Changes         []model.Changeset
	ChangesCursor   *driver.Cursor
	BelowFloor      any
	ImportReceipt   *driver.IdempotencyRecord
	ImportChangeset *model.Changeset
	Receipts        map[string]*driver.IdempotencyRecord
	Archive         []byte
	ArchiveError    any
	Posts           []b13Post
	Issues          []string
}

type b13Probe struct {
	Label, Method     string
	Params            map[string]any
	Scope             *b13Scope
	Request, Response []byte
	Status            int
	Header            http.Header
	Started, Ended    time.Time
	TransportError    error
	Before, After     b13Image
	Returns           []b13Return
	CaptureErrors     []string
}

type b13HTTPRun struct {
	t                 *testing.T
	ctx               context.Context
	root, engine, dsn string
	store             driver.Store
	lease             driver.ActiveLease
	profile           driver.RestoreImportProfile
	archive           []byte
	known             map[string]model.Ref
	scopes            map[string]b13Scope
	server            *httpjson.Server
	client            *http.Client
	serveDone         chan error
	url               string
	request           int
	mu                sync.Mutex
	active            string
	returns           []b13Return
	posts             []b13Post
	captureErrors     []string
	lifecycle         int
	cleanupReported   bool
	closeErrors       []string
}

// b13Observed delegates to the real domain unchanged. It does not inject a
// response, lower a second write, or repair an imported body. The raw return
// is durable before RPC attempts to encode it and before any after-image read.
type b13Observed struct {
	*service.Service
	run *b13HTTPRun
}

func (s *b13Observed) MemoryGet(ctx context.Context, by model.PrincipalRef, req service.MemoryGetRequest) (service.Memory, error) {
	v, err := s.Service.MemoryGet(ctx, by, req)
	s.run.returned(b13Return{Method: "memory.get", Principal: by, Request: req, Value: v, Error: err})
	return v, err
}

func (s *b13Observed) MemoryList(ctx context.Context, by model.PrincipalRef, req service.MemoryListRequest) (service.MemoryPage, error) {
	v, err := s.Service.MemoryList(ctx, by, req)
	s.run.returned(b13Return{Method: "memory.list", Principal: by, Request: req, Value: v, Error: err})
	return v, err
}

func (s *b13Observed) Brief(ctx context.Context, by model.PrincipalRef, req service.BriefRequest) (service.BriefResult, error) {
	v, err := s.Service.Brief(ctx, by, req)
	s.run.returned(b13Return{Method: "agent.brief", Principal: by, Request: req, Value: v, Error: err})
	return v, err
}

func (s *b13Observed) MemorySaveWithOutcome(ctx context.Context, by model.PrincipalRef, req service.MemorySaveRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	v, err := s.Service.MemorySaveWithOutcome(ctx, by, req, meta)
	s.run.returned(b13Return{Method: "memory.save", Principal: by, Request: req, Meta: meta, Value: v, Error: err})
	return v, err
}

func (s *b13Observed) MemoryDeleteWithOutcome(ctx context.Context, by model.PrincipalRef, req service.MemoryDeleteRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	v, err := s.Service.MemoryDeleteWithOutcome(ctx, by, req, meta)
	s.run.returned(b13Return{Method: "memory.delete", Principal: by, Request: req, Meta: meta, Value: v, Error: err})
	return v, err
}

func b13Error(err error) any {
	if err == nil {
		return nil
	}
	return map[string]any{"type": fmt.Sprintf("%T", err), "text": err.Error(), "fields": b13Raw(reflect.ValueOf(err))}
}

func (f *b13HTTPRun) returned(value b13Return) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := len(f.returns)
	f.returns = append(f.returns, value)
	path := filepath.Join(f.root, "http", f.active+fmt.Sprintf("-return-%02d.raw.json", index))
	if err := b13Write(path, append(b13JSONRaw(value), '\n')); err != nil {
		f.captureErrors = append(f.captureErrors, err.Error())
	}
}

func b13JSONRaw(value any) []byte {
	// b13Raw only contains standard JSON scalars/containers, never a model
	// MarshalJSON implementation. No successful public commit is fabricated.
	data, err := json.Marshal(b13Raw(reflect.ValueOf(value)))
	if err != nil {
		panic(err)
	}
	return data
}

func (f *b13HTTPRun) After(ctx context.Context, change port.CommittedChange, r port.SnapshotReader) error {
	post := b13Post{Change: change, View: r.Snapshot(), Rows: []driver.Record{}}
	var cursor *driver.Cursor
	var failure error
	for pageCount := 0; pageCount < 100; pageCount++ {
		page, err := r.Query(ctx, driver.Query{Limit: 1000, Cursor: cursor})
		post.Rows = append(post.Rows, page.Items...)
		if err != nil {
			failure = err
			break
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || pageCount == 99 {
			failure = errors.New("subscriber query did not finish")
			break
		}
		cursor = page.NextCursor
	}
	post.Error = b13Error(failure)
	f.mu.Lock()
	index := len(f.posts)
	f.posts = append(f.posts, post)
	path := filepath.Join(f.root, "subscribers", fmt.Sprintf("%04d.raw.json", index))
	writeErr := b13Write(path, append(b13JSONRaw(post), '\n'))
	if writeErr != nil {
		f.captureErrors = append(f.captureErrors, writeErr.Error())
	}
	f.mu.Unlock()
	return errors.Join(failure, writeErr)
}

func b13Public(t *testing.T, parent, workspace, engine string, archiveBytes []byte, expected map[string]string) {
	t.Helper()
	copyExpected := map[string]string{}
	for key, value := range expected {
		copyExpected[key] = value
	}
	expected = copyExpected
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	root := filepath.Join(parent, "public-"+engine)
	if err := b13Mkdir(root); err != nil {
		t.Fatal(err)
	}
	f := &b13HTTPRun{t: t, ctx: ctx, root: root, engine: engine, archive: bytes.Clone(archiveBytes), known: map[string]model.Ref{}, scopes: map[string]b13Scope{}}
	b13Bytes(t, root, "original-native-archive.tar", archiveBytes)
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(archiveBytes))
	b13RawSave(t, root, "inspection.raw.json", struct {
		Value converterroundtrip.ImportInspection
		Error error
	}{inspection, err})
	if err != nil {
		t.Fatalf("unchanged actual B13 archive inspection: %v", err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		t.Fatal(err)
	}
	// This is the isolated recorder host's explicit snapshot policy/admission,
	// not a claim of source-history replay or an external production login.
	admission := map[string]any{"schema": "plug-b13-recorder-host-admission/1", "engine": engine, "archive": inspection.ArchiveSHA256, "source_store": inspection.SourceStoreID, "history_mode": "snapshot", "actor": "user:b13-import-recorder", "authority": "isolated recorder host; no external production admission"}
	b13Save(t, root, "host-admission.json", admission)
	f.profile = driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "user:b13-import-recorder"}, Authority: "restore-admin", AdmissionDigest: b13SHA(b13JSON(t, admission))},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: b13RandomKey(t),
		ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	b13Save(t, root, "import-profile.json", f.profile)
	switch engine {
	case "memory":
		f.store = memory.New()
	case "sqlite":
		f.dsn = filepath.Join(workspace, "b13-public.sqlite")
		f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.dsn = testclock.PostgresDSN(t)
		f.store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: f.dsn})
	default:
		t.Fatalf("unknown public engine %q", engine)
	}
	b13RawSave(t, root, "open.raw.json", struct {
		Engine, DSN string
		Error       error
	}{engine, f.dsn, err})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.close(true) })
	for _, source := range archive.Records("memory") {
		f.remember(model.RefByID("source/memory/"+source.ID, model.Natural64(0)))
	}
	f.remember(model.RefByID("b13-missing-memory", model.Natural64(0)))
	before := f.image("import-before")
	report, importErr := converterroundtrip.RestoreImport(ctx, bytes.NewReader(archiveBytes), f.store, f.profile)
	b13RawSave(t, root, "import-return.raw.json", struct {
		Report converterroundtrip.Report
		Error  error
	}{report, importErr})
	after := f.image("import-after")
	if importErr != nil {
		t.Fatalf("actual B13 restore refused; complete return/target evidence retained: %v", importErr)
	}
	f.validImage(before)
	f.validImage(after)
	if !before.Meta.Head.IsZero() || after.Meta.Head.String() != "1" || after.Meta.HistoryFloor != after.Meta.Head || !reflect.DeepEqual(after.Meta, report.Restored) || after.Meta.Incarnation == before.Meta.Incarnation || after.Meta.StoreID != f.profile.SourceStoreID {
		t.Fatal("actual snapshot import identity/head/floor/return binding changed")
	}
	for _, row := range after.Rows {
		versions := after.Versions[b13Ref(row.Ref)]
		if len(versions) != 1 || !versions[0].Base || versions[0].Deleted || versions[0].Revision != after.Meta.Head || !bytes.Equal(versions[0].Body, row.Body) {
			t.Fatal("snapshot baseline is not one actual full base per row")
		}
	}
	ids := b13ImportedMemories(t, archive, after, expected)
	f.start()
	// Acquire every independent initial read response before judging the known
	// converter body-shape gap. An error does not erase get/list/brief bytes,
	// typed returns, the complete imported image, or the other initial probes.
	type check struct {
		p                b13Probe
		kind, key, query string
	}
	checks := []check{}
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for index, key := range keys {
		checks = append(checks, check{p: f.probe(fmt.Sprintf("imported-get-%02d", index), "memory.get", map[string]any{"id": ids[key]}, nil), kind: "get", key: key})
	}
	checks = append(checks, check{p: f.probe("imported-missing", "memory.get", map[string]any{"id": "b13-missing-memory"}, nil), kind: "missing"})
	for index, query := range []string{"", "ALPHA", "EXACT UNICODE", "no-such-memory-needle"} {
		checks = append(checks, check{p: f.probe(fmt.Sprintf("imported-list-%02d", index), "memory.list", map[string]any{"query": query, "limit": 1000}, nil), kind: "list", query: query})
	}
	checks = append(checks, check{p: f.probe("imported-brief", "agent.brief", map[string]any{}, nil), kind: "brief"})
	f.reverse("imported-live", after)
	problems := []string{}
	for _, c := range checks {
		f.validProbe(c.p)
		if !reflect.DeepEqual(c.p.Before, c.p.After) {
			problems = append(problems, c.p.Label+": read changed complete state/subscribers")
		}
		if c.kind == "missing" {
			if err := b13ExpectError(c.p, "not_found"); err != nil {
				problems = append(problems, err.Error())
			}
			continue
		}
		result, err := b13Result(c.p)
		if err == nil {
			switch c.kind {
			case "get":
				err = b13MemoryResult(result, ids[c.key], c.key, expected[c.key], c.p.Before)
			case "list":
				err = b13MemoryItems(result["items"], ids, expected, c.query, c.p.Before)
			case "brief":
				err = b13MemoryItems(result["memories"], ids, expected, "", c.p.Before)
			}
		}
		if err != nil {
			problems = append(problems, c.p.Label+": "+err.Error())
		}
	}
	b13Save(t, root, "initial-public-verdict.json", map[string]any{"problems": problems, "archive_sha256": b13SHA(archiveBytes), "no_imported_body_rewrite": true})
	if len(problems) != 0 {
		t.Fatalf("actual imported B13 public boundary failed: %s; later mutations/reopens are NOT_RUN", strings.Join(problems, "; "))
	}

	// Only actual successful initial public reads reach these writes. Each
	// ordinary HTTP write has its own receipt, exact replay and fresh-key noop;
	// all return/after/post evidence precedes the conservation assertions.
	var durableReplay *b13Probe
	for _, c := range []struct{ label, key, content string }{
		{"verbatim", " B13 public key/Ω ", "  Public\n東京 Ω\tverbatim\r\nend  "},
		{"derived-eight", "one-two-three-four-five-six-seven-eight", "ONE TWO three FOUR five SIX seven EIGHT public replacement"},
		{"derived-cap", strings.Repeat("a", 60), strings.Repeat("A", 61) + " public replacement"},
		{"derived-separator", strings.Repeat("b", 59), strings.Repeat("B", 59) + " c public replacement"},
		{"derived-collapse", "hello-world-preserve-exact-words", "HELLO...world; preserve  EXACT\tWORDS!!"},
		{"replacement", "alpha", "Public replacement\n東京\tretained verbatim"},
	} {
		params := map[string]any{"content": c.content}
		if !strings.HasPrefix(c.label, "derived-") {
			params["key"] = c.key
		}
		// Retain any actual imported metadata when replacing an existing key;
		// omission must not silently discard its source-lineage evidence.
		image := f.image(c.label + "-request-basis")
		for _, row := range image.Rows {
			if row.Kind != "memory" {
				continue
			}
			body := b13Object(t, row.Body)
			if body["key"] == c.key {
				if metadata, ok := body["metadata"]; ok {
					params["metadata"] = metadata
				}
			}
		}
		first := f.write(c.label, "memory.save", params)
		f.checkWrite(first, c.key, &c.content, false)
		if durableReplay == nil {
			saved := first
			durableReplay = &saved
		}
		replay := f.probe(c.label+"-replay", "memory.save", params, first.Scope)
		f.checkReplay(first, replay)
		noop := f.write(c.label+"-noop", "memory.save", params)
		f.checkWrite(noop, c.key, &c.content, true)
		expected[c.key] = c.content
		outcome := first.Returns[0].Value.(port.WriteOutcome)
		ids[c.key] = outcome.Commit.Results[0].Ref.ID
	}
	for _, c := range []struct {
		label  string
		params map[string]any
	}{
		{"blank-content", map[string]any{"key": "blank", "content": " \t\n "}},
		{"blank-key", map[string]any{"key": " \t ", "content": "valid body"}},
		{"underivable", map[string]any{"content": "!!!"}},
		{"unicode-underivable", map[string]any{"content": "東京"}},
	} {
		f.checkRefusal(f.write(c.label, "memory.save", c.params), "invalid_params")
	}
	for _, key := range []string{"alpha", "empty"} {
		p := f.write("delete-"+key, "memory.delete", map[string]any{"id": ids[key]})
		f.checkWrite(p, key, nil, false)
		f.checkReplay(p, f.probe("delete-"+key+"-replay", "memory.delete", p.Params, p.Scope))
		missing := f.probe("deleted-get-"+key, "memory.get", map[string]any{"id": ids[key]}, nil)
		f.validProbe(missing)
		if err := b13ExpectError(missing, "not_found"); err != nil || !reflect.DeepEqual(missing.Before, missing.After) {
			t.Fatalf("deleted memory public read/conservation: %v", err)
		}
		f.checkRefusal(f.write("delete-"+key+"-missing", "memory.delete", map[string]any{"id": ids[key]}), "not_found")
		delete(expected, key)
		delete(ids, key)
	}
	final := f.probe("final-list", "memory.list", map[string]any{"limit": 1000}, nil)
	f.validProbe(final)
	if !reflect.DeepEqual(final.Before, final.After) {
		t.Fatal("final list changed complete state")
	}
	result, err := b13Result(final)
	if err == nil {
		err = b13MemoryItems(result["items"], ids, expected, "", final.Before)
	}
	if err != nil {
		t.Fatal(err)
	}
	// Reverse conversion must retain the unmodified native source archive and
	// the original settings plane after reachable live memory operations.
	f.reverse("after-public-writes", final.After)
	if engine != "memory" {
		if durableReplay == nil {
			t.Fatal("no actual successful HTTP write retained for durable replay")
		}
		f.reopen(final.After, ids, expected, *durableReplay)
	}
	f.close(true)
	if len(f.closeErrors) != 0 {
		t.Fatal("actual HTTP/store/lease close returned errors; no complete recording credit")
	}
	b13Save(t, root, "bounded-complete.json", map[string]any{"status": "B13_NATIVE_AND_PUBLIC_RECORDING_COMPLETE_REQUIRES_INDEPENDENT_REVIEW", "engine": engine, "archive_sha256": b13SHA(archiveBytes), "snapshot_policy": "explicit; source history not replayed", "NativeAdmission": "None", "FullS1": "0/28"})
}

func (f *b13HTTPRun) start() {
	f.t.Helper()
	var err error
	f.lease, err = f.store.Lease(f.ctx, "b13-public-"+f.engine, driver.LeaseTTL)
	f.lifecycleReturn("lease", err)
	if err != nil {
		f.t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b13-complete-post", Subscriber: f}))
	domain := &b13Observed{Service: service.New(f.store, f.lease, writer, service.WithOntology(ontology)), run: f}
	registry, err := rpc.NewRegistry()
	if err == nil {
		err = rpc.BindService(registry, domain)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "b13"})
	if err != nil {
		f.t.Fatal(err)
	}
	auth, err := authn.New(authn.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	handler, err := httpjson.New(server, auth, httpjson.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	f.server, err = handler.Listen(f.ctx, "127.0.0.1:0", nil)
	if err != nil {
		f.t.Fatal(err)
	}
	f.url = "http://" + f.server.Addr().String() + httpjson.Path
	f.client = &http.Client{Timeout: 35 * time.Second, Transport: &http.Transport{Proxy: nil}}
	f.serveDone = make(chan error, 1)
	go func() { f.serveDone <- f.server.Serve() }()
	b13Save(f.t, f.root, fmt.Sprintf("http-listener-%02d.json", f.lifecycle), map[string]any{"url": f.url, "transport": "real loopback HTTPJSON/authn/RPC; local anonymous posture", "incarnation": meta.Incarnation})
}

func (f *b13HTTPRun) lifecycleReturn(operation string, err error) {
	f.lifecycle++
	b13RawSave(f.t, f.root, fmt.Sprintf("lifecycle/%03d-%s.raw.json", f.lifecycle, operation), struct {
		Operation string
		Ended     time.Time
		Error     error
	}{operation, time.Now().UTC(), err})
}

func (f *b13HTTPRun) close(cleanup bool) {
	record := func(operation string, err error) {
		f.lifecycleReturn(operation, err)
		if err != nil {
			f.closeErrors = append(f.closeErrors, operation+": "+err.Error())
			f.t.Error(err)
		}
	}
	if f.client != nil {
		f.client.CloseIdleConnections()
		f.client = nil
	}
	if f.server != nil {
		err := f.server.Close()
		record("http-close", err)
		serveErr := <-f.serveDone
		f.lifecycleReturn("http-serve-return", serveErr)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			f.closeErrors = append(f.closeErrors, "http-serve-return: "+serveErr.Error())
			f.t.Error(serveErr)
		}
		f.server = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if f.lease != nil {
		err := f.lease.Release(ctx)
		record("lease-release", err)
		f.lease = nil
	}
	if f.store != nil {
		err := f.store.Close()
		record("store-close", err)
		f.store = nil
	}
	if cleanup && !f.cleanupReported {
		f.cleanupReported = true
		b13Save(f.t, f.root, "cleanup-result.json", map[string]any{"resources_relinquished": f.server == nil && f.store == nil && f.lease == nil, "errors": f.closeErrors, "utc": time.Now().UTC()})
	}
}

func (f *b13HTTPRun) reopen(before b13Image, ids, expected map[string]string, durable b13Probe) {
	f.close(false)
	var err error
	switch f.engine {
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatal("only actual SQL stores have a reopen obligation")
	}
	f.lifecycleReturn("store-reopen", err)
	if err != nil {
		f.t.Fatal(err)
	}
	after := f.image("actual-sql-reopen")
	f.validImage(after)
	if !reflect.DeepEqual(before, after) {
		f.t.Fatal("actual SQL Close/Open changed full reader/versions/history/derived/receipts/archive/subscriber image")
	}
	f.start()
	p := f.probe("reopened-list", "memory.list", map[string]any{"limit": 1000}, nil)
	f.validProbe(p)
	result, err := b13Result(p)
	if err == nil {
		err = b13MemoryItems(result["items"], ids, expected, "", p.Before)
	}
	if err != nil || !reflect.DeepEqual(p.Before, p.After) {
		f.t.Fatalf("reopened actual HTTP list: %v", err)
	}
	f.checkReplay(durable, f.probe("reopened-stored-replay", durable.Method, durable.Params, durable.Scope))
}

func (f *b13HTTPRun) write(label, method string, params map[string]any) b13Probe {
	principal, err := driver.PrincipalKey(b13Anonymous)
	if err != nil {
		f.t.Fatal(err)
	}
	scope := &b13Scope{Principal: principal, Agent: b13Agent, Method: method, Key: b13RandomKey(f.t)}
	return f.probe(label, method, params, scope)
}

func (f *b13HTTPRun) probe(label, method string, params map[string]any, scope *b13Scope) b13Probe {
	f.t.Helper()
	if scope != nil {
		f.scopes[scope.Key] = *scope
	}
	if f.lease != nil {
		// Ordinary service-lifetime maintenance, not a lease/clock/B05 oracle.
		err := f.lease.Renew(f.ctx)
		b13RawSave(f.t, f.root, "http/"+label+"-lifetime.raw.json", struct{ Error error }{err})
		if err != nil {
			f.t.Fatal(err)
		}
	}
	f.mu.Lock()
	f.active = label
	f.returns = nil
	f.captureErrors = nil
	f.mu.Unlock()
	p := b13Probe{Label: label, Method: method, Params: params, Scope: scope, Before: f.image(label + "-before")}
	copyParams := map[string]any{}
	for key, value := range params {
		if key != "_meta" {
			copyParams[key] = value
		}
	}
	meta := map[string]any{"project": "b13", "agent": b13Agent}
	if scope != nil {
		meta["idempotencyKey"] = scope.Key
		meta["return"] = "full"
	}
	copyParams["_meta"] = meta
	f.request++
	p.Request = b13JSON(f.t, map[string]any{"jsonrpc": "2.0", "id": f.request, "method": method, "params": copyParams})
	b13Bytes(f.t, f.root, "http/"+label+".request", p.Request)
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.url, bytes.NewReader(p.Request))
	if err != nil {
		f.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	p.Started = time.Now().UTC()
	response, err := f.client.Do(req)
	p.TransportError = err
	if response != nil {
		p.Status = response.StatusCode
		p.Header = response.Header.Clone()
		p.Response, err = io.ReadAll(response.Body)
		p.TransportError = errors.Join(p.TransportError, err, response.Body.Close())
	}
	p.Ended = time.Now().UTC()
	b13Bytes(f.t, f.root, "http/"+label+".response", p.Response)
	b13RawSave(f.t, f.root, "http/"+label+"-transport.raw.json", struct {
		Status         int
		Header         http.Header
		Started, Ended time.Time
		Error          error
	}{p.Status, p.Header, p.Started, p.Ended, p.TransportError})
	f.mu.Lock()
	p.Returns = append([]b13Return(nil), f.returns...)
	p.CaptureErrors = append([]string(nil), f.captureErrors...)
	f.mu.Unlock()
	p.After = f.image(label + "-after")
	b13RawSave(f.t, f.root, "http/"+label+"-complete.raw.json", p)
	return p
}

func (f *b13HTTPRun) remember(ref model.Ref) {
	f.known[b13Ref(ref)] = ref
}

func b13Ref(ref model.Ref) string {
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	if err != nil {
		return "INVALID:" + err.Error()
	}
	return key
}

func b13RandomKey(t *testing.T) string {
	t.Helper()
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data[:])
}

func (f *b13HTTPRun) image(label string) b13Image {
	f.t.Helper()
	x := b13Image{Rows: []driver.Record{}, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, DerivedStates: map[driver.DerivedTable][]driver.LockState{}, Changes: []model.Changeset{}, Receipts: map[string]*driver.IdempotencyRecord{}, Issues: []string{}}
	observe := func(phase string, value any, err error) {
		// Save each actual page/value/error, including null/empty containers,
		// before aggregation. Aggregates are explicitly collector structures.
		path := filepath.Join(f.root, "images", label+"-calls", phase+".raw.json")
		if e := b13Write(path, append(b13JSONRaw(struct {
			Value any
			Error error
		}{value, err}), '\n')); e != nil {
			x.Issues = append(x.Issues, phase+": capture: "+e.Error())
		}
		if err != nil {
			x.Issues = append(x.Issues, phase+": "+err.Error())
		}
	}
	var err error
	x.Meta, err = f.store.Meta(f.ctx)
	observe("meta", x.Meta, err)
	if err == nil {
		r, readErr := f.store.Read(f.ctx, driver.View{Incarnation: x.Meta.Incarnation})
		observe("read", struct{ Opened bool }{r != nil}, readErr)
		if readErr == nil && r != nil {
			x.View = r.Snapshot()
			var cursor *driver.Cursor
			for pageIndex := 0; pageIndex < 100; pageIndex++ {
				page, e := r.Query(f.ctx, driver.Query{Limit: 1000, Cursor: cursor})
				observe(fmt.Sprintf("query-%03d", pageIndex), page, e)
				x.Rows = append(x.Rows, page.Items...)
				if e != nil || page.NextCursor == nil {
					break
				}
				if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) || pageIndex == 99 {
					x.Issues = append(x.Issues, "record query did not finish")
					break
				}
				cursor = page.NextCursor
			}
			for _, row := range x.Rows {
				f.remember(row.Ref)
			}
			// Keep known deleted and explicitly missing refs in all later
			// snapshots. Query-only version capture would lose tombstones.
			refKeys := make([]string, 0, len(f.known))
			for key := range f.known {
				refKeys = append(refKeys, key)
			}
			sort.Strings(refKeys)
			for refIndex, key := range refKeys {
				ref := f.known[key]
				x.RecordKeys = append(x.RecordKeys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
				versions := []driver.Version{}
				var next *driver.Cursor
				for pageIndex := 0; pageIndex < 100; pageIndex++ {
					page, e := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000, Cursor: next})
					observe(fmt.Sprintf("versions-%03d-%03d", refIndex, pageIndex), page, e)
					versions = append(versions, page.Items...)
					if e != nil || page.NextCursor == nil {
						break
					}
					if len(page.Items) == 0 || reflect.DeepEqual(next, page.NextCursor) || pageIndex == 99 {
						x.Issues = append(x.Issues, "version query did not finish for "+key)
						break
					}
					next = page.NextCursor
				}
				x.Versions[key] = versions
			}
			if e := driver.ValidateLockKeys(x.RecordKeys); e != nil {
				observe("record-key-validation", x.RecordKeys, e)
			} else {
				x.RecordStates, err = r.Observe(f.ctx, x.RecordKeys)
				observe("record-observations", x.RecordStates, err)
			}
			for _, table := range b13DerivedTables {
				x.Derived[table], err = r.DerivedRows(f.ctx, table, "")
				observe("derived-"+string(table), x.Derived[table], err)
				keys := make([]driver.LockKey, len(x.Derived[table]))
				for i, row := range x.Derived[table] {
					keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
				}
				sort.Slice(keys, func(i, j int) bool { return keys[i].Derived.Key < keys[j].Derived.Key })
				if e := driver.ValidateLockKeys(keys); e != nil {
					observe("derived-keys-"+string(table), keys, e)
					continue
				}
				x.DerivedStates[table], err = r.Observe(f.ctx, keys)
				observe("derived-observations-"+string(table), x.DerivedStates[table], err)
			}
			if !x.Meta.HistoryFloor.IsZero() {
				value, e := r.Changesets(f.ctx, nil, 1000)
				x.BelowFloor = struct {
					Value []model.Changeset
					Error any
				}{value, b13Error(e)}
				// This refusal is expected evidence, not a normalized empty log.
				if captureErr := b13Write(filepath.Join(f.root, "images", label+"-calls", "below-floor.raw.json"), append(b13JSONRaw(struct {
					Value []model.Changeset
					Error error
				}{value, e}), '\n')); captureErr != nil {
					x.Issues = append(x.Issues, captureErr.Error())
				}
				var truncated driver.ErrTruncated
				if !errors.As(e, &truncated) || truncated.Floor != x.Meta.HistoryFloor || value != nil {
					x.Issues = append(x.Issues, "nil-cursor history did not return exact typed floor refusal")
				}
				x.ChangesCursor = &driver.Cursor{Incarnation: x.View.Incarnation, Revision: x.Meta.HistoryFloor}
			}
			cursor = x.ChangesCursor
			for pageIndex := 0; pageIndex < 100; pageIndex++ {
				page, e := r.Changesets(f.ctx, cursor, 1000)
				observe(fmt.Sprintf("changesets-%03d", pageIndex), page, e)
				x.Changes = append(x.Changes, page...)
				if e != nil || len(page) == 0 {
					break
				}
				last := page[len(page)-1].Revision
				if cursor != nil && last == cursor.Revision || pageIndex == 99 {
					x.Issues = append(x.Issues, "history query did not finish")
					break
				}
				cursor = &driver.Cursor{Incarnation: x.View.Incarnation, Revision: last}
			}
			principal, e := driver.PrincipalKey(f.profile.Admission.Actor)
			if e != nil {
				observe("import-principal", principal, e)
			} else {
				x.ImportReceipt, err = r.IdempotencyByKey(f.ctx, principal, f.profile.AgentLabel(), f.profile.Method, f.profile.IdempotencyKey)
				observe("import-receipt", x.ImportReceipt, err)
				x.ImportChangeset, err = r.ChangesetByKey(f.ctx, principal, f.profile.AgentLabel(), f.profile.Method, f.profile.IdempotencyKey)
				observe("import-changeset", x.ImportChangeset, err)
			}
			keys := make([]string, 0, len(f.scopes))
			for key := range f.scopes {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for index, key := range keys {
				scope := f.scopes[key]
				x.Receipts[key], err = r.IdempotencyByKey(f.ctx, scope.Principal, scope.Agent, scope.Method, scope.Key)
				observe(fmt.Sprintf("receipt-%03d", index), x.Receipts[key], err)
			}
			x.Archive, err = r.Blob(f.ctx, driver.Hash(f.profile.ArchiveDigest))
			x.ArchiveError = b13Error(err)
			if x.Meta.Head.IsZero() && errors.Is(err, driver.ErrNotFound) {
				// A real empty store has no retained original archive yet.
				if e := b13Write(filepath.Join(f.root, "images", label+"-calls", "archive.raw.json"), append(b13JSONRaw(struct {
					Value []byte
					Error error
				}{x.Archive, err}), '\n')); e != nil {
					x.Issues = append(x.Issues, e.Error())
				}
			} else {
				observe("archive", x.Archive, err)
			}
			observe("reader-close", nil, r.Close())
		}
	}
	f.mu.Lock()
	x.Posts = append([]b13Post{}, f.posts...)
	f.mu.Unlock()
	// Only collector aggregation is ordered here; every original query page
	// and raw returned value above remains separately available, unchanged.
	sort.Slice(x.Rows, func(i, j int) bool { return b13Ref(x.Rows[i].Ref) < b13Ref(x.Rows[j].Ref) })
	b13RawSave(f.t, f.root, "images/"+label+".raw.json", x)
	data, encodeErr := json.Marshal(x)
	if encodeErr == nil {
		b13Bytes(f.t, f.root, "images/"+label+".json", append(data, '\n'))
	} else {
		// A raw zero changeset in a legitimate no-op receipt is not itself a
		// public Commit. Preserve the literal encoding error, never fill arrays.
		b13Save(f.t, f.root, "images/"+label+"-public-encoding.json", map[string]any{"error": encodeErr.Error(), "primary_evidence": label + ".raw.json", "no_fields_rewritten": true})
	}
	return x
}

func (f *b13HTTPRun) validImage(x b13Image) {
	f.t.Helper()
	if len(x.Issues) != 0 {
		f.t.Fatalf("incomplete captured reader image: %s", strings.Join(x.Issues, "; "))
	}
	if x.View.At == nil || *x.View.At != x.Meta.Head || x.View.Incarnation != x.Meta.Incarnation || !x.View.Branch.IsZero() {
		f.t.Fatal("reader image identity/snapshot not pinned to its actual metadata")
	}
	head, err := x.Meta.Head.Uint64()
	if err != nil {
		f.t.Fatal(err)
	}
	rows := map[string]driver.Record{}
	for _, row := range x.Rows {
		key := b13Ref(row.Ref)
		if _, exists := rows[key]; exists || row.Kind == "" || row.Ref.Validate() != nil {
			f.t.Fatal("duplicate/invalid record census")
		}
		rows[key] = row
		versions, ok := x.Versions[key]
		if !ok || len(versions) == 0 || versions[len(versions)-1].Deleted || versions[len(versions)-1].Revision != row.Revision {
			f.t.Fatal("live row lost its actual latest version")
		}
	}
	if len(x.RecordKeys) != len(x.RecordStates) || len(x.RecordKeys) != len(x.Versions) || driver.ValidateLockKeys(x.RecordKeys) != nil {
		f.t.Fatal("incomplete known-record version/observation census")
	}
	for index, key := range x.RecordKeys {
		state := x.RecordStates[index]
		if key.Ref == nil || state.Key.Ref == nil || !reflect.DeepEqual(key, state.Key) || state.Derived != nil {
			f.t.Fatal("record observation key/payload mismatch")
		}
		row, live := rows[b13Ref(*key.Ref)]
		if state.Present != live || live && (state.Record == nil || !reflect.DeepEqual(*state.Record, row) || state.Revision != row.Revision) || !live && state.Record != nil {
			f.t.Fatal("point observation disagrees with complete record census")
		}
		versions := x.Versions[b13Ref(*key.Ref)]
		if len(versions) == 0 && (!state.Revision.IsZero() || state.Present) {
			f.t.Fatal("unversioned missing record has a nonzero observed revision or presence")
		}
		var previous uint64
		for _, version := range versions {
			revision, e := version.Revision.Uint64()
			if e != nil || revision == 0 || revision <= previous || revision > head || b13Ref(version.Ref) != b13Ref(*key.Ref) {
				f.t.Fatal("invalid/duplicate/unordered stored version")
			}
			previous = revision
		}
		if len(versions) > 0 && (versions[len(versions)-1].Revision != state.Revision || versions[len(versions)-1].Deleted == state.Present) {
			f.t.Fatal("latest version/tombstone observation mismatch")
		}
	}
	if len(x.Derived) != len(b13DerivedTables) || len(x.DerivedStates) != len(b13DerivedTables) {
		f.t.Fatal("missing/extra derived table census")
	}
	for _, table := range b13DerivedTables {
		values, ok := x.Derived[table]
		states, observed := x.DerivedStates[table]
		if !ok || !observed || len(values) != len(states) {
			f.t.Fatal("derived enumerated/observed row census differs")
		}
		prior := ""
		for index, row := range values {
			state := states[index]
			if row.Key == "" || index > 0 && row.Key <= prior || state.Key.Validate() != nil || state.Key.Kind != driver.LockDerived || state.Key.Derived == nil || state.Key.Derived.Table != table || state.Key.Derived.Key != row.Key || state.Record != nil || !state.Present || state.Derived == nil || state.Derived.Key != row.Key || !bytes.Equal(state.Derived.Value, row.Value) || row.Index != nil && !reflect.DeepEqual(row.Index, state.Derived.Index) {
				f.t.Fatal("derived key/value/index correspondence mismatch")
			}
			revision, e := state.Revision.Uint64()
			if e != nil || revision == 0 || revision > head {
				f.t.Fatal("derived observation has invalid/future revision")
			}
			prior = row.Key
		}
	}
	floor, _ := x.Meta.HistoryFloor.Uint64()
	previous := floor
	for _, entry := range x.Changes {
		revision, e := entry.Revision.Uint64()
		if e != nil || revision != previous+1 || revision > head || entry.Incarnation != x.Meta.Incarnation {
			f.t.Fatal("public changeset suffix is not complete, ordered and incarnation-bound")
		}
		previous = revision
	}
	if previous != head {
		f.t.Fatal("public changeset suffix does not reach actual head")
	}
	if head > 0 {
		hash, e := f.profile.RequestHash()
		receipt := x.ImportReceipt
		if e != nil || receipt == nil || receipt.ResponsePending || receipt.RequestHash != hash || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Incarnation != x.Meta.Incarnation || receipt.Changeset.Revision != x.Meta.HistoryFloor || receipt.Commit.Revision != x.Meta.HistoryFloor || receipt.Changeset.IdempotencyKey != f.profile.IdempotencyKey || x.ImportChangeset == nil || !reflect.DeepEqual(receipt.Changeset, *x.ImportChangeset) || !bytes.Equal(x.Archive, f.archive) || b13SHA(x.Archive) != f.profile.ArchiveDigest {
			f.t.Fatal("actual floor receipt/changeset/original archive binding changed")
		}
	}
}

func (f *b13HTTPRun) validProbe(p b13Probe) {
	f.t.Helper()
	f.validImage(p.Before)
	f.validImage(p.After)
	if p.TransportError != nil || p.Status != http.StatusOK || len(p.CaptureErrors) != 0 {
		f.t.Fatalf("actual HTTP/capture boundary %s: status=%d transport=%v capture=%v", p.Label, p.Status, p.TransportError, p.CaptureErrors)
	}
	if len(p.Returns) != 1 || p.Returns[0].Method != p.Method || !reflect.DeepEqual(p.Returns[0].Principal, b13Anonymous) {
		f.t.Fatalf("actual typed domain boundary missing/wrong for %s", p.Label)
	}
	request, err := wire.DecodeJSON(p.Request)
	response, e := wire.DecodeJSON(p.Response)
	req, requestOK := request.(map[string]any)
	res, responseOK := response.(map[string]any)
	if err != nil || e != nil || !requestOK || !responseOK || res["jsonrpc"] != "2.0" || !reflect.DeepEqual(req["id"], res["id"]) {
		f.t.Fatalf("HTTP exact request/response identity/strict JSON failed for %s", p.Label)
	}
}

func b13Result(p b13Probe) (map[string]any, error) {
	value, err := wire.DecodeJSON(p.Response)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s response is not an object", p.Label)
	}
	if failure, exists := object["error"]; exists {
		return nil, fmt.Errorf("%s actual public error: %s", p.Label, b13OrdinaryJSON(failure))
	}
	result, ok := object["result"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s lacks an object result", p.Label)
	}
	if len(p.Returns) != 1 || p.Returns[0].Error != nil {
		return nil, fmt.Errorf("%s success did not retain one successful actual typed return", p.Label)
	}
	return result, nil
}

func b13ExpectError(p b13Probe, code string) error {
	value, err := wire.DecodeJSON(p.Response)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return errors.New("error response is not an object")
	}
	failure, ok := object["error"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s expected actual %s refusal", p.Label, code)
	}
	data, ok := failure["data"].(map[string]any)
	if !ok || data["code"] != code {
		return fmt.Errorf("%s actual error code differs: %s", p.Label, b13OrdinaryJSON(failure))
	}
	if _, exists := object["result"]; exists {
		return errors.New("error response also contains result")
	}
	if len(p.Returns) != 1 || p.Returns[0].Error == nil {
		return errors.New("actual typed refusal return missing")
	}
	var typed *model.Error
	if !errors.As(p.Returns[0].Error, &typed) || string(typed.Data.Code) != code {
		return errors.New("typed domain error and public refusal differ")
	}
	return nil
}

func b13OrdinaryJSON(value any) string { data, _ := json.Marshal(value); return string(data) }

func b13ImportedMemories(t *testing.T, archive *dumpformat.Archive, image b13Image, expected map[string]string) map[string]string {
	t.Helper()
	if !bytes.Equal(image.Archive, archive.TarBytes()) || b13SHA(image.Archive) != archive.SHA256() {
		t.Fatal("imported memory source is not the exact retained native archive")
	}
	ids := map[string]string{}
	for _, source := range archive.Records("memory") {
		key, ok := source.Body["key"].(string)
		value, stringValue := source.Body["value"].(string)
		wanted, present := expected[key]
		if !ok || !stringValue || !present || value != wanted || source.ID != key || source.Source.Table != "config" || source.Source.Fields["key"] != "kv.memory."+key {
			t.Fatal("actual exported native memory corpus is not the complete unmodified string corpus; no coercion/filtering is permitted")
		}
		if _, duplicate := ids[key]; duplicate {
			t.Fatal("duplicate native memory key")
		}
		id := "source/memory/" + source.ID
		found := 0
		for _, row := range image.Rows {
			if row.Kind == "memory" && row.Ref.ID == id {
				found++
				// Bind the complete stored projection independently of the
				// later HTTP result's comparison to its stored metadata.
				want := pipeline.MemoryDocument{ID: id, Key: strings.TrimSpace(key), Content: value, Metadata: map[string]any{
					"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table,
					"plug.restore.source": source.Source.Fields, "plug.restore.archive_sha256": archive.SHA256(),
				}}
				encoded, err := json.Marshal(want)
				if err != nil || !bytes.Equal(row.Body, encoded) || row.Revision != image.Meta.Head {
					t.Fatalf("stored native memory projection/content/provenance/revision differs: %s: %v", id, err)
				}
			}
		}
		if found != 1 {
			t.Fatal("actual imported native memory ref/kind is missing or duplicated")
		}
		ids[key] = id
	}
	memories := 0
	for _, row := range image.Rows {
		if row.Kind == "memory" {
			memories++
		}
	}
	if len(ids) != len(expected) || memories != len(expected) {
		t.Fatal("native/imported complete memory census differs; no substituted public corpus")
	}
	return ids
}

func b13MemoryResult(result map[string]any, id, key, content string, image b13Image) error {
	if result["id"] != id || result["key"] != key || result["content"] != content {
		return fmt.Errorf("actual public memory ID/key/verbatim content differs for %q", key)
	}
	found := false
	for _, row := range image.Rows {
		if row.Kind != "memory" || row.Ref.ID != id {
			continue
		}
		found = true
		body, err := wire.DecodeJSON(row.Body)
		stored, ok := body.(map[string]any)
		if err != nil || !ok {
			return errors.New("actual memory row body is not strict JSON")
		}
		wanted := map[string]any{"id": id, "key": key, "content": content, "revision": b13DecodedValue(row.Revision)}
		if metadata, present := stored["metadata"]; present && metadata != nil {
			wanted["metadata"] = metadata
		}
		if !reflect.DeepEqual(result, wanted) {
			return errors.New("complete public memory fields/metadata/revision differ from actual stored row and expected native content")
		}
	}
	if !found {
		return errors.New("public memory has no actual stored memory row")
	}
	return nil
}

func b13MemoryItems(value any, ids, expected map[string]string, query string, image b13Image) error {
	items, ok := value.([]any)
	if !ok {
		return errors.New("public memories is not a nonnull array")
	}
	wanted := map[string]string{}
	for key, content := range expected {
		if query == "" || strings.Contains(strings.ToLower(key), strings.ToLower(query)) || strings.Contains(strings.ToLower(content), strings.ToLower(query)) {
			wanted[key] = content
		}
	}
	if len(items) != len(wanted) {
		return errors.New("actual public memory census/search differs or settings rows leaked")
	}
	seen := map[string]bool{}
	prior := ""
	for index, value := range items {
		memory, ok := value.(map[string]any)
		if !ok {
			return errors.New("public memory item is not an object")
		}
		key, keyOK := memory["key"].(string)
		content, present := wanted[key]
		if !keyOK || !present || seen[key] || index > 0 && key <= prior {
			return errors.New("public memory key/census/order is invalid")
		}
		if err := b13MemoryResult(memory, ids[key], key, content, image); err != nil {
			return err
		}
		seen[key] = true
		prior = key
	}
	return nil
}

func b13DecodedValue(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	decoded, err := wire.DecodeJSON(data)
	if err != nil {
		return nil
	}
	return decoded
}

func (f *b13HTTPRun) checkRefusal(p b13Probe, code string) {
	f.t.Helper()
	f.validProbe(p)
	if err := b13ExpectError(p, code); err != nil {
		f.t.Fatal(err)
	}
	outcome, ok := p.Returns[0].Value.(port.WriteOutcome)
	if !ok || !reflect.DeepEqual(outcome, port.WriteOutcome{}) || !reflect.DeepEqual(p.Before, p.After) {
		f.t.Fatalf("%s refusal did not preserve its zero/nil return and complete state/posts/receipt absence", p.Label)
	}
}

func (f *b13HTTPRun) checkReplay(first, replay b13Probe) {
	f.t.Helper()
	f.validProbe(replay)
	if _, err := b13Result(replay); err != nil {
		f.t.Fatal(err)
	}
	initial, ok := first.Returns[0].Value.(port.WriteOutcome)
	actual, valid := replay.Returns[0].Value.(port.WriteOutcome)
	if !ok || !valid || !actual.Replayed || actual.NullResult || !reflect.DeepEqual(first.Scope, replay.Scope) || !reflect.DeepEqual(first.Params, replay.Params) || !reflect.DeepEqual(first.Returns[0].Meta, replay.Returns[0].Meta) || !reflect.DeepEqual(initial.Commit, actual.Commit) || !reflect.DeepEqual(replay.Before, replay.After) {
		f.t.Fatal("exact HTTP replay changed response/state/history/receipts/subscribers")
	}
	decoded := b13Object(f.t, replay.Response)
	meta, ok := decoded["_meta"].(map[string]any)
	if !ok || meta["replayed"] != true || !reflect.DeepEqual(decoded["result"], b13DecodedValue(actual.Commit)) {
		f.t.Fatal("HTTP replay bit/result do not match actual transient return")
	}
}

func (f *b13HTTPRun) checkWrite(p b13Probe, key string, content *string, noop bool) {
	f.t.Helper()
	f.validProbe(p)
	result, err := b13Result(p)
	if err != nil {
		f.t.Fatal(err)
	}
	if p.Scope == nil || p.Before.Receipts[p.Scope.Key] != nil {
		f.t.Fatal("new HTTP write did not begin with its own absent receipt")
	}
	outcome, ok := p.Returns[0].Value.(port.WriteOutcome)
	if !ok || outcome.Replayed || outcome.NullResult || outcome.Commit.Validate() != nil || len(outcome.Commit.Results) != 1 || len(outcome.Commit.Warnings) != 0 || outcome.Commit.Warnings == nil || outcome.Commit.WouldCommit || outcome.Commit.Violations != nil || outcome.Commit.LeaseExpiresAt != nil || outcome.Commit.LeaseTtlMs != nil {
		f.t.Fatal("actual memory write is not the exact ordinary one-result/zero-warning commit")
	}
	actualResult := outcome.Commit.Results[0]
	ref := actualResult.Ref
	if !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
		f.t.Fatal("memory write returned invalid actual ref")
	}
	oldRows := map[string]driver.Record{}
	newRows := map[string]driver.Record{}
	for _, row := range p.Before.Rows {
		oldRows[b13Ref(row.Ref)] = row
	}
	for _, row := range p.After.Rows {
		newRows[b13Ref(row.Ref)] = row
	}
	old, exists := oldRows[b13Ref(ref)]
	actual, live := newRows[b13Ref(ref)]
	if exists && old.Kind != "memory" || live && actual.Kind != "memory" {
		f.t.Fatal("public memory operation targeted another record kind")
	}
	if content == nil && (!exists || live || noop) {
		f.t.Fatal("exact requested memory deletion was not observed")
	}
	if content != nil && !live {
		f.t.Fatal("actual requested memory row is absent after success")
	}
	priorBody := map[string]any{}
	if exists {
		priorBody = b13Object(f.t, old.Body)
		if priorBody["id"] != ref.ID || priorBody["key"] != key {
			f.t.Fatal("replacement/delete did not retain actual original ID/key")
		}
	}
	requestHash := b13MemoryRequestHash(f.t, p, key)
	if content != nil {
		expectedBody := map[string]any{"id": ref.ID, "key": key, "content": *content}
		for name, value := range priorBody {
			expectedBody[name] = value
		}
		expectedBody["content"] = *content
		// MemoryDocument omits an empty metadata map. The declared request
		// replaces metadata, while every other existing field is conserved.
		metadata, hasMetadata := p.Params["metadata"].(map[string]any)
		if hasMetadata && len(metadata) > 0 {
			expectedBody["metadata"] = metadata
		} else {
			delete(expectedBody, "metadata")
		}
		if !reflect.DeepEqual(b13Object(f.t, actual.Body), b13Decoded(f.t, expectedBody)) {
			f.t.Fatal("successful memory write changed undeclared body/source fields or verbatim content")
		}
	}
	head, e := p.Before.Meta.Head.Uint64()
	if e != nil {
		f.t.Fatal(e)
	}
	revision := model.Natural64(head + 1)
	if noop {
		revision = p.Before.Meta.Head
	}
	if p.After.Meta.Head != revision || outcome.Commit.Revision != revision {
		f.t.Fatal("memory success/noop advanced an unexpected head")
	}
	op := model.OpCreate
	from := model.Natural64(0)
	setBy := map[string]string{"key": "request", "content": "request", "metadata": "request"}
	paths := []string{"/content", "/key", "/metadata"}
	if exists {
		op = model.OpPatch
		from = old.Revision
		setBy = map[string]string{"content": "request", "metadata": "request"}
		paths = []string{"/content", "/metadata"}
	}
	if content == nil {
		op = model.OpDelete
		setBy = nil
		paths = []string{}
	}
	resultRevision := revision
	if noop {
		if !exists {
			f.t.Fatal("noop cannot create a memory")
		}
		resultRevision = old.Revision
	}
	expectedResult := model.OpResult{Ref: ref, Revision: resultRevision, SetBy: setBy}
	if content != nil {
		expectedResult.Record = bytes.Clone(actual.Body)
	}
	expectedCommit := model.Commit{Revision: revision, Results: []model.OpResult{expectedResult}, Warnings: []model.Warning{}}
	if !reflect.DeepEqual(expectedCommit, outcome.Commit) || !reflect.DeepEqual(result, b13Decoded(f.t, expectedCommit)) {
		f.t.Fatal("complete typed/HTTP memory commit differs from its independently expected response")
	}
	expected := p.Before
	expected.Receipts = map[string]*driver.IdempotencyRecord{}
	for scope, receipt := range p.Before.Receipts {
		expected.Receipts[scope] = receipt
	}
	if noop {
		if !bytes.Equal(actual.Body, old.Body) || actual.Revision != old.Revision {
			f.t.Fatal("fresh-key memory noop changed original row/body/revision")
		}
		expected.Receipts[p.Scope.Key] = &driver.IdempotencyRecord{RequestHash: requestHash, Commit: outcome.Commit}
		if !reflect.DeepEqual(expected, p.After) {
			f.t.Fatal("fresh-key noop changed anything besides its exact finalized receipt")
		}
		return
	}
	if len(p.After.Changes) != len(p.Before.Changes)+1 || !reflect.DeepEqual(p.After.Changes[:len(p.Before.Changes)], p.Before.Changes) {
		f.t.Fatal("successful memory write rewrote prior ordered history")
	}
	actualChange := p.After.Changes[len(p.Before.Changes)]
	if actualChange.Time.IsZero() {
		f.t.Fatal("successful memory write has no actual commit time")
	}
	_, offset := actualChange.Time.Zone()
	if offset != 0 {
		f.t.Fatal("actual commit time is not UTC")
	}
	expectedChange := model.Changeset{Revision: revision, Branch: p.Before.View.Branch, Incarnation: p.Before.Meta.Incarnation, Kind: model.ChangesetWrite, Time: actualChange.Time, Principal: b13Anonymous, IdempotencyKey: p.Scope.Key, Changes: []model.ChangeRecord{{Ref: ref, Op: op, FromRevision: from, ChangedPaths: paths}}}
	if !reflect.DeepEqual(actualChange, expectedChange) {
		f.t.Fatal("successful memory write changed undeclared changeset/principal/paths/legacy event fields")
	}
	expected.Meta.Head = revision
	at := revision
	expected.View.At = &at
	expected.Rows = []driver.Record{}
	for _, row := range p.Before.Rows {
		if b13Ref(row.Ref) != b13Ref(ref) {
			expected.Rows = append(expected.Rows, row)
		}
	}
	if content != nil {
		expected.Rows = append(expected.Rows, driver.Record{Ref: ref, Kind: "memory", Revision: revision, Body: bytes.Clone(actual.Body)})
	}
	sort.Slice(expected.Rows, func(i, j int) bool { return b13Ref(expected.Rows[i].Ref) < b13Ref(expected.Rows[j].Ref) })
	expected.Versions = map[string][]driver.Version{}
	for identity, versions := range p.Before.Versions {
		expected.Versions[identity] = versions
	}
	oldVersions := p.Before.Versions[b13Ref(ref)]
	version := driver.Version{Ref: ref, Revision: revision, CreatedAt: actualChange.Time, Principal: b13Anonymous, Deleted: content == nil}
	if content != nil {
		version.Base = len(oldVersions)%driver.VersionBaseEvery == 0 || len(oldVersions) > 0 && oldVersions[len(oldVersions)-1].Deleted
		if version.Base {
			version.Body = bytes.Clone(actual.Body)
		} else {
			version.Patch = b13JSON(f.t, []map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(actual.Body)}})
		}
	}
	expected.Versions[b13Ref(ref)] = append(append([]driver.Version{}, oldVersions...), version)
	expected.RecordKeys = append([]driver.LockKey{}, p.Before.RecordKeys...)
	if _, known := p.Before.Versions[b13Ref(ref)]; !known {
		r := ref
		expected.RecordKeys = append(expected.RecordKeys, driver.LockKey{Kind: driver.LockRecord, Ref: &r})
		sort.Slice(expected.RecordKeys, func(i, j int) bool { return b13Ref(*expected.RecordKeys[i].Ref) < b13Ref(*expected.RecordKeys[j].Ref) })
	}
	expected.RecordStates = make([]driver.LockState, len(expected.RecordKeys))
	states := map[string]driver.LockState{}
	for _, state := range p.Before.RecordStates {
		states[b13Ref(*state.Key.Ref)] = state
	}
	for index, lock := range expected.RecordKeys {
		state := states[b13Ref(*lock.Ref)]
		if b13Ref(*lock.Ref) == b13Ref(ref) {
			state = driver.LockState{Key: lock, Revision: revision, Present: content != nil}
			if content != nil {
				row := driver.Record{Ref: ref, Kind: "memory", Revision: revision, Body: bytes.Clone(actual.Body)}
				state.Record = &row
			}
		}
		expected.RecordStates[index] = state
	}
	expected.Changes = append(append([]model.Changeset{}, p.Before.Changes...), expectedChange)
	expected.Receipts[p.Scope.Key] = &driver.IdempotencyRecord{RequestHash: requestHash, Commit: outcome.Commit, Changeset: expectedChange}
	if len(p.After.Posts) != len(p.Before.Posts)+1 || !reflect.DeepEqual(p.After.Posts[:len(p.Before.Posts)], p.Before.Posts) {
		f.t.Fatal("successful memory write did not produce exactly one new complete subscriber body")
	}
	post := p.After.Posts[len(p.Before.Posts)]
	if post.Error != nil || post.View.At == nil || *post.View.At != revision || post.View.Incarnation != p.Before.Meta.Incarnation || !post.View.Branch.IsZero() || post.Change.RequestID != p.Scope.Key || post.Change.Revision != revision || !reflect.DeepEqual(post.Change.Principal, b13Anonymous) || !reflect.DeepEqual(post.Change.Meta, p.Returns[0].Meta) || len(post.Change.Changes) != 1 {
		f.t.Fatal("actual post callback snapshot/request/principal/meta binding differs")
	}
	change := post.Change.Changes[0]
	expectedOp := model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: ref}}
	postSetBy := setBy
	if content == nil {
		postSetBy = map[string]string{}
	} else {
		metadata, _ := p.Params["metadata"].(map[string]any)
		if exists {
			if metadata == nil {
				metadata = map[string]any{}
			}
			expectedOp = model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: map[string]any{"content": *content, "metadata": metadata}}}
		} else {
			// Generic JSON null in the actual callback is an untyped nil.
			// Do not mistake a typed nil map for that raw returned field.
			var metadataValue any
			if metadata != nil {
				metadataValue = metadata
			}
			expectedOp = model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "memory", Fields: map[string]any{"key": key, "content": *content, "metadata": metadataValue}}}
		}
	}
	if change.Resource.Kind != "memory" || !reflect.DeepEqual(change.Resource.Ref, ref) || !reflect.DeepEqual(change.Op, expectedOp) || !bytes.Equal(change.Before, old.Body) || !reflect.DeepEqual(change.SetBy, postSetBy) {
		f.t.Fatal("actual post callback target/operation/preimage/set-by differs")
	}
	if content == nil {
		if change.After != nil {
			f.t.Fatal("deleted memory post retains a live after body")
		}
	} else if !bytes.Equal(change.After, actual.Body) {
		f.t.Fatal("post callback body differs from actual stored memory")
	}
	if !b13SameRows(post.Rows, p.After.Rows) {
		f.t.Fatal("post callback did not observe the complete actual committed row image")
	}
	expected.Posts = append(append([]b13Post{}, p.Before.Posts...), post)
	// This final comparison includes every captured field. Only the exact
	// requested record, one version/head/history row/receipt and one validated
	// post body above have been replaced in the independently built expectation.
	if !reflect.DeepEqual(expected, p.After) {
		f.t.Fatalf("%s successful write failed global reader/version/history/derived/index/receipt/archive conservation", p.Label)
	}
}

func b13SameRows(first, second []driver.Record) bool {
	a := append([]driver.Record{}, first...)
	b := append([]driver.Record{}, second...)
	sort.Slice(a, func(i, j int) bool { return b13Ref(a[i].Ref) < b13Ref(a[j].Ref) })
	sort.Slice(b, func(i, j int) bool { return b13Ref(b[i].Ref) < b13Ref(b[j].Ref) })
	return reflect.DeepEqual(a, b)
}

func b13MemoryRequestHash(t *testing.T, p b13Probe, resolvedKey string) string {
	t.Helper()
	meta := model.WriteMeta{Method: p.Method, Project: "b13", Agent: b13Agent, AgentLabel: b13Agent, IdempotencyKey: p.Scope.Key, Return: "full"}
	if !reflect.DeepEqual(p.Returns[0].Meta, meta) {
		t.Fatal("typed memory write metadata differs from exact HTTP request")
	}
	var cs model.ChangeSet
	switch request := p.Returns[0].Request.(type) {
	case service.MemorySaveRequest:
		content, ok := p.Params["content"].(string)
		key, _ := p.Params["key"].(string)
		var metadata map[string]any
		if value, present := p.Params["metadata"]; present {
			var object bool
			metadata, object = value.(map[string]any)
			if !object {
				t.Fatal("successful memory.save metadata is not the exact request object")
			}
		}
		if p.Method != "memory.save" || !ok || request.Content != content || request.Key != key || !reflect.DeepEqual(request.Metadata, metadata) || request.Expect != nil {
			t.Fatal("typed memory.save did not preserve actual request fields")
		}
		cs = model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "memory", Fields: map[string]any{"key": resolvedKey, "content": content, "metadata": metadata}}}}}
	case service.MemoryDeleteRequest:
		id, ok := p.Params["id"].(string)
		if p.Method != "memory.delete" || !ok || request.ID.ID != id || !request.ID.IsID() || !request.ID.Branch.IsZero() || request.Expect != nil {
			t.Fatal("typed memory.delete did not preserve actual request ref")
		}
		cs = model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: request.ID}}}}
	default:
		t.Fatal("unexpected actual memory write request type")
	}
	// Independently reproduce the frozen ordinary request identity; no hash
	// or normalized operation is borrowed from the durable receipt itself.
	data := b13JSON(t, struct {
		ChangeSet               model.ChangeSet
		Method, Project, Return string
		ClaimNextIdentity       string `json:",omitempty"`
		DryRun                  bool
	}{cs, p.Method, "b13", "full", "", false})
	return b13SHA(data)
}

func (f *b13HTTPRun) reverse(label string, basis b13Image) {
	f.t.Helper()
	value, err := converterroundtrip.ReverseRestored(f.ctx, f.store)
	b13RawSave(f.t, f.root, "reverse/"+label+"-return.raw.json", struct {
		Value converterroundtrip.LiveReverse
		Error error
	}{value, err})
	// Save actual byte/report output before interpreting success/refusal.
	b13Bytes(f.t, f.root, "reverse/"+label+".jsonl", value.Result.JSONL)
	b13RawSave(f.t, f.root, "reverse/"+label+"-report.raw.json", value.Result.Report)
	after := f.image("reverse-" + label + "-after")
	f.validImage(after)
	if !reflect.DeepEqual(basis, after) {
		f.t.Fatal("read-only reverse conversion changed complete state")
	}
	if label != "imported-live" {
		// The frozen reverse seam declares changed/new/deleted source memories
		// unsupported. Preserve that typed refusal; never export stale archive
		// values or count this branch as successful live-memory reverse parity.
		var refusal converterroundtrip.ReverseRefusal
		if !errors.As(err, &refusal) || (refusal.Reason != "changed_source_mirror" && refusal.Reason != "missing_source_record" && refusal.Reason != "unmapped_live_record") || value.Result.JSONL != nil {
			f.t.Fatalf("post-write reverse must preserve its literal unsupported-memory refusal: %v", err)
		}
		b13Save(f.t, f.root, "reverse/"+label+"-disposition.json", map[string]any{"status": "TYPED_REFUSAL_NOT_LIVE_MEMORY_REVERSE_PARITY", "reason": refusal.Reason, "source_archive_unchanged": true})
		return
	}
	if err != nil {
		f.t.Fatalf("initial unchanged native corpus reverse refused: %v", err)
	}
	archive, e := dumpformat.Read(bytes.NewReader(f.archive))
	if e != nil {
		f.t.Fatal(e)
	}
	memories := map[string]any{}
	for _, line := range bytes.Split(value.Result.JSONL, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		row := b13Object(f.t, line)
		if row["_type"] != "memory" {
			continue
		}
		key, ok := row["key"].(string)
		if !ok || key == "" {
			f.t.Fatal("reverse memory key is invalid")
		}
		if _, duplicate := memories[key]; duplicate {
			f.t.Fatal("duplicate reverse memory key")
		}
		memories[key] = row["value"]
	}
	for _, source := range archive.Records("memory") {
		if !reflect.DeepEqual(memories[source.ID], b13DecodedValue(source.Body["value"])) {
			f.t.Fatal("unchanged native memory value/source lineage was lost in actual reverse")
		}
	}
	if value.Snapshot.At == nil || *value.Snapshot.At != basis.Meta.Head || value.Snapshot.Incarnation != basis.Meta.Incarnation {
		f.t.Fatal("initial reverse did not use actual imported snapshot")
	}
	// Generic config conversion is separately visible in the retained complete
	// report/JSONL. Its namespaced compatibility memories grant no public-list
	// settings-isolation credit; HTTP must expose only the actual memory kind.
}
