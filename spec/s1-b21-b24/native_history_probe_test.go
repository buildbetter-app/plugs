package s1b21b24

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

type nativeHistoryOffObservation struct {
	Target    string                          `json:"target"`
	Settings  []nativePublicConfigObservation `json:"settings"`
	WokenRow  map[string]any                  `json:"woken_row"`
	Columns   []map[string]any                `json:"source_columns"`
	IssueRows int                             `json:"source_issue_rows"`
}

func nativeHistoryOffSourceRow(t *testing.T, census map[string]any, target string) map[string]any {
	t.Helper()
	rows, ok := census["issues"].([]map[string]any)
	if !ok {
		t.Fatal("history-off source census has no full physical issue rows")
	}
	var found map[string]any
	count := 0
	for _, row := range rows {
		if row["id"] == target {
			found = row
			count++
		}
	}
	if target == "" || count != 1 {
		t.Fatalf("actual history-off allocated ID has %d full physical source rows", count)
	}
	return found
}

// The public SQL command returns JSON cells as strings; the archive carries
// their decoded JSON values. Use independently read physical column types,
// never archive keys or values, and retain the unmodified raw row separately.
func nativePhysicalSourceFields(raw map[string]any, columns []map[string]any) (map[string]any, error) {
	if len(raw) == 0 || len(raw) != len(columns) {
		return nil, fmt.Errorf("physical source columns and complete row differ")
	}
	fields := map[string]any{}
	for _, column := range columns {
		name, nameOK := column["column_name"].(string)
		dataType, typeOK := column["data_type"].(string)
		value, present := raw[name]
		if _, duplicate := fields[name]; !nameOK || name == "" || !typeOK || dataType == "" || !present || duplicate {
			return nil, fmt.Errorf("missing, duplicate or invalid actual physical source column %q", name)
		}
		if value != nil && (strings.Contains(strings.ToUpper(dataType), "BLOB") || strings.Contains(strings.ToUpper(dataType), "BINARY")) {
			return nil, fmt.Errorf("physical binary column %q requires a lossless byte observation", name)
		}
		if value != nil && strings.EqualFold(dataType, "json") {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("actual physical JSON column %q is not SQL text", name)
			}
			decoder := json.NewDecoder(strings.NewReader(text))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				return nil, fmt.Errorf("physical JSON column %q: %w", name, err)
			}
			var trailing any
			if err := decoder.Decode(&trailing); err != io.EOF {
				return nil, fmt.Errorf("physical JSON column %q has trailing or invalid data", name)
			}
		}
		fields[name] = value
	}
	return fields, nil
}

// The pinned c8eb3ba7 exporter independently specifies these exclusions in
// internal/plugdump/map.go:200-201 and build.go:100-101. Preserve the complete
// physical image; project only those fixed fields after checking the actual
// manifest against the independently counted, unfiltered source issue rows.
func nativeHistoryOffSourceCorrespondence(observed nativeHistoryOffObservation, sources []dumpformat.Disposition, table string, archived map[string]any) error {
	fields, err := nativePhysicalSourceFields(observed.WokenRow, observed.Columns)
	if err != nil {
		return err
	}
	if observed.IssueRows < 1 || table != "issues" {
		return fmt.Errorf("history-off source table or physical issue count is invalid")
	}
	policies := map[string]dumpformat.Disposition{
		"issues":            {Family: "issues", Rows: observed.IssueRows, Action: "carry", Reason: "head state and source provenance"},
		"issues.row_lock":   {Family: "issues.row_lock", Rows: observed.IssueRows, Action: "manifest/rebuild", Reason: "transient locks excluded"},
		"issues.is_blocked": {Family: "issues.is_blocked", Rows: observed.IssueRows, Action: "manifest/rebuild", Reason: "derived blocking excluded; Plug must recompute"},
	}
	seen := map[string]bool{}
	for _, source := range sources {
		if source.Family != "issues" && !strings.HasPrefix(source.Family, "issues.") {
			continue
		}
		want, known := policies[source.Family]
		if !known || seen[source.Family] || source != want {
			return fmt.Errorf("unexpected, duplicate or changed issue source policy: %+v", source)
		}
		seen[source.Family] = true
	}
	if len(seen) != len(policies) {
		return fmt.Errorf("history-off archive omitted an exact issue source policy")
	}
	for _, name := range []string{"row_lock", "is_blocked"} {
		if _, present := fields[name]; !present {
			return fmt.Errorf("physical source omitted excluded column %q", name)
		}
		delete(fields, name)
	}
	if !nativeEqualJSON(fields, archived) {
		return fmt.Errorf("history-off archive exported source row differs in keys, values or null presence: expected=%v archived=%v", fields, archived)
	}
	return nil
}

func nativeAssertHistoryOffArchive(t *testing.T, archive *dumpformat.Archive, observed nativeHistoryOffObservation) dumpformat.Record {
	t.Helper()
	if observed.Target == "" || observed.WokenRow["id"] != observed.Target || observed.WokenRow["issue_type"] != "task" || observed.WokenRow["status"] != "open" || observed.WokenRow["defer_until"] != nil {
		t.Fatal("history-off archive requires the actual successful source observation")
	}
	settingsCount := 0
	for _, setting := range observed.Settings {
		if setting.Key == "versioned-history.enabled" {
			settingsCount++
			if setting.Value != "false" {
				t.Fatal("actual source observation did not disable history")
			}
		}
	}
	if settingsCount != 1 {
		t.Fatal("actual source observation lost the unique history setting")
	}
	var found dumpformat.Record
	count := 0
	for _, record := range archive.Records("bead") {
		if record.ID != observed.Target {
			continue
		}
		count++
		found = record
		if err := nativeHistoryOffSourceCorrespondence(observed, archive.Manifest().Sources, record.Source.Table, record.Source.Fields); err != nil {
			t.Fatal(err)
		}
	}
	if count != 1 {
		t.Fatalf("history-off task must occur once in the complete archive, got %d", count)
	}
	configCount := 0
	for _, record := range archive.Records("config") {
		if record.ID == "config/versioned-history.enabled" {
			configCount++
			if record.Source.Table != "config" || record.Source.Fields["key"] != "versioned-history.enabled" || record.Source.Fields["value"] != "false" || record.Body["value"] != "false" {
				t.Fatal("history-off archive lost the actual disabled source setting")
			}
		}
	}
	if configCount != 1 {
		t.Fatalf("history-off archive disabled setting count=%d", configCount)
	}
	return found
}

// Observe the actual driver.Reader boundary. Delegation is unconditional,
// including failed reads; results and errors are retained without substitution.
// This does not observe SQL internal to a driver or the separate native engine.
type nativeReadCall struct {
	Operation     string          `json:"operation"`
	Request       json.RawMessage `json:"request"`
	Response      json.RawMessage `json:"response"`
	Error         string          `json:"error,omitempty"`
	EvidenceError string          `json:"evidence_error,omitempty"`
}

type nativeReadTrace struct {
	mu    sync.Mutex
	calls []nativeReadCall
}

func (trace *nativeReadTrace) record(operation string, request, response any, err error) {
	call := nativeReadCall{Operation: operation}
	var requestErr, responseErr error
	call.Request, requestErr = json.Marshal(request)
	call.Response, responseErr = json.Marshal(response)
	if requestErr != nil || responseErr != nil {
		call.EvidenceError = fmt.Sprintf("request=%v response=%v", requestErr, responseErr)
	}
	if err != nil {
		call.Error = err.Error()
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.calls = append(trace.calls, call)
}

func (trace *nativeReadTrace) snapshot() []nativeReadCall {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return append([]nativeReadCall(nil), trace.calls...)
}

type nativeObservedStore struct {
	driver.Store
	trace *nativeReadTrace
}

func (store *nativeObservedStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := store.Store.Read(ctx, view)
	var snapshot any
	if err == nil {
		snapshot = reader.Snapshot()
	}
	store.trace.record("Read", view, snapshot, err)
	if err != nil {
		return nil, err
	}
	return &nativeObservedReader{Reader: reader, trace: store.trace}, nil
}

type nativeObservedReader struct {
	driver.Reader
	trace *nativeReadTrace
}

func (reader *nativeObservedReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	rows, err := reader.Reader.Get(ctx, refs)
	reader.trace.record("Get", refs, rows, err)
	return rows, err
}

func (reader *nativeObservedReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	page, err := reader.Reader.Query(ctx, query)
	reader.trace.record("Query", query, page, err)
	return page, err
}

func (reader *nativeObservedReader) Versions(ctx context.Context, ref model.Ref, request driver.PageReq) (driver.Page[driver.Version], error) {
	page, err := reader.Reader.Versions(ctx, ref, request)
	reader.trace.record("Versions", map[string]any{"ref": ref, "page": request}, page, err)
	return page, err
}

func nativeAssertReadyReadTrace(t *testing.T, calls []nativeReadCall) {
	t.Helper()
	reads, queries := 0, 0
	for _, call := range calls {
		if call.Error != "" || call.EvidenceError != "" {
			t.Fatalf("imported ready read failed: %+v", call)
		}
		switch call.Operation {
		case "Read":
			reads++
		case "Query":
			queries++
		case "Versions":
			t.Fatalf("imported ready loaded version rows: %+v", call)
		}
	}
	if reads == 0 || queries == 0 {
		t.Fatal("zero version reads requires an observed real snapshot and row scan")
	}
}

func nativeVersionReadControl(t *testing.T, f *fixture, ref model.Ref) []nativeReadCall {
	t.Helper()
	trace := &nativeReadTrace{}
	store := &nativeObservedStore{Store: f.store, trace: trace}
	meta, err := store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
	if err != nil || len(page.Items) == 0 {
		t.Fatalf("real version-read control returned no persisted version: %+v %v", page, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// A failed attempt must also be visible. This reaches the same real closed
	// reader, rather than a fake callback that returns a chosen error.
	if _, err := reader.Versions(f.ctx, ref, driver.PageReq{Limit: 1000}); err == nil {
		t.Fatal("closed-reader version control unexpectedly succeeded")
	}
	calls := trace.snapshot()
	if len(calls) != 3 || calls[1].Operation != "Versions" || calls[1].Error != "" || calls[2].Operation != "Versions" || calls[2].Error == "" {
		t.Fatalf("version-read observer lost a successful or failed actual call: %+v", calls)
	}
	for _, call := range calls {
		if call.EvidenceError != "" {
			t.Fatal(call.EvidenceError)
		}
	}
	return calls
}

func nativeImportedClock(t *testing.T, f *fixture) map[string]any {
	t.Helper()
	goTime := f.clock.Now()
	evidence := map[string]any{"go_utc": goTime, "engine": f.engine, "sql_clock_applicable": f.engine == "postgres"}
	if f.engine == "postgres" {
		sqlTime := testclock.PostgresNow(t, f.dsn)
		if !sqlTime.Equal(goTime) {
			t.Fatalf("actual imported SQL/Go clock mismatch: SQL=%s Go=%s", sqlTime, goTime)
		}
		evidence["sql_utc"] = sqlTime
		evidence["exact_sql_go_equality"] = true
	} else {
		evidence["durable_clock"] = "driver uses the injected Go clock; no production SQL clock expression"
	}
	evidence["native_dolt_clock_claim"] = "UNQUALIFIED: independent native wall-clock samples are not exact equality"
	return evidence
}

func nativeImportedHistoryOffParity(t *testing.T, root string, f *fixture, observer *nativePostObserver, denied *atomic.Bool, phase string, source dumpformat.Record, observed nativeHistoryOffObservation) {
	t.Helper()
	if !t.Run("B24-history-off-"+phase, func(t *testing.T) {
		ref := model.RefByID(observed.Target, model.Natural64(0))
		doc := nativeDocument(t, f, ref)
		var sourceFields map[string]any
		if err := json.Unmarshal(doc.Metadata["beads.source_fields"], &sourceFields); err != nil || !nativeEqualJSON(sourceFields, source.Source.Fields) || doc.Fields["status"] != "todo" {
			t.Fatalf("history-off task import lost actual source identity/image or open status: %s %v", observed.Target, err)
		}
		configCount := 0
		for _, record := range nativeImportedState(t, f).Records {
			if record.Kind != "config" {
				continue
			}
			var body map[string]any
			if err := json.Unmarshal(record.Body, &body); err != nil {
				t.Fatal(err)
			}
			if body["source_id"] == "config/versioned-history.enabled" {
				configCount++
				if body["source_table"] != "config" || body["value"] != "false" {
					t.Fatal("imported history-off source setting changed")
				}
			}
		}
		if configCount != 1 {
			t.Fatalf("actual imported disabled-setting count=%d", configCount)
		}
		probe := nativeImportedReadyProbe(t, f, observer, denied, "history-off-"+phase, ref, true)
		probe["source"] = source
		probe["observation"] = observed
		probe["document"] = doc
		probe["physical_sql_reopen"] = phase == "reopen" && f.engine != "memory"
		probe["history_mode"] = "snapshot: restore versions are not native source history"
		nativeWriteJSON(t, filepath.Join(root, f.engine+"-history-off-"+phase+".json"), probe)
	}) {
		t.Fatal("actual history-off imported task parity failed")
	}
}

func nativeImportedReadyProbe(t *testing.T, f *fixture, observer *nativePostObserver, denied *atomic.Bool, label string, ref model.Ref, wantReady bool) map[string]any {
	t.Helper()
	control := nativeVersionReadControl(t, f, ref)
	trace := &nativeReadTrace{}
	nativeBindImported(t, f, observer, denied, trace)
	before := nativeImportedState(t, f)
	posts := observer.count()
	responses := []reply{}
	reads := [][]nativeReadCall{}
	for _, attempt := range []string{"first", "second"} {
		start := len(trace.snapshot())
		response := f.call("bead.ready", label+"-"+attempt, map[string]any{"limit": 100, "filter": map[string]any{"resources": []string{ref.ID}}})
		ids, cursor := nativeViewIDs(t, response)
		if cursor != "" || wantReady && !reflect.DeepEqual(ids, []string{ref.ID}) || !wantReady && len(ids) != 0 {
			t.Fatalf("actual imported history-off ready %s/%s: ids=%v cursor=%s want_ready=%t", label, attempt, ids, cursor, wantReady)
		}
		calls := trace.snapshot()[start:]
		nativeAssertReadyReadTrace(t, calls)
		reads = append(reads, calls)
		responses = append(responses, response)
		sameState(t, before, nativeImportedState(t, f))
		if observer.count() != posts {
			t.Fatal("imported history-off ready ran a post subscriber")
		}
	}
	return map[string]any{
		"target": ref, "ready": wantReady, "responses": responses,
		"version_read_controls": control, "ready_reads": reads,
		"before": nativeRecordState(t, before), "after": nativeRecordState(t, nativeImportedState(t, f)),
		"clock": nativeImportedClock(t, f), "all_planes_equal": true, "second_ready_noop": true,
		"read_observation_scope": "real imported driver.Reader calls; no native SQL-read claim",
	}
}

func nativeAdvanceImportedClock(t *testing.T, f *fixture, observer *nativePostObserver, denied *atomic.Bool, delta time.Duration) {
	t.Helper()
	observer.stopHostLease(t)
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	f.clock.Advance(t, delta)
	nativeBindImported(t, f, observer, denied)
}

// A named defer with a reason lowers to an ordered patch and comment. Both
// OpResults carry the same final record at one durable revision.
func nativeAssertDeferWithReason(t *testing.T, ref model.Ref, reason string, got model.Commit, before, after storedState) {
	t.Helper()
	oldHead, err := before.Meta.Head.Uint64()
	if err != nil || len(got.Results) != 2 || got.Revision != model.Natural64(oldHead+1) || after.Meta.Head != got.Revision {
		t.Fatal("defer with reason must return two ordered results at one new revision")
	}
	// Omitted branch and explicit zero select the same typed reference.
	sameRef := func(actual model.Ref) bool {
		return actual.Validate() == nil && ref.Validate() == nil && actual.IsID() && ref.IsID() && actual.ID == ref.ID && actual.Branch.String() == ref.Branch.String()
	}
	find := func(state storedState) driver.Record {
		var found driver.Record
		count := 0
		for _, row := range state.Records {
			if sameRef(row.Ref) {
				found, count = row, count+1
			}
		}
		if count != 1 {
			t.Fatalf("defer target must have one stored record, got %d", count)
		}
		return found
	}
	oldRecord, record := find(before), find(after)
	for i, operation := range []string{"patch", "comment"} {
		result := got.Results[i]
		if !sameRef(result.Ref) || result.Revision != got.Revision || len(result.Record) == 0 || string(result.Record) != string(record.Body) {
			t.Fatalf("defer %s result lost its exact target, revision or final record", operation)
		}
	}
	oldDocument, err := pipeline.DecodeDocument(oldRecord.Body)
	if err != nil {
		t.Fatal(err)
	}
	document, err := pipeline.DecodeDocument(record.Body)
	if err != nil {
		t.Fatal(err)
	}
	if document.Fields["status"] != "backlog" || len(document.Comments) != len(oldDocument.Comments)+1 || document.Comments[len(oldDocument.Comments)].Body != reason {
		t.Fatal("defer must enter backlog and append exactly the requested reason once")
	}
	if len(oldDocument.Comments) != 0 && !reflect.DeepEqual(oldDocument.Comments, document.Comments[:len(oldDocument.Comments)]) || !reflect.DeepEqual(oldDocument.Metadata, document.Metadata) {
		t.Fatal("defer changed prior comments or complete source metadata")
	}
	if len(after.Records) != len(before.Records) || len(after.Versions[ref.ID]) != len(before.Versions[ref.ID])+1 || len(after.Changesets) != len(before.Changesets)+1 || !reflect.DeepEqual(before.Changesets, after.Changesets[:len(before.Changesets)]) {
		t.Fatal("defer must preserve prior history and add one target version and changeset")
	}
	change := after.Changesets[len(before.Changesets)]
	if change.Revision != got.Revision || len(change.Changes) != 1 || !sameRef(change.Changes[0].Ref) {
		t.Fatal("defer patch and reason must publish one changed target at the commit revision")
	}
}

func nativeImportedHistoryOffLifecycle(t *testing.T, root string, f *fixture, observer *nativePostObserver, denied *atomic.Bool, source dumpformat.Record, observed nativeHistoryOffObservation) {
	t.Helper()
	if !t.Run("B24-history-off-imported-defer-clock-and-reopen", func(t *testing.T) {
		ref := model.RefByID(observed.Target, model.Natural64(0))
		nativeRefuse(t, f, observer, "bead.defer", "history-off-imported-blank-reason", map[string]any{"id": ref, "reason": " "}, "invalid_params")
		label := "history-off-imported-defer"
		before := nativeImportedState(t, f)
		first := f.call("bead.defer", label, map[string]any{"id": ref, "reason": "explicit imported history-off waiting"})
		deferred := commit(t, first)
		state := nativeImportedState(t, f)
		nativeAssertDeferWithReason(t, ref, "explicit imported history-off waiting", deferred, before, state)
		versions := state.Versions[ref.ID]
		if len(versions) == 0 || !versions[len(versions)-1].CreatedAt.Equal(f.clock.Now()) {
			t.Fatal("imported deferral persisted a different time from its Go clock")
		}
		receiptKey := "bead.defer/" + key(label)
		if state.Receipts[receiptKey] == nil {
			t.Fatal("actual imported deferral has no retained visible receipt")
		}
		origin := f.clock.Now()
		tick := time.Nanosecond
		if f.engine == "postgres" {
			tick = time.Microsecond
		}
		phases := []map[string]any{}
		for _, boundary := range []struct {
			name    string
			delta   time.Duration
			elapsed time.Duration
			visible bool
		}{
			{"before", driver.IdempotencyWindow - tick, driver.IdempotencyWindow - tick, true},
			{"exactly", tick, driver.IdempotencyWindow, false},
			{"after", tick, driver.IdempotencyWindow + tick, false},
		} {
			nativeAdvanceImportedClock(t, f, observer, denied, boundary.delta)
			current := nativeImportedState(t, f)
			if f.clock.Now().Sub(origin) != boundary.elapsed || (current.Receipts[receiptKey] != nil) != boundary.visible {
				t.Fatalf("imported deferral receipt boundary %s: now=%s visible=%t", boundary.name, f.clock.Now(), current.Receipts[receiptKey] != nil)
			}
			durableBefore, durableAfter := state, current
			durableBefore.Receipts, durableAfter.Receipts = nil, nil
			sameState(t, durableBefore, durableAfter)
			probe := nativeImportedReadyProbe(t, f, observer, denied, "history-off-clock-"+boundary.name, ref, false)
			probe["phase"], probe["elapsed"], probe["receipt_visible"] = boundary.name, boundary.elapsed.String(), boundary.visible
			phases = append(phases, probe)
		}
		beforeReopen := nativeImportedState(t, f)
		nativeReopenImported(t, f, observer, denied)
		sameState(t, beforeReopen, nativeImportedState(t, f))
		reopened := f.call("bead.reopen", "history-off-imported-reopen", map[string]any{"id": ref, "reason": "explicit reopen of actual imported task"})
		commit(t, reopened)
		if nativeDocument(t, f, ref).Fields["status"] != "todo" {
			t.Fatal("actual history-off imported task was not explicitly reopened")
		}
		final := nativeImportedReadyProbe(t, f, observer, denied, "history-off-explicit-reopen", ref, true)
		nativeWriteJSON(t, filepath.Join(root, f.engine+"-history-off-lifecycle.json"), map[string]any{
			"source": source, "observation": observed, "origin": origin,
			"deferral_response": first, "deferral_state": nativeRecordState(t, state),
			"receipt_boundaries": phases, "explicit_reopen_response": reopened, "final": final,
			"adopted": "untimed defer and explicit reopen; no native timed-clock equivalence",
		})
	}) {
		t.Fatal("actual imported history-off lifecycle failed")
	}
}

// Ordinary gates compile every native source file and exercise this observer
// against actual stores. These controls are not a public native recording.
func TestNativeB24ProbeObservationAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref, _ := f.create(t, "native-probe-control", "actual read-observer control")
			control := nativeVersionReadControl(t, f, ref)
			before := f.state(t)
			request := service.ReadyRequest{Project: project, Limit: 100}
			plain, err := f.service.Ready(f.ctx, anonymous, request)
			if err != nil {
				t.Fatal(err)
			}
			trace := &nativeReadTrace{}
			store := &nativeObservedStore{Store: f.store, trace: trace}
			observed := service.New(store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.clock.Now))
			actual, err := observed.Ready(f.ctx, anonymous, request)
			if err != nil || !reflect.DeepEqual(plain, actual) || len(actual.Items) != 1 || actual.Items[0].ID != ref.ID {
				t.Fatalf("real ready delegation differs from ordinary service: %+v %+v %v", plain, actual, err)
			}
			nativeAssertReadyReadTrace(t, trace.snapshot())
			sameState(t, before, f.state(t))
			clock := nativeImportedClock(t, f)
			t.Logf("B24_PROBE_CONTROL engine=%s actual_version_calls=2 successful=1 failed=1 ready_versions=0 state_equal=true clock=%v control=%v", engine, clock, control)
		})
		t.Run("defer-with-reason/"+engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref, _ := f.create(t, "defer-result-control", "actual defer result control")
			before := nativeImportedState(t, f)
			params := map[string]any{"id": ref, "reason": "explicit defer result control"}
			first := f.call("bead.defer", "defer-result-control", params)
			got := commit(t, first)
			after := nativeImportedState(t, f)
			if first.Meta.Replayed {
				t.Fatal("first defer result control was a replay")
			}
			nativeAssertDeferWithReason(t, ref, "explicit defer result control", got, before, after)
			replayed := f.call("bead.defer", "defer-result-control", params)
			if !replayed.Meta.Replayed || !reflect.DeepEqual(got, commit(t, replayed)) {
				t.Fatal("defer replay changed the ordered two-result commit")
			}
			sameState(t, after, nativeImportedState(t, f))
		})
	}
	t.Run("physical-source-json-types-and-null-presence", func(t *testing.T) {
		raw := map[string]any{"id": "actual-row-control", "metadata": `{"n":9007199254740993}`, "title": `{"n":1}`, "defer_until": nil}
		columns := []map[string]any{
			{"column_name": "id", "data_type": "varchar"},
			{"column_name": "metadata", "data_type": "json"},
			{"column_name": "title", "data_type": "text"},
			{"column_name": "defer_until", "data_type": "datetime"},
		}
		fields, err := nativePhysicalSourceFields(raw, columns)
		want := map[string]any{"id": raw["id"], "metadata": map[string]any{"n": json.Number("9007199254740993")}, "title": raw["title"], "defer_until": nil}
		if err != nil || !nativeEqualJSON(fields, want) || raw["metadata"] != `{"n":9007199254740993}` {
			t.Fatalf("physical JSON conversion lost precision, text typing, null presence or raw source: %v %v", fields, err)
		}
		delete(raw, "defer_until")
		if _, err := nativePhysicalSourceFields(raw, columns); err == nil {
			t.Fatal("missing physical column was accepted as explicit null")
		}
		raw["defer_until"] = nil
		columns[3] = columns[2]
		if _, err := nativePhysicalSourceFields(raw, columns); err == nil {
			t.Fatal("duplicate physical column hid the absent column")
		}
	})
	t.Run("reverse-source-null-representation", func(t *testing.T) {
		declared := []string{"actor", "compaction_level", "current_revision", "no_history", "participation_generation", "sender"}
		control := func() ([]byte, []dumpformat.Record) {
			projection := nativeSemanticProjection{
				Issues: map[string]map[string]any{"source-control": {
					"title": "unchanged title", "metadata": map[string]any{"user-null": nil, "user-text": "unchanged"},
				}},
				Memories: map[string]any{"user-memory": "unchanged"}, Edges: []map[string]any{}, Comments: []map[string]any{},
			}
			ordinary, err := json.Marshal(projection)
			if err != nil {
				t.Fatal(err)
			}
			fields := dumpformat.Row{
				"actor": nil, "compaction_level": json.Number("0"), "current_revision": json.Number("9007199254740993"),
				"no_history": false, "participation_generation": nil, "sender": "", "status": "open", "pinned": false,
			}
			for _, field := range dumpformat.OpaqueIssueFields() {
				fields[field] = nil
			}
			return ordinary, []dumpformat.Record{{
				Kind: "bead", ID: "source-control", Body: dumpformat.Row{"status_category": "unstarted"},
				Source: dumpformat.Provenance{Table: "issues", Fields: fields},
			}}
		}
		check := func(t *testing.T, ordinary []byte, records []dumpformat.Record, archiveText string) {
			t.Helper()
			before, err := json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := nativeProjectPublicReverseExpectation(ordinary, records, declared)
			if err != nil {
				t.Fatal(err)
			}
			want, err := nativeDecodePublicProjection(ordinary)
			if err != nil {
				t.Fatal(err)
			}
			metadata := want.Issues["source-control"]["metadata"].(map[string]any)
			for _, field := range dumpformat.OpaqueIssueFields() {
				metadata["beads."+field] = nil
			}
			if archiveText != "" {
				metadata["_plug_source_fields"] = archiveText
			}
			got, err := nativeDecodePublicProjection(actual)
			if err != nil || !nativeEqualJSON(got, want) {
				t.Fatalf("reverse representation changed keys, nulls, values or number precision: %s %v", actual, err)
			}
			after, err := json.Marshal(records)
			if err != nil || string(before) != string(after) {
				t.Fatal("reverse expectation changed complete source provenance")
			}
		}
		t.Run("null-with-zero-false-empty-and-large-number", func(t *testing.T) {
			ordinary, records := control()
			check(t, ordinary, records, `{"compaction_level":0,"current_revision":9007199254740993,"no_history":false,"sender":""}`)
		})
		for _, textualPinned := range []bool{false, true} {
			t.Run(fmt.Sprintf("all-null-textual-pinned-%t", textualPinned), func(t *testing.T) {
				ordinary, records := control()
				for _, field := range declared {
					records[0].Source.Fields[field] = nil
				}
				archiveText := ""
				if textualPinned {
					records[0].Source.Fields["pinned"] = "0"
					archiveText = `{"pinned":"0"}`
				}
				check(t, ordinary, records, archiveText)
			})
		}
		for _, field := range declared {
			t.Run("missing-declared-"+field, func(t *testing.T) {
				ordinary, records := control()
				delete(records[0].Source.Fields, field)
				if _, err := nativeProjectPublicReverseExpectation(ordinary, records, declared); err == nil {
					t.Fatal("absent source field was accepted as explicit null")
				}
			})
		}
		t.Run("missing-opaque-source-field", func(t *testing.T) {
			ordinary, records := control()
			delete(records[0].Source.Fields, dumpformat.OpaqueIssueFields()[0])
			if _, err := nativeProjectPublicReverseExpectation(ordinary, records, declared); err == nil {
				t.Fatal("absent opaque source field was accepted as explicit null")
			}
		})
		for _, emptyArchive := range []bool{false, true} {
			t.Run(fmt.Sprintf("reserved-key-with-empty-archive-%t", emptyArchive), func(t *testing.T) {
				ordinary, records := control()
				projection, err := nativeDecodePublicProjection(ordinary)
				if err != nil {
					t.Fatal(err)
				}
				projection.Issues["source-control"]["metadata"].(map[string]any)["_plug_source_fields"] = nil
				ordinary, err = json.Marshal(projection)
				if err != nil {
					t.Fatal(err)
				}
				if emptyArchive {
					for _, field := range declared {
						records[0].Source.Fields[field] = nil
					}
					check(t, ordinary, records, "")
				} else if _, err := nativeProjectPublicReverseExpectation(ordinary, records, declared); err == nil {
					t.Fatal("reverse source metadata overwrote an existing key")
				}
			})
		}
	})
	t.Run("source-export-policy-correspondence", func(t *testing.T) {
		control := func() (nativeHistoryOffObservation, []dumpformat.Disposition, map[string]any) {
			return nativeHistoryOffObservation{
					IssueRows: 3,
					WokenRow: map[string]any{
						"id": "source-control", "title": `{"n":1}`, "defer_until": nil,
						"metadata": `{"n":9007199254740993}`, "row_lock": float64(9), "is_blocked": true,
					},
					Columns: []map[string]any{
						{"column_name": "id", "data_type": "varchar"},
						{"column_name": "title", "data_type": "text"},
						{"column_name": "defer_until", "data_type": "datetime"},
						{"column_name": "metadata", "data_type": "json"},
						{"column_name": "row_lock", "data_type": "bigint"},
						{"column_name": "is_blocked", "data_type": "boolean"},
					},
				}, []dumpformat.Disposition{
					{Family: "issues", Rows: 3, Action: "carry", Reason: "head state and source provenance"},
					{Family: "issues.row_lock", Rows: 3, Action: "manifest/rebuild", Reason: "transient locks excluded"},
					{Family: "issues.is_blocked", Rows: 3, Action: "manifest/rebuild", Reason: "derived blocking excluded; Plug must recompute"},
				}, map[string]any{
					"id": "source-control", "title": `{"n":1}`, "defer_until": nil,
					"metadata": map[string]any{"n": json.Number("9007199254740993")},
				}
		}
		observed, sources, archived := control()
		before, err := json.Marshal(observed)
		if err != nil {
			t.Fatal(err)
		}
		if err := nativeHistoryOffSourceCorrespondence(observed, sources, "issues", archived); err != nil {
			t.Fatalf("exact fixed export policies rejected: %v", err)
		}
		after, err := json.Marshal(observed)
		if err != nil || string(before) != string(after) {
			t.Fatal("source projection changed the complete raw observation or excluded values")
		}
		for _, test := range []struct {
			name   string
			mutate func(*nativeHistoryOffObservation, *[]dumpformat.Disposition, *string, map[string]any)
		}{
			{"missing-lock-policy", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				*s = append((*s)[:1], (*s)[2:]...)
			}},
			{"missing-blocked-policy", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				*s = (*s)[:2]
			}},
			{"duplicate-policy", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				*s = append(*s, (*s)[1])
			}},
			{"changed-action", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				(*s)[1].Action = "carry"
			}},
			{"changed-reason", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				(*s)[2].Reason = "other exclusion"
			}},
			{"changed-policy-row-count", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				(*s)[1].Rows++
			}},
			{"changed-issue-row-count", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				(*s)[0].Rows++
			}},
			{"changed-census-row-count", func(o *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				o.IssueRows++
			}},
			{"absent-census-row-count", func(o *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				o.IssueRows = 0
			}},
			{"extra-column-policy", func(_ *nativeHistoryOffObservation, s *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				*s = append(*s, dumpformat.Disposition{Family: "issues.title", Rows: 3, Action: "manifest/rebuild", Reason: "other exclusion"})
			}},
			{"wrong-source-table", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, table *string, _ map[string]any) {
				*table = "other"
			}},
			{"missing-raw-excluded-column", func(o *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				delete(o.WokenRow, "row_lock")
				o.Columns = append(o.Columns[:4], o.Columns[5:]...)
			}},
			{"extra-physical-column", func(o *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, _ map[string]any) {
				o.WokenRow["priority"] = float64(2)
				o.Columns = append(o.Columns, map[string]any{"column_name": "priority", "data_type": "int"})
			}},
			{"missing-exported-field", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				delete(a, "title")
			}},
			{"missing-explicit-null", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				delete(a, "defer_until")
			}},
			{"changed-exported-value", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				a["title"] = "changed"
			}},
			{"changed-json-precision", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				a["metadata"] = map[string]any{"n": json.Number("9007199254740992")}
			}},
			{"unexpected-lock-field", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				a["row_lock"] = nil
			}},
			{"unexpected-blocked-field", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				a["is_blocked"] = true
			}},
			{"extra-exported-field", func(_ *nativeHistoryOffObservation, _ *[]dumpformat.Disposition, _ *string, a map[string]any) {
				a["extra"] = nil
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				observed, sources, archived := control()
				table := "issues"
				test.mutate(&observed, &sources, &table, archived)
				if err := nativeHistoryOffSourceCorrespondence(observed, sources, table, archived); err == nil {
					t.Fatal("changed source fields or export policy was admitted")
				}
			})
		}
	})
}
