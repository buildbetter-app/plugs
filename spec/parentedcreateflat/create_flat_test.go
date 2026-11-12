package parentedcreateflat

// The public fixture and retry/retention mechanics come from diagnostic
// 94ac7526. This permanent regression adds independently authored expected
// outputs, flatness/work assertions, and durable SQL reopen/replay checks.
import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type readWork struct {
	mu     sync.Mutex
	counts map[driver.ReadWorkKind]int64
}

func (w *readWork) ObserveReadWork(kind driver.ReadWorkKind, n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.counts[kind] += n
}
func (w *readWork) snapshot() map[driver.ReadWorkKind]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := map[driver.ReadWorkKind]int64{}
	for kind, n := range w.counts {
		result[kind] = n
	}
	return result
}
func (w *readWork) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(rw, r.WithContext(driver.WithReadWorkObserver(r.Context(), w)))
	})
}

type expectedCreate struct {
	ID, Title, Parent string
	Priority          int
}

func (e expectedCreate) body(t *testing.T, at time.Time) []byte {
	t.Helper()
	doc := pipeline.Document{ID: e.ID, Type: "issue", Project: project, CreatedAt: &at,
		Fields: map[string]any{"title": e.Title, "priority": e.Priority, "status": "todo", "template": false}}
	if e.Parent != "" {
		p := ref(e.Parent)
		doc.Parent = &p
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func (e expectedCreate) setBy() map[string]string {
	s := map[string]string{"createdAt": "core", "fields.title": "request", "fields.priority": "request", "fields.status": "request", "fields.template": "request"}
	if e.Parent != "" {
		s["parent"] = "request"
	}
	return s
}
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	return reflect.DeepEqual(decodeObject(t, a), decodeObject(t, b))
}
func sameRef(a, b model.Ref) bool {
	return a.Validate() == nil && b.Validate() == nil && a.IsID() == b.IsID() && a.ID == b.ID && a.Key == b.Key && a.Branch.String() == b.Branch.String()
}
func expectedCommit(t *testing.T, revision int, expected []expectedCreate, at time.Time) model.Commit {
	c := model.Commit{Revision: model.Natural64(uint64(revision)), Results: []model.OpResult{}, Warnings: []model.Warning{}}
	for _, e := range expected {
		// Ordinary create constructs an ID ref with its zero Go branch, so the
		// result omits branch on the wire. Request refs may spell branch:0.
		c.Results = append(c.Results, model.OpResult{Ref: model.RefByID(e.ID, ""), Revision: c.Revision, Record: e.body(t, at), SetBy: e.setBy()})
	}
	return c
}
func assertCommit(t *testing.T, actual, expected model.Commit) {
	t.Helper()
	if actual.Revision != expected.Revision || len(actual.Results) != len(expected.Results) || len(actual.Warnings) != 0 || actual.WouldCommit || actual.LeaseExpiresAt != nil || actual.LeaseTtlMs != nil || len(actual.Violations) != 0 {
		t.Fatalf("unexpected commit envelope: %+v", actual)
	}
	for i, want := range expected.Results {
		got := actual.Results[i]
		if !sameRef(got.Ref, want.Ref) || got.Revision != want.Revision || !reflect.DeepEqual(got.SetBy, want.SetBy) || !sameJSON(t, got.Record, want.Record) {
			t.Fatalf("result[%d] refEqual=%t branchRaw=%q/%q revisionEqual=%t setByEqual=%t bodyEqual=%t got=%+v body=%s want=%+v body=%s", i, sameRef(got.Ref, want.Ref), string(got.Ref.Branch), string(want.Ref.Branch), got.Revision == want.Revision, reflect.DeepEqual(got.SetBy, want.SetBy), sameJSON(t, got.Record, want.Record), got, got.Record, want, want.Record)
		}
	}
}

type durableReadback struct {
	Head       model.Revision `json:"head"`
	Records    int            `json:"records"`
	Versions   *int           `json:"versions,omitempty"`
	Changesets int            `json:"changesets"`
}

func snapshot(t *testing.T, o *ordinaryB04, full bool, expected map[string]expectedCreate) durableReadback {
	t.Helper()
	f := o.f
	meta := f.metaSnapshot()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got := durableReadback{Head: meta.Head}
	versions := 0
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		got.Records += len(page.Items)
		if full {
			for _, record := range page.Items {
				e, ok := expected[record.Ref.ID]
				if !ok || record.Kind != "bead" || !sameJSON(t, record.Body, e.body(t, f.clock.Now())) {
					t.Fatalf("unexpected durable record: %+v", record)
				}
				v, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
				if err != nil || v.NextCursor != nil || len(v.Items) != 1 {
					t.Fatalf("versions for %s: %+v err=%v", record.Ref.ID, v, err)
				}
				version := v.Items[0]
				if !sameRef(version.Ref, record.Ref) || version.Revision != record.Revision || !version.Base || version.Deleted || !version.CreatedAt.Equal(f.clock.Now()) || !reflect.DeepEqual(version.Principal, o.principal()) || !sameJSON(t, version.Body, e.body(t, f.clock.Now())) {
					t.Fatalf("independent version expectation failed: %+v", version)
				}
				versions += len(v.Items)
			}
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	history, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	got.Changesets = len(history)
	if full {
		got.Versions = &versions
		seen := map[string]bool{}
		for i, entry := range history {
			if entry.Revision != model.Natural64(uint64(i+1)) || entry.Kind != model.ChangesetWrite || !reflect.DeepEqual(entry.Principal, o.principal()) || !entry.Time.Equal(f.clock.Now()) {
				t.Fatalf("independent history expectation failed: %+v", entry)
			}
			for _, change := range entry.Changes {
				if _, ok := expected[change.Ref.ID]; !ok || seen[change.Ref.ID] || change.Op != model.OpCreate || !change.FromRevision.IsZero() {
					t.Fatalf("unexpected durable create change: %+v", change)
				}
				seen[change.Ref.ID] = true
			}
		}
		if len(seen) != len(expected) {
			t.Fatalf("history creates=%d want=%d", len(seen), len(expected))
		}
	}
	return got
}

type createBatch struct {
	End        int                           `json:"end"`
	Size       int                           `json:"size"`
	ElapsedMS  float64                       `json:"elapsedMs"`
	Attempts   int                           `json:"attempts"`
	Retries    int                           `json:"retries"`
	Completed  bool                          `json:"completed"`
	ReadWork   map[driver.ReadWorkKind]int64 `json:"readWork"`
	QueryPages []queryPageWork               `json:"queryPages"`
	Readback   durableReadback               `json:"readback"`
}
type milestone struct {
	End                      int
	MedianMS, LiteralBatchMS float64
}
type controlResult struct {
	Backend, Scenario string
	Anchor, Completed int
	Batches           []createBatch
	Milestones        []milestone
}

func publicInputs(scenario string) (ids []string, ops []model.Op, expected []expectedCreate) {
	if scenario == "original" {
		for i := 0; i < 1001; i++ {
			id := fmt.Sprintf("b04-skip-%04d", i)
			ids = append(ids, id)
			expected = append(expected, expectedCreate{ID: id, Title: id})
		}
		ids = append(ids, "b04-walkwinner", "b04-walkdecoy")
		expected = append(expected, expectedCreate{ID: "b04-walkwinner", Title: "winner after full front", Priority: 3}, expectedCreate{ID: "b04-walkdecoy", Title: "excluded by resource filter", Priority: 1})
		for i := 0; i < 1001; i++ {
			title := fmt.Sprintf("b04-guard-child-%04d", i)
			parent := fmt.Sprintf("b04-skip-%04d", i)
			ids = append(ids, title)
			expected = append(expected, expectedCreate{ID: parent + ".1", Title: title, Parent: parent, Priority: 4})
		}
	} else {
		if scenario == "parented" {
			ids = append(ids, "pg-growth-anchor")
		}
		for i := 0; i < 2500; i++ {
			title := fmt.Sprintf("pg-growth-control-%04d", i)
			ids = append(ids, title)
			e := expectedCreate{ID: title, Title: title, Priority: 4}
			if scenario == "parented" {
				e.ID = fmt.Sprintf("pg-growth-anchor.%d", i+1)
				e.Parent = "pg-growth-anchor"
			}
			expected = append(expected, e)
		}
	}
	for _, e := range expected {
		key := e.Title
		if e.ID == "b04-walkwinner" {
			key = "winner"
		}
		if e.ID == "b04-walkdecoy" {
			key = "decoy"
		}
		ops = append(ops, ordinaryCreateOp(key, project, map[string]any{"title": e.Title, "priority": e.Priority}, e.Parent))
	}
	return
}
func medians(batches []createBatch) []milestone {
	var result []milestone
	for _, n := range []int{500, 1000, 1500, 2000, 2500} {
		i := n/50 - 1
		if i >= len(batches) {
			continue
		}
		times := make([]float64, 5)
		for j := 0; j < 5; j++ {
			times[j] = batches[i-4+j].ElapsedMS
		}
		sort.Float64s(times)
		result = append(result, milestone{End: n, MedianMS: times[2], LiteralBatchMS: batches[i].ElapsedMS})
	}
	return result
}

func TestParentedCreateFlatPublicRPC(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var h *postgresHarness
			if backend == "postgres" {
				h = newPostgresHarness(t)
			}
			controls := map[string]controlResult{}
			for _, scenario := range []string{"original", "flat", "parented"} {
				t.Run(scenario, func(t *testing.T) {
					dsn := ""
					if h != nil {
						dsn = h.dsn(t)
					}
					controls[scenario] = runPublicControl(t, backend, dsn, scenario)
				})
			}
			flat, parented := controls["flat"], controls["parented"]
			if len(flat.Milestones) != 5 || len(parented.Milestones) != 5 {
				t.Fatal("all five controls milestones are required")
			}
			p0, f0 := parented.Milestones[0].MedianMS, flat.Milestones[0].MedianMS
			for i := 1; i < 5; i++ {
				p, f := parented.Milestones[i], flat.Milestones[i]
				ratioBound := 1.5 * p0 / f0
				absoluteBound := math.Max(1.5*p0, p0+1.5*math.Max(0, f.MedianMS-f0))
				t.Logf("FLATNESS engine=%s N=%d P=%.6fms F=%.6fms P/F=%.6f ratioCap=%.6f absoluteCap=%.6fms", backend, p.End, p.MedianMS, f.MedianMS, p.MedianMS/f.MedianMS, ratioBound, absoluteBound)
				if p.MedianMS/f.MedianMS > ratioBound || p.MedianMS > absoluteBound || p.MedianMS >= 2000 {
					t.Errorf("flatness oracle failed at %s/%d", backend, p.End)
				}
			}
		})
	}
}

func runPublicControl(t *testing.T, backend, dsn, scenario string) (result controlResult) {
	ids, ops, expected := publicInputs(scenario)
	w := &readWork{counts: map[driver.ReadWorkKind]int64{}}
	o := newOrdinaryB04Observed(t, backend, dsn, ids, w.wrap, func(doc *ontology.Document) {
		workflow := doc.Workflows["issue"]
		workflow.Transitions[0].Guards = append(workflow.Transitions[0].Guards, ontology.Guard{Kind: ontology.NoOpenChildren})
		doc.Workflows["issue"] = workflow
	})
	census := &queryCensus{}
	installQueryCensus(o, census)
	result = controlResult{Backend: backend, Scenario: scenario}
	defer func() { o.retain("control-result", result) }()
	known := map[string]expectedCreate{}
	if scenario == "parented" {
		result.Anchor = 1
		e := expectedCreate{ID: "pg-growth-anchor", Title: "control parent anchor", Priority: 4}
		known[e.ID] = e
		o.seed("untimed-parent-anchor", ordinaryCreateOp(e.ID, project, map[string]any{"title": e.Title, "priority": e.Priority}, ""))
		assertCommit(t, o.receipts["untimed-parent-anchor"].Commit, expectedCommit(t, 1, []expectedCreate{e}, o.f.clock.Now()))
		assertGraphCertificate(t, o, false)
	}
	var attempts []map[string]any
	o.seedAttemptReadback = func(label string, attempt int, phase string, raw []byte, status int, elapsed float64) {
		attempts = append(attempts, map[string]any{"label": label, "attempt": attempt, "phase": phase, "status": status, "httpElapsedMS": elapsed, "response": json.RawMessage(raw), "readWork": w.snapshot(), "readback": snapshot(t, o, false, nil)})
	}
	defer func() { o.retain("attempt-readbacks", attempts) }()
	for start := 0; start < len(ops); start += 50 {
		end := min(start+50, len(ops))
		label := fmt.Sprintf("public-seed-%04d", start)
		params := map[string]any{"changeSet": model.ChangeSet{Ops: ops[start:end]}}
		before := w.snapshot()
		queryStart := len(census.snapshot())
		raw := o.callSeedWithRetry(label, "tx.apply", owner, params)
		after := w.snapshot()
		delta := map[driver.ReadWorkKind]int64{}
		for kind, n := range after {
			delta[kind] = n - before[kind]
		}
		batch := createBatch{End: end, Size: end - start, ElapsedMS: o.lastSeedElapsedMS, Attempts: o.lastSeedAttempts, Retries: o.lastSeedRetries, ReadWork: delta, QueryPages: census.snapshot()[queryStart:]}
		var frame struct {
			Error *model.Error `json:"error"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Error != nil {
			result.Batches = append(result.Batches, batch)
			t.Fatalf("public create did not complete: end=%d code=%s response=%s", end, frame.Error.Data.Code, raw)
		}
		batch.Completed = true
		actual := o.commitResponse(label, "tx.apply", raw)
		want := expectedCommit(t, start/50+1+result.Anchor, expected[start:end], o.f.clock.Now())
		assertCommit(t, actual, want)
		for _, e := range expected[start:end] {
			known[e.ID] = e
		}
		result.Completed = end
		full := end%500 == 0 || end == len(ops)
		batch.Readback = snapshot(t, o, full, known)
		result.Batches = append(result.Batches, batch)
		if batch.Attempts != 1 || batch.Retries != 0 || batch.Readback.Records != end+result.Anchor || batch.Readback.Changesets != start/50+1+result.Anchor || batch.Readback.Head != want.Revision {
			t.Fatalf("batch conservation or zero-retry oracle failed: %+v", batch)
		}
		if full && (batch.Readback.Versions == nil || *batch.Readback.Versions != end+result.Anchor) {
			t.Fatal("version conservation failed")
		}
		if scenario == "parented" {
			assertGraphCertificate(t, o, true)
			cap := int64(1275)
			if backend == "memory" {
				cap = 3200
			}
			wantQueries := 0
			if end == 50 {
				wantQueries = 1
				assertOneCensus(t, backend, batch.QueryPages, 1)
			}
			if delta[driver.ReadChildIDCandidates] <= 0 || delta[driver.ReadChildIDCandidates] > cap || len(batch.QueryPages) != wantQueries || backend != "memory" && delta[driver.ReadQueryCalls] != int64(wantQueries) || delta[driver.ReadPayloadRecords] > 1000 {
				t.Fatalf("fixed request-work cap failed (%s cap=%d): %v", backend, cap, delta)
			}
			if backend != "memory" && (delta[driver.ReadPayloadRecords] <= 0 || end > 50 && delta[driver.ReadSelectorKeys] <= 0) {
				t.Fatalf("SQL observer liveness failed: %v", delta)
			}
		}
		// Replay is outside measured creation time/work on BOTH controls. The
		// same request key and payload must return the exact retained result.
		replay := o.call(label, "tx.apply", owner, params)
		var replayFrame struct {
			Result model.Commit `json:"result"`
			Error  *model.Error `json:"error"`
		}
		if err := json.Unmarshal(replay, &replayFrame); err != nil || replayFrame.Error != nil {
			t.Fatalf("replay failed: %s err=%v", replay, err)
		}
		assertCommit(t, replayFrame.Result, want)
		if !reflect.DeepEqual(actual, replayFrame.Result) {
			t.Fatal("replay changed original commit")
		}
	}
	result.Milestones = medians(result.Batches)
	for _, m := range result.Milestones {
		if scenario == "parented" && m.MedianMS >= 2000 {
			t.Errorf("parented median exceeds unchanged 2s budget at %d", m.End)
		}
	}
	before := snapshot(t, o, true, known)
	o.reopen("durable-reopen")
	after := snapshot(t, o, true, known)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable reopen conservation failed: before=%+v after=%+v", before, after)
	}
	o.retain("scope", map[string]any{"sourceDiagnostic": "94ac7526f5dcf58137e52c8eb8ac95a39d833d72", "anchorUntimed": true, "measuredCreates": len(ops), "batchSize": 50, "timing": "supplied callSeedWithRetry wall time including ordinary request/response retention; callbacks excluded; one attempt required", "readWork": "HTTP request context only, exact create deltas; replay/readback/anchor excluded; SQL executor work is separately plan checked", "profile": "none on either changed-head control", "reopen": backend != "memory", "expectedOutputs": "authored from request inputs, fixed clock, explicit ids/status/attribution and batch revisions"})
	return result
}
