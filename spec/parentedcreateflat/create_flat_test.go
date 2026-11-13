package parentedcreateflat

// The public fixture and retry/retention mechanics come from diagnostic
// 94ac7526. This permanent regression adds independently authored expected
// outputs, flatness/work assertions, and durable SQL reopen/replay checks.
import (
	"encoding/json"
	"errors"
	"fmt"
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
	End                      int                           `json:"end"`
	Size                     int                           `json:"size"`
	ElapsedMS                float64                       `json:"elapsedMs"`
	Attempts                 int                           `json:"attempts"`
	Retries                  int                           `json:"retries"`
	Completed                bool                          `json:"completed"`
	TransitionRetryException bool                          `json:"transitionRetryException,omitempty"`
	ReadWork                 map[driver.ReadWorkKind]int64 `json:"readWork"`
	QueryPages               []queryPageWork               `json:"queryPages"`
	IdentityWork             capabilityWork                `json:"identityWork"`
	HierarchyWork            capabilityWork                `json:"hierarchyWork"`
	Readback                 durableReadback               `json:"readback"`
}

type transitionChildNumber struct {
	Maximum uint64
	Counter *driver.ParentCounter
}

// seedAttemptGuard is installed around every ordinary parented batch that can
// run in the canonical whole. A retryable busy response is allowed there, but
// the refused attempt must leave the complete durable image and the real child
// continuation state unchanged. Dedicated focus supplies AllowBusy=false for
// fixed-path batches; the original PostgreSQL workload is the documented
// sibling-growth exception alongside the mixed transition.
type seedAttemptGuard struct {
	Before    ordinaryB04State
	Parents   []expectedCreate
	Numbers   map[string]transitionChildNumber
	AllowBusy bool
}

type seedAttemptResult struct {
	Code            string
	Refused         bool
	RefusedReadback *ordinaryB04State
	RefusedNumbers  map[string]transitionChildNumber
}

// validateSeedAttempt is the single refusal comparison used by the public
// workload and the ordinary initialization/child regressions. Keep this
// unconditional: strict focus changes whether busy is permitted, never what a
// refused attempt is allowed to mutate.
func validateSeedAttempt(t *testing.T, o *ordinaryB04, guard seedAttemptGuard, label string, attempt int, raw []byte) seedAttemptResult {
	t.Helper()
	var frame struct {
		Error *model.Error `json:"error"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Error == nil {
		return seedAttemptResult{Code: "success"}
	}
	code := string(frame.Error.Data.Code)
	if code != "busy" || !frame.Error.Data.Retryable || attempt > 5 || !guard.AllowBusy {
		t.Fatalf("batch does not permit this refusal/retry: label=%s attempt=%d response=%s", label, attempt, raw)
	}
	after := o.captureState()
	numbers := transitionChildNumbers(t, o, guard.Parents)
	if !reflect.DeepEqual(after, guard.Before) || !reflect.DeepEqual(numbers, guard.Numbers) {
		t.Fatalf("refused batch changed durable state or consumed child numbers: before=%+v after=%+v beforeNumbers=%+v afterNumbers=%+v", guard.Before, after, guard.Numbers, numbers)
	}
	return seedAttemptResult{Code: code, Refused: true, RefusedReadback: &after, RefusedNumbers: numbers}
}

func installSeedAttemptReadback(t *testing.T, o *ordinaryB04, guard func(label string) seedAttemptGuard, observe func(label string, attempt int, phase string, raw []byte, status int, elapsed float64, result seedAttemptResult)) {
	t.Helper()
	o.seedAttemptReadback = func(label string, attempt int, phase string, raw []byte, status int, elapsed float64) {
		var result seedAttemptResult
		if phase == "after" {
			result = validateSeedAttempt(t, o, guard(label), label, attempt, raw)
		}
		if observe != nil {
			observe(label, attempt, phase, raw, status, elapsed, result)
		}
	}
}

// Read the real committed identity maximum and continuation state outside the
// measured request. A refused transition must leave both exactly unchanged.
func transitionChildNumbers(t *testing.T, o *ordinaryB04, batch []expectedCreate) map[string]transitionChildNumber {
	t.Helper()
	r, err := o.f.store.Read(o.f.ctx, driver.View{Incarnation: o.f.metaSnapshot().Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	result := map[string]transitionChildNumber{}
	for _, e := range batch {
		if e.Parent == "" {
			continue
		}
		maximum, err := r.(driver.NumericChildReader).MaxChildNumber(o.f.ctx, e.Parent, nil)
		if err != nil {
			t.Fatal(err)
		}
		state := transitionChildNumber{Maximum: maximum}
		counter, err := r.(driver.ParentCounterReader).LoadParentCounter(o.f.ctx, e.Parent)
		if err == nil {
			state.Counter = &counter
		} else if !errors.Is(err, driver.ErrNotFound) {
			t.Fatal(err)
		}
		result[e.Parent] = state
	}
	return result
}

// Only the three top-level identities in the original transition come from
// the committed attempt. Fields, child suffixes, refs and all other outputs
// retain independently authored expectations. Verify those identities against
// durable records before using them in later parent references.
func committedTransitionIDs(t *testing.T, o *ordinaryB04, actual model.Commit, start, end int, expected []expectedCreate, ops []model.Op) []map[string]string {
	t.Helper()
	if len(actual.Results) != end-start {
		t.Fatal("transition result count differs from request")
	}
	r, err := o.f.store.Read(o.f.ctx, driver.View{Incarnation: o.f.metaSnapshot().Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var ids []map[string]string
	remap := map[string]string{}
	seen := map[string]bool{}
	for i := start; i < end; i++ {
		e := expected[i]
		if e.Parent != "" {
			continue
		}
		got := actual.Results[i-start]
		if got.Ref.Validate() != nil || !got.Ref.IsID() || got.Ref.ID == "" || got.Ref.Branch.String() != "0" || seen[got.Ref.ID] {
			t.Fatalf("invalid committed top-level transition ref: %+v", got.Ref)
		}
		seen[got.Ref.ID] = true
		before := e.ID
		e.ID = got.Ref.ID
		rows, err := r.Get(o.f.ctx, []model.Ref{got.Ref})
		if err != nil || len(rows) != 1 || rows[0].Kind != "bead" || !sameRef(rows[0].Ref, got.Ref) || rows[0].Revision != actual.Revision || !sameJSON(t, rows[0].Body, e.body(t, o.f.clock.Now())) || !sameJSON(t, got.Record, rows[0].Body) {
			t.Fatalf("committed transition identity differs from independently expected durable record: ref=%+v rows=%+v err=%v", got.Ref, rows, err)
		}
		expected[i] = e
		remap[before] = e.ID
		ids = append(ids, map[string]string{"beforeRequest": before, "committed": e.ID})
	}
	if len(ids) != 3 {
		t.Fatalf("original transition top-level creates=%d want3", len(ids))
	}
	for i := end; i < len(expected); i++ {
		if parent, ok := remap[expected[i].Parent]; ok {
			if expected[i].ID != expected[i].Parent+".1" {
				t.Fatal("original child must independently expect the first sub-item number")
			}
			expected[i].Parent, expected[i].ID = parent, parent+".1"
			p := ref(parent)
			ops[i].Create.Parent = &p
		}
	}
	return ids
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

func assertParentedPayloadBaseline(t *testing.T, backend string, children, creates int, work map[driver.ReadWorkKind]int64) {
	t.Helper()
	if backend == "memory" {
		return // Memory payload accounting is NOT_EXPOSED.
	}
	perCreate := float64(work[driver.ReadPayloadRecords]) / float64(creates)
	maximum := 0.08*float64(children) + 10
	if perCreate > maximum {
		t.Fatalf("parented payload baseline exceeded: engine=%s children=%d materializationsPerCreate=%.6f maximum=%.6f", backend, children, perCreate, maximum)
	}
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
	strictFocus := strictFocusAssertions()
	t.Logf("PARENTED_CREATE_FLAT_ASSERTION_MODE strictFocus=%t env=%s", strictFocus, strictFocusEnv)
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
					controls[scenario] = runPublicControl(t, backend, dsn, scenario, strictFocus)
				})
			}
			flat, parented := controls["flat"], controls["parented"]
			if len(flat.Milestones) != 5 || len(parented.Milestones) != 5 {
				t.Fatal("all five controls milestones are required")
			}
			// Root 13:29 oracle v4 uses the literal milestone batches, not a
			// smoothed median. Retain medians and flat timings as observations.
			p500, p2500 := parented.Milestones[0].LiteralBatchMS, parented.Milestones[4].LiteralBatchMS
			t.Logf("ALLOCATOR_REGRESSION_V4 engine=%s P500=%.6fms P2500=%.6fms ratio=%.6f ratioCap=5 absoluteCap=2000ms", backend, p500, p2500, p2500/p500)
			if strictFocus && (p500 <= 0 || p2500 > 5*p500 || p2500 >= 2000) {
				t.Errorf("v4 literal-batch wall-time regression guard failed for %s: P500=%.6fms P2500=%.6fms; report numbers, do not change guard", backend, p500, p2500)
			}
			for _, n := range []int{500, 1000, 1500, 2000, 2500} {
				b := parented.Batches[n/50-1]
				t.Logf("PARENTED_BATCH_TIME_V4 engine=%s N=%d elapsedMS=%.6f", backend, n, b.ElapsedMS)
				t.Logf("RETAINED_HIERARCHY engine=%s N=%d readerCalls=%d returned=%d readerElapsedMS=%.6f headerSQLPages=%d imageSQLPages=%d headerElapsedMS=%.6f imageElapsedMS=%.6f images=%d identity=%+v", backend, n, b.HierarchyWork.Calls, b.HierarchyWork.Returned, float64(b.HierarchyWork.ElapsedNS)/1e6, b.ReadWork[driver.ReadHierarchyHeaderPages], b.ReadWork[driver.ReadHierarchyImagePages], float64(b.ReadWork[driver.ReadHierarchyHeaderNanos])/1e6, float64(b.ReadWork[driver.ReadHierarchyImageNanos])/1e6, b.ReadWork[driver.ReadHierarchyImages], b.IdentityWork)
				perCreate := "NOT_EXPOSED"
				if backend != "memory" {
					perCreate = fmt.Sprintf("%.6f", float64(b.ReadWork[driver.ReadPayloadRecords])/float64(b.Size))
				}
				t.Logf("PARENTED_PAYLOAD_BASELINE_V3 engine=%s N=%d materializationsPerCreate=%s maximum=%.6f", backend, n, perCreate, 0.08*float64(n)+10)
			}
		})
	}
}

func runPublicControl(t *testing.T, backend, dsn, scenario string, strictFocus bool) (result controlResult) {
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
	var attempts []map[string]any
	transitionLabel := ""
	var transitionBatch []expectedCreate
	var transitionNumbers map[string]transitionChildNumber
	var transitionAttempts []map[string]any
	var transitionIDs []map[string]string
	var activeBatchBefore ordinaryB04State
	var activeBatchGuardParents []expectedCreate
	var activeBatchNumbers map[string]transitionChildNumber
	var activeBatchParents []expectedCreate
	var measuredReadBefore map[driver.ReadWorkKind]int64
	var measuredIdentityBefore, measuredHierarchyBefore capabilityWork
	var measuredQueryStart int
	// The original PostgreSQL workload retains the unchanged per-sibling guard
	// read. Its seed batches may cross the preparation deadline even in the
	// dedicated focus; fixed-path controls remain strict.
	originalPostgres := scenario == "original" && backend == "postgres"
	allowBusy := func(label string) bool {
		return label == transitionLabel || originalPostgres
	}
	o.allowSeedRetry = allowBusy
	defer func() {
		o.allowSeedRetry = nil
		if transitionLabel != "" {
			o.retain("transition-retry-v4", map[string]any{"engine": backend, "label": transitionLabel, "maximumAttempts": 5, "attempts": transitionAttempts, "childNumbersBefore": transitionNumbers, "topLevelIDs": transitionIDs, "identityExplanation": "the fixture ID generator advances for top-level creates during refused preparation; its external sequence is not rewound; committed child suffixes retain the independently expected next number", "excludedFromScalingTimeGuards": true})
		}
	}()
	known := map[string]expectedCreate{}
	if scenario == "parented" {
		result.Anchor = 1
		e := expectedCreate{ID: "pg-growth-anchor", Title: "control parent anchor", Priority: 4}
		known[e.ID] = e
		o.seed("untimed-parent-anchor", ordinaryCreateOp(e.ID, project, map[string]any{"title": e.Title, "priority": e.Priority}, ""))
		assertCommit(t, o.receipts["untimed-parent-anchor"].Commit, expectedCommit(t, 1, []expectedCreate{e}, o.f.clock.Now()))
		assertGraphCertificate(t, o, false)
	}
	installSeedAttemptReadback(t, o, func(label string) seedAttemptGuard {
		return seedAttemptGuard{Before: activeBatchBefore, Parents: activeBatchGuardParents, Numbers: activeBatchNumbers, AllowBusy: !strictFocus || allowBusy(label)}
	}, func(label string, attempt int, phase string, raw []byte, status int, elapsed float64, result seedAttemptResult) {
		attempts = append(attempts, map[string]any{"label": label, "attempt": attempt, "phase": phase, "status": status, "httpElapsedMS": elapsed, "response": json.RawMessage(raw), "readWork": w.snapshot(), "readback": snapshot(t, o, false, nil)})
		if phase == "after" && result.Refused {
			// A refused preparation may have performed guard work before the
			// committed attempt. Reset the measured baseline so structural
			// counts describe the committed attempt, not the refusal.
			measuredReadBefore = w.snapshot()
			measuredIdentityBefore, measuredHierarchyBefore = census.capabilitySnapshot()
			measuredQueryStart = len(census.snapshot())
		}
		if phase == "after" && label == transitionLabel {
			transitionAttempts = append(transitionAttempts, map[string]any{"attempt": attempt, "httpElapsedMS": elapsed, "code": result.Code, "refusedStateUnchanged": result.Refused, "refusedReadback": result.RefusedReadback, "childNumbersAfterRefusal": result.RefusedNumbers})
			t.Logf("ORIGINAL_TRANSITION_ATTEMPT_V4 engine=%s attempt=%d elapsedMS=%.6f code=%s", backend, attempt, elapsed, result.Code)
		}
	})
	defer func() { o.retain("attempt-readbacks", attempts) }()
	for start := 0; start < len(ops); start += 50 {
		end := min(start+50, len(ops))
		label := fmt.Sprintf("public-seed-%04d", start)
		transition := false
		if scenario == "original" && start > 0 && expected[start-1].Parent == "" {
			for _, e := range expected[start:end] {
				transition = transition || e.Parent != ""
			}
		}
		if transition {
			if transitionLabel != "" {
				t.Fatal("only one original flat-to-parented transition is permitted")
			}
			transitionLabel, transitionBatch = label, expected[start:end]
		}
		activeBatchParents = expected[start:end]
		activeBatchGuardParents = append([]expectedCreate(nil), activeBatchParents...)
		activeBatchBefore = ordinaryB04State{}
		if !strictFocus || allowBusy(label) {
			activeBatchBefore = o.captureState()
		}
		activeBatchNumbers = transitionChildNumbers(t, o, activeBatchParents)
		if transition {
			transitionNumbers = activeBatchNumbers
			for _, e := range transitionBatch {
				if e.Parent != "" && (e.ID != e.Parent+".1" || transitionNumbers[e.Parent].Maximum != 0 || transitionNumbers[e.Parent].Counter != nil) {
					t.Fatal("original transition must begin with independently expected next child number1")
				}
			}
		}
		params := map[string]any{"changeSet": model.ChangeSet{Ops: ops[start:end]}}
		measuredReadBefore = w.snapshot()
		measuredQueryStart = len(census.snapshot())
		measuredIdentityBefore, measuredHierarchyBefore = census.capabilitySnapshot()
		raw := o.callSeedWithRetry(label, "tx.apply", owner, params)
		after := w.snapshot()
		delta := map[driver.ReadWorkKind]int64{}
		for kind, n := range after {
			delta[kind] = n - measuredReadBefore[kind]
		}
		identityAfter, hierarchyAfter := census.capabilitySnapshot()
		batch := createBatch{TransitionRetryException: transition, IdentityWork: capabilityDelta(identityAfter, measuredIdentityBefore), HierarchyWork: capabilityDelta(hierarchyAfter, measuredHierarchyBefore), End: end, Size: end - start, ElapsedMS: o.lastSeedElapsedMS, Attempts: o.lastSeedAttempts, Retries: o.lastSeedRetries, ReadWork: delta, QueryPages: census.snapshot()[measuredQueryStart:]}
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
		if transition {
			transitionIDs = committedTransitionIDs(t, o, actual, start, end, expected, ops)
		}
		want := expectedCommit(t, start/50+1+result.Anchor, expected[start:end], o.f.clock.Now())
		assertCommit(t, actual, want)
		if transition {
			t.Logf("ORIGINAL_TRANSITION_COMMITTED_V4 engine=%s attempts=%d elapsedMS=%.6f topLevelIDs=%v childNumbersContiguous=true", backend, batch.Attempts, batch.ElapsedMS, transitionIDs)
		}
		for _, e := range expected[start:end] {
			known[e.ID] = e
		}
		result.Completed = end
		full := end%500 == 0 || end == len(ops) || transition
		batch.Readback = snapshot(t, o, full, known)
		result.Batches = append(result.Batches, batch)
		retriesOK := batch.Attempts >= 1 && batch.Attempts <= 5 && batch.Retries == batch.Attempts-1
		if strictFocus && !allowBusy(label) {
			retriesOK = batch.Attempts == 1 && batch.Retries == 0
		}
		if transition {
			retriesOK = batch.Attempts >= 1 && batch.Attempts <= 5 && batch.Retries == batch.Attempts-1 && len(transitionAttempts) == batch.Attempts
		}
		if !retriesOK || batch.Readback.Records != end+result.Anchor || batch.Readback.Changesets != start/50+1+result.Anchor || batch.Readback.Head != want.Revision {
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
			if batch.IdentityWork.Calls != 50 || delta[driver.ReadChildIDLookups] != 50 || delta[driver.ReadAllocatorDecodes] != 0 || batch.IdentityWork.ReadWork[driver.ReadPayloadRecords] != 0 || delta[driver.ReadChildIDCandidates] <= 0 || delta[driver.ReadChildIDCandidates] > cap || len(batch.QueryPages) != wantQueries || backend != "memory" && delta[driver.ReadQueryCalls] != int64(wantQueries) {
				t.Fatalf("fixed request-work cap failed (%s cap=%d): %v", backend, cap, delta)
			}
			assertParentedPayloadBaseline(t, backend, end, batch.Size, delta)
			if backend != "memory" && (delta[driver.ReadPayloadRecords] <= 0 || end > 50 && delta[driver.ReadSelectorKeys] <= 0) {
				t.Fatalf("SQL observer liveness failed: %v", delta)
			}
		} else {
			// The original workload changes from roots to children inside one
			// batch. Apply the flat cap only to wholly flat batches, retaining
			// that original operation mix and the full raw batch accounting.
			flatBatch := true
			for _, e := range expected[start:end] {
				flatBatch = flatBatch && e.Parent == ""
			}
			if flatBatch && delta[driver.ReadPayloadRecords] > 1000 {
				t.Fatalf("flat batch payload cap exceeded: engine=%s scenario=%s end=%d materializations=%d", backend, scenario, end, delta[driver.ReadPayloadRecords])
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
		if strictFocus && scenario == "parented" && m.MedianMS >= 2000 {
			t.Errorf("parented median exceeds unchanged 2s budget at %d", m.End)
		}
	}
	before := snapshot(t, o, true, known)
	o.reopen("durable-reopen")
	after := snapshot(t, o, true, known)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable reopen conservation failed: before=%+v after=%+v", before, after)
	}
	policy := "whole-or-hosted: typed retryable busy may retry up to five attempts on any batch; wall-time guards logged only"
	if strictFocus {
		policy = "dedicated focus: zero retry on fixed-path batches; original transition and PostgreSQL original workload may retry typed busy; wall-time guards asserted on fixed path"
	}
	o.retain("scope", map[string]any{"sourceDiagnostic": "94ac7526f5dcf58137e52c8eb8ac95a39d833d72", "anchorUntimed": true, "measuredCreates": len(ops), "batchSize": 50, "timing": "supplied callSeedWithRetry wall time including ordinary request/response retention; callbacks excluded", "retryAndTimingPolicy": policy, "strictFocus": strictFocus, "readWork": "HTTP request context only, exact create deltas; replay/readback/anchor excluded; SQL executor work is separately plan checked", "profile": "none on either changed-head control", "reopen": backend != "memory", "expectedOutputs": "authored from request inputs, fixed clock, explicit ids/status/attribution and batch revisions"})
	return result
}
