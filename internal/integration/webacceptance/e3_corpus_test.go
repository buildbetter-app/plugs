package webacceptance_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	e3Project = "e3-scripted"
	e3Beads   = 20_000
	e3Agents  = 50
)

// This is population/workload preparation, not a browser qualification. It
// uses the actual no-plugin anonymous installation and never supplies an
// AllowAll event authorizer or a successful fixture watch implementation.
type e3Fixture struct {
	*fixture
	sessions *collab.Manager
	evidence string
	receipt  e3CorpusReceipt
	browser  *e3BrowserHost
}

type e3BatchReceipt struct {
	StartID   string         `json:"startId"`
	Count     int            `json:"count"`
	Revision  model.Revision `json:"revision"`
	ElapsedMS float64        `json:"elapsedMs"`
}

type e3CallReceipt struct {
	Method    string         `json:"method"`
	Params    map[string]any `json:"params"`
	Response  reply          `json:"response"`
	ElapsedMS float64        `json:"elapsedMs"`
}

type e3AgentReceipt struct {
	Label       string             `json:"label"`
	DisplayName string             `json:"displayName"`
	Principal   model.PrincipalRef `json:"principal"`
	SessionID   string             `json:"sessionId"`
	WorkingOn   string             `json:"workingOn"`
}

type e3CorpusReceipt struct {
	Schema                  string           `json:"schema"`
	Qualification           string           `json:"qualification"`
	SourceHead              string           `json:"sourceHead"`
	SourceTree              string           `json:"sourceTree"`
	Engine                  map[string]any   `json:"engine"`
	Audit                   map[string]any   `json:"audit"`
	Clock                   string           `json:"workloadClock"`
	ExpectedBeads           int              `json:"expectedStoredBeads"`
	StoredBeads             int              `json:"storedBeads"`
	SQLBeads                int              `json:"sqlStoredBeads"`
	StoredDigest            string           `json:"orderedStoredRecordsSha256"`
	StoredStatusCounts      map[string]int   `json:"storedStatusCounts"`
	ActualAgents            int              `json:"actualSimulatedAgents"`
	ActualLiveSessions      int              `json:"actualLiveSessions"`
	ActualWorkingRows       int              `json:"actualWorkingRows"`
	EpicID                  string           `json:"epicID,omitempty"`
	EpicDirectChildren      int              `json:"epicDirectChildren,omitempty"`
	EpicHiddenChildren      int              `json:"epicHiddenChildren,omitempty"`
	Authorization           string           `json:"authorization"`
	DisplayNameSupport      string           `json:"displayNameSupport"`
	DisplayNamesRequested   int              `json:"displayNamesRequested"`
	DisplayNamesAccepted    int              `json:"displayNamesAccepted"`
	Agents                  []e3AgentReceipt `json:"agents"`
	Batches                 []e3BatchReceipt `json:"batches"`
	Calls                   []e3CallReceipt  `json:"rpcCalls"`
	Incarnation             string           `json:"incarnation"`
	Revision                model.Revision   `json:"revision"`
	PreparationElapsedMS    float64          `json:"preparationElapsedMs"`
	DeterministicPlanSHA256 string           `json:"deterministicPlanSha256"`
	Complete                bool             `json:"preparationComplete"`
}

func e3ID(index int) string { return fmt.Sprintf("e3-%05d", index) }

func e3Key(operation string) string {
	sum := sha256.Sum256([]byte("e3-browser-scripted-v1:" + operation))
	return hex.EncodeToString(sum[:])
}

func e3Fields(index int) map[string]any {
	return map[string]any{
		"title":       fmt.Sprintf("E3 stored bead %05d", index),
		"description": "Deterministic disposable E3 corpus; stored by the real Writer.",
	}
}

func newE3Fixture(t *testing.T) *e3Fixture {
	return newE3FixtureMode(t, false)
}

func newE3FixtureMode(t *testing.T, browser bool) *e3Fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	base := &fixture{
		t: t, backend: sqliteBackend, ctx: ctx, cancel: cancel,
		path: filepath.Join(t.TempDir(), "e3.db"), cursorKey: make([]byte, 32),
		clock:          func() time.Time { return now },
		ontologySource: &service.OntologySource{Base: ontology.DefaultDocument()},
	}
	if _, err := rand.Read(base.cursorKey); err != nil {
		t.Fatal(err)
	}
	nextID := 0
	// start installs the tracked audit from the actual protected source it
	// creates, without consuming the deterministic bead ID generator.
	base.writerOptions = []pipeline.Option{pipeline.WithIDGenerator(func() (string, error) {
		nextID++
		return e3ID(nextID), nil
	})}
	f := &e3Fixture{fixture: base}
	base.bind = func(registry *rpc.Registry) error {
		var err error
		config := collab.Config{
			Service: base.domain, Lease: base.lease, Clock: base.clock,
			AuthorizationInstalled: func() bool { return false },
		}
		if browser {
			f.browser = newE3BrowserHost(f)
			config.Policy = f.browser.policy
			config.AuthorizationInstalled = func() bool { return true }
		}
		f.sessions, err = collab.New(ctx, config)
		if err != nil {
			return err
		}
		if err := collab.Bind(registry, f.sessions); err != nil {
			return err
		}
		if browser {
			if err := f.browser.bind(registry); err != nil {
				return err
			}
		}
		// Current main binds tx.apply through BindService; do not register a
		// second fixture-only handler after the forward merge.
		return nil
	}
	base.start()
	t.Cleanup(func() {
		if f.browser != nil {
			f.browser.close()
		}
		if f.sessions != nil {
			if err := f.sessions.Close(); err != nil {
				t.Error(err)
			}
		}
		base.stop()
		cancel()
	})
	root := os.Getenv("PLUG_WEB_ACCEPTANCE_EVIDENCE_DIR")
	if root == "" {
		root = os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	}
	if root == "" {
		root = t.TempDir()
	}
	f.evidence = filepath.Join(root, "e3")
	if !browser {
		f.evidence = filepath.Join(root, "e3-population")
	}
	if err := os.MkdirAll(f.evidence, 0o755); err != nil {
		t.Fatal(err)
	}
	f.receipt = e3CorpusReceipt{
		Schema: "plug-e3-corpus/1", Qualification: "population-only; five browser tasks not qualified",
		SourceHead: os.Getenv("PLUG_E3_SOURCE_HEAD"), SourceTree: os.Getenv("PLUG_E3_SOURCE_TREE"),
		Clock: now.Format(time.RFC3339Nano), ExpectedBeads: e3Beads,
		Authorization:      "real anonymous no-plugin installation; no installed-policy or authorized/denied qualification",
		StoredStatusCounts: map[string]int{}, Engine: map[string]any{"driver": "sqlite", "disposable": true},
		Audit: map[string]any{
			"writerOption": "WithOntologyAudit(service.NewOntologyAudit(service.StaticOntologySource(sourceRef)))",
			"sourceRef":    base.ontologyRef, "sourceRevision": base.ontologyRevision, "definitionDigest": base.ontologyDigest,
			"scope": "actual protected fixture source; same definition for Writer, tracked audit and pinned direct-blocker reads; blocks workload only",
		},
	}
	t.Cleanup(func() { writeE3JSON(t, filepath.Join(f.evidence, "corpus-workload.json"), f.receipt) })
	return f
}

func writeE3JSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(raw, '\n'), 0o644)
	}
	if err != nil {
		t.Errorf("write E3 receipt %s: %v", path, err)
	}
}

func (f *e3Fixture) call(method string, params map[string]any, target any) reply {
	f.t.Helper()
	started := time.Now()
	response := f.invoke(method, params)
	f.receipt.Calls = append(f.receipt.Calls, e3CallReceipt{Method: method, Params: params, Response: response, ElapsedMS: float64(time.Since(started).Microseconds()) / 1000})
	if response.Error != nil {
		f.t.Fatalf("E3 %s failed: %+v", method, response.Error)
	}
	if target != nil {
		if err := json.Unmarshal(response.Result, target); err != nil {
			f.t.Fatalf("decode E3 %s: %v", method, err)
		}
	}
	return response
}

func (f *e3Fixture) populate(first, last int) {
	f.t.Helper()
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	const batchSize = 100
	for start := first; start <= last; start += batchSize {
		ops := make([]model.Op, 0, batchSize)
		for index := start; index < start+batchSize; index++ {
			fields := e3Fields(index)
			ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: e3Project, Fields: fields}})
		}
		batchStarted := time.Now()
		commit, err := f.writer.Apply(f.ctx, by, model.ChangeSet{Ops: ops}, model.WriteMeta{
			Method: "tx.apply", Project: e3Project, Agent: "e3-populator", IdempotencyKey: e3Key(fmt.Sprintf("seed-%05d", start)),
		})
		if err != nil {
			f.t.Fatalf("E3 real Writer batch starting at %d: %v", start, err)
		}
		if len(commit.Results) != len(ops) {
			f.t.Fatalf("E3 Writer batch %d returned %d/%d results", start, len(commit.Results), len(ops))
		}
		for offset, result := range commit.Results {
			if result.Ref.ID != e3ID(start+offset) {
				f.t.Fatalf("E3 nondeterministic Writer ID: %+v", result.Ref)
			}
		}
		f.receipt.Batches = append(f.receipt.Batches, e3BatchReceipt{StartID: e3ID(start), Count: len(ops), Revision: commit.Revision, ElapsedMS: float64(time.Since(batchStarted).Microseconds()) / 1000})
	}
	f.t.Logf("E3 real Writer seeded IDs %s through %s; %d committed batches so far", e3ID(first), e3ID(last), len(f.receipt.Batches))
}

func (f *e3Fixture) runAgents() {
	f.t.Helper()
	// Persist the epic graph through the real tx.apply/Reparent path before any
	// child is claimed. The lease guard intentionally rejects reparenting a
	// claimed bead, so hierarchy setup must precede the collaboration workload.
	// The parent and 51 direct children are existing Writer-created beads, so
	// this preserves the exact 20,000-bead corpus while exercising the
	// hierarchy seam used by the browser's epic-progress producer. Child 60
	// remains the stored but unreadable canary; its identity and status must
	// never escape.
	parent := model.RefByID(e3ID(99), model.Natural64(0))
	ops := make([]model.Op, 0, e3Agents+1)
	for index := 1; index <= e3Agents; index++ {
		child := model.RefByID(e3ID(index), model.Natural64(0))
		parentCopy := parent
		ops = append(ops, model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: child, Parent: &parentCopy}})
	}
	hidden := model.RefByID(e3ID(60), model.Natural64(0))
	parentCopy := parent
	ops = append(ops, model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: hidden, Parent: &parentCopy}})
	f.call("tx.apply", map[string]any{
		"changeSet": model.ChangeSet{Ops: ops},
		"_meta":     map[string]any{"project": e3Project, "agent": "e3-populator", "idempotencyKey": e3Key("epic-hierarchy")},
	}, nil)
	f.receipt.EpicID = parent.ID
	f.receipt.EpicDirectChildren = e3Agents + 1
	f.receipt.EpicHiddenChildren = 1

	displayNamesSupported := true
	probeMeta := map[string]any{"project": e3Project, "agent": "e3-displayName-probe", "idempotencyKey": e3Key("displayName-probe")}
	f.receipt.DisplayNamesRequested++
	probe := f.invoke("session.open", map[string]any{"displayName": "e3-displayName-probe", "_meta": probeMeta})
	f.receipt.Calls = append(f.receipt.Calls, e3CallReceipt{Method: "session.open", Params: map[string]any{"displayName": "e3-displayName-probe", "_meta": probeMeta}, Response: probe})
	if probe.Error != nil {
		if string(probe.Error.Data.Code) != "invalid_params" {
			f.t.Fatalf("E3 displayName capability probe failed: %+v", probe.Error)
		}
		displayNamesSupported = false
		f.receipt.DisplayNameSupport = "not-admitted: session.open rejected displayName with invalid_params"
	} else {
		var opened collab.Session
		if err := json.Unmarshal(probe.Result, &opened); err != nil || opened.SessionID == "" {
			f.t.Fatalf("E3 displayName capability probe returned invalid session: %s", probe.Result)
		}
		f.receipt.DisplayNameSupport = "session.open accepted displayName; presence output remains authoritative"
		f.receipt.DisplayNamesAccepted++
		f.call("session.close", map[string]any{"sessionId": opened.SessionID, "_meta": map[string]any{"project": e3Project, "agent": "e3-displayName-probe", "idempotencyKey": e3Key("displayName-probe-close")}}, nil)
	}
	for index := 1; index <= e3Agents; index++ {
		label := fmt.Sprintf("e3-agent-%02d", index)
		ref := model.RefByID(e3ID(index), model.Natural64(0))
		meta := func(operation string) map[string]any {
			return map[string]any{"project": e3Project, "agent": label, "idempotencyKey": e3Key(label + ":" + operation)}
		}
		var session collab.Session
		openParams := map[string]any{"_meta": meta("open")}
		if displayNamesSupported {
			openParams["displayName"] = label
			f.receipt.DisplayNamesRequested++
		}
		f.call("session.open", openParams, &session)
		if displayNamesSupported {
			f.receipt.DisplayNamesAccepted++
		}
		if session.SessionID == "" || session.Principal.Kind != "anonymous" {
			f.t.Fatalf("E3 real collaboration session is invalid: %+v", session)
		}
		f.call("bead.claim", map[string]any{"id": ref, "_meta": meta("claim")}, nil)
		f.call("bead.update", map[string]any{
			"id": ref, "fields": map[string]any{"title": fmt.Sprintf("E3 bead %05d updated by %s", index, label)}, "_meta": meta("update"),
		}, nil)
		resource := "bead/" + ref.ID
		var presence collab.Presence
		f.call("presence.set", map[string]any{
			"sessionId": session.SessionID, "focus": resource, "workingOn": []string{resource}, "_meta": meta("presence"),
		}, &presence)
		if presence.Focus != resource || len(presence.WorkingOn) != 1 || presence.WorkingOn[0] != resource {
			f.t.Fatalf("E3 presence did not reflect the real service claim for %s: %+v", label, presence)
		}
		caller := collab.Caller{Principal: session.Principal, Agent: label}
		lifetime, err := f.sessions.SessionContext(f.ctx, caller, session.SessionID)
		if err != nil || lifetime.Err() != nil {
			f.t.Fatalf("E3 session %s is not live: %v", label, err)
		}
		displayName := ""
		if displayNamesSupported {
			displayName = label
		}
		f.receipt.Agents = append(f.receipt.Agents, e3AgentReceipt{Label: label, DisplayName: displayName, Principal: session.Principal, SessionID: session.SessionID, WorkingOn: resource})
	}
	// A real directed dependency provides a stable blocked candidate and an
	// unclaimed blocker. It is never converted to an invented parent/epic edge.
	f.call("tx.apply", map[string]any{
		"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{
			Kind: model.OpLink, From: model.RefByID(e3ID(52), model.Natural64(0)), To: model.RefByID(e3ID(51), model.Natural64(0)), LinkKind: "blocks",
		}}}},
		"_meta": map[string]any{"project": e3Project, "agent": "e3-populator", "idempotencyKey": e3Key("dependency")},
	}, nil)
	for index := 53; index <= 57; index++ {
		f.call("bead.close", map[string]any{
			"id": model.RefByID(e3ID(index), model.Natural64(0)), "reason": "E3 deterministic completed work",
			"_meta": map[string]any{"project": e3Project, "agent": "e3-populator", "idempotencyKey": e3Key(fmt.Sprintf("close-%d", index))},
		}, nil)
	}
	var result struct {
		Presence []collab.Presence `json:"presence"`
	}
	f.call("presence.list", map[string]any{"project": e3Project, "_meta": map[string]any{"project": e3Project}}, &result)
	labels, sessions, resources := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, agent := range f.receipt.Agents {
		labels[agent.Label], sessions[agent.SessionID], resources[agent.WorkingOn] = true, true, true
	}
	workingRows := 0
	for _, row := range result.Presence {
		if sessions[row.SessionID] && len(row.WorkingOn) == 1 && resources[row.WorkingOn[0]] {
			workingRows++
		}
	}
	f.receipt.ActualAgents = len(labels)
	f.receipt.ActualLiveSessions = len(result.Presence)
	f.receipt.ActualWorkingRows = workingRows
	if len(labels) != e3Agents || len(sessions) != e3Agents || len(resources) != e3Agents || len(result.Presence) != e3Agents || workingRows != e3Agents {
		f.t.Fatalf("E3 workload is not exactly 50 distinct agents, sessions and current claims: labels=%d sessions=%d resources=%d presence=%d working=%d", len(labels), len(sessions), len(resources), len(result.Presence), workingRows)
	}
	f.t.Logf("E3 executed sequential deterministic RPC workload: %d distinct anonymous agent labels, %d live sessions, %d live service claims", len(labels), len(sessions), workingRows)
}

func (f *e3Fixture) runPopulatedAgentUpdates() {
	f.t.Helper()
	for index, agent := range f.receipt.Agents {
		f.call("bead.update", map[string]any{
			"id":     model.RefByID(e3ID(index+1), model.Natural64(0)),
			"fields": map[string]any{"title": fmt.Sprintf("E3 full-corpus update %05d by %s", index+1, agent.Label)},
			"_meta":  map[string]any{"project": e3Project, "agent": agent.Label, "idempotencyKey": e3Key(agent.Label + ":full-corpus-update")},
		}, nil)
	}
	var result struct {
		Presence []collab.Presence `json:"presence"`
	}
	f.call("presence.list", map[string]any{"project": e3Project, "_meta": map[string]any{"project": e3Project}}, &result)
	if len(result.Presence) != e3Agents {
		f.t.Fatalf("E3 full-corpus workload lost simulated agents: got %d", len(result.Presence))
	}
	for _, row := range result.Presence {
		if len(row.WorkingOn) != 1 {
			f.t.Fatalf("E3 full-corpus presence lost a live claim: %+v", row)
		}
	}
	f.receipt.ActualLiveSessions = len(result.Presence)
	f.receipt.ActualWorkingRows = len(result.Presence)
	f.t.Log("E3 all 50 simulated agents executed a real RPC update with the complete 20,000-bead corpus stored")
}

func (f *e3Fixture) verifyStoredCorpus() {
	f.t.Helper()
	meta := f.meta()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	var records []driver.Record
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			f.t.Fatal(err)
		}
		if page.AsOf.String() != r.Snapshot().At.String() {
			f.t.Fatal("E3 driver changed the pinned snapshot during the count")
		}
		records = append(records, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			f.t.Fatal("E3 driver returned a non-advancing corpus page")
		}
		cursor = page.NextCursor
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Ref.ID < records[j].Ref.ID })
	digest := sha256.New()
	plan := sha256.New()
	f.receipt.StoredStatusCounts = map[string]int{}
	for index, record := range records {
		body, err := json.Marshal(e3Fields(index + 1))
		if err != nil {
			f.t.Fatal(err)
		}
		fmt.Fprintf(plan, "%s\x00%s\n", e3ID(index+1), body)
		var document pipeline.Document
		if err := json.Unmarshal(record.Body, &document); err != nil {
			f.t.Fatal(err)
		}
		if record.Kind != "bead" || document.ID != e3ID(index+1) || record.Ref.ID != document.ID || document.Project != e3Project || document.Type != "issue" {
			f.t.Fatalf("E3 stored corpus has a missing, duplicate or foreign row at %d: %+v", index+1, record.Ref)
		}
		status, ok := document.Fields["status"].(string)
		if !ok {
			f.t.Fatal("E3 stored status is absent")
		}
		f.receipt.StoredStatusCounts[status]++
		fmt.Fprintf(digest, "%s\x00%s\x00%s\n", record.Ref.ID, record.Revision.String(), record.Body)
	}
	f.receipt.StoredBeads = len(records)
	f.receipt.StoredDigest = hex.EncodeToString(digest.Sum(nil))
	f.receipt.DeterministicPlanSHA256 = hex.EncodeToString(plan.Sum(nil))
	f.receipt.Incarnation, f.receipt.Revision = meta.Incarnation, meta.Head
	// Independently count physical current rows through read-only SQL. The
	// driver enumeration above additionally checks every stored identity/body.
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	defer db.Close()
	var version, mode string
	if err = db.QueryRowContext(f.ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		f.t.Fatal(err)
	}
	if err = db.QueryRowContext(f.ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		f.t.Fatal(err)
	}
	if err = db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM records r JOIN objects o USING(key) WHERE o.kind = 'bead'").Scan(&f.receipt.SQLBeads); err != nil {
		f.t.Fatal(err)
	}
	f.receipt.Engine["version"], f.receipt.Engine["journalMode"] = version, mode
	if f.receipt.StoredBeads != e3Beads || f.receipt.SQLBeads != e3Beads || mode != "wal" {
		f.t.Fatalf("E3 stored corpus mismatch: driver=%d SQL=%d mode=%s", f.receipt.StoredBeads, f.receipt.SQLBeads, mode)
	}
	// Keep an independent, compact list of every actual stored identity. This
	// is an observation receipt and is never served to the product frontend.
	file, err := os.Create(filepath.Join(f.evidence, "stored-beads.jsonl"))
	if err != nil {
		f.t.Fatal(err)
	}
	for _, record := range records {
		raw, err := json.Marshal(record)
		if err != nil {
			file.Close()
			f.t.Fatal(err)
		}
		if _, err := io.WriteString(file, string(raw)+"\n"); err != nil {
			file.Close()
			f.t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.t.Logf("E3 independent stored count: driver=%d SQL=%d; SQLite=%s WAL; ordered digest=%s", len(records), f.receipt.SQLBeads, version, f.receipt.StoredDigest)
}

func TestE3RealStoredCorpusAndFiftyAgentWorkload(t *testing.T) {
	f := newE3Fixture(t)
	started := time.Now()
	// Claims exercise the real lifecycle guard before growing the corpus.
	// An earlier retained run hit the Writer's unchanged two-second pre-phase
	// deadline while claiming after the full seed; it is not hidden or retried.
	f.populate(1, 100)
	f.runAgents()
	f.populate(101, e3Beads)
	f.runPopulatedAgentUpdates()
	f.verifyStoredCorpus()
	f.receipt.PreparationElapsedMS = float64(time.Since(started).Microseconds()) / 1000
	f.receipt.Complete = true
}
