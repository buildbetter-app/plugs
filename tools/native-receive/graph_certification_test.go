package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/graphcertfixture"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type graphDeadline struct {
	ObservedAt  time.Time
	Deadline    time.Time
	RemainingMS float64
}

type graphRequestWork struct {
	mu                       sync.Mutex
	Attempts, Queries, Beads int
	Deadlines                []graphDeadline
}

func (w *graphRequestWork) ObservePhase(phase pipeline.Phase, _ time.Duration, _ pipeline.Result) {
	if phase == pipeline.PhaseAdmission {
		w.mu.Lock()
		w.Attempts++
		w.mu.Unlock()
	}
}
func (*graphRequestWork) ObserveConflict(pipeline.ConflictReason) {}
func (*graphRequestWork) ObserveOutcome(pipeline.Result)          {}

type graphObservedStore struct {
	driver.Store
	work *graphRequestWork
}

func (s *graphObservedStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	if at, ok := ctx.Deadline(); ok {
		s.work.mu.Lock()
		now := time.Now()
		s.work.Deadlines = append(s.work.Deadlines, graphDeadline{now, at, float64(at.Sub(now)) / float64(time.Millisecond)})
		s.work.mu.Unlock()
	}
	for _, capability := range graphOptionalReaderCapabilities {
		if capability.name == "DerivedPrefixReader" {
			continue
		}
		if !capability.has(r) {
			_ = r.Close()
			return nil, fmt.Errorf("graph driver lacks required reader capability %s", capability.name)
		}
	}
	observedBase := &graphObservedReader{
		Reader:                           r,
		AllocatorReader:                  r.(driver.AllocatorReader),
		NumericChildReader:               r.(driver.NumericChildReader),
		ParentCounterReader:              r.(driver.ParentCounterReader),
		HierarchyChildrenExcludingReader: r.(driver.HierarchyChildrenExcludingReader),
		InverseSourceLinksReader:         r.(driver.InverseSourceLinksReader),
		work:                             s.work,
	}
	var observed driver.Reader = observedBase
	if derived, ok := r.(driver.DerivedPrefixReader); ok {
		observed = &graphObservedDerivedReader{graphObservedReader: observedBase, DerivedPrefixReader: derived}
	}
	// Keep the observer's optional reader surface identical to the driver;
	// otherwise prepare silently selects a different correctness path.
	if missing := graphMissingReaderCapability(r, observed); missing != "" {
		_ = r.Close()
		return nil, fmt.Errorf("graph observer dropped optional reader capability %s", missing)
	}
	return observed, nil
}

type graphObservedReader struct {
	driver.AllocatorReader
	driver.NumericChildReader
	driver.ParentCounterReader
	driver.Reader
	driver.HierarchyChildrenExcludingReader
	driver.InverseSourceLinksReader
	work *graphRequestWork
}

type graphObservedDerivedReader struct {
	*graphObservedReader
	driver.DerivedPrefixReader
}

// These are the optional Reader capabilities checked by pipeline preparation
// and hierarchy application. WriteTx continuations are returned by Write and
// are not hidden by this read observer.
var graphOptionalReaderCapabilities = []struct {
	name string
	has  func(driver.Reader) bool
}{
	{"NumericChildReader", func(r driver.Reader) bool { _, ok := r.(driver.NumericChildReader); return ok }},
	{"ParentCounterReader", func(r driver.Reader) bool { _, ok := r.(driver.ParentCounterReader); return ok }},
	{"AllocatorReader", func(r driver.Reader) bool { _, ok := r.(driver.AllocatorReader); return ok }},
	{"HierarchyChildrenExcludingReader", func(r driver.Reader) bool { _, ok := r.(driver.HierarchyChildrenExcludingReader); return ok }},
	{"HierarchyChildrenReader", func(r driver.Reader) bool { _, ok := r.(driver.HierarchyChildrenReader); return ok }},
	{"InverseSourceLinksReader", func(r driver.Reader) bool { _, ok := r.(driver.InverseSourceLinksReader); return ok }},
	{"DerivedPrefixReader", func(r driver.Reader) bool { _, ok := r.(driver.DerivedPrefixReader); return ok }},
}

func graphMissingReaderCapability(raw, observed driver.Reader) string {
	for _, capability := range graphOptionalReaderCapabilities {
		if capability.has(raw) != capability.has(observed) {
			return capability.name
		}
	}
	return ""
}

func (r *graphObservedReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, q)
	r.work.mu.Lock()
	defer r.work.mu.Unlock()
	r.work.Queries++
	for _, row := range page.Items {
		if row.Kind == "bead" {
			r.work.Beads++
		}
	}
	return page, err
}

// Suppress only memory Close so the test can make its first real request after
// the production handler has verified/recovered/reacquired its process store.
// All underlying driver/transaction methods and capabilities remain real.
type graphRetainedMemory struct {
	driver.Store
	driver.WorkflowCheckpointStore
}

func (*graphRetainedMemory) Close() error { return nil }

func graphPostgres(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	docker := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...).CombinedOutput()
	}
	name := "plug-graph-cert-" + graphcertfixture.Hash([]byte(t.TempDir()))[:16]
	if out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=graph_cert", "-p", "127.0.0.1::5432", "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"); err != nil {
		t.Fatal(err, string(out))
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(c, "docker", "--host=unix:///var/run/docker.sock", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Error(err, string(out))
		}
	})
	out, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dsn := "postgres://postgres@" + strings.TrimSpace(string(out)) + "/graph_cert?sslmode=disable"
	for {
		out, err = docker("exec", name, "pg_isready", "-U", "postgres", "-d", "graph_cert")
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err, string(out))
		}
		time.Sleep(20 * time.Millisecond)
	}
	return dsn
}

func graphArtifact(t *testing.T, name string, value any) {
	t.Helper()
	raw := graphcertfixture.JSON(t, value)
	t.Logf("%s=%s", name, raw)
	if root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); root != "" {
		dir := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "-"))
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func graphFreshReader(t *testing.T, store driver.Store) driver.Reader {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNativeReceiveFirstGraphWriteCertification(t *testing.T) {
	if os.Getenv("PLUG_PUBLIC_BUNDLE") == "" {
		t.Skip("requires explicit PLUG_PUBLIC_BUNDLE opt-in")
	}
	for _, count := range []int{3, 5000, 8000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			capture, parent := graphcertfixture.Capture(t, "../../internal/integration/converterroundtrip/testdata/public-native-delta-small-0e4d5da4.tar.gz", count)
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					ctx := context.Background()
					archive, err := dumpformat.ReadNativeCaptureSetDirectory(ctx, capture, dumpformat.NativeCaptureSetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					defer archive.ReleaseNativeDecoded()
					if len(archive.Records("bead")) != count {
						t.Fatal("wrong admitted bead count")
					}
					lifetime := driver.CheckpointLifetimeProcess
					if engine != "memory" {
						lifetime = driver.CheckpointLifetimeRestart
					}
					input := request{Format: "plug-native-receiving-request/1", ArchiveSHA256: archive.SHA256(), SourceStoreID: archive.Manifest().StoreID, Projects: []string{archive.Manifest().Projects[0].Prefix}, Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, IdempotencyKey: strings.Repeat("b", 32), SourcePlan: graphcertfixture.Plan(t, archive, lifetime)}
					requestRaw := graphcertfixture.JSON(t, input)
					planBytes, err := driver.SourcePlanePlanEncodedSizeV1(input.SourcePlan)
					if err != nil || len(requestRaw) > driver.SourcePlaneMaxPlanBytes {
						t.Fatal("acceptance count exceeds unchanged plan/request bound", planBytes, len(requestRaw), err)
					}
					graphArtifact(t, "plan-size", map[string]any{"current_items": count, "canonical_bytes": planBytes, "json_request_bytes": len(requestRaw), "limit_bytes": driver.SourcePlaneMaxPlanBytes, "field_count": len(input.SourcePlan.Entities[0].Fields), "fixture": "authored scale, not physical producer proof"})
					if count == 8000 {
						entityBytes := len(bytes.TrimSpace(graphcertfixture.JSON(t, input.SourcePlan.Entities[len(input.SourcePlan.Entities)-1])))
						if len(requestRaw)+1000*(entityBytes+1) <= driver.SourcePlaneMaxPlanBytes {
							t.Fatal("8000 is not the largest whole-thousand fixture count")
						}
					}
					requestPath := filepath.Join(t.TempDir(), "request.json")
					if err := os.WriteFile(requestPath, requestRaw, 0600); err != nil {
						t.Fatal(err)
					}
					target := ""
					if engine == "sqlite" {
						target = filepath.Join(t.TempDir(), "import.sqlite")
					}
					if engine == "postgres" {
						target = graphPostgres(t)
					}
					var retained driver.Store
					open := func(ctx context.Context, engine, target string) (func() (driver.Store, error), error) {
						factory, err := opener(ctx, engine, target)
						if err != nil {
							return nil, err
						}
						if engine != "memory" {
							return factory, nil
						}
						return func() (driver.Store, error) {
							s, err := factory()
							if err != nil {
								return nil, err
							}
							retained = s
							return &graphRetainedMemory{Store: s, WorkflowCheckpointStore: s.(driver.WorkflowCheckpointStore)}, nil
						}, nil
					}
					received, err := runCaptureSetWithOpener(ctx, capture, requestPath, engine, target, "source-built-native-receive-test", "", "", "", open)
					if err != nil {
						t.Fatal("actual capture-set receiving handler", err)
					}
					if received.HistoryMode != "full" || received.Report.GraphCertification == nil || received.Report.GraphCertification.State != "certified" || received.Report.GraphCertification.From != received.Report.Restored.Head || received.Report.Receipt == nil {
						t.Fatal("missing atomic import certificate", received.Report.GraphCertification)
					}
					if engine != "memory" {
						factory, err := opener(ctx, engine, target)
						if err != nil {
							t.Fatal(err)
						}
						retained, err = factory()
						if err != nil {
							t.Fatal(err)
						}
					}
					defer retained.Close()
					op, lease, release, err := hold(ctx, retained)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := release(); err != nil {
							t.Error(err)
						}
					}()
					H := received.Report.Restored.Head
					r := graphFreshReader(t, retained)
					states, err := r.Observe(op, []driver.LockKey{pipeline.GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
					if err != nil || len(states) != 2 {
						t.Fatal(err)
					}
					for _, state := range states {
						if !state.Present || state.Revision != H {
							t.Fatal("stale/missing import fact or sentinel", state)
						}
					}
					parentRef := model.RefByID(parent, model.Natural64(0))
					importChildren, childMore, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(op, []model.Ref{parentRef}, 2)
					if err != nil || childMore || len(importChildren) != 0 {
						t.Fatal("imported isolated parent coverage", err, importChildren)
					}
					importIncoming, incomingMore, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(op, []model.Ref{parentRef}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 2)
					if err != nil || incomingMore || len(importIncoming) != 0 {
						t.Fatal("imported isolated link coverage", err, importIncoming)
					}
					rows, err := r.Get(op, []model.Ref{model.RefByID(parent, model.Natural64(0)), model.RefByID("instance_config/restore-import/binding", model.Natural64(0)), model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0))})
					if err != nil || len(rows) != 3 {
						t.Fatal("durable parent/binding/source binding", err, len(rows))
					}
					graphArtifact(t, "import-coverage-readback", map[string]any{"snapshot": r.Snapshot(), "fact_states": states, "binding_rows": rows, "parent_target": parentRef, "parent_members": importChildren, "parent_more": childMore, "inverse_target": parentRef, "inverse_kind": driver.IncomingLinkBlocks, "inverse_members": importIncoming, "inverse_more": incomingMore})
					r.Close()
					prior, _ := H.Uint64()
					prior--
					historical, err := retained.Read(op, driver.View{Incarnation: received.Report.Restored.Incarnation, At: ptrRevision(model.Natural64(prior))})
					if err != nil {
						t.Fatal(err)
					}
					_, _, parentErr := historical.(driver.HierarchyChildrenReader).HierarchyChildren(op, []model.Ref{parentRef}, 2)
					if !errors.Is(parentErr, driver.ErrUnsupported) {
						t.Fatal("imported history falsely certified", parentErr)
					}
					_, _, linkErr := historical.(driver.InverseSourceLinksReader).InverseSourceLinks(op, []model.Ref{parentRef}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 2)
					if !errors.Is(linkErr, driver.ErrUnsupported) {
						t.Fatal("imported historical links falsely certified", linkErr)
					}
					graphArtifact(t, "historical-coverage-readback", map[string]any{"snapshot": historical.Snapshot(), "parent_target": parentRef, "parent_error": parentErr.Error(), "inverse_target": parentRef, "inverse_kind": driver.IncomingLinkBlocks, "inverse_error": linkErr.Error()})
					historical.Close()
					work := &graphRequestWork{}
					observed := &graphObservedStore{Store: retained, work: work}
					ontology := service.NewOntology(nil)
					writer := pipeline.New(observed, lease, pipeline.WithOntology(ontology), pipeline.WithObserver(work))
					domain := service.New(observed, lease, writer, service.WithOntology(ontology))
					registry, err := rpc.NewRegistry()
					if err != nil {
						t.Fatal(err)
					}
					if err = rpc.BindService(registry, domain); err != nil {
						t.Fatal(err)
					}
					server, err := rpc.New(registry, rpc.Config{DefaultProject: input.Projects[0], Incarnation: received.Report.Restored.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					auth, err := authn.New(authn.Config{})
					if err != nil {
						t.Fatal(err)
					}
					handler, err := httpjson.New(server, auth, httpjson.Config{})
					if err != nil {
						t.Fatal(err)
					}
					call := func(method string, params map[string]any, name string) (model.Commit, *model.ErrorData) {
						t.Helper()
						params["_meta"] = map[string]any{"idempotencyKey": graphcertfixture.Hash([]byte(t.Name() + name)), "deadlineMs": 10000}
						raw := graphcertfixture.JSON(t, map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
						request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(raw))
						request.Header.Set("Content-Type", "application/json")
						request.Header.Set("Plug-RPC", "1")
						request.Host = "127.0.0.1"
						request.RemoteAddr = "127.0.0.1:12345"
						response := httptest.NewRecorder()
						started := time.Now()
						handler.ServeHTTP(response, request)
						finished := time.Now()
						var envelope struct {
							Result model.Commit    `json:"result"`
							Error  json.RawMessage `json:"error"`
						}
						graphcertfixture.Decode(t, response.Body.Bytes(), &envelope)
						graphArtifact(t, name, map[string]any{"http_attempt": 1, "request": json.RawMessage(raw), "response": json.RawMessage(response.Body.Bytes()), "http_status": response.Code, "started": started, "finished": finished, "elapsed_ms": float64(finished.Sub(started)) / float64(time.Millisecond), "pipeline_attempts": work.Attempts, "observed_deadlines": work.Deadlines, "query_calls": work.Queries, "bead_rows_returned": work.Beads, "ordinary_preparation_budget_ms": 2000})
						if response.Code != http.StatusOK {
							t.Fatal("RPC HTTP status", response.Code)
						}
						if len(envelope.Error) == 0 || string(envelope.Error) == "null" {
							return envelope.Result, nil
						}
						var failure struct {
							Data model.ErrorData `json:"data"`
						}
						graphcertfixture.Decode(t, envelope.Error, &failure)
						return envelope.Result, &failure.Data
					}
					created, failed := call("bead.create", map[string]any{"type": "issue", "parent": model.RefByID(parent, model.Natural64(0)), "fields": map[string]any{"title": "first imported child"}}, "first-create")
					if failed != nil || len(created.Results) != 1 || created.Results[0].Ref.ID != parent+".1" || work.Attempts != 1 || work.Beads != 0 {
						t.Fatal("first create must succeed once without full graph read", created, work)
					}
					preparationDeadline := false
					for _, deadline := range work.Deadlines {
						if deadline.RemainingMS > 1900 && deadline.RemainingMS <= 2000 {
							preparationDeadline = true
						}
					}
					if !preparationDeadline {
						t.Fatal("missing actual unchanged 2s preparation deadline", work.Deadlines)
					}
					child := created.Results[0].Ref
					r = graphFreshReader(t, retained)
					rows, err = r.Get(op, []model.Ref{model.RefByID(parent, model.Natural64(0)), child})
					if err != nil || len(rows) != 2 {
						t.Fatal("fresh parent/child readback", err)
					}
					var childDoc pipeline.Document
					for _, row := range rows {
						if row.Ref.ID == child.ID && row.Ref.Branch.IsZero() == child.Branch.IsZero() {
							childDoc, err = pipeline.DecodeDocument(row.Body)
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					if childDoc.Parent == nil || childDoc.Parent.ID != parent {
						t.Fatal("first parent-child link missing", childDoc.Parent)
					}
					children, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(op, []model.Ref{model.RefByID(parent, model.Natural64(0))}, 2)
					if err != nil || more || len(children) != 1 || children[0].Ref.ID != child.ID || children[0].Ref.Branch.IsZero() != child.Branch.IsZero() {
						t.Fatal("durable parent membership", err)
					}
					allocator, err := r.(driver.AllocatorReader).LoadAllocator(op, input.Projects[0])
					if err != nil || allocator.Children[parent] != 1 {
						t.Fatal("durable first child number", err, allocator.Children[parent])
					}
					graphArtifact(t, "fresh-readback", map[string]any{"snapshot": r.Snapshot(), "rows": rows, "children": children, "child_number": allocator.Children[parent], "import_head": H, "incarnation": received.Report.Restored.Incarnation})
					r.Close()
					work.mu.Lock()
					work.Attempts, work.Queries, work.Beads = 0, 0, 0
					work.Deadlines = nil
					work.mu.Unlock()
					linked, linkFailed := call("bead.link", map[string]any{"from": child, "to": model.RefByID(archive.Records("bead")[0].ID, model.Natural64(0)), "kind": "blocks"}, "explicit-link")
					if work.Queries == 0 {
						t.Fatal("explicit link did not take unchanged full-read path")
					}
					if linkFailed != nil {
						if count == 3 {
							t.Fatal("small explicit-link control must succeed", linkFailed)
						}
						if linkFailed.Code != "busy" || !linkFailed.Retryable {
							t.Fatal("unexpected explicit link refusal", linkFailed)
						}
						t.Log("explicit link retained its existing full-read limit; typed busy refusal preserved without retry")
					} else {
						if work.Attempts != 1 || len(linked.Results) != 1 {
							t.Fatal("explicit link must commit on its first attempt", linked, work.Attempts)
						}
						r = graphFreshReader(t, retained)
						linkRows, err := r.Get(op, []model.Ref{child})
						if err != nil || len(linkRows) != 1 {
							t.Fatal("link durable readback", err)
						}
						doc, err := pipeline.DecodeDocument(linkRows[0].Body)
						if err != nil {
							t.Fatal(err)
						}
						found := false
						for _, edge := range doc.Links {
							found = found || edge.Kind == "blocks" && edge.To.ID == archive.Records("bead")[0].ID
						}
						if !found {
							t.Fatal("successful explicit link not durable")
						}
						inverse, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(op, []model.Ref{model.RefByID(archive.Records("bead")[0].ID, model.Natural64(0))}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 4)
						if err != nil || more {
							t.Fatal("fresh inverse link readback", err, more)
						}
						inverseFound := false
						for _, row := range inverse {
							if row.Ref.ID == linkRows[0].Ref.ID && row.Ref.Branch.IsZero() == linkRows[0].Ref.Branch.IsZero() && row.Revision == linked.Revision && bytes.Equal(row.Body, linkRows[0].Body) {
								inverseFound = true
							}
						}
						if !inverseFound {
							t.Fatal("successful link lacks exact durable inverse membership", err, inverse)
						}
						graphArtifact(t, "explicit-link-readback", map[string]any{"snapshot": r.Snapshot(), "commit": linked, "source": linkRows[0], "inverse": inverse, "inverse_more": more, "required_success_control": count == 3})
						r.Close()
					}
				})
			}
		})
	}
}

func ptrRevision(r model.Revision) *model.Revision { return &r }

// Both existing size gates refuse before a destination can be opened. This is
// an import boundary, independent of graph certification. No bound is raised.
func TestNativeReceiveGraphCertificationTwentyThousandPlanRefusal(t *testing.T) {
	if os.Getenv("PLUG_PUBLIC_BUNDLE") == "" {
		t.Skip("requires explicit PLUG_PUBLIC_BUNDLE opt-in")
	}
	capture, _ := graphcertfixture.Capture(t, "../../internal/integration/converterroundtrip/testdata/public-native-delta-small-0e4d5da4.tar.gz", 20000)
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), capture, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.ReleaseNativeDecoded()
	plan := graphcertfixture.Plan(t, archive, driver.CheckpointLifetimeProcess)
	// The receiver applies these provisional values before validating the
	// declaration. Keep the actual request below unchanged (with its empty
	// target/SHA fields), but use the same validation-ready copy for the typed
	// plan-size control so the existing byte-limit refusal is the first error.
	validationPlan := plan
	validationPlan.Target.StoreID, validationPlan.Target.Incarnation = "pending-new-target", "pending-new-target"
	validationPlan.Providers = append([]driver.SourcePlaneProviderV1(nil), plan.Providers...)
	for i := range validationPlan.Providers {
		if validationPlan.Providers[i].LogicalImageSHA256 == "" && validationPlan.Providers[i].LogicalImageSize == 0 {
			validationPlan.Providers[i].LogicalImageSHA256 = strings.Repeat("0", 64)
		}
	}
	encoded, err := driver.EncodeSourcePlanePlanV1(validationPlan)
	if !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), "plan byte limit") || len(encoded) != 0 {
		t.Fatal("20k must retain typed plan-size refusal with no partial encoding", err, len(encoded))
	}
	input := request{Format: "plug-native-receiving-request/1", ArchiveSHA256: archive.SHA256(), SourceStoreID: archive.Manifest().StoreID, Projects: []string{archive.Manifest().Projects[0].Prefix}, Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourcePlan: plan}
	raw := graphcertfixture.JSON(t, input)
	if len(raw) <= driver.SourcePlaneMaxPlanBytes {
		t.Fatal("expected existing complete-request bound at 20k")
	}
	path := filepath.Join(t.TempDir(), "oversized-request.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	opened := 0
	blocked := func(context.Context, string, string) (func() (driver.Store, error), error) {
		opened++
		return nil, errors.New("destination must not open")
	}
	result, receiveErr := runCaptureSetWithOpener(context.Background(), capture, path, "memory", "", "source-built-native-receive-test", "", "", "", blocked)
	if receiveErr == nil || !strings.Contains(receiveErr.Error(), "regular bounded receiving request required") || opened != 0 || result.Report.Receipt != nil {
		t.Fatal("oversized request staged partial output", receiveErr, opened)
	}
	graphArtifact(t, "twenty-thousand-size-refusal", map[string]any{"current_items": 20000, "limit_bytes": driver.SourcePlaneMaxPlanBytes, "typed_canonical_refusal": err.Error(), "partial_canonical_bytes": len(encoded), "json_request_bytes": len(raw), "request_sha256": graphcertfixture.Hash(raw), "receiver_refusal": receiveErr.Error(), "destination_opens": opened, "receipt": result.Report.Receipt})
}
