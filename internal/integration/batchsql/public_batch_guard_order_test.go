package batchsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const publicBatchAgent = "i1-public-batch-guard-order"

type publicBatchReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

// Observe the very same Writer outcome used by the public service. Private
// domain messages intentionally become canonical catalog messages on the wire;
// this passive observer proves the precise graph guard without exposing it.
type publicBatchWriter struct {
	*pipeline.Writer
	mu    sync.Mutex
	calls int
	err   error
}

func (w *publicBatchWriter) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	outcome, err := w.Writer.ApplyWithOutcome(ctx, by, cs, meta)
	w.mu.Lock()
	w.calls++
	w.err = err
	w.mu.Unlock()
	return outcome, err
}

func (w *publicBatchWriter) observed() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls, w.err
}

type publicBatchHost struct {
	*httptest.Server
	writer *publicBatchWriter
}

// Public no-plugin mode retains the real ontology, graph and revision guards.
// In particular, a fixed-policy tx.apply refusal would never reach this test's
// two distinct guards. All calls traverse HTTP, BindService and Service.Apply.
func publicBatchEndpoint(t *testing.T, f *fixture) *publicBatchHost {
	t.Helper()
	source := model.RefByID("ontology/public-batch-guard-order", model.Natural64(0))
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: source, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	writer := &publicBatchWriter{Writer: pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID),
		pipeline.WithOntologyAudit(service.NewOntologyAudit(service.StaticOntologySource(source))),
	)}
	domain := service.New(f.store, f.lease, writer, service.WithOntology(f.ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "batch-local"})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(handler)
	t.Cleanup(endpoint.Close)
	return &publicBatchHost{Server: endpoint, writer: writer}
}

func publicBatchCall(t *testing.T, f *fixture, endpoint *publicBatchHost, key string, cs model.ChangeSet) publicBatchReply {
	t.Helper()
	beforeCalls, _ := endpoint.writer.observed()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "guard-order", "method": "tx.apply",
		"params": map[string]any{"changeSet": cs, "_meta": map[string]any{"project": "batch-local", "agent": publicBatchAgent, "idempotencyKey": key}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, endpoint.URL+httpjson.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := endpoint.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var reply publicBatchReply
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&reply); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("public tx.apply HTTP %d: %s (%v)", response.StatusCode, raw, err)
	}
	calls, domainErr := endpoint.writer.observed()
	if calls != beforeCalls+1 || (reply.Error == nil) != (domainErr == nil) {
		t.Fatalf("HTTP call must observe exactly one matching Writer outcome: calls=%d->%d error=%v reply=%s", beforeCalls, calls, domainErr, raw)
	}
	return reply
}

type publicBatchState struct {
	Durable    durableState
	Derived    map[driver.DerivedTable][]driver.DerivedRow
	Predicates []driver.LockState
	Receipt    *driver.IdempotencyRecord
}

func publicBatchRead(t *testing.T, f *fixture, key string) publicBatchState {
	t.Helper()
	state := publicBatchState{Durable: f.state(t), Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: state.Durable.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Enumerate whole tables so an orphan or an unexpected key cannot hide
	// behind a point lookup of only the rows we intended to write.
	for _, table := range []driver.DerivedTable{
		driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure,
		driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins,
	} {
		state.Derived[table], err = r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	for id := range state.Durable.Records {
		sentinel, err := driver.NewBeadSentinel(id)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	state.Predicates, err = r.Observe(f.ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	state.Receipt, err = r.IdempotencyByKey(f.ctx, principal, publicBatchAgent, "tx.apply", key)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func publicBatchCommit(t *testing.T, reply publicBatchReply) model.Commit {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("public batch refused: %+v", reply.Error.Data)
	}
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || len(commit.Results) == 0 {
		t.Fatalf("missing public batch commit: %s (%v)", reply.Result, err)
	}
	return commit
}

func TestPublicBatchRevisionRefusalPrecedesCycleGuardAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine) // TestMain requires disposable real PostgreSQL.
			endpoint := publicBatchEndpoint(t, f)
			link := func(from, to model.Ref) model.Op {
				return model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: from, To: to, LinkKind: "blocks"}}
			}
			seed := publicBatchCommit(t, publicBatchCall(t, f, endpoint, f.meta("seed").IdempotencyKey, model.ChangeSet{Ops: []model.Op{
				createOp("a", "original a", "batch-local"), createOp("b", "original b", "batch-local"), createOp("c", "original c", "batch-local"),
				link(model.RefByKey("a"), model.RefByKey("b")),
			}}))
			a, b, c := seed.Results[0].Ref, seed.Results[1].Ref, seed.Results[2].Ref
			key := f.meta("refuse-then-correct").IdempotencyKey
			before := publicBatchRead(t, f, key)
			if before.Receipt != nil || len(before.Derived[driver.DerivedAncestorClosure]) == 0 || len(before.Durable.Log[len(before.Durable.Log)-1].Events) != 3 {
				t.Fatal("fixture must have real persisted derived rows/events and an unused request key")
			}
			batch := model.ChangeSet{Ops: []model.Op{
				patchOp(a, map[string]any{"title": "accepted prefix"}), commentOp(b, "accepted once"), link(b, a),
			}, Expect: []model.Precondition{model.PreconditionByRevision(a, model.Natural64(0))}}
			for _, refusal := range []struct {
				name string
				code model.ErrorCode
			}{{"stale-revision-before-cycle", "precondition_failed"}, {"current-revision-reaches-cycle", "ontology_guard_failed"}} {
				if !t.Run(refusal.name, func(t *testing.T) {
					reply := publicBatchCall(t, f, endpoint, key, batch)
					if reply.Error == nil || reply.Error.Data.Code != refusal.code || reply.Error.Data.Retryable || len(reply.Result) != 0 || reply.Meta.Replayed {
						t.Fatalf("want pure %s refusal, got %+v result=%s", refusal.code, reply.Error, reply.Result)
					}
					if refusal.code == "precondition_failed" {
						current, ok := reply.Error.Data.Details["currentRevision"].(json.Number)
						if reply.Error.Data.Details["reason"] != "revision" || !ok || current.String() != seed.Revision.String() {
							t.Fatalf("revision refusal did not reach real committed CAS: %+v", reply.Error.Data)
						}
					} else {
						var domain *model.Error
						_, err := endpoint.writer.observed()
						if !errors.As(err, &domain) || domain.Data.Code != refusal.code || domain.Message != "blocking link would form a cycle" {
							t.Fatalf("correcting only the expectation missed the actual Writer cycle guard: %v", err)
						}
					}
					if after := publicBatchRead(t, f, key); !reflect.DeepEqual(before, after) {
						t.Fatalf("refused batch published records/derived/predicates/versions/history/events/receipt: before=%+v after=%+v", before, after)
					}
				}) {
					return
				}
				batch.Expect = []model.Precondition{model.PreconditionByRevision(a, seed.Revision)}
			}
			batch.Ops[2] = link(b, c) // Only the cycle-causing endpoint changes.
			first := publicBatchCall(t, f, endpoint, key, batch)
			commit := publicBatchCommit(t, first)
			if first.Meta.Replayed || commit.Revision.String() != "3" || len(commit.Results) != 3 || commit.Results[0].Ref.ID != a.ID || commit.Results[1].Ref.ID != b.ID || commit.Results[2].Ref.ID != b.ID {
				t.Fatalf("corrected batch did not commit once in operation order: %+v", commit)
			}
			for _, result := range commit.Results {
				if result.Revision != commit.Revision {
					t.Fatal("batch result did not share its single committed revision")
				}
			}
			after := publicBatchRead(t, f, key)
			if after.Receipt == nil || after.Receipt.ResponsePending || !reflect.DeepEqual(after.Receipt.Commit, commit) || len(after.Durable.Log) != len(before.Durable.Log)+1 || len(after.Durable.Versions[a.ID]) != 2 || len(after.Durable.Versions[b.ID]) != 2 || len(after.Durable.Versions[c.ID]) != 1 || !reflect.DeepEqual(after.Durable.Records[c.ID], before.Durable.Records[c.ID]) {
				t.Fatal("corrected batch split receipt/history/versions or wrote the unchanged endpoint")
			}
			docA, docB := decodeDocument(t, after.Durable.Records[a.ID]), decodeDocument(t, after.Durable.Records[b.ID])
			if docA.Fields["title"] != "accepted prefix" || len(docB.Comments) != 1 || docB.Comments[0].Body != "accepted once" || len(docB.Links) != 1 || docB.Links[0].To.ID != c.ID || docB.Links[0].Kind != "blocks" {
				t.Fatal("corrected batch lost or duplicated its patch/comment/link")
			}
			last := after.Durable.Log[len(after.Durable.Log)-1]
			if last.Revision != commit.Revision || len(last.Changes) != 2 || len(last.Events) != 2 {
				t.Fatalf("corrected batch did not persist one complete changeset/event plan: %+v", last)
			}
			for i, event := range last.Events {
				if event.Index != model.Natural64(uint64(i)) || event.Resource.Ref.ID != last.Changes[i].Ref.ID || event.Provenance.Method != "tx.apply" || event.Provenance.Cause.Root != key {
					t.Fatalf("batch event escaped its committed change/request: %+v", event)
				}
			}
			replay := publicBatchCall(t, f, endpoint, key, batch)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) || !reflect.DeepEqual(after, publicBatchRead(t, f, key)) {
				t.Fatal("exact replay changed the response, graph, derived rows, history, events or receipt")
			}
			t.Logf("I1 public %s: stale revision precedes cycle guard; both refusals publish nothing across all nine derived tables, records, predicates, versions, history, events and receipt; corrected same-key batch commits once and replays", engine)
		})
	}
}
