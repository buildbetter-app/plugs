package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type recoveryFixture struct {
	store  driver.Store
	lease  driver.ActiveLease
	svc    *Service
	writer *pipeline.Writer
	now    time.Time
	dsn    string
}

func recoveryStores(t *testing.T, run func(*testing.T, *recoveryFixture)) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := &recoveryFixture{now: time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)}
			if engine == "memory" {
				f.store = memory.New()
			} else {
				f.dsn = filepath.Join(t.TempDir(), "recovery.db")
				var err error
				f.store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: f.dsn})
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				_ = f.lease.Release(context.Background())
				_ = f.store.Close()
			})
			f.start(t)
			run(t, f)
		})
	}
}

func (f *recoveryFixture) start(t *testing.T) {
	t.Helper()
	var err error
	f.lease, err = f.store.Lease(context.Background(), "recovery-integration", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	o := NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
	f.svc = New(f.store, f.lease, f.writer, WithOntology(o), WithClock(func() time.Time { return f.now }))
}

func (f *recoveryFixture) claim(t *testing.T) model.Ref {
	t.Helper()
	created, err := f.svc.Create(context.Background(), anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "actual claim"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Results[0].Ref
	if _, err := f.svc.Claim(context.Background(), anonymous, ClaimRequest{ID: id}, meta(t)); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f *recoveryFixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	m, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Capture the complete stored bead, versions, log, predicates and the request's
// receipt. Refusals and replays must preserve all of these, not just the status.
func (f *recoveryFixture) capture(t *testing.T, id model.Ref, by model.PrincipalRef, key model.WriteMeta) string {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	rows, err := r.Get(context.Background(), []model.Ref{id})
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	versions, err := r.Versions(context.Background(), id, driver.PageReq{Limit: 1000})
	if err != nil || versions.NextCursor != nil {
		t.Fatalf("versions=%v err=%v", versions, err)
	}
	log, err := r.Changesets(context.Background(), nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	sentinel, _ := driver.NewBeadSentinel(id.ID)
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock, {Kind: driver.LockSentinel, Sentinel: sentinel}, driver.OntologySentinelLock, driver.PluginsSentinelLock}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	states, err := r.Observe(context.Background(), keys)
	if err != nil {
		t.Fatal(err)
	}
	principal, _ := driver.PrincipalKey(by)
	agent := ""
	if by.Kind == "anonymous" {
		agent = key.AgentLabel
	}
	receipt, err := r.IdempotencyByKey(context.Background(), principal, agent, "bead.release", key.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal([]any{r.Snapshot(), rows, versions, log, states, receipt})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestClaimRecoveryRefusalsAndDryRunPreserveCompleteState(t *testing.T) {
	recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
		ctx := context.Background()
		id := f.claim(t)
		refuse := func(name string, by model.PrincipalRef, code model.ErrorCode, call func(model.WriteMeta) error) {
			t.Helper()
			t.Run(name, func(t *testing.T) {
				key := meta(t)
				before := f.capture(t, id, by, key)
				wantCode(t, call(key), code)
				if after := f.capture(t, id, by, key); after != before {
					t.Fatal("refusal changed stored state or receipt")
				}
			})
		}
		refuse("live-core-recovery", claimRecoverySystem, "lease_expired", func(key model.WriteMeta) error {
			_, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key)
			return err
		})
		refuse("foreign-anonymous-label", anonymous, "lease_not_held", func(key model.WriteMeta) error {
			key.Agent, key.AgentLabel = "bob", "bob"
			_, err := f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id}, key)
			return err
		})
		foreign := model.PrincipalRef{Kind: "user", ID: "bob"}
		refuse("foreign-principal-lower-writer", foreign, "lease_not_held", func(key model.WriteMeta) error {
			_, err := f.writer.ApplyClaimRelease(ctx, foreign, id, "", key)
			return err
		})
		refuse("generic-writer-cannot-release", claimRecoverySystem, "invalid_params", func(key model.WriteMeta) error {
			key.Method = "bead.release"
			_, err := f.writer.ApplyWithOutcome(ctx, claimRecoverySystem, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: id, Fields: map[string]any{}}}}}, key)
			return err
		})
		refuse("direct-status-patch", anonymous, "ontology_use_slot_verb", func(key model.WriteMeta) error {
			_, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"status": "todo"}}, key)
			return err
		})
		for _, age := range []time.Duration{5 * time.Minute, 15 * time.Minute} {
			f.now = time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC).Add(age)
			refuse("expiry-or-grace-"+age.String(), claimRecoverySystem, "lease_expired", func(key model.WriteMeta) error {
				_, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key)
				return err
			})
		}
		refuse("expired-holder-release", anonymous, "lease_expired", func(key model.WriteMeta) error {
			_, err := f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id}, key)
			return err
		})
		refuse("expired-foreign-direct-claim", anonymous, "invalid_params", func(key model.WriteMeta) error {
			key.Agent, key.AgentLabel = "bob", "bob"
			_, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, key)
			return err
		})
		f.now = f.now.Add(time.Nanosecond)
		key := meta(t)
		key.DryRun = true
		before := f.capture(t, id, claimRecoverySystem, key)
		dry, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key)
		if err != nil || !dry.Commit.WouldCommit || dry.Replayed {
			t.Fatalf("dry=%+v err=%v", dry, err)
		}
		if after := f.capture(t, id, claimRecoverySystem, key); after != before {
			t.Fatal("dry recovery published state")
		}
	})
}

func TestClaimRecoveryDurableEventReplayAndNormalClaimAcrossStores(t *testing.T) {
	recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
		ctx := context.Background()
		id := f.claim(t)
		// Same-holder reuse still stages nothing and keeps the original expiry.
		key := meta(t)
		before := f.capture(t, id, anonymous, key)
		if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, key); err != nil {
			t.Fatal(err)
		}
		if after := f.capture(t, id, anonymous, key); after != before {
			t.Fatal("same-holder claim changed graph state")
		}
		f.now = f.now.Add(15*time.Minute + time.Nanosecond)
		key = meta(t)
		request := ReleaseRequest{ID: id, Reason: "stale worker recovered"}
		out, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, request, key)
		if err != nil || out.Replayed {
			t.Fatalf("recovery=%+v err=%v", out, err)
		}
		r := f.reader(t)
		rows, err := r.Get(ctx, []model.Ref{id})
		if err != nil || len(rows) != 1 {
			t.Fatalf("rows=%v err=%v", rows, err)
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil || doc.Lease != nil || doc.Fields["status"] != "todo" || len(doc.Comments) != 1 || doc.Comments[0].Principal.ID != "core" || doc.Comments[0].Body != request.Reason {
			t.Fatalf("doc=%+v err=%v", doc, err)
		}
		log, err := r.Changesets(ctx, nil, 100)
		_ = r.Close()
		if err != nil || len(log) != 3 {
			t.Fatalf("log=%v err=%v", log, err)
		}
		e := log[2]
		if e.Principal.Kind != "system" || e.Principal.ID != "core" || len(e.Events) != 1 || e.Events[0].Type != model.EventBeadLeaseReclaimed || e.Events[0].Provenance.Method != "bead.release" || e.Events[0].Provenance.Cause.Root != key.IdempotencyKey || len(e.Chain) != 0 {
			t.Fatalf("event=%+v", e)
		}
		bob := meta(t)
		bob.Agent, bob.AgentLabel = "bob", "bob"
		if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, bob); err != nil {
			t.Fatal(err)
		}
		before = f.capture(t, id, claimRecoverySystem, key)
		replay, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, request, key)
		a, _ := json.Marshal(out.Commit)
		b, _ := json.Marshal(replay.Commit)
		if err != nil || !replay.Replayed || string(a) != string(b) || f.capture(t, id, claimRecoverySystem, key) != before {
			t.Fatalf("replay=%+v err=%v", replay, err)
		}
		_, err = f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id, Reason: "different"}, key)
		wantCode(t, err, "idempotency_key_reused")
		if f.capture(t, id, claimRecoverySystem, key) != before {
			t.Fatal("key reuse changed state")
		}
		if f.dsn != "" {
			_ = f.lease.Release(ctx)
			_ = f.store.Close()
			f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: f.dsn})
			if err != nil {
				t.Fatal(err)
			}
			f.start(t)
			replay, err = f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, request, key)
			b, _ = json.Marshal(replay.Commit)
			if err != nil || !replay.Replayed || string(a) != string(b) || f.capture(t, id, claimRecoverySystem, key) != before {
				t.Fatalf("SQLite restart replay=%+v err=%v", replay, err)
			}
		}
	})
}

type recoveryValidator func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (fn recoveryValidator) Validate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	return fn(ctx, p, r)
}

func TestClaimRecoveryCASRejectsRenewedOrReclaimedAndTakenOverImage(t *testing.T) {
	for _, competitor := range []string{"lease-renewal", "reclaim-and-takeover"} {
		t.Run(competitor, func(t *testing.T) {
			recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
				ctx := context.Background()
				id := f.claim(t)
				f.now = f.now.Add(15*time.Minute + time.Nanosecond)
				entered, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				validator := recoveryValidator(func(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
					once.Do(func() { close(entered) })
					select {
					case <-resume:
						return port.Verdict{Allow: true}, nil
					case <-ctx.Done():
						return port.Verdict{}, ctx.Err()
					}
				})
				slowWriter := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.svc.ontology), pipeline.WithClock(func() time.Time { return f.now }), pipeline.WithValidators(pipeline.ValidatorBinding{ID: "hold-reclaim-before-CAS", Validator: validator}))
				slow := New(f.store, f.lease, slowWriter, WithClock(func() time.Time { return f.now }))
				key := meta(t)
				done := make(chan error, 1)
				go func() {
					_, err := slow.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key)
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("recovery did not reach pre-CAS barrier")
				}
				if competitor == "lease-renewal" {
					r := f.reader(t)
					rows, err := r.Get(ctx, []model.Ref{id})
					_ = r.Close()
					if err != nil || len(rows) != 1 {
						t.Fatal(err)
					}
					doc, err := pipeline.DecodeDocument(rows[0].Body)
					if err != nil {
						t.Fatal(err)
					}
					doc.Lease.ExpiresAt = f.now.Add(5 * time.Minute)
					body, _ := json.Marshal(doc)
					// This is an actual driver CAS competitor, changing only lease
					// expiry on the service-produced claim. It is not a heartbeat
					// implementation or a synthetic status/recovery fixture.
					links := make([]driver.SourceLink, len(doc.Links))
					for i, link := range doc.Links {
						links[i] = driver.SourceLink{Kind: link.Kind, To: link.To}
					}
					_, err = conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.now, Principal: anonymous, Method: "test.lease-renewal"}, []conformance.FixtureChange{{Ref: id, Kind: "bead", Op: model.OpPatch, Body: body, ChangedPaths: []string{"/lease/expiresAt"}, Graph: &conformance.FixtureGraphSource{Parent: doc.Parent, Links: links}}})
					if err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, meta(t)); err != nil {
						t.Fatal(err)
					}
					bob := meta(t)
					bob.Agent, bob.AgentLabel = "bob", "bob"
					if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, bob); err != nil {
						t.Fatal(err)
					}
				}
				before := f.capture(t, id, claimRecoverySystem, key)
				close(resume)
				select {
				case err := <-done:
					wantCode(t, err, "lease_expired")
				case <-time.After(time.Second):
					t.Fatal("stale recovery did not finish")
				}
				if f.capture(t, id, claimRecoverySystem, key) != before {
					t.Fatal("stale recovery overwrote winning lease or published a receipt")
				}
				t.Log("the current record CAS rejected the stale recovery; competitor lease and full durable state survived")
			})
		})
	}
}

type releaseAuthorizer func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error

func (fn releaseAuthorizer) Authorize(ctx context.Context, stage port.AuthorizationStage, p port.ProposedChange, r port.SnapshotReader) error {
	return fn(ctx, stage, p, r)
}

type releaseContext struct{}

func (releaseContext) EventContext(context.Context, port.ProposedChange, port.SnapshotReader) (port.EventContext, error) {
	return port.EventContext{Cause: model.EventCause{Root: "trusted-release-root", Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{"authz/project"}}, nil
}

func TestClaimReleaseAuthorizesAndRetainsTrustedEventContext(t *testing.T) {
	recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
		ctx := context.Background()
		id := f.claim(t)
		auth := releaseAuthorizer(func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error {
			return nil
		})
		configure := func(options ...pipeline.Option) {
			base := []pipeline.Option{pipeline.WithOntology(f.svc.ontology), pipeline.WithClock(func() time.Time { return f.now }), pipeline.WithAuthorizer(auth)}
			f.svc.writer = pipeline.New(f.store, f.lease, append(base, options...)...)
		}
		configure()
		key := meta(t)
		before := f.capture(t, id, anonymous, key)
		_, err := f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id}, key)
		wantCode(t, err, "invalid_params")
		if f.capture(t, id, anonymous, key) != before {
			t.Fatal("missing trusted context release changed state")
		}
		configure(pipeline.WithEventContext(releaseContext{}))
		out, err := f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id, Reason: "done for now"}, key)
		if err != nil || out.Replayed {
			t.Fatalf("release=%+v err=%v", out, err)
		}
		r := f.reader(t)
		log, err := r.Changesets(ctx, nil, 100)
		_ = r.Close()
		if err != nil || len(log) != 3 {
			t.Fatalf("log=%v err=%v", log, err)
		}
		e := log[2]
		if len(e.Events) != 1 || e.Events[0].Type != model.EventBeadReleased || e.Events[0].Provenance.Cause.Root != "trusted-release-root" || len(e.Chain) != 1 || e.Chain[0] != "authz/project" {
			t.Fatalf("released event lost trusted attribution: %+v", e)
		}
		auth = releaseAuthorizer(func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error {
			return failure("forbidden", "permission revoked")
		})
		configure(pipeline.WithEventContext(releaseContext{}))
		before = f.capture(t, id, anonymous, key)
		_, err = f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id, Reason: "done for now"}, key)
		wantCode(t, err, "forbidden")
		if f.capture(t, id, anonymous, key) != before {
			t.Fatal("denied release replay changed state")
		}
	})
}

func TestClaimRecoveryPreservesConfiguredReopenGuards(t *testing.T) {
	recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
		ctx := context.Background()
		doc := ontology.DefaultDocument()
		workflow := doc.Workflows["issue"]
		for i := range workflow.Transitions {
			if workflow.Transitions[i].To == workflow.Slots[ontology.Reopen] {
				workflow.Transitions[i].Guards = append(workflow.Transitions[i].Guards, ontology.Guard{Kind: ontology.EvidenceRequired, Kinds: []string{"commit"}})
			}
		}
		doc.Workflows["issue"] = workflow
		compiled, problems := ontology.Compile(doc, nil, nil)
		if len(problems) != 0 {
			t.Fatal(problems)
		}
		o := NewOntology(compiled.Effective)
		f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
		f.svc = New(f.store, f.lease, f.writer, WithOntology(o), WithClock(func() time.Time { return f.now }))
		id := f.claim(t)
		f.now = f.now.Add(15*time.Minute + time.Nanosecond)
		key := meta(t)
		before := f.capture(t, id, claimRecoverySystem, key)
		_, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key)
		wantCode(t, err, "ontology_guard_failed")
		if f.capture(t, id, claimRecoverySystem, key) != before {
			t.Fatal("recovery bypassed configured reopen guard")
		}
	})
}
