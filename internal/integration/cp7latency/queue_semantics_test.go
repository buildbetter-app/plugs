package cp7latency_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func queueDomain(f *fixture, now func() time.Time) *service.Service {
	o := service.NewOntology(nil)
	w := pipeline.New(f.store.Store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(now))
	return service.New(f.store.Store, f.lease, w, service.WithOntology(o), service.WithClock(now))
}

func queueFixturePatch(t *testing.T, f *fixture, n int, change func(*pipeline.Document)) {
	t.Helper()
	r := f.reader(t, nil)
	rows, err := r.Get(context.Background(), []model.Ref{beadRef(n)})
	r.Close()
	if err != nil || len(rows) != 1 {
		t.Fatalf("queue setup source %d: %v", n, err)
	}
	var doc pipeline.Document
	if err := json.Unmarshal(rows[0].Body, &doc); err != nil {
		t.Fatal(err)
	}
	change(&doc)
	if _, err := pipeline.HierarchyParent(doc); err != nil {
		t.Fatalf("queue setup has conflicting hierarchy for %d: %v", n, err)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	f.patchSource(t, "queue_dependency", beadRef(n), raw, []string{"/fields", "/links", "/lease"})
}

// This only seeds the existing checkpoint read contract for the queue's lease
// interpreter. It does not invoke or change heartbeat/reclaim behavior.
func queueOverlay(t *testing.T, f *fixture, n int, principal model.PrincipalRef, now time.Time) {
	t.Helper()
	digest := sha256.Sum256([]byte("plug/lease/" + beadRef(n).ID))
	raw, err := json.Marshal(map[string]any{"principal": principal, "expiresAt": now.Add(time.Hour), "ttlMs": 60000,
		"epoch": f.lease.Epoch(), "lastHeartbeat": now})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.Store.(driver.CheckpointStore).SaveCheckpoint(context.Background(), f.lease, "plug/lease/"+hex.EncodeToString(digest[:]), 0, raw)
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueueProjectionMatchesExactReadinessBeforeRotation(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 55)
			now := time.Now().UTC()
			clock := func() time.Time { return now }
			principal := f.agent(1, false)
			queueFixturePatch(t, f, 1, func(d *pipeline.Document) { d.Fields["status"] = "backlog" })
			queueFixturePatch(t, f, 13, func(d *pipeline.Document) { d.Fields["status"] = "done" })
			// waits-for needs a real terminal child and a real open child of
			// 12. Use the production reparent writer so canonical Parent,
			// descendants and closure annotations agree; a contradictory legacy
			// edge is not a valid fixture. Started is open but not ready, while
			// its own open parent does not block the later descendant controls.
			parent := beadRef(12)
			writer := pipeline.New(f.store.Store, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(clock))
			_, err := writer.Apply(context.Background(), principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: beadRef(15), Parent: &parent}}}}, model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: requestKey("queue-parent-setup", 1, engine, 0)})
			if err != nil {
				t.Fatal(err)
			}
			queueFixturePatch(t, f, 15, func(d *pipeline.Document) {
				if d.Parent == nil || !driver.SelectionSameRef(*d.Parent, parent) {
					t.Fatal("production reparent did not publish the open sibling")
				}
				d.Fields["status"] = "in_progress"
				d.Links = []pipeline.Link{{Kind: "parent-child", To: parent}}
			})
			queueFixturePatch(t, f, 17, func(d *pipeline.Document) { d.Fields["template"] = true })
			for _, n := range []int{18, 19, 20, 21} {
				queueFixturePatch(t, f, n, func(d *pipeline.Document) {
					expires := now.Add(-time.Hour)
					if n == 18 {
						expires = now.Add(time.Hour)
					}
					d.Lease = &pipeline.ClaimLease{Principal: principal, ExpiresAt: expires, TtlMs: model.Natural64(60000), Epoch: model.Natural64(f.lease.Epoch())}
				})
			}
			queueOverlay(t, f, 20, principal, now)
			queueOverlay(t, f, 21, model.PrincipalRef{Kind: "user", ID: "stale-owner"}, now)
			// Existing roots isolate satisfied blockers from the separate
			// transitive waits-for controls without changing canonical parents.
			queueFixturePatch(t, f, 23, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "blocks", To: beadRef(42)}} })
			for _, n := range []int{34, 35, 36, 37} {
				queueFixturePatch(t, f, n, func(d *pipeline.Document) {
					link := pipeline.Link{Kind: "waits-for", To: beadRef(12)}
					if n != 37 {
						gate := "any-children"
						if n == 35 {
							gate = "all-children"
						}
						link.Metadata = &model.LinkMetadata{Gate: gate, AlsoBlocks: n == 36}
					}
					d.Links = []pipeline.Link{link}
				})
			}
			// The same canonical parent carries 37's unsatisfied waits-for
			// dependency into this subtree. Both parent spellings must agree.
			queueFixturePatch(t, f, 38, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "parent-child", To: beadRef(37)}} })
			queueFixturePatch(t, f, 39, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "conditional-blocks", To: beadRef(40)}} })
			queueFixturePatch(t, f, 40, func(d *pipeline.Document) { d.Fields["status"] = "failed" })
			queueFixturePatch(t, f, 41, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "blocks", To: beadRef(42)}} })
			queueFixturePatch(t, f, 42, func(d *pipeline.Document) { d.Fields["status"] = "done" })
			queueFixturePatch(t, f, 43, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "blocks", To: beadRef(999)}} })
			queueFixturePatch(t, f, 45, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "conditional-blocks", To: beadRef(40)}} })
			queueFixturePatch(t, f, 46, func(d *pipeline.Document) {
				d.Fields["priority"] = 2
				d.Fields["extra"] = "retained source field"
				d.Labels = []string{"retained-label"}
				d.Metadata = map[string]json.RawMessage{"custom": json.RawMessage(`{"v":1}`)}
			})
			queueFixturePatch(t, f, 49, func(d *pipeline.Document) { d.Links = []pipeline.Link{{Kind: "conditional-blocks", To: beadRef(50)}} })
			queueFixturePatch(t, f, 50, func(d *pipeline.Document) { d.Fields["status"] = "done" })
			domain := queueDomain(f, clock)
			req := service.ReadyRequest{Project: projectName, Limit: 1000}
			reference, err := domain.Ready(context.Background(), principal, req)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(reference.Items))
			for i, bead := range reference.Items {
				ids[i] = bead.ID
			}
			for _, n := range []int{14, 19, 21, 23, 34, 45, 46} {
				if !slices.Contains(ids, beadRef(n).ID) {
					t.Fatalf("reference lost ready dependency control %d", n)
				}
			}
			for _, n := range []int{1, 2, 13, 15, 17, 18, 20, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 49, 50, 51} {
				if slices.Contains(ids, beadRef(n).ID) {
					t.Fatalf("reference admitted blocked control %d", n)
				}
			}
			for _, seed := range []string{"queue-one", "queue-two", "queue-three"} {
				req.ClaimNextSeed = seed
				var counts counters
				ctx := driver.WithReadWorkObserver(context.Background(), &counts)
				page, err := domain.ClaimNextReadyPage(ctx, principal, req)
				if err != nil {
					t.Fatal(err)
				}
				want := queueRotateReference(t, reference.Items, seed, now)
				if !reflect.DeepEqual(page.Items, want) || page.Next != nil {
					t.Fatal("queue changed complete READY rotation or hydrated fields/metadata/labels")
				}
				if counts.value().Queries != 0 || counts.work().SelectorKeys < int64(f.n) || counts.work().PrefixStates < int64(f.n) {
					t.Fatalf("queue silently fell back or hid thin work: %+v %+v", counts.value(), counts.work())
				}
			}
		})
	}
}

// The fixture has real, fixed creation instants and no persisted rank rows
// before this read. Apply the reference hybrid-key rule at the captured clock;
// rotate only ties in bucket, rank and creation time, preserving every member.
func queueRotateReference(t *testing.T, items []service.Bead, seed string, at time.Time) []service.Bead {
	t.Helper()
	copy := append([]service.Bead(nil), items...)
	priority := func(b service.Bead) int {
		if b.Fields["priority"] == nil {
			return 0
		}
		value, err := model.Natural(fmt.Sprint(b.Fields["priority"])).Uint64()
		if err != nil || value > 4 {
			t.Fatalf("invalid priority in reference row %s", b.ID)
		}
		return int(value)
	}
	type key struct {
		bucket, rank int
		created      time.Time
	}
	order := func(b service.Bead) key {
		if b.CreatedAt.IsZero() {
			t.Fatalf("reference row %s has no creation instant", b.ID)
		}
		if b.CreatedAt.Before(at.Add(-48 * time.Hour)) {
			return key{bucket: 1, rank: 999, created: b.CreatedAt}
		}
		return key{rank: priority(b), created: b.CreatedAt}
	}
	for start := 0; start < len(copy); {
		groupKey := order(copy[start])
		end := start + 1
		for end < len(copy) {
			next := order(copy[end])
			if next.bucket != groupKey.bucket || next.rank != groupKey.rank || !next.created.Equal(groupKey.created) {
				break
			}
			end++
		}
		digest := sha256.Sum256([]byte(seed + "\x00" + fmt.Sprint(priority(copy[start]))))
		offset := int(binary.BigEndian.Uint64(digest[:8]) % uint64(end-start))
		group := append([]service.Bead(nil), copy[start:end]...)
		for i := start; i < end; i++ {
			copy[i] = group[(i-start+offset)%len(group)]
		}
		start = end
	}
	return copy
}

func TestQueueProjectionPublicClaimAndLegacyFallback(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 2200)
			req := service.ClaimNextRequest{Project: projectName}
			identity, err := service.ClaimNextRequestIdentity(req)
			if err != nil {
				t.Fatal(err)
			}
			key := requestKey("queue-bounded", 2, "public", 0)
			seed := service.ClaimNextSelectionSeed(identity, key)
			digest := sha256.Sum256([]byte(seed + "\x000"))
			offset := int(binary.BigEndian.Uint64(digest[:8]) % uint64(f.n))
			winner := offset + 1
			for winner%2 != 0 {
				winner = winner%f.n + 1
			}
			p := f.agent(2, false)
			meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: key}
			var counts counters
			deadline, cancel := context.WithTimeout(context.Background(), requestDeadline)
			out, err := f.app.Service.ClaimNextWithOutcome(driver.WithReadWorkObserver(deadline, &counts), p, req, meta)
			cancel()
			if err != nil || out == nil || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != beadRef(winner).ID {
				t.Fatalf("real guarded ClaimNext changed raw-ready winner or authority: winner=%d out=%+v err=%v", winner, out, err)
			}
			api, work := counts.value(), counts.work()
			if api.Queries != 0 || work.SelectorKeys < int64(f.n) || work.PrefixStates < int64(f.n) || work.PrefixBytes == 0 || work.Payloads == 0 || work.Payloads >= int64(f.n) {
				t.Fatalf("public claim hid thin census or materialized the full corpus: api=%+v work=%+v", api, work)
			}
			f.checkClaim(t, p, out, meta, 2)
			t.Logf("W8_QUEUE_PUBLIC full_guarded_path=true selected_page=1000 corpus=%d winner=%d api=%+v work=%+v thin_work=O(N) SQL_executor=UNMEASURED SLO=UNQUALIFIED", f.n, winner, api, work)
			t.Run("PostClaimQueueProjectionReadback", func(t *testing.T) {
				r := f.reader(t, nil)
				defer r.Close()
				projection, err := r.(driver.QueueProjectionReader).QueueProjection(context.Background())
				if err != nil {
					t.Errorf("canonical public claim lost complete committed queue source: %v", err)
					return
				}
				if projection.View.At == nil || projection.View.At.String() != out.Commit.Revision.String() || len(projection.Beads) != f.n {
					t.Errorf("post-claim queue source has incomplete domain or wrong snapshot: %+v beads=%d", projection.View, len(projection.Beads))
				}
				for _, bead := range projection.Beads {
					if bead.Bead.Ref.ID == beadRef(winner).ID {
						if !bead.Facts.LeasePresent || bead.Source.Revision.String() != out.Commit.Revision.String() {
							t.Errorf("actual claimed image lacks queue lease/revision binding: %+v", bead)
						}
						return
					}
				}
				t.Error("complete post-claim queue source omitted the claimed bead")
			})
			// Empty membership must still execute the fresh post-Head committed
			// census, rather than treating a missing accelerator as no work.
			counts.reset()
			emptyReq := service.ClaimNextRequest{Project: projectName, Filter: &service.Filter{Types: []string{"no-such-type"}}}
			emptyMeta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: requestKey("queue-empty", 1, "public", 0)}
			empty, err := f.app.Service.ClaimNextWithOutcome(driver.WithReadWorkObserver(context.Background(), &counts), f.agent(1, false), emptyReq, emptyMeta)
			if err != nil || empty == nil || !empty.NullResult || empty.Commit.Revision.IsZero() || len(empty.Commit.Results) != 0 {
				t.Fatalf("public terminal empty receipt: %+v %v", empty, err)
			}
			t.Logf("W8_QUEUE_TERMINAL full_guarded_path=true api=%+v work=%+v validation=%+v", counts.value(), counts.work(), counts.validation.work())
			if counts.value().Queries != 0 || counts.work().SelectorKeys < 3*int64(f.n) {
				t.Fatalf("terminal empty skipped fresh complete census: %+v %+v", counts.value(), counts.work())
			}
			// One explicit-null lease is deliberately unknown. It must use the
			// exact interpreter and expose every fallback source materialization.
			legacy := strings.TrimSuffix(string(body(t, f.n)), "}") + `,"lease":null}`
			f.patchSource(t, "queue_legacy", beadRef(f.n), []byte(legacy), []string{"/lease"})
			r := f.reader(t, nil)
			_, projectionErr := r.(driver.QueueProjectionReader).QueueProjection(context.Background())
			r.Close()
			if !errors.Is(projectionErr, driver.ErrSelectionUnavailable) {
				t.Fatalf("same-version ready marker certified a legacy queue annotation: %v", projectionErr)
			}
			counts.reset()
			domain := queueDomain(f, time.Now)
			page, err := domain.ClaimNextReadyPage(driver.WithReadWorkObserver(context.Background(), &counts), f.agent(1, false), service.ReadyRequest{Project: projectName, Limit: 1, Filter: &service.Filter{Resources: []string{beadRef(f.n).ID}}})
			if err != nil || len(page.Items) != 1 || page.Items[0].ID != beadRef(f.n).ID {
				t.Fatalf("legacy fallback lost actual ready bead: %+v %v", page, err)
			}
			if counts.value().Queries == 0 || counts.work().Payloads < int64(f.n) {
				t.Fatalf("legacy fallback concealed full source-image work: %+v %+v", counts.value(), counts.work())
			}
			t.Logf("W8_QUEUE_LEGACY fallback=exact all_request_api=%+v work=%+v", counts.value(), counts.work())
		})
	}
}
