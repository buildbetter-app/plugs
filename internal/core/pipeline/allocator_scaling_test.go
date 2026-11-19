package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type allocatorWorkCounts struct {
	mu     sync.Mutex
	counts map[driver.ReadWorkKind]int64
}

func (c *allocatorWorkCounts) ObserveReadWork(k driver.ReadWorkKind, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[k] += n
}

// Seed through a single ordinary storage transaction, outside the measured
// preparation. The IDs are fixed HASH-shaped top-level identities; importing
// them does not mint N intermediate public requests or warm a preparation cache.
func seedAllocatorScaleBeads(t *testing.T, h *harness, n int) {
	t.Helper()
	seedAllocatorScaleStore(t, h.store, h.lease, n)
}
func seedAllocatorScaleStore(t *testing.T, store driver.Store, lease driver.ActiveLease, n int) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	refs := make([]model.Ref, n)
	keys := make([]driver.LockKey, n)
	locks := make(driver.LockSet, n)
	sentinels := make([]driver.Sentinel, n)
	changes := make([]model.ChangeRecord, n)
	for i := range refs {
		refs[i] = model.Ref{ID: fmt.Sprintf("demo-%08x", i)}
		keys[i] = driver.LockKey{Kind: driver.LockRecord, Ref: &refs[i]}
		locks[i] = driver.LockRequest{Key: keys[i], Mode: driver.LockUpdate}
		sentinels[i], err = driver.NewBeadSentinel(refs[i].ID)
		if err != nil {
			t.Fatal(err)
		}
		sentinelKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinels[i]}
		keys = append(keys, sentinelKey)
		locks = append(locks, driver.LockRequest{Key: sentinelKey, Mode: driver.LockUpdate})
		changes[i] = model.ChangeRecord{Ref: refs[i], Op: model.OpCreate, FromRevision: model.Natural64(0)}
	}
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		body, err := json.Marshal(Document{ID: ref.ID, Project: "demo", Type: "issue", Fields: map[string]any{"title": "seed"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	metadata, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: metadata.Incarnation, Kind: model.ChangesetWrite, Principal: by, Changes: changes, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
}

func TestHashAllocatorPrepareScaling(t *testing.T) {
	for _, n := range []int{1000, 5000, 20000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			h := setup(t)
			seedAllocator(t, h.store, h.lease, driver.NewHashAllocator("demo", 3, 8, 0.25))
			seedAllocatorScaleBeads(t, h, n)
			reader := h.read(t)
			counts := &allocatorWorkCounts{counts: map[driver.ReadWorkKind]int64{}}
			ctx := driver.WithReadWorkObserver(context.Background(), counts)
			request := meta(701)
			request.Project = "demo"
			writer := h.writer()
			cold := &allocatorWorkCounts{counts: map[driver.ReadWorkKind]int64{}}
			if _, err := writer.prepare(driver.WithReadWorkObserver(context.Background(), cold), reader, by, model.ChangeSet{Ops: []model.Op{create("cold")}}, request, nil); err != nil {
				t.Fatal(err)
			}
			if cold.counts[driver.ReadAllocatorDecodes] != int64(n) {
				t.Fatalf("cold census decodes=%d want=%d", cold.counts[driver.ReadAllocatorDecodes], n)
			}
			t.Logf("HASH_COLD n=%d allocator_decodes=%d", n, cold.counts[driver.ReadAllocatorDecodes])
			start := time.Now()
			p, err := writer.prepare(ctx, reader, by, model.ChangeSet{Ops: []model.Op{create("probe")}}, request, nil)
			elapsed := time.Since(start)
			t.Logf("HASH_PREPARE n=%d elapsed=%s allocator_decodes=%d payload_records=%d work=%v", n, elapsed, counts.counts[driver.ReadAllocatorDecodes], counts.counts[driver.ReadPayloadRecords], counts.counts)
			if err != nil {
				t.Fatal(err)
			}
			if p.beadsLoaded || len(p.beads) != 0 {
				t.Fatal("warm create scanned beads")
			}
			if len(p.writes) != 1 {
				t.Fatalf("prepared writes=%d", len(p.writes))
			}
			// The old implementation decodes exactly N beads, independent of paging.
			// A warm Writer must reuse its exact-head census plus candidate lookups.
			if got := counts.counts[driver.ReadAllocatorDecodes]; got != 0 {
				t.Fatalf("per-create full census: allocator decodes=%d; want 0", got)
			}

		})
	}
}

// Compare a warmed Writer against a separate cold Writer using the same census
// snapshot. No fixture supplies a population or expected ID.
func TestHashPopulationIDEquivalenceAtLengthThresholds(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for length := uint32(3); length < 8; length++ {
			for _, n := range []int{19, 21} {
				t.Run(fmt.Sprintf("%s/length%d/n%d", backend, length, n), func(t *testing.T) {
					h := setup(t)
					var store driver.Store = h.store
					lease := h.lease
					if backend == "sqlite" {
						var err error
						store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "threshold.db")})
						if err != nil {
							t.Fatal(err)
						}
						defer store.Close()
						lease, err = store.Lease(context.Background(), "threshold", driver.LeaseTTL)
						if err != nil {
							t.Fatal(err)
						}
						defer lease.Release(context.Background())
					}
					// Place the chosen length's threshold at population 20, so every
					// supported transition is tested without million-record fixtures.
					probability := 1 - math.Exp(-400/(2*math.Pow(36, float64(length))))
					state := driver.NewHashAllocator("demo", length, length+1, probability)
					seedAllocator(t, store, lease, state)
					seedAllocatorScaleStore(t, store, lease, n)
					metadata, err := store.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					reader, err := store.Read(context.Background(), driver.View{Incarnation: metadata.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Close()
					request := meta(702)
					request.Project = "demo"
					w := New(store, lease, WithClock(func() time.Time { return h.now }), WithOntology(testOntology{}))
					cs := model.ChangeSet{Ops: []model.Op{create("probe")}}
					if _, err := w.prepare(context.Background(), reader, by, cs, request, nil); err != nil {
						t.Fatal(err)
					}
					optimized, err := w.prepare(context.Background(), reader, by, cs, request, nil)
					if err != nil {
						t.Fatal(err)
					}
					legacy := New(store, lease, WithClock(func() time.Time { return h.now }), WithOntology(testOntology{}))
					census, err := legacy.prepare(context.Background(), reader, by, cs, request, nil)
					if err != nil {
						t.Fatal(err)
					}
					optimizedID := optimized.proposal.Changes[0].Resource.Ref.ID
					censusID := census.proposal.Changes[0].Resource.Ref.ID
					if optimizedID != censusID {
						t.Fatalf("optimized=%s census=%s", optimizedID, censusID)
					}
					expectedLength := int(length)
					if n > 20 {
						expectedLength++
					}
					if len(optimizedID)-len("demo-") != expectedLength {
						t.Fatalf("ID=%s expected suffix length=%d", optimizedID, expectedLength)
					}
				})
			}
		}
	}
}

func TestHashPopulationStagedDeletesAndReplacements(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.NewHashAllocator("demo", 3, 8, 0.25))
	seedAllocatorScaleBeads(t, h, 5)
	ctx := context.Background()
	request := meta(703)
	request.Project = "demo"
	p, err := h.writer().prepare(ctx, h.read(t), by, model.ChangeSet{Ops: []model.Op{create("probe")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := p.load(ctx, model.Ref{ID: "demo-00000000"})
	if err != nil {
		t.Fatal(err)
	}
	deleted.deleted = true
	replaced, err := p.load(ctx, model.Ref{ID: "demo-00000001"})
	if err != nil {
		t.Fatal(err)
	}
	replaced.doc.Project = "different-project"
	replaced.doc.Fields["title"] = "replacement"
	p.writes["demo-staged"] = &pending{ref: model.Ref{ID: "demo-staged"}, kind: "bead", deleted: true}
	p.writes["demo-staged.1"] = &pending{ref: model.Ref{ID: "demo-staged.1"}, kind: "bead"}
	got, err := p.hashPopulation(ctx, "demo")
	if err != nil {
		t.Fatal(err)
	}
	want, _, _, err := p.allocatorObservations(ctx, "demo", "")
	if err != nil {
		t.Fatal(err)
	}
	if got != want || got != 5 {
		t.Fatalf("staged population optimized=%d census=%d want=5", got, want)
	}
}

func TestPopulationCacheOwnCommitAndExternalHeadMove(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	seedAllocator(t, h.store, h.lease, driver.NewHashAllocator("demo", 3, 8, 0.25))
	seedAllocatorScaleBeads(t, h, 19)
	w := h.writer()
	request := meta(704)
	request.Project = "demo"
	// First real create builds the census then advances it in the commit path.
	commit, err := w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("first")}}, request)
	if err != nil {
		t.Fatal(err)
	}
	warm := &allocatorWorkCounts{counts: map[driver.ReadWorkKind]int64{}}
	request = meta(705)
	request.Project = "demo"
	if _, err := w.Apply(driver.WithReadWorkObserver(ctx, warm), by, model.ChangeSet{Ops: []model.Op{create("second")}}, request); err != nil {
		t.Fatal(err)
	}
	if warm.counts[driver.ReadAllocatorDecodes] != 0 {
		t.Fatalf("own commit lost cache: %v", warm.counts)
	}
	first := commit.Results[0].Ref
	// A delete and replacement in one own commit must update the live count,
	// irrespective of the stored document's project.
	request = meta(706)
	request.Project = "demo"
	if _, err := w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: first}}, patch(model.Ref{ID: "demo-00000000"}, map[string]any{"title": "replacement"})}}, request); err != nil {
		t.Fatal(err)
	}
	reader := h.read(t)
	p, err := w.prepare(ctx, reader, by, model.ChangeSet{Ops: []model.Op{create("preview")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.hashPopulation(ctx, "demo")
	if err != nil {
		t.Fatal(err)
	}
	want, _, _, err := p.allocatorObservations(ctx, "demo", "")
	if err != nil || got != want {
		t.Fatalf("own delta got=%d census=%d err=%v", got, want, err)
	}
	// An independent Writer moves the head; the first Writer must cold-rebuild.
	external := h.writer()
	request = meta(707)
	request.Project = "demo"
	if _, err := external.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("external")}}, request); err != nil {
		t.Fatal(err)
	}
	moved := &allocatorWorkCounts{counts: map[driver.ReadWorkKind]int64{}}
	request = meta(708)
	request.Project = "demo"
	snapshot := h.read(t)
	optimized, err := w.prepare(driver.WithReadWorkObserver(ctx, moved), snapshot, by, model.ChangeSet{Ops: []model.Op{create("after-external")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := h.writer().prepare(ctx, snapshot, by, model.ChangeSet{Ops: []model.Op{create("after-external")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if optimized.proposal.Changes[0].Resource.Ref.ID != legacy.proposal.Changes[0].Resource.Ref.ID {
		t.Fatal("external head cache changed allocated ID")
	}
	if moved.counts[driver.ReadAllocatorDecodes] != 21 {
		t.Fatalf("external move census=%d want21", moved.counts[driver.ReadAllocatorDecodes])
	}
	t.Logf("own warm create decodes=0; external-head census decodes=%d", moved.counts[driver.ReadAllocatorDecodes])
}
func TestPopulationCandidateBoundaries(t *testing.T) {
	cases := map[string][]string{"a/b-c": nil, " demo-tail": nil, "demo-": nil, "demo-a.1": nil, "demo-a-b": {"demo", "demo-a"}, "demo.x-tail": {"demo.x"}, "demo--tail": {"demo", "demo-"}, "demo-tail": {"demo"}}
	for id, want := range cases {
		if got := populationPrefixes(id); !reflect.DeepEqual(got, want) {
			t.Errorf("%q candidates=%v want=%v", id, got, want)
		}
	}
}

// The channels force the reviewed old-patch/external-head/cold-census schedule;
// no timing assumption or production hook participates in the reproduction.
func TestPopulationColdPublishGenerationAfterStalePatch(t *testing.T) {
	h := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	seedAllocator(t, h.store, h.lease, driver.NewHashAllocator("demo", 3, 8, 0.25))
	seedAllocatorScaleBeads(t, h, 3)
	w := h.writer()
	oldMeta := meta(801)
	oldMeta.Project = "demo"
	cs := model.ChangeSet{Ops: []model.Op{patch(model.Ref{ID: "demo-00000000"}, map[string]any{"title": "old prepared patch"})}}
	old, err := w.prepare(ctx, h.read(t), by, cs, oldMeta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.pre(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := old.graph(ctx); err != nil {
		t.Fatal(err)
	}
	if !old.graphPreserved {
		t.Fatal("fixture must preserve graph without a census")
	}
	if err := w.claims(old, by, oldMeta); err != nil {
		t.Fatal(err)
	}
	if err := old.detectChanges(); err != nil {
		t.Fatal(err)
	}
	if err := w.prepareReadySortKeys(old); err != nil {
		t.Fatal(err)
	}
	if err := w.prepareAudit(ctx, old); err != nil {
		t.Fatal(err)
	}
	externalMeta := meta(802)
	externalMeta.Project = "demo"
	if _, err := h.writer().Apply(ctx, by, model.ChangeSet{Ops: []model.Op{patch(model.Ref{ID: "demo-00000001"}, map[string]any{"title": "external unrelated patch"})}}, externalMeta); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	reader := h.read(t)
	barrier := &populationCensusBarrier{Reader: reader, AllocatorReader: reader.(driver.AllocatorReader), entered: entered, resume: resume}
	request := meta(803)
	request.Project = "demo"
	type result struct {
		p   *prepared
		err error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		defer func() {
			if v := recover(); v != nil {
				r.err = fmt.Errorf("cold census panicked: %v", v)
			}
			done <- r
		}()
		r.p, r.err = w.prepare(ctx, barrier, by, model.ChangeSet{Ops: []model.Op{create("paused-cold")}}, request, nil)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("cold census did not reach barrier", ctx.Err())
	}
	w.population.mu.Lock()
	before := w.population.generation
	at := w.population.at
	w.population.mu.Unlock()
	if at.String() != reader.Snapshot().At.String() {
		t.Fatal("census did not install exact H cache")
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := requestHash(cs, oldMeta)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := w.transact(ctx, old, by, oldMeta, principal, hash, "generation-patch-attempt", nil)
	if err != nil {
		t.Fatal("older graph-preserving patch did not commit", err)
	}
	expectedHead, e := at.Uint64()
	if e != nil {
		t.Fatal(e)
	}
	if committed.Revision.String() != model.Natural64(expectedHead+1).String() {
		t.Fatal("patch did not commit from actual H to H+1")
	}
	if committed.Revision.String() == old.snapshot.Snapshot().At.String() {
		t.Fatal("patch did not advance head")
	}
	w.population.mu.Lock()
	after := w.population.generation
	invalidated := w.population.counts == nil
	w.population.mu.Unlock()
	if !invalidated || after <= before {
		t.Fatalf("cache invalidation generation before=%d after=%d nil=%v", before, after, invalidated)
	}
	release.Do(func() { close(resume) })
	var cold result
	select {
	case cold = <-done:
	case <-ctx.Done():
		t.Fatal("cold census did not finish", ctx.Err())
	}
	if cold.err != nil {
		t.Fatal(cold.err)
	}
	// The pinned-H result remains valid, but must not republish into the invalidated cache.
	w.population.mu.Lock()
	discarded := w.population.counts == nil && w.population.generation == after
	w.population.mu.Unlock()
	if !discarded {
		t.Fatal("stale cold completion was published")
	}
	expectedCold, err := h.writer().prepare(ctx, reader, by, model.ChangeSet{Ops: []model.Op{create("paused-cold")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cold.p.proposal.Changes[0].Resource.Ref.ID != expectedCold.proposal.Changes[0].Resource.Ref.ID {
		t.Fatal("pinned cold ID changed")
	}
	work := &allocatorWorkCounts{counts: map[driver.ReadWorkKind]int64{}}
	current := h.read(t)
	request = meta(804)
	request.Project = "demo"
	next, err := w.prepare(driver.WithReadWorkObserver(ctx, work), current, by, model.ChangeSet{Ops: []model.Op{create("after-invalidation")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := h.writer().prepare(ctx, current, by, model.ChangeSet{Ops: []model.Op{create("after-invalidation")}}, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.proposal.Changes[0].Resource.Ref.ID != oracle.proposal.Changes[0].Resource.Ref.ID {
		t.Fatal("post-invalidation ID differs from census")
	}
	if work.counts[driver.ReadAllocatorDecodes] != 3 {
		t.Fatalf("rebuild decodes=%d want3", work.counts[driver.ReadAllocatorDecodes])
	}
	warm := &allocatorWorkCounts{counts: map[driver.ReadWorkKind]int64{}}
	if _, err := w.prepare(driver.WithReadWorkObserver(ctx, warm), current, by, model.ChangeSet{Ops: []model.Op{create("warm")}}, request, nil); err != nil {
		t.Fatal(err)
	}
	if warm.counts[driver.ReadAllocatorDecodes] != 0 {
		t.Fatal("new generation did not warm")
	}
	t.Logf("forced H-1 patch / external H / blocked H census / H+1 patch: generation %d -> %d, stale census discarded, rebuild=3 warm=0", before, after)
}

type populationCensusBarrier struct {
	driver.Reader
	driver.AllocatorReader
	once    sync.Once
	entered chan struct{}
	resume  <-chan struct{}
}

func (r *populationCensusBarrier) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	var err error
	r.once.Do(func() {
		close(r.entered)
		select {
		case <-r.resume:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	return r.Reader.Query(ctx, q)
}
