package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

const (
	defaultRecords  = 200
	defaultClaimers = 200
	defaultMutators = 200
)

var probeSequence atomic.Uint64

func newRunID() string {
	return fmt.Sprintf("%d-%d", time.Now().UTC().UnixNano(), probeSequence.Add(1))
}

// ProbeConfig controls the bounded stress run.  The defaults are the E5
// acceptance matrix; smaller values are useful for focused unit tests.
type ProbeConfig struct {
	Records  int
	Claimers int
	Mutators int
	Deadline time.Duration
	Seed     int64
	// RunID namespaces seed records and idempotency keys. When empty, each
	// invocation receives a fresh ID so a reused DSN cannot replay an earlier
	// probe. Supplying a fixed ID is useful for testing replay refusal.
	RunID string
}

func (c ProbeConfig) normalized() (ProbeConfig, error) {
	if c.Records == 0 {
		c.Records = defaultRecords
	}
	if c.Claimers == 0 {
		c.Claimers = defaultClaimers
	}
	if c.Mutators == 0 {
		c.Mutators = defaultMutators
	}
	if c.Deadline == 0 {
		c.Deadline = 45 * time.Second
	}
	if c.Records < 1 || c.Claimers < 1 || c.Mutators < 1 || c.Deadline <= 0 {
		return ProbeConfig{}, fmt.Errorf("runtime probe: counts and deadline must be positive")
	}
	c.RunID = strings.TrimSpace(c.RunID)
	if c.RunID != "" {
		if len(c.RunID) > 96 || strings.IndexFunc(c.RunID, func(r rune) bool {
			return !(r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r))
		}) >= 0 {
			return ProbeConfig{}, fmt.Errorf("runtime probe: run ID must contain only letters, digits, '-' or '_'")
		}
	}
	return c, nil
}

// ProbeReport is the durable, JSON-friendly result written by the spike
// runner.  A zero deadlock count is meaningful only together with NoDeadlock
// and the timeout/error fields being clear.
type ProbeReport struct {
	Engine             string         `json:"engine"`
	RunID              string         `json:"runId"`
	Records            int            `json:"records"`
	Claimers           int            `json:"claimers"`
	Mutators           int            `json:"mutators"`
	Claimed            int64          `json:"claimed"`
	Mutations          int64          `json:"mutations"`
	CandidatesExamined int64          `json:"candidatesExamined"`
	SkippedLocked      int64          `json:"skippedLocked"`
	NotReady           int64          `json:"notReady"`
	Attempts           int64          `json:"attempts"`
	Retries            int64          `json:"retries"`
	Conflicts          int64          `json:"conflicts"`
	Deadlocks          int64          `json:"deadlocks"`
	Fenced             int64          `json:"fenced"`
	Duration           time.Duration  `json:"duration"`
	NoDeadlock         bool           `json:"noDeadlock"`
	StaleLeaseClosed   bool           `json:"staleLeaseClosed"`
	Failover           FailoverReport `json:"failover"`
}

// FailoverReport records the decision required by P70-P72.  The harness can
// prove the stale writer fence, but it cannot turn a local memory or a plain
// PostgreSQL instance into managed HA; that topology proof remains a separate
// production gate.
type FailoverReport struct {
	Choice                    string `json:"choice"`
	RPO                       string `json:"rpo"`
	RTO                       string `json:"rto"`
	RequiresNewIncarnation    bool   `json:"requiresNewIncarnation"`
	ObservedEpochIncrease     bool   `json:"observedEpochIncrease"`
	ObservedIncarnationChange bool   `json:"observedIncarnationChange"`
}

type counters struct {
	claimed, mutations, examined, skipped, notReady atomic.Int64
	attempts, retries, conflicts, deadlocks, fenced atomic.Int64
}

func (c *counters) report(config ProbeConfig, engine string, started time.Time, failover FailoverReport) ProbeReport {
	return ProbeReport{
		Engine: engine, RunID: config.RunID, Records: config.Records, Claimers: config.Claimers, Mutators: config.Mutators,
		Claimed: c.claimed.Load(), Mutations: c.mutations.Load(), CandidatesExamined: c.examined.Load(),
		SkippedLocked: c.skipped.Load(), NotReady: c.notReady.Load(), Attempts: c.attempts.Load(),
		Retries: c.retries.Load(), Conflicts: c.conflicts.Load(), Deadlocks: c.deadlocks.Load(),
		Fenced: c.fenced.Load(), Duration: time.Since(started), NoDeadlock: c.deadlocks.Load() == 0,
		StaleLeaseClosed: c.fenced.Load() > 0, Failover: failover,
	}
}

// RunMemory runs the complete E5 harness against the immutable memory driver.
// It is the fast, deterministic lane used by the Bazel target.
func RunMemory(ctx context.Context, config ProbeConfig) (ProbeReport, error) {
	store := memory.New()
	defer store.Close()
	return runStore(ctx, store, "memory", config)
}

// RunPostgres runs the same harness against a caller-provided disposable
// PostgreSQL DSN.  The caller owns the server/container lifecycle and must not
// put credentials in reports or checked-in files.
func RunPostgres(ctx context.Context, dsn string, config ProbeConfig) (ProbeReport, error) {
	if strings.TrimSpace(dsn) == "" {
		return ProbeReport{}, fmt.Errorf("runtime probe: PostgreSQL DSN is required")
	}
	store, err := (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		return ProbeReport{}, err
	}
	defer store.Close()
	return runStore(ctx, store, "postgres", config)
}

func runStore(parent context.Context, store driver.Store, engine string, config ProbeConfig) (ProbeReport, error) {
	config, err := config.normalized()
	if err != nil {
		return ProbeReport{}, err
	}
	if config.RunID == "" {
		config.RunID = newRunID()
	}
	ctx, cancel := context.WithTimeout(parent, config.Deadline)
	defer cancel()
	started := time.Now()
	// A probe may intentionally run longer than the production 10-second lease
	// while hundreds of database workers contend. The harness asks for a
	// deadline-bound lease instead of changing the driver's production default;
	// stale fencing is exercised explicitly by releasing this lease below.
	leaseTTL := driver.LeaseTTL
	if config.Deadline >= leaseTTL {
		leaseTTL = config.Deadline + driver.LeaseRenewEvery
	}
	lease, err := store.Lease(ctx, "e5-active-"+config.RunID, leaseTTL)
	if err != nil {
		return ProbeReport{}, err
	}
	defer func() { _ = lease.Release(context.Background()) }()
	refs, err := seedQueue(ctx, store, lease, config.Records, config.RunID)
	if err != nil {
		return ProbeReport{}, err
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return ProbeReport{}, err
	}
	incarnation := meta.Incarnation
	c := &counters{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var firstErrMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		firstErrMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		firstErrMu.Unlock()
	}
	for i := 0; i < config.Claimers; i++ {
		startIndex := i % len(refs)
		wg.Add(1)
		go func(startIndex int) {
			defer wg.Done()
			<-start
			if _, _, err := claimNext(ctx, store, lease, refs, startIndex, incarnation, c); err != nil {
				recordErr(err)
			}
		}(startIndex)
	}
	for i := 0; i < config.Mutators; i++ {
		index := i % len(refs)
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			if err := retry(ctx, c, func() error { return mutateOne(ctx, store, lease, refs[index], index) }); err != nil {
				recordErr(err)
			}
		}(index)
	}
	close(start)
	wg.Wait()
	firstErrMu.Lock()
	err = firstErr
	firstErrMu.Unlock()
	if err != nil {
		return c.report(config, engine, started, FailoverReport{}), err
	}
	if err := requireCurrentClaims(c, config.Records); err != nil {
		return c.report(config, engine, started, FailoverReport{}), err
	}
	if err := assertClaimed(ctx, store, refs, config.Records); err != nil {
		return c.report(config, engine, started, FailoverReport{}), err
	}
	// Force one real stale-snapshot conflict, then let the normal retry path
	// perform the same mutation with a fresh observation.
	if err := injectConflict(ctx, store, lease, refs[0], config.RunID, c); err != nil {
		return c.report(config, engine, started, FailoverReport{}), err
	}
	failover, err := exerciseFailover(ctx, store, lease, refs[0], config.RunID, c)
	if err != nil {
		return c.report(config, engine, started, failover), err
	}
	return c.report(config, engine, started, failover), nil
}

func requireCurrentClaims(c *counters, records int) error {
	if got := c.claimed.Load(); got != int64(records) {
		return fmt.Errorf("runtime probe: current run claimed %d records, want %d", got, records)
	}
	return nil
}

type queueDocument struct {
	ID      string         `json:"id"`
	Type    string         `json:"type"`
	Project string         `json:"project"`
	Fields  map[string]any `json:"fields"`
}

func seedQueue(ctx context.Context, store driver.Store, lease driver.ActiveLease, count int, runID string) ([]model.Ref, error) {
	refs := make([]model.Ref, count)
	for i := range refs {
		refs[i] = model.RefByID(fmt.Sprintf("e5-%s-%04d", runID, i), model.Natural64(0))
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	cleanup := func(err error) ([]model.Ref, error) {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	keys := make([]driver.LockKey, 0, count*2+2)
	for _, ref := range refs {
		keys = append(keys, recordLock(ref), beadLock(ref))
	}
	ready, err := driver.NewReadySentinel("queue", "issue")
	if err != nil {
		return cleanup(err)
	}
	keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	for _, ref := range refs {
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: ref.ID}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
	}
	sortLockKeys(keys)
	requests := updateRequests(keys)
	states, err := tx.Lock(ctx, requests)
	if err != nil {
		return cleanup(err)
	}
	if len(states) != len(keys) {
		return cleanup(fmt.Errorf("runtime probe: seed lock set was incomplete"))
	}
	for _, ref := range refs {
		body, _ := json.Marshal(queueDocument{ID: ref.ID, Type: "issue", Project: "queue", Fields: map[string]any{"status": "todo", "mutations": 0}})
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
			return cleanup(err)
		}
	}
	beadSentinels := make([]driver.Sentinel, 0, count+1)
	for _, ref := range refs {
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			return cleanup(err)
		}
		beadSentinels = append(beadSentinels, bead)
	}
	beadSentinels = append(beadSentinels, ready)
	if err := tx.BumpSentinels(ctx, beadSentinels); err != nil {
		return cleanup(err)
	}
	rows := make([]driver.DerivedRow, 0, count)
	for _, ref := range refs {
		rows = append(rows, driver.DerivedRow{Key: ref.ID, Value: []byte("ready")})
	}
	if err := tx.PutDerived(ctx, driver.DerivedReadyState, nil, rows); err != nil {
		return cleanup(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		return cleanup(err)
	}
	changes := make([]model.ChangeRecord, 0, count)
	for _, ref := range refs {
		changes = append(changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	seedKey := "e5-seed-" + runID
	header := driver.ChangesetHeader{
		Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Time: time.Now().UTC(),
		Principal: model.PrincipalRef{Kind: "system", ID: seedKey}, Method: "e5.seed",
		IdempotencyKey: seedKey, RequestHash: seedKey + "-v1", LockKeys: keys, Changes: changes,
		Response: model.Commit{Results: []model.OpResult{}},
	}
	if _, err := tx.Commit(ctx, header); err != nil {
		return cleanup(err)
	}
	return refs, nil
}

func claimNext(ctx context.Context, store driver.Store, lease driver.ActiveLease, refs []model.Ref, startIndex int, incarnation string, c *counters) (model.Ref, bool, error) {
	if len(refs) == 0 {
		return model.Ref{}, false, nil
	}
	startIndex %= len(refs)
	for {
		foundTodo := false
		for offset := range refs {
			ref := refs[(startIndex+offset)%len(refs)]
			c.examined.Add(1)
			tx, err := store.Write(ctx, lease)
			if err != nil {
				return model.Ref{}, false, countError(c, err)
			}
			keys := queueKeys(ref, true)
			states, err := tx.Lock(ctx, skipRequests(keys))
			if err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, countError(c, err)
			}
			if len(states) != len(keys) {
				c.skipped.Add(1)
				_ = tx.Rollback(context.Background())
				continue
			}
			state := findRecordState(states)
			if state == nil || !state.Present || state.Record == nil {
				_ = tx.Rollback(context.Background())
				continue
			}
			doc, err := decodeQueue(state.Record.Body)
			if err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, err
			}
			if status, _ := doc.Fields["status"].(string); status != "todo" {
				c.notReady.Add(1)
				_ = tx.Rollback(context.Background())
				continue
			}
			foundTodo = true
			doc.Fields["status"] = "in_progress"
			doc.Fields["claimer"] = "e5"
			body, _ := json.Marshal(doc)
			if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, countError(c, err)
			}
			bead, _ := driver.NewBeadSentinel(ref.ID)
			ready, _ := driver.NewReadySentinel("queue", "issue")
			if err := tx.BumpSentinels(ctx, []driver.Sentinel{bead, ready}); err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, countError(c, err)
			}
			if err := tx.PutDerived(ctx, driver.DerivedReadyState, []driver.DerivedRow{{Key: ref.ID}}, nil); err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, countError(c, err)
			}
			if _, err := tx.Head(ctx); err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, countError(c, err)
			}
			c.attempts.Add(1)
			header := changeHeader(tx.Snapshot().Incarnation, "e5.claim", fmt.Sprintf("e5-claim-%s-%d", ref.ID, time.Now().UnixNano()), state.Revision, ref, keys)
			if _, err := tx.Commit(ctx, header); err != nil {
				_ = tx.Rollback(context.Background())
				return model.Ref{}, false, countError(c, err)
			}
			c.claimed.Add(1)
			return ref, true, nil
		}
		if !foundTodo {
			ready, err := anyReady(ctx, store, refs, incarnation)
			if err != nil {
				return model.Ref{}, false, err
			}
			if !ready {
				return model.Ref{}, false, nil
			}
		}
		select {
		case <-ctx.Done():
			return model.Ref{}, false, ctx.Err()
		case <-time.After(200 * time.Microsecond):
			runtime.Gosched()
		}
	}
}

func mutateOne(ctx context.Context, store driver.Store, lease driver.ActiveLease, ref model.Ref, index int) error {
	tx, err := store.Write(ctx, lease)
	if err != nil {
		return err
	}
	keys := queueKeys(ref, false)
	states, err := tx.Lock(ctx, updateRequests(keys))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	state := findRecordState(states)
	if state == nil || !state.Present || state.Record == nil {
		_ = tx.Rollback(context.Background())
		return driver.ErrNotFound
	}
	doc, err := decodeQueue(state.Record.Body)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	mutations, _ := doc.Fields["mutations"].(json.Number)
	value := 0
	if mutations != "" {
		_, _ = fmt.Sscanf(string(mutations), "%d", &value)
	} else if number, ok := doc.Fields["mutations"].(float64); ok {
		value = int(number)
	}
	doc.Fields["mutations"] = value + 1
	doc.Fields["lastMutator"] = index
	body, _ := json.Marshal(doc)
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	bead, _ := driver.NewBeadSentinel(ref.ID)
	ready, _ := driver.NewReadySentinel("queue", "issue")
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{bead, ready}); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	if _, err := tx.Head(ctx); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	header := changeHeader(tx.Snapshot().Incarnation, "e5.mutate", fmt.Sprintf("e5-mutate-%s-%d", ref.ID, time.Now().UnixNano()), state.Revision, ref, keys)
	if _, err := tx.Commit(ctx, header); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	return nil
}

func retry(ctx context.Context, c *counters, operation func() error) error {
	for attempt := 0; attempt < 3; attempt++ {
		c.attempts.Add(1)
		err := operation()
		if err == nil {
			if attempt > 0 {
				c.retries.Add(int64(attempt))
			}
			c.mutations.Add(1)
			return nil
		}
		if errors.Is(err, driver.ErrConflict) || strings.Contains(strings.ToLower(err.Error()), "serialization") {
			c.conflicts.Add(1)
			c.retries.Add(1)
			continue
		}
		return countError(c, err)
	}
	return fmt.Errorf("runtime probe: retry budget exhausted")
}

func injectConflict(ctx context.Context, store driver.Store, lease driver.ActiveLease, ref model.Ref, runID string, c *counters) error {
	tx, err := store.Write(ctx, lease)
	if err != nil {
		return err
	}
	keys := queueKeys(ref, false)
	states, err := tx.Lock(ctx, updateRequests(keys))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	state := findRecordState(states)
	if state == nil || state.Record == nil {
		_ = tx.Rollback(context.Background())
		return driver.ErrNotFound
	}
	doc, err := decodeQueue(state.Record.Body)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	doc.Fields["conflictProbe"] = true
	body, _ := json.Marshal(doc)
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	bead, _ := driver.NewBeadSentinel(ref.ID)
	ready, _ := driver.NewReadySentinel("queue", "issue")
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{bead, ready}); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	if _, err := tx.Head(ctx); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	header := changeHeader(tx.Snapshot().Incarnation, "e5.conflict", "e5-conflict-"+runID, model.Natural64(0), ref, keys)
	_, err = tx.Commit(ctx, header)
	_ = tx.Rollback(context.Background())
	if !errors.Is(err, driver.ErrConflict) {
		return fmt.Errorf("runtime probe: injected stale snapshot returned %v", err)
	}
	c.conflicts.Add(1)
	c.retries.Add(1)
	return retry(ctx, c, func() error { return mutateOne(ctx, store, lease, ref, 0) })
}

func exerciseFailover(ctx context.Context, store driver.Store, oldLease driver.ActiveLease, ref model.Ref, runID string, c *counters) (FailoverReport, error) {
	oldEpoch := oldLease.Epoch()
	tx, err := store.Write(ctx, oldLease)
	if err != nil {
		return FailoverReport{}, err
	}
	keys := queueKeys(ref, false)
	states, err := tx.Lock(ctx, updateRequests(keys))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return FailoverReport{}, err
	}
	state := findRecordState(states)
	if state == nil || state.Record == nil {
		_ = tx.Rollback(context.Background())
		return FailoverReport{}, driver.ErrNotFound
	}
	beforeBody := append([]byte(nil), state.Record.Body...)
	if _, err := tx.Head(ctx); err != nil {
		_ = tx.Rollback(context.Background())
		return FailoverReport{}, err
	}
	if err := oldLease.Release(ctx); err != nil {
		_ = tx.Rollback(context.Background())
		return FailoverReport{}, err
	}
	staleErr := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"id":"stale","type":"issue","project":"queue","fields":{"status":"in_progress"}}`)})
	var fenced driver.ErrFenced
	if !errors.As(staleErr, &fenced) {
		_ = tx.Rollback(context.Background())
		if staleErr == nil {
			return FailoverReport{}, fmt.Errorf("runtime probe: stale transaction committed after lease release")
		}
		return FailoverReport{}, fmt.Errorf("runtime probe: stale transaction returned %v", staleErr)
	}
	c.fenced.Add(1)
	_ = tx.Rollback(context.Background())
	next, err := store.Lease(ctx, "e5-standby-"+runID, driver.LeaseTTL)
	if err != nil {
		return FailoverReport{}, err
	}
	defer next.Release(context.Background())
	meta, err := store.Meta(ctx)
	if err != nil {
		return FailoverReport{}, err
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return FailoverReport{}, err
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	closeErr := reader.Close()
	if err != nil {
		return FailoverReport{}, err
	}
	if closeErr != nil {
		return FailoverReport{}, closeErr
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].Body, beforeBody) {
		return FailoverReport{}, fmt.Errorf("runtime probe: stale transaction changed the record")
	}
	return FailoverReport{
		Choice:                    "managed-ha-postgres",
		RPO:                       "5m",
		RTO:                       "1h",
		RequiresNewIncarnation:    true,
		ObservedEpochIncrease:     next.Epoch() > oldEpoch,
		ObservedIncarnationChange: false,
	}, nil
}

func assertClaimed(ctx context.Context, store driver.Store, refs []model.Ref, want int) error {
	meta, err := store.Meta(ctx)
	if err != nil {
		return err
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return err
	}
	defer reader.Close()
	count := 0
	for _, ref := range refs {
		rows, err := reader.Get(ctx, []model.Ref{ref})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("runtime probe: missing record %s", ref.ID)
		}
		doc, err := decodeQueue(rows[0].Body)
		if err != nil {
			return err
		}
		if doc.Fields["status"] == "in_progress" {
			count++
		}
	}
	if count != want {
		return fmt.Errorf("runtime probe: claimed %d records, want %d", count, want)
	}
	return nil
}

func anyReady(ctx context.Context, store driver.Store, refs []model.Ref, incarnation string) (bool, error) {
	reader, err := store.Read(ctx, driver.View{Incarnation: incarnation})
	if err != nil {
		return false, err
	}
	defer reader.Close()
	for _, ref := range refs {
		rows, err := reader.Get(ctx, []model.Ref{ref})
		if err != nil {
			return false, err
		}
		if len(rows) == 1 {
			doc, err := decodeQueue(rows[0].Body)
			if err != nil {
				return false, err
			}
			if doc.Fields["status"] == "todo" {
				return true, nil
			}
		}
	}
	return false, nil
}

func decodeQueue(body []byte) (queueDocument, error) {
	var doc queueDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return queueDocument{}, err
	}
	if doc.ID == "" || doc.Type == "" || doc.Fields == nil {
		return queueDocument{}, fmt.Errorf("runtime probe: malformed queue record")
	}
	return doc, nil
}

func queueKeys(ref model.Ref, derived bool) []driver.LockKey {
	keys := []driver.LockKey{recordLock(ref), beadLock(ref)}
	ready, _ := driver.NewReadySentinel("queue", "issue")
	keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	if derived {
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: ref.ID}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
	}
	sortLockKeys(keys)
	return keys
}

func recordLock(ref model.Ref) driver.LockKey {
	copy := ref
	return driver.LockKey{Kind: driver.LockRecord, Ref: &copy}
}

func beadLock(ref model.Ref) driver.LockKey {
	bead, _ := driver.NewBeadSentinel(ref.ID)
	return driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead}
}

func sortLockKeys(keys []driver.LockKey) {
	sort.Slice(keys, func(i, j int) bool {
		left, _ := driver.EncodeLockKey(keys[i])
		right, _ := driver.EncodeLockKey(keys[j])
		return left < right
	})
}

func updateRequests(keys []driver.LockKey) []driver.LockRequest {
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	return requests
}

func skipRequests(keys []driver.LockKey) []driver.LockRequest {
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockSkipLocked}
	}
	return requests
}

func findRecordState(states []driver.LockState) *driver.LockState {
	for i := range states {
		if states[i].Key.Kind == driver.LockRecord {
			return &states[i]
		}
	}
	return nil
}

func changeHeader(incarnation string, method, key string, from model.Revision, ref model.Ref, keys []driver.LockKey) driver.ChangesetHeader {
	return driver.ChangesetHeader{
		Incarnation: incarnation, Kind: model.ChangesetWrite, Time: time.Now().UTC(),
		Principal: model.PrincipalRef{Kind: "system", ID: "e5"}, Method: method,
		IdempotencyKey: key, RequestHash: key + "-request", LockKeys: keys,
		Changes:  []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: from}},
		Response: model.Commit{Results: []model.OpResult{}},
	}
}

func countError(c *counters, err error) error {
	if strings.Contains(strings.ToLower(err.Error()), "deadlock") {
		c.deadlocks.Add(1)
	}
	var fenced driver.ErrFenced
	if errors.As(err, &fenced) {
		c.fenced.Add(1)
	}
	return err
}

// Keep the deterministic seed available to callers that want to build a
// reproducible operation schedule around the harness.
func NewRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }
