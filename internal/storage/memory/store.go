// Package memory implements the transactional driver with immutable committed
// snapshots. Each New call creates an independent, disposable store.
package memory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type Driver struct{}

func (Driver) Info() driver.Info {
	return driver.Info{Name: "memory", Version: "1", Capabilities: []string{"transactions", "history", "leases", "checkpoints", "allocator_continuation", driver.CapabilityRestoreCurrentImage}}
}

func (Driver) Open(ctx context.Context, cfg driver.Config) (driver.Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if (cfg.DSN != "" && cfg.DSN != ":memory:") || len(cfg.Options) != 0 {
		return nil, fmt.Errorf("memory: unsupported configuration")
	}
	return New(), nil
}

type scope struct{ principal, agent, method, key string }
type replay struct {
	value driver.IdempotencyRecord
	at    time.Time
	post  []model.Warning
}
type blob struct {
	bytes    []byte
	revision uint64
}
type state struct {
	allocatorHistory *allocatorImage
	meta             driver.Meta
	records          map[string]driver.Record
	kinds            map[string]string
	versions         map[string][]driver.Version
	keys             map[string]driver.LockState
	keyHistory       map[string][]driver.LockState
	derivedKeys      map[driver.DerivedTable][]string // sorted historical key catalog, immutable after publication
	blobs            map[driver.Hash]blob
	log              []model.Changeset
	replays          map[scope]replay
	allocators       map[string]driver.OfflineAllocator
	parentCounters   map[string]driver.ParentCounter
	hierarchy        *hierarchyIndex  // immutable; nil/empty roots never certify coverage
	sourceLinks      *sourceLinkIndex // separate complete outgoing-link coverage
}

func (s *state) fork() *state {
	n := *s
	n.records = maps.Clone(s.records)
	n.kinds = maps.Clone(s.kinds)
	n.versions = maps.Clone(s.versions)
	n.keys = maps.Clone(s.keys)
	n.keyHistory = maps.Clone(s.keyHistory)
	n.derivedKeys = maps.Clone(s.derivedKeys)
	n.blobs = maps.Clone(s.blobs)
	n.replays = maps.Clone(s.replays)
	n.parentCounters = maps.Clone(s.parentCounters)
	n.allocators = make(map[string]driver.OfflineAllocator, len(s.allocators))
	for prefix, allocator := range s.allocators {
		n.allocators[prefix] = allocator.Clone()
	}
	return &n
}

// Store satisfies driver.Store, ResponseFinalizer and CheckpointStore. No
// committed map, slice or record body is ever mutated after publication.
type Store struct {
	mu sync.Mutex
	// leaseMu is independent from the committed data mutex. Lease heartbeats
	// must remain serviceable while a transaction prepares a large publication.
	// Code which needs both locks always takes mu first, then leaseMu.
	leaseMu      sync.Mutex
	state        *state
	locks        map[string]*heldLock
	transactions map[*transaction]bool
	head         *transaction
	notify       chan struct{}
	epoch        uint64
	active       *activeLease
	checkpoints  map[string]driver.Checkpoint
	closed       bool
	now          func() time.Time
	// beforeCommitHook is a deterministic test seam for pausing publication
	// preparation. It is nil for normal callers and is invoked without mu.
	beforeCommitHook func()
}

// SupportsRestore advertises the only restore mode this store can publish
// through its existing atomic WriteTx path. Full-history injection requires a
// separate offline-history port and therefore remains explicitly unsupported.
func (*Store) SupportsRestore(mode driver.RestoreMode) bool {
	return mode == driver.RestoreModeCurrentImage
}

// Options supplies deterministic dependencies for acceptance fixtures. Empty
// identities and a nil Clock use secure random identities and time.Now.
// The clock must be concurrency-safe and nondecreasing, like time.Now.
type Options struct {
	Clock       func() time.Time
	StoreID     driver.StoreID
	Incarnation string
}

func New() *Store {
	s, err := NewWithOptions(Options{})
	if err != nil {
		panic(err)
	}
	return s
}

func NewWithOptions(options Options) (*Store, error) {
	identity := func() (string, error) {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return "", err
		}
		return hex.EncodeToString(id[:]), nil
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StoreID == "" {
		id, err := identity()
		if err != nil {
			return nil, err
		}
		options.StoreID = driver.StoreID(id)
	}
	if options.Incarnation == "" {
		id, err := identity()
		if err != nil {
			return nil, err
		}
		options.Incarnation = id
	}
	if !canonicalKey(string(options.StoreID)) || !canonicalKey(options.Incarnation) {
		return nil, fmt.Errorf("memory: invalid store identity")
	}
	s := &Store{locks: make(map[string]*heldLock), transactions: make(map[*transaction]bool), notify: make(chan struct{}), checkpoints: make(map[string]driver.Checkpoint), now: time.Now}
	s.now = options.Clock
	s.state = &state{
		meta:    driver.Meta{StoreID: options.StoreID, Incarnation: options.Incarnation, SchemaVersion: 1, State: "ready", Head: model.Natural64(0), HistoryFloor: model.Natural64(0)},
		records: make(map[string]driver.Record), kinds: make(map[string]string), versions: make(map[string][]driver.Version),
		keys: make(map[string]driver.LockState), keyHistory: make(map[string][]driver.LockState), blobs: make(map[driver.Hash]blob), replays: make(map[scope]replay), allocators: make(map[string]driver.OfflineAllocator),
		derivedKeys: make(map[driver.DerivedTable][]string),
		hierarchy:   newHierarchyIndex(options.Incarnation),
		sourceLinks: newSourceLinkIndex(options.Incarnation),
	}
	return s, nil
}

func (s *Store) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return fmt.Errorf("memory: store closed")
	}
	return nil
}

func (s *Store) signal() { close(s.notify); s.notify = make(chan struct{}) }

func (s *Store) Meta(ctx context.Context) (driver.Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	return s.state.meta, nil
}

func (s *Store) Migrate(ctx context.Context, to driver.SchemaVersion) (driver.MigrationReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.MigrationReport{}, err
	}
	from := s.state.meta.SchemaVersion
	if to != from {
		return driver.MigrationReport{}, fmt.Errorf("memory: unsupported schema version %d", to)
	}
	return driver.MigrationReport{From: from, To: to, Applied: []string{}, Warnings: []string{}}, nil
}

func (s *Store) Health(ctx context.Context) (driver.StoreHealth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.StoreHealth{}, err
	}
	s.expireLease()
	role := "standby"
	s.leaseMu.Lock()
	active, epoch := s.active != nil, s.epoch
	s.leaseMu.Unlock()
	if active {
		role = "active"
	}
	return driver.StoreHealth{Driver: "memory", Role: role, LeaseEpoch: epoch, Head: s.state.meta.Head, Incarnation: s.state.meta.Incarnation, AuthorizationChain: []string{}}, nil
}

func (s *Store) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	return newReader(s.state, view, s.now)
}

func (s *Store) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if err := s.checkLease(lease); err != nil {
		return nil, err
	}
	t := &transaction{store: s, lease: lease, base: s.state, snapshotAt: s.state.meta.Head, incarnation: s.state.meta.Incarnation, records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState), blobs: make(map[driver.Hash][]byte), bumps: make(map[string]driver.LockKey), allocators: make(map[string]driver.OfflineAllocator)}
	s.transactions[t] = true
	return t, nil
}

// Close releases store state, timers and the lease, rolling back unfinished
// writers. Callers close readers and finish transactions before the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.endLease()
		for tx := range s.transactions {
			tx.finish()
		}
		s.state = nil
		s.checkpoints = nil
		s.locks = nil
		s.signal()
	}
	return nil
}

var _ driver.Driver = Driver{}
var _ driver.Store = (*Store)(nil)
var _ driver.RestoreCapabilities = (*Store)(nil)
var _ driver.ResponseFinalizer = (*Store)(nil)
var _ driver.CheckpointStore = (*Store)(nil)
