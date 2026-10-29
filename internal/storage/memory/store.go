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
	return driver.Info{Name: "memory", Version: "1", Capabilities: []string{"transactions", "history", "leases", "checkpoints"}}
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
	value     driver.IdempotencyRecord
	at        time.Time
	finalized bool
	post      []model.Warning
}
type blob struct {
	bytes    []byte
	revision uint64
}
type state struct {
	meta       driver.Meta
	records    map[string]driver.Record
	kinds      map[string]string
	versions   map[string][]driver.Version
	keys       map[string]driver.LockState
	keyHistory map[string][]driver.LockState
	blobs      map[driver.Hash]blob
	log        []model.Changeset
	replays    map[scope]replay
}

func (s *state) fork() *state {
	n := *s
	n.records = maps.Clone(s.records)
	n.kinds = maps.Clone(s.kinds)
	n.versions = maps.Clone(s.versions)
	n.keys = maps.Clone(s.keys)
	n.keyHistory = maps.Clone(s.keyHistory)
	n.blobs = maps.Clone(s.blobs)
	n.replays = maps.Clone(s.replays)
	return &n
}

// Store satisfies driver.Store, ResponseFinalizer and CheckpointStore. No
// committed map, slice or record body is ever mutated after publication.
type Store struct {
	mu           sync.Mutex
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
}

func New() *Store {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	s := &Store{locks: make(map[string]*heldLock), transactions: make(map[*transaction]bool), notify: make(chan struct{}), checkpoints: make(map[string]driver.Checkpoint), now: time.Now}
	s.state = &state{
		meta:    driver.Meta{StoreID: driver.StoreID(hex.EncodeToString(id[:])), Incarnation: hex.EncodeToString(id[:]), SchemaVersion: 1, State: "ready", Head: model.Natural64(0), HistoryFloor: model.Natural64(0)},
		records: make(map[string]driver.Record), kinds: make(map[string]string), versions: make(map[string][]driver.Version),
		keys: make(map[string]driver.LockState), keyHistory: make(map[string][]driver.LockState), blobs: make(map[driver.Hash]blob), replays: make(map[scope]replay),
	}
	return s
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
	if s.active != nil {
		role = "active"
	}
	return driver.StoreHealth{Driver: "memory", Role: role, LeaseEpoch: s.epoch, Head: s.state.meta.Head, Incarnation: s.state.meta.Incarnation, AuthorizationChain: []string{}}, nil
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
	t := &transaction{store: s, lease: lease, base: s.state, records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState), blobs: make(map[driver.Hash][]byte), bumps: make(map[string]driver.LockKey)}
	s.transactions[t] = true
	return t, nil
}

// Close releases timers and fences outstanding writers. Previously returned
// immutable readers remain usable, just as a captured snapshot does.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		if s.active != nil {
			s.endLease()
		}
		s.signal()
	}
	return nil
}

var _ driver.Driver = Driver{}
var _ driver.Store = (*Store)(nil)
var _ driver.ResponseFinalizer = (*Store)(nil)
var _ driver.CheckpointStore = (*Store)(nil)
