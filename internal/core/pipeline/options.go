// Package pipeline implements the single transactional graph write path.
package pipeline

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type MutatorBinding struct {
	ID       string
	Reads    []string
	Owns     []string
	FailOpen bool
	Mutator  port.Mutator
}

type ValidatorBinding struct {
	ID        string
	FailOpen  bool
	Validator port.Validator
}

type PostBinding struct {
	ID         string
	Subscriber port.PostSubscriber
}

type config struct {
	ontology            port.Ontology
	audit               port.OntologyAudit
	auditSet            bool
	eventContext        port.EventContextSource
	authorizer          port.Authorizer
	mutators            []MutatorBinding
	validators          []ValidatorBinding
	posts               []PostBinding
	postFailures        driver.CheckpointStore
	now                 func() time.Time
	newID               func() (string, error)
	newAllocatorID      func(driver.OfflineAllocator) (string, error)
	newAllocatorChildID func(driver.OfflineAllocator, string) (string, error)
	hierarchyMaxDepth   int
	claimTTL            time.Duration
	rollbackWindow      bool
	reverseKindMapping  reverseconverter.KindMapping
	observer            Observer
}

type Option func(*config)

func WithOntology(o port.Ontology) Option { return func(c *config) { c.ontology = o } }

// WithOntologyAudit opts into complete ordered event plans and conditional
// notices. The source must resolve actual pinned ontology through tracked reads.
// Omitting this option retains the legacy nil-plan behavior of existing hosts.
func WithOntologyAudit(o port.OntologyAudit) Option {
	return func(c *config) { c.audit, c.auditSet = o, true }
}

// WithEventContext supplies trusted causal and authorization provenance for
// audited writes. It is required when an authorizer is installed with audit.
func WithEventContext(s port.EventContextSource) Option {
	return func(c *config) { c.eventContext = s }
}
func WithAuthorizer(a port.Authorizer) Option { return func(c *config) { c.authorizer = a } }
func WithMutators(b ...MutatorBinding) Option {
	return func(c *config) { c.mutators = append(c.mutators, b...) }
}
func WithValidators(b ...ValidatorBinding) Option {
	return func(c *config) { c.validators = append(c.validators, b...) }
}
func WithPostSubscribers(b ...PostBinding) Option {
	return func(c *config) { c.posts = append(c.posts, b...) }
}

// WithPostFailureStore preserves the underlying metadata capability when a
// host decorates Store. Ordinary stores supply it directly. It must be the
// same store as the Writer: dead letters use its active lease and incarnation.
func WithPostFailureStore(store driver.CheckpointStore) Option {
	return func(c *config) { c.postFailures = store }
}

// WithClock is for hosts that own a controlled clock; client input never sets it.
func WithClock(now func() time.Time) Option { return func(c *config) { c.now = now } }
func WithIDGenerator(generate func() (string, error)) Option {
	return func(c *config) { c.newID = generate }
}

// WithAllocatorChildGenerator is a host seam for the opaque random child
// profile. Sequential and hash child IDs are derived from observed counters;
// this callback is used only when a restored random allocator mints a child.
func WithAllocatorChildGenerator(generate func(driver.OfflineAllocator, string) (string, error)) Option {
	return func(c *config) { c.newAllocatorChildID = generate }
}

// WithHierarchyMaxDepth sets the exclusive dotted depth limit. A root has
// depth zero; with the default of three, root.1.2 is valid and root.1.2.3
// cannot receive another child. Nonpositive values are rejected at configure
// time rather than silently changing the host's contract.
func WithHierarchyMaxDepth(depth int) Option {
	return func(c *config) { c.hierarchyMaxDepth = depth }
}
func WithClaimTTL(ttl time.Duration) Option { return func(c *config) { c.claimTTL = ttl } }

// WithRollbackWindow enables the common write-boundary admission guard. The
// guard is disabled by default; host configuration is the only intended way to
// enable it for a prepared switch-day window.
func WithRollbackWindow(enabled bool) Option {
	return func(c *config) { c.rollbackWindow = enabled }
}

// WithRollbackKindMapping supplies the trusted reverse mapping used for kinds
// that have gained an actual Beads representation. The callback must come from
// the host's canonical reverse/export path, never from client input.
func WithRollbackKindMapping(mapping reverseconverter.KindMapping) Option {
	return func(c *config) { c.reverseKindMapping = mapping }
}

// WithObserver replaces the default bounded in-process collector. Hosts may
// use this only for another internal observer that obeys the same closed
// vocabulary; pipeline inputs are never passed to it. Nil retains the default.
func WithObserver(observer Observer) Option { return func(c *config) { c.observer = observer } }

type Writer struct {
	store driver.Store
	lease driver.ActiveLease
	cfg   config
	err   error
}

var _ model.Writer = (*Writer)(nil)
var _ port.OutcomeWriter = (*Writer)(nil)

// New uses an already active storage lease. It never creates a second store.
// Invalid callback configuration is refused by Apply before any storage write.
func New(store driver.Store, lease driver.ActiveLease, options ...Option) *Writer {
	w := &Writer{store: store, lease: lease, cfg: config{now: time.Now, newID: newID, newAllocatorID: func(a driver.OfflineAllocator) (string, error) { return a.NextID() }, newAllocatorChildID: func(a driver.OfflineAllocator, parent string) (string, error) { return a.NextChildID(parent) }, hierarchyMaxDepth: 3, claimTTL: 5 * time.Minute}}
	for _, option := range options {
		option(&w.cfg)
	}
	if w.cfg.observer == nil {
		w.cfg.observer = NewCollector()
	}
	w.err = w.configure()
	return w
}

func (w *Writer) configure() error {
	if w.store == nil || w.lease == nil || w.cfg.now == nil || w.cfg.newID == nil || w.cfg.newAllocatorID == nil || w.cfg.newAllocatorChildID == nil || w.cfg.claimTTL < time.Millisecond || w.cfg.hierarchyMaxDepth < 1 {
		return fmt.Errorf("pipeline requires a store, active lease, clock and positive claim TTL")
	}
	if w.cfg.auditSet && (w.cfg.audit == nil || w.cfg.authorizer != nil && w.cfg.eventContext == nil) {
		return fmt.Errorf("audit requires a pinned ontology source and trusted context for an installed authorizer")
	}
	ids := map[string]bool{}
	add := func(id string, missing bool) error {
		if id == "" || ids[id] || missing {
			return fmt.Errorf("invalid or duplicate callback %q", id)
		}
		ids[id] = true
		return nil
	}
	owners := map[string]int{}
	for i, b := range w.cfg.mutators {
		if err := add(b.ID, b.Mutator == nil); err != nil {
			return err
		}
		for _, path := range b.Owns {
			if _, err := fieldName(path); err != nil {
				return err
			}
			if _, exists := owners[path]; exists {
				return fmt.Errorf("multiple owners for %s", path)
			}
			owners[path] = i
		}
	}
	// Deterministic Kahn ordering: a field's owner runs before its readers.
	remaining := append([]MutatorBinding(nil), w.cfg.mutators...)
	ordered := make([]MutatorBinding, 0, len(remaining))
	done := map[string]bool{}
	for len(ordered) < len(remaining) {
		candidates := []int{}
		for i, b := range remaining {
			if done[b.ID] {
				continue
			}
			ready := true
			for _, path := range b.Reads {
				if owner, ok := owners[path]; ok && owner != i && !done[remaining[owner].ID] {
					ready = false
				}
			}
			if ready {
				candidates = append(candidates, i)
			}
		}
		if len(candidates) == 0 {
			return fmt.Errorf("mutator dataflow contains a cycle")
		}
		sort.Slice(candidates, func(i, j int) bool { return remaining[candidates[i]].ID < remaining[candidates[j]].ID })
		for _, i := range candidates {
			ordered = append(ordered, remaining[i])
			done[remaining[i].ID] = true
		}
	}
	w.cfg.mutators = ordered
	for _, b := range w.cfg.validators {
		if err := add(b.ID, b.Validator == nil); err != nil {
			return err
		}
	}
	for _, b := range w.cfg.posts {
		if err := add(b.ID, b.Subscriber == nil); err != nil {
			return err
		}
	}
	if len(w.cfg.posts) > 0 {
		if _, ok := w.store.(driver.ResponseFinalizer); !ok {
			return fmt.Errorf("post subscribers require durable response finalization")
		}
		if w.cfg.postFailures == nil {
			w.cfg.postFailures, _ = w.store.(driver.CheckpointStore)
		}
		if w.cfg.postFailures == nil {
			return fmt.Errorf("post subscribers require durable dead-letter metadata")
		}
	}
	return nil
}

func newID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "pl-" + hex.EncodeToString(value[:]), nil
}
