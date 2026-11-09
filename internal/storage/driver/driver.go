// Package driver defines the internal transactional storage boundary. Only
// drivers and the core write pipeline may use WriteTx; plugins receive no
// transaction or engine handles.
package driver

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type SchemaVersion uint64
type Hash string
type StoreID string

type Config struct {
	DSN     string
	Options map[string]any
}
type Info struct {
	Name         string
	Version      string
	Capabilities []string
}
type Meta struct {
	StoreID       StoreID
	Incarnation   string
	SchemaVersion SchemaVersion
	State         string
	Head          model.Revision
	HistoryFloor  model.Revision
}
type View struct {
	Branch      model.Branch
	At          *model.Revision
	Incarnation string // required; must match Meta.Incarnation
}
type Ref = model.Ref
type Record struct {
	Ref      Ref
	Kind     string
	Revision model.Revision
	Body     []byte
}
type Query struct {
	Filter map[string]any
	Cursor *Cursor
	Limit  int
	AsOf   *model.Revision
}
type PageReq struct {
	Cursor *Cursor
	Limit  int
}
type Page[T any] struct {
	Items      []T
	NextCursor *Cursor
	AsOf       model.Revision
}
type Version struct {
	Ref       Ref
	Revision  model.Revision
	Base      bool
	Patch     []byte
	Body      []byte
	CreatedAt time.Time
	Principal model.PrincipalRef
	// Deleted is a tombstone. A nil body alone never means deletion. Versions
	// are append-only, with a full base at creation and at least every 32
	// versions thereafter, so historical replay needs at most 31 diffs.
	Deleted bool
}
type DerivedTable string
type DerivedRow struct {
	Key   string
	Value []byte
	// Index is an optional trusted-core projection of these exact logical
	// bytes. It changes neither this row's identity nor its lock/sentinel
	// obligations. Drivers must preserve it atomically or refuse the write.
	Index *SelectionIndex
}
type LockKeyKind string
type ReadFact string
type Sentinel string

const (
	LockRecord   LockKeyKind = "record"
	LockReadFact LockKeyKind = "read_fact"
	LockSentinel LockKeyKind = "sentinel"
	LockDerived  LockKeyKind = "derived"
)

// LockKey is the engine-neutral name for every item in the transaction lock
// set. Ref is used for records, ReadFact for recorded predicate/read facts,
// Sentinel for predicates, and DerivedRowKey for derived rows. Exactly one
// key form matches Kind. Drivers compare the tuple (kind-rank, encoded-key)
// bytewise.
type LockKey struct {
	Kind     LockKeyKind
	Ref      *Ref
	Fact     ReadFact
	Sentinel Sentinel
	Derived  *DerivedRowKey
}
type DerivedRowKey struct {
	Table DerivedTable
	Key   string
}
type LockRequest struct {
	Key  LockKey
	Mode LockMode
}
type LockState struct {
	Key      LockKey
	Revision model.Revision
	Record   *Record
	Derived  *DerivedRow
	// Present distinguishes an absent key at revision zero from an existing
	// zero-revision key. Deleted keys retain their last revision, with Present
	// false and no Record/Derived value, so delete/recreate races are visible.
	Present bool
}
type LockSet []LockRequest

const (
	SentinelGraphBlocking Sentinel = "graph/blocking"
	SentinelOntology      Sentinel = "ontology"
	SentinelPlugins       Sentinel = "plugins"
)

const (
	DerivedReadyState      DerivedTable = "ready_state"
	DerivedAncestorClosure DerivedTable = "ancestor_closure"
	DerivedCutClosure      DerivedTable = "cut_closure"
	DerivedAttribute       DerivedTable = "derived_attribute"
	DerivedSortKey         DerivedTable = "sort_key"
	DerivedReferenceIndex  DerivedTable = "reference_index"
	DerivedGrant           DerivedTable = "grant"
	DerivedOntology        DerivedTable = "ontology"
	DerivedPlugins         DerivedTable = "plugins"
)

var (
	GraphBlockingSentinelLock = LockKey{Kind: LockSentinel, Sentinel: SentinelGraphBlocking}
	OntologySentinelLock      = LockKey{Kind: LockSentinel, Sentinel: SentinelOntology}
	PluginsSentinelLock       = LockKey{Kind: LockSentinel, Sentinel: SentinelPlugins}
)

func validateSentinelPart(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "/\x00") {
		return fmt.Errorf("sentinel %s component is not canonical", name)
	}
	return nil
}

// NewSentinel is the sole boundary for values entering LockKey.Sentinel. It
// accepts the complete design census, including its parameterized forms, and
// refuses old aliases such as ready-set, closure, grant, and plugin.
func NewSentinel(raw string) (Sentinel, error) {
	s := Sentinel(raw)
	if err := ValidateSentinel(s); err != nil {
		return "", err
	}
	return s, nil
}

func NewBeadSentinel(id string) (Sentinel, error) {
	if err := validateSentinelPart("id", id); err != nil {
		return "", err
	}
	return NewSentinel("bead/" + id)
}

func NewReadySentinel(project, typ string) (Sentinel, error) {
	if err := validateSentinelPart("project", project); err != nil {
		return "", err
	}
	if err := validateSentinelPart("type", typ); err != nil {
		return "", err
	}
	return NewSentinel("ready/" + project + "/" + typ)
}

func NewClosureSentinel(id string) (Sentinel, error) {
	if err := validateSentinelPart("id", id); err != nil {
		return "", err
	}
	return NewSentinel("closure/" + id)
}

func NewGrantSentinel(scope string) (Sentinel, error) {
	if err := validateSentinelPart("scope", scope); err != nil {
		return "", err
	}
	return NewSentinel("grant/" + scope)
}

func ValidateSentinel(s Sentinel) error {
	raw := string(s)
	switch raw {
	case string(SentinelGraphBlocking), string(SentinelOntology), string(SentinelPlugins):
		return nil
	}
	if strings.HasPrefix(raw, "bead/") {
		return validateSentinelPart("id", strings.TrimPrefix(raw, "bead/"))
	}
	if strings.HasPrefix(raw, "ready/") {
		parts := strings.Split(raw, "/")
		if len(parts) != 3 {
			return fmt.Errorf("ready sentinel must be ready/<project>/<type>")
		}
		if err := validateSentinelPart("project", parts[1]); err != nil {
			return err
		}
		return validateSentinelPart("type", parts[2])
	}
	if strings.HasPrefix(raw, "closure/") {
		return validateSentinelPart("id", strings.TrimPrefix(raw, "closure/"))
	}
	if strings.HasPrefix(raw, "grant/") {
		return validateSentinelPart("scope", strings.TrimPrefix(raw, "grant/"))
	}
	return fmt.Errorf("unknown sentinel %q", raw)
}

func NewReadFact(raw string) (ReadFact, error) {
	fact := ReadFact(raw)
	if err := fact.Validate(); err != nil {
		return "", err
	}
	return fact, nil
}

func (fact ReadFact) Validate() error {
	if string(fact) == "" || strings.TrimSpace(string(fact)) != string(fact) || strings.ContainsRune(string(fact), '\x00') {
		return fmt.Errorf("read fact is not canonical")
	}
	return nil
}

func (key DerivedRowKey) Validate() error {
	switch key.Table {
	case DerivedReadyState, DerivedAncestorClosure, DerivedCutClosure, DerivedAttribute,
		DerivedSortKey, DerivedReferenceIndex, DerivedGrant, DerivedOntology, DerivedPlugins:
	default:
		return fmt.Errorf("unknown derived table %q", key.Table)
	}
	if key.Key == "" || strings.TrimSpace(key.Key) != key.Key || strings.ContainsRune(key.Key, '\x00') {
		return fmt.Errorf("derived row key is not canonical")
	}
	return nil
}

func (k LockKey) Validate() error {
	switch k.Kind {
	case LockRecord:
		if k.Ref == nil || k.Fact != "" || k.Sentinel != "" || k.Derived != nil {
			return fmt.Errorf("record lock key has multiple or missing forms")
		}
		return k.Ref.Validate()
	case LockReadFact:
		if k.Ref != nil || k.Fact == "" || k.Sentinel != "" || k.Derived != nil {
			return fmt.Errorf("read-fact lock key has multiple or missing forms")
		}
		return k.Fact.Validate()
	case LockSentinel:
		if k.Ref != nil || k.Fact != "" || k.Sentinel == "" || k.Derived != nil {
			return fmt.Errorf("sentinel lock key has multiple or missing forms")
		}
		return ValidateSentinel(k.Sentinel)
	case LockDerived:
		if k.Ref != nil || k.Fact != "" || k.Sentinel != "" || k.Derived == nil {
			return fmt.Errorf("derived lock key has multiple or missing forms")
		}
		return k.Derived.Validate()
	default:
		return fmt.Errorf("unknown lock key kind %q", k.Kind)
	}
}

func ValidateLockSet(keys LockSet) error {
	previous := ""
	for _, request := range keys {
		if err := request.Key.Validate(); err != nil {
			return err
		}
		if err := validateLockMode(request.Mode); err != nil {
			return err
		}
		current := lockKeySortKey(request.Key)
		if previous != "" && current <= previous {
			return fmt.Errorf("lock set is unsorted or contains a duplicate key")
		}
		previous = current
	}
	return nil
}

func validateLockMode(mode LockMode) error {
	switch mode {
	case LockShare, LockUpdate, LockSkipLocked:
		return nil
	default:
		return fmt.Errorf("unknown lock mode %q", mode)
	}
}

func lockKeySortKey(key LockKey) string {
	var rank string
	var value string
	switch key.Kind {
	case LockRecord:
		rank = "0"
		if key.Ref.IsID() {
			value = fmt.Sprintf("id:%s:%s", key.Ref.ID, key.Ref.Branch.String())
		} else {
			value = "key:" + key.Ref.Key
		}
	case LockReadFact:
		rank, value = "1", string(key.Fact)
	case LockSentinel:
		rank, value = "2", string(key.Sentinel)
	case LockDerived:
		rank, value = "3", string(key.Derived.Table)+":"+key.Derived.Key
	}
	return rank + ":" + value
}

// Cursor is the decoded driver form of the opaque RPC cursor. The binding
// fields prevent a cursor issued for another method, filter, principal, or
// ontology from being replayed against this reader.
type Cursor struct {
	Incarnation      string
	Revision         model.Revision
	Index            model.Natural
	Method           string
	FilterDigest     string
	PrincipalDigest  string
	OntologyRevision model.Revision
}

type ErrFenced struct {
	ExpectedEpoch uint64
	ObservedEpoch uint64
}

func (e ErrFenced) Error() string { return "driver lease fenced" }

type ErrTruncated struct {
	Floor model.Revision
}

func (e ErrTruncated) Error() string { return "history is truncated" }

// ErrFutureRevision identifies an as-of address that is newer than the
// reader's pinned head. The service turns this storage boundary into the
// catalog's typed not_found outcome for history reads.
type ErrFutureRevision struct {
	Requested model.Revision
	Head      model.Revision
}

func (e ErrFutureRevision) Error() string { return "requested revision is beyond head" }

// ErrIndeterminate means publication may or may not have completed; it is not
// a cancellation/fencing rejection and must not unwrap to one. Graph commits and
// response finalization retain the original IdempotencyKey for receipt readback.
// Checkpoint publication leaves it empty: the caller already holds the checkpoint
// key and expected generation and resolves them through LoadCheckpoint.
type ErrIndeterminate struct {
	IdempotencyKey string
}

func (e ErrIndeterminate) Error() string { return "commit outcome indeterminate" }

type ErrIncarnationChanged struct {
	From string
	To   string
}

func (e ErrIncarnationChanged) Error() string { return "store incarnation changed" }

type LockMode string

const (
	LockShare      LockMode = "share"
	LockUpdate     LockMode = "update"
	LockSkipLocked LockMode = "skipLocked"
)

type ChangesetHeader struct {
	Branch         model.Branch
	Incarnation    string
	Kind           model.ChangesetKind
	Time           time.Time
	Principal      model.PrincipalRef
	Chain          []string
	IdempotencyKey string
	// Method and AgentLabel complete the G4 uniqueness scope. AgentLabel is
	// used only for anonymous principals; authenticated callers use "".
	Method      string
	AgentLabel  string
	RequestHash string
	// Response is stored with the write for exact idempotent replay. The
	// driver fills its top-level revision from the committed changeset, or
	// the existing head for CommitNoop; per-result revisions are preserved.
	Response model.Commit
	// ResponsePending is published atomically with a keyed graph write that
	// will run post subscribers. Without an IdempotencyKey it is invalid.
	// False publishes an immutable final response; CommitNoop requires false.
	// Only ResponseFinalizer may clear a pending receipt, without another write.
	ResponsePending bool
	Mutators        []string
	Changes         []model.ChangeRecord
	// Events is the complete ordered durable-event plan for this changeset.
	// A nil slice preserves the legacy history discriminator; a non-nil slice
	// is validated as a complete plan by model.Changeset.ValidateEvents. A
	// driver owns its retained copy through model.CloneEventDescriptors when
	// it accepts the header; the plan is part of the same atomic commit.
	Events   []model.EventDescriptor
	LockKeys []LockKey // complete, sorted set acquired before any write
}
type StoreHealth struct {
	Driver               string
	Role                 string // active, standby, unknown
	ReplicationLag       time.Duration
	LastBackupCheckpoint time.Time
	LeaseEpoch           uint64
	Head                 model.Revision
	Incarnation          string
	Ontology             string
	PluginSet            string
	AuthorizationChain   []string
}

// CoreSessionRecord is the durable, non-graph state for an interactive Plug
// login. TokenDigest is the SHA-256 digest of the opaque bearer token; the
// bearer itself is never handed to a driver. Principal is deliberately kept
// as a typed value so a session store cannot silently turn an API key or
// another principal kind into interactive human proof.
type CoreSessionRecord struct {
	ID          string
	TokenDigest []byte
	Principal   model.PrincipalRef
	ExpiresAt   time.Time
	Revoked     bool
}

func (r CoreSessionRecord) Validate() error {
	if len(r.ID) != 32 || r.ID != strings.ToLower(r.ID) {
		return fmt.Errorf("core session id is not canonical")
	}
	if _, err := hex.DecodeString(r.ID); err != nil {
		return fmt.Errorf("core session id is not hexadecimal")
	}
	if len(r.TokenDigest) != 32 {
		return fmt.Errorf("core session digest has the wrong length")
	}
	if r.Principal.Kind != "user" || !strings.HasPrefix(r.Principal.ID, "user:") ||
		len(r.Principal.ID) <= len("user:") || strings.ContainsAny(r.Principal.ID, " \t\r\n\x00") ||
		r.Principal.Sponsor != nil || len(r.Principal.Caveats) != 0 {
		return fmt.Errorf("core session principal is not an unscoped user")
	}
	if r.ExpiresAt.IsZero() {
		return fmt.Errorf("core session expiry is required")
	}
	return nil
}

// CoreSessionStore is an optional durable capability implemented by storage
// drivers that can retain revocable core sessions. It is intentionally
// separate from Store so the memory driver and existing storage fakes do not
// acquire a false durability contract. Implementations must make Put atomic
// with their active-session capacity check and must preserve revoked rows.
type CoreSessionStore interface {
	PutCoreSession(context.Context, CoreSessionRecord, time.Time, int) error
	LookupCoreSession(context.Context, string) (CoreSessionRecord, error)
	RevokeCoreSession(context.Context, string) (bool, error)
}

type MigrationReport struct {
	From, To SchemaVersion
	Applied  []string
	Warnings []string
}
type DumpOptions struct {
	History  string
	PageSize int
}

// IdempotencyRecord retains the committed response and request digest in the
// same atomic publication as the graph and log, or as a CommitNoop receipt.
// For a no-op, Changeset is the zero value (Kind == ""): there is no log entry.
// Commit.Revision is authoritative for response lookup and finalization.
// A repeated key with a different RequestHash is ErrConflict, never permission
// to execute another graph write. Receipts share the same G4 scope/window.
// While ResponsePending, Commit is the initial committed response, not a
// successful result core may return. Its metadata remains available for
// digest checks, authorization and bounded polling through fresh readers.
// False means Commit is final and immutable, including all post warnings.
type IdempotencyRecord struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       model.Changeset
	ResponsePending bool
}

// NoopCommitter is an optional WriteTx capability; callers must return
// ErrUnsupported when absent. CommitNoop requires the complete acquired locks,
// Head taken last, and exact header LockKeys, just like Commit. It rejects any
// staged record, tombstone, blob, derived-row or predicate/read-fact mutation,
// and any header Changes. It atomically checks the active lease (G5) and G4.
// With an IdempotencyKey, the principal/anonymous-agent scope, Method and
// RequestHash must be valid and a caller-owned response is saved durably;
// without a key it may succeed without a receipt. A matching live receipt
// replays its final Commit, including an ordinary commit's response. A pending
// header is invalid; an existing pending receipt returns ErrConflict, never
// an unfinished successful response. No-op receipts are final immediately and
// never invoke post subscribers.
// Otherwise Response.Revision becomes the serialized current head, preserving
// every per-result revision, result and warning. It publishes no graph data,
// versions, blobs, derived rows, bumped revisions, log entry or head advance.
// Success completes the transaction and releases its locks. Determinate
// failures publish nothing; uncertain outcomes return ErrIndeterminate with
// the original key so callers can resolve a possibly published receipt.
// Ordinary Commit encountering a no-op receipt returns ErrConflict because
// it cannot return a changeset; callers resolve it with IdempotencyByKey.
type NoopCommitter interface {
	CommitNoop(context.Context, ChangesetHeader) (model.Commit, error)
}

// Checkpoint is unversioned durable delivery metadata. It never advances the
// graph head or appears in graph history. Generation zero means absent.
type Checkpoint struct {
	Generation uint64
	Payload    []byte
}

// CheckpointStore is an optional driver capability. Callers must fail with
// ErrUnsupported when it is absent; an in-process substitute is not durable.
// Missing keys load as Checkpoint{0, nil}. Keys pass ValidateCheckpointKey.
// Save atomically checks the current store's active lease and expected
// generation, then publishes generation+1. A stale/foreign lease is ErrFenced;
// a CAS mismatch is ErrCheckpointConflict. Overflow refuses without writing.
// Inputs and every returned Payload are copied, including an empty payload.
// Fencing, cancellation, CAS failure, invalid input and overflow publish nothing.
// An uncertain publication instead returns ErrIndeterminate with an empty
// IdempotencyKey and no successful Checkpoint, never a cancellation/fencing
// rejection. Reload with a fresh context and compare generation and payload;
// later generations cannot identify this attempt. Any retry keeps the original
// expected generation, so a possibly completed CAS cannot be applied twice.
type CheckpointStore interface {
	LoadCheckpoint(context.Context, string) (Checkpoint, error)
	SaveCheckpoint(context.Context, ActiveLease, string, uint64, []byte) (Checkpoint, error)
}

// PersistentCheckpointStore is the stronger checkpoint capability required by
// plugin state that promises restart durability. CheckpointStore alone is also
// implemented by disposable stores such as memory, so callers must not infer
// persistence from that interface.
type PersistentCheckpointStore interface {
	CheckpointStore
	Persistent() bool
}

// DurableCheckpointStore is the vocabulary alias used by durable plugin
// adapters. Keep one contract so callers do not treat ordinary checkpoints as
// restart durable.
type DurableCheckpointStore = PersistentCheckpointStore

const (
	CapabilityPersistentCheckpoints = "checkpoints.persistent"
)

// CheckpointGuardTx stages an unversioned metadata observation for Commit.
// Commit checks every expected generation atomically with graph publication,
// serialized against SaveCheckpoint even when the key is absent. A changed
// generation returns ErrCheckpointConflict and publishes nothing. The guard
// never bumps a graph sentinel or changes the checkpoint. Callers fail closed
// with ErrUnsupported if a transaction lacks this capability.
type CheckpointGuardTx interface {
	ExpectCheckpoint(context.Context, string, uint64) error
}

// RecordGuardedCheckpointStore publishes metadata only while the named live
// record still has the observed revision in the supplied incarnation. The
// record/incarnation check, checkpoint CAS, lease fence and publication are
// atomic; a changed/deleted record returns ErrConflict without writing. The
// CheckpointStore uncertain-publication and original-generation retry contract
// applies here too. This closes the opposite side of the heartbeat/reclaim race
// without turning a quiet heartbeat into a graph write.
type RecordGuardedCheckpointStore interface {
	SaveCheckpointIfRecord(context.Context, ActiveLease, string, uint64, []byte, Ref, model.Revision, string) (Checkpoint, error)
}

// ResponseFinalizer is an optional capability for durable post-subscriber
// outcomes; callers must return ErrUnsupported when absent. FinalizeResponse
// atomically checks the active lease, full G4 scope and Commit.Revision, appends
// the post warnings exactly once, and clears ResponsePending. Even an empty
// post outcome must clear pending. Initial results, per-result revisions,
// warnings, request hash, receipt age and changeset are preserved; graph data,
// versions, log and head never change. Warning inputs and receipt reads are
// caller-owned copies. Nil and empty post-warning lists are the same outcome.
//
// False is immutable: repeating the same post outcome succeeds, a different
// outcome is ErrConflict. An initially final receipt accepts only an empty
// post outcome. A revision mismatch is ErrConflict; missing/expired scope is
// ErrNotFound; a stale/foreign lease is ErrFenced. Validation, cancellation,
// fencing and other determinate errors publish nothing and leave the receipt
// intact. An uncertain finalization instead returns ErrIndeterminate with the
// original key, never a cancellation/fencing rejection. Resolve it through
// IdempotencyByKey in a fresh reader/context for the same scope and revision;
// retry only the same known post outcome with an active lease, never the graph
// write or its callbacks. Uncertainty alone is not proof of finalization.
//
// Core must never return a pending Commit as success or rerun callbacks for
// it. Pending replay polls fresh pinned readers with a bounded, cancellable
// context: an existing reader's snapshot does not observe finalization. The
// original writer retries only its same known post outcome in a bounded
// detached window, then reloads the final durable receipt before any success.
// Exhaustion, cancellation, fencing or restart without the post outcome must
// return a retryable busy error with committed revision, idempotency key and
// responsePending details, never fabricate success or run posts again. A
// durable unresolved pending receipt is a recoverable committed outcome, not
// an indeterminate graph write. It survives finalizer failure/lease takeover
// for same-outcome retry by an active holder within the unchanged 24h window.
type ResponseFinalizer interface {
	FinalizeResponse(ctx context.Context, lease ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error
}

const (
	LeaseTTL          = 10 * time.Second
	LeaseRenewEvery   = 3 * time.Second
	IdempotencyWindow = 24 * time.Hour
	VersionBaseEvery  = 32
)

type Driver interface {
	Info() Info
	Open(context.Context, Config) (Store, error)
}
type Store interface {
	Meta(context.Context) (Meta, error)
	Migrate(context.Context, SchemaVersion) (MigrationReport, error)

	// Lease blocks until the holder is active or ctx ends. The active holder
	// is fenced by epoch in every write, heartbeat and checkpoint commit.
	Lease(ctx context.Context, holder string, ttl time.Duration) (ActiveLease, error)
	Health(context.Context) (StoreHealth, error)

	// Read never waits for writers. It captures an immutable committed
	// snapshot, resolving a nil At to the head once. A missing/mismatched
	// incarnation fails with ErrIncarnationChanged before reading; history
	// below HistoryFloor fails with ErrTruncated. Returned values are owned
	// by the caller and cannot mutate storage or another reader's snapshot.
	Read(context.Context, View) (Reader, error)
	// Write binds the transaction to this store's active lease epoch. It
	// checks fencing on every mutation and atomically with commit (G5).
	Write(context.Context, ActiveLease) (WriteTx, error)
	// Close releases the store's handles and active lease. Callers must close
	// every reader and commit or roll back every transaction before closing
	// the store. Close is idempotent and never commits staged writes.
	Close() error
}

type publicationAdmissionKey struct{}
type preparationAdmissionKey struct{}

// WithPreparationAdmission marks a bounded pipeline preparation snapshot.
// Pooled drivers may admit it before reserving a connection; ordinary readers
// remain independent. The snapshot must close before publication admission.
func WithPreparationAdmission(ctx context.Context) context.Context {
	return context.WithValue(ctx, preparationAdmissionKey{}, true)
}

// PreparationAdmission reports whether a pipeline snapshot requested resource
// admission before reserving a pooled connection.
func PreparationAdmission(ctx context.Context) bool {
	marked, _ := ctx.Value(preparationAdmissionKey{}).(bool)
	return marked
}

// WithPublicationAdmission marks a pipeline transaction that will serialize
// through the publication head. A pooled driver may admit it before reserving
// a connection. This is only a resource hint: all lease and lock checks remain
// mandatory, and ordinary Write transactions retain independent admission.
func WithPublicationAdmission(ctx context.Context) context.Context {
	return context.WithValue(ctx, publicationAdmissionKey{}, true)
}

// PublicationAdmission reports whether a pipeline transaction requested
// resource admission before reserving a pooled connection.
func PublicationAdmission(ctx context.Context) bool {
	marked, _ := ctx.Value(publicationAdmissionKey{}).(bool)
	return marked
}

type ActiveLease interface {
	Epoch() uint64
	Renew(context.Context) error
	// Lost closes on fencing, release, or failure to renew before expiry.
	// Once closed, the server stops admitting reads and writes until a new
	// lease is acquired. Renew must never resurrect an expired lease.
	Lost() <-chan struct{}
	Release(context.Context) error
}

// DerivedPrefixReader optionally enumerates the union of opaque key prefixes
// in one table at the same snapshot as Reader.DerivedRows. Rows are live,
// duplicate-free, sorted bytewise and caller-owned, including staged changes.
// Empty input selects no rows; an empty prefix selects the whole table. The
// caller still guards predicate sentinels and observes consumed exact keys.
type DerivedPrefixReader interface {
	DerivedRowsForPrefixes(context.Context, DerivedTable, []string) ([]DerivedRow, error)
}

type Reader interface {
	// Snapshot returns the bound branch, incarnation and revision (At is
	// always non-nil), without another store lookup. It is caller-owned.
	Snapshot() View
	Get(context.Context, []Ref) ([]Record, error)
	// Blob reads only data committed in this snapshot (or staged by the
	// current WriteTx). It returns a caller-owned copy; absent hashes fail
	// with ErrNotFound. Blob publication is atomic with the changeset.
	Blob(context.Context, Hash) ([]byte, error)
	// Observe reads committed key revisions/presence from this snapshot in
	// input order, including missing keys. Keys must be valid, sorted and
	// duplicate-free. It never acquires transaction locks. The pipeline
	// compares these observations to Lock's current committed state.
	Observe(context.Context, []LockKey) ([]LockState, error)
	// DerivedRows enumerates the live rows of one opaque table/key prefix in
	// bytewise key order at this snapshot (including a transaction's staged
	// changes). Empty prefix selects the whole table. Callers must guard the
	// corresponding predicate sentinel and Observe the consumed exact keys;
	// enumeration itself does not acquire locks or infer semantic scopes.
	DerivedRows(context.Context, DerivedTable, string) ([]DerivedRow, error)
	Query(context.Context, Query) (Page[Record], error)
	Versions(context.Context, Ref, PageReq) (Page[Version], error)
	// Changesets returns complete committed changesets strictly after the
	// cursor's Revision in ascending order. Nil starts at the beginning.
	// Index must be zero; nonzero Index is rejected. Partial-event resume at
	// revision R reads after R-1 and filters delivered indexes in R in the
	// delivery adapter. Changesets are never partially returned or truncated.
	Changesets(context.Context, *Cursor, int) ([]model.Changeset, error)
	// ChangesetByKey looks up (principal, anonymousAgentLabel, method, key).
	// Principal is the canonical scope from PrincipalKey. A miss is (nil,
	// nil). Lookup honors the snapshot and 24-hour deduplication window;
	// the idempotency key itself remains in the log forever (G4).
	// A no-op receipt has no changeset and also returns (nil, nil).
	ChangesetByKey(ctx context.Context, principal, anonymousAgentLabel, method, key string) (*model.Changeset, error)
	// IdempotencyByKey returns both ordinary and no-op receipts from this
	// snapshot under the same scope/window; a miss is (nil, nil). Returned
	// values are caller-owned. Use Commit.Revision even if Changeset is zero.
	// Pending receipts are visible but never successful replay responses. A
	// fresh reader is required to observe a pending-to-final transition; a
	// missing receipt after observing pending never permits another write.
	IdempotencyByKey(ctx context.Context, principal, anonymousAgentLabel, method, key string) (*IdempotencyRecord, error)
	// Close releases this reader's snapshot resources and is idempotent.
	// For a WriteTx it rolls back an unfinished transaction and is a no-op
	// after commit or rollback. Closing never commits staged writes.
	Close() error
}

// WriteTx inherits Reader.Close: closing rolls back unfinished work, releases
// its resources and locks, and is a no-op after commit or rollback. It never
// commits staged writes.
type WriteTx interface {
	Reader
	// keys must be globally sorted and duplicate-free. The returned state is
	// the latest committed revision observed while holding each lock, in
	// input order, including missing keys. Acquire the complete set once,
	// before staging any write. LockSkipLocked omits unavailable keys (and
	// their states); a failed lock call acquires no partial set. Share locks
	// cannot authorize writes. WriteTx reads see staged writes and latest
	// committed locked state, never another transaction's staged writes.
	Lock(context.Context, []LockRequest) ([]LockState, error)
	// Head serializes revision allocation only after all other locks have
	// been acquired. It is the last lock (G1); no later Lock call is legal.
	// Only successful Commit advances head; Rollback consumes no revision.
	Head(context.Context) (model.Revision, error)
	// Put stages a live record under its update/skipLocked record lock.
	// Revisions and versions are assigned at commit, never by callers.
	Put(context.Context, Record) error
	// Delete stages an explicit tombstone and version, retaining the key's
	// last changed revision for observations and historical reads.
	Delete(context.Context, Ref) error
	PutBlob(context.Context, []byte) (Hash, error)
	// Every removed/added row requires its corresponding derived update
	// lock. Row mutations, versions and predicate bumps publish atomically.
	// Core selects every exact affected sentinel scope: Key/Value are opaque
	// to drivers. Drivers enforce lock/bump/family coverage; family coverage
	// alone does not prove that core declared every semantically affected
	// scope. Ready rows require ready/<project>/<type>; ancestor/cut/attribute
	// rows require closure/<id>; grant, ontology and plugin rows require their
	// respective sentinel families. Sort/reference rows need no sentinel.
	PutDerived(context.Context, DerivedTable, []DerivedRow, []DerivedRow) error
	// BumpSentinels stages predicate revisions, including previously absent
	// predicates. Each sentinel requires an acquired update/skipLocked lock.
	// At commit each bumped key receives the new changeset revision, once.
	BumpSentinels(context.Context, []Sentinel) error
	// BumpReadFacts stages opaque read-fact revisions atomically with the
	// changeset. Each fact requires an acquired update/skipLocked fact lock;
	// the driver never infers a mapping from record or sentinel identities.
	// Missing facts can be bumped; rollback publishes no new key revision.
	BumpReadFacts(context.Context, []ReadFact) error
	// Commit repeats exactly the acquired LockKeys, rejects undeclared writes
	// and atomically publishes records, tombstones, versions, blobs, derived
	// rows, bumped revisions and the changeset (G3). It enforces G4 under the
	// commit lock and G5 atomically with publication. An uncertain outcome
	// returns ErrIndeterminate with the original idempotency key.
	Commit(context.Context, ChangesetHeader) (model.Changeset, error)
	// Rollback is idempotent, releases all locks, and discards staged state.
	// A completed transaction rejects further data operations with ErrTxClosed;
	// Rollback and Close remain idempotent.
	Rollback(context.Context) error
}
