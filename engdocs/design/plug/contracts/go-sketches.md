# Plug Go contract sketches

These are public-shape sketches, not implementation code. `pluginapi` and
`sdk/go` are the only public Go surfaces (P47); the driver and core model
packages are internal. Names and fields are deliberately wire-oriented so a
native plugin and a source-built WASM plugin receive the same JSON-RPC
messages.

## Shared model and `ChangeSet`

```go
// package model (internal/core/model)
package model

import (
	"context"
	"encoding/json"
	"time"
)

type Revision int64
type Branch uint32
type Ref struct {
	ID     string `json:"id,omitempty"`
	Branch Branch `json:"branch,omitempty"`
	// Key addresses a record created earlier in the same ChangeSet. Exactly
	// one of ID or Key is set on the wire.
	Key string `json:"key,omitempty"`
}
type ResourceRef struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}
type FieldSet []string

type Precondition struct {
	Ref      Ref       `json:"ref"`
	Revision *Revision `json:"revision,omitempty"`
	Field    string    `json:"field,omitempty"`
	Hash     string    `json:"hash,omitempty"`
}

type ChangeSet struct {
	Ops    []Op           `json:"ops"` // one atomic revision; up to 1,000 ops
	Expect []Precondition `json:"expect,omitempty"`
}

type Op struct {
	Kind     OpKind      `json:"kind"`
	Create   *CreateOp   `json:"create,omitempty"`
	Patch    *PatchOp    `json:"patch,omitempty"`
	Reparent *ReparentOp `json:"reparent,omitempty"`
	Link     *LinkOp     `json:"link,omitempty"`
	Comment  *CommentOp  `json:"comment,omitempty"`
	Attach   *AttachOp   `json:"attach,omitempty"`
	Delete   *DeleteOp   `json:"delete,omitempty"`
	SetMeta  *SetMetaOp  `json:"setMeta,omitempty"`
}

type OpKind string

const (
	OpCreate   OpKind = "create"
	OpPatch    OpKind = "patch"
	OpReparent OpKind = "reparent"
	OpLink     OpKind = "link"
	OpUnlink   OpKind = "unlink"
	OpComment  OpKind = "comment"
	OpAttach   OpKind = "attach"
	OpDelete   OpKind = "delete"
	OpSetMeta  OpKind = "setMeta"
)

type CreateOp struct {
	Key     string         `json:"key,omitempty"`
	Type    string         `json:"type"`
	Fields  map[string]any `json:"fields"`
	Parent  *Ref           `json:"parent,omitempty"`
	Project string         `json:"project,omitempty"`
}
type PatchOp struct {
	Target Ref            `json:"target"`
	Fields map[string]any `json:"fields"`
}
type ReparentOp struct {
	Target  Ref    `json:"target"`
	Parent  *Ref   `json:"parent,omitempty"`
	Project string `json:"project,omitempty"`
}
type LinkOp struct {
	From Ref    `json:"from"`
	To   Ref    `json:"to"`
	Kind string `json:"kind"`
}
type CommentOp struct {
	Target Ref    `json:"target"`
	Body   string `json:"body"`
}
type AttachOp struct {
	Target     Ref    `json:"target"`
	Kind       string `json:"kind"`
	URI        string `json:"uri,omitempty"`
	ContentRef string `json:"contentRef,omitempty"`
	MediaType  string `json:"mediaType"`
	Summary    string `json:"summary"`
}
type DeleteOp struct {
	Target Ref `json:"target"`
}
type SetMetaOp struct {
	Target Ref             `json:"target"`
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value"`
}

type PrincipalRef struct {
	Kind    string         `json:"kind"` // anonymous, user, agent, service, plugin, system
	ID      string         `json:"id"`
	Sponsor *PrincipalRef  `json:"sponsor,omitempty"`
	Caveats map[string]any `json:"caveats,omitempty"`
}
type Commit struct {
	Revision Revision   `json:"revision"`
	Results  []OpResult `json:"results"`
	Warnings []Warning  `json:"warnings"`
	// Dry-run responses also carry these fields. They are omitted on a real
	// commit unless the caller requested a dry run.
	WouldCommit bool        `json:"wouldCommit,omitempty"`
	Violations  []Violation `json:"violations,omitempty"`
}
type OpResult struct {
	Ref      Ref               `json:"ref"`
	Revision Revision          `json:"revision"`
	Record   json.RawMessage   `json:"record,omitempty"`
	SetBy    map[string]string `json:"setBy,omitempty"`
}
type Warning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
type Violation struct {
	Guard   string         `json:"guard"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// Writer is the only core entry point for graph writes. Named verbs lower to
// this same pipeline; tx.apply supplies ChangeSet directly.
type Writer interface {
	Apply(ctx context.Context, by PrincipalRef, cs ChangeSet, meta WriteMeta) (Commit, error)
}
type WriteMeta struct {
	Project        string
	IdempotencyKey string
	DryRun         bool
	Return         string // full or minimal
	Traceparent    string
	Agent          string
}

type ChangesetKind string

const (
	ChangesetWrite     ChangesetKind = "write"
	ChangesetDerived   ChangesetKind = "derived"
	ChangesetRevert    ChangesetKind = "revert"
	ChangesetMigration ChangesetKind = "migration"
	ChangesetRedact    ChangesetKind = "redact"
	ChangesetRestore   ChangesetKind = "restore"
)

type Changeset struct {
	Revision       Revision       `json:"revision"`
	Branch         Branch         `json:"branch"`
	Incarnation    string         `json:"incarnation"`
	Kind           ChangesetKind  `json:"kind"`
	Time           time.Time      `json:"time"`
	Principal      PrincipalRef   `json:"principal"`
	Chain          []string       `json:"chain"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty"`
	Mutators       []string       `json:"mutators,omitempty"`
	Changes        []ChangeRecord `json:"changes"`
}
type ChangeRecord struct {
	Ref          Ref      `json:"ref"`
	Op           OpKind   `json:"op"`
	FromRevision Revision `json:"fromRevision"`
	ChangedPaths []string `json:"changedPaths"`
}
```

`Op.Kind` is a closed union: the selected pointer must match `Kind`, and
startup registry lint rejects an operation or method whose effect lowers to no
member. Protected plugin-set, lock, instance-config, and ontology records are
not legal targets for `tx.apply`, `setMeta`, or `history.revert`.

## `pluginapi` (public)

```go
// package pluginapi
package pluginapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// These wire-safe types live in pluginapi itself. A public SDK cannot import
// Plug's internal core/model package; the host converts these values at the
// boundary.
type Revision int64
type Ref struct {
	ID     string
	Branch uint32
	Key    string
}
type ResourceRef struct {
	Kind string
	Ref  Ref
}
type PrincipalRef struct {
	Kind    string
	ID      string
	Sponsor *PrincipalRef
	Caveats map[string]any
}
type Violation struct {
	Guard   string
	Message string
	Details map[string]any
}
type Op struct {
	Kind    string
	Payload json.RawMessage
}
type Record struct {
	Ref      Ref
	Kind     string
	Revision Revision
	Body     json.RawMessage
}

type ConfigDoc = map[string]any
type ConfigProblem struct {
	Path, Code, Message string
	Details             map[string]any
}
type IntegrityParams struct {
	Revision Revision
	Project  string
}
type IntegrityProblem struct {
	Path, Code, Message string
	Details             map[string]any
}

type Configurable interface {
	ValidateConfig(context.Context, ConfigDoc) ([]ConfigProblem, error)
	ApplyConfig(context.Context, ConfigDoc) error
	CheckIntegrity(context.Context, IntegrityParams) ([]IntegrityProblem, error)
}

type Host interface {
	// Call is the plugin's only Plug API. The host validates the method,
	// manifest prefix, owned-field and bridge/egress grants before dispatch.
	Call(ctx context.Context, method string, params, result any) error
	State() StateStore
	HTTP() *http.Client                                  // host egress proxy; unavailable in hot calls
	Secret(context.Context, string) (SecretValue, error) // audited injection
}

type StateStore interface {
	Get(ctx context.Context, key string, dst any) (found bool, err error)
	Put(ctx context.Context, key string, value any, expect Revision) (Revision, error)
	Delete(ctx context.Context, key string, expect Revision) error
	Revision(ctx context.Context, key string) (Revision, error)
}
type SecretValue struct {
	Name     string
	Redacted bool
}
type RuntimeState string

const (
	RuntimeHealthy  RuntimeState = "healthy"
	RuntimeDegraded RuntimeState = "degraded"
)

type ProposedChange struct {
	RequestID    string       `json:"requestId"`
	Principal    PrincipalRef `json:"principal"`
	BaseRevision Revision     `json:"baseRevision"`
	DryRun       bool         `json:"dryRun"`
	Changes      []Change     `json:"changes"`
}
type CommittedChange struct {
	RequestID string       `json:"requestId"`
	Principal PrincipalRef `json:"principal"`
	Revision  Revision     `json:"revision"`
	Changes   []Change     `json:"changes"`
}
type Change struct {
	Resource ResourceRef       `json:"resource"`
	Op       Op                `json:"op"`
	Before   json.RawMessage   `json:"before,omitempty"`
	After    json.RawMessage   `json:"after,omitempty"`
	SetBy    map[string]string `json:"setBy,omitempty"`
}
type Patch struct {
	Resource ResourceRef
	Paths    map[string]json.RawMessage
}
type Verdict struct {
	Allow      bool
	Violations []Violation
}

type SnapshotReader interface {
	// Reads are pinned to ProposedChange.BaseRevision and join the commit
	// guard's read set. A hot call cannot write or use egress.
	Get(context.Context, []Ref) ([]Record, error)
	Query(context.Context, Query) (Page[Record], error)
	Beads(context.Context, []Ref) ([]View, error)
	Closure(context.Context, Ref, bool) ([]Ref, error)
	Grants(context.Context, []SubjectRef, []Ref) ([]Grant, error)
}
type Mutator interface {
	Mutate(context.Context, ProposedChange, SnapshotReader) ([]Patch, error)
}
type Validator interface {
	Validate(context.Context, ProposedChange, SnapshotReader) (Verdict, error)
}
type PostSubscriber interface {
	// Runs after commit, before the response, at the committed revision. It
	// cannot change the committed ChangeSet; failures are warnings and DLQ.
	After(context.Context, CommittedChange, SnapshotReader) error
}

type Query struct {
	Filter map[string]any
	Cursor string
	Limit  int
}
type Page[T any] struct {
	Items      []T
	NextCursor string
	AsOf       Revision
}
type View struct {
	ID     string
	Type   string
	Fields map[string]any
}
type SubjectRef struct{ Kind, ID string }
type Grant struct {
	Subject SubjectRef
	Scope   Ref
	Role    string
}

// A native or WASM plugin registers only methods under x.<alias>.*. Authz
// links and synchronous subscribers are compiled in for v1; mode-2 authz is
// not permitted.
type Method struct {
	Name       string
	Params     any
	Result     any
	Permission string
	Resource   func(params any) []Ref
}
type Registry interface{ Register(Method) error }
```

Hot-path calls (`authn.authenticate`, `authz.*`, `mutate`, `validate`) receive
only a revision-pinned `SnapshotReader`; `Host.Call` writes fail with
`plugin_host_write_in_sync_call`, and HTTP egress fails with
`plugin_egress_in_hot_path`. Post subscribers may use their granted egress and
state, but are capped by the one-second host deadline. A guest trap or pool
exhaustion is mapped by the host to the public error catalog; plugin-internal
`plugin_*` errors never cross the client boundary.

## Storage driver

```go
// package driver (internal/storage/driver)
package driver

import (
	"context"
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
	Branch model.Branch
	At     *model.Revision
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
	Cursor string
	Limit  int
	AsOf   *model.Revision
}
type PageReq struct {
	Cursor string
	Limit  int
}
type Version struct {
	Ref       Ref
	Revision  model.Revision
	Base      bool
	Patch     []byte
	Body      []byte
	CreatedAt time.Time
	Principal model.PrincipalRef
}
type DerivedTable string
type DerivedRow struct {
	Key   string
	Value []byte
}
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
	Mutators       []string
	Changes        []model.ChangeRecord
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
type MigrationReport struct {
	From, To SchemaVersion
	Applied  []string
	Warnings []string
}
type DumpOptions struct {
	History  string
	PageSize int
}

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

	// Read never waits for writers. The returned Reader is bound to the
	// requested branch/as-of view.
	Read(context.Context, View) (Reader, error)
	Write(context.Context, ActiveLease) (WriteTx, error)
}
type ActiveLease interface {
	Epoch() uint64
	Renew(context.Context) error
	Lost() <-chan struct{}
	Release(context.Context) error
}
type Reader interface {
	Get(context.Context, []Ref) ([]Record, error)
	Query(context.Context, Query) (Page[Record], error)
	Versions(context.Context, Ref, PageReq) (Page[Version], error)
	Changesets(context.Context, model.Revision, int) ([]model.Changeset, error)
	ChangesetByKey(context.Context, string, string, string) (*model.Changeset, error)
}
type WriteTx interface {
	Reader
	Lock(context.Context, []Ref, LockMode) ([]Record, error)
	Head(context.Context) (model.Revision, error)
	Put(context.Context, Record) error
	PutBlob(context.Context, []byte) (Hash, error)
	PutDerived(context.Context, DerivedTable, []DerivedRow, []DerivedRow) error
	Commit(context.Context, ChangesetHeader) (model.Changeset, error)
	Rollback(context.Context) error
}
```

The driver conformance contract is:

- **G1:** revisions are gapless and commit ordered; the head row is taken last
  in Postgres and `BEGIN IMMEDIATE` provides the SQLite equivalent.
- **G2:** `Lock` returns the latest committed state (`FOR SHARE`/`FOR UPDATE`
  on Postgres; the driver's transaction lock on SQLite; per-ref locks in
  memory).
- **G3:** records, versions, blobs, derived rows, and the changeset commit
  atomically.
- **G4:** `(principal, anonymous-agent label when applicable, method, key)` is
  unique for 24 hours and the key is retained on the changeset.
- **G5:** every write, heartbeat, and checkpoint commits only under the active
  lease epoch. A deposed holder receives `ErrFenced`; after `Lost()` closes,
  the server stops admitting reads and writes until a new lease is acquired.

The lease policy selected by P71 is a ten-second TTL and three-second renewal.
`ErrFenced`, `ErrTruncated`, and `ErrIndeterminate` are driver errors with
wire mappings in [errors.md](errors.md); they are not additional JSON-RPC
methods.

## Deliberate boundaries

The sketches leave JSON-RPC wire types in `contracts/openrpc.json` and keep
storage implementation details behind `driver`. `pluginapi` may depend on
wire-safe model aliases, but plugin code must not receive a driver transaction,
SQL handle, filesystem path, process environment, or unmediated network
client.
