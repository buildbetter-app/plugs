package driver

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	SourcePlaneAddressContractV1 = "beads-source-planes/1"
	SourcePlanePlanVersionV1     = "beads-joint-import-plan/1"
	SourcePlaneRuntimeContractV1 = "beads-source-runtime/1"
	SourcePlaneOriginVersionV1   = "beads-source-origin/1"
	SourcePlaneCommitVersionV1   = "beads-source-commit/1"
)

// CheckpointLifetime distinguishes a selected store's real process lifetime
// from restart persistence. It never changes PersistentCheckpointStore.
type CheckpointLifetime string

const (
	CheckpointLifetimeProcess CheckpointLifetime = "process"
	CheckpointLifetimeRestart CheckpointLifetime = "restart"
)

// WorkflowCheckpointStore is discovered from the selected driver, never a
// caller-supplied boolean. All publication and snapshot capabilities are real.
type WorkflowCheckpointStore interface {
	CheckpointStore
	HeadGuardedCheckpointStore
	SourcePlaneSnapshotStore
	WorkflowCheckpointLifetime() CheckpointLifetime
}

// SourcePlaneSnapshotStore pins graph and checkpoint data in one observation.
// The returned Reader owns the snapshot; Close releases every participant.
type SourcePlaneSnapshotStore interface {
	ReadSourcePlaneSnapshot(context.Context, View, []string) (SourcePlaneSnapshot, error)
}

type SourcePlaneSnapshot interface {
	Reader
	SourcePlaneCheckpoint(string) (Checkpoint, error)
}

type SourceEntityKey struct{ Project, Plane, ID string }
type SourceEdgeKey struct{ Project, Table, ID string }

// SourcePlaneRecordV1 retains a complete original NDJSON record. Raw is not
// authenticated authority and must be verified against the admitted archive.
// Provider preparation decodes numbers losslessly and retains these bytes.
type SourcePlaneRecordV1 struct {
	Kind, Project, Table, ID string
	Raw                      []byte
}

type SourcePlaneStringPairV1 struct{ From, To string }
type SourcePlaneFieldRuleV1 struct{ Source, Target, Disposition string }
type SourcePlaneDispositionV1 struct {
	// Empty StorageClass explicitly means a present SQL NULL. Nonempty values
	// match exact source text. Missing columns and raw empty strings refuse;
	// the source field commitments retain the original null/text bytes. This
	// uses the existing V1 string slot without changing its canonical layout.
	Key, SourceType, StorageClass string
	Ephemeral, NoHistory          bool
	SinkKind, Schema, Lifetime    string
	Statuses                      []SourcePlaneStringPairV1
	Fields                        []SourcePlaneFieldRuleV1
	RefusalPolicy                 string
}
type SourcePlanePrincipalV1 struct {
	Key, Source, Authority string
	PrincipalJSON          []byte
	UnverifiedOriginOnly   bool
}
type SourcePlaneRunV1 struct {
	Project, Key, TargetID, Template, Schema, Mode, State, PrincipalKey string
	Members                                                             []SourceEntityKey // ordered membership; never sorted by the codec
}
type SourcePlaneBytesV1 struct {
	Presence string // absent|null|sql-null|object|text|number|boolean|array|bytes
	Size     uint64
	SHA256   string
}
type SourcePlaneFieldV1 struct {
	Name  string
	Value SourcePlaneBytesV1
}
type SourcePlaneRouteV1 struct {
	Project, SourceID, ActiveTargetID, DurableTargetID, Preference string
}
type SourcePlaneEntityV1 struct {
	Source                                                     SourceEntityKey
	TargetKind, TargetID, DispositionKey, PrincipalKey, RunKey string
	Route                                                      SourcePlaneRouteV1
	Origin                                                     SourcePlaneBytesV1
	Fields                                                     []SourcePlaneFieldV1
}
type SourcePlaneRelationV1 struct {
	Source                  SourceEdgeKey
	From                    SourceEntityKey
	To                      *SourceEntityKey
	External                *string
	Kind, PredicateContract string
	PredicateJSON           []byte
	Origin                  SourcePlaneBytesV1
	Fields                  []SourcePlaneFieldV1
}
type SourcePlaneOriginV1 struct {
	Project, Table, ID, Action string
	Owner                      *SourceEntityKey
	Raw                        SourcePlaneBytesV1
	Fields                     []SourcePlaneFieldV1
	Path, Mode, Layer          string
	Precedence                 []string // source precedence is ordered
	Parsed                     SourcePlaneBytesV1
}
type SourcePlaneProviderV1 struct {
	Alias, Instance, Namespace, Schema, Codec, RuntimeContract string
	Lifetime                                                   CheckpointLifetime
	ExpectedGeneration                                         uint64
	LogicalImageSize                                           uint64
	LogicalImageSHA256, OriginAdapter                          string
}
type SourcePlaneTargetV1 struct {
	StoreID, Incarnation string
	Head                 uint64
	RequireEmpty         bool
	Lifetime             CheckpointLifetime
	RuntimeContract      string
}

// SourcePlanePlanV1 is the closed, canonical logical request. The mutable
// provider payload is represented by its exact checked byte commitment only.
type SourcePlanePlanV1 struct {
	Version, AddressContract, SourceStoreID, ArchiveSHA256 string
	Target                                                 SourcePlaneTargetV1
	Dispositions                                           []SourcePlaneDispositionV1
	Principals                                             []SourcePlanePrincipalV1
	Runs                                                   []SourcePlaneRunV1
	Entities                                               []SourcePlaneEntityV1
	Relations                                              []SourcePlaneRelationV1
	Origins                                                []SourcePlaneOriginV1
	Providers                                              []SourcePlaneProviderV1
}

type SourcePlaneImportBindingV1 struct {
	Version         string `json:"version"`
	CanonicalPlan   []byte `json:"canonicalPlan"`
	JointPlanDigest string `json:"jointPlanDigest"`
}

// SourcePlaneOriginBindingV1 belongs to the namespace envelope. Supported
// provider updates preserve it exactly, including after legitimate mutations.
type SourcePlaneOriginBindingV1 struct {
	Version            string             `json:"version"`
	JointPlanDigest    string             `json:"jointPlanDigest"`
	RequestHash        string             `json:"requestHash"`
	StoreID            StoreID            `json:"storeId"`
	Incarnation        string             `json:"incarnation"`
	Namespace          string             `json:"namespace"`
	InitialGeneration  uint64             `json:"initialGeneration"`
	LogicalImageSHA256 string             `json:"logicalImageSHA256"`
	Lifetime           CheckpointLifetime `json:"lifetime"`
}
type SourcePlaneProviderCommitV1 struct {
	Provider       SourcePlaneProviderV1      `json:"provider"`
	Origin         SourcePlaneOriginBindingV1 `json:"origin"`
	EnvelopeSHA256 string                     `json:"envelopeSHA256"`
}
type SourcePlaneCommitBindingV1 struct {
	Version         string                        `json:"version"`
	JointPlanDigest string                        `json:"jointPlanDigest"`
	RequestHash     string                        `json:"requestHash"`
	StoreID         StoreID                       `json:"storeId"`
	Incarnation     string                        `json:"incarnation"`
	Revision        model.Revision                `json:"revision"`
	Providers       []SourcePlaneProviderCommitV1 `json:"providers"`
}

// PreparedSourcePlaneCheckpoint is data-only. The host verifies its origin,
// payload digest and new Meta before the restore transaction stages it.
type PreparedSourcePlaneCheckpoint struct {
	Mutation CheckpointMutation
	Binding  SourcePlaneProviderCommitV1
}

// SourcePlaneCheckpointPreparer is a trusted host seam. No transaction is
// handed to provider code; no independent SaveCheckpoint is permitted here.
type SourcePlaneCheckpointPreparer interface {
	PrepareRestoreCheckpoint(context.Context, Meta, RestoreImportProfile, SourcePlaneProviderV1, []byte) (PreparedSourcePlaneCheckpoint, error)
}

// SourcePlaneOriginReader verifies the namespace's current immutable origin
// while allowing legitimate later payload/generation changes.
type SourcePlaneOriginReader interface {
	InspectSourcePlaneOrigin(Checkpoint) (SourcePlaneOriginBindingV1, json.RawMessage, error)
}
