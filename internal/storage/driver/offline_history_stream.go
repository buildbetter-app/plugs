package driver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const OfflineHistoryStreamVersion = "plug-offline-history-stream/1"

// OfflineHistoryStreamHeader contains no accumulated images or mappings. Gaps,
// native boundaries, baseline tables and provenance are separate bounded frames.
// Verified is trusted verifier output, never operator-supplied authority.
type OfflineHistoryStreamHeader struct {
	Version          string `json:"version"`
	Source           string `json:"source"`
	Verified         bool   `json:"verified"`
	Complete         bool   `json:"complete"`
	First            uint64 `json:"first"`
	Head             uint64 `json:"head"`
	BaselineVerified bool   `json:"baselineVerified"`
	NativeBoundaries uint64 `json:"nativeBoundaries"`
}

// Limits bound EACH pull, not total history. Sources enforce both limits before
// allocating/decoding a frame, including strings and metadata. MaxFrameBytes is
// the size of the frame's encoding/json encoding (without its terminating LF);
// MaxChunkBytes bounds decoded image bytes. A single indivisible metadata value
// that cannot fit must refuse explicitly, never truncate. There is no aggregate
// byte, commit, table or provenance count ceiling here.
type OfflineHistoryStreamLimits struct {
	MaxFrameBytes int
	MaxChunkBytes int
}

// OfflineHistorySourceCommit and OfflineHistorySourceTable retain PHYSICAL
// source commitments. Their hash preimages belong to the source format, not this
// port. The source verifier must reconcile them independently, including
// typed schema/cells, lexical numbers, binary/text, absence and row multiplicity.
// Root and ordered parent IDs are provenance, never new Plug revision authority.
// Tables and TablesSHA256 describe the physical source census, including
// unchanged tables. Tables is independent of the converted record count.
type OfflineHistorySourceCommit struct {
	Root         string `json:"root"`
	Tables       uint64 `json:"tables"`
	TablesSHA256 string `json:"tablesSha256"`
}

type OfflineHistorySourceTable struct {
	Present      bool   `json:"present"`
	Rows         uint64 `json:"rows"`
	SchemaSHA256 string `json:"schemaSha256"`
	TableSHA256  string `json:"tableSha256"`
}

// OfflineHistoryStreamCommit.Tables counts converted records, including row
// and table-meta records. Several records may belong to one physical table.
type OfflineHistoryStreamCommit struct {
	Baseline       bool                        `json:"baseline"`
	SourceStoreID  string                      `json:"sourceStoreId"`
	Project        string                      `json:"project"`
	SourceSequence uint64                      `json:"sourceSequence"`
	Sequence       uint64                      `json:"sequence"`
	CommitIDs      uint64                      `json:"commitIds"`
	ParentIDs      uint64                      `json:"parentIds"`
	EventIDs       uint64                      `json:"eventIds"`
	Tables         uint64                      `json:"tables"`
	Physical       *OfflineHistorySourceCommit `json:"physical,omitempty"`
}

// ID frames occur in commit, parent, event order, preserving order within each
// category (especially the first parent). Each ID is a separate bounded frame.
type OfflineHistoryStreamID struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

// Image hashes cover exact converted Record.Body bytes, independently of the
// physical hashes above. Nil Before/After means converted absence. No preimage
// may be synthesized from a later image. Body chunks can split at ANY byte.
type OfflineHistoryStreamImage struct {
	Bytes  uint64 `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// OfflineHistoryStreamTable.Name identifies the containing physical table.
// Records sharing a Name within a commit must carry identical physical
// before/after commitments, even when a converted row is deleted. A dropped
// table carries a not-present PhysicalAfter, never a row-level substitute.
// Distinct physical table names cannot exceed the commit's Physical.Tables;
// full census/hash reconciliation belongs to the source verifier.
type OfflineHistoryStreamTable struct {
	Name           string                     `json:"name"`
	Ref            model.Ref                  `json:"ref"`
	Kind           string                     `json:"kind"`
	Before         *OfflineHistoryStreamImage `json:"before,omitempty"`
	After          *OfflineHistoryStreamImage `json:"after,omitempty"`
	PhysicalBefore *OfflineHistorySourceTable `json:"physicalBefore,omitempty"`
	PhysicalAfter  *OfflineHistorySourceTable `json:"physicalAfter,omitempty"`
}

type OfflineHistoryStreamChunk struct {
	Image  string `json:"image"` // before, then after
	Offset uint64 `json:"offset"`
	Data   []byte `json:"data"`
}

type OfflineHistoryStreamCommitEnd struct {
	// SHA256 covers "plug-offline-history-commit/1\n", then the canonical
	// commit, ID and table frames, each followed by LF. Image bytes are bound
	// through independently checked image declarations. This is a converted
	// commit commitment, NOT Physical.TablesSHA256.
	SHA256 string `json:"sha256"`
}

type OfflineHistoryStreamEnd struct {
	Commits      uint64 `json:"commits"` // excludes the optional baseline
	Tables       uint64 `json:"tables"`  // includes baseline tables
	DecodedBytes uint64 `json:"decodedBytes"`
}

// A frame has exactly the payload selected by Kind; table-end has no payload.
// Kinds are gap, boundary, commit, id, table, chunk, table-end, commit-end, end.
type OfflineHistoryStreamFrame struct {
	Kind      string                         `json:"kind"`
	Gap       *SourceHistoryGap              `json:"gap,omitempty"`
	Boundary  *NativeHistoryBoundary         `json:"boundary,omitempty"`
	Commit    *OfflineHistoryStreamCommit    `json:"commit,omitempty"`
	ID        *OfflineHistoryStreamID        `json:"id,omitempty"`
	Table     *OfflineHistoryStreamTable     `json:"table,omitempty"`
	Chunk     *OfflineHistoryStreamChunk     `json:"chunk,omitempty"`
	CommitEnd *OfflineHistoryStreamCommitEnd `json:"commitEnd,omitempty"`
	End       *OfflineHistoryStreamEnd       `json:"end,omitempty"`
}

// Entry to PrepareOfflineHistoryStream transfers source ownership, including
// invalid requests: it calls Close exactly once. Next is lazy and synchronous,
// honors cancellation, and returns one bounded frame or an error, NEVER both.
// Buffers are borrowed until the next Next/Close. After a single end frame the
// next pull must return the zero frame and exactly io.EOF. Close must promptly
// release resources even after cancellation; an error invalidates completion.
type OfflineHistoryStreamSource interface {
	Next(context.Context, OfflineHistoryStreamLimits) (OfflineHistoryStreamFrame, error)
	Close() error
}

// This request is an internal trusted-host binding, not an RPC authority object.
// HistoryDigest binds the stream transcript: Version + LF, header JSON + LF,
// then every frame JSON + LF EXCEPT the end frame. HistoryTime is mandatory.
type OfflineHistoryStreamRequest struct {
	Current  Meta
	Profile  RestoreImportProfile
	Header   OfflineHistoryStreamHeader
	When     time.Time
	Lifetime CheckpointLifetime
	Limits   OfflineHistoryStreamLimits
}

type OfflineHistoryStreamBinding struct {
	Current  Meta
	Meta     Meta
	Profile  RestoreImportProfile
	Header   OfflineHistoryStreamHeader
	When     time.Time
	Lifetime CheckpointLifetime
}

// DecodedBytes counts actual converted image bytes passed through the port. It
// is NOT allocated memory or RSS. Adapters measure category charges and process
// RSS separately. The core retains only a bounded frame and active boundaries.
type OfflineHistoryStreamStats struct {
	Commits        uint64
	Tables         uint64
	DecodedBytes   uint64
	PeakFrameBytes int
	PeakChunkBytes int
}

type OfflineHistoryStreamCompletion struct {
	Meta          Meta
	HistorySHA256 string
	RequestHash   string
	Stats         OfflineHistoryStreamStats
}

// OfflineHistoryStreamPublication is issued only for one in-flight Publish
// callback after complete validation and source cleanup. The adapter MUST Claim
// it against its exact Begin binding before any durable effect. Copies share a
// single claim and all expire when that callback returns, including on failure.
// Informational Completion values cannot construct or substitute for this proof.
type OfflineHistoryStreamPublication struct{ session *offlineHistoryStreamSession }

func (p OfflineHistoryStreamPublication) Claim(binding OfflineHistoryStreamBinding) (OfflineHistoryStreamCompletion, error) {
	s := p.session
	if s == nil || !s.publishing || s.claimed {
		return OfflineHistoryStreamCompletion{}, streamError("unissued, consumed or expired publication proof")
	}
	request, err := binding.Profile.RequestHash()
	if err != nil {
		return OfflineHistoryStreamCompletion{}, err
	}
	if binding.Current != s.binding.Current || binding.Meta != s.binding.Meta || binding.Header != s.binding.Header || binding.Lifetime != s.binding.Lifetime || !binding.When.Equal(s.binding.When) || request != s.completion.RequestHash {
		return OfflineHistoryStreamCompletion{}, streamError("publication proof belongs to a different prepared binding")
	}
	s.claimed = true
	return s.completion, nil
}

// Stage owns bounded PRIVATE staging, never visible head/versions/changesets or
// a receipt. Begin rechecks the actual empty target/lifetime under admission,
// complete locks and Head. Stage consumes its borrowed frame before returning;
// it must not mutate it or retain borrowed buffers. It stages gap/boundary and
// per-commit source->revision mapping rows incrementally, not in one final slice.
// It verifies source proofs, duplicate provenance, exact converted JSON/kind/
// preimages, schema/presence and independently rebuilt final-image equality,
// using bounded pages/spooling instead of accumulated history in memory.
// Independently verified HEAD and current checkpoint inputs are bound to
// Profile.ArchiveDigest; final equality is also bound to Profile.DerivedDigest.
// They are never fabricated from consecutive imported revisions or source audit.
//
// Publish first claims its opaque proof, then rechecks target lifetime, lease,
// full record/sentinel locks, all
// staged proofs and original receipt correspondence, then publishes versions,
// changesets, current records, final head, floor, exact gaps/mappings/native
// boundaries, source-plane participants and keyed final receipt atomically, or
// none. It uses only the admitted actor/time, never source attribution. Ordinary
// Commit must refuse these stages. Abort promptly discards private resources,
// independent of a canceled request, and is safe even if Begin failed. Successful
// Publish releases its resources. After ErrIndeterminate, Abort only discards
// unpublished resources: it cannot undo a possible commit or remove its receipt.
// Recovery uses the ORIGINAL keyed receipt; a consumed token cannot republish.
// Begin/Stage borrow immutable input; implementations must not mutate it.
// Engine implementations are a separate layer.
type OfflineHistoryStreamStage interface {
	Begin(context.Context, OfflineHistoryStreamBinding) error
	Stage(context.Context, model.Revision, OfflineHistoryStreamFrame) error
	Publish(context.Context, OfflineHistoryStreamPublication, ChangesetHeader) (model.Changeset, error)
	Abort() error
}

var ErrOfflineHistoryStream = errors.New("invalid offline history stream")

func streamError(detail string) error {
	return fmt.Errorf("%w: %s", ErrOfflineHistoryStream, detail)
}

// PreparedOfflineHistoryStream is returned only after end, clean EOF, complete
// validation AND successful source Close. Copies share a single-use session.
// The caller must Commit or Close it. Calls are serialized; concurrent use is
// unsupported. Neither a zero value nor a failed preparation can publish.
type PreparedOfflineHistoryStream struct{ session *offlineHistoryStreamSession }

type offlineHistoryStreamSession struct {
	stage      OfflineHistoryStreamStage
	binding    OfflineHistoryStreamBinding
	completion OfflineHistoryStreamCompletion
	finished   bool
	publishing bool
	claimed    bool
}

func (p *PreparedOfflineHistoryStream) Completion() (OfflineHistoryStreamCompletion, error) {
	if p == nil || p.session == nil || p.session.finished {
		return OfflineHistoryStreamCompletion{}, streamError("no live completed stream")
	}
	return p.session.completion, nil
}

func (p *PreparedOfflineHistoryStream) Close() error {
	if p == nil || p.session == nil || p.session.finished {
		return nil
	}
	p.session.finished = true
	return p.session.stage.Abort()
}

func (p *PreparedOfflineHistoryStream) Commit(ctx context.Context, h ChangesetHeader) (model.Changeset, error) {
	if _, err := p.Completion(); err != nil {
		return model.Changeset{}, err
	}
	s := p.session
	s.finished = true
	err := ctx.Err()
	if err == nil {
		err = s.binding.Profile.ValidateHeader(h)
	}
	if err == nil && (h.Incarnation != s.binding.Meta.Incarnation || !h.Time.Equal(s.binding.When) || !h.Branch.IsZero() || h.Response.Revision != s.binding.Meta.Head) {
		err = streamError("receipt differs from completed lifetime/time/head")
	}
	if err != nil {
		return model.Changeset{}, errors.Join(err, s.stage.Abort())
	}
	entry, err := s.publish(ctx, h)
	if err == nil && !s.claimed {
		err = streamError("staging adapter did not claim its publication proof")
	}
	if err != nil {
		cleanup := s.stage.Abort()
		var unknown ErrIndeterminate
		var unknownPointer *ErrIndeterminate
		if cleanup != nil && (errors.As(err, &unknown) || errors.As(err, &unknownPointer)) {
			// An uncertain commit cannot become a cancellation/fencing rejection
			// merely because unpublished-resource cleanup also failed.
			return model.Changeset{}, fmt.Errorf("%w; private staging cleanup failed: %v", err, cleanup)
		}
		return model.Changeset{}, errors.Join(err, cleanup)
	}
	return entry, nil
}

func (s *offlineHistoryStreamSession) publish(ctx context.Context, h ChangesetHeader) (model.Changeset, error) {
	s.publishing = true
	defer func() { s.publishing = false }()
	return s.stage.Publish(ctx, OfflineHistoryStreamPublication{session: s}, h)
}
