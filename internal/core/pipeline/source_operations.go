package pipeline

import (
	"bytes"
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// SourceOperation is an unpublished operation on a host-owned active image.
// Its result occupies the original operation's position in the one receipt.
// It never becomes a core record, version, allocator input or ontology bead.
type SourceOperation struct {
	Change         port.Change
	Result         model.OpResult
	LeaseExpiresAt *time.Time
	LeaseTtlMs     *model.Natural
}

type SourceCoreImage func(context.Context) (map[string]Document, error)
type SourceOperationHandler func(context.Context, model.Op, SourceCoreImage) (model.Op, *SourceOperation, error)

// SourcePublication joins a prepared active image to the ordinary transaction.
// Head and Namespace remain mandatory even when Mutation is nil (read-only).
type SourcePublication struct {
	Head      driver.CoreSnapshotExpectation
	Namespace driver.CheckpointExpectation
	Mutation  *driver.CheckpointMutation
	// FinalResults can replace external results after ordered preparation,
	// when several operations share one endpoint's final image and revision.
	// Ordinals and references must still identify the original operations.
	FinalResults map[int]model.OpResult
}

// SourcePreparation runs only after canonical durable receipt replay misses,
// once per conflict attempt. The host owns routing, type checks and the image;
// the Writer owns original request identity, authorization and publication.
type SourcePreparation func(context.Context, driver.Reader, func(SourceOperationHandler) error) (SourcePublication, error)

type sourceOperationsKey struct{}
type sourceHandlerKey struct{}
type sourceOperations struct {
	prepare SourcePreparation
	kind    func(model.Ref) string
}

func WithSourceOperations(ctx context.Context, prepare SourcePreparation, kind func(model.Ref) string) context.Context {
	return context.WithValue(ctx, sourceOperationsKey{}, sourceOperations{prepare, kind})
}

func (w *Writer) prepareSourceOperations(ctx context.Context, reader driver.Reader, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, release *claimReleaseMode) (*prepared, error) {
	source, _ := ctx.Value(sourceOperationsKey{}).(sourceOperations)
	if source.prepare == nil {
		return w.prepare(ctx, reader, by, cs, meta, release)
	}
	var p *prepared
	publication, err := source.prepare(ctx, reader, func(handle SourceOperationHandler) error {
		var err error
		p, err = w.prepare(context.WithValue(ctx, sourceHandlerKey{}, handle), reader, by, cs, meta, release)
		return err
	})
	if err != nil {
		return nil, err
	}
	if p == nil || publication.Head.Incarnation != reader.Snapshot().Incarnation || publication.Head.Head.String() != reader.Snapshot().At.String() || publication.Namespace.StoreID != publication.Head.StoreID || publication.Namespace.Incarnation != publication.Head.Incarnation || driver.ValidateCheckpointKey(publication.Namespace.Key) != nil {
		return nil, driver.ErrConflict
	}
	current, err := w.store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	if publication.Head.StoreID != current.StoreID || publication.Head.Incarnation != current.Incarnation {
		return nil, driver.ErrConflict
	}
	if mutation := publication.Mutation; mutation != nil {
		if mutation.StoreID != publication.Head.StoreID || mutation.Incarnation != publication.Head.Incarnation || mutation.Key != publication.Namespace.Key || mutation.Expected != publication.Namespace.Expected {
			return nil, driver.ErrConflict
		}
		copy := *mutation
		copy.Payload = append([]byte(nil), mutation.Payload...)
		publication.Mutation = &copy
	}
	for position, result := range publication.FinalResults {
		active, exists := p.external[position]
		if !exists || active.Result.Ref != result.Ref {
			return nil, failure("internal_error", "final source result must preserve its operation reference")
		}
		detached, err := clone(result)
		if err != nil {
			return nil, err
		}
		active.Result = detached
		p.external[position] = active
	}
	publication.FinalResults = nil
	p.source = &publication
	if p.checkpointReads == nil {
		p.checkpointReads = map[string]uint64{}
	}
	p.checkpointReads[publication.Namespace.Key] = publication.Namespace.Expected
	return p, nil
}

func (p *prepared) sourceCoreImage(ctx context.Context) (map[string]Document, error) {
	docs, err := p.documents(ctx)
	if err != nil {
		return nil, err
	}
	for id, pending := range p.writes {
		if pending.deleted {
			delete(docs, id)
		} else if pending.kind == "bead" {
			data, err := pending.body()
			if err != nil {
				return nil, err
			}
			doc, err := DecodeDocument(data)
			if err != nil {
				return nil, err
			}
			docs[id] = doc
		}
	}
	return docs, nil
}

// Source decisions consumed the ordered request images. A synchronous callback
// cannot subsequently replace those core images without restaging the active
// decisions. Refuse that unsupported composition before either plane writes.
func (p *prepared) sourceCoreBodies() (map[string][]byte, error) {
	result := map[string][]byte{}
	for id, pending := range p.writes {
		body, err := pending.body()
		if err != nil {
			return nil, err
		}
		result[id] = body
	}
	return result, nil
}

func (p *prepared) checkSourceCoreBodies(before map[string][]byte) error {
	for id, body := range before {
		after, err := p.writes[id].body()
		if err != nil {
			return err
		}
		if !bytes.Equal(body, after) {
			return failure("unsupported", "mixed source decisions cannot be rewritten by synchronous core callbacks")
		}
	}
	return nil
}

// Ontology receives only its core bead domain. Authorization and native
// callbacks still receive every operation in the complete ordered proposal.
func (p *prepared) ontologyProposal() port.ProposedChange {
	proposal := p.proposal
	if p.source != nil && p.closes != nil && proposal.Meta.Method == "bead.close" {
		// Installed source calls use the trusted typed close lowering, including
		// ordinary targets. Only ontology receives the ordered batch context;
		// authorization, callbacks and receipt identity retain bead.close.
		proposal.Meta.Method = "tx.apply"
	}
	if len(p.external) == 0 {
		return proposal
	}
	proposal.Changes = nil
	for _, change := range p.proposal.Changes {
		if change.Resource.Kind == "bead" {
			proposal.Changes = append(proposal.Changes, change)
		}
	}
	return proposal
}
