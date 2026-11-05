package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// ReadyOrderMutation is a final image from the trusted core readiness compiler.
// SourceDigest binds that classification and complete semantic item to the
// exact source body staged in the same transaction. Drivers never infer core
// eligibility by decoding a source document or a derived row.
type ReadyOrderMutation struct {
	Ref                    model.Ref
	Item                   *ReadyMembershipItem
	Priority               int64
	Project, Type, Binding string
	Eligible, Local        bool
	SourceDigest           string
	Sort                   *ReadyOrderSort
}

// ReadyOrderPublicationWriter registers a bounded, independently replayable
// transition after all source, Ready-row and predicate staging. Ordinary
// PutDerived calls do not grant permission to certify Ready state.
type ReadyOrderPublicationWriter interface {
	StageReadyOrderPublication(context.Context, ReadyOrderPublication) error
}

type ReadyOrderPublicationData struct {
	Incarnation    string
	Base, Revision model.Revision
	Mutations      []ReadyOrderMutation
	Before         []LockState
	Removed, Added []DerivedRow
}

// ReadyOrderPublication owns its immutable encoding. Data always returns a
// detached copy, so a caller cannot alter a registered proof through aliases.
type ReadyOrderPublication struct{ encoded []byte }

// IsZero reports an absent optional publication without decoding its immutable
// image. Nonzero values come from NewReadyOrderPublication; engines must still
// decode and independently replay Data under their physical publication fence.
func (p ReadyOrderPublication) IsZero() bool { return len(p.encoded) == 0 }

func NewReadyOrderPublication(data ReadyOrderPublicationData) (ReadyOrderPublication, error) {
	base, err := data.Base.Uint64()
	next, nextErr := data.Revision.Uint64()
	if err != nil || nextErr != nil || base == ^uint64(0) || next != base+1 || data.Incarnation == "" {
		return ReadyOrderPublication{}, fmt.Errorf("%w: Ready publication binding", ErrUnsupported)
	}
	for _, mutation := range data.Mutations {
		if ValidateHierarchyRef(mutation.Ref) != nil || mutation.Item != nil && !ValidReadyHash(mutation.SourceDigest) {
			return ReadyOrderPublication{}, fmt.Errorf("%w: Ready publication source image", ErrUnsupported)
		}
	}
	encoded, err := json.Marshal(data)
	return ReadyOrderPublication{encoded: encoded}, err
}

func (p ReadyOrderPublication) Data() (ReadyOrderPublicationData, error) {
	var data ReadyOrderPublicationData
	if len(p.encoded) == 0 {
		return data, fmt.Errorf("%w: absent Ready publication", ErrUnsupported)
	}
	decoder := json.NewDecoder(bytes.NewReader(p.encoded))
	decoder.UseNumber()
	err := decoder.Decode(&data)
	return data, err
}

func (p ReadyOrderPublication) Digest() string { return SelectionDigest(p.encoded) }
