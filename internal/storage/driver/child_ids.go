package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// NumericChildReader observes the largest live bead ID with a direct decimal
// suffix, independently of its Parent field. Excluded identities are replaced
// by the caller's staged images. Implementations seek ordered identity metadata
// and do not materialize bead bodies. The caller records the graph predicate.
type NumericChildReader interface {
	MaxChildNumber(context.Context, string, []model.Ref) (uint64, error)
}

// ChildIDOrder is the storage-index projection of a branch-zero record key.
// Leading zeroes retain ChildNumber's interpretation. An overflowing decimal
// sorts above every uint64 so it cannot be silently ignored by a maximum seek.
// Empty results denote a nonnumeric/direct-child identity.
func ChildIDOrder(key string) (parent, number string) {
	id, ok := strings.CutPrefix(key, "0:id:")
	if !ok {
		return "", ""
	}
	id, ok = strings.CutSuffix(id, ":0")
	if !ok {
		return "", ""
	}
	i := strings.LastIndexByte(id, '.')
	if i <= 0 || i == len(id)-1 {
		return "", ""
	}
	parent, suffix := id[:i], id[i+1:]
	if strings.Trim(suffix, "0123456789") != "" {
		return "", ""
	}
	n, err := ChildNumber(parent, id)
	if err != nil {
		if errors.Is(err, ErrUnsupported) {
			return parent, "~"
		}
		return "", ""
	}
	return parent, fmt.Sprintf("%020d", n)
}
