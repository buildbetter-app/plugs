// Package readyalternative defines the unchanged CP7 source corpus shared by
// the isolated Ready alternatives. It does not install or certify projections.
package readyalternative

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

const (
	SourceCommit = "89672ed01e7822407d284e45ffd5a69c8c580528"
	CorpusSize   = 100000
	ProjectName  = "cp7-project"
	Seed         = "ready-alternative-20261104"
)

func Ref(n int) model.Ref {
	return model.RefByID(fmt.Sprintf("00000000-0000-4000-8000-%012d", n), model.Natural64(0))
}

// Document is byte-for-byte the body/document helper in the pinned CP7 fixture:
// 100k beads in chains of eleven, maximum parent depth ten, one priority group.
func Document(n int) pipeline.Document {
	d := pipeline.Document{ID: Ref(n).ID, Type: "issue", Project: ProjectName,
		Fields: map[string]any{"title": fmt.Sprintf("CP7 bead %06d", n), "status": "todo", "priority": 0}}
	if (n-1)%11 != 0 {
		parent := Ref(n - 1)
		d.Parent = &parent
	}
	return d
}

func Body(n int) ([]byte, error) { return json.Marshal(Document(n)) }

// Refs returns nil at 100%, exactly 1,000 refs at 1%, and 100 at 0.1%.
// The corpus and raw priority group remain 100,000 at every visibility fraction.
func Refs(stride int) []model.Ref {
	if stride == 1 {
		return nil
	}
	if stride != 100 && stride != 1000 {
		panic("undeclared Ready alternative visibility fraction")
	}
	refs := make([]model.Ref, 0, CorpusSize/stride)
	for n := stride; n <= CorpusSize; n += stride {
		refs = append(refs, Ref(n))
	}
	return refs
}

func BodiesDigest() (string, error) {
	h := sha256.New()
	for n := 1; n <= CorpusSize; n++ {
		body, err := Body(n)
		if err != nil {
			return "", err
		}
		_, _ = h.Write(body)
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
