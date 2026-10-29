package ontology

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CompilationPlan binds a compiled candidate to the exact expected ontology
// revision. It is a static compilation artifact, not permission to apply:
// migration directives, affected-row checks, authorization and protected
// storage writes must be supplied by the migration/apply layer.
type CompilationPlan struct {
	Expect         model.Revision `json:"expect"`
	SnapshotDigest string         `json:"snapshotDigest"`
	Digest         string         `json:"digest"`
}

func PlanCompilation(expect model.Revision, candidate Snapshot) (CompilationPlan, error) {
	canonical, err := expect.Canonical()
	if err != nil {
		return CompilationPlan{}, err
	}
	if candidate.Digest() == "" {
		return CompilationPlan{}, fmt.Errorf("candidate has not compiled successfully")
	}
	plan := CompilationPlan{Expect: model.Revision(canonical), SnapshotDigest: candidate.Digest()}
	data, err := json.Marshal(struct {
		Version        string         `json:"version"`
		Expect         model.Revision `json:"expect"`
		SnapshotDigest string         `json:"snapshotDigest"`
	}{"ontology-compilation/1", plan.Expect, plan.SnapshotDigest})
	if err != nil {
		return CompilationPlan{}, err
	}
	digest := sha256.Sum256(data)
	plan.Digest = hex.EncodeToString(digest[:])
	return plan, nil
}
