// Package depid derives the deterministic identity of a dependency edge.
//
// Plug keeps dependency type and metadata mutable, but the edge identity is
// only the ordered endpoint pair. This is the same UUIDv5 contract used by
// the pinned Beads source and lets independent stores address the same edge
// without a random database primary key.
package depid

import "github.com/google/uuid"

// Namespace is the fixed UUIDv5 namespace for dependency identities. It is a
// permanent wire value: changing it would re-key every existing edge.
var Namespace = uuid.MustParse("bdd74eb9-a20a-554a-985b-54b8f3e64d8b")

const separator = "\x1f"

// New returns the RFC-4122 UUIDv5 for (issueID, target). target is the
// resolved endpoint value, including an external: prefix when applicable.
func New(issueID, target string) string {
	return uuid.NewSHA1(Namespace, []byte(issueID+separator+target)).String()
}
