package driver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// ReadyOrderReader is an optional, uncapped, exact Ready primitive. The trusted
// caller must prove its complete authorization boundary before selecting either
// uniform authorization or a finite, complete set of authorized references.
// Unavailable maintenance coverage is ErrUnsupported, never an empty result.
type ReadyOrderReader interface {
	SelectReadyOrder(context.Context, ReadyOrderPlan) (ReadyOrderResult, error)
}

// FreshReadyOrderExistsReader is separate from snapshot selection. An
// implementation must observe committed eligibility after Head, without new
// locks, caps, staged overlays or a previously pinned snapshot. No engine
// advertises it until that stronger proof exists; absence takes exact fallback.
type FreshReadyOrderExistsReader interface {
	FreshReadyOrderExists(context.Context, ReadyOrderPlan) (bool, error)
}

type ReadyOrderScope struct{ Project, Type *string }
type ReadyOrderPlan struct {
	Incarnation                        string
	ContentRevision, AuthorityRevision model.Revision
	Scope                              ReadyOrderScope
	// Eligibility identifies the effective ontology used to maintain this scope.
	Eligibility string
	// Nil means unrestricted; a non-nil empty slice is the empty set. RawRefs
	// belongs to the request filter, AuthorizedRefs to authorization. They must
	// never be exchanged when choosing the seeded raw-priority-group pivot.
	RawRefs, AuthorizedRefs []model.Ref
	UniformAuthorization    bool
	Seed                    string
	Offset                  uint64
	Limit                   int
}
type ReadyOrderWork struct{ Nodes, Leaves, DigestBytes, WrittenNodes, WrittenBytes uint64 }
type ReadyOrderResult struct {
	Keys       []SelectionKey
	Total      uint64
	Commitment ReadyCommitment
	More       bool
	Work       ReadyOrderWork
}
type ReadyCommitment struct {
	Count uint64
	Hash  string
}

// ReadyMembershipItem is the exact JSON shape of the original Ready digest.
// Projection publication supplies the eventual SOURCE revision, not the
// derived-row revision. No selected payload is needed to hash a maintained leaf.
type ReadyMembershipItem struct {
	ID            string         `json:"id"`
	Type          string         `json:"type"`
	Project       string         `json:"project"`
	Status        string         `json:"status"`
	Revision      model.Revision `json:"revision"`
	Fields        map[string]any `json:"fields,omitempty"`
	Plane         string         `json:"plane,omitempty"`
	Kind          string         `json:"kind,omitempty"`
	Plugin        string         `json:"plugin,omitempty"`
	WorkflowRunID string         `json:"workflowRunId,omitempty"`
}
type ReadyOrderLeaf struct {
	Key      SelectionKey
	Priority int64
	Digest   string
}
type ReadyOrderChild struct {
	Prefix     string
	Commitment ReadyCommitment
}

// A deterministic compressed radix tree, with a permanent empty-prefix root.
// Nodes are keyed by the hexadecimal numeric-priority/bytewise-ID prefix.
// Compression depends only on membership, never insertion order or balancing.
type ReadyOrderNode struct {
	Prefix      string
	Leaf        *ReadyOrderLeaf   `json:",omitempty"`
	Children    []ReadyOrderChild `json:",omitempty"`
	Commitment  ReadyCommitment
	digestBytes uint64
}

// DigestBytes reports the exact canonical node preimage hashed by Seal. It is
// instrumentation only and is excluded from persistence and commitments.
func (n ReadyOrderNode) DigestBytes() uint64 { return n.digestBytes }

func ReadyOrderKey(priority int64, id string) string {
	var p [8]byte
	binary.BigEndian.PutUint64(p[:], uint64(priority)^(uint64(1)<<63))
	return hex.EncodeToString(p[:]) + hex.EncodeToString([]byte(id))
}
func ReadyOrderPivot(seed string, priority int64, count uint64) uint64 {
	if seed == "" || count < 2 {
		return 0
	}
	h := sha256.Sum256([]byte(seed + "\x00" + strconv.FormatInt(priority, 10)))
	return binary.BigEndian.Uint64(h[:8]) % count
}
func ReadyOrderLeafFor(item ReadyMembershipItem, priority int64) (ReadyOrderLeaf, error) {
	if item.ID == "" || item.Revision.Validate() != nil {
		return ReadyOrderLeaf{}, fmt.Errorf("%w: invalid Ready item", ErrUnsupported)
	}
	b, err := json.Marshal(item)
	if err != nil {
		return ReadyOrderLeaf{}, err
	}
	return ReadyOrderLeaf{Key: SelectionKey{Ref: model.RefByID(item.ID, model.Natural64(0)), Revision: item.Revision, Order: []SelectionValue{SelectionInt(priority), SelectionString(item.ID)}, MembershipLeaf: SelectionDigest(b)}, Priority: priority, Digest: SelectionDigest(b)}, nil
}
func ValidReadyHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == sha256.Size && s == strings.ToLower(s)
}

// SealReadyOrderNode binds exact cardinality, all child commitments, the sort
// prefix and the complete original item digest. Empty child subtrees are absent.
func SealReadyOrderNode(n *ReadyOrderNode) error {
	// Persistence omits empty child slices. Normalize before hashing so the
	// commitment is identical before/after reload and after deleting a tail.
	if len(n.Children) == 0 {
		n.Children = nil
	}
	if len(n.Prefix)%2 != 0 {
		return fmt.Errorf("%w: odd Ready prefix", ErrUnsupported)
	}
	if _, err := hex.DecodeString(n.Prefix); err != nil {
		return err
	}
	count := uint64(0)
	if n.Leaf != nil {
		l := n.Leaf
		if ValidateHierarchyRef(l.Key.Ref) != nil || l.Key.Revision.Validate() != nil || !ValidReadyHash(l.Digest) || l.Key.MembershipLeaf != l.Digest || ReadyOrderKey(l.Priority, l.Key.Ref.ID) != n.Prefix {
			return fmt.Errorf("%w: invalid Ready leaf", ErrUnsupported)
		}
		count = 1
	}
	previous := ""
	for i, c := range n.Children {
		if len(c.Prefix) <= len(n.Prefix) || !strings.HasPrefix(c.Prefix, n.Prefix) || len(c.Prefix)%2 != 0 || !ValidReadyHash(c.Commitment.Hash) || c.Commitment.Count == 0 || i > 0 && (c.Prefix <= previous || c.Prefix[:len(n.Prefix)+2] == previous[:len(n.Prefix)+2]) {
			return fmt.Errorf("%w: invalid Ready children", ErrUnsupported)
		}
		if _, err := hex.DecodeString(c.Prefix); err != nil {
			return err
		}
		previous = c.Prefix
		if ^uint64(0)-count < c.Commitment.Count {
			return fmt.Errorf("Ready count overflow")
		}
		count += c.Commitment.Count
	}
	b, err := json.Marshal(struct {
		Domain, Prefix string
		Leaf           *ReadyOrderLeaf
		Children       []ReadyOrderChild
	}{"plug.ready.radix.v2", n.Prefix, n.Leaf, n.Children})
	if err != nil {
		return err
	}
	n.Commitment = ReadyCommitment{Count: count, Hash: SelectionDigest(b)}
	n.digestBytes = uint64(len(b))
	return nil
}

// ReadyCommitmentFromLeaves is the exact fallback and sparse oracle. It has
// the same semantic commitment as maintained nodes, including the empty set.
func ReadyCommitmentFromLeaves(leaves []ReadyOrderLeaf) (ReadyCommitment, error) {
	nodes, err := BuildReadyOrderNodes(leaves)
	if err != nil {
		return ReadyCommitment{}, err
	}
	return nodes[""].Commitment, nil
}
func BuildReadyOrderNodes(leaves []ReadyOrderLeaf) (map[string]ReadyOrderNode, error) {
	leaves = append([]ReadyOrderLeaf(nil), leaves...)
	sort.Slice(leaves, func(i, j int) bool {
		return ReadyOrderKey(leaves[i].Priority, leaves[i].Key.Ref.ID) < ReadyOrderKey(leaves[j].Priority, leaves[j].Key.Ref.ID)
	})
	seen := map[string]bool{}
	for _, leaf := range leaves {
		if seen[leaf.Key.Ref.ID] {
			return nil, fmt.Errorf("duplicate Ready identity")
		}
		seen[leaf.Key.Ref.ID] = true
	}
	nodes := map[string]ReadyOrderNode{}
	var build func(string, []ReadyOrderLeaf) (ReadyOrderNode, error)
	build = func(prefix string, ls []ReadyOrderLeaf) (ReadyOrderNode, error) {
		n := ReadyOrderNode{Prefix: prefix}
		for start := 0; start < len(ls); {
			key := ReadyOrderKey(ls[start].Priority, ls[start].Key.Ref.ID)
			if key == prefix {
				l := ls[start]
				n.Leaf = &l
				start++
				continue
			}
			end := start + 1
			for end < len(ls) && ReadyOrderKey(ls[end].Priority, ls[end].Key.Ref.ID)[:len(prefix)+2] == key[:len(prefix)+2] {
				end++
			}
			last := ReadyOrderKey(ls[end-1].Priority, ls[end-1].Key.Ref.ID)
			common := CommonReadyPrefix(key, last)
			child, err := build(common, ls[start:end])
			if err != nil {
				return n, err
			}
			n.Children = append(n.Children, ReadyOrderChild{Prefix: common, Commitment: child.Commitment})
			start = end
		}
		if err := SealReadyOrderNode(&n); err != nil {
			return n, err
		}
		nodes[prefix] = n
		return n, nil
	}
	_, err := build("", leaves)
	return nodes, err
}
func CommonReadyPrefix(a, b string) string {
	n := 0
	for n+2 <= len(a) && n+2 <= len(b) && a[n:n+2] == b[n:n+2] {
		n += 2
	}
	return a[:n]
}

// ReadyMembershipV2 composes the maintained core order with the authorized
// workflow tail. Fallback uses the same core tree and exact workflow JSON.
// Legacy 64-hex cursors continue to use their original length-prefixed SHA256.
func ReadyMembershipV2(core ReadyCommitment, workflowJSON [][]byte) string {
	h := sha256.New()
	h.Write([]byte("plug.ready.membership.v2\x00"))
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], core.Count)
	h.Write(size[:])
	h.Write([]byte(core.Hash))
	binary.BigEndian.PutUint64(size[:], uint64(len(workflowJSON)))
	h.Write(size[:])
	for _, b := range workflowJSON {
		binary.BigEndian.PutUint64(size[:], uint64(len(b)))
		h.Write(size[:])
		h.Write(b)
	}
	return "v2:" + hex.EncodeToString(h.Sum(nil))
}
