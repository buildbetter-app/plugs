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
	"time"

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
	Sort     *ReadyOrderSort `json:",omitempty"`
}

// ReadyOrderSort is the trusted Writer's persisted public ordering tuple.
// A nil tuple retains the original priority/ID fixture domain.
type ReadyOrderSort struct {
	Bucket    int
	Rank      int
	CreatedAt time.Time
	RankedAt  time.Time
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

// ReadyOrderTreeKey preserves the legacy tree encoding for synthetic sources.
// Writer sources use the public bucket/rank/creation/ID order. Every component
// is fixed-width before the variable ID suffix, so bytewise radix order agrees
// with the public comparator.
func ReadyOrderTreeKey(l ReadyOrderLeaf) (string, error) {
	if l.Sort == nil {
		return ReadyOrderKey(l.Priority, l.Key.Ref.ID), nil
	}
	s := l.Sort
	ns := s.CreatedAt.UTC().UnixNano()
	if s.Bucket < 0 || s.Bucket > 1 || s.Rank < 0 || s.Rank > 999 || s.CreatedAt.IsZero() || s.RankedAt.IsZero() || !time.Unix(0, ns).UTC().Equal(s.CreatedAt.UTC()) {
		return "", fmt.Errorf("%w: invalid Ready sort tuple", ErrUnsupported)
	}
	if len(l.Key.Order) != 4 || l.Key.Order[0].Int == nil || *l.Key.Order[0].Int != int64(s.Bucket) ||
		l.Key.Order[1].Int == nil || *l.Key.Order[1].Int != int64(s.Rank) ||
		l.Key.Order[2].Int == nil || *l.Key.Order[2].Int != ns ||
		l.Key.Order[3].String == nil || *l.Key.Order[3].String != l.Key.Ref.ID {
		return "", fmt.Errorf("%w: Ready sort key disagrees with tuple", ErrUnsupported)
	}
	var prefix [17]byte
	prefix[0] = byte(s.Bucket)
	binary.BigEndian.PutUint64(prefix[1:9], uint64(s.Rank))
	binary.BigEndian.PutUint64(prefix[9:17], uint64(ns)^(uint64(1)<<63))
	return hex.EncodeToString(prefix[:]) + hex.EncodeToString([]byte(l.Key.Ref.ID)), nil
}
func ReadyOrderGroupPrefix(l ReadyOrderLeaf) (string, error) {
	key, err := ReadyOrderTreeKey(l)
	if err != nil || l.Sort == nil {
		return key[:min(16, len(key))], err
	}
	return key[:34], nil
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
func ReadyOrderLeafForSort(item ReadyMembershipItem, priority int64, sortTuple *ReadyOrderSort) (ReadyOrderLeaf, error) {
	leaf, err := ReadyOrderLeafFor(item, priority)
	if err != nil || sortTuple == nil {
		return leaf, err
	}
	value := *sortTuple
	leaf.Sort = &value
	leaf.Key.Order = []SelectionValue{
		SelectionInt(int64(value.Bucket)), SelectionInt(int64(value.Rank)),
		SelectionInt(value.CreatedAt.UTC().UnixNano()), SelectionString(item.ID),
	}
	_, err = ReadyOrderTreeKey(leaf)
	return leaf, err
}
func ValidReadyHash(s string) bool {
	if len(s) != 2*sha256.Size {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// SealReadyOrderNode binds exact cardinality, all child commitments, the sort
// prefix and the complete original item digest. Empty child subtrees are absent.
func SealReadyOrderNode(n *ReadyOrderNode) error {
	count, err := readyOrderNodeCount(n)
	if err != nil {
		return err
	}
	b, err := readyOrderNodePreimage(n)
	if err != nil {
		return err
	}
	n.Commitment = ReadyCommitment{Count: count, Hash: SelectionDigest(b)}
	n.digestBytes = uint64(len(b))
	return nil
}

// ValidateReadyOrderNode performs every structural, cardinality and leaf-JSON
// check made by Seal without computing a commitment. Private mutable trees may
// use its count while preparing a batch, but must Seal every final dirty node
// before serializing it or exposing a publication.
func ValidateReadyOrderNode(n *ReadyOrderNode) (uint64, error) {
	count, err := readyOrderNodeCount(n)
	if err != nil {
		return 0, err
	}
	if n.Leaf != nil {
		if _, err := json.Marshal(n.Leaf); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func readyOrderNodeCount(n *ReadyOrderNode) (uint64, error) {
	// Persistence omits empty child slices. Normalize before hashing so the
	// commitment is identical before/after reload and after deleting a tail.
	if len(n.Children) == 0 {
		n.Children = nil
	}
	if len(n.Prefix)%2 != 0 {
		return 0, fmt.Errorf("%w: odd Ready prefix", ErrUnsupported)
	}
	if _, err := hex.DecodeString(n.Prefix); err != nil {
		return 0, err
	}
	count := uint64(0)
	if n.Leaf != nil {
		l := n.Leaf
		key, keyErr := ReadyOrderTreeKey(*l)
		if ValidateHierarchyRef(l.Key.Ref) != nil || l.Key.Revision.Validate() != nil || !ValidReadyHash(l.Digest) || l.Key.MembershipLeaf != l.Digest || keyErr != nil || key != n.Prefix {
			return 0, fmt.Errorf("%w: invalid Ready leaf", ErrUnsupported)
		}
		count = 1
	}
	previous := ""
	for i, c := range n.Children {
		if len(c.Prefix) <= len(n.Prefix) || !strings.HasPrefix(c.Prefix, n.Prefix) || len(c.Prefix)%2 != 0 || !ValidReadyHash(c.Commitment.Hash) || c.Commitment.Count == 0 || i > 0 && (c.Prefix <= previous || c.Prefix[:len(n.Prefix)+2] == previous[:len(n.Prefix)+2]) {
			return 0, fmt.Errorf("%w: invalid Ready children", ErrUnsupported)
		}
		if _, err := hex.DecodeString(c.Prefix); err != nil {
			return 0, err
		}
		previous = c.Prefix
		if ^uint64(0)-count < c.Commitment.Count {
			return 0, fmt.Errorf("Ready count overflow")
		}
		count += c.Commitment.Count
	}
	return count, nil
}

// Seal has validated every prefix as hexadecimal and every child hash as
// canonical ASCII. Emit that fixed JSON envelope directly; leaves retain the
// standard encoder for refs, ordering values and all required string escaping.
// The bytes remain identical to the original four-field JSON struct encoding.
func readyOrderNodePreimage(n *ReadyOrderNode) ([]byte, error) {
	var leaf []byte
	var err error
	if n.Leaf != nil {
		leaf, err = json.Marshal(n.Leaf)
		if err != nil {
			return nil, err
		}
	}
	size := 128 + len(n.Prefix) + len(leaf)
	for _, child := range n.Children {
		size += len(child.Prefix) + 142
	}
	b := make([]byte, 0, size)
	b = append(b, `{"Domain":"plug.ready.radix.v2","Prefix":"`...)
	b = append(b, n.Prefix...)
	b = append(b, `","Leaf":`...)
	if n.Leaf == nil {
		b = append(b, "null"...)
	} else {
		b = append(b, leaf...)
	}
	b = append(b, `,"Children":`...)
	if len(n.Children) == 0 {
		return append(b, "null}"...), nil
	}
	b = append(b, '[')
	for i, child := range n.Children {
		if i != 0 {
			b = append(b, ',')
		}
		b = append(b, `{"Prefix":"`...)
		b = append(b, child.Prefix...)
		b = append(b, `","Commitment":{"Count":`...)
		b = strconv.AppendUint(b, child.Commitment.Count, 10)
		b = append(b, `,"Hash":"`...)
		b = append(b, child.Commitment.Hash...)
		b = append(b, `"}}`...)
	}
	return append(b, ']', '}'), nil
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
	keys := make(map[string]string, len(leaves))
	seen := map[string]bool{}
	for _, leaf := range leaves {
		if seen[leaf.Key.Ref.ID] {
			return nil, fmt.Errorf("duplicate Ready identity")
		}
		seen[leaf.Key.Ref.ID] = true
		key, err := ReadyOrderTreeKey(leaf)
		if err != nil {
			return nil, err
		}
		keys[leaf.Key.Ref.ID] = key
	}
	sort.Slice(leaves, func(i, j int) bool {
		return keys[leaves[i].Key.Ref.ID] < keys[leaves[j].Key.Ref.ID]
	})
	nodes := map[string]ReadyOrderNode{}
	var build func(string, []ReadyOrderLeaf) (ReadyOrderNode, error)
	build = func(prefix string, ls []ReadyOrderLeaf) (ReadyOrderNode, error) {
		n := ReadyOrderNode{Prefix: prefix}
		for start := 0; start < len(ls); {
			key := keys[ls[start].Key.Ref.ID]
			if key == prefix {
				l := ls[start]
				n.Leaf = &l
				start++
				continue
			}
			end := start + 1
			for end < len(ls) && keys[ls[end].Key.Ref.ID][:len(prefix)+2] == key[:len(prefix)+2] {
				end++
			}
			last := keys[ls[end-1].Key.Ref.ID]
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
