package driver

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestReadyOrderNodePreimageMatchesCanonicalJSON(t *testing.T) {
	check := func(t *testing.T, n ReadyOrderNode, count uint64) {
		t.Helper()
		commitment, digestBytes := n.Commitment, n.DigestBytes()
		validatedCount, err := ValidateReadyOrderNode(&n)
		if err != nil || validatedCount != count || n.Commitment != commitment || n.DigestBytes() != digestBytes {
			t.Fatalf("validation changed commitment or cardinality: count=%d err=%v", validatedCount, err)
		}
		if err := SealReadyOrderNode(&n); err != nil {
			t.Fatal(err)
		}
		// Keep the independent standard-library encoder and original wire
		// shape as the byte oracle, including null children and nested refs.
		want, err := json.Marshal(struct {
			Domain, Prefix string
			Leaf           *ReadyOrderLeaf
			Children       []ReadyOrderChild
		}{"plug.ready.radix.v2", n.Prefix, n.Leaf, n.Children})
		if err != nil {
			t.Fatal(err)
		}
		got, err := readyOrderNodePreimage(&n)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("canonical preimage changed: got %s want %s (%v)", got, want, err)
		}
		if n.Commitment.Count != count || n.Commitment.Hash != SelectionDigest(want) || n.DigestBytes() != uint64(len(want)) {
			t.Fatalf("canonical commitment or work differs: %+v bytes=%d wantCount=%d wantBytes=%d", n.Commitment, n.DigestBytes(), count, len(want))
		}
	}
	t.Run("empty", func(t *testing.T) {
		check(t, ReadyOrderNode{Children: []ReadyOrderChild{}}, 0)
	})
	for _, arity := range []int{1, 16, 256} {
		t.Run(fmt.Sprintf("children_%d", arity), func(t *testing.T) {
			n := ReadyOrderNode{}
			var count uint64
			for i := 0; i < arity; i++ {
				prefix := hex.EncodeToString([]byte{byte(i)})
				value := uint64(i + 1)
				count += value
				n.Children = append(n.Children, ReadyOrderChild{Prefix: prefix, Commitment: ReadyCommitment{Count: value, Hash: SelectionDigest([]byte(prefix))}})
			}
			check(t, n, count)
		})
	}
	t.Run("uppercase_prefix_and_maximum_count", func(t *testing.T) {
		check(t, ReadyOrderNode{Prefix: "AB", Children: []ReadyOrderChild{{Prefix: "ABCD", Commitment: ReadyCommitment{Count: ^uint64(0), Hash: strings.Repeat("f", 64)}}}}, ^uint64(0))
	})
	for i, id := range []string{"plain", `quoted"and\backslash`, "HTML<>&", "unicode-\u2028-\u2029-\u2603"} {
		t.Run(fmt.Sprintf("leaf_%d", i), func(t *testing.T) {
			item := ReadyMembershipItem{ID: id, Revision: model.Natural64(37), Fields: map[string]any{"title": "<tag>\n\"quoted\""}}
			leaf, err := ReadyOrderLeafFor(item, int64(i))
			if err != nil {
				t.Fatal(err)
			}
			n := ReadyOrderNode{Prefix: ReadyOrderKey(leaf.Priority, id), Leaf: &leaf}
			check(t, n, 1)
			n.Children = []ReadyOrderChild{{Prefix: n.Prefix + "62", Commitment: ReadyCommitment{Count: 7, Hash: strings.Repeat("a", 64)}}}
			check(t, n, 8)
		})
	}
}

func TestReadyOrderCanonicalHashRefusal(t *testing.T) {
	for _, hash := range []string{strings.Repeat("0", 64), strings.Repeat("f", 64), SelectionDigest([]byte("canonical"))} {
		if !ValidReadyHash(hash) {
			t.Fatalf("canonical hash refused: %q", hash)
		}
	}
	for _, hash := range []string{"", strings.Repeat("0", 63), strings.Repeat("0", 65), strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("0", 63) + "\x00", strings.Repeat("0", 63) + "\xff", strings.Repeat("0", 61) + "\u2603"} {
		if ValidReadyHash(hash) {
			t.Fatalf("noncanonical hash accepted: %q", hash)
		}
		n := ReadyOrderNode{Children: []ReadyOrderChild{{Prefix: "00", Commitment: ReadyCommitment{Count: 1, Hash: hash}}}}
		if _, err := ValidateReadyOrderNode(&n); err == nil {
			t.Fatalf("noncanonical child commitment validated: %q", hash)
		}
		if err := SealReadyOrderNode(&n); err == nil {
			t.Fatalf("noncanonical child commitment sealed: %q", hash)
		}
	}
	for _, n := range []ReadyOrderNode{
		{Prefix: "0"},
		{Prefix: "zz"},
		{Children: []ReadyOrderChild{{Prefix: "00", Commitment: ReadyCommitment{Count: 0, Hash: strings.Repeat("0", 64)}}}},
		{Children: []ReadyOrderChild{{Prefix: "00", Commitment: ReadyCommitment{Count: ^uint64(0), Hash: strings.Repeat("0", 64)}}, {Prefix: "01", Commitment: ReadyCommitment{Count: 1, Hash: strings.Repeat("0", 64)}}}},
	} {
		if _, err := ValidateReadyOrderNode(&n); err == nil {
			t.Fatalf("malformed structure or overflowing count validated: %+v", n)
		}
		if err := SealReadyOrderNode(&n); err == nil {
			t.Fatalf("malformed structure or overflowing count sealed: %+v", n)
		}
	}
}

func TestReadyOrderIntermediateValidationPreservesLeafRefusal(t *testing.T) {
	leaf, err := ReadyOrderLeafFor(ReadyMembershipItem{ID: "validation-leaf", Revision: model.Natural64(3)}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*ReadyOrderLeaf)
	}{
		{"identity", func(l *ReadyOrderLeaf) { l.Key.Ref = model.Ref{} }},
		{"revision", func(l *ReadyOrderLeaf) { l.Key.Revision = model.Natural("-1") }},
		{"digest", func(l *ReadyOrderLeaf) { l.Digest = "invalid" }},
		{"membership", func(l *ReadyOrderLeaf) { l.Key.MembershipLeaf = strings.Repeat("0", 64) }},
		{"sort_key", func(l *ReadyOrderLeaf) { l.Priority++ }},
		// The outer leaf identity is valid. Only its nested order reference
		// fails standard JSON encoding, so validation must retain that check.
		{"nested_json", func(l *ReadyOrderLeaf) { l.Key.Order = []SelectionValue{SelectionRef(model.Ref{})} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := leaf
			test.change(&changed)
			n := ReadyOrderNode{Prefix: ReadyOrderKey(leaf.Priority, leaf.Key.Ref.ID), Leaf: &changed}
			_, validationErr := ValidateReadyOrderNode(&n)
			sealErr := SealReadyOrderNode(&n)
			if validationErr == nil || sealErr == nil || validationErr.Error() != sealErr.Error() {
				t.Fatalf("intermediate/final refusal differs: validation=%v seal=%v", validationErr, sealErr)
			}
		})
	}
}
