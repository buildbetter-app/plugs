package viewops

import (
	"math/big"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// compareRevisionValues compares validated naturals by symbolic magnitude,
// following model's audit-revision comparison. Natural.String canonicalizes
// integral spellings; even a huge exponent never expands into zero padding.
func compareRevisionValues(a, b model.Revision) int {
	left, right := a.String(), b.String()
	if left == right {
		return 0
	}
	if left == "0" {
		return -1
	}
	if right == "0" {
		return 1
	}
	magnitude := func(value string) (string, *big.Int) {
		coefficient, exponent, _ := strings.Cut(value, "e")
		size := new(big.Int)
		if exponent != "" {
			size.SetString(exponent, 10)
		}
		size.Add(size, big.NewInt(int64(len(coefficient))))
		return coefficient, size
	}
	l, lm := magnitude(left)
	r, rm := magnitude(right)
	if order := lm.Cmp(rm); order != 0 {
		return order
	}
	for i := 0; i < max(len(l), len(r)); i++ {
		lc, rc := byte('0'), byte('0')
		if i < len(l) {
			lc = l[i]
		}
		if i < len(r) {
			rc = r[i]
		}
		if lc < rc {
			return -1
		}
		if lc > rc {
			return 1
		}
	}
	return 0
}
