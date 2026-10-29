package events

import (
	"math/big"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CompareNatural compares the full model domain without expanding a symbolic
// exponent or narrowing an untrusted cursor to a machine integer.
func CompareNatural(a, b model.Natural) (int, error) {
	as, err := a.Canonical()
	if err != nil {
		return 0, err
	}
	bs, err := b.Canonical()
	if err != nil {
		return 0, err
	}
	if as == bs {
		return 0, nil
	}
	if as == "0" {
		return -1, nil
	}
	if bs == "0" {
		return 1, nil
	}
	split := func(s string) (string, *big.Int) {
		coefficient, exponent, has := strings.Cut(s, "e")
		power := new(big.Int)
		if has {
			power.SetString(exponent, 10)
		}
		power.Add(power, big.NewInt(int64(len(coefficient))))
		return coefficient, power
	}
	ac, am := split(as)
	bc, bm := split(bs)
	if cmp := am.Cmp(bm); cmp != 0 {
		return cmp, nil
	}
	width := max(len(ac), len(bc))
	return strings.Compare(ac+strings.Repeat("0", width-len(ac)), bc+strings.Repeat("0", width-len(bc))), nil
}

func equalNatural(a, b model.Natural) bool {
	c, err := CompareNatural(a, b)
	return err == nil && c == 0
}

func eventPosition(p Position) model.EventPosition {
	return model.EventPosition{Incarnation: p.Incarnation, Revision: p.Revision, Index: p.Index}
}

func PositionOf(e model.Event) Position {
	var p model.EventPosition
	if e.Durable != nil {
		p = e.Durable.EventPosition
	} else if e.Control != nil {
		p = e.Control.EventPosition
	}
	return Position{Incarnation: p.Incarnation, Revision: p.Revision, Index: p.Index}
}
