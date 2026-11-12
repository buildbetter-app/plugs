package driver

import "testing"

func TestChildIDOrderPreservesDecimalSuffixSemantics(t *testing.T) {
	for _, v := range []struct{ key, parent, number string }{
		{"0:id:p.9:0", "p", "00000000000000000009"},
		{"0:id:p.10:0", "p", "00000000000000000010"},
		{"0:id:p.00010:0", "p", "00000000000000000010"},
		{"0:id:p.18446744073709551615:0", "p", "18446744073709551615"},
		{"0:id:p.18446744073709551616:0", "p", "~"},
		{"0:id:p.0:0", "", ""}, {"0:id:p.000:0", "", ""},
		{"0:id:p.1.2:0", "p.1", "00000000000000000002"},
		{"0:id:p.abc:0", "", ""}, {"0:id:p.1:1", "", ""},
		{"0:id:p/q.1:0", "", ""},
	} {
		parent, number := ChildIDOrder(v.key)
		if parent != v.parent || number != v.number {
			t.Fatalf("order(%q)=(%q,%q), want (%q,%q)", v.key, parent, number, v.parent, v.number)
		}
	}
}
