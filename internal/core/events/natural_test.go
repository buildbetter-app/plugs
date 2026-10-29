package events

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestCompareFullNaturalDomain(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"", "0", 0}, {"1e0", "1.0", 0}, {"18446744073709551616", "18446744073709551615", 1},
		{"1e1000000000", "9e999999999", 1}, {"12e1000000000", "119e999999999", 1},
		{"123e999999999", "1231e999999998", -1}, {"1000", "1e3", 0}, {"0", "0.0e99999", 0},
	} {
		t.Run(tc.a+"_"+tc.b, func(t *testing.T) {
			got, err := CompareNatural(model.Natural(tc.a), model.Natural(tc.b))
			if err != nil || got != tc.want {
				t.Fatalf("comparison=%d err=%v want=%d", got, err, tc.want)
			}
		})
	}
	for _, invalid := range []string{"-1", "1.2", "NaN", "01"} {
		if _, err := CompareNatural(model.Natural(invalid), model.Natural64(0)); err == nil {
			t.Fatalf("accepted %s", invalid)
		}
	}
}

func TestDeliveryKeyCanonicalAndUnambiguous(t *testing.T) {
	a := DeliveryKey{Position: Position{Incarnation: "a/b", Revision: "1.0", Index: "0e3"}, Binding: "c"}
	b := DeliveryKey{Position: Position{Incarnation: "a/b", Revision: "1", Index: "0"}, Binding: "c"}
	if a.String() != b.String() {
		t.Fatal("equivalent numeric keys differ")
	}
	b.Incarnation = "a"
	b.Binding = "b/c"
	if a.String() == b.String() {
		t.Fatal("delimiter collision")
	}
	if (Position{Incarnation: "a", Index: "1"}).Validate() == nil {
		t.Fatal("accepted index in revision zero")
	}
}
