package viewops

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestRevisionValuesCompareLosslessly(t *testing.T) {
	for _, tc := range []struct {
		name, left, right string
		want              int
	}{
		{"zero_spellings", "", "0.0", 0},
		{"zero_before_positive", "0e1000000000", "1", -1},
		{"decimal_boundary", "9", "10", -1},
		{"integral_spellings", "1.0", "1e0", 0},
		{"exponent_spellings", "1000", "10e2", 0},
		{"float_precision_boundary", "9007199254740992", "9007199254740993", -1},
		{"beyond_uint64", "18446744073709551616", "18446744073709551617", -1},
		{"symbolic_equivalence", "1e1000000000", "10e999999999", 0},
		{"symbolic_magnitude", "1e1000000000", "9e999999999", 1},
		{"symbolic_coefficient", "12e1000000000", "119e999999999", 1},
		{"implicit_trailing_zero", "12e1000000000", "1201e999999998", -1},
		{"exponent_beyond_int64", "1e1000000000000000000000000", "9e999999999999999999999999", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := model.Revision(tc.left), model.Revision(tc.right)
			if err := left.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := right.Validate(); err != nil {
				t.Fatal(err)
			}
			if got := compareRevisionValues(left, right); got != tc.want {
				t.Fatalf("compare(%q, %q)=%d want=%d", tc.left, tc.right, got, tc.want)
			}
			if got := compareRevisionValues(right, left); got != -tc.want {
				t.Fatalf("reverse compare(%q, %q)=%d want=%d", tc.right, tc.left, got, -tc.want)
			}
		})
	}
}

func TestRevisionSortPreservesOtherKeys(t *testing.T) {
	row := func(id, revision, title string) stored {
		return stored{
			record: driver.Record{Revision: model.Revision(revision)},
			doc:    pipeline.Document{ID: id, Fields: map[string]any{"title": title}},
		}
	}
	input := []stored{row("z", "9", "same"), row("b", "10.0", "z"), row("a", "1e1", "a"), row("c", "11", "same")}
	for _, tc := range []struct {
		name string
		keys []string
		want []string
	}{
		{"ascending_and_id_tie", []string{"revision"}, []string{"z", "a", "b", "c"}},
		{"descending_and_id_tie", []string{"-revision"}, []string{"c", "a", "b", "z"}},
		{"equivalent_revision_next_key", []string{"revision", "-title"}, []string{"z", "b", "a", "c"}},
		{"revision_as_secondary_key", []string{"title", "-revision"}, []string{"a", "c", "z", "b"}},
		{"non_revision_default", nil, []string{"a", "b", "c", "z"}},
		{"non_revision_id", []string{"id"}, []string{"a", "b", "c", "z"}},
		{"non_revision_descending", []string{"-id"}, []string{"z", "c", "b", "a"}},
		{"non_revision_title", []string{"title"}, []string{"a", "c", "z", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append([]stored(nil), input...)
			sortStored(rows, tc.keys)
			ids := make([]string, len(rows))
			for i, row := range rows {
				ids[i] = row.doc.ID
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("sort %v=%v want=%v", tc.keys, ids, tc.want)
			}
		})
	}
}
