package driver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type descendantIndexFunc func(context.Context, []model.Ref, int) ([]Record, bool, error)

func (f descendantIndexFunc) HierarchyChildren(ctx context.Context, refs []model.Ref, limit int) ([]Record, bool, error) {
	return f(ctx, refs, limit)
}
func descendantRow(id string) Record {
	return Record{Kind: "bead", Ref: model.RefByID(id, model.Natural64(0))}
}

func TestDescendantFilterStorageIndexBounds(t *testing.T) {
	root := model.RefByID("root", model.Natural64(0))
	t.Run("transitive-root-excluded-independent-of-page-order", func(t *testing.T) {
		var calls [][]string
		index := descendantIndexFunc(func(_ context.Context, refs []model.Ref, limit int) ([]Record, bool, error) {
			if len(refs) > HierarchyMaxParents || limit != HierarchyMaxLimit {
				t.Fatal("caller page limit leaked into traversal")
			}
			ids := make([]string, len(refs))
			rows := []Record{}
			for i, ref := range refs {
				ids[i] = ref.ID
				switch ref.ID {
				case "root":
					rows = append(rows, descendantRow("z"), descendantRow("a"))
				case "z":
					rows = append(rows, descendantRow("deep"))
				}
			}
			calls = append(calls, ids)
			return rows, false, nil
		})
		got, err := DescendantFilter(context.Background(), index, root)
		if err != nil || !reflect.DeepEqual(got, map[string]bool{"z": true, "a": true, "deep": true}) || !reflect.DeepEqual(calls, [][]string{{"root"}, {"z", "a"}, {"deep"}}) {
			t.Fatal(got, calls, err)
		}
	})
	t.Run("batches-parent-vector", func(t *testing.T) {
		calls := 0
		index := descendantIndexFunc(func(_ context.Context, refs []model.Ref, limit int) ([]Record, bool, error) {
			calls++
			if len(refs) > HierarchyMaxParents || limit != HierarchyMaxLimit {
				t.Fatal("unbounded indexed request")
			}
			if calls != 1 {
				return nil, false, nil
			}
			rows := make([]Record, HierarchyMaxParents+1)
			for i := range rows {
				rows[i] = descendantRow(fmt.Sprintf("child-%d", i))
			}
			return rows, false, nil
		})
		got, err := DescendantFilter(context.Background(), index, root)
		if err != nil || len(got) != HierarchyMaxParents+1 || calls != 3 {
			t.Fatal(len(got), calls, err)
		}
	})
	for _, name := range []string{"lookahead-overflow", "cycle-after-partial", "missing-coverage", "cancelled-during-lookup"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			index := descendantIndexFunc(func(context.Context, []model.Ref, int) ([]Record, bool, error) {
				calls++
				if calls == 1 {
					return []Record{descendantRow("child")}, false, nil
				}
				switch name {
				case "lookahead-overflow":
					return []Record{descendantRow("incomplete")}, true, nil
				case "cycle-after-partial":
					return []Record{descendantRow("root")}, false, nil
				case "missing-coverage":
					return nil, false, ErrUnsupported
				default:
					cancel()
					return nil, false, nil
				}
			})
			got, err := DescendantFilter(ctx, index, root)
			if got != nil || err == nil {
				t.Fatal("partial membership escaped", got, err)
			}
			if name == "lookahead-overflow" {
				var limit ErrDescendantFilterLimit
				if !errors.As(err, &limit) {
					t.Fatal(err)
				}
			}
			if name == "missing-coverage" && !errors.Is(err, ErrUnsupported) {
				t.Fatal(err)
			}
			if name == "cancelled-during-lookup" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
	t.Run("unknown-is-empty-only-with-certified-success", func(t *testing.T) {
		got, err := DescendantFilter(context.Background(), descendantIndexFunc(func(context.Context, []model.Ref, int) ([]Record, bool, error) { return nil, false, nil }), root)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatal(got, err)
		}
	})
}
