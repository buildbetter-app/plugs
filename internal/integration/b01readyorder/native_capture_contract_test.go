package b01readyorder_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// beads-issues-source-fields/1 omits exactly the transient lock and derived
// blocking flag from durable Source.Fields. The full physical rows, including
// both typed original cells, remain in the authenticated state/stream inputs.
// Genuine active native rows and every other family retain all their source fields.
func b01CapturedSourceRowsMatch(family string, actual, physical []dumpformat.Row) error {
	expected := make([]dumpformat.Row, len(physical))
	for i, row := range physical {
		expected[i] = row
		if family != "issues" {
			continue
		}
		raw, err := json.Marshal(row)
		if err != nil {
			return err
		}
		var cells map[string]json.RawMessage
		if err := json.Unmarshal(raw, &cells); err != nil {
			return err
		}
		lock, hasLock := cells["row_lock"]
		blocked, hasBlocked := cells["is_blocked"]
		if !hasLock || !hasBlocked {
			return fmt.Errorf("original durable row lacks explicitly omitted physical cells")
		}
		lockValue, err := strconv.ParseInt(string(lock), 10, 64)
		if err != nil || strconv.FormatInt(lockValue, 10) != string(lock) || string(blocked) != "0" && string(blocked) != "1" {
			return fmt.Errorf("original durable omissions require int64 row_lock and numeric 0/1 is_blocked")
		}
		expected[i] = dumpformat.Row{}
		for column, value := range row {
			if column != "row_lock" && column != "is_blocked" {
				expected[i][column] = value
			}
		}
	}
	canonical := func(rows []dumpformat.Row) ([]string, error) {
		out := make([]string, len(rows))
		for i, row := range rows {
			raw, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			out[i] = string(raw)
		}
		sort.Strings(out)
		return out, nil
	}
	got, err := canonical(actual)
	if err != nil {
		return err
	}
	want, err := canonical(expected)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("%s source rows differ in membership, presence, type or value outside the two explicit durable omissions", family)
	}
	return nil
}

// Controls perturb copies of the real delivered rows, never the authentic
// input or the public consumer output used as the independent expectation.
func b01CapturedIssueOmissionControls(t *testing.T, actual, physical []dumpformat.Row) {
	t.Helper()
	if len(actual) == 0 || len(physical) == 0 {
		t.Fatal("omission controls require genuine nonempty native issue rows")
	}
	clone := func(rows []dumpformat.Row) []dumpformat.Row {
		out := make([]dumpformat.Row, len(rows))
		for i, row := range rows {
			out[i] = dumpformat.Row{}
			for name, value := range row {
				out[i][name] = value
			}
		}
		return out
	}
	beforeActual, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	beforePhysical, err := json.Marshal(physical)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("exact-two-typed-durable-omissions", func(t *testing.T) {
		if err := b01CapturedSourceRowsMatch("issues", actual, physical); err != nil {
			t.Fatal(err)
		}
	})
	for _, control := range []struct {
		name   string
		change func([]dumpformat.Row, []dumpformat.Row)
	}{
		{"row-lock-must-be-omitted", func(a, p []dumpformat.Row) { a[0]["row_lock"] = p[0]["row_lock"] }},
		{"derived-flag-must-be-omitted", func(a, p []dumpformat.Row) { a[0]["is_blocked"] = p[0]["is_blocked"] }},
		{"other-fields-must-remain-present", func(a, _ []dumpformat.Row) { delete(a[0], "current_revision") }},
		{"other-field-types-must-remain-exact", func(a, _ []dumpformat.Row) { a[0]["priority"] = fmt.Sprint(a[0]["priority"]) }},
		{"other-field-values-must-remain-exact", func(a, _ []dumpformat.Row) { a[0]["title"] = "changed by omission negative control" }},
		{"no-unexpected-columns", func(a, _ []dumpformat.Row) { a[0]["unexpected_column"] = json.Number("1") }},
		{"raw-lock-cell-must-remain-present", func(_, p []dumpformat.Row) { delete(p[0], "row_lock") }},
		{"raw-derived-cell-must-remain-present", func(_, p []dumpformat.Row) { delete(p[0], "is_blocked") }},
		{"raw-lock-cell-must-remain-numeric", func(_, p []dumpformat.Row) { p[0]["row_lock"] = fmt.Sprint(p[0]["row_lock"]) }},
		{"raw-derived-cell-must-remain-numeric", func(_, p []dumpformat.Row) { p[0]["is_blocked"] = false }},
	} {
		t.Run(control.name, func(t *testing.T) {
			a, p := clone(actual), clone(physical)
			control.change(a, p)
			if err := b01CapturedSourceRowsMatch("issues", a, p); err == nil {
				t.Fatal("durable source-fields contract accepted a real-row negative control")
			}
		})
	}
	afterActual, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	afterPhysical, err := json.Marshal(physical)
	if err != nil || string(beforeActual) != string(afterActual) || string(beforePhysical) != string(afterPhysical) {
		t.Fatal("negative controls changed authentic native source rows", err)
	}
}
