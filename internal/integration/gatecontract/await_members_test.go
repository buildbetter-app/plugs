package gatecontract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Expected limits and ordered values come from the pinned public member
// contract. These calls exercise the named RPC and the product Writer, without
// seeding a gate or configuring an alternative ontology.
func TestB14PublicAwaitMemberBoundsAndWholeListReplacement(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		fields := map[string]any{
			"title":      "bounded await members on an ordinary subject",
			"await_type": strings.Repeat("é", 32), "await_id": strings.Repeat("é", 255),
			"timeout": 0, "waiters": []string{strings.Repeat("w", 65531)},
		}
		id, _ := f.create(fields, "")
		assertAwaitMembers(t, f, id, fields)
		// A replacement preserves blank and repeated entries in their given
		// order, and drops the entire previous list.
		for _, list := range [][]string{{"", "b", "a", "b"}, {"second", "second"}, {}} {
			f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"await_id": "new opaque id", "waiters": list}}, true, "")
			fields["await_id"], fields["waiters"] = "new opaque id", list
			assertAwaitMembers(t, f, id, fields)
		}
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"waiters": []string{"clear me"}}}, true, "")
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"waiters": nil}}, true, "")
		assertAwaitMembers(t, f, id, fields)
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"description": "members omitted"}}, true, "")
		assertAwaitMembers(t, f, id, fields)
		// JSON bytes, rather than character count or incoming whitespace,
		// decide the list bound. These both fit despite multibyte/escaped data.
		for _, list := range [][]string{{strings.Repeat("é", 32765)}, {strings.Repeat("<", 10921)}} {
			f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"waiters": list}}, true, "")
			fields["waiters"] = list
			assertAwaitMembers(t, f, id, fields)
		}
		if f.engine != "memory" {
			f.close()
			f.open()
			assertAwaitMembers(t, f, id, fields)
		}
	})
}

func assertAwaitMembers(t *testing.T, f *fixture, id string, want map[string]any) {
	t.Helper()
	got := f.currentBody(id)["fields"].(map[string]any)
	for _, name := range []string{"await_type", "await_id", "timeout", "waiters"} {
		actual, err := json.Marshal(got[name])
		if err != nil {
			t.Fatal(err)
		}
		expected, err := json.Marshal(want[name])
		if err != nil || string(actual) != string(expected) {
			t.Fatalf("%s did not retain its exact member value (got %d bytes, want %d bytes): %v", name, len(actual), len(expected), err)
		}
	}
}

func TestB14PublicAwaitMemberTypedRefusalsWriteNothing(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, _ := f.create(map[string]any{"title": "unchanged control", "await_id": "original", "waiters": []string{"a", "a"}}, "")
		before, history := f.currentBody(id), f.historyCount()
		for _, test := range []struct {
			name, member string
			value        any
		}{
			{"await-type-width", "await_type", strings.Repeat("é", 33)},
			{"await-type-type", "await_type", false},
			{"await-id-width", "await_id", strings.Repeat("é", 256)},
			{"await-id-type", "await_id", []string{"id"}},
			{"timeout-negative", "timeout", -1},
			{"timeout-overflow", "timeout", json.Number("9223372036854775808")},
			{"timeout-fraction", "timeout", 0.5},
			{"timeout-string", "timeout", "0"},
			{"waiters-bytes", "waiters", []string{strings.Repeat("w", 65532)}},
			{"waiters-escaped-bytes", "waiters", []string{strings.Repeat("<", 10922)}},
			{"waiters-member-type", "waiters", []any{"valid", 1}},
			{"waiters-type", "waiters", "a,b"},
		} {
			t.Run(test.name, func(t *testing.T) {
				f.refuse("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "invalid", test.member: test.value}}, "invalid_params")
				if test.member == "await_id" || test.member == "waiters" {
					f.refuse("bead.update", map[string]any{"id": id, "fields": map[string]any{"title": "must roll back", test.member: test.value}}, "invalid_params")
				}
				if !reflect.DeepEqual(f.currentBody(id), before) || f.historyCount() != history {
					t.Fatal("invalid member changed content or durable history")
				}
			})
		}
		for _, member := range []string{"await_type", "timeout"} {
			f.refuse("bead.update", map[string]any{"id": id, "fields": map[string]any{member: 0}}, "invalid_params")
		}
		// An int64 duration at the public upper bound is admitted without a
		// float conversion in validation. Exact readback is checked separately.
		maximum, _ := f.create(map[string]any{"title": "maximum timeout", "timeout": json.Number("9223372036854775807")}, "")
		reader, err := f.store.Read(context.Background(), driver.View{Incarnation: f.meta().Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		rows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(maximum, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatalf("exact timeout readback: rows=%d err=%v", len(rows), err)
		}
		var document struct{ Fields map[string]any }
		decoder := json.NewDecoder(bytes.NewReader(rows[0].Body))
		decoder.UseNumber()
		if err := decoder.Decode(&document); err != nil || document.Fields["timeout"] != json.Number("9223372036854775807") {
			t.Fatalf("timeout lost integer precision: %v %v", document.Fields["timeout"], err)
		}
	})
}

func TestB14PublicAwaitMemberInvalidBatchRollsBack(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, _ := f.create(map[string]any{"title": "batch control", "await_id": "original", "waiters": []string{"old", "old"}}, "")
		before, history := f.currentBody(id), f.historyCount()
		for _, invalid := range []map[string]any{
			{"kind": "create", "type": "issue", "fields": map[string]any{"title": "invalid second create", "timeout": -1}},
			{"kind": "patch", "target": ref(id), "fields": map[string]any{"await_id": strings.Repeat("x", 256)}},
			{"kind": "patch", "target": ref(id), "fields": map[string]any{"waiters": []any{"valid", false}}},
		} {
			f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
				map[string]any{"kind": "patch", "target": ref(id), "fields": map[string]any{"title": "valid prefix must not publish", "waiters": []string{"new"}}},
				map[string]any{"kind": "create", "type": "issue", "fields": map[string]any{"title": "valid create must not publish"}},
				invalid,
			}}}, "invalid_params")
			if !reflect.DeepEqual(f.currentBody(id), before) || f.historyCount() != history {
				t.Fatal("invalid batch changed the subject or durable history")
			}
		}
		// A valid batch uses the same public path and replaces, rather than
		// accumulating, both duplicate entries from the prior waiter list.
		f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "patch", "target": ref(id), "fields": map[string]any{"await_id": "batch id", "waiters": []string{"b", "b", "a"}}},
		}}}, true, "")
		got := f.currentBody(id)["fields"].(map[string]any)
		if got["await_id"] != "batch id" || !reflect.DeepEqual(got["waiters"], []any{"b", "b", "a"}) {
			t.Fatal("valid batch did not retain the exact replacement")
		}
		f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "patch", "target": ref(id), "fields": map[string]any{"waiters": nil}},
		}}}, true, "")
		if got := f.currentBody(id)["fields"].(map[string]any)["waiters"]; !reflect.DeepEqual(got, []any{}) {
			t.Fatalf("null batch replacement is not a nonnil empty list: %#v", got)
		}
	})
}

func TestB14PublicAwaitWholeListOrderAndInvalidSuffix(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, _ := f.create(map[string]any{"title": "ordered member control", "waiters": []string{"old"}}, "")
		patch := func(target map[string]any, value any) map[string]any {
			return map[string]any{"kind": "patch", "target": target, "fields": map[string]any{"waiters": value}}
		}
		apply := func(ops ...any) map[string]any {
			return f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": ops}}, true, "")
		}
		check := func(subject string, want []string) {
			t.Helper()
			got, err := json.Marshal(f.currentBody(subject)["fields"].(map[string]any)["waiters"])
			expected, _ := json.Marshal(want)
			if err != nil || string(got) != string(expected) {
				t.Fatalf("ordered replacement=%s want=%s err=%v", got, expected, err)
			}
		}
		apply(patch(ref(id), nil), patch(ref(id), []string{"later", "later"}))
		check(id, []string{"later", "later"})
		apply(patch(ref(id), []string{"earlier", "earlier"}), patch(ref(id), nil))
		check(id, []string{})
		before, history := f.currentBody(id), f.historyCount()
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			patch(ref(id), nil), patch(ref(id), []any{"valid", false}),
		}}}, "invalid_params")
		if !reflect.DeepEqual(f.currentBody(id), before) || f.historyCount() != history {
			t.Fatal("null prefix masked an invalid final list or published a prefix")
		}
		alias := map[string]any{"key": "await-subject"}
		create := func(value any) map[string]any {
			return map[string]any{"kind": "create", "key": "await-subject", "type": "issue", "fields": map[string]any{"title": "ordered alias", "waiters": value}}
		}
		created := apply(create(nil), patch(alias, []string{"alias", "alias"}))
		check(resultID(created, 0), []string{"alias", "alias"})
		created = apply(create([]string{"alias", "alias"}), patch(alias, nil))
		check(resultID(created, 0), []string{})
		history = f.historyCount()
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			create(nil), patch(alias, []any{"valid", false}),
		}}}, "invalid_params")
		if f.historyCount() != history {
			t.Fatal("invalid alias suffix published its create")
		}
	})
}
