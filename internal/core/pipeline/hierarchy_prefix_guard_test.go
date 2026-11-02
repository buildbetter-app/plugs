package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type closureGuardReader struct {
	driver.Reader
	rows     []driver.DerivedRow
	err      error
	mismatch bool
}

func (r *closureGuardReader) DerivedRows(context.Context, driver.DerivedTable, string) ([]driver.DerivedRow, error) {
	return r.rows, r.err
}

func (r *closureGuardReader) Observe(_ context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		row := driver.DerivedRow{Key: key.Derived.Key, Value: []byte("0")}
		if r.mismatch {
			row.Value = []byte("1")
		}
		states = append(states, driver.LockState{Key: key, Present: true, Revision: model.Natural64(1), Derived: &row})
	}
	return states, nil
}

type closureGuardPrefixReader struct{ *closureGuardReader }

func (r *closureGuardPrefixReader) DerivedRowsForPrefixes(_ context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	want, _ := AncestorPrefix("pl-a")
	if table != driver.DerivedAncestorClosure || !reflect.DeepEqual(prefixes, []string{want}) {
		return nil, errors.New("wrong table or consumed prefix union")
	}
	return r.rows, r.err
}

func TestClosurePrefixCapabilityPreservesFailClosedValidation(t *testing.T) {
	self, _ := AncestorRow("pl-a", "pl-a", 0)
	extra, _ := AncestorRow("pl-a", "pl-b", 1)
	malformed := driver.DerivedRow{Key: self.Key, Value: []byte("01")}
	for _, tc := range []struct {
		name     string
		rows     []driver.DerivedRow
		mismatch bool
		err      error
		code     string
	}{
		{name: "valid", rows: []driver.DerivedRow{self}},
		{name: "missing", code: "ontology_guard_failed"},
		{name: "extra-consumed", rows: []driver.DerivedRow{self, extra}, code: "ontology_guard_failed"},
		{name: "malformed", rows: []driver.DerivedRow{malformed}, code: "ontology_guard_failed"},
		{name: "duplicate", rows: []driver.DerivedRow{self, self}, code: "ontology_guard_failed"},
		{name: "out-of-order", rows: []driver.DerivedRow{extra, self}, code: "ontology_guard_failed"},
		{name: "observation-mismatch", rows: []driver.DerivedRow{self}, mismatch: true, code: "internal_error"},
		{name: "cancellation", err: context.Canceled},
		{name: "storage-error", err: driver.ErrUnsupported},
	} {
		for _, capability := range []bool{false, true} {
			name := "fallback"
			if capability {
				name = "prefixes"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				base := &closureGuardReader{rows: tc.rows, err: tc.err, mismatch: tc.mismatch}
				var reader driver.Reader = base
				if capability {
					reader = &closureGuardPrefixReader{base}
				}
				p := prepared{snapshot: &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}}
				err := p.checkClosures(context.Background(), map[string]bool{"pl-a": true}, map[string]driver.DerivedRow{self.Key: self})
				if tc.err != nil {
					if !errors.Is(err, tc.err) {
						t.Fatalf("error=%v want=%v", err, tc.err)
					}
				} else if tc.code != "" {
					code(t, err, tc.code)
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
