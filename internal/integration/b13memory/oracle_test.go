package b13memory_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

// This oracle never calls the service, Writer, memory decoder or key derivation.
// SQL snapshots additionally enumerate every durable data table through an
// independent database/sql connection. Lease and lock bookkeeping are excluded.
type durableState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Versions     map[string][]driver.Version
	Changesets   []model.Changeset
	Receipts     map[scope]*driver.IdempotencyRecord
	Observations []driver.LockState
	Derived      map[driver.DerivedTable][]driver.DerivedRow
	SQL          map[string][]string
}

func (f *fixture) reader() driver.Reader {
	f.t.Helper()
	meta, err := f.oracle.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.oracle.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

func (f *fixture) state() durableState {
	return f.stateAfter(nil)
}

// The original lifecycle always reads from zero. A restored lifetime must name
// its declared floor explicitly; it must never turn truncated history into [].
func (f *fixture) stateAfter(after *driver.Cursor) durableState {
	f.t.Helper()
	meta, err := f.oracle.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r := f.reader()
	defer r.Close()
	if view := r.Snapshot(); view.Incarnation != meta.Incarnation || view.At == nil || view.At.String() != meta.Head.String() {
		f.t.Fatalf("oracle lost its captured head: meta=%+v view=%+v", meta, view)
	}
	page, err := r.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		f.t.Fatalf("incomplete oracle records: %+v %v", page, err)
	}
	s := durableState{Meta: meta, Records: page.Items, Versions: map[string][]driver.Version{}, Receipts: map[scope]*driver.IdempotencyRecord{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	s.Changesets, err = r.Changesets(f.ctx, after, 1000)
	if err != nil || len(s.Changesets) >= 1000 {
		f.t.Fatalf("incomplete oracle changesets: %d %v", len(s.Changesets), err)
	}
	// Include tombstones, an absent ID and every ID the fixture may allocate,
	// so a refused create cannot hide a version behind an absent live record.
	refs := map[string]model.Ref{}
	for _, id := range []string{"b13-empty", "b13-setting", "b13-generic-setting", "b13-missing", "b13-public"} {
		refs[id] = ref(id)
	}
	for i := 1; i <= 16; i++ {
		id := fmt.Sprintf("b13-public-%02d", i)
		refs[id] = ref(id)
	}
	for _, row := range s.Records {
		refs[row.Ref.ID] = row.Ref
	}
	for _, set := range s.Changesets {
		for _, change := range set.Changes {
			refs[change.Ref.ID] = change.Ref
		}
	}
	locks := []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock}
	ready, err := driver.NewReadySentinel(project, "issue")
	if err != nil {
		f.t.Fatal(err)
	}
	locks = append(locks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	for id, ref := range refs {
		versions, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			f.t.Fatalf("incomplete oracle versions for %s: %+v %v", id, versions, err)
		}
		s.Versions[id] = versions.Items
		locks = append(locks, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		bead, err := driver.NewBeadSentinel(id)
		if err != nil {
			f.t.Fatal(err)
		}
		locks = append(locks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i])
		b, _ := driver.EncodeLockKey(locks[j])
		return a < b
	})
	s.Observations, err = r.Observe(f.ctx, locks)
	if err != nil {
		f.t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		f.t.Fatal(err)
	}
	for scope := range f.scopes {
		value, err := r.IdempotencyByKey(f.ctx, principal, agent, scope.Method, scope.Key)
		if err != nil {
			f.t.Fatal(err)
		}
		s.Receipts[scope] = value
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		s.Derived[table], err = r.DerivedRows(f.ctx, table, "")
		if err != nil {
			f.t.Fatal(err)
		}
	}
	s.SQL = f.sqlState()
	return s
}

func (f *fixture) sqlState() map[string][]string {
	f.t.Helper()
	if f.db == nil {
		return nil
	}
	tables := []string{"meta", "objects", "records", "versions", "key_history", "blobs", "changesets", "replays", "checkpoints", "core_sessions", "allocator_states"}
	prefix := ""
	if f.engine == "postgres" {
		prefix = "plug_postgres."
		tables = []string{"meta", "records", "versions", "states", "blobs", "changesets", "receipts", "checkpoints", "core_sessions", "allocator_states"}
	}
	// Source indexes and coverage markers are durable state on both engines.
	// Refusals, replay and physical reopens must conserve their complete rows.
	tables = append(tables,
		"hierarchy_index_coverage", "hierarchy_source_current", "hierarchy_source_history",
		"graph_link_index_coverage", "graph_link_source_current", "graph_link_current",
		"graph_link_source_history", "graph_link_history",
	)
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	defer tx.Rollback()
	state := map[string][]string{}
	for _, table := range tables {
		// Table identifiers come exclusively from the fixed lists above.
		rows, err := tx.QueryContext(f.ctx, "SELECT * FROM "+prefix+table)
		if err != nil {
			f.t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			f.t.Fatal(err)
		}
		state[table] = []string{}
		for rows.Next() {
			values, ptrs := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				f.t.Fatal(err)
			}
			row := make([]string, len(values))
			for i, value := range values {
				row[i] = fmt.Sprintf("%s=%T:%v", columns[i], value, value)
			}
			state[table] = append(state[table], string(encode(f.t, row)))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			f.t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			f.t.Fatal(err)
		}
		sort.Strings(state[table])
	}
	return state
}

func requireSame(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("B13 durable state changed:\nbefore=%s\nafter=%s", stateDescription(t, before), stateDescription(t, after))
	}
}

func stateDescription(t *testing.T, s durableState) []byte {
	t.Helper()
	// JSON cannot encode the struct-keyed receipt map; retain its full values.
	receipts := map[string]*driver.IdempotencyRecord{}
	for key, value := range s.Receipts {
		receipts[key.Method+":"+key.Key] = value
	}
	return encode(t, struct {
		Meta         driver.Meta
		Records      []driver.Record
		Versions     map[string][]driver.Version
		Changesets   []model.Changeset
		Receipts     map[string]*driver.IdempotencyRecord
		Observations []driver.LockState
		Derived      map[driver.DerivedTable][]driver.DerivedRow
		SQL          map[string][]string
	}{s.Meta, s.Records, s.Versions, s.Changesets, receipts, s.Observations, s.Derived, s.SQL})
}

func stored(t *testing.T, s durableState, id string) driver.Record {
	t.Helper()
	for _, row := range s.Records {
		if row.Ref.ID == id {
			return row
		}
	}
	t.Fatalf("missing persisted row %q", id)
	return driver.Record{}
}

func (f *fixture) accept(method string, params map[string]any, key string, op model.OpKind) (model.Commit, reply) {
	f.t.Helper()
	before := f.state()
	r := f.call(method, params, key)
	commit := decode[model.Commit](f.t, result(f.t, r))
	after := f.state()
	if len(commit.Results) != 1 || commit.Revision.String() != fmt.Sprint(len(before.Changesets)+1) || after.Meta.Head.String() != commit.Revision.String() || len(after.Changesets) != len(before.Changesets)+1 {
		f.t.Fatalf("accepted write did not publish exactly one result/revision/log: %+v", commit)
	}
	item := commit.Results[0]
	if method == "memory.save" {
		wantReplaced := op != model.OpCreate
		if item.Replaced == nil || *item.Replaced != wantReplaced {
			f.t.Fatalf("memory.save replaced field: got=%v want=%t", item.Replaced, wantReplaced)
		}
	} else if item.Replaced != nil {
		f.t.Fatal("other operations must omit replaced")
	}
	if item.Revision.String() != commit.Revision.String() || len(after.Versions[item.Ref.ID]) != len(before.Versions[item.Ref.ID])+1 {
		f.t.Fatalf("accepted write did not publish exactly one version: %+v", commit)
	}
	set := after.Changesets[len(after.Changesets)-1]
	if set.Revision.String() != commit.Revision.String() || set.Incarnation != after.Meta.Incarnation || set.Kind != model.ChangesetWrite || set.IdempotencyKey != key || !reflect.DeepEqual(set.Principal, anonymous) || len(set.Changes) != 1 || set.Changes[0].Ref.ID != item.Ref.ID || set.Changes[0].Op != op {
		f.t.Fatalf("accepted write lost its changeset binding: %+v", set)
	}
	var previous model.Revision
	for _, row := range before.Records {
		if row.Ref.ID == item.Ref.ID {
			previous = row.Revision
		}
	}
	if set.Changes[0].FromRevision.String() != previous.String() {
		f.t.Fatalf("changeset lost the previous record revision: %+v", set)
	}
	receipt := after.Receipts[scope{method, key}]
	if receipt == nil || receipt.ResponsePending || receipt.RequestHash == "" || !reflect.DeepEqual(receipt.Commit, commit) || !reflect.DeepEqual(receipt.Changeset, set) {
		f.t.Fatalf("public commit differs from independent final receipt: commit=%+v receipt=%+v", commit, receipt)
	}
	found := false
	for _, version := range after.Versions[item.Ref.ID] {
		if version.Revision.String() == commit.Revision.String() {
			found = true
			if version.Deleted != (op == model.OpDelete) || version.Ref.ID != item.Ref.ID || !reflect.DeepEqual(version.Principal, anonymous) {
				f.t.Fatalf("version has wrong identity/deletion state: %+v", version)
			}
			if !version.Deleted {
				body := version.Body
				if !version.Base {
					patch := decode[[]struct {
						Op    string          `json:"op"`
						Path  string          `json:"path"`
						Value json.RawMessage `json:"value"`
					}](f.t, version.Patch)
					if len(patch) != 1 || patch[0].Op != "replace" || patch[0].Path != "" {
						f.t.Fatalf("unexpected durable forward patch: %s", version.Patch)
					}
					body = patch[0].Value
				}
				if !bytes.Equal(body, stored(f.t, after, item.Ref.ID).Body) {
					f.t.Fatalf("durable version body differs from the persisted record: %+v", version)
				}
			}
		}
	}
	if !found {
		f.t.Fatal("accepted commit has no matching durable version")
	}
	return commit, r
}

func (f *fixture) refuse(method string, params map[string]any, key, code string) {
	f.t.Helper()
	if key != "" {
		f.scopes[scope{method, key}] = true
	}
	before := f.state()
	r := f.call(method, params, key)
	if r.Error == nil || r.Error.Data.Code != code || r.Result != nil || r.Meta != nil {
		f.t.Fatalf("wanted complete typed %s refusal: %+v", code, r)
	}
	for _, content := range []string{originalBody, replacedBody, hiddenBody} {
		quoted := encode(f.t, content)
		if bytes.Contains(encode(f.t, r.Error), quoted[1:len(quoted)-1]) {
			f.t.Fatalf("refused %s disclosed a memory body", method)
		}
	}
	after := f.state()
	requireSame(f.t, before, after)
	if key != "" && before.Receipts[scope{method, key}] == nil && after.Receipts[scope{method, key}] != nil {
		f.t.Fatalf("refused %s consumed a receipt", method)
	}
}

func (f *fixture) replay(method string, params map[string]any, key string, original reply) {
	f.t.Helper()
	before := f.state()
	r := f.call(method, params, key)
	if r.Error != nil || r.Meta == nil || !r.Meta.Replayed || !bytes.Equal(original.Result, r.Result) {
		f.t.Fatalf("durable replay changed result bytes: first=%s replay=%+v", original.Result, r)
	}
	requireSame(f.t, before, f.state())
}
