package s1b05b08

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This observer reads actual committed storage, not a reference recording or
// pipeline.Document. It retains bodies, version patches, complete changesets,
// all derived tables, predicate observations and every receipt issued by this
// scenario. The workflow namespace is observed even when absent; that alone is
// not evidence of a mixed-plane transaction. No storage mutation is used here.
type edgeContractState struct {
	Meta        driver.Meta
	Records     map[string]driver.Record
	Versions    map[string][]driver.Version
	Changesets  []model.Changeset
	Derived     map[driver.DerivedTable][]driver.DerivedRow
	Observed    map[string]driver.LockState
	Receipts    map[edgeReceiptProbe]*driver.IdempotencyRecord
	Checkpoints map[string]driver.Checkpoint
}

type edgeReceiptProbe struct{ Method, Key string }

type edgeContractFixture struct {
	f        *fixture
	refs     []model.Ref
	probes   []edgeReceiptProbe
	lockKeys map[string]driver.LockKey
}

func newEdgeContractFixture(t *testing.T, engine string) *edgeContractFixture {
	return &edgeContractFixture{f: newFixture(t, engine), lockKeys: map[string]driver.LockKey{}}
}

func (e *edgeContractFixture) probe(method, label string) edgeReceiptProbe {
	p := edgeReceiptProbe{method, key("edge-contract-" + label)}
	for _, old := range e.probes {
		if old == p {
			return p
		}
	}
	e.probes = append(e.probes, p)
	return p
}

func (e *edgeContractFixture) call(t *testing.T, p edgeReceiptProbe, params map[string]any) envelope {
	t.Helper()
	return e.f.call(t, e.f.ctx, p.Method, params, p.Key, actor)
}

func (e *edgeContractFixture) create(t *testing.T, title string) model.Ref {
	t.Helper()
	ref := e.f.create(t, title)
	e.refs = append(e.refs, ref)
	e.probes = append(e.probes, edgeReceiptProbe{"bead.create", key("create-" + title)})
	return ref
}

func edgeRecordKey(ref model.Ref) string { return ref.ID + ":" + ref.Branch.String() }

// Historical identity uses the validated public value domain. Natural's empty
// and explicit zero spellings are equivalent metadata; Body remains exact bytes.
func edgeRequireHistoricalRecordEqual(t *testing.T, got, want driver.Record) {
	t.Helper()
	if !bytes.Equal(got.Body, want.Body) {
		t.Fatalf("historical raw body bytes differ for %s: got=%x want=%x", edgeRecordKey(want.Ref), got.Body, want.Body)
	}
	for _, ref := range []model.Ref{got.Ref, want.Ref} {
		if err := ref.Validate(); err != nil {
			t.Fatalf("invalid historical/captured reference: %#v error=%v", ref, err)
		}
	}
	for _, revision := range []model.Revision{got.Revision, want.Revision} {
		if err := revision.Validate(); err != nil {
			t.Fatalf("invalid historical/captured revision: %#v error=%v", revision, err)
		}
	}
	if got.Ref.IsID() != want.Ref.IsID() || got.Ref.ID != want.Ref.ID || got.Ref.Key != want.Ref.Key ||
		got.Ref.Branch.String() != want.Ref.Branch.String() || got.Kind != want.Kind || got.Revision.String() != want.Revision.String() {
		t.Fatalf("historical record metadata differs: got=%#v want=%#v", got, want)
	}
}

func (e *edgeContractFixture) snapshot(t *testing.T) edgeContractState {
	t.Helper()
	f := e.f
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if view := r.Snapshot(); view.At == nil || view.At.String() != meta.Head.String() || view.Incarnation != meta.Incarnation {
		t.Fatalf("observer did not pin the selected head/incarnation: %+v", view)
	}
	s := edgeContractState{
		Meta: meta, Records: map[string]driver.Record{}, Versions: map[string][]driver.Version{},
		Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Observed: map[string]driver.LockState{},
		Receipts: map[edgeReceiptProbe]*driver.IdempotencyRecord{}, Checkpoints: map[string]driver.Checkpoint{},
	}
	// Deliberately small pages exercise completeness instead of assuming a
	// fixture fits in one page. Unexpected records are included, not filtered.
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Items {
			id := edgeRecordKey(record.Ref)
			if _, exists := s.Records[id]; exists {
				t.Fatalf("duplicate record in full observation: %s", id)
			}
			s.Records[id] = record
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
			t.Fatal("record observation did not advance")
		}
		cursor = page.NextCursor
	}
	refs := map[string]model.Ref{}
	for _, ref := range e.refs {
		refs[edgeRecordKey(ref)] = ref
	}
	for id, record := range s.Records {
		refs[id] = record.Ref
	}
	observe := func(k driver.LockKey) {
		encoded, err := driver.EncodeLockKey(k)
		if err != nil {
			t.Fatal(err)
		}
		e.lockKeys[encoded] = k
	}
	for id, ref := range refs {
		observe(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		cursor = nil
		for {
			page, err := r.Versions(f.ctx, ref, driver.PageReq{Cursor: cursor, Limit: 2})
			if err != nil {
				t.Fatal(err)
			}
			s.Versions[id] = append(s.Versions[id], page.Items...)
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
				t.Fatal("version observation did not advance")
			}
			cursor = page.NextCursor
		}
		for _, prefix := range []string{"bead/", "closure/"} {
			sentinel, err := driver.NewSentinel(prefix + ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			observe(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		}
	}
	cursor = nil
	for {
		page, err := r.Changesets(f.ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		last := page[len(page)-1].Revision
		if cursor != nil && last.String() == cursor.Revision.String() {
			t.Fatal("changeset observation did not advance")
		}
		s.Changesets = append(s.Changesets, page...)
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: last, Method: "changesets"}
	}
	for _, table := range []driver.DerivedTable{
		driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure,
		driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins,
	} {
		rows, err := r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		s.Derived[table] = rows
		for _, row := range rows {
			observe(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
		}
	}
	for _, sentinel := range []driver.Sentinel{driver.SentinelGraphBlocking, driver.SentinelOntology, driver.SentinelPlugins} {
		observe(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	ready, err := driver.NewReadySentinel(project, "issue")
	if err != nil {
		t.Fatal(err)
	}
	observe(driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	encoded := make([]string, 0, len(e.lockKeys))
	for key := range e.lockKeys {
		encoded = append(encoded, key)
	}
	sort.Strings(encoded)
	keys := make([]driver.LockKey, 0, len(encoded))
	for _, key := range encoded {
		keys = append(keys, e.lockKeys[key])
	}
	states, err := r.Observe(f.ctx, keys)
	if err != nil || len(states) != len(keys) {
		t.Fatalf("complete key observation: states=%d keys=%d error=%v", len(states), len(keys), err)
	}
	for i, state := range states {
		s.Observed[encoded[i]] = state
	}
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range e.probes {
		receipt, err := r.IdempotencyByKey(f.ctx, principal, actor, probe.Method, probe.Key)
		if err != nil {
			t.Fatal(err)
		}
		s.Receipts[probe] = receipt
	}
	checkpoints, ok := f.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("actual store lacks checkpoint observation")
	}
	// This is the real host namespace from pluginhost/durable.go, not a test
	// checkpoint and not an assertion that ephemeral edge support exists.
	const workflowKey = "plugin/workflow/default/durable"
	s.Checkpoints[workflowKey], err = checkpoints.LoadCheckpoint(f.ctx, workflowKey)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := f.store.Meta(f.ctx)
	if err != nil || !reflect.DeepEqual(meta, latest) {
		t.Fatalf("state changed while observing a quiescent scenario: before=%+v after=%+v error=%v", meta, latest, err)
	}
	return s
}

// Decode the wire/storage JSON independently. In particular, no production
// dependency classifier, ready selector, waits-for evaluator or document decoder
// is used to decide what a successful transaction was supposed to mean.
func edgeJSONObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var value map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil || value == nil {
		t.Fatalf("invalid observed JSON object: %s error=%v", raw, err)
	}
	return value
}

func edgeRequireStateEqual(t *testing.T, before, after edgeContractState) {
	t.Helper()
	// Compare each plane separately so a real refusal reports the failed
	// conservation boundary rather than hiding it behind the RPC error.
	for _, component := range []struct {
		name string
		a, b any
	}{
		{"metadata/head", before.Meta, after.Meta}, {"raw records", before.Records, after.Records},
		{"full versions", before.Versions, after.Versions}, {"full changesets/events", before.Changesets, after.Changesets},
		{"all derived tables", before.Derived, after.Derived}, {"key observations", before.Observed, after.Observed},
		{"all issued receipts", before.Receipts, after.Receipts}, {"workflow checkpoint", before.Checkpoints, after.Checkpoints},
	} {
		if !reflect.DeepEqual(component.a, component.b) {
			t.Errorf("%s changed: before=%+v after=%+v", component.name, component.a, component.b)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

func edgeRequireReceipt(t *testing.T, s edgeContractState, probe edgeReceiptProbe, result json.RawMessage, noop bool) model.Commit {
	t.Helper()
	var commit model.Commit
	if err := json.Unmarshal(result, &commit); err != nil {
		t.Fatal(err)
	}
	receipt := s.Receipts[probe]
	if receipt == nil || receipt.RequestHash == "" || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, commit) {
		t.Fatalf("response is not a complete final committed receipt: probe=%+v receipt=%+v result=%s", probe, receipt, result)
	}
	if noop {
		if !reflect.DeepEqual(receipt.Changeset, model.Changeset{}) {
			t.Fatalf("no-op acquired a history row: %+v", receipt.Changeset)
		}
	} else if receipt.Changeset.Revision.String() != commit.Revision.String() || receipt.Changeset.IdempotencyKey != probe.Key {
		t.Fatalf("ordinary receipt not bound to the graph revision/key: %+v", receipt)
	} else {
		found := false
		for _, change := range s.Changesets {
			if change.Revision.String() == commit.Revision.String() {
				found = reflect.DeepEqual(change, receipt.Changeset)
			}
		}
		if !found {
			t.Fatal("receipt does not retain the complete committed changeset/events")
		}
	}
	return commit
}

func edgeRequireSingleCommit(t *testing.T, before, after edgeContractState, commit model.Commit, changed ...model.Ref) {
	t.Helper()
	oldHead, err := before.Meta.Head.Uint64()
	if err != nil || after.Meta.Head.String() != model.Natural64(oldHead+1).String() || after.Meta.Head.String() != commit.Revision.String() {
		t.Fatalf("batch did not publish exactly one revision: before=%s after=%s commit=%s error=%v", before.Meta.Head, after.Meta.Head, commit.Revision, err)
	}
	wantMeta := before.Meta
	wantMeta.Head = after.Meta.Head
	if !reflect.DeepEqual(wantMeta, after.Meta) || len(before.Records) != len(after.Records) {
		t.Fatal("edge batch changed store identity, floor or record membership")
	}
	if len(after.Changesets) != len(before.Changesets)+1 || !reflect.DeepEqual(before.Changesets, after.Changesets[:len(before.Changesets)]) {
		t.Fatal("batch did not preserve history and append exactly one complete changeset")
	}
	want := map[string]bool{}
	for _, ref := range changed {
		want[edgeRecordKey(ref)] = true
	}
	last := after.Changesets[len(after.Changesets)-1]
	if len(last.Changes) != len(want) || len(commit.Results) < len(want) {
		t.Fatalf("changed source/result census differs: changes=%+v results=%+v want=%v", last.Changes, commit.Results, want)
	}
	seen := map[string]bool{}
	for _, change := range last.Changes {
		id := edgeRecordKey(change.Ref)
		if !want[id] || seen[id] || change.FromRevision.String() != before.Records[id].Revision.String() {
			t.Fatalf("unexpected/duplicate history source: %+v", change)
		}
		seen[id] = true
	}
	seen = map[string]bool{}
	for _, result := range commit.Results {
		id := edgeRecordKey(result.Ref)
		if !want[id] || result.Revision.String() != commit.Revision.String() {
			t.Fatalf("unexpected result source or revision: %+v", result)
		}
		seen[id] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("commit omitted a changed source: got=%v want=%v", seen, want)
	}
	for id, old := range before.Records {
		next, exists := after.Records[id]
		versions := after.Versions[id]
		if !exists {
			t.Fatalf("edge batch removed %s", id)
		}
		if !want[id] {
			if !reflect.DeepEqual(old, next) || !reflect.DeepEqual(before.Versions[id], versions) {
				t.Fatalf("read-only target/neighbor %s acquired a write or version", id)
			}
			continue
		}
		if next.Revision.String() != commit.Revision.String() || len(versions) != len(before.Versions[id])+1 {
			t.Fatalf("source %s did not change once: record=%+v versions=%d", id, next, len(versions))
		}
		// Versions are returned oldest first. Preserve every older byte and
		// require the new source version to name the one atomic revision.
		if versions[len(versions)-1].Revision.String() != commit.Revision.String() || !reflect.DeepEqual(versions[:len(versions)-1], before.Versions[id]) {
			t.Fatalf("source %s version history was rewritten or split", id)
		}
	}
	for probe, old := range before.Receipts {
		if old != nil && !reflect.DeepEqual(old, after.Receipts[probe]) {
			t.Fatalf("batch rewrote an earlier receipt: %+v", probe)
		}
	}
	if !reflect.DeepEqual(before.Checkpoints, after.Checkpoints) {
		t.Fatal("durable-only edge batch changed the workflow namespace")
	}
	graphKey, err := driver.EncodeLockKey(driver.GraphBlockingSentinelLock)
	if err != nil || after.Observed[graphKey].Revision.String() != commit.Revision.String() {
		t.Fatalf("graph predicate was not published at the atomic revision: state=%+v error=%v", after.Observed[graphKey], err)
	}
	for _, sentinel := range []driver.Sentinel{driver.SentinelOntology, driver.SentinelPlugins} {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		if err != nil || !reflect.DeepEqual(before.Observed[key], after.Observed[key]) {
			t.Fatalf("edge/lifecycle write changed unrelated predicate %s: error=%v", sentinel, err)
		}
	}
}

func edgeRequireOnlyReceipt(t *testing.T, before, after edgeContractState, probe edgeReceiptProbe) {
	t.Helper()
	if before.Receipts[probe] != nil || after.Receipts[probe] == nil {
		t.Fatal("fresh no-op did not add exactly its own successful receipt")
	}
	expected := before
	expected.Receipts = make(map[edgeReceiptProbe]*driver.IdempotencyRecord, len(before.Receipts))
	for p, receipt := range before.Receipts {
		expected.Receipts[p] = receipt
	}
	expected.Receipts[probe] = after.Receipts[probe]
	edgeRequireStateEqual(t, expected, after)
}

func edgeRequireLinks(t *testing.T, s edgeContractState, ref model.Ref, want ...map[string]any) {
	t.Helper()
	doc := edgeJSONObject(t, s.Records[edgeRecordKey(ref)].Body)
	got, _ := doc["links"].([]any)
	expected := make([]any, len(want))
	for i, link := range want {
		// Round-trip test-authored values so branch/natural wire spelling is
		// compared exactly to the stored JSON, independently of Go Ref fields.
		raw, err := json.Marshal(link)
		if err != nil {
			t.Fatal(err)
		}
		expected[i] = edgeJSONObject(t, raw)
	}
	if len(got) != len(expected) || len(got) != 0 && !reflect.DeepEqual(got, expected) {
		t.Fatalf("stored edge identity/value differs: source=%s got=%+v want=%+v", ref.ID, got, expected)
	}
}

type edgeExpectedClose struct {
	At     *time.Time
	Reason *string
}

// Pair this with the exact link-value assertion: every other member of the
// source body must remain byte-value equivalent, except the explicitly named
// field edits and an explicitly expected close time/reason transition. Association or
// metadata writes cannot quietly change lifecycle times, status,
// parent, comments, leases, labels or plugin metadata.
func edgeRequireOnlyEdgeAndFields(t *testing.T, before, after edgeContractState, ref model.Ref, fields map[string]any, close ...edgeExpectedClose) {
	t.Helper()
	want := edgeJSONObject(t, before.Records[edgeRecordKey(ref)].Body)
	got := edgeJSONObject(t, after.Records[edgeRecordKey(ref)].Body)
	if links, exists := got["links"]; exists {
		want["links"] = links
	} else {
		delete(want, "links")
	}
	for name, value := range fields {
		want["fields"].(map[string]any)[name] = value
	}
	if len(close) > 1 {
		t.Fatal("multiple expected close identity edits")
	}
	if len(close) == 1 {
		if close[0].At == nil {
			delete(want, "closedAt")
		} else {
			want["closedAt"] = close[0].At.UTC().Format(time.RFC3339Nano)
		}
		if close[0].Reason == nil {
			delete(want, "closeReason")
		} else {
			want["closeReason"] = *close[0].Reason
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source %s changed outside its edge/field edit: got=%+v want=%+v", ref.ID, got, want)
	}
}

func edgeLinkValue(to model.Ref, kind string) map[string]any {
	return map[string]any{"to": to, "kind": kind}
}

func edgeRequirePublicReady(t *testing.T, e *edgeContractFixture, want ...string) {
	t.Helper()
	got := e.f.call(t, e.f.ctx, "bead.ready", map[string]any{"limit": 1000}, "", actor)
	if got.Error != nil {
		t.Fatalf("public ready refused: %+v", got)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil || page.NextCursor != "" {
		t.Fatalf("ready was not a complete membership observation: %s error=%v", got.Result, err)
	}
	ids := make([]string, 0, len(page.Items))
	for _, row := range page.Items {
		ids = append(ids, row.ID)
	}
	sort.Strings(ids)
	sort.Strings(want)
	if len(ids) != len(want) || len(ids) != 0 && !reflect.DeepEqual(ids, want) {
		t.Fatalf("public ready membership=%v want=%v", ids, want)
	}
}
